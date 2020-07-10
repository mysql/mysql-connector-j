/*
 * Copyright (c) 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package testsuite.x.devapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.FilterInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.zip.InflaterInputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyDefinitions.Compression;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.x.SyncFlushDeflaterOutputStream;
import com.mysql.cj.xdevapi.AddResult;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.JsonParser;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SqlResult;

public class CompressionTest extends DevApiBaseTestCase {
    private final Properties compressFreeTestProperties = (Properties) this.testProperties.clone();
    private String compressFreeBaseUrl = this.baseUrl;

    private static final String shortData = "{\"data\": \"[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]\"}";
    private static final String longData = "{\"data\": \"[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]"
            + "[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]"
            + "[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]"
            + "[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]\"}";
    private static final DbDoc shortDataDoc = JsonParser.parseDoc(shortData);
    private static final DbDoc longDataDoc = JsonParser.parseDoc(longData);

    private CompressionCounters counters = null;

    private class CompressionCounters {
        private static final String MYSQLX_BYTES_RECEIVED = "Mysqlx_bytes_received";
        private static final String MYSQLX_BYTES_RECEIVED_COMPRESSED_PAYLOAD = "Mysqlx_bytes_received_compressed_payload";
        private static final String MYSQLX_BYTES_RECEIVED_UNCOMPRESSED_FRAME = "Mysqlx_bytes_received_uncompressed_frame";
        private static final String MYSQLX_BYTES_SENT = "Mysqlx_bytes_sent";
        private static final String MYSQLX_BYTES_SENT_COMPRESSED_PAYLOAD = "Mysqlx_bytes_sent_compressed_payload";
        private static final String MYSQLX_BYTES_SENT_UNCOMPRESSED_FRAME = "Mysqlx_bytes_sent_uncompressed_frame";

        private final Map<String, Integer> countersMap;
        private final Map<String, Integer> deltasMap;

        private Connection conn;

        CompressionCounters() {
            this.countersMap = new HashMap<>();
            this.deltasMap = new HashMap<>();
            Arrays.asList(MYSQLX_BYTES_RECEIVED, MYSQLX_BYTES_RECEIVED_COMPRESSED_PAYLOAD, MYSQLX_BYTES_RECEIVED_UNCOMPRESSED_FRAME, MYSQLX_BYTES_SENT,
                    MYSQLX_BYTES_SENT_COMPRESSED_PAYLOAD, MYSQLX_BYTES_SENT_UNCOMPRESSED_FRAME).stream().peek(e -> this.countersMap.put(e, 0))
                    .forEach(e -> this.deltasMap.put(e, 0));

            // Counters must be consulted using a classic connection due to Bug#30121765.
            String classicUrl = System.getProperty(PropertyDefinitions.SYSP_testsuite_url);
            Driver driver = null;
            try {
                driver = new com.mysql.cj.jdbc.NonRegisteringDriver();
                this.conn = driver.connect(classicUrl, null);
            } catch (SQLException e) {
                fail(e.getMessage());
            }

            resetCounters();
        }

        boolean resetCounters() {
            try {
                Thread.sleep(250); // Allow the server some time to update counters.
                ResultSet rs = this.conn.createStatement().executeQuery(
                        "SHOW GLOBAL STATUS WHERE Variable_name IN " + this.countersMap.keySet().stream().collect(Collectors.joining("', '", "('", "')")));
                while (rs.next()) {
                    this.deltasMap.put(rs.getString(1), rs.getInt(2) - this.countersMap.get(rs.getString(1)));
                    this.countersMap.put(rs.getString(1), rs.getInt(2));
                }
            } catch (SQLException | InterruptedException e) {
                fail(e.getMessage());
            }
            return this.deltasMap.get(MYSQLX_BYTES_RECEIVED) > 0 || this.deltasMap.get(MYSQLX_BYTES_SENT) > 0;
        }

        boolean downlinkCompressionUsed() {
            return this.deltasMap.get(MYSQLX_BYTES_SENT_COMPRESSED_PAYLOAD) > 0;
        }

        boolean uplinkCompressionUsed() {
            return this.deltasMap.get(MYSQLX_BYTES_RECEIVED_COMPRESSED_PAYLOAD) > 0;
        }

        boolean usedCompression() {
            return downlinkCompressionUsed() || uplinkCompressionUsed();
        }

        void releaseResources() {
            try {
                this.conn.close();
            } catch (SQLException e) {
                fail(e.getMessage());
            }
        }
    }

    private CompressionSettings compressionSettings = null;

    private class CompressionSettings {
        private final boolean serverSupportsCompression;
        private final String compressionAlgorithms;

        CompressionSettings() {
            SqlResult res = CompressionTest.this.session.sql("SHOW VARIABLES LIKE 'mysqlx_compression_algorithms'").execute();
            Row row = res.fetchOne();
            this.compressionAlgorithms = row != null ? row.getString(1) : null;
            this.serverSupportsCompression = row != null;
        }

        boolean serverSupportsCompression() {
            return this.serverSupportsCompression;
        }

        void setCompressionAlgorithms(String algorithms) {
            if (serverSupportsCompression() && algorithms != null) {
                CompressionTest.this.session.sql("SET GLOBAL mysqlx_compression_algorithms='" + algorithms + "'").execute();
            }
        }

        void resetCompressionSettings() {
            setCompressionAlgorithms(this.compressionAlgorithms);
        }
    }

    public static class TestInflaterInputStream extends InflaterInputStream {
        public static boolean instantiatedAtLeastOnce = false;

        public TestInflaterInputStream(InputStream in) {
            super(in);
            instantiatedAtLeastOnce = true;
        }
    }

    public static class TestSyncFlushDeflaterOutputStream extends SyncFlushDeflaterOutputStream {
        public static boolean instantiatedAtLeastOnce = false;

        public TestSyncFlushDeflaterOutputStream(OutputStream out) {
            super(out);
            instantiatedAtLeastOnce = true;
        }
    }

    public static class TestInputStream extends FilterInputStream {
        public static boolean instantiatedAtLeastOnce = false;

        public TestInputStream(InputStream in) {
            super(in);
            instantiatedAtLeastOnce = true;
        }
    }

    private String compressionAlgorithmAgreed(Session sess) {
        RowResult rows = sess.sql("SHOW STATUS LIKE 'Mysqlx_compression_algorithm'").execute();
        Row r;
        if ((r = rows.fetchOne()) != null) {
            return r.getString(1).toLowerCase();
        }
        return null;
    }

    private boolean isCompressionEnabled(Session sess) {
        if (mysqlVersionMeetsMinimum(new ServerVersion(8, 0, 20))) {
            return compressionAlgorithmAgreed(sess).length() > 0;
        }
        // Won't be accurate if not enough data was transmitted or if counters weren't properly reset.
        return this.counters.usedCompression();
    }

    @BeforeEach
    public void setupCompressionTest() {
        if (setupTestSession()) {
            this.compressFreeTestProperties.remove(PropertyKey.xdevapiCompression.getKeyName());
            this.compressFreeTestProperties.remove(PropertyKey.xdevapiCompressionAlgorithms.getKeyName());
            this.compressFreeTestProperties.remove(PropertyKey.xdevapiCompressionExtensions.getKeyName());

            this.compressFreeBaseUrl = this.baseUrl;
            this.compressFreeBaseUrl = this.compressFreeBaseUrl.replaceAll(PropertyKey.xdevapiCompression.getKeyName() + "=",
                    PropertyKey.xdevapiCompression.getKeyName() + "VOID=");
            this.compressFreeBaseUrl = this.compressFreeBaseUrl.replaceAll(PropertyKey.xdevapiCompressionAlgorithms.getKeyName() + "=",
                    PropertyKey.xdevapiCompressionAlgorithms.getKeyName() + "VOID=");
            this.compressFreeBaseUrl = this.compressFreeBaseUrl.replaceAll(PropertyKey.xdevapiCompressionExtensions.getKeyName() + "=",
                    PropertyKey.xdevapiCompressionExtensions.getKeyName() + "VOID=");
            if (!this.compressFreeBaseUrl.contains("?")) {
                this.compressFreeBaseUrl += "?";
            }

            this.counters = new CompressionCounters();
            this.compressionSettings = new CompressionSettings();
        }
    }

    @AfterEach
    public void teardownCompressionTest() {
        if (this.counters != null) {
            this.counters.releaseResources();
        }
        if (this.compressionSettings != null) {
            this.compressionSettings.resetCompressionSettings();
        }
    }

    /**
     * Tests compression negotiation, with compression algorithms restricted at server side.
     */
    @Test
    public void compressionNegotiationServerSideRestricted() {
        if (!this.isSetForXTests || !this.compressionSettings.serverSupportsCompression()) {
            return;
        }

        String[] algorithms = new String[] { "", "zstd_stream", "lz4_message", "deflate_stream" };
        boolean[] expected = new boolean[] { false, false, false, true }; // Only "deflate_stream" is supported by default.

        for (int i = 0; i < algorithms.length; i++) {
            String testCase = "[Algorithm: " + algorithms[i] + "]";
            this.compressionSettings.setCompressionAlgorithms(algorithms[i]);

            dropCollection("compressionNegotiation");
            this.schema.createCollection("compressionNegotiation");

            Session testSession = this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.PREFERRED));
            Collection col = testSession.getDefaultSchema().getCollection("compressionNegotiation");

            assertEquals(expected[i] ? algorithms[i] : "", compressionAlgorithmAgreed(testSession));
            assertTrue(this.counters.resetCounters(), testCase);

            AddResult res = col.add(longData).execute(); // Enough bytes to trigger compression.
            assertEquals(1, res.getAffectedItemsCount(), testCase);
            String docId = res.getGeneratedIds().get(0);

            assertTrue(this.counters.resetCounters(), testCase);
            if (expected[i]) {
                assertTrue(this.counters.downlinkCompressionUsed(), testCase); // Server compresses small messages anyway.
                assertTrue(this.counters.uplinkCompressionUsed(), testCase);
            } else {
                assertFalse(this.counters.downlinkCompressionUsed(), testCase);
                assertFalse(this.counters.uplinkCompressionUsed(), testCase);
            }

            DbDoc doc = col.getOne(docId);
            assertNotNull(doc, testCase);
            assertEquals(longDataDoc.get("data").toString(), doc.get("data").toString(), testCase);

            assertTrue(this.counters.resetCounters(), testCase);
            if (expected[i]) {
                assertTrue(this.counters.downlinkCompressionUsed(), testCase);
                assertFalse(this.counters.uplinkCompressionUsed(), testCase);
            } else {
                assertFalse(this.counters.downlinkCompressionUsed(), testCase);
                assertFalse(this.counters.uplinkCompressionUsed(), testCase);
            }

            dropCollection("compressionNegotiation");
            testSession.close();
        }
    }

    /**
     * Tests compression negotiation with customized compression algorithms selections.
     * "deflate_stream" is always selected as only "deflate_stream" is natively supported.
     */
    @Test
    public void compressionNegotiationClientSideSelectionNativelySupported() {
        if (!this.isSetForXTests || !this.compressionSettings.serverSupportsCompression()) {
            return;
        }

        /*
         * Default negotiation is always "deflate_stream" as only "deflate_stream" is supported by default.
         */
        String[] algorithmsOpts = new String[] { "zstd_stream,lz4_message,deflate_stream", "zstd_stream,deflate_stream,lz4_message",
                "deflate_stream,zstd_stream,lz4_message" };
        for (String algorithms : algorithmsOpts) {
            Session testSession = this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, algorithms));

            assertEquals("deflate_stream", compressionAlgorithmAgreed(testSession));
            testSession.close();
        }

    }

    /**
     * Tests compression negotiation with customized compression algorithms selections, forcing the selection of a non-default algorithm (non-"deflate_stream").
     * Additional algorithms are enabled by using a fake InputStream. Creating the session fails because the InputStream is useless, but compression negotiation
     * must succeed and an instance of the fake stream is created.
     */
    @Test
    public void compressionNegotiationClientSideSelectionOtherThanNative() {
        if (!this.isSetForXTests || !this.compressionSettings.serverSupportsCompression()) {
            return;
        }

        String[] algorithmsOpts = new String[] { "zstd_stream,lz4_message,deflate_stream", "lz4_message,zstd_stream,deflate_stream" };
        for (String algorithms : algorithmsOpts) {
            TestInputStream.instantiatedAtLeastOnce = false;
            assertThrows(CJException.class,
                    () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, algorithms) + makeParam(
                            PropertyKey.xdevapiCompressionExtensions, "zstd_stream:" + TestInputStream.class.getName() + ":" + OutputStream.class.getName())));
            assertTrue(TestInputStream.instantiatedAtLeastOnce);

            TestInputStream.instantiatedAtLeastOnce = false;
            assertThrows(CJException.class,
                    () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, algorithms) + makeParam(
                            PropertyKey.xdevapiCompressionExtensions, "lz4_message:" + TestInputStream.class.getName() + ":" + OutputStream.class.getName())));
            assertTrue(TestInputStream.instantiatedAtLeastOnce);
        }
    }

    /**
     * Tests compression negotiation with customized compression algorithms selections, containing known and unknown algorithm identifiers.
     */
    @Test
    public void compressionNegotiationClientSideSelectionUnknownIds() {
        if (!this.isSetForXTests || !this.compressionSettings.serverSupportsCompression()) {
            return;
        }

        String[] algorithmsOpts = new String[] { "foo_message,bar_stream,deflate_stream", "foo_message,deflate_stream,bar_stream",
                "deflate_stream,foo_message,bar_stream" };
        for (String algorithms : algorithmsOpts) {
            Session testSession = this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, algorithms)
                    + makeParam(PropertyKey.xdevapiCompressionExtensions, "foo_message:" + InputStream.class.getName() + ":" + OutputStream.class.getName()
                            + "," + "bar_stream:" + InputStream.class.getName() + ":" + OutputStream.class.getName()));

            assertEquals("deflate_stream", compressionAlgorithmAgreed(testSession));
            testSession.close();
        }
    }

    /**
     * Tests compression negotiation with customized compression algorithms selections, with empty or only unknown algorithm identifiers.
     */
    @Test
    public void compressionNegotiationClientSideSelectionNoCommon() {
        if (!this.isSetForXTests || !this.compressionSettings.serverSupportsCompression()) {
            return;
        }

        String[] algorithmsOpts = new String[] { "", "foo_message,bar_stream" };
        for (String algorithms : algorithmsOpts) {

            // Default xdevapi.compression (PREFERRED)
            Session testSession = this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, algorithms)
                    + makeParam(PropertyKey.xdevapiCompressionExtensions, "foo_message:" + InputStream.class.getName() + ":" + OutputStream.class.getName()
                            + "," + "bar_stream:" + InputStream.class.getName() + ":" + OutputStream.class.getName()));
            assertEquals("", compressionAlgorithmAgreed(testSession));
            testSession.close();

            // xdevapi.compression=PREFERRED
            testSession = this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, "PREFERRED")
                    + makeParam(PropertyKey.xdevapiCompressionAlgorithms, algorithms)
                    + makeParam(PropertyKey.xdevapiCompressionExtensions, "foo_message:" + InputStream.class.getName() + ":" + OutputStream.class.getName()
                            + "," + "bar_stream:" + InputStream.class.getName() + ":" + OutputStream.class.getName()));
            assertEquals("", compressionAlgorithmAgreed(testSession));
            testSession.close();

            // xdevapi.compression=DISABLED
            testSession = this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, "DISABLED")
                    + makeParam(PropertyKey.xdevapiCompressionAlgorithms, algorithms)
                    + makeParam(PropertyKey.xdevapiCompressionExtensions, "foo_message:" + InputStream.class.getName() + ":" + OutputStream.class.getName()
                            + "," + "bar_stream:" + InputStream.class.getName() + ":" + OutputStream.class.getName()));
            assertEquals("", compressionAlgorithmAgreed(testSession));
            testSession.close();

            // xdevapi.compression=REQUIRED
            assertThrows(CJException.class,
                    () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, "REQUIRED")
                            + makeParam(PropertyKey.xdevapiCompressionAlgorithms, algorithms)
                            + makeParam(PropertyKey.xdevapiCompressionExtensions, "foo_message:" + InputStream.class.getName() + ":"
                                    + OutputStream.class.getName() + "," + "bar_stream:" + InputStream.class.getName() + ":" + OutputStream.class.getName())));
        }
    }

    /**
     * Tests compression disabled by connection option.
     */
    @Test
    public void compressionDisabled() {
        if (!this.isSetForXTests || !this.compressionSettings.serverSupportsCompression()) {
            return;
        }

        dropCollection("compressionDisabled");
        this.schema.createCollection("compressionDisabled");

        Session testSession = this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.DISABLED));
        Collection col = testSession.getDefaultSchema().getCollection("compressionDisabled");

        assertFalse(isCompressionEnabled(testSession));
        assertTrue(this.counters.resetCounters());

        AddResult res = col.add("{\"foo\": \"bar\"}",
                "{\"baz\": \"[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]"
                        + "[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]"
                        + "[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]"
                        + "[[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]][[ABCDEFGHIJKLMNOPQRSTUVWXYZ]]\"}")
                .execute(); // Enough bytes to trigger compression.
        assertEquals(2, res.getAffectedItemsCount());

        assertTrue(this.counters.resetCounters());
        assertFalse(this.counters.usedCompression());

        DocResult docs = col.find().execute();
        assertEquals(2, docs.count());

        assertTrue(this.counters.resetCounters());
        assertFalse(this.counters.usedCompression());

        dropCollection("compressionDisabled");
        testSession.close();
    }

    /**
     * Tests downlink compression using each one of the compression options.
     */
    @Test
    public void downlinkCompression() {
        if (!this.isSetForXTests || !this.compressionSettings.serverSupportsCompression()) {
            return;
        }

        dropCollection("downlinkCompression");
        this.schema.createCollection("downlinkCompression");

        Session testSession = this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.DISABLED));
        Collection col = testSession.getDefaultSchema().getCollection("downlinkCompression");

        assertTrue(this.counters.resetCounters());

        AddResult res = col.add(longData).execute(); // Enough bytes to trigger compression.
        assertEquals(1, res.getAffectedItemsCount());

        assertTrue(this.counters.resetCounters());
        assertFalse(this.counters.usedCompression());

        for (Compression compr : Compression.values()) {
            testSession.close();
            // Replace DISABLED by default value.
            String testCase = "[Compression: " + (compr == Compression.DISABLED ? "<default>" : compr) + "]";
            testSession = this.fact
                    .getSession(this.compressFreeBaseUrl + (compr == Compression.DISABLED ? "" : makeParam(PropertyKey.xdevapiCompression, compr)));
            col = testSession.getDefaultSchema().getCollection("downlinkCompression");

            DocResult docs = col.find().execute();
            assertEquals(1, docs.count(), testCase);
            assertEquals(longDataDoc.get("data").toString(), docs.fetchOne().get("data").toString(), testCase);

            assertTrue(this.counters.resetCounters(), testCase);
            assertFalse(this.counters.uplinkCompressionUsed(), testCase);
            assertTrue(this.counters.downlinkCompressionUsed(), testCase);
        }

        dropCollection("downlinkCompression");
        testSession.close();
    }

    /**
     * Tests uplink compression using each one of the compression options.
     */
    @Test
    public void uplinkCompression() {
        if (!this.isSetForXTests || !this.compressionSettings.serverSupportsCompression()) {
            return;
        }

        dropCollection("uplinkCompression");

        for (Compression compr : Compression.values()) {
            this.schema.createCollection("uplinkCompression");

            // Replace DISABLED by default value.
            String testCase = "[Compression: " + (compr == Compression.DISABLED ? "<default>" : compr) + "]";
            Session testSession = this.fact
                    .getSession(this.compressFreeBaseUrl + (compr == Compression.DISABLED ? "" : makeParam(PropertyKey.xdevapiCompression, compr)));
            Collection col = testSession.getDefaultSchema().getCollection("uplinkCompression");

            assertTrue(this.counters.resetCounters(), testCase);

            AddResult res = col.add(longData).execute(); // Enough bytes to trigger compression.
            assertEquals(1, res.getAffectedItemsCount(), testCase);

            assertTrue(this.counters.resetCounters(), testCase);
            assertTrue(this.counters.downlinkCompressionUsed(), testCase); // Server compresses small messages anyway.
            assertTrue(this.counters.uplinkCompressionUsed(), testCase);

            testSession.close();
            testSession = this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.DISABLED));
            col = testSession.getDefaultSchema().getCollection("uplinkCompression");

            DocResult docs = col.find().execute();
            assertEquals(1, docs.count(), testCase);
            assertEquals(longDataDoc.get("data").toString(), docs.fetchOne().get("data").toString(), testCase);

            assertTrue(this.counters.resetCounters(), testCase);
            assertFalse(this.counters.usedCompression(), testCase);

            dropCollection("uplinkCompression");
            testSession.close();
        }
    }

    /**
     * Tests the compression threshold applied to uplink data.
     */
    @Test
    public void compressionThreshold() {
        if (!this.isSetForXTests || !this.compressionSettings.serverSupportsCompression()) {
            return;
        }

        dropCollection("compressionThreshold");
        this.schema.createCollection("compressionThreshold");

        Session testSession = this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.REQUIRED));
        Collection col = testSession.getDefaultSchema().getCollection("compressionThreshold");

        assertTrue(this.counters.resetCounters());

        AddResult res = col.add(shortData).execute(); // Not enough bytes to trigger uplink compression.
        assertEquals(1, res.getAffectedItemsCount());
        String docId = res.getGeneratedIds().get(0);

        assertTrue(this.counters.resetCounters());
        assertTrue(this.counters.downlinkCompressionUsed()); // Server compresses small messages anyway.
        assertFalse(this.counters.uplinkCompressionUsed());

        DbDoc doc = col.getOne(docId);
        assertNotNull(doc);
        assertEquals(shortDataDoc.get("data").toString(), doc.get("data").toString());

        assertTrue(this.counters.resetCounters());
        assertTrue(this.counters.downlinkCompressionUsed()); // Server compresses small messages anyway.
        assertFalse(this.counters.uplinkCompressionUsed());

        res = col.add(longData).execute(); // Enough bytes to trigger uplink compression.
        assertEquals(1, res.getAffectedItemsCount());
        docId = res.getGeneratedIds().get(0);

        assertTrue(this.counters.resetCounters());
        assertTrue(this.counters.downlinkCompressionUsed());
        assertTrue(this.counters.uplinkCompressionUsed());

        doc = col.getOne(docId);
        assertNotNull(doc);
        assertEquals(longDataDoc.get("data").toString(), doc.get("data").toString());

        assertTrue(this.counters.resetCounters());
        assertTrue(this.counters.downlinkCompressionUsed());
        assertFalse(this.counters.uplinkCompressionUsed());

        dropCollection("compressionThreshold");
        testSession.close();
    }

    /**
     * Tests invalid compression option values and returned error messages.
     */
    @Test
    public void invalidCompressionOptions() {
        if (!this.isSetForXTests || !this.compressionSettings.serverSupportsCompression()) {
            return;
        }

        assertThrows(WrongArgumentException.class,
                "The connection property 'xdevapi.compression' acceptable values are: 'DISABLED', 'PREFERRED' or 'REQUIRED'\\. The value 'true' is not acceptable\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, "true")));
        assertThrows(WrongArgumentException.class,
                "The connection property 'xdevapi.compression' acceptable values are: 'DISABLED', 'PREFERRED' or 'REQUIRED'\\. The value 'false' is not acceptable\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, "false")));
        assertThrows(WrongArgumentException.class,
                "The connection property 'xdevapi.compression' acceptable values are: 'DISABLED', 'PREFERRED' or 'REQUIRED'\\. The value '' is not acceptable\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, "")));

        assertThrows(WrongArgumentException.class,
                "The property \"xdevapi.compression-extensions\" must be a comma separated list of colon separated triplets\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionExtensions, "test")));
        assertThrows(WrongArgumentException.class,
                "The property \"xdevapi.compression-extensions\" must be a comma separated list of colon separated triplets\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.PREFERRED)
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "test")));
        assertThrows(WrongArgumentException.class,
                "The property \"xdevapi.compression-extensions\" must be a comma separated list of colon separated triplets\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.REQUIRED)
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "test")));

        assertThrows(WrongArgumentException.class,
                "The property \"xdevapi.compression-extensions\" must be a comma separated list of colon separated triplets\\.", () -> this.fact
                        .getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionExtensions, "test,anInputStream,anOutputStream")));
        assertThrows(WrongArgumentException.class,
                "The property \"xdevapi.compression-extensions\" must be a comma separated list of colon separated triplets\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.PREFERRED)
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "test,anInputStream,anOutputStream")));
        assertThrows(WrongArgumentException.class,
                "The property \"xdevapi.compression-extensions\" must be a comma separated list of colon separated triplets\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.REQUIRED)
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "test,anInputStream,anOutputStream")));

        assertThrows(WrongArgumentException.class, "Error loading the class anInputStream\\.", () -> this.fact
                .getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionExtensions, "deflate_stream:anInputStream:anOutputStream")));
        assertThrows(WrongArgumentException.class, "Error loading the class anInputStream\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.PREFERRED)
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "deflate_stream:anInputStream:anOutputStream")));
        assertThrows(WrongArgumentException.class, "Error loading the class anInputStream\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.REQUIRED)
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "deflate_stream:anInputStream:anOutputStream")));

        assertThrows(WrongArgumentException.class, "Error loading the class anOutputStream\\.", () -> this.fact.getSession(
                this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionExtensions, "deflate_stream:java.io.InputStream:anOutputStream")));
        assertThrows(WrongArgumentException.class, "Error loading the class anOutputStream\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.PREFERRED)
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "deflate_stream:java.io.InputStream:anOutputStream")));
        assertThrows(WrongArgumentException.class, "Error loading the class anOutputStream\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.REQUIRED)
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "deflate_stream:java.io.InputStream:anOutputStream")));

        assertThrows(WrongArgumentException.class,
                "Incorrect compression algorithm designation 'test'. The compression algorithm must be identified by \"name_mode\"\\.",
                () -> this.fact.getSession(
                        this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionExtensions, "test:java.io.InputStream:java.io.OutputStream")));
        assertThrows(WrongArgumentException.class,
                "Incorrect compression algorithm designation 'test'. The compression algorithm must be identified by \"name_mode\"\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.PREFERRED)
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "test:java.io.InputStream:java.io.OutputStream")));
        assertThrows(WrongArgumentException.class,
                "Incorrect compression algorithm designation 'test'. The compression algorithm must be identified by \"name_mode\"\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.REQUIRED)
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "test:java.io.InputStream:java.io.OutputStream")));

        assertThrows(WrongArgumentException.class, "Unknown or unsupported compression mode 'wrong'\\.", () -> this.fact.getSession(
                this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionExtensions, "test_wrong:java.io.InputStream:java.io.OutputStream")));
        assertThrows(WrongArgumentException.class, "Unknown or unsupported compression mode 'wrong'\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.PREFERRED)
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "test_wrong:java.io.InputStream:java.io.OutputStream")));
        assertThrows(WrongArgumentException.class, "Unknown or unsupported compression mode 'wrong'\\.",
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.REQUIRED)
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "test_wrong:java.io.InputStream:java.io.OutputStream")));
    }

    /**
     * Tests valid compression extensions option.
     */
    @Test
    public void validCompressionExtensionsOption() {
        if (!this.isSetForXTests || !this.compressionSettings.serverSupportsCompression()) {
            return;
        }

        dropCollection("validCompressionAlgorithmOption");
        this.schema.createCollection("validCompressionAlgorithmOption");

        TestInflaterInputStream.instantiatedAtLeastOnce = false;
        TestSyncFlushDeflaterOutputStream.instantiatedAtLeastOnce = false;
        Session testSession = this.fact.getSession(
                this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompression, Compression.REQUIRED) + makeParam(PropertyKey.xdevapiCompressionExtensions,
                        "deflate_stream:" + TestInflaterInputStream.class.getName() + ":" + TestSyncFlushDeflaterOutputStream.class.getName()));

        Collection col = testSession.getDefaultSchema().getCollection("validCompressionAlgorithmOption");

        assertTrue(this.counters.resetCounters());

        AddResult res = col.add(longData).execute(); // Enough bytes to trigger uplink compression.
        assertEquals(1, res.getAffectedItemsCount());
        String docId = res.getGeneratedIds().get(0);

        assertTrue(this.counters.resetCounters());
        assertTrue(this.counters.downlinkCompressionUsed());
        assertTrue(this.counters.uplinkCompressionUsed());

        DbDoc doc = col.getOne(docId);
        assertNotNull(doc);
        assertEquals(longDataDoc.get("data").toString(), doc.get("data").toString());

        assertTrue(this.counters.resetCounters());
        assertTrue(this.counters.downlinkCompressionUsed());
        assertFalse(this.counters.uplinkCompressionUsed());

        dropCollection("validCompressionAlgorithmOption");
        testSession.close();

        assertTrue(TestInflaterInputStream.instantiatedAtLeastOnce);
        assertTrue(TestSyncFlushDeflaterOutputStream.instantiatedAtLeastOnce);
    }

    /**
     * Tests usage of compression algorithms aliases ("zstd", "lz4" and "deflate") on all places compression algorithms identifiers can be used.
     */
    @Test
    public void compressionNegotiationClientSideSelectionWithAliases() {
        if (!this.isSetForXTests || !this.compressionSettings.serverSupportsCompression()) {
            return;
        }

        Session testSession = this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, "zstd,lz4,deflate")
                + makeParam(PropertyKey.xdevapiCompressionExtensions,
                        "deflate:" + TestInflaterInputStream.class.getName() + ":" + TestSyncFlushDeflaterOutputStream.class.getName()));
        assertEquals("deflate_stream", compressionAlgorithmAgreed(testSession));
        testSession.close();

        testSession = this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, "ZSTD,LZ4,DEFLATE")
                + makeParam(PropertyKey.xdevapiCompressionExtensions,
                        "deflate:" + TestInflaterInputStream.class.getName() + ":" + TestSyncFlushDeflaterOutputStream.class.getName()));
        assertEquals("deflate_stream", compressionAlgorithmAgreed(testSession));
        testSession.close();

        testSession = this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, "zstd,lz4,deflate")
                + makeParam(PropertyKey.xdevapiCompressionExtensions,
                        "DEFLATE:" + TestInflaterInputStream.class.getName() + ":" + TestSyncFlushDeflaterOutputStream.class.getName()));
        assertEquals("deflate_stream", compressionAlgorithmAgreed(testSession));
        testSession.close();

        TestInputStream.instantiatedAtLeastOnce = false;
        assertThrows(CJException.class,
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, "zstd,lz4,deflate")
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "lz4:" + TestInputStream.class.getName() + ":" + OutputStream.class.getName())));
        assertTrue(TestInputStream.instantiatedAtLeastOnce);

        TestInputStream.instantiatedAtLeastOnce = false;
        assertThrows(CJException.class,
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, "ZSTD,LZ4,DEFLATE")
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "lz4:" + TestInputStream.class.getName() + ":" + OutputStream.class.getName())));
        assertTrue(TestInputStream.instantiatedAtLeastOnce);

        TestInputStream.instantiatedAtLeastOnce = false;
        assertThrows(CJException.class,
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, "zstd,lz4,deflate")
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "LZ4:" + TestInputStream.class.getName() + ":" + OutputStream.class.getName())));
        assertTrue(TestInputStream.instantiatedAtLeastOnce);

        TestInputStream.instantiatedAtLeastOnce = false;
        assertThrows(CJException.class,
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, "zstd,lz4,deflate")
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "zstd:" + TestInputStream.class.getName() + ":" + OutputStream.class.getName())));
        assertTrue(TestInputStream.instantiatedAtLeastOnce);

        TestInputStream.instantiatedAtLeastOnce = false;
        assertThrows(CJException.class,
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, "ZSTD,LZ4,DEFLATE")
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "zstd:" + TestInputStream.class.getName() + ":" + OutputStream.class.getName())));
        assertTrue(TestInputStream.instantiatedAtLeastOnce);

        TestInputStream.instantiatedAtLeastOnce = false;
        assertThrows(CJException.class,
                () -> this.fact.getSession(this.compressFreeBaseUrl + makeParam(PropertyKey.xdevapiCompressionAlgorithms, "zstd,lz4,deflate")
                        + makeParam(PropertyKey.xdevapiCompressionExtensions, "ZSTD:" + TestInputStream.class.getName() + ":" + OutputStream.class.getName())));
        assertTrue(TestInputStream.instantiatedAtLeastOnce);
    }
}
