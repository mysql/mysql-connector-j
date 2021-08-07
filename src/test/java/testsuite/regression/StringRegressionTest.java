/*
 * Copyright (c) 2002, 2021, Oracle and/or its affiliates.
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

package testsuite.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Clob;
import java.sql.Connection;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.util.Base64Decoder;
import com.mysql.cj.util.SearchMode;
import com.mysql.cj.util.StringUtils;

import testsuite.BaseTestCase;

/**
 * Tests for regressions of bugs in String handling in the driver.
 */
public class StringRegressionTest extends BaseTestCase {

    /**
     * Tests newline being treated correctly.
     * 
     * @throws Exception
     */
    @Test
    public void testNewlines() throws Exception {
        String newlineStr = "Foo\nBar\n\rBaz";

        createTable("newlineRegressTest", "(field1 MEDIUMTEXT)");

        this.stmt.executeUpdate("INSERT INTO newlineRegressTest VALUES ('" + newlineStr + "')");
        this.pstmt = this.conn.prepareStatement("INSERT INTO newlineRegressTest VALUES (?)");
        this.pstmt.setString(1, newlineStr);
        this.pstmt.executeUpdate();

        this.rs = this.stmt.executeQuery("SELECT * FROM newlineRegressTest");

        while (this.rs.next()) {
            assertTrue(this.rs.getString(1).equals(newlineStr));
        }
    }

    /**
     * Tests that single-byte character conversion works correctly.
     * 
     * @throws Exception
     */
    // TODO: Use Unicode Literal escapes for this, for now, this test is broken :(
    /*
     * public void testSingleByteConversion() throws Exception {
     * testConversionForString("latin1", "��� ����");
     * testConversionForString("latin1", "Kaarle ��nis Ilmari");
     * testConversionForString("latin1", "������������������"); }
     */

    /**
     * Tests fix for BUG#7601, '+' duplicated in fixDecimalExponent().
     * 
     * @throws Exception
     */
    @Test
    public void testBug7601() throws Exception {
        assertTrue("1.5E+7".equals(StringUtils.fixDecimalExponent("1.5E+7")));
        assertTrue("1.5E-7".equals(StringUtils.fixDecimalExponent("1.5E-7")));
        assertTrue("1.5E+7".equals(StringUtils.fixDecimalExponent("1.5E7")));
    }

    @Test
    public void testBug11629() throws Exception {
        class TeeByteArrayOutputStream extends ByteArrayOutputStream {
            PrintStream branch;
            StackTraceElement[] callStackTrace = null;

            public TeeByteArrayOutputStream(PrintStream branch) {
                this.branch = branch;
            }

            @Override
            public void write(int b) {
                this.branch.write(b);
                super.write(b);
                setCallStackTrace();
            }

            @Override
            public void write(byte[] b) throws IOException {
                this.branch.write(b);
                super.write(b);
                setCallStackTrace();
            }

            @Override
            public void write(byte[] b, int off, int len) {
                this.branch.write(b, off, len);
                super.write(b, off, len);
                setCallStackTrace();
            }

            private void setCallStackTrace() {
                if (this.callStackTrace == null) {
                    this.callStackTrace = Thread.currentThread().getStackTrace();
                }
            }

            public void printCallStackTrace() {
                if (this.callStackTrace != null) {
                    for (StackTraceElement ste : this.callStackTrace) {
                        this.branch.println(">>> " + ste.toString());
                    }
                }
            }
        }

        PrintStream oldOut = System.out;
        PrintStream oldError = System.err;

        try {
            TeeByteArrayOutputStream bOut = new TeeByteArrayOutputStream(System.out);
            PrintStream newOut = new PrintStream(bOut);
            System.out.flush();
            System.setOut(newOut);

            TeeByteArrayOutputStream bErr = new TeeByteArrayOutputStream(System.err);
            PrintStream newErr = new PrintStream(bErr);
            System.err.flush();
            System.setErr(newErr);

            Properties props = new Properties();
            props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "utf8");
            getConnectionWithProps(props).close();
            System.setOut(oldOut);
            System.setErr(oldError);

            bOut.printCallStackTrace();
            bErr.printCallStackTrace();

            String withExclaims = new String(bOut.toByteArray());
            assertTrue(withExclaims.indexOf("!") == -1, "Unexpected: '" + withExclaims + "'");
            assertTrue(withExclaims.length() == 0, "Unexpected: '" + withExclaims + "'"); // to catch any other
            bOut.close();

            withExclaims = new String(bErr.toByteArray());
            assertTrue(withExclaims.indexOf("!") == -1, "Unexpected: '" + withExclaims + "'");
            assertTrue(withExclaims.length() == 0, "Unexpected: '" + withExclaims + "'"); // to catch any other
            bErr.close();
        } finally {
            System.setOut(oldOut);
            System.setErr(oldError);
        }
    }

    /**
     * Tests fix for BUG#11614 - StringUtils.getBytes() doesn't work when using multibyte character encodings and a length in _characters_ is specified.
     * 
     * @throws Exception
     */
    @Test
    public void testBug11614() throws Exception {
        createTable("testBug11614",
                "(`id` INTEGER UNSIGNED NOT NULL AUTO_INCREMENT, `text` TEXT NOT NULL," + "PRIMARY KEY(`id`)) CHARACTER SET utf8 COLLATE utf8_general_ci");

        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "utf8");

        Connection utf8Conn = null;

        try {
            utf8Conn = getConnectionWithProps(props);

            utf8Conn.createStatement().executeUpdate("INSERT INTO testBug11614  (`id`,`text`) values (1,'')");
            this.rs = utf8Conn.createStatement().executeQuery("SELECT `text` FROM testBug11614 WHERE id=1");
            assertTrue(this.rs.next());

            Clob c = this.rs.getClob(1);
            c.truncate(0);
            int blockSize = 8192;
            int sizeToTest = blockSize + 100;

            StringBuilder blockBuf = new StringBuilder(sizeToTest);

            for (int i = 0; i < sizeToTest; i++) {
                blockBuf.append('\u00f6');
            }

            String valueToTest = blockBuf.toString();

            c.setString(1, valueToTest);
            this.pstmt = utf8Conn.prepareStatement("UPDATE testBug11614 SET `text` = ? WHERE id=1");
            this.pstmt.setClob(1, c);
            this.pstmt.executeUpdate();
            this.pstmt.close();

            String fromDatabase = getSingleIndexedValueWithQuery(utf8Conn, 1, "SELECT `text` FROM testBug11614").toString();
            assertEquals(valueToTest, fromDatabase);
        } finally {
            if (utf8Conn != null) {
                utf8Conn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#25047 - StringUtils.indexOfIgnoreCaseRespectQuotes() isn't case-insensitive on the first character of the target.
     * 
     * UPD: Method StringUtils.indexOfIgnoreCaseRespectQuotes() was replaced by StringUtils.indexOfIgnoreCase()
     * 
     * @throws Exception
     */
    @Test
    public void testBug25047() throws Exception {
        assertEquals(26, StringUtils.indexOfIgnoreCase(0, "insert into Test (TestID) values (?)", "VALUES", "`", "`", SearchMode.__MRK_COM_MYM_HNT_WS));
        assertEquals(26, StringUtils.indexOfIgnoreCase(0, "insert into Test (TestID) VALUES (?)", "values", "`", "`", SearchMode.__MRK_COM_MYM_HNT_WS));

        assertEquals(StringUtils.indexOfIgnoreCase(0, "insert into Test (TestID) values (?)", "VALUES", "`", "`", SearchMode.__MRK_COM_MYM_HNT_WS),
                StringUtils.indexOfIgnoreCase(0, "insert into Test (TestID) VALUES (?)", "VALUES", "`", "`", SearchMode.__MRK_COM_MYM_HNT_WS));
        assertEquals(StringUtils.indexOfIgnoreCase(0, "insert into Test (TestID) values (?)", "values", "`", "`", SearchMode.__MRK_COM_MYM_HNT_WS),
                StringUtils.indexOfIgnoreCase(0, "insert into Test (TestID) VALUES (?)", "values", "`", "`", SearchMode.__MRK_COM_MYM_HNT_WS));
    }

    /**
     * Tests fix for BUG#64731 - StringUtils.getBytesWrapped throws StringIndexOutOfBoundsException.
     * 
     * @throws Exception
     */
    @Test
    public void testBug64731() throws Exception {
        byte[] data = StringUtils.getBytesWrapped("0f0f0702", '\'', '\'', "gbk");
        assertTrue(true, StringUtils.toString(data));
    }

    @Test
    public void testBase64Decoder() throws Exception {
        testBase64DecoderItem(
                "TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyByZWFzb24sIGJ1dCBieSB0\n"
                        + "aGlzIHNpbmd1bGFyIHBhc3Npb24gZnJvbSBvdGhlciBhbmltYWxzLCB3aGljaCBpcyBhIGx1\n"
                        + "c3Qgb2YgdGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodCBpbiB0\n"
                        + "aGUgY29udGludWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xlZGdl\n"
                        + "LCBleGNlZWRzIHRoZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBwbGVhc3VyZS4=",

                "Man is distinguished, not only by his reason, but by this singular passion"
                        + " from other animals, which is a lust of the mind, that by a perseverance of"
                        + " delight in the continued and indefatigable generation of knowledge, exceeds the short vehemence of any carnal pleasure.");

        testBase64DecoderItem(
                "TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyByZWFzb24sIGJ1dCBieSB0\n"
                        + "aGlzIHNpbmd1bGFyIHBhc3Npb24gZnJvbSBvdGhlciBhbmltYWxzLCB3aGljaCBpcyBhIGx1\n"
                        + "c3Qgb2YgdGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodCBpbiB0\n"
                        + "aGUgY29udGludWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xlZGdl\n"
                        + "LCBleGNlZWRzIHRoZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBwbGVhc3VyZQ==",

                "Man is distinguished, not only by his reason, but by this singular passion"
                        + " from other animals, which is a lust of the mind, that by a perseverance of"
                        + " delight in the continued and indefatigable generation of knowledge, exceeds the short vehemence of any carnal pleasure");

        testBase64DecoderItem(
                "TWFuIGlzIGRpc3Rpbmd1aXNoZWQsIG5vdCBvbmx5IGJ5IGhpcyByZWFzb24sIGJ1dCBieSB0\n"
                        + "aGlzIHNpbmd1bGFyIHBhc3Npb24gZnJvbSBvdGhlciBhbmltYWxzLCB3aGljaCBpcyBhIGx1\n"
                        + "c3Qgb2YgdGhlIG1pbmQsIHRoYXQgYnkgYSBwZXJzZXZlcmFuY2Ugb2YgZGVsaWdodCBpbiB0\n"
                        + "aGUgY29udGludWVkIGFuZCBpbmRlZmF0aWdhYmxlIGdlbmVyYXRpb24gb2Yga25vd2xlZGdl\n"
                        + "LCBleGNlZWRzIHRoZSBzaG9ydCB2ZWhlbWVuY2Ugb2YgYW55IGNhcm5hbCBwbGVhc3Vy",

                "Man is distinguished, not only by his reason, but by this singular passion"
                        + " from other animals, which is a lust of the mind, that by a perseverance of"
                        + " delight in the continued and indefatigable generation of knowledge, exceeds the short vehemence of any carnal pleasur");
    }

    private void testBase64DecoderItem(String source, String expected) throws Exception {
        assertEquals(expected, new String(Base64Decoder.decode(source.getBytes(), 0, source.length())));
    }
}
