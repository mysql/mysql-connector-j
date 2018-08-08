/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.mysql.cj.conf.PropertyKey;

import testsuite.BaseTestCase;

/**
 * Tests fixes for BLOB handling.
 */
public class BlobRegressionTest extends BaseTestCase {
    /**
     * Creates a new BlobRegressionTest.
     * 
     * @param name
     *            name of the test to run
     */
    public BlobRegressionTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(BlobRegressionTest.class);
    }

    /**
     * @throws Exception
     */
    public void testBug2670() throws Exception {
        byte[] blobData = new byte[32];

        for (int i = 0; i < blobData.length; i++) {
            blobData[i] = 1;
        }

        createTable("testBug2670", "(blobField LONGBLOB)");

        PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO testBug2670 (blobField) VALUES (?)");
        pStmt.setBytes(1, blobData);
        pStmt.executeUpdate();

        this.rs = this.stmt.executeQuery("SELECT blobField FROM testBug2670");
        this.rs.next();

        Blob blob = this.rs.getBlob(1);

        //
        // Test mid-point insertion
        //
        blob.setBytes(4, new byte[] { 2, 2, 2, 2 });

        byte[] newBlobData = blob.getBytes(1L, (int) blob.length());

        assertTrue("Blob changed length", blob.length() == blobData.length);

        assertTrue("New data inserted wrongly", ((newBlobData[3] == 2) && (newBlobData[4] == 2) && (newBlobData[5] == 2) && (newBlobData[6] == 2)));

        //
        // Test end-point insertion
        //
        blob.setBytes(32, new byte[] { 2, 2, 2, 2 });

        assertTrue("Blob length should be 3 larger", blob.length() == (blobData.length + 3));

    }

    /**
     * http://bugs.mysql.com/bug.php?id=22891
     * 
     * @throws Exception
     *             ...
     */
    public void testUpdateLongBlobGT16M() throws Exception {
        byte[] blobData = new byte[18 * 1024 * 1024]; // 18M blob

        createTable("testUpdateLongBlob", "(blobField LONGBLOB)");
        this.stmt.executeUpdate("INSERT INTO testUpdateLongBlob (blobField) VALUES (NULL)");

        this.pstmt = this.conn.prepareStatement("UPDATE testUpdateLongBlob SET blobField=?");
        this.pstmt.setBytes(1, blobData);
        try {
            this.pstmt.executeUpdate();
        } catch (SQLException sqlEx) {
            if (sqlEx.getMessage().indexOf("max_allowed_packet") != -1) {
                fail("You need to increase max_allowed_packet to at least 18M before running this test!");
            }
        }
    }

    /**
     * @throws Exception
     */
    public void testUpdatableBlobsWithCharsets() throws Exception {
        byte[] smallBlob = new byte[32];

        for (byte i = 0; i < smallBlob.length; i++) {
            smallBlob[i] = i;
        }

        createTable("testUpdatableBlobsWithCharsets", "(pk INT NOT NULL PRIMARY KEY, field1 BLOB)");
        this.pstmt = this.conn.prepareStatement("INSERT INTO testUpdatableBlobsWithCharsets (pk, field1) VALUES (1, ?)");
        this.pstmt.setBinaryStream(1, new ByteArrayInputStream(smallBlob), smallBlob.length);
        this.pstmt.executeUpdate();

        Statement updStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

        this.rs = updStmt.executeQuery("SELECT pk, field1 FROM testUpdatableBlobsWithCharsets");
        System.out.println(this.rs);
        this.rs.next();

        for (byte i = 0; i < smallBlob.length; i++) {
            smallBlob[i] = (byte) (i + 32);
        }

        this.rs.updateBinaryStream(2, new ByteArrayInputStream(smallBlob), smallBlob.length);
        this.rs.updateRow();

        ResultSet newRs = this.stmt.executeQuery("SELECT field1 FROM testUpdatableBlobsWithCharsets");

        newRs.next();

        byte[] updatedBlob = newRs.getBytes(1);

        for (byte i = 0; i < smallBlob.length; i++) {
            byte origValue = smallBlob[i];
            byte newValue = updatedBlob[i];

            assertTrue("Original byte at position " + i + ", " + origValue + " != new value, " + newValue, origValue == newValue);
        }

    }

    public void testBug5490() throws Exception {

        createTable("testBug5490", "(pk INT NOT NULL PRIMARY KEY, blobField BLOB)");
        String sql = "insert into testBug5490 values(?,?)";

        int blobFileSize = 871;
        File blobFile = newTempBinaryFile("Bug5490", blobFileSize);

        this.pstmt = this.conn.prepareStatement(sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        this.pstmt.setInt(1, 2);
        FileInputStream fis = new FileInputStream(blobFile);
        this.pstmt.setBinaryStream(2, fis, blobFileSize);
        this.pstmt.execute();
        fis.close();
        this.pstmt.close();

        this.rs = this.stmt.executeQuery("SELECT blobField FROM testBug5490");

        this.rs.next();

        byte[] returned = this.rs.getBytes(1);

        assertEquals(blobFileSize, returned.length);

    }

    /**
     * Tests BUG#8096 where emulated locators corrupt binary data when using
     * server-side prepared statements.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug8096() throws Exception {
        int dataSize = 256;

        Properties props = new Properties();
        props.setProperty(PropertyKey.emulateLocators.getKeyName(), "true");
        Connection locatorConn = getConnectionWithProps(props);

        String select = "SELECT ID, 'DATA' AS BLOB_DATA FROM testBug8096 WHERE ID = ?";
        String insert = "INSERT INTO testBug8096 (ID, DATA) VALUES (?, '')";

        String id = "1";
        byte[] testData = new byte[dataSize];

        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) i;
        }

        createTable("testBug8096", "(ID VARCHAR(10) PRIMARY KEY, DATA LONGBLOB)");
        this.pstmt = locatorConn.prepareStatement(insert);
        this.pstmt.setString(1, id);
        this.pstmt.execute();

        this.pstmt = locatorConn.prepareStatement(select);
        this.pstmt.setString(1, id);

        this.rs = this.pstmt.executeQuery();

        if (this.rs.next()) {
            Blob b = this.rs.getBlob("BLOB_DATA");
            b.setBytes(1, testData);
        }

        this.rs.close();
        this.pstmt.close();

        this.pstmt = locatorConn.prepareStatement(select);
        this.pstmt.setString(1, id);

        this.rs = this.pstmt.executeQuery();

        byte[] result = null;
        if (this.rs.next()) {
            Blob b = this.rs.getBlob("BLOB_DATA");

            result = b.getBytes(1, dataSize - 1);
        }

        this.rs.close();
        this.pstmt.close();

        assertNotNull(result);

        for (int i = 0; i < result.length && i < testData.length; i++) {
            // Will print out all of the values that don't match.
            // All negative values will instead be replaced with 63.
            if (result[i] != testData[i]) {
                assertEquals("At position " + i, testData[i], result[i]);
            }
        }
    }

    /**
     * Tests fix for BUG#9040 - PreparedStatement.addBatch() doesn't work with
     * server-side prepared statements and streaming BINARY data.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug9040() throws Exception {

        createTable("testBug9040", "(primary_key int not null primary key, data mediumblob)");

        this.pstmt = this.conn.prepareStatement("replace into testBug9040 (primary_key, data) values(?,?)");

        int primaryKey = 1;
        byte[] data = "First Row".getBytes();
        this.pstmt.setInt(1, primaryKey);
        this.pstmt.setBinaryStream(2, new ByteArrayInputStream(data), data.length);
        this.pstmt.addBatch();

        primaryKey = 2;
        data = "Second Row".getBytes();
        this.pstmt.setInt(1, primaryKey);
        this.pstmt.setBinaryStream(2, new ByteArrayInputStream(data), data.length);
        this.pstmt.addBatch();

        this.pstmt.executeBatch();

    }

    public void testBug10850() throws Exception {
        String tableName = "testBug10850";

        createTable(tableName, "(field1 TEXT)");

        this.pstmt = this.conn.prepareStatement("INSERT INTO " +

                tableName + " VALUES (?)");
        this.pstmt.setCharacterStream(1, new StringReader(""), 0);
        this.pstmt.executeUpdate();

        assertEquals("0", getSingleIndexedValueWithQuery(1, "SELECT LENGTH(field1) FROM " + tableName).toString());
        this.stmt.executeUpdate("TRUNCATE TABLE " + tableName);

        this.pstmt.clearParameters();
        this.pstmt.setBinaryStream(1, new ByteArrayInputStream(new byte[0]), 0);
        this.pstmt.executeUpdate();

        assertEquals("0", getSingleIndexedValueWithQuery(1, "SELECT LENGTH(field1) FROM " + tableName).toString());
        this.stmt.executeUpdate("TRUNCATE TABLE " + tableName);

    }

    public void testBug34677() throws Exception {
        createTable("testBug34677", "(field1 BLOB)");
        this.stmt.executeUpdate("INSERT INTO testBug34677 VALUES ('abc')");

        this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug34677");
        this.rs.next();
        Blob blob = this.rs.getBlob(1);
        blob.truncate(0L);
        assertEquals(0, blob.length());
        assertEquals(-1, blob.getBinaryStream().read());

    }

    /**
     * Tests fix for BUG#20453671 - CLOB.POSITION() API CALL WITH CLOB INPUT RETURNS EXCEPTION
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20453671() throws Exception {
        this.rs = this.stmt.executeQuery("select 'abcd', 'a', 'b', 'c', 'd', 'e'");
        this.rs.next();

        final Clob in = this.rs.getClob(1);
        final ResultSet locallyScopedRs = this.rs;
        assertThrows(SQLException.class, "Illegal starting position for search, '0'", new Callable<Void>() {
            public Void call() throws Exception {
                in.position(locallyScopedRs.getClob(2), 0);
                return null;
            }
        });
        assertThrows(SQLException.class, "Starting position for search is past end of CLOB", new Callable<Void>() {
            public Void call() throws Exception {
                in.position(locallyScopedRs.getClob(2), 10);
                return null;
            }
        });

        assertEquals(1, in.position(this.rs.getClob(2), 1));
        assertEquals(2, in.position(this.rs.getClob(3), 1));
        assertEquals(3, in.position(this.rs.getClob(4), 1));
        assertEquals(4, in.position(this.rs.getClob(5), 1));
        assertEquals(-1, in.position(this.rs.getClob(6), 1));
    }

    /**
     * Tests fix for BUG#20453712 - CLOB.SETSTRING() WITH VALID INPUT RETURNS EXCEPTION
     * server-side prepared statements and streaming BINARY data.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20453712() throws Exception {
        final String s1 = "NewClobData";
        this.rs = this.stmt.executeQuery("select 'a'");
        this.rs.next();
        final Clob c1 = this.rs.getClob(1);

        // check with wrong position
        assertThrows(SQLException.class, "Starting position can not be < 1", new Callable<Void>() {
            public Void call() throws Exception {
                c1.setString(0, s1, 7, 4);
                return null;
            }
        });

        // check with wrong substring index
        assertThrows(SQLException.class, "String index out of range: 12", new Callable<Void>() {
            public Void call() throws Exception {
                c1.setString(1, s1, 8, 4);
                return null;
            }
        });

        // full replace
        c1.setString(1, s1, 3, 4);
        assertEquals("Clob", c1.getSubString(1L, (int) c1.length()));

        // add
        c1.setString(5, s1, 7, 4);
        assertEquals("ClobData", c1.getSubString(1L, (int) c1.length()));

        // replace middle chars
        c1.setString(2, s1, 7, 4);
        assertEquals("CDataata", c1.getSubString(1L, (int) c1.length()));
    }

    /**
     * Tests fix for Bug#23535571 - EXCESSIVE MEMORY USAGE WHEN ENABLEPACKETDEBUG=TRUE
     * 
     * @throws Exception
     */
    public void testBug23535571() throws Exception {

        createTable("testBug23535571", "(blobField LONGBLOB)");
        this.stmt.executeUpdate("INSERT INTO testBug23535571 (blobField) VALUES (NULL)");

        // Insert 1 record with 18M data
        byte[] blobData = new byte[18 * 1024 * 1024];
        this.pstmt = this.conn.prepareStatement("UPDATE testBug23535571 SET blobField=?");
        this.pstmt.setBytes(1, blobData);
        this.pstmt.executeUpdate();

        Properties props = new Properties();
        props.setProperty(PropertyKey.enablePacketDebug.getKeyName(), "true");
        Connection con = getConnectionWithProps(props);

        for (int i = 0; i < 100; i++) {
            this.pstmt = con.prepareStatement("select * from testBug23535571");
            this.rs = this.pstmt.executeQuery();
            this.rs.close();
            this.pstmt.close();
            Thread.sleep(100);
        }

    }
}
