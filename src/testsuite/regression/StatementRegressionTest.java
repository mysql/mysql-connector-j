/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package testsuite.regression;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DataTruncation;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import testsuite.BaseTestCase;
import testsuite.UnreliableSocketFactory;

import com.mysql.jdbc.CachedResultSetMetaData;
import com.mysql.jdbc.CharsetMapping;
import com.mysql.jdbc.Field;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.ParameterBindings;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.ServerPreparedStatement;
import com.mysql.jdbc.StatementImpl;
import com.mysql.jdbc.StatementInterceptor;
import com.mysql.jdbc.StatementInterceptorV2;
import com.mysql.jdbc.TimeUtil;
import com.mysql.jdbc.Util;
import com.mysql.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;

/**
 * Regression tests for the Statement class
 */
public class StatementRegressionTest extends BaseTestCase {
    class PrepareThread extends Thread {
        Connection c;

        PrepareThread(Connection cn) {
            this.c = cn;
        }

        @Override
        public void run() {
            for (int i = 0; i < 20; i++) // force this to end eventually
            {
                try {
                    this.c.prepareStatement("SELECT 1");
                    StatementRegressionTest.this.testServerPrepStmtDeadlockCounter++;
                    Thread.sleep(400);
                } catch (SQLException sqlEx) {
                    throw new RuntimeException(sqlEx);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static int count = 0;

    static int nextID = 1; // The next ID we expected to generate

    /*
     * Each row in this table is to be converted into a single REPLACE
     * statement. If the value is zero, a new record is to be created using then
     * autoincrement feature. If the value is non-zero, the existing row of that
     * value is to be replace with, obviously, the same key. I expect one
     * Generated Key for each zero value - but I would accept one key for each
     * value, with non-zero values coming back as themselves.
     */
    static final int[][] tests = { { 0 }, // generate 1
            { 1, 0, 0 }, // update 1, generate 2, 3
            { 2, 0, 0, }, // update 2, generate 3, 4
    };

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(StatementRegressionTest.class);
    }

    protected int testServerPrepStmtDeadlockCounter = 0;

    /**
     * Constructor for StatementRegressionTest.
     * 
     * @param name
     *            the name of the test to run
     */
    public StatementRegressionTest(String name) {
        super(name);
    }

    private void addBatchItems(Statement statement, PreparedStatement pStmt, String tableName, int i) throws SQLException {
        pStmt.setString(1, "ps_batch_" + i);
        pStmt.setString(2, "ps_batch_" + i);
        pStmt.addBatch();

        statement.addBatch("INSERT INTO " + tableName + " (strdata1, strdata2) VALUES (\"s_batch_" + i + "\",\"s_batch_" + i + "\")");
    }

    private void createGGKTables() throws Exception {
        // Delete and recreate table
        dropGGKTables();
        createTable("testggk", "(id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,val INT NOT NULL)", "MYISAM");
    }

    private void doGGKTestPreparedStatement(int[] values, boolean useUpdate) throws Exception {
        // Generate the the multiple replace command
        StringBuffer cmd = new StringBuffer("REPLACE INTO testggk VALUES ");
        int newKeys = 0;

        for (int i = 0; i < values.length; i++) {
            cmd.append("(");

            if (values[i] == 0) {
                cmd.append("NULL");
                newKeys += 1;
            } else {
                cmd.append(values[i]);
            }

            cmd.append(", ");
            cmd.append(count++);
            cmd.append("), ");
        }

        cmd.setLength(cmd.length() - 2); // trim the final ", "

        // execute and print it
        System.out.println(cmd.toString());

        PreparedStatement pStmt = this.conn.prepareStatement(cmd.toString(), Statement.RETURN_GENERATED_KEYS);

        if (useUpdate) {
            pStmt.executeUpdate();
        } else {
            pStmt.execute();
        }

        // print out what actually happened
        System.out.println("Expect " + newKeys + " generated keys, starting from " + nextID);

        this.rs = pStmt.getGeneratedKeys();
        StringBuffer res = new StringBuffer("Got keys");

        int[] generatedKeys = new int[newKeys];
        int i = 0;

        while (this.rs.next()) {
            if (i < generatedKeys.length) {
                generatedKeys[i] = this.rs.getInt(1);
            }

            i++;

            res.append(" " + this.rs.getInt(1));
        }

        int numberOfGeneratedKeys = i;

        assertTrue("Didn't retrieve expected number of generated keys, expected " + newKeys + ", found " + numberOfGeneratedKeys,
                numberOfGeneratedKeys == newKeys);
        assertTrue("Keys didn't start with correct sequence: ", generatedKeys[0] == nextID);

        System.out.println(res.toString());

        // Read and print the new state of the table
        this.rs = this.stmt.executeQuery("SELECT id, val FROM testggk");
        System.out.println("New table contents ");

        while (this.rs.next()) {
            System.out.println("Id " + this.rs.getString(1) + " val " + this.rs.getString(2));
        }

        // Tidy up
        System.out.println("");
        nextID += newKeys;
    }

    private void doGGKTestStatement(int[] values, boolean useUpdate) throws Exception {
        // Generate the the multiple replace command
        StringBuffer cmd = new StringBuffer("REPLACE INTO testggk VALUES ");
        int newKeys = 0;

        for (int i = 0; i < values.length; i++) {
            cmd.append("(");

            if (values[i] == 0) {
                cmd.append("NULL");
                newKeys += 1;
            } else {
                cmd.append(values[i]);
            }

            cmd.append(", ");
            cmd.append(count++);
            cmd.append("), ");
        }

        cmd.setLength(cmd.length() - 2); // trim the final ", "

        // execute and print it
        System.out.println(cmd.toString());

        if (useUpdate) {
            this.stmt.executeUpdate(cmd.toString(), Statement.RETURN_GENERATED_KEYS);
        } else {
            this.stmt.execute(cmd.toString(), Statement.RETURN_GENERATED_KEYS);
        }

        // print out what actually happened
        System.out.println("Expect " + newKeys + " generated keys, starting from " + nextID);

        this.rs = this.stmt.getGeneratedKeys();
        StringBuffer res = new StringBuffer("Got keys");

        int[] generatedKeys = new int[newKeys];
        int i = 0;

        while (this.rs.next()) {
            if (i < generatedKeys.length) {
                generatedKeys[i] = this.rs.getInt(1);
            }

            i++;

            res.append(" " + this.rs.getInt(1));
        }

        int numberOfGeneratedKeys = i;

        assertTrue("Didn't retrieve expected number of generated keys, expected " + newKeys + ", found " + numberOfGeneratedKeys,
                numberOfGeneratedKeys == newKeys);
        assertTrue("Keys didn't start with correct sequence: ", generatedKeys[0] == nextID);

        System.out.println(res.toString());

        // Read and print the new state of the table
        this.rs = this.stmt.executeQuery("SELECT id, val FROM testggk");
        System.out.println("New table contents ");

        while (this.rs.next()) {
            System.out.println("Id " + this.rs.getString(1) + " val " + this.rs.getString(2));
        }

        // Tidy up
        System.out.println("");
        nextID += newKeys;
    }

    private void dropGGKTables() throws Exception {
        this.stmt.executeUpdate("DROP TABLE IF EXISTS testggk");
    }

    /**
     * @param pStmt
     * @param catId
     * @throws SQLException
     */
    private void execQueryBug5191(PreparedStatement pStmt, int catId) throws SQLException {
        pStmt.setInt(1, catId);

        this.rs = pStmt.executeQuery();

        assertTrue(this.rs.next());
        assertTrue(this.rs.next());
        // assertTrue(rs.next());

        assertFalse(this.rs.next());
    }

    private String getByteArrayString(byte[] ba) {
        StringBuffer buffer = new StringBuffer();
        if (ba != null) {
            for (int i = 0; i < ba.length; i++) {
                buffer.append("0x" + Integer.toHexString(ba[i] & 0xff) + " ");
            }
        } else {
            buffer.append("null");
        }
        return buffer.toString();
    }

    /**
     * @param continueBatchOnError
     * @throws SQLException
     */
    private void innerBug6823(boolean continueBatchOnError) throws SQLException {
        Properties continueBatchOnErrorProps = new Properties();
        continueBatchOnErrorProps.setProperty("continueBatchOnError", String.valueOf(continueBatchOnError));
        this.conn = getConnectionWithProps(continueBatchOnErrorProps);
        Statement statement = this.conn.createStatement();

        String tableName = "testBug6823";

        createTable(tableName, "(id int not null primary key auto_increment, strdata1 varchar(255) not null, strdata2 varchar(255),"
                + " UNIQUE INDEX (strdata1(100)))");

        PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO " + tableName + " (strdata1, strdata2) VALUES (?,?)");

        int c = 0;
        addBatchItems(statement, pStmt, tableName, ++c);
        addBatchItems(statement, pStmt, tableName, ++c);
        addBatchItems(statement, pStmt, tableName, ++c);
        addBatchItems(statement, pStmt, tableName, c); // duplicate entry
        addBatchItems(statement, pStmt, tableName, ++c);
        addBatchItems(statement, pStmt, tableName, ++c);

        int expectedUpdateCounts = continueBatchOnError ? 6 : 3;

        BatchUpdateException e1 = null;
        BatchUpdateException e2 = null;

        int[] updateCountsPstmt = null;
        try {
            updateCountsPstmt = pStmt.executeBatch();
        } catch (BatchUpdateException e) {
            e1 = e;
            updateCountsPstmt = e1.getUpdateCounts();
        }

        int[] updateCountsStmt = null;
        try {
            updateCountsStmt = statement.executeBatch();
        } catch (BatchUpdateException e) {
            e2 = e;
            updateCountsStmt = e1.getUpdateCounts();
        }

        assertNotNull(e1);
        assertNotNull(e2);

        assertEquals(expectedUpdateCounts, updateCountsPstmt.length);
        assertEquals(expectedUpdateCounts, updateCountsStmt.length);

        if (continueBatchOnError) {
            assertTrue(updateCountsPstmt[3] == Statement.EXECUTE_FAILED);
            assertTrue(updateCountsStmt[3] == Statement.EXECUTE_FAILED);
        }

        int psRows = 0;
        this.rs = this.stmt.executeQuery("SELECT * from " + tableName + " WHERE strdata1 like \"ps_%\"");
        while (this.rs.next()) {
            psRows++;
        }
        assertTrue(psRows > 0);

        int sRows = 0;
        this.rs = this.stmt.executeQuery("SELECT * from " + tableName + " WHERE strdata1 like \"s_%\"");
        while (this.rs.next()) {
            sRows++;
        }
        assertTrue(sRows > 0);

        assertTrue(psRows + "!=" + sRows, psRows == sRows);
    }

    /**
     * Tests fix for BUG#10155, double quotes not recognized when parsing
     * client-side prepared statements.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug10155() throws Exception {
        this.conn.prepareStatement("SELECT \"Test question mark? Test single quote'\"").executeQuery().close();
    }

    /**
     * Tests fix for BUG#10630, Statement.getWarnings() fails with NPE if
     * statement has been closed.
     */
    public void testBug10630() throws Exception {
        Connection conn2 = null;
        Statement stmt2 = null;

        try {
            conn2 = getConnectionWithProps((Properties) null);
            stmt2 = conn2.createStatement();

            conn2.close();
            stmt2.getWarnings();
            fail("Should've caught an exception here");
        } catch (SQLException sqlEx) {
            assertEquals("08003", sqlEx.getSQLState());
        } finally {
            if (stmt2 != null) {
                stmt2.close();
            }

            if (conn2 != null) {
                conn2.close();
            }
        }
    }

    /**
     * Tests fix for BUG#11115, Varbinary data corrupted when using server-side
     * prepared statements.
     */
    public void testBug11115() throws Exception {
        String tableName = "testBug11115";

        if (versionMeetsMinimum(4, 1, 0)) {

            createTable(tableName, "(pwd VARBINARY(30)) DEFAULT CHARACTER SET utf8", "InnoDB");

            byte[] bytesToTest = new byte[] { 17, 120, -1, -73, -5 };

            PreparedStatement insStmt = this.conn.prepareStatement("INSERT INTO " + tableName + " (pwd) VALUES (?)");
            insStmt.setBytes(1, bytesToTest);
            insStmt.executeUpdate();

            this.rs = this.stmt.executeQuery("SELECT pwd FROM " + tableName);
            this.rs.next();

            byte[] fromDatabase = this.rs.getBytes(1);

            assertEquals(bytesToTest.length, fromDatabase.length);

            for (int i = 0; i < bytesToTest.length; i++) {
                assertEquals(bytesToTest[i], fromDatabase[i]);
            }

            this.rs = this.conn.prepareStatement("SELECT pwd FROM " + tableName).executeQuery();
            this.rs.next();

            fromDatabase = this.rs.getBytes(1);

            assertEquals(bytesToTest.length, fromDatabase.length);

            for (int i = 0; i < bytesToTest.length; i++) {
                assertEquals(bytesToTest[i], fromDatabase[i]);
            }
        }
    }

    public void testBug11540() throws Exception {
        Locale originalLocale = Locale.getDefault();
        Connection thaiConn = null;
        Statement thaiStmt = null;
        PreparedStatement thaiPrepStmt = null;

        try {
            createTable("testBug11540", "(field1 DATE, field2 TIMESTAMP)");
            this.stmt.executeUpdate("INSERT INTO testBug11540 VALUES (NOW(), NOW())");
            Locale.setDefault(new Locale("th", "TH"));
            Properties props = new Properties();
            props.setProperty("jdbcCompliantTruncation", "false");

            thaiConn = getConnectionWithProps(props);
            thaiStmt = thaiConn.createStatement();

            this.rs = thaiStmt.executeQuery("SELECT field1, field2 FROM testBug11540");
            this.rs.next();

            Date origDate = this.rs.getDate(1);
            Timestamp origTimestamp = this.rs.getTimestamp(1);
            this.rs.close();

            thaiStmt.executeUpdate("TRUNCATE TABLE testBug11540");

            thaiPrepStmt = ((com.mysql.jdbc.Connection) thaiConn).clientPrepareStatement("INSERT INTO testBug11540 VALUES (?,?)");
            thaiPrepStmt.setDate(1, origDate);
            thaiPrepStmt.setTimestamp(2, origTimestamp);
            thaiPrepStmt.executeUpdate();

            this.rs = thaiStmt.executeQuery("SELECT field1, field2 FROM testBug11540");
            this.rs.next();

            Date testDate = this.rs.getDate(1);
            Timestamp testTimestamp = this.rs.getTimestamp(1);
            this.rs.close();

            assertEquals(origDate, testDate);
            assertEquals(origTimestamp, testTimestamp);

        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    /**
     * Tests fix for BUG#11663, autoGenerateTestcaseScript uses bogus parameter
     * names for server-side prepared statements.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug11663() throws Exception {
        if (versionMeetsMinimum(4, 1, 0) && ((com.mysql.jdbc.Connection) this.conn).getUseServerPreparedStmts()) {
            Connection testcaseGenCon = null;
            PrintStream oldErr = System.err;

            try {
                createTable("testBug11663", "(field1 int)");

                Properties props = new Properties();
                props.setProperty("autoGenerateTestcaseScript", "true");
                testcaseGenCon = getConnectionWithProps(props);
                ByteArrayOutputStream testStream = new ByteArrayOutputStream();
                PrintStream testErr = new PrintStream(testStream);
                System.setErr(testErr);
                this.pstmt = testcaseGenCon.prepareStatement("SELECT field1 FROM testBug11663 WHERE field1=?");
                this.pstmt.setInt(1, 1);
                this.pstmt.execute();
                System.setErr(oldErr);
                String testString = new String(testStream.toByteArray());

                int setIndex = testString.indexOf("SET @debug_stmt_param");
                int equalsIndex = testString.indexOf("=", setIndex);
                String paramName = testString.substring(setIndex + 4, equalsIndex);

                int usingIndex = testString.indexOf("USING " + paramName, equalsIndex);

                assertTrue(usingIndex != -1);
            } finally {
                System.setErr(oldErr);

                if (this.pstmt != null) {
                    this.pstmt.close();
                    this.pstmt = null;
                }

                if (testcaseGenCon != null) {
                    testcaseGenCon.close();
                }

            }
        }
    }

    /**
     * Tests fix for BUG#11798 - Pstmt.setObject(...., Types.BOOLEAN) throws
     * exception.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug11798() throws Exception {
        if (isRunningOnJdk131()) {
            return; // test not valid on JDK-1.3.1
        }

        try {
            this.pstmt = this.conn.prepareStatement("SELECT ?");
            this.pstmt.setObject(1, Boolean.TRUE, Types.BOOLEAN);
            this.pstmt.setObject(1, new BigDecimal("1"), Types.BOOLEAN);
            this.pstmt.setObject(1, "true", Types.BOOLEAN);
        } finally {
            if (this.pstmt != null) {
                this.pstmt.close();
                this.pstmt = null;
            }
        }
    }

    /**
     * Tests fix for BUG#13255 - Reconnect during middle of executeBatch()
     * should not happen.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug13255() throws Exception {

        createTable("testBug13255", "(field_1 int)");

        Properties props = new Properties();
        props.setProperty("autoReconnect", "true");

        Connection reconnectConn = null;
        Statement reconnectStmt = null;
        PreparedStatement reconnectPStmt = null;

        try {
            reconnectConn = getConnectionWithProps(props);
            reconnectStmt = reconnectConn.createStatement();

            String connectionId = getSingleIndexedValueWithQuery(reconnectConn, 1, "SELECT CONNECTION_ID()").toString();

            reconnectStmt.addBatch("INSERT INTO testBug13255 VALUES (1)");
            reconnectStmt.addBatch("INSERT INTO testBug13255 VALUES (2)");
            reconnectStmt.addBatch("KILL " + connectionId);

            for (int i = 0; i < 100; i++) {
                reconnectStmt.addBatch("INSERT INTO testBug13255 VALUES (" + i + ")");
            }

            try {
                reconnectStmt.executeBatch();
            } catch (SQLException sqlEx) {
                // We expect this...we killed the connection
            }

            assertEquals(2, getRowCount("testBug13255"));

            this.stmt.executeUpdate("TRUNCATE TABLE testBug13255");

            reconnectConn.close();

            reconnectConn = getConnectionWithProps(props);

            connectionId = getSingleIndexedValueWithQuery(reconnectConn, 1, "SELECT CONNECTION_ID()").toString();

            reconnectPStmt = reconnectConn.prepareStatement("INSERT INTO testBug13255 VALUES (?)");
            reconnectPStmt.setInt(1, 1);
            reconnectPStmt.addBatch();
            reconnectPStmt.setInt(1, 2);
            reconnectPStmt.addBatch();
            reconnectPStmt.addBatch("KILL " + connectionId);

            for (int i = 3; i < 100; i++) {
                reconnectPStmt.setInt(1, i);
                reconnectPStmt.addBatch();
            }

            try {
                reconnectPStmt.executeBatch();
            } catch (SQLException sqlEx) {
                // We expect this...we killed the connection
            }

            assertEquals(2, getRowCount("testBug13255"));

        } finally {
            if (reconnectStmt != null) {
                reconnectStmt.close();
            }

            if (reconnectConn != null) {
                reconnectConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#15024 - Driver incorrectly closes streams passed as
     * arguments to PreparedStatements.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug15024() throws Exception {
        createTable("testBug15024", "(field1 BLOB)");

        try {
            this.pstmt = this.conn.prepareStatement("INSERT INTO testBug15024 VALUES (?)");
            testStreamsForBug15024(false, false);

            Properties props = new Properties();
            props.setProperty("useConfigs", "3-0-Compat");

            Connection compatConn = null;

            try {
                compatConn = getConnectionWithProps(props);

                this.pstmt = compatConn.prepareStatement("INSERT INTO testBug15024 VALUES (?)");
                testStreamsForBug15024(true, false);
            } finally {
                if (compatConn != null) {
                    compatConn.close();
                }
            }
        } finally {
            if (this.pstmt != null) {
                PreparedStatement toClose = this.pstmt;
                this.pstmt = null;

                toClose.close();
            }
        }
    }

    /**
     * PreparedStatement should call EscapeProcessor.escapeSQL?
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug15141() throws Exception {
        try {
            createTable("testBug15141", "(field1 VARCHAR(32))");
            this.stmt.executeUpdate("INSERT INTO testBug15141 VALUES ('abc')");

            this.pstmt = this.conn.prepareStatement("select {d '1997-05-24'} FROM testBug15141");
            this.rs = this.pstmt.executeQuery();
            assertTrue(this.rs.next());
            assertEquals("1997-05-24", this.rs.getString(1));
            this.rs.close();
            this.rs = null;
            this.pstmt.close();
            this.pstmt = null;

            this.pstmt = ((com.mysql.jdbc.Connection) this.conn).clientPrepareStatement("select {d '1997-05-24'} FROM testBug15141");
            this.rs = this.pstmt.executeQuery();
            assertTrue(this.rs.next());
            assertEquals("1997-05-24", this.rs.getString(1));
            this.rs.close();
            this.rs = null;
            this.pstmt.close();
            this.pstmt = null;
        } finally {
            if (this.rs != null) {
                ResultSet toCloseRs = this.rs;
                this.rs = null;
                toCloseRs.close();
            }

            if (this.pstmt != null) {
                PreparedStatement toClosePstmt = this.pstmt;
                this.pstmt = null;
                toClosePstmt.close();
            }
        }
    }

    /**
     * Tests fix for BUG#18041 - Server-side prepared statements don't cause
     * truncation exceptions to be thrown.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug18041() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            createTable("testBug18041", "(`a` tinyint(4) NOT NULL, `b` char(4) default NULL)");

            Properties props = new Properties();
            props.setProperty("jdbcCompliantTruncation", "true");
            props.setProperty("useServerPrepStmts", "true");

            Connection truncConn = null;
            PreparedStatement stm = null;

            try {
                truncConn = getConnectionWithProps(props);

                stm = truncConn.prepareStatement("insert into testBug18041 values (?,?)");
                stm.setInt(1, 1000);
                stm.setString(2, "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnn");
                stm.executeUpdate();
                fail("Truncation exception should have been thrown");
            } catch (DataTruncation truncEx) {
                // we expect this
            } finally {
                if (truncConn != null) {
                    truncConn.close();
                }
            }
        }
    }

    private void testStreamsForBug15024(boolean shouldBeClosedStream, boolean shouldBeClosedReader) throws SQLException {
        IsClosedInputStream bIn = new IsClosedInputStream(new byte[4]);
        IsClosedReader readerIn = new IsClosedReader("abcdef");

        this.pstmt.setBinaryStream(1, bIn, 4);
        this.pstmt.execute();
        assertEquals(shouldBeClosedStream, bIn.isClosed());

        this.pstmt.setCharacterStream(1, readerIn, 6);
        this.pstmt.execute();
        assertEquals(shouldBeClosedReader, readerIn.isClosed());

        this.pstmt.close();
    }

    class IsClosedReader extends StringReader {

        boolean isClosed = false;

        public IsClosedReader(String arg0) {
            super(arg0);
        }

        @Override
        public void close() {
            super.close();

            this.isClosed = true;
        }

        public boolean isClosed() {
            return this.isClosed;
        }

    }

    class IsClosedInputStream extends ByteArrayInputStream {

        boolean isClosed = false;

        public IsClosedInputStream(byte[] arg0, int arg1, int arg2) {
            super(arg0, arg1, arg2);
        }

        public IsClosedInputStream(byte[] arg0) {
            super(arg0);
        }

        @Override
        public void close() throws IOException {

            super.close();
            this.isClosed = true;
        }

        public boolean isClosed() {
            return this.isClosed;
        }
    }

    /**
     * Tests fix for BUG#1774 -- Truncated words after double quote
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug1774() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1774");
            this.stmt.executeUpdate("CREATE TABLE testBug1774 (field1 VARCHAR(255))");

            PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO testBug1774 VALUES (?)");

            String testString = "The word contains \" character";

            pStmt.setString(1, testString);
            pStmt.executeUpdate();

            this.rs = this.stmt.executeQuery("SELECT * FROM testBug1774");
            this.rs.next();
            assertEquals(this.rs.getString(1), testString);
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1774");
        }
    }

    /**
     * Tests fix for BUG#1901 -- PreparedStatement.setObject(int, Object, int,
     * int) doesn't support CLOB or BLOB types.
     * 
     * @throws Exception
     *             if this test fails for any reason
     */
    public void testBug1901() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1901");
            this.stmt.executeUpdate("CREATE TABLE testBug1901 (field1 VARCHAR(255))");
            this.stmt.executeUpdate("INSERT INTO testBug1901 VALUES ('aaa')");

            this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug1901");
            this.rs.next();

            Clob valueAsClob = this.rs.getClob(1);
            Blob valueAsBlob = this.rs.getBlob(1);

            PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO testBug1901 VALUES (?)");
            pStmt.setObject(1, valueAsClob, java.sql.Types.CLOB, 0);
            pStmt.executeUpdate();
            pStmt.setObject(1, valueAsBlob, java.sql.Types.BLOB, 0);
            pStmt.executeUpdate();
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1901");
        }
    }

    /**
     * Test fix for BUG#1933 -- Driver property 'maxRows' has no effect.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug1933() throws Exception {
        if (versionMeetsMinimum(4, 0)) {
            Connection maxRowsConn = null;
            PreparedStatement maxRowsPrepStmt = null;
            Statement maxRowsStmt = null;

            try {
                Properties props = new Properties();

                props.setProperty("maxRows", "1");

                maxRowsConn = getConnectionWithProps(props);

                maxRowsStmt = maxRowsConn.createStatement();

                assertTrue(maxRowsStmt.getMaxRows() == 1);

                this.rs = maxRowsStmt.executeQuery("SELECT 1 UNION SELECT 2");

                this.rs.next();

                maxRowsPrepStmt = maxRowsConn.prepareStatement("SELECT 1 UNION SELECT 2");

                assertTrue(maxRowsPrepStmt.getMaxRows() == 1);

                this.rs = maxRowsPrepStmt.executeQuery();

                this.rs.next();

                assertTrue(!this.rs.next());

                props.setProperty("useServerPrepStmts", "false");

                maxRowsConn = getConnectionWithProps(props);

                maxRowsPrepStmt = maxRowsConn.prepareStatement("SELECT 1 UNION SELECT 2");

                assertTrue(maxRowsPrepStmt.getMaxRows() == 1);

                this.rs = maxRowsPrepStmt.executeQuery();

                this.rs.next();

                assertTrue(!this.rs.next());
            } finally {
                if (maxRowsConn != null) {
                    maxRowsConn.close();
                }
            }
        }
    }

    /**
     * Tests the fix for BUG#1934 -- prepareStatement dies silently when
     * encountering Statement.RETURN_GENERATED_KEY
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug1934() throws Exception {
        if (isRunningOnJdk131()) {
            return; // test not valid on JDK-1.3.1
        }

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1934");
            this.stmt.executeUpdate("CREATE TABLE testBug1934 (field1 INT)");

            System.out.println("Before prepareStatement()");

            this.pstmt = this.conn.prepareStatement("INSERT INTO testBug1934 VALUES (?)", java.sql.Statement.RETURN_GENERATED_KEYS);

            assertTrue(this.pstmt != null);

            System.out.println("After prepareStatement() - " + this.pstmt);
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1934");
        }
    }

    /**
     * Tests fix for BUG#1958 - Improper bounds checking on
     * PreparedStatement.setFoo().
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug1958() throws Exception {
        PreparedStatement pStmt = null;

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1958");
            this.stmt.executeUpdate("CREATE TABLE testBug1958 (field1 int)");

            pStmt = this.conn.prepareStatement("SELECT * FROM testBug1958 WHERE field1 IN (?, ?, ?)");

            try {
                pStmt.setInt(4, 1);
            } catch (SQLException sqlEx) {
                assertTrue(SQLError.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
            }
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1958");
        }
    }

    /**
     * Tests the fix for BUG#2606, server-side prepared statements not returning
     * datatype YEAR correctly.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug2606() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug2606");
            this.stmt.executeUpdate("CREATE TABLE testBug2606(year_field YEAR)");
            this.stmt.executeUpdate("INSERT INTO testBug2606 VALUES (2004)");

            PreparedStatement yrPstmt = this.conn.prepareStatement("SELECT year_field FROM testBug2606");

            this.rs = yrPstmt.executeQuery();

            assertTrue(this.rs.next());

            assertEquals(2004, this.rs.getInt(1));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug2606");
        }
    }

    /**
     * Tests the fix for BUG#2671, nulls encoded incorrectly in server-side prepared statements.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testBug2671() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            createTable("test3", "(`field1` int(8) NOT NULL auto_increment, `field2` int(8) unsigned zerofill default NULL,"
                    + " `field3` varchar(30) binary NOT NULL default '', `field4` varchar(100) default NULL, `field5` datetime NULL default NULL,"
                    + " PRIMARY KEY  (`field1`), UNIQUE KEY `unq_id` (`field2`), UNIQUE KEY  (`field3`)) CHARACTER SET utf8", "InnoDB");

            this.stmt.executeUpdate("insert into test3 (field1, field3, field4) values (1, 'blewis', 'Bob Lewis')");

            String query = "UPDATE test3 SET field2=?, field3=?, field4=?, field5=? WHERE field1 = ?";

            java.sql.Date mydate = null;

            this.pstmt = this.conn.prepareStatement(query);

            this.pstmt.setInt(1, 13);
            this.pstmt.setString(2, "abc");
            this.pstmt.setString(3, "def");
            this.pstmt.setDate(4, mydate);
            this.pstmt.setInt(5, 1);

            int retval = this.pstmt.executeUpdate();
            assertEquals(1, retval);
        }
    }

    /**
     * Tests fix for BUG#3103 -- java.util.Date not accepted as parameter to
     * PreparedStatement.setObject().
     * 
     * @throws Exception
     *             if the test fails
     * 
     * @deprecated uses deprecated methods of Date class
     */
    @Deprecated
    public void testBug3103() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3103");

            if (versionMeetsMinimum(5, 6, 4)) {
                this.stmt.executeUpdate("CREATE TABLE testBug3103 (field1 DATETIME(3))");
            } else {
                this.stmt.executeUpdate("CREATE TABLE testBug3103 (field1 DATETIME)");
            }

            PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO testBug3103 VALUES (?)");

            java.util.Date utilDate = new java.util.Date();

            pStmt.setObject(1, utilDate);
            pStmt.executeUpdate();

            this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug3103");
            this.rs.next();

            java.util.Date retrUtilDate = new java.util.Date(this.rs.getTimestamp(1).getTime());

            // We can only compare on the day/month/year hour/minute/second interval, because the timestamp has added milliseconds to the internal date...
            assertTrue(
                    "Dates not equal",
                    (utilDate.getMonth() == retrUtilDate.getMonth()) && (utilDate.getDate() == retrUtilDate.getDate())
                            && (utilDate.getYear() == retrUtilDate.getYear()) && (utilDate.getHours() == retrUtilDate.getHours())
                            && (utilDate.getMinutes() == retrUtilDate.getMinutes()) && (utilDate.getSeconds() == retrUtilDate.getSeconds()));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3103");
        }
    }

    /**
     * Tests fix for BUG#3520
     * 
     * @throws Exception
     *             ...
     */
    public void testBug3520() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS t");
            this.stmt.executeUpdate("CREATE TABLE t (s1 int,primary key (s1))");
            this.stmt.executeUpdate("INSERT INTO t VALUES (1)");
            this.stmt.executeUpdate("INSERT INTO t VALUES (1)");
        } catch (SQLException sqlEx) {
            System.out.println(sqlEx.getSQLState());
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS t");
        }
    }

    /**
     * Test fix for BUG#3557 -- UpdatableResultSet not picking up default values
     * 
     * @throws Exception
     *             if test fails.
     */
    public void testBug3557() throws Exception {
        boolean populateDefaults = ((com.mysql.jdbc.ConnectionProperties) this.conn).getPopulateInsertRowWithDefaultValues();

        try {
            ((com.mysql.jdbc.ConnectionProperties) this.conn).setPopulateInsertRowWithDefaultValues(true);

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3557");

            this.stmt.executeUpdate("CREATE TABLE testBug3557 (`a` varchar(255) NOT NULL default 'XYZ', `b` varchar(255) default '123', "
                    + "PRIMARY KEY  (`a`(100)))");

            Statement updStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            this.rs = updStmt.executeQuery("SELECT * FROM testBug3557");

            assertTrue(this.rs.getConcurrency() == ResultSet.CONCUR_UPDATABLE);

            this.rs.moveToInsertRow();

            assertEquals("XYZ", this.rs.getObject(1));
            assertEquals("123", this.rs.getObject(2));
        } finally {
            ((com.mysql.jdbc.ConnectionProperties) this.conn).setPopulateInsertRowWithDefaultValues(populateDefaults);

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3557");
        }
    }

    /**
     * Tests fix for BUG#3620 -- Timezone not respected correctly.
     * 
     * @throws SQLException
     *             if the test fails.
     */
    public void testBug3620() throws SQLException {
        if (isRunningOnJRockit()) {
            // bug with their timezones
            return;
        }

        if (isRunningOnJdk131()) {
            // bug with timezones, no update for new DST in USA
            return;
        }

        // FIXME: This test is sensitive to being in CST/CDT it seems
        if (!TimeZone.getDefault().equals(TimeZone.getTimeZone("America/Chicago"))) {
            return;
        }

        long epsillon = 3000; // 3 seconds time difference

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3620");
            this.stmt.executeUpdate("CREATE TABLE testBug3620 (field1 TIMESTAMP)");

            PreparedStatement tsPstmt = this.conn.prepareStatement("INSERT INTO testBug3620 VALUES (?)");

            Calendar pointInTime = Calendar.getInstance();
            pointInTime.set(2004, 02, 29, 10, 0, 0);

            long pointInTimeOffset = pointInTime.getTimeZone().getRawOffset();

            java.sql.Timestamp ts = new java.sql.Timestamp(pointInTime.getTime().getTime());

            tsPstmt.setTimestamp(1, ts);
            tsPstmt.executeUpdate();

            String tsValueAsString = getSingleValue("testBug3620", "field1", null).toString();

            System.out.println("Timestamp as string with no calendar: " + tsValueAsString.toString());

            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

            this.stmt.executeUpdate("DELETE FROM testBug3620");

            Properties props = new Properties();
            props.put("useTimezone", "true");
            // props.put("serverTimezone", "UTC");

            Connection tzConn = getConnectionWithProps(props);

            Statement tsStmt = tzConn.createStatement();

            tsPstmt = tzConn.prepareStatement("INSERT INTO testBug3620 VALUES (?)");

            tsPstmt.setTimestamp(1, ts, cal);
            tsPstmt.executeUpdate();

            tsValueAsString = getSingleValue("testBug3620", "field1", null).toString();

            Timestamp tsValueAsTimestamp = (Timestamp) getSingleValue("testBug3620", "field1", null);

            System.out.println("Timestamp as string with UTC calendar: " + tsValueAsString.toString());
            System.out.println("Timestamp as Timestamp with UTC calendar: " + tsValueAsTimestamp);

            this.rs = tsStmt.executeQuery("SELECT field1 FROM testBug3620");
            this.rs.next();

            Timestamp tsValueUTC = this.rs.getTimestamp(1, cal);

            //
            // We use this testcase with other vendors, JDBC spec requires result set fields can only be read once, although MySQL doesn't require this ;)
            //
            this.rs = tsStmt.executeQuery("SELECT field1 FROM testBug3620");
            this.rs.next();

            Timestamp tsValueStmtNoCal = this.rs.getTimestamp(1);

            System.out.println("Timestamp specifying UTC calendar from normal statement: " + tsValueUTC.toString());

            PreparedStatement tsPstmtRetr = tzConn.prepareStatement("SELECT field1 FROM testBug3620");

            this.rs = tsPstmtRetr.executeQuery();
            this.rs.next();

            Timestamp tsValuePstmtUTC = this.rs.getTimestamp(1, cal);

            System.out.println("Timestamp specifying UTC calendar from prepared statement: " + tsValuePstmtUTC.toString());

            //
            // We use this testcase with other vendors, JDBC spec requires result set fields can only be read once, although MySQL doesn't require this ;)
            //
            this.rs = tsPstmtRetr.executeQuery();
            this.rs.next();

            Timestamp tsValuePstmtNoCal = this.rs.getTimestamp(1);

            System.out.println("Timestamp specifying no calendar from prepared statement: " + tsValuePstmtNoCal.toString());

            long stmtDeltaTWithCal = (ts.getTime() - tsValueStmtNoCal.getTime());

            long deltaOrig = Math.abs(stmtDeltaTWithCal - pointInTimeOffset);

            assertTrue("Difference between original timestamp and timestamp retrieved using java.sql.Statement "
                    + "set in database using UTC calendar is not ~= " + epsillon + ", it is actually " + deltaOrig, (deltaOrig < epsillon));

            long pStmtDeltaTWithCal = (ts.getTime() - tsValuePstmtNoCal.getTime());

            System.out.println(Math.abs(pStmtDeltaTWithCal - pointInTimeOffset) + " < " + epsillon
                    + (Math.abs(pStmtDeltaTWithCal - pointInTimeOffset) < epsillon));
            assertTrue("Difference between original timestamp and timestamp retrieved using java.sql.PreparedStatement "
                    + "set in database using UTC calendar is not ~= " + epsillon + ", it is actually " + pStmtDeltaTWithCal,
                    (Math.abs(pStmtDeltaTWithCal - pointInTimeOffset) < epsillon));

            System.out.println("Difference between original ts and ts with no calendar: " + (ts.getTime() - tsValuePstmtNoCal.getTime())
                    + ", offset should be " + pointInTimeOffset);
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3620");
        }
    }

    /**
     * Tests that DataTruncation is thrown when data is truncated.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug3697() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3697");
            this.stmt.executeUpdate("CREATE TABLE testBug3697 (field1 VARCHAR(255))");

            StringBuffer updateBuf = new StringBuffer("INSERT INTO testBug3697 VALUES ('");

            for (int i = 0; i < 512; i++) {
                updateBuf.append("A");
            }

            updateBuf.append("')");

            try {
                this.stmt.executeUpdate(updateBuf.toString());
            } catch (DataTruncation dtEx) {
                // This is an expected exception....
            }

            SQLWarning warningChain = this.stmt.getWarnings();

            System.out.println(warningChain);
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3697");
        }
    }

    /**
     * Tests fix for BUG#3804, data truncation on server should throw
     * DataTruncation exception.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug3804() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            try {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3804");
                this.stmt.executeUpdate("CREATE TABLE testBug3804 (field1 VARCHAR(5))");

                boolean caughtTruncation = false;

                try {
                    this.stmt.executeUpdate("INSERT INTO testBug3804 VALUES ('1234567')");
                } catch (DataTruncation truncationEx) {
                    caughtTruncation = true;
                    System.out.println(truncationEx);
                }

                assertTrue("Data truncation exception should've been thrown", caughtTruncation);
            } finally {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3804");
            }
        }
    }

    /**
     * Tests BUG#3873 - PreparedStatement.executeBatch() not returning all
     * generated keys (even though that's not JDBC compliant).
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug3873() throws Exception {
        if (isRunningOnJdk131()) {
            return; // test not valid on JDK-1.3.1
        }

        PreparedStatement batchStmt = null;

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3873");
            this.stmt.executeUpdate("CREATE TABLE testBug3873 (keyField INT NOT NULL PRIMARY KEY AUTO_INCREMENT, dataField VARCHAR(32))");
            batchStmt = this.conn.prepareStatement("INSERT INTO testBug3873 (dataField) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
            batchStmt.setString(1, "abc");
            batchStmt.addBatch();
            batchStmt.setString(1, "def");
            batchStmt.addBatch();
            batchStmt.setString(1, "ghi");
            batchStmt.addBatch();

            @SuppressWarnings("unused")
            int[] updateCounts = batchStmt.executeBatch();

            this.rs = batchStmt.getGeneratedKeys();

            while (this.rs.next()) {
                System.out.println(this.rs.getInt(1));
            }

            this.rs = batchStmt.getGeneratedKeys();
            assertTrue(this.rs.next());
            assertTrue(1 == this.rs.getInt(1));
            assertTrue(this.rs.next());
            assertTrue(2 == this.rs.getInt(1));
            assertTrue(this.rs.next());
            assertTrue(3 == this.rs.getInt(1));
            assertTrue(!this.rs.next());
        } finally {
            if (batchStmt != null) {
                batchStmt.close();
            }

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3873");
        }
    }

    /**
     * Tests fix for BUG#4119 -- misbehavior in a managed environment from
     * MVCSoft JDO
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug4119() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4119");
            this.stmt.executeUpdate("CREATE TABLE `testBug4119` (`field1` varchar(255) NOT NULL default '', `field2` bigint(20) default NULL,"
                    + "`field3` int(11) default NULL, `field4` datetime default NULL, `field5` varchar(75) default NULL,"
                    + "`field6` varchar(75) default NULL, `field7` varchar(75) default NULL, `field8` datetime default NULL,"
                    + " PRIMARY KEY  (`field1`(100)))");

            PreparedStatement pStmt = this.conn.prepareStatement("insert into testBug4119 (field2, field3,"
                    + "field4, field5, field6, field7, field8, field1) values (?, ?, ?, ?, ?, ?, ?, ?)");

            pStmt.setString(1, "0");
            pStmt.setString(2, "0");
            pStmt.setTimestamp(3, new java.sql.Timestamp(System.currentTimeMillis()));
            pStmt.setString(4, "ABC");
            pStmt.setString(5, "DEF");
            pStmt.setString(6, "AA");
            pStmt.setTimestamp(7, new java.sql.Timestamp(System.currentTimeMillis()));
            pStmt.setString(8, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
            pStmt.executeUpdate();
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4119");
        }
    }

    /**
     * Tests fix for BUG#4311 - Error in JDBC retrieval of mediumint column when
     * using prepared statements and binary result sets.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug4311() throws Exception {
        try {
            int lowValue = -8388608;
            int highValue = 8388607;

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4311");
            this.stmt.executeUpdate("CREATE TABLE testBug4311 (low MEDIUMINT, high MEDIUMINT)");
            this.stmt.executeUpdate("INSERT INTO testBug4311 VALUES (" + lowValue + ", " + highValue + ")");

            PreparedStatement pStmt = this.conn.prepareStatement("SELECT low, high FROM testBug4311");
            this.rs = pStmt.executeQuery();
            assertTrue(this.rs.next());
            assertTrue(this.rs.getInt(1) == lowValue);
            assertTrue(this.rs.getInt(2) == highValue);
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4311");
        }
    }

    /**
     * Tests fix for BUG#4510 -- Statement.getGeneratedKeys() fails when key >
     * 32767
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug4510() throws Exception {
        if (isRunningOnJdk131()) {
            return; // test not valid on JDK-1.3.1
        }

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4510");
            this.stmt.executeUpdate("CREATE TABLE testBug4510 (field1 INT NOT NULL PRIMARY KEY AUTO_INCREMENT, field2 VARCHAR(100))");
            this.stmt.executeUpdate("INSERT INTO testBug4510 (field1, field2) VALUES (32767, 'bar')");

            PreparedStatement p = this.conn.prepareStatement("insert into testBug4510 (field2) values (?)", Statement.RETURN_GENERATED_KEYS);

            p.setString(1, "blah");

            p.executeUpdate();

            ResultSet genKeysRs = p.getGeneratedKeys();
            genKeysRs.next();
            System.out.println("Id: " + genKeysRs.getInt(1));
            genKeysRs.close();
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4510");
        }
    }

    /**
     * Server doesn't accept everything as a server-side prepared statement, so
     * by default we scan for stuff it can't handle.
     * 
     * @throws SQLException
     */
    public void testBug4718() throws SQLException {
        if (versionMeetsMinimum(4, 1, 0) && ((com.mysql.jdbc.Connection) this.conn).getUseServerPreparedStmts()) {
            this.pstmt = this.conn.prepareStatement("SELECT 1 LIMIT ?");
            assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

            this.pstmt = this.conn.prepareStatement("SELECT 1 LIMIT 1");
            assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);

            this.pstmt = this.conn.prepareStatement("SELECT 1 LIMIT 1, ?");
            assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

            try {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4718");
                this.stmt.executeUpdate("CREATE TABLE testBug4718 (field1 char(32))");

                this.pstmt = this.conn.prepareStatement("ALTER TABLE testBug4718 ADD INDEX (field1)");
                assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

                this.pstmt = this.conn.prepareStatement("SELECT 1");
                assertTrue(this.pstmt instanceof ServerPreparedStatement);

                this.pstmt = this.conn.prepareStatement("UPDATE testBug4718 SET field1=1");
                assertTrue(this.pstmt instanceof ServerPreparedStatement);

                this.pstmt = this.conn.prepareStatement("UPDATE testBug4718 SET field1=1 LIMIT 1");
                assertTrue(this.pstmt instanceof ServerPreparedStatement);

                this.pstmt = this.conn.prepareStatement("UPDATE testBug4718 SET field1=1 LIMIT ?");
                assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

                this.pstmt = this.conn.prepareStatement("UPDATE testBug4718 SET field1='Will we ignore LIMIT ?,?'");
                assertTrue(this.pstmt instanceof ServerPreparedStatement);

            } finally {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4718");
            }
        }
    }

    /**
     * Tests fix for BUG#5012 -- ServerPreparedStatements dealing with return of
     * DECIMAL type don't work.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug5012() throws Exception {
        PreparedStatement pStmt = null;
        String valueAsString = "12345.12";

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5012");
            this.stmt.executeUpdate("CREATE TABLE testBug5012(field1 DECIMAL(10,2))");
            this.stmt.executeUpdate("INSERT INTO testBug5012 VALUES (" + valueAsString + ")");

            pStmt = this.conn.prepareStatement("SELECT field1 FROM testBug5012");
            this.rs = pStmt.executeQuery();
            assertTrue(this.rs.next());
            assertEquals(new BigDecimal(valueAsString), this.rs.getBigDecimal(1));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5012");

            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Tests fix for BUG#5133 -- PreparedStatement.toString() doesn't return
     * correct value if no parameters are present in statement.
     * 
     * @throws Exception
     */
    public void testBug5133() throws Exception {
        String query = "SELECT 1";
        String output = this.conn.prepareStatement(query).toString();
        System.out.println(output);

        assertTrue(output.indexOf(query) != -1);
    }

    /**
     * Tests for BUG#5191 -- PreparedStatement.executeQuery() gives
     * OutOfMemoryError
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug5191() throws Exception {
        PreparedStatement pStmt = null;

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5191Q");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5191C");

            this.stmt.executeUpdate("CREATE TABLE testBug5191Q (QuestionId int NOT NULL AUTO_INCREMENT, Text VARCHAR(200), PRIMARY KEY(QuestionId))");

            this.stmt.executeUpdate("CREATE TABLE testBug5191C (CategoryId int, QuestionId int)");

            String[] questions = new String[] { "What is your name?", "What is your quest?", "What is the airspeed velocity of an unladen swollow?",
                    "How many roads must a man walk?", "Where's the tea?", };

            for (int i = 0; i < questions.length; i++) {
                this.stmt.executeUpdate("INSERT INTO testBug5191Q(Text) VALUES (\"" + questions[i] + "\")");
                int catagory = (i < 3) ? 0 : i;

                this.stmt.executeUpdate("INSERT INTO testBug5191C (CategoryId, QuestionId) VALUES (" + catagory + ", " + i + ")");
                /*
                 * this.stmt.executeUpdate("INSERT INTO testBug5191C" +
                 * "(CategoryId, QuestionId) VALUES (" + catagory + ", (SELECT
                 * testBug5191Q.QuestionId" + " FROM testBug5191Q " + "WHERE
                 * testBug5191Q.Text LIKE '" + questions[i] + "'))");
                 */
            }

            pStmt = this.conn.prepareStatement("SELECT qc.QuestionId, q.Text FROM testBug5191Q q, testBug5191C qc WHERE qc.CategoryId = ? "
                    + " AND q.QuestionId = qc.QuestionId");

            int catId = 0;
            for (int i = 0; i < 100; i++) {
                execQueryBug5191(pStmt, catId);
            }

        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5191Q");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5191C");

            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Tests for BUG#5235, ClassCastException on all-zero date field when
     * zeroDatetimeBehavior is 'convertToNull'.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug5235() throws Exception {
        Properties props = new Properties();
        props.setProperty("zeroDateTimeBehavior", "convertToNull");
        if (versionMeetsMinimum(5, 7, 4)) {
            props.put("jdbcCompliantTruncation", "false");
        }
        if (versionMeetsMinimum(5, 7, 5)) {
            String sqlMode = getMysqlVariable("sql_mode");
            if (sqlMode.contains("STRICT_TRANS_TABLES")) {
                sqlMode = removeSqlMode("STRICT_TRANS_TABLES", sqlMode);
                props.put("sessionVariables", "sql_mode='" + sqlMode + "'");
            }
        }

        Connection convertToNullConn = getConnectionWithProps(props);
        Statement convertToNullStmt = convertToNullConn.createStatement();
        try {
            convertToNullStmt.executeUpdate("DROP TABLE IF EXISTS testBug5235");
            convertToNullStmt.executeUpdate("CREATE TABLE testBug5235(field1 DATE)");
            convertToNullStmt.executeUpdate("INSERT INTO testBug5235 (field1) VALUES ('0000-00-00')");

            PreparedStatement ps = convertToNullConn.prepareStatement("SELECT field1 FROM testBug5235");
            this.rs = ps.executeQuery();

            if (this.rs.next()) {
                assertNull(this.rs.getObject("field1"));
            }
        } finally {
            convertToNullStmt.executeUpdate("DROP TABLE IF EXISTS testBug5235");
        }
    }

    public void testBug5450() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            String table = "testBug5450";
            String column = "policyname";

            try {
                Properties props = new Properties();
                props.setProperty("characterEncoding", "utf-8");

                Connection utf8Conn = getConnectionWithProps(props);
                Statement utfStmt = utf8Conn.createStatement();

                this.stmt.executeUpdate("DROP TABLE IF EXISTS " + table);

                this.stmt.executeUpdate("CREATE TABLE " + table + "(policyid int NOT NULL AUTO_INCREMENT, " + column + " VARCHAR(200), "
                        + "PRIMARY KEY(policyid)) DEFAULT CHARACTER SET utf8");

                String pname0 = "inserted \uac00 - foo - \u4e00";

                utfStmt.executeUpdate("INSERT INTO " + table + "(" + column + ") VALUES (\"" + pname0 + "\")");

                this.rs = utfStmt.executeQuery("SELECT " + column + " FROM " + table);

                this.rs.first();
                String pname1 = this.rs.getString(column);

                assertEquals(pname0, pname1);
                byte[] bytes = this.rs.getBytes(column);

                String pname2 = new String(bytes, "utf-8");
                assertEquals(pname1, pname2);

                utfStmt.executeUpdate("delete from " + table + " where " + column + " like 'insert%'");

                PreparedStatement s1 = utf8Conn.prepareStatement("insert into " + table + "(" + column + ") values (?)");

                s1.setString(1, pname0);
                s1.executeUpdate();

                String byteesque = "byte " + pname0;
                byte[] newbytes = byteesque.getBytes("utf-8");

                s1.setBytes(1, newbytes);
                s1.executeUpdate();

                this.rs = utfStmt.executeQuery("select " + column + " from " + table + " where " + column + " like 'insert%'");
                this.rs.first();
                String pname3 = this.rs.getString(column);
                assertEquals(pname0, pname3);

                this.rs = utfStmt.executeQuery("select " + column + " from " + table + " where " + column + " like 'byte insert%'");
                this.rs.first();

                String pname4 = this.rs.getString(column);
                assertEquals(byteesque, pname4);

            } finally {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    public void testBug5510() throws Exception {
        // This is a server bug that should be fixed by 4.1.6
        if (versionMeetsMinimum(4, 1, 6)) {
            createTable("`testBug5510`", "(`a` bigint(20) NOT NULL auto_increment, `b` varchar(64) default NULL, `c` varchar(64) default NULL,"
                    + "`d` varchar(255) default NULL, `e` int(11) default NULL, `f` varchar(32) default NULL, `g` varchar(32) default NULL,"
                    + "`h` varchar(80) default NULL, `i` varchar(255) default NULL, `j` varchar(255) default NULL, `k` varchar(255) default NULL,"
                    + "`l` varchar(32) default NULL, `m` varchar(32) default NULL, `n` timestamp NOT NULL default CURRENT_TIMESTAMP on update"
                    + " CURRENT_TIMESTAMP, `o` int(11) default NULL, `p` int(11) default NULL, PRIMARY KEY  (`a`)) DEFAULT CHARSET=latin1", "InnoDB ");
            PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO testBug5510 (a) VALUES (?)");
            pStmt.setNull(1, 0);
            pStmt.executeUpdate();
        }
    }

    /**
     * Tests fix for BUG#5874, timezone correction goes in wrong 'direction'
     * when useTimezone=true and server timezone differs from client timezone.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug5874() throws Exception {
        /*
         * try { String clientTimezoneName = "America/Los_Angeles"; String
         * serverTimezoneName = "America/Chicago";
         * 
         * TimeZone.setDefault(TimeZone.getTimeZone(clientTimezoneName));
         * 
         * long epsillon = 3000; // 3 seconds difference
         * 
         * long clientTimezoneOffsetMillis = TimeZone.getDefault()
         * .getRawOffset(); long serverTimezoneOffsetMillis =
         * TimeZone.getTimeZone( serverTimezoneName).getRawOffset();
         * 
         * long offsetDifference = clientTimezoneOffsetMillis -
         * serverTimezoneOffsetMillis;
         * 
         * Properties props = new Properties(); props.put("useTimezone",
         * "true"); props.put("serverTimezone", serverTimezoneName);
         * 
         * Connection tzConn = getConnectionWithProps(props); Statement tzStmt =
         * tzConn.createStatement();
         * tzStmt.executeUpdate("DROP TABLE IF EXISTS timeTest"); tzStmt
         * .executeUpdate("CREATE TABLE timeTest (tstamp DATETIME, t TIME)");
         * 
         * PreparedStatement pstmt = tzConn
         * .prepareStatement("INSERT INTO timeTest VALUES (?, ?)");
         * 
         * long now = System.currentTimeMillis(); // Time in milliseconds //
         * since 1/1/1970 GMT
         * 
         * Timestamp nowTstamp = new Timestamp(now); Time nowTime = new
         * Time(now);
         * 
         * pstmt.setTimestamp(1, nowTstamp); pstmt.setTime(2, nowTime);
         * pstmt.executeUpdate();
         * 
         * this.rs = tzStmt.executeQuery("SELECT * from timeTest");
         * 
         * // Timestamps look like this: 2004-11-29 13:43:21 SimpleDateFormat
         * timestampFormat = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss");
         * SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");
         * 
         * while (this.rs.next()) { // Driver now converts/checks
         * DATE/TIME/TIMESTAMP/DATETIME types // when calling getString()...
         * String retrTimestampString = new String(this.rs.getBytes(1));
         * Timestamp retrTimestamp = this.rs.getTimestamp(1);
         * 
         * java.util.Date timestampOnServer = timestampFormat
         * .parse(retrTimestampString);
         * 
         * long retrievedOffsetForTimestamp = retrTimestamp.getTime() -
         * timestampOnServer.getTime();
         * 
         * assertTrue(
         * "Difference between original timestamp and timestamp retrieved using client timezone is not "
         * + offsetDifference, (Math .abs(retrievedOffsetForTimestamp -
         * offsetDifference) < epsillon));
         * 
         * String retrTimeString = new String(this.rs.getBytes(2)); Time
         * retrTime = this.rs.getTime(2);
         * 
         * java.util.Date timeOnServerAsDate = timeFormat
         * .parse(retrTimeString); Time timeOnServer = new
         * Time(timeOnServerAsDate.getTime());
         * 
         * long retrievedOffsetForTime = retrTime.getTime() -
         * timeOnServer.getTime();
         * 
         * assertTrue(
         * "Difference between original times and time retrieved using client timezone is not "
         * + offsetDifference, (Math.abs(retrievedOffsetForTime -
         * offsetDifference) < epsillon)); } } finally {
         * this.stmt.executeUpdate("DROP TABLE IF EXISTS timeTest"); }
         */
    }

    public void testBug6823() throws SQLException {
        innerBug6823(true);
        innerBug6823(false);
    }

    public void testBug7461() throws Exception {
        String tableName = "testBug7461";

        try {
            createTable(tableName, "(field1 varchar(4))");
            File tempFile = File.createTempFile("mysql-test", ".txt");
            tempFile.deleteOnExit();

            FileOutputStream fOut = new FileOutputStream(tempFile);
            fOut.write("abcdefghijklmnop".getBytes());
            fOut.close();

            try {
                this.stmt.executeQuery("LOAD DATA LOCAL INFILE '" + tempFile.toString() + "' INTO TABLE " + tableName);
            } catch (SQLException sqlEx) {
                this.stmt.getWarnings();
            }

        } finally {
            dropTable(tableName);
        }

    }

    public void testBug8181() throws Exception {

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug8181");
            this.stmt.executeUpdate("CREATE TABLE testBug8181(col1 VARCHAR(20),col2 INT)");

            this.pstmt = this.conn.prepareStatement("INSERT INTO testBug8181(col1,col2) VALUES(?,?)");

            for (int i = 0; i < 20; i++) {
                this.pstmt.setString(1, "Test " + i);
                this.pstmt.setInt(2, i);
                this.pstmt.addBatch();
            }

            this.pstmt.executeBatch();

        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug8181");

            if (this.pstmt != null) {
                this.pstmt.close();
            }
        }
    }

    /**
     * Tests fix for BUG#8487 - PreparedStatements not creating streaming result
     * sets.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug8487() throws Exception {
        try {
            this.pstmt = this.conn.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            this.pstmt.setFetchSize(Integer.MIN_VALUE);
            this.rs = this.pstmt.executeQuery();
            try {
                this.conn.createStatement().executeQuery("SELECT 2");
                fail("Should have caught a streaming exception here");
            } catch (SQLException sqlEx) {
                assertTrue(sqlEx.getMessage() != null && sqlEx.getMessage().indexOf("Streaming") != -1);
            }

        } finally {
            if (this.rs != null) {
                while (this.rs.next()) {
                }

                this.rs.close();
            }

            if (this.pstmt != null) {
                this.pstmt.close();
            }
        }
    }

    /**
     * Tests multiple statement support with fix for BUG#9704.
     * 
     * @throws Exception
     */
    public void testBug9704() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            Connection multiStmtConn = null;
            Statement multiStmt = null;

            try {
                Properties props = new Properties();
                props.setProperty("allowMultiQueries", "true");

                multiStmtConn = getConnectionWithProps(props);

                multiStmt = multiStmtConn.createStatement();

                multiStmt.executeUpdate("DROP TABLE IF EXISTS testMultiStatements");
                multiStmt.executeUpdate("CREATE TABLE testMultiStatements (field1 VARCHAR(255), field2 INT, field3 DOUBLE)");
                multiStmt.executeUpdate("INSERT INTO testMultiStatements VALUES ('abcd', 1, 2)");

                multiStmt.execute("SELECT field1 FROM testMultiStatements WHERE field1='abcd'; UPDATE testMultiStatements SET field3=3;"
                        + "SELECT field3 FROM testMultiStatements WHERE field3=3");

                this.rs = multiStmt.getResultSet();

                assertTrue(this.rs.next());

                assertTrue("abcd".equals(this.rs.getString(1)));
                this.rs.close();

                // Next should be an update count...
                assertTrue(!multiStmt.getMoreResults());

                assertTrue("Update count was " + multiStmt.getUpdateCount() + ", expected 1", multiStmt.getUpdateCount() == 1);

                assertTrue(multiStmt.getMoreResults());

                this.rs = multiStmt.getResultSet();

                assertTrue(this.rs.next());

                assertTrue(this.rs.getDouble(1) == 3);

                // End of multi results
                assertTrue(!multiStmt.getMoreResults());
                assertTrue(multiStmt.getUpdateCount() == -1);
            } finally {
                if (multiStmt != null) {
                    multiStmt.executeUpdate("DROP TABLE IF EXISTS testMultiStatements");

                    multiStmt.close();
                }

                if (multiStmtConn != null) {
                    multiStmtConn.close();
                }
            }
        }
    }

    /**
     * Tests that you can close a statement twice without an NPE.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testCloseTwice() throws Exception {
        Statement closeMe = this.conn.createStatement();
        closeMe.close();
        closeMe.close();
    }

    public void testCsc4194() throws Exception {
        if (isRunningOnJdk131()) {
            return; // test not valid on JDK-1.3.1
        }

        try {
            "".getBytes("Windows-31J");
        } catch (UnsupportedEncodingException ex) {
            return; // test doesn't work on this platform
        }

        Connection sjisConn = null;
        Connection windows31JConn = null;

        try {
            String tableNameText = "testCsc4194Text";
            String tableNameBlob = "testCsc4194Blob";

            createTable(tableNameBlob, "(field1 BLOB)");
            String charset = "";

            if (versionMeetsMinimum(5, 0, 3) || versionMeetsMinimum(4, 1, 12)) {
                charset = " CHARACTER SET cp932";
            } else if (versionMeetsMinimum(4, 1, 0)) {
                charset = " CHARACTER SET sjis";
            }

            createTable(tableNameText, "(field1 TEXT)" + charset);

            Properties windows31JProps = new Properties();
            windows31JProps.setProperty("useUnicode", "true");
            windows31JProps.setProperty("characterEncoding", "Windows-31J");

            windows31JConn = getConnectionWithProps(windows31JProps);
            testCsc4194InsertCheckBlob(windows31JConn, tableNameBlob);

            if (versionMeetsMinimum(4, 1, 0)) {
                testCsc4194InsertCheckText(windows31JConn, tableNameText, "Windows-31J");
            }

            Properties sjisProps = new Properties();
            sjisProps.setProperty("useUnicode", "true");
            sjisProps.setProperty("characterEncoding", "sjis");

            sjisConn = getConnectionWithProps(sjisProps);
            testCsc4194InsertCheckBlob(sjisConn, tableNameBlob);

            if (versionMeetsMinimum(5, 0, 3)) {
                testCsc4194InsertCheckText(sjisConn, tableNameText, "Windows-31J");
            }

        } finally {

            if (windows31JConn != null) {
                windows31JConn.close();
            }

            if (sjisConn != null) {
                sjisConn.close();
            }
        }
    }

    private void testCsc4194InsertCheckBlob(Connection c, String tableName) throws Exception {
        byte[] bArray = new byte[] { (byte) 0xac, (byte) 0xed, (byte) 0x00, (byte) 0x05 };

        PreparedStatement testStmt = c.prepareStatement("INSERT INTO " + tableName + " VALUES (?)");
        testStmt.setBytes(1, bArray);
        testStmt.executeUpdate();

        this.rs = c.createStatement().executeQuery("SELECT field1 FROM " + tableName);
        assertTrue(this.rs.next());
        assertEquals(getByteArrayString(bArray), getByteArrayString(this.rs.getBytes(1)));
        this.rs.close();
    }

    private void testCsc4194InsertCheckText(Connection c, String tableName, String encoding) throws Exception {
        byte[] kabuInShiftJIS = { (byte) 0x87, // a double-byte charater("kabu") in Shift JIS
                (byte) 0x8a, };

        String expected = new String(kabuInShiftJIS, encoding);
        PreparedStatement testStmt = c.prepareStatement("INSERT INTO " + tableName + " VALUES (?)");
        testStmt.setString(1, expected);
        testStmt.executeUpdate();

        this.rs = c.createStatement().executeQuery("SELECT field1 FROM " + tableName);
        assertTrue(this.rs.next());
        assertEquals(expected, this.rs.getString(1));
        this.rs.close();
    }

    /**
     * Tests all forms of statements influencing getGeneratedKeys().
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testGetGeneratedKeysAllCases() throws Exception {
        if (isRunningOnJdk131()) {
            return; // test not valid on JDK-1.3.1
        }

        System.out.println("Using Statement.executeUpdate()\n");

        try {
            createGGKTables();

            // Do the tests
            for (int i = 0; i < tests.length; i++) {
                doGGKTestStatement(tests[i], true);
            }
        } finally {
            dropGGKTables();
        }

        nextID = 1;
        count = 0;

        System.out.println("Using Statement.execute()\n");

        try {
            createGGKTables();

            // Do the tests
            for (int i = 0; i < tests.length; i++) {
                doGGKTestStatement(tests[i], false);
            }
        } finally {
            dropGGKTables();
        }

        nextID = 1;
        count = 0;

        System.out.println("Using PreparedStatement.executeUpdate()\n");

        try {
            createGGKTables();

            // Do the tests
            for (int i = 0; i < tests.length; i++) {
                doGGKTestPreparedStatement(tests[i], true);
            }
        } finally {
            dropGGKTables();
        }

        nextID = 1;
        count = 0;

        System.out.println("Using PreparedStatement.execute()\n");

        try {
            createGGKTables();

            // Do the tests
            for (int i = 0; i < tests.length; i++) {
                doGGKTestPreparedStatement(tests[i], false);
            }
        } finally {
            dropGGKTables();
        }
    }

    /**
     * Tests that max_rows and 'limit' don't cause exceptions to be thrown.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testLimitAndMaxRows() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testMaxRowsAndLimit");
            this.stmt.executeUpdate("CREATE TABLE testMaxRowsAndLimit(limitField INT)");

            for (int i = 0; i < 500; i++) {
                this.stmt.executeUpdate("INSERT INTO testMaxRowsAndLimit VALUES (" + i + ")");
            }

            this.stmt.setMaxRows(250);
            this.stmt.executeQuery("SELECT limitField FROM testMaxRowsAndLimit");
        } finally {
            this.stmt.setMaxRows(0);

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testMaxRowsAndLimit");
        }
    }

    /*
     * public void testBug9595() throws Exception { double[] vals = new double[]
     * {52.21, 52.22, 52.23, 52.24};
     * 
     * createTable("testBug9595", "(field1 DECIMAL(10,2), sortField INT)");
     * 
     * this.pstmt = this.conn.prepareStatement("INSERT INTO testBug9595 VALUES
     * (?, ?)"); // Try setting as doubles for (int i = 0; i < vals.length; i++)
     * { this.pstmt.setDouble(1, vals[i]); this.pstmt.setInt(2, i);
     * this.pstmt.executeUpdate(); }
     * 
     * this.pstmt = this.conn.prepareStatement("SELECT field1 FROM testBug9595
     * ORDER BY sortField"); this.rs = this.pstmt.executeQuery();
     * 
     * int i = 0;
     * 
     * while (this.rs.next()) { double valToTest = vals[i++];
     * 
     * assertEquals(this.rs.getDouble(1), valToTest, 0.001);
     * assertEquals(this.rs.getBigDecimal(1).doubleValue(), valToTest, 0.001); }
     * 
     * this.pstmt = this.conn.prepareStatement("INSERT INTO testBug9595 VALUES
     * (?, ?)");
     * 
     * this.stmt.executeUpdate("TRUNCATE TABLE testBug9595"); // Now, as
     * BigDecimals for (i = 0; i < vals.length; i++) { BigDecimal foo = new
     * BigDecimal(vals[i]);
     * 
     * this.pstmt.setObject(1, foo, Types.DECIMAL, 2); this.pstmt.setInt(2, i);
     * this.pstmt.executeUpdate(); }
     * 
     * this.pstmt = this.conn.prepareStatement("SELECT field1 FROM testBug9595
     * ORDER BY sortField"); this.rs = this.pstmt.executeQuery();
     * 
     * i = 0;
     * 
     * while (this.rs.next()) { double valToTest = vals[i++];
     * System.out.println(this.rs.getString(1));
     * assertEquals(this.rs.getDouble(1), valToTest, 0.001);
     * assertEquals(this.rs.getBigDecimal(1).doubleValue(), valToTest, 0.001); }
     * }
     */

    /**
     * Tests that 'LOAD DATA LOCAL INFILE' works
     * 
     * @throws Exception
     *             if any errors occur
     */
    public void testLoadData() throws Exception {
        try {
            //int maxAllowedPacket = 1048576;

            this.stmt.executeUpdate("DROP TABLE IF EXISTS loadDataRegress");
            this.stmt.executeUpdate("CREATE TABLE loadDataRegress (field1 int, field2 int)");

            File tempFile = File.createTempFile("mysql", ".txt");

            // tempFile.deleteOnExit();
            System.out.println(tempFile);

            Writer out = new FileWriter(tempFile);

            int localCount = 0;
            int rowCount = 128; // maxAllowedPacket * 4;

            for (int i = 0; i < rowCount; i++) {
                out.write((localCount++) + "\t" + (localCount++) + "\n");
            }

            out.close();

            StringBuffer fileNameBuf = null;

            if (File.separatorChar == '\\') {
                fileNameBuf = new StringBuffer();

                String fileName = tempFile.getAbsolutePath();
                int fileNameLength = fileName.length();

                for (int i = 0; i < fileNameLength; i++) {
                    char c = fileName.charAt(i);

                    if (c == '\\') {
                        fileNameBuf.append("/");
                    } else {
                        fileNameBuf.append(c);
                    }
                }
            } else {
                fileNameBuf = new StringBuffer(tempFile.getAbsolutePath());
            }

            int updateCount = this.stmt.executeUpdate("LOAD DATA LOCAL INFILE '" + fileNameBuf.toString() + "' INTO TABLE loadDataRegress CHARACTER SET "
                    + CharsetMapping.getMysqlCharsetForJavaEncoding(((MySQLConnection) this.conn).getEncoding(), (com.mysql.jdbc.Connection) this.conn));
            assertTrue(updateCount == rowCount);
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS loadDataRegress");
        }
    }

    public void testNullClob() throws Exception {
        createTable("testNullClob", "(field1 TEXT NULL)");

        PreparedStatement pStmt = null;

        try {
            pStmt = this.conn.prepareStatement("INSERT INTO testNullClob VALUES (?)");
            pStmt.setClob(1, null);
            pStmt.executeUpdate();
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Tests fix for BUG#1658
     * 
     * @throws Exception
     *             if the fix for parameter bounds checking doesn't work.
     */
    public void testParameterBoundsCheck() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testParameterBoundsCheck");
            this.stmt.executeUpdate("CREATE TABLE testParameterBoundsCheck(f1 int, f2 int, f3 int, f4 int, f5 int)");

            PreparedStatement _pstmt = this.conn.prepareStatement("UPDATE testParameterBoundsCheck SET f1=?, f2=?,f3=?,f4=? WHERE f5=?");

            _pstmt.setString(1, "");
            _pstmt.setString(2, "");

            try {
                _pstmt.setString(25, "");
            } catch (SQLException sqlEx) {
                assertTrue(SQLError.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
            }
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testParameterBoundsCheck");
        }
    }

    public void testPStmtTypesBug() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testPStmtTypesBug");
            this.stmt.executeUpdate("CREATE TABLE testPStmtTypesBug(field1 INT)");
            this.pstmt = this.conn.prepareStatement("INSERT INTO testPStmtTypesBug VALUES (?)");
            this.pstmt.setObject(1, null, Types.INTEGER);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();

        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testPStmtTypesBug");
        }
    }

    /**
     * Tests fix for BUG#1511
     * 
     * @throws Exception
     *             if the quoteid parsing fix in PreparedStatement doesn't work.
     */
    public void testQuotedIdRecognition() throws Exception {
        if (!this.versionMeetsMinimum(4, 1)) {
            try {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS testQuotedId");
                this.stmt.executeUpdate("CREATE TABLE testQuotedId (col1 VARCHAR(32))");

                PreparedStatement pStmt = this.conn.prepareStatement("SELECT * FROM testQuotedId WHERE col1='ABC`DEF' or col1=?");
                pStmt.setString(1, "foo");
                pStmt.execute();

                this.stmt.executeUpdate("DROP TABLE IF EXISTS testQuotedId2");
                this.stmt.executeUpdate("CREATE TABLE testQuotedId2 (`Works?` INT)");
                pStmt = this.conn.prepareStatement("INSERT INTO testQuotedId2 (`Works?`) VALUES (?)");
                pStmt.setInt(1, 1);
                pStmt.executeUpdate();
            } finally {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS testQuotedId");
                this.stmt.executeUpdate("DROP TABLE IF EXISTS testQuotedId2");
            }
        }
    }

    /**
     * Tests for BUG#9288, parameter index out of range if LIKE, ESCAPE '\'
     * present in query.
     * 
     * @throws Exception
     *             if the test fails.
     */
    /*
     * public void testBug9288() throws Exception { String tableName =
     * "testBug9288"; PreparedStatement pStmt = null;
     * 
     * try { createTable(tableName, "(field1 VARCHAR(32), field2 INT)"); pStmt =
     * ((com.mysql.jdbc.Connection)this.conn).clientPrepareStatement( "SELECT
     * COUNT(1) FROM " + tableName + " WHERE " + "field1 LIKE '%' ESCAPE '\\'
     * AND " + "field2 > ?"); pStmt.setInt(1, 0);
     * 
     * this.rs = pStmt.executeQuery(); } finally { if (this.rs != null) {
     * this.rs.close(); this.rs = null; }
     * 
     * if (pStmt != null) { pStmt.close(); } } }
     */

    /*
     * public void testBug10999() throws Exception { if (versionMeetsMinimum(5,
     * 0, 5)) {
     * 
     * String tableName = "testBug10999"; String updateTrigName =
     * "testBug10999Update"; String insertTrigName = "testBug10999Insert"; try {
     * createTable(tableName, "(pkfield INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
     * field1 VARCHAR(32))");
     * 
     * try { this.stmt.executeUpdate("DROP TRIGGER " + updateTrigName); } catch
     * (SQLException sqlEx) { // ignore for now }
     * 
     * this.stmt.executeUpdate("CREATE TRIGGER " + updateTrigName + " AFTER
     * UPDATE ON " + tableName + " FOR EACH ROW " + "BEGIN " + "END");
     * 
     * try { this.stmt.executeUpdate("DROP TRIGGER " + insertTrigName); } catch
     * (SQLException sqlEx) { // ignore }
     * 
     * this.stmt.executeUpdate("CREATE TRIGGER " + insertTrigName + " AFTER
     * INSERT ON " + tableName + " FOR EACH ROW " + " BEGIN " + "END");
     * 
     * this.conn.setAutoCommit(false);
     * 
     * String updateSQL = "INSERT INTO " + tableName + " (field1) VALUES
     * ('abcdefg')"; int rowCount = this.stmt.executeUpdate(updateSQL,
     * Statement.RETURN_GENERATED_KEYS);
     * 
     * this.rs = stmt.getGeneratedKeys(); if (rs.next()) {
     * System.out.println(rs.getInt(1)); int id = rs.getInt(1); //if
     * (log.isDebugEnabled()) // log.debug("Retrieved ID = " + id); } //else {
     * //log.error("Can't retrieve ID with getGeneratedKeys."); // Retrieve ID
     * using a SELECT statement instead. // querySQL = "SELECT id from tab1
     * WHERE ...";
     * 
     * //if (log.isDebugEnabled()) // log.debug(querySQL);
     * 
     * //rs = stmt.executeQuery(querySQL); this.rs =
     * this.stmt.executeQuery("SELECT pkfield FROM " + tableName); } finally {
     * this.conn.setAutoCommit(true);
     * 
     * try { this.stmt.executeUpdate("DROP TRIGGER IF EXISTS " +
     * insertTrigName); } catch (SQLException sqlEx) { // ignore }
     * 
     * try { this.stmt.executeUpdate("DROP TRIGGER IF EXISTS " +
     * updateTrigName); } catch (SQLException sqlEx) { // ignore } } } }
     */

    /**
     * Tests that binary dates/times are encoded/decoded correctly.
     * 
     * @throws Exception
     *             if the test fails.
     * 
     * @deprecated because we need to use this particular constructor for the
     *             date class, as Calendar-constructed dates don't pass the
     *             .equals() test :(
     */
    @Deprecated
    public void testServerPrepStmtAndDate() throws Exception {
        createTable("testServerPrepStmtAndDate",
                "(`P_ID` int(10) NOT NULL default '0', `R_Date` date default NULL, UNIQUE KEY `P_ID` (`P_ID`), KEY `R_Date` (`R_Date`))");
        Date dt = new java.sql.Date(102, 1, 2); // Note, this represents the date 2002-02-02

        PreparedStatement pStmt2 = this.conn.prepareStatement("INSERT INTO testServerPrepStmtAndDate (P_ID, R_Date) VALUES (171576, ?)");
        pStmt2.setDate(1, dt);
        pStmt2.executeUpdate();
        pStmt2.close();

        this.rs = this.stmt.executeQuery("SELECT R_Date FROM testServerPrepStmtAndDate");
        this.rs.next();

        System.out.println("Date that was stored (as String) " + this.rs.getString(1)); // comes back as 2002-02-02

        PreparedStatement pStmt = this.conn.prepareStatement("Select P_ID,R_Date from testServerPrepStmtAndDate Where R_Date = ?   and P_ID = 171576");
        pStmt.setDate(1, dt);

        this.rs = pStmt.executeQuery();

        assertTrue(this.rs.next());

        assertEquals("171576", this.rs.getString(1));

        assertEquals(dt, this.rs.getDate(2));
    }

    public void testServerPrepStmtDeadlock() throws Exception {

        Connection c = getConnectionWithProps((Properties) null);

        Thread testThread1 = new PrepareThread(c);
        Thread testThread2 = new PrepareThread(c);
        testThread1.start();
        testThread2.start();
        Thread.sleep(30000);
        assertTrue(this.testServerPrepStmtDeadlockCounter >= 10);
    }

    /**
     * Tests PreparedStatement.setCharacterStream() to ensure it accepts > 4K
     * streams
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testSetCharacterStream() throws Exception {
        try {
            ((com.mysql.jdbc.Connection) this.conn).setTraceProtocol(true);

            this.stmt.executeUpdate("DROP TABLE IF EXISTS charStreamRegressTest");
            this.stmt.executeUpdate("CREATE TABLE charStreamRegressTest(field1 text)");

            this.pstmt = this.conn.prepareStatement("INSERT INTO charStreamRegressTest VALUES (?)");

            // char[] charBuf = new char[16384];
            char[] charBuf = new char[32];

            for (int i = 0; i < charBuf.length; i++) {
                charBuf[i] = 'A';
            }

            CharArrayReader reader = new CharArrayReader(charBuf);

            this.pstmt.setCharacterStream(1, reader, charBuf.length);
            this.pstmt.executeUpdate();

            this.rs = this.stmt.executeQuery("SELECT LENGTH(field1) FROM charStreamRegressTest");

            this.rs.next();

            System.out.println("Character stream length: " + this.rs.getString(1));

            this.rs = this.stmt.executeQuery("SELECT field1 FROM charStreamRegressTest");

            this.rs.next();

            String result = this.rs.getString(1);

            assertTrue(result.length() == charBuf.length);

            this.stmt.execute("TRUNCATE TABLE charStreamRegressTest");

            // Test that EOF is not thrown
            reader = new CharArrayReader(charBuf);
            this.pstmt.clearParameters();
            this.pstmt.setCharacterStream(1, reader, charBuf.length);
            this.pstmt.executeUpdate();

            this.rs = this.stmt.executeQuery("SELECT LENGTH(field1) FROM charStreamRegressTest");

            this.rs.next();

            System.out.println("Character stream length: " + this.rs.getString(1));

            this.rs = this.stmt.executeQuery("SELECT field1 FROM charStreamRegressTest");

            this.rs.next();

            result = this.rs.getString(1);

            assertTrue("Retrieved value of length " + result.length() + " != length of inserted value " + charBuf.length, result.length() == charBuf.length);

            // Test single quotes inside identifers
            this.stmt.executeUpdate("DROP TABLE IF EXISTS `charStream'RegressTest`");
            this.stmt.executeUpdate("CREATE TABLE `charStream'RegressTest`(field1 text)");

            this.pstmt = this.conn.prepareStatement("INSERT INTO `charStream'RegressTest` VALUES (?)");

            reader = new CharArrayReader(charBuf);
            this.pstmt.setCharacterStream(1, reader, (charBuf.length * 2));
            this.pstmt.executeUpdate();

            this.rs = this.stmt.executeQuery("SELECT field1 FROM `charStream'RegressTest`");

            this.rs.next();

            result = this.rs.getString(1);

            assertTrue("Retrieved value of length " + result.length() + " != length of inserted value " + charBuf.length, result.length() == charBuf.length);
        } finally {
            ((com.mysql.jdbc.Connection) this.conn).setTraceProtocol(false);

            if (this.rs != null) {
                try {
                    this.rs.close();
                } catch (Exception ex) {
                    // ignore
                }

                this.rs = null;
            }

            this.stmt.executeUpdate("DROP TABLE IF EXISTS `charStream'RegressTest`");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS charStreamRegressTest");
        }
    }

    /**
     * Tests a bug where Statement.setFetchSize() does not work for values other
     * than 0 or Integer.MIN_VALUE
     * 
     * @throws Exception
     *             if any errors occur
     */
    public void testSetFetchSize() throws Exception {
        int oldFetchSize = this.stmt.getFetchSize();

        try {
            this.stmt.setFetchSize(10);
        } finally {
            this.stmt.setFetchSize(oldFetchSize);
        }
    }

    /**
     * Tests fix for BUG#907
     * 
     * @throws Exception
     *             if an error occurs
     */
    public void testSetMaxRows() throws Exception {
        Statement maxRowsStmt = null;

        try {
            maxRowsStmt = this.conn.createStatement();
            maxRowsStmt.setMaxRows(1);
            maxRowsStmt.executeQuery("SELECT 1");
        } finally {
            if (maxRowsStmt != null) {
                maxRowsStmt.close();
            }
        }
    }

    /**
     * Tests for timestamp NPEs occuring in binary-format timestamps.
     * 
     * @throws Exception
     * 
     * @deprecated yes, we know we are using deprecated methods here :)
     */
    @Deprecated
    public void testTimestampNPE() throws Exception {
        try {
            Timestamp ts = new Timestamp(System.currentTimeMillis());

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testTimestampNPE");
            this.stmt.executeUpdate("CREATE TABLE testTimestampNPE (field1 TIMESTAMP)");

            this.pstmt = this.conn.prepareStatement("INSERT INTO testTimestampNPE VALUES (?)");
            this.pstmt.setTimestamp(1, ts);
            this.pstmt.executeUpdate();

            this.pstmt = this.conn.prepareStatement("SELECT field1 FROM testTimestampNPE");

            this.rs = this.pstmt.executeQuery();

            this.rs.next();

            System.out.println(this.rs.getString(1));

            this.rs.getDate(1);

            Timestamp rTs = this.rs.getTimestamp(1);
            assertTrue("Retrieved year of " + rTs.getYear() + " does not match " + ts.getYear(), rTs.getYear() == ts.getYear());
            assertTrue("Retrieved month of " + rTs.getMonth() + " does not match " + ts.getMonth(), rTs.getMonth() == ts.getMonth());
            assertTrue("Retrieved date of " + rTs.getDate() + " does not match " + ts.getDate(), rTs.getDate() == ts.getDate());

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testTimestampNPE");

        } finally {
        }
    }

    public void testTruncationWithChar() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testTruncationWithChar");
            this.stmt.executeUpdate("CREATE TABLE testTruncationWithChar (field1 char(2))");

            this.pstmt = this.conn.prepareStatement("INSERT INTO testTruncationWithChar VALUES (?)");
            this.pstmt.setString(1, "00");
            this.pstmt.executeUpdate();
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testTruncationWithChar");
        }
    }

    /**
     * Tests fix for updatable streams being supported in updatable result sets.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testUpdatableStream() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS updateStreamTest");
            this.stmt.executeUpdate("CREATE TABLE updateStreamTest (keyField INT NOT NULL AUTO_INCREMENT PRIMARY KEY, field1 BLOB)");

            int streamLength = 16385;
            byte[] streamData = new byte[streamLength];

            /* create an updatable statement */
            Statement updStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);

            /* fill the resultset with some values */
            ResultSet updRs = updStmt.executeQuery("SELECT * FROM updateStreamTest");

            /* move to insertRow */
            updRs.moveToInsertRow();

            /* update the table */
            updRs.updateBinaryStream("field1", new ByteArrayInputStream(streamData), streamLength);

            updRs.insertRow();
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS updateStreamTest");
        }
    }

    /**
     * Tests fix for BUG#15383 - PreparedStatement.setObject() serializes
     * BigInteger as object, rather than sending as numeric value (and is thus
     * not complementary to .getObject() on an UNSIGNED LONG type).
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug15383() throws Exception {
        createTable("testBug15383", "(id INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,value BIGINT UNSIGNED NULL DEFAULT 0,PRIMARY KEY(id))", "InnoDB");

        this.stmt.executeUpdate("INSERT INTO testBug15383(value) VALUES(1)");

        Statement updatableStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

        try {
            this.rs = updatableStmt.executeQuery("SELECT * from testBug15383");

            assertTrue(this.rs.next());

            Object bigIntObj = this.rs.getObject("value");
            assertEquals("java.math.BigInteger", bigIntObj.getClass().getName());

            this.rs.updateObject("value", new BigInteger("3"));
            this.rs.updateRow();

            assertEquals("3", this.rs.getString("value"));
        } finally {
            if (this.rs != null) {
                ResultSet toClose = this.rs;
                this.rs = null;
                toClose.close();
            }

            if (updatableStmt != null) {
                updatableStmt.close();
            }
        }
    }

    /**
     * Tests fix for BUG#17099 - Statement.getGeneratedKeys() throws NPE when no
     * query has been processed.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug17099() throws Exception {
        if (isRunningOnJdk131()) {
            return; // test not valid
        }

        PreparedStatement pStmt = this.conn.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS);
        assertNotNull(pStmt.getGeneratedKeys());

        if (versionMeetsMinimum(4, 1)) {
            pStmt = ((com.mysql.jdbc.Connection) this.conn).clientPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS);
            assertNotNull(pStmt.getGeneratedKeys());
        }
    }

    /**
     * Tests fix for BUG#17587 - clearParameters() on a closed prepared
     * statement causes NPE.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug17587() throws Exception {
        createTable("testBug17857", "(field1 int)");
        PreparedStatement pStmt = null;

        try {
            pStmt = this.conn.prepareStatement("INSERT INTO testBug17857 VALUES (?)");
            pStmt.close();
            try {
                pStmt.clearParameters();
            } catch (SQLException sqlEx) {
                assertEquals("08003", sqlEx.getSQLState());
            }

            pStmt = ((com.mysql.jdbc.Connection) this.conn).clientPrepareStatement("INSERT INTO testBug17857 VALUES (?)");
            pStmt.close();
            try {
                pStmt.clearParameters();
            } catch (SQLException sqlEx) {
                assertEquals("08003", sqlEx.getSQLState());
            }

        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Tests fix for BUG#18740 - Data truncation and getWarnings() only returns
     * last warning in set.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug18740() throws Exception {
        if (!versionMeetsMinimum(5, 0, 2)) {
            createTable("testWarnings", "(field1 smallint(6), field2 varchar(6), UNIQUE KEY field1(field1))");

            try {
                this.stmt.executeUpdate("INSERT INTO testWarnings VALUES (10001, 'data1'), (10002, 'data2 foo'), (10003, 'data3'),"
                        + "(10004999, 'data4'), (10005, 'data5')");
            } catch (SQLException sqlEx) {
                String sqlStateToCompare = "01004";

                if (isJdbc4()) {
                    sqlStateToCompare = "22001";
                }

                assertEquals(sqlStateToCompare, sqlEx.getSQLState());
                assertEquals(sqlStateToCompare, sqlEx.getNextException().getSQLState());

                SQLWarning sqlWarn = this.stmt.getWarnings();
                assertEquals("01000", sqlWarn.getSQLState());
                assertEquals("01000", sqlWarn.getNextWarning().getSQLState());
            }
        }
    }

    protected boolean isJdbc4() {
        boolean isJdbc4;

        try {
            Class.forName("java.sql.Wrapper");
            isJdbc4 = true;
        } catch (Throwable t) {
            isJdbc4 = false;
        }

        return isJdbc4;
    }

    /**
     * Tests fix for BUG#19615, PreparedStatement.setObject(int, Object, int)
     * doesn't respect scale of BigDecimals.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug19615() throws Exception {
        createTable("testBug19615", "(field1 DECIMAL(19, 12))");

        BigDecimal dec = new BigDecimal("1.234567");

        this.pstmt = this.conn.prepareStatement("INSERT INTO testBug19615 VALUES (?)");
        this.pstmt.setObject(1, dec, Types.DECIMAL);
        this.pstmt.executeUpdate();
        this.pstmt.close();

        this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug19615");
        this.rs.next();
        assertEquals(dec, this.rs.getBigDecimal(1).setScale(6));
        this.rs.close();
        this.stmt.executeUpdate("TRUNCATE TABLE testBug19615");

        this.pstmt = ((com.mysql.jdbc.Connection) this.conn).clientPrepareStatement("INSERT INTO testBug19615 VALUES (?)");
        this.pstmt.setObject(1, dec, Types.DECIMAL);
        this.pstmt.executeUpdate();
        this.pstmt.close();

        this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug19615");
        this.rs.next();
        assertEquals(dec, this.rs.getBigDecimal(1).setScale(6));
        this.rs.close();
    }

    /**
     * Tests fix for BUG#20029 - NPE thrown from executeBatch().
     * 
     * @throws Exception
     */
    public void testBug20029() throws Exception {
        createTable("testBug20029", ("(field1 int)"));

        long initialTimeout = 20; // may need to raise this depending on environment we try and do this automatically in this testcase

        for (int i = 0; i < 10; i++) {
            final Connection toBeKilledConn = getConnectionWithProps(new Properties());
            final long timeout = initialTimeout;
            PreparedStatement toBeKilledPstmt = null;

            try {
                toBeKilledPstmt = ((com.mysql.jdbc.Connection) toBeKilledConn).clientPrepareStatement("INSERT INTO testBug20029 VALUES (?)");

                for (int j = 0; j < 1000; j++) {
                    toBeKilledPstmt.setInt(1, j);
                    toBeKilledPstmt.addBatch();
                }

                Thread t = new Thread() {
                    @Override
                    public void run() {
                        try {
                            sleep(timeout);
                            toBeKilledConn.close();
                        } catch (Throwable thr) {

                        }
                    }
                };

                t.start();

                try {
                    if (!toBeKilledConn.isClosed()) {
                        initialTimeout *= 2;
                        continue;
                    }

                    toBeKilledPstmt.executeBatch();
                    fail("Should've caught a SQLException for the statement being closed here");
                } catch (BatchUpdateException batchEx) {
                    assertEquals("08003", batchEx.getSQLState());
                    break;
                } catch (SQLException sqlEx) {
                    assertEquals("08003", sqlEx.getSQLState());
                    break;
                }

                fail("Connection didn't close while in the middle of PreparedStatement.executeBatch()");
            } finally {
                if (toBeKilledPstmt != null) {
                    toBeKilledPstmt.close();
                }

                if (toBeKilledConn != null) {
                    toBeKilledConn.close();
                }
            }
        }
    }

    /**
     * Fixes BUG#20687 - Can't pool server-side prepared statements, exception
     * raised when re-using them.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20687() throws Exception {
        if (!isRunningOnJdk131() && versionMeetsMinimum(5, 0)) {
            createTable("testBug20687", "(field1 int)");
            Connection poolingConn = null;

            Properties props = new Properties();
            props.setProperty("cachePrepStmts", "true");
            props.setProperty("useServerPrepStmts", "true");
            PreparedStatement pstmt1 = null;
            PreparedStatement pstmt2 = null;

            try {
                poolingConn = getConnectionWithProps(props);
                pstmt1 = poolingConn.prepareStatement("SELECT field1 FROM testBug20687");
                pstmt1.executeQuery();
                pstmt1.close();

                pstmt2 = poolingConn.prepareStatement("SELECT field1 FROM testBug20687");
                pstmt2.executeQuery();
                assertTrue(pstmt1 == pstmt2);
                pstmt2.close();
            } finally {
                if (pstmt1 != null) {
                    pstmt1.close();
                }

                if (pstmt2 != null) {
                    pstmt2.close();
                }

                if (poolingConn != null) {
                    poolingConn.close();
                }
            }
        }
    }

    public void testLikeWithBackslashes() throws Exception {
        if (!versionMeetsMinimum(5, 0, 0)) {
            return;
        }

        Connection noBackslashEscapesConn = null;

        try {
            Properties props = new Properties();
            props.setProperty("sessionVariables", "sql_mode=NO_BACKSLASH_ESCAPES");

            noBackslashEscapesConn = getConnectionWithProps(props);

            createTable("X_TEST",
                    "(userName varchar(32) not null, ivalue integer, CNAME varchar(255), bvalue CHAR(1), svalue varchar(255), ACTIVE CHAR(1), primary key (userName))");

            String insert_sql = "insert into X_TEST (ivalue, CNAME, bvalue, svalue, ACTIVE, userName) values (?, ?, ?, ?, ?, ?)";

            this.pstmt = noBackslashEscapesConn.prepareStatement(insert_sql);
            this.pstmt.setInt(1, 0);
            this.pstmt.setString(2, "c:\\jetson");
            this.pstmt.setInt(3, 1);
            this.pstmt.setString(4, "c:\\jetson");
            this.pstmt.setInt(5, 1);
            this.pstmt.setString(6, "c:\\jetson");
            this.pstmt.execute();

            String select_sql = "select user0_.userName as userName0_0_, user0_.ivalue as ivalue0_0_, user0_.CNAME as CNAME0_0_, user0_.bvalue as bvalue0_0_, user0_.svalue as svalue0_0_, user0_.ACTIVE as ACTIVE0_0_ from X_TEST user0_ where user0_.userName like ?";
            this.pstmt = noBackslashEscapesConn.prepareStatement(select_sql);
            this.pstmt.setString(1, "c:\\j%");
            // if we comment out the previous line and uncomment the following, the like clause matches
            // this.pstmt.setString(1,"c:\\\\j%");
            System.out.println("about to execute query " + select_sql);
            this.rs = this.pstmt.executeQuery();
            assertTrue(this.rs.next());
        } finally {
            if (noBackslashEscapesConn != null) {
                noBackslashEscapesConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#20650 - Statement.cancel() causes NullPointerException
     * if underlying connection has been closed due to server failure.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20650() throws Exception {
        Connection closedConn = null;
        Statement cancelStmt = null;

        try {
            closedConn = getConnectionWithProps((String) null);
            cancelStmt = closedConn.createStatement();

            closedConn.close();

            cancelStmt.cancel();
        } finally {
            if (cancelStmt != null) {
                cancelStmt.close();
            }

            if (closedConn != null && !closedConn.isClosed()) {
                closedConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#20888 - escape of quotes in client-side prepared
     * statements parsing not respected.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20888() throws Exception {
        String s = "SELECT 'What do you think about D\\'Artanian''?', \"What do you think about D\\\"Artanian\"\"?\"";
        this.pstmt = ((com.mysql.jdbc.Connection) this.conn).clientPrepareStatement(s);

        this.rs = this.pstmt.executeQuery();
        this.rs.next();
        assertEquals(this.rs.getString(1), "What do you think about D'Artanian'?");
        assertEquals(this.rs.getString(2), "What do you think about D\"Artanian\"?");
    }

    /**
     * Tests Bug#21207 - Driver throws NPE when tracing prepared statements that
     * have been closed (in asSQL()).
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug21207() throws Exception {
        this.pstmt = this.conn.prepareStatement("SELECT 1");
        this.pstmt.close();
        this.pstmt.toString(); // this used to cause an NPE
    }

    /**
     * Tests BUG#21438, server-side PS fails when using jdbcCompliantTruncation.
     * If either is set to FALSE (&useServerPrepStmts=false or
     * &jdbcCompliantTruncation=false) test succedes.
     * 
     * @throws Exception
     *             if the test fails.
     */

    public void testBug21438() throws Exception {
        createTable("testBug21438", "(t_id int(10), test_date timestamp NOT NULL,primary key t_pk (t_id));");

        assertEquals(1, this.stmt.executeUpdate("insert into testBug21438 values (1,NOW());"));

        if (this.versionMeetsMinimum(4, 1)) {
            this.pstmt = ((com.mysql.jdbc.Connection) this.conn)
                    .serverPrepareStatement("UPDATE testBug21438 SET test_date=ADDDATE(?,INTERVAL 1 YEAR) WHERE t_id=1;");
            Timestamp ts = new Timestamp(System.currentTimeMillis());
            ts.setNanos(999999999);

            this.pstmt.setTimestamp(1, ts);

            assertEquals(1, this.pstmt.executeUpdate());

            Timestamp future = (Timestamp) getSingleIndexedValueWithQuery(1, "SELECT test_date FROM testBug21438");
            assertEquals(future.getYear() - ts.getYear(), 1);
        }
    }

    /**
     * Tests fix for BUG#22359 - Driver was using millis for
     * Statement.setQueryTimeout() when spec says argument is seconds.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug22359() throws Exception {
        if (versionMeetsMinimum(5, 0)) {
            Statement timeoutStmt = null;

            try {
                timeoutStmt = this.conn.createStatement();
                timeoutStmt.setQueryTimeout(2);

                long begin = System.currentTimeMillis();

                try {
                    timeoutStmt.execute("SELECT SLEEP(30)");
                    fail("Query didn't time out");
                } catch (MySQLTimeoutException timeoutEx) {
                    long end = System.currentTimeMillis();

                    assertTrue((end - begin) > 1000);
                }
            } finally {
                if (timeoutStmt != null) {
                    timeoutStmt.close();
                }
            }
        }
    }

    /**
     * Tests fix for BUG#22290 - Driver issues truncation on write exception
     * when it shouldn't (due to sending big decimal incorrectly to server with
     * server-side prepared statement).
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug22290() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        createTable("testbug22290", "(`id` int(11) NOT NULL default '1',`cost` decimal(10,2) NOT NULL,PRIMARY KEY  (`id`)) DEFAULT CHARSET=utf8", "InnoDB");
        assertEquals(this.stmt.executeUpdate("INSERT INTO testbug22290 (`id`,`cost`) VALUES (1,'1.00')"), 1);

        Connection configuredConn = null;

        try {
            Properties props = new Properties();
            props.setProperty("sessionVariables", "sql_mode='STRICT_TRANS_TABLES'");

            configuredConn = getConnectionWithProps(props);

            this.pstmt = configuredConn.prepareStatement("update testbug22290 set cost = cost + ? where id = 1");
            this.pstmt.setBigDecimal(1, new BigDecimal("1.11"));
            assertEquals(this.pstmt.executeUpdate(), 1);

            assertEquals(this.stmt.executeUpdate("UPDATE testbug22290 SET cost='1.00'"), 1);
            this.pstmt = ((com.mysql.jdbc.Connection) configuredConn).clientPrepareStatement("update testbug22290 set cost = cost + ? where id = 1");
            this.pstmt.setBigDecimal(1, new BigDecimal("1.11"));
            assertEquals(this.pstmt.executeUpdate(), 1);
        } finally {
            if (configuredConn != null) {
                configuredConn.close();
            }
        }
    }

    public void testClientPreparedSetBoolean() throws Exception {
        this.pstmt = ((com.mysql.jdbc.Connection) this.conn).clientPrepareStatement("SELECT ?");
        this.pstmt.setBoolean(1, false);
        assertEquals("SELECT 0", this.pstmt.toString().substring(this.pstmt.toString().indexOf("SELECT")));
        this.pstmt.setBoolean(1, true);
        assertEquals("SELECT 1", this.pstmt.toString().substring(this.pstmt.toString().indexOf("SELECT")));
    }

    /**
     * Tests fix for BUG#24360 .setFetchSize() breaks prepared SHOW and other
     * commands.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug24360() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        Connection c = null;

        Properties props = new Properties();
        props.setProperty("useServerPrepStmts", "true");

        try {
            c = getConnectionWithProps(props);

            this.pstmt = c.prepareStatement("SHOW PROCESSLIST");
            this.pstmt.setFetchSize(5);
            this.pstmt.execute();
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    /**
     * Tests fix for BUG#24344 - useJDBCCompliantTimezoneShift with server-side
     * prepared statements gives different behavior than when using client-side
     * prepared statements. (this is now fixed if moving from server-side
     * prepared statements to client-side prepared statements by setting
     * "useSSPSCompatibleTimezoneShift" to "true", as the driver can't tell if
     * this is a new deployment that never used server-side prepared statements,
     * or if it is an existing deployment that is switching to client-side
     * prepared statements from server-side prepared statements.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug24344() throws Exception {

        if (!versionMeetsMinimum(4, 1)) {
            return; // need SSPS
        }

        super.createTable("testBug24344", "(i INT AUTO_INCREMENT, t1 DATETIME, PRIMARY KEY (i)) ENGINE = MyISAM");

        Connection conn2 = null;

        try {
            Properties props = new Properties();
            props.setProperty("useServerPrepStmts", "true");
            props.setProperty("useJDBCCompliantTimezoneShift", "true");
            conn2 = super.getConnectionWithProps(props);
            this.pstmt = conn2.prepareStatement("INSERT INTO testBug24344 (t1) VALUES (?)");
            Calendar c = Calendar.getInstance();
            this.pstmt.setTimestamp(1, new Timestamp(c.getTime().getTime()));
            this.pstmt.execute();
            this.pstmt.close();
            conn2.close();

            props.setProperty("useServerPrepStmts", "false");
            props.setProperty("useJDBCCompliantTimezoneShift", "true");
            props.setProperty("useSSPSCompatibleTimezoneShift", "true");

            conn2 = super.getConnectionWithProps(props);
            this.pstmt = conn2.prepareStatement("INSERT INTO testBug24344 (t1) VALUES (?)");
            this.pstmt.setTimestamp(1, new Timestamp(c.getTime().getTime()));
            this.pstmt.execute();
            this.pstmt.close();
            conn2.close();

            props.setProperty("useServerPrepStmts", "false");
            props.setProperty("useJDBCCompliantTimezoneShift", "false");
            props.setProperty("useSSPSCompatibleTimezoneShift", "false");
            conn2 = super.getConnectionWithProps(props);
            this.pstmt = conn2.prepareStatement("INSERT INTO testBug24344 (t1) VALUES (?)");
            this.pstmt.setTimestamp(1, new Timestamp(c.getTime().getTime()));
            this.pstmt.execute();
            this.pstmt.close();

            Statement s = conn2.createStatement();
            this.rs = s.executeQuery("SELECT t1 FROM testBug24344 ORDER BY i ASC");

            Timestamp[] dates = new Timestamp[3];

            int i = 0;

            while (this.rs.next()) {
                dates[i++] = this.rs.getTimestamp(1);
            }

            assertEquals("Number of rows should be 3.", 3, i);
            assertEquals(dates[0], dates[1]);
            if (TimeZone.getDefault().getOffset(c.getTimeInMillis()) != 0) {
                assertFalse(dates[1].equals(dates[2]));
            } else {
                assertTrue(dates[1].equals(dates[2]));
            }
        } finally {
            if (conn2 != null) {
                conn2.close();
            }
        }
    }

    /**
     * Tests fix for BUG#25073 - rewriting batched statements leaks internal
     * statement instances, and causes a memory leak.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug25073() throws Exception {
        if (isRunningOnJdk131()) {
            return;
        }

        Properties props = new Properties();
        props.setProperty("rewriteBatchedStatements", "true");
        Connection multiConn = getConnectionWithProps(props);
        createTable("testBug25073", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
        Statement multiStmt = multiConn.createStatement();
        multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (1)");
        multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (2)");
        multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (3)");
        multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (4)");
        multiStmt.addBatch("UPDATE testBug25073 SET field1=5 WHERE field1=1");
        multiStmt.addBatch("UPDATE testBug25073 SET field1=6 WHERE field1=2 OR field1=3");

        int beforeOpenStatementCount = ((com.mysql.jdbc.Connection) multiConn).getActiveStatementCount();

        multiStmt.executeBatch();

        int afterOpenStatementCount = ((com.mysql.jdbc.Connection) multiConn).getActiveStatementCount();

        assertEquals(beforeOpenStatementCount, afterOpenStatementCount);

        createTable("testBug25073", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
        props.clear();
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("maxAllowedPacket", "1024");
        props.setProperty("dumpQueriesOnException", "true");
        props.setProperty("maxQuerySizeToLog", String.valueOf(1024 * 1024 * 2));
        multiConn = getConnectionWithProps(props);
        multiStmt = multiConn.createStatement();

        for (int i = 0; i < 1000; i++) {
            multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (" + i + ")");
        }

        beforeOpenStatementCount = ((com.mysql.jdbc.Connection) multiConn).getActiveStatementCount();

        multiStmt.executeBatch();

        afterOpenStatementCount = ((com.mysql.jdbc.Connection) multiConn).getActiveStatementCount();

        assertEquals(beforeOpenStatementCount, afterOpenStatementCount);

        createTable("testBug25073", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");

        props.clear();
        props.setProperty("useServerPrepStmts", "false");
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("dumpQueriesOnException", "true");
        props.setProperty("maxQuerySizeToLog", String.valueOf(1024 * 1024 * 2));
        multiConn = getConnectionWithProps(props);
        PreparedStatement pStmt = multiConn.prepareStatement("INSERT INTO testBug25073(field1) VALUES (?)", Statement.RETURN_GENERATED_KEYS);

        for (int i = 0; i < 1000; i++) {
            pStmt.setInt(1, i);
            pStmt.addBatch();
        }

        beforeOpenStatementCount = ((com.mysql.jdbc.Connection) multiConn).getActiveStatementCount();

        pStmt.executeBatch();

        afterOpenStatementCount = ((com.mysql.jdbc.Connection) multiConn).getActiveStatementCount();

        assertEquals(beforeOpenStatementCount, afterOpenStatementCount);

        createTable("testBug25073", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
        props.setProperty("useServerPrepStmts", "false");
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("maxAllowedPacket", "1024");
        props.setProperty("dumpQueriesOnException", "true");
        props.setProperty("maxQuerySizeToLog", String.valueOf(1024 * 1024 * 2));
        multiConn = getConnectionWithProps(props);
        pStmt = multiConn.prepareStatement("INSERT INTO testBug25073(field1) VALUES (?)", Statement.RETURN_GENERATED_KEYS);

        for (int i = 0; i < 1000; i++) {
            pStmt.setInt(1, i);
            pStmt.addBatch();
        }

        beforeOpenStatementCount = ((com.mysql.jdbc.Connection) multiConn).getActiveStatementCount();

        pStmt.executeBatch();

        afterOpenStatementCount = ((com.mysql.jdbc.Connection) multiConn).getActiveStatementCount();

        assertEquals(beforeOpenStatementCount, afterOpenStatementCount);
    }

    /**
     * Tests fix for BUG#25009 - Results from updates not handled correctly in
     * multi-statement queries.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug25009() throws Exception {
        if (!versionMeetsMinimum(4, 1)) {
            return;
        }

        Properties props = new Properties();
        props.setProperty("allowMultiQueries", "true");

        Connection multiConn = getConnectionWithProps(props);
        createTable("testBug25009", "(field1 INT)");

        try {
            Statement multiStmt = multiConn.createStatement();
            multiStmt.execute("SELECT 1;SET @a=1; SET @b=2; SET @c=3; INSERT INTO testBug25009 VALUES (1)");

            assertEquals(-1, multiStmt.getUpdateCount());

            this.rs = multiStmt.getResultSet();
            assertTrue(this.rs.next());
            assertEquals(multiStmt.getMoreResults(), false);

            for (int i = 0; i < 3; i++) {
                assertEquals(0, multiStmt.getUpdateCount());
                assertEquals(multiStmt.getMoreResults(), false);
            }

            assertEquals(1, multiStmt.getUpdateCount());

            this.rs = multiStmt.executeQuery("SELECT field1 FROM testBug25009");
            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(1));

        } finally {
            if (multiConn != null) {
                multiConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#25025 - Client-side prepared statement parser gets
     * confused by in-line (slash-star) comments and therefore can't rewrite
     * batched statements or reliably detect type of statements when they're
     * used.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug25025() throws Exception {

        Connection multiConn = null;

        createTable("testBug25025", "(field1 INT)");

        try {
            Properties props = new Properties();
            props.setProperty("rewriteBatchedStatements", "true");
            props.setProperty("useServerPrepStmts", "false");

            multiConn = getConnectionWithProps(props);

            this.pstmt = multiConn
                    .prepareStatement("/* insert foo.bar.baz INSERT INTO foo VALUES (?,?,?,?) to trick parser */ INSERT into testBug25025 VALUES (?)");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.addBatch();

            int[] counts = this.pstmt.executeBatch();

            assertEquals(3, counts.length);
            assertEquals(Statement.SUCCESS_NO_INFO, counts[0]);
            assertEquals(Statement.SUCCESS_NO_INFO, counts[1]);
            assertEquals(Statement.SUCCESS_NO_INFO, counts[2]);
            assertEquals(true, ((com.mysql.jdbc.PreparedStatement) this.pstmt).canRewriteAsMultiValueInsertAtSqlLevel());
        } finally {
            if (multiConn != null) {
                multiConn.close();
            }
        }
    }

    public void testBustedGGKWithPSExecute() throws Exception {
        createTable("sequence", "(sequence_name VARCHAR(32) NOT NULL PRIMARY KEY, next_val BIGINT NOT NULL)");

        // Populate with the initial value
        this.stmt.executeUpdate("INSERT INTO sequence VALUES ('test-sequence', 1234)");

        // Atomic operation to increment and return next value
        PreparedStatement pStmt = null;

        try {
            pStmt = this.conn.prepareStatement("UPDATE sequence SET next_val=LAST_INSERT_ID(next_val + ?) WHERE sequence_name = ?",
                    Statement.RETURN_GENERATED_KEYS);

            pStmt.setInt(1, 4);
            pStmt.setString(2, "test-sequence");
            pStmt.execute();

            this.rs = pStmt.getGeneratedKeys();
            this.rs.next();
            assertEquals(1238, this.rs.getLong(1));
        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Tests fix for BUG#28256 - When connection is in read-only mode, queries
     * that are parentheized incorrectly identified as DML.
     * 
     * @throws Exception
     */
    public void testBug28256() throws Exception {
        try {
            this.conn.setReadOnly(true);
            this.stmt.execute("(SELECT 1) UNION (SELECT 2)");
            this.conn.prepareStatement("(SELECT 1) UNION (SELECT 2)").execute();
            if (versionMeetsMinimum(4, 1)) {
                ((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement("(SELECT 1) UNION (SELECT 2)").execute();
            }
        } finally {
            this.conn.setReadOnly(false);
        }
    }

    /**
     * Tests fix for BUG#28469 - PreparedStatement.getMetaData() for statements
     * containing leading one-line comments is not returned correctly.
     * 
     * As part of this fix, we also overhauled detection of DML for
     * executeQuery() and SELECTs for executeUpdate() in plain and prepared
     * statements to be aware of the same types of comments.
     * 
     * @throws Exception
     */
    public void testBug28469() throws Exception {
        PreparedStatement commentStmt = null;

        try {
            String[] statementsToTest = { "-- COMMENT\nSELECT 1", "# COMMENT\nSELECT 1", "/* comment */ SELECT 1" };

            for (int i = 0; i < statementsToTest.length; i++) {
                commentStmt = this.conn.prepareStatement(statementsToTest[i]);

                assertNotNull(commentStmt.getMetaData());

                try {
                    commentStmt.executeUpdate();
                    fail("Should not be able to call executeUpdate() on a SELECT statement!");
                } catch (SQLException sqlEx) {
                    // expected
                }

                this.rs = commentStmt.executeQuery();
                this.rs.next();
                assertEquals(1, this.rs.getInt(1));
            }

            createTable("testBug28469", "(field1 INT)");

            String[] updatesToTest = { "-- COMMENT\nUPDATE testBug28469 SET field1 = 2", "# COMMENT\nUPDATE testBug28469 SET field1 = 2",
                    "/* comment */ UPDATE testBug28469 SET field1 = 2" };

            for (int i = 0; i < updatesToTest.length; i++) {
                commentStmt = this.conn.prepareStatement(updatesToTest[i]);

                assertNull(commentStmt.getMetaData());

                try {
                    commentStmt.executeQuery();
                    fail("Should not be able to call executeQuery() on a SELECT statement!");
                } catch (SQLException sqlEx) {
                    // expected
                }

                try {
                    this.stmt.executeQuery(updatesToTest[i]);
                    fail("Should not be able to call executeQuery() on a SELECT statement!");
                } catch (SQLException sqlEx) {
                    // expected
                }
            }
        } finally {
            if (commentStmt != null) {
                commentStmt.close();
            }
        }
    }

    /**
     * Tests error with slash-star comment at EOL
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testCommentParsing() throws Exception {
        createTable("PERSON", "(NAME VARCHAR(32), PERID VARCHAR(32))");

        this.pstmt = this.conn.prepareStatement("SELECT NAME AS name2749_0_, PERID AS perid2749_0_ FROM PERSON WHERE PERID=? /*FOR UPDATE*/");
    }

    /**
     * Tests fix for BUG#28851 - parser in client-side prepared statements eats
     * character following '/' if it's not a multi-line comment.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug28851() throws Exception {
        this.pstmt = ((com.mysql.jdbc.Connection) this.conn).clientPrepareStatement("SELECT 1/?");
        this.pstmt.setInt(1, 1);
        this.rs = this.pstmt.executeQuery();

        assertTrue(this.rs.next());

        assertEquals(1, this.rs.getInt(1));

    }

    /**
     * Tests fix for BUG#28596 - parser in client-side prepared statements runs
     * to end of statement, rather than end-of-line for '#' comments.
     * 
     * Also added support for '--' single-line comments
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug28596() throws Exception {
        String query = "SELECT #\n?, #\n? #?\r\n,-- abcdefg \n?";

        this.pstmt = ((com.mysql.jdbc.Connection) this.conn).clientPrepareStatement(query);
        this.pstmt.setInt(1, 1);
        this.pstmt.setInt(2, 2);
        this.pstmt.setInt(3, 3);

        assertEquals(3, this.pstmt.getParameterMetaData().getParameterCount());
        this.rs = this.pstmt.executeQuery();

        assertTrue(this.rs.next());

        assertEquals(1, this.rs.getInt(1));
        assertEquals(2, this.rs.getInt(2));
        assertEquals(3, this.rs.getInt(3));
    }

    /**
     * Tests fix for BUG#30550 - executeBatch() on an empty batch when there are
     * no elements in the batch causes a divide-by-zero error when rewriting is
     * enabled.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug30550() throws Exception {
        createTable("testBug30550", "(field1 int)");

        Connection rewriteConn = getConnectionWithProps("rewriteBatchedStatements=true");
        PreparedStatement batchPStmt = null;
        Statement batchStmt = null;

        try {
            batchStmt = rewriteConn.createStatement();
            assertEquals(0, batchStmt.executeBatch().length);

            batchStmt.addBatch("INSERT INTO testBug30550 VALUES (1)");
            int[] counts = batchStmt.executeBatch();
            assertEquals(1, counts.length);
            assertEquals(1, counts[0]);
            assertEquals(0, batchStmt.executeBatch().length);

            batchPStmt = rewriteConn.prepareStatement("INSERT INTO testBug30550 VALUES (?)");
            batchPStmt.setInt(1, 1);
            assertEquals(0, batchPStmt.executeBatch().length);
            batchPStmt.addBatch();
            counts = batchPStmt.executeBatch();
            assertEquals(1, counts.length);
            assertEquals(1, counts[0]);
            assertEquals(0, batchPStmt.executeBatch().length);
        } finally {
            if (batchPStmt != null) {
                batchPStmt.close();
            }

            if (batchStmt != null) {
                batchStmt.close();
            }
            if (rewriteConn != null) {
                rewriteConn.close();
            }
        }
    }

    /**
     * Tests fix for Bug#27412 - cached metadata with
     * PreparedStatement.execute() throws NullPointerException.
     * 
     * @throws Exception
     */
    public void testBug27412() throws Exception {
        Properties props = new Properties();
        props.put("useServerPrepStmts", "false");
        props.put("cachePreparedStatements", "true");
        props.put("cacheResultSetMetadata", "true");
        Connection conn2 = getConnectionWithProps(props);
        PreparedStatement pstm = conn2.prepareStatement("SELECT 1");
        try {
            assertTrue(pstm.execute());
        } finally {
            pstm.close();
            conn2.close();
        }
    }

    public void testBustedGGKColumnNames() throws Exception {
        createTable("testBustedGGKColumnNames", "(field1 int primary key auto_increment)");
        this.stmt.executeUpdate("INSERT INTO testBustedGGKColumnNames VALUES (null)", Statement.RETURN_GENERATED_KEYS);
        assertEquals("GENERATED_KEY", this.stmt.getGeneratedKeys().getMetaData().getColumnName(1));

        this.pstmt = this.conn.prepareStatement("INSERT INTO testBustedGGKColumnNames VALUES (null)", Statement.RETURN_GENERATED_KEYS);
        this.pstmt.executeUpdate();
        assertEquals("GENERATED_KEY", this.pstmt.getGeneratedKeys().getMetaData().getColumnName(1));

        if (versionMeetsMinimum(4, 1, 0)) {
            this.pstmt = ((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement("INSERT INTO testBustedGGKColumnNames VALUES (null)",
                    Statement.RETURN_GENERATED_KEYS);
            this.pstmt.executeUpdate();
            assertEquals("GENERATED_KEY", this.pstmt.getGeneratedKeys().getMetaData().getColumnName(1));
        }

    }

    public void testLancesBitMappingBug() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        createTable("Bit_TabXXX", "( `MAX_VAL` BIT default NULL, `MIN_VAL` BIT default NULL, `NULL_VAL` BIT default NULL) DEFAULT CHARSET=latin1", "InnoDB");

        // add Bit_In_MinXXX procedure
        createProcedure("Bit_In_MinXXX", "(MIN_PARAM TINYINT(1)) begin update Bit_TabXXX set MIN_VAL=MIN_PARAM; end");

        createProcedure("Bit_In_MaxXXX", "(MAX_PARAM TINYINT(1)) begin update Bit_TabXXX set MAX_VAL=MAX_PARAM; end");

        this.stmt.execute("insert into Bit_TabXXX values(null,0,null)");

        String sPrepStmt = "{call Bit_In_MinXXX(?)}";
        this.pstmt = this.conn.prepareStatement(sPrepStmt);
        this.pstmt.setObject(1, "true", java.sql.Types.BIT);
        this.pstmt.executeUpdate();
        assertEquals("true", getSingleIndexedValueWithQuery(1, "SELECT MIN_VAL FROM Bit_TabXXX").toString());
        this.stmt.execute("TRUNCATE TABLE Bit_TabXXX");
        this.stmt.execute("insert into Bit_TabXXX values(null,0,null)");

        this.pstmt.setObject(1, "false", java.sql.Types.BIT);
        this.pstmt.executeUpdate();
        assertEquals("false", getSingleIndexedValueWithQuery(1, "SELECT MIN_VAL FROM Bit_TabXXX").toString());
        this.stmt.execute("TRUNCATE TABLE Bit_TabXXX");
        this.stmt.execute("insert into Bit_TabXXX values(null,0,null)");

        this.pstmt.setObject(1, "1", java.sql.Types.BIT); // fails
        this.pstmt.executeUpdate();
        assertEquals("true", getSingleIndexedValueWithQuery(1, "SELECT MIN_VAL FROM Bit_TabXXX").toString());
        this.stmt.execute("TRUNCATE TABLE Bit_TabXXX");
        this.stmt.execute("insert into Bit_TabXXX values(null,0,null)");

        this.pstmt.setObject(1, "0", java.sql.Types.BIT);
        this.pstmt.executeUpdate();
        assertEquals("false", getSingleIndexedValueWithQuery(1, "SELECT MIN_VAL FROM Bit_TabXXX").toString());
        this.stmt.execute("TRUNCATE TABLE Bit_TabXXX");
        this.stmt.execute("insert into Bit_TabXXX values(null,0,null)");

        this.pstmt.setObject(1, Boolean.TRUE, java.sql.Types.BIT);
        this.pstmt.executeUpdate();
        assertEquals("true", getSingleIndexedValueWithQuery(1, "SELECT MIN_VAL FROM Bit_TabXXX").toString());
        this.stmt.execute("TRUNCATE TABLE Bit_TabXXX");
        this.stmt.execute("insert into Bit_TabXXX values(null,0,null)");

        this.pstmt.setObject(1, Boolean.FALSE, java.sql.Types.BIT);
        this.pstmt.executeUpdate();
        assertEquals("false", getSingleIndexedValueWithQuery(1, "SELECT MIN_VAL FROM Bit_TabXXX").toString());
        this.stmt.execute("TRUNCATE TABLE Bit_TabXXX");
        this.stmt.execute("insert into Bit_TabXXX values(null,0,null)");

        this.pstmt.setObject(1, new Boolean(true), java.sql.Types.BIT);
        this.pstmt.executeUpdate();
        assertEquals("true", getSingleIndexedValueWithQuery(1, "SELECT MIN_VAL FROM Bit_TabXXX").toString());
        this.stmt.execute("TRUNCATE TABLE Bit_TabXXX");
        this.stmt.execute("insert into Bit_TabXXX values(null,0,null)");

        this.pstmt.setObject(1, new Boolean(false), java.sql.Types.BIT);
        this.pstmt.executeUpdate();
        assertEquals("false", getSingleIndexedValueWithQuery(1, "SELECT MIN_VAL FROM Bit_TabXXX").toString());
        this.stmt.execute("TRUNCATE TABLE Bit_TabXXX");
        this.stmt.execute("insert into Bit_TabXXX values(null,0,null)");

        this.pstmt.setObject(1, new Byte("1"), java.sql.Types.BIT);
        this.pstmt.executeUpdate();
        assertEquals("true", getSingleIndexedValueWithQuery(1, "SELECT MIN_VAL FROM Bit_TabXXX").toString());
        this.stmt.execute("TRUNCATE TABLE Bit_TabXXX");
        this.stmt.execute("insert into Bit_TabXXX values(null,0,null)");

        this.pstmt.setObject(1, new Byte("0"), java.sql.Types.BIT);
        this.pstmt.executeUpdate();
        assertEquals("false", getSingleIndexedValueWithQuery(1, "SELECT MIN_VAL FROM Bit_TabXXX").toString());
    }

    /**
     * Tests fix for BUG#32577 - no way to store two timestamp/datetime values
     * that happens over the DST switchover, as the hours end up being the same
     * when sent as the literal that MySQL requires.
     * 
     * Note that to get this scenario to work with MySQL (since it doesn't
     * support per-value timezones), you need to configure your server (or
     * session) to be in UTC, and tell the driver not to use the legacy
     * date/time code by setting "useLegacyDatetimeCode" to "false". This will
     * cause the driver to always convert to/from the server and client timezone
     * consistently.
     * 
     * @throws Exception
     */
    public void testBug32577() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        createTable("testBug32577", "(id INT, field_datetime DATETIME, field_timestamp TIMESTAMP)");
        Properties props = new Properties();
        props.setProperty("useLegacyDatetimeCode", "false");
        props.setProperty("sessionVariables", "time_zone='+0:00'");
        props.setProperty("serverTimezone", "UTC");

        Connection nonLegacyConn = getConnectionWithProps(props);

        try {
            long earlier = 1194154200000L;
            long later = 1194157800000L;

            this.pstmt = nonLegacyConn.prepareStatement("INSERT INTO testBug32577 VALUES (?,?,?)");
            Timestamp ts = new Timestamp(earlier);
            this.pstmt.setInt(1, 1);
            this.pstmt.setTimestamp(2, ts);
            this.pstmt.setTimestamp(3, ts);
            this.pstmt.executeUpdate();

            ts = new Timestamp(later);
            this.pstmt.setInt(1, 2);
            this.pstmt.setTimestamp(2, ts);
            this.pstmt.setTimestamp(3, ts);
            this.pstmt.executeUpdate();

            this.rs = nonLegacyConn.createStatement().executeQuery(
                    "SELECT id, field_datetime, field_timestamp , UNIX_TIMESTAMP(field_datetime), UNIX_TIMESTAMP(field_timestamp) "
                            + "FROM testBug32577 ORDER BY id ASC");

            this.rs.next();

            //java.util.Date date1 = new Date(this.rs.getTimestamp(2).getTime());
            Timestamp ts1 = this.rs.getTimestamp(3);
            long datetimeSeconds1 = this.rs.getLong(4) * 1000;
            long timestampSeconds1 = this.rs.getLong(5) * 1000;

            this.rs.next();

            //java.util.Date date2 = new Date(this.rs.getTimestamp(2).getTime());
            Timestamp ts2 = this.rs.getTimestamp(3);
            long datetimeSeconds2 = this.rs.getLong(4) * 1000;
            long timestampSeconds2 = this.rs.getLong(5) * 1000;

            assertEquals(later, datetimeSeconds2);
            assertEquals(later, timestampSeconds2);
            assertEquals(earlier, datetimeSeconds1);
            assertEquals(earlier, timestampSeconds1);

            SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy HH:mm z");
            sdf.setTimeZone(TimeZone.getTimeZone("America/New York"));
            System.out.println(sdf.format(ts2));
            System.out.println(sdf.format(ts1));
        } finally {
            if (nonLegacyConn != null) {
                nonLegacyConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#30508 - ResultSet returned by
     * Statement.getGeneratedKeys() is not closed automatically when statement
     * that created it is closed.
     * 
     * @throws Exception
     */
    public void testBug30508() throws Exception {
        createTable("testBug30508", "(k INT PRIMARY KEY NOT NULL AUTO_INCREMENT, p VARCHAR(32))");
        try {
            Statement ggkStatement = this.conn.createStatement();
            ggkStatement.executeUpdate("INSERT INTO testBug30508 (p) VALUES ('abc')", Statement.RETURN_GENERATED_KEYS);

            this.rs = ggkStatement.getGeneratedKeys();
            ggkStatement.close();

            this.rs.next();
            fail("Should've had an exception here");
        } catch (SQLException sqlEx) {
            assertEquals("S1000", sqlEx.getSQLState());
        }

        try {
            this.pstmt = this.conn.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS);
            this.rs = this.pstmt.getGeneratedKeys();
            this.pstmt.close();
            this.rs.next();
            fail("Should've had an exception here");
        } catch (SQLException sqlEx) {
            assertEquals("S1000", sqlEx.getSQLState());
        }

        if (versionMeetsMinimum(5, 0)) {
            createProcedure("testBug30508", "() BEGIN SELECT 1; END");

            try {
                this.pstmt = this.conn.prepareCall("{CALL testBug30508()}");
                this.rs = this.pstmt.getGeneratedKeys();
                this.pstmt.close();
                this.rs.next();
                fail("Should've had an exception here");
            } catch (SQLException sqlEx) {
                assertEquals("S1000", sqlEx.getSQLState());
            }
        }
    }

    public void testMoreLanceBugs() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        createTable("Bit_Tab", "( `MAX_VAL` BIT default NULL, `MIN_VAL` BIT default NULL, `NULL_VAL` BIT default NULL) DEFAULT CHARSET=latin1", "InnoDB");
        // this.stmt.execute("insert into Bit_Tab values(null,0,null)");
        createProcedure("Bit_Proc", "(out MAX_PARAM TINYINT, out MIN_PARAM TINYINT, out NULL_PARAM TINYINT) "
                + "begin select MAX_VAL, MIN_VAL, NULL_VAL  into MAX_PARAM, MIN_PARAM, NULL_PARAM from Bit_Tab; end ");

        Boolean minBooleanVal;
        Boolean oRetVal;
        String Min_Val_Query = "SELECT MIN_VAL from Bit_Tab";
        String Min_Insert = "insert into Bit_Tab values(1,0,null)";
        // System.out.println("Value to insert=" + extractVal(Min_Insert,1));
        CallableStatement cstmt;

        this.stmt.executeUpdate("delete from Bit_Tab");
        this.stmt.executeUpdate(Min_Insert);
        cstmt = this.conn.prepareCall("{call Bit_Proc(?,?,?)}");

        cstmt.registerOutParameter(1, java.sql.Types.BIT);
        cstmt.registerOutParameter(2, java.sql.Types.BIT);
        cstmt.registerOutParameter(3, java.sql.Types.BIT);

        cstmt.executeUpdate();

        boolean bRetVal = cstmt.getBoolean(2);
        oRetVal = new Boolean(bRetVal);
        minBooleanVal = new Boolean("false");
        this.rs = this.stmt.executeQuery(Min_Val_Query);
        assertEquals(minBooleanVal, oRetVal);
    }

    public void testBug33823() {
        new ResultSetInternalMethods() {

            public void buildIndexMapping() throws SQLException {
            }

            public void clearNextResult() {
            }

            public ResultSetInternalMethods copy() throws SQLException {
                return null;
            }

            public char getFirstCharOfQuery() {
                return 0;
            }

            public ResultSetInternalMethods getNextResultSet() {
                return null;
            }

            public Object getObjectStoredProc(int columnIndex, int desiredSqlType) throws SQLException {
                return null;
            }

            public Object getObjectStoredProc(int i, Map<Object, Object> map, int desiredSqlType) throws SQLException {
                return null;
            }

            public Object getObjectStoredProc(String columnName, int desiredSqlType) throws SQLException {
                return null;
            }

            public Object getObjectStoredProc(String colName, Map<Object, Object> map, int desiredSqlType) throws SQLException {
                return null;
            }

            public String getServerInfo() {
                return null;
            }

            public long getUpdateCount() {
                return 0;
            }

            public long getUpdateID() {
                return 0;
            }

            public void initializeFromCachedMetaData(CachedResultSetMetaData cachedMetaData) {
                cachedMetaData.getFields();
            }

            public void initializeWithMetadata() throws SQLException {
            }

            public void populateCachedMetaData(CachedResultSetMetaData cachedMetaData) throws SQLException {
            }

            public void realClose(boolean calledExplicitly) throws SQLException {
            }

            public boolean isClosed() {
                return false;
            }

            public boolean reallyResult() {
                return false;
            }

            public void redefineFieldsForDBMD(Field[] metadataFields) {
            }

            public void setFirstCharOfQuery(char firstCharUpperCase) {
            }

            public void setOwningStatement(StatementImpl owningStatement) {
            }

            public void setStatementUsedForFetchingRows(com.mysql.jdbc.PreparedStatement stmt) {
            }

            public void setWrapperStatement(Statement wrapperStatement) {
            }

            public boolean absolute(int row) throws SQLException {
                return false;
            }

            public void afterLast() throws SQLException {
            }

            public void beforeFirst() throws SQLException {
            }

            public void cancelRowUpdates() throws SQLException {
            }

            public void clearWarnings() throws SQLException {
            }

            public void close() throws SQLException {
            }

            public void deleteRow() throws SQLException {
            }

            public int findColumn(String columnName) throws SQLException {
                return 0;
            }

            public boolean first() throws SQLException {
                return false;
            }

            public Array getArray(int i) throws SQLException {
                return null;
            }

            public Array getArray(String colName) throws SQLException {
                return null;
            }

            public InputStream getAsciiStream(int columnIndex) throws SQLException {
                return null;
            }

            public InputStream getAsciiStream(String columnName) throws SQLException {
                return null;
            }

            public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
                return null;
            }

            public BigDecimal getBigDecimal(String columnName) throws SQLException {
                return null;
            }

            public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
                return null;
            }

            public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
                return null;
            }

            public InputStream getBinaryStream(int columnIndex) throws SQLException {
                return null;
            }

            public InputStream getBinaryStream(String columnName) throws SQLException {
                return null;
            }

            public Blob getBlob(int i) throws SQLException {
                return null;
            }

            public Blob getBlob(String colName) throws SQLException {
                return null;
            }

            public boolean getBoolean(int columnIndex) throws SQLException {
                return false;
            }

            public boolean getBoolean(String columnName) throws SQLException {
                return false;
            }

            public byte getByte(int columnIndex) throws SQLException {
                return 0;
            }

            public byte getByte(String columnName) throws SQLException {
                return 0;
            }

            public byte[] getBytes(int columnIndex) throws SQLException {
                return null;
            }

            public byte[] getBytes(String columnName) throws SQLException {
                return null;
            }

            public Reader getCharacterStream(int columnIndex) throws SQLException {
                return null;
            }

            public Reader getCharacterStream(String columnName) throws SQLException {
                return null;
            }

            public Clob getClob(int i) throws SQLException {
                return null;
            }

            public Clob getClob(String colName) throws SQLException {
                return null;
            }

            public int getConcurrency() throws SQLException {
                return 0;
            }

            public String getCursorName() throws SQLException {
                return null;
            }

            public Date getDate(int columnIndex) throws SQLException {
                return null;
            }

            public Date getDate(String columnName) throws SQLException {
                return null;
            }

            public Date getDate(int columnIndex, Calendar cal) throws SQLException {
                return null;
            }

            public Date getDate(String columnName, Calendar cal) throws SQLException {
                return null;
            }

            public double getDouble(int columnIndex) throws SQLException {
                return 0;
            }

            public double getDouble(String columnName) throws SQLException {
                return 0;
            }

            public int getFetchDirection() throws SQLException {
                return 0;
            }

            public int getFetchSize() throws SQLException {
                return 0;
            }

            public float getFloat(int columnIndex) throws SQLException {
                return 0;
            }

            public float getFloat(String columnName) throws SQLException {
                return 0;
            }

            public int getInt(int columnIndex) throws SQLException {
                return 0;
            }

            public int getInt(String columnName) throws SQLException {
                return 0;
            }

            public long getLong(int columnIndex) throws SQLException {
                return 0;
            }

            public long getLong(String columnName) throws SQLException {
                return 0;
            }

            public ResultSetMetaData getMetaData() throws SQLException {
                return null;
            }

            public Object getObject(int columnIndex) throws SQLException {
                return null;
            }

            public Object getObject(String columnName) throws SQLException {
                return null;
            }

            public Object getObject(int arg0, Map<String, Class<?>> arg1) throws SQLException {
                return null;
            }

            public Object getObject(String arg0, Map<String, Class<?>> arg1) throws SQLException {
                return null;
            }

            public Ref getRef(int i) throws SQLException {
                return null;
            }

            public Ref getRef(String colName) throws SQLException {
                return null;
            }

            public int getRow() throws SQLException {
                return 0;
            }

            public short getShort(int columnIndex) throws SQLException {
                return 0;
            }

            public short getShort(String columnName) throws SQLException {
                return 0;
            }

            public Statement getStatement() throws SQLException {
                return null;
            }

            public String getString(int columnIndex) throws SQLException {
                return null;
            }

            public String getString(String columnName) throws SQLException {
                return null;
            }

            public Time getTime(int columnIndex) throws SQLException {
                return null;
            }

            public Time getTime(String columnName) throws SQLException {
                return null;
            }

            public Time getTime(int columnIndex, Calendar cal) throws SQLException {
                return null;
            }

            public Time getTime(String columnName, Calendar cal) throws SQLException {
                return null;
            }

            public Timestamp getTimestamp(int columnIndex) throws SQLException {
                return null;
            }

            public Timestamp getTimestamp(String columnName) throws SQLException {
                return null;
            }

            public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
                return null;
            }

            public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
                return null;
            }

            public int getType() throws SQLException {
                return 0;
            }

            public URL getURL(int columnIndex) throws SQLException {
                return null;
            }

            public URL getURL(String columnName) throws SQLException {
                return null;
            }

            public InputStream getUnicodeStream(int columnIndex) throws SQLException {
                return null;
            }

            public InputStream getUnicodeStream(String columnName) throws SQLException {
                return null;
            }

            public SQLWarning getWarnings() throws SQLException {
                return null;
            }

            public void insertRow() throws SQLException {
            }

            public boolean isAfterLast() throws SQLException {
                return false;
            }

            public boolean isBeforeFirst() throws SQLException {
                return false;
            }

            public boolean isFirst() throws SQLException {
                return false;
            }

            public boolean isLast() throws SQLException {
                return false;
            }

            public boolean last() throws SQLException {
                return false;
            }

            public void moveToCurrentRow() throws SQLException {
            }

            public void moveToInsertRow() throws SQLException {
            }

            public boolean next() throws SQLException {
                return false;
            }

            public boolean previous() throws SQLException {
                return false;
            }

            public void refreshRow() throws SQLException {
            }

            public boolean relative(int rows) throws SQLException {
                return false;
            }

            public boolean rowDeleted() throws SQLException {
                return false;
            }

            public boolean rowInserted() throws SQLException {
                return false;
            }

            public boolean rowUpdated() throws SQLException {
                return false;
            }

            public void setFetchDirection(int direction) throws SQLException {
            }

            public void setFetchSize(int rows) throws SQLException {
            }

            public void updateArray(int columnIndex, Array x) throws SQLException {
            }

            public void updateArray(String columnName, Array x) throws SQLException {
            }

            public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
            }

            public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
            }

            public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

            }

            public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
            }

            public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
            }

            public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
            }

            public void updateBlob(int columnIndex, Blob x) throws SQLException {
            }

            public void updateBlob(String columnName, Blob x) throws SQLException {
            }

            public void updateBoolean(int columnIndex, boolean x) throws SQLException {
            }

            public void updateBoolean(String columnName, boolean x) throws SQLException {
            }

            public void updateByte(int columnIndex, byte x) throws SQLException {
            }

            public void updateByte(String columnName, byte x) throws SQLException {
            }

            public void updateBytes(int columnIndex, byte[] x) throws SQLException {
            }

            public void updateBytes(String columnName, byte[] x) throws SQLException {
            }

            public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
            }

            public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
            }

            public void updateClob(int columnIndex, Clob x) throws SQLException {
            }

            public void updateClob(String columnName, Clob x) throws SQLException {
            }

            public void updateDate(int columnIndex, Date x) throws SQLException {
            }

            public void updateDate(String columnName, Date x) throws SQLException {
            }

            public void updateDouble(int columnIndex, double x) throws SQLException {
            }

            public void updateDouble(String columnName, double x) throws SQLException {
            }

            public void updateFloat(int columnIndex, float x) throws SQLException {
            }

            public void updateFloat(String columnName, float x) throws SQLException {
            }

            public void updateInt(int columnIndex, int x) throws SQLException {
            }

            public void updateInt(String columnName, int x) throws SQLException {
            }

            public void updateLong(int columnIndex, long x) throws SQLException {
            }

            public void updateLong(String columnName, long x) throws SQLException {
            }

            public void updateNull(int columnIndex) throws SQLException {
            }

            public void updateNull(String columnName) throws SQLException {
            }

            public void updateObject(int columnIndex, Object x) throws SQLException {
            }

            public void updateObject(String columnName, Object x) throws SQLException {
            }

            public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
            }

            public void updateObject(String columnName, Object x, int scale) throws SQLException {
            }

            public void updateRef(int columnIndex, Ref x) throws SQLException {
            }

            public void updateRef(String columnName, Ref x) throws SQLException {
            }

            public void updateRow() throws SQLException {
            }

            public void updateShort(int columnIndex, short x) throws SQLException {
            }

            public void updateShort(String columnName, short x) throws SQLException {
            }

            public void updateString(int columnIndex, String x) throws SQLException {
            }

            public void updateString(String columnName, String x) throws SQLException {
            }

            public void updateTime(int columnIndex, Time x) throws SQLException {
            }

            public void updateTime(String columnName, Time x) throws SQLException {
            }

            public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
            }

            public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
            }

            public boolean wasNull() throws SQLException {
                return false;
            }

            public int getBytesSize() throws SQLException {
                return 0;
            }
        };
    }

    /**
     * Tests fix for BUG#34093 - Statements with batched values do not return
     * correct values for getGeneratedKeys() when "rewriteBatchedStatements" is
     * set to "true", and the statement has an "ON DUPLICATE KEY UPDATE" clause.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug34093() throws Exception {
        Connection rewriteConn = null;

        rewriteConn = getConnectionWithProps("rewriteBatchedStatements=true");

        checkBug34093(rewriteConn);

        rewriteConn = getConnectionWithProps("rewriteBatchedStatements=true,useServerPrepStmts=true");

        checkBug34093(rewriteConn);
    }

    private void checkBug34093(Connection rewriteConn) throws Exception {
        try {
            String ddl = "(autoIncId INT NOT NULL PRIMARY KEY AUTO_INCREMENT, uniqueTextKey VARCHAR(255), UNIQUE KEY (uniqueTextKey(100)))";

            String[] sequence = { "c", "a", "d", "b" };
            String sql = "insert into testBug30493 (uniqueTextKey) values (?) on duplicate key UPDATE autoIncId = last_insert_id( autoIncId )";
            String tablePrimeSql = "INSERT INTO testBug30493 (uniqueTextKey) VALUES ('a'), ('b'), ('c'), ('d')";

            // setup the rewritten and non-written statements
            Statement stmts[] = new Statement[2];
            PreparedStatement pstmts[] = new PreparedStatement[2];
            stmts[0] = this.conn.createStatement();
            stmts[1] = rewriteConn.createStatement();
            pstmts[0] = this.conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            pstmts[1] = rewriteConn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            for (int i = 0; i < sequence.length; ++i) {
                String sqlLiteral = sql.replaceFirst("\\?", "'" + sequence[i] + "'");
                stmts[0].addBatch(sqlLiteral);
                stmts[1].addBatch(sqlLiteral);
                pstmts[0].setString(1, sequence[i]);
                pstmts[0].addBatch();
                pstmts[1].setString(1, sequence[i]);
                pstmts[1].addBatch();
            }

            // run the test once for Statement, and once for PreparedStatement
            Statement stmtSets[][] = new Statement[2][];
            stmtSets[0] = stmts;
            stmtSets[1] = pstmts;

            for (int stmtSet = 0; stmtSet < 2; ++stmtSet) {
                Statement testStmts[] = stmtSets[stmtSet];
                createTable("testBug30493", ddl);
                this.stmt.executeUpdate(tablePrimeSql);

                int nonRwUpdateCounts[] = testStmts[0].executeBatch();

                ResultSet nonRewrittenRsKeys = testStmts[0].getGeneratedKeys();

                createTable("testBug30493", ddl);
                this.stmt.executeUpdate(tablePrimeSql);
                int expectedUpdateCount = versionMeetsMinimum(5, 1, 0) ? 2 : 1;

                // behavior changed by fix of Bug#46675, affects servers starting from 5.5.16 and 5.6.3
                if (versionMeetsMinimum(5, 5, 16)) {
                    expectedUpdateCount = 1;
                }

                int rwUpdateCounts[] = testStmts[1].executeBatch();
                ResultSet rewrittenRsKeys = testStmts[1].getGeneratedKeys();
                for (int i = 0; i < 4; ++i) {
                    assertEquals(expectedUpdateCount, nonRwUpdateCounts[i]);
                    assertEquals(expectedUpdateCount, rwUpdateCounts[i]);
                }

                assertResultSetLength(nonRewrittenRsKeys, 4);
                assertResultSetLength(rewrittenRsKeys, 4);

                assertResultSetsEqual(nonRewrittenRsKeys, rewrittenRsKeys);
            }
        } finally {
            if (rewriteConn != null) {
                rewriteConn.close();
            }
        }
    }

    public void testBug34093_nonbatch() throws Exception {
        Connection rewriteConn = null;

        try {
            String ddl = "(autoIncId INT NOT NULL PRIMARY KEY AUTO_INCREMENT, uniqueTextKey VARCHAR(255) UNIQUE KEY)";

            String sql = "insert into testBug30493 (uniqueTextKey) values ('c') on duplicate key UPDATE autoIncId = last_insert_id( autoIncId )";
            String tablePrimeSql = "INSERT INTO testBug30493 (uniqueTextKey) VALUES ('a'), ('b'), ('c'), ('d')";

            try {
                createTable("testBug30493", ddl);
            } catch (SQLException sqlEx) {
                if (sqlEx.getMessage().indexOf("max key length") != -1) {
                    createTable("testBug30493", "(autoIncId INT NOT NULL PRIMARY KEY AUTO_INCREMENT, uniqueTextKey VARCHAR(180) UNIQUE KEY)");
                }
            }
            this.stmt.executeUpdate(tablePrimeSql);

            Statement stmt1 = this.conn.createStatement();
            stmt1.execute(sql, Statement.RETURN_GENERATED_KEYS);
            int expectedUpdateCount = versionMeetsMinimum(5, 1, 0) ? 2 : 1;

            // behavior changed by fix of Bug#46675, affects servers starting from 5.5.16 and 5.6.3
            if (versionMeetsMinimum(5, 5, 16)) {
                expectedUpdateCount = 1;
            }

            assertEquals(expectedUpdateCount, stmt1.getUpdateCount());
            ResultSet stmtKeys = stmt1.getGeneratedKeys();
            assertResultSetLength(stmtKeys, 1);

            try {
                createTable("testBug30493", ddl);
            } catch (SQLException sqlEx) {
                if (sqlEx.getMessage().indexOf("max key length") != -1) {
                    createTable("testBug30493", "(autoIncId INT NOT NULL PRIMARY KEY AUTO_INCREMENT, uniqueTextKey VARCHAR(180) UNIQUE KEY)");
                }
            }
            this.stmt.executeUpdate(tablePrimeSql);

            this.pstmt = this.conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            this.pstmt.execute();
            assertEquals(expectedUpdateCount, this.pstmt.getUpdateCount());
            ResultSet pstmtKeys = this.pstmt.getGeneratedKeys();
            assertResultSetLength(pstmtKeys, 1);

            assertResultSetsEqual(stmtKeys, pstmtKeys);
        } finally {
            if (rewriteConn != null) {
                rewriteConn.close();
            }
        }
    }

    public void testBug34518() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        Connection fetchConn = getConnectionWithProps("useCursorFetch=true");
        Statement fetchStmt = fetchConn.createStatement();

        int stmtCount = ((com.mysql.jdbc.Connection) fetchConn).getActiveStatementCount();

        fetchStmt.setFetchSize(100);
        this.rs = fetchStmt.executeQuery("SELECT 1");

        assertEquals(((com.mysql.jdbc.Connection) fetchConn).getActiveStatementCount(), stmtCount + 1);
        this.rs.close();
        assertEquals(((com.mysql.jdbc.Connection) fetchConn).getActiveStatementCount(), stmtCount);
    }

    public void testBug35170() throws Exception {
        Statement stt = null;

        try {
            stt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            stt.setFetchSize(Integer.MIN_VALUE);
            this.rs = stt.executeQuery("select 1");
            this.rs.next();
            while (!this.rs.isAfterLast()) {
                this.rs.getString(1);
                this.rs.next();
            }
        } finally {
            if (stt != null) {
                stt.close();
            }
        }

    }

    /*
     * public void testBug35307() throws Exception { createTable("testBug35307",
     * "(`id` int(11) unsigned NOT NULL auto_increment," +
     * "`field` varchar(20) NOT NULL," + "`date` datetime NOT NULL," +
     * "PRIMARY KEY  (`id`)" + ") ENGINE=MyISAM DEFAULT CHARSET=latin1");
     * 
     * this.stmt.executeUpdate("INSERT INTO testBug35307 (field) values ('works')"
     * ); }
     */

    public void testBug35666() throws Exception {
        Connection loggingConn = getConnectionWithProps("logSlowQueries=true");
        this.pstmt = ((com.mysql.jdbc.Connection) loggingConn).serverPrepareStatement("SELECT SLEEP(4)");
        this.pstmt.execute();
    }

    public void testDeadlockBatchBehavior() throws Exception {
        try {
            createTable("t1", "(id INTEGER, x INTEGER)", "INNODB");
            createTable("t2", "(id INTEGER, x INTEGER)", "INNODB");
            this.stmt.executeUpdate("INSERT INTO t1 VALUES (0, 0)");

            this.conn.setAutoCommit(false);
            this.conn.createStatement().executeQuery("SELECT * FROM t1 WHERE id=0 FOR UPDATE");

            final Connection deadlockConn = getConnectionWithProps("includeInnodbStatusInDeadlockExceptions=true");
            deadlockConn.setAutoCommit(false);

            final Statement deadlockStmt = deadlockConn.createStatement();
            deadlockStmt.executeUpdate("INSERT INTO t2 VALUES (1, 0)");
            deadlockStmt.executeQuery("SELECT * FROM t2 WHERE id=0 FOR UPDATE");

            new Thread() {
                @Override
                public void run() {
                    try {
                        deadlockStmt.addBatch("INSERT INTO t2 VALUES (1, 0)");
                        deadlockStmt.addBatch("INSERT INTO t2 VALUES (2, 0)");
                        deadlockStmt.addBatch("UPDATE t1 SET x=2 WHERE id=0");
                        deadlockStmt.executeBatch();
                    } catch (SQLException sqlEx) {
                        sqlEx.printStackTrace();
                        try {
                            deadlockConn.rollback();
                        } catch (SQLException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }.run();

            this.stmt.executeUpdate("INSERT INTO t1 VALUES (0, 0)");

        } catch (BatchUpdateException sqlEx) {
            int[] updateCounts = sqlEx.getUpdateCounts();
            for (int i = 0; i < updateCounts.length; i++) {
                System.out.println(updateCounts[i]);
            }
        } finally {
            this.conn.rollback();
            this.conn.setAutoCommit(true);
        }
    }

    public void testBug39352() throws Exception {
        Connection affectedRowsConn = getConnectionWithProps("useAffectedRows=true");

        try {

            createTable("bug39352", "(id INT PRIMARY KEY, data VARCHAR(100))");
            assertEquals(1, this.stmt.executeUpdate("INSERT INTO bug39352 (id,data) values (1,'a')"));
            int rowsAffected = this.stmt.executeUpdate("INSERT INTO bug39352 (id, data) VALUES(2, 'bb') ON DUPLICATE KEY UPDATE data=values(data)");
            assertEquals("First UPD failed", 1, rowsAffected);

            rowsAffected = affectedRowsConn.createStatement().executeUpdate(
                    "INSERT INTO bug39352 (id, data) VALUES(2, 'bbb') ON DUPLICATE KEY UPDATE data=values(data)");
            assertEquals("2nd UPD failed", 2, rowsAffected);

            rowsAffected = affectedRowsConn.createStatement().executeUpdate(
                    "INSERT INTO bug39352 (id, data) VALUES(2, 'bbb') ON DUPLICATE KEY UPDATE data=values(data)");
            assertEquals("3rd UPD failed", 0, rowsAffected);

        } finally {
            affectedRowsConn.close();
        }
    }

    public void testBug38747() throws Exception {
        try {
            this.conn.setReadOnly(true);
            this.pstmt = this.conn.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            this.pstmt.setFetchSize(Integer.MIN_VALUE);

            this.rs = this.pstmt.executeQuery();

            while (this.rs.next()) {
            }

            this.rs.close();
            this.pstmt.close();

        } finally {
            this.conn.setReadOnly(false);
        }
    }

    public void testBug39956() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        ResultSet enginesRs = this.conn.createStatement().executeQuery("SHOW ENGINES");

        while (enginesRs.next()) {
            if ("YES".equalsIgnoreCase(enginesRs.getString("Support")) || "DEFAULT".equalsIgnoreCase(enginesRs.getString("Support"))) {

                String engineName = enginesRs.getString("Engine");

                if ("CSV".equalsIgnoreCase(engineName) || "BLACKHOLE".equalsIgnoreCase(engineName) || "FEDERATED".equalsIgnoreCase(engineName)
                        || "MRG_MYISAM".equalsIgnoreCase(engineName) || "PARTITION".equalsIgnoreCase(engineName) || "EXAMPLE".equalsIgnoreCase(engineName)
                        || "PERFORMANCE_SCHEMA".equalsIgnoreCase(engineName) || engineName.endsWith("_SCHEMA")) {
                    continue; // not supported
                }

                if ("ARCHIVE".equalsIgnoreCase(engineName) && !versionMeetsMinimum(5, 1, 6)) {
                    continue;
                }

                String tableName = "testBug39956_" + engineName;

                Connection twoConn = getConnectionWithProps("sessionVariables=auto_increment_increment=2");

                try {
                    for (int i = 0; i < 2; i++) {
                        createTable(tableName, "(k int primary key auto_increment, p varchar(4)) ENGINE=" + engineName);

                        ((com.mysql.jdbc.Connection) twoConn).setRewriteBatchedStatements(i == 1);

                        this.pstmt = twoConn.prepareStatement("INSERT INTO " + tableName + " (p) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
                        this.pstmt.setString(1, "a");
                        this.pstmt.addBatch();
                        this.pstmt.setString(1, "b");
                        this.pstmt.addBatch();
                        this.pstmt.executeBatch();

                        this.rs = this.pstmt.getGeneratedKeys();

                        this.rs.next();
                        assertEquals("For engine " + engineName + ((i == 1) ? " rewritten " : " plain "), 1, this.rs.getInt(1));
                        this.rs.next();
                        assertEquals("For engine " + engineName + ((i == 1) ? " rewritten " : " plain "), 3, this.rs.getInt(1));

                        createTable(tableName, "(k int primary key auto_increment, p varchar(4)) ENGINE=" + engineName);
                        Statement twoStmt = twoConn.createStatement();
                        for (int j = 0; j < 10; j++) {
                            twoStmt.addBatch("INSERT INTO " + tableName + " (p) VALUES ('" + j + "')");
                        }

                        twoStmt.executeBatch(); // No getGeneratedKeys() support in JDBC spec, but we allow it...might have to rewrite test if/when we don't
                        this.rs = twoStmt.getGeneratedKeys();

                        int key = 1;

                        for (int j = 0; j < 10; j++) {
                            this.rs.next();
                            assertEquals("For engine " + engineName + ((i == 1) ? " rewritten " : " plain "), key, this.rs.getInt(1));
                            key += 2;
                        }
                    }
                } finally {
                    if (twoConn != null) {
                        twoConn.close();
                    }
                }
            }
        }
    }

    public void testBug34185() throws Exception {
        this.stmt.executeQuery("SELECT 1");

        try {
            this.stmt.getGeneratedKeys();
            fail("Expected exception");
        } catch (SQLException sqlEx) {
            assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());
        }

        this.pstmt = this.conn.prepareStatement("SELECT 1");

        try {
            this.pstmt.execute();
            this.pstmt.getGeneratedKeys();
            fail("Expected exception");
        } catch (SQLException sqlEx) {
            assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());
        }
    }

    public void testBug41161() throws Exception {
        createTable("testBug41161", "(a int, b int)");

        Connection rewriteConn = getConnectionWithProps("rewriteBatchedStatements=true");

        try {
            this.pstmt = rewriteConn.prepareStatement("INSERT INTO testBug41161 (a, b) VALUES (?, ?, ?)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 1);

            try {
                this.pstmt.addBatch();
                fail("Should have thrown an exception");
            } catch (SQLException sqlEx) {
                assertEquals("07001", sqlEx.getSQLState());
            }

            this.pstmt.executeBatch(); // NPE when this bug exists
        } finally {
            rewriteConn.close();
        }
    }

    /**
     * Ensures that cases listed in Bug#41448 actually work - we don't think
     * there's a bug here right now
     */

    public void testBug41448() throws Exception {
        createTable("testBug41448", "(pk INT PRIMARY KEY AUTO_INCREMENT, field1 VARCHAR(4))");

        this.stmt.executeUpdate("INSERT INTO testBug41448 (field1) VALUES ('abc')", Statement.RETURN_GENERATED_KEYS);
        this.stmt.getGeneratedKeys();

        this.stmt.executeUpdate("INSERT INTO testBug41448 (field1) VALUES ('def')", new int[] { 1 });
        this.stmt.getGeneratedKeys();

        this.stmt.executeUpdate("INSERT INTO testBug41448 (field1) VALUES ('ghi')", new String[] { "pk" });
        this.stmt.getGeneratedKeys();

        this.stmt.executeUpdate("INSERT INTO testBug41448 (field1) VALUES ('ghi')");

        try {
            this.stmt.getGeneratedKeys();
            fail("Expected a SQLException here");
        } catch (SQLException sqlEx) {
            // expected
        }

        this.stmt.execute("INSERT INTO testBug41448 (field1) VALUES ('jkl')", Statement.RETURN_GENERATED_KEYS);
        this.stmt.getGeneratedKeys();

        this.stmt.execute("INSERT INTO testBug41448 (field1) VALUES ('mno')", new int[] { 1 });
        this.stmt.getGeneratedKeys();

        this.stmt.execute("INSERT INTO testBug41448 (field1) VALUES ('pqr')", new String[] { "pk" });
        this.stmt.getGeneratedKeys();

        this.stmt.execute("INSERT INTO testBug41448 (field1) VALUES ('stu')");

        try {
            this.stmt.getGeneratedKeys();
            fail("Expected a SQLException here");
        } catch (SQLException sqlEx) {
            // expected
        }

        this.pstmt = this.conn.prepareStatement("INSERT INTO testBug41448 (field1) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
        this.pstmt.setString(1, "abc");
        this.pstmt.executeUpdate();
        this.pstmt.getGeneratedKeys();
        this.pstmt.execute();
        this.pstmt.getGeneratedKeys();

        this.pstmt = this.conn.prepareStatement("INSERT INTO testBug41448 (field1) VALUES (?)", new int[] { 1 });
        this.pstmt.setString(1, "abc");
        this.pstmt.executeUpdate();
        this.pstmt.getGeneratedKeys();
        this.pstmt.execute();
        this.pstmt.getGeneratedKeys();

        this.pstmt = this.conn.prepareStatement("INSERT INTO testBug41448 (field1) VALUES (?)", new String[] { "pk" });
        this.pstmt.setString(1, "abc");
        this.pstmt.executeUpdate();
        this.pstmt.getGeneratedKeys();
        this.pstmt.execute();
        this.pstmt.getGeneratedKeys();

        this.pstmt = this.conn.prepareStatement("INSERT INTO testBug41448 (field1) VALUES (?)");
        this.pstmt.setString(1, "abc");
        this.pstmt.executeUpdate();
        try {
            this.pstmt.getGeneratedKeys();
            fail("Expected a SQLException here");
        } catch (SQLException sqlEx) {
            // expected
        }

        this.pstmt.execute();

        try {
            this.pstmt.getGeneratedKeys();
            fail("Expected a SQLException here");
        } catch (SQLException sqlEx) {
            // expected
        }
    }

    public void testBug48172() throws Exception {
        createTable("testBatchInsert", "(a INT PRIMARY KEY AUTO_INCREMENT)");
        Connection rewriteConn = getConnectionWithProps("rewriteBatchedStatements=true,dumpQueriesOnException=true");
        assertEquals("0", getSingleIndexedValueWithQuery(rewriteConn, 2, "SHOW SESSION STATUS LIKE 'Com_insert'").toString());

        this.pstmt = rewriteConn.prepareStatement("INSERT INTO testBatchInsert VALUES (?)");
        this.pstmt.setNull(1, java.sql.Types.INTEGER);
        this.pstmt.addBatch();
        this.pstmt.setNull(1, java.sql.Types.INTEGER);
        this.pstmt.addBatch();
        this.pstmt.setNull(1, java.sql.Types.INTEGER);
        this.pstmt.addBatch();
        this.pstmt.executeBatch();

        assertEquals("1", getSingleIndexedValueWithQuery(rewriteConn, 2, "SHOW SESSION STATUS LIKE 'Com_insert'").toString());
        this.pstmt = rewriteConn.prepareStatement("INSERT INTO `testBatchInsert`VALUES (?)");
        this.pstmt.setNull(1, java.sql.Types.INTEGER);
        this.pstmt.addBatch();
        this.pstmt.setNull(1, java.sql.Types.INTEGER);
        this.pstmt.addBatch();
        this.pstmt.setNull(1, java.sql.Types.INTEGER);
        this.pstmt.addBatch();
        this.pstmt.executeBatch();

        assertEquals("2", getSingleIndexedValueWithQuery(rewriteConn, 2, "SHOW SESSION STATUS LIKE 'Com_insert'").toString());

        this.pstmt = rewriteConn.prepareStatement("INSERT INTO testBatchInsert VALUES(?)");
        this.pstmt.setNull(1, java.sql.Types.INTEGER);
        this.pstmt.addBatch();
        this.pstmt.setNull(1, java.sql.Types.INTEGER);
        this.pstmt.addBatch();
        this.pstmt.setNull(1, java.sql.Types.INTEGER);
        this.pstmt.addBatch();
        this.pstmt.executeBatch();

        assertEquals("3", getSingleIndexedValueWithQuery(rewriteConn, 2, "SHOW SESSION STATUS LIKE 'Com_insert'").toString());

        this.pstmt = rewriteConn.prepareStatement("INSERT INTO testBatchInsert VALUES\n(?)");
        this.pstmt.setNull(1, java.sql.Types.INTEGER);
        this.pstmt.addBatch();
        this.pstmt.setNull(1, java.sql.Types.INTEGER);
        this.pstmt.addBatch();
        this.pstmt.setNull(1, java.sql.Types.INTEGER);
        this.pstmt.addBatch();
        this.pstmt.executeBatch();

        assertEquals("4", getSingleIndexedValueWithQuery(rewriteConn, 2, "SHOW SESSION STATUS LIKE 'Com_insert'").toString());

    }

    /**
     * Tests fix for Bug#41532 - regression in performance for batched inserts
     * when using ON DUPLICATE KEY UPDATE
     */
    public void testBug41532() throws Exception {
        createTable("testBug41532", "(ID INTEGER, S1 VARCHAR(100), S2 VARCHAR(100), S3 VARCHAR(100), D1 DATETIME, D2 DATETIME, D3 DATETIME, "
                + "N1 DECIMAL(28,6), N2 DECIMAL(28,6), N3 DECIMAL(28,6), UNIQUE KEY UNIQUE_KEY_TEST_DUPLICATE (ID) )");

        int numTests = 5000;
        Connection rewriteConn = getConnectionWithProps("rewriteBatchedStatements=true,dumpQueriesOnException=true");

        assertEquals("0", getSingleIndexedValueWithQuery(rewriteConn, 2, "SHOW SESSION STATUS LIKE 'Com_insert'").toString());
        long batchedTime = timeBatch(rewriteConn, numTests);
        assertEquals("1", getSingleIndexedValueWithQuery(rewriteConn, 2, "SHOW SESSION STATUS LIKE 'Com_insert'").toString());

        this.stmt.executeUpdate("TRUNCATE TABLE testBug41532");

        assertEquals("0", getSingleIndexedValueWithQuery(this.conn, 2, "SHOW SESSION STATUS LIKE 'Com_insert'").toString());
        long unbatchedTime = timeBatch(this.conn, numTests);
        assertEquals(String.valueOf(numTests), getSingleIndexedValueWithQuery(this.conn, 2, "SHOW SESSION STATUS LIKE 'Com_insert'").toString());
        assertTrue(batchedTime < unbatchedTime);

        rewriteConn = getConnectionWithProps("rewriteBatchedStatements=true,useCursorFetch=true,defaultFetchSize=10000");
        timeBatch(rewriteConn, numTests);
    }

    private long timeBatch(Connection c, int numberOfRows) throws SQLException {
        this.pstmt = c.prepareStatement("INSERT INTO testBug41532(ID, S1, S2, S3, D1, D2, D3, N1, N2, N3) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                + " ON DUPLICATE KEY UPDATE S1 = VALUES(S1), S2 = VALUES(S2), S3 = VALUES(S3), D1 = VALUES(D1), D2 ="
                + " VALUES(D2), D3 = VALUES(D3), N1 = N1 + VALUES(N1), N2 = N2 + VALUES(N2), N2 = N2 + VALUES(N2)");
        c.setAutoCommit(false);
        c.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        Date d1 = new Date(currentTimeMillis());
        Date d2 = new Date(currentTimeMillis() + 1000000);
        Date d3 = new Date(currentTimeMillis() + 1250000);

        for (int i = 0; i < numberOfRows; i++) {
            this.pstmt.setObject(1, new Integer(i), Types.INTEGER);
            this.pstmt.setObject(2, String.valueOf(i), Types.VARCHAR);
            this.pstmt.setObject(3, String.valueOf(i * 0.1), Types.VARCHAR);
            this.pstmt.setObject(4, String.valueOf(i / 3), Types.VARCHAR);
            this.pstmt.setObject(5, new Timestamp(d1.getTime()), Types.TIMESTAMP);
            this.pstmt.setObject(6, new Timestamp(d2.getTime()), Types.TIMESTAMP);
            this.pstmt.setObject(7, new Timestamp(d3.getTime()), Types.TIMESTAMP);
            this.pstmt.setObject(8, new BigDecimal(i + 0.1), Types.DECIMAL);
            this.pstmt.setObject(9, new BigDecimal(i * 0.1), Types.DECIMAL);
            this.pstmt.setObject(10, new BigDecimal(i / 3), Types.DECIMAL);
            this.pstmt.addBatch();
        }
        long startTime = currentTimeMillis();
        this.pstmt.executeBatch();
        c.commit();
        long stopTime = currentTimeMillis();

        this.rs = this.conn.createStatement().executeQuery("SELECT COUNT(*) FROM testBug41532");
        assertTrue(this.rs.next());
        assertEquals(numberOfRows, this.rs.getInt(1));

        return stopTime - startTime;
    }

    /**
     * Tests fix for Bug#44056 - Statement.getGeneratedKeys() retains result set
     * instances until statement is closed.
     */

    public void testBug44056() throws Exception {
        createTable("testBug44056", "(pk int primary key not null auto_increment)");
        Statement newStmt = this.conn.createStatement();

        try {
            newStmt.executeUpdate("INSERT INTO testBug44056 VALUES (null)", Statement.RETURN_GENERATED_KEYS);
            checkOpenResultsFor44056(newStmt);
            this.pstmt = this.conn.prepareStatement("INSERT INTO testBug44056 VALUES (null)", Statement.RETURN_GENERATED_KEYS);
            this.pstmt.executeUpdate();
            checkOpenResultsFor44056(this.pstmt);
            this.pstmt = ((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement("INSERT INTO testBug44056 VALUES (null)",
                    Statement.RETURN_GENERATED_KEYS);
            this.pstmt.executeUpdate();
            checkOpenResultsFor44056(this.pstmt);
        } finally {
            newStmt.close();
        }
    }

    private void checkOpenResultsFor44056(Statement newStmt) throws SQLException {
        this.rs = newStmt.getGeneratedKeys();
        assertEquals(0, ((com.mysql.jdbc.Statement) newStmt).getOpenResultSetCount());
        this.rs.close();
        assertEquals(0, ((com.mysql.jdbc.Statement) newStmt).getOpenResultSetCount());
    }

    /**
     * Bug #41730 - SQL Injection when using U+00A5 and SJIS/Windows-31J
     */
    public void testBug41730() throws Exception {
        try {
            "".getBytes("sjis");
        } catch (UnsupportedEncodingException ex) {
            return; // test doesn't work on this platform
        }

        Connection conn2 = null;
        PreparedStatement pstmt2 = null;
        try {
            conn2 = getConnectionWithProps("characterEncoding=sjis");
            pstmt2 = conn2.prepareStatement("select ?");
            pstmt2.setString(1, "\u00A5'");
            // this will throw an exception with a syntax error if it fails
            pstmt2.executeQuery();
        } finally {
            try {
                if (pstmt2 != null) {
                    pstmt2.close();
                }
            } catch (SQLException ex) {
            }
            try {
                if (conn2 != null) {
                    conn2.close();
                }
            } catch (SQLException ex) {
            }
        }
    }

    public void testBug43196() throws Exception {
        createTable("`bug43196`",
                "(`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `a` bigint(20) unsigned NOT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1;");

        Connection conn1 = null;

        try {
            assertEquals(1, this.stmt.executeUpdate("INSERT INTO bug43196 (a) VALUES (1)", Statement.RETURN_GENERATED_KEYS));

            this.rs = this.stmt.getGeneratedKeys();

            if (this.rs.next()) {

                Object id = this.rs.getObject(1);// use long

                assertEquals(Long.class, id.getClass());
            }

            this.rs.close();

            this.rs = this.stmt.executeQuery("select id from bug43196");

            if (this.rs.next()) {
                Object id = this.rs.getObject(1);// use BigInteger

                assertEquals(BigInteger.class, id.getClass());
            }

            this.rs.close();

            // insert a id > Long.MAX_VALUE(9223372036854775807)

            assertEquals(1, this.stmt.executeUpdate("insert into bug43196(id,a) values(18446744073709551200,1)", Statement.RETURN_GENERATED_KEYS));

            this.rs = this.stmt.getGeneratedKeys();

            this.rs.first();

            assertTrue("No rows returned", this.rs.isFirst());
            assertEquals("18446744073709551200", this.rs.getObject(1).toString());
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    /**
     * Bug #42253 - multiple escaped quotes cause exception from
     * EscapeProcessor.
     */
    public void testBug42253() throws Exception {
        this.rs = this.stmt.executeQuery("select '\\'\\'','{t\\'}'");
        this.rs.next();
        assertEquals("''", this.rs.getString(1));
        assertEquals("{t'}", this.rs.getString(2));
    }

    /**
     * Bug #41566 - Quotes within comments not correctly ignored by escape
     * parser
     */
    public void testBug41566() throws Exception {
        this.rs = this.stmt.executeQuery("-- this should't change the literal\n select '{1}'");
        this.rs.next();
        assertEquals("{1}", this.rs.getString(1));
    }

    /*
     * Bug #40439 - Error rewriting batched statement if table name ends with
     * "values".
     */
    public void testBug40439() throws Exception {
        Connection conn2 = null;
        try {
            createTable("testBug40439VALUES", "(x int)");
            conn2 = getConnectionWithProps("rewriteBatchedStatements=true");
            PreparedStatement ps = conn2.prepareStatement("insert into testBug40439VALUES (x) values (?)");
            ps.setInt(1, 1);
            ps.addBatch();
            ps.setInt(1, 2);
            ps.addBatch();
            ps.executeBatch();
        } finally {
            if (conn2 != null) {
                try {
                    conn2.close();
                } catch (SQLException ex) {
                }
            }
        }
    }

    public static class Bug39426Interceptor implements StatementInterceptor {
        public static List<Integer> vals = new ArrayList<Integer>();
        String prevSql;

        public void destroy() {
        }

        public boolean executeTopLevelOnly() {
            return false;
        }

        public void init(com.mysql.jdbc.Connection conn, Properties props) throws SQLException {
        }

        public ResultSetInternalMethods postProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, ResultSetInternalMethods originalResultSet,
                com.mysql.jdbc.Connection connection) throws SQLException {
            return null;
        }

        public ResultSetInternalMethods preProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, com.mysql.jdbc.Connection connection)
                throws SQLException {
            String asSql = sql;

            if (interceptedStatement instanceof com.mysql.jdbc.PreparedStatement) {
                asSql = interceptedStatement.toString();
                int firstColon = asSql.indexOf(":");
                asSql = asSql.substring(firstColon + 2);

                if (asSql.equals(this.prevSql)) {
                    throw new RuntimeException("Previous statement matched current: " + sql);
                }
                this.prevSql = asSql;
                ParameterBindings b = ((com.mysql.jdbc.PreparedStatement) interceptedStatement).getParameterBindings();
                vals.add(new Integer(b.getInt(1)));
            }
            return null;
        }
    }

    /**
     * Bug #39426 - executeBatch passes most recent PreparedStatement params to
     * StatementInterceptor
     */
    public void testBug39426() throws Exception {
        Connection c = null;
        try {
            createTable("testBug39426", "(x int)");
            c = getConnectionWithProps("statementInterceptors=testsuite.regression.StatementRegressionTest$Bug39426Interceptor,useServerPrepStmts=false");
            PreparedStatement ps = c.prepareStatement("insert into testBug39426 values (?)");
            ps.setInt(1, 1);
            ps.addBatch();
            ps.setInt(1, 2);
            ps.addBatch();
            ps.setInt(1, 3);
            ps.addBatch();
            ps.executeBatch();
            List<Integer> vals = Bug39426Interceptor.vals;
            assertEquals(new Integer(1), vals.get(0));
            assertEquals(new Integer(2), vals.get(1));
            assertEquals(new Integer(3), vals.get(2));
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    public void testBugDupeKeySingle() throws Exception {
        createTable("testBugDupeKeySingle", "(field1 int not null primary key)");
        Connection conn2 = null;
        try {
            conn2 = getConnectionWithProps("rewriteBatchedStatements=true");

            this.pstmt = conn2.prepareStatement("INSERT INTO testBugDupeKeySingle VALUES (?) ON DUPLICATE KEY UPDATE field1=VALUES(field1)");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();

            // this should be a syntax error
            this.pstmt = conn2.prepareStatement("INSERT INTO testBugDupeKeySingle VALUES (?) ON DUPLICATE KEY UPDATE");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            try {
                this.pstmt.executeBatch();
            } catch (SQLException sqlEx) {
                assertEquals(SQLError.SQL_STATE_SYNTAX_ERROR, sqlEx.getSQLState());
            }

            this.pstmt = conn2.prepareStatement("INSERT INTO testBugDupeKeySingle VALUES (?)");
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(1, 4);
            this.pstmt.executeBatch();
        } finally {
            if (conn2 != null) {
                conn2.close();
            }
        }
    }

    /**
     * Bug #37458 - MySQL 5.1 returns generated keys in ascending order
     */
    public void testBug37458() throws Exception {
        int ids[] = { 13, 1, 8 };
        String vals[] = { "c", "a", "b" };
        createTable("testBug37458", "(id int not null auto_increment, val varchar(100), primary key (id), unique (val))");
        this.stmt.executeUpdate("insert into testBug37458 values (1, 'a'), (8, 'b'), (13, 'c')");
        this.pstmt = this.conn.prepareStatement("insert into testBug37458 (val) values (?) on duplicate key update id = last_insert_id(id)",
                Statement.RETURN_GENERATED_KEYS);
        for (int i = 0; i < ids.length; ++i) {
            this.pstmt.setString(1, vals[i]);
            this.pstmt.addBatch();
        }
        this.pstmt.executeBatch();
        ResultSet keys = this.pstmt.getGeneratedKeys();
        for (int i = 0; i < ids.length; ++i) {
            assertTrue(keys.next());
            assertEquals(ids[i], keys.getInt(1));
        }
    }

    public void testBug34555() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return; // no KILL QUERY prior to this
        }

        createTable("testBug34555", "(field1 int)", "INNODB");
        this.stmt.executeUpdate("INSERT INTO testBug34555 VALUES (0)");

        final Connection lockerConn = getConnectionWithProps("");
        lockerConn.setAutoCommit(false);
        lockerConn.createStatement().execute("SELECT * FROM testBug34555 WHERE field1=0 FOR UPDATE");

        this.conn.setAutoCommit(false);

        this.pstmt = this.conn.prepareStatement("UPDATE testBug34555 SET field1=1 WHERE field1=?");
        this.pstmt.setQueryTimeout(1);
        this.pstmt.setInt(1, 0);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 2);
        this.pstmt.addBatch();

        try {
            this.pstmt.executeBatch();
        } catch (BatchUpdateException batchEx) {
            assertTrue(batchEx.getMessage().startsWith("Statement cancelled"));
        } finally {
            this.conn.setAutoCommit(true);
            lockerConn.commit();
        }
    }

    public void testBug46788() throws Exception {
        createTable("testBug46788", "(modified varchar(32), id varchar(32))");

        Connection rewriteConn = getConnectionWithProps("rewriteBatchedStatements=true");

        this.pstmt = rewriteConn.prepareStatement("insert into testBug46788 (modified,id) values (?,?) ON DUPLICATE KEY UPDATE modified=?");

        this.pstmt.setString(1, "theID");
        this.pstmt.setString(2, "Hello_world_");
        this.pstmt.setString(3, "Hello_world_");

        for (int i = 0; i < 10; i++) {
            this.pstmt.addBatch();
        }

        this.pstmt.executeBatch();
    }

    public void testBug31193() throws Exception {
        createTable("bug31193", "(sometime datetime, junk text)");
        Connection fetchConn = getConnectionWithProps("useCursorFetch=true");
        Statement fetchStmt = fetchConn.createStatement();

        fetchStmt.setFetchSize(10000);

        assertEquals(1, fetchStmt.executeUpdate("INSERT INTO bug31193 (sometime) values ('2007-01-01 12:34:56.7')"));
        this.rs = fetchStmt.executeQuery("SELECT * FROM bug31193");
        this.rs.next();
        String badDatetime = this.rs.getString("sometime");

        this.rs = fetchStmt.executeQuery("SELECT sometime FROM bug31193");
        this.rs.next();
        String goodDatetime = this.rs.getString("sometime");
        assertEquals(goodDatetime, badDatetime);
    }

    public void testBug51666() throws Exception {
        Connection testConn = getConnectionWithProps("statementInterceptors=" + IncrementStatementCountInterceptor.class.getName());
        createTable("testStatementInterceptorCount", "(field1 int)");
        this.stmt.executeUpdate("INSERT INTO testStatementInterceptorCount VALUES (0)");
        ResultSet testRs = testConn.createStatement().executeQuery("SHOW SESSION STATUS LIKE 'Com_select'");
        testRs.next();
        int s = testRs.getInt(2);
        testConn.createStatement().executeQuery("SELECT 1");
        testRs = testConn.createStatement().executeQuery("SHOW SESSION STATUS LIKE 'Com_select'");
        testRs.next();
        assertEquals(s + 1, testRs.getInt(2));

    }

    public void testBug51776() throws Exception {

        Properties props = new Properties();
        NonRegisteringDriver d = new NonRegisteringDriver();
        this.copyBasePropertiesIntoProps(props, d);
        props.setProperty("socketFactory", "testsuite.UnreliableSocketFactory");
        Properties parsed = d.parseURL(BaseTestCase.dbUrl, props);
        String db = parsed.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
        String port = parsed.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);
        String host = getPortFreeHostname(props, d);
        UnreliableSocketFactory.flushAllHostLists();
        UnreliableSocketFactory.mapHost("first", host);
        props.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);

        Connection testConn = getConnectionWithProps("jdbc:mysql://first:" + port + "/" + db, props);
        testConn.setAutoCommit(false);
        testConn.createStatement().execute("SELECT 1");
        UnreliableSocketFactory.downHost("first");
        try {
            testConn.rollback();
            fail("Should receive SQLException on rollback().");
        } catch (SQLException e) {

        }

    }

    public static class IncrementStatementCountInterceptor implements StatementInterceptorV2 {
        public void destroy() {
        }

        public boolean executeTopLevelOnly() {
            return false;
        }

        public void init(com.mysql.jdbc.Connection conn, Properties props) throws SQLException {
        }

        public ResultSetInternalMethods postProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, ResultSetInternalMethods originalResultSet,
                com.mysql.jdbc.Connection connection, int warningCount, boolean noIndexUsed, boolean noGoodIndexUsed, SQLException statementException)
                throws SQLException {
            return null;
        }

        public ResultSetInternalMethods preProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, com.mysql.jdbc.Connection conn)
                throws SQLException {
            java.sql.Statement test = conn.createStatement();
            if (sql.equals("SELECT 1")) {
                return (ResultSetInternalMethods) test.executeQuery("/* execute this, not the original */ SELECT 1");
            }
            return null;
        }

    }

    public void testReversalOfScanFlags() throws Exception {
        createTable("testReversalOfScanFlags", "(field1 int)");
        this.stmt.executeUpdate("INSERT INTO testReversalOfScanFlags VALUES (1),(2),(3)");

        Connection scanningConn = getConnectionWithProps("statementInterceptors=" + ScanDetectingInterceptor.class.getName());

        try {
            ScanDetectingInterceptor.watchForScans = true;
            scanningConn.createStatement().executeQuery("SELECT field1 FROM testReversalOfScanFlags");
            assertTrue(ScanDetectingInterceptor.hasSeenScan);
            assertFalse(ScanDetectingInterceptor.hasSeenBadIndex);
        } finally {
            scanningConn.close();
        }

    }

    public static class ScanDetectingInterceptor implements StatementInterceptorV2 {
        static boolean watchForScans = false;
        static boolean hasSeenScan = false;
        static boolean hasSeenBadIndex = false;

        public void destroy() {

        }

        public boolean executeTopLevelOnly() {
            return false;
        }

        public void init(com.mysql.jdbc.Connection conn, Properties props) throws SQLException {

        }

        public ResultSetInternalMethods postProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, ResultSetInternalMethods originalResultSet,
                com.mysql.jdbc.Connection connection, int warningCount, boolean noIndexUsed, boolean noGoodIndexUsed, SQLException statementException)
                throws SQLException {
            if (watchForScans) {
                if (noIndexUsed) {
                    hasSeenScan = true;
                }

                if (noGoodIndexUsed) {
                    hasSeenBadIndex = true;
                }
            }

            return null;
        }

        public ResultSetInternalMethods preProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, com.mysql.jdbc.Connection connection)
                throws SQLException {
            return null;
        }

    }

    /**
     * Tests fix for Bug#51704, rewritten batched statements don't honor escape
     * processing flag of Statement that they are created for
     */
    public void testBug51704() throws Exception {
        createTable("testBug51704", "(field1 TIMESTAMP)");
        Connection rewriteConn = getConnectionWithProps("rewriteBatchedStatements=true");
        Statement rewriteStmt = rewriteConn.createStatement();

        try {
            rewriteStmt.setEscapeProcessing(false);

            for (int i = 0; i < 20; i++) {
                rewriteStmt.addBatch("INSERT INTO testBug51704 VALUES ({tsp '2002-11-12 10:00:00'})");
            }

            rewriteStmt.executeBatch(); // this should pass, because mysqld doesn't validate any escape sequences, 
                                        // it just strips them, where our escape processor validates them

            Statement batchStmt = this.conn.createStatement();
            batchStmt.setEscapeProcessing(false);
            batchStmt.addBatch("INSERT INTO testBug51704 VALUES ({tsp '2002-11-12 10:00:00'})");
            batchStmt.executeBatch(); // same here
        } finally {
            rewriteConn.close();
        }
    }

    public void testBug54175() throws Exception {
        if (!versionMeetsMinimum(5, 5)) {
            return;
        }

        Connection utf8conn = getConnectionWithProps("characterEncoding=utf8");

        createTable("testBug54175", "(a VARCHAR(10)) CHARACTER SET utf8mb4");
        this.stmt.execute("INSERT INTO testBug54175 VALUES(0xF0AFA6B2)");
        this.rs = utf8conn.createStatement().executeQuery("SELECT * FROM testBug54175");
        assertTrue(this.rs.next());
        assertEquals(55422, this.rs.getString(1).charAt(0));
    }

    /**
     * Tests fix for Bug#58728, NPE in com.mysql.jdbc.jdbc2.optional.StatementWrappe.getResultSet()
     * ((com.mysql.jdbc.ResultSetInternalMethods) rs).setWrapperStatement(this);
     * when rs is null
     */
    public void testBug58728() throws Exception {
        createTable("testbug58728", "(Id INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY, txt VARCHAR(50))", "InnoDB");
        this.stmt.executeUpdate("INSERT INTO testbug58728 VALUES (NULL, 'Text 1'), (NULL, 'Text 2')");

        MysqlConnectionPoolDataSource pds = new MysqlConnectionPoolDataSource();
        pds.setUrl(dbUrl);
        Statement stmt1 = pds.getPooledConnection().getConnection().createStatement();
        stmt1.executeUpdate("UPDATE testbug58728 SET txt = 'New text' WHERE Id > 0");
        ResultSet rs1 = stmt1.getResultSet();
        stmt1.close();
        if (rs1 != null) {
            rs1.close();
        }
    }

    public void testBug61501() throws Exception {
        createTable("testBug61501", "(id int)");
        this.stmt.executeUpdate("INSERT INTO testBug61501 VALUES (1)");
        String sql = "SELECT id FROM testBug61501 where id=1";
        this.pstmt = this.conn.prepareStatement(sql);
        this.pstmt.executeQuery();
        this.pstmt.cancel();
        this.pstmt.close();

        this.pstmt = this.conn.prepareStatement(sql);
        this.rs = this.pstmt.executeQuery();

        this.stmt.cancel();
        this.stmt.executeQuery(sql);
        this.stmt.cancel();
        this.stmt.execute(sql);
        this.pstmt = ((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement(sql);
        this.pstmt.execute();
        this.pstmt.cancel();
        this.pstmt.execute();

        sql = "INSERT INTO testBug61501 VALUES (2)";
        this.pstmt = this.conn.prepareStatement(sql);
        this.pstmt.execute();
        assertEquals(1, this.pstmt.getUpdateCount());
        this.pstmt.cancel();
        this.pstmt.close();

        this.pstmt = this.conn.prepareStatement(sql);
        assertEquals(1, this.pstmt.executeUpdate());

        this.stmt.cancel();
        assertEquals(1, this.stmt.executeUpdate(sql));
        this.stmt.cancel();
        this.stmt.execute(sql);
        assertEquals(1, this.stmt.getUpdateCount());

        this.pstmt = ((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement(sql);
        this.pstmt.execute();
        assertEquals(1, this.pstmt.getUpdateCount());
        this.pstmt.cancel();
        this.pstmt.close();

        this.pstmt = ((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement(sql);
        assertEquals(1, this.pstmt.executeUpdate());

        this.pstmt.cancel();
        this.pstmt.addBatch();
        this.pstmt.addBatch();
        this.pstmt.addBatch();
        int[] counts = this.pstmt.executeBatch();

        for (int i = 0; i < counts.length; i++) {
            assertEquals(1, counts[i]);
        }

        this.pstmt = this.conn.prepareStatement(sql);
        this.pstmt.cancel();
        this.pstmt.addBatch();
        this.pstmt.addBatch();
        this.pstmt.addBatch();
        counts = this.pstmt.executeBatch();

        for (int i = 0; i < counts.length; i++) {
            assertEquals(1, counts[i]);
        }

        this.stmt.cancel();
        this.stmt.addBatch(sql);
        this.stmt.addBatch(sql);
        this.stmt.addBatch(sql);

        counts = this.stmt.executeBatch();

        for (int i = 0; i < counts.length; i++) {
            assertEquals(1, counts[i]);
        }

    }

    public void testbug61866() throws Exception {

        createProcedure("WARN_PROCEDURE", "() BEGIN	DECLARE l_done INT;	SELECT 1 	INTO l_done	FROM DUAL	WHERE 1=2; END");
        this.pstmt = this.conn.prepareCall("{CALL WARN_PROCEDURE()}");
        this.pstmt.execute();
        assertTrue("No warning when expected",
                this.pstmt.getWarnings().toString().contentEquals("java.sql.SQLWarning: No data - zero rows fetched, selected, or processed"));
        this.pstmt.clearWarnings();
        assertNull("Warning when not expected", this.pstmt.getWarnings());
    }

    public void testbug12565726() throws Exception {
        // Not putting the space between VALUES() and ON DUPLICATE KEY UPDATE
        // causes C/J a) enter rewriting the query altrhough it has ON UPDATE 
        // and b) to generate the wrong query with multiple ON DUPLICATE KEY

        Properties props = new Properties();
        props.put("rewriteBatchedStatements", "true");
        props.put("useServerPrepStmts", "false");
        props.put("enablePacketDebug", "true");
        this.conn = getConnectionWithProps(props);
        this.stmt = this.conn.createStatement();

        try {
            createTable("testbug12565726", "(id int primary key, txt1 varchar(32))");
            this.stmt.executeUpdate("INSERT INTO testbug12565726 (id, txt1) VALUES (1, 'something')");

            this.pstmt = this.conn.prepareStatement("INSERT INTO testbug12565726 (id, txt1) VALUES (?, ?)ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id)+10");

            this.pstmt.setInt(1, 1);
            this.pstmt.setString(2, "something else");
            this.pstmt.addBatch();

            this.pstmt.setInt(1, 2);
            this.pstmt.setString(2, "hope it is not error again!");
            this.pstmt.addBatch();

            this.pstmt.executeBatch();

        } finally {
        }

    }

    public void testBug36478() throws Exception {

        createTable("testBug36478", "(`limit` varchar(255) not null primary key, id_limit INT, limit1 INT, maxlimit2 INT)");

        this.stmt.execute("INSERT INTO testBug36478 VALUES ('bahblah',1,1,1)");
        this.stmt.execute("INSERT INTO testBug36478 VALUES ('bahblah2',2,2,2)");
        this.pstmt = this.conn.prepareStatement("select 1 FROM testBug36478");

        this.pstmt.setMaxRows(1);
        this.rs = this.pstmt.executeQuery();
        this.rs.first();
        assertTrue(this.rs.isFirst());
        assertTrue(this.rs.isLast());

        this.pstmt = this.conn.prepareStatement("select `limit`, id_limit, limit1, maxlimit2 FROM testBug36478");
        this.pstmt.setMaxRows(0);
        this.rs = this.pstmt.executeQuery();
        this.rs.first();
        assertTrue(this.rs.isFirst());
        assertFalse(this.rs.isLast());

        //SSPS
        Connection _conn = null;
        PreparedStatement s = null;
        try {
            Properties props = new Properties();
            props.setProperty("useServerPrepStmts", "true");

            _conn = getConnectionWithProps(props);
            s = _conn.prepareStatement("select 1 FROM testBug36478");

            s.setMaxRows(1);
            ResultSet _rs = s.executeQuery();
            _rs.first();
            assertTrue(_rs.isFirst());
            assertTrue(_rs.isLast());

            s = _conn.prepareStatement("select `limit`, id_limit, limit1, maxlimit2 FROM testBug36478");
            s.setMaxRows(0);
            _rs = s.executeQuery();
            _rs.first();
            assertTrue(_rs.isFirst());
            assertFalse(_rs.isLast());

        } finally {
            if (s != null) {
                s.close();
            }
            if (_conn != null) {
                _conn.close();
            }
        }

    }

    /**
     * Tests fix for BUG#40279 - Timestamp values get truncated when passed as prepared statement parameters
     * (and duplicate BUG#60584 - prepared statements truncate milliseconds)
     * 
     * [13 Sep 2012 21:06] Mark Matthews
     * This was fixed with http://bazaar.launchpad.net/~mysql/connectorj/5.1/revision/1107 in 2011,
     * it supports MySQL-5.6.4 or later.
     * 
     * But that fix did not cover useLegacyDatetimeCode=true case.
     * 
     * @throws Exception
     */
    public void testBug40279() throws Exception {
        if (!versionMeetsMinimum(5, 6, 4)) {
            return;
        }

        createTable("testBug40279", "(f1 int, f2 timestamp(6))");

        Timestamp ts = new Timestamp(1300791248001L);

        Connection ps_conn_legacy = null;
        Connection ps_conn_nolegacy = null;

        Connection ssps_conn_legacy = null;
        Connection ssps_conn_nolegacy = null;

        try {
            Properties props = new Properties();
            props.setProperty("serverTimezone", "UTC");
            props.setProperty("useLegacyDatetimeCode", "true");
            props.setProperty("useServerPrepStmts", "false");
            ps_conn_legacy = getConnectionWithProps(props);

            props.setProperty("useLegacyDatetimeCode", "false");
            ps_conn_nolegacy = getConnectionWithProps(props);

            props.setProperty("useLegacyDatetimeCode", "true");
            props.setProperty("useServerPrepStmts", "true");
            ssps_conn_legacy = getConnectionWithProps(props);

            props.setProperty("useLegacyDatetimeCode", "false");
            ssps_conn_nolegacy = getConnectionWithProps(props);

            this.pstmt = ps_conn_legacy.prepareStatement("INSERT INTO testBug40279(f1, f2) VALUES (?, ?)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setTimestamp(2, ts);
            this.pstmt.execute();
            this.pstmt.close();

            this.pstmt = ps_conn_nolegacy.prepareStatement("INSERT INTO testBug40279(f1, f2) VALUES (?, ?)");
            this.pstmt.setInt(1, 2);
            this.pstmt.setTimestamp(2, ts);
            this.pstmt.execute();
            this.pstmt.close();

            this.pstmt = ssps_conn_legacy.prepareStatement("INSERT INTO testBug40279(f1, f2) VALUES (?, ?)");
            this.pstmt.setInt(1, 3);
            this.pstmt.setTimestamp(2, ts);
            this.pstmt.execute();
            this.pstmt.close();

            this.pstmt = ssps_conn_nolegacy.prepareStatement("INSERT INTO testBug40279(f1, f2) VALUES (?, ?)");
            this.pstmt.setInt(1, 4);
            this.pstmt.setTimestamp(2, ts);
            this.pstmt.execute();
            this.pstmt.close();

            this.rs = this.stmt.executeQuery("SELECT f2 FROM testBug40279");
            while (this.rs.next()) {
                assertEquals(ts.getNanos(), this.rs.getTimestamp("f2").getNanos());
            }

        } finally {
            if (ps_conn_legacy != null) {
                ps_conn_legacy.close();
            }
            if (ps_conn_nolegacy != null) {
                ps_conn_nolegacy.close();
            }
            if (ssps_conn_legacy != null) {
                ssps_conn_legacy.close();
            }
            if (ssps_conn_nolegacy != null) {
                ssps_conn_nolegacy.close();
            }
        }

    }

    /**
     * Tests fix for BUG#35653 - executeQuery() in Statement.java let "TRUNCATE" queries being executed.
     * "RENAME" is also filtered now.
     * 
     * @throws Exception
     */
    public void testBug35653() throws Exception {
        createTable("testBug35653", "(f1 int)");
        try {
            this.stmt.executeQuery("TRUNCATE testBug35653");
            fail("executeQuery() shouldn't allow TRUNCATE");
        } catch (SQLException e) {
            assertTrue(SQLError.SQL_STATE_ILLEGAL_ARGUMENT == e.getSQLState());
        }

        try {
            this.stmt.executeQuery("RENAME TABLE testBug35653 TO testBug35653_new");
            fail("executeQuery() shouldn't allow RENAME");
        } catch (SQLException e) {
            assertTrue(SQLError.SQL_STATE_ILLEGAL_ARGUMENT == e.getSQLState());
        } finally {
            dropTable("testBug35653_new");
        }
    }

    /**
     * Tests fix for BUG#64805 - StatementImpl$CancelTask occasionally throws NullPointerExceptions.
     * 
     * @throws Exception
     */
    public void testBug64805() throws Exception {

        try {
            this.stmt.setQueryTimeout(5);
            this.stmt.executeQuery("select sleep(5)");
        } catch (NullPointerException e) {
            e.printStackTrace();
            fail();
        } catch (Exception e) {
            if (e instanceof MySQLTimeoutException) {
                // expected behavior in slow environment
            } else {
                throw e;
            }
        }

    }

    /**
     * WL#4897 - Add EXPLAIN INSERT/UPDATE/DELETE
     * 
     * Added support for EXPLAIN INSERT/REPLACE/UPDATE/DELETE. Connector/J must issue a warning containing the execution
     * plan for slow queries when connection properties logSlowQueries=true and explainSlowQueries=true are used.
     * 
     * @throws SQLException
     */
    public void testExecutionPlanForSlowQueries() throws Exception {
        // once slow query (with execution plan) warning is sent to System.err, we capture messages sent here to check proper operation.
        final class TestHandler {
            // System.err diversion handling
            PrintStream systemErrBackup = null;
            ByteArrayOutputStream systemErrDetour = null;

            // Connection handling
            Connection testConn = null;

            TestHandler() {
                this.systemErrBackup = System.err;
                this.systemErrDetour = new ByteArrayOutputStream(8192);
                System.setErr(new PrintStream(this.systemErrDetour));
            }

            boolean containsSlowQueryMsg(String lookFor) {
                String errMsg = this.systemErrDetour.toString();
                boolean found = false;

                if (errMsg.indexOf("Slow query explain results for '" + lookFor + "'") != -1) {
                    found = true;
                }
                this.systemErrDetour.reset();
                // print message in original OutputStream.
                this.systemErrBackup.print(errMsg);
                return found;
            }

            void undoSystemErrDiversion() throws IOException {
                this.systemErrBackup.print(this.systemErrDetour.toString());
                this.systemErrDetour.close();
                System.setErr(this.systemErrBackup);
                this.systemErrDetour = null;
                this.systemErrBackup = null;
            }

            @SuppressWarnings("synthetic-access")
            Connection getNewConnectionForSlowQueries() throws SQLException {
                releaseConnectionResources();
                this.testConn = getConnectionWithProps("logSlowQueries=true,explainSlowQueries=true");
                Statement st = this.testConn.createStatement();
                // execute several fast queries to unlock slow query analysis and lower query execution time mean
                for (int i = 0; i < 25; i++) {
                    st.executeQuery("SELECT 1");
                }
                return this.testConn;
            }

            void releaseConnectionResources() throws SQLException {
                if (this.testConn != null) {
                    this.testConn.close();
                    this.testConn = null;
                }
            }
        }

        TestHandler testHandler = new TestHandler();
        Statement testStatement = null;

        try {
            if (versionMeetsMinimum(5, 6, 3)) {
                createTable("testWL4897", "(f1 INT NOT NULL PRIMARY KEY, f2 CHAR(50))");

                // when executed in the following sequence, each one of these queries take approximately 1 sec.
                final String[] slowQueries = { "INSERT INTO testWL4897 VALUES (SLEEP(0.5) + 1, 'MySQL'), (SLEEP(0.5) + 2, 'Connector/J')",
                        "SELECT * FROM testWL4897 WHERE f1 + SLEEP(0.5) = f1",
                        "REPLACE INTO testWL4897 VALUES (SLEEP(0.33) + 2, 'Database'), (SLEEP(0.33) + 3, 'Connector'), (SLEEP(0.33) + 4, 'Java')",
                        "UPDATE testWL4897 SET f1 = f1 * 10 + SLEEP(0.25)", "DELETE FROM testWL4897 WHERE f1 + SLEEP(0.25) = f1" };

                for (String query : slowQueries) {
                    testStatement = testHandler.getNewConnectionForSlowQueries().createStatement();
                    testStatement.execute(query);
                    assertTrue("A slow query explain results warning should have been issued for: '" + query + "'.", testHandler.containsSlowQueryMsg(query));
                    testStatement.close();
                }
            } else {
                // only SELECT is qualified to log slow query explain results warning
                final String query = "SELECT SLEEP(1)";

                testStatement = testHandler.getNewConnectionForSlowQueries().createStatement();
                testStatement.execute(query);
                assertTrue("A slow query explain results warning should have been issued for: '" + query + "'.", testHandler.containsSlowQueryMsg(query));
                testStatement.close();
            }

        } finally {
            testHandler.releaseConnectionResources();
            testHandler.undoSystemErrDiversion();
        }
    }

    /**
     * Tests fix for BUG#68562 - Combination rewriteBatchedStatements and useAffectedRows not working as expected
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug68562() throws Exception {
        testBug68562BatchWithSize(1);
        testBug68562BatchWithSize(3);
    }

    private void testBug68562BatchWithSize(int batchSize) throws Exception {

        // 5.1 server returns wrong values for found_rows because Bug#46675 was fixed only for 5.5+
        if (!versionMeetsMinimum(5, 5)) {
            return;
        }

        createTable("testBug68562_found", "(id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, version VARCHAR(255)) ENGINE=InnoDB;");
        createTable("testBug68562_affected", "(id INTEGER NOT NULL PRIMARY KEY, name VARCHAR(255) NOT NULL, version VARCHAR(255)) ENGINE=InnoDB;");

        // insert the records (no update)
        int[] foundRows = testBug68562ExecuteBatch(batchSize, false, false, false);
        for (int foundRow : foundRows) {
            assertEquals(1, foundRow);
        }
        int[] affectedRows = testBug68562ExecuteBatch(batchSize, true, false, false);
        for (int affectedRow : affectedRows) {
            assertEquals(1, affectedRow);
        }

        // update the inserted records with same values
        foundRows = testBug68562ExecuteBatch(batchSize, false, false, false);
        for (int foundRow : foundRows) {
            assertEquals(1, foundRow);
        }
        affectedRows = testBug68562ExecuteBatch(batchSize, true, false, false);
        for (int affectedRow : affectedRows) {
            assertEquals(0, affectedRow);
        }

        // update the inserted records with same values REWRITING THE BATCHED STATEMENTS
        foundRows = testBug68562ExecuteBatch(batchSize, false, true, false);
        for (int foundRow : foundRows) {
            assertEquals(batchSize > 1 ? Statement.SUCCESS_NO_INFO : batchSize, foundRow);
        }
        affectedRows = testBug68562ExecuteBatch(batchSize, true, true, false);
        for (int affectedRow : affectedRows) {
            assertEquals(0, affectedRow);
        }

        // update the inserted records with NEW values REWRITING THE BATCHED STATEMENTS
        foundRows = testBug68562ExecuteBatch(batchSize, false, true, true);
        for (int foundRow : foundRows) {
            assertEquals(batchSize > 1 ? Statement.SUCCESS_NO_INFO : 2 * batchSize, foundRow);
        }
        affectedRows = testBug68562ExecuteBatch(batchSize, true, true, true);
        for (int affectedRow : affectedRows) {
            assertEquals(batchSize > 1 ? Statement.SUCCESS_NO_INFO : 2 * batchSize, affectedRow);
        }
    }

    private int[] testBug68562ExecuteBatch(int batchSize, boolean useAffectedRows, boolean rewriteBatchedStatements, boolean realUpdate)
            throws ClassNotFoundException, SQLException {

        String tableName = "testBug68562";

        Properties properties = new Properties();
        if (useAffectedRows) {
            properties.put("useAffectedRows", "true");
            tableName += "_affected";
        } else {
            tableName += "_found";
        }
        if (rewriteBatchedStatements) {
            properties.put("rewriteBatchedStatements", "true");
        }
        Connection connection = getConnectionWithProps(properties);

        PreparedStatement statement = connection.prepareStatement("INSERT INTO " + tableName
                + "(id, name, version) VALUES(?,?,?) ON DUPLICATE KEY UPDATE version = "
                + (realUpdate ? "CONCAT(VALUES(version),'updated'), name = CONCAT(VALUES(name),'updated')" : "VALUES(version), name = VALUES(name)"));
        for (int i = 0; i < batchSize; i++) {
            statement.setInt(1, i);
            statement.setString(2, "name" + i);
            statement.setString(3, "version" + i);
            statement.addBatch();
        }

        int[] affectedRows = statement.executeBatch();

        statement.close();
        connection.close();

        return affectedRows;

    }

    /**
     * Tests fix for BUG#55340 - initializeResultsMetadataFromCache fails on second call to stored proc
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug55340() throws Exception {
        Connection testConnCacheRSMD = getConnectionWithProps("cacheResultSetMetadata=true");
        ResultSetMetaData rsmd;

        createTable("testBug55340", "(col1 INT, col2 CHAR(10))");
        createProcedure("testBug55340", "() BEGIN SELECT * FROM testBug55340; END");

        assertEquals(this.stmt.executeUpdate("INSERT INTO testBug55340 (col1, col2) VALUES (1, 'one'), (2, 'two'), (3, 'three')"), 3);

        for (Connection testConn : new Connection[] { this.conn, testConnCacheRSMD }) {
            String testDesc = testConn == testConnCacheRSMD ? "Conn. with 'cacheResultSetMetadata=true'" : "Default connection";

            // bug occurs in 2nd call only
            for (int i = 1; i <= 2; i++) {
                for (PreparedStatement testStmt : new PreparedStatement[] { testConn.prepareStatement("SELECT * FROM testBug55340"),
                        testConn.prepareCall("CALL testBug55340()") }) {

                    assertTrue(testStmt.execute());
                    this.rs = testStmt.getResultSet();
                    assertResultSetLength(this.rs, 3);

                    rsmd = this.rs.getMetaData();
                    assertEquals("(" + i + ") " + testDesc + " - " + testStmt.getClass().getSimpleName() + ":RSMetaData - wrong column count.", 2,
                            rsmd.getColumnCount());
                    assertEquals("(" + i + ") " + testDesc + " - " + testStmt.getClass().getSimpleName() + ":RSMetaData - wrong column(1) type.",
                            Integer.class.getName(), rsmd.getColumnClassName(1));
                    assertEquals("(" + i + ") " + testDesc + " - " + testStmt.getClass().getSimpleName() + ":RSMetaData - wrong column(2) type.",
                            String.class.getName(), rsmd.getColumnClassName(2));

                    testStmt.close();
                }
            }
        }

        testConnCacheRSMD.close();
    }

    /**
     * Tests fix for BUG#71396 - setMaxRows (SQL_SELECT_LIMIT) from one query used in later queries (sometimes)
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug71396() throws Exception {
        final String queryLimitClause = "SELECT * FROM testBug71396 LIMIT 2";
        final String queryLimitClauseInJoin = "SELECT * FROM testBug71396 A JOIN (SELECT * FROM testBug71396 LIMIT 2) B ON A.c != B.c";
        final String queryLimitInQuotes = "SELECT * FROM testBug71396 WHERE c != 'Unlimited'";
        final String queryLimitInComment = "SELECT * FROM testBug71396 -- Unlimited";
        final String queryNoLimit = "SELECT * FROM testBug71396";

        final String[] queries = new String[] { queryLimitClause, queryLimitClauseInJoin, queryLimitInQuotes, queryLimitInComment, queryNoLimit };

        Connection testConn;
        Statement testStmt;
        ResultSet testRS;
        PreparedStatement testPStmtSet[];

        createTable("testBug71396", "(c VARCHAR(5))");
        this.stmt.execute("INSERT INTO testBug71396 VALUES ('One'), ('Two'), ('Three')");

        /*
         * Case 1: Statement.executeQuery() and Statement.execute() with plain Connection.
         */
        testConn = getConnectionWithProps("");

        // safety check
        testBug71396StatementMultiCheck(testConn, queries, new int[] { 2, 4, 3, 3, 3 });

        // initialize Statement with a given maxRow value, keep open until end of the case
        testStmt = testBug71396StatementInit(testConn, 1);

        // check results count using the same Statement[maxRows = 1] for all queries
        testBug71396StatementMultiCheck(testStmt, queries, new int[] { 1, 1, 1, 1, 1 });

        // check results count using same Connection and one new Statement[default maxRows] per query
        testBug71396StatementMultiCheck(testConn, queries, new int[] { 2, 4, 3, 3, 3 });

        // recheck results count reusing the first Statement[maxRows = 1] for all queries - confirm maxRows wasn't lost
        testBug71396StatementMultiCheck(testStmt, queries, new int[] { 1, 1, 1, 1, 1 });

        testStmt.close();
        testConn.close();

        /*
         * Case 2: PreparedStatement.executeQuery() and PreparedStatement.execute() with plain Connection.
         */
        testConn = getConnectionWithProps("");

        // safety check
        testBug71396PrepStatementMultiCheck(testConn, queries, new int[] { 2, 4, 3, 3, 3 });

        // initialize Statement with a given maxRow value, keep open until end of the case
        testStmt = testBug71396StatementInit(testConn, 1);

        // initialize a set of PreparedStatements with a given maxRow value, keep open until end of the case
        testPStmtSet = testBug71396PrepStatementInit(testConn, queries, 1);

        // check results count using same Connection and one PreparedStatement[maxRows = 1] per query
        testBug71396PrepStatementMultiCheck(testPStmtSet, queries, new int[] { 1, 1, 1, 1, 1 });

        // check results count using same Connection and one new PreparedStatement[default maxRows] per query
        testBug71396PrepStatementMultiCheck(testConn, queries, new int[] { 2, 4, 3, 3, 3 });

        // check results count reusing the first PreparedStatement[maxRows = 1] per query - confirm maxRows wasn't lost
        testBug71396PrepStatementMultiCheck(testPStmtSet, queries, new int[] { 1, 1, 1, 1, 1 });

        testBug71396PrepStatementClose(testPStmtSet);
        testStmt.close();
        testConn.close();

        /*
         * Case 3: PreparedStatement.executeQuery() and PreparedStatement.execute() with
         * Connection[useServerPrepStmts=true].
         */
        testConn = getConnectionWithProps("useServerPrepStmts=true");

        // safety check
        testBug71396PrepStatementMultiCheck(testConn, queries, new int[] { 2, 4, 3, 3, 3 });

        // initialize Statement with a given maxRow value, keep open until end of the case.
        testStmt = testBug71396StatementInit(testConn, 1);

        // initialize a set of PreparedStatements with a given maxRow value, keep open until end of the case
        testPStmtSet = testBug71396PrepStatementInit(testConn, queries, 1);

        // check results count using same Connection and one PreparedStatement[maxRows = 1] per query
        testBug71396PrepStatementMultiCheck(testPStmtSet, queries, new int[] { 1, 1, 1, 1, 1 });

        // check results count using same Connection and one new PreparedStatement[default maxRows] per query
        testBug71396PrepStatementMultiCheck(testConn, queries, new int[] { 2, 4, 3, 3, 3 });

        // check results count reusing the first PreparedStatement[maxRows = 1] per query - confirm maxRows wasn't lost
        testBug71396PrepStatementMultiCheck(testPStmtSet, queries, new int[] { 1, 1, 1, 1, 1 });

        testBug71396PrepStatementClose(testPStmtSet);
        testStmt.close();
        testConn.close();

        /*
         * Case 4: Statement.executeQuery() and Statement.execute() with Connection[maxRows=2].
         */
        testConn = getConnectionWithProps("maxRows=2");

        // safety check
        testBug71396StatementMultiCheck(testConn, queries, new int[] { 2, 2, 2, 2, 2 });

        // initialize Statement with a given maxRow value, keep open until end of the case
        testStmt = testBug71396StatementInit(testConn, 1);

        // check results count using the same Statement[maxRows = 1] for all queries
        testBug71396StatementMultiCheck(testStmt, queries, new int[] { 1, 1, 1, 1, 1 });

        // check results count using same Connection and one new Statement[default maxRows] per query
        testBug71396StatementMultiCheck(testConn, queries, new int[] { 2, 2, 2, 2, 2 });

        // recheck results count reusing the first Statement[maxRows = 1] for all queries - confirm maxRows wasn't lost
        testBug71396StatementMultiCheck(testStmt, queries, new int[] { 1, 1, 1, 1, 1 });

        testStmt.close();
        testConn.close();

        /*
         * Case 5: PreparedStatement.executeQuery() and PreparedStatement.execute() with Connection[maxRows=2].
         */
        testConn = getConnectionWithProps("maxRows=2");

        // safety check
        testBug71396PrepStatementMultiCheck(testConn, queries, new int[] { 2, 2, 2, 2, 2 });

        // initialize Statement with a given maxRow value, keep open until end of the case
        testStmt = testBug71396StatementInit(testConn, 1);

        // initialize a set of PreparedStatements with a given maxRow value, keep open until end of the case
        testPStmtSet = testBug71396PrepStatementInit(testConn, queries, 1);

        // check results count using same Connection and one PreparedStatement[maxRows = 1] per query
        testBug71396PrepStatementMultiCheck(testPStmtSet, queries, new int[] { 1, 1, 1, 1, 1 });

        // check results count using same Connection and one new PreparedStatement[default maxRows] per query
        testBug71396PrepStatementMultiCheck(testConn, queries, new int[] { 2, 2, 2, 2, 2 });

        // check results count reusing the first PreparedStatement[maxRows = 1] per query - confirm maxRows wasn't lost
        testBug71396PrepStatementMultiCheck(testPStmtSet, queries, new int[] { 1, 1, 1, 1, 1 });

        testBug71396PrepStatementClose(testPStmtSet);
        testStmt.close();
        testConn.close();

        /*
         * Case 6: PreparedStatement.executeQuery() and PreparedStatement.execute() with
         * Connection[useServerPrepStmts=true;maxRows=2].
         */
        testConn = getConnectionWithProps("maxRows=2,useServerPrepStmts=true");

        // safety check
        testBug71396PrepStatementMultiCheck(testConn, queries, new int[] { 2, 2, 2, 2, 2 });

        // initialize Statement with a given maxRow value, keep open until end of the case
        testStmt = testBug71396StatementInit(testConn, 1);

        // initialize a set of PreparedStatements with a given maxRow value, keep open until end of the case
        testPStmtSet = testBug71396PrepStatementInit(testConn, queries, 1);

        // check results count using same Connection and one PreparedStatement[maxRows = 1] per query
        testBug71396PrepStatementMultiCheck(testPStmtSet, queries, new int[] { 1, 1, 1, 1, 1 });

        // check results count using same Connection and one new PreparedStatement[default maxRows] per query
        testBug71396PrepStatementMultiCheck(testConn, queries, new int[] { 2, 2, 2, 2, 2 });

        // check results count reusing the first PreparedStatement[maxRows = 1] per query - confirm maxRows wasn't lost
        testBug71396PrepStatementMultiCheck(testPStmtSet, queries, new int[] { 1, 1, 1, 1, 1 });

        testBug71396PrepStatementClose(testPStmtSet);
        testStmt.close();
        testConn.close();

        /*
         * Case 7: Multiple combinations between maxRows connection prop, Statement.setMaxRows() and LIMIT clause.
         * Covers some cases not tested previously.
         */
        testBug71396MultiSettingsCheck("", -1, 1, 1);
        testBug71396MultiSettingsCheck("", -1, 2, 2);
        testBug71396MultiSettingsCheck("", 1, 1, 1);
        testBug71396MultiSettingsCheck("", 1, 2, 1);
        testBug71396MultiSettingsCheck("", 2, 1, 1);
        testBug71396MultiSettingsCheck("", 2, 2, 2);

        testBug71396MultiSettingsCheck("maxRows=1", -1, 1, 1);
        testBug71396MultiSettingsCheck("maxRows=1", -1, 2, 1);
        testBug71396MultiSettingsCheck("maxRows=1", 1, 1, 1);
        testBug71396MultiSettingsCheck("maxRows=1", 1, 2, 1);
        testBug71396MultiSettingsCheck("maxRows=1", 2, 1, 1);
        testBug71396MultiSettingsCheck("maxRows=1", 2, 2, 2);

        testBug71396MultiSettingsCheck("maxRows=2", -1, 1, 1);
        testBug71396MultiSettingsCheck("maxRows=2", -1, 2, 2);
        testBug71396MultiSettingsCheck("maxRows=2", 1, 1, 1);
        testBug71396MultiSettingsCheck("maxRows=2", 1, 2, 1);
        testBug71396MultiSettingsCheck("maxRows=2", 2, 1, 1);
        testBug71396MultiSettingsCheck("maxRows=2", 2, 2, 2);

        // Case 8: New session bue to user change
        try {
            this.stmt.execute("CREATE USER 'testBug71396User'@'%' IDENTIFIED BY 'testBug71396User'");
            this.stmt.execute("GRANT SELECT ON *.* TO 'testBug71396User'@'%'");

            testConn = getConnectionWithProps("");
            testStmt = testBug71396StatementInit(testConn, 5);

            ((MySQLConnection) testConn).changeUser("testBug71396User", "testBug71396User");

            Statement testStmtTmp = testConn.createStatement();
            testRS = testStmtTmp.executeQuery("SELECT CURRENT_USER(), @@SESSION.SQL_SELECT_LIMIT");
            assertTrue(testRS.next());
            assertEquals("testBug71396User@%", testRS.getString(1));
            assertTrue(String.format("expected:higher than<%d> but was:<%s>", Integer.MAX_VALUE, testRS.getBigDecimal(2)),
                    testRS.getBigDecimal(2).compareTo(new BigDecimal(Integer.MAX_VALUE)) == 1);
            testRS.close();
            testStmtTmp.close();

            testRS = testStmt.executeQuery("SELECT CURRENT_USER(), @@SESSION.SQL_SELECT_LIMIT");
            assertTrue(testRS.next());
            assertEquals("testBug71396User@%", testRS.getString(1));
            assertEquals(new BigDecimal(5), testRS.getBigDecimal(2));
            testRS.close();

            testStmt.close();
            testConn.close();
        } finally {
            this.stmt.execute("DROP USER 'testBug71396User'");
        }

        // Case 9: New session due to reconnection
        testConn = getConnectionWithProps("");
        testStmt = testBug71396StatementInit(testConn, 5);

        ((MySQLConnection) testConn).createNewIO(true); // true or false argument is irrelevant for this test case

        Statement testStmtTmp = testConn.createStatement();
        testRS = testStmtTmp.executeQuery("SELECT @@SESSION.SQL_SELECT_LIMIT");
        assertTrue(testRS.next());
        assertTrue(String.format("expected:higher than<%d> but was:<%s>", Integer.MAX_VALUE, testRS.getBigDecimal(1)),
                testRS.getBigDecimal(1).compareTo(new BigDecimal(Integer.MAX_VALUE)) == 1);
        testRS.close();
        testStmtTmp.close();

        testRS = testStmt.executeQuery("SELECT @@SESSION.SQL_SELECT_LIMIT");
        assertTrue(testRS.next());
        assertEquals(new BigDecimal(5), testRS.getBigDecimal(1));
        testRS.close();

        testStmt.close();
        testConn.close();
    }

    /**
     * Initializes and returns a Statement with maxRows defined. Tests the SQL_SELECT_LIMIT defined. Executing this
     * query also forces this limit to be defined at session level.
     */
    private Statement testBug71396StatementInit(Connection testConn, int maxRows) throws SQLException {
        ResultSet testRS;
        Statement testStmt = testConn.createStatement();

        testStmt.setMaxRows(maxRows);
        // while consulting SQL_SELECT_LIMIT setting also forces limit to be applied into current session
        testRS = testStmt.executeQuery("SELECT @@SESSION.SQL_SELECT_LIMIT");
        testRS.next();
        assertEquals("Wrong @@SESSION.SQL_SELECT_LIMIT", maxRows, testRS.getInt(1));

        return testStmt;
    }

    /**
     * Executes a set of queries using a Statement (newly created) and tests if the results count is the expected.
     */
    private void testBug71396StatementMultiCheck(Connection testConn, String[] queries, int[] expRowCount) throws SQLException {
        if (queries.length != expRowCount.length) {
            fail("Bad arguments!");
        }
        Statement testStmt = testConn.createStatement();
        testBug71396StatementMultiCheck(testStmt, queries, expRowCount);
        testStmt.close();
    }

    /**
     * Executes a set of queries using a Statement and tests if the results count is the expected.
     */
    private void testBug71396StatementMultiCheck(Statement testStmt, String[] queries, int[] expRowCount) throws SQLException {
        if (queries.length != expRowCount.length) {
            fail("Bad arguments!");
        }
        for (int i = 0; i < queries.length; i++) {
            testBug71396StatementCheck(testStmt, queries[i], expRowCount[i]);
        }
    }

    /**
     * Executes one query using a Statement and tests if the results count is the expected.
     */
    private void testBug71396StatementCheck(Statement testStmt, String query, int expRowCount) throws SQLException {
        ResultSet testRS;

        testRS = testStmt.executeQuery(query);
        assertTrue(testRS.last());
        assertEquals(String.format("Wrong number of rows for query '%s'", query), expRowCount, testRS.getRow());
        testRS.close();

        testStmt.execute(query);
        testRS = testStmt.getResultSet();
        assertTrue(testRS.last());
        assertEquals(String.format("Wrong number of rows for query '%s'", query), expRowCount, testRS.getRow());
        testRS.close();
    }

    /**
     * Initializes and returns an array of PreparedStatements, with maxRows defined, for a set of queries.
     */
    private PreparedStatement[] testBug71396PrepStatementInit(Connection testConn, String[] queries, int maxRows) throws SQLException {
        PreparedStatement[] testPStmt = new PreparedStatement[queries.length];

        for (int i = 0; i < queries.length; i++) {
            testPStmt[i] = testConn.prepareStatement(queries[i]);
            if (maxRows > 0) {
                testPStmt[i].setMaxRows(maxRows);
            }
        }
        return testPStmt;
    }

    /**
     * Closes all PreparedStatements in the array.
     */
    private void testBug71396PrepStatementClose(PreparedStatement[] testPStmt) throws SQLException {
        for (Statement testStmt : testPStmt) {
            testStmt.close();
        }
    }

    /**
     * Executes a set of queries using newly created PreparedStatements and tests if the results count is the expected.
     */
    private void testBug71396PrepStatementMultiCheck(Connection testConn, String[] queries, int[] expRowCount) throws SQLException {
        if (queries.length != expRowCount.length) {
            fail("Bad arguments!");
        }
        for (int i = 0; i < queries.length; i++) {
            testBug71396PrepStatementCheck(testConn, queries[i], expRowCount[i], -1);
        }
    }

    /**
     * Executes a set of queries using the given PreparedStatements and tests if the results count is the expected.
     */
    private void testBug71396PrepStatementMultiCheck(PreparedStatement[] testPStmt, String[] queries, int[] expRowCount) throws SQLException {
        if (testPStmt.length != queries.length || testPStmt.length != expRowCount.length) {
            fail("Bad arguments!");
        }
        for (int i = 0; i < queries.length; i++) {
            testBug71396PrepStatementCheck(testPStmt[i], queries[i], expRowCount[i]);
        }
    }

    /**
     * Executes one query using a newly created PreparedStatement, setting its maxRows limit, and tests if the results
     * count is the expected.
     */
    private void testBug71396PrepStatementCheck(Connection testConn, String query, int expRowCount, int maxRows) throws SQLException {
        PreparedStatement chkPStmt;

        chkPStmt = testConn.prepareStatement(query);
        if (maxRows > 0) {
            chkPStmt.setMaxRows(maxRows);
        }
        testBug71396PrepStatementCheck(chkPStmt, query, expRowCount);
        chkPStmt.close();
    }

    /**
     * Executes one query using a PreparedStatement and tests if the results count is the expected.
     */
    private void testBug71396PrepStatementCheck(PreparedStatement testPStmt, String query, int expRowCount) throws SQLException {
        ResultSet testRS;

        testRS = testPStmt.executeQuery();
        assertTrue(testRS.last());
        assertEquals(String.format("Wrong number of rows for query '%s'", query), expRowCount, testRS.getRow());
        testRS.close();

        testPStmt.execute();
        testRS = testPStmt.getResultSet();
        assertTrue(testRS.last());
        assertEquals(String.format("Wrong number of rows for query '%s'", query), expRowCount, testRS.getRow());
        testRS.close();
    }

    /**
     * Executes a query containing the clause LIMIT with a Statement and a PreparedStatement, using a combination of
     * Connection properties, maxRows value and limit clause value, and tests if the results count is the expected.
     */
    private void testBug71396MultiSettingsCheck(String connProps, int maxRows, int limitClause, int expRowCount) throws SQLException {
        Connection testConn = getConnectionWithProps(connProps);

        Statement testStmt = testConn.createStatement();
        if (maxRows > 0) {
            testStmt.setMaxRows(maxRows);
        }
        testStmt.executeQuery("SELECT 1"); // force limit to be applied into current session

        testBug71396StatementCheck(testStmt, String.format("SELECT * FROM testBug71396 LIMIT %d", limitClause), expRowCount);
        testBug71396PrepStatementCheck(testConn, String.format("SELECT * FROM testBug71396 LIMIT %d", limitClause), expRowCount, maxRows);

        testStmt.close();
        testConn.close();
    }

    /**
     * Tests fix for 18091639 - STRINGINDEXOUTOFBOUNDSEXCEPTION IN PREPAREDSTATEMENT.SETTIMESTAMP WITH 5.6.15
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug18091639() throws SQLException {
        String str = TimeUtil.formatNanos(1, true, false);
        assertEquals("000000001", str);

        str = TimeUtil.formatNanos(1, true, true);
        assertEquals("0", str);

        str = TimeUtil.formatNanos(1999, true, false);
        assertEquals("000001999", str);

        str = TimeUtil.formatNanos(1999, true, true);
        assertEquals("000001", str);

        str = TimeUtil.formatNanos(1000000010, true, false);
        assertEquals("00000001", str);

        str = TimeUtil.formatNanos(1000000010, true, true);
        assertEquals("0", str);
    }

    /**
     * Tests fix for Bug#66947 (16004987) - Calling ServerPreparedStatement.close() twiche corrupts cached statements
     * 
     * @throws Exception
     */
    public void testBug66947() throws Exception {

        Connection con = null;
        try {
            Properties props = new Properties();
            props.setProperty("useServerPrepStmts", "true");
            props.setProperty("cachePrepStmts", "true");
            props.setProperty("prepStmtCacheSize", "2");

            con = getConnectionWithProps(props);

            PreparedStatement ps1_1;
            PreparedStatement ps1_2;

            String query = "Select 'a' from dual";

            ps1_1 = con.prepareStatement(query);
            ps1_1.executeQuery();
            ps1_1.close();

            ps1_2 = con.prepareStatement(query);
            assertSame("SSPS should be taken from cache but is not the same.", ps1_1, ps1_2);
            ps1_2.executeQuery();
            ps1_2.close();
            ps1_2.close();

            ps1_1 = con.prepareStatement(query);
            assertNotSame("SSPS should not be taken from cache but is the same.", ps1_2, ps1_1);
            ps1_1.executeQuery();
            ps1_1.close();
            ps1_1.close();

            // check that removeEldestEntry doesn't remove elements twice
            PreparedStatement ps2_1;
            PreparedStatement ps2_2;
            PreparedStatement ps3_1;
            PreparedStatement ps3_2;

            ps1_1 = con.prepareStatement("Select 'b' from dual");
            ps1_1.executeQuery();
            ps1_1.close();
            ps2_1 = con.prepareStatement("Select 'c' from dual");
            ps2_1.executeQuery();
            ps2_1.close();
            ps3_1 = con.prepareStatement("Select 'd' from dual");
            ps3_1.executeQuery();
            ps3_1.close();

            ps1_2 = con.prepareStatement("Select 'b' from dual");
            assertNotSame("SSPS should not be taken from cache but is the same.", ps1_1, ps1_2);

            ps2_2 = con.prepareStatement("Select 'c' from dual");
            assertSame("SSPS should be taken from cache but is not the same.", ps2_1, ps2_2);

            ps3_2 = con.prepareStatement("Select 'd' from dual");
            assertSame("SSPS should be taken from cache but is not the same.", ps3_1, ps3_2);

        } finally {
            if (con != null) {
                con.close();
            }
        }

    }

    /**
     * Tests fix for Bug#71672 - Every SQL statement is checked if it contains "ON DUPLICATE KEY UPDATE" or not
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug71672() throws SQLException {
        boolean lastTest = false;
        int testStep = 0;

        Connection testConn = null;
        Statement testStmt = null;
        PreparedStatement testPStmt = null;
        ResultSet testRS = null;
        int[] res = null;

        int[] expectedUpdCount = null;
        int[][] expectedGenKeys = null;
        int[][] expectedGenKeysBatchStmt = null;
        int[] expectedGenKeysMultiQueries = null;
        int[] expectedUpdCountBatchPStmt = null;
        int[] expectedGenKeysBatchPStmt = null;

        final String tableDDL = "(id INT AUTO_INCREMENT PRIMARY KEY, ch CHAR(1) UNIQUE KEY, ct INT)";

        // WARNING: MySQL 5.1 behaves differently in the way the affected rows and generated keys (from AUTO_INCREMENT
        // columns) are computed. Specific expected date set must be accounted.

        // *** CONTROL DATA SET 1: queries for both Statement and PreparedStatement
        final String[] queries = new String[] { "INSERT INTO testBug71672 (ch, ct) VALUES ('A', 100), ('C', 100), ('D', 100)",
                "INSERT INTO testBug71672 (ch, ct) VALUES ('B', 2), ('C', 3), ('D', 4), ('E', 5) ON DUPLICATE KEY UPDATE ct = -1 * (ABS(ct) + VALUES(ct))",
                "INSERT INTO testBug71672 (ch, ct) VALUES ('F', 100) ON DUPLICATE KEY UPDATE ct = -1 * (ABS(ct) + VALUES(ct))",
                "INSERT INTO testBug71672 (ch, ct) VALUES ('B', 2), ('F', 6) ON DUPLICATE KEY UPDATE ct = -1 * (ABS(ct) + VALUES(ct))",
                "INSERT INTO testBug71672 (ch, ct) VALUES ('G', 100)" }; // rewriteBatchedStatements needs > 4 queries

        // expected update counts per query:
        final int[] expectedUpdCountDef = new int[] { 3, 6, 1, 4, 1 };
        // expected generated keys per query:
        final int[][] expectedGenKeysForChkODKU = new int[][] { { 1, 2, 3 }, { 4 }, { 8 }, { 8 }, { 11 } };
        final int[][] expectedGenKeysForNoChkODKU = new int[][] { { 1, 2, 3 }, { 4, 5, 6, 7, 8, 9 }, { 8 }, { 8, 9, 10, 11 }, { 11 } };
        final int[][] expectedGenKeysForBatchStmtRW = new int[][] { { 1 }, { 4 }, { 8 }, { 8 }, { 11 } };

        // expected update counts per query (MySQL 5.1):
        final int[] expectedUpdCountDef51 = new int[] { 3, 8, 1, 6, 1 };
        // expected generated keys per query (MySQL 5.1):
        final int[][] expectedGenKeysForChkODKU51 = new int[][] { { 1, 2, 3 }, { 4 }, { 6 }, { 6 }, { 7 } };
        final int[][] expectedGenKeysForNoChkODKU51 = new int[][] { { 1, 2, 3 }, { 4, 5, 6, 7, 8, 9, 10, 11 }, { 6 }, { 6, 7, 8, 9, 10, 11 }, { 7 } };
        final int[][] expectedGenKeysForBatchStmtRW51 = new int[][] { { 1 }, { 4 }, { 6 }, { 6 }, { 7 } };

        // *** CONTROL DATA SET 2: query and params for batch PrepatedStatement
        final String queryBatchPStmt = "INSERT INTO testBug71672 (ch, ct) VALUES (?, ?) ON DUPLICATE KEY UPDATE ct = -1 * (ABS(ct) + VALUES(ct))";
        final String[] paramsBatchPStmt = new String[] { "A100", "C100", "D100", "B2", "C3", "D4", "E5", "F100", "B2", "F6", "G100" };

        // expected update counts per param:
        final int[] expectedUpdCountBatchPStmtNoRW = new int[] { 1, 1, 1, 1, 2, 2, 1, 1, 2, 2, 1 };
        final int sni = Statement.SUCCESS_NO_INFO;
        final int[] expectedUpdCountBatchPStmtRW = new int[] { sni, sni, sni, sni, sni, sni, sni, sni, sni, sni, sni };
        // expected generated keys:
        final int[] expectedGenKeysForBatchPStmtChkODKU = new int[] { 1, 2, 3, 4, 2, 3, 7, 8, 4, 8, 11 };
        final int[] expectedGenKeysForBatchPStmtNoChkODKU = new int[] { 1, 2, 3, 4, 2, 3, 3, 4, 7, 8, 4, 5, 8, 9, 11 };
        final int[] expectedGenKeysForBatchPStmtRW = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };

        // expected update counts per param:
        final int[] expectedUpdCountBatchPStmtNoRW51 = new int[] { 1, 1, 1, 1, 3, 3, 1, 1, 3, 3, 1 };
        // expected generated keys:
        final int[] expectedGenKeysForBatchPStmtChkODKU51 = new int[] { 1, 2, 3, 4, 2, 3, 5, 6, 4, 6, 7 };
        final int[] expectedGenKeysForBatchPStmtNoChkODKU51 = new int[] { 1, 2, 3, 4, 2, 3, 4, 3, 4, 5, 5, 6, 4, 5, 6, 6, 7, 8, 7 };
        final int[] expectedGenKeysForBatchPStmtRW51 = new int[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 };

        // Test multiple connection props
        do {
            switch (++testStep) {
                case 1:
                    testConn = getConnectionWithProps("");
                    expectedUpdCount = versionMeetsMinimum(5, 5) ? expectedUpdCountDef : expectedUpdCountDef51;
                    expectedGenKeys = versionMeetsMinimum(5, 5) ? expectedGenKeysForChkODKU : expectedGenKeysForChkODKU51;
                    expectedGenKeysBatchStmt = expectedGenKeys;
                    expectedUpdCountBatchPStmt = versionMeetsMinimum(5, 5) ? expectedUpdCountBatchPStmtNoRW : expectedUpdCountBatchPStmtNoRW51;
                    expectedGenKeysBatchPStmt = versionMeetsMinimum(5, 5) ? expectedGenKeysForBatchPStmtChkODKU : expectedGenKeysForBatchPStmtChkODKU51;
                    break;
                case 2:
                    testConn = getConnectionWithProps("dontCheckOnDuplicateKeyUpdateInSQL=true");
                    expectedUpdCount = versionMeetsMinimum(5, 5) ? expectedUpdCountDef : expectedUpdCountDef51;
                    expectedGenKeys = versionMeetsMinimum(5, 5) ? expectedGenKeysForNoChkODKU : expectedGenKeysForNoChkODKU51;
                    expectedGenKeysBatchStmt = expectedGenKeys;
                    expectedUpdCountBatchPStmt = versionMeetsMinimum(5, 5) ? expectedUpdCountBatchPStmtNoRW : expectedUpdCountBatchPStmtNoRW51;
                    expectedGenKeysBatchPStmt = versionMeetsMinimum(5, 5) ? expectedGenKeysForBatchPStmtNoChkODKU : expectedGenKeysForBatchPStmtNoChkODKU51;
                    break;
                case 3:
                    testConn = getConnectionWithProps("rewriteBatchedStatements=true");
                    expectedUpdCount = versionMeetsMinimum(5, 5) ? expectedUpdCountDef : expectedUpdCountDef51;
                    expectedGenKeys = versionMeetsMinimum(5, 5) ? expectedGenKeysForChkODKU : expectedGenKeysForChkODKU51;
                    expectedGenKeysBatchStmt = versionMeetsMinimum(5, 5) ? expectedGenKeysForBatchStmtRW : expectedGenKeysForBatchStmtRW51;
                    expectedUpdCountBatchPStmt = expectedUpdCountBatchPStmtRW;
                    expectedGenKeysBatchPStmt = versionMeetsMinimum(5, 5) ? expectedGenKeysForBatchPStmtRW : expectedGenKeysForBatchPStmtRW51;
                    break;
                case 4:
                    // dontCheckOnDuplicateKeyUpdateInSQL=true is canceled by rewriteBatchedStatements=true
                    testConn = getConnectionWithProps("rewriteBatchedStatements=true,dontCheckOnDuplicateKeyUpdateInSQL=true");
                    expectedUpdCount = versionMeetsMinimum(5, 5) ? expectedUpdCountDef : expectedUpdCountDef51;
                    expectedGenKeys = versionMeetsMinimum(5, 5) ? expectedGenKeysForChkODKU : expectedGenKeysForChkODKU51;
                    expectedGenKeysBatchStmt = versionMeetsMinimum(5, 5) ? expectedGenKeysForBatchStmtRW : expectedGenKeysForBatchStmtRW51;
                    expectedUpdCountBatchPStmt = expectedUpdCountBatchPStmtRW;
                    expectedGenKeysBatchPStmt = versionMeetsMinimum(5, 5) ? expectedGenKeysForBatchPStmtRW : expectedGenKeysForBatchPStmtRW51;
                    lastTest = true;
                    break;
            }

            // A. Test Statement.execute() results
            createTable("testBug71672", tableDDL);
            for (int i = 0; i < queries.length; i++) {
                testBug71672Statement(testStep, testConn, queries[i], -1, expectedGenKeys[i]);
            }
            dropTable("testBug71672");

            // B. Test Statement.executeUpdate() results
            createTable("testBug71672", tableDDL);
            for (int i = 0; i < queries.length; i++) {
                testBug71672Statement(testStep, testConn, queries[i], expectedUpdCount[i], expectedGenKeys[i]);
            }
            dropTable("testBug71672");

            // C. Test Statement.executeBatch() results
            createTable("testBug71672", tableDDL);
            testStmt = testConn.createStatement();
            for (String query : queries) {
                testStmt.addBatch(query);
            }
            res = testStmt.executeBatch();
            assertEquals(testStep + ". Satement.executeBatch() result", expectedUpdCount.length, res.length);
            for (int i = 0; i < expectedUpdCount.length; i++) {
                assertEquals(testStep + "." + i + ". Satement.executeBatch() result", expectedUpdCount[i], res[i]);
            }
            testRS = testStmt.getGeneratedKeys();
            for (int i = 0; i < expectedGenKeysBatchStmt.length; i++) {
                for (int j = 0; j < expectedGenKeysBatchStmt[i].length; j++) {
                    assertTrue(testStep + ". Row expected in generated keys ResultSet", testRS.next());
                    assertEquals(testStep + ".[" + i + "][" + j + "]. Wrong generated key", expectedGenKeysBatchStmt[i][j], testRS.getInt(1));
                }
            }
            assertFalse(testStep + ". No more rows expected in generated keys ResultSet", testRS.next());
            testRS.close();
            testStmt.close();
            dropTable("testBug71672");

            // D. Test PreparedStatement.execute() results
            createTable("testBug71672", tableDDL);
            for (int i = 0; i < queries.length; i++) {
                testBug71672PreparedStatement(testStep, testConn, queries[i], -1, expectedGenKeys[i]);
            }
            dropTable("testBug71672");

            // E. Test PreparedStatement.executeUpdate() results
            createTable("testBug71672", tableDDL);
            for (int i = 0; i < queries.length; i++) {
                testBug71672PreparedStatement(testStep, testConn, queries[i], expectedUpdCount[i], expectedGenKeys[i]);
            }
            dropTable("testBug71672");

            // F. Test PreparedStatement.executeBatch() results
            createTable("testBug71672", tableDDL);
            testPStmt = testConn.prepareStatement(queryBatchPStmt, Statement.RETURN_GENERATED_KEYS);
            for (String param : paramsBatchPStmt) {
                testPStmt.setString(1, param.substring(0, 1));
                testPStmt.setInt(2, Integer.parseInt(param.substring(1)));
                testPStmt.addBatch();
            }
            res = testPStmt.executeBatch();
            assertEquals(testStep + ". PreparedSatement.executeBatch() result", expectedUpdCountBatchPStmt.length, res.length);
            for (int i = 0; i < expectedUpdCountBatchPStmt.length; i++) {
                assertEquals(testStep + "." + i + ". PreparedSatement.executeBatch() result", expectedUpdCountBatchPStmt[i], res[i]);
            }
            testRS = testPStmt.getGeneratedKeys();
            for (int i = 0; i < expectedGenKeysBatchPStmt.length; i++) {
                assertTrue(testStep + ". Row expected in generated keys ResultSet", testRS.next());
                assertEquals(testStep + ".[" + i + "]. Wrong generated key", expectedGenKeysBatchPStmt[i], testRS.getInt(1));
            }
            assertFalse(testStep + ". No more rows expected in generated keys ResultSet", testRS.next());
            testRS.close();
            testPStmt.close();
            dropTable("testBug71672");

            testConn.close();
        } while (!lastTest);

        // Test connection prop allowMultiQueries=true
        // (behaves as if only first query has been executed)
        lastTest = false;
        String allQueries = "";
        for (String q : queries) {
            allQueries += q + ";";
        }
        do {
            switch (++testStep) {
                case 5:
                    testConn = getConnectionWithProps("allowMultiQueries=true");
                    expectedGenKeysMultiQueries = new int[] { 1 };
                    break;
                case 6:
                    testConn = getConnectionWithProps("allowMultiQueries=true,dontCheckOnDuplicateKeyUpdateInSQL=true");
                    expectedGenKeysMultiQueries = new int[] { 1, 2, 3 };
                    lastTest = true;
                    break;
            }

            // A. Test Statement.execute() results
            createTable("testBug71672", tableDDL);
            testBug71672Statement(testStep, testConn, allQueries, -1, expectedGenKeysMultiQueries);
            dropTable("testBug71672");

            // B. Test Statement.executeUpdate() results
            createTable("testBug71672", tableDDL);
            testBug71672Statement(testStep, testConn, allQueries, 3, expectedGenKeysMultiQueries);
            dropTable("testBug71672");

            // C. Test PreparedStatement.execute() results
            createTable("testBug71672", tableDDL);
            testBug71672PreparedStatement(testStep, testConn, allQueries, -1, expectedGenKeysMultiQueries);
            dropTable("testBug71672");

            // D. Test PreparedStatement.executeUpdate() results
            createTable("testBug71672", tableDDL);
            testBug71672PreparedStatement(testStep, testConn, allQueries, 3, expectedGenKeysMultiQueries);
            dropTable("testBug71672");

            testConn.close();
        } while (!lastTest);
    }

    /**
     * Check the update count and returned keys for an INSERT query using a Statement object. If expectedUpdateCount < 0 then runs Statement.execute() otherwise
     * Statement.executeUpdate().
     */
    public void testBug71672Statement(int testStep, Connection testConn, String query, int expectedUpdateCount, int[] expectedKeys) throws SQLException {
        Statement testStmt = testConn.createStatement();

        if (expectedUpdateCount < 0) {
            assertFalse(testStep + ". Stmt.execute() result", testStmt.execute(query, Statement.RETURN_GENERATED_KEYS));
        } else {
            assertEquals(testStep + ". Stmt.executeUpdate() result", expectedUpdateCount, testStmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS));
        }

        ResultSet testRS = testStmt.getGeneratedKeys();
        for (int k : expectedKeys) {
            assertTrue(testStep + ". Row expected in generated keys ResultSet", testRS.next());
            assertEquals(testStep + ". Wrong generated key", k, testRS.getInt(1));
        }
        assertFalse(testStep + ". No more rows expected in generated keys ResultSet", testRS.next());
        testRS.close();
        testStmt.close();
    }

    /**
     * Check the update count and returned keys for an INSERT query using a PreparedStatement object. If expectedUpdateCount < 0 then runs
     * PreparedStatement.execute() otherwise PreparedStatement.executeUpdate().
     */
    public void testBug71672PreparedStatement(int testStep, Connection testConn, String query, int expectedUpdateCount, int[] expectedKeys) throws SQLException {
        PreparedStatement testPStmt = testConn.prepareStatement(query);

        if (expectedUpdateCount < 0) {
            assertFalse(testStep + ". PrepStmt.execute() result", testPStmt.execute(query, Statement.RETURN_GENERATED_KEYS));
        } else {
            assertEquals(testStep + ". PrepStmt.executeUpdate() result", expectedUpdateCount, testPStmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS));
        }

        ResultSet testRS = testPStmt.getGeneratedKeys();
        for (int k : expectedKeys) {
            assertTrue(testStep + ". Row expected in generated keys ResultSet", testRS.next());
            assertEquals(testStep + ". Wrong generated key", k, testRS.getInt(1));
        }
        assertFalse(testStep + ". No more rows expected in generated keys ResultSet", testRS.next());
        testRS.close();
        testPStmt.close();
    }

    /**
     * Tests fix for BUG#71923 - Incorrect generated keys if ON DUPLICATE KEY UPDATE not exact
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug71923() throws Exception {
        final String tableDDL = "(id INT AUTO_INCREMENT PRIMARY KEY, ch CHAR(1) UNIQUE KEY, ct INT, dt VARCHAR(100))";
        final String defaultQuery = "Insert into testBug71923 (ch, ct) values ('A', 1), ('B', 2)";
        final String[] testQueriesPositiveMatches = new String[] {
                "INSERT INTO testBug71923 (ch, ct) VALUES ('B', 2), ('C', 3) ON DUPLICATE KEY UPDATE ct = ABS(ct) + VALUES(ct)",
                "INSERT INTO testBug71923 (ch, ct) VALUES ('B', 2), ('C', 3) ON  DUPLICATE  KEY  UPDATE ct = ABS(ct) + VALUES(ct)",
                "INSERT INTO testBug71923 (ch, ct) VALUES ('B', 2), ('C', 3) /*! ON   DUPLICATE */ KEY /*!UPDATE*/ ct = ABS(ct) + VALUES(ct)",
                "INSERT INTO testBug71923 (ch, ct) VALUES ('B', 2), ('C', 3) ON/* ON */DUPLICATE /* DUPLICATE */KEY/* KEY *//* KEY */ UPDATE /* UPDATE */ ct = ABS(ct) + VALUES(ct)",
                "INSERT INTO testBug71923 (ch, ct) VALUES ('B', 2), ('C', 3) ON -- new line\n DUPLICATE KEY UPDATE ct = ABS(ct) + VALUES(ct)",
                "INSERT INTO testBug71923 (ch, ct) VALUES ('B', 2), ('C', 3) ON DUPLICATE # new line\n KEY UPDATE ct = ABS(ct) + VALUES(ct)",
                "INSERT INTO testBug71923 (ch, ct) VALUES ('B', 2), ('C', 3) ON/* comment */DUPLICATE# new line\nKEY-- new line\nUPDATE ct = ABS(ct) + VALUES(ct)" };
        final String[] testQueriesNegativeMatches = new String[] {
                "INSERT INTO testBug71923 (ch, ct, dt) VALUES ('C', 3, NULL), ('D', 4, NULL) /* ON DUPLICATE KEY UPDATE */",
                "INSERT INTO testBug71923 (ch, ct, dt) VALUES ('C', 3, NULL), ('D', 4, NULL) -- ON DUPLICATE KEY UPDATE",
                "INSERT INTO testBug71923 (ch, ct, dt) VALUES ('C', 3, NULL), ('D', 4, NULL) # ON DUPLICATE KEY UPDATE",
                "INSERT INTO testBug71923 (ch, ct, dt) VALUES ('C', 3, NULL), ('D', 4, 'ON DUPLICATE KEY UPDATE')" };

        int c = 0;
        for (String query : testQueriesPositiveMatches) {
            c++;

            // A. test Statement.execute()
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertFalse(this.stmt.execute(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.stmt.getGeneratedKeys();
            assertTrue(c + ".A Statement.execute() - generated keys row expected", this.rs.next());
            assertEquals(c + ".A Statement.execute() - wrong generated key value", 3, this.rs.getInt(1));
            assertFalse(c + ".A Statement.execute() - no more generated keys rows expected", this.rs.next());
            this.rs.close();

            dropTable("testBug71923");

            // B. test Statement.executeUpdate
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertEquals(versionMeetsMinimum(5, 5) ? 3 : 4, this.stmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.stmt.getGeneratedKeys();
            assertTrue(c + ".B Statement.executeUpdate() - generated keys row expected", this.rs.next());
            assertEquals(c + ".B Statement.executeUpdate() - wrong generated key value", 3, this.rs.getInt(1));
            assertFalse(c + ".B Statement.executeUpdate() - no more generated keys rows expected", this.rs.next());
            this.rs.close();

            // prepare statement for next tet cases
            this.pstmt = this.conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);

            // C. test PreparedStatment.execute()
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertFalse(this.pstmt.execute(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.pstmt.getGeneratedKeys();
            assertTrue(c + ".C PreparedStatment.execute() - generated keys row expected", this.rs.next());
            assertEquals(c + ".C PreparedStatment.execute() - wrong generated key value", 3, this.rs.getInt(1));
            assertFalse(c + ".C PreparedStatment.execute() - no more generated keys rows expected", this.rs.next());
            this.rs.close();

            dropTable("testBug71923");

            // D. test PreparedStatment.executeUpdate
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertEquals(versionMeetsMinimum(5, 5) ? 3 : 4, this.pstmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.pstmt.getGeneratedKeys();
            assertTrue(c + ".D PreparedStatment.executeUpdate() - generated keys row expected", this.rs.next());
            assertEquals(c + ".D PreparedStatment.executeUpdate() - wrong generated key value", 3, this.rs.getInt(1));
            assertFalse(c + ".D PreparedStatment.executeUpdate() - no more generated keys rows expected", this.rs.next());
            this.rs.close();

            dropTable("testBug71923");
        }

        c = 0;
        for (String query : testQueriesNegativeMatches) {
            c++;

            // E. test Statement.execute()
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertFalse(this.stmt.execute(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.stmt.getGeneratedKeys();
            assertTrue(c + ".E Statement.execute() - generated keys 1st row expected", this.rs.next());
            assertEquals(c + ".E Statement.execute() - wrong 1st generated key value", 3, this.rs.getInt(1));
            assertTrue(c + ".E Statement.execute() - generated keys 2nd row expected", this.rs.next());
            assertEquals(c + ".E Statement.execute() - wrong 2nd generated key value", 4, this.rs.getInt(1));
            assertFalse(c + ".E Statement.execute() - no more generated keys rows expected", this.rs.next());
            this.rs.close();

            dropTable("testBug71923");

            // F. test Statement.executeUpdate
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertEquals(2, this.stmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.stmt.getGeneratedKeys();
            assertTrue(c + ".F Statement.execute() - generated keys 1st row expected", this.rs.next());
            assertEquals(c + ".F Statement.execute() - wrong 1st generated key value", 3, this.rs.getInt(1));
            assertTrue(c + ".F Statement.execute() - generated keys 2nd row expected", this.rs.next());
            assertEquals(c + ".F Statement.execute() - wrong 2nd generated key value", 4, this.rs.getInt(1));
            assertFalse(c + ".F Statement.execute() - no more generated keys rows expected", this.rs.next());
            this.rs.close();

            // prepare statement for next tet cases
            this.pstmt = this.conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);

            // G. test PreparedStatment.execute()
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertFalse(this.pstmt.execute(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.pstmt.getGeneratedKeys();
            assertTrue(c + ".G PreparedStatment.execute() - generated keys 1st row expected", this.rs.next());
            assertEquals(c + ".G PreparedStatment.execute() - wrong 1st generated key value", 3, this.rs.getInt(1));
            assertTrue(c + ".G PreparedStatment.execute() - generated keys 2nd row expected", this.rs.next());
            assertEquals(c + ".G PreparedStatment.execute() - wrong 2nd generated key value", 4, this.rs.getInt(1));
            assertFalse(c + ".G PreparedStatment.execute() - no more generated keys rows expected", this.rs.next());
            this.rs.close();

            dropTable("testBug71923");

            // H. test PreparedStatment.executeUpdate
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertEquals(2, this.pstmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.pstmt.getGeneratedKeys();
            assertTrue(c + ".H PreparedStatment.executeUpdate() - generated keys 1st row expected", this.rs.next());
            assertEquals(c + ".H PreparedStatment.executeUpdate() - wrong 1st generated key value", 3, this.rs.getInt(1));
            assertTrue(c + ".H PreparedStatment.executeUpdate() - generated keys 2nd row expected", this.rs.next());
            assertEquals(c + ".H PreparedStatment.executeUpdate() - wrong 2nd generated key value", 4, this.rs.getInt(1));
            assertFalse(c + ".H PreparedStatment.executeUpdate() - no more generated keys rows expected", this.rs.next());
            this.rs.close();

            dropTable("testBug71923");
        }
    }

    /**
     * Tests fix for BUG#73163 - IndexOutOfBoundsException thrown preparing statement.
     * 
     * This bug occurs only if running with Java6+. Duplicated in testsuite.regression.StatementRegressionTest.jdbc4.testBug73163().
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug73163() throws Exception {
        try {
            this.stmt = this.conn.prepareStatement("LOAD DATA INFILE ? INTO TABLE testBug73163");
        } catch (SQLException e) {
            if (e.getCause() instanceof IndexOutOfBoundsException && Util.isJdbc4()) {
                fail("IOOBE thrown in Java6+ while preparing a LOAD DATA statement with placeholders.");
            } else {
                throw e;
            }
        }
    }
}
