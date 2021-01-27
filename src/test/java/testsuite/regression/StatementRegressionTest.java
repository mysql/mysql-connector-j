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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import java.lang.reflect.Field;
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
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import javax.sql.XAConnection;

import org.junit.jupiter.api.Test;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.ClientPreparedQuery;
import com.mysql.cj.MysqlConnection;
import com.mysql.cj.Query;
import com.mysql.cj.ServerPreparedQuery;
import com.mysql.cj.Session;
import com.mysql.cj.conf.PropertyDefinitions.DatabaseTerm;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.ClientPreparedStatement;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPreparedStatement;
import com.mysql.cj.jdbc.JdbcStatement;
import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;
import com.mysql.cj.jdbc.MysqlXADataSource;
import com.mysql.cj.jdbc.ParameterBindings;
import com.mysql.cj.jdbc.ServerPreparedStatement;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.cj.jdbc.ha.ReplicationConnection;
import com.mysql.cj.jdbc.interceptors.ResultSetScannerInterceptor;
import com.mysql.cj.jdbc.result.CachedResultSetMetaData;
import com.mysql.cj.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ResultsetRows;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.util.LRUCache;
import com.mysql.cj.util.TimeUtil;

import testsuite.BaseQueryInterceptor;
import testsuite.BaseTestCase;
import testsuite.UnreliableSocketFactory;

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
     * Each row in this table is to be converted into a single REPLACE statement. If the value is zero, a new record is to be created using then autoincrement
     * feature. If the value is non-zero, the existing row of that value is to be replace with, obviously, the same key. I expect one Generated Key for each
     * zero value - but I would accept one key for each value, with non-zero values coming back as themselves.
     */
    static final int[][] tests = { { 0 }, // generate 1
            { 1, 0, 0 }, // update 1, generate 2, 3
            { 2, 0, 0, }, // update 2, generate 3, 4
    };

    protected int testServerPrepStmtDeadlockCounter = 0;

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
        StringBuilder cmd = new StringBuilder("REPLACE INTO testggk VALUES ");
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
        StringBuilder res = new StringBuilder("Got keys");

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

        assertTrue(numberOfGeneratedKeys == newKeys,
                "Didn't retrieve expected number of generated keys, expected " + newKeys + ", found " + numberOfGeneratedKeys);
        assertTrue(generatedKeys[0] == nextID, "Keys didn't start with correct sequence: ");

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
        StringBuilder cmd = new StringBuilder("REPLACE INTO testggk VALUES ");
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
        StringBuilder res = new StringBuilder("Got keys");

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

        assertTrue(numberOfGeneratedKeys == newKeys,
                "Didn't retrieve expected number of generated keys, expected " + newKeys + ", found " + numberOfGeneratedKeys);
        assertTrue(generatedKeys[0] == nextID, "Keys didn't start with correct sequence: ");

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
        StringBuilder buffer = new StringBuilder();
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
        continueBatchOnErrorProps.setProperty(PropertyKey.continueBatchOnError.getKeyName(), String.valueOf(continueBatchOnError));
        this.conn = getConnectionWithProps(continueBatchOnErrorProps);
        Statement statement = this.conn.createStatement();

        String tableName = "testBug6823";

        createTable(tableName,
                "(id int not null primary key auto_increment, strdata1 varchar(255) not null, strdata2 varchar(255), UNIQUE INDEX (strdata1(100)))");

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

        assertTrue(psRows == sRows, psRows + "!=" + sRows);
    }

    /**
     * Tests fix for BUG#10155, double quotes not recognized when parsing client-side prepared statements.
     * 
     * @throws Exception
     *             if the test fails.
     */
    @Test
    public void testBug10155() throws Exception {
        this.conn.prepareStatement("SELECT \"Test question mark? Test single quote'\"").executeQuery().close();
    }

    /**
     * Tests fix for BUG#10630, Statement.getWarnings() fails with NPE if statement has been closed.
     * 
     * @throws Exception
     */
    @Test
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
            assertEquals(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());
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
     * Tests fix for BUG#11115, Varbinary data corrupted when using server-side prepared statements.
     * 
     * @throws Exception
     */
    @Test
    public void testBug11115() throws Exception {
        String tableName = "testBug11115";

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

    @Test
    public void testBug11540() throws Exception {
        Locale originalLocale = Locale.getDefault();
        Connection thaiConn = null;
        Statement thaiStmt = null;
        PreparedStatement thaiPrepStmt = null;

        try {
            createTable("testBug11540", "(field1 DATE, field2 DATETIME)");
            this.stmt.executeUpdate("INSERT INTO testBug11540 VALUES (NOW(), NOW())");
            Locale.setDefault(new Locale("th", "TH"));
            Properties props = new Properties();
            props.setProperty(PropertyKey.jdbcCompliantTruncation.getKeyName(), "false");
            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "LOCAL");

            thaiConn = getConnectionWithProps(props);
            thaiStmt = thaiConn.createStatement();

            this.rs = thaiStmt.executeQuery("SELECT field1, field2 FROM testBug11540");
            this.rs.next();

            Date origDate = this.rs.getDate(1);
            Timestamp origTimestamp = this.rs.getTimestamp(2);
            this.rs.close();

            thaiStmt.executeUpdate("TRUNCATE TABLE testBug11540");

            thaiPrepStmt = ((com.mysql.cj.jdbc.JdbcConnection) thaiConn).clientPrepareStatement("INSERT INTO testBug11540 VALUES (?,?)");
            thaiPrepStmt.setDate(1, origDate);
            thaiPrepStmt.setTimestamp(2, origTimestamp);
            thaiPrepStmt.executeUpdate();

            this.rs = thaiStmt.executeQuery("SELECT field1, field2 FROM testBug11540");
            this.rs.next();

            Date testDate = this.rs.getDate(1);
            Timestamp testTimestamp = this.rs.getTimestamp(2);
            this.rs.close();

            props = new Properties();
            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "LOCAL");
            Connection testConn = getConnectionWithProps(props);
            TimeZone serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
            Timestamp expTs = Timestamp.from(origTimestamp.toInstant().atZone(ZoneId.systemDefault()).withZoneSameInstant(serverTz.toZoneId())
                    .withZoneSameLocal(ZoneId.systemDefault()).toInstant());
            assertEquals(origDate, testDate);
            assertEquals(expTs, testTimestamp);

        } finally {
            Locale.setDefault(originalLocale);
        }
    }

    /**
     * Tests fix for BUG#11663, autoGenerateTestcaseScript uses bogus parameter names for server-side prepared statements.
     * 
     * @throws Exception
     */
    @Test
    public void testBug11663() throws Exception {
        if (((com.mysql.cj.jdbc.JdbcConnection) this.conn).getPropertySet().getBooleanProperty(PropertyKey.useServerPrepStmts).getValue()) {
            Connection testcaseGenCon = null;
            PrintStream oldErr = System.err;

            try {
                createTable("testBug11663", "(field1 int)");

                Properties props = new Properties();
                props.setProperty(PropertyKey.autoGenerateTestcaseScript.getKeyName(), "true");
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
     * Tests fix for BUG#11798 - Pstmt.setObject(...., Types.BOOLEAN) throws exception.
     * 
     * @throws Exception
     *             if the test fails.
     */
    @Test
    public void testBug11798() throws Exception {
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
     */
    @Test
    public void testBug13255() throws Exception {
        createTable("testBug13255", "(field_1 int)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");

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
     */
    @Test
    public void testBug15024() throws Exception {
        createTable("testBug15024", "(field1 BLOB)");

        try {
            this.pstmt = this.conn.prepareStatement("INSERT INTO testBug15024 VALUES (?)");
            testStreamsForBug15024(false, false);

            Properties props = new Properties();
            props.setProperty(PropertyKey.useConfigs.getKeyName(), "3-0-Compat");

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
     */
    @Test
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

            this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement("select {d '1997-05-24'} FROM testBug15141");
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
     */
    @Test
    public void testBug18041() throws Exception {
        createTable("testBug18041", "(`a` tinyint(4) NOT NULL, `b` char(4) default NULL)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.jdbcCompliantTruncation.getKeyName(), "true");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");

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
     */
    @Test
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
     */
    @Test
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
     */
    @Test
    public void testBug1933() throws Exception {
        Connection maxRowsConn = null;
        PreparedStatement maxRowsPrepStmt = null;
        Statement maxRowsStmt = null;

        try {
            Properties props = new Properties();

            props.setProperty(PropertyKey.maxRows.getKeyName(), "1");

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

            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false");

            maxRowsConn.close();
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

    /**
     * Tests the fix for BUG#1934 -- prepareStatement dies silently when
     * encountering Statement.RETURN_GENERATED_KEY
     * 
     * @throws Exception
     */
    @Test
    public void testBug1934() throws Exception {
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
     */
    @Test
    public void testBug1958() throws Exception {
        PreparedStatement pStmt = null;

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1958");
            this.stmt.executeUpdate("CREATE TABLE testBug1958 (field1 int)");

            pStmt = this.conn.prepareStatement("SELECT * FROM testBug1958 WHERE field1 IN (?, ?, ?)");

            try {
                pStmt.setInt(4, 1);
            } catch (SQLException sqlEx) {
                assertTrue(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
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
     */
    @Test
    public void testBug2606() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug2606");
            this.stmt.executeUpdate("CREATE TABLE testBug2606(year_field YEAR)");
            this.stmt.executeUpdate("INSERT INTO testBug2606 VALUES (2004)");

            PreparedStatement yrPstmt = this.conn.prepareStatement("SELECT year_field FROM testBug2606");

            this.rs = yrPstmt.executeQuery();

            assertTrue(this.rs.next());

            assertEquals("2004-01-01", this.rs.getDate(1).toString());
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug2606");
        }
    }

    /**
     * Tests the fix for BUG#2671, nulls encoded incorrectly in server-side prepared statements.
     * 
     * @throws Exception
     */
    @Test
    public void testBug2671() throws Exception {
        createTable("test3",
                "(`field1` int(8) NOT NULL auto_increment, `field2` int(8) unsigned zerofill default NULL,"
                        + " `field3` varchar(30) binary NOT NULL default '', `field4` varchar(100) default NULL, `field5` datetime NULL default NULL,"
                        + " PRIMARY KEY  (`field1`), UNIQUE KEY `unq_id` (`field2`), UNIQUE KEY  (`field3`)) CHARACTER SET utf8",
                "InnoDB");

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

    /**
     * Tests fix for BUG#3103 -- java.util.Date not accepted as parameter to
     * PreparedStatement.setObject().
     * 
     * @throws Exception
     * 
     * @deprecated uses deprecated methods of Date class
     */
    @Deprecated
    @Test
    public void testBug3103() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3103");

            if (versionMeetsMinimum(5, 6, 4)) {
                this.stmt.executeUpdate("CREATE TABLE testBug3103 (field1 TIMESTAMP(3))");
            } else {
                this.stmt.executeUpdate("CREATE TABLE testBug3103 (field1 TIMESTAMP)");
            }

            PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO testBug3103 VALUES (?)");

            java.util.Date utilDate = new java.util.Date();

            pStmt.setObject(1, utilDate);
            pStmt.executeUpdate();

            this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug3103");
            this.rs.next();

            java.util.Date retrUtilDate = new java.util.Date(this.rs.getTimestamp(1).getTime());

            // We can only compare on the day/month/year hour/minute/second interval, because the timestamp has added milliseconds to the internal date...
            assertTrue((utilDate.getMonth() == retrUtilDate.getMonth()) && (utilDate.getDate() == retrUtilDate.getDate())
                    && (utilDate.getYear() == retrUtilDate.getYear()) && (utilDate.getHours() == retrUtilDate.getHours())
                    && (utilDate.getMinutes() == retrUtilDate.getMinutes()) && (utilDate.getSeconds() == retrUtilDate.getSeconds()), "Dates not equal");
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
    @Test
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
     */
    @Test
    public void testBug3557() throws Exception {
        boolean populateDefaults = ((JdbcConnection) this.conn).getPropertySet().getBooleanProperty(PropertyKey.populateInsertRowWithDefaultValues).getValue();

        try {
            ((JdbcConnection) this.conn).getPropertySet().getBooleanProperty(PropertyKey.populateInsertRowWithDefaultValues).setValue(true);

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3557");

            this.stmt.executeUpdate(
                    "CREATE TABLE testBug3557 (`a` varchar(255) NOT NULL default 'XYZ', `b` varchar(255) default '123', PRIMARY KEY  (`a`(100)))");

            Statement updStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            this.rs = updStmt.executeQuery("SELECT * FROM testBug3557");

            assertTrue(this.rs.getConcurrency() == ResultSet.CONCUR_UPDATABLE);

            this.rs.moveToInsertRow();

            assertEquals("XYZ", this.rs.getObject(1));
            assertEquals("123", this.rs.getObject(2));
        } finally {
            ((JdbcConnection) this.conn).getPropertySet().getBooleanProperty(PropertyKey.populateInsertRowWithDefaultValues).setValue(populateDefaults);

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3557");
        }
    }

    /**
     * Tests fix for BUG#3620 -- Timezone not respected correctly.
     * 
     * @throws SQLException
     */
    @Test
    public void testBug3620() throws SQLException {
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

            Statement tsStmt = this.conn.createStatement();

            tsPstmt = this.conn.prepareStatement("INSERT INTO testBug3620 VALUES (?)");

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

            PreparedStatement tsPstmtRetr = this.conn.prepareStatement("SELECT field1 FROM testBug3620");

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

            assertTrue((deltaOrig < epsillon), "Difference between original timestamp and timestamp retrieved using java.sql.Statement "
                    + "set in database using UTC calendar is not ~= " + epsillon + ", it is actually " + deltaOrig);

            long pStmtDeltaTWithCal = (ts.getTime() - tsValuePstmtNoCal.getTime());

            System.out.println(
                    Math.abs(pStmtDeltaTWithCal - pointInTimeOffset) + " < " + epsillon + (Math.abs(pStmtDeltaTWithCal - pointInTimeOffset) < epsillon));
            assertTrue((Math.abs(pStmtDeltaTWithCal - pointInTimeOffset) < epsillon),
                    "Difference between original timestamp and timestamp retrieved using java.sql.PreparedStatement "
                            + "set in database using UTC calendar is not ~= " + epsillon + ", it is actually " + pStmtDeltaTWithCal);

            System.out.println("Difference between original ts and ts with no calendar: " + (ts.getTime() - tsValuePstmtNoCal.getTime()) + ", offset should be "
                    + pointInTimeOffset);
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3620");
        }
    }

    /**
     * Tests fix for BUG#3620 -- Timezone not respected correctly.
     * 
     * @throws SQLException
     *
     */
    @Test
    public void testBug3620new() throws SQLException {
        // TODO: should replace testBug3620()
        if (this.DISABLED_testBug3620new) {
            // TODO: this test is working in c/J 5.1 but fails here; disable for later analysis
            return;
        }

        final long epsillon = 3000; // allow 3 seconds time difference

        TimeZone defaultTimeZone = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("America/Chicago"));

            createTable("testBug3620", "(field1 TIMESTAMP) ENGINE=InnoDB");

            Properties props = new Properties();
            props.put(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");

            Connection connNoTz = getConnectionWithProps(props);
            PreparedStatement tsPstmt = connNoTz.prepareStatement("INSERT INTO testBug3620 VALUES (?)");

            Calendar pointInTime = Calendar.getInstance();
            pointInTime.set(2004, 02, 29, 10, 0, 0);
            long pointInTimeOffset = pointInTime.getTimeZone().getRawOffset();
            Timestamp ts = new Timestamp(pointInTime.getTime().getTime());

            tsPstmt.setTimestamp(1, ts);
            tsPstmt.executeUpdate();

            this.rs = connNoTz.createStatement().executeQuery("SELECT field1 FROM testBug3620");
            this.rs.next();
            String tsValueAsString = new String(this.rs.getBytes(1));
            Timestamp tsValueAsTimestamp = this.rs.getTimestamp(1);
            System.out.println("Timestamp as String, inserted with no calendar: " + tsValueAsString.toString());
            System.out.println("Timestamp as Timestamp, inserted with no calendar: " + tsValueAsTimestamp);

            connNoTz.createStatement().executeUpdate("DELETE FROM testBug3620");

            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

            props.put("useTimezone", "true");
            props.put("serverTimezone", "UTC");

            Connection connWithTz = getConnectionWithProps(props);
            Statement tsStmt = connWithTz.createStatement();
            tsPstmt = connWithTz.prepareStatement("INSERT INTO testBug3620 VALUES (?)");

            tsPstmt.setTimestamp(1, ts, cal);
            tsPstmt.executeUpdate();

            this.rs = connNoTz.createStatement().executeQuery("SELECT field1 FROM testBug3620");
            this.rs.next();
            tsValueAsString = new String(this.rs.getBytes(1));
            tsValueAsTimestamp = this.rs.getTimestamp(1);
            System.out.println("Timestamp as String, inserted with UTC calendar: " + tsValueAsString.toString());
            System.out.println("Timestamp as Timestamp, inserted with UTC calendar: " + tsValueAsTimestamp);

            this.rs = tsStmt.executeQuery("SELECT field1 FROM testBug3620");
            this.rs.next();
            Timestamp tsValueUTC = this.rs.getTimestamp(1, cal);
            System.out.println("Timestamp specifying UTC calendar from statement: " + tsValueUTC.toString());

            // We use this testcase with other vendors, JDBC spec requires result set fields can only be read once, although MySQL doesn't require this ;)
            this.rs = tsStmt.executeQuery("SELECT field1 FROM testBug3620");
            this.rs.next();
            Timestamp tsValueStmtNoCal = this.rs.getTimestamp(1);
            System.out.println("Timestamp specifying no calendar from statement: " + tsValueStmtNoCal.toString());

            PreparedStatement tsPstmtRetr = connWithTz.prepareStatement("SELECT field1 FROM testBug3620");
            this.rs = tsPstmtRetr.executeQuery();
            this.rs.next();
            Timestamp tsValuePstmtUTC = this.rs.getTimestamp(1, cal);
            System.out.println("Timestamp specifying UTC calendar from prepared statement: " + tsValuePstmtUTC.toString());

            // We use this testcase with other vendors, JDBC spec requires result set fields can only be read once, although MySQL doesn't require this ;)
            this.rs = tsPstmtRetr.executeQuery();
            this.rs.next();
            Timestamp tsValuePstmtNoCal = this.rs.getTimestamp(1);
            System.out.println("Timestamp specifying no calendar from prepared statement: " + tsValuePstmtNoCal.toString());

            long stmtDeltaTWithCal = (tsValueStmtNoCal.getTime() - ts.getTime());
            long deltaOrig = Math.abs(stmtDeltaTWithCal - pointInTimeOffset);
            assertTrue((deltaOrig < epsillon), "Difference between original timestamp and timestamp retrieved using java.sql.Statement "
                    + "set in database using UTC calendar is not ~= " + epsillon + " it is actually " + deltaOrig);

            long pStmtDeltaTWithCal = (tsValuePstmtNoCal.getTime() - ts.getTime());
            deltaOrig = Math.abs(pStmtDeltaTWithCal - pointInTimeOffset);
            assertTrue((deltaOrig < epsillon), "Difference between original timestamp and timestamp retrieved using java.sql.PreparedStatement "
                    + "set in database using UTC calendar is not ~= " + epsillon + ", it is actually " + deltaOrig);

            System.out.println("Difference between original ts and ts with no calendar: " + (tsValuePstmtNoCal.getTime() - ts.getTime()) + ", offset should be "
                    + pointInTimeOffset);
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    /**
     * Tests that DataTruncation is thrown when data is truncated.
     * 
     * @throws Exception
     */
    @Test
    public void testBug3697() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3697");
            this.stmt.executeUpdate("CREATE TABLE testBug3697 (field1 VARCHAR(255))");

            StringBuilder updateBuf = new StringBuilder("INSERT INTO testBug3697 VALUES ('");

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
     */
    @Test
    public void testBug3804() throws Exception {
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

            assertTrue(caughtTruncation, "Data truncation exception should've been thrown");
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3804");
        }
    }

    /**
     * Tests BUG#3873 - PreparedStatement.executeBatch() not returning all
     * generated keys (even though that's not JDBC compliant).
     * 
     * @throws Exception
     */
    @Test
    public void testBug3873() throws Exception {
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
     */
    @Test
    public void testBug4119() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4119");
            this.stmt.executeUpdate("CREATE TABLE `testBug4119` (`field1` varchar(255) NOT NULL default '', `field2` bigint(20) default NULL,"
                    + "`field3` int(11) default NULL, `field4` datetime default NULL, `field5` varchar(75) default NULL,"
                    + "`field6` varchar(75) default NULL, `field7` varchar(75) default NULL, `field8` datetime default NULL,"
                    + " PRIMARY KEY  (`field1`(100)))");

            PreparedStatement pStmt = this.conn.prepareStatement(
                    "insert into testBug4119 (field2, field3, field4, field5, field6, field7, field8, field1) values (?, ?, ?, ?, ?, ?, ?, ?)");

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
     */
    @Test
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
     */
    @Test
    public void testBug4510() throws Exception {
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
    @Test
    public void testBug4718() throws SQLException {
        if (((com.mysql.cj.jdbc.JdbcConnection) this.conn).getPropertySet().getBooleanProperty(PropertyKey.useServerPrepStmts).getValue()) {
            this.pstmt = this.conn.prepareStatement("SELECT 1 LIMIT ?");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);

            this.pstmt = this.conn.prepareStatement("SELECT 1 LIMIT 1");
            assertTrue(this.pstmt instanceof com.mysql.cj.jdbc.ServerPreparedStatement);

            this.pstmt = this.conn.prepareStatement("SELECT 1 LIMIT 1, ?");
            assertTrue(this.pstmt instanceof ClientPreparedStatement);

            try {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4718");
                this.stmt.executeUpdate("CREATE TABLE testBug4718 (field1 char(32))");

                this.pstmt = this.conn.prepareStatement("ALTER TABLE testBug4718 ADD INDEX (field1)");
                assertTrue(this.pstmt instanceof ClientPreparedStatement);

                this.pstmt = this.conn.prepareStatement("SELECT 1");
                assertTrue(this.pstmt instanceof ServerPreparedStatement);

                this.pstmt = this.conn.prepareStatement("UPDATE testBug4718 SET field1=1");
                assertTrue(this.pstmt instanceof ServerPreparedStatement);

                this.pstmt = this.conn.prepareStatement("UPDATE testBug4718 SET field1=1 LIMIT 1");
                assertTrue(this.pstmt instanceof ServerPreparedStatement);

                this.pstmt = this.conn.prepareStatement("UPDATE testBug4718 SET field1=1 LIMIT ?");
                assertTrue(this.pstmt instanceof ClientPreparedStatement);

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
     */
    @Test
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
    @Test
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
     */
    @Test
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

            pStmt = this.conn.prepareStatement(
                    "SELECT qc.QuestionId, q.Text FROM testBug5191Q q, testBug5191C qc WHERE qc.CategoryId = ? AND q.QuestionId = qc.QuestionId");

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
     * zeroDatetimeBehavior is 'CONVERT_TO_NULL'.
     * 
     * @throws Exception
     */
    @Test
    public void testBug5235() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.zeroDateTimeBehavior.getKeyName(), "CONVERT_TO_NULL");
        if (versionMeetsMinimum(5, 7, 4)) {
            props.setProperty(PropertyKey.jdbcCompliantTruncation.getKeyName(), "false");
        }

        if (versionMeetsMinimum(5, 7, 5)) {
            String sqlMode = getMysqlVariable("sql_mode");
            if (sqlMode.contains("STRICT_TRANS_TABLES")) {
                sqlMode = removeSqlMode("STRICT_TRANS_TABLES", sqlMode);
                props.setProperty(PropertyKey.sessionVariables.getKeyName(), "sql_mode='" + sqlMode + "'");
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

    @Test
    public void testBug5450() throws Exception {
        String table = "testBug5450";
        String column = "policyname";

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "utf-8");

            Connection utf8Conn = getConnectionWithProps(props);
            Statement utfStmt = utf8Conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

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

    @Test
    public void testBug5510() throws Exception {
        createTable("`testBug5510`",
                "(`a` bigint(20) NOT NULL auto_increment, `b` varchar(64) default NULL, `c` varchar(64) default NULL,"
                        + "`d` varchar(255) default NULL, `e` int(11) default NULL, `f` varchar(32) default NULL, `g` varchar(32) default NULL,"
                        + "`h` varchar(80) default NULL, `i` varchar(255) default NULL, `j` varchar(255) default NULL, `k` varchar(255) default NULL,"
                        + "`l` varchar(32) default NULL, `m` varchar(32) default NULL, `n` timestamp NOT NULL default CURRENT_TIMESTAMP on update"
                        + " CURRENT_TIMESTAMP, `o` int(11) default NULL, `p` int(11) default NULL, PRIMARY KEY  (`a`)) DEFAULT CHARSET=latin1",
                "InnoDB ");
        PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO testBug5510 (a) VALUES (?)");
        pStmt.setNull(1, 0);
        pStmt.executeUpdate();
    }

    /**
     * Tests fix for BUG#5874, timezone correction goes in wrong 'direction' (when useTimezone=true and server timezone differs from client timezone).
     * 
     * @throws Exception
     */
    @Test
    public void testBug5874() throws Exception {
        TimeZone defaultTimeZone = TimeZone.getDefault();

        try {
            String clientTimeZoneName = "America/Los_Angeles";
            String connectionTimeZoneName = "America/Chicago";

            TimeZone.setDefault(TimeZone.getTimeZone(clientTimeZoneName));

            long offsetDifference = TimeZone.getDefault().getRawOffset() - TimeZone.getTimeZone(connectionTimeZoneName).getRawOffset();

            SimpleDateFormat timestampFormat = TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd HH:mm:ss", null);
            SimpleDateFormat timeFormat = TimeUtil.getSimpleDateFormat(null, "HH:mm:ss", null);

            long pointInTime = timestampFormat.parse("2004-10-04 09:19:00").getTime();

            Properties props = new Properties();
            props.put("useTimezone", "true");
            props.put(PropertyKey.connectionTimeZone.getKeyName(), connectionTimeZoneName);
            props.put(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "true");
            props.put(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
            props.setProperty(PropertyKey.preserveInstants.getKeyName(), "true");

            Connection tzConn = getConnectionWithProps(props);
            Statement tzStmt = tzConn.createStatement();
            createTable("testBug5874", "(tstamp DATETIME, t TIME)");

            PreparedStatement tsPstmt = tzConn.prepareStatement("INSERT INTO testBug5874 VALUES (?, ?)");

            tsPstmt.setTimestamp(1, new Timestamp(pointInTime));
            tsPstmt.setTime(2, new Time(pointInTime));
            tsPstmt.executeUpdate();

            this.rs = tzStmt.executeQuery("SELECT * from testBug5874");

            while (this.rs.next()) { // Driver now converts/checks DATE/TIME/TIMESTAMP/DATETIME types when calling getString()...
                String retrTimestampString = new String(this.rs.getBytes(1));
                Timestamp retrTimestamp = this.rs.getTimestamp(1);

                java.util.Date timestampOnServer = timestampFormat.parse(retrTimestampString);

                long retrievedOffsetForTimestamp = retrTimestamp.getTime() - timestampOnServer.getTime();

                assertEquals(offsetDifference, retrievedOffsetForTimestamp,
                        "Original timestamp and timestamp retrieved using client timezone are not the same");

                String retrTimeString = new String(this.rs.getBytes(2));
                Time retrTime = this.rs.getTime(2);

                java.util.Date timeOnServerAsDate = timeFormat.parse(retrTimeString);
                Time timeOnServer = new Time(timeOnServerAsDate.getTime());

                long retrievedOffsetForTime = retrTime.getTime() - timeOnServer.getTime();

                assertEquals(0, retrievedOffsetForTime, "Original time and time retrieved using client timezone are not the same");
            }

            tzConn.close();
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    @Test
    public void testBug6823() throws SQLException {
        innerBug6823(true);
        innerBug6823(false);
    }

    @Test
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

    @Test
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
     */
    @Test
    public void testBug8487() throws Exception {
        try {
            this.pstmt = this.conn.prepareStatement("SELECT 1", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            this.pstmt.setFetchSize(Integer.MIN_VALUE);
            this.rs = this.pstmt.executeQuery();
            try {
                this.rs = this.conn.createStatement().executeQuery("SELECT 2");
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
    @Test
    public void testBug9704() throws Exception {
        Connection multiStmtConn = null;
        Statement multiStmt = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "true");

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

            assertTrue(multiStmt.getUpdateCount() == 1, "Update count was " + multiStmt.getUpdateCount() + ", expected 1");

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

    /**
     * Tests that you can close a statement twice without an NPE.
     * 
     * @throws Exception
     */
    @Test
    public void testCloseTwice() throws Exception {
        Statement closeMe = this.conn.createStatement();
        closeMe.close();
        closeMe.close();
    }

    @Test
    public void testCsc4194() throws Exception {
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

            charset = " CHARACTER SET cp932";

            createTable(tableNameText, "(field1 TEXT)" + charset);

            Properties windows31JProps = new Properties();
            windows31JProps.setProperty(PropertyKey.characterEncoding.getKeyName(), "Windows-31J");

            windows31JConn = getConnectionWithProps(windows31JProps);
            testCsc4194InsertCheckBlob(windows31JConn, tableNameBlob);

            testCsc4194InsertCheckText(windows31JConn, tableNameText, "Windows-31J");

            Properties sjisProps = new Properties();
            sjisProps.setProperty(PropertyKey.characterEncoding.getKeyName(), "sjis");

            sjisConn = getConnectionWithProps(sjisProps);
            testCsc4194InsertCheckBlob(sjisConn, tableNameBlob);
            testCsc4194InsertCheckText(sjisConn, tableNameText, "Windows-31J");

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
     */
    @Test
    public void testGetGeneratedKeysAllCases() throws Exception {
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
     */
    @Test
    public void testLimitAndMaxRows() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testMaxRowsAndLimit");
            this.stmt.executeUpdate("CREATE TABLE testMaxRowsAndLimit(limitField INT)");

            for (int i = 0; i < 500; i++) {
                this.stmt.executeUpdate("INSERT INTO testMaxRowsAndLimit VALUES (" + i + ")");
            }

            this.stmt.setMaxRows(250);
            this.rs = this.stmt.executeQuery("SELECT limitField FROM testMaxRowsAndLimit");
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
     */
    @Test
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

            StringBuilder fileNameBuf = null;

            if (File.separatorChar == '\\') {
                fileNameBuf = new StringBuilder();

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
                fileNameBuf = new StringBuilder(tempFile.getAbsolutePath());
            }
            final String fileName = fileNameBuf.toString();

            assertThrows(SQLSyntaxErrorException.class,
                    versionMeetsMinimum(8, 0, 19) ? "Loading local data is disabled;.*" : "The used command is not allowed with this MySQL version", () -> {
                        this.stmt.executeUpdate("LOAD DATA LOCAL INFILE '" + fileName + "' INTO TABLE loadDataRegress CHARACTER SET "
                                + CharsetMapping.getMysqlCharsetForJavaEncoding(
                                        ((MysqlConnection) this.conn).getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue(),
                                        this.serverVersion));
                        return null;
                    });

            Properties props = new Properties();
            props.setProperty(PropertyKey.allowLoadLocalInfile.getKeyName(), "true");
            Connection testConn = getConnectionWithProps(props);
            int updateCount = testConn.createStatement()
                    .executeUpdate("LOAD DATA LOCAL INFILE '" + fileNameBuf.toString() + "' INTO TABLE loadDataRegress CHARACTER SET "
                            + CharsetMapping.getMysqlCharsetForJavaEncoding(
                                    ((MysqlConnection) this.conn).getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue(),
                                    this.serverVersion));
            assertTrue(updateCount == rowCount);
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS loadDataRegress");
        }
    }

    @Test
    public void testNullClob() throws Exception {
        createTable("testNullClob", "(field1 TEXT NULL)");

        PreparedStatement pStmt = null;

        try {
            pStmt = this.conn.prepareStatement("INSERT INTO testNullClob VALUES (?)");
            pStmt.setClob(1, (Clob) null);
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
     */
    @Test
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
                assertTrue(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
            }
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testParameterBoundsCheck");
        }
    }

    @Test
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
     * Tests for BUG#9288, parameter index out of range if LIKE, ESCAPE '\'
     * present in query.
     * 
     * @throws Exception
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
    @Test
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

    @Test
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
     */
    @Test
    public void testSetCharacterStream() throws Exception {
        try {
            ((com.mysql.cj.jdbc.JdbcConnection) this.conn).getPropertySet().getBooleanProperty(PropertyKey.traceProtocol).setValue(true);

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

            assertTrue(result.length() == charBuf.length, "Retrieved value of length " + result.length() + " != length of inserted value " + charBuf.length);

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

            assertTrue(result.length() == charBuf.length, "Retrieved value of length " + result.length() + " != length of inserted value " + charBuf.length);
        } finally {
            ((com.mysql.cj.jdbc.JdbcConnection) this.conn).getPropertySet().getBooleanProperty(PropertyKey.traceProtocol).setValue(false);

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
     */
    @Test
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
     */
    @Test
    public void testSetMaxRows() throws Exception {
        Statement maxRowsStmt = null;

        try {
            maxRowsStmt = this.conn.createStatement();
            maxRowsStmt.setMaxRows(1);
            this.rs = maxRowsStmt.executeQuery("SELECT 1");
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
    @Test
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
            assertTrue(rTs.getYear() == ts.getYear(), "Retrieved year of " + rTs.getYear() + " does not match " + ts.getYear());
            assertTrue(rTs.getMonth() == ts.getMonth(), "Retrieved month of " + rTs.getMonth() + " does not match " + ts.getMonth());
            assertTrue(rTs.getDate() == ts.getDate(), "Retrieved date of " + rTs.getDate() + " does not match " + ts.getDate());

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testTimestampNPE");

        } finally {
        }
    }

    @Test
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
     */
    @Test
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
     * Tests fix for BUG#15383 - PreparedStatement.setObject() serializes BigInteger as object, rather than sending as numeric value (and is thus not
     * complementary to .getObject() on an UNSIGNED LONG type).
     * 
     * @throws Exception
     */
    @Test
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
     * Tests fix for BUG#17099 - Statement.getGeneratedKeys() throws NPE when no query has been processed.
     * 
     * @throws Exception
     */
    @Test
    public void testBug17099() throws Exception {

        PreparedStatement pStmt = this.conn.prepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS);
        assertNotNull(pStmt.getGeneratedKeys());

        pStmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS);
        assertNotNull(pStmt.getGeneratedKeys());
    }

    /**
     * Tests fix for BUG#17587 - clearParameters() on a closed prepared statement causes NPE.
     * 
     * @throws Exception
     */
    @Test
    public void testBug17587() throws Exception {
        createTable("testBug17857", "(field1 int)");
        PreparedStatement pStmt = null;

        try {
            pStmt = this.conn.prepareStatement("INSERT INTO testBug17857 VALUES (?)");
            pStmt.close();
            try {
                pStmt.clearParameters();
            } catch (SQLException sqlEx) {
                assertEquals(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());
            }

            pStmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement("INSERT INTO testBug17857 VALUES (?)");
            pStmt.close();
            try {
                pStmt.clearParameters();
            } catch (SQLException sqlEx) {
                assertEquals(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());
            }

        } finally {
            if (pStmt != null) {
                pStmt.close();
            }
        }
    }

    /**
     * Tests fix for BUG#19615, PreparedStatement.setObject(int, Object, int) doesn't respect scale of BigDecimals.
     * 
     * @throws Exception
     */
    @Test
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

        this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement("INSERT INTO testBug19615 VALUES (?)");
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
    @Test
    public void testBug20029() throws Exception {
        createTable("testBug20029", ("(field1 int)"));

        long initialTimeout = 20; // may need to raise this depending on environment we try and do this automatically in this testcase

        for (int i = 0; i < 10; i++) {
            final Connection toBeKilledConn = getConnectionWithProps(new Properties());
            final long timeout = initialTimeout;
            PreparedStatement toBeKilledPstmt = null;

            try {
                toBeKilledPstmt = ((com.mysql.cj.jdbc.JdbcConnection) toBeKilledConn).clientPrepareStatement("INSERT INTO testBug20029 VALUES (?)");

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
     * Fixes BUG#20687 - Can't pool server-side prepared statements, exception raised when re-using them.
     * 
     * @throws Exception
     */
    @Test
    public void testBug20687() throws Exception {
        createTable("testBug20687", "(field1 int)");
        Connection poolingConn = null;

        Properties props = new Properties();
        props.setProperty(PropertyKey.cachePrepStmts.getKeyName(), "true");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
        PreparedStatement pstmt1 = null;
        PreparedStatement pstmt2 = null;

        try {
            poolingConn = getConnectionWithProps(props);
            pstmt1 = poolingConn.prepareStatement("SELECT field1 FROM testBug20687");
            this.rs = pstmt1.executeQuery();
            pstmt1.close();

            pstmt2 = poolingConn.prepareStatement("SELECT field1 FROM testBug20687");
            this.rs = pstmt2.executeQuery();
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

    @Test
    public void testLikeWithBackslashes() throws Exception {
        Connection noBackslashEscapesConn = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sessionVariables.getKeyName(), "sql_mode=NO_BACKSLASH_ESCAPES");

            noBackslashEscapesConn = getConnectionWithProps(props);

            createTable("X_TEST",
                    "(userName varchar(32) not null, ivalue integer, CNAME varchar(255), bvalue CHAR(1), svalue varchar(255), ACTIVE CHAR(1), primary key (userName)) DEFAULT CHARSET=latin1");

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
     * Tests fix for BUG#20650 - Statement.cancel() causes NullPointerException if underlying connection has been closed due to server failure.
     * 
     * @throws Exception
     */
    @Test
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
     * Tests fix for BUG#20888 - escape of quotes in client-side prepared statements parsing not respected.
     * 
     * @throws Exception
     */
    @Test
    public void testBug20888() throws Exception {
        String s = "SELECT 'What do you think about D\\'Artanian''?', \"What do you think about D\\\"Artanian\"\"?\"";
        this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement(s);

        this.rs = this.pstmt.executeQuery();
        this.rs.next();
        assertEquals(this.rs.getString(1), "What do you think about D'Artanian'?");
        assertEquals(this.rs.getString(2), "What do you think about D\"Artanian\"?");
    }

    /**
     * Tests Bug#21207 - Driver throws NPE when tracing prepared statements that have been closed (in asSQL()).
     * 
     * @throws Exception
     */
    @Test
    public void testBug21207() throws Exception {
        this.pstmt = this.conn.prepareStatement("SELECT 1");
        this.pstmt.close();
        this.pstmt.toString(); // this used to cause an NPE
    }

    /**
     * Tests BUG#21438, server-side PS fails when using jdbcCompliantTruncation. If either is set to FALSE (&useServerPrepStmts=false or
     * &jdbcCompliantTruncation=false) test succedes.
     * 
     * @throws Exception
     */
    @SuppressWarnings("deprecation")
    @Test
    public void testBug21438() throws Exception {
        createTable("testBug21438", "(t_id int(10), test_date timestamp NOT NULL,primary key t_pk (t_id));");

        assertEquals(1, this.stmt.executeUpdate("insert into testBug21438 values (1,NOW());"));

        this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn)
                .serverPrepareStatement("UPDATE testBug21438 SET test_date=ADDDATE(?,INTERVAL 1 YEAR) WHERE t_id=1;");
        Timestamp ts = new Timestamp(System.currentTimeMillis());
        ts.setNanos(999999999);

        this.pstmt.setTimestamp(1, ts);

        assertEquals(1, this.pstmt.executeUpdate());

        Timestamp future = (Timestamp) getSingleIndexedValueWithQuery(1, "SELECT test_date FROM testBug21438");
        assertEquals(future.getYear() - ts.getYear(), 1);
    }

    /**
     * Tests fix for BUG#22359 - Driver was using millis for Statement.setQueryTimeout() when spec says argument is seconds.
     * 
     * @throws Exception
     */
    @Test
    public void testBug22359() throws Exception {
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

    /**
     * Tests fix for BUG#22290 - Driver issues truncation on write exception when it shouldn't (due to sending big decimal incorrectly to server with
     * server-side prepared statement).
     * 
     * @throws Exception
     */
    @Test
    public void testBug22290() throws Exception {
        createTable("testbug22290", "(`id` int(11) NOT NULL default '1',`cost` decimal(10,2) NOT NULL,PRIMARY KEY  (`id`)) DEFAULT CHARSET=utf8", "InnoDB");
        assertEquals(this.stmt.executeUpdate("INSERT INTO testbug22290 (`id`,`cost`) VALUES (1,'1.00')"), 1);

        Connection configuredConn = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sessionVariables.getKeyName(), "sql_mode='STRICT_TRANS_TABLES'");

            configuredConn = getConnectionWithProps(props);

            this.pstmt = configuredConn.prepareStatement("update testbug22290 set cost = cost + ? where id = 1");
            this.pstmt.setBigDecimal(1, new BigDecimal("1.11"));
            assertEquals(this.pstmt.executeUpdate(), 1);

            assertEquals(this.stmt.executeUpdate("UPDATE testbug22290 SET cost='1.00'"), 1);
            this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) configuredConn).clientPrepareStatement("update testbug22290 set cost = cost + ? where id = 1");
            this.pstmt.setBigDecimal(1, new BigDecimal("1.11"));
            assertEquals(this.pstmt.executeUpdate(), 1);
        } finally {
            if (configuredConn != null) {
                configuredConn.close();
            }
        }
    }

    @Test
    public void testClientPreparedSetBoolean() throws Exception {
        this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement("SELECT ?");
        this.pstmt.setBoolean(1, false);
        assertEquals("SELECT 0", this.pstmt.toString().substring(this.pstmt.toString().indexOf("SELECT")));
        this.pstmt.setBoolean(1, true);
        assertEquals("SELECT 1", this.pstmt.toString().substring(this.pstmt.toString().indexOf("SELECT")));
    }

    /**
     * Tests fix for BUG#24360 .setFetchSize() breaks prepared SHOW and other commands.
     * 
     * @throws Exception
     */
    @Test
    public void testBug24360() throws Exception {
        Connection c = null;

        Properties props = new Properties();
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");

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
     * Tests fix for BUG#24344 - useJDBCCompliantTimezoneShift with server-side prepared statements gives different behavior than when using client-side
     * prepared statements. (this is now fixed if moving from server-side prepared statements to client-side prepared statements by setting
     * "useSSPSCompatibleTimezoneShift" to "true", as the driver can't tell if this is a new deployment that never used server-side prepared statements, or if
     * it is an existing deployment that is switching to client-side prepared statements from server-side prepared statements.
     * 
     * Note: The properties 'useJDBCCompliantTimezoneShift' and 'useSSPSCompatibleTimezoneShift' no longer exist in Connector/J 6.0.
     * 
     * @throws Exception
     */
    @Test
    public void testBug24344() throws Exception {
        super.createTable("testBug24344", "(i INT AUTO_INCREMENT, t1 DATETIME, PRIMARY KEY (i)) ENGINE = MyISAM");

        Connection conn2 = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
            props.setProperty(PropertyKey.preserveInstants.getKeyName(), "false");
            conn2 = getConnectionWithProps(props);
            this.pstmt = conn2.prepareStatement("INSERT INTO testBug24344 (t1) VALUES (?)");
            Calendar c = Calendar.getInstance();
            c.set(Calendar.MILLISECOND, 789);
            this.pstmt.setTimestamp(1, new Timestamp(c.getTime().getTime()));
            this.pstmt.execute();
            this.pstmt.close();
            conn2.close();

            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false");
            props.setProperty(PropertyKey.preserveInstants.getKeyName(), "false");
            conn2 = getConnectionWithProps(props);
            this.pstmt = conn2.prepareStatement("INSERT INTO testBug24344 (t1) VALUES (?)");
            this.pstmt.setTimestamp(1, new Timestamp(c.getTime().getTime()));
            this.pstmt.execute();
            this.pstmt.close();

            Statement s = conn2.createStatement();
            this.rs = s.executeQuery("SELECT t1 FROM testBug24344 ORDER BY i ASC");

            Timestamp[] dates = new Timestamp[2];

            int i = 0;
            while (this.rs.next()) {
                dates[i++] = this.rs.getTimestamp(1);
            }

            assertEquals(2, i, "Number of rows should be 2.");
            assertEquals(dates[0], dates[1]);
        } finally {
            if (conn2 != null) {
                conn2.close();
            }
        }
    }

    /**
     * Tests fix for BUG#25073 - rewriting batched statements leaks internal statement instances, and causes a memory leak.
     * 
     * @throws Exception
     */
    @Test
    public void testBug25073() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
        Connection multiConn = getConnectionWithProps(props);
        createTable("testBug25073", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
        Statement multiStmt = multiConn.createStatement();
        multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (1)");
        multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (2)");
        multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (3)");
        multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (4)");
        multiStmt.addBatch("UPDATE testBug25073 SET field1=5 WHERE field1=1");
        multiStmt.addBatch("UPDATE testBug25073 SET field1=6 WHERE field1=2 OR field1=3");

        int beforeOpenStatementCount = ((com.mysql.cj.jdbc.JdbcConnection) multiConn).getActiveStatementCount();

        multiStmt.executeBatch();

        int afterOpenStatementCount = ((com.mysql.cj.jdbc.JdbcConnection) multiConn).getActiveStatementCount();

        assertEquals(beforeOpenStatementCount, afterOpenStatementCount);

        createTable("testBug25073", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
        props.clear();
        props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
        props.setProperty(PropertyKey.maxAllowedPacket.getKeyName(), "1024");
        props.setProperty(PropertyKey.dumpQueriesOnException.getKeyName(), "true");
        props.setProperty(PropertyKey.maxQuerySizeToLog.getKeyName(), String.valueOf(1024 * 1024 * 2));
        multiConn = getConnectionWithProps(props);
        multiStmt = multiConn.createStatement();

        for (int i = 0; i < 1000; i++) {
            multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (" + i + ")");
        }

        beforeOpenStatementCount = ((com.mysql.cj.jdbc.JdbcConnection) multiConn).getActiveStatementCount();

        multiStmt.executeBatch();

        afterOpenStatementCount = ((com.mysql.cj.jdbc.JdbcConnection) multiConn).getActiveStatementCount();

        assertEquals(beforeOpenStatementCount, afterOpenStatementCount);

        createTable("testBug25073", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");

        props.clear();
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false");
        props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
        props.setProperty(PropertyKey.dumpQueriesOnException.getKeyName(), "true");
        props.setProperty(PropertyKey.maxQuerySizeToLog.getKeyName(), String.valueOf(1024 * 1024 * 2));
        multiConn = getConnectionWithProps(props);
        PreparedStatement pStmt = multiConn.prepareStatement("INSERT INTO testBug25073(field1) VALUES (?)", Statement.RETURN_GENERATED_KEYS);

        for (int i = 0; i < 1000; i++) {
            pStmt.setInt(1, i);
            pStmt.addBatch();
        }

        beforeOpenStatementCount = ((com.mysql.cj.jdbc.JdbcConnection) multiConn).getActiveStatementCount();

        pStmt.executeBatch();

        afterOpenStatementCount = ((com.mysql.cj.jdbc.JdbcConnection) multiConn).getActiveStatementCount();

        assertEquals(beforeOpenStatementCount, afterOpenStatementCount);

        createTable("testBug25073", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false");
        props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
        props.setProperty(PropertyKey.maxAllowedPacket.getKeyName(), "1024");
        props.setProperty(PropertyKey.dumpQueriesOnException.getKeyName(), "true");
        props.setProperty(PropertyKey.maxQuerySizeToLog.getKeyName(), String.valueOf(1024 * 1024 * 2));
        multiConn = getConnectionWithProps(props);
        pStmt = multiConn.prepareStatement("INSERT INTO testBug25073(field1) VALUES (?)", Statement.RETURN_GENERATED_KEYS);

        for (int i = 0; i < 1000; i++) {
            pStmt.setInt(1, i);
            pStmt.addBatch();
        }

        beforeOpenStatementCount = ((com.mysql.cj.jdbc.JdbcConnection) multiConn).getActiveStatementCount();

        pStmt.executeBatch();

        afterOpenStatementCount = ((com.mysql.cj.jdbc.JdbcConnection) multiConn).getActiveStatementCount();

        assertEquals(beforeOpenStatementCount, afterOpenStatementCount);
    }

    /**
     * Tests fix for BUG#25009 - Results from updates not handled correctly in multi-statement queries.
     * 
     * @throws Exception
     */
    @Test
    public void testBug25009() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "true");

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
     * Tests fix for BUG#25025 - Client-side prepared statement parser gets confused by in-line (slash-star) comments and therefore can't rewrite batched
     * statements or reliably detect type of statements when they're used.
     * 
     * @throws Exception
     */
    @Test
    public void testBug25025() throws Exception {
        Connection multiConn = null;

        createTable("testBug25025", "(field1 INT)");

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false");

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
            assertEquals(true, ((ClientPreparedStatement) this.pstmt).getParseInfo().canRewriteAsMultiValueInsertAtSqlLevel());
        } finally {
            if (multiConn != null) {
                multiConn.close();
            }
        }
    }

    @Test
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
     * Tests fix for BUG#28256 - When connection is in read-only mode, queries that are parentheized incorrectly identified as DML.
     * 
     * @throws Exception
     */
    @Test
    public void testBug28256() throws Exception {
        try {
            this.conn.setReadOnly(true);
            this.stmt.execute("(SELECT 1) UNION (SELECT 2)");
            this.conn.prepareStatement("(SELECT 1) UNION (SELECT 2)").execute();
            ((com.mysql.cj.jdbc.JdbcConnection) this.conn).serverPrepareStatement("(SELECT 1) UNION (SELECT 2)").execute();
        } finally {
            this.conn.setReadOnly(false);
        }
    }

    /**
     * Tests fix for BUG#28469 - PreparedStatement.getMetaData() for statements containing leading one-line comments is not returned correctly.
     * 
     * As part of this fix, we also overhauled detection of DML for executeQuery() and SELECTs for executeUpdate() in plain and prepared statements to be aware
     * of the same types of comments.
     * 
     * @throws Exception
     */
    @Test
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
                    this.rs = commentStmt.executeQuery();
                    fail("Should not be able to call executeQuery() on a SELECT statement!");
                } catch (SQLException sqlEx) {
                    // expected
                }

                try {
                    this.rs = this.stmt.executeQuery(updatesToTest[i]);
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
     */
    @Test
    public void testCommentParsing() throws Exception {
        createTable("PERSON", "(NAME VARCHAR(32), PERID VARCHAR(32))");

        this.pstmt = this.conn.prepareStatement("SELECT NAME AS name2749_0_, PERID AS perid2749_0_ FROM PERSON WHERE PERID=? /*FOR UPDATE*/");
    }

    /**
     * Tests fix for BUG#28851 - parser in client-side prepared statements eats character following '/' if it's not a multi-line comment.
     * 
     * @throws Exception
     */
    @Test
    public void testBug28851() throws Exception {
        this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement("SELECT 1/?");
        this.pstmt.setInt(1, 1);
        this.rs = this.pstmt.executeQuery();

        assertTrue(this.rs.next());

        assertEquals(1, this.rs.getInt(1));
    }

    /**
     * Tests fix for BUG#28596 - parser in client-side prepared statements runs to end of statement, rather than end-of-line for '#' comments.
     * 
     * Also added support for '--' single-line comments
     * 
     * @throws Exception
     */
    @Test
    public void testBug28596() throws Exception {
        String query = "SELECT #\n?, #\n? #?\r\n,-- abcdefg \n?";

        this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement(query);
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
     * Tests fix for BUG#30550 - executeBatch() on an empty batch when there are no elements in the batch causes a divide-by-zero error when rewriting is
     * enabled.
     * 
     * @throws Exception
     */
    @Test
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
     * Tests fix for Bug#27412 - cached metadata with PreparedStatement.execute() throws NullPointerException.
     * 
     * @throws Exception
     */
    @Test
    public void testBug27412() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false");
        props.setProperty(PropertyKey.cachePrepStmts.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheResultSetMetadata.getKeyName(), "true");
        Connection conn2 = getConnectionWithProps(props);
        PreparedStatement pstm = conn2.prepareStatement("SELECT 1");
        try {
            assertTrue(pstm.execute());
        } finally {
            pstm.close();
            conn2.close();
        }
    }

    @Test
    public void testBustedGGKColumnNames() throws Exception {
        createTable("testBustedGGKColumnNames", "(field1 int primary key auto_increment)");
        this.stmt.executeUpdate("INSERT INTO testBustedGGKColumnNames VALUES (null)", Statement.RETURN_GENERATED_KEYS);
        assertEquals("GENERATED_KEY", this.stmt.getGeneratedKeys().getMetaData().getColumnName(1));

        this.pstmt = this.conn.prepareStatement("INSERT INTO testBustedGGKColumnNames VALUES (null)", Statement.RETURN_GENERATED_KEYS);
        this.pstmt.executeUpdate();
        assertEquals("GENERATED_KEY", this.pstmt.getGeneratedKeys().getMetaData().getColumnName(1));

        this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).serverPrepareStatement("INSERT INTO testBustedGGKColumnNames VALUES (null)",
                Statement.RETURN_GENERATED_KEYS);
        this.pstmt.executeUpdate();
        assertEquals("GENERATED_KEY", this.pstmt.getGeneratedKeys().getMetaData().getColumnName(1));
    }

    @Test
    public void testLancesBitMappingBug() throws Exception {
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
     * Tests fix for BUG#32577 - no way to store two timestamp/datetime values that happens over the DST switchover, as the hours end up being the same when
     * sent as the literal that MySQL requires.
     * 
     * Note that to get this scenario to work with MySQL (since it doesn't support per-value timezones), you need to configure your server (or session) to be in
     * UTC. This will cause the driver to always convert to/from the server and client timezone consistently.
     * 
     * @throws Exception
     */
    @Test
    public void testBug32577() throws Exception {
        createTable("testBug32577", "(id INT, field_datetime DATETIME, field_timestamp TIMESTAMP)");
        Properties props = new Properties();
        props.setProperty(PropertyKey.sessionVariables.getKeyName(), "time_zone='+0:00'");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "UTC");

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

            this.rs = nonLegacyConn.createStatement()
                    .executeQuery("SELECT id, field_datetime, field_timestamp , UNIX_TIMESTAMP(field_datetime), UNIX_TIMESTAMP(field_timestamp) "
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

            SimpleDateFormat sdf = TimeUtil.getSimpleDateFormat(null, "MM/dd/yyyy HH:mm z", TimeZone.getTimeZone("America/New_York"));
            System.out.println(sdf.format(ts2));
            System.out.println(sdf.format(ts1));
        } finally {
            if (nonLegacyConn != null) {
                nonLegacyConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#30508 - ResultSet returned by Statement.getGeneratedKeys() is not closed automatically when statement that created it is closed.
     * 
     * @throws Exception
     */
    @Test
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

    @Test
    public void testMoreLanceBugs() throws Exception {
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

    @Test
    public void testBug33823() throws Exception {
        ResultSetInternalMethods resultSetInternalMethods = new ResultSetInternalMethods() {

            public void clearNextResultset() {
            }

            public char getFirstCharOfQuery() {
                return 0;
            }

            public ResultSetInternalMethods getNextResultset() {
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

            public void initializeWithMetadata() throws SQLException {
            }

            public void populateCachedMetaData(CachedResultSetMetaData cachedMetaData) throws SQLException {
            }

            public void realClose(boolean calledExplicitly) throws SQLException {
            }

            public boolean isClosed() {
                return false;
            }

            public boolean hasRows() {
                return false;
            }

            public void setFirstCharOfQuery(char firstCharUpperCase) {
            }

            public void setOwningStatement(JdbcStatement owningStatement) {
            }

            public void setStatementUsedForFetchingRows(JdbcPreparedStatement stmt) {
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

            @Deprecated
            public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
                return null;
            }

            @Deprecated
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

            @Deprecated
            public InputStream getUnicodeStream(int columnIndex) throws SQLException {
                return null;
            }

            @Deprecated
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

            public RowId getRowId(int columnIndex) throws SQLException {
                return null;
            }

            public RowId getRowId(String columnLabel) throws SQLException {
                return null;
            }

            public void updateRowId(int columnIndex, RowId x) throws SQLException {
            }

            public void updateRowId(String columnLabel, RowId x) throws SQLException {
            }

            public int getHoldability() throws SQLException {
                return 0;
            }

            public void updateNString(int columnIndex, String nString) throws SQLException {
            }

            public void updateNString(String columnLabel, String nString) throws SQLException {
            }

            public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
            }

            public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
            }

            public NClob getNClob(int columnIndex) throws SQLException {
                return null;
            }

            public NClob getNClob(String columnLabel) throws SQLException {
                return null;
            }

            public SQLXML getSQLXML(int columnIndex) throws SQLException {
                return null;
            }

            public SQLXML getSQLXML(String columnLabel) throws SQLException {
                return null;
            }

            public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
            }

            public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
            }

            public String getNString(int columnIndex) throws SQLException {
                return null;
            }

            public String getNString(String columnLabel) throws SQLException {
                return null;
            }

            public Reader getNCharacterStream(int columnIndex) throws SQLException {
                return null;
            }

            public Reader getNCharacterStream(String columnLabel) throws SQLException {
                return null;
            }

            public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
            }

            public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
            }

            public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
            }

            public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
            }

            public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
            }

            public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
            }

            public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
            }

            public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
            }

            public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
            }

            public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
            }

            public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
            }

            public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
            }

            public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
            }

            public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
            }

            public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
            }

            public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
            }

            public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
            }

            public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
            }

            public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
            }

            public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
            }

            public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
            }

            public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
            }

            public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
            }

            public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
            }

            public void updateClob(int columnIndex, Reader reader) throws SQLException {
            }

            public void updateClob(String columnLabel, Reader reader) throws SQLException {
            }

            public void updateNClob(int columnIndex, Reader reader) throws SQLException {
            }

            public void updateNClob(String columnLabel, Reader reader) throws SQLException {
            }

            public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
                return null;
            }

            public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
                return null;
            }

            public <T> T unwrap(Class<T> iface) throws SQLException {
                return null;
            }

            public boolean isWrapperFor(Class<?> iface) throws SQLException {
                return false;
            }

            @Override
            public BigInteger getBigInteger(int columnIndex) throws SQLException {
                return null;
            }

            @Override
            public void closeOwner(boolean calledExplicitly) {
            }

            @Override
            public MysqlConnection getConnection() {
                return null;
            }

            @Override
            public Session getSession() {
                return null;
            }

            @Override
            public String getPointOfOrigin() {
                return null;
            }

            @Override
            public int getOwnerFetchSize() {
                return 0;
            }

            @Override
            public Query getOwningQuery() {
                return null;
            }

            @Override
            public int getOwningStatementMaxRows() {
                return 0;
            }

            @Override
            public int getOwningStatementFetchSize() {
                return 0;
            }

            @Override
            public long getOwningStatementServerId() {
                return 0;
            }

            @Override
            public int getResultId() {
                return 0;
            }

            @Override
            public void initRowsWithMetadata() {
            }

            @Override
            public ColumnDefinition getColumnDefinition() {
                return null;
            }

            @Override
            public void setColumnDefinition(ColumnDefinition metadata) {
            }

            @Override
            public void setNextResultset(Resultset nextResultset) {
            }

            @Override
            public ResultsetRows getRows() {
                return null;
            }

            @Override
            public Object getSyncMutex() {
                return null;
            }
        };

        resultSetInternalMethods.close();
    }

    /**
     * Tests fix for BUG#34093 - Statements with batched values do not return correct values for getGeneratedKeys() when "rewriteBatchedStatements" is set to
     * "true", and the statement has an "ON DUPLICATE KEY UPDATE" clause.
     * 
     * @throws Exception
     */
    @Test
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
                int expectedUpdateCount = versionMeetsMinimum(5, 5, 16) ? 1 : 2; // behavior changed by fix of Bug#46675, affects servers starting from 5.5.16 and 5.6.3

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

    @Test
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
            int expectedUpdateCount = versionMeetsMinimum(5, 5, 16) ? 1 : 2; // behavior changed by fix of Bug#46675, affects servers starting from 5.5.16 and 5.6.3

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

    @Test
    public void testBug34518() throws Exception {
        Connection fetchConn = getConnectionWithProps("useCursorFetch=true");
        Statement fetchStmt = fetchConn.createStatement();

        int stmtCount = ((com.mysql.cj.jdbc.JdbcConnection) fetchConn).getActiveStatementCount();

        fetchStmt.setFetchSize(100);
        this.rs = fetchStmt.executeQuery("SELECT 1");

        assertEquals(((com.mysql.cj.jdbc.JdbcConnection) fetchConn).getActiveStatementCount(), stmtCount + 1);
        this.rs.close();
        assertEquals(((com.mysql.cj.jdbc.JdbcConnection) fetchConn).getActiveStatementCount(), stmtCount);
    }

    @Test
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

    @Test
    public void testBug35666() throws Exception {
        Connection loggingConn = getConnectionWithProps("logSlowQueries=true");
        this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) loggingConn).serverPrepareStatement("SELECT SLEEP(4)");
        this.pstmt.execute();
    }

    @Test
    public void testDeadlockBatchBehavior() throws Exception {
        try {
            createTable("t1", "(id INTEGER, x INTEGER)", "INNODB");
            createTable("t2", "(id INTEGER, x INTEGER)", "INNODB");
            this.stmt.executeUpdate("INSERT INTO t1 VALUES (0, 0)");

            this.conn.setAutoCommit(false);
            this.rs = this.conn.createStatement().executeQuery("SELECT * FROM t1 WHERE id=0 FOR UPDATE");

            final Connection deadlockConn = getConnectionWithProps("includeInnodbStatusInDeadlockExceptions=true");
            deadlockConn.setAutoCommit(false);

            final Statement deadlockStmt = deadlockConn.createStatement();
            deadlockStmt.executeUpdate("INSERT INTO t2 VALUES (1, 0)");
            this.rs = deadlockStmt.executeQuery("SELECT * FROM t2 WHERE id=0 FOR UPDATE");

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

    @Test
    public void testBug39352() throws Exception {
        Connection affectedRowsConn = getConnectionWithProps("useAffectedRows=true");

        try {

            createTable("bug39352", "(id INT PRIMARY KEY, data VARCHAR(100))");
            assertEquals(1, this.stmt.executeUpdate("INSERT INTO bug39352 (id,data) values (1,'a')"));
            int rowsAffected = this.stmt.executeUpdate("INSERT INTO bug39352 (id, data) VALUES(2, 'bb') ON DUPLICATE KEY UPDATE data=values(data)");
            assertEquals(1, rowsAffected, "First UPD failed");

            rowsAffected = affectedRowsConn.createStatement()
                    .executeUpdate("INSERT INTO bug39352 (id, data) VALUES(2, 'bbb') ON DUPLICATE KEY UPDATE data=values(data)");
            assertEquals(2, rowsAffected, "2nd UPD failed");

            rowsAffected = affectedRowsConn.createStatement()
                    .executeUpdate("INSERT INTO bug39352 (id, data) VALUES(2, 'bbb') ON DUPLICATE KEY UPDATE data=values(data)");
            assertEquals(0, rowsAffected, "3rd UPD failed");

        } finally {
            affectedRowsConn.close();
        }
    }

    @Test
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

    @Test
    public void testBug39956() throws Exception {
        ResultSet enginesRs = this.conn.createStatement().executeQuery("SHOW ENGINES");

        while (enginesRs.next()) {
            if ("YES".equalsIgnoreCase(enginesRs.getString("Support")) || "DEFAULT".equalsIgnoreCase(enginesRs.getString("Support"))) {

                String engineName = enginesRs.getString("Engine");

                if ("CSV".equalsIgnoreCase(engineName) || "BLACKHOLE".equalsIgnoreCase(engineName) || "FEDERATED".equalsIgnoreCase(engineName)
                        || "MRG_MYISAM".equalsIgnoreCase(engineName) || "PARTITION".equalsIgnoreCase(engineName) || "EXAMPLE".equalsIgnoreCase(engineName)
                        || "PERFORMANCE_SCHEMA".equalsIgnoreCase(engineName) || engineName.endsWith("_SCHEMA")) {
                    continue; // not supported
                }

                String tableName = "testBug39956_" + engineName;

                Connection twoConn = getConnectionWithProps("sessionVariables=auto_increment_increment=2");

                try {
                    for (int i = 0; i < 2; i++) {
                        createTable(tableName, "(k int primary key auto_increment, p varchar(4)) ENGINE=" + engineName);

                        ((com.mysql.cj.jdbc.JdbcConnection) twoConn).getPropertySet().getBooleanProperty(PropertyKey.rewriteBatchedStatements).setValue(i == 1);

                        this.pstmt = twoConn.prepareStatement("INSERT INTO " + tableName + " (p) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
                        this.pstmt.setString(1, "a");
                        this.pstmt.addBatch();
                        this.pstmt.setString(1, "b");
                        this.pstmt.addBatch();
                        this.pstmt.executeBatch();

                        this.rs = this.pstmt.getGeneratedKeys();

                        this.rs.next();
                        assertEquals(1, this.rs.getInt(1), "For engine " + engineName + ((i == 1) ? " rewritten " : " plain "));
                        this.rs.next();
                        assertEquals(3, this.rs.getInt(1), "For engine " + engineName + ((i == 1) ? " rewritten " : " plain "));

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
                            assertEquals(key, this.rs.getInt(1), "For engine " + engineName + ((i == 1) ? " rewritten " : " plain "));
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

    @Test
    public void testBug34185() throws Exception {
        this.rs = this.stmt.executeQuery("SELECT 1");

        try {
            this.stmt.getGeneratedKeys();
            fail("Expected exception");
        } catch (SQLException sqlEx) {
            assertEquals(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());
        }

        this.pstmt = this.conn.prepareStatement("SELECT 1");

        try {
            this.pstmt.execute();
            this.pstmt.getGeneratedKeys();
            fail("Expected exception");
        } catch (SQLException sqlEx) {
            assertEquals(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());
        }
    }

    @Test
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
     * Ensures that cases listed in Bug#41448 actually work - we don't think there's a bug here right now
     * 
     * @throws Exception
     */
    @Test
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

    @Test
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
     * Tests fix for Bug#41532 - regression in performance for batched inserts when using ON DUPLICATE KEY UPDATE
     * 
     * @throws Exception
     */
    @Test
    public void testBug41532() throws Exception {
        createTable("testBug41532", "(ID INTEGER, S1 VARCHAR(100), S2 VARCHAR(100), S3 VARCHAR(100), D1 DATETIME, D2 DATETIME, D3 DATETIME, "
                + "N1 DECIMAL(28,6), N2 DECIMAL(28,6), N3 DECIMAL(28,6), UNIQUE KEY UNIQUE_KEY_TEST_DUPLICATE (ID) )");

        int numTests = 5000;
        Connection rewriteConn = getConnectionWithProps("useSSL=false,allowPublicKeyRetrieval=true,rewriteBatchedStatements=true,dumpQueriesOnException=true");

        assertEquals("0", getSingleIndexedValueWithQuery(rewriteConn, 2, "SHOW SESSION STATUS LIKE 'Com_insert'").toString());
        long batchedTime = timeBatch(rewriteConn, numTests);
        assertEquals("1", getSingleIndexedValueWithQuery(rewriteConn, 2, "SHOW SESSION STATUS LIKE 'Com_insert'").toString());

        this.stmt.executeUpdate("TRUNCATE TABLE testBug41532");

        assertEquals("0", getSingleIndexedValueWithQuery(this.conn, 2, "SHOW SESSION STATUS LIKE 'Com_insert'").toString());
        long unbatchedTime = timeBatch(this.conn, numTests);
        assertEquals(String.valueOf(numTests), getSingleIndexedValueWithQuery(this.conn, 2, "SHOW SESSION STATUS LIKE 'Com_insert'").toString());
        assertTrue(batchedTime < unbatchedTime);

        rewriteConn = getConnectionWithProps(
                "useSSL=false,allowPublicKeyRetrieval=true,rewriteBatchedStatements=true,useCursorFetch=true,defaultFetchSize=10000");
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
     * Tests fix for Bug#44056 - Statement.getGeneratedKeys() retains result set instances until statement is closed.
     * 
     * @throws Exception
     */
    @Test
    public void testBug44056() throws Exception {
        createTable("testBug44056", "(pk int primary key not null auto_increment)");
        Statement newStmt = this.conn.createStatement();

        try {
            newStmt.executeUpdate("INSERT INTO testBug44056 VALUES (null)", Statement.RETURN_GENERATED_KEYS);
            checkOpenResultsFor44056(newStmt);
            this.pstmt = this.conn.prepareStatement("INSERT INTO testBug44056 VALUES (null)", Statement.RETURN_GENERATED_KEYS);
            this.pstmt.executeUpdate();
            checkOpenResultsFor44056(this.pstmt);
            this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).serverPrepareStatement("INSERT INTO testBug44056 VALUES (null)",
                    Statement.RETURN_GENERATED_KEYS);
            this.pstmt.executeUpdate();
            checkOpenResultsFor44056(this.pstmt);
        } finally {
            newStmt.close();
        }
    }

    private void checkOpenResultsFor44056(Statement newStmt) throws SQLException {
        this.rs = newStmt.getGeneratedKeys();
        assertEquals(0, ((com.mysql.cj.jdbc.JdbcStatement) newStmt).getOpenResultSetCount());
        this.rs.close();
        assertEquals(0, ((com.mysql.cj.jdbc.JdbcStatement) newStmt).getOpenResultSetCount());
    }

    /**
     * Bug #41730 - SQL Injection when using U+00A5 and SJIS/Windows-31J
     * 
     * @throws Exception
     */
    @Test
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
            this.rs = pstmt2.executeQuery();
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

    @Test
    public void testBug43196() throws Exception {
        createTable("`bug43196`",
                "(`id` bigint(20) unsigned NOT NULL AUTO_INCREMENT PRIMARY KEY, `a` bigint(20) unsigned NOT NULL) ENGINE=MyISAM DEFAULT CHARSET=latin1;");

        Connection conn1 = null;

        try {
            assertEquals(1, this.stmt.executeUpdate("INSERT INTO bug43196 (a) VALUES (1)", Statement.RETURN_GENERATED_KEYS));

            this.rs = this.stmt.getGeneratedKeys();

            if (this.rs.next()) {

                Object id = this.rs.getObject(1);// use long

                assertEquals(BigInteger.class, id.getClass());
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

            assertTrue(this.rs.isFirst(), "No rows returned");
            assertEquals("18446744073709551200", this.rs.getObject(1).toString());
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    /**
     * Bug #42253 - multiple escaped quotes cause exception from EscapeProcessor.
     * 
     * @throws Exception
     */
    @Test
    public void testBug42253() throws Exception {
        this.rs = this.stmt.executeQuery("select '\\'\\'','{t\\'}'");
        this.rs.next();
        assertEquals("''", this.rs.getString(1));
        assertEquals("{t'}", this.rs.getString(2));
    }

    /**
     * Bug #41566 - Quotes within comments not correctly ignored by escape parser
     * 
     * @throws Exception
     */
    @Test
    public void testBug41566() throws Exception {
        this.rs = this.stmt.executeQuery("-- this should't change the literal\n select '{1}'");
        this.rs.next();
        assertEquals("{1}", this.rs.getString(1));
    }

    /*
     * Bug #40439 - Error rewriting batched statement if table name ends with
     * "values".
     */
    @Test
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

    public static class Bug39426Interceptor extends BaseQueryInterceptor {
        public static List<Integer> vals = new ArrayList<>();
        String prevSql;

        @Override
        public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {

            if (interceptedQuery instanceof ClientPreparedStatement) {
                String asSql = interceptedQuery.toString();
                int firstColon = asSql.indexOf(":");
                asSql = asSql.substring(firstColon + 2);

                if (asSql.equals(this.prevSql)) {
                    throw new RuntimeException("Previous statement matched current: " + sql.get());
                }
                this.prevSql = asSql;
                try {
                    ParameterBindings b = ((ClientPreparedStatement) interceptedQuery).getParameterBindings();
                    vals.add(new Integer(b.getInt(1)));

                } catch (SQLException ex) {
                    throw ExceptionFactory.createException(ex.getMessage(), ex);
                }
            }
            return null;
        }
    }

    /**
     * Bug #39426 - executeBatch passes most recent PreparedStatement params to StatementInterceptor
     * 
     * @throws Exception
     */
    @Test
    public void testBug39426() throws Exception {
        for (boolean useSPS : new boolean[] { false, true }) {
            Properties props = new Properties();
            props.setProperty(PropertyKey.queryInterceptors.getKeyName(), "testsuite.regression.StatementRegressionTest$Bug39426Interceptor");
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            Connection c = null;
            try {
                createTable("testBug39426", "(x int)");
                c = getConnectionWithProps(props);
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
    }

    @Test
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
                assertEquals(MysqlErrorNumbers.SQL_STATE_SYNTAX_ERROR, sqlEx.getSQLState());
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
     * 
     * @throws Exception
     */
    @Test
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

    @Test
    public void testBug34555() throws Exception {
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

    @Test
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

    @Test
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

    @Test
    public void testBug51776() throws Exception {
        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.socketFactory.getKeyName(), "testsuite.UnreliableSocketFactory");

        Properties parsed = getPropertiesFromTestsuiteUrl();
        String db = parsed.getProperty(PropertyKey.DBNAME.getKeyName());
        String port = parsed.getProperty(PropertyKey.PORT.getKeyName());
        String host = getPortFreeHostname(props);

        UnreliableSocketFactory.flushAllStaticData();
        UnreliableSocketFactory.mapHost("first", host);

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

    @Test
    public void testBug51666() throws Exception {
        Connection testConn = getConnectionWithProps("queryInterceptors=" + TestBug51666QueryInterceptor.class.getName());
        createTable("testQueryInterceptorCount", "(field1 int)");
        this.stmt.executeUpdate("INSERT INTO testQueryInterceptorCount VALUES (0)");
        ResultSet testRs = testConn.createStatement().executeQuery("SHOW SESSION STATUS LIKE 'Com_select'");
        testRs.next();
        int s = testRs.getInt(2);
        this.rs = testConn.createStatement().executeQuery("SELECT 1");
        testRs = testConn.createStatement().executeQuery("SHOW SESSION STATUS LIKE 'Com_select'");
        testRs.next();
        assertEquals(s + 1, testRs.getInt(2));
    }

    public static class TestBug51666QueryInterceptor extends BaseQueryInterceptor {

        private JdbcConnection connection;

        @Override
        public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
            this.connection = (JdbcConnection) conn;
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
            if (sql.get().equals("SELECT 1")) {
                try {
                    java.sql.Statement test = this.connection.createStatement();
                    return (T) test.executeQuery("/* execute this, not the original */ SELECT 1");
                } catch (SQLException ex) {
                    throw ExceptionFactory.createException(ex.getMessage(), ex);
                }
            }
            return null;
        }

        @Override
        public void destroy() {
            this.connection = null;
        }
    }

    @Test
    public void testReversalOfScanFlags() throws Exception {
        createTable("testReversalOfScanFlags", "(field1 int)");
        this.stmt.executeUpdate("INSERT INTO testReversalOfScanFlags VALUES (1),(2),(3)");

        Connection scanningConn = getConnectionWithProps("queryInterceptors=" + ScanDetectingInterceptor.class.getName());

        try {
            this.rs = scanningConn.createStatement().executeQuery("SELECT field1 FROM testReversalOfScanFlags");
            assertTrue(ScanDetectingInterceptor.hasSeenScan);
            assertFalse(ScanDetectingInterceptor.hasSeenBadIndex);
        } finally {
            scanningConn.close();
        }
    }

    public static class ScanDetectingInterceptor extends BaseQueryInterceptor {
        static boolean hasSeenScan = false;
        static boolean hasSeenBadIndex = false;

        @Override
        public <T extends Resultset> T postProcess(Supplier<String> sql, Query interceptedQuery, T originalResultSet, ServerSession serverSession) {

            if (serverSession.noIndexUsed()) {
                hasSeenScan = true;
            }

            if (serverSession.noGoodIndexUsed()) {
                hasSeenBadIndex = true;
            }

            return null;
        }
    }

    /**
     * Tests fix for Bug#51704, rewritten batched statements don't honor escape processing flag of Statement that they are created for
     * 
     * @throws Exception
     */
    @Test
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

    @Test
    public void testBug54175() throws Exception {
        Connection utf8conn = getConnectionWithProps("characterEncoding=utf8");

        createTable("testBug54175", "(a VARCHAR(10)) CHARACTER SET utf8mb4");
        this.stmt.execute("INSERT INTO testBug54175 VALUES(0xF0AFA6B2)");
        this.rs = utf8conn.createStatement().executeQuery("SELECT * FROM testBug54175");
        assertTrue(this.rs.next());
        assertEquals(55422, this.rs.getString(1).charAt(0));
    }

    /**
     * Tests fix for Bug#58728, NPE in com.mysql.jdbc.StatementWrappe.getResultSet()
     * ((com.mysql.jdbc.ResultSetInternalMethods) rs).setWrapperStatement(this);
     * when rs is null
     * 
     * @throws Exception
     */
    @Test
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

    @Test
    public void testBug61501() throws Exception {
        createTable("testBug61501", "(id int)");
        this.stmt.executeUpdate("INSERT INTO testBug61501 VALUES (1)");
        String sql = "SELECT id FROM testBug61501 where id=1";
        this.pstmt = this.conn.prepareStatement(sql);
        this.rs = this.pstmt.executeQuery();
        this.pstmt.cancel();
        this.pstmt.close();

        this.pstmt = this.conn.prepareStatement(sql);
        this.rs = this.pstmt.executeQuery();

        this.stmt.cancel();
        this.rs = this.stmt.executeQuery(sql);
        this.stmt.cancel();
        this.stmt.execute(sql);
        this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).serverPrepareStatement(sql);
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

        this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).serverPrepareStatement(sql);
        this.pstmt.execute();
        assertEquals(1, this.pstmt.getUpdateCount());
        this.pstmt.cancel();
        this.pstmt.close();

        this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).serverPrepareStatement(sql);
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

    @Test
    public void testbug61866() throws Exception {
        createProcedure("WARN_PROCEDURE", "() BEGIN	DECLARE l_done INT;	SELECT 1 	INTO l_done	FROM DUAL	WHERE 1=2; END");
        this.pstmt = this.conn.prepareCall("{CALL WARN_PROCEDURE()}");
        this.pstmt.execute();
        assertTrue(this.pstmt.getWarnings().toString().contentEquals("java.sql.SQLWarning: No data - zero rows fetched, selected, or processed"),
                "No warning when expected");
        this.pstmt.clearWarnings();
        assertNull(this.pstmt.getWarnings(), "Warning when not expected");
    }

    @Test
    public void testbug12565726() throws Exception {
        // Not putting the space between VALUES() and ON DUPLICATE KEY UPDATE
        // causes C/J a) enter rewriting the query altrhough it has ON UPDATE 
        // and b) to generate the wrong query with multiple ON DUPLICATE KEY

        Properties props = new Properties();
        props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false");
        props.setProperty(PropertyKey.enablePacketDebug.getKeyName(), "true");
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

    @Test
    public void testBug36478() throws Exception {
        createTable("testBug36478", "(`limit` varchar(255) not null primary key, id_limit INT, limit1 INT, maxlimit2 INT)");

        this.stmt.execute("INSERT INTO testBug36478 VALUES ('bahblah',1,1,1)");
        this.stmt.execute("INSERT INTO testBug36478 VALUES ('bahblah2',2,2,2)");
        this.pstmt = this.conn.prepareStatement("select 1 FROM testBug36478", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        this.pstmt.setMaxRows(1);
        this.rs = this.pstmt.executeQuery();
        this.rs.first();
        assertTrue(this.rs.isFirst());
        assertTrue(this.rs.isLast());

        this.pstmt = this.conn.prepareStatement("select `limit`, id_limit, limit1, maxlimit2 FROM testBug36478", ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
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
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");

            _conn = getConnectionWithProps(props);
            s = _conn.prepareStatement("select 1 FROM testBug36478", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            s.setMaxRows(1);
            ResultSet _rs = s.executeQuery();
            _rs.first();
            assertTrue(_rs.isFirst());
            assertTrue(_rs.isLast());

            s.close();
            s = _conn.prepareStatement("select `limit`, id_limit, limit1, maxlimit2 FROM testBug36478", ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
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
     * @throws Exception
     */
    @Test
    public void testBug40279() throws Exception {
        if (!versionMeetsMinimum(5, 6, 4)) {
            return;
        }

        createTable("testBug40279", "(f1 int, f2 timestamp(6))");

        Timestamp ts = new Timestamp(1300791248001L);

        Connection ps_conn = null;
        Connection ssps_conn = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "UTC");
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false");
            ps_conn = getConnectionWithProps(props);

            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
            ssps_conn = getConnectionWithProps(props);

            this.pstmt = ps_conn.prepareStatement("INSERT INTO testBug40279(f1, f2) VALUES (?, ?)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setTimestamp(2, ts);
            this.pstmt.execute();
            this.pstmt.close();

            this.pstmt = ssps_conn.prepareStatement("INSERT INTO testBug40279(f1, f2) VALUES (?, ?)");
            this.pstmt.setInt(1, 2);
            this.pstmt.setTimestamp(2, ts);
            this.pstmt.execute();
            this.pstmt.close();

            this.rs = this.stmt.executeQuery("SELECT f2 FROM testBug40279");
            while (this.rs.next()) {
                assertEquals(ts.getNanos(), this.rs.getTimestamp("f2").getNanos());
            }

        } finally {
            if (ps_conn != null) {
                ps_conn.close();
            }
            if (ssps_conn != null) {
                ssps_conn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#35653 - executeQuery() in Statement.java let "TRUNCATE" queries being executed.
     * "RENAME" is also filtered now.
     * 
     * @throws Exception
     */
    @Test
    public void testBug35653() throws Exception {
        createTable("testBug35653", "(f1 int)");
        try {
            this.rs = this.stmt.executeQuery("TRUNCATE testBug35653");
            fail("executeQuery() shouldn't allow TRUNCATE");
        } catch (SQLException e) {
            assertTrue(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT == e.getSQLState());
        }

        try {
            this.rs = this.stmt.executeQuery("RENAME TABLE testBug35653 TO testBug35653_new");
            fail("executeQuery() shouldn't allow RENAME");
        } catch (SQLException e) {
            assertTrue(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT == e.getSQLState());
        } finally {
            dropTable("testBug35653_new");
        }
    }

    /**
     * Tests fix for BUG#64805 - StatementImpl$CancelTask occasionally throws NullPointerExceptions.
     * 
     * @throws Exception
     */
    @Test
    public void testBug64805() throws Exception {
        try {
            this.stmt.setQueryTimeout(5);
            this.rs = this.stmt.executeQuery("select sleep(5)");
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
     * Added support for EXPLAIN INSERT/REPLACE/UPDATE/DELETE. Connector/J must issue a warning containing the execution plan for slow queries when connection
     * properties logSlowQueries=true and explainSlowQueries=true are used.
     * 
     * @throws Exception
     */
    @Test
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

            Connection getNewConnectionForSlowQueries() throws SQLException {
                releaseConnectionResources();
                this.testConn = getConnectionWithProps("logSlowQueries=true,explainSlowQueries=true");
                Statement st = this.testConn.createStatement();
                // execute several fast queries to unlock slow query analysis and lower query execution time mean
                for (int i = 0; i < 25; i++) {
                    st.execute("SELECT 1");
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
                    assertTrue(testHandler.containsSlowQueryMsg(query), "A slow query explain results warning should have been issued for: '" + query + "'.");
                    testStatement.close();
                }
            } else {
                // only SELECT is qualified to log slow query explain results warning
                final String query = "SELECT SLEEP(1)";

                testStatement = testHandler.getNewConnectionForSlowQueries().createStatement();
                testStatement.execute(query);
                assertTrue(testHandler.containsSlowQueryMsg(query), "A slow query explain results warning should have been issued for: '" + query + "'.");
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
     */
    @Test
    public void testBug68562() throws Exception {
        testBug68562BatchWithSize(1);
        testBug68562BatchWithSize(3);
    }

    private void testBug68562BatchWithSize(int batchSize) throws Exception {

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
            properties.setProperty(PropertyKey.useAffectedRows.getKeyName(), "true");
            tableName += "_affected";
        } else {
            tableName += "_found";
        }
        if (rewriteBatchedStatements) {
            properties.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
        }
        Connection connection = getConnectionWithProps(properties);

        PreparedStatement statement = connection
                .prepareStatement("INSERT INTO " + tableName + "(id, name, version) VALUES(?,?,?) ON DUPLICATE KEY UPDATE version = "
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
     */
    @Test
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
                for (PreparedStatement testStmt : new PreparedStatement[] {
                        testConn.prepareStatement("SELECT * FROM testBug55340", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY),
                        testConn.prepareCall("CALL testBug55340()", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY) }) {

                    assertTrue(testStmt.execute());
                    this.rs = testStmt.getResultSet();
                    assertResultSetLength(this.rs, 3);

                    rsmd = this.rs.getMetaData();
                    assertEquals(2, rsmd.getColumnCount(),
                            "(" + i + ") " + testDesc + " - " + testStmt.getClass().getSimpleName() + ":RSMetaData - wrong column count.");
                    assertEquals(Integer.class.getName(), rsmd.getColumnClassName(1),
                            "(" + i + ") " + testDesc + " - " + testStmt.getClass().getSimpleName() + ":RSMetaData - wrong column(1) type.");
                    assertEquals(String.class.getName(), rsmd.getColumnClassName(2),
                            "(" + i + ") " + testDesc + " - " + testStmt.getClass().getSimpleName() + ":RSMetaData - wrong column(2) type.");

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
     */
    @Test
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

        // Case 8: New session due to user change
        createUser("'testBug71396User'@'%'", "IDENTIFIED BY 'testBug71396User'");
        this.stmt.execute("GRANT SELECT ON *.* TO 'testBug71396User'@'%'");

        testConn = getConnectionWithProps("");
        testStmt = testBug71396StatementInit(testConn, 5);

        ((JdbcConnection) testConn).changeUser("testBug71396User", "testBug71396User");

        Statement testStmtTmp = testConn.createStatement();
        testRS = testStmtTmp.executeQuery("SELECT CURRENT_USER(), @@SESSION.SQL_SELECT_LIMIT");
        assertTrue(testRS.next());
        assertEquals("testBug71396User@%", testRS.getString(1));
        assertTrue(testRS.getBigDecimal(2).compareTo(new BigDecimal(Integer.MAX_VALUE)) == 1,
                String.format("expected:higher than<%d> but was:<%s>", Integer.MAX_VALUE, testRS.getBigDecimal(2)));
        testRS.close();
        testStmtTmp.close();

        testRS = testStmt.executeQuery("SELECT CURRENT_USER(), @@SESSION.SQL_SELECT_LIMIT");
        assertTrue(testRS.next());
        assertEquals("testBug71396User@%", testRS.getString(1));
        assertEquals(new BigDecimal(5), testRS.getBigDecimal(2));
        testRS.close();

        testStmt.close();
        testConn.close();

        // Case 9: New session due to reconnection
        testConn = getConnectionWithProps("");
        testStmt = testBug71396StatementInit(testConn, 5);

        ((JdbcConnection) testConn).createNewIO(true); // true or false argument is irrelevant for this test case

        testStmtTmp = testConn.createStatement();
        testRS = testStmtTmp.executeQuery("SELECT @@SESSION.SQL_SELECT_LIMIT");
        assertTrue(testRS.next());
        assertTrue(testRS.getBigDecimal(1).compareTo(new BigDecimal(Integer.MAX_VALUE)) == 1,
                String.format("expected:higher than<%d> but was:<%s>", Integer.MAX_VALUE, testRS.getBigDecimal(1)));
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
     * Initializes and returns a Statement with maxRows defined. Tests the SQL_SELECT_LIMIT defined. Executing this query also forces this limit to be defined
     * at session level.
     * 
     * @param testConn
     * @param maxRows
     * @return a Statement
     * @throws SQLException
     */
    private Statement testBug71396StatementInit(Connection testConn, int maxRows) throws SQLException {
        ResultSet testRS;
        Statement testStmt = testConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

        testStmt.setMaxRows(maxRows);
        // while consulting SQL_SELECT_LIMIT setting also forces limit to be applied into current session
        testRS = testStmt.executeQuery("SELECT @@SESSION.SQL_SELECT_LIMIT");
        testRS.next();
        assertEquals(maxRows, testRS.getInt(1), "Wrong @@SESSION.SQL_SELECT_LIMIT");

        return testStmt;
    }

    /**
     * Executes a set of queries using a Statement (newly created) and tests if the results count is the expected.
     * 
     * @param testConn
     * @param queries
     * @param expRowCount
     * @throws SQLException
     */
    private void testBug71396StatementMultiCheck(Connection testConn, String[] queries, int[] expRowCount) throws SQLException {
        if (queries.length != expRowCount.length) {
            fail("Bad arguments!");
        }
        Statement testStmt = testConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        testBug71396StatementMultiCheck(testStmt, queries, expRowCount);
        testStmt.close();
    }

    /**
     * Executes a set of queries using a Statement and tests if the results count is the expected.
     * 
     * @param testStmt
     * @param queries
     * @param expRowCount
     * @throws SQLException
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
     * 
     * @param testStmt
     * @param query
     * @param expRowCount
     * @throws SQLException
     */
    private void testBug71396StatementCheck(Statement testStmt, String query, int expRowCount) throws SQLException {
        ResultSet testRS;

        testRS = testStmt.executeQuery(query);
        assertTrue(testRS.last());
        assertEquals(expRowCount, testRS.getRow(), String.format("Wrong number of rows for query '%s'", query));
        testRS.close();

        testStmt.execute(query);
        testRS = testStmt.getResultSet();
        assertTrue(testRS.last());
        assertEquals(expRowCount, testRS.getRow(), String.format("Wrong number of rows for query '%s'", query));
        testRS.close();
    }

    /**
     * Initializes and returns an array of PreparedStatements, with maxRows defined, for a set of queries.
     * 
     * @param testConn
     * @param queries
     * @param maxRows
     * @return a PreparedStatement array
     * @throws SQLException
     */
    private PreparedStatement[] testBug71396PrepStatementInit(Connection testConn, String[] queries, int maxRows) throws SQLException {
        PreparedStatement[] testPStmt = new PreparedStatement[queries.length];

        for (int i = 0; i < queries.length; i++) {
            testPStmt[i] = testConn.prepareStatement(queries[i], ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            if (maxRows > 0) {
                testPStmt[i].setMaxRows(maxRows);
            }
        }
        return testPStmt;
    }

    /**
     * Closes all PreparedStatements in the array.
     * 
     * @param testPStmt
     * @throws SQLException
     */
    private void testBug71396PrepStatementClose(PreparedStatement[] testPStmt) throws SQLException {
        for (Statement testStmt : testPStmt) {
            testStmt.close();
        }
    }

    /**
     * Executes a set of queries using newly created PreparedStatements and tests if the results count is the expected.
     * 
     * @param testConn
     * @param queries
     * @param expRowCount
     * @throws SQLException
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
     * 
     * @param testPStmt
     * @param queries
     * @param expRowCount
     * @throws SQLException
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
     * Executes one query using a newly created PreparedStatement, setting its maxRows limit, and tests if the results count is the expected.
     * 
     * @param testConn
     * @param query
     * @param expRowCount
     * @param maxRows
     * @throws SQLException
     */
    private void testBug71396PrepStatementCheck(Connection testConn, String query, int expRowCount, int maxRows) throws SQLException {
        PreparedStatement chkPStmt;

        chkPStmt = testConn.prepareStatement(query, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        if (maxRows > 0) {
            chkPStmt.setMaxRows(maxRows);
        }
        testBug71396PrepStatementCheck(chkPStmt, query, expRowCount);
        chkPStmt.close();
    }

    /**
     * Executes one query using a PreparedStatement and tests if the results count is the expected.
     * 
     * @param testPStmt
     * @param query
     * @param expRowCount
     * @throws SQLException
     */
    private void testBug71396PrepStatementCheck(PreparedStatement testPStmt, String query, int expRowCount) throws SQLException {
        ResultSet testRS;

        testRS = testPStmt.executeQuery();
        assertTrue(testRS.last());
        assertEquals(expRowCount, testRS.getRow(), String.format("Wrong number of rows for query '%s'", query));
        testRS.close();

        testPStmt.execute();
        testRS = testPStmt.getResultSet();
        assertTrue(testRS.last());
        assertEquals(expRowCount, testRS.getRow(), String.format("Wrong number of rows for query '%s'", query));
        testRS.close();
    }

    /**
     * Executes a query containing the clause LIMIT with a Statement and a PreparedStatement, using a combination of Connection properties, maxRows value and
     * limit clause value, and tests if the results count is the expected.
     * 
     * @param connProps
     * @param maxRows
     * @param limitClause
     * @param expRowCount
     * @throws SQLException
     */
    private void testBug71396MultiSettingsCheck(String connProps, int maxRows, int limitClause, int expRowCount) throws SQLException {
        Connection testConn = getConnectionWithProps(connProps);

        Statement testStmt = testConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        if (maxRows > 0) {
            testStmt.setMaxRows(maxRows);
        }
        testStmt.execute("SELECT 1"); // force limit to be applied into current session

        testBug71396StatementCheck(testStmt, String.format("SELECT * FROM testBug71396 LIMIT %d", limitClause), expRowCount);
        testBug71396PrepStatementCheck(testConn, String.format("SELECT * FROM testBug71396 LIMIT %d", limitClause), expRowCount, maxRows);

        testStmt.close();
        testConn.close();
    }

    /**
     * Tests fix for 18091639 - STRINGINDEXOUTOFBOUNDSEXCEPTION IN PREPAREDSTATEMENT.SETTIMESTAMP WITH 5.6.15
     * 
     * @throws SQLException
     */
    @Test
    public void testBug18091639() throws SQLException {
        String str = TimeUtil.formatNanos(900000000, 1);
        assertEquals("9", str);

        str = TimeUtil.formatNanos(90000000, 1);
        assertEquals("0", str);

        str = TimeUtil.formatNanos(900000000, 0);
        assertEquals("0", str);

        assertThrows(WrongArgumentException.class, "fsp value must be in 0 to 6 range but was 9", new Callable<Void>() {
            public Void call() throws Exception {
                TimeUtil.formatNanos(1, 9);
                return null;
            }
        });

        str = TimeUtil.formatNanos(1, 6);
        assertEquals("0", str);

        str = TimeUtil.formatNanos(1999, 6);
        assertEquals("000001", str);

        str = TimeUtil.formatNanos(123999, 3);
        assertEquals("0", str);

        str = TimeUtil.formatNanos(123999, 4);
        assertEquals("0001", str);

        assertThrows(WrongArgumentException.class, "nanos value must be in 0 to 999999999 range but was 1000000010", new Callable<Void>() {
            public Void call() throws Exception {
                TimeUtil.formatNanos(1000000010, 6);
                return null;
            }
        });

        str = TimeUtil.formatNanos(100000000, 6);
        assertEquals("1", str);
    }

    /**
     * Tests fix for Bug#66947 (16004987) - Calling ServerPreparedStatement.close() twice corrupts cached statements
     * 
     * @throws Exception
     */
    @Test
    public void testBug66947() throws Exception {

        Connection con = null;
        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
            props.setProperty(PropertyKey.cachePrepStmts.getKeyName(), "true");
            props.setProperty(PropertyKey.prepStmtCacheSize.getKeyName(), "2");

            con = getConnectionWithProps(props);

            PreparedStatement ps1_1;
            PreparedStatement ps1_2;

            String query = "Select 'a' from dual";

            ps1_1 = con.prepareStatement(query);
            ps1_1.execute();
            ps1_1.close();

            ps1_2 = con.prepareStatement(query);
            assertSame(ps1_1, ps1_2, "SSPS should be taken from cache but is not the same.");
            ps1_2.execute();
            ps1_2.close();
            ps1_2.close(); // doesn't matter how many times close() is called.
            ps1_2.close();

            ps1_1 = con.prepareStatement(query);
            assertSame(ps1_2, ps1_1, "SSPS should be taken from cache but is not the same.");
            ps1_1.execute();
            ps1_1.close();
            ps1_1.close();

            // check that removeEldestEntry doesn't remove elements twice
            PreparedStatement ps2_1;
            PreparedStatement ps2_2;
            PreparedStatement ps3_1;
            PreparedStatement ps3_2;

            ps1_1 = con.prepareStatement("Select 'b' from dual");
            ps1_1.execute();
            ps1_1.close();
            ps2_1 = con.prepareStatement("Select 'c' from dual");
            ps2_1.execute();
            ps2_1.close();
            ps3_1 = con.prepareStatement("Select 'd' from dual");
            ps3_1.execute();
            ps3_1.close();

            ps1_2 = con.prepareStatement("Select 'b' from dual");
            assertNotSame(ps1_1, ps1_2, "SSPS should not be taken from cache but is the same.");

            ps2_2 = con.prepareStatement("Select 'c' from dual");
            assertSame(ps2_1, ps2_2, "SSPS should be taken from cache but is not the same.");

            ps3_2 = con.prepareStatement("Select 'd' from dual");
            assertSame(ps3_1, ps3_2, "SSPS should be taken from cache but is not the same.");

        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    /**
     * Tests fix for BUG#68916 - closeOnCompletion doesn't work.
     * 
     * This test requires help and timezone tables in mysql database to be initialized, see http://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html and
     * http://dev.mysql.com/doc/refman/5.7/en/server-side-help-support.html
     * 
     * @throws Exception
     */
    @Test
    public void testBug68916() throws Exception {
        // Prepare common test objects
        createProcedure("testBug68916_proc", "() BEGIN SELECT 1; SELECT 2; SELECT 3; END");
        createTable("testBug68916_tbl", "(fld1 INT NOT NULL AUTO_INCREMENT, fld2 INT, PRIMARY KEY(fld1))");

        // STEP 1: Test using standard connection (no properties)
        subTestBug68916ForStandardConnection();

        // STEP 2: Test using connection property holdResultsOpenOverStatementClose=true
        subTestBug68916ForHoldResultsOpenOverStatementClose();

        // STEP 3: Test using connection property dontTrackOpenResources=true
        subTestBug68916ForDontTrackOpenResources();

        // STEP 4: Test using connection property allowMultiQueries=true
        subTestBug68916ForAllowMultiQueries();

        // STEP 5: Test concurrent Statement/ResultSet sharing same Connection
        subTestBug68916ForConcurrency();
    }

    private void subTestBug68916ForStandardConnection() throws Exception {
        Connection testConnection = this.conn;
        String testStep;
        ResultSet testResultSet1, testResultSet2, testResultSet3;

        // We are testing against code that was compiled with Java 6, so methods isCloseOnCompletion() and
        // closeOnCompletion() aren't available in the Statement interface. We need to test directly our implementations.
        StatementImpl testStatement = null;
        PreparedStatement testPrepStatement = null;
        CallableStatement testCallStatement = null;

        /*
         * Testing with standard connection (no properties)
         */
        testStep = "Standard Connection";

        /*
         * SUB-STEP 0: The basics (connection without properties)
         */
        // **testing Statement**
        // ResultSets should be closed when owning Statement is closed
        testStatement = (StatementImpl) testConnection.createStatement();

        assertFalse(testStatement.isCloseOnCompletion(), testStep + ".ST:0. Statement.isCloseOnCompletion(): false by default.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): false.");

        testStatement.closeOnCompletion();

        assertTrue(testStatement.isCloseOnCompletion(), testStep + ".ST:0. Statement.isCloseOnCompletion(): true after Statement.closeOnCompletion().");
        assertFalse(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): false.");

        testStatement.closeOnCompletion();

        assertTrue(testStatement.isCloseOnCompletion(), testStep + ".ST:0. Statement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().");

        // test Statement.close()
        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:0. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): false.");

        testStatement.close();

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:0. ResultSet.isClosed(): true after Statement.Close().");
        assertTrue(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): true after Statement.Close().");

        // **testing PreparedStatement**
        // ResultSets should be closed when owning PreparedStatement is closed
        testPrepStatement = testConnection.prepareStatement("SELECT 1");

        assertFalse(testPrepStatement.isCloseOnCompletion(), testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): false by default.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): false.");

        testPrepStatement.closeOnCompletion();

        assertTrue(testPrepStatement.isCloseOnCompletion(),
                testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after Statement.closeOnCompletion().");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): false.");

        testPrepStatement.closeOnCompletion();

        assertTrue(testPrepStatement.isCloseOnCompletion(),
                testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().");

        // test PreparedStatement.close()
        testPrepStatement.execute();
        testResultSet1 = testPrepStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:0. ResultSet.isClosed(): false.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): false.");

        testPrepStatement.close();

        assertTrue(testResultSet1.isClosed(), testStep + ".PS:0. ResultSet.isClosed(): true after PreparedStatement.close().");
        assertTrue(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): true after PreparedStatement.close().");

        /*
         * SUB-STEP 1: One ResultSet (connection without properties)
         */
        // **testing Statement**
        // Statement using closeOnCompletion should be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        while (testResultSet1.next()) {
        }

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): false after ResultSet have reached the end.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.");

        // test implicit resultset close, keeping statement open, when following with an executeBatch()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.addBatch("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)");
        testStatement.executeBatch();

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true after executeBatch() in same Statement.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.");

        // test implicit resultset close keeping statement open, when following with an executeUpdate()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", Statement.RETURN_GENERATED_KEYS);

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true after executeUpdate() in same Statement.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.");

        // **testing PreparedStatement**
        // PreparedStatement using closeOnCompletion should be closed when last ResultSet is closed
        testPrepStatement = testConnection.prepareStatement("SELECT 1");
        testPrepStatement.closeOnCompletion();

        testResultSet1 = testPrepStatement.executeQuery();

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:1. ResultSet.isClosed(): false.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:1. PreparedStatement.isClosed(): false.");

        while (testResultSet1.next()) {
        }

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:1. ResultSet.isClosed(): false after ResultSet have reached the end.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:1. PreparedStatement.isClosed(): false.");

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".PS:1. ResultSet.isClosed(): true.");
        assertTrue(testPrepStatement.isClosed(), testStep + ".PS:1. PreparedStatement.isClosed(): true when last ResultSet is closed.");

        /*
         * SUB-STEP 2: Multiple ResultSets, sequentially (connection without properties)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testResultSet2 = testStatement.executeQuery("SELECT 2"); // closes testResultSet1

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true after 2nd Statement.executeQuery().");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        while (testResultSet2.next()) {
        }

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false after ResultSet have reached the end.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        testResultSet3 = testStatement.executeQuery("SELECT 3"); // closes testResultSet2

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true after 3rd Statement.executeQuery().");
        assertFalse(testResultSet3.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        testResultSet3.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertTrue(testResultSet3.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): true when last ResultSet is closed.");

        /*
         * SUB-STEP 3: Multiple ResultSets, returned at once (connection without properties)
         */
        // **testing Statement**
        // Statement using closeOnCompletion should be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        assertTrue(testStatement.execute("CALL testBug68916_proc"), testStep + ".ST:3. There should be some ResultSets.");
        testResultSet1 = testStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false.");

        assertTrue(testStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT), testStep + ".ST:3. There should be more ResultSets.");
        testResultSet2 = testStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false.");

        assertTrue(testStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS), testStep + ".ST:3. There should be more ResultSets.");
        testResultSet3 = testStatement.getResultSet();

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertFalse(testResultSet3.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false.");

        // no more ResultSets, must close Statement
        assertFalse(testStatement.getMoreResults(), testStep + ".ST:3. There should be no more ResultSets.");

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet3.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true after last Satement.getMoreResults().");
        assertTrue(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): true when last ResultSet is closed.");

        // **testing CallableStatement**
        // CallableStatement using closeOnCompletion should be closed when last ResultSet is closed
        testCallStatement = testConnection.prepareCall("CALL testBug68916_proc");
        testCallStatement.closeOnCompletion();

        assertTrue(testCallStatement.execute(), testStep + ".CS:3. There should be some ResultSets.");
        testResultSet1 = testCallStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false.");

        assertTrue(testCallStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT), testStep + ".CS:3. There should be more ResultSets.");
        testResultSet2 = testCallStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).");
        assertFalse(testResultSet2.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false.");

        assertTrue(testCallStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS), testStep + ".CS:3. There should be more ResultSets.");
        testResultSet3 = testCallStatement.getResultSet();

        assertTrue(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertTrue(testResultSet2.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertFalse(testResultSet3.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false.");

        // no more ResultSets, must close Statement
        assertFalse(testCallStatement.getMoreResults(), testStep + ".CS:3. There should be no more ResultSets.");

        assertTrue(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet3.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true after last Satement.getMoreResults().");
        assertTrue(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): true when last ResultSet is closed.");

        /*
         * SUB-STEP 4: Generated Keys ResultSet (connection without properties)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testResultSet1.next(), testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): false.");

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): true when last ResultSet is closed.");

        // test again and combine with simple query
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (4), (5), (6)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testResultSet1.next(), testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.");

        testResultSet2 = testStatement.executeQuery("SELECT 2");

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true after executeQuery() in same Statement.");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): false.");

        testResultSet2.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): true when last ResultSet is closed.");
    }

    private void subTestBug68916ForHoldResultsOpenOverStatementClose() throws Exception {
        Connection testConnection;
        String testStep;
        ResultSet testResultSet1, testResultSet2, testResultSet3;

        // We are testing against code that was compiled with Java 6, so methods isCloseOnCompletion() and
        // closeOnCompletion() aren't available in the Statement interface. We need to test directly our
        // implementations.
        StatementImpl testStatement = null;
        PreparedStatement testPrepStatement = null;
        CallableStatement testCallStatement = null;

        /*
         * Testing with connection property holdResultsOpenOverStatementClose=true
         */
        testStep = "Conn. Prop. 'holdResultsOpenOverStatementClose'";
        testConnection = getConnectionWithProps("holdResultsOpenOverStatementClose=true");

        /*
         * SUB-STEP 0: The basics (holdResultsOpenOverStatementClose=true)
         */
        // **testing Statement**
        // ResultSets should stay open when owning Statement is closed
        testStatement = (StatementImpl) testConnection.createStatement();

        assertFalse(testStatement.isCloseOnCompletion(), testStep + ".ST:0. Statement.isCloseOnCompletion(): false dy default.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): false.");

        testStatement.closeOnCompletion();

        assertTrue(testStatement.isCloseOnCompletion(), testStep + ".ST:0. Statement.isCloseOnCompletion(): true after Statement.closeOnCompletion().");
        assertFalse(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): false.");

        testStatement.closeOnCompletion();

        assertTrue(testStatement.isCloseOnCompletion(), testStep + ".ST:0. Statement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().");

        // test Statement.close()
        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:0. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): false.");

        testStatement.close();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:0. ResultSet.isClosed(): false after Statement.Close().");
        assertTrue(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): true after Statement.Close().");

        // **testing PreparedStatement**
        // ResultSets should stay open when owning PreparedStatement is closed
        testPrepStatement = testConnection.prepareStatement("SELECT 1");

        assertFalse(testPrepStatement.isCloseOnCompletion(), testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): false by default.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): false.");

        testPrepStatement.closeOnCompletion();

        assertTrue(testPrepStatement.isCloseOnCompletion(),
                testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after Statement.closeOnCompletion().");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): false.");

        testPrepStatement.closeOnCompletion();

        assertTrue(testPrepStatement.isCloseOnCompletion(),
                testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().");

        // test PreparedStatement.close()
        testPrepStatement.execute();
        testResultSet1 = testPrepStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:0. ResultSet.isClosed(): false.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): false.");

        testPrepStatement.close();

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:0. ResultSet.isClosed(): false after PreparedStatement.close().");
        assertTrue(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): true after PreparedStatement.close().");

        /*
         * SUB-STEP 1: One ResultSet (holdResultsOpenOverStatementClose=true)
         */
        // **testing Statement**
        // Statement using closeOnCompletion should be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        while (testResultSet1.next()) {
        }

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): false after ResultSet have reached the end.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.");

        // test implicit resultset close keeping statement open, when following with an executeBatch()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.addBatch("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)");
        testStatement.executeBatch();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): false after executeBatch() in same Statement.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.");

        // test implicit resultset close keeping statement open, when following with an executeUpdate()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", Statement.RETURN_GENERATED_KEYS);

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): false after executeUpdate() in same Statement.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.");

        // **testing PreparedStatement**
        // PreparedStatement using closeOnCompletion should be closed when last ResultSet is closed
        testPrepStatement = testConnection.prepareStatement("SELECT 1");
        testPrepStatement.closeOnCompletion();

        testResultSet1 = testPrepStatement.executeQuery();

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:1. ResultSet.isClosed(): false.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:1. PreparedStatement.isClosed(): false.");

        while (testResultSet1.next()) {
        }

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:1. ResultSet.isClosed(): false after ResultSet have reached the end.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:1. PreparedStatement.isClosed(): false.");

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".PS:1. ResultSet.isClosed(): true.");
        assertTrue(testPrepStatement.isClosed(), testStep + ".PS:1. PreparedStatement.isClosed(): true when last ResultSet is closed.");

        /*
         * SUB-STEP 2: Multiple ResultSets, sequentially (holdResultsOpenOverStatementClose=true)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testResultSet2 = testStatement.executeQuery("SELECT 2"); // mustn't close testResultSet1

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false after 2nd Statement.executeQuery().");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        while (testResultSet2.next()) {
        }

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false after ResultSet have reached the end.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        testResultSet3 = testStatement.executeQuery("SELECT 3"); // mustn't close testResultSet1 nor testResultSet2

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false after 3rd Statement.executeQuery().");
        assertFalse(testResultSet3.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        testResultSet2.close();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertFalse(testResultSet3.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        testResultSet1.close();
        testResultSet3.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertTrue(testResultSet3.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): true when last ResultSet is closed.");

        /*
         * SUB-STEP 3: Multiple ResultSets, returned at once (holdResultsOpenOverStatementClose=true)
         */
        // **testing Statement**
        // Statement using closeOnCompletion should be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        assertTrue(testStatement.execute("CALL testBug68916_proc"), testStep + ".ST:3. There should be some ResultSets.");
        testResultSet1 = testStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false.");

        assertTrue(testStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT), testStep + ".ST:3. There should be more ResultSets.");
        testResultSet2 = testStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false.");

        assertTrue(testStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS), testStep + ".ST:3. There should be more ResultSets.");
        testResultSet3 = testStatement.getResultSet();

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertFalse(testResultSet3.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false.");

        // no more ResultSets, must close Statement
        assertFalse(testStatement.getMoreResults(), testStep + ".ST:3. There should be no more ResultSets.");

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet3.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true after last Satement.getMoreResults().");
        assertTrue(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): true when last ResultSet is closed.");

        // **testing CallableStatement**
        // CallableStatement using closeOnCompletion should be closed when last ResultSet is closed
        testCallStatement = testConnection.prepareCall("CALL testBug68916_proc");
        testCallStatement.closeOnCompletion();

        assertTrue(testCallStatement.execute(), testStep + ".CS:3. There should be some ResultSets.");
        testResultSet1 = testCallStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false.");

        assertTrue(testCallStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT), testStep + ".CS:3. There should be more ResultSets.");
        testResultSet2 = testCallStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).");
        assertFalse(testResultSet2.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false.");

        assertTrue(testCallStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS), testStep + ".CS:3. There should be more ResultSets.");
        testResultSet3 = testCallStatement.getResultSet();

        assertTrue(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertTrue(testResultSet2.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertFalse(testResultSet3.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false.");

        // no more ResultSets, must close Statement
        assertFalse(testCallStatement.getMoreResults(), testStep + ".CS:3. There should be no more ResultSets.");

        assertTrue(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet3.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true after last Satement.getMoreResults().");
        assertTrue(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): true when last ResultSet is closed.");

        /*
         * SUB-STEP 4: Generated Keys ResultSet (holdResultsOpenOverStatementClose=true)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testResultSet1.next(), testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): false.");

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): true when last ResultSet is closed.");

        // test again and combine with simple query
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (4), (5), (6)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testResultSet1.next(), testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.");

        testResultSet2 = testStatement.executeQuery("SELECT 2");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): false after executeQuery() in same Statement.");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): false.");

        testResultSet2.close();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): false.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): false when last ResultSet is closed (still one open).");

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): true when last ResultSet is closed.");

        testConnection.close();
    }

    private void subTestBug68916ForDontTrackOpenResources() throws Exception {
        Connection testConnection;
        String testStep;
        ResultSet testResultSet1, testResultSet2, testResultSet3;

        // We are testing against code that was compiled with Java 6, so methods isCloseOnCompletion() and
        // closeOnCompletion() aren't available in the Statement interface. We need to test directly our
        // implementations.
        StatementImpl testStatement = null;
        PreparedStatement testPrepStatement = null;
        CallableStatement testCallStatement = null;

        /*
         * Testing with connection property dontTrackOpenResources=true
         */
        testStep = "Conn. Prop. 'dontTrackOpenResources'";
        testConnection = getConnectionWithProps("dontTrackOpenResources=true");

        /*
         * SUB-STEP 0: The basics (dontTrackOpenResources=true)
         */
        // **testing Statement**
        // ResultSets should stay open when owning Statement is closed
        testStatement = (StatementImpl) testConnection.createStatement();

        assertFalse(testStatement.isCloseOnCompletion(), testStep + ".ST:0. Statement.isCloseOnCompletion(): false by default.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): false.");

        testStatement.closeOnCompletion();

        assertTrue(testStatement.isCloseOnCompletion(), testStep + ".ST:0. Statement.isCloseOnCompletion(): true after Statement.closeOnCompletion().");
        assertFalse(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): false.");

        testStatement.closeOnCompletion();

        assertTrue(testStatement.isCloseOnCompletion(), testStep + ".ST:0. Statement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().");

        // test Statement.close()
        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:0. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): false.");

        testStatement.close();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:0. ResultSet.isClosed(): false after Statement.Close().");
        assertTrue(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): true after Statement.Close().");

        // **testing PreparedStatement**
        // ResultSets should stay open when owning PreparedStatement is closed
        testPrepStatement = testConnection.prepareStatement("SELECT 1");

        assertFalse(testPrepStatement.isCloseOnCompletion(), testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): false by default.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): false.");

        testPrepStatement.closeOnCompletion();

        assertTrue(testPrepStatement.isCloseOnCompletion(),
                testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after Statement.closeOnCompletion().");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): false.");

        testPrepStatement.closeOnCompletion();

        assertTrue(testPrepStatement.isCloseOnCompletion(),
                testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().");

        // test PreparedStatement.close()
        testPrepStatement.execute();
        testResultSet1 = testPrepStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:0. ResultSet.isClosed(): false.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): false.");

        testPrepStatement.close();

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:0. ResultSet.isClosed(): false after PreparedStatement.close().");
        assertTrue(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): true after PreparedStatement.close().");

        /*
         * SUB-STEP 1: One ResultSet (dontTrackOpenResources=true)
         */
        // **testing Statement**
        // Statement, although using closeOnCompletion, shouldn't be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        while (testResultSet1.next()) {
        }

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): false after ResultSet have reached the end.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        testResultSet1.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false when last ResultSet is closed.");

        // test implicit resultset (not) close, keeping statement open, when following with an executeBatch()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.addBatch("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)");
        testStatement.executeBatch();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): false after executeBatch() in same Statement.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false when last ResultSet is closed.");

        // test implicit resultset (not) close keeping statement open, when following with an executeUpdate()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", Statement.RETURN_GENERATED_KEYS);

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): false after executeUpdate() in same Statement.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false when last ResultSet is closed.");

        // **testing PreparedStatement**
        // PreparedStatement, although using closeOnCompletion, shouldn't be closed when last ResultSet is closed
        testPrepStatement = testConnection.prepareStatement("SELECT 1");
        testPrepStatement.closeOnCompletion();

        testResultSet1 = testPrepStatement.executeQuery();

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:1. ResultSet.isClosed(): false.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:1. PreparedStatement.isClosed(): false.");

        while (testResultSet1.next()) {
        }

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:1. ResultSet.isClosed(): false after ResultSet have reached the end.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:1. PreparedStatement.isClosed(): false.");

        testResultSet1.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testResultSet1.isClosed(), testStep + ".PS:1. ResultSet.isClosed(): true.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:1. PreparedStatement.isClosed(): false when last ResultSet is closed.");

        /*
         * SUB-STEP 2: Multiple ResultSets, sequentially (dontTrackOpenResources=true)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testResultSet2 = testStatement.executeQuery("SELECT 2"); // mustn't close testResultSet1

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false after 2nd Statement.executeQuery().");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        while (testResultSet2.next()) {
        }

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false after ResultSet have reached the end.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        testResultSet3 = testStatement.executeQuery("SELECT 3"); // mustn't close testResultSet1 nor testResultSet2

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false after 3rd Statement.executeQuery().");
        assertFalse(testResultSet3.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        testResultSet2.close();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertFalse(testResultSet3.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        testResultSet1.close();
        testResultSet3.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertTrue(testResultSet3.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false when last ResultSet is closed.");

        /*
         * SUB-STEP 3: Multiple ResultSets, returned at once (dontTrackOpenResources=true)
         */
        // **testing Statement**
        // Statement, although using closeOnCompletion, shouldn't be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        assertTrue(testStatement.execute("CALL testBug68916_proc"), testStep + ".ST:3. There should be some ResultSets.");
        testResultSet1 = testStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false.");

        assertTrue(testStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT), testStep + ".ST:3. There should be more ResultSets.");
        testResultSet2 = testStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false.");

        assertTrue(testStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS), testStep + ".ST:3. There should be more ResultSets.");
        testResultSet3 = testStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertFalse(testResultSet3.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false.");

        assertFalse(testStatement.getMoreResults(), testStep + ".ST:3. There should be no more ResultSets.");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testResultSet3.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false after last Satement.getMoreResults().");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false after last Satement.getMoreResults().");

        // since open ResultSets aren't tracked, we need to close all manually
        testResultSet1.close();
        testResultSet2.close();
        testResultSet3.close();
        // although there are no more ResultSets, Statement mustn't be closed

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet3.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false when last ResultSet is closed.");

        // **testing CallableStatement**
        // CallableStatement, although using closeOnCompletion, shouldn't be closed when last ResultSet is closed
        testCallStatement = testConnection.prepareCall("CALL testBug68916_proc");
        testCallStatement.closeOnCompletion();

        assertTrue(testCallStatement.execute(), testStep + ".CS:3. There should be some ResultSets.");
        testResultSet1 = testCallStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false.");

        assertTrue(testCallStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT), testStep + ".CS:3. There should be more ResultSets.");
        testResultSet2 = testCallStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).");
        assertFalse(testResultSet2.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false.");

        assertTrue(testCallStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS), testStep + ".CS:3. There should be more ResultSets.");
        testResultSet3 = testCallStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertFalse(testResultSet2.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertFalse(testResultSet3.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false.");

        assertFalse(testCallStatement.getMoreResults(), testStep + ".CS:3. There should be no more ResultSets.");

        assertFalse(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testResultSet2.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testResultSet3.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false after last Satement.getMoreResults().");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false after last Satement.getMoreResults().");

        // since open ResultSets aren't tracked, we need to close all manually
        testResultSet1.close();
        testResultSet2.close();
        testResultSet3.close();
        // although there are no more ResultSets, Statement mustn't be closed

        assertTrue(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet3.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true.");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false when last ResultSet is closed.");

        /*
         * SUB-STEP 4: Generated Keys ResultSet (dontTrackOpenResources=true)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testResultSet1.next(), testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): false.");

        testResultSet1.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): false when last ResultSet is closed.");

        // test again and combine with simple query
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (4), (5), (6)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testResultSet1.next(), testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.");

        testResultSet2 = testStatement.executeQuery("SELECT 2");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): false after executeQuery() in same Statement.");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): false.");

        testResultSet2.close();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): false.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): false when last ResultSet is closed (still one open).");

        testResultSet1.close(); // although it's last open ResultSet, Statement mustn't be closed

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): false when last ResultSet is closed.");

        testConnection.close();
    }

    private void subTestBug68916ForAllowMultiQueries() throws Exception {
        Connection testConnection;
        String testStep;
        ResultSet testResultSet1, testResultSet2, testResultSet3;

        // We are testing against code that was compiled with Java 6, so methods isCloseOnCompletion() and
        // closeOnCompletion() aren't available in the Statement interface. We need to test directly our
        // implementations.
        StatementImpl testStatement = null;
        PreparedStatement testPrepStatement = null;
        CallableStatement testCallStatement = null;

        /*
         * Testing with connection property allowMultiQueries=true
         */
        testStep = "Conn. Prop. 'allowMultiQueries'";
        testConnection = getConnectionWithProps("allowMultiQueries=true");

        /*
         * SUB-STEP 0: The basics (allowMultiQueries=true)
         */
        // **testing Statement**
        // ResultSets should be closed when owning Statement is closed
        testStatement = (StatementImpl) testConnection.createStatement();

        assertFalse(testStatement.isCloseOnCompletion(), testStep + ".ST:0. Statement.isCloseOnCompletion(): false by default.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): false.");

        testStatement.closeOnCompletion();

        assertTrue(testStatement.isCloseOnCompletion(), testStep + ".ST:0. Statement.isCloseOnCompletion(): true after Statement.closeOnCompletion().");
        assertFalse(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): false.");

        testStatement.closeOnCompletion();

        assertTrue(testStatement.isCloseOnCompletion(), testStep + ".ST:0. Statement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().");

        // test Statement.close()
        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:0. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): false.");

        testStatement.close();

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:0. ResultSet.isClosed(): true after Statement.Close().");
        assertTrue(testStatement.isClosed(), testStep + ".ST:0. Statement.isClosed(): true after Statement.Close().");

        // **testing PreparedStatement**
        // ResultSets should be closed when owning PreparedStatement is closed
        testPrepStatement = testConnection.prepareStatement("SELECT 1");

        assertFalse(testPrepStatement.isCloseOnCompletion(), testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): false by default.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): false.");

        testPrepStatement.closeOnCompletion();

        assertTrue(testPrepStatement.isCloseOnCompletion(),
                testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after Statement.closeOnCompletion().");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): false.");

        testPrepStatement.closeOnCompletion();

        assertTrue(testPrepStatement.isCloseOnCompletion(),
                testStep + ".PS:0. PreparedStatement.isCloseOnCompletion(): true after 2nd Statement.closeOnCompletion().");

        // test PreparedStatement.close()
        testPrepStatement.execute();
        testResultSet1 = testPrepStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:0. ResultSet.isClosed(): false.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): false.");

        testPrepStatement.close();

        assertTrue(testResultSet1.isClosed(), testStep + ".PS:0. ResultSet.isClosed(): true after PreparedStatement.close().");
        assertTrue(testPrepStatement.isClosed(), testStep + ".PS:0. PreparedStatement.isClosed(): true after PreparedStatement.close().");

        /*
         * SUB-STEP 1: One ResultSet (allowMultiQueries=true)
         */
        // **testing Statement**
        // Statement using closeOnCompletion should be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        while (testResultSet1.next()) {
        }

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): false after ResultSet have reached the end.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.");

        // test implicit resultset close, keeping statement open, when following with an executeBatch()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.addBatch("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)");
        testStatement.executeBatch();

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true after executeBatch() in same Statement.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.");

        // test implicit resultset close keeping statement open, when following with an executeUpdate()
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3)", Statement.RETURN_GENERATED_KEYS);

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true after executeUpdate() in same Statement.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): false.");

        testResultSet1 = testStatement.getGeneratedKeys();
        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:1. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:1. Statement.isClosed(): true when last ResultSet is closed.");

        // **testing PreparedStatement**
        // PreparedStatement using closeOnCompletion should be closed when last ResultSet is closed
        testPrepStatement = testConnection.prepareStatement("SELECT 1");
        testPrepStatement.closeOnCompletion();

        testResultSet1 = testPrepStatement.executeQuery();

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:1. ResultSet.isClosed(): false.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:1. PreparedStatement.isClosed(): false.");

        while (testResultSet1.next()) {
        }

        assertFalse(testResultSet1.isClosed(), testStep + ".PS:1. ResultSet.isClosed(): false after ResultSet have reached the end.");
        assertFalse(testPrepStatement.isClosed(), testStep + ".PS:1. PreparedStatement.isClosed(): false.");

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".PS:1. ResultSet.isClosed(): true.");
        assertTrue(testPrepStatement.isClosed(), testStep + ".PS:1. PreparedStatement.isClosed(): true when last ResultSet is closed.");

        /*
         * SUB-STEP 2: Multiple ResultSets, sequentially (allowMultiQueries=true)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1");
        testResultSet2 = testStatement.executeQuery("SELECT 2; SELECT 3"); // closes testResultSet1

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true after 2nd Statement.executeQuery().");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        while (testResultSet2.next()) {
        }

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false after ResultSet have reached the end.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        assertTrue(testStatement.getMoreResults(), testStep + ".ST:3. There should be more ResultSets."); // closes
                                                                                                         // testResultSet2
        testResultSet3 = testStatement.getResultSet();

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true after Statement.getMoreResults().");
        assertFalse(testResultSet3.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): false.");

        testResultSet3.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertTrue(testResultSet3.isClosed(), testStep + ".ST:2. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:2. Statement.isClosed(): true when last ResultSet is closed.");

        /*
         * SUB-STEP 3: Multiple ResultSets, returned at once (allowMultiQueries=true)
         */
        // **testing Statement**
        // Statement using closeOnCompletion should be closed when last ResultSet is closed
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testResultSet1 = testStatement.executeQuery("SELECT 1; SELECT 2; SELECT 3");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false.");

        assertTrue(testStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT), testStep + ".ST:3. There should be more ResultSets.");
        testResultSet2 = testStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false.");

        assertTrue(testStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS), testStep + ".ST:3. There should be more ResultSets.");
        testResultSet3 = testStatement.getResultSet();

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertFalse(testResultSet3.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): false.");

        // no more ResultSets, must close Statement
        assertFalse(testStatement.getMoreResults(), testStep + ".ST:3. There should be no more ResultSets.");

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet3.isClosed(), testStep + ".ST:3. ResultSet.isClosed(): true after last Satement.getMoreResults().");
        assertTrue(testStatement.isClosed(), testStep + ".ST:3. Statement.isClosed(): true when last ResultSet is closed.");

        // **testing CallableStatement**
        // CallableStatement using closeOnCompletion should be closed when last ResultSet is closed
        testCallStatement = testConnection.prepareCall("CALL testBug68916_proc");
        testCallStatement.closeOnCompletion();

        assertTrue(testCallStatement.execute(), testStep + ".CS:3. There should be some ResultSets.");
        testResultSet1 = testCallStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false.");

        assertTrue(testCallStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT), testStep + ".CS:3. There should be more ResultSets.");
        testResultSet2 = testCallStatement.getResultSet();

        assertFalse(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false after Statement.getMoreResults(Statement.KEEP_CURRENT_RESULT).");
        assertFalse(testResultSet2.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false.");

        assertTrue(testCallStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS), testStep + ".CS:3. There should be more ResultSets.");
        testResultSet3 = testCallStatement.getResultSet();

        assertTrue(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertTrue(testResultSet2.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true after Statement.getMoreResults(Statement.CLOSE_ALL_RESULTS).");
        assertFalse(testResultSet3.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): false.");
        assertFalse(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): false.");

        // no more ResultSets, must close Statement
        assertFalse(testCallStatement.getMoreResults(), testStep + ".CS:3. There should be no more ResultSets.");

        assertTrue(testResultSet1.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true.");
        assertTrue(testResultSet3.isClosed(), testStep + ".CS:3. ResultSet.isClosed(): true after last Satement.getMoreResults().");
        assertTrue(testCallStatement.isClosed(), testStep + ".CS:3. CallableStatement.isClosed(): true when last ResultSet is closed.");

        /*
         * SUB-STEP 4: Generated Keys ResultSet (allowMultiQueries=true)
         */
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (1), (2), (3); INSERT INTO testBug68916_tbl (fld2) VALUES (4), (5), (6)",
                Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testResultSet1.next(), testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.");

        assertFalse(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): false.");

        testResultSet1.close(); // last open ResultSet, must close Statement

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertTrue(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): true when last ResultSet is closed.");

        // test again and combine with simple query
        testStatement = (StatementImpl) testConnection.createStatement();
        testStatement.closeOnCompletion();

        testStatement.executeUpdate("INSERT INTO testBug68916_tbl (fld2) VALUES (4), (5), (6)", Statement.RETURN_GENERATED_KEYS);

        testResultSet1 = testStatement.getGeneratedKeys();
        assertTrue(testResultSet1.next(), testStep + ".ST:4. Statement.getGeneratedKeys(): should return some values.");

        testResultSet2 = testStatement.executeQuery("SELECT 2; SELECT 3");

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true after executeQuery() in same Statement.");
        assertFalse(testResultSet2.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): false.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): false.");

        // last open ResultSet won't close the Statement
        // because we didn't fetch the next one (SELECT 3)
        testResultSet2.close();

        assertTrue(testResultSet1.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertTrue(testResultSet2.isClosed(), testStep + ".ST:4. ResultSet.isClosed(): true.");
        assertFalse(testStatement.isClosed(), testStep + ".ST:4. Statement.isClosed(): true when last ResultSet is closed.");
        testStatement.close();

        testConnection.close();
    }

    private void subTestBug68916ForConcurrency() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        CompletionService<String> complService = new ExecutorCompletionService<>(executor);

        String[] connectionProperties = new String[] { "", "holdResultsOpenOverStatementClose=true", "dontTrackOpenResources=true" };
        // overridesCloseOnCompletion[n] refers to the effect of connectionProperties[n] on
        // Statement.closeOnCompletion()
        boolean[] overridesCloseOnCompletion = new boolean[] { false, false, true };
        String[] sampleQueries = new String[] { "SELECT * FROM mysql.help_topic", "SELECT SLEEP(1)",
                "SELECT * FROM mysql.time_zone tz INNER JOIN mysql.time_zone_name tzn ON tz.time_zone_id = tzn.time_zone_id "
                        + "INNER JOIN mysql.time_zone_transition tzt ON tz.time_zone_id = tzt.time_zone_id "
                        + "INNER JOIN mysql.time_zone_transition_type tztt ON tzt.time_zone_id = tztt.time_zone_id "
                        + "AND tzt.transition_type_id = tztt.transition_type_id ORDER BY tzn.name , tztt.abbreviation , tzt.transition_time",
                "SELECT 1" };
        int threadCount = sampleQueries.length;

        for (int c = 0; c < connectionProperties.length; c++) {
            System.out.println("Test Connection with property '" + connectionProperties[c] + "'");
            Connection testConnection = getConnectionWithProps(connectionProperties[c]);

            for (int t = 0; t < threadCount; t++) {
                complService.submit(new subTestBug68916ConcurrentTask(testConnection, sampleQueries[t], overridesCloseOnCompletion[c]));
            }

            for (int t = 0; t < threadCount; t++) {
                try {
                    System.out.println("   " + complService.take().get());
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                } catch (ExecutionException ex) {
                    if (ex.getCause() instanceof Error) {
                        // let JUnit try to report as Failure instead of Error
                        throw (Error) ex.getCause();
                    }
                }
            }

            testConnection.close();
        }
    }

    private class subTestBug68916ConcurrentTask implements Callable<String> {
        Connection testConnection = null;
        String query = null;
        boolean closeOnCompletionIsOverriden = false;

        subTestBug68916ConcurrentTask(Connection testConnection, String query, boolean closeOnCompletionIsOverriden) {
            this.testConnection = testConnection;
            this.query = query;
            this.closeOnCompletionIsOverriden = closeOnCompletionIsOverriden;
        }

        public String call() throws Exception {
            String threadName = Thread.currentThread().getName();
            long startTime = System.currentTimeMillis();
            long stopTime = startTime;
            StatementImpl testStatement = null;
            int count1 = 0;

            try {
                testStatement = (StatementImpl) this.testConnection.createStatement();
                testStatement.closeOnCompletion();

                System.out.println(threadName + " is executing: " + this.query);
                ResultSet testResultSet = testStatement.executeQuery(this.query);
                while (testResultSet.next()) {
                    count1++;
                }
                assertTrue(count1 > 0, threadName + ": Query should return some values.");
                assertFalse(testStatement.isClosed(), threadName + ": Statement shouldn't be closed.");

                testResultSet.close(); // should close statement if not closeOnCompletionIsOverriden
                if (this.closeOnCompletionIsOverriden) {
                    assertFalse(testStatement.isClosed(), threadName + ": Statement shouldn't be closed.");
                } else {
                    assertTrue(testStatement.isClosed(), threadName + ": Statement should be closed.");
                }

            } catch (SQLException e) {
                e.printStackTrace();
                fail(threadName + ": Something went wrong, maybe Connection or Statement was closed before its time.");

            } finally {
                if (testStatement != null) {
                    try {
                        testStatement.close();
                    } catch (SQLException e) {
                    }
                }
                stopTime = System.currentTimeMillis();
            }
            return threadName + ": processed " + count1 + " rows in " + (stopTime - startTime) + " milliseconds.";
        }
    }

    /**
     * Tests fix for Bug#71672 - Every SQL statement is checked if it contains "ON DUPLICATE KEY UPDATE" or not
     * 
     * @throws SQLException
     */
    @Test
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

        // Test multiple connection props
        do {
            switch (++testStep) {
                case 1:
                    testConn = getConnectionWithProps("");
                    expectedUpdCount = expectedUpdCountDef;
                    expectedGenKeys = expectedGenKeysForChkODKU;
                    expectedGenKeysBatchStmt = expectedGenKeys;
                    expectedUpdCountBatchPStmt = expectedUpdCountBatchPStmtNoRW;
                    expectedGenKeysBatchPStmt = expectedGenKeysForBatchPStmtChkODKU;
                    break;
                case 2:
                    testConn = getConnectionWithProps("dontCheckOnDuplicateKeyUpdateInSQL=true");
                    expectedUpdCount = expectedUpdCountDef;
                    expectedGenKeys = expectedGenKeysForNoChkODKU;
                    expectedGenKeysBatchStmt = expectedGenKeys;
                    expectedUpdCountBatchPStmt = expectedUpdCountBatchPStmtNoRW;
                    expectedGenKeysBatchPStmt = expectedGenKeysForBatchPStmtNoChkODKU;
                    break;
                case 3:
                    testConn = getConnectionWithProps("rewriteBatchedStatements=true");
                    expectedUpdCount = expectedUpdCountDef;
                    expectedGenKeys = expectedGenKeysForChkODKU;
                    expectedGenKeysBatchStmt = expectedGenKeysForBatchStmtRW;
                    expectedUpdCountBatchPStmt = expectedUpdCountBatchPStmtRW;
                    expectedGenKeysBatchPStmt = expectedGenKeysForBatchPStmtRW;
                    break;
                case 4:
                    // dontCheckOnDuplicateKeyUpdateInSQL=true is canceled by rewriteBatchedStatements=true
                    testConn = getConnectionWithProps("rewriteBatchedStatements=true,dontCheckOnDuplicateKeyUpdateInSQL=true");
                    expectedUpdCount = expectedUpdCountDef;
                    expectedGenKeys = expectedGenKeysForChkODKU;
                    expectedGenKeysBatchStmt = expectedGenKeysForBatchStmtRW;
                    expectedUpdCountBatchPStmt = expectedUpdCountBatchPStmtRW;
                    expectedGenKeysBatchPStmt = expectedGenKeysForBatchPStmtRW;
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
            assertEquals(expectedUpdCount.length, res.length, testStep + ". Satement.executeBatch() result");
            for (int i = 0; i < expectedUpdCount.length; i++) {
                assertEquals(expectedUpdCount[i], res[i], testStep + "." + i + ". Satement.executeBatch() result");
            }
            testRS = testStmt.getGeneratedKeys();
            for (int i = 0; i < expectedGenKeysBatchStmt.length; i++) {
                for (int j = 0; j < expectedGenKeysBatchStmt[i].length; j++) {
                    assertTrue(testRS.next(), testStep + ". Row expected in generated keys ResultSet");
                    assertEquals(expectedGenKeysBatchStmt[i][j], testRS.getInt(1), testStep + ".[" + i + "][" + j + "]. Wrong generated key");
                }
            }
            assertFalse(testRS.next(), testStep + ". No more rows expected in generated keys ResultSet");
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
            assertEquals(expectedUpdCountBatchPStmt.length, res.length, testStep + ". PreparedSatement.executeBatch() result");
            for (int i = 0; i < expectedUpdCountBatchPStmt.length; i++) {
                assertEquals(expectedUpdCountBatchPStmt[i], res[i], testStep + "." + i + ". PreparedSatement.executeBatch() result");
            }
            testRS = testPStmt.getGeneratedKeys();
            for (int i = 0; i < expectedGenKeysBatchPStmt.length; i++) {
                assertTrue(testRS.next(), testStep + ". Row expected in generated keys ResultSet");
                assertEquals(expectedGenKeysBatchPStmt[i], testRS.getInt(1), testStep + ".[" + i + "]. Wrong generated key");
            }
            assertFalse(testRS.next(), testStep + ". No more rows expected in generated keys ResultSet");
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
     * 
     * @param testStep
     * @param testConn
     * @param query
     * @param expectedUpdateCount
     * @param expectedKeys
     * @throws SQLException
     */
    public void testBug71672Statement(int testStep, Connection testConn, String query, int expectedUpdateCount, int[] expectedKeys) throws SQLException {
        Statement testStmt = testConn.createStatement();

        if (expectedUpdateCount < 0) {
            assertFalse(testStmt.execute(query, Statement.RETURN_GENERATED_KEYS), testStep + ". Stmt.execute() result");
        } else {
            assertEquals(expectedUpdateCount, testStmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS), testStep + ". Stmt.executeUpdate() result");
        }

        ResultSet testRS = testStmt.getGeneratedKeys();
        for (int k : expectedKeys) {
            assertTrue(testRS.next(), testStep + ". Row expected in generated keys ResultSet");
            assertEquals(k, testRS.getInt(1), testStep + ". Wrong generated key");
        }
        assertFalse(testRS.next(), testStep + ". No more rows expected in generated keys ResultSet");
        testRS.close();
        testStmt.close();
    }

    /**
     * Check the update count and returned keys for an INSERT query using a PreparedStatement object. If expectedUpdateCount < 0 then runs
     * PreparedStatement.execute() otherwise PreparedStatement.executeUpdate().
     * 
     * @param testStep
     * @param testConn
     * @param query
     * @param expectedUpdateCount
     * @param expectedKeys
     * @throws SQLException
     */
    public void testBug71672PreparedStatement(int testStep, Connection testConn, String query, int expectedUpdateCount, int[] expectedKeys)
            throws SQLException {
        PreparedStatement testPStmt = testConn.prepareStatement(query);

        if (expectedUpdateCount < 0) {
            assertFalse(testPStmt.execute(query, Statement.RETURN_GENERATED_KEYS), testStep + ". PrepStmt.execute() result");
        } else {
            assertEquals(expectedUpdateCount, testPStmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS), testStep + ". PrepStmt.executeUpdate() result");
        }

        ResultSet testRS = testPStmt.getGeneratedKeys();
        for (int k : expectedKeys) {
            assertTrue(testRS.next(), testStep + ". Row expected in generated keys ResultSet");
            assertEquals(k, testRS.getInt(1), testStep + ". Wrong generated key");
        }
        assertFalse(testRS.next(), testStep + ". No more rows expected in generated keys ResultSet");
        testRS.close();
        testPStmt.close();
    }

    /**
     * Tests fix for BUG#71923 - Incorrect generated keys if ON DUPLICATE KEY UPDATE not exact
     * 
     * @throws Exception
     */
    @Test
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
            assertTrue(this.rs.next(), c + ".A Statement.execute() - generated keys row expected");
            assertEquals(3, this.rs.getInt(1), c + ".A Statement.execute() - wrong generated key value");
            assertFalse(this.rs.next(), c + ".A Statement.execute() - no more generated keys rows expected");
            this.rs.close();

            dropTable("testBug71923");

            // B. test Statement.executeUpdate
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertEquals(3, this.stmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.stmt.getGeneratedKeys();
            assertTrue(this.rs.next(), c + ".B Statement.executeUpdate() - generated keys row expected");
            assertEquals(3, this.rs.getInt(1), c + ".B Statement.executeUpdate() - wrong generated key value");
            assertFalse(this.rs.next(), c + ".B Statement.executeUpdate() - no more generated keys rows expected");
            this.rs.close();

            // prepare statement for next tet cases
            this.pstmt = this.conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);

            // C. test PreparedStatment.execute()
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertFalse(this.pstmt.execute(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.pstmt.getGeneratedKeys();
            assertTrue(this.rs.next(), c + ".C PreparedStatment.execute() - generated keys row expected");
            assertEquals(3, this.rs.getInt(1), c + ".C PreparedStatment.execute() - wrong generated key value");
            assertFalse(this.rs.next(), c + ".C PreparedStatment.execute() - no more generated keys rows expected");
            this.rs.close();

            dropTable("testBug71923");

            // D. test PreparedStatment.executeUpdate
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertEquals(3, this.pstmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.pstmt.getGeneratedKeys();
            assertTrue(this.rs.next(), c + ".D PreparedStatment.executeUpdate() - generated keys row expected");
            assertEquals(3, this.rs.getInt(1), c + ".D PreparedStatment.executeUpdate() - wrong generated key value");
            assertFalse(this.rs.next(), c + ".D PreparedStatment.executeUpdate() - no more generated keys rows expected");
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
            assertTrue(this.rs.next(), c + ".E Statement.execute() - generated keys 1st row expected");
            assertEquals(3, this.rs.getInt(1), c + ".E Statement.execute() - wrong 1st generated key value");
            assertTrue(this.rs.next(), c + ".E Statement.execute() - generated keys 2nd row expected");
            assertEquals(4, this.rs.getInt(1), c + ".E Statement.execute() - wrong 2nd generated key value");
            assertFalse(this.rs.next(), c + ".E Statement.execute() - no more generated keys rows expected");
            this.rs.close();

            dropTable("testBug71923");

            // F. test Statement.executeUpdate
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertEquals(2, this.stmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.stmt.getGeneratedKeys();
            assertTrue(this.rs.next(), c + ".F Statement.execute() - generated keys 1st row expected");
            assertEquals(3, this.rs.getInt(1), c + ".F Statement.execute() - wrong 1st generated key value");
            assertTrue(this.rs.next(), c + ".F Statement.execute() - generated keys 2nd row expected");
            assertEquals(4, this.rs.getInt(1), c + ".F Statement.execute() - wrong 2nd generated key value");
            assertFalse(this.rs.next(), c + ".F Statement.execute() - no more generated keys rows expected");
            this.rs.close();

            // prepare statement for next tet cases
            this.pstmt = this.conn.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);

            // G. test PreparedStatment.execute()
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertFalse(this.pstmt.execute(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.pstmt.getGeneratedKeys();
            assertTrue(this.rs.next(), c + ".G PreparedStatment.execute() - generated keys 1st row expected");
            assertEquals(3, this.rs.getInt(1), c + ".G PreparedStatment.execute() - wrong 1st generated key value");
            assertTrue(this.rs.next(), c + ".G PreparedStatment.execute() - generated keys 2nd row expected");
            assertEquals(4, this.rs.getInt(1), c + ".G PreparedStatment.execute() - wrong 2nd generated key value");
            assertFalse(this.rs.next(), c + ".G PreparedStatment.execute() - no more generated keys rows expected");
            this.rs.close();

            dropTable("testBug71923");

            // H. test PreparedStatment.executeUpdate
            createTable("testBug71923", tableDDL);
            assertEquals(2, this.stmt.executeUpdate(defaultQuery));

            assertEquals(2, this.pstmt.executeUpdate(query, Statement.RETURN_GENERATED_KEYS));
            this.rs = this.pstmt.getGeneratedKeys();
            assertTrue(this.rs.next(), c + ".H PreparedStatment.executeUpdate() - generated keys 1st row expected");
            assertEquals(3, this.rs.getInt(1), c + ".H PreparedStatment.executeUpdate() - wrong 1st generated key value");
            assertTrue(this.rs.next(), c + ".H PreparedStatment.executeUpdate() - generated keys 2nd row expected");
            assertEquals(4, this.rs.getInt(1), c + ".H PreparedStatment.executeUpdate() - wrong 2nd generated key value");
            assertFalse(this.rs.next(), c + ".H PreparedStatment.executeUpdate() - no more generated keys rows expected");
            this.rs.close();

            dropTable("testBug71923");
        }
    }

    /**
     * Tests fix for BUG#73163 - IndexOutOfBoundsException thrown preparing statement.
     * 
     * This bug occurs only if running with Java6+.
     * 
     * @throws Exception
     */
    @Test
    public void testBug73163() throws Exception {
        try {
            this.stmt = this.conn.prepareStatement("LOAD DATA INFILE ? INTO TABLE testBug73163");
        } catch (SQLException e) {
            if (e.getCause() instanceof IndexOutOfBoundsException) {
                fail("IOOBE thrown in Java6+ while preparing a LOAD DATA statement with placeholders.");
            } else {
                throw e;
            }
        }
    }

    /**
     * Tests fix for BUG#74998 - readRemainingMultiPackets not computed correctly for rows larger than 16 MB.
     * 
     * This bug is observed only when a multipacket uses packets 127 and 128. It happens due to the transition from positive to negative values in a signed byte
     * numeric value (127 + 1 == -128).
     * 
     * The test case forces a multipacket to use packets 127, 128 and 129, where packet 129 is 0-length, this being another boundary case.
     * Query (*1) generates the following MySQL protocol packets from the server:
     * - Packets 1 to 4 contain protocol control data and results metadata info. (*2)
     * - Packets 5 to 126 contain each row "X". (*3)
     * - Packets 127 to 129 contain row "Y..." as a multipacket (size("Y...") = 32*1024*1024-15 requires 3 packets). (*4)
     * - Packet 130 contains row "Z". (*5)
     * 
     * @throws Exception
     */
    @Test
    public void testBug74998() throws Exception {
        int maxAllowedPacketAtServer = Integer.parseInt(((JdbcConnection) this.conn).getSession().getServerSession().getServerVariable("max_allowed_packet"));
        int maxAllowedPacketMinimumForTest = 32 * 1024 * 1024;
        if (maxAllowedPacketAtServer < maxAllowedPacketMinimumForTest) {
            fail("You need to increase max_allowed_packet to at least " + maxAllowedPacketMinimumForTest + " before running this test!");
        }

        createTable("testBug74998", "(id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY, data LONGBLOB)"); // (*2)

        StringBuilder query = new StringBuilder("INSERT INTO testBug74998 (data) VALUES ('X')");
        for (int i = 0; i < 121; i++) {
            query.append(",('X')");
        }
        assertEquals(122, this.stmt.executeUpdate(query.toString())); // (*3)

        int lengthOfRowForMultiPacket = maxAllowedPacketMinimumForTest - 15; // 32MB - 15Bytes causes an empty packet at the end of the multipacket sequence

        this.stmt.executeUpdate("INSERT INTO testBug74998 (data) VALUES (REPEAT('Y', " + lengthOfRowForMultiPacket + "))"); // (*4)
        this.stmt.executeUpdate("INSERT INTO testBug74998 (data) VALUES ('Z')"); // (*5)

        try {
            this.rs = this.stmt.executeQuery("SELECT id, data FROM testBug74998 ORDER BY id"); // (*1)
        } catch (CJCommunicationsException | CommunicationsException e) {
            if (e.getCause() instanceof IOException && "Packets received out of order".compareTo(e.getCause().getMessage()) == 0) {
                fail("Failed to correctly fetch all data from communications layer due to wrong processing of muli-packet number.");
            } else {
                throw e;
            }
        }

        // safety check
        for (int i = 1; i <= 122; i++) {
            assertTrue(this.rs.next());
            assertEquals(i, this.rs.getInt(1));
            assertEquals("X", this.rs.getString(2));
        }
        assertTrue(this.rs.next());
        assertEquals(123, this.rs.getInt(1));
        assertEquals("YYYYY", this.rs.getString(2).substring(0, 5));
        assertEquals("YYYYY", this.rs.getString(2).substring(lengthOfRowForMultiPacket - 5));
        assertTrue(this.rs.next());
        assertEquals(124, this.rs.getInt(1));
        assertEquals("Z", this.rs.getString(2));
        assertFalse(this.rs.next());
    }

    /**
     * Tests fix for BUG#50348 - mysql connector/j 5.1.10 render the wrong value for dateTime column in GMT DB.
     * 
     * With the right time zone settings in server and client, and using the property 'useTimezone=true', time shifts are computed in the opposite direction of
     * those that are computed otherwise.
     * 
     * This issue is observed when the server is configured with time zone 'GMT' and the client other than 'GMT'. However, if the server's time zone is one
     * equivalent to 'GMT' but under a different identifier, say "UTC" or "GMT+00", the wrong behavior isn't observed anymore.
     * 
     * @throws Exception
     */
    @Test
    public void testBug50348() throws Exception {
        final TimeZone defaultTZ = TimeZone.getDefault();

        final Properties testConnProps = new Properties();
        testConnProps.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        testConnProps.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");
        testConnProps.setProperty(PropertyKey.preserveInstants.getKeyName(), "true");

        Connection testConn = null;

        try {
            TimeZone.setDefault(TimeZone.getTimeZone("America/Chicago")); // ~~ CST (UTC-06)
            final SimpleDateFormat tsFormat = TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd HH:mm:ss", null);
            final Timestamp timestamp = new Timestamp(tsFormat.parse("2015-01-01 10:00:00").getTime());
            final SimpleDateFormat tFormat = TimeUtil.getSimpleDateFormat(null, "HH:mm:ss", null);
            final Time time = new Time(tFormat.parse("10:00:00").getTime());

            // Test a number of time zones that coincide with 'GMT' on the some specific point in time.
            for (String tz : new String[] { "Europe/Lisbon", "UTC", "GMT+00", "GMT" }) {
                //  Europe/Lisbon ~~ WET (UTC) on 2015-01-01; ~~ CET (UTC+01) on 1970-01-01
                System.out.println("\nServer time zone: " + tz);
                System.out.println("---------------------------------------------------");

                testConnProps.setProperty(PropertyKey.connectionTimeZone.getKeyName(), tz);
                testConn = getConnectionWithProps(testConnProps);

                checkResultSetForTestBug50348(testConn, "2015-01-01 04:00:00.0", "10:00:00");
                checkPreparedStatementForTestBug50348(testConn, timestamp, time, "2015-01-01 16:00:00", "10:00:00");

                testConn.close();
            }

            // Cycle through a wide range of generic 'GMT+/-hh:mm' and assert the expected time shift for a specific point in time. 
            for (int tzOffset = -15; tzOffset <= 15; tzOffset++) { // cover a wider range than standard
                for (int tzSubOffset : new int[] { 0, 30 }) {
                    final StringBuilder tz = new StringBuilder("GMT");
                    tz.append(tzOffset < 0 ? "-" : "+").append(String.format("%02d", Math.abs(tzOffset)));
                    tz.append(String.format(":%02d", tzSubOffset));

                    System.out.println("\nServer time zone: " + tz.toString());
                    System.out.println("---------------------------------------------------");
                    testConnProps.setProperty(PropertyKey.connectionTimeZone.getKeyName(), tz.toString());
                    testConn = getConnectionWithProps(testConnProps);

                    final int diffTzOffset = tzOffset + 6; // CST offset = -6 hours
                    final Calendar cal = Calendar.getInstance();

                    cal.setTime(tsFormat.parse("2015-01-01 10:00:00"));
                    cal.add(Calendar.HOUR, -diffTzOffset);
                    cal.add(Calendar.MINUTE, tzOffset < 0 ? tzSubOffset : -tzSubOffset);
                    String expectedTimestampFromRS = tsFormat.format(cal.getTime()) + ".0";
                    cal.setTime(tFormat.parse("10:00:00"));
                    cal.add(Calendar.HOUR, -diffTzOffset);
                    cal.add(Calendar.MINUTE, tzOffset < 0 ? tzSubOffset : -tzSubOffset);
                    checkResultSetForTestBug50348(testConn, expectedTimestampFromRS, "10:00:00");

                    cal.setTime(tsFormat.parse("2015-01-01 10:00:00"));
                    cal.add(Calendar.HOUR, diffTzOffset);
                    cal.add(Calendar.MINUTE, tzOffset < 0 ? -tzSubOffset : tzSubOffset);
                    String expectedTimestampFromPS = tsFormat.format(cal.getTime());
                    cal.setTime(tFormat.parse("10:00:00"));
                    cal.add(Calendar.HOUR, diffTzOffset);
                    cal.add(Calendar.MINUTE, tzOffset < 0 ? -tzSubOffset : tzSubOffset);
                    checkPreparedStatementForTestBug50348(testConn, timestamp, time, expectedTimestampFromPS, "10:00:00");

                    testConn.close();
                }
            }
        } finally {
            TimeZone.setDefault(defaultTZ);

            if (testConn != null) {
                testConn.close();
            }
        }
    }

    private void checkResultSetForTestBug50348(Connection testConn, String expectedTimestamp, String expectedTime) throws SQLException {
        this.rs = testConn.createStatement().executeQuery("SELECT '2015-01-01 10:00:00', '10:00:00'");
        this.rs.next();
        String timestampAsString = this.rs.getTimestamp(1).toString();
        String timeAsString = this.rs.getTime(2).toString();
        String alert = expectedTimestamp.equals(timestampAsString) && expectedTime.equals(timeAsString) ? "" : " <-- (!)";
        System.out.printf("[RS] expected: '%s' | '%s'%n", expectedTimestamp, expectedTime);
        System.out.printf("       actual: '%s' | '%s' %s%n", timestampAsString, timeAsString, alert);
        assertEquals(expectedTimestamp, timestampAsString);
        assertEquals(expectedTime, timeAsString);
    }

    private void checkPreparedStatementForTestBug50348(Connection testConn, Timestamp timestamp, Time time, String expectedTimestamp, String expectedTime)
            throws SQLException {
        PreparedStatement testPstmt = testConn.prepareStatement("SELECT ?, ?");
        testPstmt.setTimestamp(1, timestamp);
        testPstmt.setTime(2, time);

        this.rs = testPstmt.executeQuery();
        this.rs.next();
        String timestampAsString = new String(this.rs.getBytes(1));
        String timeAsString = new String(this.rs.getBytes(2));
        String alert = expectedTimestamp.equals(timestampAsString) && expectedTime.equals(timeAsString) ? "" : " <-- (!)";
        System.out.printf("[PS] expected: '%s' | '%s'%n", expectedTimestamp, expectedTime);
        System.out.printf("       actual: '%s' | '%s' %s%n", timestampAsString, timeAsString, alert);
        assertEquals(expectedTimestamp, timestampAsString);
        assertEquals(expectedTime, timeAsString);
    }

    /**
     * Tests fix for Bug#77449 - Add 'truncateFractionalSeconds=true|false' property (contribution).
     * 
     * The property actually added was 'sendFractionalSeconds' and works as the opposite of the proposed one.
     * 
     * @throws Exception
     */
    @Test
    public void testBug77449() throws Exception {
        if (!versionMeetsMinimum(5, 6, 4)) {
            return;
        }

        Timestamp originalTs = new Timestamp(TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd HH:mm:ss.SSS", null).parse("2014-12-31 23:59:59.999").getTime());
        Timestamp roundedTs = new Timestamp(originalTs.getTime() + 1);
        Timestamp truncatedTs = new Timestamp(originalTs.getTime() - 999);
        LocalDateTime originalLdt = LocalDateTime.of(2014, 12, 31, 23, 59, 59, 999000000);
        LocalDateTime roundedLdt = LocalDateTime.of(2015, 1, 1, 0, 0);
        LocalDateTime truncatedLdt = LocalDateTime.of(2014, 12, 31, 23, 59, 59);
        // MySQL can store 24:00:00, but LocalTime cannot.
        // ResultSet#getObject(i, LocalTime.class) might need some adjustment.
        LocalTime originalLt = LocalTime.of(22, 59, 59, 999000000);
        LocalTime roundedLt = LocalTime.of(23, 0, 0);
        LocalTime truncatedLt = LocalTime.of(22, 59, 59);

        assertEquals("2014-12-31 23:59:59.999", originalTs.toString());
        assertEquals("2014-12-31 23:59:59.0", TimeUtil.truncateFractionalSeconds(originalTs).toString());

        createTable("testBug77449", "(id INT PRIMARY KEY, ts_short TIMESTAMP, ts_long TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6))");
        createProcedure("testBug77449", "(ts_short TIMESTAMP, ts_long TIMESTAMP(6)) BEGIN SELECT ts_short, ts_long; END");
        createTable("testBug77449_time", "(id INT PRIMARY KEY, t_short TIME, t_long TIME(6))");
        createProcedure("testBug77449_time", "(t_short TIME, t_long TIME(6)) BEGIN SELECT t_short, t_long; END");

        for (int tst = 0; tst < 8; tst++) {
            boolean useServerPrepStmts = (tst & 0x2) != 0;
            boolean sendFractionalSeconds = (tst & 0x4) != 0;

            String testCase = String.format("Case: %d [ useSSPS=%s | sendFracSecs=%s ]", tst, useServerPrepStmts, sendFractionalSeconds);

            Properties props = new Properties();
            props.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestBug77449QueryInterceptor.class.getName());
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useServerPrepStmts));
            props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), Boolean.toString(sendFractionalSeconds));
            props.setProperty(PropertyKey.preserveInstants.getKeyName(), "false");

            Connection testConn = getConnectionWithProps(props);

            // Send timestamps as Strings, using Statement -> no truncation occurs.
            Statement testStmt = testConn.createStatement();
            testStmt.executeUpdate("INSERT INTO testBug77449 VALUES (1, '2014-12-31 23:59:59.999', '2014-12-31 23:59:59.999')/* no_ts_trunk */");
            testStmt.close();

            // Send timestamps using PreparedStatement -> truncation occurs according to 'sendFractionalSeconds' value.
            PreparedStatement testPStmt = testConn.prepareStatement("INSERT INTO testBug77449 VALUES (2, ?, ?)");
            testPStmt.setTimestamp(1, originalTs);
            testPStmt.setTimestamp(2, originalTs);
            assertEquals(1, testPStmt.executeUpdate(), testCase);
            testPStmt.close();

            // Send timestamps using UpdatableResultSet -> truncation occurs according to 'sendFractionalSeconds' value.
            testStmt = testConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            testStmt.executeUpdate("INSERT INTO testBug77449 VALUES (3, NOW(), NOW())/* no_ts_trunk */"); // insert dummy row
            this.rs = testStmt.executeQuery("SELECT * FROM testBug77449 WHERE id = 3");
            assertTrue(this.rs.next(), testCase);
            this.rs.updateTimestamp("ts_short", originalTs);
            this.rs.updateTimestamp("ts_long", originalTs);
            this.rs.updateRow();
            this.rs.moveToInsertRow();
            this.rs.updateInt("id", 4);
            this.rs.updateTimestamp("ts_short", originalTs);
            this.rs.updateTimestamp("ts_long", originalTs);
            this.rs.insertRow();

            // Send LocalDateTime using PreparedStatement -> truncation occurs according to 'sendFractionalSeconds' value.
            testPStmt = testConn.prepareStatement("INSERT INTO testBug77449 VALUES (5, ?, ?)");
            testPStmt.setObject(1, originalLdt);
            testPStmt.setObject(2, originalLdt);
            assertEquals(1, testPStmt.executeUpdate(), testCase);
            testPStmt.close();

            // Send LocalDateTime using UpdatableResultSet -> truncation occurs according to 'sendFractionalSeconds' value.
            testStmt = testConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            testStmt.executeUpdate("INSERT INTO testBug77449 VALUES (6, NOW(), NOW())/* no_ts_trunk */"); // insert dummy row
            this.rs = testStmt.executeQuery("SELECT * FROM testBug77449 WHERE id = 6");
            assertTrue(this.rs.next(), testCase);
            this.rs.updateObject("ts_short", originalLdt);
            this.rs.updateObject("ts_long", originalLdt);
            this.rs.updateRow();
            this.rs.moveToInsertRow();
            this.rs.updateInt("id", 7);
            this.rs.updateObject("ts_short", originalLdt);
            this.rs.updateObject("ts_long", originalLdt);
            this.rs.insertRow();

            // Assert values from previous inserts/updates.
            // 1st row: from Statement sent as String, no subject to TZ conversions.
            this.rs = testStmt.executeQuery("SELECT * FROM testBug77449 WHERE id = 1");
            assertTrue(this.rs.next(), testCase);
            assertEquals(1, this.rs.getInt(1), testCase);
            assertEquals(roundedTs, this.rs.getTimestamp(2), testCase);
            assertEquals(originalTs, this.rs.getTimestamp(3), testCase);
            // 2nd row: from PreparedStatement; 3rd row: from UpdatableResultSet.updateRow(); 4th row: from UpdatableResultSet.insertRow()
            this.rs = testStmt.executeQuery("SELECT * FROM testBug77449 WHERE id >= 2");
            for (int i = 2; i <= 4; i++) {
                assertTrue(this.rs.next(), testCase);
                assertEquals(i, this.rs.getInt(1), testCase);
                assertEquals(sendFractionalSeconds ? roundedTs : truncatedTs, this.rs.getTimestamp(2), testCase);
                assertEquals(sendFractionalSeconds ? originalTs : truncatedTs, this.rs.getTimestamp(3), testCase);
            }
            // 5th row: from PreparedStatement; 6th row: from UpdatableResultSet.updateRow(); 7th row: from UpdatableResultSet.insertRow()
            for (int i = 5; i <= 7; i++) {
                assertTrue(this.rs.next(), testCase);
                assertEquals(i, this.rs.getInt(1), testCase);
                assertEquals(sendFractionalSeconds ? roundedLdt : truncatedLdt, this.rs.getObject(2, LocalDateTime.class), testCase);
                assertEquals(sendFractionalSeconds ? originalLdt : truncatedLdt, this.rs.getObject(3, LocalDateTime.class), testCase);
            }

            this.stmt.execute("DELETE FROM testBug77449");

            // Send LocalTime using PreparedStatement -> truncation occurs according to 'sendFractionalSeconds' value.
            testPStmt = testConn.prepareStatement("INSERT INTO testBug77449_time VALUES (1, ?, ?)");
            testPStmt.setObject(1, originalLt);
            testPStmt.setObject(2, originalLt);
            assertEquals(1, testPStmt.executeUpdate(), testCase);
            testPStmt.close();
            // Send LocalTime using UpdatableResultSet -> truncation occurs according to 'sendFractionalSeconds' value.
            testStmt = testConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
            testStmt.executeUpdate("INSERT INTO testBug77449_time VALUES (2, NOW(), NOW())/* no_ts_trunk */"); // insert dummy row
            this.rs = testStmt.executeQuery("SELECT * FROM testBug77449_time WHERE id = 2");
            assertTrue(this.rs.next(), testCase);
            this.rs.updateObject("t_short", originalLt);
            this.rs.updateObject("t_long", originalLt);
            this.rs.updateRow();
            this.rs.moveToInsertRow();
            this.rs.updateInt("id", 3);
            this.rs.updateObject("t_short", originalLt);
            this.rs.updateObject("t_long", originalLt);
            this.rs.insertRow();
            // Assert previous LocalTime insertions
            this.rs = this.stmt.executeQuery("SELECT * FROM testBug77449_time");
            for (int i = 1; i <= 3; i++) {
                assertTrue(this.rs.next(), testCase);
                assertEquals(i, this.rs.getInt(1), testCase);
                assertEquals(sendFractionalSeconds ? roundedLt : truncatedLt, this.rs.getObject(2, LocalTime.class), testCase);
                assertEquals(sendFractionalSeconds ? originalLt : truncatedLt, this.rs.getObject(3, LocalTime.class), testCase);
            }

            this.stmt.execute("DELETE FROM testBug77449_time");

            // Compare Connector/J with client truncation -> truncation occurs according to 'sendFractionalSeconds' value.
            testPStmt = testConn.prepareStatement("SELECT ? = ?");
            testPStmt.setTimestamp(1, originalTs);
            testPStmt.setTimestamp(2, truncatedTs);
            this.rs = testPStmt.executeQuery();
            assertTrue(this.rs.next(), testCase);
            if (sendFractionalSeconds) {
                assertFalse(this.rs.getBoolean(1), testCase);
            } else {
                assertTrue(this.rs.getBoolean(1), testCase);
            }
            testPStmt.close();
            // Same test with LocalDateTime
            testPStmt = testConn.prepareStatement("SELECT ? = ?");
            testPStmt.setObject(1, originalLdt);
            testPStmt.setObject(2, truncatedLdt);
            this.rs = testPStmt.executeQuery();
            assertTrue(this.rs.next(), testCase);
            if (sendFractionalSeconds) {
                assertFalse(this.rs.getBoolean(1), testCase);
            } else {
                assertTrue(this.rs.getBoolean(1), testCase);
            }
            testPStmt.close();
            // Same test with LocalTime
            testPStmt = testConn.prepareStatement("SELECT ? = ?");
            testPStmt.setObject(1, originalLt);
            testPStmt.setObject(2, truncatedLt);
            this.rs = testPStmt.executeQuery();
            assertTrue(this.rs.next(), testCase);
            if (sendFractionalSeconds) {
                assertFalse(this.rs.getBoolean(1), testCase);
            } else {
                assertTrue(this.rs.getBoolean(1), testCase);
            }
            testPStmt.close();

            // Send timestamps using CallableStatement -> truncation occurs according to 'sendFractionalSeconds' value.
            CallableStatement cstmt = testConn.prepareCall("{call testBug77449(?, ?)}");
            cstmt.setTimestamp("ts_short", originalTs);
            cstmt.setTimestamp("ts_long", originalTs);
            cstmt.execute();
            this.rs = cstmt.getResultSet();
            assertTrue(this.rs.next(), testCase);
            assertEquals(sendFractionalSeconds ? roundedTs : truncatedTs, this.rs.getTimestamp(1), testCase);
            assertEquals(sendFractionalSeconds ? originalTs : truncatedTs, this.rs.getTimestamp(2), testCase);
            cstmt.close();
            // Same test with LocalDateTime
            cstmt = testConn.prepareCall("{call testBug77449(?, ?)}");
            cstmt.setObject("ts_short", originalLdt);
            cstmt.setObject("ts_long", originalLdt);
            cstmt.execute();
            this.rs = cstmt.getResultSet();
            assertTrue(this.rs.next(), testCase);
            assertEquals(sendFractionalSeconds ? roundedLdt : truncatedLdt, this.rs.getObject(1, LocalDateTime.class), testCase);
            assertEquals(sendFractionalSeconds ? originalLdt : truncatedLdt, this.rs.getObject(2, LocalDateTime.class), testCase);
            cstmt.close();
            // Same test with LocalTime
            cstmt = testConn.prepareCall("{call testBug77449_time(?, ?)}");
            cstmt.setObject("t_short", originalLt);
            cstmt.setObject("t_long", originalLt);
            cstmt.execute();
            this.rs = cstmt.getResultSet();
            assertTrue(this.rs.next(), testCase);
            assertEquals(sendFractionalSeconds ? roundedLt : truncatedLt, this.rs.getObject(1, LocalTime.class), testCase);
            assertEquals(sendFractionalSeconds ? originalLt : truncatedLt, this.rs.getObject(2, LocalTime.class), testCase);
            cstmt.close();

            testConn.close();
        }
    }

    public static class TestBug77449QueryInterceptor extends BaseQueryInterceptor {
        private boolean sendFracSecs = false;

        @Override
        public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
            this.sendFracSecs = Boolean.parseBoolean(props.getProperty(PropertyKey.sendFractionalSeconds.getKeyName()));
            return this;
        }

        @Override
        public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
            if (!(interceptedQuery instanceof ServerPreparedStatement || interceptedQuery instanceof ServerPreparedQuery)) {
                String query = sql.get();
                if (query == null && (interceptedQuery instanceof ClientPreparedStatement || interceptedQuery instanceof ClientPreparedQuery)) {
                    query = interceptedQuery.toString();
                    query = query.substring(query.indexOf(':') + 2);
                }

                if (query != null
                        && ((query.startsWith("INSERT") || query.startsWith("UPDATE") || query.startsWith("CALL")) && !query.contains("no_ts_trunk"))) {
                    if (this.sendFracSecs ^ query.contains(".999")) {
                        fail("Wrong TIMESTAMP trunctation in query [" + query + "]");
                    }
                }
            }
            return super.preProcess(sql, interceptedQuery);
        }
    }

    /**
     * Tests fix for BUG#77681 - rewrite replace sql like insert when rewriteBatchedStatements=true (contribution)
     * 
     * When using 'rewriteBatchedStatements=true' we rewrite several batched statements into one single query by extending its VALUES clause. Although INSERT
     * REPLACE have the same syntax, this wasn't happening for REPLACE statements.
     * 
     * This tests the number of queries actually sent to server when rewriteBatchedStatements is used and not by using a QueryInterceptor. The test is
     * repeated for server side prepared statements. Without the fix, this test fails while checking the number of expected REPLACE queries.
     * 
     * @throws Exception
     */
    @Test
    public void testBug77681() throws Exception {
        createTable("testBug77681", "(id INT, txt VARCHAR(50), PRIMARY KEY (id))");

        Properties props = new Properties();
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestBug77681QueryInterceptor.class.getName());

        for (int tst = 0; tst < 4; tst++) {
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString((tst & 0x1) != 0));
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString((tst & 0x2) != 0));
            Connection testConn = getConnectionWithProps(props);

            PreparedStatement testPstmt = testConn.prepareStatement("INSERT INTO testBug77681 VALUES (?, ?)");
            testPstmt.setInt(1, 1);
            testPstmt.setString(2, "one");
            testPstmt.addBatch();
            testPstmt.setInt(1, 2);
            testPstmt.setString(2, "two");
            testPstmt.addBatch();
            testPstmt.setInt(1, 3);
            testPstmt.setString(2, "three");
            testPstmt.addBatch();
            testPstmt.setInt(1, 4);
            testPstmt.setString(2, "four");
            testPstmt.addBatch();
            testPstmt.setInt(1, 5);
            testPstmt.setString(2, "five");
            testPstmt.addBatch();
            testPstmt.executeBatch();
            testPstmt.close();

            testPstmt = testConn.prepareStatement("REPLACE INTO testBug77681 VALUES (?, ?)");
            testPstmt.setInt(1, 2);
            testPstmt.setString(2, "TWO");
            testPstmt.addBatch();
            testPstmt.setInt(1, 4);
            testPstmt.setString(2, "FOUR");
            testPstmt.addBatch();
            testPstmt.setInt(1, 6);
            testPstmt.setString(2, "SIX");
            testPstmt.addBatch();
            testPstmt.executeBatch();
            testPstmt.close();

            Statement testStmt = testConn.createStatement();
            testStmt.clearBatch();
            testStmt.addBatch("INSERT INTO testBug77681 VALUES (7, 'seven')");
            testStmt.addBatch("INSERT INTO testBug77681 VALUES (8, 'eight')");
            testStmt.addBatch("INSERT INTO testBug77681 VALUES (9, 'nine')");
            testStmt.addBatch("INSERT INTO testBug77681 VALUES (10, 'ten')");
            testStmt.addBatch("INSERT INTO testBug77681 VALUES (11, 'eleven')");
            testStmt.executeBatch();

            testStmt.clearBatch();
            testStmt.addBatch("REPLACE INTO testBug77681 VALUES (8, 'EIGHT')");
            testStmt.addBatch("REPLACE INTO testBug77681 VALUES (10, 'TEN')");
            testStmt.addBatch("REPLACE INTO testBug77681 VALUES (12, 'TWELVE')");
            testStmt.addBatch("REPLACE INTO testBug77681 VALUES (14, 'FOURTEEN')");
            testStmt.addBatch("REPLACE INTO testBug77681 VALUES (16, 'SIXTEEN')");
            testStmt.executeBatch();

            this.stmt.executeUpdate("DELETE FROM testBug77681");
        }
    }

    public static class TestBug77681QueryInterceptor extends BaseQueryInterceptor {
        private static final char[] expectedNonRWBS = new char[] { 'I', 'I', 'I', 'I', 'I', 'R', 'R', 'R', 'I', 'I', 'I', 'I', 'I', 'R', 'R', 'R', 'R', 'R' };
        private static final char[] expectedRWBS = new char[] { 'I', 'R', 'I', 'R' };

        private char[] expected;
        private int execCounter = 0;

        @Override
        public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
            // TODO Auto-generated method stub
            super.init(conn, props, log);
            System.out.println("\nuseServerPrepStmts: " + props.getProperty(PropertyKey.useServerPrepStmts.getKeyName()) + " | rewriteBatchedStatements: "
                    + props.getProperty(PropertyKey.rewriteBatchedStatements.getKeyName()));
            System.out.println("--------------------------------------------------------------------------------");
            this.expected = Boolean.parseBoolean(props.getProperty(PropertyKey.rewriteBatchedStatements.getKeyName())) ? expectedRWBS : expectedNonRWBS;
            return this;
        }

        @Override
        public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
            String query = sql.get();
            if (query == null && interceptedQuery instanceof ClientPreparedStatement) {
                query = interceptedQuery.toString();
                query = query.substring(query.indexOf(':') + 2);
            }
            if (query != null && query.indexOf("testBug77681") != -1) {
                System.out.println(this.execCounter + " --> " + query);
                if (this.execCounter > this.expected.length) {
                    fail("Failed to rewrite statements");
                }
                assertEquals(this.expected[this.execCounter++], query.charAt(0), "Wrong statement at execution number " + this.execCounter);
            }
            return super.preProcess(sql, interceptedQuery);
        }
    }

    /**
     * Tests fix for Bug#21876798 - CONNECTOR/J WITH MYSQL FABRIC AND SPRING PRODUCES PROXY ERROR.
     * 
     * Although this is a Fabric related bug we are able reproduce it using a couple of multi-host connections.
     * 
     * @throws Exception
     */
    @Test
    public void testBug21876798() throws Exception {
        createTable("testBug21876798", "(tst INT, val INT)");

        for (int tst = 0; tst < 4; tst++) {
            boolean useServerPrepStmts = (tst & 0x1) != 0;
            boolean rewriteBatchedStatements = (tst & 0x2) != 0;

            Properties props = new Properties();
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useServerPrepStmts));
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString(rewriteBatchedStatements));

            String testCase = String.format("Case: %d [ %s | %s ]", tst, useServerPrepStmts ? "useSPS" : "-",
                    rewriteBatchedStatements ? "rwBatchedStmts" : "-");

            Connection highLevelConn = getLoadBalancedConnection(props);
            assertTrue(highLevelConn.getClass().getName().startsWith("com.sun.proxy") || highLevelConn.getClass().getName().startsWith("$Proxy"), testCase);

            Connection lowLevelConn = getSourceReplicaReplicationConnection(props);
            // This simulates the behavior from Fabric connections that are causing the problem.
            ((ReplicationConnection) lowLevelConn).setProxy((JdbcConnection) highLevelConn);

            // Insert data. We need at least 4 rows to force rewriting batch statements.
            this.pstmt = lowLevelConn.prepareStatement("INSERT INTO testBug21876798 VALUES (?, ?)");
            for (int i = 1; i <= 4; i++) {
                this.pstmt.setInt(1, tst);
                this.pstmt.setInt(2, i);
                this.pstmt.addBatch();
            }
            this.pstmt.executeBatch();

            // Check if data was inserted correctly.
            this.rs = this.stmt.executeQuery("SELECT val FROM testBug21876798 WHERE tst = " + tst);
            for (int i = 1; i <= 4; i++) {
                assertTrue(this.rs.next(), testCase + "/Row#" + i);
                assertEquals(i, this.rs.getInt(1), testCase + "/Row#" + i);
            }
            assertFalse(this.rs.next(), testCase);

            // Update data. We need at least 4 rows to force rewriting batch statements.
            this.pstmt = lowLevelConn.prepareStatement("UPDATE testBug21876798 SET val = ? WHERE tst = ? AND val = ?");
            for (int i = 1; i <= 4; i++) {
                this.pstmt.setInt(1, -i);
                this.pstmt.setInt(2, tst);
                this.pstmt.setInt(3, i);
                this.pstmt.addBatch();
            }
            this.pstmt.executeBatch();

            // Check if data was updated correctly.
            this.rs = this.stmt.executeQuery("SELECT val FROM testBug21876798 WHERE tst = " + tst);
            for (int i = 1; i <= 4; i++) {
                assertTrue(this.rs.next(), testCase + "/Row#" + i);
                assertEquals(-i, this.rs.getInt(1), testCase + "/Row#" + i);
            }
            assertFalse(this.rs.next(), testCase);

            lowLevelConn.close();
            highLevelConn.close();
        }
    }

    /**
     * Tests fix for Bug#78961 - Can't call MySQL procedure with InOut parameters in Fabric environment.
     * 
     * Although this is a Fabric related bug we are able reproduce it using a couple of multi-host connections.
     * 
     * @throws Exception
     */
    @Test
    public void testBug78961() throws Exception {
        createProcedure("testBug78961", "(IN c1 FLOAT, IN c2 FLOAT, OUT h FLOAT, INOUT t FLOAT) BEGIN SET h = SQRT(c1 * c1 + c2 * c2); SET t = t + h; END;");

        Connection highLevelConn = getLoadBalancedConnection(null);
        assertTrue(highLevelConn.getClass().getName().startsWith("com.sun.proxy") || highLevelConn.getClass().getName().startsWith("$Proxy"));

        Connection lowLevelConn = getSourceReplicaReplicationConnection(null);
        // This simulates the behavior from Fabric connections that are causing the problem.
        ((ReplicationConnection) lowLevelConn).setProxy((JdbcConnection) highLevelConn);

        CallableStatement cstmt = lowLevelConn.prepareCall("{CALL testBug78961 (?, ?, ?, ?)}");
        cstmt.setFloat(1, 3.0f);
        cstmt.setFloat(2, 4.0f);
        cstmt.setFloat(4, 5.0f);
        cstmt.registerOutParameter(3, Types.FLOAT);
        cstmt.registerOutParameter(4, Types.FLOAT);
        cstmt.execute();

        assertEquals(5.0f, cstmt.getFloat(3));
        assertEquals(10.0f, cstmt.getFloat(4));
    }

    /**
     * Test Bug#75956 - Inserting timestamps using a server PreparedStatement and useLegacyDatetimeCode=false
     * 
     * @throws Exception
     */
    @Test
    public void testBug75956() throws Exception {
        createTable("bug75956", "(id int not null primary key auto_increment, dt1 datetime, dt2 datetime)");
        Connection sspsConn = getConnectionWithProps("useCursorFetch=true,useLegacyDatetimeCode=false");
        this.pstmt = sspsConn.prepareStatement("insert into bug75956 (dt1, dt2) values (?, ?)");
        this.pstmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        this.pstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
        this.pstmt.addBatch();
        this.pstmt.clearParameters();
        this.pstmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        this.pstmt.setTimestamp(2, null);
        this.pstmt.addBatch();
        this.pstmt.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
        this.pstmt.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
        this.pstmt.addBatch();
        this.pstmt.executeBatch();
        this.pstmt.close();
        this.rs = sspsConn.createStatement().executeQuery("select count(*) from bug75956 where dt2 is NULL");
        this.rs.next();
        assertEquals(1, this.rs.getInt(1));
        sspsConn.close();
    }

    /**
     * Tests fix for Bug#71131 - Poor error message in CallableStatement.java.
     * 
     * @throws Exception
     */
    @Test
    public void testBug71131() throws Exception {
        createProcedure("testBug71131", "(IN r DOUBLE, OUT p DOUBLE) BEGIN SET p = 2 * r * PI(); END");
        final CallableStatement cstmt = this.conn.prepareCall("{ CALL testBug71131 (?, 5) }");
        assertThrows(SQLException.class, "Parameter p is not registered as an output parameter", new Callable<Void>() {
            public Void call() throws Exception {
                cstmt.execute();
                return null;
            }
        });
        cstmt.close();
    }

    /**
     * Tests fix for Bug#23188498 - CLIENT HANG WHILE USING SERVERPREPSTMT WHEN PROFILESQL=TRUE AND USEIS=TRUE.
     * 
     * @throws Exception
     */
    @Test
    public void testBug23188498() throws Exception {
        createTable("testBug23188498", "(id INT)");

        JdbcConnection testConn = (JdbcConnection) getConnectionWithProps("useServerPrepStmts=true,useInformationSchema=true,profileSQL=true");
        ExecutorService executor = Executors.newSingleThreadExecutor();

        // Insert data:
        this.pstmt = testConn.prepareStatement("INSERT INTO testBug23188498 (id) VALUES (?)");
        this.pstmt.setInt(1, 10);
        final PreparedStatement localPStmt1 = this.pstmt;
        Future<Void> future1 = executor.submit(new Callable<Void>() {
            public Void call() throws Exception {
                localPStmt1.executeUpdate();
                return null;
            }
        });
        try {
            future1.get(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // The connection hung, forcibly closing it releases resources.
            this.stmt.execute("KILL CONNECTION " + testConn.getSession().getThreadId());
            fail("Connection hung after executeUpdate().");
        }
        this.pstmt.close();

        // Fetch data:
        this.pstmt = testConn.prepareStatement("SELECT * FROM testBug23188498 WHERE id > ?");
        this.pstmt.setInt(1, 1);
        final PreparedStatement localPStmt2 = this.pstmt;
        Future<ResultSet> future2 = executor.submit(new Callable<ResultSet>() {
            public ResultSet call() throws Exception {
                return localPStmt2.executeQuery();
            }
        });
        try {
            this.rs = future2.get(5, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            // The connection hung, forcibly closing it releases resources.
            this.stmt.execute("KILL CONNECTION " + testConn.getSession().getThreadId());
            fail("Connection hung after executeQuery().");
        }
        assertTrue(this.rs.next());
        assertEquals(10, this.rs.getInt(1));
        assertFalse(this.rs.next());
        this.pstmt.close();

        executor.shutdownNow();
        testConn.close();
    }

    /**
     * Tests fix for Bug#23201930 - CLIENT HANG WHEN RSLT CUNCURRENCY=CONCUR_UPDATABLE AND RSLTSET TYPE=FORWARD_ONLY.
     * 
     * @throws Exception
     */
    @Test
    public void testBug23201930() throws Exception {
        boolean useSSL = false;
        boolean useSPS = false;
        boolean useCursor = false;
        boolean useCompr = false;

        final char[] chars = new char[32 * 1024];
        Arrays.fill(chars, 'x');
        final String longData = String.valueOf(chars); // Using large data makes SSL connections hang sometimes.

        do {
            final String testCase = String.format("Case [SSL: %s, SPS: %s, Cursor: %s, Compr: %s]", useSSL ? "Y" : "N", useSPS ? "Y" : "N",
                    useCursor ? "Y" : "N", useCompr ? "Y" : "N");

            createTable("testBug23201930", "(id TINYINT AUTO_INCREMENT PRIMARY KEY, f1 INT DEFAULT 1, f2 INT DEFAULT 1, f3 INT DEFAULT 1, "
                    + "f4 INT DEFAULT 1, f5 INT DEFAULT 1, fl LONGBLOB)");

            final Properties props = new Properties();
            props.setProperty(PropertyKey.useSSL.getKeyName(), Boolean.toString(useSSL));
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            if (useSSL) {
                props.setProperty(PropertyKey.requireSSL.getKeyName(), "true");
                props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");
            }
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));
            props.setProperty(PropertyKey.useCursorFetch.getKeyName(), Boolean.toString(useCursor));
            if (useCursor) {
                props.setProperty(PropertyKey.defaultFetchSize.getKeyName(), "1");
            }
            props.setProperty(PropertyKey.useCompression.getKeyName(), Boolean.toString(useCompr));

            final JdbcConnection testConn = (JdbcConnection) getConnectionWithProps(props);

            final ExecutorService executor = Executors.newSingleThreadExecutor();
            final Future<Void> future = executor.submit(new Callable<Void>() {
                public Void call() throws Exception {
                    final Statement testStmt = testConn.createStatement();
                    testStmt.execute("INSERT INTO testBug23201930 (id) VALUES (100)");

                    PreparedStatement testPstmt = testConn.prepareStatement("INSERT INTO testBug23201930 (id, fl) VALUES (?, ?)", ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_UPDATABLE);
                    testPstmt.setObject(1, 101, java.sql.Types.INTEGER);
                    testPstmt.setObject(2, longData, java.sql.Types.VARCHAR);
                    testPstmt.execute();
                    testPstmt.setObject(1, 102, java.sql.Types.INTEGER);
                    testPstmt.execute();
                    testPstmt.close();

                    testPstmt = testConn.prepareStatement("SELECT * FROM testBug23201930 WHERE id >= ? ORDER BY id ASC", ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_UPDATABLE);
                    testPstmt.setObject(1, 100, java.sql.Types.INTEGER);
                    final ResultSet testRs = testPstmt.executeQuery();
                    assertTrue(testRs.next());
                    assertEquals(100, testRs.getInt(1));
                    assertTrue(testRs.next());
                    assertEquals(101, testRs.getInt(1));
                    assertTrue(testRs.next());
                    assertEquals(102, testRs.getInt(1));
                    assertFalse(testRs.next());
                    testPstmt.close();
                    return null;
                }
            });

            try {
                future.get(10, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                // The connection hung, forcibly closing it releases resources.
                this.stmt.executeQuery("KILL CONNECTION " + testConn.getSession().getThreadId());
                fail(testCase + ": Connection hung!");
            }
            executor.shutdownNow();

            testConn.close();
        } while ((useSSL = !useSSL) || (useSPS = !useSPS) || (useCursor = !useCursor) || (useCompr = !useCompr)); // Cycle through all possible combinations.
    }

    /**
     * Tests fix for Bug#80615 - prepared statement leak when rewriteBatchedStatements=true and useServerPrepStmt.
     * 
     * There are two bugs here:
     * 1. A server prepared statement leakage by not actually closing the statement on server when .close() is called in the client side. This occurs when
     * setting 'cachePrepStmts=true&useServerPrepStmts=true' and a prepared statement is set as non-poolable ('setPoolable(false)'). By itself this doesn't
     * cause any visible issue because the connector has a fail-safe mechanism that uses client-side prepared statements when server-side prepared statements
     * fail to be prepared. So, the connector ends up using client-side prepared statements after the number of open prepared statements on server hits the
     * value of 'max_prepared_stmt_count'.
     * 2. A prepared statement fails to be prepared when there are too many open prepared statements on server. By setting the options
     * 'rewriteBatchedStatements=true&useServerPrepStmts=true' when a query happens to be rewritten a new (server-side) prepared statement is required but the
     * fail-safe mechanism isn't implemented in this spot, so, since the leakage described above already consumed all available prepared statements on server,
     * this ends up throwing the exception.
     * 
     * This test combines three elements:
     * 1. Call .close() on a server prepared statement. This promotes a prepared statement for caching if prepared statements cache is enabled.
     * 2. cachePrepStmts=true|false. Turns on/off the prepared statements cache.
     * 3. Call .setPoolable(true|false) on the prepared statement. This allows canceling the prepared statement caching, on a per statement basis. It has no
     * effect if the prepared statements cache if turned off for the current connection.
     * 
     * Expected behavior:
     * - If .close() is not called on server prepared statements then they also can't be promoted for caching. This causes a server prepared statements leak in
     * all remaining combinations.
     * - If .close() is called on server prepared statements and the prepared statements cache is disabled by any form (either per connection or per statement),
     * then the statements is immediately closed on server side too.
     * - If .close() is called on server prepared statements and the prepared statements cache is enabled (both in the connection and in the statement) then the
     * statement is cached and only effectively closed in the server side if and when removed from the cache.
     * 
     * @throws Exception
     */
    @Test
    public void testBug80615() throws Exception {
        final int prepStmtCacheSize = 5;
        final int maxPrepStmtCount = 25;
        final int testRepetitions = maxPrepStmtCount + 5;
        int maxPrepStmtCountOri = -1;

        try {
            // Check if it is possible to create a server prepared statement with the current max_prepared_stmt_count.
            Connection checkConn = getConnectionWithProps("useServerPrepStmts=true");
            PreparedStatement checkPstmt = checkConn.prepareStatement("SELECT 1");
            assertTrue(checkPstmt instanceof ServerPreparedStatement,
                    "Failed to create a server prepared statement possibly because there are too many active prepared statements on server already.");
            checkPstmt.close();

            this.rs = this.stmt.executeQuery("SELECT @@GLOBAL.max_prepared_stmt_count");
            this.rs.next();
            maxPrepStmtCountOri = this.rs.getInt(1);

            this.stmt.execute("SET GLOBAL max_prepared_stmt_count = " + maxPrepStmtCount);
            this.stmt.execute("FLUSH STATUS");

            // Check if it is still possible to prepare new statements after setting the new max. This test requires at least prepStmtCacheSize + 2.
            // The extra two statements are:
            // 1 - The first statement that only gets cached in the end (when calling .close() on it).
            // 2 - The statement that triggers the expelling of the oldest element of the cache to get room for itself.  
            for (int i = 1; i <= prepStmtCacheSize + 2; i++) {
                checkPstmt = checkConn.prepareStatement("SELECT " + i);
                assertTrue(checkPstmt instanceof ServerPreparedStatement,
                        "Test ABORTED because the server doesn't allow preparing at least " + (prepStmtCacheSize + 2) + " more statements.");
            }
            checkConn.close(); // Also closes all prepared statements.

            // Good to go, start the test.
            boolean closeStmt = false;
            boolean useCache = false;
            boolean poolable = false;
            do {
                final String testCase = String.format("Case: [Close STMTs: %s, Use cache: %s, Poolable: %s ]", closeStmt ? "Y" : "N", useCache ? "Y" : "N",
                        poolable ? "Y" : "N");

                System.out.println();
                System.out.println(testCase);
                System.out.println("********************************************************************************");

                createTable("testBug80615", "(id INT)");

                final Properties props = new Properties();
                props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
                props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
                props.setProperty(PropertyKey.cachePrepStmts.getKeyName(), Boolean.toString(useCache));
                if (useCache) {
                    props.setProperty(PropertyKey.prepStmtCacheSize.getKeyName(), String.valueOf(prepStmtCacheSize));
                }

                final Connection testConn = getConnectionWithProps(props);
                final Statement checkStmt = testConn.createStatement();

                // Prepare a statement to be executed later. This is prepare #1.
                PreparedStatement testPstmt1 = testConn.prepareStatement("INSERT INTO testBug80615 VALUES (?)");
                assertTrue(testPstmt1 instanceof ServerPreparedStatement, testCase);
                ((StatementImpl) testPstmt1).setPoolable(poolable); // Need to cast, this is a JDBC 4.0 feature.
                testPstmt1.setInt(1, 100);
                testPstmt1.addBatch();
                testPstmt1.setInt(1, 200);
                testPstmt1.addBatch();

                int prepCount = 1; // One server-side prepared statement already prepared.
                int expectedPrepCount = prepCount;
                int expectedExecCount = 0;
                int expectedCloseCount = 0;

                testBug80615CheckComStmtStatus(prepCount, true, testCase, checkStmt, expectedPrepCount, expectedExecCount, expectedCloseCount);

                // Prepare a number of statements higher than the limit set on server. There are at most (*) maxPrepStmtCount - 1 prepares available.
                // This should exhaust the number of allowed prepared statements, forcing the connector to use client-side prepared statements from that point
                // forward unless statements are closed correctly.
                // Under the tested circumstances there where some unexpected server prepared statements leaks (1st bug).
                // (*) There's no canonical way of knowing exactly how many preparing statement slots are available because other sessions may be using them.
                boolean isSPS = true;
                do {
                    PreparedStatement testPstmt2 = testConn.prepareStatement("INSERT INTO testBug80615 VALUES (" + prepCount + " + ?)");
                    prepCount++;

                    isSPS = testPstmt2 instanceof ServerPreparedStatement;
                    if (closeStmt) {
                        // Statements are being correctly closed so there is room to create new ones every time.
                        assertTrue(isSPS, testCase);
                    } else if (prepCount > maxPrepStmtCount) {
                        // Not closing statements causes a server prepared statements leak on server.
                        // In this iteration (if not before) it should have started failing-over to a client-side prepared statement.
                        assertFalse(isSPS, testCase);
                    } else if (prepCount <= prepStmtCacheSize + 2) {
                        // There should be enough room to prepare server-side prepared statements. (This was checked in the beginning.)
                        assertTrue(isSPS, testCase);
                    } // prepStmtCacheSize + 1 < prepCount <= maxPrepStmtCount --> can't assert anything as there can statements prepared externally.

                    ((StatementImpl) testPstmt2).setPoolable(poolable); // Need to cast, this is a JDBC 4.0 feature.
                    testPstmt2.setInt(1, 0);
                    testPstmt2.execute();
                    if (isSPS) {
                        expectedPrepCount++;
                        expectedExecCount++;
                    }
                    if (closeStmt) {
                        testPstmt2.close();
                        if (isSPS) {
                            if (useCache && poolable && (prepCount - 1) > prepStmtCacheSize) { // The first statement isn't cached yet.
                                // A statement (oldest in cache) is effectively closed on server side only after local statements cache is full.
                                expectedCloseCount++;
                            } else if (!useCache || !poolable) {
                                // The statement is closed immediately on server side.
                                expectedCloseCount++;
                            }
                        }
                    }

                    testBug80615CheckComStmtStatus(prepCount, isSPS, testCase, checkStmt, expectedPrepCount, expectedExecCount, expectedCloseCount);
                } while (prepCount < testRepetitions && isSPS);

                if (closeStmt) {
                    assertEquals(testRepetitions, prepCount, testCase);
                } else {
                    assertTrue(prepCount > prepStmtCacheSize + 2, testCase);
                    assertTrue(prepCount <= maxPrepStmtCount + 1, testCase);
                }

                // Batched statements are being rewritten so this will prepare another statement underneath.
                // It was failing before if the the number of stmt prepares on server was exhausted at this point (2nd Bug).
                testPstmt1.executeBatch();
                testPstmt1.close();

                testConn.close();
            } while ((closeStmt = !closeStmt) || (useCache = !useCache) || (poolable = !poolable));
        } finally {
            if (maxPrepStmtCountOri >= 0) {
                this.stmt.execute("SET GLOBAL max_prepared_stmt_count = " + maxPrepStmtCountOri);
                this.stmt.execute("FLUSH STATUS");
            }
        }
    }

    private void testBug80615CheckComStmtStatus(int prepCount, boolean isSPS, String testCase, Statement testStmt, int expectedPrepCount, int expectedExecCount,
            int expectedCloseCount) throws Exception {
        System.out.print(prepCount + ". ");
        System.out.print(isSPS ? "[SPS]" : "[CPS]");

        testCase += "\nIteration: " + prepCount;

        int actualPrepCount = 0;
        int actualExecCount = 0;
        int actualCloseCount = 0;
        this.rs = testStmt.executeQuery("SHOW SESSION STATUS WHERE Variable_name IN ('Com_stmt_prepare', 'Com_stmt_execute', 'Com_stmt_close')");
        while (this.rs.next()) {
            System.out.print(" (" + this.rs.getString(1).replace("Com_stmt_", "") + " " + this.rs.getInt(2) + ")");
            if (this.rs.getString(1).equalsIgnoreCase("Com_stmt_prepare")) {
                actualPrepCount = this.rs.getInt(2);
            } else if (this.rs.getString(1).equalsIgnoreCase("Com_stmt_execute")) {
                actualExecCount = this.rs.getInt(2);
            } else if (this.rs.getString(1).equalsIgnoreCase("Com_stmt_close")) {
                actualCloseCount = this.rs.getInt(2);
            }
        }
        System.out.println();

        assertEquals(expectedPrepCount, actualPrepCount, testCase);
        assertEquals(expectedExecCount, actualExecCount, testCase);
        assertEquals(expectedCloseCount, actualCloseCount, testCase);
    }

    /**
     * Tests fix for Bug#81706 - NullPointerException in driver.
     * 
     * @throws Exception
     */
    @Test
    public void testBug81706() throws Exception {
        boolean useSPS = false;
        boolean cacheRsMd = false;
        boolean readOnly = false;

        do {
            final String testCase = String.format("Case [SPS: %s, CacheRsMd: %s, Read-only: %s]", useSPS ? "Y" : "N", cacheRsMd ? "Y" : "N",
                    readOnly ? "Y" : "N");

            Properties props = new Properties();
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));
            props.setProperty(PropertyKey.cacheResultSetMetadata.getKeyName(), Boolean.toString(cacheRsMd));
            props.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestBug81706QueryInterceptor.class.getName());

            Connection testConn = getConnectionWithProps(props);
            testConn.setReadOnly(readOnly);
            Statement testStmt;
            PreparedStatement testPstmt;

            TestBug81706QueryInterceptor.isActive = true;
            TestBug81706QueryInterceptor.testCase = testCase;

            // Statement.executeQuery();
            testStmt = testConn.createStatement();
            testStmt.setFetchSize(Integer.MIN_VALUE);
            testStmt.executeQuery("/* ping */");
            testStmt.close();

            // Statemente.execute();
            testStmt = testConn.createStatement();
            testStmt.setFetchSize(Integer.MIN_VALUE);
            testStmt.execute("/* ping */");
            testStmt.close();

            // PreparedStatement.executeQuery();
            testPstmt = testConn.prepareStatement("/* ping */");
            assertFalse(testPstmt instanceof ServerPreparedStatement, testCase + ": Not the right Statement type.");
            testPstmt.setFetchSize(Integer.MIN_VALUE);
            testPstmt.executeQuery();
            testPstmt.close();

            // PreparedStatement.execute();
            testPstmt = testConn.prepareStatement("/* ping */");
            assertFalse(testPstmt instanceof ServerPreparedStatement, testCase + ": Not the right Statement type.");
            testPstmt.setFetchSize(Integer.MIN_VALUE);
            testPstmt.execute();
            testPstmt.close();

            TestBug81706QueryInterceptor.isActive = false;
            testConn.close();

        } while ((useSPS = !useSPS) || (cacheRsMd = !cacheRsMd) || (readOnly = !readOnly)); // Cycle through all possible combinations.
    }

    public static class TestBug81706QueryInterceptor extends BaseQueryInterceptor {
        public static boolean isActive = false;
        public static String testCase = "";

        @Override
        public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
            if (isActive) {
                String query = sql.get();
                if (query == null && interceptedQuery instanceof ClientPreparedStatement) {
                    query = interceptedQuery.toString();
                    query = query.substring(query.indexOf(':') + 2);
                }
                fail(testCase + ": Unexpected query executed - " + query);
            }
            return super.preProcess(sql, interceptedQuery);
        }
    }

    /**
     * Tests fix for Bug#66430 - setCatalog on connection leaves ServerPreparedStatement cache for old catalog.
     * 
     * @throws Exception
     */
    @Test
    public void testBug66430() throws Exception {
        createDatabase("testBug66430DB1");
        createTable("testBug66430DB1.testBug66430", "(id INT)");
        this.stmt.executeUpdate("INSERT INTO testBug66430DB1.testBug66430 VALUES (1)");

        createDatabase("testBug66430DB2");
        createTable("testBug66430DB2.testBug66430", "(id INT)");
        this.stmt.executeUpdate("INSERT INTO testBug66430DB2.testBug66430 VALUES (2)");

        boolean useSPS = false;
        boolean cachePS = false;
        do {
            final String testCase = String.format("Case: [useSPS: %s, cachePS: %s ]", useSPS ? "Y" : "N", cachePS ? "Y" : "N");

            Properties props = new Properties();
            props.setProperty(PropertyKey.cachePrepStmts.getKeyName(), Boolean.toString(cachePS));
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            Connection testConn = getConnectionWithProps(props);

            if (((JdbcConnection) testConn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA) {
                testConn.setSchema("testBug66430DB1");
            } else {
                testConn.setCatalog("testBug66430DB1");
            }
            PreparedStatement testPStmt = testConn.prepareStatement("SELECT * FROM testBug66430 WHERE id > ?");
            testPStmt.setInt(1, 0);
            this.rs = testPStmt.executeQuery();
            assertTrue(this.rs.next(), testCase);
            assertEquals(1, this.rs.getInt(1), testCase);
            assertFalse(this.rs.next(), testCase);
            testPStmt.close();

            if (((JdbcConnection) testConn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA) {
                testConn.setSchema("testBug66430DB2");
            } else {
                testConn.setCatalog("testBug66430DB2");
            }
            testPStmt = testConn.prepareStatement("SELECT * FROM testBug66430 WHERE id > ?");
            testPStmt.setInt(1, 0);
            this.rs = testPStmt.executeQuery();
            assertTrue(this.rs.next(), testCase);
            assertEquals(2, this.rs.getInt(1), testCase);
            assertFalse(this.rs.next(), testCase);
            testPStmt.close();

            // Do it again to make sure cached prepared statements behave correctly.
            if (((JdbcConnection) testConn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA) {
                testConn.setSchema("testBug66430DB1");
            } else {
                testConn.setCatalog("testBug66430DB1");
            }
            testPStmt = testConn.prepareStatement("SELECT * FROM testBug66430 WHERE id > ?");
            testPStmt.setInt(1, 0);
            this.rs = testPStmt.executeQuery();
            assertTrue(this.rs.next(), testCase);
            assertEquals(1, this.rs.getInt(1), testCase);
            assertFalse(this.rs.next(), testCase);
            testPStmt.close();

            if (((JdbcConnection) testConn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA) {
                testConn.setSchema("testBug66430DB2");
            } else {
                testConn.setCatalog("testBug66430DB2");
            }
            testPStmt = testConn.prepareStatement("SELECT * FROM testBug66430 WHERE id > ?");
            testPStmt.setInt(1, 0);
            this.rs = testPStmt.executeQuery();
            assertTrue(this.rs.next(), testCase);
            assertEquals(2, this.rs.getInt(1), testCase);
            assertFalse(this.rs.next(), testCase);
            testPStmt.close();

            testConn.close();
        } while ((useSPS = !useSPS) || (cachePS = !cachePS));
    }

    /**
     * Tests fix for Bug#84783 - query timeout is not working(thread hang).
     * 
     * @throws Exception
     */
    @Test
    public void testBug84783() throws Exception {
        // Test using a standard connection.
        final Statement testStmt = this.conn.createStatement();
        testStmt.setQueryTimeout(1);
        assertThrows(SQLException.class, "Statement cancelled due to timeout or client request", new Callable<Void>() {
            public Void call() throws Exception {
                testStmt.executeQuery("SELECT SLEEP(3)");
                return null;
            }
        });
        testStmt.close();

        boolean useSPS = false;
        do {
            final Properties props = new Properties();
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            final String testCase = String.format("Case [SPS: %s]", useSPS ? "Y" : "N");

            Connection testConn;

            // Test using a failover connection.
            testConn = getUnreliableFailoverConnection(new String[] { "host1", "host2" }, null);
            final Statement testStmtFO = testConn.createStatement();
            testStmtFO.setQueryTimeout(1);
            assertThrows(testCase, SQLException.class, "Statement cancelled due to timeout or client request", new Callable<Void>() {
                public Void call() throws Exception {
                    testStmtFO.executeQuery("SELECT SLEEP(3)");
                    return null;
                }
            });
            final PreparedStatement testPstmtFO = testConn.prepareStatement("SELECT SLEEP(3)");
            testPstmtFO.setQueryTimeout(1);
            assertThrows(testCase, SQLException.class, "Statement cancelled due to timeout or client request", new Callable<Void>() {
                public Void call() throws Exception {
                    testPstmtFO.executeQuery();
                    return null;
                }
            });
            testConn.close();

            // Test using a load-balanced connection.
            testConn = getUnreliableLoadBalancedConnection(new String[] { "host1", "host2" }, null);
            final Statement testStmtLB = testConn.createStatement();
            testStmtLB.setQueryTimeout(1);
            assertThrows(testCase, SQLException.class, "Statement cancelled due to timeout or client request", new Callable<Void>() {
                public Void call() throws Exception {
                    testStmtLB.executeQuery("SELECT SLEEP(3)");
                    return null;
                }
            });
            final PreparedStatement testPstmtLB = testConn.prepareStatement("SELECT SLEEP(3)");
            testPstmtLB.setQueryTimeout(1);
            assertThrows(testCase, SQLException.class, "Statement cancelled due to timeout or client request", new Callable<Void>() {
                public Void call() throws Exception {
                    testPstmtLB.executeQuery();
                    return null;
                }
            });
            testConn.close();

            // Test using a replication connection.
            testConn = getUnreliableReplicationConnection(new String[] { "host1", "host2" }, null);
            final Statement testStmtR = testConn.createStatement();
            testStmtR.setQueryTimeout(1);
            assertThrows(testCase, SQLException.class, "Statement cancelled due to timeout or client request", new Callable<Void>() {
                public Void call() throws Exception {
                    testStmtR.executeQuery("SELECT SLEEP(3)");
                    return null;
                }
            });
            final PreparedStatement testPstmtR = testConn.prepareStatement("SELECT SLEEP(3)");
            testPstmtR.setQueryTimeout(1);
            assertThrows(testCase, SQLException.class, "Statement cancelled due to timeout or client request", new Callable<Void>() {
                public Void call() throws Exception {
                    testPstmtR.executeQuery();
                    return null;
                }
            });
            testConn.close();
        } while (useSPS = !useSPS);
    }

    /**
     * Tests fix for Bug#74932 - ConnectionImp Doesn't Close Server Prepared Statement (PreparedStatement Leak).
     * 
     * @throws Exception
     */
    @Test
    public void testBug74932() throws Exception {
        createTable("testBug74932", "(c1 INT, c2 INT)");
        this.stmt.executeUpdate("INSERT INTO testBug74932 VALUES (1, 1), (1, 2), (2, 1), (2, 2)");

        String sql1 = "SELECT * FROM testBug74932 WHERE c1 = ?";
        String sql2 = "SELECT * FROM testBug74932 WHERE c2 = ?";

        final Properties props = new Properties();
        props.setProperty(PropertyKey.prepStmtCacheSize.getKeyName(), "10");
        props.setProperty(PropertyKey.cachePrepStmts.getKeyName(), "true");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");

        // Prepare different statements.
        Connection testConn = getConnectionWithProps(props);
        this.rs = testConn.createStatement().executeQuery("SHOW STATUS LIKE 'Prepared_stmt_count'");
        assertTrue(this.rs.next());
        int currPrepCount = this.rs.getInt(2);
        for (int i = 0; i < 10; i++) {
            testBug74932ExecuteStmts(testConn, sql1, sql2);

            this.rs = testConn.createStatement().executeQuery("SHOW STATUS LIKE 'Prepared_stmt_count'");
            assertTrue(this.rs.next());
            assertEquals(2, this.rs.getInt(2) - currPrepCount);
        }
        testConn.close();

        // Prepare same statement.
        testConn = getConnectionWithProps(props);
        this.rs = testConn.createStatement().executeQuery("SHOW STATUS LIKE 'Prepared_stmt_count'");
        assertTrue(this.rs.next());
        currPrepCount = this.rs.getInt(2);
        for (int i = 0; i < 10; i++) {
            testBug74932ExecuteStmts(testConn, sql1, sql1);
            this.rs = testConn.createStatement().executeQuery("SHOW STATUS LIKE 'Prepared_stmt_count'");
            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(2) - currPrepCount);
        }
        testConn.close();
    }

    private void testBug74932ExecuteStmts(final Connection testConn, final String sqlOuter, final String sqlInner) throws SQLException {
        PreparedStatement psOuter = null;
        PreparedStatement psInner = null;
        ResultSet rsOuter = null;
        ResultSet rsInner = null;

        psOuter = testConn.prepareStatement(sqlOuter);
        psOuter.setInt(1, 1);
        rsOuter = psOuter.executeQuery();
        for (int i = 0; i < 2; i++) {
            assertTrue(rsOuter.next());
            try {
                psInner = testConn.prepareStatement(sqlInner);
                psInner.setInt(1, 2);
                rsInner = psInner.executeQuery();
                assertTrue(rsInner.next());
                assertTrue(rsInner.next());
                assertFalse(rsInner.next());
            } finally {
                if (rsInner != null) {
                    rsInner.close();
                }
                psInner.close();
            }
        }
        assertFalse(rsOuter.next());
        rsOuter.close();
        rsInner.close();
        psOuter.close();
    }

    /**
     * Tests fix for Bug#78313 - proxies not handling Object.equals(Object) calls correctly.
     * 
     * @throws Exception
     */
    @Test
    public void testBug78313() throws Exception {
        Connection testConn;

        // Plain connection.
        testConn = getConnectionWithProps("");
        assertFalse(testConn.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(testConn.equals(testConn));
        this.stmt = testConn.createStatement();
        assertFalse(this.stmt.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.stmt.equals(this.stmt));
        this.rs = this.stmt.executeQuery("SELECT 'testBug78313'");
        assertFalse(this.rs.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.rs.equals(this.rs));
        this.pstmt = testConn.prepareStatement("SELECT 'testBug78313'");
        assertFalse(this.pstmt.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.pstmt.equals(this.pstmt));
        assertFalse(this.rs.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.rs.equals(this.rs));
        testConn.close();

        // Plain connection with proxied result sets.
        final Properties props = new Properties();
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), ResultSetScannerInterceptor.class.getName());
        props.setProperty(ResultSetScannerInterceptor.PNAME_resultSetScannerRegex, ".*");
        testConn = getConnectionWithProps(props);
        assertFalse(testConn.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(testConn.equals(testConn));
        this.stmt = testConn.createStatement();
        assertFalse(this.stmt.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.stmt.equals(this.stmt));
        this.rs = this.stmt.executeQuery("SELECT 'testBug78313'");
        assertTrue(this.rs.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.rs.equals(this.rs));
        this.pstmt = testConn.prepareStatement("SELECT 'testBug78313'");
        assertFalse(this.pstmt.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.pstmt.equals(this.pstmt));
        this.rs = this.pstmt.executeQuery();
        assertTrue(this.rs.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.rs.equals(this.rs));
        testConn.close();

        // Fail-over connection; all JDBC objects are proxied.
        testConn = getFailoverConnection();
        assertTrue(testConn.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(testConn.equals(testConn));
        this.stmt = testConn.createStatement();
        assertTrue(this.stmt.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.stmt.equals(this.stmt));
        this.rs = this.stmt.executeQuery("SELECT 'testBug78313'");
        assertTrue(this.rs.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.rs.equals(this.rs));
        this.pstmt = testConn.prepareStatement("SELECT 'testBug78313'");
        assertTrue(this.pstmt.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.pstmt.equals(this.pstmt));
        this.rs = this.pstmt.executeQuery();
        assertTrue(this.rs.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.rs.equals(this.rs));
        testConn.close();

        // Load-balanced connection; all JDBC objects are proxied. 
        testConn = getLoadBalancedConnection();
        assertTrue(testConn.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(testConn.equals(testConn));
        this.stmt = testConn.createStatement();
        assertTrue(this.stmt.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.stmt.equals(this.stmt));
        this.rs = this.stmt.executeQuery("SELECT 'testBug78313'");
        assertTrue(this.rs.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.rs.equals(this.rs));
        this.pstmt = testConn.prepareStatement("SELECT 'testBug78313'");
        assertTrue(this.pstmt.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.pstmt.equals(this.pstmt));
        this.rs = this.pstmt.executeQuery();
        assertTrue(this.rs.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.rs.equals(this.rs));
        testConn.close();

        // Replication connection; all JDBC objects are proxied.
        testConn = getSourceReplicaReplicationConnection();
        assertTrue(testConn.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(testConn.equals(testConn));
        this.stmt = testConn.createStatement();
        assertTrue(this.stmt.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.stmt.equals(this.stmt));
        this.rs = this.stmt.executeQuery("SELECT 'testBug78313'");
        assertTrue(this.rs.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.rs.equals(this.rs));
        this.pstmt = testConn.prepareStatement("SELECT 'testBug78313'");
        assertTrue(this.pstmt.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.pstmt.equals(this.pstmt));
        this.rs = this.pstmt.executeQuery();
        assertTrue(this.rs.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(this.rs.equals(this.rs));
        testConn.close();

        // XA Connection; unwrapped connections and statements are proxied.
        MysqlXADataSource xaDs = new MysqlXADataSource();
        xaDs.setUrl(BaseTestCase.dbUrl);
        XAConnection xaTestConn = xaDs.getXAConnection();
        testConn = xaTestConn.getConnection();
        Connection unwrappedTestConn = testConn.unwrap(JdbcConnection.class);
        assertTrue(unwrappedTestConn.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(unwrappedTestConn.equals(unwrappedTestConn));
        this.stmt = testConn.createStatement();
        Statement unwrappedStmt = this.stmt.unwrap(com.mysql.cj.jdbc.JdbcStatement.class);
        assertTrue(unwrappedStmt.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(unwrappedStmt.equals(unwrappedStmt));
        this.pstmt = testConn.prepareStatement("SELECT 'testBug78313'");
        Statement unwrappedPstmt = this.pstmt.unwrap(com.mysql.cj.jdbc.JdbcStatement.class);
        assertTrue(unwrappedPstmt.getClass().getName().matches("^(?:com\\.sun\\.proxy\\.)?\\$Proxy\\d*"));
        assertTrue(unwrappedPstmt.equals(unwrappedPstmt));
        testConn.close();
        xaTestConn.close();
    }

    /**
     * Tests fix for Bug#87429 - repeated close of ServerPreparedStatement causes memory leak.
     * 
     * Original de-cache on double close() behavior modified by:
     * WL#11101 - Remove de-cache and close of SSPSs on double call to close().
     * 
     * @throws Exception
     */
    @Test
    public void testBug87429() throws Exception {
        Field stmtsCacheField = ConnectionImpl.class.getDeclaredField("serverSideStatementCache");
        stmtsCacheField.setAccessible(true);
        ToIntFunction<Connection> getStmtsCacheSize = (c) -> {
            try {
                LRUCache<?, ?> stmtsCacheObj = (LRUCache<?, ?>) stmtsCacheField.get(c);
                return stmtsCacheObj == null ? -1 : stmtsCacheObj.size();
            } catch (IllegalArgumentException | IllegalAccessException e) {
                fail("Fail getting the statemets cache size.");
                return -1;
            }
        };

        final String sql1 = "SELECT 1, ?";
        final String sql2 = "SELECT 2, ?";

        boolean useSPS = false;
        boolean cachePS = false;
        do {
            Properties props = new Properties();
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));
            props.setProperty(PropertyKey.cachePrepStmts.getKeyName(), Boolean.toString(cachePS));
            props.setProperty(PropertyKey.prepStmtCacheSize.getKeyName(), "5");

            boolean cachedSPS = useSPS && cachePS;

            JdbcConnection testConn = (JdbcConnection) getConnectionWithProps(props);
            // Single PreparedStatement, closed multiple times.
            for (int i = 0; i < 100; i++) {
                this.pstmt = testConn.prepareStatement(sql1);
                assertTrue(this.pstmt.isPoolable());
                assertEquals(1, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));
                this.pstmt.close(); // Close & cache.
                assertEquals(cachedSPS ? 1 : 0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 1 : -1, getStmtsCacheSize.applyAsInt(testConn));
                this.pstmt.close();  // No-op.
                assertEquals(cachedSPS ? 1 : 0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 1 : -1, getStmtsCacheSize.applyAsInt(testConn));
                this.pstmt.close();  // No-op.
                assertEquals(cachedSPS ? 1 : 0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 1 : -1, getStmtsCacheSize.applyAsInt(testConn));

                try {
                    this.pstmt.setPoolable(false); // De-caches the statement or no-op if not cached.
                } catch (SQLException e) {
                    if (cachedSPS) {
                        fail("Exception [" + e.getMessage() + "] not expected.");
                    }
                }
                this.pstmt.close(); // No-op.
                assertEquals(0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));
            }
            testConn.close();
            assertEquals(0, testConn.getActiveStatementCount());
            assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

            testConn = (JdbcConnection) getConnectionWithProps(props);
            // Multiple PreparedStatements interchanged, two queries, closed multiple times.
            for (int i = 0; i < 100; i++) {
                for (int j = 1; j <= 4; j++) {
                    PreparedStatement pstmt1 = testConn.prepareStatement(j == 1 ? sql2 : sql1);
                    PreparedStatement pstmt2 = testConn.prepareStatement(j == 2 ? sql2 : sql1);
                    PreparedStatement pstmt3 = testConn.prepareStatement(j == 3 ? sql2 : sql1);
                    PreparedStatement pstmt4 = testConn.prepareStatement(j == 4 ? sql2 : sql1);
                    assertEquals(4, testConn.getActiveStatementCount());

                    // Close and cache statements successively.
                    pstmt4.close();
                    pstmt4.close(); // No-op.
                    assertEquals(cachedSPS ? 4 : 3, testConn.getActiveStatementCount());
                    pstmt3.close();
                    pstmt3.close(); // No-op.
                    assertEquals(cachedSPS ? (j >= 3 ? 4 : 3) : 2, testConn.getActiveStatementCount());
                    pstmt2.close();
                    pstmt2.close(); // No-op.
                    assertEquals(cachedSPS ? (j >= 2 ? 3 : 2) : 1, testConn.getActiveStatementCount());
                    pstmt1.close();
                    pstmt1.close(); // No-op.
                    assertEquals(cachedSPS ? 2 : 0, testConn.getActiveStatementCount());

                    // No-ops.
                    pstmt4.close();
                    pstmt4.close();
                    pstmt3.close();
                    pstmt3.close();
                    pstmt2.close();
                    pstmt2.close();
                    pstmt1.close();
                    pstmt1.close();
                    assertEquals(cachedSPS ? 2 : 0, testConn.getActiveStatementCount());

                    // De-cache statements successively.
                    try {
                        pstmt4.setPoolable(false); // De-caches the statement or no-op if not cached.
                    } catch (SQLException e) {
                        if (cachedSPS && j == 4) {
                            fail("Exception [" + e.getMessage() + "] not expected.");
                        }
                    }
                    pstmt4.close(); // No-op.
                    assertEquals(cachedSPS ? (j < 4 ? 2 : 1) : 0, testConn.getActiveStatementCount());
                    try {
                        pstmt3.setPoolable(false); // De-caches the statement or no-op if not cached.
                    } catch (SQLException e) {
                        if (cachedSPS && j == 3) {
                            fail("Exception [" + e.getMessage() + "] not expected.");
                        }
                    }
                    pstmt3.close(); // No-op.
                    assertEquals(cachedSPS ? (j < 3 ? 2 : 1) : 0, testConn.getActiveStatementCount());
                    try {
                        pstmt2.setPoolable(false); // De-caches the statement or no-op if not cached.
                    } catch (SQLException e) {
                        if (cachedSPS && j == 2) {
                            fail("Exception [" + e.getMessage() + "] not expected.");
                        }
                    }
                    pstmt2.close(); // No-op.
                    assertEquals(cachedSPS ? 1 : 0, testConn.getActiveStatementCount());
                    try {
                        pstmt1.setPoolable(false); // De-caches the statement or no-op if not cached.
                    } catch (SQLException e) {
                        if (cachedSPS) {
                            fail("Exception [" + e.getMessage() + "] not expected.");
                        }
                    }
                    pstmt1.close(); // No-op.
                    assertEquals(0, testConn.getActiveStatementCount());
                }
            }
            testConn.close();
            assertEquals(0, testConn.getActiveStatementCount());
        } while ((useSPS = !useSPS) || (cachePS = !cachePS));
    }

    /**
     * Tests fix for Bug#26995710 - WL#11161 : NULL POINTER EXCEPTION IN EXECUTEBATCH() AND CLOSE().
     * 
     * @throws Exception
     */
    @Test
    public void testBug26995710() throws Exception {
        createTable("testBug26995710", "(c1 char(20),c2 char(20))");

        boolean useSPS = false;
        do {
            Properties props = new Properties();
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
            props.setProperty(PropertyKey.autoGenerateTestcaseScript.getKeyName(), "true");
            props.setProperty(PropertyKey.sessionVariables.getKeyName(), "sql_mode=STRICT_TRANS_TABLES");

            Connection con = null;
            try {
                this.stmt.executeUpdate("truncate table testBug26995710");
                this.stmt.executeUpdate("insert into testBug26995710 values('a','b')");

                int colCnt = 0, i = 0;
                ResultSetMetaData rsmd = null;

                con = getConnectionWithProps(props);

                PreparedStatement ps = con.prepareStatement("Insert into testBug26995710 values(?,?) ", ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                ps.setQueryTimeout(2);
                assertEquals(2, ps.getQueryTimeout());
                ps.setString(1, "abc1");
                ps.setString(2, "xyz1");
                ps.addBatch();
                ps.setString(1, "abc2");
                ps.setString(2, "xyz2");
                ps.addBatch();
                ps.setNull(1, java.sql.Types.VARCHAR);
                ps.setString(2, "xyz4");
                ps.addBatch();
                ps.setString(1, "abc4");
                ps.setNull(2, java.sql.Types.VARCHAR);
                ps.addBatch();
                ps.setNull(1, java.sql.Types.VARCHAR);
                ps.setNull(2, java.sql.Types.VARCHAR);
                ps.addBatch();
                ps.executeBatch();
                ps.close();

                ResultSet rset = this.stmt.executeQuery("select * from testBug26995710");
                rsmd = rset.getMetaData();
                colCnt = rsmd.getColumnCount();
                System.out.println(" Column Cnt = " + colCnt);
                while (rset.next()) {
                    for (i = 1; i <= colCnt; i++) {
                        System.out.print(" [" + rset.getString(i) + "]");
                    }
                    System.out.print("\n");
                }
                rset.close();
                ps.close();

                ps = con.prepareStatement("delete from testBug26995710 where c1=? ", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ps.setQueryTimeout(2);
                ps.setString(1, "abc1");
                ps.addBatch();
                ps.setNull(1, java.sql.Types.VARCHAR);
                ps.addBatch();
                ps.setString(1, "abc4");
                ps.addBatch();
                ps.setString(1, "abc4");
                ps.addBatch();
                ps.executeBatch();
                ps.close();

                ps = con.prepareStatement("select count(*) from testBug26995710 ", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                rset = ps.executeQuery();
                rset.next();
                assertEquals(4, rset.getInt(1));
                rset.close();

                ps = con.prepareStatement("select * from testBug26995710 ", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                rset = ps.executeQuery();
                rsmd = rset.getMetaData();
                colCnt = rsmd.getColumnCount();
                System.out.println(" Column Cnt = " + colCnt);
                while (rset.next()) {
                    for (i = 1; i <= colCnt; i++) {
                        System.out.print(" [" + rset.getString(i) + "]");
                    }
                    System.out.print("\n");
                }
                rset.close();
                ps.close();

                ps = con.prepareStatement("insert into testBug26995710 select sleep(?)+1,sleep(?)+2 ", ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                ps.setQueryTimeout(2);
                ps.setInt(1, 2);
                ps.setInt(2, 3);
                ps.addBatch();
                ps.setInt(1, 0);
                ps.setInt(2, 1);
                ps.addBatch();
                ps.setNull(1, java.sql.Types.INTEGER);
                ps.setInt(2, 1);
                ps.addBatch();
                ps.setInt(1, 4);
                ps.setNull(2, java.sql.Types.INTEGER);
                ps.addBatch();
                ps.setNull(1, java.sql.Types.INTEGER);
                ps.setNull(2, java.sql.Types.INTEGER);
                ps.addBatch();
                try {
                    ps.executeBatch();
                    fail("Expected Timeout Error ");
                } catch (SQLException ex) {
                    System.out.print("Error " + ex.getMessage());
                    assertTrue(ex.getMessage().contains("Statement cancelled due to timeout or client request"));
                }
                ps.close();

                ps = con.prepareStatement("select count(*) from testBug26995710 ", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                rset = ps.executeQuery();
                rset.next();
                System.out.println(" Rec Cnt " + rset.getInt(1));
                //  assertEquals(4,rs.getInt(1));   

                rset.close();
                ps.close();

                ps = con.prepareStatement("select * from testBug26995710 ", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                rset = ps.executeQuery();
                rsmd = rset.getMetaData();
                colCnt = rsmd.getColumnCount();
                System.out.println(" Column Cnt = " + colCnt);
                while (rset.next()) {
                    for (i = 1; i <= colCnt; i++) {
                        System.out.print(" [" + rset.getString(i) + "]");
                    }
                    System.out.print("\n");
                }
                rset.close();
                ps.close();
            } finally {
                if (con != null) {
                    con.close();
                }
            }
        } while (useSPS = !useSPS);
    }

    /**
     * Tests fix for Bug#26748909, NO OPERATIONS ALLOWED AFTER STATEMENT CLOSED FOR TOSTRING()
     * 
     * @throws Exception
     */
    @Test
    public void testBug26748909() throws Exception {
        createTable("testBug26748909", "(id int)");

        Connection con = null;

        PreparedStatement ps1 = null;
        PreparedStatement ps2 = null;
        PreparedStatement ps3 = null;

        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        boolean useSPS = false;

        do {

            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            try {
                con = getConnectionWithProps(props);
                ps1 = con.prepareStatement("Select 'aaaaaaaaa' from dual");
                ps2 = con.prepareStatement("insert into testBug26748909 values(?)");
                ps3 = con.prepareStatement("select * from testBug26748909 where id=?");

                ps1.execute();
                ps1.close();

                ps2.setInt(1, 10);
                ps2.execute();
                ps2.close();

                ps3.setInt(1, 10);
                ResultSet r = ps3.executeQuery();
                assertTrue(r.next());
                assertEquals(10, r.getInt(1));
                ps3.close();

                System.out.println("'" + ps1 + "'");
                System.out.println("'" + ps2 + "'");
                System.out.println("'" + ps3 + "'");

                if (useSPS) {
                    assertEquals(ps1.toString(), "com.mysql.cj.jdbc.ServerPreparedStatement[1]: Select 'aaaaaaaaa' from dual");
                    assertEquals(ps2.toString(), "com.mysql.cj.jdbc.ServerPreparedStatement[2]: insert into testBug26748909 values(** NOT SPECIFIED **)");
                    assertEquals(ps3.toString(), "com.mysql.cj.jdbc.ServerPreparedStatement[3]: select * from testBug26748909 where id=** NOT SPECIFIED **");
                } else {
                    assertEquals(ps1.toString(), "com.mysql.cj.jdbc.ClientPreparedStatement: Select 'aaaaaaaaa' from dual");
                    assertEquals(ps2.toString(), "com.mysql.cj.jdbc.ClientPreparedStatement: insert into testBug26748909 values(** NOT SPECIFIED **)");
                    assertEquals(ps3.toString(), "com.mysql.cj.jdbc.ClientPreparedStatement: select * from testBug26748909 where id=** NOT SPECIFIED **");
                }

            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            } finally {
                if (con != null) {
                    con.close();
                }
            }
        } while (useSPS = !useSPS);
    }

    /**
     * Tests fix for Bug#87534 - UNION ALL query fails when useServerPrepStmts=true on database connection.
     * Base Bug#27422376 - NEWDATE TYPE IS LEAKING OUT, fixed in MySQL 5.7.22.
     * 
     * @throws Exception
     */
    @Test
    public void testBug87534() throws Exception {
        if (versionMeetsMinimum(5, 7) && !versionMeetsMinimum(5, 7, 22)) {
            return;
        }

        System.out.println("running");

        boolean useSPS = false;
        do {
            Connection testConn = getConnectionWithProps(PropertyKey.useServerPrepStmts + "=" + Boolean.toString(useSPS));

            this.pstmt = testConn.prepareStatement("SELECT CAST(NOW() AS DATE) UNION SELECT CAST(NOW() AS DATE)");
            this.rs = this.pstmt.executeQuery();
            assertTrue(this.rs.next());
            assertFalse(this.rs.next());
            this.pstmt.close();

            this.pstmt = testConn.prepareStatement("SELECT CAST(NOW() AS DATE) UNION ALL SELECT CAST(NOW() AS DATE)");
            this.rs = this.pstmt.executeQuery();
            assertTrue(this.rs.next());
            assertTrue(this.rs.next());
            assertFalse(this.rs.next());
            this.pstmt.close();

            createTable("testBug87534", "(dt DATE DEFAULT NULL)");
            this.stmt.execute("INSERT INTO testBug87534 VALUES ('2018-01-01')");

            this.pstmt = testConn.prepareStatement("SELECT dt FROM testBug87534 UNION SELECT dt FROM testBug87534");
            this.rs = this.pstmt.executeQuery();
            assertTrue(this.rs.next());
            assertFalse(this.rs.next());
            this.pstmt.close();

            this.pstmt = testConn.prepareStatement("SELECT dt FROM testBug87534 UNION ALL SELECT dt FROM testBug87534");
            this.rs = this.pstmt.executeQuery();
            assertTrue(this.rs.next());
            assertTrue(this.rs.next());
            assertFalse(this.rs.next());
            this.pstmt.close();

            testConn.close();
        } while (useSPS = !useSPS);
    }

    /**
     * Tests fix for Bug#84813 (25501750), rewriteBatchedStatements fails in INSERT.
     * 
     * @throws Exception
     */
    @Test
    public void testBug84813() throws Exception {
        createTable("testBug84813", "(id INT AUTO_INCREMENT PRIMARY KEY, z INT, n INT)");

        boolean rwBS = false;
        boolean useSPS = false;

        do {
            final String testCase = String.format("Case [rwBS: %s, useSPS: %s]", rwBS ? "Y" : "N", useSPS ? "Y" : "N");

            Properties props = new Properties();
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString(rwBS));
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));
            props.setProperty(PropertyKey.emulateUnsupportedPstmts.getKeyName(), "false");

            Connection testConn = getConnectionWithProps(props);

            for (int r = 1; r <= 5; r++) {
                for (String odku : new String[] { "", " ON DUPLICATE KEY UPDATE id = -id" }) {
                    final String testCaseExtra = odku.length() > 0 ? "/ODKU" : "/non-ODKU";
                    this.pstmt = testConn.prepareStatement("INSERT INTO testBug84813 VALUES (NULL, 0, 0) /* Comment (?) */" + odku);
                    for (int i = 0; i < r; i++) {
                        this.pstmt.addBatch();
                    }
                    this.pstmt.executeBatch();
                    testBug84813CheckAndReset(testCase + testCaseExtra, r, true);
                    this.pstmt.close();

                    this.pstmt = testConn.prepareStatement("INSERT INTO testBug84813 VALUES (NULL, ?, ?) /* Comment (?) */ " + odku);
                    for (int i = 0; i < r; i++) {
                        this.pstmt.setInt(1, 0);
                        this.pstmt.setInt(2, i);
                        this.pstmt.addBatch();
                    }
                    this.pstmt.executeBatch();
                    testBug84813CheckAndReset(testCase + testCaseExtra, r, false);
                    this.pstmt.close();
                }
            }

            testConn.close();
        } while ((rwBS = !rwBS) || (useSPS = !useSPS));
    }

    private void testBug84813CheckAndReset(String testCase, int repetitions, boolean allZero) throws Exception {
        this.rs = this.stmt.executeQuery("SELECT * FROM testBug84813");
        for (int i = 0; i < repetitions; i++) {
            assertTrue(this.rs.next());
            assertEquals(i + 1, this.rs.getInt(1), testCase);
            assertEquals(0, this.rs.getInt(2), testCase);
            assertEquals(allZero ? 0 : i, this.rs.getInt(3), testCase);
        }
        assertFalse(this.rs.next());
        this.stmt.execute("TRUNCATE TABLE testBug84813");
    }

    /**
     * Tests fix for Bug#81063 (23098159), w/ rewriteBatchedStatements, when 2 tables involved, the rewriting not correct.
     * 
     * @throws Exception
     */
    @Test
    public void testBug81063() throws Exception {
        createTable("testBug81063a", "(c1 INT, c2 INT, c3 INT DEFAULT 0, c4 INT DEFAULT 0)");
        createTable("testBug81063b", "(c1 INT, c2 INT)");

        boolean rwBS = false;
        boolean useSPS = false;

        do {
            final String testCase = String.format("Case [rwBS: %s, useSPS: %s]", rwBS ? "Y" : "N", useSPS ? "Y" : "N");

            Properties props = new Properties();
            props.setProperty(PropertyKey.emulateUnsupportedPstmts.getKeyName(), "true");
            props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "true");
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString(rwBS));
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            Connection testConn = getConnectionWithProps(props);

            for (String sql : new String[] { "INSERT INTO testBug81063a VALUES (?, ?, ?, ?)",
                    "INSERT INTO testBug81063a (c1, c2) VALUES (?, ?); INSERT INTO testBug81063b VALUES (?, ?)" }) {
                int valsTbl1 = 4;
                int valsTbl2 = 0;
                if (sql.indexOf(';') != -1) {
                    valsTbl1 = 2;
                    valsTbl2 = 2;
                }
                for (int r = 1; r <= 5; r++) {
                    this.pstmt = testConn.prepareStatement(sql);
                    for (int i = 0; i < r; i++) {
                        this.pstmt.setLong(1, 4 * i + 1);
                        this.pstmt.setLong(2, 4 * i + 2);
                        this.pstmt.setLong(3, 4 * i + 3);
                        this.pstmt.setLong(4, 4 * i + 4);
                        this.pstmt.addBatch();
                    }
                    this.pstmt.executeBatch();
                    this.pstmt.close();
                    testBug81063CheckAndReset(testCase, valsTbl1, valsTbl2, r);
                }
            }
            testConn.close();
        } while ((rwBS = !rwBS) || (useSPS = !useSPS));
    }

    private void testBug81063CheckAndReset(String testCase, int t1Vals, int t2Vals, int repetitions) throws Exception {
        testCase += " (" + repetitions + " x " + t1Vals + "/" + t2Vals + ")";

        if (t1Vals > 0) {
            this.rs = this.stmt.executeQuery("SELECT * FROM testBug81063a");
            for (int r = 1, e = 1; r <= repetitions; r++, e += 4 - t1Vals) {
                assertTrue(this.rs.next(), testCase);
                for (int c = 1; c <= t1Vals; c++, e++) {
                    assertEquals(e, this.rs.getInt(c), testCase);
                }
            }
            assertFalse(this.rs.next());
        }
        this.stmt.execute("TRUNCATE TABLE testBug81063a");

        if (t2Vals > 0) {
            this.rs = this.stmt.executeQuery("SELECT * FROM testBug81063b");
            for (int r = 1, e = 3; r <= repetitions; r++, e += 4 - t2Vals) {
                assertTrue(this.rs.next(), testCase);
                for (int c = 1; c <= t2Vals; c++, e++) {
                    assertEquals(e, this.rs.getInt(c), testCase);
                }
            }
            assertFalse(this.rs.next());
        }
        this.stmt.execute("TRUNCATE TABLE testBug81063b");
    }

    /**
     * Tests fix for Bug#22931700, BINDINGS.GETBOOLEAN() ALWAYS RETURNS FALSE.
     * 
     * @throws Exception
     */
    @Test
    public void testBug22931700() throws Exception {
        createTable("testBug22931700", "(c1 int,c2 bool)");

        for (boolean useSPS : new boolean[] { false, true }) {
            this.stmt.executeUpdate("truncate table testBug22931700");
            this.stmt.executeUpdate("INSERT INTO testBug22931700 values(100,false)");

            Properties props = new Properties();
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            Connection testConn = getConnectionWithProps(props);

            this.pstmt = testConn.prepareStatement("update testBug22931700 set c1=?,c2=? ");
            this.pstmt.setNull(1, java.sql.Types.INTEGER);
            this.pstmt.setBoolean(2, true);

            ParameterBindings bindings = ((JdbcPreparedStatement) this.pstmt).getParameterBindings();
            assertTrue(bindings.isNull(1));
            assertFalse(bindings.isNull(2));
            assertTrue(bindings.getBoolean(2));

            this.pstmt.executeUpdate();
        }
    }

    /**
     * Tests fix for Bug#27453692, CHARACTERS GET GARBLED IN CONCAT() IN PS WHEN USECURSORFETCH=TRUE.
     * 
     * @throws Exception
     */
    @Test
    public void testBug27453692() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "utf8");

        String str = "\u30c6\u30b9\u30c8:";

        boolean useSPS = false;
        boolean useCursorFetch = false;
        do {
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));
            props.setProperty(PropertyKey.useCursorFetch.getKeyName(), Boolean.toString(useCursorFetch));
            Connection con1 = getConnectionWithProps(props);

            PreparedStatement ps1 = con1.prepareStatement("SELECT CONCAT('" + str + "', ?) AS res");
            ps1.setInt(1, 123);
            ResultSet rs1 = ps1.executeQuery();
            assertTrue(rs1.next());
            System.out.println(rs1.getString(1));
            assertEquals(str + "123", rs1.getString(1));

            con1.close();

        } while ((useSPS = !useSPS) || (useCursorFetch = !useCursorFetch));
    }

    /**
     * Tests fix for Bug#96442 (30151808), INCORRECT DATE ERROR WHEN CALLING GETMETADATA ON PREPARED STATEMENT.
     * 
     * @throws Exception
     */
    @Test
    public void testBug96442() throws Exception {
        createTable("testBug96442", "(id INT UNSIGNED NOT NULL, rdate DATE NOT NULL, ts TIMESTAMP NOT NULL)");

        Properties props = new Properties();
        boolean useSPS = false;
        do {
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));
            Connection con = getConnectionWithProps(props);
            try {
                con.prepareStatement("SELECT id FROM testBug96442 WHERE rdate = ? AND ts = ?").getMetaData();
                con.prepareStatement("SELECT DISTINCT id FROM testBug96442 WHERE rdate = ? AND ts = ?").getMetaData();
                con.prepareStatement("SELECT * FROM testBug96442 WHERE id = ? AND ts = ?").getMetaData();
                con.prepareStatement("SELECT * FROM testBug96442 WHERE rdate = ? AND ts = ?").getMetaData();
                con.prepareStatement("SELECT id,rdate FROM testBug96442 WHERE rdate = ? AND ts = ?").getMetaData();
                con.prepareStatement("SELECT * FROM testBug96442 WHERE rdate = '2000-01-01' AND ts = ?").getMetaData();
                con.prepareStatement("SELECT count(id) FROM testBug96442 WHERE rdate = ? AND ts = ?").getMetaData();
                con.prepareStatement("SELECT * FROM testBug96442 HAVING rdate = ? AND ts = ?").getMetaData();
            } finally {
                con.close();
            }
        } while (useSPS = !useSPS);
    }

    /**
     * Tests fix for Bug#Bug#91112 (28125069), AGAIN WRONG JAVA.SQL.DATE.
     * 
     * @throws Exception
     */
    @Test
    public void testBug91112() throws Exception {
        createTable("testBug91112", "(d1 DATE, d2 DATE, d3 DATE)");
        String query = "select d1, CONVERT(d1, CHAR(10)) as d1c, d2, CONVERT(d2, CHAR(10)) as d2c, d3, CONVERT(d3, CHAR(10)) as d3c from testBug91112";

        TimeZone defaultTimeZone = TimeZone.getDefault();
        Properties props = new Properties();
        props.put(PropertyKey.connectionTimeZone, "UTC");
        try {
            // setting a custom calendar time zone later than the client one to detect day switch 
            Calendar cal_Los_Angeles = GregorianCalendar.getInstance(TimeZone.getTimeZone("America/Los_Angeles"), Locale.US);
            SimpleDateFormat sdf_Los_Angeles = TimeUtil.getSimpleDateFormat("yyyy-MM-dd HH:mm:ss", cal_Los_Angeles);

            for (boolean useSSPS : new boolean[] { false, true }) {
                for (boolean cacheDefaultTimeZone : new boolean[] { true, false }) {
                    String errMsg = "Using useSSPS=" + useSSPS + ", cacheDefaultTimeZone=" + cacheDefaultTimeZone + ":";
                    System.out.println(errMsg);
                    props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                    props.put(PropertyKey.cacheDefaultTimeZone.getKeyName(), "" + cacheDefaultTimeZone);

                    // setting a client time zone ahead of the server one to detect day switch 
                    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Moscow"));

                    Connection con = getConnectionWithProps(props);

                    PreparedStatement pst1 = con.prepareStatement("insert into testBug91112 values (?, ?, ?)");
                    java.sql.Date d_1982_04_01_MSK = java.sql.Date.valueOf("1982-04-01");
                    java.sql.Date d_1990_10_20_MSK = java.sql.Date.valueOf("1990-10-20");
                    LocalDate ld_1990_10_20 = LocalDate.of(1990, 10, 20);

                    pst1.setDate(1, d_1982_04_01_MSK);
                    pst1.setDate(2, d_1982_04_01_MSK, cal_Los_Angeles);
                    pst1.setObject(3, ld_1990_10_20);
                    pst1.execute();

                    this.rs = con.createStatement().executeQuery(query);
                    testBug91112CheckResultMSK(d_1982_04_01_MSK, d_1990_10_20_MSK, cal_Los_Angeles, sdf_Los_Angeles);

                    PreparedStatement pst2 = con.prepareStatement(query);
                    this.rs = pst2.executeQuery();
                    testBug91112CheckResultMSK(d_1982_04_01_MSK, d_1990_10_20_MSK, cal_Los_Angeles, sdf_Los_Angeles);

                    // check the cacheDefaultTimeZone behaviour, changing time zone in runtime
                    TimeZone.setDefault(TimeZone.getTimeZone("Europe/Berlin"));

                    this.stmt.execute("TRUNCATE TABLE testBug91112");

                    pst1.setDate(1, d_1982_04_01_MSK);
                    pst1.setDate(2, d_1982_04_01_MSK, cal_Los_Angeles);
                    pst1.setObject(3, ld_1990_10_20);
                    pst1.execute();

                    this.rs = con.createStatement().executeQuery(query);
                    testBug91112CheckResultBerlin(d_1982_04_01_MSK, d_1990_10_20_MSK, cal_Los_Angeles, sdf_Los_Angeles, cacheDefaultTimeZone);

                    this.rs = pst2.executeQuery();
                    testBug91112CheckResultBerlin(d_1982_04_01_MSK, d_1990_10_20_MSK, cal_Los_Angeles, sdf_Los_Angeles, cacheDefaultTimeZone);

                    PreparedStatement pst3 = con.prepareStatement(query);
                    this.rs = pst3.executeQuery();
                    testBug91112CheckResultBerlin(d_1982_04_01_MSK, d_1990_10_20_MSK, cal_Los_Angeles, sdf_Los_Angeles, cacheDefaultTimeZone);

                    this.stmt.execute("TRUNCATE TABLE testBug91112");
                }
            }

        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    public void testBug91112CheckResultMSK(java.sql.Date d_1982_04_01_MSK, java.sql.Date d_1990_10_20_MSK, Calendar cal_Los_Angeles,
            SimpleDateFormat sdf_Los_Angeles) throws Exception {
        assertTrue(this.rs.next());
        assertEquals("1982-04-01", this.rs.getString(1));
        assertEquals("1982-04-01", this.rs.getString(2));
        assertEquals("1982-03-31", this.rs.getString(3));
        assertEquals("1982-03-31", this.rs.getString(4));
        assertEquals("1990-10-20", this.rs.getString(5));
        assertEquals("1990-10-20", this.rs.getString(6));
        assertEquals(d_1982_04_01_MSK, this.rs.getDate(1));
        assertEquals(d_1982_04_01_MSK, this.rs.getDate(1, Calendar.getInstance()));
        assertEquals("1982-03-30 13:00:00", sdf_Los_Angeles.format(this.rs.getDate(3)));
        assertEquals("1982-03-31 00:00:00", sdf_Los_Angeles.format(this.rs.getDate(3, cal_Los_Angeles)));
        assertEquals(d_1990_10_20_MSK, this.rs.getDate(5));
        assertEquals(d_1990_10_20_MSK, this.rs.getDate(5, Calendar.getInstance()));
    }

    public void testBug91112CheckResultBerlin(java.sql.Date d_1982_04_01_MSK, java.sql.Date d_1990_10_20_MSK, Calendar cal_Los_Angeles,
            SimpleDateFormat sdf_Los_Angeles, boolean cacheDefaultTimeZone) throws Exception {
        assertTrue(this.rs.next());
        if (cacheDefaultTimeZone) {
            assertEquals("1982-04-01", this.rs.getString(1));
            assertEquals("1982-04-01", this.rs.getString(2));
            assertEquals("1982-03-30 13:00:00", sdf_Los_Angeles.format(this.rs.getDate(3)));
        } else {
            assertEquals("1982-03-31", this.rs.getString(1));
            assertEquals("1982-03-31", this.rs.getString(2));
            assertEquals("1982-03-30 14:00:00", sdf_Los_Angeles.format(this.rs.getDate(3)));
        }
        assertEquals("1982-03-31", this.rs.getString(4));
        assertEquals("1990-10-20", this.rs.getString(5));
        assertEquals("1990-10-20", this.rs.getString(6));

        java.sql.Date d_1982_03_31_Berlin = java.sql.Date.valueOf("1982-03-31");
        java.sql.Date d_1990_10_20_Berlin = java.sql.Date.valueOf("1990-10-20");

        if (cacheDefaultTimeZone) {
            assertEquals(d_1982_04_01_MSK, this.rs.getDate(1));
            assertEquals(d_1982_04_01_MSK, this.rs.getDate(1, Calendar.getInstance(TimeZone.getTimeZone("Europe/Moscow"))));
        } else {
            assertEquals(d_1982_03_31_Berlin, this.rs.getDate(1));
            assertEquals(d_1982_03_31_Berlin, this.rs.getDate(1, Calendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"))));
        }

        assertEquals("1982-03-31", this.rs.getString(3));
        assertEquals("1982-03-31 00:00:00", sdf_Los_Angeles.format(this.rs.getDate(3, cal_Los_Angeles)));

        if (cacheDefaultTimeZone) {
            assertEquals(d_1990_10_20_MSK, this.rs.getDate(5));
            assertEquals(d_1990_10_20_MSK, this.rs.getDate(5, Calendar.getInstance(TimeZone.getTimeZone("Europe/Moscow"))));
        } else {
            assertEquals(d_1990_10_20_Berlin, this.rs.getDate(5));
            assertEquals(d_1990_10_20_Berlin, this.rs.getDate(5, Calendar.getInstance(TimeZone.getTimeZone("Europe/Berlin"))));
        }
    }

    /**
     * Tests fix for Bug#98536 (30877755), SIMPLEDATEFORMAT COULD CACHE A WRONG CALENDAR.
     * 
     * @throws Exception
     */
    @Test
    public void testBug98536() throws Exception {
        GregorianCalendar prolepticGc = new GregorianCalendar();
        prolepticGc.setGregorianChange(new Date(Long.MIN_VALUE));
        prolepticGc.clear();
        prolepticGc.set(Calendar.DAY_OF_MONTH, 8);
        prolepticGc.set(Calendar.MONTH, Calendar.OCTOBER);
        prolepticGc.set(Calendar.YEAR, 1582);

        GregorianCalendar gc = new GregorianCalendar();
        gc.clear();
        gc.setTimeInMillis(prolepticGc.getTimeInMillis());

        assertEquals(1582, gc.get(Calendar.YEAR));
        assertEquals(8, gc.get(Calendar.MONTH));
        assertEquals(28, gc.get(Calendar.DAY_OF_MONTH));

        createTable("testBug98536", "(d1 date, pd date, d2 date)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        boolean useServerPrepStmts = false;

        do {
            final String testCase = String.format("Case: [useServerPrepStmts=%s]", useServerPrepStmts ? "Y" : "N");
            System.out.println(testCase);

            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useServerPrepStmts);

            Connection c1 = getConnectionWithProps(props);
            Statement st1 = c1.createStatement();

            st1.execute("truncate table testBug98536");

            java.sql.Date d1 = new java.sql.Date(prolepticGc.getTime().getTime());

            this.pstmt = c1.prepareStatement("insert into testBug98536 values(?,?,?)");
            this.pstmt.setDate(1, d1);
            this.pstmt.setDate(2, d1, prolepticGc);
            this.pstmt.setDate(3, d1);
            this.pstmt.execute();

            /*
             * Checking stored values by retrieving them as strings to avoid conversions on c/J side.
             */
            this.rs = st1.executeQuery(
                    "select DATE_FORMAT(d1, '%Y-%m-%d') as d1, DATE_FORMAT(pd, '%Y-%m-%d') as pd, DATE_FORMAT(d2, '%Y-%m-%d') as d2 from testBug98536");
            this.rs.next();
            System.out.println(this.rs.getString(1) + ", " + this.rs.getString(2) + ", " + this.rs.getString(3));

            assertEquals("1582-09-28", this.rs.getString(1), testCase); // according to Julian calendar
            assertEquals("1582-10-08", this.rs.getString(2), testCase); // according to proleptic Gregorian calendar
            assertEquals("1582-09-28", this.rs.getString(3), testCase); // according to Julian calendar

            c1.close();

        } while (useServerPrepStmts = !useServerPrepStmts);
    }

    /**
     * Tests fix for Bug#98237 (30911870), PREPAREDSTATEMENT.SETOBJECT(I, "FALSE", TYPES.BOOLEAN) ALWAYS SETS TRUE OR 1.
     * 
     * @throws Exception
     */
    @Test
    public void testBug98237() throws Exception {
        createTable("testBug98237", "(b tinyint)");

        String[] falses = new String[] { "False", "n", "0", "-0", "0.00", "-0.0" };
        String[] trues = new String[] { "true", "y", "1", "-1", "1.0", "-1.0", "0.01", "-0.01" };

        Properties props = new Properties();
        boolean useSPS = false;
        do {
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));
            Connection con = getConnectionWithProps(props);
            try {
                PreparedStatement ps = con.prepareStatement("insert into testBug98237 values(?)");
                Statement st = con.createStatement();

                con.createStatement().execute("truncate table testBug98237");
                for (String val : falses) {
                    ps.clearParameters();
                    ps.setObject(1, val, Types.BOOLEAN);
                    ps.execute();
                }
                this.rs = st.executeQuery("select * from testBug98237");
                for (String val : falses) {
                    assertTrue(this.rs.next());
                    assertEquals(0, this.rs.getInt(1), "'false' was expected for " + val);
                }

                con.createStatement().execute("truncate table testBug98237");
                for (String val : trues) {
                    ps.clearParameters();
                    ps.setObject(1, val, Types.BOOLEAN);
                    ps.execute();
                }
                this.rs = st.executeQuery("select * from testBug98237");
                for (String val : trues) {
                    assertTrue(this.rs.next());
                    assertEquals(1, this.rs.getInt(1), "'true' was expected for " + val);
                }

                ps.clearParameters();
                assertThrows(SQLException.class, ".+ No conversion from abc to Types.BOOLEAN possible.+", new Callable<Void>() {
                    public Void call() throws Exception {
                        ps.setObject(1, "abc", Types.BOOLEAN);
                        return null;
                    }
                });

            } finally {
                con.close();
            }
        } while (useSPS = !useSPS);
    }

    /**
     * Tests fix for Bug#99713 (31418928), NPE DURING COM.MYSQL.CJ.SERVERPREPAREDQUERYBINDVALUE.STOREDATE().
     * 
     * @throws Exception
     */
    @Test
    public void testBug99713() throws Exception {
        createTable("testBug99713", "(d1 DATE)");
        Connection con = null;
        Properties props = new Properties();
        try {
            for (boolean useSSPS : new boolean[] { false, true }) {
                for (boolean cacheDefaultTimeZone : new boolean[] { true, false }) {
                    props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                    props.put(PropertyKey.cacheDefaultTimeZone.getKeyName(), "" + cacheDefaultTimeZone);
                    con = getConnectionWithProps(props);
                    this.pstmt = con.prepareStatement("INSERT into testBug99713 VALUES (?)");
                    this.pstmt.setDate(1, java.sql.Date.valueOf("1982-04-01"));
                    this.pstmt.addBatch();
                    assertEquals(1, this.pstmt.executeBatch()[0]);
                }
            }
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    /**
     * Tests fix for Bug#101242 (32046007), CANNOT USE BYTEARRAYINPUTSTREAM AS ARGUMENTS IN PREPARED STATEMENTS ANYMORE.
     * 
     * @throws Exception
     */
    @Test
    public void testBug101242() throws Exception {
        createTable("testBug101242", "(my_column blob NOT NULL)");

        byte[] value = "TEST".getBytes();

        Connection con = null;
        Properties props = new Properties();
        try {
            for (boolean useSSPS : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                con = getConnectionWithProps(props);

                this.stmt.executeUpdate("TRUNCATE TABLE testBug101242");

                this.pstmt = con.prepareStatement("INSERT INTO testBug101242(my_column) VALUES (?)");
                this.pstmt.setObject(1, new ByteArrayInputStream(value));
                this.pstmt.execute();

                this.rs = this.stmt.executeQuery("SELECT * FROM testBug101242");
                this.rs.next();

                assertByteArrayEquals("Different value retrieved than inserted", value, this.rs.getBytes("my_column"));
            }
        } finally {
            if (con != null) {
                con.close();
            }
        }

    }

    /**
     * Tests fix for Bug#98695 (30962953), EXECUTION OF "LOAD DATA LOCAL INFILE" COMMAND THROUGH JDBC FOR DATETIME COLUMN.
     * 
     * @throws Exception
     */
    @Test
    public void testBug98695() throws Exception {
        createTable("testBug98695",
                "(id bigint(20) NOT NULL AUTO_INCREMENT, ts timestamp NULL DEFAULT NULL, dt datetime DEFAULT NULL,date_input_str varchar(45) DEFAULT NULL, PRIMARY KEY (id)) ENGINE=InnoDB");

        File tempFile = File.createTempFile("data", ".csv");
        Writer out = new FileWriter(tempFile);
        out.write("2019-03-09 23:01:54,2019-03-09 23:01:54,2019-03-09 23:01:54\n" + //
                "2019-03-10 03:00:00,2019-03-10 03:00:00,2019-03-10 03:00:00\n" + //
                "2019-03-10 23:01:54,2019-03-10 23:01:54,2019-03-10 23:01:54\n" + //
                "2019-03-11 23:01:54,2019-03-11 23:01:54,2019-03-11 23:01:54\n" + //
                "2019-04-13 23:01:54,2019-04-13 23:01:54,2019-04-13 23:01:54\n" + //
                "2019-11-02 23:01:54,2019-11-02 23:01:54,2019-11-02 23:01:54\n" + //
                "2019-11-03 02:00:00,2019-11-03 02:00:00,2019-11-03 02:00:00\n" + //
                "2019-11-03 01:00:00,2019-11-03 01:00:00,2019-11-03 01:00:00\n" + //
                "2019-11-03 03:00:00,2019-11-03 03:00:00,2019-11-03 03:00:00\n" + //
                "2019-11-03 04:00:00,2019-11-03 04:00:00,2019-11-03 04:00:00\n" + //
                "2019-11-03 23:01:54,2019-11-03 23:01:54,2019-11-03 23:01:54\n" + //
                "2019-11-21 23:01:54,2019-11-21 23:01:54,2019-11-21 23:01:54");
        out.close();

        Properties props = new Properties();
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "LOCAL");
        props.setProperty(PropertyKey.allowLoadLocalInfile.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);

        String fileName = tempFile.getAbsolutePath();

        if (File.separatorChar == '\\') {
            StringBuilder fileNameBuf = new StringBuilder();
            int fileNameLength = fileName.length();
            for (int i = 0; i < fileNameLength; i++) {
                char c = fileName.charAt(i);
                fileNameBuf.append(c == '\\' ? "/" : c);
            }
            fileName = fileNameBuf.toString();
        }

        PreparedStatement preparedStmt = testConn.prepareStatement(
                "LOAD DATA LOCAL INFILE '" + fileName + "' INTO TABLE testBug98695 FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' (ts,dt,date_input_str)");
        preparedStmt.execute();
        preparedStmt.close();

        preparedStmt = testConn.prepareStatement("select ts, dt, date_input_str from testBug98695");
        ResultSet resultSet = preparedStmt.executeQuery();

        while (resultSet.next()) {
            Object ts = resultSet.getObject("ts");
            Object dt = resultSet.getObject("dt");
            String name = resultSet.getString("date_input_str");

            assertTrue(ts instanceof Timestamp);
            assertTrue(dt instanceof LocalDateTime);

            System.out.print("ts:" + ts + " ,");
            System.out.print("dt:" + ((LocalDateTime) dt).format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET) + ", ");
            System.out.println("name:" + name);

            assertEquals(name, ((Timestamp) ts).toLocalDateTime().format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET));
            assertEquals(name, ((LocalDateTime) dt).format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET));

        }

    }

    /**
     * Tests fix for Bug#101413 (32099505), JAVA.TIME.LOCALDATETIME CANNOT BE CAST TO JAVA.SQL.TIMESTAMP.
     * 
     * @throws Exception
     */
    @Test
    public void testBug101413() throws Exception {
        createTable("testBug101413", "(createtime1 TIMESTAMP, createtime2 DATETIME)");

        Properties props = new Properties();
        for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
            for (boolean preserveInstants : new boolean[] { false, true }) {
                for (boolean useSSPS : new boolean[] { false, true }) {
                    props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                    props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                    props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                    props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");

                    Connection con = getConnectionWithProps(props);
                    PreparedStatement ps = con.prepareStatement("insert into testBug101413(createtime1, createtime2) values(?, ?)");

                    con.setAutoCommit(false);
                    for (int i = 1; i <= 20000; i++) {
                        ps.setObject(1, LocalDateTime.now());
                        ps.setObject(2, LocalDateTime.now());
                        ps.addBatch();
                        if (i % 500 == 0) {
                            ps.executeBatch();
                            ps.clearBatch();
                        }
                    }
                    con.commit();
                }
            }
        }
    }

    /**
     * Tests fix for Bug#101558 (32141210), NULLPOINTEREXCEPTION WHEN EXECUTING INVALID QUERY WITH USEUSAGEADVISOR ENABLED.
     * 
     * @throws Exception
     */
    @Test
    public void testBug101558() throws Exception {

        Properties props = new Properties();
        props.setProperty(PropertyKey.cachePrepStmts.getKeyName(), "true");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
        props.setProperty(PropertyKey.useUsageAdvisor.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);

        // The query would work after setting allowMultiQueries=true, but when it is set the original bug is not reproducible.
        assertThrows(SQLException.class,
                "You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'SELECT 1 FROM DUAL' at line 1",
                () -> {
                    PreparedStatement statement = testConn.prepareStatement("SELECT 1 FROM DUAL; SELECT 1 FROM DUAL");
                    statement.execute();
                    return null;
                });

        testConn.close();
    }
}
