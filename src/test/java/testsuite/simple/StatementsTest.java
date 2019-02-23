/*
 * Copyright (c) 2002, 2019, Oracle and/or its affiliates. All rights reserved.
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

package testsuite.simple;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.MysqlConnection;
import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.ClientPreparedStatement;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.ParameterBindings;
import com.mysql.cj.jdbc.ServerPreparedStatement;
import com.mysql.cj.jdbc.exceptions.MySQLStatementCancelledException;
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.cj.jdbc.interceptors.ServerStatusDiffInterceptor;
import com.mysql.cj.util.LRUCache;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;

import testsuite.BaseTestCase;
import testsuite.regression.ConnectionRegressionTest.CountingReBalanceStrategy;

public class StatementsTest extends BaseTestCase {
    private static final int MAX_COLUMN_LENGTH = 255;

    private static final int MAX_COLUMNS_TO_TEST = 40;

    private static final int STEP = 8;

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(StatementsTest.class);
    }

    /**
     * Creates a new StatementsTest object.
     * 
     * @param name
     */
    public StatementsTest(String name) {
        super(name);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        this.stmt.executeUpdate("DROP TABLE IF EXISTS statement_test");

        this.stmt.executeUpdate("DROP TABLE IF EXISTS statement_batch_test");

        this.stmt.executeUpdate(
                "CREATE TABLE statement_test (id int not null primary key auto_increment, strdata1 varchar(255) not null, strdata2 varchar(255))");

        try {
            this.stmt.executeUpdate("CREATE TABLE statement_batch_test (id int not null primary key auto_increment, "
                    + "strdata1 varchar(255) not null, strdata2 varchar(255), UNIQUE INDEX (strdata1))");
        } catch (SQLException sqlEx) {
            if (sqlEx.getMessage().indexOf("max key length") != -1) {
                createTable("statement_batch_test",
                        "(id int not null primary key auto_increment, strdata1 varchar(175) not null, strdata2 varchar(175), " + "UNIQUE INDEX (strdata1))");
            }
        }

        for (int i = 6; i < MAX_COLUMNS_TO_TEST; i += STEP) {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS statement_col_test_" + i);

            StringBuilder insertBuf = new StringBuilder("INSERT INTO statement_col_test_");
            StringBuilder stmtBuf = new StringBuilder("CREATE TABLE IF NOT EXISTS statement_col_test_");
            stmtBuf.append(i);
            insertBuf.append(i);
            stmtBuf.append(" (");
            insertBuf.append(" VALUES (");

            boolean firstTime = true;

            for (int j = 0; j < i; j++) {
                if (!firstTime) {
                    stmtBuf.append(",");
                    insertBuf.append(",");
                } else {
                    firstTime = false;
                }

                stmtBuf.append("col_");
                stmtBuf.append(j);
                stmtBuf.append(" VARCHAR(");
                stmtBuf.append(MAX_COLUMN_LENGTH);
                stmtBuf.append(")");
                insertBuf.append("'");

                int numChars = 16;

                for (int k = 0; k < numChars; k++) {
                    insertBuf.append("A");
                }

                insertBuf.append("'");
            }

            stmtBuf.append(")");
            insertBuf.append(")");
            this.stmt.executeUpdate(stmtBuf.toString());
            this.stmt.executeUpdate(insertBuf.toString());
        }

        // explicitly set the catalog to exercise code in execute(), executeQuery() and executeUpdate()
        // FIXME: Only works on Windows!
        // this.conn.setCatalog(this.conn.getCatalog().toUpperCase());
    }

    @Override
    public void tearDown() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE statement_test");

            for (int i = 6; i < MAX_COLUMNS_TO_TEST; i += STEP) {
                StringBuilder stmtBuf = new StringBuilder("DROP TABLE IF EXISTS statement_col_test_");
                stmtBuf.append(i);
                this.stmt.executeUpdate(stmtBuf.toString());
            }

            try {
                this.stmt.executeUpdate("DROP TABLE statement_batch_test");
            } catch (SQLException sqlEx) {
            }
        } finally {
            super.tearDown();
        }
    }

    public void testAccessorsAndMutators() throws SQLException {
        assertTrue("Connection can not be null, and must be same connection", this.stmt.getConnection() == this.conn);

        // Set max rows, to exercise code in execute(), executeQuery() and executeUpdate()
        Statement accessorStmt = null;

        try {
            accessorStmt = this.conn.createStatement();
            accessorStmt.setMaxRows(1);
            accessorStmt.setMaxRows(0); // FIXME, test that this actually affects rows returned
            accessorStmt.setMaxFieldSize(255);
            assertTrue("Max field size should match what was set", accessorStmt.getMaxFieldSize() == 255);

            try {
                accessorStmt.setMaxFieldSize(Integer.MAX_VALUE);
                fail("Should not be able to set max field size > max_packet_size");
            } catch (SQLException sqlEx) {
                // ignore
            }

            accessorStmt.setCursorName("undef");
            accessorStmt.setEscapeProcessing(true);
            accessorStmt.setFetchDirection(java.sql.ResultSet.FETCH_FORWARD);

            int fetchDirection = accessorStmt.getFetchDirection();
            assertTrue("Set fetch direction != get fetch direction", fetchDirection == java.sql.ResultSet.FETCH_FORWARD);

            try {
                accessorStmt.setFetchDirection(Integer.MAX_VALUE);
                fail("Should not be able to set fetch direction to invalid value");
            } catch (SQLException sqlEx) {
                // ignore
            }

            try {
                accessorStmt.setMaxRows(50000000 + 10);
                fail("Should not be able to set max rows > 50000000");
            } catch (SQLException sqlEx) {
                // ignore
            }

            try {
                accessorStmt.setMaxRows(Integer.MIN_VALUE);
                fail("Should not be able to set max rows < 0");
            } catch (SQLException sqlEx) {
                // ignore
            }

            int fetchSize = this.stmt.getFetchSize();

            try {
                accessorStmt.setMaxRows(4);
                accessorStmt.setFetchSize(Integer.MAX_VALUE);
                fail("Should not be able to set FetchSize > max rows");
            } catch (SQLException sqlEx) {
                // ignore
            }

            try {
                accessorStmt.setFetchSize(-2);
                fail("Should not be able to set FetchSize < 0");
            } catch (SQLException sqlEx) {
                // ignore
            }

            assertTrue("Fetch size before invalid setFetchSize() calls should match fetch size now", fetchSize == this.stmt.getFetchSize());
        } finally {
            if (accessorStmt != null) {
                try {
                    accessorStmt.close();
                } catch (SQLException sqlEx) {
                    // ignore
                }

                accessorStmt = null;
            }
        }
    }

    public void testAutoIncrement() throws SQLException {
        try {
            this.stmt.setFetchSize(Integer.MIN_VALUE);

            this.stmt.executeUpdate("INSERT INTO statement_test (strdata1) values ('blah')", Statement.RETURN_GENERATED_KEYS);

            int autoIncKeyFromApi = -1;
            this.rs = this.stmt.getGeneratedKeys();

            if (this.rs.next()) {
                autoIncKeyFromApi = this.rs.getInt(1);
            } else {
                fail("Failed to retrieve AUTO_INCREMENT using Statement.getGeneratedKeys()");
            }

            this.rs.close();

            int autoIncKeyFromFunc = -1;
            this.rs = this.stmt.executeQuery("SELECT LAST_INSERT_ID()");

            if (this.rs.next()) {
                autoIncKeyFromFunc = this.rs.getInt(1);
            } else {
                fail("Failed to retrieve AUTO_INCREMENT using LAST_INSERT_ID()");
            }

            if ((autoIncKeyFromApi != -1) && (autoIncKeyFromFunc != -1)) {
                assertTrue("Key retrieved from API (" + autoIncKeyFromApi + ") does not match key retrieved from LAST_INSERT_ID() " + autoIncKeyFromFunc
                        + ") function", autoIncKeyFromApi == autoIncKeyFromFunc);
            } else {
                fail("AutoIncrement keys were '0'");
            }
        } finally {
            if (this.rs != null) {
                try {
                    this.rs.close();
                } catch (Exception ex) {
                    // ignore
                }
            }

            this.rs = null;
        }
    }

    /**
     * Tests all variants of numerical types (signed/unsigned) for correct
     * operation when used as return values from a prepared statement.
     * 
     * @throws Exception
     */
    public void testBinaryResultSetNumericTypes() throws Exception {
        testBinaryResultSetNumericTypesInternal(this.conn);
        Connection sspsConn = getConnectionWithProps("useServerPrepStmts=true");
        testBinaryResultSetNumericTypesInternal(sspsConn);
        sspsConn.close();
    }

    private void testBinaryResultSetNumericTypesInternal(Connection con) throws Exception {
        /*
         * TINYINT 1 -128 127 SMALLINT 2 -32768 32767 MEDIUMINT 3 -8388608
         * 8388607 INT 4 -2147483648 2147483647 BIGINT 8 -9223372036854775808
         * 9223372036854775807
         */

        String unsignedMinimum = "0";

        String tiMinimum = "-128";
        String tiMaximum = "127";
        String utiMaximum = "255";

        String siMinimum = "-32768";
        String siMaximum = "32767";
        String usiMaximum = "65535";

        String miMinimum = "-8388608";
        String miMaximum = "8388607";
        String umiMaximum = "16777215";

        String iMinimum = "-2147483648";
        String iMaximum = "2147483647";
        String uiMaximum = "4294967295";

        String biMinimum = "-9223372036854775808";
        String biMaximum = "9223372036854775807";
        String ubiMaximum = "18446744073709551615";

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBinaryResultSetNumericTypes");
            this.stmt.executeUpdate("CREATE TABLE testBinaryResultSetNumericTypes(rowOrder TINYINT, ti TINYINT,uti TINYINT UNSIGNED, si SMALLINT,"
                    + "usi SMALLINT UNSIGNED, mi MEDIUMINT,umi MEDIUMINT UNSIGNED, i INT, ui INT UNSIGNED,bi BIGINT, ubi BIGINT UNSIGNED)");
            PreparedStatement inserter = this.conn.prepareStatement("INSERT INTO testBinaryResultSetNumericTypes VALUES (?,?,?,?,?,?,?,?,?,?,?)");
            inserter.setInt(1, 0);
            inserter.setString(2, tiMinimum);
            inserter.setString(3, unsignedMinimum);
            inserter.setString(4, siMinimum);
            inserter.setString(5, unsignedMinimum);
            inserter.setString(6, miMinimum);
            inserter.setString(7, unsignedMinimum);
            inserter.setString(8, iMinimum);
            inserter.setString(9, unsignedMinimum);
            inserter.setString(10, biMinimum);
            inserter.setString(11, unsignedMinimum);
            inserter.executeUpdate();

            inserter.setInt(1, 1);
            inserter.setString(2, tiMaximum);
            inserter.setString(3, utiMaximum);
            inserter.setString(4, siMaximum);
            inserter.setString(5, usiMaximum);
            inserter.setString(6, miMaximum);
            inserter.setString(7, umiMaximum);
            inserter.setString(8, iMaximum);
            inserter.setString(9, uiMaximum);
            inserter.setString(10, biMaximum);
            inserter.setString(11, ubiMaximum);
            inserter.executeUpdate();

            PreparedStatement selector = con.prepareStatement("SELECT * FROM testBinaryResultSetNumericTypes ORDER by rowOrder ASC");
            this.rs = selector.executeQuery();

            assertTrue(this.rs.next());

            assertEquals(tiMinimum, this.rs.getString(2));
            assertEquals(unsignedMinimum, this.rs.getString(3));
            assertEquals(siMinimum, this.rs.getString(4));
            assertEquals(unsignedMinimum, this.rs.getString(5));
            assertEquals(miMinimum, this.rs.getString(6));
            assertEquals(unsignedMinimum, this.rs.getString(7));
            assertEquals(iMinimum, this.rs.getString(8));
            assertEquals(unsignedMinimum, this.rs.getString(9));
            assertEquals(biMinimum, this.rs.getString(10));
            assertEquals(unsignedMinimum, this.rs.getString(11));

            assertTrue(this.rs.next());

            assertEquals(tiMaximum, this.rs.getString(2));
            assertEquals(utiMaximum, this.rs.getString(3));
            assertEquals(siMaximum, this.rs.getString(4));
            assertEquals(usiMaximum, this.rs.getString(5));
            assertEquals(miMaximum, this.rs.getString(6));
            assertEquals(umiMaximum, this.rs.getString(7));
            assertEquals(iMaximum, this.rs.getString(8));
            assertEquals(uiMaximum, this.rs.getString(9));
            assertEquals(biMaximum, this.rs.getString(10));
            assertEquals(ubiMaximum, this.rs.getString(11));

            assertTrue(!this.rs.next());
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBinaryResultSetNumericTypes");
        }
    }

    /**
     * Tests stored procedure functionality
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testCallableStatement() throws Exception {
        CallableStatement cStmt = null;
        String stringVal = "abcdefg";
        int intVal = 42;

        try {
            try {
                this.stmt.executeUpdate("DROP PROCEDURE testCallStmt");
            } catch (SQLException sqlEx) {
                if (sqlEx.getMessage().indexOf("does not exist") == -1) {
                    throw sqlEx;
                }
            }

            this.stmt.executeUpdate("DROP TABLE IF EXISTS callStmtTbl");
            this.stmt.executeUpdate("CREATE TABLE callStmtTbl (x CHAR(16), y INT)");

            this.stmt.executeUpdate(
                    "CREATE PROCEDURE testCallStmt(n INT, x CHAR(16), y INT) WHILE n DO SET n = n - 1;" + " INSERT INTO callStmtTbl VALUES (x, y); END WHILE;");

            int rowsToCheck = 15;

            cStmt = this.conn.prepareCall("{call testCallStmt(?,?,?)}");
            cStmt.setInt(1, rowsToCheck);
            cStmt.setString(2, stringVal);
            cStmt.setInt(3, intVal);
            cStmt.execute();

            this.rs = this.stmt.executeQuery("SELECT x,y FROM callStmtTbl");

            int numRows = 0;

            while (this.rs.next()) {
                assertTrue(this.rs.getString(1).equals(stringVal) && (this.rs.getInt(2) == intVal));

                numRows++;
            }

            this.rs.close();
            this.rs = null;

            cStmt.close();
            cStmt = null;

            System.out.println(rowsToCheck + " rows returned");

            assertTrue(numRows == rowsToCheck);
        } finally {
            try {
                this.stmt.executeUpdate("DROP PROCEDURE testCallStmt");
            } catch (SQLException sqlEx) {
                if (sqlEx.getMessage().indexOf("does not exist") == -1) {
                    throw sqlEx;
                }
            }

            this.stmt.executeUpdate("DROP TABLE IF EXISTS callStmtTbl");

            if (cStmt != null) {
                cStmt.close();
            }
        }
    }

    public void testCancelStatement() throws Exception {

        Connection cancelConn = null;

        try {
            cancelConn = getConnectionWithProps((String) null);
            final Statement cancelStmt = cancelConn.createStatement();

            cancelStmt.setQueryTimeout(1);

            long begin = System.currentTimeMillis();

            try {
                cancelStmt.execute("SELECT SLEEP(30)");
            } catch (SQLException sqlEx) {
                assertTrue("Probably wasn't actually cancelled", System.currentTimeMillis() - begin < 30000);
            }

            for (int i = 0; i < 1000; i++) {
                try {
                    cancelStmt.executeQuery("SELECT 1");
                } catch (SQLException timedOutEx) {
                    break;
                }
            }

            // Make sure we can still use the connection...

            cancelStmt.setQueryTimeout(0);
            this.rs = cancelStmt.executeQuery("SELECT 1");

            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(1));

            cancelStmt.setQueryTimeout(0);

            new Thread() {

                @Override
                public void run() {
                    try {
                        try {
                            sleep(5000);
                        } catch (InterruptedException iEx) {
                            // ignore
                        }

                        cancelStmt.cancel();
                    } catch (SQLException sqlEx) {
                        throw new RuntimeException(sqlEx.toString());
                    }
                }

            }.start();

            begin = System.currentTimeMillis();

            try {
                cancelStmt.execute("SELECT SLEEP(30)");
            } catch (SQLException sqlEx) {
                assertTrue("Probably wasn't actually cancelled", System.currentTimeMillis() - begin < 30000);
            }

            for (int i = 0; i < 1000; i++) {
                try {
                    cancelStmt.executeQuery("SELECT 1");
                } catch (SQLException timedOutEx) {
                    break;
                }
            }

            // Make sure we can still use the connection...

            this.rs = cancelStmt.executeQuery("SELECT 1");

            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(1));

            final PreparedStatement cancelPstmt = cancelConn.prepareStatement("SELECT SLEEP(30)");

            cancelPstmt.setQueryTimeout(1);

            begin = System.currentTimeMillis();

            try {
                cancelPstmt.execute();
            } catch (SQLException sqlEx) {
                assertTrue("Probably wasn't actually cancelled", System.currentTimeMillis() - begin < 30000);
            }

            for (int i = 0; i < 1000; i++) {
                try {
                    cancelPstmt.executeQuery("SELECT 1");
                } catch (SQLException timedOutEx) {
                    break;
                }
            }

            // Make sure we can still use the connection...

            this.rs = cancelStmt.executeQuery("SELECT 1");

            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(1));

            cancelPstmt.setQueryTimeout(0);

            new Thread() {

                @Override
                public void run() {
                    try {
                        try {
                            sleep(5000);
                        } catch (InterruptedException iEx) {
                            // ignore
                        }

                        cancelPstmt.cancel();
                    } catch (SQLException sqlEx) {
                        throw new RuntimeException(sqlEx.toString());
                    }
                }

            }.start();

            begin = System.currentTimeMillis();

            try {
                cancelPstmt.execute();
            } catch (SQLException sqlEx) {
                assertTrue("Probably wasn't actually cancelled", System.currentTimeMillis() - begin < 30000);
            }

            for (int i = 0; i < 1000; i++) {
                try {
                    cancelPstmt.executeQuery("SELECT 1");
                } catch (SQLException timedOutEx) {
                    break;
                }
            }

            // Make sure we can still use the connection...

            this.rs = cancelStmt.executeQuery("SELECT 1");

            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(1));

            final PreparedStatement cancelClientPstmt = ((com.mysql.cj.jdbc.JdbcConnection) cancelConn).clientPrepareStatement("SELECT SLEEP(30)");

            cancelClientPstmt.setQueryTimeout(1);

            begin = System.currentTimeMillis();

            try {
                cancelClientPstmt.execute();
            } catch (SQLException sqlEx) {
                assertTrue("Probably wasn't actually cancelled", System.currentTimeMillis() - begin < 30000);
            }

            for (int i = 0; i < 1000; i++) {
                try {
                    cancelStmt.executeQuery("SELECT 1");
                } catch (SQLException timedOutEx) {
                    break;
                }
            }

            // Make sure we can still use the connection...

            this.rs = cancelStmt.executeQuery("SELECT 1");

            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(1));

            cancelClientPstmt.setQueryTimeout(0);

            new Thread() {

                @Override
                public void run() {
                    try {
                        try {
                            sleep(5000);
                        } catch (InterruptedException iEx) {
                            // ignore
                        }

                        cancelClientPstmt.cancel();
                    } catch (SQLException sqlEx) {
                        throw new RuntimeException(sqlEx.toString());
                    }
                }

            }.start();

            begin = System.currentTimeMillis();

            try {
                cancelClientPstmt.execute();
            } catch (SQLException sqlEx) {
                assertTrue("Probably wasn't actually cancelled", System.currentTimeMillis() - begin < 30000);
            }

            for (int i = 0; i < 1000; i++) {
                try {
                    cancelClientPstmt.executeQuery("SELECT 1");
                } catch (SQLException timedOutEx) {
                    break;
                }
            }

            // Make sure we can still use the connection...

            this.rs = cancelStmt.executeQuery("SELECT 1");

            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(1));

            final Connection forceCancel = getConnectionWithProps("queryTimeoutKillsConnection=true");
            final Statement forceStmt = forceCancel.createStatement();
            forceStmt.setQueryTimeout(1);

            assertThrows(MySQLTimeoutException.class, new Callable<Void>() {
                public Void call() throws Exception {
                    forceStmt.execute("SELECT SLEEP(30)");
                    return null;
                }
            });

            int count = 1000;

            for (; count > 0; count--) {
                if (forceCancel.isClosed()) {
                    break;
                }

                Thread.sleep(100);
            }

            if (count == 0) {
                fail("Connection was never killed");
            }

            assertThrows(MySQLStatementCancelledException.class, new Callable<Void>() {
                public Void call() throws Exception {
                    forceCancel.setAutoCommit(true);
                    return null;
                }
            });

        } finally {
            if (this.rs != null) {
                ResultSet toClose = this.rs;
                this.rs = null;
                toClose.close();
            }

            if (cancelConn != null) {
                cancelConn.close();
            }
        }
    }

    public void testClose() throws SQLException {
        Statement closeStmt = null;
        boolean exceptionAfterClosed = false;

        try {
            closeStmt = this.conn.createStatement();
            closeStmt.close();

            try {
                closeStmt.executeQuery("SELECT 1");
            } catch (SQLException sqlEx) {
                exceptionAfterClosed = true;
            }
        } finally {
            if (closeStmt != null) {
                try {
                    closeStmt.close();
                } catch (SQLException sqlEx) {
                    /* ignore */
                }
            }

            closeStmt = null;
        }

        assertTrue("Operations not allowed on Statement after .close() is called!", exceptionAfterClosed);
    }

    public void testEnableStreamingResults() throws Exception {
        Statement streamStmt = this.conn.createStatement();
        ((com.mysql.cj.jdbc.JdbcStatement) streamStmt).enableStreamingResults();
        assertEquals(streamStmt.getFetchSize(), Integer.MIN_VALUE);
        assertEquals(streamStmt.getResultSetType(), ResultSet.TYPE_FORWARD_ONLY);
    }

    public void testHoldingResultSetsOverClose() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.holdResultsOpenOverStatementClose.getKeyName(), "true");

        Connection conn2 = getConnectionWithProps(props);

        Statement stmt2 = null;
        PreparedStatement pstmt2 = null;

        ResultSet rs2 = null;

        try {
            stmt2 = conn2.createStatement();

            this.rs = stmt2.executeQuery("SELECT 1");
            this.rs.next();
            this.rs.getInt(1);
            stmt2.close();
            this.rs.getInt(1);

            stmt2 = conn2.createStatement();
            stmt2.execute("SELECT 1");
            this.rs = stmt2.getResultSet();
            this.rs.next();
            this.rs.getInt(1);
            stmt2.execute("SELECT 2");
            this.rs.getInt(1);

            pstmt2 = conn2.prepareStatement("SELECT 1");
            this.rs = pstmt2.executeQuery();
            this.rs.next();
            this.rs.getInt(1);
            pstmt2.close();
            this.rs.getInt(1);

            pstmt2 = conn2.prepareStatement("SELECT 1");
            this.rs = pstmt2.executeQuery();
            this.rs.next();
            this.rs.getInt(1);
            rs2 = pstmt2.executeQuery();
            this.rs.getInt(1);
            pstmt2.execute();
            this.rs.getInt(1);
            rs2.close();

            pstmt2 = ((com.mysql.cj.jdbc.JdbcConnection) conn2).clientPrepareStatement("SELECT 1");
            this.rs = pstmt2.executeQuery();
            this.rs.next();
            this.rs.getInt(1);
            pstmt2.close();
            this.rs.getInt(1);

            pstmt2 = ((com.mysql.cj.jdbc.JdbcConnection) conn2).clientPrepareStatement("SELECT 1");
            this.rs = pstmt2.executeQuery();
            this.rs.next();
            this.rs.getInt(1);
            rs2 = pstmt2.executeQuery();
            this.rs.getInt(1);
            pstmt2.execute();
            this.rs.getInt(1);
            rs2.close();

            stmt2 = conn2.createStatement();
            this.rs = stmt2.executeQuery("SELECT 1");
            this.rs.next();
            this.rs.getInt(1);
            rs2 = stmt2.executeQuery("SELECT 2");
            this.rs.getInt(1);
            this.rs = stmt2.executeQuery("SELECT 1");
            this.rs.next();
            this.rs.getInt(1);
            stmt2.executeUpdate("SET @var=1");
            this.rs.getInt(1);
            stmt2.execute("SET @var=2");
            this.rs.getInt(1);
            rs2.close();
        } finally {
            if (stmt2 != null) {
                stmt2.close();
            }
        }
    }

    public void testInsert() throws SQLException {
        try {
            boolean autoCommit = this.conn.getAutoCommit();

            // Test running a query for an update. It should fail.
            try {
                this.conn.setAutoCommit(false);
                this.stmt.executeUpdate("SELECT * FROM statement_test");
            } catch (SQLException sqlEx) {
                assertTrue("Exception thrown for unknown reason", sqlEx.getSQLState().equalsIgnoreCase("01S03"));
            } finally {
                this.conn.setAutoCommit(autoCommit);
            }

            // Test running a update for an query. It should fail.
            try {
                this.conn.setAutoCommit(false);
                this.stmt.execute("UPDATE statement_test SET strdata1='blah' WHERE 1=0");
            } catch (SQLException sqlEx) {
                assertTrue("Exception thrown for unknown reason", sqlEx.getSQLState().equalsIgnoreCase(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT));
            } finally {
                this.conn.setAutoCommit(autoCommit);
            }

            for (int i = 0; i < 10; i++) {
                int updateCount = this.stmt.executeUpdate("INSERT INTO statement_test (strdata1,strdata2) values ('abcdefg', 'poi')");
                assertTrue("Update count must be '1', was '" + updateCount + "'", (updateCount == 1));
            }

            int insertIdFromGeneratedKeys = Integer.MIN_VALUE;

            this.stmt.executeUpdate("INSERT INTO statement_test (strdata1, strdata2) values ('a', 'a'), ('b', 'b'), ('c', 'c')",
                    Statement.RETURN_GENERATED_KEYS);
            this.rs = this.stmt.getGeneratedKeys();

            if (this.rs.next()) {
                insertIdFromGeneratedKeys = this.rs.getInt(1);
            }

            this.rs.close();
            this.rs = this.stmt.executeQuery("SELECT LAST_INSERT_ID()");

            int insertIdFromServer = Integer.MIN_VALUE;

            if (this.rs.next()) {
                insertIdFromServer = this.rs.getInt(1);
            }

            assertEquals(insertIdFromGeneratedKeys, insertIdFromServer);
        } finally {
            if (this.rs != null) {
                try {
                    this.rs.close();
                } catch (Exception ex) {
                    // ignore
                }
            }

            this.rs = null;
        }
    }

    /**
     * Tests multiple statement support
     * 
     * @throws Exception
     */
    public void testMultiStatements() throws Exception {
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

            multiStmt.execute("SELECT field1 FROM testMultiStatements WHERE field1='abcd';UPDATE testMultiStatements SET field3=3;"
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

    /**
     * Tests that NULLs and '' work correctly.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public void testNulls() throws SQLException {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS nullTest");
            this.stmt.executeUpdate("CREATE TABLE IF NOT EXISTS nullTest (field_1 CHAR(20), rowOrder INT)");
            this.stmt.executeUpdate("INSERT INTO nullTest VALUES (null, 1), ('', 2)");

            this.rs = this.stmt.executeQuery("SELECT field_1 FROM nullTest ORDER BY rowOrder");

            this.rs.next();

            assertTrue("NULL field not returned as NULL", (this.rs.getString("field_1") == null) && this.rs.wasNull());

            this.rs.next();

            assertTrue("Empty field not returned as \"\"", this.rs.getString("field_1").equals("") && !this.rs.wasNull());

            this.rs.close();
        } finally {
            if (this.rs != null) {
                try {
                    this.rs.close();
                } catch (Exception ex) {
                    // ignore
                }
            }

            this.stmt.executeUpdate("DROP TABLE IF EXISTS nullTest");
        }
    }

    public void testParsedConversionWarning() throws Exception {
        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.useUsageAdvisor.getKeyName(), "true");
            Connection warnConn = getConnectionWithProps(props);

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testParsedConversionWarning");
            this.stmt.executeUpdate("CREATE TABLE testParsedConversionWarning(field1 VARCHAR(255))");
            this.stmt.executeUpdate("INSERT INTO testParsedConversionWarning VALUES ('1.0')");

            PreparedStatement badStmt = warnConn.prepareStatement("SELECT field1 FROM testParsedConversionWarning");

            this.rs = badStmt.executeQuery();
            assertTrue(this.rs.next());
            this.rs.getFloat(1);
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testParsedConversionWarning");
        }
    }

    public void testPreparedStatement() throws SQLException {
        this.stmt.executeUpdate("INSERT INTO statement_test (id, strdata1,strdata2) values (999,'abcdefg', 'poi')");
        this.pstmt = this.conn.prepareStatement("UPDATE statement_test SET strdata1=?, strdata2=? where id=999");
        this.pstmt.setString(1, "iop");
        this.pstmt.setString(2, "higjklmn");

        int updateCount = this.pstmt.executeUpdate();
        assertTrue("Update count must be '1', was '" + updateCount + "'", (updateCount == 1));

        this.pstmt.clearParameters();

        this.pstmt.close();

        this.rs = this.stmt.executeQuery("SELECT id, strdata1, strdata2 FROM statement_test");

        assertTrue(this.rs.next());
        assertTrue(this.rs.getInt(1) == 999);
        assertTrue("Expected 'iop', received '" + this.rs.getString(2) + "'", "iop".equals(this.rs.getString(2)));
        assertTrue("Expected 'higjklmn', received '" + this.rs.getString(3) + "'", "higjklmn".equals(this.rs.getString(3)));
    }

    public void testPreparedStatementBatch() throws SQLException {
        this.pstmt = this.conn.prepareStatement("INSERT INTO statement_batch_test (strdata1, strdata2) VALUES (?,?)");

        for (int i = 0; i < 1000; i++) {
            this.pstmt.setString(1, "batch_" + i);
            this.pstmt.setString(2, "batch_" + i);
            this.pstmt.addBatch();
        }

        int[] updateCounts = this.pstmt.executeBatch();

        for (int i = 0; i < updateCounts.length; i++) {
            assertTrue("Update count must be '1', was '" + updateCounts[i] + "'", (updateCounts[i] == 1));
        }
    }

    public void testRowFetch() throws Exception {
        createTable("testRowFetch", "(field1 int)");

        this.stmt.executeUpdate("INSERT INTO testRowFetch VALUES (1)");

        Connection fetchConn = null;

        Properties props = new Properties();
        props.setProperty(PropertyKey.useCursorFetch.getKeyName(), "true");

        try {
            fetchConn = getConnectionWithProps(props);

            PreparedStatement fetchStmt = fetchConn.prepareStatement("SELECT field1 FROM testRowFetch WHERE field1=1");
            fetchStmt.setFetchSize(10);
            this.rs = fetchStmt.executeQuery();
            assertTrue(this.rs.next());

            this.stmt.executeUpdate("INSERT INTO testRowFetch VALUES (2), (3)");

            fetchStmt = fetchConn.prepareStatement("SELECT field1 FROM testRowFetch ORDER BY field1");
            fetchStmt.setFetchSize(1);
            this.rs = fetchStmt.executeQuery();

            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(1));
            assertTrue(this.rs.next());
            assertEquals(2, this.rs.getInt(1));
            assertTrue(this.rs.next());
            assertEquals(3, this.rs.getInt(1));
            assertEquals(false, this.rs.next());

            this.rs = fetchStmt.executeQuery();
        } finally {
            if (fetchConn != null) {
                fetchConn.close();
            }
        }
    }

    public void testSelectColumns() throws SQLException {
        for (int i = 6; i < MAX_COLUMNS_TO_TEST; i += STEP) {
            long start = System.currentTimeMillis();
            this.rs = this.stmt.executeQuery("SELECT * from statement_col_test_" + i);

            if (this.rs.next()) {
            }

            long end = System.currentTimeMillis();
            System.out.println(i + " columns = " + (end - start) + " ms");
        }
    }

    /**
     * Tests for PreparedStatement.setObject()
     * 
     * @throws Exception
     */
    public void testSetObject() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.noDatetimeStringSync.getKeyName(), "true"); // value=true for #5
        Connection conn1 = getConnectionWithProps(props);
        Statement stmt1 = conn1.createStatement();
        createTable("t1", " (c1 DECIMAL," // instance of String
                + "c2 VARCHAR(255)," // instance of String
                + "c3 BLOB," // instance of byte[]
                + "c4 DATE," // instance of java.util.Date
                + "c5 TIMESTAMP," // instance of String
                + "c6 TIME," // instance of String
                + "c7 TIME)"); // instance of java.sql.Timestamp

        this.pstmt = conn1.prepareStatement("INSERT INTO t1 VALUES (?, ?, ?, ?, ?, ?, ?)");

        long currentTime = System.currentTimeMillis();

        this.pstmt.setObject(1, "1000", Types.DECIMAL);
        this.pstmt.setObject(2, "2000", Types.VARCHAR);
        this.pstmt.setObject(3, new byte[] { 0 }, Types.BLOB);
        this.pstmt.setObject(4, new java.util.Date(currentTime), Types.DATE);
        this.pstmt.setObject(5, "2000-01-01 23-59-59", Types.TIMESTAMP);
        this.pstmt.setObject(6, "11:22:33", Types.TIME);
        this.pstmt.setObject(7, new java.sql.Timestamp(currentTime), Types.TIME);
        this.pstmt.execute();
        this.rs = stmt1.executeQuery("SELECT * FROM t1");
        this.rs.next();

        assertEquals("1000", this.rs.getString(1));
        assertEquals("2000", this.rs.getString(2));
        assertEquals(1, ((byte[]) this.rs.getObject(3)).length);
        assertEquals(0, ((byte[]) this.rs.getObject(3))[0]);
        assertEquals(new java.sql.Date(currentTime).toString(), this.rs.getDate(4).toString());

        assertEquals("2000-01-01 23:59:59", this.rs.getString(5));

        assertEquals("11:22:33", this.rs.getString(6));
        assertEquals(new java.sql.Time(currentTime).toString(), this.rs.getString(7));
    }

    /**
     * Tests for PreparedStatement.setObject(...SQLType...)
     * 
     * @throws Exception
     */
    public void testSetObjectWithMysqlType() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.noDatetimeStringSync.getKeyName(), "true"); // value=true for #5
        Connection conn1 = getConnectionWithProps(props);
        Statement stmt1 = conn1.createStatement();
        createTable("t1", " (c1 DECIMAL," // instance of String
                + "c2 VARCHAR(255)," // instance of String
                + "c3 BLOB," // instance of byte[]
                + "c4 DATE," // instance of java.util.Date
                + "c5 TIMESTAMP NULL," // instance of String
                + "c6 TIME," // instance of String
                + "c7 TIME)"); // instance of java.sql.Timestamp

        this.pstmt = conn1.prepareStatement("INSERT INTO t1 VALUES (?, ?, ?, ?, ?, ?, ?)");

        long currentTime = System.currentTimeMillis();

        this.pstmt.setObject(1, "1000", MysqlType.DECIMAL);
        this.pstmt.setObject(2, "2000", MysqlType.VARCHAR);
        this.pstmt.setObject(3, new byte[] { 0 }, MysqlType.BLOB);
        this.pstmt.setObject(4, new java.util.Date(currentTime), MysqlType.DATE);
        this.pstmt.setObject(5, "2000-01-01 23-59-59", MysqlType.TIMESTAMP);
        this.pstmt.setObject(6, "11:22:33", MysqlType.TIME);
        this.pstmt.setObject(7, new java.sql.Timestamp(currentTime), MysqlType.TIME);
        this.pstmt.execute();

        this.pstmt.setObject(1, null, MysqlType.DECIMAL);
        this.pstmt.setObject(2, null, MysqlType.VARCHAR);
        this.pstmt.setObject(3, null, MysqlType.BLOB);
        this.pstmt.setObject(4, null, MysqlType.DATE);
        this.pstmt.setObject(5, null, MysqlType.TIMESTAMP);
        this.pstmt.setObject(6, null, MysqlType.TIME);
        this.pstmt.setObject(7, null, MysqlType.TIME);
        this.pstmt.execute();

        this.rs = stmt1.executeQuery("SELECT * FROM t1");

        this.rs.next();
        assertEquals("1000", this.rs.getString(1));
        assertEquals("2000", this.rs.getString(2));
        assertEquals(1, ((byte[]) this.rs.getObject(3)).length);
        assertEquals(0, ((byte[]) this.rs.getObject(3))[0]);
        assertEquals(new java.sql.Date(currentTime).toString(), this.rs.getDate(4).toString());
        assertEquals("2000-01-01 23:59:59", this.rs.getString(5));
        assertEquals("11:22:33", this.rs.getString(6));
        assertEquals(new java.sql.Time(currentTime).toString(), this.rs.getString(7));

        this.rs.next();
        assertEquals(null, this.rs.getString(1));
        assertEquals(null, this.rs.getString(2));
        assertEquals(null, this.rs.getObject(3));
        assertEquals(null, this.rs.getObject(3));
        assertEquals(null, this.rs.getDate(4));
        assertEquals(null, this.rs.getString(5));
        assertEquals(null, this.rs.getString(6));
        assertEquals(null, this.rs.getString(7));
    }

    public void testStatementRewriteBatch() throws Exception {
        for (int j = 0; j < 2; j++) {
            Properties props = new Properties();

            if (j == 0) {
                props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
            }

            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
            Connection multiConn = getConnectionWithProps(props);
            createTable("testStatementRewriteBatch", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
            Statement multiStmt = multiConn.createStatement();
            multiStmt.addBatch("INSERT INTO testStatementRewriteBatch(field1) VALUES (1)");
            multiStmt.addBatch("INSERT INTO testStatementRewriteBatch(field1) VALUES (2)");
            multiStmt.addBatch("INSERT INTO testStatementRewriteBatch(field1) VALUES (3)");
            multiStmt.addBatch("INSERT INTO testStatementRewriteBatch(field1) VALUES (4)");
            multiStmt.addBatch("UPDATE testStatementRewriteBatch SET field1=5 WHERE field1=1");
            multiStmt.addBatch("UPDATE testStatementRewriteBatch SET field1=6 WHERE field1=2 OR field1=3");

            int[] counts = multiStmt.executeBatch();

            ResultSet genKeys = multiStmt.getGeneratedKeys();

            for (int i = 1; i < 5; i++) {
                genKeys.next();
                assertEquals(i, genKeys.getInt(1));
            }

            assertEquals(counts.length, 6);
            assertEquals(counts[0], 1);
            assertEquals(counts[1], 1);
            assertEquals(counts[2], 1);
            assertEquals(counts[3], 1);
            assertEquals(counts[4], 1);
            assertEquals(counts[5], 2);

            this.rs = multiStmt.executeQuery("SELECT field1 FROM testStatementRewriteBatch ORDER BY field1");
            assertTrue(this.rs.next());
            assertEquals(this.rs.getInt(1), 4);
            assertTrue(this.rs.next());
            assertEquals(this.rs.getInt(1), 5);
            assertTrue(this.rs.next());
            assertEquals(this.rs.getInt(1), 6);
            assertTrue(this.rs.next());
            assertEquals(this.rs.getInt(1), 6);

            createTable("testStatementRewriteBatch", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
            props.clear();
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
            props.setProperty(PropertyKey.maxAllowedPacket.getKeyName(), "1024");
            multiConn = getConnectionWithProps(props);
            multiStmt = multiConn.createStatement();

            for (int i = 0; i < 1000; i++) {
                multiStmt.addBatch("INSERT INTO testStatementRewriteBatch(field1) VALUES (" + i + ")");
            }

            multiStmt.executeBatch();

            genKeys = multiStmt.getGeneratedKeys();

            for (int i = 1; i < 1000; i++) {
                genKeys.next();
                assertEquals(i, genKeys.getInt(1));
            }

            createTable("testStatementRewriteBatch", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");

            props.clear();
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), j == 0 ? "true" : "false");
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
            multiConn = getConnectionWithProps(props);

            PreparedStatement pStmt = null;

            pStmt = multiConn.prepareStatement("INSERT INTO testStatementRewriteBatch(field1) VALUES (?)", Statement.RETURN_GENERATED_KEYS);

            for (int i = 0; i < 1000; i++) {
                pStmt.setInt(1, i);
                pStmt.addBatch();
            }

            pStmt.executeBatch();

            genKeys = pStmt.getGeneratedKeys();

            for (int i = 1; i < 1000; i++) {
                genKeys.next();
                assertEquals(i, genKeys.getInt(1));
            }

            createTable("testStatementRewriteBatch", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), j == 0 ? "true" : "false");
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
            props.setProperty(PropertyKey.maxAllowedPacket.getKeyName(), j == 0 ? "10240" : "1024");
            multiConn = getConnectionWithProps(props);

            pStmt = multiConn.prepareStatement("INSERT INTO testStatementRewriteBatch(field1) VALUES (?)", Statement.RETURN_GENERATED_KEYS);

            for (int i = 0; i < 1000; i++) {
                pStmt.setInt(1, i);
                pStmt.addBatch();
            }

            pStmt.executeBatch();

            genKeys = pStmt.getGeneratedKeys();

            for (int i = 1; i < 1000; i++) {
                genKeys.next();
                assertEquals(i, genKeys.getInt(1));
            }

            Object[][] differentTypes = new Object[1000][14];

            createTable("rewriteBatchTypes",
                    "(internalOrder int, f1 tinyint null, " + "f2 smallint null, f3 int null, f4 bigint null, "
                            + "f5 decimal(8, 2) null, f6 float null, f7 double null, " + "f8 varchar(255) null, f9 text null, f10 blob null, f11 blob null, "
                            + (versionMeetsMinimum(5, 6, 4) ? "f12 datetime(3) null, f13 time(3) null, f14 date null)"
                                    : "f12 datetime null, f13 time null, f14 date null)"));

            for (int i = 0; i < 1000; i++) {
                differentTypes[i][0] = Math.random() < .5 ? null : new Byte((byte) (Math.random() * 127));
                differentTypes[i][1] = Math.random() < .5 ? null : new Short((short) (Math.random() * Short.MAX_VALUE));
                differentTypes[i][2] = Math.random() < .5 ? null : new Integer((int) (Math.random() * Integer.MAX_VALUE));
                differentTypes[i][3] = Math.random() < .5 ? null : new Long((long) (Math.random() * Long.MAX_VALUE));
                differentTypes[i][4] = Math.random() < .5 ? null : new BigDecimal("19.95");
                differentTypes[i][5] = Math.random() < .5 ? null : new Float(3 + ((float) (Math.random())));
                differentTypes[i][6] = Math.random() < .5 ? null : new Double(3 + (Math.random()));
                differentTypes[i][7] = Math.random() < .5 ? null : randomString();
                differentTypes[i][8] = Math.random() < .5 ? null : randomString();
                differentTypes[i][9] = Math.random() < .5 ? null : randomString().getBytes();
                differentTypes[i][10] = Math.random() < .5 ? null : randomString().getBytes();
                differentTypes[i][11] = Math.random() < .5 ? null : new Timestamp(System.currentTimeMillis());
                differentTypes[i][12] = Math.random() < .5 ? null : new Time(System.currentTimeMillis());
                differentTypes[i][13] = Math.random() < .5 ? null : new Date(System.currentTimeMillis());
            }

            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), j == 0 ? "true" : "false");
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
            props.setProperty(PropertyKey.maxAllowedPacket.getKeyName(), j == 0 ? "10240" : "1024");
            multiConn = getConnectionWithProps(props);
            pStmt = multiConn.prepareStatement(
                    "INSERT INTO rewriteBatchTypes(internalOrder,f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14) VALUES " + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

            for (int i = 0; i < 1000; i++) {
                pStmt.setInt(1, i);
                for (int k = 0; k < 14; k++) {
                    if (k == 8) {
                        String asString = (String) differentTypes[i][k];

                        if (asString == null) {
                            pStmt.setObject(k + 2, null);
                        } else {
                            pStmt.setCharacterStream(k + 2, new StringReader(asString), asString.length());
                        }
                    } else if (k == 9) {
                        byte[] asBytes = (byte[]) differentTypes[i][k];

                        if (asBytes == null) {
                            pStmt.setObject(k + 2, null);
                        } else {
                            pStmt.setBinaryStream(k + 2, new ByteArrayInputStream(asBytes), asBytes.length);
                        }
                    } else {
                        pStmt.setObject(k + 2, differentTypes[i][k]);
                    }
                }
                pStmt.addBatch();
            }

            pStmt.executeBatch();

            this.rs = this.stmt
                    .executeQuery("SELECT f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14 FROM rewriteBatchTypes ORDER BY internalOrder");

            int idx = 0;

            // We need to format this ourselves, since we have to strip the nanos off of TIMESTAMPs, so .equals() doesn't really work...

            SimpleDateFormat sdf = TimeUtil.getSimpleDateFormat(null, "''yyyy-MM-dd HH:mm:ss''", null, null);

            while (this.rs.next()) {
                for (int k = 0; k < 14; k++) {
                    if (differentTypes[idx][k] == null) {
                        assertTrue("On row " + idx + " expected NULL, found " + this.rs.getObject(k + 1) + " in column " + (k + 1),
                                this.rs.getObject(k + 1) == null);
                    } else {
                        String className = differentTypes[idx][k].getClass().getName();

                        if (className.equals("java.io.StringReader")) {
                            StringReader reader = (StringReader) differentTypes[idx][k];
                            StringBuilder buf = new StringBuilder();

                            int c = 0;

                            while ((c = reader.read()) != -1) {
                                buf.append((char) c);
                            }

                            String asString = this.rs.getString(k + 1);

                            assertEquals("On row " + idx + ", column " + (k + 1), buf.toString(), asString);

                        } else if (differentTypes[idx][k] instanceof java.io.InputStream) {
                            ByteArrayOutputStream bOut = new ByteArrayOutputStream();

                            int bytesRead = 0;

                            byte[] buf = new byte[128];
                            InputStream in = (InputStream) differentTypes[idx][k];

                            while ((bytesRead = in.read(buf)) != -1) {
                                bOut.write(buf, 0, bytesRead);
                            }

                            byte[] expected = bOut.toByteArray();
                            byte[] actual = this.rs.getBytes(k + 1);

                            assertEquals("On row " + idx + ", column " + (k + 1), StringUtils.dumpAsHex(expected, expected.length),
                                    StringUtils.dumpAsHex(actual, actual.length));
                        } else if (differentTypes[idx][k] instanceof byte[]) {
                            byte[] expected = (byte[]) differentTypes[idx][k];
                            byte[] actual = this.rs.getBytes(k + 1);
                            assertEquals("On row " + idx + ", column " + (k + 1), StringUtils.dumpAsHex(expected, expected.length),
                                    StringUtils.dumpAsHex(actual, actual.length));
                        } else if (differentTypes[idx][k] instanceof Timestamp) {
                            assertEquals("On row " + idx + ", column " + (k + 1), sdf.format(differentTypes[idx][k]), sdf.format(this.rs.getObject(k + 1)));
                        } else if (differentTypes[idx][k] instanceof Double) {
                            assertEquals("On row " + idx + ", column " + (k + 1), ((Double) differentTypes[idx][k]).doubleValue(), this.rs.getDouble(k + 1),
                                    .1);
                        } else if (differentTypes[idx][k] instanceof Float) {
                            assertEquals("On row " + idx + ", column " + (k + 1), ((Float) differentTypes[idx][k]).floatValue(), this.rs.getFloat(k + 1), .1);
                        } else if (className.equals("java.lang.Byte")) {
                            // special mapping in JDBC for ResultSet.getObject()
                            assertEquals("On row " + idx + ", column " + (k + 1), new Integer(((Byte) differentTypes[idx][k]).byteValue()),
                                    this.rs.getObject(k + 1));
                        } else if (className.equals("java.lang.Short")) {
                            // special mapping in JDBC for ResultSet.getObject()
                            assertEquals("On row " + idx + ", column " + (k + 1), new Integer(((Short) differentTypes[idx][k]).shortValue()),
                                    this.rs.getObject(k + 1));
                        } else {
                            assertEquals("On row " + idx + ", column " + (k + 1) + " (" + differentTypes[idx][k].getClass() + "/"
                                    + this.rs.getObject(k + 1).getClass(), differentTypes[idx][k].toString(), this.rs.getObject(k + 1).toString());
                        }
                    }
                }

                idx++;
            }
        }
    }

    public void testBatchRewriteErrors() throws Exception {
        createTable("rewriteErrors", "(field1 int not null primary key) ENGINE=MyISAM");

        Properties props = new Properties();
        Connection multiConn = null;

        for (int j = 0; j < 2; j++) {
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false");

            if (j == 1) {
                props.setProperty(PropertyKey.continueBatchOnError.getKeyName(), "false");
            } else {
                props.setProperty(PropertyKey.continueBatchOnError.getKeyName(), "true");
            }

            props.setProperty(PropertyKey.maxAllowedPacket.getKeyName(), "4096");
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
            multiConn = getConnectionWithProps(props);
            this.pstmt = multiConn.prepareStatement("INSERT INTO rewriteErrors VALUES (?)");
            Statement multiStmt = multiConn.createStatement();

            for (int i = 0; i < 4096; i++) {
                multiStmt.addBatch("INSERT INTO rewriteErrors VALUES (" + i + ")");
                this.pstmt.setInt(1, i);
                this.pstmt.addBatch();
            }

            multiStmt.addBatch("INSERT INTO rewriteErrors VALUES (2048)");

            this.pstmt.setInt(1, 2048);
            this.pstmt.addBatch();

            try {
                this.pstmt.executeBatch();
            } catch (BatchUpdateException bUpE) {
                int[] counts = bUpE.getUpdateCounts();

                for (int i = 4059; i < counts.length; i++) {
                    assertEquals(counts[i], Statement.EXECUTE_FAILED);
                }

                // this depends on max_allowed_packet, only a sanity check
                assertTrue(getRowCount("rewriteErrors") >= 4000);
            }

            this.stmt.execute("TRUNCATE TABLE rewriteErrors");

            try {
                multiStmt.executeBatch();
            } catch (BatchUpdateException bUpE) {
                int[] counts = bUpE.getUpdateCounts();

                for (int i = 4094; i < counts.length; i++) {
                    assertEquals(counts[i], Statement.EXECUTE_FAILED);
                }

                // this depends on max_allowed_packet, only a sanity check
                assertTrue(getRowCount("rewriteErrors") >= 4000);
            }

            this.stmt.execute("TRUNCATE TABLE rewriteErrors");

            createProcedure("sp_rewriteErrors", "(param1 INT)\nBEGIN\nINSERT INTO rewriteErrors VALUES (param1);\nEND");

            CallableStatement cStmt = multiConn.prepareCall("{ CALL sp_rewriteErrors(?)}");

            for (int i = 0; i < 4096; i++) {
                cStmt.setInt(1, i);
                cStmt.addBatch();
            }

            cStmt.setInt(1, 2048);
            cStmt.addBatch();

            try {
                cStmt.executeBatch();
            } catch (BatchUpdateException bUpE) {
                int[] counts = bUpE.getUpdateCounts();

                for (int i = 4093; i < counts.length; i++) {
                    assertEquals(counts[i], Statement.EXECUTE_FAILED);
                }

                // this depends on max_allowed_packet, only a sanity check
                assertTrue(getRowCount("rewriteErrors") >= 4000);
            }
        }
    }

    public void testStreamChange() throws Exception {
        createTable("testStreamChange", "(field1 varchar(32), field2 int, field3 TEXT, field4 BLOB)");
        this.pstmt = this.conn.prepareStatement("INSERT INTO testStreamChange VALUES (?, ?, ?, ?)");

        try {
            this.pstmt.setString(1, "A");
            this.pstmt.setInt(2, 1);

            char[] cArray = { 'A', 'B', 'C' };
            Reader r = new CharArrayReader(cArray);
            this.pstmt.setCharacterStream(3, r, cArray.length);

            byte[] bArray = { 'D', 'E', 'F' };
            ByteArrayInputStream bais = new ByteArrayInputStream(bArray);
            this.pstmt.setBinaryStream(4, bais, bArray.length);

            assertEquals(1, this.pstmt.executeUpdate());

            this.rs = this.stmt.executeQuery("SELECT field3, field4 from testStreamChange where field1='A'");
            this.rs.next();
            assertEquals("ABC", this.rs.getString(1));
            assertEquals("DEF", this.rs.getString(2));

            char[] ucArray = { 'C', 'E', 'S', 'U' };
            this.pstmt.setString(1, "CESU");
            this.pstmt.setInt(2, 3);
            Reader ucReader = new CharArrayReader(ucArray);
            this.pstmt.setCharacterStream(3, ucReader, ucArray.length);
            this.pstmt.setBinaryStream(4, null, 0);
            assertEquals(1, this.pstmt.executeUpdate());

            this.rs = this.stmt.executeQuery("SELECT field3, field4 from testStreamChange where field1='CESU'");
            this.rs.next();
            assertEquals("CESU", this.rs.getString(1));
            assertEquals(null, this.rs.getString(2));
        } finally {
            if (this.rs != null) {
                this.rs.close();
                this.rs = null;
            }

            if (this.pstmt != null) {
                this.pstmt.close();
                this.pstmt = null;
            }
        }
    }

    public void testStubbed() throws SQLException {
        try {
            this.stmt.getResultSetHoldability();
        } catch (SQLFeatureNotSupportedException notImplEx) {
        }
    }

    public void testTruncationOnRead() throws Exception {
        this.rs = this.stmt.executeQuery("SELECT '" + Long.MAX_VALUE + "'");
        this.rs.next();

        try {
            this.rs.getByte(1);
            fail("Should've thrown an out-of-range exception");
        } catch (SQLException sqlEx) {
            assertTrue(MysqlErrorNumbers.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.getShort(1);
            fail("Should've thrown an out-of-range exception");
        } catch (SQLException sqlEx) {
            assertTrue(MysqlErrorNumbers.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.getInt(1);
            fail("Should've thrown an out-of-range exception");
        } catch (SQLException sqlEx) {
            assertTrue(MysqlErrorNumbers.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE.equals(sqlEx.getSQLState()));
        }

        this.rs = this.stmt.executeQuery("SELECT '" + Double.MAX_VALUE + "'");

        this.rs.next();

        try {
            this.rs.getByte(1);
            fail("Should've thrown an out-of-range exception");
        } catch (SQLException sqlEx) {
            assertTrue(MysqlErrorNumbers.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.getShort(1);
            fail("Should've thrown an out-of-range exception");
        } catch (SQLException sqlEx) {
            assertTrue(MysqlErrorNumbers.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.getInt(1);
            fail("Should've thrown an out-of-range exception");
        } catch (SQLException sqlEx) {
            assertTrue(MysqlErrorNumbers.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.getLong(1);
            fail("Should've thrown an out-of-range exception");
        } catch (SQLException sqlEx) {
            assertTrue(MysqlErrorNumbers.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.getLong(1);
            fail("Should've thrown an out-of-range exception");
        } catch (SQLException sqlEx) {
            assertTrue(MysqlErrorNumbers.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE.equals(sqlEx.getSQLState()));
        }

        PreparedStatement pStmt = null;

        System.out.println("Testing prepared statements with binary result sets now");

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testTruncationOnRead");
            this.stmt.executeUpdate("CREATE TABLE testTruncationOnRead(intField INTEGER, bigintField BIGINT, doubleField DOUBLE)");
            this.stmt.executeUpdate("INSERT INTO testTruncationOnRead VALUES (" + Integer.MAX_VALUE + ", " + Long.MAX_VALUE + ", " + Double.MAX_VALUE + ")");
            this.stmt.executeUpdate("INSERT INTO testTruncationOnRead VALUES (" + Integer.MIN_VALUE + ", " + Long.MIN_VALUE + ", " + Double.MIN_VALUE + ")");

            pStmt = this.conn.prepareStatement("SELECT intField, bigintField, doubleField FROM testTruncationOnRead ORDER BY intField DESC");
            this.rs = pStmt.executeQuery();

            this.rs.next();

            try {
                this.rs.getByte(1);
                fail("Should've thrown an out-of-range exception");
            } catch (SQLException sqlEx) {
                assertTrue(MysqlErrorNumbers.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE.equals(sqlEx.getSQLState()));
            }

            try {
                this.rs.getInt(2);
                fail("Should've thrown an out-of-range exception");
            } catch (SQLException sqlEx) {
                assertTrue(MysqlErrorNumbers.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE.equals(sqlEx.getSQLState()));
            }

            try {
                this.rs.getLong(3);
                fail("Should've thrown an out-of-range exception");
            } catch (SQLException sqlEx) {
                assertTrue(MysqlErrorNumbers.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE.equals(sqlEx.getSQLState()));
            }
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testTruncationOnRead");
        }

    }

    public void testQueryInterceptors() throws Exception {
        Connection interceptedConn = null;

        /*
         * try {
         * Properties props = new Properties();
         * props.setProperty(PropertyKey.queryInterceptors", "com.mysql.jdbc.interceptors.ResultSetScannerInterceptor");
         * props.setProperty(PropertyKey.resultSetScannerRegex", ".*");
         * interceptedConn = getConnectionWithProps(props);
         * this.rs = interceptedConn.createStatement().executeQuery("SELECT 'abc'");
         * this.rs.next();
         * this.rs.getString(1);
         * } finally {
         * closeMemberJDBCResources();
         * 
         * if (interceptedConn != null) {
         * interceptedConn.close();
         * }
         * }
         */

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.queryInterceptors.getKeyName(), ServerStatusDiffInterceptor.class.getName());

            interceptedConn = getConnectionWithProps(props);
            this.rs = interceptedConn.createStatement().executeQuery("SELECT 'abc'");
        } finally {
            if (interceptedConn != null) {
                interceptedConn.close();
            }
        }
    }

    public void testParameterBindings() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
        props.setProperty(PropertyKey.treatUtilDateAsTimestamp.getKeyName(), "false");
        props.setProperty(PropertyKey.autoDeserialize.getKeyName(), "true");

        for (boolean useSPS : new boolean[] { false, true }) {
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            // Need to check character set stuff, so need a new connection
            Connection utfConn = getConnectionWithProps(props);

            java.util.Date now = new java.util.Date();

            Object[] valuesToTest = new Object[] { new Byte(Byte.MIN_VALUE), new Short(Short.MIN_VALUE), new Integer(Integer.MIN_VALUE),
                    new Long(Long.MIN_VALUE), new Double(Double.MIN_VALUE), "\u4E2D\u6587", new BigDecimal(Math.PI), null, // to test isNull
                    now // to test serialization
            };

            StringBuilder statementText = new StringBuilder("SELECT ?");

            for (int i = 1; i < valuesToTest.length; i++) {
                statementText.append(",?");
            }

            this.pstmt = utfConn.prepareStatement(statementText.toString());

            for (int i = 0; i < valuesToTest.length; i++) {
                this.pstmt.setObject(i + 1, valuesToTest[i]);
            }

            ParameterBindings bindings = ((ClientPreparedStatement) this.pstmt).getParameterBindings();

            for (int i = 0; i < valuesToTest.length; i++) {
                Object boundObject = bindings.getObject(i + 1);

                if (boundObject == null || valuesToTest[i] == null) {
                    continue;
                }

                Class<?> boundObjectClass = boundObject.getClass();
                Class<?> testObjectClass = valuesToTest[i].getClass();

                if (boundObject instanceof Number) {
                    assertEquals("For binding #" + (i + 1) + " of class " + boundObjectClass + " compared to " + testObjectClass, valuesToTest[i].toString(),
                            boundObject.toString());
                } else if (boundObject instanceof Date) {

                } else {
                    assertEquals("For binding #" + (i + 1) + " of class " + boundObjectClass + " compared to " + testObjectClass, valuesToTest[i], boundObject);
                }
            }
        }
    }

    public void testLocalInfileHooked() throws Exception {
        createTable("localInfileHooked", "(field1 int, field2 varchar(255))");
        String streamData = "1\tabcd\n2\tefgh\n3\tijkl";
        InputStream stream = new ByteArrayInputStream(streamData.getBytes());

        Properties props = new Properties();
        props.setProperty(PropertyKey.allowLoadLocalInfile.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);
        Statement testStmt = testConn.createStatement();

        try {
            ((com.mysql.cj.jdbc.JdbcStatement) testStmt).setLocalInfileInputStream(stream);
            testStmt.execute(
                    "LOAD DATA LOCAL INFILE 'bogusFileName' INTO TABLE localInfileHooked CHARACTER SET " + CharsetMapping.getMysqlCharsetForJavaEncoding(
                            ((MysqlConnection) this.conn).getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue(), this.serverVersion));
            assertEquals(-1, stream.read());
            this.rs = testStmt.executeQuery("SELECT field2 FROM localInfileHooked ORDER BY field1 ASC");
            this.rs.next();
            assertEquals("abcd", this.rs.getString(1));
            this.rs.next();
            assertEquals("efgh", this.rs.getString(1));
            this.rs.next();
            assertEquals("ijkl", this.rs.getString(1));
        } finally {
            ((com.mysql.cj.jdbc.JdbcStatement) testStmt).setLocalInfileInputStream(null);
            testConn.close();
        }
    }

    /**
     * Tests for ResultSet.getNCharacterStream()
     * 
     * @throws Exception
     */
    public void testGetNCharacterStream() throws Exception {
        createTable("testGetNCharacterStream", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10))");
        this.stmt.executeUpdate("INSERT INTO testGetNCharacterStream (c1, c2) VALUES (_utf8 'aaa', _utf8 'bbb')");
        this.rs = this.stmt.executeQuery("SELECT c1, c2 FROM testGetNCharacterStream");
        this.rs.next();
        char[] c1 = new char[3];
        this.rs.getNCharacterStream(1).read(c1);
        assertEquals("aaa", new String(c1));
        char[] c2 = new char[3];
        this.rs.getNCharacterStream("c2").read(c2);
        assertEquals("bbb", new String(c2));
        this.rs.close();
    }

    /**
     * Tests for ResultSet.getNClob()
     * 
     * @throws Exception
     */
    public void testGetNClob() throws Exception {
        createTable("testGetNClob", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10))");
        this.stmt.executeUpdate("INSERT INTO testGetNClob (c1, c2) VALUES (_utf8 'aaa', _utf8 'bbb')");
        this.rs = this.stmt.executeQuery("SELECT c1, c2 FROM testGetNClob");
        this.rs.next();
        char[] c1 = new char[3];
        this.rs.getNClob(1).getCharacterStream().read(c1);
        assertEquals("aaa", new String(c1));
        char[] c2 = new char[3];
        this.rs.getNClob("c2").getCharacterStream().read(c2);
        assertEquals("bbb", new String(c2));
        this.rs.close();

        // for isBinaryEncoded = true, using PreparedStatement
        createTable("testGetNClob", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10))");
        this.stmt.executeUpdate("INSERT INTO testGetNClob (c1, c2) VALUES (_utf8 'aaa', _utf8 'bbb')");
        this.pstmt = this.conn.prepareStatement("SELECT c1, c2 FROM testGetNClob");
        this.rs = this.pstmt.executeQuery();
        this.rs.next();
        c1 = new char[3];
        this.rs.getNClob(1).getCharacterStream().read(c1);
        assertEquals("aaa", new String(c1));
        c2 = new char[3];
        this.rs.getNClob("c2").getCharacterStream().read(c2);
        assertEquals("bbb", new String(c2));
        this.rs.close();
    }

    /**
     * Tests for ResultSet.getNString()
     * 
     * @throws Exception
     */
    public void testGetNString() throws Exception {
        createTable("testGetNString", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10))");
        this.stmt.executeUpdate("INSERT INTO testGetNString (c1, c2) VALUES (_utf8 'aaa', _utf8 'bbb')");
        this.rs = this.stmt.executeQuery("SELECT c1, c2 FROM testGetNString");
        this.rs.next();
        assertEquals("aaa", this.rs.getNString(1));
        assertEquals("bbb", this.rs.getNString("c2"));
        this.rs.close();
    }

    /**
     * Tests for PreparedStatement.setNCharacterSteam()
     * 
     * @throws Exception
     */
    public void testSetNCharacterStream() throws Exception {
        // suppose sql_mode don't include "NO_BACKSLASH_ESCAPES"

        createTable("testSetNCharacterStream", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " + "c3 NATIONAL CHARACTER(10)) ENGINE=InnoDB");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false"); // use client-side prepared statement
        props1.setProperty(PropertyKey.characterEncoding.getKeyName(), "latin1"); // ensure charset isn't utf8 here
        Connection conn1 = getConnectionWithProps(props1);
        ClientPreparedStatement pstmt1 = (ClientPreparedStatement) conn1.prepareStatement("INSERT INTO testSetNCharacterStream (c1, c2, c3) VALUES (?, ?, ?)");
        pstmt1.setNCharacterStream(1, null, 0);
        pstmt1.setNCharacterStream(2, new StringReader("aaa"), 3);
        pstmt1.setNCharacterStream(3, new StringReader("\'aaa\'"), 5);
        pstmt1.execute();
        ResultSet rs1 = this.stmt.executeQuery("SELECT c1, c2, c3 FROM testSetNCharacterStream");
        rs1.next();
        assertEquals(null, rs1.getString(1));
        assertEquals("aaa", rs1.getString(2));
        assertEquals("\'aaa\'", rs1.getString(3));
        rs1.close();
        pstmt1.close();
        conn1.close();

        createTable("testSetNCharacterStream", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " + "c3 NATIONAL CHARACTER(10)) ENGINE=InnoDB");
        Properties props2 = new Properties();
        props2.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false"); // use client-side prepared statement
        props2.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8"); // ensure charset is utf8 here
        Connection conn2 = getConnectionWithProps(props2);
        ClientPreparedStatement pstmt2 = (ClientPreparedStatement) conn2.prepareStatement("INSERT INTO testSetNCharacterStream (c1, c2, c3) VALUES (?, ?, ?)");
        pstmt2.setNCharacterStream(1, null, 0);
        pstmt2.setNCharacterStream(2, new StringReader("aaa"), 3);
        pstmt2.setNCharacterStream(3, new StringReader("\'aaa\'"), 5);
        pstmt2.execute();
        ResultSet rs2 = this.stmt.executeQuery("SELECT c1, c2, c3 FROM testSetNCharacterStream");
        rs2.next();
        assertEquals(null, rs2.getString(1));
        assertEquals("aaa", rs2.getString(2));
        assertEquals("\'aaa\'", rs2.getString(3));
        rs2.close();
        pstmt2.close();
        conn2.close();
    }

    /**
     * Tests for ServerPreparedStatement.setNCharacterSteam()
     * 
     * @throws Exception
     */
    public void testSetNCharacterStreamServer() throws Exception {
        createTable("testSetNCharacterStreamServer", "(c1 NATIONAL CHARACTER(10)) ENGINE=InnoDB");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props1.setProperty(PropertyKey.characterEncoding.getKeyName(), "latin1"); // ensure charset isn't utf8 here
        Connection conn1 = getConnectionWithProps(props1);
        PreparedStatement pstmt1 = conn1.prepareStatement("INSERT INTO testSetNCharacterStreamServer (c1) VALUES (?)");
        try {
            pstmt1.setNCharacterStream(1, new StringReader("aaa"), 3);
            fail();
        } catch (SQLException e) {
            // ok
            assertEquals("Can not call setNCharacterStream() when connection character set isn't UTF-8", e.getMessage());
        }
        pstmt1.close();
        conn1.close();

        createTable("testSetNCharacterStreamServer", "(c1 LONGTEXT charset utf8) ENGINE=InnoDB");
        Properties props2 = new Properties();
        props2.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props2.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8"); // ensure charset is utf8 here
        Connection conn2 = getConnectionWithProps(props2);
        PreparedStatement pstmt2 = conn2.prepareStatement("INSERT INTO testSetNCharacterStreamServer (c1) VALUES (?)");
        pstmt2.setNCharacterStream(1, new StringReader(new String(new char[81921])), 81921); // 10 Full Long Data Packet's chars + 1 char
        pstmt2.execute();
        ResultSet rs2 = this.stmt.executeQuery("SELECT c1 FROM testSetNCharacterStreamServer");
        rs2.next();
        assertEquals(new String(new char[81921]), rs2.getString(1));
        rs2.close();
        pstmt2.close();
        conn2.close();
    }

    /**
     * Tests for PreparedStatement.setNClob()
     * 
     * @throws Exception
     */
    public void testSetNClob() throws Exception {
        // suppose sql_mode don't include "NO_BACKSLASH_ESCAPES"

        createTable("testSetNClob", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " + "c3 NATIONAL CHARACTER(10)) ENGINE=InnoDB");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false"); // use client-side prepared statement
        props1.setProperty(PropertyKey.characterEncoding.getKeyName(), "latin1"); // ensure charset isn't utf8 here
        Connection conn1 = getConnectionWithProps(props1);
        PreparedStatement pstmt1 = conn1.prepareStatement("INSERT INTO testSetNClob (c1, c2, c3) VALUES (?, ?, ?)");
        pstmt1.setNClob(1, (NClob) null);
        NClob nclob2 = conn1.createNClob();
        nclob2.setString(1, "aaa");
        pstmt1.setNClob(2, nclob2);                   // for setNClob(int, NClob)
        Reader reader3 = new StringReader("\'aaa\'");
        pstmt1.setNClob(3, reader3, 5);               // for setNClob(int, Reader, long)
        pstmt1.execute();
        ResultSet rs1 = this.stmt.executeQuery("SELECT c1, c2, c3 FROM testSetNClob");
        rs1.next();
        assertEquals(null, rs1.getString(1));
        assertEquals("aaa", rs1.getString(2));
        assertEquals("\'aaa\'", rs1.getString(3));
        rs1.close();
        pstmt1.close();
        conn1.close();

        createTable("testSetNClob", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " + "c3 NATIONAL CHARACTER(10)) ENGINE=InnoDB");
        Properties props2 = new Properties();
        props2.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false"); // use client-side prepared statement
        props2.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8"); // ensure charset is utf8 here
        Connection conn2 = getConnectionWithProps(props2);
        PreparedStatement pstmt2 = conn2.prepareStatement("INSERT INTO testSetNClob (c1, c2, c3) VALUES (?, ?, ?)");
        pstmt2.setNClob(1, (NClob) null);
        nclob2 = conn2.createNClob();
        nclob2.setString(1, "aaa");
        pstmt2.setNClob(2, nclob2);             // for setNClob(int, NClob)
        reader3 = new StringReader("\'aaa\'");
        pstmt2.setNClob(3, reader3, 5);         // for setNClob(int, Reader, long)
        pstmt2.execute();
        ResultSet rs2 = this.stmt.executeQuery("SELECT c1, c2, c3 FROM testSetNClob");
        rs2.next();
        assertEquals(null, rs2.getString(1));
        assertEquals("aaa", rs2.getString(2));
        assertEquals("\'aaa\'", rs2.getString(3));
        rs2.close();
        pstmt2.close();
        conn2.close();
    }

    /**
     * Tests for ServerPreparedStatement.setNClob()
     * 
     * @throws Exception
     */
    public void testSetNClobServer() throws Exception {
        createTable("testSetNClobServer", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10)) ENGINE=InnoDB");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props1.setProperty(PropertyKey.characterEncoding.getKeyName(), "latin1"); // ensure charset isn't utf8 here
        Connection conn1 = getConnectionWithProps(props1);
        PreparedStatement pstmt1 = conn1.prepareStatement("INSERT INTO testSetNClobServer (c1, c2) VALUES (?, ?)");
        NClob nclob1 = conn1.createNClob();
        nclob1.setString(1, "aaa");
        Reader reader2 = new StringReader("aaa");
        try {
            pstmt1.setNClob(1, nclob1);
            fail();
        } catch (SQLException e) {
            // ok
            assertEquals("Can not call setNClob() when connection character set isn't UTF-8", e.getMessage());
        }
        try {
            pstmt1.setNClob(2, reader2, 3);
            fail();
        } catch (SQLException e) {
            // ok
            assertEquals("Can not call setNClob() when connection character set isn't UTF-8", e.getMessage());
        }
        pstmt1.close();
        conn1.close();

        createTable("testSetNClobServer", "(c1 NATIONAL CHARACTER(10), c2 LONGTEXT charset utf8) ENGINE=InnoDB");
        Properties props2 = new Properties();
        props2.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props2.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8"); // ensure charset is utf8 here
        Connection conn2 = getConnectionWithProps(props2);
        PreparedStatement pstmt2 = conn2.prepareStatement("INSERT INTO testSetNClobServer (c1, c2) VALUES (?, ?)");
        nclob1 = conn2.createNClob();
        nclob1.setString(1, "aaa");
        pstmt2.setNClob(1, nclob1);
        pstmt2.setNClob(2, new StringReader(new String(new char[81921])), 81921); // 10 Full Long Data Packet's chars + 1 char
        pstmt2.execute();
        ResultSet rs2 = this.stmt.executeQuery("SELECT c1, c2 FROM testSetNClobServer");
        rs2.next();
        assertEquals("aaa", rs2.getString(1));
        assertEquals(new String(new char[81921]), rs2.getString(2));
        rs2.close();
        pstmt2.close();
        conn2.close();
    }

    /**
     * Tests for PreparedStatement.setNString()
     * 
     * @throws Exception
     */
    public void testSetNString() throws Exception {
        // suppose sql_mode don't include "NO_BACKSLASH_ESCAPES"

        createTable("testSetNString",
                "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " + "c3 NATIONAL CHARACTER(10)) DEFAULT CHARACTER SET cp932 ENGINE=InnoDB");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false"); // use client-side prepared statement
        props1.setProperty(PropertyKey.characterEncoding.getKeyName(), "MS932"); // ensure charset isn't utf8 here
        Connection conn1 = getConnectionWithProps(props1);
        PreparedStatement pstmt1 = conn1.prepareStatement("INSERT INTO testSetNString (c1, c2, c3) VALUES (?, ?, ?)");
        pstmt1.setNString(1, null);
        pstmt1.setNString(2, "aaa");
        pstmt1.setNString(3, "\'aaa\'");
        pstmt1.execute();
        ResultSet rs1 = this.stmt.executeQuery("SELECT c1, c2, c3 FROM testSetNString");
        rs1.next();
        assertEquals(null, rs1.getString(1));
        assertEquals("aaa", rs1.getString(2));
        assertEquals("\'aaa\'", rs1.getString(3));
        rs1.close();
        pstmt1.close();
        conn1.close();

        createTable("testSetNString",
                "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " + "c3 NATIONAL CHARACTER(10)) DEFAULT CHARACTER SET cp932 ENGINE=InnoDB");
        Properties props2 = new Properties();
        props2.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false"); // use client-side prepared statement
        props2.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8"); // ensure charset is utf8 here
        Connection conn2 = getConnectionWithProps(props2);
        PreparedStatement pstmt2 = conn2.prepareStatement("INSERT INTO testSetNString (c1, c2, c3) VALUES (?, ?, ?)");
        pstmt2.setNString(1, null);
        pstmt2.setNString(2, "aaa");
        pstmt2.setNString(3, "\'aaa\'");
        pstmt2.execute();
        ResultSet rs2 = this.stmt.executeQuery("SELECT c1, c2, c3 FROM testSetNString");
        rs2.next();
        assertEquals(null, rs2.getString(1));
        assertEquals("aaa", rs2.getString(2));
        assertEquals("\'aaa\'", rs2.getString(3));
        rs2.close();
        pstmt2.close();
        conn2.close();
    }

    /**
     * Tests for ServerPreparedStatement.setNString()
     * 
     * @throws Exception
     */
    public void testSetNStringServer() throws Exception {
        createTable("testSetNStringServer", "(c1 NATIONAL CHARACTER(10)) ENGINE=InnoDB");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props1.setProperty(PropertyKey.characterEncoding.getKeyName(), "latin1"); // ensure charset isn't utf8 here
        Connection conn1 = getConnectionWithProps(props1);
        PreparedStatement pstmt1 = conn1.prepareStatement("INSERT INTO testSetNStringServer (c1) VALUES (?)");
        try {
            pstmt1.setNString(1, "aaa");
            fail();
        } catch (SQLException e) {
            // ok
            assertEquals("Can not call setNString() when connection character set isn't UTF-8", e.getMessage());
        }
        pstmt1.close();
        conn1.close();

        createTable("testSetNStringServer", "(c1 NATIONAL CHARACTER(10)) ENGINE=InnoDB");
        Properties props2 = new Properties();
        props2.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props2.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8"); // ensure charset is utf8 here
        Connection conn2 = getConnectionWithProps(props2);
        PreparedStatement pstmt2 = conn2.prepareStatement("INSERT INTO testSetNStringServer (c1) VALUES (?)");
        pstmt2.setNString(1, "\'aaa\'");
        pstmt2.execute();
        ResultSet rs2 = this.stmt.executeQuery("SELECT c1 FROM testSetNStringServer");
        rs2.next();
        assertEquals("\'aaa\'", rs2.getString(1));
        rs2.close();
        pstmt2.close();
        conn2.close();
    }

    /**
     * Tests for ResultSet.updateNCharacterStream()
     * 
     * @throws Exception
     */
    public void testUpdateNCharacterStream() throws Exception {
        createTable("testUpdateNCharacterStream", "(c1 CHAR(10) PRIMARY KEY, c2 NATIONAL CHARACTER(10)) default character set sjis");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props1.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8"); // ensure charset isn't utf8 here
        Connection conn1 = getConnectionWithProps(props1);
        PreparedStatement pstmt1 = conn1.prepareStatement("INSERT INTO testUpdateNCharacterStream (c1, c2) VALUES (?, ?)");
        pstmt1.setString(1, "1");
        pstmt1.setNCharacterStream(2, new StringReader("aaa"), 3);
        pstmt1.execute();
        Statement stmt1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs1 = stmt1.executeQuery("SELECT c1, c2 FROM testUpdateNCharacterStream");
        rs1.next();
        rs1.updateNCharacterStream("c2", new StringReader("bbb"), 3);
        rs1.updateRow();
        rs1.moveToInsertRow();
        rs1.updateString("c1", "2");
        rs1.updateNCharacterStream("c2", new StringReader("ccc"), 3);
        rs1.insertRow();
        ResultSet rs2 = stmt1.executeQuery("SELECT c1, c2 FROM testUpdateNCharacterStream");
        rs2.next();
        assertEquals("1", rs2.getString("c1"));
        assertEquals("bbb", rs2.getNString("c2"));
        rs2.next();
        assertEquals("2", rs2.getString("c1"));
        assertEquals("ccc", rs2.getNString("c2"));
        pstmt1.close();
        stmt1.close();
        conn1.close();

        createTable("testUpdateNCharacterStream", "(c1 CHAR(10) PRIMARY KEY, c2 CHAR(10)) default character set sjis"); // sjis field
        Properties props2 = new Properties();
        props2.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props2.setProperty(PropertyKey.characterEncoding.getKeyName(), "SJIS"); // ensure charset isn't utf8 here
        Connection conn2 = getConnectionWithProps(props2);
        PreparedStatement pstmt2 = conn2.prepareStatement("INSERT INTO testUpdateNCharacterStream (c1, c2) VALUES (?, ?)");
        pstmt2.setString(1, "1");
        pstmt2.setString(2, "aaa");
        pstmt2.execute();
        Statement stmt2 = conn2.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs3 = stmt2.executeQuery("SELECT c1, c2 FROM testUpdateNCharacterStream");
        rs3.next();
        try {
            rs3.updateNCharacterStream("c2", new StringReader("bbb"), 3); // field's charset isn't utf8
            fail();
        } catch (SQLException ex) {
            assertEquals("Can not call updateNCharacterStream() when field's character set isn't UTF-8", ex.getMessage());
        }
        rs3.close();
        pstmt2.close();
        stmt2.close();
        conn2.close();
    }

    /**
     * Tests for ResultSet.updateNClob()
     * 
     * @throws Exception
     */
    public void testUpdateNClob() throws Exception {
        createTable("testUpdateNChlob", "(c1 CHAR(10) PRIMARY KEY, c2 NATIONAL CHARACTER(10)) default character set sjis");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props1.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8"); // ensure charset isn't utf8 here
        Connection conn1 = getConnectionWithProps(props1);
        PreparedStatement pstmt1 = conn1.prepareStatement("INSERT INTO testUpdateNChlob (c1, c2) VALUES (?, ?)");
        pstmt1.setString(1, "1");
        NClob nClob1 = conn1.createNClob();
        nClob1.setString(1, "aaa");
        pstmt1.setNClob(2, nClob1);
        pstmt1.execute();
        Statement stmt1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs1 = stmt1.executeQuery("SELECT c1, c2 FROM testUpdateNChlob");
        rs1.next();
        NClob nClob2 = conn1.createNClob();
        nClob2.setString(1, "bbb");
        rs1.updateNClob("c2", nClob2);
        rs1.updateRow();
        rs1.moveToInsertRow();
        rs1.updateString("c1", "2");
        NClob nClob3 = conn1.createNClob();
        nClob3.setString(1, "ccc");
        rs1.updateNClob("c2", nClob3);
        rs1.insertRow();
        ResultSet rs2 = stmt1.executeQuery("SELECT c1, c2 FROM testUpdateNChlob");
        rs2.next();
        assertEquals("1", rs2.getString("c1"));
        assertEquals("bbb", rs2.getNString("c2"));
        rs2.next();
        assertEquals("2", rs2.getString("c1"));
        assertEquals("ccc", rs2.getNString("c2"));
        pstmt1.close();
        stmt1.close();
        conn1.close();

        createTable("testUpdateNChlob", "(c1 CHAR(10) PRIMARY KEY, c2 CHAR(10)) default character set sjis"); // sjis field
        Properties props2 = new Properties();
        props2.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props2.setProperty(PropertyKey.characterEncoding.getKeyName(), "SJIS"); // ensure charset isn't utf8 here
        Connection conn2 = getConnectionWithProps(props2);
        PreparedStatement pstmt2 = conn2.prepareStatement("INSERT INTO testUpdateNChlob (c1, c2) VALUES (?, ?)");
        pstmt2.setString(1, "1");
        pstmt2.setString(2, "aaa");
        pstmt2.execute();
        Statement stmt2 = conn2.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs3 = stmt2.executeQuery("SELECT c1, c2 FROM testUpdateNChlob");
        rs3.next();
        NClob nClob4 = conn2.createNClob();
        nClob4.setString(1, "bbb");
        try {
            rs3.updateNClob("c2", nClob4); // field's charset isn't utf8
            fail();
        } catch (SQLException ex) {
            assertEquals("Can not call updateNClob() when field's character set isn't UTF-8", ex.getMessage());
        }
        rs3.close();
        pstmt2.close();
        stmt2.close();
        conn2.close();
    }

    /**
     * Tests for ResultSet.updateNString()
     * 
     * @throws Exception
     */
    public void testUpdateNString() throws Exception {
        createTable("testUpdateNString", "(c1 CHAR(10) PRIMARY KEY, c2 NATIONAL CHARACTER(10)) default character set sjis");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props1.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8"); // ensure charset is utf8 here
        Connection conn1 = getConnectionWithProps(props1);
        PreparedStatement pstmt1 = conn1.prepareStatement("INSERT INTO testUpdateNString (c1, c2) VALUES (?, ?)");
        pstmt1.setString(1, "1");
        pstmt1.setNString(2, "aaa");
        pstmt1.execute();
        Statement stmt1 = conn1.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs1 = stmt1.executeQuery("SELECT c1, c2 FROM testUpdateNString");
        rs1.next();
        rs1.updateNString("c2", "bbb");
        rs1.updateRow();
        rs1.moveToInsertRow();
        rs1.updateString("c1", "2");
        rs1.updateNString("c2", "ccc");
        rs1.insertRow();
        ResultSet rs2 = stmt1.executeQuery("SELECT c1, c2 FROM testUpdateNString");
        rs2.next();
        assertEquals("1", rs2.getString("c1"));
        assertEquals("bbb", rs2.getNString("c2"));
        rs2.next();
        assertEquals("2", rs2.getString("c1"));
        assertEquals("ccc", rs2.getNString("c2"));
        pstmt1.close();
        stmt1.close();
        conn1.close();

        createTable("testUpdateNString", "(c1 CHAR(10) PRIMARY KEY, c2 CHAR(10)) default character set sjis"); // sjis field
        Properties props2 = new Properties();
        props2.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props2.setProperty(PropertyKey.characterEncoding.getKeyName(), "SJIS"); // ensure charset isn't utf8 here
        Connection conn2 = getConnectionWithProps(props2);
        PreparedStatement pstmt2 = conn2.prepareStatement("INSERT INTO testUpdateNString (c1, c2) VALUES (?, ?)");
        pstmt2.setString(1, "1");
        pstmt2.setString(2, "aaa");
        pstmt2.execute();
        Statement stmt2 = conn2.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        ResultSet rs3 = stmt2.executeQuery("SELECT c1, c2 FROM testUpdateNString");
        rs3.next();
        try {
            rs3.updateNString("c2", "bbb"); // field's charset isn't utf8
            fail();
        } catch (SQLException ex) {
            assertEquals("Can not call updateNString() when field's character set isn't UTF-8", ex.getMessage());
        }
        rs3.close();
        pstmt2.close();
        stmt2.close();
        conn2.close();
    }

    public void testJdbc4LoadBalancing() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), CountingReBalanceStrategy.class.getName());
        props.setProperty(PropertyKey.loadBalanceAutoCommitStatementThreshold.getKeyName(), "3");

        String portNumber = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.PORT.getKeyName());

        if (portNumber == null) {
            portNumber = "3306";
        }

        Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);
        try {
            conn2.createNClob();
        } catch (SQLException e) {
            fail("Unable to call Connection.createNClob() in load-balanced connection");
        }

    }

    // Shared test data
    private final String testDateString = "2015-08-04";
    private final String testTimeString = "12:34:56";
    private final String testDateTimeString = this.testDateString + " " + this.testTimeString;
    private final String testISODateTimeString = this.testDateString + "T" + this.testTimeString;

    private final Date testSqlDate = Date.valueOf(this.testDateString);
    private final Time testSqlTime = Time.valueOf(this.testTimeString);
    private final Timestamp testSqlTimeStamp = Timestamp.valueOf(this.testDateTimeString);

    private final LocalDate testLocalDate = LocalDate.parse(this.testDateString);
    private final LocalTime testLocalTime = LocalTime.parse(this.testTimeString);
    private final LocalDateTime testLocalDateTime = LocalDateTime.parse(this.testISODateTimeString);

    private final OffsetDateTime testOffsetDateTime = OffsetDateTime.of(2015, 8, 04, 12, 34, 56, 7890, ZoneOffset.UTC);
    private final OffsetTime testOffsetTime = OffsetTime.of(12, 34, 56, 7890, ZoneOffset.UTC);

    /**
     * Test shared test data validity.
     */
    public void testSharedTestData() throws Exception {
        assertEquals(this.testSqlDate, Date.valueOf(this.testLocalDate));
        assertEquals(this.testSqlTime, Time.valueOf(this.testLocalTime));
        assertEquals(this.testSqlTimeStamp, Timestamp.valueOf(this.testLocalDateTime));

        assertEquals(this.testLocalDate, this.testSqlDate.toLocalDate());
        assertEquals(this.testLocalTime, this.testSqlTime.toLocalTime());
        assertEquals(this.testLocalDateTime, this.testSqlTimeStamp.toLocalDateTime());
    }

    /**
     * Test for Statement.executeLargeBatch(). Validate update count returned and generated keys.
     */
    public void testStmtExecuteLargeBatch() throws Exception {
        /*
         * Fully working batch
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (1)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (2)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (3)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (4)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (5), (6), (7)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (8)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (9), (10)");

        long[] counts = this.stmt.executeLargeBatch();
        assertEquals(7, counts.length);
        assertEquals(1, counts[0]);
        assertEquals(1, counts[1]);
        assertEquals(1, counts[2]);
        assertEquals(1, counts[3]);
        assertEquals(3, counts[4]);
        assertEquals(1, counts[5]);
        assertEquals(2, counts[6]);

        this.rs = this.stmt.getGeneratedKeys();

        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals(JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
        assertEquals(20, rsmd.getColumnDisplaySize(1));

        long generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(10, generatedKey);
        this.rs.close();

        /*
         * Batch with failing queries
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (1)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (2)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch VALUES (3)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (4)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (5), (6), (7)");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES ('eight')");
        this.stmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (9), (10)");

        try {
            this.stmt.executeLargeBatch();
            fail("BatchUpdateException expected");
        } catch (BatchUpdateException e) {
            assertEquals("Incorrect integer value: 'eight' for column 'n' at row 1", e.getMessage());
            counts = e.getLargeUpdateCounts();
            assertEquals(7, counts.length);
            assertEquals(1, counts[0]);
            assertEquals(1, counts[1]);
            assertEquals(Statement.EXECUTE_FAILED, counts[2]);
            assertEquals(1, counts[3]);
            assertEquals(3, counts[4]);
            assertEquals(Statement.EXECUTE_FAILED, counts[5]);
            assertEquals(2, counts[6]);
        } catch (Exception e) {
            fail("BatchUpdateException expected");
        }

        this.rs = this.stmt.getGeneratedKeys();
        generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(8, generatedKey);
        this.rs.close();
    }

    /**
     * Test for Statement.executeLargeUpdate(String).
     * Validate update count returned and generated keys.
     * Case: without requesting generated keys.
     */
    public void testStmtExecuteLargeUpdateNoGeneratedKeys() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        long count = this.stmt.executeLargeUpdate("INSERT INTO testExecuteLargeUpdate (n) VALUES (1), (2), (3), (4), (5)");
        assertEquals(5, count);
        assertEquals(5, this.stmt.getLargeUpdateCount());

        final Statement stmtTmp = this.stmt;
        assertThrows(SQLException.class, "Generated keys not requested. You need to specify Statement.RETURN_GENERATED_KEYS to Statement.executeUpdate\\(\\), "
                + "Statement.executeLargeUpdate\\(\\) or Connection.prepareStatement\\(\\).", new Callable<Void>() {
                    public Void call() throws Exception {
                        stmtTmp.getGeneratedKeys();
                        return null;
                    }
                });
    }

    /**
     * Test for Statement.executeLargeUpdate(String, _).
     * Validate update count returned and generated keys.
     * Case 1: explicitly requesting generated keys.
     * Case 2: requesting generated keys by defining column indexes.
     * Case 3: requesting generated keys by defining column names.
     */
    public void testStmtExecuteLargeUpdate() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        for (int tst = 1; tst <= 3; tst++) {
            this.stmt.execute("TRUNCATE TABLE testExecuteLargeUpdate");
            String tstCase = "Case " + tst;
            long count = 0;
            switch (tst) {
                case 1:
                    count = this.stmt.executeLargeUpdate("INSERT INTO testExecuteLargeUpdate (n) VALUES (1), (2), (3), (4), (5)",
                            Statement.RETURN_GENERATED_KEYS);
                    break;
                case 2:
                    count = this.stmt.executeLargeUpdate("INSERT INTO testExecuteLargeUpdate (n) VALUES (1), (2), (3), (4), (5)", new int[] { 1 });
                    break;
                case 3:
                    count = this.stmt.executeLargeUpdate("INSERT INTO testExecuteLargeUpdate (n) VALUES (1), (2), (3), (4), (5)", new String[] { "id" });
                    break;
            }
            assertEquals(tstCase, 5, count);
            assertEquals(tstCase, 5, this.stmt.getLargeUpdateCount());

            this.rs = this.stmt.getGeneratedKeys();

            ResultSetMetaData rsmd = this.rs.getMetaData();
            assertEquals(tstCase, 1, rsmd.getColumnCount());
            assertEquals(tstCase, JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
            assertEquals(tstCase, 20, rsmd.getColumnDisplaySize(1));

            long generatedKey = 0;
            while (this.rs.next()) {
                assertEquals(tstCase, ++generatedKey, this.rs.getLong(1));
            }
            assertEquals(tstCase, 5, generatedKey);
            this.rs.close();
        }
    }

    /**
     * Test for PreparedStatement.executeLargeBatch().
     * Validate update count returned and generated keys.
     */
    public void testPrepStmtExecuteLargeBatch() throws Exception {
        /*
         * Fully working batch
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.pstmt = this.conn.prepareStatement("INSERT INTO testExecuteLargeBatch (n) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
        this.pstmt.setInt(1, 1);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 2);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 3);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 4);
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (5), (6), (7)");
        this.pstmt.setInt(1, 8);
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (9), (10)");

        long[] counts = this.pstmt.executeLargeBatch();
        assertEquals(7, counts.length);
        assertEquals(1, counts[0]);
        assertEquals(1, counts[1]);
        assertEquals(1, counts[2]);
        assertEquals(1, counts[3]);
        assertEquals(3, counts[4]);
        assertEquals(1, counts[5]);
        assertEquals(2, counts[6]);

        this.rs = this.pstmt.getGeneratedKeys();

        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals(JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
        assertEquals(20, rsmd.getColumnDisplaySize(1));

        long generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(10, generatedKey);
        this.rs.close();

        /*
         * Batch with failing queries
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.pstmt = this.conn.prepareStatement("INSERT INTO testExecuteLargeBatch (n) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
        this.pstmt.setInt(1, 1);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 2);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 3);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 4);
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (5), (6), (7)");
        this.pstmt.setString(1, "eight");
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (9), (10)");

        try {
            this.pstmt.executeLargeBatch();
            fail("BatchUpdateException expected");
        } catch (BatchUpdateException e) {
            assertEquals("Incorrect integer value: 'eight' for column 'n' at row 1", e.getMessage());
            counts = e.getLargeUpdateCounts();
            assertEquals(7, counts.length);
            assertEquals(1, counts[0]);
            assertEquals(1, counts[1]);
            assertEquals(1, counts[2]);
            assertEquals(1, counts[3]);
            assertEquals(3, counts[4]);
            assertEquals(Statement.EXECUTE_FAILED, counts[5]);
            assertEquals(2, counts[6]);
        } catch (Exception e) {
            fail("BatchUpdateException expected");
        }

        this.rs = this.pstmt.getGeneratedKeys();
        generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(9, generatedKey);
        this.rs.close();
    }

    /**
     * Test for PreparedStatement.executeLargeUpdate().
     * Validate update count returned and generated keys.
     * Case: without requesting generated keys.
     */
    public void testPrepStmtExecuteLargeUpdateNoGeneratedKeys() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.pstmt = this.conn.prepareStatement("INSERT INTO testExecuteLargeUpdate (n) VALUES (?), (?), (?), (?), (?)");
        this.pstmt.setInt(1, 1);
        this.pstmt.setInt(2, 2);
        this.pstmt.setInt(3, 3);
        this.pstmt.setInt(4, 4);
        this.pstmt.setInt(5, 5);

        long count = this.pstmt.executeLargeUpdate();
        assertEquals(5, count);
        assertEquals(5, this.pstmt.getLargeUpdateCount());

        final Statement stmtTmp = this.pstmt;
        assertThrows(SQLException.class, "Generated keys not requested. You need to specify Statement.RETURN_GENERATED_KEYS to Statement.executeUpdate\\(\\), "
                + "Statement.executeLargeUpdate\\(\\) or Connection.prepareStatement\\(\\).", new Callable<Void>() {
                    public Void call() throws Exception {
                        stmtTmp.getGeneratedKeys();
                        return null;
                    }
                });
    }

    /**
     * Test for PreparedStatement.executeLargeUpdate().
     * Validate update count returned and generated keys.
     * Case: explicitly requesting generated keys.
     */
    public void testPrepStmtExecuteLargeUpdateExplicitGeneratedKeys() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.pstmt = this.conn.prepareStatement("INSERT INTO testExecuteLargeUpdate (n) VALUES (?), (?), (?), (?), (?)", Statement.RETURN_GENERATED_KEYS);
        this.pstmt.setInt(1, 1);
        this.pstmt.setInt(2, 2);
        this.pstmt.setInt(3, 3);
        this.pstmt.setInt(4, 4);
        this.pstmt.setInt(5, 5);

        long count = this.pstmt.executeLargeUpdate();
        assertEquals(5, count);
        assertEquals(5, this.pstmt.getLargeUpdateCount());

        this.rs = this.pstmt.getGeneratedKeys();

        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals(JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
        assertEquals(20, rsmd.getColumnDisplaySize(1));

        long generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(5, generatedKey);
        this.rs.close();
    }

    /**
     * Test for CallableStatement.executeLargeBatch().
     * Validate update count returned and generated keys.
     */
    public void testCallStmtExecuteLargeBatch() throws Exception {
        /*
         * Fully working batch
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");
        createProcedure("testExecuteLargeBatchProc", "(IN n INT) BEGIN INSERT INTO testExecuteLargeBatch (n) VALUES (n); END");

        CallableStatement testCstmt = this.conn.prepareCall("{CALL testExecuteLargeBatchProc(?)}");
        testCstmt.setInt(1, 1);
        testCstmt.addBatch();
        testCstmt.setInt(1, 2);
        testCstmt.addBatch();
        testCstmt.setInt(1, 3);
        testCstmt.addBatch();
        testCstmt.setInt(1, 4);
        testCstmt.addBatch();
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(5)}");
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(6)}");
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(7)}");
        testCstmt.setInt(1, 8);
        testCstmt.addBatch();
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(9)}");
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(10)}");

        long[] counts = testCstmt.executeLargeBatch();
        assertEquals(10, counts.length);
        assertEquals(1, counts[0]);
        assertEquals(1, counts[1]);
        assertEquals(1, counts[2]);
        assertEquals(1, counts[3]);
        assertEquals(1, counts[4]);
        assertEquals(1, counts[5]);
        assertEquals(1, counts[6]);
        assertEquals(1, counts[7]);
        assertEquals(1, counts[8]);
        assertEquals(1, counts[9]);

        this.rs = testCstmt.getGeneratedKeys();

        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals(JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
        assertEquals(20, rsmd.getColumnDisplaySize(1));

        // We can't check the generated keys as they are not returned correctly in this case (last_insert_id is missing from OK_PACKET when executing inserts
        // within a stored procedure - Bug#21792359).
        //        long generatedKey = 0;
        //        while (this.rs.next()) {
        //            assertEquals(++generatedKey, this.rs.getLong(1));
        //        }
        //        assertEquals(10, generatedKey);
        this.rs.close();

        testCstmt.close();

        /*
         * Batch with failing queries
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        testCstmt = this.conn.prepareCall("{call testExecuteLargeBatchProc(?)}");
        testCstmt.setInt(1, 1);
        testCstmt.addBatch();
        testCstmt.setInt(1, 2);
        testCstmt.addBatch();
        testCstmt.setInt(1, 3);
        testCstmt.addBatch();
        testCstmt.setInt(1, 4);
        testCstmt.addBatch();
        testCstmt.addBatch("{call testExecuteLargeBatchProc(5)}");
        testCstmt.addBatch("{call testExecuteLargeBatchProc('six')}");
        testCstmt.addBatch("{call testExecuteLargeBatchProc(7)}");
        testCstmt.setString(1, "eight");
        testCstmt.addBatch();
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(9)}");
        testCstmt.addBatch("{CALL testExecuteLargeBatchProc(10)}");

        try {
            testCstmt.executeLargeBatch();
            fail("BatchUpdateException expected");
        } catch (BatchUpdateException e) {
            assertEquals("Incorrect integer value: 'eight' for column 'n' at row 1", e.getMessage());
            counts = e.getLargeUpdateCounts();
            assertEquals(10, counts.length);
            assertEquals(1, counts[0]);
            assertEquals(1, counts[1]);
            assertEquals(1, counts[2]);
            assertEquals(1, counts[3]);
            assertEquals(1, counts[4]);
            assertEquals(Statement.EXECUTE_FAILED, counts[5]);
            assertEquals(1, counts[6]);
            assertEquals(Statement.EXECUTE_FAILED, counts[7]);
            assertEquals(1, counts[8]);
            assertEquals(1, counts[9]);
        } catch (Exception e) {
            fail("BatchUpdateException expected");
        }

        testCstmt.close();
    }

    /**
     * Test for CallableStatement.executeLargeUpdate().
     * Validate update count returned and generated keys.
     */
    public void testCallStmtExecuteLargeUpdate() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");
        createProcedure("testExecuteLargeUpdateProc", "(IN n1 INT, IN n2 INT, IN n3 INT, IN n4 INT, IN n5 INT) BEGIN "
                + "INSERT INTO testExecuteLargeUpdate (n) VALUES (n1), (n2), (n3), (n4), (n5); END");

        CallableStatement testCstmt = this.conn.prepareCall("{CALL testExecuteLargeUpdateProc(?, ?, ?, ?, ?)}");
        testCstmt.setInt(1, 1);
        testCstmt.setInt(2, 2);
        testCstmt.setInt(3, 3);
        testCstmt.setInt(4, 4);
        testCstmt.setInt(5, 5);

        long count = testCstmt.executeLargeUpdate();
        assertEquals(5, count);
        assertEquals(5, testCstmt.getLargeUpdateCount());

        this.rs = testCstmt.getGeneratedKeys();

        // Although not requested, CallableStatements makes gerenated keys always available.
        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals(JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
        assertEquals(20, rsmd.getColumnDisplaySize(1));

        // We can't check the generated keys as they are not returned correctly in this case (last_insert_id is missing from OK_PACKET when executing inserts
        // within a stored procedure - Bug#21792359).
        //        long generatedKey = 0;
        //        while (this.rs.next()) {
        //            assertEquals(++generatedKey, this.rs.getLong(1));
        //        }
        //        assertEquals(5, generatedKey);
        this.rs.close();
    }

    /**
     * Test for (Server)PreparedStatement.executeLargeBatch().
     * Validate update count returned and generated keys.
     */
    public void testServerPrepStmtExecuteLargeBatch() throws Exception {
        /*
         * Fully working batch
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        Connection testConn = getConnectionWithProps("useServerPrepStmts=true");

        this.pstmt = testConn.prepareStatement("INSERT INTO testExecuteLargeBatch (n) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
        this.pstmt.setInt(1, 1);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 2);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 3);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 4);
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (5), (6), (7)");
        this.pstmt.setInt(1, 8);
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (9), (10)");

        long[] counts = this.pstmt.executeLargeBatch();
        assertEquals(7, counts.length);
        assertEquals(1, counts[0]);
        assertEquals(1, counts[1]);
        assertEquals(1, counts[2]);
        assertEquals(1, counts[3]);
        assertEquals(3, counts[4]);
        assertEquals(1, counts[5]);
        assertEquals(2, counts[6]);

        this.rs = this.pstmt.getGeneratedKeys();

        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertEquals(1, rsmd.getColumnCount());
        assertEquals(JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1));
        assertEquals(20, rsmd.getColumnDisplaySize(1));

        long generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(10, generatedKey);
        this.rs.close();

        /*
         * Batch with failing queries
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        this.pstmt = testConn.prepareStatement("INSERT INTO testExecuteLargeBatch (n) VALUES (?)", Statement.RETURN_GENERATED_KEYS);
        this.pstmt.setInt(1, 1);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 2);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 3);
        this.pstmt.addBatch();
        this.pstmt.setInt(1, 4);
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (5), (6), (7)");
        this.pstmt.setString(1, "eight");
        this.pstmt.addBatch();
        this.pstmt.addBatch("INSERT INTO testExecuteLargeBatch (n) VALUES (9), (10)");

        try {
            this.pstmt.executeLargeBatch();
            fail("BatchUpdateException expected");
        } catch (BatchUpdateException e) {
            assertEquals("Incorrect integer value: 'eight' for column 'n' at row 1", e.getMessage());
            counts = e.getLargeUpdateCounts();
            assertEquals(7, counts.length);
            assertEquals(1, counts[0]);
            assertEquals(1, counts[1]);
            assertEquals(1, counts[2]);
            assertEquals(1, counts[3]);
            assertEquals(3, counts[4]);
            assertEquals(Statement.EXECUTE_FAILED, counts[5]);
            assertEquals(2, counts[6]);
        }

        this.rs = this.pstmt.getGeneratedKeys();
        generatedKey = 0;
        while (this.rs.next()) {
            assertEquals(++generatedKey, this.rs.getLong(1));
        }
        assertEquals(9, generatedKey);
        this.rs.close();

        testConn.close();
    }

    /**
     * Test for Statement.[get/set]LargeMaxRows().
     */
    public void testStmtGetSetLargeMaxRows() throws Exception {
        assertEquals(0, this.stmt.getMaxRows());
        assertEquals(0, this.stmt.getLargeMaxRows());

        this.stmt.setMaxRows(50000000);

        assertEquals(50000000, this.stmt.getMaxRows());
        assertEquals(50000000, this.stmt.getLargeMaxRows());

        final Statement stmtTmp = this.stmt;
        assertThrows(SQLException.class, "setMaxRows\\(\\) out of range. 50000001 > 50000000.", new Callable<Void>() {
            public Void call() throws Exception {
                stmtTmp.setMaxRows(50000001);
                return null;
            }
        });

        this.stmt.setLargeMaxRows(0);

        assertEquals(0, this.stmt.getMaxRows());
        assertEquals(0, this.stmt.getLargeMaxRows());

        this.stmt.setLargeMaxRows(50000000);

        assertEquals(50000000, this.stmt.getMaxRows());
        assertEquals(50000000, this.stmt.getLargeMaxRows());

        assertThrows(SQLException.class, "setMaxRows\\(\\) out of range. 50000001 > 50000000.", new Callable<Void>() {
            public Void call() throws Exception {
                stmtTmp.setLargeMaxRows(50000001L);
                return null;
            }
        });
    }

    /**
     * Test for PreparedStatement.setObject().
     * Validate new methods as well as support for the types java.time.Local[Date][Time] and java.time.Offset[Date]Time.
     */
    public void testPrepStmtSetObjectAndNewSupportedTypes() throws Exception {
        /*
         * Objects java.time.Local[Date][Time] are supported via conversion to/from java.sql.[Date|Time|Timestamp].
         */
        createTable("testSetObjectPS1", "(id INT, d DATE, t TIME, dt DATETIME, ts TIMESTAMP)");

        this.pstmt = this.conn.prepareStatement("INSERT INTO testSetObjectPS1 VALUES (?, ?, ?, ?, ?)");
        validateTestDataLocalDTTypes("testSetObjectPS1", insertTestDataLocalDTTypes(this.pstmt));

        /*
         * Objects java.time.Offset[Date]Time are supported via conversion to *CHAR or serialization.
         */
        createTable("testSetObjectPS2", "(id INT, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB)");

        this.pstmt = this.conn.prepareStatement("INSERT INTO testSetObjectPS2 VALUES (?, ?, ?, ?, ?)");
        validateTestDataOffsetDTTypes("testSetObjectPS2", insertTestDataOffsetDTTypes(this.pstmt));
    }

    /**
     * Test for PreparedStatement.setObject(), unsupported SQL types TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE and REF_CURSOR.
     */
    public void testPrepStmtSetObjectAndNewUnsupportedTypes() throws Exception {
        checkUnsupportedTypesBehavior(this.conn.prepareStatement("SELECT ?"));
    }

    /**
     * Test for CallableStatement.setObject().
     * Validate new methods as well as support for the types java.time.Local[Date][Time] and java.time.Offset[Date]Time.
     */
    public void testCallStmtSetObjectAndNewSupportedTypes() throws Exception {
        /*
         * Objects java.time.Local[Date][Time] are supported via conversion to/from java.sql.[Date|Time|Timestamp].
         */
        createTable("testSetObjectCS1", "(id INT, d DATE, t TIME, dt DATETIME, ts TIMESTAMP)");
        createProcedure("testSetObjectCS1Proc",
                "(IN id INT, IN d DATE, IN t TIME, IN dt DATETIME, IN ts TIMESTAMP) BEGIN " + "INSERT INTO testSetObjectCS1 VALUES (id, d, t, dt, ts); END");

        CallableStatement testCstmt = this.conn.prepareCall("{CALL testSetObjectCS1Proc(?, ?, ?, ?, ?)}");
        validateTestDataLocalDTTypes("testSetObjectCS1", insertTestDataLocalDTTypes(testCstmt));

        /*
         * Objects java.time.Offset[Date]Time are supported via conversion to *CHAR or serialization.
         */
        createTable("testSetObjectCS2", "(id INT, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB)");
        createProcedure("testSetObjectCS2Proc",
                "(id INT, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB) BEGIN INSERT INTO testSetObjectCS2 VALUES (id, ot1, ot2, odt1, odt2); END");

        testCstmt = this.conn.prepareCall("{CALL testSetObjectCS2Proc(?, ?, ?, ?, ?)}");
        validateTestDataOffsetDTTypes("testSetObjectCS2", insertTestDataOffsetDTTypes(testCstmt));
    }

    /**
     * Test for CallableStatement.setObject(), unsupported SQL types TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE and REF_CURSOR.
     */
    public void testCallStmtSetObjectAndNewUnsupportedTypes() throws Exception {
        createProcedure("testUnsupportedTypesProc", "(OUT param VARCHAR(20)) BEGIN SELECT 1; END");
        checkUnsupportedTypesBehavior(this.conn.prepareCall("{CALL testUnsupportedTypesProc(?)}"));
    }

    /**
     * Test for (Server)PreparedStatement.setObject().
     * Validate new methods as well as support for the types java.time.Local[Date][Time] and java.time.Offset[Date]Time.
     */
    public void testServPrepStmtSetObjectAndNewSupportedTypes() throws Exception {
        /*
         * Objects java.time.Local[Date][Time] are supported via conversion to/from java.sql.[Date|Time|Timestamp].
         */
        createTable("testSetObjectSPS1", "(id INT, d DATE, t TIME, dt DATETIME, ts TIMESTAMP)");

        Connection testConn = getConnectionWithProps("useServerPrepStmts=true");

        this.pstmt = testConn.prepareStatement("INSERT INTO testSetObjectSPS1 VALUES (?, ?, ?, ?, ?)");
        validateTestDataLocalDTTypes("testSetObjectSPS1", insertTestDataLocalDTTypes(this.pstmt));

        /*
         * Objects java.time.Offset[Date]Time are supported via conversion to *CHAR or serialization.
         */
        createTable("testSetObjectSPS2", "(id INT, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB)");

        this.pstmt = testConn.prepareStatement("INSERT INTO testSetObjectSPS2 VALUES (?, ?, ?, ?, ?)");
        validateTestDataOffsetDTTypes("testSetObjectSPS2", insertTestDataOffsetDTTypes(this.pstmt));
    }

    /**
     * Test for (Server)PreparedStatement.setObject(), unsupported SQL types TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE and REF_CURSOR.
     */
    public void testServPrepStmtSetObjectAndNewUnsupportedTypes() throws Exception {
        Connection testConn = getConnectionWithProps("useServerPrepStmts=true");
        checkUnsupportedTypesBehavior(testConn.prepareStatement("SELECT ?"));
        testConn.close();
    }

    /**
     * Helper method for *SetObject* tests.
     * Insert data into the given PreparedStatement, or any of its subclasses, with the following structure:
     * 1 - `id` INT
     * 2 - `d` DATE (or any kind of *CHAR)
     * 3 - `t` TIME (or any kind of *CHAR)
     * 4 - `dt` DATETIME (or any kind of *CHAR)
     * 5 - `ts` TIMESTAMP (or any kind of *CHAR)
     * 
     * @param prepStmt
     * @return the row count of inserted records.
     * @throws Exception
     */
    private int insertTestDataLocalDTTypes(PreparedStatement prepStmt) throws Exception {
        prepStmt.setInt(1, 1);
        prepStmt.setDate(2, this.testSqlDate);
        prepStmt.setTime(3, this.testSqlTime);
        prepStmt.setTimestamp(4, this.testSqlTimeStamp);
        prepStmt.setTimestamp(5, this.testSqlTimeStamp);
        assertEquals(1, prepStmt.executeUpdate());

        prepStmt.setInt(1, 2);
        prepStmt.setObject(2, this.testLocalDate);
        prepStmt.setObject(3, this.testLocalTime);
        prepStmt.setObject(4, this.testLocalDateTime);
        prepStmt.setObject(5, this.testLocalDateTime);
        assertEquals(1, prepStmt.executeUpdate());

        prepStmt.setInt(1, 3);
        prepStmt.setObject(2, this.testLocalDate, JDBCType.DATE);
        prepStmt.setObject(3, this.testLocalTime, JDBCType.TIME);
        prepStmt.setObject(4, this.testLocalDateTime, JDBCType.TIMESTAMP);
        prepStmt.setObject(5, this.testLocalDateTime, JDBCType.TIMESTAMP);
        assertEquals(1, prepStmt.executeUpdate());

        prepStmt.setInt(1, 4);
        prepStmt.setObject(2, this.testLocalDate, JDBCType.DATE, 10);
        prepStmt.setObject(3, this.testLocalTime, JDBCType.TIME, 8);
        prepStmt.setObject(4, this.testLocalDateTime, JDBCType.TIMESTAMP, 20);
        prepStmt.setObject(5, this.testLocalDateTime, JDBCType.TIMESTAMP, 20);
        assertEquals(1, prepStmt.executeUpdate());

        prepStmt.setInt(1, 5);
        prepStmt.setObject(2, this.testLocalDate, JDBCType.VARCHAR);
        prepStmt.setObject(3, this.testLocalTime, JDBCType.VARCHAR);
        prepStmt.setObject(4, this.testLocalDateTime, JDBCType.VARCHAR);
        prepStmt.setObject(5, this.testLocalDateTime, JDBCType.VARCHAR);
        assertEquals(1, prepStmt.executeUpdate());

        prepStmt.setInt(1, 6);
        prepStmt.setObject(2, this.testLocalDate, JDBCType.VARCHAR, 10);
        prepStmt.setObject(3, this.testLocalTime, JDBCType.VARCHAR, 8);
        prepStmt.setObject(4, this.testLocalDateTime, JDBCType.VARCHAR, 20);
        prepStmt.setObject(5, this.testLocalDateTime, JDBCType.VARCHAR, 20);
        assertEquals(1, prepStmt.executeUpdate());

        if (prepStmt instanceof CallableStatement) {
            CallableStatement cstmt = (CallableStatement) prepStmt;

            cstmt.setInt("id", 7);
            cstmt.setDate("d", this.testSqlDate);
            cstmt.setTime("t", this.testSqlTime);
            cstmt.setTimestamp("dt", this.testSqlTimeStamp);
            cstmt.setTimestamp("ts", this.testSqlTimeStamp);
            assertEquals(1, cstmt.executeUpdate());

            cstmt.setInt("id", 8);
            cstmt.setObject("d", this.testLocalDate);
            cstmt.setObject("t", this.testLocalTime);
            cstmt.setObject("dt", this.testLocalDateTime);
            cstmt.setObject("ts", this.testLocalDateTime);
            assertEquals(1, cstmt.executeUpdate());

            cstmt.setInt("id", 9);
            cstmt.setObject("d", this.testLocalDate, JDBCType.DATE);
            cstmt.setObject("t", this.testLocalTime, JDBCType.TIME);
            cstmt.setObject("dt", this.testLocalDateTime, JDBCType.TIMESTAMP);
            cstmt.setObject("ts", this.testLocalDateTime, JDBCType.TIMESTAMP);
            assertEquals(1, cstmt.executeUpdate());

            cstmt.setInt("id", 10);
            cstmt.setObject("d", this.testLocalDate, JDBCType.DATE, 10);
            cstmt.setObject("t", this.testLocalTime, JDBCType.TIME, 8);
            cstmt.setObject("dt", this.testLocalDateTime, JDBCType.TIMESTAMP, 20);
            cstmt.setObject("ts", this.testLocalDateTime, JDBCType.TIMESTAMP, 20);
            assertEquals(1, cstmt.executeUpdate());

            cstmt.setInt("id", 11);
            cstmt.setObject("d", this.testLocalDate, JDBCType.VARCHAR);
            cstmt.setObject("t", this.testLocalTime, JDBCType.VARCHAR);
            cstmt.setObject("dt", this.testLocalDateTime, JDBCType.VARCHAR);
            cstmt.setObject("ts", this.testLocalDateTime, JDBCType.VARCHAR);
            assertEquals(1, cstmt.executeUpdate());

            cstmt.setInt("id", 12);
            cstmt.setObject("d", this.testLocalDate, JDBCType.VARCHAR, 10);
            cstmt.setObject("t", this.testLocalTime, JDBCType.VARCHAR, 8);
            cstmt.setObject("dt", this.testLocalDateTime, JDBCType.VARCHAR, 20);
            cstmt.setObject("ts", this.testLocalDateTime, JDBCType.VARCHAR, 20);
            assertEquals(1, cstmt.executeUpdate());

            return 12;
        }

        return 6;
    }

    /**
     * Helper method for *SetObject* tests.
     * Validate the test data contained in the given ResultSet with following structure:
     * 1 - `id` INT
     * 2 - `d` DATE (or any kind of *CHAR)
     * 3 - `t` TIME (or any kind of *CHAR)
     * 4 - `dt` DATETIME (or any kind of *CHAR)
     * 5 - `ts` TIMESTAMP (or any kind of *CHAR)
     * 
     * Additionally validate support for the types java.time.Local[Date][Time] in ResultSet.getObject().
     * 
     * @param tableName
     * @param expectedRowCount
     * @throws Exception
     */
    private void validateTestDataLocalDTTypes(String tableName, int expectedRowCount) throws Exception {
        this.rs = this.stmt.executeQuery("SELECT * FROM " + tableName);

        int rowCount = 0;
        while (this.rs.next()) {
            String row = "Row " + this.rs.getInt(1);
            assertEquals(row, ++rowCount, this.rs.getInt(1));

            assertEquals(row, this.testDateString, this.rs.getString(2));
            assertEquals(row, this.testTimeString, this.rs.getString(3));
            assertEquals(row, this.testDateTimeString, this.rs.getString(4));
            assertEquals(row, this.testDateTimeString, this.rs.getString(5));

            assertEquals(row, this.testSqlDate, this.rs.getDate(2));
            assertEquals(row, this.testSqlTime, this.rs.getTime(3));
            assertEquals(row, this.testSqlTimeStamp, this.rs.getTimestamp(4));
            assertEquals(row, this.testSqlTimeStamp, this.rs.getTimestamp(5));

            assertEquals(row, this.testLocalDate, this.rs.getObject(2, LocalDate.class));
            assertEquals(row, this.testLocalTime, this.rs.getObject(3, LocalTime.class));
            assertEquals(row, this.testLocalDateTime, this.rs.getObject(4, LocalDateTime.class));
            assertEquals(row, this.testLocalDateTime, this.rs.getObject(5, LocalDateTime.class));

            assertEquals(row, rowCount, this.rs.getInt("id"));

            assertEquals(row, this.testDateString, this.rs.getString("d"));
            assertEquals(row, this.testTimeString, this.rs.getString("t"));
            assertEquals(row, this.testDateTimeString, this.rs.getString("dt"));
            assertEquals(row, this.testDateTimeString, this.rs.getString("ts"));

            assertEquals(row, this.testSqlDate, this.rs.getDate("d"));
            assertEquals(row, this.testSqlTime, this.rs.getTime("t"));
            assertEquals(row, this.testSqlTimeStamp, this.rs.getTimestamp("dt"));
            assertEquals(row, this.testSqlTimeStamp, this.rs.getTimestamp("ts"));

            assertEquals(row, this.testLocalDate, this.rs.getObject("d", LocalDate.class));
            assertEquals(row, this.testLocalTime, this.rs.getObject("t", LocalTime.class));
            assertEquals(row, this.testLocalDateTime, this.rs.getObject("dt", LocalDateTime.class));
            assertEquals(row, this.testLocalDateTime, this.rs.getObject("ts", LocalDateTime.class));
        }
        assertEquals(expectedRowCount, rowCount);
    }

    /**
     * Helper method for *SetObject* tests.
     * Insert data into the given PreparedStatement, or any of its subclasses, with the following structure:
     * 1 - `id` INT
     * 2 - `ot1` VARCHAR
     * 3 - `ot2` BLOB
     * 4 - `odt1` VARCHAR
     * 5 - `odt2` BLOB
     * 
     * @param prepStmt
     * @return the row count of inserted records.
     * @throws Exception
     */
    private int insertTestDataOffsetDTTypes(PreparedStatement prepStmt) throws Exception {
        prepStmt.setInt(1, 1);
        prepStmt.setObject(2, this.testOffsetTime, JDBCType.VARCHAR);
        prepStmt.setObject(3, this.testOffsetTime);
        prepStmt.setObject(4, this.testOffsetDateTime, JDBCType.VARCHAR);
        prepStmt.setObject(5, this.testOffsetDateTime);
        assertEquals(1, prepStmt.executeUpdate());

        if (prepStmt instanceof CallableStatement) {
            CallableStatement cstmt = (CallableStatement) prepStmt;

            cstmt.setInt("id", 2);
            cstmt.setObject("ot1", this.testOffsetTime, JDBCType.VARCHAR);
            cstmt.setObject("ot2", this.testOffsetTime);
            cstmt.setObject("odt1", this.testOffsetDateTime, JDBCType.VARCHAR);
            cstmt.setObject("odt2", this.testOffsetDateTime);
            assertEquals(1, cstmt.executeUpdate());

            return 2;
        }

        return 1;
    }

    /**
     * Helper method for *SetObject* tests.
     * Validate the test data contained in the given ResultSet with following structure:
     * 1 - `id` INT
     * 2 - `ot1` VARCHAR
     * 3 - `ot2` BLOB
     * 4 - `odt1` VARCHAR
     * 5 - `odt2` BLOB
     * 
     * Additionally validate support for the types java.time.Offset[Date]Time in ResultSet.getObject().
     * 
     * @param tableName
     * @param expectedRowCount
     * @throws Exception
     */
    private void validateTestDataOffsetDTTypes(String tableName, int expectedRowCount) throws Exception {
        Connection testConn = getConnectionWithProps("autoDeserialize=true"); // Offset[Date]Time are supported via object serialization too.
        Statement testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT * FROM " + tableName);

        int rowCount = 0;
        while (this.rs.next()) {
            String row = "Row " + this.rs.getInt(1);
            assertEquals(++rowCount, this.rs.getInt(1));

            assertEquals(row, this.testOffsetTime, this.rs.getObject(2, OffsetTime.class));
            assertEquals(row, this.testOffsetTime, this.rs.getObject(3, OffsetTime.class));
            assertEquals(row, this.testOffsetDateTime, this.rs.getObject(4, OffsetDateTime.class));
            assertEquals(row, this.testOffsetDateTime, this.rs.getObject(5, OffsetDateTime.class));

            assertEquals(row, rowCount, this.rs.getInt("id"));

            assertEquals(row, this.testOffsetTime, this.rs.getObject("ot1", OffsetTime.class));
            assertEquals(row, this.testOffsetTime, this.rs.getObject("ot2", OffsetTime.class));
            assertEquals(row, this.testOffsetDateTime, this.rs.getObject("odt1", OffsetDateTime.class));
            assertEquals(row, this.testOffsetDateTime, this.rs.getObject("odt2", OffsetDateTime.class));
        }
        assertEquals(expectedRowCount, rowCount);
        testConn.close();
    }

    /**
     * Helper method for *SetObject* tests.
     * Check unsupported types behavior for the given PreparedStatement with a single placeholder. If this is a CallableStatement then the placeholder must
     * coincide with a parameter named `param`.
     * 
     * @param prepStmt
     */
    private void checkUnsupportedTypesBehavior(final PreparedStatement prepStmt) {
        final CallableStatement cstmt = prepStmt instanceof CallableStatement ? (CallableStatement) prepStmt : null;

        /*
         * Unsupported SQL types TIME_WITH_TIMEZONE and TIMESTAMP_WITH_TIMEZONE.
         */
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                prepStmt.setObject(1, OffsetTime.now(), JDBCType.TIME_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                prepStmt.setObject(1, OffsetDateTime.now(), JDBCType.TIMESTAMP_WITH_TIMEZONE);
                return null;
            }
        });
        if (cstmt != null) {
            assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    cstmt.setObject("param", OffsetTime.now(), JDBCType.TIME_WITH_TIMEZONE);
                    return null;
                }
            });
            assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    cstmt.setObject("param", OffsetDateTime.now(), JDBCType.TIMESTAMP_WITH_TIMEZONE);
                    return null;
                }
            });
        }
        /*
         * Unsupported SQL type REF_CURSOR.
         */
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                prepStmt.setObject(1, new Object(), JDBCType.REF_CURSOR);
                return null;
            }
        });
        if (cstmt != null) {
            assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    cstmt.setObject("param", new Object(), JDBCType.REF_CURSOR);
                    return null;
                }
            });
        }
    }

    /**
     * Test for CallableStatement.registerOutParameter().
     */
    public void testCallStmtRegisterOutParameter() throws Exception {
        createProcedure("testRegisterOutParameterProc", "(OUT b BIT, OUT i INT, OUT c CHAR(10)) BEGIN SELECT 1, 1234, 'MySQL' INTO b, i, c; END");
        final CallableStatement testCstmt = this.conn.prepareCall("{CALL testRegisterOutParameterProc(?, ?, ?)}");

        // registerOutParameter by parameter index
        testCstmt.registerOutParameter(1, JDBCType.BOOLEAN);
        testCstmt.registerOutParameter(2, JDBCType.INTEGER);
        testCstmt.registerOutParameter(3, JDBCType.CHAR);
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        testCstmt.registerOutParameter(1, JDBCType.BOOLEAN, 1);
        testCstmt.registerOutParameter(2, JDBCType.INTEGER, 1);
        testCstmt.registerOutParameter(3, JDBCType.CHAR, 1);
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        testCstmt.registerOutParameter(1, JDBCType.BOOLEAN, "dummy");
        testCstmt.registerOutParameter(2, JDBCType.INTEGER, "dummy");
        testCstmt.registerOutParameter(3, JDBCType.CHAR, "dummy");
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        // registerOutParameter by parameter name
        testCstmt.registerOutParameter("b", JDBCType.BOOLEAN);
        testCstmt.registerOutParameter("i", JDBCType.INTEGER);
        testCstmt.registerOutParameter("c", JDBCType.CHAR);
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        testCstmt.registerOutParameter("b", JDBCType.BOOLEAN, 1);
        testCstmt.registerOutParameter("i", JDBCType.INTEGER, 1);
        testCstmt.registerOutParameter("c", JDBCType.CHAR, 1);
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        testCstmt.registerOutParameter("b", JDBCType.BOOLEAN, "dummy");
        testCstmt.registerOutParameter("i", JDBCType.INTEGER, "dummy");
        testCstmt.registerOutParameter("c", JDBCType.CHAR, "dummy");
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));
    }

    /**
     * Test for CallableStatement.registerOutParameter(...MysqlType...).
     */
    public void testCallStmtRegisterOutParameterWithMysqlType() throws Exception {
        createProcedure("testRegisterOutParameterProc", "(OUT b BIT, OUT i INT, OUT c CHAR(10)) BEGIN SELECT 1, 1234, 'MySQL' INTO b, i, c; END");
        final CallableStatement testCstmt = this.conn.prepareCall("{CALL testRegisterOutParameterProc(?, ?, ?)}");

        // registerOutParameter by parameter index
        testCstmt.registerOutParameter(1, MysqlType.BOOLEAN);
        testCstmt.registerOutParameter(2, MysqlType.INT);
        testCstmt.registerOutParameter(3, MysqlType.CHAR);
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        testCstmt.registerOutParameter(1, MysqlType.BOOLEAN, 1);
        testCstmt.registerOutParameter(2, MysqlType.INT, 1);
        testCstmt.registerOutParameter(3, MysqlType.CHAR, 1);
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        testCstmt.registerOutParameter(1, MysqlType.BOOLEAN, "dummy");
        testCstmt.registerOutParameter(2, MysqlType.INT, "dummy");
        testCstmt.registerOutParameter(3, MysqlType.CHAR, "dummy");
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        // registerOutParameter by parameter name
        testCstmt.registerOutParameter("b", MysqlType.BOOLEAN);
        testCstmt.registerOutParameter("i", MysqlType.INT);
        testCstmt.registerOutParameter("c", MysqlType.CHAR);
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        testCstmt.registerOutParameter("b", MysqlType.BOOLEAN, 1);
        testCstmt.registerOutParameter("i", MysqlType.INT, 1);
        testCstmt.registerOutParameter("c", MysqlType.CHAR, 1);
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));

        testCstmt.registerOutParameter("b", MysqlType.BOOLEAN, "dummy");
        testCstmt.registerOutParameter("i", MysqlType.INT, "dummy");
        testCstmt.registerOutParameter("c", MysqlType.CHAR, "dummy");
        testCstmt.execute();

        assertEquals(Boolean.TRUE, testCstmt.getObject(1));
        assertEquals(Integer.valueOf(1234), testCstmt.getObject(2));
        assertEquals("MySQL", testCstmt.getObject(3));
    }

    /**
     * Test for CallableStatement.registerOutParameter(), unsupported SQL types TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE and REF_CURSOR.
     */
    public void testCallStmtRegisterOutParameterNewUnsupportedTypes() throws Exception {
        createProcedure("testUnsupportedTypesProc", "(OUT param VARCHAR(20)) BEGIN SELECT 1; END");
        final CallableStatement testCstmt = this.conn.prepareCall("{CALL testUnsupportedTypesProc(?)}");

        /*
         * Unsupported SQL types TIME_WITH_TIMEZONE and TIMESTAMP_WITH_TIMEZONE.
         */
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.TIME_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.TIME_WITH_TIMEZONE, 1);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.TIME_WITH_TIMEZONE, "dummy");
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.TIME_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.TIME_WITH_TIMEZONE, 1);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.TIME_WITH_TIMEZONE, "dummy");
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.TIMESTAMP_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.TIMESTAMP_WITH_TIMEZONE, 1);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.TIMESTAMP_WITH_TIMEZONE, "dummy");
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.TIMESTAMP_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.TIMESTAMP_WITH_TIMEZONE, 1);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.TIMESTAMP_WITH_TIMEZONE, "dummy");
                return null;
            }
        });

        /*
         * Unsupported SQL type REF_CURSOR.
         */
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.REF_CURSOR);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.REF_CURSOR, 1);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter(1, JDBCType.REF_CURSOR, "dummy");
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.REF_CURSOR);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.REF_CURSOR, 1);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                testCstmt.registerOutParameter("param", JDBCType.REF_CURSOR, "dummy");
                return null;
            }
        });
    }

    /**
     * WL#11101 - Remove de-cache and close of SSPSs on double call to close()
     */
    public void testServerPreparedStatementsCaching() throws Exception {
        // Non prepared statements must be non-poolable by default.
        assertFalse(this.stmt.isPoolable());

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
        Function<Connection, ServerPreparedStatement> getStmtsCacheSingleElem = (c) -> {
            try {
                @SuppressWarnings("unchecked")
                LRUCache<?, ServerPreparedStatement> stmtsCacheObj = (LRUCache<?, ServerPreparedStatement>) stmtsCacheField.get(c);
                return stmtsCacheObj.get(stmtsCacheObj.keySet().iterator().next());
            } catch (IllegalArgumentException | IllegalAccessException e) {
                fail("Fail getting the statemets cache element.");
                return null;
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

            /*
             * Cache the prepared statement and de-cache it later.
             * (*) if server prepared statement and caching is enabled.
             */
            {
                JdbcConnection testConn = (JdbcConnection) getConnectionWithProps(props);
                PreparedStatement testPstmt = testConn.prepareStatement(sql1);
                assertEquals(1, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                assertTrue(testPstmt.isPoolable());

                testPstmt.close(); // Caches this PS (*).
                assertEquals(cachedSPS ? 1 : 0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 1 : -1, getStmtsCacheSize.applyAsInt(testConn));

                testPstmt.close(); // No-op.
                assertEquals(cachedSPS ? 1 : 0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 1 : -1, getStmtsCacheSize.applyAsInt(testConn));

                if (cachedSPS) {
                    assertTrue(testPstmt.isPoolable());

                    testPstmt.setPoolable(false); // De-caches this PS; it gets automatically closed (*).
                    assertEquals(0, testConn.getActiveStatementCount());
                    assertEquals(0, getStmtsCacheSize.applyAsInt(testConn));
                }

                testPstmt.close(); // No-op.
                assertEquals(0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                assertThrows(SQLException.class, "No operations allowed after statement closed\\.", () -> {
                    testPstmt.setPoolable(false);
                    return null;
                });
                assertThrows(SQLException.class, "No operations allowed after statement closed\\.", () -> {
                    testPstmt.isPoolable();
                    return null;
                });

                testConn.close();
                assertEquals(0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));
            }

            /*
             * Set not to cache the prepared statement.
             * (*) if server prepared statement and caching is enabled.
             */
            {
                JdbcConnection testConn = (JdbcConnection) getConnectionWithProps(props);
                PreparedStatement testPstmt = testConn.prepareStatement(sql1);
                assertEquals(1, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                assertTrue(testPstmt.isPoolable());
                testPstmt.setPoolable(false); // Don't cache this PS (*).
                assertFalse(testPstmt.isPoolable());

                testPstmt.close(); // Doesn't cache this PS (*).
                assertEquals(0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                testPstmt.close(); // No-op.
                assertEquals(0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                assertThrows(SQLException.class, "No operations allowed after statement closed\\.", () -> {
                    testPstmt.setPoolable(true);
                    return null;
                });
                assertThrows(SQLException.class, "No operations allowed after statement closed\\.", () -> {
                    testPstmt.isPoolable();
                    return null;
                });

                testConn.close();
                assertEquals(0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));
            }

            /*
             * Set not to cache the prepared statement but change mind before closing it.
             * Reuse the cached prepared statement and don't re-cache it.
             * (*) if server prepared statement and caching is enabled.
             */
            {
                JdbcConnection testConn = (JdbcConnection) getConnectionWithProps(props);
                PreparedStatement testPstmt = testConn.prepareStatement(sql1);
                assertEquals(1, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                testPstmt.setPoolable(false); // Don't cache this PS (*).
                assertFalse(testPstmt.isPoolable());
                testPstmt.setPoolable(true);
                assertTrue(testPstmt.isPoolable()); // Changed my mind, let it be cached (*).

                testPstmt.close(); // Caches this PS (*).
                assertEquals(cachedSPS ? 1 : 0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 1 : -1, getStmtsCacheSize.applyAsInt(testConn));

                testPstmt.close(); // No-op.
                assertEquals(cachedSPS ? 1 : 0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 1 : -1, getStmtsCacheSize.applyAsInt(testConn));

                PreparedStatement testPstmtOld = testPstmt;
                testPstmt = testConn.prepareStatement(sql1); // Takes the cached statement (*), or creates a fresh one.
                if (cachedSPS) {
                    assertSame(testPstmtOld, testPstmt);
                } else {
                    assertNotSame(testPstmtOld, testPstmt);
                }
                assertEquals(1, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                assertTrue(testPstmt.isPoolable());
                testPstmt.setPoolable(false); // Don't cache this PS (*).
                assertFalse(testPstmt.isPoolable());

                testPstmt.close(); // Doesn't cache this PS (*).
                assertEquals(0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                testPstmtOld = testPstmt;
                testPstmt = testConn.prepareStatement(sql1); // Creates a fresh prepared statement.
                assertNotSame(testPstmtOld, testPstmt);
                assertEquals(1, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                assertTrue(testPstmt.isPoolable());

                testConn.close();
                assertEquals(0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));
            }

            /*
             * Caching of multiple copies of same prepared statement.
             * (*) if server prepared statement and caching is enabled.
             */
            {
                int psCount = 5;
                JdbcConnection testConn = (JdbcConnection) getConnectionWithProps(props);
                PreparedStatement[] testPstmts = new PreparedStatement[psCount];
                for (int i = 0; i < psCount; i++) {
                    testPstmts[i] = testConn.prepareStatement(sql1);
                }
                assertEquals(5, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                for (int i = 0; i < psCount; i++) {
                    assertTrue(testPstmts[i].isPoolable());
                    testPstmts[i].close(); // Caches this PS and replaces existing if same (*).
                    assertEquals(cachedSPS ? psCount - i : psCount - i - 1, testConn.getActiveStatementCount());
                    assertEquals(cachedSPS ? 1 : -1, getStmtsCacheSize.applyAsInt(testConn));
                    if (cachedSPS) {
                        assertSame(testPstmts[i], getStmtsCacheSingleElem.apply(testConn));
                    }
                }

                PreparedStatement testPstmt = testConn.prepareStatement(sql1);
                assertEquals(1, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));
                for (int i = 0; i < psCount; i++) {
                    if (cachedSPS && i == psCount - 1) {
                        assertSame(testPstmts[i], testPstmt);
                    } else {
                        assertNotSame(testPstmts[i], testPstmt);
                    }
                }

                testPstmt.setPoolable(false); // Don't cache this PS (*).
                testPstmt.close(); // Doesn't cache this PS (*).
                assertEquals(0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                testConn.close();
                assertEquals(0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));
            }

            /*
             * Combine caching different prepared statements.
             * (*) if server prepared statement and caching is enabled.
             */
            {
                int psCount = 5;
                JdbcConnection testConn = (JdbcConnection) getConnectionWithProps(props);
                PreparedStatement[] testPstmts1 = new PreparedStatement[psCount];
                for (int i = 0; i < psCount; i++) {
                    testPstmts1[i] = testConn.prepareStatement(sql1);
                }
                PreparedStatement testPstmt = testConn.prepareStatement(sql2);
                assertEquals(6, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                assertTrue(testPstmt.isPoolable());
                testPstmt.close(); // Caches this PS (*).
                assertEquals(cachedSPS ? 1 : -1, getStmtsCacheSize.applyAsInt(testConn));
                for (int i = 0; i < psCount; i++) {
                    assertTrue(testPstmts1[i].isPoolable());
                    testPstmts1[i].close(); // Caches this PS and replaces existing if same (*).
                    assertEquals(cachedSPS ? psCount - i + 1 : psCount - i - 1, testConn.getActiveStatementCount());
                    assertEquals(cachedSPS ? 2 : -1, getStmtsCacheSize.applyAsInt(testConn));
                }

                PreparedStatement testPstmt1 = testConn.prepareStatement(sql1);
                assertEquals(cachedSPS ? 2 : 1, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 1 : -1, getStmtsCacheSize.applyAsInt(testConn));
                for (int i = 0; i < psCount; i++) {
                    if (cachedSPS && i == psCount - 1) {
                        assertSame(testPstmts1[i], testPstmt1);
                    } else {
                        assertNotSame(testPstmts1[i], testPstmt1);
                    }
                }

                PreparedStatement testPstmt2 = testConn.prepareStatement(sql2);
                assertEquals(2, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));
                if (cachedSPS) {
                    assertSame(testPstmt, testPstmt2);
                } else {
                    assertNotSame(testPstmt, testPstmt2);
                }

                testPstmt1.setPoolable(false); // Don't cache this PS (*).
                testPstmt1.close(); // Doesn't cache this PS (*).
                assertEquals(1, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                testPstmt2.setPoolable(false); // Don't cache this PS (*).
                testPstmt2.close(); // Doesn't cache this PS (*).
                assertEquals(0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));

                testConn.close();
                assertEquals(0, testConn.getActiveStatementCount());
                assertEquals(cachedSPS ? 0 : -1, getStmtsCacheSize.applyAsInt(testConn));
            }
        } while ((useSPS = !useSPS) || (cachePS = !cachePS));
    }
}
