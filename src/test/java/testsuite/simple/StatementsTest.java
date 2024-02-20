/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package testsuite.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.InputStream;
import java.io.ObjectInputStream;
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
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

import org.junit.jupiter.api.Test;

import com.mysql.cj.CharsetMappingWrapper;
import com.mysql.cj.MysqlConnection;
import com.mysql.cj.MysqlType;
import com.mysql.cj.Query;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
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
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.util.LRUCache;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;

import testsuite.BaseQueryInterceptor;
import testsuite.BaseTestCase;
import testsuite.regression.ConnectionRegressionTest.CountingReBalanceStrategy;

public class StatementsTest extends BaseTestCase {

    @Test
    public void testAccessorsAndMutators() throws SQLException {
        assertTrue(this.stmt.getConnection() == this.conn, "Connection can not be null, and must be same connection");

        // Set max rows, to exercise code in execute(), executeQuery() and executeUpdate()
        Statement accessorStmt = this.conn.createStatement();
        accessorStmt.setMaxRows(1);
        accessorStmt.setMaxRows(0); // FIXME, test that this actually affects rows returned
        accessorStmt.setMaxFieldSize(255);
        assertTrue(accessorStmt.getMaxFieldSize() == 255, "Max field size should match what was set");

        assertThrows("Should not be able to set max field size > max_packet_size", SQLException.class, () -> {
            accessorStmt.setMaxFieldSize(Integer.MAX_VALUE);
            return null;
        });

        accessorStmt.setCursorName("undef");
        accessorStmt.setEscapeProcessing(true);
        accessorStmt.setFetchDirection(java.sql.ResultSet.FETCH_FORWARD);

        int fetchDirection = accessorStmt.getFetchDirection();
        assertTrue(fetchDirection == java.sql.ResultSet.FETCH_FORWARD, "Set fetch direction != get fetch direction");

        assertThrows("Should not be able to set fetch direction to invalid value", SQLException.class, () -> {
            accessorStmt.setFetchDirection(Integer.MAX_VALUE);
            return null;
        });

        assertThrows("Should not be able to set max rows > 50000000", SQLException.class, () -> {
            accessorStmt.setMaxRows(50000000 + 10);
            return null;
        });

        assertThrows("Should not be able to set max rows < 0", SQLException.class, () -> {
            accessorStmt.setMaxRows(Integer.MIN_VALUE);
            return null;
        });

        int fetchSize = this.stmt.getFetchSize();

        accessorStmt.setMaxRows(4);
        assertThrows("Should not be able to set FetchSize > max rows", SQLException.class, () -> {
            accessorStmt.setFetchSize(Integer.MAX_VALUE);
            return null;
        });

        assertThrows("Should not be able to set FetchSize < 0", SQLException.class, () -> {
            accessorStmt.setFetchSize(-2);
            return null;
        });

        assertTrue(fetchSize == this.stmt.getFetchSize(), "Fetch size before invalid setFetchSize() calls should match fetch size now");
    }

    @Test
    public void testAutoIncrement() throws SQLException {
        createTable("statement_test", "(id int not null primary key auto_increment, strdata1 varchar(255) not null, strdata2 varchar(255))");

        try {
            this.stmt.setFetchSize(Integer.MIN_VALUE);

            this.stmt.executeUpdate("INSERT INTO statement_test (strdata1) values ('blah')", Statement.RETURN_GENERATED_KEYS);

            int autoIncKeyFromApi = -1;
            this.rs = this.stmt.getGeneratedKeys();

            assertTrue(this.rs.next(), "Failed to retrieve AUTO_INCREMENT using Statement.getGeneratedKeys()");
            autoIncKeyFromApi = this.rs.getInt(1);

            this.rs.close();

            int autoIncKeyFromFunc = -1;
            this.rs = this.stmt.executeQuery("SELECT LAST_INSERT_ID()");

            assertTrue(this.rs.next(), "Failed to retrieve AUTO_INCREMENT using LAST_INSERT_ID()");
            autoIncKeyFromFunc = this.rs.getInt(1);

            assertTrue(autoIncKeyFromApi != -1 && autoIncKeyFromFunc != -1, "AutoIncrement keys were '0'");
            assertTrue(autoIncKeyFromApi == autoIncKeyFromFunc, "Key retrieved from API (" + autoIncKeyFromApi
                    + ") does not match key retrieved from LAST_INSERT_ID() " + autoIncKeyFromFunc + ") function");
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
     * Tests all variants of numerical types (signed/unsigned) for correct operation when used as return values from a prepared statement.
     *
     * @throws Exception
     */
    @Test
    public void testBinaryResultSetNumericTypes() throws Exception {
        testBinaryResultSetNumericTypesInternal(this.conn);
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
        Connection sspsConn = getConnectionWithProps(props);
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
     */
    @Test
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
                assertTrue(this.rs.getString(1).equals(stringVal) && this.rs.getInt(2) == intVal);

                numRows++;
            }

            this.rs.close();
            this.rs = null;

            cStmt.close();
            cStmt = null;

            assertEquals(numRows, rowsToCheck);
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

    @Test
    public void testCancelStatement() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        Connection cancelConn = null;

        try {
            cancelConn = getConnectionWithProps(props);
            final Statement cancelStmt = cancelConn.createStatement();

            cancelStmt.setQueryTimeout(1);

            long begin = System.currentTimeMillis();

            try {
                cancelStmt.execute("SELECT SLEEP(30)");
            } catch (SQLException sqlEx) {
                assertTrue(System.currentTimeMillis() - begin < 30000, "Probably wasn't actually cancelled");
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
                assertTrue(System.currentTimeMillis() - begin < 30000, "Probably wasn't actually cancelled");
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
                assertTrue(System.currentTimeMillis() - begin < 30000, "Probably wasn't actually cancelled");
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
                assertTrue(System.currentTimeMillis() - begin < 30000, "Probably wasn't actually cancelled");
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
                assertTrue(System.currentTimeMillis() - begin < 30000, "Probably wasn't actually cancelled");
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
                assertTrue(System.currentTimeMillis() - begin < 30000, "Probably wasn't actually cancelled");
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

            Properties props2 = new Properties();
            props2.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props2.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props2.setProperty(PropertyKey.queryTimeoutKillsConnection.getKeyName(), "true");
            final Connection forceCancel = getConnectionWithProps(props2);
            final Statement forceStmt = forceCancel.createStatement();
            forceStmt.setQueryTimeout(1);

            assertThrows(MySQLTimeoutException.class, () -> {
                forceStmt.execute("SELECT SLEEP(30)");
                return null;
            });

            int count = 1000;

            for (; count > 0; count--) {
                if (forceCancel.isClosed()) {
                    break;
                }

                Thread.sleep(100);
            }

            assertFalse(count == 0, "Connection was never killed");

            assertThrows(MySQLStatementCancelledException.class, () -> {
                forceCancel.setAutoCommit(true);
                return null;
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

    @Test
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

        assertTrue(exceptionAfterClosed, "Operations not allowed on Statement after .close() is called!");
    }

    @Test
    public void testEnableStreamingResults() throws Exception {
        Statement streamStmt = this.conn.createStatement();
        ((com.mysql.cj.jdbc.JdbcStatement) streamStmt).enableStreamingResults();
        assertEquals(streamStmt.getFetchSize(), Integer.MIN_VALUE);
        assertEquals(streamStmt.getResultSetType(), ResultSet.TYPE_FORWARD_ONLY);
    }

    @Test
    public void testHoldingResultSetsOverClose() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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

    @Test
    public void testInsert() throws SQLException {
        createTable("statement_test", "(id int not null primary key auto_increment, strdata1 varchar(255) not null, strdata2 varchar(255))");

        try {
            boolean autoCommit = this.conn.getAutoCommit();

            // Test running a query for an update. It should fail.
            try {
                this.conn.setAutoCommit(false);
                this.stmt.executeUpdate("SELECT * FROM statement_test");
            } catch (SQLException sqlEx) {
                assertTrue(sqlEx.getSQLState().equalsIgnoreCase("01S03"), "Exception thrown for unknown reason");
            } finally {
                this.conn.setAutoCommit(autoCommit);
            }

            // Test running a update for an query. It should fail.
            try {
                this.conn.setAutoCommit(false);
                this.stmt.execute("UPDATE statement_test SET strdata1='blah' WHERE 1=0");
            } catch (SQLException sqlEx) {
                assertTrue(sqlEx.getSQLState().equalsIgnoreCase(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT), "Exception thrown for unknown reason");
            } finally {
                this.conn.setAutoCommit(autoCommit);
            }

            for (int i = 0; i < 10; i++) {
                int updateCount = this.stmt.executeUpdate("INSERT INTO statement_test (strdata1,strdata2) values ('abcdefg', 'poi')");
                assertTrue(updateCount == 1, "Update count must be '1', was '" + updateCount + "'");
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
    @Test
    public void testMultiStatements() throws Exception {
        Connection multiStmtConn = null;
        Statement multiStmt = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
     * Tests that NULLs and '' work correctly.
     *
     * @throws SQLException
     */
    @Test
    public void testNulls() throws SQLException {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS nullTest");
            this.stmt.executeUpdate("CREATE TABLE IF NOT EXISTS nullTest (field_1 CHAR(20), rowOrder INT)");
            this.stmt.executeUpdate("INSERT INTO nullTest VALUES (null, 1), ('', 2)");

            this.rs = this.stmt.executeQuery("SELECT field_1 FROM nullTest ORDER BY rowOrder");

            this.rs.next();

            assertTrue(this.rs.getString("field_1") == null && this.rs.wasNull(), "NULL field not returned as NULL");

            this.rs.next();

            assertTrue(this.rs.getString("field_1").equals("") && !this.rs.wasNull(), "Empty field not returned as \"\"");

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

    @Test
    public void testParsedConversionWarning() throws Exception {
        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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

    @Test
    public void testPreparedStatement() throws SQLException {
        createTable("statement_test", "(id int not null primary key auto_increment, strdata1 varchar(255) not null, strdata2 varchar(255))");

        this.stmt.executeUpdate("INSERT INTO statement_test (id, strdata1,strdata2) values (999,'abcdefg', 'poi')");
        this.pstmt = this.conn.prepareStatement("UPDATE statement_test SET strdata1=?, strdata2=? where id=999");
        this.pstmt.setString(1, "iop");
        this.pstmt.setString(2, "higjklmn");

        int updateCount = this.pstmt.executeUpdate();
        assertTrue(updateCount == 1, "Update count must be '1', was '" + updateCount + "'");

        this.pstmt.clearParameters();

        this.pstmt.close();

        this.rs = this.stmt.executeQuery("SELECT id, strdata1, strdata2 FROM statement_test");

        assertTrue(this.rs.next());
        assertTrue(this.rs.getInt(1) == 999);
        assertTrue("iop".equals(this.rs.getString(2)), "Expected 'iop', received '" + this.rs.getString(2) + "'");
        assertTrue("higjklmn".equals(this.rs.getString(3)), "Expected 'higjklmn', received '" + this.rs.getString(3) + "'");
    }

    @Test
    public void testPreparedStatementBatch() throws SQLException {
        try {
            createTable("statement_batch_test",
                    "(id int not null primary key auto_increment, strdata1 varchar(255) not null, strdata2 varchar(255), UNIQUE INDEX (strdata1))");
        } catch (SQLException sqlEx) {
            if (sqlEx.getMessage().indexOf("max key length") != -1) {
                createTable("statement_batch_test",
                        "(id int not null primary key auto_increment, strdata1 varchar(175) not null, strdata2 varchar(175), UNIQUE INDEX (strdata1))");
            }
        }

        this.pstmt = this.conn.prepareStatement("INSERT INTO statement_batch_test (strdata1, strdata2) VALUES (?,?)");

        for (int i = 0; i < 1000; i++) {
            this.pstmt.setString(1, "batch_" + i);
            this.pstmt.setString(2, "batch_" + i);
            this.pstmt.addBatch();
        }

        int[] updateCounts = this.pstmt.executeBatch();

        for (int i = 0; i < updateCounts.length; i++) {
            assertTrue(updateCounts[i] == 1, "Update count must be '1', was '" + updateCounts[i] + "'");
        }
    }

    @Test
    public void testRowFetch() throws Exception {
        createTable("testRowFetch", "(field1 int)");

        this.stmt.executeUpdate("INSERT INTO testRowFetch VALUES (1)");

        Connection fetchConn = null;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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

    @Test
    public void testSelectColumns() throws SQLException {
        final int maxColLength = 255;
        final int maxCols = 40;
        final int step = 8;

        for (int i = 6; i < maxCols; i += step) {
            StringBuilder insertBuf = new StringBuilder("INSERT INTO statement_col_test_").append(i).append(" VALUES (");
            StringBuilder tnameBuf = new StringBuilder("statement_col_test_").append(i);
            StringBuilder ddlBuf = new StringBuilder("(");

            boolean firstTime = true;
            for (int j = 0; j < i; j++) {
                if (!firstTime) {
                    ddlBuf.append(",");
                    insertBuf.append(",");
                } else {
                    firstTime = false;
                }

                ddlBuf.append("col_");
                ddlBuf.append(j);
                ddlBuf.append(" VARCHAR(");
                ddlBuf.append(maxColLength);
                ddlBuf.append(")");
                insertBuf.append("'AAAAAAAAAAAAAAAA'");
            }

            ddlBuf.append(")");
            insertBuf.append(")");
            createTable(tnameBuf.toString(), ddlBuf.toString());
            this.stmt.executeUpdate(insertBuf.toString());
        }

        for (int i = 6; i < maxCols; i += step) {
            long start = System.currentTimeMillis();
            this.rs = this.stmt.executeQuery("SELECT * from statement_col_test_" + i);

            this.rs.next();

            long end = System.currentTimeMillis();
            System.out.println(i + " columns = " + (end - start) + " ms");
        }
    }

    /**
     * Tests for PreparedStatement.setObject()
     *
     * @throws Exception
     */
    @Test
    public void testSetObject() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.noDatetimeStringSync.getKeyName(), "true"); // value=true for #5
        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "false");
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

        long currentTime = System.currentTimeMillis() / 1000 * 1000; // removing fractional seconds

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
    @Test
    public void testSetObjectWithMysqlType() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.noDatetimeStringSync.getKeyName(), "true"); // value=true for #5
        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "false");
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

        long currentTime = System.currentTimeMillis() / 1000 * 1000; // removing fractional seconds

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

    @Test
    public void testStatementRewriteBatch() throws Exception {
        for (boolean useSSPS : new boolean[] { false, true }) {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
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
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
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
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
            props.setProperty(PropertyKey.maxAllowedPacket.getKeyName(), useSSPS ? "10240" : "1024");
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

            Object[][] differentTypes = new Object[1000][15];

            createTable("rewriteBatchTypes",
                    "(internalOrder int, f1 tinyint null, f2 smallint null, f3 int null, f4 bigint null, f5 decimal(8, 2) null, "
                            + "f6 float null, f7 double null, f8 varchar(255) null, f9 text null, f10 blob null, f11 blob null, "
                            + (versionMeetsMinimum(5, 6, 4) ? "f12 datetime(3) null, f13 time(3) null, f14 date null, f15 timestamp(3) null)"
                                    : "f12 datetime null, f13 time null, f14 date null, f15 timestamp null)"));

            for (int i = 0; i < 1000; i++) {
                differentTypes[i][0] = Math.random() < .5 ? null : new Byte((byte) (Math.random() * 127));
                differentTypes[i][1] = Math.random() < .5 ? null : new Short((short) (Math.random() * Short.MAX_VALUE));
                differentTypes[i][2] = Math.random() < .5 ? null : new Integer((int) (Math.random() * Integer.MAX_VALUE));
                differentTypes[i][3] = Math.random() < .5 ? null : new Long((long) (Math.random() * Long.MAX_VALUE));
                differentTypes[i][4] = Math.random() < .5 ? null : new BigDecimal("19.95");
                differentTypes[i][5] = Math.random() < .5 ? null : new Float(3 + (float) Math.random());
                differentTypes[i][6] = Math.random() < .5 ? null : new Double(3 + Math.random());
                differentTypes[i][7] = Math.random() < .5 ? null : randomString();
                differentTypes[i][8] = Math.random() < .5 ? null : randomString();
                differentTypes[i][9] = Math.random() < .5 ? null : randomString().getBytes();
                differentTypes[i][10] = Math.random() < .5 ? null : randomString().getBytes();
                differentTypes[i][11] = Math.random() < .5 ? null : LocalDateTime.now();
                differentTypes[i][12] = Math.random() < .5 ? null : new Time(System.currentTimeMillis());
                differentTypes[i][13] = Math.random() < .5 ? null : new Date(System.currentTimeMillis());
                differentTypes[i][14] = Math.random() < .5 ? null : new Timestamp(System.currentTimeMillis());
            }

            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
            props.setProperty(PropertyKey.maxAllowedPacket.getKeyName(), useSSPS ? "10240" : "1024");
            multiConn = getConnectionWithProps(props);
            pStmt = multiConn.prepareStatement("INSERT INTO rewriteBatchTypes(internalOrder,f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14,f15) VALUES "
                    + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

            for (int i = 0; i < 1000; i++) {
                pStmt.setInt(1, i);
                for (int k = 0; k < 15; k++) {
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
                    .executeQuery("SELECT f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15 FROM rewriteBatchTypes ORDER BY internalOrder");

            int idx = 0;

            // We need to format this ourselves, since we have to strip the nanos off of TIMESTAMPs, so .equals() doesn't really work...

            SimpleDateFormat sdf = TimeUtil.getSimpleDateFormat(null, "''yyyy-MM-dd HH:mm:ss''", null);
            DateTimeFormatter dtf = TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET;

            int cnt = 0;
            while (this.rs.next()) {
                System.out.println(++cnt);
                for (int k = 0; k < 14; k++) {
                    if (differentTypes[idx][k] == null) {
                        assertTrue(this.rs.getObject(k + 1) == null,
                                "On row " + idx + " expected NULL, found " + this.rs.getObject(k + 1) + " in column " + (k + 1));
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

                            assertEquals(buf.toString(), asString, "On row " + idx + ", column " + (k + 1));

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

                            assertEquals(StringUtils.dumpAsHex(expected, expected.length), StringUtils.dumpAsHex(actual, actual.length),
                                    "On row " + idx + ", column " + (k + 1));
                        } else if (differentTypes[idx][k] instanceof byte[]) {
                            byte[] expected = (byte[]) differentTypes[idx][k];
                            byte[] actual = this.rs.getBytes(k + 1);
                            assertEquals(StringUtils.dumpAsHex(expected, expected.length), StringUtils.dumpAsHex(actual, actual.length),
                                    "On row " + idx + ", column " + (k + 1));
                        } else if (differentTypes[idx][k] instanceof Timestamp) {
                            assertEquals(sdf.format(differentTypes[idx][k]), sdf.format(this.rs.getObject(k + 1)), "On row " + idx + ", column " + (k + 1));
                        } else if (differentTypes[idx][k] instanceof LocalDateTime) {
                            assertEquals(((LocalDateTime) differentTypes[idx][k]).format(dtf), ((LocalDateTime) this.rs.getObject(k + 1)).format(dtf),
                                    "On row " + idx + ", column " + (k + 1));
                        } else if (differentTypes[idx][k] instanceof Double) {
                            assertEquals(((Double) differentTypes[idx][k]).doubleValue(), this.rs.getDouble(k + 1), .1,
                                    "On row " + idx + ", column " + (k + 1));
                        } else if (differentTypes[idx][k] instanceof Float) {
                            assertEquals(((Float) differentTypes[idx][k]).floatValue(), this.rs.getFloat(k + 1), .1, "On row " + idx + ", column " + (k + 1));
                        } else if (className.equals("java.lang.Byte")) {
                            // special mapping in JDBC for ResultSet.getObject()
                            assertEquals(new Integer(((Byte) differentTypes[idx][k]).byteValue()), this.rs.getObject(k + 1),
                                    "On row " + idx + ", column " + (k + 1));
                        } else if (className.equals("java.lang.Short")) {
                            // special mapping in JDBC for ResultSet.getObject()
                            assertEquals(new Integer(((Short) differentTypes[idx][k]).shortValue()), this.rs.getObject(k + 1),
                                    "On row " + idx + ", column " + (k + 1));
                        } else {
                            System.out.println(k + 1 + ": " + this.rs.getMetaData().getColumnName(k + 1) + ": " + differentTypes[idx][k].getClass().getName());//+ " " + this.rs.getObject(k + 1).getClass().getName());
                            assertEquals(differentTypes[idx][k].toString(), this.rs.getObject(k + 1).toString(), "On row " + idx + ", column " + (k + 1) + " ("
                                    + differentTypes[idx][k].getClass() + "/" + this.rs.getObject(k + 1).getClass());
                        }
                    }
                }

                idx++;
            }
        }
    }

    @Test
    public void testBatchRewriteErrors() throws Exception {
        createTable("rewriteErrors", "(field1 int not null primary key) ENGINE=MyISAM");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false");
        props.setProperty(PropertyKey.maxAllowedPacket.getKeyName(), "5725");
        props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
        Connection multiConn = null;

        for (boolean continueBatchOnError : new boolean[] { false, true }) {
            props.setProperty(PropertyKey.continueBatchOnError.getKeyName(), Boolean.toString(continueBatchOnError));

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
                for (int i = 3555; i < counts.length; i++) {
                    assertEquals(Statement.EXECUTE_FAILED, counts[i]);
                }

                // this depends on max_allowed_packet, only a sanity check
                assertTrue(getRowCount("rewriteErrors") >= 4000);
            }

            this.stmt.execute("TRUNCATE TABLE rewriteErrors");

            try {
                multiStmt.executeBatch();
            } catch (BatchUpdateException bUpE) {
                int[] counts = bUpE.getUpdateCounts();
                for (int i = 4095; i < counts.length; i++) {
                    assertEquals(Statement.EXECUTE_FAILED, counts[i]);
                }

                // this depends on max_allowed_packet, only a sanity check
                assertTrue(getRowCount("rewriteErrors") >= 4000);
            }

            this.stmt.execute("TRUNCATE TABLE rewriteErrors");

            createProcedure("sp_rewriteErrors", "(param1 INT)\nBEGIN\nINSERT INTO rewriteErrors VALUES (param1);\nEND");

            CallableStatement cStmt = multiConn.prepareCall("{ CALL sp_rewriteErrors(?) }");

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
                for (int i = 3991; i < counts.length; i++) {
                    assertEquals(Statement.EXECUTE_FAILED, counts[i]);
                }

                // this depends on max_allowed_packet, only a sanity check
                assertTrue(getRowCount("rewriteErrors") >= 4000);
            }

            this.stmt.execute("TRUNCATE TABLE rewriteErrors");
        }
    }

    @Test
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

    @Test
    public void testStubbed() throws SQLException {
        try {
            this.stmt.getResultSetHoldability();
        } catch (SQLFeatureNotSupportedException notImplEx) {
        }
    }

    @Test
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

        // Testing prepared statements with binary result sets now.

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

    @Test
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
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.queryInterceptors.getKeyName(), ServerStatusDiffInterceptor.class.getName());

            interceptedConn = getConnectionWithProps(props);
            this.rs = interceptedConn.createStatement().executeQuery("SELECT 'abc'");
        } finally {
            if (interceptedConn != null) {
                interceptedConn.close();
            }
        }
    }

    @Test
    public void testParameterBindings() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
        props.setProperty(PropertyKey.treatUtilDateAsTimestamp.getKeyName(), "false");

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
                    assertEquals(valuesToTest[i].toString(), boundObject.toString(),
                            "For binding #" + (i + 1) + " of class " + boundObjectClass + " compared to " + testObjectClass);
                } else if (boundObject instanceof byte[]) {
                    // Deserialize java.util.Date value.
                    ByteArrayInputStream bytesInStream = new ByteArrayInputStream((byte[]) boundObject);
                    ObjectInputStream objInStream = new ObjectInputStream(bytesInStream);
                    Object obj = objInStream.readObject();
                    objInStream.close();
                    bytesInStream.close();

                    assertEquals(java.util.Date.class, obj.getClass());
                    assertEquals(valuesToTest[i], obj, "For binding #" + (i + 1) + " of class " + boundObjectClass + " compared to " + testObjectClass);

                } else {
                    assertEquals(valuesToTest[i], boundObject, "For binding #" + (i + 1) + " of class " + boundObjectClass + " compared to " + testObjectClass);
                }
            }
        }
    }

    @Test
    public void testLocalInfileHooked() throws Exception {
        this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'local_infile'");
        assumeTrue(this.rs.next() && "ON".equalsIgnoreCase(this.rs.getString(2)), "This test requires the server started with --local-infile=ON");
        this.rs.close();

        createTable("localInfileHooked", "(field1 int, field2 varchar(255))");
        String streamData = "1\tabcd\n2\tefgh\n3\tijkl";
        InputStream stream = new ByteArrayInputStream(streamData.getBytes());

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.allowLoadLocalInfile.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);
        Statement testStmt = testConn.createStatement();

        try {
            ((com.mysql.cj.jdbc.JdbcStatement) testStmt).setLocalInfileInputStream(stream);
            testStmt.execute("LOAD DATA LOCAL INFILE 'bogusFileName' INTO TABLE localInfileHooked CHARACTER SET "
                    + CharsetMappingWrapper.getStaticMysqlCharsetForJavaEncoding(
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
    @Test
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
    @Test
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
    @Test
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
    @Test
    public void testSetNCharacterStream() throws Exception {
        // suppose sql_mode don't include "NO_BACKSLASH_ESCAPES"

        createTable("testSetNCharacterStream", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " + "c3 NATIONAL CHARACTER(10)) ENGINE=InnoDB");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props1.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
        props2.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props2.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
    @Test
    public void testSetNCharacterStreamServer() throws Exception {
        createTable("testSetNCharacterStreamServer", "(c1 NATIONAL CHARACTER(10)) ENGINE=InnoDB");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props1.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props1.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props1.setProperty(PropertyKey.characterEncoding.getKeyName(), "latin1"); // ensure charset isn't utf8 here
        Connection conn1 = getConnectionWithProps(props1);
        PreparedStatement pstmt1 = conn1.prepareStatement("INSERT INTO testSetNCharacterStreamServer (c1) VALUES (?)");
        pstmt1.setNCharacterStream(1, new StringReader("aaa"), 3);
        try {
            pstmt1.execute();
            fail();
        } catch (SQLException e) {
            // ok
            assertEquals("Can not send national characters when connection character set isn't UTF-8", e.getMessage());
        }
        pstmt1.close();
        conn1.close();

        createTable("testSetNCharacterStreamServer", "(c1 LONGTEXT charset utf8) ENGINE=InnoDB");
        Properties props2 = new Properties();
        props2.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props2.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
    @Test
    public void testSetNClob() throws Exception {
        // suppose sql_mode don't include "NO_BACKSLASH_ESCAPES"

        createTable("testSetNClob", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " + "c3 NATIONAL CHARACTER(10)) ENGINE=InnoDB");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false"); // use client-side prepared statement

        for (String enc : new String[] { "latin1", "UTF-8" }) {
            this.stmt.execute("TRUNCATE TABLE testSetNClob");
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), enc); // ensure charset isn't utf8 here
            Connection conn1 = getConnectionWithProps(props);
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
        }
    }

    /**
     * Tests for ServerPreparedStatement.setNClob()
     *
     * @throws Exception
     */
    @Test
    public void testSetNClobServer() throws Exception {
        createTable("testSetNClobServer", "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10)) ENGINE=InnoDB");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props1.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props1.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props1.setProperty(PropertyKey.characterEncoding.getKeyName(), "latin1"); // ensure charset isn't utf8 here
        Connection conn1 = getConnectionWithProps(props1);
        PreparedStatement pstmt1 = conn1.prepareStatement("INSERT INTO testSetNClobServer (c1, c2) VALUES (?, ?)");
        NClob nclob1 = conn1.createNClob();
        nclob1.setString(1, "aaa");
        Reader reader2 = new StringReader("aaa");
        pstmt1.setNClob(1, nclob1);
        pstmt1.setString(2, "abc");
        try {
            pstmt1.execute();
            fail();
        } catch (SQLException e) {
            // ok
            assertEquals("Can not send national characters when connection character set isn't UTF-8", e.getMessage());
        }
        pstmt1.setString(1, "abc");
        pstmt1.setNClob(2, reader2, 3);
        try {
            pstmt1.execute();
            fail();
        } catch (SQLException e) {
            // ok
            assertEquals("Can not send national characters when connection character set isn't UTF-8", e.getMessage());
        }
        pstmt1.close();
        conn1.close();

        createTable("testSetNClobServer", "(c1 NATIONAL CHARACTER(10), c2 LONGTEXT charset utf8) ENGINE=InnoDB");
        Properties props2 = new Properties();
        props2.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props2.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
    @Test
    public void testSetNString() throws Exception {
        // suppose sql_mode don't include "NO_BACKSLASH_ESCAPES"

        createTable("testSetNString",
                "(c1 NATIONAL CHARACTER(10), c2 NATIONAL CHARACTER(10), " + "c3 NATIONAL CHARACTER(10)) DEFAULT CHARACTER SET cp932 ENGINE=InnoDB");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props1.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
        props2.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props2.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
    @Test
    public void testSetNStringServer() throws Exception {
        createTable("testSetNStringServer", "(c1 NATIONAL CHARACTER(10)) ENGINE=InnoDB");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props1.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props1.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // use server-side prepared statement
        props1.setProperty(PropertyKey.characterEncoding.getKeyName(), "latin1"); // ensure charset isn't utf8 here
        Connection conn1 = getConnectionWithProps(props1);
        PreparedStatement pstmt1 = conn1.prepareStatement("INSERT INTO testSetNStringServer (c1) VALUES (?)");
        pstmt1.setNString(1, "aaa");
        try {
            pstmt1.execute();
            fail();
        } catch (SQLException e) {
            // ok
            assertEquals("Can not send national characters when connection character set isn't UTF-8", e.getMessage());
        }
        pstmt1.close();
        conn1.close();

        createTable("testSetNStringServer", "(c1 NATIONAL CHARACTER(10)) ENGINE=InnoDB");
        Properties props2 = new Properties();
        props2.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props2.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
    @Test
    public void testUpdateNCharacterStream() throws Exception {
        createTable("testUpdateNCharacterStream", "(c1 CHAR(10) PRIMARY KEY, c2 NATIONAL CHARACTER(10)) default character set sjis");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props1.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
        props2.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props2.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
    @Test
    public void testUpdateNClob() throws Exception {
        createTable("testUpdateNChlob", "(c1 CHAR(10) PRIMARY KEY, c2 NATIONAL CHARACTER(10)) default character set sjis");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props1.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
        props2.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props2.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
    @Test
    public void testUpdateNString() throws Exception {
        createTable("testUpdateNString", "(c1 CHAR(10) PRIMARY KEY, c2 NATIONAL CHARACTER(10)) default character set sjis");
        Properties props1 = new Properties();
        props1.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props1.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
        props2.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props2.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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

    @Test
    public void testJdbc4LoadBalancing() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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

    private final ZonedDateTime testZonedDateTime = ZonedDateTime.of(2015, 8, 04, 12, 34, 56, 7890, ZoneOffset.UTC);

    /**
     * Test shared test data validity.
     *
     * @throws Exception
     */
    @Test
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
     *
     * @throws Exception
     */
    @Test
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
     *
     * @throws Exception
     */
    @Test
    public void testStmtExecuteLargeUpdateNoGeneratedKeys() throws Exception {
        createTable("testExecuteLargeUpdate", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        long count = this.stmt.executeLargeUpdate("INSERT INTO testExecuteLargeUpdate (n) VALUES (1), (2), (3), (4), (5)");
        assertEquals(5, count);
        assertEquals(5, this.stmt.getLargeUpdateCount());

        final Statement stmtTmp = this.stmt;
        assertThrows(SQLException.class, "Generated keys not requested. You need to specify Statement.RETURN_GENERATED_KEYS to Statement.executeUpdate\\(\\), "
                + "Statement.executeLargeUpdate\\(\\) or Connection.prepareStatement\\(\\).", () -> {
                    stmtTmp.getGeneratedKeys();
                    return null;
                });
    }

    /**
     * Test for Statement.executeLargeUpdate(String, _).
     * Validate update count returned and generated keys.
     * Case 1: explicitly requesting generated keys.
     * Case 2: requesting generated keys by defining column indexes.
     * Case 3: requesting generated keys by defining column names.
     *
     * @throws Exception
     */
    @Test
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
            assertEquals(5, count, tstCase);
            assertEquals(5, this.stmt.getLargeUpdateCount(), tstCase);

            this.rs = this.stmt.getGeneratedKeys();

            ResultSetMetaData rsmd = this.rs.getMetaData();
            assertEquals(1, rsmd.getColumnCount(), tstCase);
            assertEquals(JDBCType.BIGINT.getVendorTypeNumber().intValue(), rsmd.getColumnType(1), tstCase);
            assertEquals(20, rsmd.getColumnDisplaySize(1), tstCase);

            long generatedKey = 0;
            while (this.rs.next()) {
                assertEquals(++generatedKey, this.rs.getLong(1), tstCase);
            }
            assertEquals(5, generatedKey, tstCase);
            this.rs.close();
        }
    }

    /**
     * Test for PreparedStatement.executeLargeBatch().
     * Validate update count returned and generated keys.
     *
     * @throws Exception
     */
    @Test
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
     *
     * @throws Exception
     */
    @Test
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
                + "Statement.executeLargeUpdate\\(\\) or Connection.prepareStatement\\(\\).", () -> {
                    stmtTmp.getGeneratedKeys();
                    return null;
                });
    }

    /**
     * Test for PreparedStatement.executeLargeUpdate().
     * Validate update count returned and generated keys.
     * Case: explicitly requesting generated keys.
     *
     * @throws Exception
     */
    @Test
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
     *
     * @throws Exception
     */
    @Test
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
     *
     * @throws Exception
     */
    @Test
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
     *
     * @throws Exception
     */
    @Test
    public void testServerPrepStmtExecuteLargeBatch() throws Exception {
        /*
         * Fully working batch
         */
        createTable("testExecuteLargeBatch", "(id BIGINT AUTO_INCREMENT PRIMARY KEY, n INT)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);

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
     *
     * @throws Exception
     */
    @Test
    public void testStmtGetSetLargeMaxRows() throws Exception {
        assertEquals(0, this.stmt.getMaxRows());
        assertEquals(0, this.stmt.getLargeMaxRows());

        this.stmt.setMaxRows(50000000);

        assertEquals(50000000, this.stmt.getMaxRows());
        assertEquals(50000000, this.stmt.getLargeMaxRows());

        final Statement stmtTmp = this.stmt;
        assertThrows(SQLException.class, "setMaxRows\\(\\) out of range. 50000001 > 50000000.", () -> {
            stmtTmp.setMaxRows(50000001);
            return null;
        });

        this.stmt.setLargeMaxRows(0);

        assertEquals(0, this.stmt.getMaxRows());
        assertEquals(0, this.stmt.getLargeMaxRows());

        this.stmt.setLargeMaxRows(50000000);

        assertEquals(50000000, this.stmt.getMaxRows());
        assertEquals(50000000, this.stmt.getLargeMaxRows());

        assertThrows(SQLException.class, "setMaxRows\\(\\) out of range. 50000001 > 50000000.", () -> {
            stmtTmp.setLargeMaxRows(50000001L);
            return null;
        });
    }

    /**
     * Test for PreparedStatement.setObject().
     * Validate new methods as well as support for the types java.time.Local[Date][Time] and java.time.Offset[Date]Time.
     *
     * @throws Exception
     */
    @Test
    public void testPrepStmtSetObjectAndNewSupportedTypes() throws Exception {
        /*
         * Objects java.time.Local[Date][Time] are supported via conversion to/from java.sql.[Date|Time|Timestamp].
         */
        createTable("testSetObjectPS1", "(id INT, d DATE, t TIME, dt DATETIME, ts TIMESTAMP)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "false");

        Connection testConn = getConnectionWithProps(timeZoneFreeDbUrl, props);
        this.pstmt = testConn.prepareStatement("INSERT INTO testSetObjectPS1 VALUES (?, ?, ?, ?, ?)");
        this.stmt = testConn.createStatement();
        validateTestDataLocalDTTypes("testSetObjectPS1", insertTestDataLocalDTTypes(this.pstmt));

        createTable("testSetObjectPS2", "(id INT, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB, zdt1 VARCHAR(100), zdt2 BLOB)");

        this.pstmt = testConn.prepareStatement("INSERT INTO testSetObjectPS2 VALUES (?, ?, ?, ?, ?, ?, ?)");
        validateTestDataOffsetDTTypes("testSetObjectPS2", insertTestDataOffsetDTTypes(this.pstmt));
    }

    /**
     * Test for PreparedStatement.setObject(), unsupported SQL types TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE and REF_CURSOR.
     *
     * @throws Exception
     */
    @Test
    public void testPrepStmtSetObjectAndNewUnsupportedTypes() throws Exception {
        checkUnsupportedTypesBehavior(this.conn.prepareStatement("SELECT ?"));
    }

    /**
     * Test for CallableStatement.setObject().
     * Validate new methods as well as support for the types java.time.Local[Date][Time] and java.time.Offset[Date]Time.
     *
     * @throws Exception
     */
    @Test
    public void testCallStmtSetObjectAndNewSupportedTypes() throws Exception {
        /*
         * Objects java.time.Local[Date][Time] are supported via conversion to/from java.sql.[Date|Time|Timestamp].
         */
        createTable("testSetObjectCS1", "(id INT, d DATE, t TIME, dt DATETIME, ts TIMESTAMP)");
        createProcedure("testSetObjectCS1Proc",
                "(IN id INT, IN d DATE, IN t TIME, IN dt DATETIME, IN ts TIMESTAMP) BEGIN " + "INSERT INTO testSetObjectCS1 VALUES (id, d, t, dt, ts); END");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "false");

        Connection testConn = getConnectionWithProps(timeZoneFreeDbUrl, props);
        this.stmt = testConn.createStatement();

        CallableStatement testCstmt = testConn.prepareCall("{CALL testSetObjectCS1Proc(?, ?, ?, ?, ?)}");
        validateTestDataLocalDTTypes("testSetObjectCS1", insertTestDataLocalDTTypes(testCstmt));

        createTable("testSetObjectCS2", "(id INT, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB, zdt1 VARCHAR(100), zdt2 BLOB)");
        createProcedure("testSetObjectCS2Proc",
                "(id INT, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB, zdt1 VARCHAR(100), zdt2 BLOB) BEGIN INSERT INTO testSetObjectCS2 VALUES (id, ot1, ot2, odt1, odt2, zdt1, zdt2); END");

        testCstmt = testConn.prepareCall("{CALL testSetObjectCS2Proc(?, ?, ?, ?, ?, ?, ?)}");
        validateTestDataOffsetDTTypes("testSetObjectCS2", insertTestDataOffsetDTTypes(testCstmt));
    }

    /**
     * Test for CallableStatement.setObject(), unsupported SQL types TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE and REF_CURSOR.
     *
     * @throws Exception
     */
    @Test
    public void testCallStmtSetObjectAndNewUnsupportedTypes() throws Exception {
        createProcedure("testUnsupportedTypesProc", "(OUT param VARCHAR(20)) BEGIN SELECT 1; END");
        checkUnsupportedTypesBehavior(this.conn.prepareCall("{CALL testUnsupportedTypesProc(?)}"));
    }

    /**
     * Test for (Server)PreparedStatement.setObject().
     * Validate new methods as well as support for the types java.time.Local[Date][Time] and java.time.Offset[Date]Time.
     *
     * @throws Exception
     */
    @Test
    public void testServPrepStmtSetObjectAndNewSupportedTypes() throws Exception {
        /*
         * Objects java.time.Local[Date][Time] are supported via conversion to/from java.sql.[Date|Time|Timestamp].
         */
        createTable("testSetObjectSPS1", "(id INT, d DATE, t TIME, dt DATETIME, ts TIMESTAMP)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "false");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");

        Connection testConn = getConnectionWithProps(timeZoneFreeDbUrl, props);

        this.stmt = testConn.createStatement();
        this.pstmt = testConn.prepareStatement("INSERT INTO testSetObjectSPS1 VALUES (?, ?, ?, ?, ?)");
        validateTestDataLocalDTTypes("testSetObjectSPS1", insertTestDataLocalDTTypes(this.pstmt));

        createTable("testSetObjectSPS2", "(id INT, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB, zdt1 VARCHAR(100), zdt2 BLOB)");

        this.pstmt = testConn.prepareStatement("INSERT INTO testSetObjectSPS2 VALUES (?, ?, ?, ?, ?, ?, ?)");
        validateTestDataOffsetDTTypes("testSetObjectSPS2", insertTestDataOffsetDTTypes(this.pstmt));
    }

    /**
     * Test for (Server)PreparedStatement.setObject(), unsupported SQL types TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE and REF_CURSOR.
     *
     * @throws Exception
     */
    @Test
    public void testServPrepStmtSetObjectAndNewUnsupportedTypes() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);
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
            assertEquals(++rowCount, this.rs.getInt(1), row);

            assertEquals(this.testDateString, this.rs.getString(2), row);
            assertEquals(this.testTimeString, this.rs.getString(3), row);
            assertEquals(this.testDateTimeString, this.rs.getString(4), row);
            assertEquals(this.testDateTimeString, this.rs.getString(5), row);

            assertEquals(this.testSqlDate, this.rs.getDate(2), row);
            assertEquals(this.testSqlTime, this.rs.getTime(3), row);
            assertEquals(this.testSqlTimeStamp, this.rs.getTimestamp(4), row);
            assertEquals(this.testSqlTimeStamp, this.rs.getTimestamp(5), row);

            assertEquals(this.testLocalDate, this.rs.getObject(2, LocalDate.class), row);
            assertEquals(this.testLocalTime, this.rs.getObject(3, LocalTime.class), row);
            assertEquals(this.testLocalDateTime, this.rs.getObject(4, LocalDateTime.class), row);
            assertEquals(this.testLocalDateTime, this.rs.getObject(5, LocalDateTime.class), row);

            assertEquals(rowCount, this.rs.getInt("id"), row);

            assertEquals(this.testDateString, this.rs.getString("d"), row);
            assertEquals(this.testTimeString, this.rs.getString("t"), row);
            assertEquals(this.testDateTimeString, this.rs.getString("dt"), row);
            assertEquals(this.testDateTimeString, this.rs.getString("ts"), row);

            assertEquals(this.testSqlDate, this.rs.getDate("d"), row);
            assertEquals(this.testSqlTime, this.rs.getTime("t"), row);
            assertEquals(this.testSqlTimeStamp, this.rs.getTimestamp("dt"), row);
            assertEquals(this.testSqlTimeStamp, this.rs.getTimestamp("ts"), row);

            assertEquals(this.testLocalDate, this.rs.getObject("d", LocalDate.class), row);
            assertEquals(this.testLocalTime, this.rs.getObject("t", LocalTime.class), row);
            assertEquals(this.testLocalDateTime, this.rs.getObject("dt", LocalDateTime.class), row);
            assertEquals(this.testLocalDateTime, this.rs.getObject("ts", LocalDateTime.class), row);
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
        prepStmt.setObject(6, this.testZonedDateTime, JDBCType.VARCHAR);
        prepStmt.setObject(7, this.testZonedDateTime);
        assertEquals(1, prepStmt.executeUpdate());

        if (prepStmt instanceof CallableStatement) {
            CallableStatement cstmt = (CallableStatement) prepStmt;

            cstmt.setInt("id", 2);
            cstmt.setObject("ot1", this.testOffsetTime, JDBCType.VARCHAR);
            cstmt.setObject("ot2", this.testOffsetTime);
            cstmt.setObject("odt1", this.testOffsetDateTime, JDBCType.VARCHAR);
            cstmt.setObject("odt2", this.testOffsetDateTime);
            cstmt.setObject("zdt1", this.testZonedDateTime, JDBCType.VARCHAR);
            cstmt.setObject("zdt2", this.testZonedDateTime);
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
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "false");
        Connection testConn = getConnectionWithProps(timeZoneFreeDbUrl, props);
        Statement testStmt = testConn.createStatement();
        this.rs = testStmt.executeQuery("SELECT * FROM " + tableName);

        long expTimeSeconds = this.testOffsetTime.atDate(LocalDate.now()).toEpochSecond();
        long expDatetimeSeconds = this.testOffsetDateTime.toEpochSecond();
        long expZdtSeconds = this.testZonedDateTime.toEpochSecond();
        int rowCount = 0;
        while (this.rs.next()) {
            String row = "Row " + this.rs.getInt(1);
            assertEquals(++rowCount, this.rs.getInt(1));

            assertEquals(expTimeSeconds, this.rs.getObject(2, OffsetTime.class).atDate(LocalDate.now()).toEpochSecond(), row);
            assertEquals(expTimeSeconds, this.rs.getObject(3, OffsetTime.class).atDate(LocalDate.now()).toEpochSecond(), row);
            assertEquals(this.testOffsetDateTime, this.rs.getObject(4, OffsetDateTime.class), row);
            assertEquals(expDatetimeSeconds, this.rs.getObject(5, OffsetDateTime.class).toEpochSecond(), row);
            assertEquals(expZdtSeconds, this.rs.getObject(6, ZonedDateTime.class).toEpochSecond(), row);
            assertEquals(expZdtSeconds, this.rs.getObject(7, ZonedDateTime.class).toEpochSecond(), row);

            assertEquals(rowCount, this.rs.getInt("id"), row);

            assertEquals(expTimeSeconds, this.rs.getObject("ot1", OffsetTime.class).atDate(LocalDate.now()).toEpochSecond(), row);
            assertEquals(expTimeSeconds, this.rs.getObject("ot2", OffsetTime.class).atDate(LocalDate.now()).toEpochSecond(), row);
            assertEquals(this.testOffsetDateTime, this.rs.getObject("odt1", OffsetDateTime.class), row);
            assertEquals(expDatetimeSeconds, this.rs.getObject("odt2", OffsetDateTime.class).toEpochSecond(), row);
            assertEquals(expZdtSeconds, this.rs.getObject("zdt1", ZonedDateTime.class).toEpochSecond(), row);
            assertEquals(expZdtSeconds, this.rs.getObject("zdt2", ZonedDateTime.class).toEpochSecond(), row);
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
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", () -> {
            prepStmt.setObject(1, OffsetTime.now(), JDBCType.TIME_WITH_TIMEZONE);
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", () -> {
            prepStmt.setObject(1, OffsetDateTime.now(), JDBCType.TIMESTAMP_WITH_TIMEZONE);
            return null;
        });
        if (cstmt != null) {
            assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", () -> {
                cstmt.setObject("param", OffsetTime.now(), JDBCType.TIME_WITH_TIMEZONE);
                return null;
            });
            assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", () -> {
                cstmt.setObject("param", OffsetDateTime.now(), JDBCType.TIMESTAMP_WITH_TIMEZONE);
                return null;
            });
        }
        /*
         * Unsupported SQL type REF_CURSOR.
         */
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", () -> {
            prepStmt.setObject(1, new Object(), JDBCType.REF_CURSOR);
            return null;
        });
        if (cstmt != null) {
            assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", () -> {
                cstmt.setObject("param", new Object(), JDBCType.REF_CURSOR);
                return null;
            });
        }
    }

    /**
     * Test for CallableStatement.registerOutParameter().
     *
     * @throws Exception
     */
    @Test
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
     *
     * @throws Exception
     */
    @Test
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
     *
     * @throws Exception
     */
    @Test
    public void testCallStmtRegisterOutParameterNewUnsupportedTypes() throws Exception {
        createProcedure("testUnsupportedTypesProc", "(OUT param VARCHAR(20)) BEGIN SELECT 1; END");
        final CallableStatement testCstmt = this.conn.prepareCall("{CALL testUnsupportedTypesProc(?)}");

        /*
         * Unsupported SQL types TIME_WITH_TIMEZONE and TIMESTAMP_WITH_TIMEZONE.
         */
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", () -> {
            testCstmt.registerOutParameter(1, JDBCType.TIME_WITH_TIMEZONE);
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", () -> {
            testCstmt.registerOutParameter(1, JDBCType.TIME_WITH_TIMEZONE, 1);
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", () -> {
            testCstmt.registerOutParameter(1, JDBCType.TIME_WITH_TIMEZONE, "dummy");
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", () -> {
            testCstmt.registerOutParameter("param", JDBCType.TIME_WITH_TIMEZONE);
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", () -> {
            testCstmt.registerOutParameter("param", JDBCType.TIME_WITH_TIMEZONE, 1);
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", () -> {
            testCstmt.registerOutParameter("param", JDBCType.TIME_WITH_TIMEZONE, "dummy");
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", () -> {
            testCstmt.registerOutParameter(1, JDBCType.TIMESTAMP_WITH_TIMEZONE);
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", () -> {
            testCstmt.registerOutParameter(1, JDBCType.TIMESTAMP_WITH_TIMEZONE, 1);
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", () -> {
            testCstmt.registerOutParameter(1, JDBCType.TIMESTAMP_WITH_TIMEZONE, "dummy");
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", () -> {
            testCstmt.registerOutParameter("param", JDBCType.TIMESTAMP_WITH_TIMEZONE);
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", () -> {
            testCstmt.registerOutParameter("param", JDBCType.TIMESTAMP_WITH_TIMEZONE, 1);
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", () -> {
            testCstmt.registerOutParameter("param", JDBCType.TIMESTAMP_WITH_TIMEZONE, "dummy");
            return null;
        });

        /*
         * Unsupported SQL type REF_CURSOR.
         */
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", () -> {
            testCstmt.registerOutParameter(1, JDBCType.REF_CURSOR);
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", () -> {
            testCstmt.registerOutParameter(1, JDBCType.REF_CURSOR, 1);
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", () -> {
            testCstmt.registerOutParameter(1, JDBCType.REF_CURSOR, "dummy");
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", () -> {
            testCstmt.registerOutParameter("param", JDBCType.REF_CURSOR);
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", () -> {
            testCstmt.registerOutParameter("param", JDBCType.REF_CURSOR, 1);
            return null;
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", () -> {
            testCstmt.registerOutParameter("param", JDBCType.REF_CURSOR, "dummy");
            return null;
        });
    }

    /**
     * WL#11101 - Remove de-cache and close of SSPSs on double call to close()
     *
     * @throws Exception
     */
    @Test
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
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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

    @Test
    public void testResultSetProducingQueries() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 19), "MySQL 8.0.19+ is required to run this test.");

        // Prepare testing entities and data.
        createTable("rsProdQuery", "(col1 INT, col2 VARCHAR(100))");
        createProcedure("rsProdQueryProc", "() BEGIN SELECT * FROM rsProdQuery; END");
        assertEquals(2, this.stmt.executeUpdate("INSERT INTO rsProdQuery VALUES (1, 'test1'), (2, 'test2')"));
        assertFalse(this.stmt.execute("PREPARE rsProdQueryPS FROM \"SELECT * FROM rsProdQuery\""));

        String[] okQueries = new String[] {
                // Data Manipulation Statements:
                "SELECT * FROM rsProdQuery", "TABLE rsProdQuery", "VALUES ROW (1, 'test1'), ROW (2, 'test2')", "CALL rsProdQueryProc()",
                "WITH cte1 AS (TABLE rsProdQuery), cte2 AS (TABLE rsProdQuery) SELECT * FROM cte1", "WITH cte1 AS (TABLE rsProdQuery) TABLE cte1",
                "WITH cte1 AS (TABLE rsProdQuery) VALUES ROW (1, 'test1'), ROW (2, 'test2')",
                // Transactional and Locking Statements:
                "XA RECOVER",
                // Prepared Statements:
                "EXECUTE rsProdQueryPS",
                // Database Administration Statements/Table Maintenance Statements:
                "ANALYZE TABLE rsProdQuery", "CHECK TABLE rsProdQuery", "CHECKSUM TABLE rsProdQuery", "OPTIMIZE TABLE rsProdQuery", "REPAIR TABLE rsProdQuery",
                // Database Administration Statements/SHOW Statements:
                "SHOW CREATE TABLE rsProdQuery",
                // Utility Statements:
                "DESC rsProdQuery", "DESCRIBE rsProdQuery", "EXPLAIN rsProdQuery", "HELP 'SELECT'" };
        for (String query : okQueries) {
            try {
                this.rs = this.stmt.executeQuery(query);
                this.rs.absolute(2);
                this.rs.beforeFirst();
                this.rs.next();
            } catch (SQLException e) {
                e.printStackTrace();
                fail("Should not have thrown an Exception while executing \"" + query + "\"");
            }
        }

        String[] notOkQueries = new String[] {
                // Data Manipulation Statements:
                "INSERT INTO rsProdQuery VALUES (99, 'test99')", "REPLACE INTO rsProdQuery VALUES (99, 'test99')", "UPDATE rsProdQuery SET col1 = col1 + 1",
                "DELETE FROM rsProdQuery", "TRUNCATE TABLE rsProdQuery", "DO 1 + 1", "HANDLER rsProdQuery OPEN AS hrsProdQuery",
                "IMPORT TABLE FROM 'rsProdQuery'", "LOAD DATA INFILE 'rsProdQuery' INTO TABLE rsProdQuery",
                "WITH cte1 AS (TABLE rsProdQuery) UPDATE rsProdQuery SET c = c + 1", "WITH cte1 AS (TABLE rsProdQuery) DELETE FROM rsProdQuery",
                // Transactional and Locking Statements:
                "BEGIN", "START TRANSACTION", "SAVEPOINT rsProdQuery", "RELEASE SAVEPOINT rsProdQuery", "ROLLBACK", "COMMIT", "LOCK INSTANCE FOR BACKUP",
                "UNLOCK INSTANCE", "XA START 'rsProdQuery'",
                // Replication Statements:
                "PURGE BINARY LOGS TO 'rsProdQuery'", "CHANGE REPLICATION SOURCE TO SOURCE_DELAY=0", "RESET REPLICA", "STOP REPLICA",
                // Prepared Statements:
                "PREPARE rsProdQueryPS FROM 'TABLE rsProdQuery'", "DEALLOCATE PREPARE rsProdQueryPS",
                // Compound Statement Syntax/Condition Handling:
                "SIGNAL SQLSTATE '01000'", "RESIGNAL", "GET DIAGNOSTICS @n = NUMBER",
                // Database Administration Statements/Account Management Statements:
                "CREATE USER rsProdQueryUser", "ALTER USER rsProdQueryUser", "RENAME USER rsProdQueryUser to rsProdQueryUserNew",
                "GRANT SELECT ON rsProdQueryDb.* TO rsProdQueryUser", "REVOKE ALL ON *.* FROM rsProdQueryUser", "DROP USER rsProdQuery",
                // Database Administration Statements/Component, Plugin, and Loadable Function Statements:
                "INSTALL COMPONENT 'rsProdQuery'", "UNINSTALL COMPONENT 'rsProdQuery'",
                // Database Administration Statements/CLONE Statement & SET Statements:
                "CLONE LOCAL DATA DIRECTORY '/tmp'", "SET @rsProdQuery = 'rsProdQuery'",
                // Database Administration Statements/Other Administrative Statements:
                "BINLOG 'rsProdQuery'", "CACHE INDEX rsProdQueryIdx IN rsProdQueryCache", "FLUSH STATUS", "KILL 0", "RESTART", "SHUTDOWN",
                //  Utility Statements
                "USE rsProdQueryDb" };
        for (String query : notOkQueries) {
            assertThrows("Query: " + query, SQLException.class, "Statement\\.executeQuery\\(\\) cannot issue statements that do not produce result sets\\.",
                    () -> {
                        this.stmt.executeQuery(query);
                        return null;
                    });
        }
    }

    @Test
    public void testReadOnlySafeStatements() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 19), "MySQL 8.0.19+ is required to run this test.");

        // Prepare testing entities and data.
        createTable("roSafeTest", "(col1 INT, col2 VARCHAR(100))");
        createProcedure("roSafeTestProc", "() BEGIN SELECT * FROM roSafeTest; END");
        assertEquals(2, this.stmt.executeUpdate("INSERT INTO roSafeTest VALUES (1, 'test1'), (2, 'test2')"));
        assertFalse(this.stmt.execute("PREPARE roSafeTestPS FROM \"SELECT * FROM roSafeTest\""));

        Connection testConn = getConnectionWithProps("");
        Statement testStmt = testConn.createStatement();
        testConn.setReadOnly(true);

        String[] okQueries = new String[] {
                // Data Manipulation Statements:
                "SELECT * FROM roSafeTest", "TABLE roSafeTest", "VALUES ROW (1, 'test1'), ROW (2, 'test2')", "CALL roSafeTestProc()",
                "WITH cte1 AS (TABLE roSafeTest), cte2 AS (TABLE roSafeTest) SELECT * FROM cte1", "WITH cte1 AS (TABLE roSafeTest) TABLE cte1",
                "WITH cte1 AS (TABLE roSafeTest) VALUES ROW (1, 'test1'), ROW (2, 'test2')", "DO 1 + 1", "HANDLER roSafeTest OPEN AS hroSafeTest",
                // Transactional and Locking Statements:
                "BEGIN", "START TRANSACTION", "SAVEPOINT roSafeTest", "RELEASE SAVEPOINT roSafeTest", "ROLLBACK", "COMMIT", "LOCK INSTANCE FOR BACKUP",
                "UNLOCK INSTANCE", "XA START 'roSafeTest'", "XA END 'roSafeTest'", "XA ROLLBACK 'roSafeTest'",
                // Replication Statements:
                "PURGE BINARY LOGS TO 'roSafeTest'", "STOP REPLICA",
                // Prepared Statements:
                "PREPARE roSafeTestPS FROM 'TABLE roSafeTest'", "EXECUTE roSafeTestPS", "DEALLOCATE PREPARE roSafeTestPS",
                // Compound Statement Syntax/Condition Handling:
                "SIGNAL SQLSTATE '01000'", "RESIGNAL", "GET DIAGNOSTICS @n = NUMBER",
                // Database Administration Statements/Table Maintenance Statements:
                "ANALYZE TABLE roSafeTest", "CHECK TABLE roSafeTest", "CHECKSUM TABLE roSafeTest",
                // Database Administration Statements/CLONE Statement, SET & SHOW Statements:
                "CLONE LOCAL DATA DIRECTORY '/tmp'", "SET @roSafeTest = 'roSafeTest'", "SHOW CREATE TABLE roSafeTest",
                // Database Administration Statements/Other Administrative Statements:
                "BINLOG 'roSafeTest'", "CACHE INDEX roSafeTestIdx IN roSafeTestCache", "FLUSH STATUS", "KILL 0",
                // "RESTART", it's safe but can't be executed in this test
                // "SHUTDOWN", it's safe but can't be executed in this test
                //  Utility Statements
                "USE roSafeTestDb",
                // Utility Statements:
                "DESC roSafeTest", "DESCRIBE roSafeTest", "EXPLAIN roSafeTest", "HELP 'SELECT'" };
        for (String query : okQueries) {
            try {
                testStmt.execute(query);
            } catch (SQLException e) {
                assertNotEquals("Connection is read-only. Queries leading to data modification are not allowed.", e.getMessage());
            }
        }

        String[] notOkQueries = new String[] {
                // Data Manipulation Statements:
                "INSERT INTO roSafeTest VALUES (99, 'test99')", "REPLACE INTO roSafeTest VALUES (99, 'test99')", "UPDATE roSafeTest SET col1 = col1 + 1",
                "DELETE FROM roSafeTest", "TRUNCATE TABLE roSafeTest", "IMPORT TABLE FROM 'roSafeTest'", "LOAD DATA INFILE 'roSafeTest' INTO TABLE roSafeTest",
                "WITH cte1 AS (TABLE roSafeTest) UPDATE roSafeTest SET c = c + 1", "WITH cte1 AS (TABLE roSafeTest) DELETE FROM roSafeTest",
                // Replication Statements:
                "CHANGE REPLICATION SOURCE TO SOURCE_DELAY=0", "RESET REPLICA",
                // Database Administration Statements/Account Management Statements:
                "CREATE USER roSafeTestUser", "ALTER USER roSafeTestUser", "RENAME USER roSafeTestUser to roSafeTestUserNew",
                "GRANT SELECT ON roSafeTestDb.* TO roSafeTestUser", "REVOKE ALL ON *.* FROM roSafeTestUser", "DROP USER roSafeTest",
                // Database Administration Statements/Table Maintenance Statements:
                "OPTIMIZE TABLE roSafeTest", "REPAIR TABLE roSafeTest",
                // Database Administration Statements/Component, Plugin, and Loadable Function Statements:
                "INSTALL COMPONENT 'roSafeTest'", "UNINSTALL COMPONENT 'roSafeTest'", };
        for (String query : notOkQueries) {
            assertThrows("Query: " + query, SQLException.class, "Connection is read-only\\. Queries leading to data modification are not allowed\\.", () -> {
                testStmt.execute(query);
                return null;
            });
        }
    }

    @Test
    public void testQueryInfoParsingAndRewrittingLoadData() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 19), "MySQL 8.0.19+ is required to run this test.");

        createTable("testQueryInfo", "(c1 INT, c2 INT)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), QueryInfoQueryInterceptor.class.getName());
        props.setProperty(PropertyKey.continueBatchOnError.getKeyName(), "true");
        props.setProperty(PropertyKey.emulateUnsupportedPstmts.getKeyName(), "true");

        boolean rwBS = false;
        boolean useSPS = false;

        do {
            final String testCase = String.format("Case [rwBS: %s, useSPS: %s]", rwBS ? "Y" : "N", useSPS ? "Y" : "N");

            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString(rwBS));
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            Connection testConn = getConnectionWithProps(props);

            // LOAD DATA INFILE ? ... --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("LOAD DATA INFILE ? INTO TABLE testQueryInfo");
            this.pstmt.setString(1, "path1/file1");
            this.pstmt.addBatch();
            this.pstmt.setString(1, "path2/file2");
            this.pstmt.addBatch();
            assertThrows(BatchUpdateException.class, () -> this.pstmt.executeBatch());
            QueryInfoQueryInterceptor.assertCapturedSql(testCase, "LOAD DATA INFILE 'path1/file1' INTO TABLE testQueryInfo",
                    "LOAD DATA INFILE 'path2/file2' INTO TABLE testQueryInfo");

            testConn.close();
        } while ((useSPS = !useSPS) || (rwBS = !rwBS));
    }

    @Test
    public void testQueryInfoParsingAndRewrittingInsertValuesStatic() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 19), "MySQL 8.0.19+ is required to run this test.");

        createTable("testQueryInfo", "(c1 INT, c2 INT)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), QueryInfoQueryInterceptor.class.getName());
        props.setProperty(PropertyKey.continueBatchOnError.getKeyName(), "true");
        props.setProperty(PropertyKey.emulateUnsupportedPstmts.getKeyName(), "true");

        boolean rwBS = false;
        boolean useSPS = false;

        do {
            final String testCase = String.format("Case [rwBS: %s, useSPS: %s]", rwBS ? "Y" : "N", useSPS ? "Y" : "N");

            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString(rwBS));
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            Connection testConn = getConnectionWithProps(props);

            // INSERT ... VALUES (n, m) --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (1, 2)");
            this.pstmt.addBatch();
            this.pstmt.addBatch();
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS) { // && (useSPS || !useSPS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2),(1, 2),(1, 2)");
            } else { // !rwBS && (useSPS || !useSPS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2)", "INSERT INTO testQueryInfo VALUES (1, 2)",
                        "INSERT INTO testQueryInfo VALUES (1, 2)");
            }

            // INSERT ... VALUES (n, m) AS ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (1, 2) AS new(v1, v2)");
            this.pstmt.addBatch();
            this.pstmt.addBatch();
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS) { // && (useSPS || !useSPS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2),(1, 2),(1, 2) AS new(v1, v2)");
            } else { // !rwBS && (useSPS || !useSPS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2) AS new(v1, v2)",
                        "INSERT INTO testQueryInfo VALUES (1, 2) AS new(v1, v2)", "INSERT INTO testQueryInfo VALUES (1, 2) AS new(v1, v2)");
            }

            // INSERT ... VALUES (n, m) AS ... ON DUPLICATE KEY UPDATE ... VALUES() --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn
                    .prepareStatement("INSERT INTO testQueryInfo VALUES (1, 2) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            this.pstmt.addBatch();
            this.pstmt.addBatch();
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS) { // && (useSPS || !useSPS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES (1, 2),(1, 2),(1, 2) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            } else { // !rwBS && (useSPS || !useSPS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES (1, 2) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)",
                        "INSERT INTO testQueryInfo VALUES (1, 2) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)",
                        "INSERT INTO testQueryInfo VALUES (1, 2) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            }

            // INSERT ... VALUES (n, m) ON DUPLICATE KEY UPDATE ... VALUES() --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (1, 2) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            this.pstmt.addBatch();
            this.pstmt.addBatch();
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS) { // && (useSPS || !useSPS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES (1, 2),(1, 2),(1, 2) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else { // !rwBS && (useSPS || !useSPS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo VALUES (1, 2) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo VALUES (1, 2) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            }

            // INSERT ... VALUES (n, m) ON DUPLICATE KEY UPDATE ... LAST_INSERT_ID() --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (1, 2) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()");
            this.pstmt.addBatch();
            this.pstmt.addBatch();
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()",
                    "INSERT INTO testQueryInfo VALUES (1, 2) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()",
                    "INSERT INTO testQueryInfo VALUES (1, 2) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()");

            // INSERT ... VALUES (n, LAST_INSERT_ID())  --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (1, LAST_INSERT_ID())");
            this.pstmt.addBatch();
            this.pstmt.addBatch();
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, LAST_INSERT_ID())",
                    "INSERT INTO testQueryInfo VALUES (1, LAST_INSERT_ID())", "INSERT INTO testQueryInfo VALUES (1, LAST_INSERT_ID())");

            testConn.close();
        } while ((useSPS = !useSPS) || (rwBS = !rwBS));
    }

    @Test
    public void testQueryInfoParsingAndRewrittingInsertValuesEroteme() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 19), "MySQL 8.0.19+ is required to run this test.");

        createTable("testQueryInfo", "(c1 INT, c2 INT)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), QueryInfoQueryInterceptor.class.getName());
        props.setProperty(PropertyKey.continueBatchOnError.getKeyName(), "true");
        props.setProperty(PropertyKey.emulateUnsupportedPstmts.getKeyName(), "true");

        boolean rwBS = false;
        boolean useSPS = false;

        do {
            final String testCase = String.format("Case [rwBS: %s, useSPS: %s]", rwBS ? "Y" : "N", useSPS ? "Y" : "N");

            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString(rwBS));
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            Connection testConn = getConnectionWithProps(props);

            // INSERT ... VALUES (?, ?) --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (?, ?)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 5);
            this.pstmt.setInt(2, 6);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, ?),(?, ?),(?, ?)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, ?)", "INSERT INTO testQueryInfo VALUES (?, ?)",
                        "INSERT INTO testQueryInfo VALUES (?, ?)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2),(3, 4),(5, 6)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2)", "INSERT INTO testQueryInfo VALUES (3, 4)",
                        "INSERT INTO testQueryInfo VALUES (5, 6)");
            }

            // INSERT ... VALUES (?, ?) AS ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (?, ?) AS new(v1, v2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, ?),(?, ?) AS new(v1, v2)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, ?) AS new(v1, v2)",
                        "INSERT INTO testQueryInfo VALUES (?, ?) AS new(v1, v2)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2),(3, 4) AS new(v1, v2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2) AS new(v1, v2)",
                        "INSERT INTO testQueryInfo VALUES (3, 4) AS new(v1, v2)");
            }

            // INSERT ... VALUES (?, ?) AS ... ON DUPLICATE KEY UPDATE ... VALUES() --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn
                    .prepareStatement("INSERT INTO testQueryInfo VALUES (?, ?) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES (?, ?),(?, ?) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES (?, ?) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)",
                        "INSERT INTO testQueryInfo VALUES (?, ?) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES (1, 2),(3, 4) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES (1, 2) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)",
                        "INSERT INTO testQueryInfo VALUES (3, 4) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            }

            // INSERT ... VALUES (?, ?) ON DUPLICATE KEY UPDATE ... VALUES() --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (?, ?) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, ?),(?, ?) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, ?) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo VALUES (?, ?) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2),(3, 4) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo VALUES (3, 4) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            }

            // INSERT ... VALUES (?, m) ON DUPLICATE KEY UPDATE ... ? --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (?, 2) ON DUPLICATE KEY UPDATE c1 = ?");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, 2) ON DUPLICATE KEY UPDATE c1 = ?",
                        "INSERT INTO testQueryInfo VALUES (?, 2) ON DUPLICATE KEY UPDATE c1 = ?");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2) ON DUPLICATE KEY UPDATE c1 = 2",
                        "INSERT INTO testQueryInfo VALUES (3, 2) ON DUPLICATE KEY UPDATE c1 = 4");
            }

            // INSERT ... VALUES (?, ?) ON DUPLICATE KEY UPDATE ... LAST_INSERT_ID() --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (?, ?) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, ?) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()",
                        "INSERT INTO testQueryInfo VALUES (?, ?) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 2) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()",
                        "INSERT INTO testQueryInfo VALUES (3, 4) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()");
            }

            // INSERT ... VALUES (?, LAST_INSERT_ID()) --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (?, LAST_INSERT_ID())");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, LAST_INSERT_ID())",
                        "INSERT INTO testQueryInfo VALUES (?, LAST_INSERT_ID())");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, LAST_INSERT_ID())",
                        "INSERT INTO testQueryInfo VALUES (2, LAST_INSERT_ID())");
            }

            testConn.close();
        } while ((useSPS = !useSPS) || (rwBS = !rwBS));
    }

    @Test
    public void testQueryInfoParsingAndRewrittingInsertValuesRowEroteme() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 19), "MySQL 8.0.19+ is required to run this test.");

        createTable("testQueryInfo", "(c1 INT, c2 INT)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), QueryInfoQueryInterceptor.class.getName());
        props.setProperty(PropertyKey.continueBatchOnError.getKeyName(), "true");
        props.setProperty(PropertyKey.emulateUnsupportedPstmts.getKeyName(), "true");

        boolean rwBS = false;
        boolean useSPS = false;

        do {
            final String testCase = String.format("Case [rwBS: %s, useSPS: %s]", rwBS ? "Y" : "N", useSPS ? "Y" : "N");

            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString(rwBS));
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            Connection testConn = getConnectionWithProps(props);

            // INSERT ... VALUES ROW(?, ?) --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES ROW(?, ?)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 5);
            this.pstmt.setInt(2, 6);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(?, ?),ROW(?, ?),ROW(?, ?)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(?, ?)",
                        "INSERT INTO testQueryInfo VALUES ROW(?, ?)", "INSERT INTO testQueryInfo VALUES ROW(?, ?)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(1, 2),ROW(3, 4),ROW(5, 6)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(1, 2)",
                        "INSERT INTO testQueryInfo VALUES ROW(3, 4)", "INSERT INTO testQueryInfo VALUES ROW(5, 6)");
            }

            // INSERT ... VALUES ROW(?, ?) AS ... --> rewritable.
            // -- ROW(...) AS is not currently supported, so it fails preparing the statement and, thus, falls back to ClientPreparedStatement - Bug#33917022.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES ROW(?, ?) AS new(v1, v2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            assertThrows(BatchUpdateException.class, () -> this.pstmt.executeBatch()); // ROW(...) AS is not currently supported.
            // if (rwBS && useSPS) {
            //     QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(?, ?),ROW(?, ?) AS new(v1, v2)");
            // } else if (!rwBS && useSPS) {
            //     QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(?, ?) AS new(v1, v2)",
            //             "INSERT INTO testQueryInfo VALUES ROW(?, ?) AS new(v1, v2)");
            // } else
            if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(1, 2),ROW(3, 4) AS new(v1, v2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(1, 2) AS new(v1, v2)",
                        "INSERT INTO testQueryInfo VALUES ROW(3, 4) AS new(v1, v2)");
            }

            // INSERT ... VALUES ROW(?, ?) AS ... ON DUPLICATE KEY UPDATE ... VALUES() --> rewritable.
            // -- ROW(...) AS is not currently supported, so it fails preparing the statement and, thus, falls back to ClientPreparedStatement - Bug#33917022.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn
                    .prepareStatement("INSERT INTO testQueryInfo VALUES ROW(?, ?) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            assertThrows(BatchUpdateException.class, () -> this.pstmt.executeBatch()); // ROW(...) AS is not currently supported.
            // if (rwBS && useSPS) {
            //     QueryInfoQueryInterceptor.assertCapturedSql(testCase,
            //             "INSERT INTO testQueryInfo VALUES ROW(?, ?),ROW(?, ?) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            // } else if (!rwBS && useSPS) {
            //     QueryInfoQueryInterceptor.assertCapturedSql(testCase,
            //             "INSERT INTO testQueryInfo VALUES ROW(?, ?) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)",
            //             "INSERT INTO testQueryInfo VALUES ROW(?, ?) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            // } else
            if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES ROW(1, 2),ROW(3, 4) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES ROW(1, 2) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)",
                        "INSERT INTO testQueryInfo VALUES ROW(3, 4) AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            }

            // INSERT ... VALUES ROW(?, ?) ON DUPLICATE KEY UPDATE ... VALUES() --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES ROW(?, ?) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES ROW(?, ?),ROW(?, ?) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(?, ?) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo VALUES ROW(?, ?) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES ROW(1, 2),ROW(3, 4) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(1, 2) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo VALUES ROW(3, 4) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            }

            // INSERT ... VALUES ROW(?, m) ON DUPLICATE KEY UPDATE ... ? --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES ROW(?, 2) ON DUPLICATE KEY UPDATE c1 = ?");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(?, 2) ON DUPLICATE KEY UPDATE c1 = ?",
                        "INSERT INTO testQueryInfo VALUES ROW(?, 2) ON DUPLICATE KEY UPDATE c1 = ?");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(1, 2) ON DUPLICATE KEY UPDATE c1 = 2",
                        "INSERT INTO testQueryInfo VALUES ROW(3, 2) ON DUPLICATE KEY UPDATE c1 = 4");
            }

            // INSERT ... VALUES ROW(?, ?) ON DUPLICATE KEY UPDATE ... LAST_INSERT_ID() --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES ROW(?, ?) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES ROW(?, ?) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()",
                        "INSERT INTO testQueryInfo VALUES ROW(?, ?) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES ROW(1, 2) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()",
                        "INSERT INTO testQueryInfo VALUES ROW(3, 4) ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()");
            }

            // INSERT ... VALUES ROW(?, LAST_INSERT_ID()) --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES ROW(?, LAST_INSERT_ID())");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(?, LAST_INSERT_ID())",
                        "INSERT INTO testQueryInfo VALUES ROW(?, LAST_INSERT_ID())");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES ROW(1, LAST_INSERT_ID())",
                        "INSERT INTO testQueryInfo VALUES ROW(2, LAST_INSERT_ID())");
            }

            testConn.close();
        } while ((useSPS = !useSPS) || (rwBS = !rwBS));
    }

    @Test
    public void testQueryInfoParsingAndRewrittingInsertSetEroteme() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 19), "MySQL 8.0.19+ is required to run this test.");

        createTable("testQueryInfo", "(c1 INT, c2 INT)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), QueryInfoQueryInterceptor.class.getName());
        props.setProperty(PropertyKey.continueBatchOnError.getKeyName(), "true");
        props.setProperty(PropertyKey.emulateUnsupportedPstmts.getKeyName(), "true");

        boolean rwBS = false;
        boolean useSPS = false;

        do {
            final String testCase = String.format("Case [rwBS: %s, useSPS: %s]", rwBS ? "Y" : "N", useSPS ? "Y" : "N");

            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString(rwBS));
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            Connection testConn = getConnectionWithProps(props);

            // INSERT ... SET ?, ? --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo SET c1 = ?, c2 = ?");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 5);
            this.pstmt.setInt(2, 6);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo SET c1 = ?, c2 = ?",
                        "INSERT INTO testQueryInfo SET c1 = ?, c2 = ?", "INSERT INTO testQueryInfo SET c1 = ?, c2 = ?");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo SET c1 = 1, c2 = 2",
                        "INSERT INTO testQueryInfo SET c1 = 3, c2 = 4", "INSERT INTO testQueryInfo SET c1 = 5, c2 = 6");
            }

            // INSERT ... SET ?, ? AS ... --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo SET c1 = ?, c2 = ? AS new(v1, v2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo SET c1 = ?, c2 = ? AS new(v1, v2)",
                        "INSERT INTO testQueryInfo SET c1 = ?, c2 = ? AS new(v1, v2)");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo SET c1 = 1, c2 = 2 AS new(v1, v2)",
                        "INSERT INTO testQueryInfo SET c1 = 3, c2 = 4 AS new(v1, v2)");
            }

            // INSERT ... SET ?, ? AS ... ON DUPLICATE KEY UPDATE ... VALUES() --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn
                    .prepareStatement("INSERT INTO testQueryInfo SET c1 = ?, c2 = ? AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo SET c1 = ?, c2 = ? AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)",
                        "INSERT INTO testQueryInfo SET c1 = ?, c2 = ? AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo SET c1 = 1, c2 = 2 AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)",
                        "INSERT INTO testQueryInfo SET c1 = 3, c2 = 4 AS new(v1, v2) ON DUPLICATE KEY UPDATE c1 = new.v2, c2 = VALUES(c1)");
            }

            // INSERT ... SET ?, ? ON DUPLICATE KEY UPDATE ... VALUES() --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo SET c1 = ?, c2 = ? ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo SET c1 = ?, c2 = ? ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo SET c1 = ?, c2 = ? ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo SET c1 = 1, c2 = 2 ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo SET c1 = 3, c2 = 4 ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            }

            // INSERT ... SET ?, ? ON DUPLICATE KEY UPDATE ... ? --> not not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo SET c1 = ?, c2 = 2 ON DUPLICATE KEY UPDATE c1 = ?");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo SET c1 = ?, c2 = 2 ON DUPLICATE KEY UPDATE c1 = ?",
                        "INSERT INTO testQueryInfo SET c1 = ?, c2 = 2 ON DUPLICATE KEY UPDATE c1 = ?");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo SET c1 = 1, c2 = 2 ON DUPLICATE KEY UPDATE c1 = 2",
                        "INSERT INTO testQueryInfo SET c1 = 3, c2 = 2 ON DUPLICATE KEY UPDATE c1 = 4");
            }

            // INSERT ... SET ?, ? ON DUPLICATE KEY UPDATE ... LAST_INSERT_ID() --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo SET c1 = ?, c2 = ? ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo SET c1 = ?, c2 = ? ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()",
                        "INSERT INTO testQueryInfo SET c1 = ?, c2 = ? ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo SET c1 = 1, c2 = 2 ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()",
                        "INSERT INTO testQueryInfo SET c1 = 3, c2 = 4 ON DUPLICATE KEY UPDATE c1 = LAST_INSERT_ID()");
            }

            // INSERT ... SET ?, LAST_INSERT_ID() --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo SET c1 = ?, c2 = LAST_INSERT_ID()");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo SET c1 = ?, c2 = LAST_INSERT_ID()",
                        "INSERT INTO testQueryInfo SET c1 = ?, c2 = LAST_INSERT_ID()");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo SET c1 = 1, c2 = LAST_INSERT_ID()",
                        "INSERT INTO testQueryInfo SET c1 = 2, c2 = LAST_INSERT_ID()");
            }

            testConn.close();
        } while ((useSPS = !useSPS) || (rwBS = !rwBS));
    }

    @Test
    public void testQueryInfoParsingAndRewrittingReplaceVauesEroteme() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 19), "MySQL 8.0.19+ is required to run this test.");

        createTable("testQueryInfo", "(c1 INT, c2 INT)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), QueryInfoQueryInterceptor.class.getName());
        props.setProperty(PropertyKey.continueBatchOnError.getKeyName(), "true");
        props.setProperty(PropertyKey.emulateUnsupportedPstmts.getKeyName(), "true");

        boolean rwBS = false;
        boolean useSPS = false;

        do {
            final String testCase = String.format("Case [rwBS: %s, useSPS: %s]", rwBS ? "Y" : "N", useSPS ? "Y" : "N");

            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString(rwBS));
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            Connection testConn = getConnectionWithProps(props);

            // REPLACE ... VALUES (?, ?) --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("REPLACE INTO testQueryInfo VALUES (?, ?)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 5);
            this.pstmt.setInt(2, 6);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo VALUES (?, ?),(?, ?),(?, ?)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo VALUES (?, ?)", "REPLACE INTO testQueryInfo VALUES (?, ?)",
                        "REPLACE INTO testQueryInfo VALUES (?, ?)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo VALUES (1, 2),(3, 4),(5, 6)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo VALUES (1, 2)", "REPLACE INTO testQueryInfo VALUES (3, 4)",
                        "REPLACE INTO testQueryInfo VALUES (5, 6)");
            }

            // REPLACE ... VALUES (?, LAST_INSERT_ID()) --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("REPLACE INTO testQueryInfo VALUES (?, LAST_INSERT_ID())");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo VALUES (?, LAST_INSERT_ID())",
                        "REPLACE INTO testQueryInfo VALUES (?, LAST_INSERT_ID())");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo VALUES (1, LAST_INSERT_ID())",
                        "REPLACE INTO testQueryInfo VALUES (2, LAST_INSERT_ID())");
            }

            testConn.close();
        } while ((useSPS = !useSPS) || (rwBS = !rwBS));
    }

    @Test
    public void testQueryInfoParsingAndRewrittingReplaceValuesRowEroteme() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 19), "MySQL 8.0.19+ is required to run this test.");

        createTable("testQueryInfo", "(c1 INT, c2 INT)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), QueryInfoQueryInterceptor.class.getName());
        props.setProperty(PropertyKey.continueBatchOnError.getKeyName(), "true");
        props.setProperty(PropertyKey.emulateUnsupportedPstmts.getKeyName(), "true");

        boolean rwBS = false;
        boolean useSPS = false;

        do {
            final String testCase = String.format("Case [rwBS: %s, useSPS: %s]", rwBS ? "Y" : "N", useSPS ? "Y" : "N");

            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString(rwBS));
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            Connection testConn = getConnectionWithProps(props);

            // REPLACE ... VALUES ROW(?, ?) --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("REPLACE INTO testQueryInfo VALUES ROW(?, ?)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 5);
            this.pstmt.setInt(2, 6);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo VALUES ROW(?, ?),ROW(?, ?),ROW(?, ?)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo VALUES ROW(?, ?)",
                        "REPLACE INTO testQueryInfo VALUES ROW(?, ?)", "REPLACE INTO testQueryInfo VALUES ROW(?, ?)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo VALUES ROW(1, 2),ROW(3, 4),ROW(5, 6)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo VALUES ROW(1, 2)",
                        "REPLACE INTO testQueryInfo VALUES ROW(3, 4)", "REPLACE INTO testQueryInfo VALUES ROW(5, 6)");
            }

            // REPLACE ... VALUES ROW(?, LAST_INSERT_ID()) --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("REPLACE INTO testQueryInfo VALUES ROW(?, LAST_INSERT_ID())");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo VALUES ROW(?, LAST_INSERT_ID())",
                        "REPLACE INTO testQueryInfo VALUES ROW(?, LAST_INSERT_ID())");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo VALUES ROW(1, LAST_INSERT_ID())",
                        "REPLACE INTO testQueryInfo VALUES ROW(2, LAST_INSERT_ID())");
            }

            testConn.close();
        } while ((useSPS = !useSPS) || (rwBS = !rwBS));
    }

    @Test
    public void testQueryInfoParsingAndRewrittingReplaceSetEroteme() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 19), "MySQL 8.0.19+ is required to run this test.");

        createTable("testQueryInfo", "(c1 INT, c2 INT)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), QueryInfoQueryInterceptor.class.getName());
        props.setProperty(PropertyKey.continueBatchOnError.getKeyName(), "true");
        props.setProperty(PropertyKey.emulateUnsupportedPstmts.getKeyName(), "true");

        boolean rwBS = false;
        boolean useSPS = false;

        do {
            final String testCase = String.format("Case [rwBS: %s, useSPS: %s]", rwBS ? "Y" : "N", useSPS ? "Y" : "N");

            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString(rwBS));
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            Connection testConn = getConnectionWithProps(props);

            // REPLACE ... SET ?, ? --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("REPLACE INTO testQueryInfo SET c1 = ?, c2 = ?");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 5);
            this.pstmt.setInt(2, 6);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo SET c1 = ?, c2 = ?",
                        "REPLACE INTO testQueryInfo SET c1 = ?, c2 = ?", "REPLACE INTO testQueryInfo SET c1 = ?, c2 = ?");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo SET c1 = 1, c2 = 2",
                        "REPLACE INTO testQueryInfo SET c1 = 3, c2 = 4", "REPLACE INTO testQueryInfo SET c1 = 5, c2 = 6");
            }

            // REPLACE ... SET ?, LAST_INSERT_ID() --> not rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("REPLACE INTO testQueryInfo SET c1 = ?, c2 = LAST_INSERT_ID()");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (useSPS) { // && (rwBS || !rwBS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo SET c1 = ?, c2 = LAST_INSERT_ID()",
                        "REPLACE INTO testQueryInfo SET c1 = ?, c2 = LAST_INSERT_ID()");
            } else { // (rwBS || !rwBS) && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "REPLACE INTO testQueryInfo SET c1 = 1, c2 = LAST_INSERT_ID()",
                        "REPLACE INTO testQueryInfo SET c1 = 2, c2 = LAST_INSERT_ID()");
            }

            testConn.close();
        } while ((useSPS = !useSPS) || (rwBS = !rwBS));
    }

    @Test
    public void testQueryInfoParsingAndRewrittingSpecialCases() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 19), "MySQL 8.0.19+ is required to run this test.");

        createTable("testQueryInfo", "(c1 INT, c2 INT)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), QueryInfoQueryInterceptor.class.getName());
        props.setProperty(PropertyKey.continueBatchOnError.getKeyName(), "true");
        props.setProperty(PropertyKey.emulateUnsupportedPstmts.getKeyName(), "true");

        boolean rwBS = false;
        boolean useSPS = false;

        do {
            final String testCase = String.format("Case [rwBS: %s, useSPS: %s]", rwBS ? "Y" : "N", useSPS ? "Y" : "N");

            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString(rwBS));
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));

            Connection testConn = getConnectionWithProps(props);

            /*
             * Special Case 1: Parsing around VALUES.
             */

            // INSERT ... VALUES(?,?)AS ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES(?,?)AS new(v1, v2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES(?,?),(?,?)AS new(v1, v2)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES(?,?)AS new(v1, v2)",
                        "INSERT INTO testQueryInfo VALUES(?,?)AS new(v1, v2)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES(1,2),(3,4)AS new(v1, v2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES(1,2)AS new(v1, v2)",
                        "INSERT INTO testQueryInfo VALUES(3,4)AS new(v1, v2)");
            }

            // INSERT ... VALUES(?, ?)ON DUPLICATE KEY UPDATE ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES(?, ?)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES(?, ?),(?, ?)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES(?, ?)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo VALUES(?, ?)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES(1, 2),(3, 4)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES(1, 2)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo VALUES(3, 4)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            }

            /*
             * Special Case 2: VALUE clause & spaces around it.
             */

            // INSERT ... VALUE(?, ?) AS ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUE(?, ?) AS new(v1, v2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUE(?, ?),(?, ?) AS new(v1, v2)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUE(?, ?) AS new(v1, v2)",
                        "INSERT INTO testQueryInfo VALUE(?, ?) AS new(v1, v2)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUE(1, 2),(3, 4) AS new(v1, v2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUE(1, 2) AS new(v1, v2)",
                        "INSERT INTO testQueryInfo VALUE(3, 4) AS new(v1, v2)");
            }

            // INSERT ... VALUE (?, ?)ON DUPLICATE KEY UPDATE ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUE (?, ?)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUE (?, ?),(?, ?)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUE (?, ?)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo VALUE (?, ?)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUE (1, 2),(3, 4)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUE (1, 2)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo VALUE (3, 4)ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            }

            /*
             * Special Case 3: Table and column names.
             */

            // INSERT ... tbl (c1, c2) ... VALUES (?, ?) AS ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo (c1, c2) VALUES (?, ?) AS new(v1, v2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo (c1, c2) VALUES (?, ?),(?, ?) AS new(v1, v2)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo (c1, c2) VALUES (?, ?) AS new(v1, v2)",
                        "INSERT INTO testQueryInfo (c1, c2) VALUES (?, ?) AS new(v1, v2)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo (c1, c2) VALUES (1, 2),(3, 4) AS new(v1, v2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo (c1, c2) VALUES (1, 2) AS new(v1, v2)",
                        "INSERT INTO testQueryInfo (c1, c2) VALUES (3, 4) AS new(v1, v2)");
            }

            // INSERT ... tbl (c1, c2) ... VALUES (?, ?) ON DUPLICATE KEY UPDATE ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo (c1, c2) VALUES (?, ?) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo (c1, c2) VALUES (?, ?),(?, ?) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo (c1, c2) VALUES (?, ?) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo (c1, c2) VALUES (?, ?) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo (c1, c2) VALUES (1, 2),(3, 4) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo (c1, c2) VALUES (1, 2) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo (c1, c2) VALUES (3, 4) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            }

            /*
             * Special Case 4: Comments in place of spaces.
             */

            // /* */INSERT/* */.../* */VALUES/* */(?,/* */?)/* */AS/* */.../* */ON/* */DUPLICATE/* */KEY/* */UPDATE/* */.../* */VALUES() --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("/* DELETE */INSERT/* SELECT */INTO/* SHOW */testQueryInfo# testQueryInfo\n"
                    + "VALUES/* AS */(?,/* ON */?)/* LAST_INSERT_ID() */AS-- \nnew(v1,/**/v2)/* */ON/* AT */DUPLICATE# DUPLICATE\n"
                    + "KEY/* */UPDATE/* */c1/* = */=/* */new.v2,/* , , */c2/* VALUES */=/* VALUES */VALUES(c1)# the end");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "/* DELETE */INSERT/* SELECT */INTO/* SHOW */testQueryInfo# testQueryInfo\n"
                                + "VALUES/* AS */(?,/* ON */?),(?,/* ON */?)/* LAST_INSERT_ID() */AS-- \nnew(v1,/**/v2)/* */ON/* AT */DUPLICATE# DUPLICATE\n"
                                + "KEY/* */UPDATE/* */c1/* = */=/* */new.v2,/* , , */c2/* VALUES */=/* VALUES */VALUES(c1)# the end");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "/* DELETE */INSERT/* SELECT */INTO/* SHOW */testQueryInfo# testQueryInfo\n"
                                + "VALUES/* AS */(?,/* ON */?)/* LAST_INSERT_ID() */AS-- \nnew(v1,/**/v2)/* */ON/* AT */DUPLICATE# DUPLICATE\n"
                                + "KEY/* */UPDATE/* */c1/* = */=/* */new.v2,/* , , */c2/* VALUES */=/* VALUES */VALUES(c1)# the end",
                        "/* DELETE */INSERT/* SELECT */INTO/* SHOW */testQueryInfo# testQueryInfo\n"
                                + "VALUES/* AS */(?,/* ON */?)/* LAST_INSERT_ID() */AS-- \nnew(v1,/**/v2)/* */ON/* AT */DUPLICATE# DUPLICATE\n"
                                + "KEY/* */UPDATE/* */c1/* = */=/* */new.v2,/* , , */c2/* VALUES */=/* VALUES */VALUES(c1)# the end");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "/* DELETE */INSERT/* SELECT */INTO/* SHOW */testQueryInfo# testQueryInfo\n"
                                + "VALUES/* AS */(1,/* ON */2),(3,/* ON */4)/* LAST_INSERT_ID() */AS-- \nnew(v1,/**/v2)/* */ON/* AT */DUPLICATE# DUPLICATE\n"
                                + "KEY/* */UPDATE/* */c1/* = */=/* */new.v2,/* , , */c2/* VALUES */=/* VALUES */VALUES(c1)# the end");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "/* DELETE */INSERT/* SELECT */INTO/* SHOW */testQueryInfo# testQueryInfo\n"
                                + "VALUES/* AS */(1,/* ON */2)/* LAST_INSERT_ID() */AS-- \nnew(v1,/**/v2)/* */ON/* AT */DUPLICATE# DUPLICATE\n"
                                + "KEY/* */UPDATE/* */c1/* = */=/* */new.v2,/* , , */c2/* VALUES */=/* VALUES */VALUES(c1)# the end",
                        "/* DELETE */INSERT/* SELECT */INTO/* SHOW */testQueryInfo# testQueryInfo\n"
                                + "VALUES/* AS */(3,/* ON */4)/* LAST_INSERT_ID() */AS-- \nnew(v1,/**/v2)/* */ON/* AT */DUPLICATE# DUPLICATE\n"
                                + "KEY/* */UPDATE/* */c1/* = */=/* */new.v2,/* , , */c2/* VALUES */=/* VALUES */VALUES(c1)# the end");
            }

            /*
             * Special Case 5: Doubling eroteme - only works with ClientPreparedStatements.
             */

            // INSERT ... VALUES (??, ?) --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (??, ?)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.setInt(3, 3);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 4);
            this.pstmt.setInt(2, 5);
            this.pstmt.setInt(3, 6);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS) { // && (useSPS || !useSPS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (12, 3),(45, 6)");
            } else { // !rwBS && (useSPS || !useSPS)
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (12, 3)", "INSERT INTO testQueryInfo VALUES (45, 6)");
            }

            /*
             * Special Case 6: Multiple VALUES lists.
             */

            // INSERT ... VALUES (?, n), (?, m) --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (?, 0), (?, 1)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, 0), (?, 1),(?, 0), (?, 1)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, 0), (?, 1)",
                        "INSERT INTO testQueryInfo VALUES (?, 0), (?, 1)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 0), (2, 1),(3, 0), (4, 1)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 0), (2, 1)",
                        "INSERT INTO testQueryInfo VALUES (3, 0), (4, 1)");
            }

            // INSERT ... VALUES (?, n), (?, m) AS ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (?, 0), (?, 1) AS new(v1, v2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, 0), (?, 1),(?, 0), (?, 1) AS new(v1, v2)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, 0), (?, 1) AS new(v1, v2)",
                        "INSERT INTO testQueryInfo VALUES (?, 0), (?, 1) AS new(v1, v2)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 0), (2, 1),(3, 0), (4, 1) AS new(v1, v2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 0), (2, 1) AS new(v1, v2)",
                        "INSERT INTO testQueryInfo VALUES (3, 0), (4, 1) AS new(v1, v2)");
            }

            // INSERT ... VALUES (?, n), (?, m) ON DUPLICATE KEY UPDATE ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO testQueryInfo VALUES (?, 0), (?, 1) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            this.pstmt.setInt(1, 1);
            this.pstmt.setInt(2, 2);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 3);
            this.pstmt.setInt(2, 4);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES (?, 0), (?, 1),(?, 0), (?, 1) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (?, 0), (?, 1) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo VALUES (?, 0), (?, 1) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase,
                        "INSERT INTO testQueryInfo VALUES (1, 0), (2, 1),(3, 0), (4, 1) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO testQueryInfo VALUES (1, 0), (2, 1) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)",
                        "INSERT INTO testQueryInfo VALUES (3, 0), (4, 1) ON DUPLICATE KEY UPDATE c1 = VALUES(c2)");
            }

            /*
             * Special Case 7: "VALUE" as table and column name.
             */

            createTable("value", "(value INT)");

            // INSERT ... VALUE (?) --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO value (value) VALUE (?)");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (value) VALUE (?),(?)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (value) VALUE (?)", "INSERT INTO value (value) VALUE (?)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (value) VALUE (1),(2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (value) VALUE (1)", "INSERT INTO value (value) VALUE (2)");
            }

            // INSERT ... VALUE (?) AS ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO value (value) VALUE (?) AS new(v)");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (value) VALUE (?),(?) AS new(v)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (value) VALUE (?) AS new(v)",
                        "INSERT INTO value (value) VALUE (?) AS new(v)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (value) VALUE (1),(2) AS new(v)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (value) VALUE (1) AS new(v)",
                        "INSERT INTO value (value) VALUE (2) AS new(v)");
            }

            // INSERT ... VALUE (?) ON DUPLICATE KEY UPDATE ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO value (value) VALUE (?) ON DUPLICATE KEY UPDATE value = VALUES(value)");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (value) VALUE (?),(?) ON DUPLICATE KEY UPDATE value = VALUES(value)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (value) VALUE (?) ON DUPLICATE KEY UPDATE value = VALUES(value)",
                        "INSERT INTO value (value) VALUE (?) ON DUPLICATE KEY UPDATE value = VALUES(value)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (value) VALUE (1),(2) ON DUPLICATE KEY UPDATE value = VALUES(value)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (value) VALUE (1) ON DUPLICATE KEY UPDATE value = VALUES(value)",
                        "INSERT INTO value (value) VALUE (2) ON DUPLICATE KEY UPDATE value = VALUES(value)");
            }

            /*
             * Special Case 8: "VALUE" as table name and "?" as column name.
             */

            createTable("value", "(`?` INT)");

            // INSERT ... VALUE (?) --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO value (`?`) VALUE (?)");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (`?`) VALUE (?),(?)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (`?`) VALUE (?)", "INSERT INTO value (`?`) VALUE (?)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (`?`) VALUE (1),(2)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (`?`) VALUE (1)", "INSERT INTO value (`?`) VALUE (2)");
            }

            // INSERT ... VALUE (?) AS ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO value (`?`) VALUE (?) AS `values`(`?`)");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (`?`) VALUE (?),(?) AS `values`(`?`)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (`?`) VALUE (?) AS `values`(`?`)",
                        "INSERT INTO value (`?`) VALUE (?) AS `values`(`?`)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (`?`) VALUE (1),(2) AS `values`(`?`)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (`?`) VALUE (1) AS `values`(`?`)",
                        "INSERT INTO value (`?`) VALUE (2) AS `values`(`?`)");
            }

            // INSERT ... VALUE (?) ON DUPLICATE KEY UPDATE ... --> rewritable.
            QueryInfoQueryInterceptor.startCapturing();
            this.pstmt = testConn.prepareStatement("INSERT INTO value (`?`) VALUE (?) ON DUPLICATE KEY UPDATE `?` = VALUES(`?`)");
            this.pstmt.setInt(1, 1);
            this.pstmt.addBatch();
            this.pstmt.setInt(1, 2);
            this.pstmt.addBatch();
            this.pstmt.executeBatch();
            if (rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (`?`) VALUE (?),(?) ON DUPLICATE KEY UPDATE `?` = VALUES(`?`)");
            } else if (!rwBS && useSPS) {
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (`?`) VALUE (?) ON DUPLICATE KEY UPDATE `?` = VALUES(`?`)",
                        "INSERT INTO value (`?`) VALUE (?) ON DUPLICATE KEY UPDATE `?` = VALUES(`?`)");
            } else if (rwBS) { // && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (`?`) VALUE (1),(2) ON DUPLICATE KEY UPDATE `?` = VALUES(`?`)");
            } else { // !rwBS && !useSPS
                QueryInfoQueryInterceptor.assertCapturedSql(testCase, "INSERT INTO value (`?`) VALUE (1) ON DUPLICATE KEY UPDATE `?` = VALUES(`?`)",
                        "INSERT INTO value (`?`) VALUE (2) ON DUPLICATE KEY UPDATE `?` = VALUES(`?`)");
            }

            testConn.close();
        } while ((useSPS = !useSPS) || (rwBS = !rwBS));
    }

    public static class QueryInfoQueryInterceptor extends BaseQueryInterceptor {

        private static boolean enabled = false;
        private static List<String> capturedSql = new ArrayList<>();

        public static void startCapturing() {
            enabled = true;
            capturedSql.clear();
        }

        public static void assertCapturedSql(String testCase, String... expectedSql) {
            enabled = false;
            assertEquals(expectedSql.length, capturedSql.size(), testCase);
            for (int i = 0; i < expectedSql.length; i++) {
                assertEquals(expectedSql[i], capturedSql.get(i), testCase);
            }
        }

        @Override
        public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
            if (enabled && interceptedQuery != null) {
                capturedSql.add(sql.get());
            }
            return super.preProcess(sql, interceptedQuery);
        }

    }

}
