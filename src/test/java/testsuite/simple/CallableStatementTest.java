/*
 * Copyright (c) 2002, 2023, Oracle and/or its affiliates.
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Types;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.CallableStatementWrapper;
import com.mysql.cj.jdbc.ConnectionWrapper;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.MysqlPooledConnection;

import testsuite.BaseTestCase;
import testsuite.BufferingLogger;

/**
 * Tests callable statement functionality.
 */
public class CallableStatementTest extends BaseTestCase {

    /**
     * Tests functioning of inout parameters
     *
     * @throws Exception
     */
    @Test
    public void testInOutParams() throws Exception {
        CallableStatement storedProc = null;

        createProcedure("testInOutParam",
                "(IN p1 VARCHAR(255), INOUT p2 INT)\nbegin\n DECLARE z INT;\nSET z = p2 + 1;\nSET p2 = z;\n" + "SELECT p1;\nSELECT CONCAT('zyxw', p1);\nend\n");
        createProcedure("testInOutParam2", "(IN p1 VARCHAR(255), INOUT p2 VARCHAR(20))\nbegin\nSET p2=CONCAT(p1, p2);\nend\n");

        storedProc = this.conn.prepareCall("{call testInOutParam(?, ?)}");
        storedProc.setString(1, "abcd");
        storedProc.setInt(2, 4);
        storedProc.registerOutParameter(2, Types.INTEGER);
        storedProc.execute();
        assertEquals(5, storedProc.getInt(2));
        storedProc.close();

        storedProc = this.conn.prepareCall("{call testInOutParam2(?, ?)}");
        storedProc.setString(1, "1");
        storedProc.setBytes(2, "2".getBytes());
        storedProc.registerOutParameter(2, Types.VARCHAR);
        storedProc.execute();
        assertEquals("12", storedProc.getString(2));
        storedProc.close();
    }

    @Test
    public void testBatch() throws Exception {
        Connection batchedConn = null;

        try {
            createTable("testBatchTable", "(field1 INT)");
            createProcedure("testBatch", "(IN foo VARCHAR(15))\nbegin\nINSERT INTO testBatchTable VALUES (foo);\nend\n");

            executeBatchedStoredProc(this.conn);

            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.logger.getKeyName(), BufferingLogger.class.getName());
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
            props.setProperty(PropertyKey.profileSQL.getKeyName(), "true");
            batchedConn = getConnectionWithProps(props);

            BufferingLogger.startLoggingToBuffer();
            executeBatchedStoredProc(batchedConn);
            String[] log = BufferingLogger.getBuffer().toString().split(";");
            assertTrue(log.length > 20);
        } finally {
            BufferingLogger.dropBuffer();

            if (batchedConn != null) {
                batchedConn.close();
            }
        }
    }

    private void executeBatchedStoredProc(Connection c) throws Exception {
        this.stmt.executeUpdate("TRUNCATE TABLE testBatchTable");

        CallableStatement storedProc = c.prepareCall("{call testBatch(?)}");

        try {
            int numBatches = 300;

            for (int i = 0; i < numBatches; i++) {
                storedProc.setInt(1, i + 1);
                storedProc.addBatch();
            }

            int[] counts = storedProc.executeBatch();

            assertEquals(numBatches, counts.length);

            for (int i = 0; i < numBatches; i++) {
                assertEquals(1, counts[i]);
            }

            this.rs = this.stmt.executeQuery("SELECT field1 FROM testBatchTable ORDER BY field1 ASC");

            for (int i = 0; i < numBatches; i++) {
                assertTrue(this.rs.next());
                assertEquals(i + 1, this.rs.getInt(1));
            }
        } finally {

            if (storedProc != null) {
                storedProc.close();
            }
        }
    }

    /**
     * Tests functioning of output parameters.
     *
     * @throws Exception
     */
    @Test
    public void testOutParams() throws Exception {
        CallableStatement storedProc = null;

        createProcedure("testOutParam", "(x int, out y int)\nbegin\ndeclare z int;\nset z = x+1, y = z;\nend\n");

        storedProc = this.conn.prepareCall("{call testOutParam(?, ?)}");

        storedProc.setInt(1, 5);
        storedProc.registerOutParameter(2, Types.INTEGER);

        storedProc.execute();

        System.out.println(storedProc);

        int indexedOutParamToTest = storedProc.getInt(2);

        int namedOutParamToTest = storedProc.getInt("y");

        assertTrue(indexedOutParamToTest == namedOutParamToTest, "Named and indexed parameter are not the same");
        assertTrue(indexedOutParamToTest == 6, "Output value not returned correctly");

        // Start over, using named parameters, this time
        storedProc.clearParameters();
        storedProc.setInt("x", 32);
        storedProc.registerOutParameter("y", Types.INTEGER);

        storedProc.execute();

        indexedOutParamToTest = storedProc.getInt(2);
        namedOutParamToTest = storedProc.getInt("y");

        assertTrue(indexedOutParamToTest == namedOutParamToTest, "Named and indexed parameter are not the same");
        assertTrue(indexedOutParamToTest == 33, "Output value not returned correctly");

        try {
            storedProc.registerOutParameter("x", Types.INTEGER);
            assertTrue(true, "Should not be able to register an out parameter on a non-out parameter");
        } catch (SQLException sqlEx) {
            if (!MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState())) {
                throw sqlEx;
            }
        }

        try {
            storedProc.getInt("x");
            assertTrue(true, "Should not be able to retreive an out parameter on a non-out parameter");
        } catch (SQLException sqlEx) {
            if (!MysqlErrorNumbers.SQL_STATE_COLUMN_NOT_FOUND.equals(sqlEx.getSQLState())) {
                throw sqlEx;
            }
        }

        try {
            storedProc.registerOutParameter(1, Types.INTEGER);
            assertTrue(true, "Should not be able to register an out parameter on a non-out parameter");
        } catch (SQLException sqlEx) {
            if (!MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState())) {
                throw sqlEx;
            }
        }
    }

    /**
     * Tests functioning of output parameters.
     *
     * @throws Exception
     */
    @Test
    public void testResultSet() throws Exception {
        CallableStatement storedProc = null;

        createTable("testSpResultTbl1", "(field1 INT)");
        this.stmt.executeUpdate("INSERT INTO testSpResultTbl1 VALUES (1), (2)");
        createTable("testSpResultTbl2", "(field2 varchar(255))");
        this.stmt.executeUpdate("INSERT INTO testSpResultTbl2 VALUES ('abc'), ('def')");

        createProcedure("testSpResult", "()\nBEGIN\nSELECT field2 FROM testSpResultTbl2 WHERE field2='abc';\n"
                + "UPDATE testSpResultTbl1 SET field1=2;\nSELECT field2 FROM testSpResultTbl2 WHERE field2='def';\nend\n");

        storedProc = this.conn.prepareCall("{call testSpResult()}");

        storedProc.execute();

        this.rs = storedProc.getResultSet();

        ResultSetMetaData rsmd = this.rs.getMetaData();

        assertTrue(rsmd.getColumnCount() == 1);
        assertTrue("field2".equals(rsmd.getColumnName(1)));
        assertTrue(rsmd.getColumnType(1) == Types.VARCHAR);

        assertTrue(this.rs.next());

        assertTrue("abc".equals(this.rs.getString(1)));

        assertTrue(storedProc.getMoreResults());

        ResultSet nextResultSet = storedProc.getResultSet();

        rsmd = nextResultSet.getMetaData();

        assertTrue(rsmd.getColumnCount() == 1);
        assertTrue("field2".equals(rsmd.getColumnName(1)));
        assertTrue(rsmd.getColumnType(1) == Types.VARCHAR);

        assertTrue(nextResultSet.next());

        assertTrue("def".equals(nextResultSet.getString(1)));

        nextResultSet.close();

        this.rs.close();

        storedProc.execute();
    }

    /**
     * Tests parsing of stored procedures
     *
     * @throws Exception
     */
    @Test
    public void testSPParse() throws Exception {
        CallableStatement storedProc = null;

        createProcedure("testSpParse", "(IN FOO VARCHAR(15))\nBEGIN\nSELECT 1;\nend\n");

        storedProc = this.conn.prepareCall("{call testSpParse()}");
        storedProc.close();
    }

    /**
     * Tests parsing/execution of stored procedures with no parameters...
     *
     * @throws Exception
     */
    @Test
    public void testSPNoParams() throws Exception {
        CallableStatement storedProc = null;

        createProcedure("testSPNoParams", "()\nBEGIN\nSELECT 1;\nend\n");

        storedProc = this.conn.prepareCall("{call testSPNoParams()}");
        storedProc.execute();
    }

    /**
     * Tests parsing of stored procedures
     *
     * @throws Exception
     */
    @Test
    public void testSPCache() throws Exception {
        CallableStatement storedProc = null;

        createProcedure("testSpParse", "(IN FOO VARCHAR(15))\nBEGIN\nSELECT 1;\nend\n");

        int numIterations = 10;

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < numIterations; i++) {
            storedProc = this.conn.prepareCall("{call testSpParse(?)}");
            storedProc.close();
        }

        long elapsedTime = System.currentTimeMillis() - startTime;

        System.out.println("Standard parsing/execution: " + elapsedTime + " ms");

        storedProc = this.conn.prepareCall("{call testSpParse(?)}");
        storedProc.setString(1, "abc");
        this.rs = storedProc.executeQuery();

        assertTrue(this.rs.next());
        assertTrue(this.rs.getInt(1) == 1);

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheCallableStmts.getKeyName(), "true");

        Connection cachedSpConn = getConnectionWithProps(props);

        startTime = System.currentTimeMillis();

        for (int i = 0; i < numIterations; i++) {
            storedProc = cachedSpConn.prepareCall("{call testSpParse(?)}");
            storedProc.close();
        }

        elapsedTime = System.currentTimeMillis() - startTime;

        System.out.println("Cached parse stage: " + elapsedTime + " ms");

        storedProc = cachedSpConn.prepareCall("{call testSpParse(?)}");
        storedProc.setString(1, "abc");
        this.rs = storedProc.executeQuery();

        assertTrue(this.rs.next());
        assertTrue(this.rs.getInt(1) == 1);
    }

    @Test
    public void testOutParamsNoBodies() throws Exception {
        CallableStatement storedProc = null;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.noAccessToProcedureBodies.getKeyName(), "true");

        Connection spConn = getConnectionWithProps(props);

        createProcedure("testOutParam", "(x int, out y int)\nbegin\ndeclare z int;\nset z = x+1, y = z;\nend\n");

        storedProc = spConn.prepareCall("{call testOutParam(?, ?)}");

        storedProc.setInt(1, 5);
        storedProc.registerOutParameter(2, Types.INTEGER);

        storedProc.execute();

        int indexedOutParamToTest = storedProc.getInt(2);

        assertTrue(indexedOutParamToTest == 6, "Output value not returned correctly");

        storedProc.clearParameters();
        storedProc.setInt(1, 32);
        storedProc.registerOutParameter(2, Types.INTEGER);

        storedProc.execute();

        indexedOutParamToTest = storedProc.getInt(2);

        assertTrue(indexedOutParamToTest == 33, "Output value not returned correctly");
    }

    /**
     * Tests the new parameter parser that doesn't require "BEGIN" or "\n" at
     * end of parameter declaration
     *
     * @throws Exception
     */
    @Test
    public void testParameterParser() throws Exception {
        CallableStatement cstmt = null;

        try {

            createTable("t1", "(id   char(16) not null default '', data int not null)");
            createTable("t2", "(s   char(16),  i   int,  d   double)");

            createProcedure("foo42", "() insert into test.t1 values ('foo', 42);");
            this.conn.prepareCall("{CALL foo42()}");
            this.conn.prepareCall("{CALL foo42}");

            createProcedure("bar", "(x char(16), y int, z DECIMAL(10)) insert into test.t1 values (x, y);");
            cstmt = this.conn.prepareCall("{CALL bar(?, ?, ?)}");

            ParameterMetaData md = cstmt.getParameterMetaData();
            assertEquals(3, md.getParameterCount());
            assertEquals(Types.CHAR, md.getParameterType(1));
            assertEquals(Types.INTEGER, md.getParameterType(2));
            assertEquals(Types.DECIMAL, md.getParameterType(3));

            cstmt.close();

            createProcedure("p", "() label1: WHILE @a=0 DO SET @a=1; END WHILE");
            this.conn.prepareCall("{CALL p()}");

            createFunction("f", "() RETURNS INT NO SQL return 1; ");
            cstmt = this.conn.prepareCall("{? = CALL f()}");

            md = cstmt.getParameterMetaData();
            assertEquals(Types.INTEGER, md.getParameterType(1));
        } finally {
            if (cstmt != null) {
                cstmt.close();
            }
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testClosedWrapper() throws Exception {
        String sql = "SELECT 1";
        int autoGeneratedKeys = 0;
        int[] columnIndexes = new int[] { 0 };
        String[] columnNames = new String[] { "f1" };
        int parameterIndex = 1;
        String parameterName = "p1";
        Calendar cal = new GregorianCalendar();

        Map<String, Class<?>> typeMap = new HashMap<>();
        typeMap.put("1", String.class);

        int scale = 3;
        String typeName = String.class.getName();
        InputStream istr = new InputStream() {

            @Override
            public int read() throws IOException {
                return 0;
            }

        };
        Reader reader = new StringReader(sql);

        MysqlPooledConnection con = new MysqlPooledConnection((JdbcConnection) this.conn);
        CallableStatementWrapper w = new CallableStatementWrapper(new ConnectionWrapper(con, (JdbcConnection) this.conn, false), con, null);

        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.addBatch();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.addBatch(sql);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.cancel();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.clearBatch();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.clearParameters();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.clearWarnings();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.closeOnCompletion();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.enableStreamingResults();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.execute();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.execute("SELECT 1");
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.execute(sql, autoGeneratedKeys);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.execute(sql, columnIndexes);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.execute(sql, columnNames);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeBatch();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeLargeBatch();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeLargeUpdate();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeLargeUpdate(sql);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeLargeUpdate(sql, autoGeneratedKeys);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeLargeUpdate(sql, columnIndexes);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeLargeUpdate(sql, columnNames);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeQuery();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeQuery(sql);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeUpdate();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeUpdate(sql);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeUpdate(sql, autoGeneratedKeys);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeUpdate(sql, columnIndexes);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.executeUpdate(sql, columnNames);
            return null;
        });

        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getArray(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getArray(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getBigDecimal(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getBigDecimal(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getBigDecimal(parameterIndex, 10);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getBlob(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getBlob(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getBoolean(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getBoolean(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getByte(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getByte(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getBytes(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getCharacterStream(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getCharacterStream(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getClob(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getClob(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getDate(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getDate(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getDate(parameterIndex, cal);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getDate(parameterName, cal);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getDouble(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getDouble(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getFloat(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getFloat(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getInt(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getInt(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getLong(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getLong(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getNCharacterStream(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getNCharacterStream(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getNClob(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getNClob(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getNString(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getNString(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getObject(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getObject(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getObject(parameterIndex, String.class);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getObject(parameterName, String.class);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getObject(parameterIndex, typeMap);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getObject(parameterName, typeMap);
            return null;
        });

        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getParameterMetaData();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getQueryTimeout();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getRef(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getRef(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getResultSet();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getResultSetConcurrency();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getResultSetHoldability();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getResultSetType();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getRowId(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getRowId(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getShort(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getShort(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getSQLXML(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getSQLXML(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getString(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getString(parameterName);
            return null;
        });

        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getTime(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getTime(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getTime(parameterIndex, cal);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getTime(parameterName, cal);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getTimestamp(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getTimestamp(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getTimestamp(parameterIndex, cal);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getTimestamp(parameterName, cal);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getUpdateCount();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getURL(parameterIndex);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getURL(parameterName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getWarnings();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getConnection();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getFetchDirection();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getFetchSize();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getGeneratedKeys();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getLargeMaxRows();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getLargeUpdateCount();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getMaxFieldSize();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getMaxRows();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getMetaData();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getMoreResults();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.getMoreResults(0);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.isClosed();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.isCloseOnCompletion();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.isPoolable();
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.registerOutParameter(parameterIndex, Types.VARCHAR);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.registerOutParameter(parameterIndex, MysqlType.VARCHAR);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.registerOutParameter(parameterName, Types.VARCHAR);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.registerOutParameter(parameterName, MysqlType.VARCHAR);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.registerOutParameter(parameterIndex, Types.VARCHAR, scale);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.registerOutParameter(parameterIndex, MysqlType.VARCHAR, scale);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.registerOutParameter(parameterIndex, Types.VARCHAR, typeName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.registerOutParameter(parameterIndex, MysqlType.VARCHAR, typeName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.registerOutParameter(parameterName, Types.VARCHAR, scale);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.registerOutParameter(parameterName, Types.VARCHAR, typeName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.registerOutParameter(parameterName, MysqlType.VARCHAR, scale);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.registerOutParameter(parameterName, MysqlType.VARCHAR, typeName);
            return null;
        });

        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setArray(parameterIndex, (java.sql.Array) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setAsciiStream(parameterIndex, istr);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setAsciiStream(parameterName, istr);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setAsciiStream(parameterIndex, istr, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setAsciiStream(parameterIndex, istr, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setAsciiStream(parameterName, istr, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setAsciiStream(parameterName, istr, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBigDecimal(parameterIndex, BigDecimal.valueOf(1L));
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBigDecimal(parameterName, BigDecimal.valueOf(1L));
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBinaryStream(parameterIndex, istr);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBinaryStream(parameterName, istr);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBinaryStream(parameterIndex, istr, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBinaryStream(parameterIndex, istr, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBinaryStream(parameterName, istr, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBinaryStream(parameterName, istr, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBlob(parameterIndex, (Blob) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBlob(parameterIndex, istr);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBlob(parameterName, (Blob) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBlob(parameterName, istr);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBlob(parameterIndex, istr, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBlob(parameterName, istr, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBoolean(parameterIndex, true);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBoolean(parameterName, true);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setByte(parameterIndex, (byte) 0);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setByte(parameterName, (byte) 0);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBytes(parameterIndex, (byte[]) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setBytes(parameterName, (byte[]) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setCharacterStream(parameterIndex, reader);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setCharacterStream(parameterName, reader);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setCharacterStream(parameterIndex, reader, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setCharacterStream(parameterIndex, reader, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setCharacterStream(parameterName, reader, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setCharacterStream(parameterName, reader, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setClob(parameterIndex, (Clob) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setClob(parameterIndex, reader);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setClob(parameterName, (Clob) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setClob(parameterName, reader);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setClob(parameterIndex, reader, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setClob(parameterName, reader, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setCursorName("qqq");
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setDate(parameterIndex, (Date) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setDate(parameterName, (Date) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setDate(parameterIndex, (Date) null, cal);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setDate(parameterName, (Date) null, cal);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setDouble(parameterIndex, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setDouble(parameterName, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setEscapeProcessing(true);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setFetchDirection(ResultSet.FETCH_FORWARD);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setFetchSize(1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setFloat(parameterIndex, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setFloat(parameterName, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setInt(parameterIndex, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setInt(parameterName, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setLargeMaxRows(1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setLong(parameterIndex, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setLong(parameterName, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setMaxFieldSize(1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setMaxRows(1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNCharacterStream(parameterIndex, reader);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNCharacterStream(parameterName, reader);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNCharacterStream(parameterIndex, reader, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNCharacterStream(parameterName, reader, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNClob(parameterIndex, (NClob) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNClob(parameterIndex, reader);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNClob(parameterName, (NClob) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNClob(parameterName, reader);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNClob(parameterIndex, reader, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNClob(parameterName, reader, 1L);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNString(parameterIndex, "");
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNString(parameterName, "");
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNull(parameterIndex, Types.VARCHAR);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNull(parameterName, Types.VARCHAR);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNull(parameterIndex, Types.VARCHAR, typeName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setNull(parameterName, Types.VARCHAR, typeName);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setObject(parameterIndex, (Object) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setObject(parameterName, (Object) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setObject(parameterIndex, (Object) null, Types.VARCHAR);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setObject(parameterIndex, (Object) null, MysqlType.VARCHAR);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setObject(parameterName, (Object) null, Types.VARCHAR);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setObject(parameterName, (Object) null, MysqlType.VARCHAR);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setObject(parameterIndex, (Object) null, Types.VARCHAR, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setObject(parameterIndex, (Object) null, MysqlType.VARCHAR, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setObject(parameterName, (Object) null, Types.VARCHAR, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setObject(parameterName, (Object) null, MysqlType.VARCHAR, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setPoolable(true);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setQueryTimeout(5);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setRef(parameterIndex, (Ref) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setRowId(parameterIndex, (RowId) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setRowId(parameterName, (RowId) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setShort(parameterIndex, (short) 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setShort(parameterName, (short) 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setSQLXML(parameterIndex, (SQLXML) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setSQLXML(parameterName, (SQLXML) null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setString(parameterIndex, "");
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setString(parameterName, "");
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setTime(parameterIndex, null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setTime(parameterName, null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setTime(parameterIndex, null, cal);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setTime(parameterName, null, cal);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setTimestamp(parameterIndex, null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setTimestamp(parameterName, null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setTimestamp(parameterIndex, null, cal);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setTimestamp(parameterName, null, cal);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setUnicodeStream(parameterIndex, istr, 1);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setURL(parameterIndex, null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.setURL(parameterName, null);
            return null;
        });
        assertThrows(SQLException.class, "No operations allowed after statement closed.", () -> {
            w.wasNull();
            return null;
        });

        w.close();
    }

}
