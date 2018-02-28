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

package testsuite.simple;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Properties;

import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.log.StandardLogger;

import testsuite.BaseTestCase;

/**
 * Tests callable statement functionality.
 */
public class CallableStatementTest extends BaseTestCase {
    public CallableStatementTest(String name) {
        super(name);
    }

    /**
     * Tests functioning of inout parameters
     * 
     * @throws Exception
     *             if the test fails
     */

    public void testInOutParams() throws Exception {
        CallableStatement storedProc = null;

        createProcedure("testInOutParam",
                "(IN p1 VARCHAR(255), INOUT p2 INT)\nbegin\n DECLARE z INT;\nSET z = p2 + 1;\nSET p2 = z;\n" + "SELECT p1;\nSELECT CONCAT('zyxw', p1);\nend\n");

        storedProc = this.conn.prepareCall("{call testInOutParam(?, ?)}");

        storedProc.setString(1, "abcd");
        storedProc.setInt(2, 4);
        storedProc.registerOutParameter(2, Types.INTEGER);

        storedProc.execute();

        assertEquals(5, storedProc.getInt(2));
    }

    public void testBatch() throws Exception {
        Connection batchedConn = null;

        try {
            createTable("testBatchTable", "(field1 INT)");
            createProcedure("testBatch", "(IN foo VARCHAR(15))\nbegin\nINSERT INTO testBatchTable VALUES (foo);\nend\n");

            executeBatchedStoredProc(this.conn);

            batchedConn = getConnectionWithProps("logger=StandardLogger,rewriteBatchedStatements=true,profileSQL=true");

            StandardLogger.startLoggingToBuffer();
            executeBatchedStoredProc(batchedConn);
            String[] log = StandardLogger.getBuffer().toString().split(";");
            assertTrue(log.length > 20);
        } finally {
            StandardLogger.dropBuffer();

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
     *             if the test fails.
     */
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

        assertTrue("Named and indexed parameter are not the same", indexedOutParamToTest == namedOutParamToTest);
        assertTrue("Output value not returned correctly", indexedOutParamToTest == 6);

        // Start over, using named parameters, this time
        storedProc.clearParameters();
        storedProc.setInt("x", 32);
        storedProc.registerOutParameter("y", Types.INTEGER);

        storedProc.execute();

        indexedOutParamToTest = storedProc.getInt(2);
        namedOutParamToTest = storedProc.getInt("y");

        assertTrue("Named and indexed parameter are not the same", indexedOutParamToTest == namedOutParamToTest);
        assertTrue("Output value not returned correctly", indexedOutParamToTest == 33);

        try {
            storedProc.registerOutParameter("x", Types.INTEGER);
            assertTrue("Should not be able to register an out parameter on a non-out parameter", true);
        } catch (SQLException sqlEx) {
            if (!MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState())) {
                throw sqlEx;
            }
        }

        try {
            storedProc.getInt("x");
            assertTrue("Should not be able to retreive an out parameter on a non-out parameter", true);
        } catch (SQLException sqlEx) {
            if (!MysqlErrorNumbers.SQL_STATE_COLUMN_NOT_FOUND.equals(sqlEx.getSQLState())) {
                throw sqlEx;
            }
        }

        try {
            storedProc.registerOutParameter(1, Types.INTEGER);
            assertTrue("Should not be able to register an out parameter on a non-out parameter", true);
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
     *             if the test fails.
     */
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

        // TODO: This does not yet work in MySQL 5.0
        // assertTrue(!storedProc.getMoreResults());
        // assertTrue(storedProc.getUpdateCount() == 2);
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
     *             if an error occurs.
     */
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
     *             if an error occurs.
     */
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
     *             if an error occurs.
     */
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
        props.setProperty(PropertyDefinitions.PNAME_cacheCallableStmts, "true");

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

    public void testOutParamsNoBodies() throws Exception {
        CallableStatement storedProc = null;

        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_noAccessToProcedureBodies, "true");

        Connection spConn = getConnectionWithProps(props);

        createProcedure("testOutParam", "(x int, out y int)\nbegin\ndeclare z int;\nset z = x+1, y = z;\nend\n");

        storedProc = spConn.prepareCall("{call testOutParam(?, ?)}");

        storedProc.setInt(1, 5);
        storedProc.registerOutParameter(2, Types.INTEGER);

        storedProc.execute();

        int indexedOutParamToTest = storedProc.getInt(2);

        assertTrue("Output value not returned correctly", indexedOutParamToTest == 6);

        storedProc.clearParameters();
        storedProc.setInt(1, 32);
        storedProc.registerOutParameter(2, Types.INTEGER);

        storedProc.execute();

        indexedOutParamToTest = storedProc.getInt(2);

        assertTrue("Output value not returned correctly", indexedOutParamToTest == 33);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(CallableStatementTest.class);
    }

    /**
     * Tests the new parameter parser that doesn't require "BEGIN" or "\n" at
     * end of parameter declaration
     * 
     * @throws Exception
     */
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
}
