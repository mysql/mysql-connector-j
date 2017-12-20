/*
  Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.SQLError;

import testsuite.BaseTestCase;

/**
 * Tests fixes for bugs in CallableStatement code.
 */
public class CallableStatementRegressionTest extends BaseTestCase {
    public CallableStatementRegressionTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     *            ignored
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(CallableStatementRegressionTest.class);
    }

    /**
     * Tests fix for BUG#3539 getProcedures() does not return any procedures in
     * result set
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testBug3539() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createProcedure("testBug3539", "()\nBEGIN\nSELECT 1;end\n");

        this.rs = this.conn.getMetaData().getProcedures(null, null, "testBug3539");

        assertTrue(this.rs.next());
        assertTrue("testBug3539".equals(this.rs.getString(3)));
    }

    /**
     * Tests fix for BUG#3540 getProcedureColumns doesn't work with wildcards
     * for procedure name
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testBug3540() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createProcedure("testBug3540", "(x int, out y int)\nBEGIN\nSELECT 1;end\n");

        this.rs = this.conn.getMetaData().getProcedureColumns(null, null, "testBug3540%", "%");

        assertTrue(this.rs.next());
        assertEquals("testBug3540", this.rs.getString(3));
        assertEquals("x", this.rs.getString(4));

        assertTrue(this.rs.next());
        assertEquals("testBug3540", this.rs.getString(3));
        assertEquals("y", this.rs.getString(4));

        assertTrue(!this.rs.next());
    }

    /**
     * Tests fix for BUG#7026 - DBMD.getProcedures() doesn't respect catalog
     * parameter
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug7026() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createProcedure("testBug7026", "(x int, out y int)\nBEGIN\nSELECT 1;end\n");

        //
        // Should be found this time.
        //
        this.rs = this.conn.getMetaData().getProcedures(this.conn.getCatalog(), null, "testBug7026");

        assertTrue(this.rs.next());
        assertTrue("testBug7026".equals(this.rs.getString(3)));

        assertTrue(!this.rs.next());

        //
        // This time, shouldn't be found, because not associated with this (bogus) catalog
        //
        this.rs = this.conn.getMetaData().getProcedures("abfgerfg", null, "testBug7026");
        assertTrue(!this.rs.next());

        //
        // Should be found this time as well, as we haven't specified a catalog.
        //
        this.rs = this.conn.getMetaData().getProcedures(null, null, "testBug7026");

        assertTrue(this.rs.next());
        assertTrue("testBug7026".equals(this.rs.getString(3)));

        assertTrue(!this.rs.next());
    }

    /**
     * Tests fix for BUG#9319 -- Stored procedures with same name in different
     * databases confuse the driver when it tries to determine parameter
     * counts/types.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug9319() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        boolean doASelect = true; // SELECT currently causes the server to hang on the last execution of this testcase, filed as BUG#9405

        if (isAdminConnectionConfigured()) {
            Connection db2Connection = null;
            Connection db1Connection = null;

            db2Connection = getAdminConnection();
            db1Connection = getAdminConnection();

            Statement db1st = db1Connection.createStatement();
            Statement db2st = db2Connection.createStatement();

            createDatabase(db2st, "db_9319_2");
            db2Connection.setCatalog("db_9319_2");
            createProcedure(db2st, "COMPROVAR_USUARI",
                    "(IN p_CodiUsuari VARCHAR(10),\nIN p_contrasenya VARCHAR(10),\nOUT p_userId INTEGER,"
                            + "\nOUT p_userName VARCHAR(30),\nOUT p_administrador VARCHAR(1),\nOUT p_idioma VARCHAR(2))\nBEGIN"
                            + (doASelect ? "\nselect 2;" : "\nSELECT 2 INTO p_administrador;") + "\nEND");

            createDatabase(db1st, "db_9319_1");
            db1Connection.setCatalog("db_9319_1");
            createProcedure(db1st, "COMPROVAR_USUARI",
                    "(IN p_CodiUsuari VARCHAR(10),\nIN p_contrasenya VARCHAR(10),\nOUT p_userId INTEGER,"
                            + "\nOUT p_userName VARCHAR(30),\nOUT p_administrador VARCHAR(1))\nBEGIN"
                            + (doASelect ? "\nselect 1;" : "\nSELECT 1 INTO p_administrador;") + "\nEND");

            CallableStatement cstmt = db2Connection.prepareCall("{ call COMPROVAR_USUARI(?, ?, ?, ?, ?, ?) }");
            cstmt.setString(1, "abc");
            cstmt.setString(2, "def");
            cstmt.registerOutParameter(3, java.sql.Types.INTEGER);
            cstmt.registerOutParameter(4, java.sql.Types.VARCHAR);
            cstmt.registerOutParameter(5, java.sql.Types.VARCHAR);

            cstmt.registerOutParameter(6, java.sql.Types.VARCHAR);

            cstmt.execute();

            if (doASelect) {
                this.rs = cstmt.getResultSet();
                assertTrue(this.rs.next());
                assertEquals(2, this.rs.getInt(1));
            } else {
                assertEquals(2, cstmt.getInt(5));
            }

            cstmt = db1Connection.prepareCall("{ call COMPROVAR_USUARI(?, ?, ?, ?, ?, ?) }");
            cstmt.setString(1, "abc");
            cstmt.setString(2, "def");
            cstmt.registerOutParameter(3, java.sql.Types.INTEGER);
            cstmt.registerOutParameter(4, java.sql.Types.VARCHAR);
            cstmt.registerOutParameter(5, java.sql.Types.VARCHAR);

            try {
                cstmt.registerOutParameter(6, java.sql.Types.VARCHAR);
                fail("Should've thrown an exception");
            } catch (SQLException sqlEx) {
                assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());
            }

            cstmt = db1Connection.prepareCall("{ call COMPROVAR_USUARI(?, ?, ?, ?, ?) }");
            cstmt.setString(1, "abc");
            cstmt.setString(2, "def");
            cstmt.registerOutParameter(3, java.sql.Types.INTEGER);
            cstmt.registerOutParameter(4, java.sql.Types.VARCHAR);
            cstmt.registerOutParameter(5, java.sql.Types.VARCHAR);

            cstmt.execute();

            if (doASelect) {
                this.rs = cstmt.getResultSet();
                assertTrue(this.rs.next());
                assertEquals(1, this.rs.getInt(1));
            } else {
                assertEquals(1, cstmt.getInt(5));
            }

            String quoteChar = db2Connection.getMetaData().getIdentifierQuoteString();

            cstmt = db2Connection.prepareCall(
                    "{ call " + quoteChar + db1Connection.getCatalog() + quoteChar + "." + quoteChar + "COMPROVAR_USUARI" + quoteChar + "(?, ?, ?, ?, ?) }");
            cstmt.setString(1, "abc");
            cstmt.setString(2, "def");
            cstmt.registerOutParameter(3, java.sql.Types.INTEGER);
            cstmt.registerOutParameter(4, java.sql.Types.VARCHAR);
            cstmt.registerOutParameter(5, java.sql.Types.VARCHAR);

            cstmt.execute();

            if (doASelect) {
                this.rs = cstmt.getResultSet();
                assertTrue(this.rs.next());
                assertEquals(1, this.rs.getInt(1));
            } else {
                assertEquals(1, cstmt.getInt(5));
            }
        }
    }

    /*
     * public void testBug9319() throws Exception { boolean doASelect = false;
     * // SELECT currently causes the server to hang on the // last execution of
     * this testcase, filed as BUG#9405
     * 
     * if (versionMeetsMinimum(5, 0, 2)) { if (isAdminConnectionConfigured()) {
     * Connection db2Connection = null; Connection db1Connection = null;
     * 
     * try { db2Connection = getAdminConnection();
     * 
     * db2Connection.createStatement().executeUpdate( "CREATE DATABASE IF NOT
     * EXISTS db_9319"); db2Connection.setCatalog("db_9319");
     * 
     * db2Connection.createStatement().executeUpdate( "DROP PROCEDURE IF EXISTS
     * COMPROVAR_USUARI");
     * 
     * db2Connection.createStatement().executeUpdate( "CREATE PROCEDURE
     * COMPROVAR_USUARI(IN p_CodiUsuari VARCHAR(10)," + "\nIN p_contrasenya
     * VARCHAR(10)," + "\nOUT p_userId INTEGER," + "\nOUT p_userName
     * VARCHAR(30)," + "\nOUT p_administrador VARCHAR(1)," + "\nOUT p_idioma
     * VARCHAR(2))" + "\nBEGIN" + (doASelect ? "\nselect 2;" : "\nSELECT 2 INTO
     * p_administrador;" ) + "\nEND");
     * 
     * this.stmt .executeUpdate("DROP PROCEDURE IF EXISTS COMPROVAR_USUARI");
     * this.stmt .executeUpdate("CREATE PROCEDURE COMPROVAR_USUARI(IN
     * p_CodiUsuari VARCHAR(10)," + "\nIN p_contrasenya VARCHAR(10)," + "\nOUT
     * p_userId INTEGER," + "\nOUT p_userName VARCHAR(30)," + "\nOUT
     * p_administrador VARCHAR(1))" + "\nBEGIN" + (doASelect ? "\nselect 1;" :
     * "\nSELECT 1 INTO p_administrador;" ) + "\nEND");
     * 
     * CallableStatement cstmt = db2Connection .prepareCall("{ call
     * COMPROVAR_USUARI(?, ?, ?, ?, ?, ?) }"); cstmt.setString(1, "abc");
     * cstmt.setString(2, "def"); cstmt.registerOutParameter(3,
     * java.sql.Types.INTEGER); cstmt.registerOutParameter(4,
     * java.sql.Types.VARCHAR); cstmt.registerOutParameter(5,
     * java.sql.Types.VARCHAR);
     * 
     * cstmt.registerOutParameter(6, java.sql.Types.VARCHAR);
     * 
     * cstmt.execute();
     * 
     * if (doASelect) { this.rs = cstmt.getResultSet();
     * assertTrue(this.rs.next()); assertEquals(2, this.rs.getInt(1)); } else {
     * assertEquals(2, cstmt.getInt(5)); }
     * 
     * cstmt = this.conn .prepareCall("{ call COMPROVAR_USUARI(?, ?, ?, ?, ?, ?)
     * }"); cstmt.setString(1, "abc"); cstmt.setString(2, "def");
     * cstmt.registerOutParameter(3, java.sql.Types.INTEGER);
     * cstmt.registerOutParameter(4, java.sql.Types.VARCHAR);
     * cstmt.registerOutParameter(5, java.sql.Types.VARCHAR);
     * 
     * try { cstmt.registerOutParameter(6, java.sql.Types.VARCHAR);
     * fail("Should've thrown an exception"); } catch (SQLException sqlEx) {
     * assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx .getSQLState());
     * }
     * 
     * cstmt = this.conn .prepareCall("{ call COMPROVAR_USUARI(?, ?, ?, ?, ?)
     * }"); cstmt.setString(1, "abc"); cstmt.setString(2, "def");
     * cstmt.registerOutParameter(3, java.sql.Types.INTEGER);
     * cstmt.registerOutParameter(4, java.sql.Types.VARCHAR);
     * cstmt.registerOutParameter(5, java.sql.Types.VARCHAR);
     * 
     * cstmt.execute();
     * 
     * if (doASelect) { this.rs = cstmt.getResultSet();
     * assertTrue(this.rs.next()); assertEquals(1, this.rs.getInt(1)); } else {
     * assertEquals(1, cstmt.getInt(5)); }
     * 
     * String quoteChar =
     * db2Connection.getMetaData().getIdentifierQuoteString();
     * 
     * cstmt = db2Connection .prepareCall("{ call " + quoteChar +
     * this.conn.getCatalog() + quoteChar + "." + quoteChar + "COMPROVAR_USUARI"
     * + quoteChar + "(?, ?, ?, ?, ?) }"); cstmt.setString(1, "abc");
     * cstmt.setString(2, "def"); cstmt.registerOutParameter(3,
     * java.sql.Types.INTEGER); cstmt.registerOutParameter(4,
     * java.sql.Types.VARCHAR); cstmt.registerOutParameter(5,
     * java.sql.Types.VARCHAR);
     * 
     * cstmt.execute();
     * 
     * if (doASelect) { this.rs = cstmt.getResultSet();
     * assertTrue(this.rs.next()); assertEquals(1, this.rs.getInt(1)); } else {
     * assertEquals(1, cstmt.getInt(5)); } } finally { if (db2Connection !=
     * null) { db2Connection.createStatement().executeUpdate( "DROP PROCEDURE IF
     * EXISTS COMPROVAR_USUARI"); //
     * db2Connection.createStatement().executeUpdate( // "DROP DATABASE IF
     * EXISTS db_9319"); }
     * 
     * this.stmt .executeUpdate("DROP PROCEDURE IF EXISTS COMPROVAR_USUARI"); }
     * } } }
     */

    /**
     * Tests fix for BUG#9682 - Stored procedures with DECIMAL parameters with
     * storage specifications that contained "," in them would fail.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug9682() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createProcedure("testBug9682", "(decimalParam DECIMAL(18,0))\nBEGIN\n   SELECT 1;\nEND");

        CallableStatement cStmt = null;

        try {
            cStmt = this.conn.prepareCall("Call testBug9682(?)");
            cStmt.setDouble(1, 18.0);
            cStmt.execute();
        } finally {
            if (cStmt != null) {
                cStmt.close();
            }
        }
    }

    /**
     * Tests fix forBUG#10310 - Driver doesn't support {?=CALL(...)} for calling
     * stored functions. This involved adding support for function retrieval to
     * DatabaseMetaData.getProcedures() and getProcedureColumns() as well.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug10310() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        CallableStatement cStmt = null;

        try {
            this.stmt.executeUpdate("DROP FUNCTION IF EXISTS testBug10310");
            this.stmt.executeUpdate("CREATE FUNCTION testBug10310(a float, b bigint, c int) RETURNS INT NO SQL\nBEGIN\nRETURN a;\nEND");
            cStmt = this.conn.prepareCall("{? = CALL testBug10310(?,?,?)}");
            cStmt.registerOutParameter(1, Types.INTEGER);
            cStmt.setFloat(2, 2);
            cStmt.setInt(3, 1);
            cStmt.setInt(4, 1);

            assertEquals(4, cStmt.getParameterMetaData().getParameterCount());
            assertEquals(Types.INTEGER, cStmt.getParameterMetaData().getParameterType(1));

            java.sql.DatabaseMetaData dbmd = this.conn.getMetaData();

            this.rs = ((com.mysql.jdbc.DatabaseMetaData) dbmd).getFunctionColumns(this.conn.getCatalog(), null, "testBug10310", "%");
            ResultSetMetaData rsmd = this.rs.getMetaData();

            assertEquals(17, rsmd.getColumnCount());
            assertEquals("FUNCTION_CAT", rsmd.getColumnName(1));
            assertEquals("FUNCTION_SCHEM", rsmd.getColumnName(2));
            assertEquals("FUNCTION_NAME", rsmd.getColumnName(3));
            assertEquals("COLUMN_NAME", rsmd.getColumnName(4));
            assertEquals("COLUMN_TYPE", rsmd.getColumnName(5));
            assertEquals("DATA_TYPE", rsmd.getColumnName(6));
            assertEquals("TYPE_NAME", rsmd.getColumnName(7));
            assertEquals("PRECISION", rsmd.getColumnName(8));
            assertEquals("LENGTH", rsmd.getColumnName(9));
            assertEquals("SCALE", rsmd.getColumnName(10));
            assertEquals("RADIX", rsmd.getColumnName(11));
            assertEquals("NULLABLE", rsmd.getColumnName(12));
            assertEquals("REMARKS", rsmd.getColumnName(13));
            assertEquals("CHAR_OCTET_LENGTH", rsmd.getColumnName(14));
            assertEquals("ORDINAL_POSITION", rsmd.getColumnName(15));
            assertEquals("IS_NULLABLE", rsmd.getColumnName(16));
            assertEquals("SPECIFIC_NAME", rsmd.getColumnName(17));

            this.rs.close();

            assertFalse(cStmt.execute());
            assertEquals(2f, cStmt.getInt(1), .001);
            assertEquals("java.lang.Integer", cStmt.getObject(1).getClass().getName());

            assertEquals(-1, cStmt.executeUpdate());
            assertEquals(2f, cStmt.getInt(1), .001);
            assertEquals("java.lang.Integer", cStmt.getObject(1).getClass().getName());

            cStmt.setFloat("a", 4);
            cStmt.setInt("b", 1);
            cStmt.setInt("c", 1);

            assertFalse(cStmt.execute());
            assertEquals(4f, cStmt.getInt(1), .001);
            assertEquals("java.lang.Integer", cStmt.getObject(1).getClass().getName());

            assertEquals(-1, cStmt.executeUpdate());
            assertEquals(4f, cStmt.getInt(1), .001);
            assertEquals("java.lang.Integer", cStmt.getObject(1).getClass().getName());

            // Check metadata while we're at it

            this.rs = dbmd.getProcedures(this.conn.getCatalog(), null, "testBug10310");
            this.rs.next();
            assertEquals("testBug10310", this.rs.getString("PROCEDURE_NAME"));
            assertEquals(java.sql.DatabaseMetaData.procedureReturnsResult, this.rs.getShort("PROCEDURE_TYPE"));
            cStmt.setNull(2, Types.FLOAT);
            cStmt.setInt(3, 1);
            cStmt.setInt(4, 1);

            assertFalse(cStmt.execute());
            assertEquals(0f, cStmt.getInt(1), .001);
            assertEquals(true, cStmt.wasNull());
            assertEquals(null, cStmt.getObject(1));
            assertEquals(true, cStmt.wasNull());

            assertEquals(-1, cStmt.executeUpdate());
            assertEquals(0f, cStmt.getInt(1), .001);
            assertEquals(true, cStmt.wasNull());
            assertEquals(null, cStmt.getObject(1));
            assertEquals(true, cStmt.wasNull());

            // Check with literals, not all parameters filled!
            cStmt = this.conn.prepareCall("{? = CALL testBug10310(4,5,?)}");
            cStmt.registerOutParameter(1, Types.INTEGER);
            cStmt.setInt(2, 1);

            assertFalse(cStmt.execute());
            assertEquals(4f, cStmt.getInt(1), .001);
            assertEquals("java.lang.Integer", cStmt.getObject(1).getClass().getName());

            assertEquals(-1, cStmt.executeUpdate());
            assertEquals(4f, cStmt.getInt(1), .001);
            assertEquals("java.lang.Integer", cStmt.getObject(1).getClass().getName());

            assertEquals(2, cStmt.getParameterMetaData().getParameterCount());
            assertEquals(Types.INTEGER, cStmt.getParameterMetaData().getParameterType(1));
            assertEquals(Types.INTEGER, cStmt.getParameterMetaData().getParameterType(2));
        } finally {
            if (this.rs != null) {
                this.rs.close();
                this.rs = null;
            }

            if (cStmt != null) {
                cStmt.close();
            }

            this.stmt.executeUpdate("DROP FUNCTION IF EXISTS testBug10310");
        }
    }

    /**
     * Tests fix for Bug#12417 - stored procedure catalog name is case-sensitive
     * on Windows (this is actually a server bug, but we have a workaround in
     * place for it now).
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug12417() throws Exception {
        if (serverSupportsStoredProcedures() && isServerRunningOnWindows()) {

            createProcedure("testBug12417", "()\nBEGIN\nSELECT 1;end\n");

            Connection ucCatalogConn = null;

            try {
                ucCatalogConn = getConnectionWithProps((Properties) null);
                ucCatalogConn.setCatalog(this.conn.getCatalog().toUpperCase());
                ucCatalogConn.prepareCall("{call testBug12417()}");
            } finally {
                if (ucCatalogConn != null) {
                    ucCatalogConn.close();
                }
            }
        }
    }

    public void testBug15121() throws Exception {
        if (!this.DISABLED_testBug15121 /* needs to be fixed on server */) {
            if (versionMeetsMinimum(5, 0)) {
                createProcedure("p_testBug15121", "()\nBEGIN\nSELECT * from idonotexist;\nEND");

                Properties props = new Properties();
                props.setProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY, "");

                Connection noDbConn = null;

                try {
                    noDbConn = getConnectionWithProps(props);

                    StringBuilder queryBuf = new StringBuilder("{call ");
                    String quotedId = this.conn.getMetaData().getIdentifierQuoteString();
                    queryBuf.append(quotedId);
                    queryBuf.append(this.conn.getCatalog());
                    queryBuf.append(quotedId);
                    queryBuf.append(".p_testBug15121()}");

                    noDbConn.prepareCall(queryBuf.toString()).execute();
                } finally {
                    if (noDbConn != null) {
                        noDbConn.close();
                    }
                }
            }
        }
    }

    /**
     * Tests fix for BUG#15464 - INOUT parameter does not store IN value.
     * 
     * @throws Exception
     *             if the test fails
     */

    public void testBug15464() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createProcedure("testInOutParam",
                "(IN p1 VARCHAR(255), INOUT p2 INT)\nbegin\n DECLARE z INT;\n" + "SET z = p2 + 1;\nSET p2 = z;\nSELECT p1;\nSELECT CONCAT('zyxw', p1);\nend\n");

        CallableStatement storedProc = null;

        storedProc = this.conn.prepareCall("{call testInOutParam(?, ?)}");

        storedProc.setString(1, "abcd");
        storedProc.setInt(2, 4);
        storedProc.registerOutParameter(2, Types.INTEGER);

        storedProc.execute();

        assertEquals(5, storedProc.getInt(2));
    }

    /**
     * Tests fix for BUG#17898 - registerOutParameter not working when some
     * parameters pre-populated. Still waiting for feedback from JDBC experts
     * group to determine what correct parameter count from getMetaData() should
     * be, however.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug17898() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createProcedure("testBug17898", "(param1 VARCHAR(50), OUT param2 INT)\nBEGIN\nDECLARE rtn INT;\n" + "SELECT 1 INTO rtn;\nSET param2=rtn;\nEND");

        CallableStatement cstmt = this.conn.prepareCall("{CALL testBug17898('foo', ?)}");
        cstmt.registerOutParameter(1, Types.INTEGER);
        cstmt.execute();
        assertEquals(1, cstmt.getInt(1));

        cstmt.clearParameters();
        cstmt.registerOutParameter("param2", Types.INTEGER);
        cstmt.execute();
        assertEquals(1, cstmt.getInt(1));
    }

    /**
     * Tests fix for BUG#21462 - JDBC (and ODBC) specifications allow
     * no-parenthesis CALL statements for procedures with no arguments, MySQL
     * server does not.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug21462() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createProcedure("testBug21462", "() BEGIN SELECT 1; END");

        CallableStatement cstmt = null;

        try {
            cstmt = this.conn.prepareCall("{CALL testBug21462}");
            cstmt.execute();
        } finally {
            if (cstmt != null) {
                cstmt.close();
            }
        }

    }

    /**
     * Tests fix for BUG#22024 - Newlines causing whitespace to span confuse
     * procedure parser when getting parameter metadata for stored procedures.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug22024() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createProcedure("testBug22024_1", "(\r\n)\r\n BEGIN SELECT 1; END");
        createProcedure("testBug22024_2", "(\r\na INT)\r\n BEGIN SELECT 1; END");

        CallableStatement cstmt = null;

        try {
            cstmt = this.conn.prepareCall("{CALL testBug22024_1()}");
            cstmt.execute();

            cstmt = this.conn.prepareCall("{CALL testBug22024_2(?)}");
            cstmt.setInt(1, 1);
            cstmt.execute();
        } finally {
            if (cstmt != null) {
                cstmt.close();
            }
        }

    }

    /**
     * Tests workaround for server crash when calling stored procedures via a
     * server-side prepared statement (driver now detects prepare(stored
     * procedure) and substitutes client-side prepared statement).
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug22297() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createTable("tblTestBug2297_1", "(id varchar(20) NOT NULL default '',Income double(19,2) default NULL)");

        createTable("tblTestBug2297_2", "(id varchar(20) NOT NULL default '',CreatedOn datetime default NULL)");

        createProcedure("testBug22297", "(pcaseid INT) BEGIN\nSET @sql = \"DROP TEMPORARY TABLE IF EXISTS tmpOrders\";"
                + " PREPARE stmt FROM @sql; EXECUTE stmt; DEALLOCATE PREPARE stmt;"
                + "\nSET @sql = \"CREATE TEMPORARY TABLE tmpOrders SELECT id, 100 AS Income FROM tblTestBug2297_1 GROUP BY id\"; PREPARE stmt FROM @sql;"
                + " EXECUTE stmt; DEALLOCATE PREPARE stmt;\n SELECT id, Income FROM (SELECT e.id AS id ,COALESCE(prof.Income,0) AS Income"
                + "\n FROM tblTestBug2297_2 e LEFT JOIN tmpOrders prof ON e.id = prof.id\n WHERE e.CreatedOn > '2006-08-01') AS Final ORDER BY id;\nEND");

        this.stmt.executeUpdate("INSERT INTO tblTestBug2297_1 (`id`,`Income`) VALUES ('a',4094.00),('b',500.00),('c',3462.17), ('d',500.00), ('e',600.00)");

        this.stmt.executeUpdate("INSERT INTO tblTestBug2297_2 (`id`,`CreatedOn`) VALUES ('d','2006-08-31 00:00:00'),('e','2006-08-31 00:00:00'),"
                + "('b','2006-08-31 00:00:00'),('c','2006-08-31 00:00:00'),('a','2006-08-31 00:00:00')");

        this.pstmt = this.conn.prepareStatement("{CALL testBug22297(?)}");
        this.pstmt.setInt(1, 1);
        this.rs = this.pstmt.executeQuery();

        String[] ids = new String[] { "a", "b", "c", "d", "e" };
        int pos = 0;

        while (this.rs.next()) {
            assertEquals(ids[pos++], this.rs.getString(1));
            assertEquals(100, this.rs.getInt(2));
        }

        assertTrue(this.pstmt.getClass().getName().indexOf("Server") == -1);
    }

    public void testHugeNumberOfParameters() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        StringBuilder procDef = new StringBuilder("(OUT param_0 VARCHAR(32)");
        StringBuilder placeholders = new StringBuilder("?");

        for (int i = 1; i < 274; i++) {
            procDef.append(", OUT param_" + i + " VARCHAR(32)");
            placeholders.append(",?");
        }
        procDef.append(")\nBEGIN\nSELECT 1;\nEND");

        createProcedure("testHugeNumberOfParameters", procDef.toString());

        CallableStatement cStmt = null;

        try {
            cStmt = this.conn.prepareCall("{call testHugeNumberOfParameters(" + placeholders.toString() + ")}");
            cStmt.registerOutParameter(274, Types.VARCHAR);

            cStmt.execute();
        } finally {
            if (cStmt != null) {
                cStmt.close();
            }
        }
    }

    public void testPrepareOfMultiRs() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createProcedure("p", "() begin select 1; select 2; end;");

        PreparedStatement ps = null;

        try {
            ps = this.conn.prepareStatement("call p()");

            ps.execute();
            this.rs = ps.getResultSet();
            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(1));
            assertTrue(ps.getMoreResults());
            this.rs = ps.getResultSet();
            assertTrue(this.rs.next());
            assertEquals(2, this.rs.getInt(1));
            assertTrue(!ps.getMoreResults());
        } finally {
            if (this.rs != null) {
                this.rs.close();
                this.rs = null;
            }

            if (ps != null) {
                ps.close();
            }
        }

    }

    /**
     * Tests fix for BUG#25379 - INOUT parameters in CallableStatements get
     * doubly-escaped.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug25379() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createTable("testBug25379", "(col char(40))");

        createProcedure("sp_testBug25379", "(INOUT invalue char(255))\nBEGIN" + "\ninsert into testBug25379(col) values(invalue);\nEND");

        CallableStatement cstmt = this.conn.prepareCall("{call sp_testBug25379(?)}");
        cstmt.setString(1, "'john'");
        cstmt.executeUpdate();
        assertEquals("'john'", cstmt.getString(1));
        assertEquals("'john'", getSingleValue("testBug25379", "col", "").toString());
    }

    /**
     * Tests fix for BUG#25715 - CallableStatements with OUT/INOUT parameters
     * that are "binary" have extra 7 bytes (which happens to be the _binary
     * introducer!)
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug25715() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return; // no stored procs
        }

        createProcedure("spbug25715", "(INOUT mblob MEDIUMBLOB) BEGIN SELECT 1 FROM DUAL WHERE 1=0;\nEND");
        CallableStatement cstmt = null;

        try {
            cstmt = this.conn.prepareCall("{call spbug25715(?)}");

            byte[] buf = new byte[65];
            for (int i = 0; i < 65; i++) {
                buf[i] = 1;
            }
            int il = buf.length;

            int[] typesToTest = new int[] { Types.BIT, Types.BINARY, Types.BLOB, Types.JAVA_OBJECT, Types.LONGVARBINARY, Types.VARBINARY };

            for (int i = 0; i < typesToTest.length; i++) {

                cstmt.setBinaryStream("mblob", new ByteArrayInputStream(buf), buf.length);
                cstmt.registerOutParameter("mblob", typesToTest[i]);

                cstmt.executeUpdate();

                InputStream is = cstmt.getBlob("mblob").getBinaryStream();
                ByteArrayOutputStream bOut = new ByteArrayOutputStream();

                int bytesRead = 0;
                byte[] readBuf = new byte[256];

                while ((bytesRead = is.read(readBuf)) != -1) {
                    bOut.write(readBuf, 0, bytesRead);
                }

                byte[] fromSelectBuf = bOut.toByteArray();

                int ol = fromSelectBuf.length;

                assertEquals(il, ol);
            }

            cstmt.close();
        } finally {

            if (cstmt != null) {
                cstmt.close();
            }
        }

    }

    protected boolean serverSupportsStoredProcedures() throws SQLException {
        return versionMeetsMinimum(5, 0);
    }

    public void testBug26143() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return; // no stored procedure support
        }

        try {

            dropProcedure("testBug26143");

            this.stmt.executeUpdate("CREATE DEFINER=CURRENT_USER PROCEDURE testBug26143(I INT) COMMENT 'abcdefg'\nBEGIN\nSELECT I * 10;\nEND");

            this.conn.prepareCall("{call testBug26143(?)").close();

        } finally {
            dropProcedure("testBug26143");
        }
    }

    /**
     * Tests fix for BUG#26959 - comments confuse procedure parser.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug26959() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createProcedure("testBug26959",
                "(_ACTION varchar(20),\n`/*dumb-identifier-1*/` int,\n`#dumb-identifier-2` int,\n`--dumb-identifier-3` int,"
                        + "\n_CLIENT_ID int, -- ABC\n_LOGIN_ID  int, # DEF\n_WHERE varchar(2000),\n_SORT varchar(2000),"
                        + "\n out _SQL varchar(/* inline right here - oh my gosh! */ 8000),\n _SONG_ID int,\n  _NOTES varchar(2000),\n out _RESULT varchar(10)"
                        + "\n /*\n ,    -- Generic result parameter"
                        + "\n out _PERIOD_ID int,         -- Returns the period_id. Useful when using @PREDEFLINK to return which is the last period"
                        + "\n   _SONGS_LIST varchar(8000),\n  _COMPOSERID int,\n  _PUBLISHERID int,"
                        + "\n   _PREDEFLINK int        -- If the user is accessing through a predefined link: 0=none  1=last period\n */) BEGIN SELECT 1; END");

        createProcedure("testBug26959_1", "(`/*id*/` /* before type 1 */ varchar(20),"
                + "/* after type 1 */ OUT result2 DECIMAL(/*size1*/10,/*size2*/2) /* p2 */)BEGIN SELECT action, result; END");

        this.conn.prepareCall("{call testBug26959(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}").close();
        this.rs = this.conn.getMetaData().getProcedureColumns(this.conn.getCatalog(), null, "testBug26959", "%");

        String[] parameterNames = new String[] { "_ACTION", "/*dumb-identifier-1*/", "#dumb-identifier-2", "--dumb-identifier-3", "_CLIENT_ID", "_LOGIN_ID",
                "_WHERE", "_SORT", "_SQL", "_SONG_ID", "_NOTES", "_RESULT" };

        int[] parameterTypes = new int[] { Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.VARCHAR, Types.VARCHAR };

        int[] direction = new int[] { java.sql.DatabaseMetaData.procedureColumnIn, java.sql.DatabaseMetaData.procedureColumnIn,
                java.sql.DatabaseMetaData.procedureColumnIn, java.sql.DatabaseMetaData.procedureColumnIn, java.sql.DatabaseMetaData.procedureColumnIn,
                java.sql.DatabaseMetaData.procedureColumnIn, java.sql.DatabaseMetaData.procedureColumnIn, java.sql.DatabaseMetaData.procedureColumnIn,
                java.sql.DatabaseMetaData.procedureColumnOut, java.sql.DatabaseMetaData.procedureColumnIn, java.sql.DatabaseMetaData.procedureColumnIn,
                java.sql.DatabaseMetaData.procedureColumnOut };

        int[] precision = new int[] { 20, 10, 10, 10, 10, 10, 2000, 2000, 8000, 10, 2000, 10 };

        int index = 0;

        while (this.rs.next()) {
            assertEquals(parameterNames[index], this.rs.getString("COLUMN_NAME"));
            assertEquals(parameterTypes[index], this.rs.getInt("DATA_TYPE"));

            switch (index) {
                case 0:
                case 6:
                case 7:
                case 8:
                case 10:
                case 11:
                    assertEquals(precision[index], this.rs.getInt("LENGTH"));
                    break;
                default:
                    assertEquals(precision[index], this.rs.getInt("PRECISION"));
            }

            assertEquals(direction[index], this.rs.getInt("COLUMN_TYPE"));
            index++;
        }

        this.rs.close();

        index = 0;
        parameterNames = new String[] { "/*id*/", "result2" };
        parameterTypes = new int[] { Types.VARCHAR, Types.DECIMAL };
        precision = new int[] { 20, 10 };
        direction = new int[] { java.sql.DatabaseMetaData.procedureColumnIn, java.sql.DatabaseMetaData.procedureColumnOut };
        int[] scale = new int[] { 0, 2 };

        this.conn.prepareCall("{call testBug26959_1(?, ?)}").close();

        this.rs = this.conn.getMetaData().getProcedureColumns(this.conn.getCatalog(), null, "testBug26959_1", "%");

        while (this.rs.next()) {
            assertEquals(parameterNames[index], this.rs.getString("COLUMN_NAME"));
            assertEquals(parameterTypes[index], this.rs.getInt("DATA_TYPE"));
            switch (index) {
                case 0:
                case 6:
                case 7:
                case 8:
                case 10:
                case 11:
                    assertEquals(precision[index], this.rs.getInt("LENGTH"));
                    break;
                default:
                    assertEquals(precision[index], this.rs.getInt("PRECISION"));
            }
            assertEquals(scale[index], this.rs.getInt("SCALE"));
            assertEquals(direction[index], this.rs.getInt("COLUMN_TYPE"));

            index++;
        }
    }

    /**
     * Tests fix for BUG#27400 - CALL [comment] some_proc() doesn't work
     */
    public void testBug27400() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return; // SPs not supported
        }

        createProcedure("testBug27400", "(a INT, b VARCHAR(32)) BEGIN SELECT 1; END");

        CallableStatement cStmt = null;

        try {
            cStmt = this.conn.prepareCall("{CALL /* SOME COMMENT */ testBug27400( /* does this work too? */ ?, ?)} # and a commented ? here too");
            assertTrue(cStmt.toString().indexOf("/*") != -1); // we don't want
                                                             // to strip the
                                                             // comments
            cStmt.setInt(1, 1);
            cStmt.setString(2, "bleh");
            cStmt.execute();
        } finally {
            if (cStmt != null) {
                cStmt.close();
            }
        }
    }

    /**
     * Tests fix for BUG#28689 - CallableStatement.executeBatch() doesn't work
     * when connection property "noAccessToProcedureBodies" has been set to
     * "true".
     * 
     * The fix involves changing the behavior of "noAccessToProcedureBodies", in
     * that the driver will now report all paramters as "IN" paramters but allow
     * callers to call registerOutParameter() on them.
     * 
     * @throws Exception
     */
    public void testBug28689() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return; // no stored procedures
        }

        createTable("testBug28689", "(" +

                "`id` int(11) NOT NULL auto_increment,`usuario` varchar(255) default NULL,PRIMARY KEY  (`id`))");

        this.stmt.executeUpdate("INSERT INTO testBug28689 (usuario) VALUES ('AAAAAA')");

        createProcedure("sp_testBug28689", "(tid INT)\nBEGIN\nUPDATE testBug28689 SET usuario = 'BBBBBB' WHERE id = tid;\nEND");

        Connection noProcedureBodiesConn = getConnectionWithProps("noAccessToProcedureBodies=true");
        CallableStatement cStmt = null;

        try {
            cStmt = noProcedureBodiesConn.prepareCall("{CALL sp_testBug28689(?)}");
            cStmt.setInt(1, 1);
            cStmt.addBatch();
            cStmt.executeBatch();

            assertEquals("BBBBBB", getSingleIndexedValueWithQuery(noProcedureBodiesConn, 1, "SELECT `usuario` FROM testBug28689 WHERE id=1"));
        } finally {
            if (cStmt != null) {
                cStmt.close();
            }

            if (noProcedureBodiesConn != null) {
                noProcedureBodiesConn.close();
            }
        }
    }

    /**
     * Tests fix for Bug#31823 - CallableStatement.setNull() on a stored
     * function would throw an ArrayIndexOutOfBounds when setting the last
     * parameter to null when calling setNull().
     * 
     * @throws Exception
     */
    public void testBug31823() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return; // no stored functions
        }

        createTable("testBug31823", "(value_1 BIGINT PRIMARY KEY,value_2 VARCHAR(20))");

        createFunction("f_testBug31823", "(value_1_v BIGINT,value_2_v VARCHAR(20)) RETURNS BIGINT "
                + "DETERMINISTIC MODIFIES SQL DATA BEGIN INSERT INTO testBug31823 VALUES (value_1_v,value_2_v); RETURN value_1_v; END;");

        // Prepare the function call
        CallableStatement callable = null;

        try {
            callable = this.conn.prepareCall("{? = call f_testBug31823(?,?)}");

            callable.registerOutParameter(1, Types.BIGINT);

            // Add row with non-null value
            callable.setLong(2, 1);
            callable.setString(3, "Non-null value");
            callable.executeUpdate();
            assertEquals(1, callable.getLong(1));

            // Add row with null value
            callable.setLong(2, 2);
            callable.setNull(3, Types.VARCHAR);
            callable.executeUpdate();
            assertEquals(2, callable.getLong(1));

            Method[] setters = CallableStatement.class.getMethods();

            for (int i = 0; i < setters.length; i++) {
                if (setters[i].getName().startsWith("set")) {
                    Class<?>[] args = setters[i].getParameterTypes();

                    if (args.length == 2 && args[0].equals(Integer.TYPE)) {
                        if (!args[1].isPrimitive()) {
                            try {
                                setters[i].invoke(callable, new Object[] { new Integer(2), null });
                            } catch (InvocationTargetException ive) {
                                if (!(ive.getCause() instanceof com.mysql.jdbc.NotImplemented
                                        || ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                    throw ive;
                                }
                            }
                        } else {
                            if (args[1].getName().equals("boolean")) {
                                try {
                                    setters[i].invoke(callable, new Object[] { new Integer(2), Boolean.FALSE });
                                } catch (InvocationTargetException ive) {
                                    if (!(ive.getCause() instanceof com.mysql.jdbc.NotImplemented
                                            || ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                        throw ive;
                                    }
                                }
                            }

                            if (args[1].getName().equals("byte")) {

                                try {
                                    setters[i].invoke(callable, new Object[] { new Integer(2), new Byte((byte) 0) });
                                } catch (InvocationTargetException ive) {
                                    if (!(ive.getCause() instanceof com.mysql.jdbc.NotImplemented
                                            || ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                        throw ive;
                                    }
                                }

                            }

                            if (args[1].getName().equals("double")) {

                                try {
                                    setters[i].invoke(callable, new Object[] { new Integer(2), new Double(0) });
                                } catch (InvocationTargetException ive) {
                                    if (!(ive.getCause() instanceof com.mysql.jdbc.NotImplemented
                                            || ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                        throw ive;
                                    }
                                }

                            }

                            if (args[1].getName().equals("float")) {

                                try {
                                    setters[i].invoke(callable, new Object[] { new Integer(2), new Float(0) });
                                } catch (InvocationTargetException ive) {
                                    if (!(ive.getCause() instanceof com.mysql.jdbc.NotImplemented
                                            || ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                        throw ive;
                                    }
                                }

                            }

                            if (args[1].getName().equals("int")) {

                                try {
                                    setters[i].invoke(callable, new Object[] { new Integer(2), new Integer(0) });
                                } catch (InvocationTargetException ive) {
                                    if (!(ive.getCause() instanceof com.mysql.jdbc.NotImplemented
                                            || ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                        throw ive;
                                    }
                                }

                            }

                            if (args[1].getName().equals("long")) {
                                try {
                                    setters[i].invoke(callable, new Object[] { new Integer(2), new Long(0) });
                                } catch (InvocationTargetException ive) {
                                    if (!(ive.getCause() instanceof com.mysql.jdbc.NotImplemented
                                            || ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                        throw ive;
                                    }
                                }
                            }

                            if (args[1].getName().equals("short")) {
                                try {
                                    setters[i].invoke(callable, new Object[] { new Integer(2), new Short((short) 0) });
                                } catch (InvocationTargetException ive) {
                                    if (!(ive.getCause() instanceof com.mysql.jdbc.NotImplemented
                                            || ive.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException"))) {
                                        throw ive;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            if (callable != null) {
                callable.close();
            }
        }
    }

    /**
     * Tests fix for Bug#32246 - When unpacking rows directly, we don't hand off
     * error message packets to the internal method which decodes them
     * correctly, so no exception is rasied, and the driver than hangs trying to
     * read rows that aren't there...
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug32246() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        dropTable("test_table_2");
        dropTable("test_table_1");
        doBug32246(this.conn);
        dropTable("test_table_2");
        dropTable("test_table_1");
        doBug32246(getConnectionWithProps("useDirectRowUnpack=false"));
    }

    private void doBug32246(Connection aConn) throws SQLException {
        createTable("test_table_1", "(value_1 BIGINT PRIMARY KEY) ENGINE=InnoDB");
        this.stmt.executeUpdate("INSERT INTO test_table_1 VALUES (1)");
        createTable("test_table_2", "(value_2 BIGINT PRIMARY KEY) ENGINE=InnoDB");
        this.stmt.executeUpdate("DROP FUNCTION IF EXISTS test_function");
        createFunction("test_function", "() RETURNS BIGINT DETERMINISTIC MODIFIES SQL DATA BEGIN DECLARE max_value BIGINT; "
                + "SELECT MAX(value_1) INTO max_value FROM test_table_2; RETURN max_value; END;");

        CallableStatement callable = null;

        try {
            callable = aConn.prepareCall("{? = call test_function()}");

            callable.registerOutParameter(1, Types.BIGINT);

            try {
                callable.executeUpdate();
                fail("impossible; we should never get here.");
            } catch (SQLException sqlEx) {
                assertEquals("42S22", sqlEx.getSQLState());
            }

            createTable("test_table_1", "(value_1 BIGINT PRIMARY KEY) ENGINE=InnoDB");
            this.stmt.executeUpdate("INSERT INTO test_table_1 VALUES (1)");
            createTable("test_table_2",
                    "(value_2 BIGINT PRIMARY KEY, " + " FOREIGN KEY (value_2) REFERENCES test_table_1 (value_1) ON DELETE CASCADE) ENGINE=InnoDB");
            createFunction("test_function",
                    "(value BIGINT) RETURNS BIGINT DETERMINISTIC MODIFIES SQL DATA BEGIN " + "INSERT INTO test_table_2 VALUES (value); RETURN value; END;");

            callable = aConn.prepareCall("{? = call test_function(?)}");
            callable.registerOutParameter(1, Types.BIGINT);

            callable.setLong(2, 1);
            callable.executeUpdate();

            callable.setLong(2, 2);

            try {
                callable.executeUpdate();
                fail("impossible; we should never get here.");
            } catch (SQLException sqlEx) {
                assertEquals("23000", sqlEx.getSQLState());
            }
        } finally {
            if (callable != null) {
                callable.close();
            }
        }
    }

    public void testBitSp() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        createTable("`Bit_Tab`", "( `MAX_VAL` tinyint(1) default NULL, `MIN_VAL` tinyint(1) default NULL, `NULL_VAL` tinyint(1) default NULL)");

        createProcedure("Bit_Proc", "(out MAX_PARAM TINYINT, out MIN_PARAM TINYINT, out NULL_PARAM TINYINT)"
                + "begin select MAX_VAL, MIN_VAL, NULL_VAL  into MAX_PARAM, MIN_PARAM, NULL_PARAM from Bit_Tab; end");

        Boolean minBooleanVal;
        Boolean oRetVal;

        String Min_Val_Query = "SELECT MIN_VAL from Bit_Tab";
        //String sMaxBooleanVal = "1";
        // sMaxBooleanVal = "true";
        //Boolean bool = Boolean.valueOf("true");
        String Min_Insert = "insert into Bit_Tab values(1,0,null)";
        // System.out.println("Value to insert=" + extractVal(Min_Insert,1));
        CallableStatement cstmt;

        this.stmt.executeUpdate("delete from Bit_Tab");
        this.stmt.executeUpdate(Min_Insert);
        cstmt = this.conn.prepareCall("{call Bit_Proc(?,?,?)}");

        System.out.println("register the output parameters");
        cstmt.registerOutParameter(1, java.sql.Types.BIT);
        cstmt.registerOutParameter(2, java.sql.Types.BIT);
        cstmt.registerOutParameter(3, java.sql.Types.BIT);

        System.out.println("execute the procedure");
        cstmt.executeUpdate();

        System.out.println("invoke getBoolean method");
        boolean bRetVal = cstmt.getBoolean(2);
        oRetVal = new Boolean(bRetVal);
        minBooleanVal = new Boolean("false");
        this.rs = this.stmt.executeQuery(Min_Val_Query);
        if (oRetVal.equals(minBooleanVal)) {
            System.out.println("getBoolean returns the Minimum value ");
        } else {
            System.out.println("getBoolean() did not return the Minimum value, getBoolean Failed!");

        }
    }

    public void testNotReallyCallableStatement() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        CallableStatement cstmt = null;

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testNotReallyCallableStatement");
            cstmt = this.conn.prepareCall("CREATE TABLE testNotReallyCallableStatement(field1 INT)");

        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testNotReallyCallableStatement");

            if (cstmt != null) {
                cstmt.close();
            }
        }
    }

    public void testBug35199() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        createFunction("test_function", "(a varchar(40), b bigint(20), c varchar(80)) RETURNS bigint(20) LANGUAGE SQL DETERMINISTIC "
                + "MODIFIES SQL DATA COMMENT 'bbb' BEGIN RETURN 1; END; ");

        CallableStatement callable = null;
        try {
            callable = this.conn.prepareCall("{? = call test_function(?,101,?)}");
            callable.registerOutParameter(1, Types.BIGINT);

            callable.setString(2, "FOO");
            callable.setString(3, "BAR");
            callable.executeUpdate();
        } finally {
            if (callable != null) {
                callable.close();
            }
        }
    }

    public void testBug49831() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createTable("testBug49831", "(val varchar(32))");

        createProcedure("pTestBug49831", "(testval varchar(32)) BEGIN insert into testBug49831 (val) values (testval);END;");

        execProcBug49831(this.conn);
        this.stmt.execute("TRUNCATE TABLE testBug49831");
        assertEquals(0, getRowCount("testBug49831"));
        Connection noBodiesConn = getConnectionWithProps("noAccessToProcedureBodies=true,jdbcCompliantTruncation=false,characterEncoding=utf8,useUnicode=yes");
        try {
            execProcBug49831(noBodiesConn);
        } finally {
            noBodiesConn.close();
        }

    }

    private void execProcBug49831(Connection c) throws Exception {
        CallableStatement cstmt = c.prepareCall("{call pTestBug49831(?)}");
        cstmt.setObject(1, "abc", Types.VARCHAR, 32);
        cstmt.addBatch();
        cstmt.setObject(1, "def", Types.VARCHAR, 32);
        cstmt.addBatch();
        cstmt.executeBatch();
        assertEquals(2, getRowCount("testBug49831"));
        this.rs = this.stmt.executeQuery("SELECT * from testBug49831 ORDER BY VAL ASC");
        this.rs.next();
        assertEquals("abc", this.rs.getString(1));
        this.rs.next();
        assertEquals("def", this.rs.getString(1));
    }

    public void testBug43576() throws Exception {
        createTable("TMIX91P",
                "(F01SMALLINT         SMALLINT NOT NULL, F02INTEGER          INTEGER,F03REAL             REAL,"
                        + "F04FLOAT            FLOAT,F05NUMERIC31X4      NUMERIC(31,4), F06NUMERIC16X16     NUMERIC(16,16), F07CHAR_10          CHAR(10),"
                        + " F08VARCHAR_10       VARCHAR(10), F09CHAR_20          CHAR(20), F10VARCHAR_20       VARCHAR(20), F11DATE         DATE,"
                        + " F12DATETIME         DATETIME, PRIMARY KEY (F01SMALLINT))");

        this.stmt.executeUpdate("INSERT INTO TMIX91P VALUES (1,1,1234567.12,1234567.12,111111111111111111111111111.1111,.111111111111111,'1234567890',"
                + "'1234567890','CHAR20CHAR20','VARCHAR20ABCD','2001-01-01','2001-01-01 01:01:01.111')");

        this.stmt.executeUpdate("INSERT INTO TMIX91P VALUES (7,1,1234567.12,1234567.12,22222222222.0001,.99999999999,'1234567896','1234567896','CHAR20',"
                + "'VARCHAR20ABCD','2001-01-01','2001-01-01 01:01:01.111')");

        this.stmt.executeUpdate("INSERT INTO TMIX91P VALUES (12,12,1234567.12,1234567.12,111222333.4444,.1234567890,'2234567891','2234567891','CHAR20',"
                + "'VARCHAR20VARCHAR20','2001-01-01','2001-01-01 01:01:01.111')");

        createProcedure("MSQSPR100",
                "\n( p1_in  INTEGER , p2_in  CHAR(20), OUT p3_out INTEGER, OUT p4_out CHAR(11))\nBEGIN "
                        + "\n SELECT F01SMALLINT,F02INTEGER, F11DATE,F12DATETIME,F03REAL \n FROM TMIX91P WHERE F02INTEGER = p1_in; "
                        + "\n SELECT F02INTEGER,F07CHAR_10,F08VARCHAR_10,F09CHAR_20 \n FROM TMIX91P WHERE  F09CHAR_20 = p2_in ORDER BY F02INTEGER ; "
                        + "\n SET p3_out  = 144; \n SET p4_out  = 'CHARACTER11'; \n SELECT p3_out, p4_out; END");

        String sql = "{call MSQSPR100(1,'CHAR20',?,?)}";

        CallableStatement cs = this.conn.prepareCall(sql);

        cs.registerOutParameter(1, Types.INTEGER);
        cs.registerOutParameter(2, Types.CHAR);

        cs.execute();
        cs.close();

        createProcedure("bug43576_1", "(OUT nfact VARCHAR(100), IN ccuenta VARCHAR(100),\nOUT ffact VARCHAR(100),\nOUT fdoc VARCHAR(100))\nBEGIN"
                + "\nSET nfact = 'ncfact string';\nSET ffact = 'ffact string';\nSET fdoc = 'fdoc string';\nEND");

        createProcedure("bug43576_2", "(IN ccuent1 VARCHAR(100), IN ccuent2 VARCHAR(100),\nOUT nfact VARCHAR(100),\nOUT ffact VARCHAR(100),"
                + "\nOUT fdoc VARCHAR(100))\nBEGIN\nSET nfact = 'ncfact string';\nSET ffact = 'ffact string';\nSET fdoc = 'fdoc string';\nEND");

        Properties props = new Properties();
        props.put("jdbcCompliantTruncation", "true");
        props.put("useInformationSchema", "true");
        Connection conn1 = null;
        conn1 = getConnectionWithProps(props);
        try {
            CallableStatement callSt = conn1.prepareCall("{ call bug43576_1(?, ?, ?, ?) }");
            callSt.setString(2, "xxx");
            callSt.registerOutParameter(1, java.sql.Types.VARCHAR);
            callSt.registerOutParameter(3, java.sql.Types.VARCHAR);
            callSt.registerOutParameter(4, java.sql.Types.VARCHAR);
            callSt.execute();

            assertEquals("ncfact string", callSt.getString(1));
            assertEquals("ffact string", callSt.getString(3));
            assertEquals("fdoc string", callSt.getString(4));

            CallableStatement callSt2 = conn1.prepareCall("{ call bug43576_2(?, ?, ?, ?, ?) }");
            callSt2.setString(1, "xxx");
            callSt2.setString(2, "yyy");
            callSt2.registerOutParameter(3, java.sql.Types.VARCHAR);
            callSt2.registerOutParameter(4, java.sql.Types.VARCHAR);
            callSt2.registerOutParameter(5, java.sql.Types.VARCHAR);
            callSt2.execute();

            assertEquals("ncfact string", callSt2.getString(3));
            assertEquals("ffact string", callSt2.getString(4));
            assertEquals("fdoc string", callSt2.getString(5));

            CallableStatement callSt3 = conn1.prepareCall("{ call bug43576_2(?, 'yyy', ?, ?, ?) }");
            callSt3.setString(1, "xxx");
            // callSt3.setString(2, "yyy");
            callSt3.registerOutParameter(2, java.sql.Types.VARCHAR);
            callSt3.registerOutParameter(3, java.sql.Types.VARCHAR);
            callSt3.registerOutParameter(4, java.sql.Types.VARCHAR);
            callSt3.execute();

            assertEquals("ncfact string", callSt3.getString(2));
            assertEquals("ffact string", callSt3.getString(3));
            assertEquals("fdoc string", callSt3.getString(4));
        } finally {
            conn1.close();
        }
    }

    /**
     * Tests fix for Bug#57022 - cannot execute a store procedure with output
     * parameters Problem was in CallableStatement.java, private void
     * determineParameterTypes() throws SQLException if (procName.indexOf(".")
     * == -1) { useCatalog = true; } The fix will be to "sanitize" db.sp call
     * just like in noAccessToProcedureBodies.
     * 
     * @throws Exception
     *             if the test fails
     */

    public void testBug57022() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        String originalCatalog = this.conn.getCatalog();

        createDatabase("bug57022");

        createProcedure("bug57022.procbug57022", "(x int, out y int)\nbegin\ndeclare z int;\nset z = x+1, y = z;\nend\n");

        CallableStatement cStmt = null;
        try {
            cStmt = this.conn.prepareCall("{call `bug57022`.`procbug57022`(?, ?)}");
            cStmt.setInt(1, 5);
            cStmt.registerOutParameter(2, Types.INTEGER);

            cStmt.execute();
            assertEquals(6, cStmt.getInt(2));
            cStmt.clearParameters();
            cStmt.close();

            this.conn.setCatalog("bug57022");
            cStmt = this.conn.prepareCall("{call bug57022.procbug57022(?, ?)}");
            cStmt.setInt(1, 5);
            cStmt.registerOutParameter(2, Types.INTEGER);

            cStmt.execute();
            assertEquals(6, cStmt.getInt(2));
            cStmt.clearParameters();
            cStmt.close();

            this.conn.setCatalog("mysql");
            cStmt = this.conn.prepareCall("{call `bug57022`.`procbug57022`(?, ?)}");
            cStmt.setInt(1, 5);
            cStmt.registerOutParameter(2, Types.INTEGER);

            cStmt.execute();
            assertEquals(6, cStmt.getInt(2));
        } finally {
            if (cStmt != null) {
                cStmt.clearParameters();
                cStmt.close();
            }
            this.conn.setCatalog(originalCatalog);
        }

    }

    /**
     * Tests fix for BUG#60816 - Cannot pass NULL to an INOUT procedure parameter
     * 
     * @throws Exception
     */
    public void testBug60816() throws Exception {

        createProcedure("test60816_1", "(INOUT x INTEGER)\nBEGIN\nSET x = x + 1;\nEND");
        createProcedure("test60816_2", "(x INTEGER, OUT y INTEGER)\nBEGIN\nSET y = x + 1;\nEND");
        createProcedure("test60816_3", "(INOUT x INTEGER)\nBEGIN\nSET x = 10;\nEND");

        CallableStatement call = this.conn.prepareCall("{ call test60816_1(?) }");
        call.setInt(1, 1);
        call.registerOutParameter(1, Types.INTEGER);
        call.execute();
        assertEquals(2, call.getInt(1));

        call = this.conn.prepareCall("{ call test60816_2(?, ?) }");
        call.setInt(1, 1);
        call.registerOutParameter(2, Types.INTEGER);
        call.execute();
        assertEquals(2, call.getInt(2));

        call = this.conn.prepareCall("{ call test60816_2(?, ?) }");
        call.setNull(1, Types.INTEGER);
        call.registerOutParameter(2, Types.INTEGER);
        call.execute();
        assertEquals(0, call.getInt(2));
        assertTrue(call.wasNull());

        call = this.conn.prepareCall("{ call test60816_1(?) }");
        call.setNull(1, Types.INTEGER);
        call.registerOutParameter(1, Types.INTEGER);
        call.execute();
        assertEquals(0, call.getInt(1));
        assertTrue(call.wasNull());

        call = this.conn.prepareCall("{ call test60816_3(?) }");
        call.setNull(1, Types.INTEGER);
        call.registerOutParameter(1, Types.INTEGER);
        call.execute();
        assertEquals(10, call.getInt(1));
    }

    /**
     * Tests fix for Bug#79561 - NullPointerException when calling a fully qualified stored procedure
     */
    public void testBug79561() throws Exception {
        createProcedure("testBug79561", "(OUT o VARCHAR(100)) BEGIN SELECT 'testBug79561 data' INTO o; END");

        String dbName = this.conn.getCatalog();
        String[] sql = new String[] { String.format("{CALL %s.testBug79561(?)}", dbName), String.format("{CALL `%s`.testBug79561(?)}", dbName),
                String.format("{CALL %s.`testBug79561`(?)}", dbName), String.format("{CALL `%s`.`testBug79561`(?)}", dbName) };

        for (int i = 0; i < sql.length; i++) {
            for (int m = 0; m < 4; m++) { // Method call type: 0) by index; 1) by name; 2) by invalid index; 3) by invalid name;
                final String testCase = String.format("Case: [sql: %d, method: %d ]", i, m);
                final CallableStatement cstmt = this.conn.prepareCall(sql[i]);
                boolean dataExpected = true;

                // Register the output parameter using one of the different methods.
                if (m == 0) {
                    cstmt.registerOutParameter(1, Types.VARCHAR);
                } else if (m == 1) {
                    cstmt.registerOutParameter("o", Types.VARCHAR);
                } else if (m == 2) {
                    assertThrows(testCase, SQLException.class, "Parameter index of 2 is out of range \\(1, 1\\)", new Callable<Void>() {
                        public Void call() throws Exception {
                            cstmt.registerOutParameter(2, Types.VARCHAR);
                            return null;
                        }
                    });
                    dataExpected = false;
                } else {
                    assertThrows(testCase, SQLException.class, "No parameter named 'oparam'", new Callable<Void>() {
                        public Void call() throws Exception {
                            cstmt.registerOutParameter("oparam", Types.VARCHAR);
                            return null;
                        }
                    });
                    dataExpected = false;
                }

                // Check the returned data, if any expected, using different methods.
                if (dataExpected) {
                    cstmt.execute();
                    assertEquals(testCase, "testBug79561 data", cstmt.getString(1));
                    assertEquals(testCase, "testBug79561 data", cstmt.getString("o"));
                    assertThrows(testCase, SQLException.class, "Parameter index of 2 is out of range \\(1, 1\\)", new Callable<Void>() {
                        public Void call() throws Exception {
                            cstmt.getString(2);
                            return null;
                        }
                    });
                    assertThrows(testCase, SQLException.class, "Column '@com_mysql_jdbc_outparam_oparam' not found\\.", new Callable<Void>() {
                        public Void call() throws Exception {
                            cstmt.getString("oparam");
                            return null;
                        }
                    });
                }

                cstmt.close();
            }
        }
    }

    /**
     * Tests fix for Bug#84324 - CallableStatement.extractProcedureName() not work when catalog name with dash.
     */
    public void testBug84324() throws Exception {
        createDatabase("`testBug84324-db`");

        /*
         * Test procedure.
         */
        createProcedure("`testBug84324-db`.`testBug84324-proc`", "(IN a INT, INOUT b VARCHAR(100)) BEGIN SELECT a, b; END");

        final CallableStatement cstmtP = this.conn.prepareCall("CALL testBug84324-db.testBug84324-proc(?, ?)");
        ParameterMetaData pmd = cstmtP.getParameterMetaData();

        assertEquals(2, pmd.getParameterCount());
        // 1st parameter
        assertEquals("INT", pmd.getParameterTypeName(1));
        assertEquals(Types.INTEGER, pmd.getParameterType(1));
        assertEquals(Integer.class.getName(), pmd.getParameterClassName(1));
        assertEquals(ParameterMetaData.parameterModeIn, pmd.getParameterMode(1));
        // 2nd parameter
        assertEquals("VARCHAR", pmd.getParameterTypeName(2));
        assertEquals(Types.VARCHAR, pmd.getParameterType(2));
        assertEquals(String.class.getName(), pmd.getParameterClassName(2));
        assertEquals(ParameterMetaData.parameterModeInOut, pmd.getParameterMode(2));

        cstmtP.setInt(1, 1);
        cstmtP.setString(2, "foo");
        assertThrows(SQLException.class, new Callable<Void>() {
            public Void call() throws Exception {
                cstmtP.execute();
                return null;
            }
        }); // Although the procedure metadata could be obtained, the end query actually fails due to syntax errors.
        cstmtP.close();

        /*
         * Test function.
         */
        createFunction("`testBug84324-db`.`testBug84324-func`", "(a INT, b VARCHAR(123)) RETURNS INT DETERMINISTIC BEGIN RETURN a + LENGTH(b); END");

        final CallableStatement cstmtF = this.conn.prepareCall("{? = CALL testBug84324-db.testBug84324-func(?, ?)}");
        pmd = cstmtF.getParameterMetaData();

        assertEquals(3, pmd.getParameterCount());
        // 1st parameter
        assertEquals("INT", pmd.getParameterTypeName(1));
        assertEquals(Types.INTEGER, pmd.getParameterType(1));
        assertEquals(Integer.class.getName(), pmd.getParameterClassName(1));
        assertEquals(ParameterMetaData.parameterModeOut, pmd.getParameterMode(1));
        // 2nd parameter
        assertEquals("INT", pmd.getParameterTypeName(2));
        assertEquals(Types.INTEGER, pmd.getParameterType(2));
        assertEquals(Integer.class.getName(), pmd.getParameterClassName(2));
        assertEquals(ParameterMetaData.parameterModeIn, pmd.getParameterMode(2));
        // 3rd parameter
        assertEquals("VARCHAR", pmd.getParameterTypeName(3));
        assertEquals(Types.VARCHAR, pmd.getParameterType(3));
        assertEquals(String.class.getName(), pmd.getParameterClassName(3));
        assertEquals(ParameterMetaData.parameterModeIn, pmd.getParameterMode(3));

        cstmtF.registerOutParameter(1, Types.INTEGER);
        cstmtF.setInt(2, 1);
        cstmtF.setString(3, "foo");
        assertThrows(SQLException.class, new Callable<Void>() {
            public Void call() throws Exception {
                cstmtF.execute();
                return null;
            }
        }); // Although the function metadata could be obtained, the end query actually fails due to syntax errors.
        cstmtP.close();
        cstmtF.close();
    }

    /**
     * Tests fix for BUG#87704 (26771560) - THE STREAM GETS THE RESULT SET ?THE DRIVER SIDE GET WRONG ABOUT GETLONG().
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testBug87704() throws Exception {
        if (!serverSupportsStoredProcedures()) {
            return;
        }

        createProcedure("testBug87704",
                "(IN PARAMIN BIGINT, OUT PARAM_OUT_LONG BIGINT, OUT PARAM_OUT_STR VARCHAR(100))\nBEGIN\nSET PARAM_OUT_LONG = PARAMIN + 100000;\nSET PARAM_OUT_STR = concat('STR' ,PARAM_OUT_LONG);end\n");

        final Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        props.setProperty("useServerPrepStmts", "true");
        props.setProperty("cachePrepStmts", "true");
        props.setProperty("prepStmtCacheSize", "500");
        props.setProperty("prepStmtCacheSqlLimit", "2048");
        props.setProperty("useOldAliasMetadataBehavior", "true");
        props.setProperty("rewriteBatchedStatements", "true");
        props.setProperty("useCursorFetch", "true");
        props.setProperty("defaultFetchSize", "100");

        Connection con = getConnectionWithProps(props);

        CallableStatement callableStatement = null;
        try {
            callableStatement = con.prepareCall("call testBug87704(?,?,?)");
            callableStatement.setLong(1, 30214567L);
            callableStatement.registerOutParameter(2, Types.BIGINT);
            callableStatement.registerOutParameter(3, Types.VARCHAR);
            callableStatement.execute();
            System.out.println(callableStatement.getLong(2));
            System.out.println(callableStatement.getString(3));

            assertEquals(30314567L, callableStatement.getLong(2));
            assertEquals("STR30314567", callableStatement.getString(3));

        } finally {
            if (callableStatement != null) {
                callableStatement.close();
            }
            if (con != null) {
                con.close();
            }
        }
    }
}