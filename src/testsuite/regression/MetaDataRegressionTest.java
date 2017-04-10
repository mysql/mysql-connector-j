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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.mysql.jdbc.CharsetMapping;
import com.mysql.jdbc.ConnectionProperties;
import com.mysql.jdbc.Driver;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.Util;

import testsuite.BaseStatementInterceptor;
import testsuite.BaseTestCase;

import junit.framework.ComparisonFailure;

/**
 * Regression tests for DatabaseMetaData
 */
public class MetaDataRegressionTest extends BaseTestCase {
    /**
     * Creates a new MetaDataRegressionTest.
     * 
     * @param name
     *            the name of the test
     */
    public MetaDataRegressionTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(MetaDataRegressionTest.class);
    }

    public void testBug2607() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug2607");
            this.stmt.executeUpdate("CREATE TABLE testBug2607 (field1 INT PRIMARY KEY)");

            this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug2607");

            ResultSetMetaData rsmd = this.rs.getMetaData();

            assertTrue(!rsmd.isAutoIncrement(1));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug2607");
        }
    }

    /**
     * Tests fix for BUG#2852, where RSMD is not returning correct (or matching)
     * types for TINYINT and SMALLINT.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug2852() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug2852");
            this.stmt.executeUpdate("CREATE TABLE testBug2852 (field1 TINYINT, field2 SMALLINT)");
            this.stmt.executeUpdate("INSERT INTO testBug2852 VALUES (1,1)");

            this.rs = this.stmt.executeQuery("SELECT * from testBug2852");

            assertTrue(this.rs.next());

            ResultSetMetaData rsmd = this.rs.getMetaData();

            assertTrue(rsmd.getColumnClassName(1).equals(this.rs.getObject(1).getClass().getName()));
            assertTrue("java.lang.Integer".equals(rsmd.getColumnClassName(1)));

            assertTrue(rsmd.getColumnClassName(2).equals(this.rs.getObject(2).getClass().getName()));
            assertTrue("java.lang.Integer".equals(rsmd.getColumnClassName(2)));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug2852");
        }
    }

    /**
     * Tests fix for BUG#2855, where RSMD is not returning correct (or matching)
     * types for FLOAT.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug2855() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug2855");
            this.stmt.executeUpdate("CREATE TABLE testBug2855 (field1 FLOAT)");
            this.stmt.executeUpdate("INSERT INTO testBug2855 VALUES (1)");

            this.rs = this.stmt.executeQuery("SELECT * from testBug2855");

            assertTrue(this.rs.next());

            ResultSetMetaData rsmd = this.rs.getMetaData();

            assertTrue(rsmd.getColumnClassName(1).equals(this.rs.getObject(1).getClass().getName()));
            assertTrue("java.lang.Float".equals(rsmd.getColumnClassName(1)));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug2855");
        }
    }

    /**
     * Tests fix for BUG#3570 -- inconsistent reporting of column type
     * 
     * @throws Exception
     *             if an error occurs
     */
    public void testBug3570() throws Exception {
        String createTableQuery = " CREATE TABLE testBug3570(field_tinyint TINYINT,field_smallint SMALLINT,field_mediumint MEDIUMINT"
                + ",field_int INT,field_integer INTEGER,field_bigint BIGINT,field_real REAL,field_float FLOAT,field_decimal DECIMAL"
                + ",field_numeric NUMERIC,field_double DOUBLE,field_char CHAR(3),field_varchar VARCHAR(255),field_date DATE"
                + ",field_time TIME,field_year YEAR,field_timestamp TIMESTAMP,field_datetime DATETIME,field_tinyblob TINYBLOB"
                + ",field_blob BLOB,field_mediumblob MEDIUMBLOB,field_longblob LONGBLOB,field_tinytext TINYTEXT,field_text TEXT"
                + ",field_mediumtext MEDIUMTEXT,field_longtext LONGTEXT,field_enum ENUM('1','2','3'),field_set SET('1','2','3'))";

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3570");
            this.stmt.executeUpdate(createTableQuery);

            ResultSet dbmdRs = this.conn.getMetaData().getColumns(this.conn.getCatalog(), null, "testBug3570", "%");

            this.rs = this.stmt.executeQuery("SELECT * FROM testBug3570");

            ResultSetMetaData rsmd = this.rs.getMetaData();

            while (dbmdRs.next()) {
                String columnName = dbmdRs.getString(4);
                int typeFromGetColumns = dbmdRs.getInt(5);
                int typeFromRSMD = rsmd.getColumnType(this.rs.findColumn(columnName));

                //
                // TODO: Server needs to send these types correctly....
                //
                if (!"field_tinyblob".equals(columnName) && !"field_tinytext".equals(columnName)) {
                    assertTrue(columnName + " -> type from DBMD.getColumns(" + typeFromGetColumns + ") != type from RSMD.getColumnType(" + typeFromRSMD + ")",
                            typeFromGetColumns == typeFromRSMD);
                }
            }
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3570");
        }
    }

    /**
     * Tests char/varchar bug
     * 
     * @throws Exception
     *             if any errors occur
     */
    public void testCharVarchar() throws Exception {
        try {
            this.stmt.execute("DROP TABLE IF EXISTS charVarCharTest");
            this.stmt.execute("CREATE TABLE charVarCharTest (  TableName VARCHAR(64),  FieldName VARCHAR(64),  NextCounter INTEGER);");

            String query = "SELECT TableName, FieldName, NextCounter FROM charVarCharTest";
            this.rs = this.stmt.executeQuery(query);

            ResultSetMetaData rsmeta = this.rs.getMetaData();

            assertTrue(rsmeta.getColumnTypeName(1).equalsIgnoreCase("VARCHAR"));

            // is "CHAR", expected "VARCHAR"
            assertTrue(rsmeta.getColumnType(1) == 12);

            // is 1 (java.sql.Types.CHAR), expected 12 (java.sql.Types.VARCHAR)
        } finally {
            this.stmt.execute("DROP TABLE IF EXISTS charVarCharTest");
        }
    }

    /**
     * Tests fix for BUG#1673, where DatabaseMetaData.getColumns() is not
     * returning correct column ordinal info for non '%' column name patterns.
     * 
     * @throws Exception
     *             if the test fails for any reason
     */
    public void testFixForBug1673() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1673");
            this.stmt.executeUpdate("CREATE TABLE testBug1673 (field_1 INT, field_2 INT)");

            DatabaseMetaData dbmd = this.conn.getMetaData();

            int ordinalPosOfCol2Full = 0;

            this.rs = dbmd.getColumns(this.conn.getCatalog(), null, "testBug1673", null);

            while (this.rs.next()) {
                if (this.rs.getString(4).equals("field_2")) {
                    ordinalPosOfCol2Full = this.rs.getInt(17);
                }
            }

            int ordinalPosOfCol2Scoped = 0;

            this.rs = dbmd.getColumns(this.conn.getCatalog(), null, "testBug1673", "field_2");

            while (this.rs.next()) {
                if (this.rs.getString(4).equals("field_2")) {
                    ordinalPosOfCol2Scoped = this.rs.getInt(17);
                }
            }

            assertTrue("Ordinal position in full column list of '" + ordinalPosOfCol2Full + "' != ordinal position in pattern search, '"
                    + ordinalPosOfCol2Scoped + "'.",
                    (ordinalPosOfCol2Full != 0) && (ordinalPosOfCol2Scoped != 0) && (ordinalPosOfCol2Scoped == ordinalPosOfCol2Full));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1673");
        }
    }

    /**
     * Tests bug reported by OpenOffice team with getColumns and LONGBLOB
     * 
     * @throws Exception
     *             if any errors occur
     */
    public void testGetColumns() throws Exception {
        try {
            this.stmt.execute("CREATE TABLE IF NOT EXISTS longblob_regress(field_1 longblob)");

            DatabaseMetaData dbmd = this.conn.getMetaData();
            ResultSet dbmdRs = null;

            try {
                dbmdRs = dbmd.getColumns("", "", "longblob_regress", "%");

                while (dbmdRs.next()) {
                    dbmdRs.getInt(7);
                }
            } finally {
                if (dbmdRs != null) {
                    try {
                        dbmdRs.close();
                    } catch (SQLException ex) {
                    }
                }
            }
        } finally {
            this.stmt.execute("DROP TABLE IF EXISTS longblob_regress");
        }
    }

    /**
     * Tests fix for Bug#
     * 
     * @throws Exception
     *             if an error occurs
     */
    public void testGetColumnsBug1099() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testGetColumnsBug1099");

            DatabaseMetaData dbmd = this.conn.getMetaData();

            this.rs = dbmd.getTypeInfo();

            StringBuilder types = new StringBuilder();

            HashMap<String, String> alreadyDoneTypes = new HashMap<String, String>();

            while (this.rs.next()) {
                String typeName = this.rs.getString("TYPE_NAME");
                //String createParams = this.rs.getString("CREATE_PARAMS");

                if ((typeName.indexOf("BINARY") == -1) && !typeName.equals("LONG VARCHAR")) {
                    if (!alreadyDoneTypes.containsKey(typeName)) {
                        alreadyDoneTypes.put(typeName, null);

                        if (types.length() != 0) {
                            types.append(", \n");
                        }

                        int typeNameLength = typeName.length();
                        StringBuilder safeTypeName = new StringBuilder(typeNameLength);

                        for (int i = 0; i < typeNameLength; i++) {
                            char c = typeName.charAt(i);

                            if (Character.isWhitespace(c)) {
                                safeTypeName.append("_");
                            } else {
                                safeTypeName.append(c);
                            }
                        }

                        types.append(safeTypeName.toString());
                        types.append("Column ");
                        types.append(typeName);

                        if (typeName.indexOf("CHAR") != -1) {
                            types.append(" (1)");
                        } else if (typeName.equalsIgnoreCase("enum") || typeName.equalsIgnoreCase("set")) {
                            types.append("('a', 'b', 'c')");
                        }
                    }
                }
            }

            this.stmt.executeUpdate("CREATE TABLE testGetColumnsBug1099(" + types.toString() + ")");

            dbmd.getColumns(null, this.conn.getCatalog(), "testGetColumnsBug1099", "%");
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testGetColumnsBug1099");
        }
    }

    /**
     * Tests whether or not unsigned columns are reported correctly in
     * DBMD.getColumns
     * 
     * @throws Exception
     */
    public void testGetColumnsUnsigned() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testGetUnsignedCols");
            this.stmt.executeUpdate("CREATE TABLE testGetUnsignedCols (field1 BIGINT, field2 BIGINT UNSIGNED)");

            DatabaseMetaData dbmd = this.conn.getMetaData();

            this.rs = dbmd.getColumns(this.conn.getCatalog(), null, "testGetUnsignedCols", "%");

            assertTrue(this.rs.next());
            // This row doesn't have 'unsigned' attribute
            assertTrue(this.rs.next());
            assertTrue(this.rs.getString(6).toLowerCase().indexOf("unsigned") != -1);
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testGetUnsignedCols");
        }
    }

    /**
     * Tests whether bogus parameters break Driver.getPropertyInfo().
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testGetPropertyInfo() throws Exception {
        new Driver().getPropertyInfo("", null);
    }

    /**
     * Tests whether ResultSetMetaData returns correct info for CHAR/VARCHAR
     * columns.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testIsCaseSensitive() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testIsCaseSensitive");
            this.stmt.executeUpdate(
                    "CREATE TABLE testIsCaseSensitive (bin_char CHAR(1) BINARY, bin_varchar VARCHAR(64) BINARY, ci_char CHAR(1), ci_varchar VARCHAR(64))");
            this.rs = this.stmt.executeQuery("SELECT bin_char, bin_varchar, ci_char, ci_varchar FROM testIsCaseSensitive");

            ResultSetMetaData rsmd = this.rs.getMetaData();
            assertTrue(rsmd.isCaseSensitive(1));
            assertTrue(rsmd.isCaseSensitive(2));
            assertTrue(!rsmd.isCaseSensitive(3));
            assertTrue(!rsmd.isCaseSensitive(4));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testIsCaseSensitive");
        }

        if (versionMeetsMinimum(4, 1)) {
            try {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS testIsCaseSensitiveCs");
                this.stmt.executeUpdate("CREATE TABLE testIsCaseSensitiveCs (bin_char CHAR(1) CHARACTER SET latin1 COLLATE latin1_general_cs,"
                        + "bin_varchar VARCHAR(64) CHARACTER SET latin1 COLLATE latin1_general_cs,"
                        + "ci_char CHAR(1) CHARACTER SET latin1 COLLATE latin1_general_ci,"
                        + "ci_varchar VARCHAR(64) CHARACTER SET latin1 COLLATE latin1_general_ci, "
                        + "bin_tinytext TINYTEXT CHARACTER SET latin1 COLLATE latin1_general_cs,"
                        + "bin_text TEXT CHARACTER SET latin1 COLLATE latin1_general_cs,"
                        + "bin_med_text MEDIUMTEXT CHARACTER SET latin1 COLLATE latin1_general_cs,"
                        + "bin_long_text LONGTEXT CHARACTER SET latin1 COLLATE latin1_general_cs,"
                        + "ci_tinytext TINYTEXT CHARACTER SET latin1 COLLATE latin1_general_ci,"
                        + "ci_text TEXT CHARACTER SET latin1 COLLATE latin1_general_ci,"
                        + "ci_med_text MEDIUMTEXT CHARACTER SET latin1 COLLATE latin1_general_ci,"
                        + "ci_long_text LONGTEXT CHARACTER SET latin1 COLLATE latin1_general_ci)");

                this.rs = this.stmt.executeQuery("SELECT bin_char, bin_varchar, ci_char, ci_varchar, bin_tinytext, bin_text, bin_med_text, bin_long_text, "
                        + "ci_tinytext, ci_text, ci_med_text, ci_long_text FROM testIsCaseSensitiveCs");

                ResultSetMetaData rsmd = this.rs.getMetaData();
                assertTrue(rsmd.isCaseSensitive(1));
                assertTrue(rsmd.isCaseSensitive(2));
                assertTrue(!rsmd.isCaseSensitive(3));
                assertTrue(!rsmd.isCaseSensitive(4));

                assertTrue(rsmd.isCaseSensitive(5));
                assertTrue(rsmd.isCaseSensitive(6));
                assertTrue(rsmd.isCaseSensitive(7));
                assertTrue(rsmd.isCaseSensitive(8));

                assertTrue(!rsmd.isCaseSensitive(9));
                assertTrue(!rsmd.isCaseSensitive(10));
                assertTrue(!rsmd.isCaseSensitive(11));
                assertTrue(!rsmd.isCaseSensitive(12));
            } finally {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS testIsCaseSensitiveCs");
            }
        }
    }

    /**
     * Tests whether or not DatabaseMetaData.getColumns() returns the correct
     * java.sql.Types info.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testLongText() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testLongText");
            this.stmt.executeUpdate("CREATE TABLE testLongText (field1 LONGTEXT)");

            this.rs = this.conn.getMetaData().getColumns(this.conn.getCatalog(), null, "testLongText", "%");

            assertTrue(this.rs.next());

            assertTrue(this.rs.getInt("DATA_TYPE") == java.sql.Types.LONGVARCHAR);
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testLongText");
        }
    }

    /**
     * Tests for types being returned correctly
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testTypes() throws Exception {
        try {
            this.stmt.execute("DROP TABLE IF EXISTS typesRegressTest");
            this.stmt.execute("CREATE TABLE typesRegressTest (varcharField VARCHAR(32), charField CHAR(2), enumField ENUM('1','2'),"
                    + "setField  SET('1','2','3'), tinyblobField TINYBLOB, mediumBlobField MEDIUMBLOB, longblobField LONGBLOB, blobField BLOB)");

            this.rs = this.stmt.executeQuery("SELECT * from typesRegressTest");

            ResultSetMetaData rsmd = this.rs.getMetaData();

            int numCols = rsmd.getColumnCount();

            for (int i = 0; i < numCols; i++) {
                String columnName = rsmd.getColumnName(i + 1);
                String columnTypeName = rsmd.getColumnTypeName(i + 1);
                System.out.println(columnName + " -> " + columnTypeName);
            }
        } finally {
            this.stmt.execute("DROP TABLE IF EXISTS typesRegressTest");
        }
    }

    /**
     * Tests fix for BUG#4742, 'DOUBLE' mapped twice in getTypeInfo().
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug4742() throws Exception {
        HashMap<String, String> clashMap = new HashMap<String, String>();

        this.rs = this.conn.getMetaData().getTypeInfo();

        while (this.rs.next()) {
            String name = this.rs.getString(1);
            assertTrue("Type represented twice in type info, '" + name + "'.", !clashMap.containsKey(name));
            clashMap.put(name, name);
        }
    }

    /**
     * Tests fix for BUG#4138, getColumns() returns incorrect JDBC type for
     * unsigned columns.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug4138() throws Exception {
        try {
            String[] typesToTest = new String[] { "TINYINT", "SMALLINT", "MEDIUMINT", "INT", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL" };

            short[] jdbcMapping = new short[] { Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.INTEGER, Types.BIGINT, Types.REAL, Types.DOUBLE,
                    Types.DECIMAL };

            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4138");

            StringBuilder createBuf = new StringBuilder();

            createBuf.append("CREATE TABLE testBug4138 (");

            boolean firstColumn = true;

            for (int i = 0; i < typesToTest.length; i++) {
                if (!firstColumn) {
                    createBuf.append(", ");
                } else {
                    firstColumn = false;
                }

                createBuf.append("field");
                createBuf.append((i + 1));
                createBuf.append(" ");
                createBuf.append(typesToTest[i]);
                createBuf.append(" UNSIGNED");
            }
            createBuf.append(")");
            this.stmt.executeUpdate(createBuf.toString());

            DatabaseMetaData dbmd = this.conn.getMetaData();
            this.rs = dbmd.getColumns(this.conn.getCatalog(), null, "testBug4138", "field%");

            assertTrue(this.rs.next());

            for (int i = 0; i < typesToTest.length; i++) {
                assertTrue(
                        "JDBC Data Type of " + this.rs.getShort("DATA_TYPE") + " for MySQL type '" + this.rs.getString("TYPE_NAME")
                                + "' from 'DATA_TYPE' column does not match expected value of " + jdbcMapping[i] + ".",
                        jdbcMapping[i] == this.rs.getShort("DATA_TYPE"));
                this.rs.next();
            }

            this.rs.close();

            StringBuilder queryBuf = new StringBuilder("SELECT ");
            firstColumn = true;

            for (int i = 0; i < typesToTest.length; i++) {
                if (!firstColumn) {
                    queryBuf.append(", ");
                } else {
                    firstColumn = false;
                }

                queryBuf.append("field");
                queryBuf.append((i + 1));
            }

            queryBuf.append(" FROM testBug4138");

            this.rs = this.stmt.executeQuery(queryBuf.toString());

            ResultSetMetaData rsmd = this.rs.getMetaData();

            for (int i = 0; i < typesToTest.length; i++) {

                assertTrue(jdbcMapping[i] == rsmd.getColumnType(i + 1));
                String desiredTypeName = typesToTest[i] + " unsigned";

                assertTrue(rsmd.getColumnTypeName((i + 1)) + " != " + desiredTypeName, desiredTypeName.equalsIgnoreCase(rsmd.getColumnTypeName(i + 1)));
            }
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4138");
        }
    }

    /**
     * Here for housekeeping only, the test is actually in testBug4138().
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug4860() throws Exception {
        testBug4138();
    }

    /**
     * Tests fix for BUG#4880 - RSMD.getPrecision() returns '0' for non-numeric
     * types.
     * 
     * Why-oh-why is this not in the spec, nor the api-docs, but in some
     * 'optional' book, _and_ it is a variance from both ODBC and the ANSI SQL
     * standard :p
     * 
     * (from the CTS testsuite)....
     * 
     * The getPrecision(int colindex) method returns an integer value
     * representing the number of decimal digits for number types,maximum length
     * in characters for character types,maximum length in bytes for JDBC binary
     * datatypes.
     * 
     * (See Section 27.3 of JDBC 2.0 API Reference & Tutorial 2nd edition)
     * 
     * @throws Exception
     *             if the test fails.
     */

    public void testBug4880() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4880");
            this.stmt.executeUpdate("CREATE TABLE testBug4880 (field1 VARCHAR(80), field2 TINYBLOB, field3 BLOB, field4 MEDIUMBLOB, field5 LONGBLOB)");
            this.rs = this.stmt.executeQuery("SELECT field1, field2, field3, field4, field5 FROM testBug4880");
            ResultSetMetaData rsmd = this.rs.getMetaData();

            assertEquals(80, rsmd.getPrecision(1));
            assertEquals(Types.VARCHAR, rsmd.getColumnType(1));
            assertEquals(80, rsmd.getColumnDisplaySize(1));

            assertEquals(255, rsmd.getPrecision(2));
            assertEquals(Types.VARBINARY, rsmd.getColumnType(2));
            assertTrue("TINYBLOB".equalsIgnoreCase(rsmd.getColumnTypeName(2)));
            assertEquals(255, rsmd.getColumnDisplaySize(2));

            assertEquals(65535, rsmd.getPrecision(3));
            assertEquals(Types.LONGVARBINARY, rsmd.getColumnType(3));
            assertTrue("BLOB".equalsIgnoreCase(rsmd.getColumnTypeName(3)));
            assertEquals(65535, rsmd.getColumnDisplaySize(3));

            assertEquals(16777215, rsmd.getPrecision(4));
            assertEquals(Types.LONGVARBINARY, rsmd.getColumnType(4));
            assertTrue("MEDIUMBLOB".equalsIgnoreCase(rsmd.getColumnTypeName(4)));
            assertEquals(16777215, rsmd.getColumnDisplaySize(4));

            if (versionMeetsMinimum(4, 1)) {
                // Server doesn't send us enough information to detect LONGBLOB
                // type
                assertEquals(Integer.MAX_VALUE, rsmd.getPrecision(5));
                assertEquals(Types.LONGVARBINARY, rsmd.getColumnType(5));
                assertTrue("LONGBLOB".equalsIgnoreCase(rsmd.getColumnTypeName(5)));
                assertEquals(Integer.MAX_VALUE, rsmd.getColumnDisplaySize(5));
            }
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4880");
        }
    }

    /**
     * Tests fix for BUG#6399, ResultSetMetaData.getDisplaySize() is wrong for
     * multi-byte charsets.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug6399() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            try {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug6399");
                this.stmt.executeUpdate(
                        "CREATE TABLE testBug6399 (field1 CHAR(3) CHARACTER SET UTF8, field2 CHAR(3) CHARACTER SET LATIN1, field3 CHAR(3) CHARACTER SET SJIS)");
                this.stmt.executeUpdate("INSERT INTO testBug6399 VALUES ('a', 'a', 'a')");

                this.rs = this.stmt.executeQuery("SELECT field1, field2, field3 FROM testBug6399");
                ResultSetMetaData rsmd = this.rs.getMetaData();

                assertEquals(3, rsmd.getColumnDisplaySize(1));
                assertEquals(3, rsmd.getColumnDisplaySize(2));
                assertEquals(3, rsmd.getColumnDisplaySize(3));
            } finally {
                this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug6399");
            }
        }
    }

    /**
     * Tests fix for BUG#7081, DatabaseMetaData.getIndexInfo() ignoring 'unique'
     * parameters.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug7081() throws Exception {
        String tableName = "testBug7081";

        try {
            createTable(tableName, "(field1 INT, INDEX(field1))");

            DatabaseMetaData dbmd = this.conn.getMetaData();
            this.rs = dbmd.getIndexInfo(this.conn.getCatalog(), null, tableName, true, false);
            assertTrue(!this.rs.next()); // there should be no rows that meet
            // this requirement

            this.rs = dbmd.getIndexInfo(this.conn.getCatalog(), null, tableName, false, false);
            assertTrue(this.rs.next()); // there should be one row that meets
            // this requirement
            assertTrue(!this.rs.next());

        } finally {
            dropTable(tableName);
        }
    }

    /**
     * Tests fix for BUG#7033 - PreparedStatements don't encode Big5 (and other
     * multibyte) character sets correctly in static SQL strings.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug7033() throws Exception {
        if (!this.DISABLED_testBug7033) {
            Connection big5Conn = null;
            Statement big5Stmt = null;
            PreparedStatement big5PrepStmt = null;

            String testString = "\u5957 \u9910";

            try {
                Properties props = new Properties();
                props.setProperty("useUnicode", "true");
                props.setProperty("characterEncoding", "Big5");

                big5Conn = getConnectionWithProps(props);
                big5Stmt = big5Conn.createStatement();

                byte[] foobar = testString.getBytes("Big5");
                System.out.println(Arrays.toString(foobar));

                this.rs = big5Stmt.executeQuery("select 1 as '\u5957 \u9910'");
                String retrString = this.rs.getMetaData().getColumnName(1);
                assertTrue(testString.equals(retrString));

                big5PrepStmt = big5Conn.prepareStatement("select 1 as '\u5957 \u9910'");
                this.rs = big5PrepStmt.executeQuery();
                retrString = this.rs.getMetaData().getColumnName(1);
                assertTrue(testString.equals(retrString));
            } finally {
                if (this.rs != null) {
                    this.rs.close();
                    this.rs = null;
                }

                if (big5Stmt != null) {
                    big5Stmt.close();

                }

                if (big5PrepStmt != null) {
                    big5PrepStmt.close();
                }

                if (big5Conn != null) {
                    big5Conn.close();
                }
            }
        }
    }

    /**
     * Tests fix for Bug#8812, DBMD.getIndexInfo() returning inverted values for
     * 'NON_UNIQUE' column.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug8812() throws Exception {
        String tableName = "testBug8812";

        try {
            createTable(tableName, "(field1 INT, field2 INT, INDEX(field1), UNIQUE INDEX(field2))");

            DatabaseMetaData dbmd = this.conn.getMetaData();
            this.rs = dbmd.getIndexInfo(this.conn.getCatalog(), null, tableName, true, false);
            assertTrue(this.rs.next()); // there should be one row that meets
            // this requirement
            assertEquals(this.rs.getBoolean("NON_UNIQUE"), false);

            this.rs = dbmd.getIndexInfo(this.conn.getCatalog(), null, tableName, false, false);
            assertTrue(this.rs.next()); // there should be two rows that meets
            // this requirement
            assertEquals(this.rs.getBoolean("NON_UNIQUE"), false);
            assertTrue(this.rs.next());
            assertEquals(this.rs.getBoolean("NON_UNIQUE"), true);

        } finally {
            dropTable(tableName);
        }
    }

    /**
     * Tests fix for BUG#8800 - supportsMixedCase*Identifiers() returns wrong
     * value on servers running on case-sensitive filesystems.
     */

    public void testBug8800() throws Exception {
        assertEquals(((com.mysql.jdbc.Connection) this.conn).lowerCaseTableNames(), !this.conn.getMetaData().supportsMixedCaseIdentifiers());
        assertEquals(((com.mysql.jdbc.Connection) this.conn).lowerCaseTableNames(), !this.conn.getMetaData().supportsMixedCaseQuotedIdentifiers());

    }

    /**
     * Tests fix for BUG#8792 - DBMD.supportsResultSetConcurrency() not
     * returning true for forward-only/read-only result sets (we obviously
     * support this).
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug8792() throws Exception {
        DatabaseMetaData dbmd = this.conn.getMetaData();

        assertTrue(dbmd.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY));

        assertTrue(dbmd.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE));

        assertTrue(dbmd.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY));

        assertTrue(dbmd.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE));

        assertTrue(!dbmd.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY));

        assertTrue(!dbmd.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE));

        // Check error conditions
        try {
            dbmd.supportsResultSetConcurrency(ResultSet.TYPE_FORWARD_ONLY, Integer.MIN_VALUE);
            fail("Exception should've been raised for bogus concurrency value");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
        }

        try {
            assertTrue(dbmd.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, Integer.MIN_VALUE));
            fail("Exception should've been raised for bogus concurrency value");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
        }

        try {
            assertTrue(dbmd.supportsResultSetConcurrency(Integer.MIN_VALUE, Integer.MIN_VALUE));
            fail("Exception should've been raised for bogus concurrency value");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
        }
    }

    /**
     * Tests fix for BUG#8803, 'DATA_TYPE' column from
     * DBMD.getBestRowIdentifier() causes ArrayIndexOutOfBoundsException when
     * accessed (and in fact, didn't return any value).
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug8803() throws Exception {
        String tableName = "testBug8803";
        createTable(tableName, "(field1 INT NOT NULL PRIMARY KEY)");
        DatabaseMetaData metadata = this.conn.getMetaData();
        try {
            this.rs = metadata.getBestRowIdentifier(this.conn.getCatalog(), null, tableName, DatabaseMetaData.bestRowNotPseudo, true);

            assertTrue(this.rs.next());

            this.rs.getInt("DATA_TYPE"); // **** Fails here *****
        } finally {
            if (this.rs != null) {
                this.rs.close();

                this.rs = null;
            }
        }

    }

    /**
     * Tests fix for BUG#9320 - PreparedStatement.getMetaData() inserts blank
     * row in database under certain conditions when not using server-side
     * prepared statements.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug9320() throws Exception {
        createTable("testBug9320", "(field1 int)");

        testAbsenceOfMetadataForQuery("INSERT INTO testBug9320 VALUES (?)");
        testAbsenceOfMetadataForQuery("UPDATE testBug9320 SET field1=?");
        testAbsenceOfMetadataForQuery("DELETE FROM testBug9320 WHERE field1=?");
    }

    /**
     * Tests fix for BUG#9778, DBMD.getTables() shouldn't return tables if views
     * are asked for, even if the database version doesn't support views.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug9778() throws Exception {
        String tableName = "testBug9778";

        try {
            createTable(tableName, "(field1 int)");
            this.rs = this.conn.getMetaData().getTables(null, null, tableName, new String[] { "VIEW" });
            assertEquals(false, this.rs.next());

            this.rs = this.conn.getMetaData().getTables(null, null, tableName, new String[] { "TABLE" });
            assertEquals(true, this.rs.next());
        } finally {
            if (this.rs != null) {
                this.rs.close();
                this.rs = null;
            }
        }
    }

    /**
     * Tests fix for BUG#9769 - Should accept null for procedureNamePattern,
     * even though it isn't JDBC compliant, for legacy's sake.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug9769() throws Exception {
        boolean defaultPatternConfig = ((com.mysql.jdbc.Connection) this.conn).getNullNamePatternMatchesAll();

        // We're going to change this in 3.2.x, so make that test here, so we
        // catch it.

        if (this.conn.getMetaData().getDriverMajorVersion() == 3 && this.conn.getMetaData().getDriverMinorVersion() >= 2) {
            assertEquals(false, defaultPatternConfig);
        } else {
            assertEquals(true, defaultPatternConfig);
        }

        try {
            this.conn.getMetaData().getProcedures(this.conn.getCatalog(), "%", null);

            if (!defaultPatternConfig) {
                // we shouldn't have gotten here
                fail("Exception should've been thrown");
            }
        } catch (SQLException sqlEx) {
            if (!defaultPatternConfig) {
                assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());
            } else {
                throw sqlEx; // we shouldn't have gotten an exception here
            }
        }

        // FIXME: TO test for 3.1.9
        // getColumns();
        // getTablePrivileges();
        // getTables();

    }

    /**
     * Tests fix for BUG#9917 - Should accept null for catalog in DBMD methods,
     * even though it's not JDBC-compliant for legacy's sake.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug9917() throws Exception {
        String tableName = "testBug9917";
        boolean defaultCatalogConfig = ((com.mysql.jdbc.Connection) this.conn).getNullCatalogMeansCurrent();

        // We're going to change this in 3.2.x, so make that test here, so we
        // catch it.

        if (this.conn.getMetaData().getDriverMajorVersion() == 3 && this.conn.getMetaData().getDriverMinorVersion() >= 2) {
            assertEquals(false, defaultCatalogConfig);
        } else {
            assertEquals(true, defaultCatalogConfig);
        }

        try {
            createTable(tableName, "(field1 int)");
            String currentCatalog = this.conn.getCatalog();

            try {
                this.rs = this.conn.getMetaData().getTables(null, null, tableName, new String[] { "TABLE" });

                if (!defaultCatalogConfig) {
                    // we shouldn't have gotten here
                    fail("Exception should've been thrown");
                }

                assertEquals(true, this.rs.next());
                assertEquals(currentCatalog, this.rs.getString("TABLE_CAT"));

                // FIXME: Methods to test for 3.1.9
                //
                // getBestRowIdentifier()
                // getColumns()
                // getCrossReference()
                // getExportedKeys()
                // getImportedKeys()
                // getIndexInfo()
                // getPrimaryKeys()
                // getProcedures()

            } catch (SQLException sqlEx) {
                if (!defaultCatalogConfig) {
                    assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());
                } else {
                    throw sqlEx; // we shouldn't have gotten an exception
                    // here
                }
            }

        } finally {
            if (this.rs != null) {
                this.rs.close();
                this.rs = null;
            }
        }
    }

    /**
     * Tests fix for BUG#11575 -- DBMD.storesLower/Mixed/UpperIdentifiers()
     * reports incorrect values for servers deployed on Windows.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug11575() throws Exception {
        DatabaseMetaData dbmd = this.conn.getMetaData();

        if (isServerRunningOnWindows()) {
            assertEquals(true, dbmd.storesLowerCaseIdentifiers());
            assertEquals(true, dbmd.storesLowerCaseQuotedIdentifiers());
            assertEquals(false, dbmd.storesMixedCaseIdentifiers());
            assertEquals(false, dbmd.storesMixedCaseQuotedIdentifiers());
            assertEquals(false, dbmd.storesUpperCaseIdentifiers());
            assertEquals(true, dbmd.storesUpperCaseQuotedIdentifiers());
        } else {
            assertEquals(false, dbmd.storesLowerCaseIdentifiers());
            assertEquals(false, dbmd.storesLowerCaseQuotedIdentifiers());
            assertEquals(true, dbmd.storesMixedCaseIdentifiers());
            assertEquals(true, dbmd.storesMixedCaseQuotedIdentifiers());
            assertEquals(false, dbmd.storesUpperCaseIdentifiers());
            assertEquals(true, dbmd.storesUpperCaseQuotedIdentifiers());
        }
    }

    /**
     * Tests fix for BUG#11781, foreign key information that is quoted is parsed
     * incorrectly.
     */
    public void testBug11781() throws Exception {

        if (versionMeetsMinimum(5, 1)) {
            if (!versionMeetsMinimum(5, 2)) {
                // server bug prevents this test from functioning

                return;
            }
        }

        createTable("`app tab`", "( C1 int(11) NULL, C2 int(11) NULL, INDEX NEWINX (C1), INDEX NEWINX2 (C1, C2))", "InnoDB");

        this.stmt.executeUpdate("ALTER TABLE `app tab` ADD CONSTRAINT APPFK FOREIGN KEY (C1) REFERENCES `app tab` (C1)");

        /*
         * this.rs = this.conn.getMetaData().getCrossReference(
         * this.conn.getCatalog(), null, "app tab", this.conn.getCatalog(),
         * null, "app tab");
         */
        this.rs = ((com.mysql.jdbc.DatabaseMetaData) this.conn.getMetaData()).extractForeignKeyFromCreateTable(this.conn.getCatalog(), "app tab");
        assertTrue("must return a row", this.rs.next());

        String catalog = this.conn.getCatalog();

        assertEquals("comment; APPFK(`C1`) REFER `" + catalog + "`/ `app tab` (`C1`)", this.rs.getString(3));

        this.rs.close();

        this.rs = this.conn.getMetaData().getImportedKeys(this.conn.getCatalog(), null, "app tab");

        assertTrue(this.rs.next());

        this.rs = this.conn.getMetaData().getExportedKeys(this.conn.getCatalog(), null, "app tab");

        assertTrue(this.rs.next());
    }

    /**
     * Tests fix for BUG#12970 - java.sql.Types.OTHER returned for binary and
     * varbinary columns.
     */
    public void testBug12970() throws Exception {
        if (versionMeetsMinimum(5, 0, 8)) {
            String tableName = "testBug12970";

            createTable(tableName, "(binary_field BINARY(32), varbinary_field VARBINARY(64))");

            try {
                this.rs = this.conn.getMetaData().getColumns(this.conn.getCatalog(), null, tableName, "%");
                assertTrue(this.rs.next());
                assertEquals(Types.BINARY, this.rs.getInt("DATA_TYPE"));
                assertEquals(32, this.rs.getInt("COLUMN_SIZE"));
                assertTrue(this.rs.next());
                assertEquals(Types.VARBINARY, this.rs.getInt("DATA_TYPE"));
                assertEquals(64, this.rs.getInt("COLUMN_SIZE"));
                this.rs.close();

                this.rs = this.stmt.executeQuery("SELECT binary_field, varbinary_field FROM " + tableName);
                ResultSetMetaData rsmd = this.rs.getMetaData();
                assertEquals(Types.BINARY, rsmd.getColumnType(1));
                assertEquals(32, rsmd.getPrecision(1));
                assertEquals(Types.VARBINARY, rsmd.getColumnType(2));
                assertEquals(64, rsmd.getPrecision(2));
                this.rs.close();
            } finally {
                if (this.rs != null) {
                    this.rs.close();
                }
            }
        }
    }

    /**
     * Tests fix for BUG#12975 - OpenOffice expects DBMD.supportsIEF() to return
     * "true" if foreign keys are supported by the datasource, even though this
     * method also covers support for check constraints, which MySQL _doesn't_
     * have.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug12975() throws Exception {
        assertEquals(false, this.conn.getMetaData().supportsIntegrityEnhancementFacility());

        Connection overrideConn = null;

        try {
            Properties props = new Properties();

            props.setProperty("overrideSupportsIntegrityEnhancementFacility", "true");

            overrideConn = getConnectionWithProps(props);
            assertEquals(true, overrideConn.getMetaData().supportsIntegrityEnhancementFacility());
        } finally {
            if (overrideConn != null) {
                overrideConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#13277 - RSMD for generated keys has NPEs when a
     * connection is referenced.
     * 
     * @throws Exception
     */
    public void testBug13277() throws Exception {
        createTable("testBug13277", "(field1 INT NOT NULL PRIMARY KEY AUTO_INCREMENT, field2 VARCHAR(32))");

        try {
            this.stmt.executeUpdate("INSERT INTO testBug13277 (field2) VALUES ('abcdefg')", Statement.RETURN_GENERATED_KEYS);

            this.rs = this.stmt.getGeneratedKeys();

            ResultSetMetaData rsmd = this.rs.getMetaData();
            checkRsmdForBug13277(rsmd);
            this.rs.close();

            for (int i = 0; i < 5; i++) {
                this.stmt.addBatch("INSERT INTO testBug13277 (field2) VALUES ('abcdefg')");
            }

            this.stmt.executeBatch();

            this.rs = this.stmt.getGeneratedKeys();

            rsmd = this.rs.getMetaData();
            checkRsmdForBug13277(rsmd);
            this.rs.close();

            this.pstmt = this.conn.prepareStatement("INSERT INTO testBug13277 (field2) VALUES ('abcdefg')", Statement.RETURN_GENERATED_KEYS);
            this.pstmt.executeUpdate();

            this.rs = this.pstmt.getGeneratedKeys();

            rsmd = this.rs.getMetaData();
            checkRsmdForBug13277(rsmd);
            this.rs.close();

            this.pstmt.addBatch();
            this.pstmt.addBatch();

            this.pstmt.executeUpdate();

            this.rs = this.pstmt.getGeneratedKeys();

            rsmd = this.rs.getMetaData();
            checkRsmdForBug13277(rsmd);
            this.rs.close();

        } finally {
            if (this.pstmt != null) {
                this.pstmt.close();
                this.pstmt = null;
            }

            if (this.rs != null) {
                this.rs.close();
                this.rs = null;
            }
        }
    }

    /**
     * Tests BUG13601 (which doesn't seem to be present in 3.1.11, but we'll
     * leave it in here for regression's-sake).
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug13601() throws Exception {

        if (versionMeetsMinimum(5, 0)) {
            createTable("testBug13601", "(field1 BIGINT NOT NULL, field2 BIT default 0 NOT NULL) ENGINE=MyISAM");

            this.rs = this.stmt.executeQuery("SELECT field1, field2 FROM testBug13601 WHERE 1=-1");
            ResultSetMetaData rsmd = this.rs.getMetaData();
            assertEquals(Types.BIT, rsmd.getColumnType(2));
            assertEquals(Boolean.class.getName(), rsmd.getColumnClassName(2));

            this.rs = this.conn.prepareStatement("SELECT field1, field2 FROM testBug13601 WHERE 1=-1").executeQuery();
            rsmd = this.rs.getMetaData();
            assertEquals(Types.BIT, rsmd.getColumnType(2));
            assertEquals(Boolean.class.getName(), rsmd.getColumnClassName(2));

        }
    }

    /**
     * Tests fix for BUG#14815 - DBMD.getColumns() doesn't return TABLE_NAME
     * correctly.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug14815() throws Exception {
        try {
            createTable("testBug14815_1", "(field_1_1 int)");
            createTable("testBug14815_2", "(field_2_1 int)");

            boolean lcTableNames = this.conn.getMetaData().storesLowerCaseIdentifiers();

            String tableName1 = lcTableNames ? "testbug14815_1" : "testBug14815_1";
            String tableName2 = lcTableNames ? "testbug14815_2" : "testBug14815_2";

            this.rs = this.conn.getMetaData().getColumns(this.conn.getCatalog(), null, "testBug14815%", "%");

            assertTrue(this.rs.next());
            assertEquals(tableName1, this.rs.getString("TABLE_NAME"));
            assertEquals("field_1_1", this.rs.getString("COLUMN_NAME"));

            assertTrue(this.rs.next());
            assertEquals(tableName2, this.rs.getString("TABLE_NAME"));
            assertEquals("field_2_1", this.rs.getString("COLUMN_NAME"));

        } finally {
            if (this.rs != null) {
                this.rs.close();
                this.rs = null;
            }
        }
    }

    /**
     * Tests fix for BUG#15854 - DBMD.getColumns() returns wrong type for BIT.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug15854() throws Exception {
        if (versionMeetsMinimum(5, 0)) {
            createTable("testBug15854", "(field1 BIT)");
            try {
                this.rs = this.conn.getMetaData().getColumns(this.conn.getCatalog(), null, "testBug15854", "field1");
                assertTrue(this.rs.next());
                assertEquals(Types.BIT, this.rs.getInt("DATA_TYPE"));
            } finally {
                if (this.rs != null) {
                    ResultSet toClose = this.rs;
                    this.rs = null;
                    toClose.close();
                }
            }

        }
    }

    /**
     * Tests fix for BUG#16277 - Invalid classname returned for
     * RSMD.getColumnClassName() for BIGINT type.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug16277() throws Exception {
        createTable("testBug16277", "(field1 BIGINT, field2 BIGINT UNSIGNED)");
        ResultSetMetaData rsmd = this.stmt.executeQuery("SELECT field1, field2 FROM testBug16277").getMetaData();
        assertEquals("java.lang.Long", rsmd.getColumnClassName(1));
        assertEquals("java.math.BigInteger", rsmd.getColumnClassName(2));
    }

    /**
     * Tests fix for BUG#18554 - Aliased column names where length of name > 251
     * are corrupted.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug18554() throws Exception {
        testBug18554(249);
        testBug18554(250);
        testBug18554(251);
        testBug18554(252);
        testBug18554(253);
        testBug18554(254);
        testBug18554(255);
    }

    private void testBug18554(int columnNameLength) throws Exception {
        StringBuilder buf = new StringBuilder(columnNameLength + 2);

        for (int i = 0; i < columnNameLength; i++) {
            buf.append((char) ((Math.random() * 26) + 65));
        }

        String colName = buf.toString();
        this.rs = this.stmt.executeQuery("select curtime() as `" + colName + "`");
        ResultSetMetaData meta = this.rs.getMetaData();

        assertEquals(colName, meta.getColumnLabel(1));

    }

    private void checkRsmdForBug13277(ResultSetMetaData rsmd) throws SQLException {

        int i = ((com.mysql.jdbc.ConnectionImpl) this.conn)
                .getMaxBytesPerChar(CharsetMapping.getJavaEncodingForMysqlCharset(((com.mysql.jdbc.Connection) this.conn).getServerCharset()));
        if (i == 1) {
            // This is INT field but still processed in
            // ResultsetMetaData.getColumnDisplaySize
            assertEquals(20, rsmd.getColumnDisplaySize(1));
        }

        if (versionMeetsMinimum(4, 1)) {
            assertEquals(false, rsmd.isDefinitelyWritable(1));
            assertEquals(true, rsmd.isReadOnly(1));
            assertEquals(false, rsmd.isWritable(1));
        }
    }

    public void testSupportsCorrelatedSubqueries() throws Exception {
        DatabaseMetaData dbmd = this.conn.getMetaData();

        assertEquals(versionMeetsMinimum(4, 1), dbmd.supportsCorrelatedSubqueries());
    }

    public void testSupportesGroupByUnrelated() throws Exception {
        DatabaseMetaData dbmd = this.conn.getMetaData();

        assertEquals(true, dbmd.supportsGroupByUnrelated());
    }

    /**
     * Tests fix for BUG#21267, ParameterMetaData throws NullPointerException
     * when prepared SQL actually has a syntax error
     * 
     * @throws Exception
     */
    public void testBug21267() throws Exception {
        createTable("bug21267", "(`Col1` int(11) NOT NULL,`Col2` varchar(45) default NULL,`Col3` varchar(45) default NULL,PRIMARY KEY  (`Col1`))");

        this.pstmt = this.conn.prepareStatement("SELECT Col1, Col2,Col4 FROM bug21267 WHERE Col1=?");
        this.pstmt.setInt(1, 1);

        java.sql.ParameterMetaData psMeta = this.pstmt.getParameterMetaData();

        try {
            assertEquals(0, psMeta.getParameterType(1));
        } catch (SQLException sqlEx) {
            assertEquals(SQLError.SQL_STATE_DRIVER_NOT_CAPABLE, sqlEx.getSQLState());
        }

        this.pstmt.close();

        Properties props = new Properties();
        props.setProperty("generateSimpleParameterMetadata", "true");

        this.pstmt = getConnectionWithProps(props).prepareStatement("SELECT Col1, Col2,Col4 FROM bug21267 WHERE Col1=?");

        psMeta = this.pstmt.getParameterMetaData();

        assertEquals(Types.VARCHAR, psMeta.getParameterType(1));
    }

    /**
     * Tests fix for BUG#21544 - When using information_schema for metadata,
     * COLUMN_SIZE for getColumns() is not clamped to range of java.lang.Integer
     * as is the case when not using information_schema, thus leading to a
     * truncation exception that isn't present when not using
     * information_schema.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug21544() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        createTable("testBug21544", "(foo_id INT NOT NULL, stuff LONGTEXT, PRIMARY KEY (foo_id))", "INNODB");

        Connection infoSchemConn = null;

        Properties props = new Properties();
        props.setProperty("useInformationSchema", "true");
        props.setProperty("jdbcCompliantTruncation", "false");

        infoSchemConn = getConnectionWithProps(props);

        try {
            this.rs = infoSchemConn.getMetaData().getColumns(null, null, "testBug21544", null);

            while (this.rs.next()) {
                this.rs.getInt("COLUMN_SIZE");
            }
        } finally {
            if (infoSchemConn != null) {
                infoSchemConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#22613 - DBMD.getColumns() does not return expected
     * COLUMN_SIZE for the SET type (fixed to be consistent with the ODBC
     * driver)
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug22613() throws Exception {

        createTable("bug22613",
                "( s set('a','bc','def','ghij') default NULL, t enum('a', 'ab', 'cdef'), s2 SET('1','2','3','4','1585','ONE','TWO','Y','N','THREE'))");

        checkMetadataForBug22613(this.conn);

        if (versionMeetsMinimum(5, 0)) {
            Connection infoSchemConn = null;

            try {
                Properties props = new Properties();
                props.setProperty("useInformationSchema", "true");

                infoSchemConn = getConnectionWithProps(props);

                checkMetadataForBug22613(infoSchemConn);
            } finally {
                if (infoSchemConn != null) {
                    infoSchemConn.close();
                }
            }
        }
    }

    private void checkMetadataForBug22613(Connection c) throws Exception {
        String maxValue = "a,bc,def,ghij";
        String maxValue2 = "1,2,3,4,1585,ONE,TWO,Y,N,THREE";

        DatabaseMetaData meta = c.getMetaData();
        this.rs = meta.getColumns(null, this.conn.getCatalog(), "bug22613", "s");
        this.rs.first();

        assertEquals(maxValue.length(), this.rs.getInt("COLUMN_SIZE"));

        this.rs = meta.getColumns(null, this.conn.getCatalog(), "bug22613", "s2");
        this.rs.first();

        assertEquals(maxValue2.length(), this.rs.getInt("COLUMN_SIZE"));

        this.rs = meta.getColumns(null, c.getCatalog(), "bug22613", "t");
        this.rs.first();

        assertEquals(4, this.rs.getInt("COLUMN_SIZE"));
    }

    /**
     * Fix for BUG#22628 - Driver.getPropertyInfo() throws NullPointerException
     * for URL that only specifies host and/or port.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug22628() throws Exception {
        DriverPropertyInfo[] dpi = new NonRegisteringDriver().getPropertyInfo("jdbc:mysql://bogus:9999", new Properties());

        boolean foundHost = false;
        boolean foundPort = false;

        for (int i = 0; i < dpi.length; i++) {
            if ("bogus".equals(dpi[i].value)) {
                foundHost = true;
            }

            if ("9999".equals(dpi[i].value)) {
                foundPort = true;
            }
        }

        assertTrue(foundHost && foundPort);
    }

    private void testAbsenceOfMetadataForQuery(String query) throws Exception {
        try {
            this.pstmt = this.conn.prepareStatement(query);
            ResultSetMetaData rsmd = this.pstmt.getMetaData();

            assertNull(rsmd);

            this.pstmt = ((com.mysql.jdbc.Connection) this.conn).clientPrepareStatement(query);
            rsmd = this.pstmt.getMetaData();

            assertNull(rsmd);
        } finally {
            if (this.pstmt != null) {
                this.pstmt.close();
            }
        }
    }

    public void testRSMDToStringFromDBMD() throws Exception {

        this.rs = this.conn.getMetaData().getTypeInfo();

        this.rs.getMetaData().toString(); // used to cause NPE

    }

    public void testCharacterSetForDBMD() throws Exception {
        if (versionMeetsMinimum(4, 0)) {
            // server is broken, fixed in 5.2/6.0?

            if (!versionMeetsMinimum(5, 2)) {
                return;
            }
        }

        String quoteChar = this.conn.getMetaData().getIdentifierQuoteString();

        String tableName = quoteChar + "\u00e9\u0074\u00e9" + quoteChar;
        createTable(tableName, "(field1 int)");
        this.rs = this.conn.getMetaData().getTables(this.conn.getCatalog(), null, tableName, new String[] { "TABLE" });
        assertEquals(true, this.rs.next());
        System.out.println(this.rs.getString("TABLE_NAME"));
        System.out.println(new String(this.rs.getBytes("TABLE_NAME"), "UTF-8"));
    }

    /**
     * Tests fix for BUG#18258 - Nonexistent catalog/database causes
     * SQLException to be raised, rather than returning empty result set.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug18258() throws Exception {
        String bogusDatabaseName = "abcdefghijklmnopqrstuvwxyz";
        this.conn.getMetaData().getTables(bogusDatabaseName, "%", "%", new String[] { "TABLE", "VIEW" });
        this.conn.getMetaData().getColumns(bogusDatabaseName, "%", "%", "%");
        this.conn.getMetaData().getProcedures(bogusDatabaseName, "%", "%");
    }

    /**
     * Tests fix for BUG#23303 - DBMD.getSchemas() doesn't return a
     * TABLE_CATALOG column.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug23303() throws Exception {

        this.rs = this.conn.getMetaData().getSchemas();
        this.rs.findColumn("TABLE_CATALOG");

    }

    /**
     * Tests fix for BUG#23304 - DBMD using "show" and DBMD using
     * information_schema do not return results consistent with eachother.
     * 
     * (note this fix only addresses the inconsistencies, not the issue that the
     * driver is treating schemas differently than some users expect.
     * 
     * We will revisit this behavior when there is full support for schemas in
     * MySQL).
     * 
     * @throws Exception
     */
    public void testBug23304() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        Connection connShow = null;
        Connection connInfoSchema = null;

        ResultSet rsShow = null;
        ResultSet rsInfoSchema = null;

        try {
            Properties noInfoSchemaProps = new Properties();
            noInfoSchemaProps.setProperty("useInformationSchema", "false");

            Properties infoSchemaProps = new Properties();
            infoSchemaProps.setProperty("useInformationSchema", "true");
            infoSchemaProps.setProperty("dumpQueriesOnException", "true");

            connShow = getConnectionWithProps(noInfoSchemaProps);
            connInfoSchema = getConnectionWithProps(infoSchemaProps);

            DatabaseMetaData dbmdUsingShow = connShow.getMetaData();
            DatabaseMetaData dbmdUsingInfoSchema = connInfoSchema.getMetaData();

            assertNotSame(dbmdUsingShow.getClass(), dbmdUsingInfoSchema.getClass());

            rsShow = dbmdUsingShow.getSchemas();
            rsInfoSchema = dbmdUsingInfoSchema.getSchemas();

            compareResultSets(rsShow, rsInfoSchema);

            /*
             * rsShow = dbmdUsingShow.getTables(connShow.getCatalog(), null,
             * "%", new String[] {"TABLE", "VIEW"}); rsInfoSchema =
             * dbmdUsingInfoSchema.getTables(connInfoSchema.getCatalog(), null,
             * "%", new String[] {"TABLE", "VIEW"});
             * 
             * compareResultSets(rsShow, rsInfoSchema);
             * 
             * rsShow = dbmdUsingShow.getTables(null, null, "%", new String[]
             * {"TABLE", "VIEW"}); rsInfoSchema =
             * dbmdUsingInfoSchema.getTables(null, null, "%", new String[]
             * {"TABLE", "VIEW"});
             * 
             * compareResultSets(rsShow, rsInfoSchema);
             */

            createTable("t_testBug23304",
                    "(field1 int primary key not null, field2 tinyint, field3 mediumint, field4 mediumint, field5 bigint, field6 float, field7 double, field8 decimal, field9 char(32), field10 varchar(32), field11 blob, field12 mediumblob, field13 longblob, field14 text, field15 mediumtext, field16 longtext, field17 date, field18 time, field19 datetime, field20 timestamp)");

            rsShow = dbmdUsingShow.getColumns(connShow.getCatalog(), null, "t_testBug23304", "%");
            rsInfoSchema = dbmdUsingInfoSchema.getColumns(connInfoSchema.getCatalog(), null, "t_testBug23304", "%");

            compareResultSets(rsShow, rsInfoSchema);
        } finally {
            if (rsShow != null) {
                rsShow.close();
            }

            if (rsInfoSchema != null) {
                rsInfoSchema.close();
            }
        }
    }

    private void compareResultSets(ResultSet expected, ResultSet actual) throws Exception {
        if (expected == null) {
            if (actual != null) {
                fail("Expected null result set, actual was not null.");
            } else {
                return;
            }
        } else if (actual == null) {
            fail("Expected non-null actual result set.");
        }

        expected.last();

        int expectedRows = expected.getRow();

        actual.last();

        int actualRows = actual.getRow();

        assertEquals(expectedRows, actualRows);

        ResultSetMetaData metadataExpected = expected.getMetaData();
        ResultSetMetaData metadataActual = actual.getMetaData();

        assertEquals(metadataExpected.getColumnCount(), metadataActual.getColumnCount());

        for (int i = 0; i < metadataExpected.getColumnCount(); i++) {
            assertEquals(metadataExpected.getColumnName(i + 1), metadataActual.getColumnName(i + 1));
            assertEquals(metadataExpected.getColumnType(i + 1), metadataActual.getColumnType(i + 1));
            assertEquals(metadataExpected.getColumnClassName(i + 1), metadataActual.getColumnClassName(i + 1));
        }

        expected.beforeFirst();
        actual.beforeFirst();

        StringBuilder messageBuf = null;

        while (expected.next() && actual.next()) {

            if (messageBuf != null) {
                messageBuf.append("\n");
            }

            for (int i = 0; i < metadataExpected.getColumnCount(); i++) {
                if (expected.getObject(i + 1) == null && actual.getObject(i + 1) == null) {
                    continue;
                }

                if ((expected.getObject(i + 1) == null && actual.getObject(i + 1) != null)
                        || (expected.getObject(i + 1) != null && actual.getObject(i + 1) == null)
                        || (!expected.getObject(i + 1).equals(actual.getObject(i + 1)))) {
                    if ("COLUMN_DEF".equals(metadataExpected.getColumnName(i + 1))
                            && (expected.getObject(i + 1) == null && actual.getString(i + 1).length() == 0)
                            || (expected.getString(i + 1).length() == 0 && actual.getObject(i + 1) == null)) {
                        continue; // known bug with SHOW FULL COLUMNS, and we
                                 // can't distinguish between null and ''
                                 // for a default
                    }

                    if ("CHAR_OCTET_LENGTH".equals(metadataExpected.getColumnName(i + 1))) {
                        if (((com.mysql.jdbc.ConnectionImpl) this.conn).getMaxBytesPerChar(
                                CharsetMapping.getJavaEncodingForMysqlCharset(((com.mysql.jdbc.Connection) this.conn).getServerCharset())) > 1) {
                            continue; // SHOW CREATE and CHAR_OCT *will* differ
                        }
                    }

                    if (messageBuf == null) {
                        messageBuf = new StringBuilder();
                    } else {
                        messageBuf.append("\n");
                    }

                    messageBuf.append("On row " + expected.getRow() + " ,for column named " + metadataExpected.getColumnName(i + 1) + ", expected '"
                            + expected.getObject(i + 1) + "', found '" + actual.getObject(i + 1) + "'");

                }
            }
        }

        if (messageBuf != null) {
            fail(messageBuf.toString());
        }
    }

    /**
     * Tests fix for BUG#25624 - Whitespace surrounding storage/size specifiers
     * in stored procedure declaration causes NumberFormatException to be thrown
     * when calling stored procedure.
     * 
     * @throws Exception
     */
    public void testBug25624() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        //
        // we changed up the parameters to get coverage of the fixes,
        // also note that whitespace _is_ significant in the DDL...
        //
        createProcedure("testBug25624", "(in _par1 decimal( 10 , 2 ) , in _par2 varchar( 4 )) BEGIN select 1; END");

        this.conn.prepareCall("{call testBug25624(?,?)}").close();
    }

    /**
     * Tests fix for BUG#27867 - Schema objects with identifiers other than the
     * connection character aren't retrieved correctly in ResultSetMetadata.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug27867() throws Exception {
        if (!versionMeetsMinimum(4, 1)) {
            return;
        }

        String gbkColumnName = "\u00e4\u00b8\u00ad\u00e6\u2013\u2021\u00e6\u00b5\u2039\u00e8\u00af\u2022";
        createTable("ColumnNameEncoding",
                "(`" + gbkColumnName + "` varchar(1) default NULL, `ASCIIColumn` varchar(1) default NULL" + ")ENGINE=MyISAM DEFAULT CHARSET=utf8");

        this.rs = this.stmt.executeQuery("SELECT * FROM ColumnNameEncoding");
        java.sql.ResultSetMetaData tblMD = this.rs.getMetaData();

        assertEquals(gbkColumnName, tblMD.getColumnName(1));
        assertEquals("ASCIIColumn", tblMD.getColumnName(2));
    }

    /**
     * Fixed BUG#27915 - DatabaseMetaData.getColumns() doesn't contain SCOPE_*
     * or IS_AUTOINCREMENT columns.
     * 
     * @throws Exception
     */
    public void testBug27915() throws Exception {
        createTable("testBug27915", "(field1 int not null primary key auto_increment, field2 int)");
        DatabaseMetaData dbmd = this.conn.getMetaData();

        this.rs = dbmd.getColumns(this.conn.getCatalog(), null, "testBug27915", "%");
        this.rs.next();

        checkBug27915();

        if (versionMeetsMinimum(5, 0)) {
            this.rs = getConnectionWithProps("useInformationSchema=true").getMetaData().getColumns(this.conn.getCatalog(), null, "testBug27915", "%");
            this.rs.next();

            checkBug27915();
        }
    }

    private void checkBug27915() throws SQLException {
        assertNull(this.rs.getString("SCOPE_CATALOG"));
        assertNull(this.rs.getString("SCOPE_SCHEMA"));
        assertNull(this.rs.getString("SCOPE_TABLE"));
        assertNull(this.rs.getString("SOURCE_DATA_TYPE"));
        assertEquals("YES", this.rs.getString("IS_AUTOINCREMENT"));

        this.rs.next();

        assertNull(this.rs.getString("SCOPE_CATALOG"));
        assertNull(this.rs.getString("SCOPE_SCHEMA"));
        assertNull(this.rs.getString("SCOPE_TABLE"));
        assertNull(this.rs.getString("SOURCE_DATA_TYPE"));
        assertEquals("NO", this.rs.getString("IS_AUTOINCREMENT"));
    }

    /**
     * Tests fix for BUG#27916 - UNSIGNED types not reported via
     * DBMD.getTypeInfo(), and capitalization of types is not consistent between
     * DBMD.getColumns(), RSMD.getColumnTypeName() and DBMD.getTypeInfo().
     * 
     * This fix also ensures that the precision of UNSIGNED MEDIUMINT and
     * UNSIGNED BIGINT is reported correctly via DBMD.getColumns().
     * 
     * Second fix ensures that list values of ENUM and SET types containing
     * 'unsigned' are not taken in account.
     * 
     * @throws Exception
     */
    public void testBug27916() throws Exception {
        createTable("testBug27916",
                "(field1 TINYINT UNSIGNED, field2 SMALLINT UNSIGNED, field3 INT UNSIGNED, field4 INTEGER UNSIGNED, field5 MEDIUMINT UNSIGNED, field6 BIGINT UNSIGNED)");

        ResultSetMetaData rsmd = this.stmt.executeQuery("SELECT * FROM testBug27916").getMetaData();

        HashMap<String, Object> typeNameToPrecision = new HashMap<String, Object>();
        this.rs = this.conn.getMetaData().getTypeInfo();

        while (this.rs.next()) {
            typeNameToPrecision.put(this.rs.getString("TYPE_NAME"), this.rs.getObject("PRECISION"));
        }

        this.rs = this.conn.getMetaData().getColumns(this.conn.getCatalog(), null, "testBug27916", "%");

        for (int i = 0; i < rsmd.getColumnCount(); i++) {
            this.rs.next();
            String typeName = this.rs.getString("TYPE_NAME");

            assertEquals(typeName, rsmd.getColumnTypeName(i + 1));
            assertEquals(typeName, this.rs.getInt("COLUMN_SIZE"), rsmd.getPrecision(i + 1));
            assertEquals(typeName, new Integer(rsmd.getPrecision(i + 1)), typeNameToPrecision.get(typeName));
        }

        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        Properties props = new Properties();
        props.setProperty("useInformationSchema", "false");
        ArrayList<String> types = new ArrayList<String>();
        Connection PropConn = getConnectionWithProps(props);
        try {
            DatabaseMetaData dbmd = PropConn.getMetaData();
            this.rs = dbmd.getTypeInfo();
            while (this.rs.next()) {
                types.add(this.rs.getString("TYPE_NAME"));
            }
            this.rs.close();

            this.rs = dbmd.getColumns("mysql", null, "time_zone_transition", "%");
            while (this.rs.next()) {
                String typeName = this.rs.getString("TYPE_NAME");
                assertTrue(typeName, types.contains(typeName));
            }
            this.rs.close();
            this.rs = dbmd.getColumns("mysql", null, "proc", "%");
            while (this.rs.next()) {
                String typeName = this.rs.getString("TYPE_NAME");
                assertTrue(typeName, types.contains(typeName));
            }
            this.rs.close();
            PropConn.close();
            props.clear();

            props.setProperty("useInformationSchema", "true");
            PropConn = getConnectionWithProps(props);
            dbmd = PropConn.getMetaData();

            this.rs = dbmd.getColumns("mysql", null, "time_zone_transition", "%");
            while (this.rs.next()) {
                String typeName = this.rs.getString("TYPE_NAME");
                assertTrue(typeName, types.contains(typeName));
            }
            this.rs.close();
            this.rs = dbmd.getColumns("mysql", null, "proc", "%");
            while (this.rs.next()) {
                String typeName = this.rs.getString("TYPE_NAME");
                assertTrue(typeName, types.contains(typeName));
            }
            this.rs.close();
            PropConn.close();
            props.clear();
        } finally {
            if (PropConn != null) {
                PropConn.close();
            }
        }
    }

    public void testBug20491() throws Exception {
        String[] fields = { "field1_ae_\u00e4", "field2_ue_\u00fc", "field3_oe_\u00f6", "field4_sz_\u00df" };

        createTable("tst", "(`" + fields[0] + "` int(10) unsigned NOT NULL default '0', `" + fields[1] + "` varchar(45) default '', `" + fields[2]
                + "` varchar(45) default '', `" + fields[3] + "` varchar(45) default '', PRIMARY KEY  (`" + fields[0] + "`))");

        // demonstrate that these are all in the Cp1252 encoding

        for (int i = 0; i < fields.length; i++) {
            try {
                assertEquals(fields[i], new String(fields[i].getBytes("Cp1252"), "Cp1252"));
            } catch (ComparisonFailure cfEx) {
                if (i == 3) {
                    // If we're on a mac, we're out of luck
                    // we can't store this in the filesystem...

                    if (!System.getProperty("os.name").startsWith("Mac")) {
                        throw cfEx;
                    }
                }
            }
        }

        @SuppressWarnings("unused")
        byte[] asBytes = fields[0].getBytes("utf-8");

        DatabaseMetaData md = this.conn.getMetaData();

        this.rs = md.getColumns(null, "%", "tst", "%");

        int j = 0;

        while (this.rs.next()) {
            try {
                assertEquals("Wrong column name:" + this.rs.getString(4), fields[j++], this.rs.getString(4));
            } catch (ComparisonFailure cfEx) {
                if (j == 3) {
                    // If we're on a mac, we're out of luck
                    // we can't store this in the filesystem...

                    if (!System.getProperty("os.name").startsWith("Mac")) {
                        throw cfEx;
                    }
                }
            }
        }

        this.rs.close();

        this.rs = this.stmt.executeQuery("SELECT * FROM tst");

        ResultSetMetaData rsmd = this.rs.getMetaData();

        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            try {
                assertEquals("Wrong column name:" + rsmd.getColumnName(i), fields[i - 1], rsmd.getColumnName(i));
            } catch (ComparisonFailure cfEx) {
                if (i - 1 == 3) {
                    // If we're on a mac, we're out of luck
                    // we can't store this in the filesystem...

                    if (!System.getProperty("os.name").startsWith("Mac")) {
                        throw cfEx;
                    }
                }
            }
        }
    }

    /**
     * Tests fix for Bug#33594 - When cursor fetch is enabled, wrong metadata is
     * returned from DBMD.
     * 
     * The fix is two parts.
     * 
     * First, when asking for the first column value twice from a cursor-fetched
     * row, the driver didn't re-position, and thus the "next" column was
     * returned.
     * 
     * Second, metadata statements and internal statements the driver uses
     * shouldn't use cursor-based fetching at all, so we've ensured that
     * internal statements have their fetch size set to "0".
     */
    public void testBug33594() throws Exception {
        if (!versionMeetsMinimum(5, 0, 7)) {
            return;
        }
        boolean max_key_l_bug = false;

        try {
            createTable("bug33594", "(fid varchar(255) not null primary key, id INT, geom linestring, name varchar(255))");
        } catch (SQLException sqlEx) {
            if (sqlEx.getMessage().indexOf("max key length") != -1) {
                createTable("bug33594", "(fid varchar(180) not null primary key, id INT, geom linestring, name varchar(255))");
                max_key_l_bug = true;
            }
        }

        Properties props = new Properties();
        props.put("useInformationSchema", "false");
        props.put("useCursorFetch", "false");
        props.put("defaultFetchSize", "100");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getColumns(null, null, "bug33594", null);
            this.rs.next();
            assertEquals("bug33594", this.rs.getString("TABLE_NAME"));
            assertEquals("fid", this.rs.getString("COLUMN_NAME"));
            assertEquals("VARCHAR", this.rs.getString("TYPE_NAME"));
            if (!max_key_l_bug) {
                assertEquals("255", this.rs.getString("COLUMN_SIZE"));
            } else {
                assertEquals("180", this.rs.getString("COLUMN_SIZE"));
            }

            Properties props2 = new Properties();
            props2.put("useInformationSchema", "false");
            props2.put("useCursorFetch", "true");
            props2.put("defaultFetchSize", "100");

            Connection conn2 = null;

            try {
                conn2 = getConnectionWithProps(props2);
                DatabaseMetaData metaData2 = conn2.getMetaData();
                this.rs = metaData2.getColumns(null, null, "bug33594", null);
                this.rs.next();
                assertEquals("bug33594", this.rs.getString("TABLE_NAME"));
                assertEquals("fid", this.rs.getString("COLUMN_NAME"));
                assertEquals("VARCHAR", this.rs.getString("TYPE_NAME"));
                if (!max_key_l_bug) {
                    assertEquals("255", this.rs.getString("COLUMN_SIZE"));
                } else {
                    assertEquals("180", this.rs.getString("COLUMN_SIZE"));
                }

                // we should only see one server-side prepared statement, and
                // that's
                // caused by us going off to ask about the count!
                assertEquals("1", getSingleIndexedValueWithQuery(conn2, 2, "SHOW SESSION STATUS LIKE 'Com_stmt_prepare'").toString());
            } finally {
                if (conn2 != null) {
                    conn2.close();
                }
            }
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
        }

    }

    public void testBug34194() throws Exception {
        createTable("bug34194", "(id integer,geom geometry)");

        if (!versionMeetsMinimum(5, 6)) {
            this.stmt.execute("insert into bug34194 values('1', GeomFromText('POINT(622572.881 5156121.034)'))");
        } else {
            this.stmt.execute("insert into bug34194 values('1', ST_GeomFromText('POINT(622572.881 5156121.034)'))");
        }
        this.rs = this.stmt.executeQuery("select * from bug34194");
        ResultSetMetaData RSMD = this.rs.getMetaData();
        assertEquals("GEOMETRY", RSMD.getColumnTypeName(2));
    }

    public void testNoSystemTablesReturned() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return; // no information schema
        }

        this.rs = this.conn.getMetaData().getTables("information_schema", "null", "%", new String[] { "SYSTEM VIEW" });
        assertTrue(this.rs.next());
        this.rs = this.conn.getMetaData().getTables("information_schema", "null", "%", new String[] { "SYSTEM TABLE" });
        assertFalse(this.rs.next());
        this.rs = this.conn.getMetaData().getTables("information_schema", "null", "%", new String[] { "TABLE" });
        assertFalse(this.rs.next());
        this.rs = this.conn.getMetaData().getTables("information_schema", "null", "%", new String[] { "VIEW" });
        assertFalse(this.rs.next());
        this.rs = this.conn.getMetaData().getTables("information_schema", "null", "%", new String[] { "SYSTEM TABLE", "SYSTEM VIEW", "TABLE", "VIEW" });
        assertTrue(this.rs.next());
        this.rs = this.conn.getMetaData().getColumns("information_schema", null, "TABLES", "%");
        assertTrue(this.rs.next());
    }

    public void testABunchOfReturnTypes() throws Exception {
        checkABunchOfReturnTypesForConnection(this.conn);

        if (versionMeetsMinimum(5, 0)) {
            checkABunchOfReturnTypesForConnection(getConnectionWithProps("useInformationSchema=true"));
        }
    }

    private void checkABunchOfReturnTypesForConnection(Connection mdConn) throws Exception {

        DatabaseMetaData md = mdConn.getMetaData();

        // Bug#44862 - getBestRowIdentifier does not return resultset as per JDBC API specifications
        this.rs = md.getBestRowIdentifier(this.conn.getCatalog(), null, "returnTypesTest", DatabaseMetaData.bestRowSession, false);

        int[] types = new int[] { Types.SMALLINT, // 1. SCOPE short => actual scope of result
                Types.CHAR, // 2. COLUMN_NAME String => column name
                Types.INTEGER, // 3. DATA_TYPE int => SQL data type from java.sql.Types
                Types.CHAR, // 4. TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
                Types.INTEGER, // 5. COLUMN_SIZE int => precision
                Types.INTEGER, // 6. BUFFER_LENGTH int => not used
                Types.SMALLINT, // 7. DECIMAL_DIGITS short => scale
                Types.SMALLINT, // 8. PSEUDO_COLUMN short => is this a pseudo column like an Oracle ROWID
        };

        checkTypes(this.rs, types);

        // Bug#44683 - getVersionColumns does not return resultset as per JDBC API specifications
        this.rs = md.getVersionColumns(this.conn.getCatalog(), null, "returnTypesTest");

        types = new int[] { Types.SMALLINT, // SCOPE short => is not used
                Types.CHAR, // COLUMN_NAME String => column name
                Types.INTEGER, // DATA_TYPE int => SQL data type from java.sql.Types
                Types.CHAR, // TYPE_NAME String => Data source-dependent type name
                Types.INTEGER, // COLUMN_SIZE int => precision
                Types.INTEGER, // BUFFER_LENGTH int => length of column value in bytes
                Types.SMALLINT, // DECIMAL_DIGITS short => scale
                Types.SMALLINT // PSEUDO_COLUMN short => whether this is pseudo column like an Oracle ROWID
        };

        checkTypes(this.rs, types);

        // Bug#44865 - getColumns does not return resultset as per JDBC API specifications
        this.rs = md.getColumns(this.conn.getCatalog(), null, "returnTypesTest", "foo");

        types = new int[] { Types.CHAR, // 1. TABLE_CAT String => table catalog (may be null)
                Types.CHAR, // 2. TABLE_SCHEM String => table schema (may be null)
                Types.CHAR, // 3. TABLE_NAME String => table name
                Types.CHAR, // 4. COLUMN_NAME String => column name
                Types.INTEGER, // 5. DATA_TYPE int => SQL type from java.sql.Types
                Types.CHAR, // 6. TYPE_NAME String => Data source dependent type name, for a UDT the type name is fully qualified
                Types.INTEGER, // 7. COLUMN_SIZE int => column size. For char or date types this is the maximum number of characters, for numeric or decimal
                // types this is precision.
                Types.INTEGER, // 8. BUFFER_LENGTH is not used.
                Types.INTEGER, // 9. DECIMAL_DIGITS int => the number of fractional digits
                Types.INTEGER, // 10. NUM_PREC_RADIX int => Radix (typically either 10 or 2)
                Types.INTEGER, // 11. NULLABLE int => is NULL allowed.
                Types.CHAR, // 12. REMARKS String => comment describing column (may be null)
                Types.CHAR, // 13. COLUMN_DEF String => default value (may be null)
                Types.INTEGER, // 14. SQL_DATA_TYPE int => unused
                Types.INTEGER, // 15. SQL_DATETIME_SUB int => unused
                Types.INTEGER, // 16. CHAR_OCTET_LENGTH int => for char types the maximum number of bytes in the column
                Types.INTEGER, // 17. ORDINAL_POSITION int => index of column in table (starting at 1)
                Types.CHAR, // 18. IS_NULLABLE String => "NO" means column definitely does not allow NULL values; "YES" means the column might allow NULL 
                // values. An empty string means nobody knows.
                Types.CHAR, // 19. SCOPE_CATLOG String => catalog of table that is the scope of a reference attribute (null if DATA_TYPE isn't REF)
                Types.CHAR, // 20. SCOPE_SCHEMA String => schema of table that is the scope of a reference attribute (null if the DATA_TYPE isn't REF)
                Types.CHAR, // 21. SCOPE_TABLE String => table name that this the scope of a reference attribute (null if the DATA_TYPE isn't REF)
                Types.SMALLINT, // 22. SOURCE_DATA_TYPE short => source type of a distinct type or user-generated Ref type, SQL type from java.sql.Types (null
                // if DATA_TYPE isn't DISTINCT or user-generated REF)
                Types.CHAR, // 23. IS_AUTOINCREMENT String => Indicates whether this column is auto incremented
                Types.CHAR // 24. IS_GENERATEDCOLUMN String => Indicates whether this is a generated column 
        };

        checkTypes(this.rs, types);

        // Bug#44868 - getTypeInfo does not return resultset as per JDBC API specifications
        this.rs = md.getTypeInfo();

        types = new int[] { Types.CHAR, // 1. TYPE_NAME String => Type name
                Types.INTEGER, // 2. DATA_TYPE int => SQL data type from java.sql.Types
                Types.INTEGER, // 3. PRECISION int => maximum precision
                Types.CHAR, // 4. LITERAL_PREFIX String => prefix used to quote a literal (may be null)
                Types.CHAR, // 5. LITERAL_SUFFIX String => suffix used to quote a literal (may be null)
                Types.CHAR, // 6. CREATE_PARAMS String => parameters used in creating the type (may be null)
                Types.SMALLINT, // 7. NULLABLE short => can you use NULL for this type.
                Types.BOOLEAN, // 8. CASE_SENSITIVE boolean=> is it case sensitive.
                Types.SMALLINT, // 9. SEARCHABLE short => can you use "WHERE" based on this type:
                Types.BOOLEAN, // 10. UNSIGNED_ATTRIBUTE boolean => is it unsigned.
                Types.BOOLEAN, // 11. FIXED_PREC_SCALE boolean => can it be a money value.
                Types.BOOLEAN, // 12. AUTO_INCREMENT boolean => can it be used for an auto-increment value.
                Types.CHAR, // 13. LOCAL_TYPE_NAME String => localized version of type name (may be null)
                Types.SMALLINT, // 14. MINIMUM_SCALE short => minimum scale supported
                Types.SMALLINT, // 15. MAXIMUM_SCALE short => maximum scale supported
                Types.INTEGER, // 16. SQL_DATA_TYPE int => unused
                Types.INTEGER, // 17. SQL_DATETIME_SUB int => unused
                Types.INTEGER // 18. NUM_PREC_RADIX int => usually 2 or 10
        };

        checkTypes(this.rs, types);

        // Bug#44869 - getIndexInfo does not return resultset as per JDBC API specifications
        this.rs = md.getIndexInfo(this.conn.getCatalog(), null, "returnTypesTest", false, false);

        types = new int[] { Types.CHAR, // 1. TABLE_CAT String => table catalog (may be null)
                Types.CHAR, // 2. TABLE_SCHEM String => table schema (may be null)
                Types.CHAR, // 3. TABLE_NAME String => table name
                Types.BOOLEAN, // 4. NON_UNIQUE boolean => Can index values be non-unique. false when TYPE is tableIndexStatistic
                Types.CHAR, // 5. INDEX_QUALIFIER String => index catalog (may be null); null when TYPE is tableIndexStatistic
                Types.CHAR, // 6. INDEX_NAME String => index name; null when TYPE is tableIndexStatistic
                Types.SMALLINT, // 7. TYPE short => index type:
                Types.SMALLINT, // 8. ORDINAL_POSITION short => column sequence number within index; zero when TYPE is tableIndexStatistic
                Types.CHAR, // 9. COLUMN_NAME String => column name; null when TYPE is tableIndexStatistic
                Types.CHAR, // 10. ASC_OR_DESC String => column sort sequence, "A" => ascending, "D" => descending, may be null if sort sequence is not
                // supported; null when TYPE is tableIndexStatistic
                Util.isJdbc42() ? Types.BIGINT : Types.INTEGER, // 11. CARDINALITY int/long => When TYPE is tableIndexStatistic, then this is the number of rows
                // in the table; otherwise, it is the number of unique values in the index.
                Util.isJdbc42() ? Types.BIGINT : Types.INTEGER, // 12. PAGES int/long => When TYPE is tableIndexStatisic then this is the number of pages used
                // for the table, otherwise it is the number of pages used for the current index.
                Types.CHAR // 13. FILTER_CONDITION String => Filter condition, if any. (may be null)
        };

        checkTypes(this.rs, types);

        // Bug#44867 - getImportedKeys/exportedKeys/crossReference doesn't have correct type for DEFERRABILITY
        this.rs = md.getImportedKeys(this.conn.getCatalog(), null, "returnTypesTest");

        types = new int[] { Types.CHAR, // PKTABLE_CAT String => primary key table catalog being imported (may be null)
                Types.CHAR, // PKTABLE_SCHEM String => primary key table schema being imported (may be null)
                Types.CHAR, // PKTABLE_NAME String => primary key table name being imported
                Types.CHAR, // PKCOLUMN_NAME String => primary key column name being imported
                Types.CHAR, // FKTABLE_CAT String => foreign key table catalog (may be null)
                Types.CHAR, // FKTABLE_SCHEM String => foreign key table schema (may be null)
                Types.CHAR, // FKTABLE_NAME String => foreign key table name
                Types.CHAR, // FKCOLUMN_NAME String => foreign key column name
                Types.SMALLINT, // KEY_SEQ short => sequence number within a foreign key
                Types.SMALLINT, // UPDATE_RULE short => What happens to a foreign key when the primary key is updated:
                Types.SMALLINT, // DELETE_RULE short => What happens to the foreign key when primary is deleted
                Types.CHAR, // FK_NAME String => foreign key name (may be null)
                Types.CHAR, // PK_NAME String => primary key name (may be null)
                Types.SMALLINT // DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
        };

        checkTypes(this.rs, types);

        this.rs = md.getExportedKeys(this.conn.getCatalog(), null, "returnTypesTest");

        types = new int[] { Types.CHAR, // PKTABLE_CAT String => primary key table catalog being imported (may be null)
                Types.CHAR, // PKTABLE_SCHEM String => primary key table schema being imported (may be null)
                Types.CHAR, // PKTABLE_NAME String => primary key table name being imported
                Types.CHAR, // PKCOLUMN_NAME String => primary key column name being imported
                Types.CHAR, // FKTABLE_CAT String => foreign key table catalog (may be null)
                Types.CHAR, // FKTABLE_SCHEM String => foreign key table schema (may be null)
                Types.CHAR, // FKTABLE_NAME String => foreign key table name
                Types.CHAR, // FKCOLUMN_NAME String => foreign key column name
                Types.SMALLINT, // KEY_SEQ short => sequence number within a foreign key
                Types.SMALLINT, // UPDATE_RULE short => What happens to a foreign key when the primary key is updated:
                Types.SMALLINT, // DELETE_RULE short => What happens to the foreign key when primary is deleted
                Types.CHAR, // FK_NAME String => foreign key name (may be null)
                Types.CHAR, // PK_NAME String => primary key name (may be null)
                Types.SMALLINT // DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
        };

        checkTypes(this.rs, types);

        this.rs = md.getCrossReference(this.conn.getCatalog(), null, "returnTypesTest", this.conn.getCatalog(), null, "bar");

        types = new int[] { Types.CHAR, // PKTABLE_CAT String => primary key table catalog being imported (may be null)
                Types.CHAR, // PKTABLE_SCHEM String => primary key table schema being imported (may be null)
                Types.CHAR, // PKTABLE_NAME String => primary key table name being imported
                Types.CHAR, // PKCOLUMN_NAME String => primary key column name being imported
                Types.CHAR, // FKTABLE_CAT String => foreign key table catalog (may be null)
                Types.CHAR, // FKTABLE_SCHEM String => foreign key table schema (may be null)
                Types.CHAR, // FKTABLE_NAME String => foreign key table name
                Types.CHAR, // FKCOLUMN_NAME String => foreign key column name
                Types.SMALLINT, // KEY_SEQ short => sequence number within a foreign key
                Types.SMALLINT, // UPDATE_RULE short => What happens to a foreign key when the primary key is updated:
                Types.SMALLINT, // DELETE_RULE short => What happens to the foreign key when primary is deleted
                Types.CHAR, // FK_NAME String => foreign key name (may be null)
                Types.CHAR, // PK_NAME String => primary key name (may be null)
                Types.SMALLINT // DEFERRABILITY short => can the evaluation of foreign key constraints be deferred until commit
        };

        checkTypes(this.rs, types);
    }

    private final static Map<Integer, String> TYPES_MAP = new HashMap<Integer, String>();

    static {
        Field[] typeFields = Types.class.getFields();

        for (int i = 0; i < typeFields.length; i++) {
            System.out.println(typeFields[i].getName() + " -> " + typeFields[i].getType().getClass());

            if (Modifier.isStatic(typeFields[i].getModifiers())) {
                try {
                    TYPES_MAP.put(new Integer(typeFields[i].getInt(null)), "java.sql.Types." + typeFields[i].getName());
                } catch (IllegalArgumentException e) {
                    // ignore
                } catch (IllegalAccessException e) {
                    // ignore
                }
            }
        }
    }

    private void checkTypes(ResultSet rsToCheck, int[] types) throws Exception {
        ResultSetMetaData rsmd = rsToCheck.getMetaData();
        assertEquals(types.length, rsmd.getColumnCount());
        for (int i = 0; i < types.length; i++) {
            String expectedType = TYPES_MAP.get(new Integer(types[i]));
            String actualType = TYPES_MAP.get(new Integer(rsmd.getColumnType(i + 1)));
            assertNotNull(expectedType);
            assertNotNull(actualType);
            assertEquals("Unexpected type in column " + (i + 1), expectedType, actualType);
        }
    }

    /**
     * Bug #43714 - useInformationSchema with DatabaseMetaData.getExportedKeys()
     * throws exception
     */
    public void testBug43714() throws Exception {
        Connection c_IS = null;
        try {
            c_IS = getConnectionWithProps("useInformationSchema=true");
            DatabaseMetaData dbmd = c_IS.getMetaData();
            this.rs = dbmd.getExportedKeys("x", "y", "z");
        } finally {
            try {
                if (c_IS != null) {
                    c_IS.close();
                }
            } catch (SQLException ex) {
            }
        }
    }

    /**
     * Bug #41269 - DatabaseMetadata.getProcedureColumns() returns wrong value
     * for column length
     */
    public void testBug41269() throws Exception {
        createProcedure("bug41269", "(in param1 int, out result varchar(197)) BEGIN select 1, ''; END");

        ResultSet procMD = this.conn.getMetaData().getProcedureColumns(null, null, "bug41269", "%");
        assertTrue(procMD.next());
        assertEquals("Int param length", 10, procMD.getInt(9));
        assertTrue(procMD.next());
        assertEquals("String param length", 197, procMD.getInt(9));
        assertFalse(procMD.next());

    }

    public void testBug31187() throws Exception {
        createTable("testBug31187", "(field1 int)");

        Connection nullCatConn = getConnectionWithProps("nullCatalogMeansCurrent=false");
        DatabaseMetaData dbmd = nullCatConn.getMetaData();
        ResultSet dbTblCols = dbmd.getColumns(null, null, "testBug31187", "%");

        boolean found = false;

        while (dbTblCols.next()) {
            String catalog = dbTblCols.getString("TABLE_CAT");
            String table = dbTblCols.getString("TABLE_NAME");
            boolean useLowerCaseTableNames = dbmd.storesLowerCaseIdentifiers();

            if (catalog.equals(nullCatConn.getCatalog())
                    && (((useLowerCaseTableNames && "testBug31187".equalsIgnoreCase(table)) || "testBug31187".equals(table)))) {
                found = true;
            }
        }

        assertTrue("Didn't find any columns for table named 'testBug31187' in database " + this.conn.getCatalog(), found);
    }

    public void testBug44508() throws Exception {
        DatabaseMetaData dbmd = this.conn.getMetaData();

        this.rs = dbmd.getSuperTypes("", "", "");
        ResultSetMetaData rsmd = this.rs.getMetaData();

        assertEquals("TYPE_CAT", rsmd.getColumnName(1)); // Gives TABLE_CAT
        assertEquals("TYPE_SCHEM", rsmd.getColumnName(2)); // Gives TABLE_SCHEM
    }

    /**
     * Tests fix for BUG#52167 - Can't parse parameter list with special
     * characters inside
     * 
     * @throws Exception
     */
    public void testBug52167() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        // DatabaseMetaData.java (~LN 1730)
        // + //Bug#52167, tokenizer will break if declaration contains special
        // characters like \n
        // + declaration = declaration.replaceAll("[\\t\\n\\x0B\\f\\r]", " ");
        // StringTokenizer declarationTok = new StringTokenizer(
        // declaration, " \t");
        createProcedure("testBug52167", "(in _par1 decimal( 10 , 2 ) , in _par2\n varchar( 4 )) BEGIN select 1; END");

        this.conn.prepareCall("{call testBug52167(?,?)}").close();
    }

    /**
     * Tests fix for BUG#51912 - Passing NULL as cat. param to
     * getProcedureColumns with nullCatalogMeansCurrent = false
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug51912() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        Connection overrideConn = null;
        try {
            Properties props = new Properties();
            props.setProperty("nullCatalogMeansCurrent", "false");
            overrideConn = getConnectionWithProps(props);

            DatabaseMetaData dbmd = overrideConn.getMetaData();
            this.rs = dbmd.getProcedureColumns(null, null, "%", null);
            this.rs.close();

        } finally {
            if (overrideConn != null) {
                overrideConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#38367 - DatabaseMetaData dbMeta = this.conn.getMetaData();
     * this.rs = dbMeta.getProcedureColumns("test", null, "nullableParameterTest", null);
     * ...
     * Short columnNullable = new Short(this.rs.getShort(12));
     * assertTrue("Parameter " + columnName + " do not allow null arguments",
     * columnNullable.intValue() == java.sql.DatabaseMetaData.procedureNullable);
     * was failing for no good reason.
     * 
     * @throws Exception
     *             if the test fails.
     */

    public void testBug38367() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        try {
            createProcedure("sptestBug38367",
                    "(OUT nfact VARCHAR(100), IN ccuenta VARCHAR(100),\nOUT ffact VARCHAR(100),\nOUT fdoc VARCHAR(100))" + "\nBEGIN\nEND");

            DatabaseMetaData dbMeta = this.conn.getMetaData();
            this.rs = dbMeta.getProcedureColumns(this.conn.getCatalog(), null, "sptestBug38367", null);
            while (this.rs.next()) {
                String columnName = this.rs.getString(4);
                Short columnNullable = new Short(this.rs.getShort(12));
                assertTrue("Parameter " + columnName + " is not java.sql.DatabaseMetaData.procedureNullable.",
                        columnNullable.intValue() == java.sql.DatabaseMetaData.procedureNullable);
            }
        } finally {
        }
    }

    /**
     * Tests fix for BUG#57808 - wasNull not set
     * for DATE field with value 0000-00-00
     * in getDate() although
     * zeroDateTimeBehavior is convertToNull.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug57808() throws Exception {
        try {
            createTable("bug57808", "(ID INT(3) NOT NULL PRIMARY KEY, ADate DATE NOT NULL)");
            Properties props = new Properties();
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
            props.put("zeroDateTimeBehavior", "convertToNull");
            Connection conn1 = null;

            conn1 = getConnectionWithProps(props);
            this.stmt = conn1.createStatement();
            this.stmt.executeUpdate("INSERT INTO bug57808(ID, ADate) VALUES(1, 0000-00-00)");

            this.rs = this.stmt.executeQuery("SELECT ID, ADate FROM bug57808 WHERE ID = 1");
            if (this.rs.first()) {
                Date theDate = this.rs.getDate("ADate");
                if (theDate == null) {
                    assertTrue("wasNull is FALSE", this.rs.wasNull());
                } else {
                    fail("Original date was not NULL!");
                }
            }
        } finally {
        }
    }

    /**
     * Tests fix for BUG#61150 - First call to SP
     * fails with "No Database Selected"
     * The workaround introduced in DatabaseMetaData.getCallStmtParameterTypes
     * to fix the bug in server where SHOW CREATE PROCEDURE was not respecting
     * lower-case table names is misbehaving when connection is not attached to
     * database and on non-casesensitive OS.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug61150() throws Exception {
        NonRegisteringDriver driver = new NonRegisteringDriver();
        Properties oldProps = driver.parseURL(BaseTestCase.dbUrl, null);

        String host = driver.host(oldProps);
        int port = driver.port(oldProps);
        StringBuilder newUrlToTestNoDB = new StringBuilder("jdbc:mysql://");
        if (host != null) {
            newUrlToTestNoDB.append(host);
        }
        newUrlToTestNoDB.append(":").append(port).append("/");

        Statement savedSt = this.stmt;

        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        props.remove(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
        Connection conn1 = DriverManager.getConnection(newUrlToTestNoDB.toString(), props);

        this.stmt = conn1.createStatement();
        createDatabase("TST1");
        createProcedure("TST1.PROC", "(x int, out y int)\nbegin\ndeclare z int;\nset z = x+1, y = z;\nend\n");

        CallableStatement cStmt = null;
        cStmt = conn1.prepareCall("{call `TST1`.`PROC`(?, ?)}");
        cStmt.setInt(1, 5);
        cStmt.registerOutParameter(2, Types.INTEGER);

        cStmt.execute();
        assertEquals(6, cStmt.getInt(2));
        cStmt.clearParameters();
        cStmt.close();

        conn1.setCatalog("TST1");
        cStmt = null;
        cStmt = conn1.prepareCall("{call TST1.PROC(?, ?)}");
        cStmt.setInt(1, 5);
        cStmt.registerOutParameter(2, Types.INTEGER);

        cStmt.execute();
        assertEquals(6, cStmt.getInt(2));
        cStmt.clearParameters();
        cStmt.close();

        conn1.setCatalog("mysql");
        cStmt = null;
        cStmt = conn1.prepareCall("{call `TST1`.`PROC`(?, ?)}");
        cStmt.setInt(1, 5);
        cStmt.registerOutParameter(2, Types.INTEGER);

        cStmt.execute();
        assertEquals(6, cStmt.getInt(2));
        cStmt.clearParameters();
        cStmt.close();

        this.stmt = savedSt;
    }

    /**
     * Tests fix for BUG#61332 - Check if "LIKE" or "=" is sent
     * to server in I__S query when no wildcards are supplied
     * for schema parameter.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug61332() throws Exception {
        Properties props = new Properties();
        props.setProperty("useInformationSchema", "true");
        props.setProperty("statementInterceptors", StatementInterceptorBug61332.class.getName());

        createDatabase("dbbug61332");
        Connection testConn = getConnectionWithProps(props);

        if (versionMeetsMinimum(5, 0, 7)) {
            try {
                createTable("dbbug61332.bug61332", "(c1 char(1))");
                DatabaseMetaData metaData = testConn.getMetaData();

                this.rs = metaData.getColumns("dbbug61332", null, "bug61332", null);
                this.rs.next();
            } finally {
            }
        }
    }

    public static class StatementInterceptorBug61332 extends BaseStatementInterceptor {
        @Override
        public ResultSetInternalMethods preProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, com.mysql.jdbc.Connection conn)
                throws SQLException {
            if (interceptedStatement instanceof com.mysql.jdbc.PreparedStatement) {
                sql = ((com.mysql.jdbc.PreparedStatement) interceptedStatement).getPreparedSql();
                assertTrue("Assereet failed on: " + sql,
                        StringUtils.indexOfIgnoreCase(0, sql, "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND COLUMN_NAME LIKE ?") > -1);
            }
            return null;
        }
    }

    public void testQuotedGunk() throws Exception {
        createTable("testQuotedGunk", "(field1 int)");

        String quotedCatalog = "`" + this.conn.getCatalog() + "`";
        String unquotedCatalog = this.conn.getCatalog();

        DatabaseMetaData dbmd = this.conn.getMetaData();
        this.rs = dbmd.getTables(quotedCatalog, null, "testQuotedGunk", new String[] { "TABLE" });
        assertTrue(this.rs.next());
        this.rs = dbmd.getTables(unquotedCatalog, null, "testQuotedGunk", new String[] { "TABLE" });
        assertTrue(this.rs.next());
        this.rs = dbmd.getColumns(quotedCatalog, null, "testQuotedGunk", "field1");
        assertTrue(this.rs.next());
        this.rs = dbmd.getColumns(unquotedCatalog, null, "testQuotedGunk", "field1");
        assertTrue(this.rs.next());

    }

    /**
     * Tests fix for BUG#61203 - noAccessToProcedureBodies does not work anymore.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug61203() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return; // no stored procedures
        }

        Connection rootConn = null;
        Connection userConn = null;
        CallableStatement cStmt = null;

        try {
            Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
            String dbname = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
            if (dbname == null) {
                assertTrue("No database selected", false);
            }

            createUser("'bug61203user'@'%'", "identified by 'foo'");
            this.stmt.executeUpdate("delete from mysql.db where user='bug61203user'");
            this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, "
                    + "Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,"
                    + "Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '" + dbname
                    + "', 'bug61203user', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
            this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, "
                    + "Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,"
                    + "Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES "
                    + "('%', 'information\\_schema', 'bug61203user', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', "
                    + "'Y', 'N', 'N')");
            this.stmt.executeUpdate("flush privileges");

            // 1. underprivileged user is the creator
            this.stmt.executeUpdate("DROP FUNCTION IF EXISTS testbug61203fn;");
            this.stmt.executeUpdate("CREATE DEFINER='bug61203user'@'%' FUNCTION testbug61203fn(a float) RETURNS INT NO SQL BEGIN RETURN a; END");
            this.stmt.executeUpdate("DROP PROCEDURE IF EXISTS testbug61203pr;");
            this.stmt.executeUpdate(
                    "CREATE DEFINER='bug61203user'@'%' PROCEDURE testbug61203pr(INOUT a float, b bigint, c int) " + "NO SQL BEGIN SET @a = b + c; END");
            testBug61203checks(rootConn, userConn);
            this.stmt.executeUpdate("DROP FUNCTION IF EXISTS testbug61203fn;");
            this.stmt.executeUpdate("DROP PROCEDURE IF EXISTS testbug61203pr;");

            // 2. root user is the creator
            createFunction("testbug61203fn", "(a float) RETURNS INT NO SQL BEGIN RETURN a; END");
            createProcedure("testbug61203pr", "(INOUT a float, b bigint, c int) NO SQL BEGIN SET @a = b + c; END");
            testBug61203checks(rootConn, userConn);

        } finally {
            dropFunction("testbug61203fn");
            dropProcedure("testbug61203pr");

            if (cStmt != null) {
                cStmt.close();
            }
            if (rootConn != null) {
                rootConn.close();
            }
            if (userConn != null) {
                userConn.close();
            }
        }
    }

    private void testBug61203checks(Connection rootConn, Connection userConn) throws SQLException {
        CallableStatement cStmt = null;
        // 1.1. with information schema
        rootConn = getConnectionWithProps("noAccessToProcedureBodies=true,useInformationSchema=true");
        userConn = getConnectionWithProps("noAccessToProcedureBodies=true,useInformationSchema=true,user=bug61203user,password=foo");
        // 1.1.1. root call;
        callFunction(cStmt, rootConn);
        callProcedure(cStmt, rootConn);
        // 1.1.2. underprivileged user call;
        callFunction(cStmt, userConn);
        callProcedure(cStmt, userConn);

        // 1.2. no information schema
        rootConn = getConnectionWithProps("noAccessToProcedureBodies=true,useInformationSchema=false");
        userConn = getConnectionWithProps("noAccessToProcedureBodies=true,useInformationSchema=false,user=bug61203user,password=foo");
        // 1.2.1. root call;
        callFunction(cStmt, rootConn);
        callProcedure(cStmt, rootConn);
        // 1.2.2. underprivileged user call;
        callFunction(cStmt, userConn);
        callProcedure(cStmt, userConn);
    }

    private void callFunction(CallableStatement cStmt, Connection c) throws SQLException {
        cStmt = c.prepareCall("{? = CALL testbug61203fn(?)}");
        cStmt.registerOutParameter(1, Types.INTEGER);
        cStmt.setFloat(2, 2);
        cStmt.execute();
        assertEquals(2f, cStmt.getInt(1), .001);
    }

    private void callProcedure(CallableStatement cStmt, Connection c) throws SQLException {
        cStmt = c.prepareCall("{CALL testbug61203pr(?,?,?)}");
        cStmt.setFloat(1, 2);
        cStmt.setInt(2, 1);
        cStmt.setInt(3, 1);
        cStmt.registerOutParameter(1, Types.INTEGER);
        cStmt.execute();
        assertEquals(2f, cStmt.getInt(1), .001);
    }

    /**
     * Tests fix for BUG#63456 - MetaData precision is different when using UTF8 or Latin1 tables
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug63456() throws Exception {

        //createTable("testBug63456_custom1", "(TEST VARCHAR(10)) ENGINE = MyISAM CHARACTER SET custom1 COLLATE custom1_general_ci");
        createTable("testBug63456_latin1", "(TEST VARCHAR(10)) DEFAULT CHARACTER SET latin1 COLLATE latin1_swedish_ci");
        createTable("testBug63456_utf8", "(TEST VARCHAR(10)) DEFAULT CHARACTER SET utf8");
        createTable("testBug63456_utf8_bin", "(TEST VARCHAR(10)) DEFAULT CHARACTER SET utf8 COLLATE utf8_bin");

        //this.rs = this.stmt.executeQuery("select * from testBug63456_custom1"); 
        //int precision_custom1 = this.rs.getMetaData().getPrecision(1); 
        //assertEquals(10, precision_custom1);

        this.rs = this.stmt.executeQuery("select * from testBug63456_latin1");
        int precision_latin1 = this.rs.getMetaData().getPrecision(1);

        this.rs = this.stmt.executeQuery("select * from testBug63456_utf8");
        int precision_utf8 = this.rs.getMetaData().getPrecision(1);

        this.rs = this.stmt.executeQuery("select * from testBug63456_utf8_bin");
        int precision_utf8bin = this.rs.getMetaData().getPrecision(1);

        assertEquals(precision_latin1, precision_utf8);
        assertEquals(precision_utf8, precision_utf8bin);

    }

    /**
     * Tests fix for BUG#63800 - getVersionColumns() does not return timestamp fields; always empty.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug63800() throws Exception {
        try {
            Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
            String dbname = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
            if (dbname == null) {
                fail("No database selected");
            }

            for (String prop : new String[] { "dummyProp", "useInformationSchema" }) {
                props = new Properties();
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
                props.setProperty(prop, "true");
                Connection conn2 = getConnectionWithProps(props);
                Statement stmt2 = null;

                try {
                    stmt2 = conn2.createStatement();
                    testTimestamp(conn2, stmt2, dbname);
                    if (versionMeetsMinimum(5, 6, 5)) {
                        testDatetime(conn2, stmt2, dbname);
                    }
                } finally {
                    if (stmt2 != null) {
                        stmt2.close();
                    }
                    if (conn2 != null) {
                        conn2.close();
                    }
                }
            }
        } finally {
            dropTable("testBug63800");
        }
    }

    private void testTimestamp(Connection con, Statement st, String dbname) throws SQLException {
        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
        DatabaseMetaData dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue("1 column must be found", this.rs.next());
        assertEquals("Wrong column or single column not found", this.rs.getString(2), "f1");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue("1 column must be found", this.rs.next());
        assertEquals("Wrong column or single column not found", this.rs.getString(2), "f1");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertFalse("0 column must be found", this.rs.next());

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP DEFAULT 0)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertFalse("0 column must be found", this.rs.next());

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue("1 column must be found", this.rs.next());
        assertEquals("Wrong column or single column not found", this.rs.getString(2), "f1");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue("1 column must be found", this.rs.next());
        assertEquals("Wrong column or single column not found", this.rs.getString(2), "f1");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP NULL, f2 TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue("1 column must be found", this.rs.next());
        assertEquals("Wrong column or single column not found", this.rs.getString(2), "f2");

        // ALTER test
        st.execute("ALTER TABLE testBug63800 CHANGE COLUMN `f2` `f2` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00', "
                + "ADD COLUMN `f3` TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP  AFTER `f2`");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue("1 column must be found", this.rs.next());
        assertEquals("Wrong column or single column not found", this.rs.getString(2), "f3");
    }

    private void testDatetime(Connection con, Statement st, String dbname) throws SQLException {
        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
        DatabaseMetaData dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue("1 column must be found", this.rs.next());
        assertEquals("Wrong column or single column not found", this.rs.getString(2), "f1");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertFalse("0 column must be found", this.rs.next());

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME DEFAULT CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertFalse("0 column must be found", this.rs.next());

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME DEFAULT 0)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertFalse("0 column must be found", this.rs.next());

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue("1 column must be found", this.rs.next());
        assertEquals("Wrong column or single column not found", this.rs.getString(2), "f1");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue("1 column must be found", this.rs.next());
        assertEquals("Wrong column or single column not found", this.rs.getString(2), "f1");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME NULL, f2 DATETIME ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        int cnt = 0;
        while (this.rs.next()) {
            cnt++;
            assertEquals("1 column must be found", cnt, 1);
            assertEquals("Wrong column or single column not found", this.rs.getString(2), "f2");
        }

        // ALTER 1 test
        st.execute("ALTER TABLE testBug63800 CHANGE COLUMN `f2` `f2` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00', "
                + "ADD COLUMN `f3` DATETIME NULL ON UPDATE CURRENT_TIMESTAMP  AFTER `f2`");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        cnt = 0;
        while (this.rs.next()) {
            cnt++;
            assertEquals("1 column must be found", cnt, 1);
            assertEquals("Wrong column or single column not found", this.rs.getString(2), "f3");
        }

        // ALTER 2 test
        st.execute("ALTER TABLE testBug63800 CHANGE COLUMN `f2` `f2` TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        cnt = 0;
        while (this.rs.next()) {
            cnt++;
        }
        assertEquals("2 column must be found", cnt, 2);

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP, f2 DATETIME ON UPDATE CURRENT_TIMESTAMP, f3 TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        cnt = 0;
        while (this.rs.next()) {
            cnt++;
        }
        assertEquals("3 column must be found", cnt, 3);

    }

    /**
     * Tests fix for BUG#16436511 - getDriverName() returns a string with company name "MySQL-AB"
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug16436511() throws Exception {
        DatabaseMetaData dbmd = this.conn.getMetaData();
        assertEquals("MySQL Connector Java", dbmd.getDriverName());
    }

    /**
     * Test fix for BUG#68098 - DatabaseMetaData.getIndexInfo sorts results incorrectly.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug68098() throws Exception {
        String[] testStepDescription = new String[] { "MySQL MetaData", "I__S MetaData" };
        Connection connUseIS = getConnectionWithProps("useInformationSchema=true");
        Connection[] testConnections = new Connection[] { this.conn, connUseIS };
        String[] expectedIndexesOrder = new String[] { "index_1", "index_1", "index_3", "PRIMARY", "index_2", "index_2", "index_4" };

        this.stmt.execute("DROP TABLE IF EXISTS testBug68098");

        createTable("testBug68098", "(column_1 INT NOT NULL, column_2 INT NOT NULL, column_3 INT NOT NULL, PRIMARY KEY (column_1))");

        this.stmt.execute("CREATE INDEX index_4 ON testBug68098 (column_2)");
        this.stmt.execute("CREATE UNIQUE INDEX index_3 ON testBug68098 (column_3)");
        this.stmt.execute("CREATE INDEX index_2 ON testBug68098 (column_2, column_1)");
        this.stmt.execute("CREATE UNIQUE INDEX index_1 ON testBug68098 (column_3, column_2)");

        for (int i = 0; i < testStepDescription.length; i++) {
            DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();
            this.rs = testDbMetaData.getIndexInfo(null, null, "testBug68098", false, false);
            int ind = 0;
            while (this.rs.next()) {
                assertEquals(testStepDescription[i] + ", sort order is wrong", expectedIndexesOrder[ind++], this.rs.getString("INDEX_NAME"));
            }
            this.rs.close();
        }

        connUseIS.close();
    }

    /**
     * Tests fix for BUG#68307 - getFunctionColumns() returns incorrect "COLUMN_TYPE" information. This JDBC4
     * feature required some changes in method getProcedureColumns().
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug68307() throws Exception {
        String[] testStepDescription = new String[] { "MySQL MetaData", "I__S MetaData" };
        Connection connUseIS = getConnectionWithProps("useInformationSchema=true");
        Connection[] testConnections = new Connection[] { this.conn, connUseIS };

        createFunction("testBug68307_func", "(func_param_in INT) RETURNS INT DETERMINISTIC RETURN 1");

        createProcedure("testBug68307_proc", "(IN proc_param_in INT, OUT proc_param_out INT, INOUT proc_param_inout INT) SELECT 1");

        for (int i = 0; i < testStepDescription.length; i++) {
            DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();
            this.rs = testDbMetaData.getProcedureColumns(null, null, "testBug68307_%", "%");

            while (this.rs.next()) {
                String message = testStepDescription[i] + ", procedure/function <" + this.rs.getString("PROCEDURE_NAME") + "."
                        + this.rs.getString("COLUMN_NAME") + ">";
                if (this.rs.getString("COLUMN_NAME") == null || this.rs.getString("COLUMN_NAME").length() == 0) {
                    assertEquals(message, DatabaseMetaData.procedureColumnReturn, this.rs.getShort("COLUMN_TYPE"));
                } else if (this.rs.getString("COLUMN_NAME").endsWith("_in")) {
                    assertEquals(message, DatabaseMetaData.procedureColumnIn, this.rs.getShort("COLUMN_TYPE"));
                } else if (this.rs.getString("COLUMN_NAME").endsWith("_inout")) {
                    assertEquals(message, DatabaseMetaData.procedureColumnInOut, this.rs.getShort("COLUMN_TYPE"));
                } else if (this.rs.getString("COLUMN_NAME").endsWith("_out")) {
                    assertEquals(message, DatabaseMetaData.procedureColumnOut, this.rs.getShort("COLUMN_TYPE"));
                } else {
                    fail(testStepDescription[i] + ", column '" + this.rs.getString("FUNCTION_NAME") + "." + this.rs.getString("COLUMN_NAME")
                            + "' not expected within test case.");
                }
            }

            this.rs.close();
        }
    }

    /**
     * Tests fix for BUG#44451 - getTables does not return resultset with expected columns.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug44451() throws Exception {
        String methodName;
        List<String> expectedFields;
        String[] testStepDescription = new String[] { "MySQL MetaData", "I__S MetaData" };
        Connection connUseIS = getConnectionWithProps("useInformationSchema=true");
        Connection[] testConnections = new Connection[] { this.conn, connUseIS };

        methodName = "getColumns()";
        expectedFields = Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH",
                "DECIMAL_DIGITS", "NUM_PREC_RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SCOPE_CATALOG", "SCOPE_SCHEMA", "SCOPE_TABLE", "SOURCE_DATA_TYPE", "IS_AUTOINCREMENT",
                "IS_GENERATEDCOLUMN");
        for (int i = 0; i < testStepDescription.length; i++) {
            DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();
            this.rs = testDbMetaData.getColumns(null, null, "%", "%");
            checkReturnedColumnsForBug44451(testStepDescription[i], methodName, expectedFields, this.rs);
            this.rs.close();
        }

        methodName = "getProcedureColumns()";
        expectedFields = Arrays.asList("PROCEDURE_CAT", "PROCEDURE_SCHEM", "PROCEDURE_NAME", "COLUMN_NAME", "COLUMN_TYPE", "DATA_TYPE", "TYPE_NAME",
                "PRECISION", "LENGTH", "SCALE", "RADIX", "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB", "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION", "IS_NULLABLE", "SPECIFIC_NAME");
        for (int i = 0; i < testStepDescription.length; i++) {
            DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();
            this.rs = testDbMetaData.getProcedureColumns(null, null, "%", "%");
            checkReturnedColumnsForBug44451(testStepDescription[i], methodName, expectedFields, this.rs);
            this.rs.close();
        }

        methodName = "getTables()";
        expectedFields = Arrays.asList("TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME",
                "SELF_REFERENCING_COL_NAME", "REF_GENERATION");
        for (int i = 0; i < testStepDescription.length; i++) {
            DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();
            this.rs = testDbMetaData.getTables(null, null, "%", null);
            checkReturnedColumnsForBug44451(testStepDescription[i], methodName, expectedFields, this.rs);
            this.rs.close();
        }

        methodName = "getUDTs()";
        expectedFields = Arrays.asList("TYPE_CAT", "TYPE_SCHEM", "TYPE_NAME", "CLASS_NAME", "DATA_TYPE", "REMARKS", "BASE_TYPE");
        for (int i = 0; i < testStepDescription.length; i++) {
            DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();
            this.rs = testDbMetaData.getUDTs(null, null, "%", null);
            checkReturnedColumnsForBug44451(testStepDescription[i], methodName, expectedFields, this.rs);
            this.rs.close();
        }

        connUseIS.close();
    }

    private void checkReturnedColumnsForBug44451(String stepDescription, String methodName, List<String> expectedFields, ResultSet resultSetToCheck)
            throws Exception {
        ResultSetMetaData rsMetaData = resultSetToCheck.getMetaData();
        int numberOfColumns = rsMetaData.getColumnCount();

        assertEquals(stepDescription + ", wrong column count in method '" + methodName + "'.", expectedFields.size(), numberOfColumns);
        for (int i = 0; i < numberOfColumns; i++) {
            int position = i + 1;
            assertEquals(stepDescription + ", wrong column at position '" + position + "' in method '" + methodName + "'.", expectedFields.get(i),
                    rsMetaData.getColumnName(position));
        }
        this.rs.close();
    }

    /**
     * Tests fix for BUG#65871 - DatabaseMetaData.getColumns() thows an MySQLSyntaxErrorException.
     * Delimited names of databases and tables are handled correctly now. The edge case is ANSI quoted
     * identifiers with leading and trailing "`" symbols, for example CREATE DATABASE "`dbname`". Methods
     * like DatabaseMetaData.getColumns() allow parameters passed both in unquoted and quoted form,
     * quoted form is not JDBC-compliant but used by third party tools. So when you pass the indentifier
     * "`dbname`" in unquoted form (`dbname`) driver handles it as quoted by "`" symbol. To handle such
     * identifiers correctly a new behavior was added to pedantic mode (connection property pedantic=true),
     * now if it set to true methods like DatabaseMetaData.getColumns() treat all parameters as unquoted.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug65871() throws Exception {
        createTable("testbug65871_foreign",
                "(cpd_foreign_1_id int(8) not null, cpd_foreign_2_id int(8) not null," + "primary key (cpd_foreign_1_id, cpd_foreign_2_id)) ", "InnoDB");

        Connection pedanticConn = null;
        Connection pedanticConn_IS = null;
        Connection nonPedanticConn = null;
        Connection nonPedanticConn_IS = null;

        try {
            Properties props = new Properties();
            props.setProperty("sessionVariables", "sql_mode=ansi");
            nonPedanticConn = getConnectionWithProps(props);

            props.setProperty("useInformationSchema", "true");
            nonPedanticConn_IS = getConnectionWithProps(props);

            props.setProperty("pedantic", "true");
            pedanticConn_IS = getConnectionWithProps(props);

            props.setProperty("useInformationSchema", "false");
            pedanticConn = getConnectionWithProps(props);

            System.out.println("1. Non-pedantic, without I_S.");
            testBug65871_testCatalogs(nonPedanticConn);

            System.out.println("2. Pedantic, without I_S.");
            testBug65871_testCatalogs(pedanticConn);

            System.out.println("3. Non-pedantic, with I_S.");
            testBug65871_testCatalogs(nonPedanticConn_IS);

            System.out.println("4. Pedantic, with I_S.");
            testBug65871_testCatalogs(pedanticConn_IS);

        } finally {
            if (pedanticConn != null) {
                pedanticConn.close();
            }
            if (nonPedanticConn != null) {
                nonPedanticConn.close();
            }
        }
    }

    private void testBug65871_testCatalogs(Connection conn1) throws Exception {
        testBug65871_testCatalog("db1`testbug65871", StringUtils.quoteIdentifier("db1`testbug65871", ((ConnectionProperties) conn1).getPedantic()), conn1);

        testBug65871_testCatalog("db2`testbug65871", StringUtils.quoteIdentifier("db2`testbug65871", "\"", ((ConnectionProperties) conn1).getPedantic()),
                conn1);

        testBug65871_testCatalog("`db3`testbug65871`", StringUtils.quoteIdentifier("`db3`testbug65871`", "\"", ((ConnectionProperties) conn1).getPedantic()),
                conn1);
    }

    private void testBug65871_testCatalog(String unquotedDbName, String quotedDbName, Connection conn1) throws Exception {

        Statement st1 = null;

        try {
            st1 = conn1.createStatement();

            // 1. catalog
            st1.executeUpdate("DROP DATABASE IF EXISTS " + quotedDbName);
            st1.executeUpdate("CREATE DATABASE " + quotedDbName);
            this.rs = st1.executeQuery("show databases like '" + unquotedDbName + "'");
            if (this.rs.next()) {
                assertEquals(unquotedDbName, this.rs.getString(1));
            } else {
                fail("Database " + unquotedDbName + " (quoted " + quotedDbName + ") not found.");
            }

            testBug65871_testTable(unquotedDbName, quotedDbName, "table1`testbug65871",
                    StringUtils.quoteIdentifier("table1`testbug65871", ((ConnectionProperties) conn1).getPedantic()), conn1, st1);

            testBug65871_testTable(unquotedDbName, quotedDbName, "table2`testbug65871",
                    StringUtils.quoteIdentifier("table2`testbug65871", "\"", ((ConnectionProperties) conn1).getPedantic()), conn1, st1);

            testBug65871_testTable(unquotedDbName, quotedDbName, "table3\"testbug65871",
                    StringUtils.quoteIdentifier("table3\"testbug65871", "\"", ((ConnectionProperties) conn1).getPedantic()), conn1, st1);

            testBug65871_testTable(unquotedDbName, quotedDbName, "`table4`testbug65871`",
                    StringUtils.quoteIdentifier("`table4`testbug65871`", "\"", ((ConnectionProperties) conn1).getPedantic()), conn1, st1);

        } finally {
            if (st1 != null) {
                st1.executeUpdate("DROP DATABASE IF EXISTS " + quotedDbName);
                st1.close();
            }
        }

    }

    private void testBug65871_testTable(String unquotedDbName, String quotedDbName, String unquotedTableName, String quotedTableName, Connection conn1,
            Statement st1) throws Exception {

        StringBuilder failedTests = new StringBuilder();
        try {

            String sql = "CREATE  TABLE " + quotedDbName + "." + quotedTableName + "(\"`B`EST`\" INT NOT NULL PRIMARY KEY, `C\"1` int(11) DEFAULT NULL,"
                    + " TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP, \"cpd_f\"\"oreign_1_id\" int(8) not null,"
                    + " \"`cpd_f\"\"oreign_2_id`\" int(8) not null, KEY `NEWINX` (`C\"1`), KEY `NEWINX2` (`C\"1`, `TS`),"
                    + " foreign key (\"cpd_f\"\"oreign_1_id\", \"`cpd_f\"\"oreign_2_id`\")  references " + this.conn.getCatalog()
                    + ".testbug65871_foreign(cpd_foreign_1_id, cpd_foreign_2_id),  CONSTRAINT `APPFK` FOREIGN KEY (`C\"1`) REFERENCES " + quotedDbName + "."
                    + quotedTableName + " (`C\"1`)) ENGINE=InnoDB";
            st1.executeUpdate(sql);

            // 1. Create table
            try {
                this.rs = st1.executeQuery("SHOW TABLES FROM " + quotedDbName + " LIKE '" + unquotedTableName + "'");
                if (!this.rs.next() || !unquotedTableName.equals(this.rs.getString(1))) {
                    failedTests.append(sql + "\n");
                }
            } catch (Exception e) {
                failedTests.append(sql + "\n");
            }

            // 2. extractForeignKeyFromCreateTable(...)
            if (!(versionMeetsMinimum(5, 1) && !versionMeetsMinimum(5, 2))) {
                try {
                    this.rs = ((com.mysql.jdbc.DatabaseMetaData) conn1.getMetaData()).extractForeignKeyFromCreateTable(unquotedDbName, unquotedTableName);
                    if (!this.rs.next()) {
                        failedTests.append("conn.getMetaData.extractForeignKeyFromCreateTable(unquotedDbName, unquotedTableName);\n");
                    }
                } catch (Exception e) {
                    failedTests.append("conn.getMetaData.extractForeignKeyFromCreateTable(unquotedDbName, unquotedTableName);\n");
                }
            }

            // 3. getColumns(...)
            try {
                boolean found = false;
                this.rs = conn1.getMetaData().getColumns(unquotedDbName, null, unquotedTableName, "`B`EST`");
                while (this.rs.next()) {
                    if ("`B`EST`".equals(this.rs.getString("COLUMN_NAME"))) {
                        found = true;
                    }
                }
                if (!found) {
                    failedTests.append("conn.getMetaData.getColumns(unquotedDbName, null, unquotedTableName, null);\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.getColumns(unquotedDbName, null, unquotedTableName, null);\n");
            }

            // 4. getBestRowIdentifier(...)
            try {
                this.rs = conn1.getMetaData().getBestRowIdentifier(unquotedDbName, null, unquotedTableName, DatabaseMetaData.bestRowNotPseudo, true);
                if (!this.rs.next() || !"`B`EST`".equals(this.rs.getString("COLUMN_NAME"))) {
                    failedTests.append(
                            "conn.getMetaData.getBestRowIdentifier(unquotedDbName, null, unquotedTableName, DatabaseMetaData.bestRowNotPseudo, " + "true);\n");
                }
            } catch (Exception e) {
                failedTests
                        .append("conn.getMetaData.getBestRowIdentifier(unquotedDbName, null, unquotedTableName, DatabaseMetaData.bestRowNotPseudo, true);\n");
            }

            // 5. getCrossReference(...)
            try {
                this.rs = conn1.getMetaData().getCrossReference(this.conn.getCatalog(), null, "testbug65871_foreign", unquotedDbName, null, unquotedTableName);
                if (!this.rs.next()) {
                    failedTests.append("conn.getMetaData.getCrossReference(this.conn.getCatalog(), null, \"testbug65871_foreign\", unquotedDbName, null, "
                            + "unquotedTableName);\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.getCrossReference(this.conn.getCatalog(), null, \"testbug65871_foreign\", unquotedDbName, null, "
                        + "unquotedTableName);\n");
            }

            // 6.getExportedKeys(...)
            try {
                this.rs = conn1.getMetaData().getExportedKeys(unquotedDbName, null, unquotedTableName);
                if (!this.rs.next()) {
                    failedTests.append("conn.getMetaData.getExportedKeys(unquotedDbName, null, unquotedTableName);\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.getExportedKeys(unquotedDbName, null, unquotedTableName);\n");
            }

            // 7. getImportedKeys(...)
            try {
                this.rs = conn1.getMetaData().getImportedKeys(unquotedDbName, null, unquotedTableName);
                if (!this.rs.next()) {
                    failedTests.append("conn.getMetaData.getImportedKeys(unquotedDbName, null, unquotedTableName);\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.getImportedKeys(unquotedDbName, null, unquotedTableName);\n");
            }

            // 8. getIndexInfo(...)
            try {
                this.rs = conn1.getMetaData().getIndexInfo(unquotedDbName, null, unquotedTableName, true, false);
                if (!this.rs.next()) {
                    failedTests.append("conn.getMetaData.getIndexInfo(unquotedDbName, null, unquotedTableName, true, false);\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.getIndexInfo(unquotedDbName, null, unquotedTableName, true, false);\n");
            }

            // 9. getPrimaryKeys(...)
            try {
                this.rs = conn1.getMetaData().getPrimaryKeys(unquotedDbName, null, unquotedTableName);
                if (!this.rs.next()) {
                    failedTests.append("conn.getMetaData.getPrimaryKeys(unquotedDbName, null, unquotedTableName);\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.getPrimaryKeys(unquotedDbName, null, unquotedTableName);\n");
            }

            // 10. getTables(...)
            try {
                this.rs = conn1.getMetaData().getTables(unquotedDbName, null, unquotedTableName, new String[] { "TABLE" });
                if (!this.rs.next()) {
                    failedTests.append("conn.getMetaData.getTables(unquotedDbName, null, unquotedTableName, new String[] {\"TABLE\"});\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.getTables(unquotedDbName, null, unquotedTableName, new String[] {\"TABLE\"});\n");
            }

            // 11. getVersionColumns(...)
            try {
                this.rs = conn1.getMetaData().getVersionColumns(unquotedDbName, null, unquotedTableName);
                if (!this.rs.next() || !"TS".equals(this.rs.getString(2))) {
                    failedTests.append("conn.getMetaData.getVersionColumns(unquotedDbName, null, unquotedTableName);\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.getVersionColumns(unquotedDbName, null, unquotedTableName);\n");
            }

        } finally {
            try {
                st1.executeUpdate("DROP TABLE IF EXISTS " + quotedDbName + "." + quotedTableName);
            } catch (Exception e) {
                failedTests.append("DROP TABLE IF EXISTS " + quotedDbName + "." + quotedTableName + "\n");
            }
        }

        if (failedTests.length() > 0) {
            throw new Exception("Failed tests for catalog " + quotedDbName + " and table " + quotedTableName + " ("
                    + (((ConnectionProperties) conn1).getPedantic() ? "pedantic mode" : "non-pedantic mode") + "):\n" + failedTests.toString());
        }
    }

    /**
     * Tests fix for BUG#69298 - Different outcome from DatabaseMetaData.getFunctions() when using I__S.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug69298() throws Exception {
        if (Util.isJdbc4()) {
            return;
        }

        Connection testConn;

        createFunction("testBug69298_func", "(param_func INT) RETURNS INT COMMENT 'testBug69298_func comment' DETERMINISTIC RETURN 1");
        createProcedure("testBug69298_proc", "(IN param_proc INT) COMMENT 'testBug69298_proc comment' SELECT 1");

        // test with standard connection
        assertFalse("Property useInformationSchema should be false", ((ConnectionProperties) this.conn).getUseInformationSchema());
        assertTrue("Property getProceduresReturnsFunctions should be true", ((ConnectionProperties) this.conn).getGetProceduresReturnsFunctions());
        checkGetProceduresForBug69298("Std. Connection MetaData", this.conn);
        checkGetProcedureColumnsForBug69298("Std. Connection MetaData", this.conn);

        // test with property useInformationSchema=true
        testConn = getConnectionWithProps("useInformationSchema=true");
        assertTrue("Property useInformationSchema should be true", ((ConnectionProperties) testConn).getUseInformationSchema());
        assertTrue("Property getProceduresReturnsFunctions should be true", ((ConnectionProperties) testConn).getGetProceduresReturnsFunctions());
        checkGetProceduresForBug69298("Prop. useInfoSchema(1) MetaData", testConn);
        checkGetProcedureColumnsForBug69298("Prop. useInfoSchema(1) MetaData", testConn);
        testConn.close();

        // test with property getProceduresReturnsFunctions=false
        testConn = getConnectionWithProps("getProceduresReturnsFunctions=false");
        assertFalse("Property useInformationSchema should be false", ((ConnectionProperties) testConn).getUseInformationSchema());
        assertFalse("Property getProceduresReturnsFunctions should be false", ((ConnectionProperties) testConn).getGetProceduresReturnsFunctions());
        checkGetProceduresForBug69298("Prop. getProcRetFunc(0) MetaData", testConn);
        checkGetProcedureColumnsForBug69298("Prop. getProcRetFunc(0) MetaData", testConn);
        testConn.close();

        // test with property useInformationSchema=true & getProceduresReturnsFunctions=false
        testConn = getConnectionWithProps("useInformationSchema=true,getProceduresReturnsFunctions=false");
        assertTrue("Property useInformationSchema should be true", ((ConnectionProperties) testConn).getUseInformationSchema());
        assertFalse("Property getProceduresReturnsFunctions should be false", ((ConnectionProperties) testConn).getGetProceduresReturnsFunctions());
        checkGetProceduresForBug69298("Prop. useInfoSchema(1) + getProcRetFunc(0) MetaData", testConn);
        checkGetProcedureColumnsForBug69298("Prop. useInfoSchema(1) + getProcRetFunc(0) MetaData", testConn);
        testConn.close();
    }

    private void checkGetProceduresForBug69298(String stepDescription, Connection testConn) throws Exception {
        DatabaseMetaData testDbMetaData = testConn.getMetaData();
        ResultSet proceduresMD = testDbMetaData.getProcedures(null, null, "testBug69298_%");
        String sd = stepDescription + " getProcedures() ";

        assertTrue(sd + "1st of 2 rows expected.", proceduresMD.next());

        // function: testBug69298_func
        assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), proceduresMD.getString("PROCEDURE_CAT"));
        assertEquals(sd + "-> PROCEDURE_SCHEM", null, proceduresMD.getString("PROCEDURE_SCHEM"));
        assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_func", proceduresMD.getString("PROCEDURE_NAME"));
        assertEquals(sd + "-> REMARKS", "testBug69298_func comment", proceduresMD.getString("REMARKS"));
        assertEquals(sd + "-> PROCEDURE_TYPE", DatabaseMetaData.procedureReturnsResult, proceduresMD.getShort("PROCEDURE_TYPE"));

        assertTrue(sd + "2nd of 2 rows expected.", proceduresMD.next());

        // procedure: testBug69298_proc
        assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), proceduresMD.getString("PROCEDURE_CAT"));
        assertEquals(sd + "-> PROCEDURE_SCHEM", null, proceduresMD.getString("PROCEDURE_SCHEM"));
        assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_proc", proceduresMD.getString("PROCEDURE_NAME"));
        assertEquals(sd + "-> REMARKS", "testBug69298_proc comment", proceduresMD.getString("REMARKS"));
        assertEquals(sd + "-> PROCEDURE_TYPE", DatabaseMetaData.procedureNoResult, proceduresMD.getShort("PROCEDURE_TYPE"));

        assertFalse(stepDescription + "no more rows expected.", proceduresMD.next());
    }

    private void checkGetProcedureColumnsForBug69298(String stepDescription, Connection testConn) throws Exception {
        DatabaseMetaData testDbMetaData = testConn.getMetaData();
        ResultSet procColsMD = testDbMetaData.getProcedureColumns(null, null, "testBug69298_%", "%");
        String sd = stepDescription + " getProcedureColumns() ";

        assertTrue(sd + "1st of 3 rows expected.", procColsMD.next());

        // function column: testBug69298_func return
        assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), procColsMD.getString("PROCEDURE_CAT"));
        assertEquals(sd + "-> PROCEDURE_SCHEM", null, procColsMD.getString("PROCEDURE_SCHEM"));
        assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_func", procColsMD.getString("PROCEDURE_NAME"));
        assertEquals(sd + "-> COLUMN_NAME", "", procColsMD.getString("COLUMN_NAME"));
        assertEquals(sd + "-> COLUMN_TYPE", DatabaseMetaData.procedureColumnReturn, procColsMD.getShort("COLUMN_TYPE"));
        assertEquals(sd + "-> DATA_TYPE", Types.INTEGER, procColsMD.getInt("DATA_TYPE"));
        assertEquals(sd + "-> TYPE_NAME", "INT", procColsMD.getString("TYPE_NAME"));
        assertEquals(sd + "-> PRECISION", 10, procColsMD.getInt("PRECISION"));
        assertEquals(sd + "-> LENGTH", 10, procColsMD.getInt("LENGTH"));
        assertEquals(sd + "-> SCALE", 0, procColsMD.getShort("SCALE"));
        assertEquals(sd + "-> RADIX", 10, procColsMD.getShort("RADIX"));
        assertEquals(sd + "-> NULLABLE", DatabaseMetaData.procedureNullable, procColsMD.getShort("NULLABLE"));
        assertEquals(sd + "-> REMARKS", null, procColsMD.getString("REMARKS"));

        assertTrue(sd + "2nd of 3 rows expected.", procColsMD.next());

        // function column: testBug69298_func.param_func
        assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), procColsMD.getString("PROCEDURE_CAT"));
        assertEquals(sd + "-> PROCEDURE_SCHEM", null, procColsMD.getString("PROCEDURE_SCHEM"));
        assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_func", procColsMD.getString("PROCEDURE_NAME"));
        assertEquals(sd + "-> COLUMN_NAME", "param_func", procColsMD.getString("COLUMN_NAME"));
        assertEquals(sd + "-> COLUMN_TYPE", DatabaseMetaData.procedureColumnIn, procColsMD.getShort("COLUMN_TYPE"));
        assertEquals(sd + "-> DATA_TYPE", Types.INTEGER, procColsMD.getInt("DATA_TYPE"));
        assertEquals(sd + "-> TYPE_NAME", "INT", procColsMD.getString("TYPE_NAME"));
        assertEquals(sd + "-> PRECISION", 10, procColsMD.getInt("PRECISION"));
        assertEquals(sd + "-> LENGTH", 10, procColsMD.getInt("LENGTH"));
        assertEquals(sd + "-> SCALE", 0, procColsMD.getShort("SCALE"));
        assertEquals(sd + "-> RADIX", 10, procColsMD.getShort("RADIX"));
        assertEquals(sd + "-> NULLABLE", DatabaseMetaData.procedureNullable, procColsMD.getShort("NULLABLE"));
        assertEquals(sd + "-> REMARKS", null, procColsMD.getString("REMARKS"));

        assertTrue(sd + "3rd of 3 rows expected.", procColsMD.next());

        // procedure column: testBug69298_proc.param_proc
        assertEquals(sd + "-> PROCEDURE_CAT", testConn.getCatalog(), procColsMD.getString("PROCEDURE_CAT"));
        assertEquals(sd + "-> PROCEDURE_SCHEM", null, procColsMD.getString("PROCEDURE_SCHEM"));
        assertEquals(sd + "-> PROCEDURE_NAME", "testBug69298_proc", procColsMD.getString("PROCEDURE_NAME"));
        assertEquals(sd + "-> COLUMN_NAME", "param_proc", procColsMD.getString("COLUMN_NAME"));
        assertEquals(sd + "-> COLUMN_TYPE", DatabaseMetaData.procedureColumnIn, procColsMD.getShort("COLUMN_TYPE"));
        assertEquals(sd + "-> DATA_TYPE", Types.INTEGER, procColsMD.getInt("DATA_TYPE"));
        assertEquals(sd + "-> TYPE_NAME", "INT", procColsMD.getString("TYPE_NAME"));
        assertEquals(sd + "-> PRECISION", 10, procColsMD.getInt("PRECISION"));
        assertEquals(sd + "-> LENGTH", 10, procColsMD.getInt("LENGTH"));
        assertEquals(sd + "-> SCALE", 0, procColsMD.getShort("SCALE"));
        assertEquals(sd + "-> RADIX", 10, procColsMD.getShort("RADIX"));
        assertEquals(sd + "-> NULLABLE", DatabaseMetaData.procedureNullable, procColsMD.getShort("NULLABLE"));
        assertEquals(sd + "-> REMARKS", null, procColsMD.getString("REMARKS"));

        assertFalse(sd + "no more rows expected.", procColsMD.next());
    }

    /**
     * Tests fix for BUG#17248345 - GETFUNCTIONCOLUMNS() METHOD RETURNS COLUMNS OF PROCEDURE. (this happens when
     * functions and procedures have a common name)
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug17248345() throws Exception {
        if (Util.isJdbc4()) {
            // there is a specific JCDB4 test for this
            return;
        }

        Connection testConn;

        // create one stored procedure and one function with same name
        createFunction("testBug17248345", "(funccol INT) RETURNS INT DETERMINISTIC RETURN 1");
        createProcedure("testBug17248345", "(IN proccol INT) SELECT 1");

        // test with standard connection (getProceduresReturnsFunctions=true & useInformationSchema=false)
        assertFalse("Property useInformationSchema should be false", ((ConnectionProperties) this.conn).getUseInformationSchema());
        assertTrue("Property getProceduresReturnsFunctions should be true", ((ConnectionProperties) this.conn).getGetProceduresReturnsFunctions());
        checkMetaDataInfoForBug17248345(this.conn);

        // test with property useInformationSchema=true (getProceduresReturnsFunctions=true)
        testConn = getConnectionWithProps("useInformationSchema=true");
        assertTrue("Property useInformationSchema should be true", ((ConnectionProperties) testConn).getUseInformationSchema());
        assertTrue("Property getProceduresReturnsFunctions should be true", ((ConnectionProperties) testConn).getGetProceduresReturnsFunctions());
        checkMetaDataInfoForBug17248345(testConn);
        testConn.close();

        // test with property getProceduresReturnsFunctions=false (useInformationSchema=false)
        testConn = getConnectionWithProps("getProceduresReturnsFunctions=false");
        assertFalse("Property useInformationSchema should be false", ((ConnectionProperties) testConn).getUseInformationSchema());
        assertFalse("Property getProceduresReturnsFunctions should be false", ((ConnectionProperties) testConn).getGetProceduresReturnsFunctions());
        checkMetaDataInfoForBug17248345(testConn);
        testConn.close();

        // test with property useInformationSchema=true & getProceduresReturnsFunctions=false
        testConn = getConnectionWithProps("useInformationSchema=true,getProceduresReturnsFunctions=false");
        assertTrue("Property useInformationSchema should be true", ((ConnectionProperties) testConn).getUseInformationSchema());
        assertFalse("Property getProceduresReturnsFunctions should be false", ((ConnectionProperties) testConn).getGetProceduresReturnsFunctions());
        checkMetaDataInfoForBug17248345(testConn);
        testConn.close();
    }

    private void checkMetaDataInfoForBug17248345(Connection testConn) throws Exception {
        DatabaseMetaData testDbMetaData = testConn.getMetaData();
        ResultSet rsMD;
        boolean useInfoSchema = ((ConnectionProperties) testConn).getUseInformationSchema();
        boolean getProcRetFunc = ((ConnectionProperties) testConn).getGetProceduresReturnsFunctions();
        String stepDescription = "Prop. useInfoSchema(" + (useInfoSchema ? 1 : 0) + ") + getProcRetFunc(" + (getProcRetFunc ? 1 : 0) + "):";
        String sd;

        // getProcedures() must return 2 records, even if getProceduresReturnsFunctions is false once this flag only
        // applies to JDBC4. When exists a procedure and a function with same name, function is returned first.
        sd = stepDescription + " getProcedures() ";
        rsMD = testDbMetaData.getProcedures(null, null, "testBug17248345");
        assertTrue(sd + "1st of 2 rows expected.", rsMD.next());
        assertEquals(sd + " -> PROCEDURE_NAME", "testBug17248345", rsMD.getString("PROCEDURE_NAME"));
        assertTrue(sd + "2nd of 2 rows expected.", rsMD.next());
        assertEquals(sd + " -> PROCEDURE_NAME", "testBug17248345", rsMD.getString("PROCEDURE_NAME"));
        assertFalse(sd + "no more rows expected.", rsMD.next());

        // getProcedureColumns() must return 3 records, even if getProceduresReturnsFunctions is false once this flag
        // only applies to JDBC4. When exists a procedure and a function with same name, function is returned first.
        sd = stepDescription + " getProcedureColumns() ";
        rsMD = testDbMetaData.getProcedureColumns(null, null, "testBug17248345", "%");
        assertTrue(sd + "1st of 3 rows expected.", rsMD.next());
        assertEquals(sd + " -> PROCEDURE_NAME", "testBug17248345", rsMD.getString("PROCEDURE_NAME"));
        assertEquals(sd + " -> COLUMN_NAME", "", rsMD.getString("COLUMN_NAME"));
        assertTrue(sd + "2nd of 3 rows expected.", rsMD.next());
        assertEquals(sd + " -> PROCEDURE_NAME", "testBug17248345", rsMD.getString("PROCEDURE_NAME"));
        assertEquals(sd + " -> COLUMN_NAME", "funccol", rsMD.getString("COLUMN_NAME"));
        assertTrue(sd + "3rd of 3 rows expected.", rsMD.next());
        assertEquals(sd + " -> PROCEDURE_NAME", "testBug17248345", rsMD.getString("PROCEDURE_NAME"));
        assertEquals(sd + " -> COLUMN_NAME", "proccol", rsMD.getString("COLUMN_NAME"));
        assertFalse(sd + "no more rows expected.", rsMD.next());
    }

    /**
     * Tests fix for BUG#69290 - JDBC Table type "SYSTEM TABLE" is used inconsistently.
     * 
     * Tests DatabaseMetaData.getTableTypes() and DatabaseMetaData.getTables() against schemas: mysql,
     * information_schema, performance_schema, test.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug69290() throws Exception {
        String[] testStepDescription = new String[] { "MySQL MetaData", "I__S MetaData" };
        Connection connUseIS = getConnectionWithProps("useInformationSchema=true");
        Connection connNullAll = getConnectionWithProps("nullCatalogMeansCurrent=false");
        Connection connUseISAndNullAll = getConnectionWithProps("useInformationSchema=true,nullCatalogMeansCurrent=false");
        final String testCatalog = this.conn.getCatalog();

        Connection[] testConnections = new Connection[] { this.conn, connUseIS };

        // check table types returned in getTableTypes()
        final List<String> tableTypes = Arrays.asList(new String[] { "LOCAL TEMPORARY", "SYSTEM TABLE", "SYSTEM VIEW", "TABLE", "VIEW" });

        for (int i = 0; i < testStepDescription.length; i++) {
            DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();
            this.rs = testDbMetaData.getTableTypes();

            int idx = 0;
            while (this.rs.next()) {
                String message = testStepDescription[i] + ", table type '" + this.rs.getString("TABLE_TYPE") + "'";
                if (idx >= tableTypes.size()) {
                    fail(message + " not expected.");
                }
                assertEquals(message, tableTypes.get(idx++), this.rs.getString("TABLE_TYPE"));
            }
        }

        // create table and view in '(test)' schema
        createTable("testBug69290_table", "(c1 INT)");
        createView("testBug69290_view", "AS SELECT * FROM testBug69290_table WHERE c1 > 1");

        int[][] countResults = new int[][] { { 0, 0, 0 }, { 0, 0, 0 } };

        // check table types returned in getTables() for each catalog/schema
        for (int i = 0; i < testStepDescription.length; i++) {
            DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();

            // check catalog/schema 'information_schema'
            this.rs = testDbMetaData.getTables("information_schema", null, "%", null);
            while (this.rs.next()) {
                assertEquals(testStepDescription[i] + ", 'information_schema' catalog/schema, wrong table type for '" + this.rs.getString("TABLE_NAME") + "'.",
                        "SYSTEM VIEW", this.rs.getString("TABLE_TYPE"));
                countResults[i][0]++;
            }

            // check catalog/schema 'mysql'
            this.rs = testDbMetaData.getTables("mysql", null, "%", null);
            while (this.rs.next()) {
                assertEquals(testStepDescription[i] + ", 'mysql' catalog/schema, wrong table type for '" + this.rs.getString("TABLE_NAME") + "'.",
                        "SYSTEM TABLE", this.rs.getString("TABLE_TYPE"));
                countResults[i][1]++;
            }

            // check catalog/schema 'performance_schema'
            this.rs = testDbMetaData.getTables("performance_schema", null, "%", null);
            while (this.rs.next()) {
                assertEquals(testStepDescription[i] + ", 'performance_schema' catalog/schema, wrong table type for '" + this.rs.getString("TABLE_NAME") + "'.",
                        "SYSTEM TABLE", this.rs.getString("TABLE_TYPE"));
                countResults[i][2]++;
            }

            // check catalog/schema '(test)'
            this.rs = testDbMetaData.getTables(testCatalog, null, "testBug69290_%", null);
            assertTrue(testStepDescription[i] + ", '" + testCatalog + "' catalog/schema, expected row from getTables().", this.rs.next());
            assertEquals(testStepDescription[i] + ", '" + testCatalog + "' catalog/schema, wrong table type for '" + this.rs.getString("TABLE_NAME") + "'.",
                    "TABLE", this.rs.getString("TABLE_TYPE"));
            assertTrue(testStepDescription[i] + ", '" + testCatalog + "' catalog/schema, expected row from getTables().", this.rs.next());
            assertEquals(testStepDescription[i] + ", '" + testCatalog + "' catalog/schema, wrong table type for '" + this.rs.getString("TABLE_NAME") + "'.",
                    "VIEW", this.rs.getString("TABLE_TYPE"));
        }

        // compare results count
        assertTrue("The number of results from getTables() MySQl(" + countResults[0][0] + ") and I__S(" + countResults[1][0]
                + ") should be the same for 'information_schema' catalog/schema.", countResults[0][0] == countResults[1][0]);
        assertTrue("The number of results from getTables() MySQl(" + countResults[0][1] + ") and I__S(" + countResults[1][1]
                + ") should be the same for 'mysql' catalog/schema.", countResults[0][1] == countResults[1][1]);
        assertTrue("The number of results from getTables() MySQl(" + countResults[0][2] + ") and I__S(" + countResults[1][2]
                + ") should be the same for 'performance_schema' catalog/schema.", countResults[0][2] == countResults[1][2]);

        testConnections = new Connection[] { connNullAll, connUseISAndNullAll };
        countResults = new int[][] { { 0, 0, 0, 0, 0 }, { 0, 0, 0, 0, 0 } };

        // check table types returned in getTables() for all catalogs/schemas and filter by table type (tested with property nullCatalogMeansCurrent=false)
        for (int i = 0; i < testStepDescription.length; i++) {
            DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();
            int j = 0;

            // check table type filters
            for (String tableType : tableTypes) {
                this.rs = testDbMetaData.getTables(null, null, "%", new String[] { tableType });
                while (this.rs.next()) {
                    assertEquals(
                            testStepDescription[i] + ", table type filter '" + tableType + "', wrong table type for '" + this.rs.getString("TABLE_NAME") + "'.",
                            tableType, this.rs.getString("TABLE_TYPE"));
                    countResults[i][j]++;
                }
                j++;
            }
        }

        // compare results count
        int i = 0;
        for (String tableType : tableTypes) {
            assertTrue("The number of results from getTables() MySQl(" + countResults[0][i] + ") and I__S(" + countResults[1][i] + ") should be the same for '"
                    + tableType + "' table type filter.", countResults[0][i] == countResults[1][i]);
            i++;
        }
    }

    /**
     * Tests fix for BUG#35115 - yearIsDateType=false has no effect on result's column type and class.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug35115() throws Exception {
        Connection testConnection = null;
        ResultSetMetaData rsMetaData = null;

        createTable("testBug35115", "(year YEAR)");

        this.stmt = this.conn.createStatement();
        this.stmt.executeUpdate("INSERT INTO testBug35115 VALUES ('2002'), ('2013')");

        /*
         * test connection with property 'yearIsDateType=false'
         */
        testConnection = getConnectionWithProps("yearIsDateType=false");
        this.stmt = testConnection.createStatement();
        this.rs = this.stmt.executeQuery("SELECT * FROM testBug35115");
        rsMetaData = this.rs.getMetaData();

        assertTrue(this.rs.next());
        assertEquals("YEAR columns should be treated as java.sql.Types.DATE", Types.DATE, rsMetaData.getColumnType(1));
        assertEquals("YEAR columns should be identified as 'YEAR'", "YEAR", rsMetaData.getColumnTypeName(1));
        assertEquals("YEAR columns should be mapped to java.lang.Short", java.lang.Short.class.getName(), rsMetaData.getColumnClassName(1));
        assertEquals("YEAR columns should be returned as java.lang.Short", java.lang.Short.class.getName(), this.rs.getObject(1).getClass().getName());

        testConnection.close();

        /*
         * test connection with property 'yearIsDateType=true'
         */
        testConnection = getConnectionWithProps("yearIsDateType=true");
        this.stmt = testConnection.createStatement();
        this.rs = this.stmt.executeQuery("SELECT * FROM testBug35115");
        rsMetaData = this.rs.getMetaData();

        assertTrue(this.rs.next());
        assertEquals("YEAR columns should be treated as java.sql.Types.DATE", Types.DATE, rsMetaData.getColumnType(1));
        assertEquals("YEAR columns should be identified as 'YEAR'", "YEAR", rsMetaData.getColumnTypeName(1));
        assertEquals("YEAR columns should be mapped to java.sql.Date", java.sql.Date.class.getName(), rsMetaData.getColumnClassName(1));
        assertEquals("YEAR columns should be returned as java.sql.Date", java.sql.Date.class.getName(), this.rs.getObject(1).getClass().getName());

        testConnection.close();
    }

    /*
     * Tests DatabaseMetaData.getSQLKeywords().
     * (Related to BUG#70701 - DatabaseMetaData.getSQLKeywords() doesn't match MySQL 5.6 reserved words)
     * 
     * The keywords list that this method returns depends on JDBC version.
     * 
     * @throws Exception if the test fails.
     */
    public void testReservedWords() throws Exception {
        if (Util.isJdbc4()) {
            // there is a specific JCDB4 test for this
            return;
        }
        final String mysqlKeywords = "ACCESSIBLE,ANALYZE,ASENSITIVE,BEFORE,BIGINT,BINARY,BLOB,CALL,CHANGE,CONDITION,DATABASE,DATABASES,DAY_HOUR,"
                + "DAY_MICROSECOND,DAY_MINUTE,DAY_SECOND,DELAYED,DETERMINISTIC,DISTINCTROW,DIV,DUAL,EACH,ELSEIF,ENCLOSED,ESCAPED,EXIT,EXPLAIN,FLOAT4,FLOAT8,"
                + "FORCE,FULLTEXT,GENERATED,HIGH_PRIORITY,HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,IF,IGNORE,INDEX,INFILE,INOUT,INT1,INT2,INT3,INT4,INT8,"
                + "IO_AFTER_GTIDS,IO_BEFORE_GTIDS,ITERATE,KEYS,KILL,LEAVE,LIMIT,LINEAR,LINES,LOAD,LOCALTIME,LOCALTIMESTAMP,LOCK,LONG,LONGBLOB,LONGTEXT,LOOP,"
                + "LOW_PRIORITY,MASTER_BIND,MASTER_SSL_VERIFY_SERVER_CERT,MAXVALUE,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MIDDLEINT,MINUTE_MICROSECOND,MINUTE_SECOND,"
                + "MOD,MODIFIES,NO_WRITE_TO_BINLOG,OPTIMIZE,OPTIMIZER_COSTS,OPTIONALLY,OUT,OUTFILE,PARTITION,PURGE,RANGE,READS,READ_WRITE,REGEXP,RELEASE,"
                + "RENAME,REPEAT,REPLACE,REQUIRE,RESIGNAL,RETURN,RLIKE,SCHEMAS,SECOND_MICROSECOND,SENSITIVE,SEPARATOR,SHOW,SIGNAL,SPATIAL,SPECIFIC,"
                + "SQLEXCEPTION,SQLWARNING,SQL_BIG_RESULT,SQL_CALC_FOUND_ROWS,SQL_SMALL_RESULT,SSL,STARTING,STORED,STRAIGHT_JOIN,TERMINATED,TINYBLOB,TINYINT,"
                + "TINYTEXT,TRIGGER,UNDO,UNLOCK,UNSIGNED,USE,UTC_DATE,UTC_TIME,UTC_TIMESTAMP,VARBINARY,VARCHARACTER,VIRTUAL,WHILE,XOR,YEAR_MONTH,ZEROFILL";
        assertEquals("MySQL keywords don't match expected.", mysqlKeywords, this.conn.getMetaData().getSQLKeywords());
    }

    /**
     * Tests fix for BUG#20504139 - GETFUNCTIONCOLUMNS() AND GETPROCEDURECOLUMNS() RETURNS ERROR FOR VALID INPUTS.
     * 
     * Test duplicated in testsuite.regression.jdbc4.MetaDataRegressionTest.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20504139() throws Exception {
        if (Util.isJdbc4()) {
            // there is a specific JCDB4 test for this
            return;
        }

        createFunction("testBug20504139f", "(namef CHAR(20)) RETURNS CHAR(50) DETERMINISTIC RETURN CONCAT('Hello, ', namef, '!')");
        createFunction("`testBug20504139``f`", "(namef CHAR(20)) RETURNS CHAR(50) DETERMINISTIC RETURN CONCAT('Hello, ', namef, '!')");
        createProcedure("testBug20504139p", "(INOUT namep CHAR(50)) SELECT  CONCAT('Hello, ', namep, '!') INTO namep");
        createProcedure("`testBug20504139``p`", "(INOUT namep CHAR(50)) SELECT  CONCAT('Hello, ', namep, '!') INTO namep");

        for (int testCase = 0; testCase < 8; testCase++) { // 3 props, 8 combinations: 2^3 = 8
            boolean usePedantic = (testCase & 1) == 1;
            boolean useInformationSchema = (testCase & 2) == 2;
            boolean useFuncsInProcs = (testCase & 4) == 4;

            String connProps = String.format("pedantic=%s,useInformationSchema=%s,getProceduresReturnsFunctions=%s", usePedantic, useInformationSchema,
                    useFuncsInProcs);
            System.out.printf("testBug20504139_%d: %s%n", testCase, connProps);

            Connection testConn = getConnectionWithProps(connProps);
            DatabaseMetaData dbmd = testConn.getMetaData();

            ResultSet testRs = null;

            try {
                /*
                 * test DatabaseMetadata.getProcedureColumns for function
                 */
                int i = 1;
                try {
                    for (String name : new String[] { "testBug20504139f", "testBug20504139`f" }) {
                        testRs = dbmd.getProcedureColumns(null, "", name, "%");

                        assertTrue(testRs.next());
                        assertEquals(testCase + "." + i + ". expected function column name (empty)", "", testRs.getString(4));
                        assertEquals(testCase + "." + i + ". expected function column type (empty)", DatabaseMetaData.procedureColumnReturn, testRs.getInt(5));
                        assertTrue(testRs.next());
                        assertEquals(testCase + "." + i + ". expected function column name", "namef", testRs.getString(4));
                        assertEquals(testCase + "." + i + ". expected function column type (empty)", DatabaseMetaData.procedureColumnIn, testRs.getInt(5));
                        assertFalse(testRs.next());

                        testRs.close();
                        i++;
                    }
                } catch (SQLException e) {
                    if (e.getMessage().matches("FUNCTION `testBug20504139(:?`{2})?[fp]` does not exist")) {
                        fail(testCase + "." + i + ". failed to retrieve function columns from database meta data.");
                    }
                    throw e;
                }

                /*
                 * test DatabaseMetadata.getProcedureColumns for procedure
                 */
                i = 1;
                try {
                    for (String name : new String[] { "testBug20504139p", "testBug20504139`p" }) {
                        testRs = dbmd.getProcedureColumns(null, "", name, "%");

                        assertTrue(testRs.next());
                        assertEquals(testCase + "." + i + ". expected procedure column name", "namep", testRs.getString(4));
                        assertEquals(testCase + "." + i + ". expected procedure column type (empty)", DatabaseMetaData.procedureColumnInOut, testRs.getInt(5));
                        assertFalse(testRs.next());

                        testRs.close();
                        i++;
                    }
                } catch (SQLException e) {
                    if (e.getMessage().matches("PROCEDURE `testBug20504139(:?`{2})?[fp]` does not exist")) {
                        fail(testCase + "." + i + ". failed to retrieve procedure columns from database meta data.");
                    }
                    throw e;
                }
            } finally {
                testConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#21215151 - DATABASEMETADATA.GETCATALOGS() FAILS TO SORT RESULTS.
     * 
     * DatabaseMetaData.GetCatalogs() relies on the results of 'SHOW DATABASES' which deliver a sorted list of databases except for 'information_schema' which
     * is always returned in the first position.
     * This test creates set of databases around the relative position of 'information_schema' and checks the ordering of the final ResultSet.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug21215151() throws Exception {
        createDatabase("z_testBug21215151");
        createDatabase("j_testBug21215151");
        createDatabase("h_testBug21215151");
        createDatabase("i_testBug21215151");
        createDatabase("a_testBug21215151");

        DatabaseMetaData dbmd = this.conn.getMetaData();
        this.rs = dbmd.getCatalogs();

        System.out.println("Catalogs:");
        System.out.println("--------------------------------------------------");
        while (this.rs.next()) {
            System.out.println("\t" + this.rs.getString(1));
        }
        this.rs.beforeFirst();

        // check the relative position of each element in the result set compared to the previous element.
        String previousDb = "";
        while (this.rs.next()) {
            assertTrue("'" + this.rs.getString(1) + "' is lexicographically lower than the previous catalog. Check the system output to see the catalogs list.",
                    previousDb.compareTo(this.rs.getString(1)) < 0);
            previousDb = this.rs.getString(1);
        }
    }

    /**
     * Tests fix for BUG#19803348 - GETPROCEDURES() RETURNS INCORRECT O/P WHEN USEINFORMATIONSCHEMA=FALSE.
     * 
     * Composed by two parts:
     * 1. Confirm that getProcedures() and getProcedureColumns() aren't returning more results than expected (as per reported bug).
     * 2. Confirm that the results from getProcedures() and getProcedureColumns() are in the right order (secondary bug).
     * 
     * Test duplicated in testsuite.regression.jdbc4.MetaDataRegressionTest.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug19803348() throws Exception {
        Connection testConn = null;
        try {
            testConn = getConnectionWithProps("useInformationSchema=false,nullCatalogMeansCurrent=false");
            DatabaseMetaData dbmd = testConn.getMetaData();

            String testDb1 = "testBug19803348_db1";
            String testDb2 = "testBug19803348_db2";

            if (!dbmd.supportsMixedCaseIdentifiers()) {
                testDb1 = testDb1.toLowerCase();
                testDb2 = testDb2.toLowerCase();
            }

            createDatabase(testDb1);
            createDatabase(testDb2);

            // 1. Check if getProcedures() and getProcedureColumns() aren't returning more results than expected (as per reported bug).
            createFunction(testDb1 + ".testBug19803348_f", "(d INT) RETURNS INT DETERMINISTIC BEGIN RETURN d; END");
            createProcedure(testDb1 + ".testBug19803348_p", "(d int) BEGIN SELECT d; END");

            this.rs = dbmd.getProcedures(null, null, "testBug19803348_%");
            assertTrue(this.rs.next());
            assertEquals(testDb1, this.rs.getString(1));
            assertEquals("testBug19803348_f", this.rs.getString(3));
            assertTrue(this.rs.next());
            assertEquals(testDb1, this.rs.getString(1));
            assertEquals("testBug19803348_p", this.rs.getString(3));
            assertFalse(this.rs.next());

            this.rs = dbmd.getProcedureColumns(null, null, "testBug19803348_%", "%");
            assertTrue(this.rs.next());
            assertEquals(testDb1, this.rs.getString(1));
            assertEquals("testBug19803348_f", this.rs.getString(3));
            assertEquals("", this.rs.getString(4));
            assertTrue(this.rs.next());
            assertEquals(testDb1, this.rs.getString(1));
            assertEquals("testBug19803348_f", this.rs.getString(3));
            assertEquals("d", this.rs.getString(4));
            assertTrue(this.rs.next());
            assertEquals(testDb1, this.rs.getString(1));
            assertEquals("testBug19803348_p", this.rs.getString(3));
            assertEquals("d", this.rs.getString(4));
            assertFalse(this.rs.next());

            dropFunction(testDb1 + ".testBug19803348_f");
            dropProcedure(testDb1 + ".testBug19803348_p");

            // 2. Check if the results from getProcedures() and getProcedureColumns() are in the right order (secondary bug).
            createFunction(testDb1 + ".testBug19803348_B_f", "(d INT) RETURNS INT DETERMINISTIC BEGIN RETURN d; END");
            createProcedure(testDb1 + ".testBug19803348_B_p", "(d int) BEGIN SELECT d; END");
            createFunction(testDb2 + ".testBug19803348_A_f", "(d INT) RETURNS INT DETERMINISTIC BEGIN RETURN d; END");
            createProcedure(testDb2 + ".testBug19803348_A_p", "(d int) BEGIN SELECT d; END");

            this.rs = dbmd.getProcedures(null, null, "testBug19803348_%");
            assertTrue(this.rs.next());
            assertEquals(testDb1, this.rs.getString(1));
            assertEquals("testBug19803348_B_f", this.rs.getString(3));
            assertTrue(this.rs.next());
            assertEquals(testDb1, this.rs.getString(1));
            assertEquals("testBug19803348_B_p", this.rs.getString(3));
            assertTrue(this.rs.next());
            assertEquals(testDb2, this.rs.getString(1));
            assertEquals("testBug19803348_A_f", this.rs.getString(3));
            assertTrue(this.rs.next());
            assertEquals(testDb2, this.rs.getString(1));
            assertEquals("testBug19803348_A_p", this.rs.getString(3));
            assertFalse(this.rs.next());

            this.rs = dbmd.getProcedureColumns(null, null, "testBug19803348_%", "%");
            assertTrue(this.rs.next());
            assertEquals(testDb1, this.rs.getString(1));
            assertEquals("testBug19803348_B_f", this.rs.getString(3));
            assertEquals("", this.rs.getString(4));
            assertTrue(this.rs.next());
            assertEquals(testDb1, this.rs.getString(1));
            assertEquals("testBug19803348_B_f", this.rs.getString(3));
            assertEquals("d", this.rs.getString(4));
            assertTrue(this.rs.next());
            assertEquals(testDb1, this.rs.getString(1));
            assertEquals("testBug19803348_B_p", this.rs.getString(3));
            assertEquals("d", this.rs.getString(4));
            assertTrue(this.rs.next());
            assertEquals(testDb2, this.rs.getString(1));
            assertEquals("testBug19803348_A_f", this.rs.getString(3));
            assertEquals("", this.rs.getString(4));
            assertTrue(this.rs.next());
            assertEquals(testDb2, this.rs.getString(1));
            assertEquals("testBug19803348_A_f", this.rs.getString(3));
            assertEquals("d", this.rs.getString(4));
            assertTrue(this.rs.next());
            assertEquals(testDb2, this.rs.getString(1));
            assertEquals("testBug19803348_A_p", this.rs.getString(3));
            assertEquals("d", this.rs.getString(4));
            assertFalse(this.rs.next());

        } finally {
            if (testConn != null) {
                testConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#20727196 - GETPROCEDURECOLUMNS() RETURNS EXCEPTION FOR FUNCTION WHICH RETURNS ENUM/SET TYPE.
     * 
     * Test duplicated in testsuite.regression.jdbc4.MetaDataRegressionTest.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20727196() throws Exception {
        createFunction("testBug20727196_f1",
                "(p ENUM ('Yes', 'No')) RETURNS VARCHAR(10) DETERMINISTIC BEGIN RETURN IF(p='Yes', 'Yay!', if(p='No', 'Ney!', 'What?')); END");
        createFunction("testBug20727196_f2", "(p CHAR(1)) RETURNS ENUM ('Yes', 'No') DETERMINISTIC BEGIN RETURN IF(p='y', 'Yes', if(p='n', 'No', '?')); END");
        createFunction("testBug20727196_f3",
                "(p ENUM ('Yes', 'No')) RETURNS ENUM ('Yes', 'No') DETERMINISTIC BEGIN RETURN IF(p='Yes', 'Yes', if(p='No', 'No', '?')); END");
        createProcedure("testBug20727196_p1", "(p ENUM ('Yes', 'No')) BEGIN SELECT IF(p='Yes', 'Yay!', if(p='No', 'Ney!', 'What?')); END");

        for (String connProps : new String[] { "useInformationSchema=false", "useInformationSchema=true" }) {

            Connection testConn = null;
            try {
                testConn = getConnectionWithProps(connProps);
                DatabaseMetaData dbmd = testConn.getMetaData();

                this.rs = dbmd.getProcedureColumns(null, null, "testBug20727196_%", "%");

                // testBug20727196_f1 columns:
                assertTrue(this.rs.next());
                assertEquals("testBug20727196_f1", this.rs.getString(3));
                assertEquals("", this.rs.getString(4));
                assertEquals("VARCHAR", this.rs.getString(7));
                assertTrue(this.rs.next());
                assertEquals("testBug20727196_f1", this.rs.getString(3));
                assertEquals("p", this.rs.getString(4));
                assertEquals("ENUM", this.rs.getString(7));

                // testBug20727196_f2 columns:
                assertTrue(this.rs.next());
                assertEquals("testBug20727196_f2", this.rs.getString(3));
                assertEquals("", this.rs.getString(4));
                assertEquals("ENUM", this.rs.getString(7));
                assertTrue(this.rs.next());
                assertEquals("testBug20727196_f2", this.rs.getString(3));
                assertEquals("p", this.rs.getString(4));
                assertEquals("CHAR", this.rs.getString(7));

                // testBug20727196_f3 columns:
                assertTrue(this.rs.next());
                assertEquals("testBug20727196_f3", this.rs.getString(3));
                assertEquals("", this.rs.getString(4));
                assertEquals("ENUM", this.rs.getString(7));
                assertTrue(this.rs.next());
                assertEquals("testBug20727196_f3", this.rs.getString(3));
                assertEquals("p", this.rs.getString(4));
                assertEquals("ENUM", this.rs.getString(7));

                // testBug20727196_p1 columns:
                assertTrue(this.rs.next());
                assertEquals("testBug20727196_p1", this.rs.getString(3));
                assertEquals("p", this.rs.getString(4));
                assertEquals("ENUM", this.rs.getString(7));

                assertFalse(this.rs.next());
            } finally {
                if (testConn != null) {
                    testConn.close();
                }
            }
        }
    }

    /**
     * Tests fix for BUG#76187 (20675539), getTypeInfo report maximum precision of 255 for varchar.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug76187() throws Exception {

        DatabaseMetaData meta = this.conn.getMetaData();
        this.rs = meta.getTypeInfo();
        while (this.rs.next()) {
            if (this.rs.getString("TYPE_NAME").equals("VARCHAR")) {
                if (versionMeetsMinimum(5, 0, 3)) {
                    assertEquals(65535, this.rs.getInt("PRECISION"));
                } else {
                    assertEquals(255, this.rs.getInt("PRECISION"));
                }
            }
        }

    }

    /**
     * Tests fix for BUG#21978216, GETTYPEINFO REPORT MAXIMUM PRECISION OF 255 FOR VARBINARY
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug21978216() throws Exception {

        DatabaseMetaData meta = this.conn.getMetaData();
        this.rs = meta.getTypeInfo();
        while (this.rs.next()) {
            if (this.rs.getString("TYPE_NAME").equals("VARBINARY")) {
                if (versionMeetsMinimum(5, 0, 3)) {
                    assertEquals(65535, this.rs.getInt("PRECISION"));
                } else {
                    assertEquals(255, this.rs.getInt("PRECISION"));
                }
            }
        }

    }

    /**
     * Tests fix for Bug#23212347, ALL API CALLS ON RESULTSET METADATA RESULTS IN NPE WHEN USESERVERPREPSTMTS=TRUE.
     */
    public void testBug23212347() throws Exception {
        boolean useSPS = false;
        do {
            String testCase = String.format("Case [SPS: %s]", useSPS ? "Y" : "N");
            createTable("testBug23212347", "(id INT)");

            Properties props = new Properties();
            props.setProperty("useServerPrepStmts", Boolean.toString(useSPS));

            Connection testConn = getConnectionWithProps(props);
            Statement testStmt = testConn.createStatement();
            testStmt.execute("INSERT INTO testBug23212347 VALUES (1)");

            this.pstmt = testConn.prepareStatement("SELECT * FROM testBug23212347 WHERE id = 1");
            this.rs = this.pstmt.executeQuery();
            assertTrue(testCase, this.rs.next());
            assertEquals(testCase, 1, this.rs.getInt(1));
            assertFalse(testCase, this.rs.next());
            ResultSetMetaData rsmd = this.pstmt.getMetaData();
            assertEquals(testCase, "id", rsmd.getColumnName(1));

            this.pstmt = testConn.prepareStatement("SELECT * FROM testBug23212347 WHERE id = ?");
            this.pstmt.setInt(1, 1);
            this.rs = this.pstmt.executeQuery();
            assertTrue(testCase, this.rs.next());
            assertEquals(testCase, 1, this.rs.getInt(1));
            assertFalse(this.rs.next());
            rsmd = this.pstmt.getMetaData();
            assertEquals(testCase, "id", rsmd.getColumnName(1));
        } while (useSPS = !useSPS);
    }

    /**
     * Tests fix for Bug#73775 - DBMD.getProcedureColumns()/.getFunctionColumns() fail to filter by columnPattern
     * 
     * Test duplicated in testsuite.regression.jdbc4.MetaDataRegressionTest.
     */
    public void testBug73775() throws Exception {
        createFunction("testBug73775f", "(param1 CHAR(20), param2 CHAR(20)) RETURNS CHAR(40) DETERMINISTIC RETURN CONCAT(param1, param2)");
        createProcedure("testBug73775p", "(INOUT param1 CHAR(20), IN param2 CHAR(20)) BEGIN  SELECT CONCAT(param1, param2) INTO param1; END");

        boolean useIS = false;
        do {
            final String testCase = String.format("Case: [useIS: %s]", useIS ? "Y" : "N");

            final Properties props = new Properties();
            final Connection testConn = getConnectionWithProps(props);
            props.setProperty("useInformationSchema", Boolean.toString(useIS));
            final DatabaseMetaData dbmd = testConn.getMetaData();

            this.rs = dbmd.getProcedureColumns(null, "", "testBug73775%", "%");
            assertTrue(testCase, this.rs.next());
            assertEquals(testCase, "testBug73775f", this.rs.getString(3));
            assertEquals(testCase, "", this.rs.getString(4)); // Function return param is always returned.
            assertEquals(testCase, DatabaseMetaData.procedureColumnReturn, this.rs.getInt(5));
            assertTrue(testCase, this.rs.next());
            assertEquals(testCase, "testBug73775f", this.rs.getString(3));
            assertEquals(testCase, "param1", this.rs.getString(4));
            assertTrue(testCase, this.rs.next());
            assertEquals(testCase, "testBug73775f", this.rs.getString(3));
            assertEquals(testCase, "param2", this.rs.getString(4));
            assertTrue(testCase, this.rs.next());
            assertEquals(testCase, "testBug73775p", this.rs.getString(3));
            assertEquals(testCase, "param1", this.rs.getString(4));
            assertTrue(testCase, this.rs.next());
            assertEquals(testCase, "testBug73775p", this.rs.getString(3));
            assertEquals(testCase, "param2", this.rs.getString(4));
            assertFalse(testCase, this.rs.next());

            for (String ptn : new String[] { "param1", "_____1", "%1", "p_r_m%1" }) {
                this.rs = dbmd.getProcedureColumns(null, "", "testBug73775%", ptn);
                assertTrue(this.rs.next());
                assertEquals(testCase, "testBug73775f", this.rs.getString(3));
                assertEquals(testCase, "", this.rs.getString(4)); // Function return param is always returned.
                assertEquals(testCase, DatabaseMetaData.procedureColumnReturn, this.rs.getInt(5));
                assertTrue(testCase, this.rs.next());
                assertEquals(testCase, "testBug73775f", this.rs.getString(3));
                assertEquals(testCase, "param1", this.rs.getString(4));
                assertTrue(testCase, this.rs.next());
                assertEquals(testCase, "testBug73775p", this.rs.getString(3));
                assertEquals(testCase, "param1", this.rs.getString(4));
                assertFalse(testCase, this.rs.next());
            }

            for (String ptn : new String[] { "param2", "_____2", "%2", "p_r_m%2" }) {
                this.rs = dbmd.getProcedureColumns(null, "", "testBug73775%", "param2");
                assertTrue(this.rs.next());
                assertEquals(testCase, "testBug73775f", this.rs.getString(3));
                assertEquals(testCase, "", this.rs.getString(4)); // Function return param is always returned.
                assertEquals(testCase, DatabaseMetaData.procedureColumnReturn, this.rs.getInt(5));
                assertTrue(testCase, this.rs.next());
                assertEquals(testCase, "testBug73775f", this.rs.getString(3));
                assertEquals(testCase, "param2", this.rs.getString(4));
                assertTrue(testCase, this.rs.next());
                assertEquals(testCase, "testBug73775p", this.rs.getString(3));
                assertEquals(testCase, "param2", this.rs.getString(4));
                assertFalse(testCase, this.rs.next());
            }

            this.rs = dbmd.getProcedureColumns(null, "", "testBug73775%", "");
            assertTrue(testCase, this.rs.next());
            assertEquals(testCase, "testBug73775f", this.rs.getString(3));
            assertEquals(testCase, "", this.rs.getString(4)); // Function return param is always returned.
            assertEquals(testCase, DatabaseMetaData.procedureColumnReturn, this.rs.getInt(5));
            assertFalse(testCase, this.rs.next());

            testConn.close();
        } while (useIS = !useIS);
    }
}
