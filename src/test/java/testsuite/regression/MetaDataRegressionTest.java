/*
 * Copyright (c) 2002, 2022, Oracle and/or its affiliates.
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import com.mysql.cj.CharsetMappingWrapper;
import com.mysql.cj.Constants;
import com.mysql.cj.MysqlConnection;
import com.mysql.cj.MysqlType;
import com.mysql.cj.NativeCharsetSettings;
import com.mysql.cj.Query;
import com.mysql.cj.conf.PropertyDefinitions.DatabaseTerm;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.ClientPreparedStatement;
import com.mysql.cj.jdbc.Driver;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.NonRegisteringDriver;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.util.StringUtils;

import testsuite.BaseQueryInterceptor;
import testsuite.BaseTestCase;

/**
 * Regression tests for DatabaseMetaData
 */
public class MetaDataRegressionTest extends BaseTestCase {
    @Test
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
     * Tests fix for BUG#2852, where RSMD is not returning correct (or matching) types for TINYINT and SMALLINT.
     * 
     * @throws Exception
     */
    @Test
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
     * Tests fix for BUG#2855, where RSMD is not returning correct (or matching) types for FLOAT.
     * 
     * @throws Exception
     */
    @Test
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
     */
    @Test
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
                    assertTrue(typeFromGetColumns == typeFromRSMD,
                            columnName + " -> type from DBMD.getColumns(" + typeFromGetColumns + ") != type from RSMD.getColumnType(" + typeFromRSMD + ")");
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
     */
    @Test
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
     * Tests fix for BUG#1673, where DatabaseMetaData.getColumns() is not returning correct column ordinal info for non '%' column name patterns.
     * 
     * @throws Exception
     */
    @Test
    public void testFixForBug1673() throws Exception {

        createTable("testBug1673", "(field_1 INT, field_2 INT)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        Connection con = getConnectionWithProps(props);
        try {

            DatabaseMetaData dbmd = con.getMetaData();

            int ordinalPosOfCol2Full = 0;

            this.rs = dbmd.getColumns(con.getCatalog(), null, "testBug1673", null);

            while (this.rs.next()) {
                if (this.rs.getString(4).equals("field_2")) {
                    ordinalPosOfCol2Full = this.rs.getInt(17);
                }
            }

            int ordinalPosOfCol2Scoped = 0;

            this.rs = dbmd.getColumns(con.getCatalog(), null, "testBug1673", "field_2");

            while (this.rs.next()) {
                if (this.rs.getString(4).equals("field_2")) {
                    ordinalPosOfCol2Scoped = this.rs.getInt(17);
                }
            }

            assertTrue((ordinalPosOfCol2Full != 0) && (ordinalPosOfCol2Scoped != 0) && (ordinalPosOfCol2Scoped == ordinalPosOfCol2Full),
                    "Ordinal position in full column list of '" + ordinalPosOfCol2Full + "' != ordinal position in pattern search, '" + ordinalPosOfCol2Scoped
                            + "'.");

        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    /**
     * Tests bug reported by OpenOffice team with getColumns and LONGBLOB
     * 
     * @throws Exception
     */
    @Test
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
     * Tests fix for Bug#1099
     * 
     * @throws Exception
     */
    @Test
    public void testGetColumnsBug1099() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testGetColumnsBug1099");

            DatabaseMetaData dbmd = this.conn.getMetaData();

            this.rs = dbmd.getTypeInfo();

            StringBuilder types = new StringBuilder();

            HashMap<String, String> alreadyDoneTypes = new HashMap<>();

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
     * Tests whether or not unsigned columns are reported correctly in DBMD.getColumns
     * 
     * @throws Exception
     */
    @Test
    public void testGetColumnsUnsigned() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testGetUnsignedCols");
            this.stmt.executeUpdate("CREATE TABLE testGetUnsignedCols (field1 BIGINT, field2 BIGINT UNSIGNED)");

            DatabaseMetaData dbmd = this.conn.getMetaData();

            this.rs = dbmd.getColumns(this.conn.getCatalog(), null, "testGetUnsignedCols", "%");

            assertTrue(this.rs.next());
            // This row doesn't have 'unsigned' attribute
            assertTrue(this.rs.next());
            assertTrue(StringUtils.indexOfIgnoreCase(this.rs.getString(6), "unsigned") != -1);
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testGetUnsignedCols");
        }
    }

    /**
     * Tests whether bogus parameters break Driver.getPropertyInfo().
     * 
     * @throws Exception
     */
    @Test
    public void testGetPropertyInfo() throws Exception {
        new Driver().getPropertyInfo("", null);
    }

    /**
     * Tests whether ResultSetMetaData returns correct info for CHAR/VARCHAR columns.
     * 
     * @throws Exception
     */
    @Test
    public void testIsCaseSensitive() throws Exception {
        createSchemaObject(this.stmt, "DATABASE", "testIsCaseSensitive", "DEFAULT CHARACTER SET utf8mb4");
        createTable("testIsCaseSensitive.testIsCaseSensitive",
                "(bin_char CHAR(1) BINARY, bin_varchar VARCHAR(64) BINARY, ci_char CHAR(1), ci_varchar VARCHAR(64))");
        createTable("testIsCaseSensitive.testIsCaseSensitiveCs",
                "(bin_char CHAR(1) CHARACTER SET latin1 COLLATE latin1_general_cs, bin_varchar VARCHAR(64) CHARACTER SET latin1 COLLATE latin1_general_cs,"
                        + "ci_char CHAR(1) CHARACTER SET latin1 COLLATE latin1_general_ci,"
                        + "ci_varchar VARCHAR(64) CHARACTER SET latin1 COLLATE latin1_general_ci, "
                        + "bin_tinytext TINYTEXT CHARACTER SET latin1 COLLATE latin1_general_cs, bin_text TEXT CHARACTER SET latin1 COLLATE latin1_general_cs,"
                        + "bin_med_text MEDIUMTEXT CHARACTER SET latin1 COLLATE latin1_general_cs,"
                        + "bin_long_text LONGTEXT CHARACTER SET latin1 COLLATE latin1_general_cs,"
                        + "ci_tinytext TINYTEXT CHARACTER SET latin1 COLLATE latin1_general_ci, ci_text TEXT CHARACTER SET latin1 COLLATE latin1_general_ci,"
                        + "ci_med_text MEDIUMTEXT CHARACTER SET latin1 COLLATE latin1_general_ci,"
                        + "ci_long_text LONGTEXT CHARACTER SET latin1 COLLATE latin1_general_ci)");

        this.rs = this.stmt.executeQuery("SELECT bin_char, bin_varchar, ci_char, ci_varchar FROM testIsCaseSensitive.testIsCaseSensitive");
        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertTrue(rsmd.isCaseSensitive(1));
        assertTrue(rsmd.isCaseSensitive(2));
        assertTrue(!rsmd.isCaseSensitive(3));
        assertTrue(!rsmd.isCaseSensitive(4));

        this.rs = this.stmt.executeQuery("SELECT bin_char, bin_varchar, ci_char, ci_varchar, bin_tinytext, bin_text, bin_med_text, bin_long_text, "
                + "ci_tinytext, ci_text, ci_med_text, ci_long_text FROM testIsCaseSensitive.testIsCaseSensitiveCs");

        rsmd = this.rs.getMetaData();
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
    }

    /**
     * Tests whether or not DatabaseMetaData.getColumns() returns the correct java.sql.Types info.
     * 
     * @throws Exception
     */
    @Test
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
     */
    @Test
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
     */
    @Test
    public void testBug4742() throws Exception {
        HashMap<String, String> clashMap = new HashMap<>();

        this.rs = this.conn.getMetaData().getTypeInfo();

        while (this.rs.next()) {
            String name = this.rs.getString(1);
            assertTrue(!clashMap.containsKey(name), "Type represented twice in type info, '" + name + "'.");
            clashMap.put(name, name);
        }
    }

    /**
     * Tests fix for BUG#4138, getColumns() returns incorrect JDBC type for unsigned columns.
     * 
     * @throws Exception
     */
    @Test
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
                assertTrue(jdbcMapping[i] == this.rs.getShort("DATA_TYPE"), "JDBC Data Type of " + this.rs.getShort("DATA_TYPE") + " for MySQL type '"
                        + this.rs.getString("TYPE_NAME") + "' from 'DATA_TYPE' column does not match expected value of " + jdbcMapping[i] + ".");
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

                assertTrue(desiredTypeName.equalsIgnoreCase(rsmd.getColumnTypeName(i + 1)), rsmd.getColumnTypeName((i + 1)) + " != " + desiredTypeName);
            }
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4138");
        }
    }

    /**
     * Here for housekeeping only, the test is actually in testBug4138().
     * 
     * @throws Exception
     */
    @Test
    public void testBug4860() throws Exception {
        testBug4138();
    }

    /**
     * Tests fix for BUG#4880 - RSMD.getPrecision() returns '0' for non-numeric types.
     * 
     * Why-oh-why is this not in the spec, nor the api-docs, but in some 'optional' book, _and_ it is a variance from both ODBC and the ANSI SQL standard :p
     * 
     * (from the CTS testsuite)....
     * 
     * The getPrecision(int colindex) method returns an integer value representing the number of decimal digits for number types,maximum length in characters
     * for character types,maximum length in bytes for JDBC binary datatypes.
     * 
     * (See Section 27.3 of JDBC 2.0 API Reference & Tutorial 2nd edition)
     * 
     * @throws Exception
     */
    @Test
    public void testBug4880() throws Exception {
        createSchemaObject(this.stmt, "DATABASE", "testBug4880Db", "DEFAULT CHARACTER SET latin1");
        createTable("testBug4880Db.testBug4880", "(field1 VARCHAR(80), field2 TINYBLOB, field3 BLOB, field4 MEDIUMBLOB, field5 LONGBLOB)");

        this.rs = this.stmt.executeQuery("SELECT field1, field2, field3, field4, field5 FROM testBug4880Db.testBug4880");
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

        // Server doesn't send us enough information to detect LONGBLOB type
        assertEquals(Integer.MAX_VALUE, rsmd.getPrecision(5));
        assertEquals(Types.LONGVARBINARY, rsmd.getColumnType(5));
        assertTrue("LONGBLOB".equalsIgnoreCase(rsmd.getColumnTypeName(5)));
        assertEquals(Integer.MAX_VALUE, rsmd.getColumnDisplaySize(5));
    }

    /**
     * Tests fix for BUG#6399, ResultSetMetaData.getDisplaySize() is wrong for multi-byte charsets.
     * 
     * @throws Exception
     */
    @Test
    public void testBug6399() throws Exception {
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

    /**
     * Tests fix for BUG#7081, DatabaseMetaData.getIndexInfo() ignoring 'unique' parameters.
     * 
     * @throws Exception
     */
    @Test
    public void testBug7081() throws Exception {
        String tableName = "testBug7081";

        try {
            createTable(tableName, "(field1 INT, INDEX(field1))");

            DatabaseMetaData dbmd = this.conn.getMetaData();
            this.rs = dbmd.getIndexInfo(this.conn.getCatalog(), null, tableName, true, false);
            assertTrue(!this.rs.next()); // there should be no rows that meet this requirement

            this.rs = dbmd.getIndexInfo(this.conn.getCatalog(), null, tableName, false, false);
            assertTrue(this.rs.next()); // there should be one row that meets this requirement
            assertTrue(!this.rs.next());

        } finally {
            dropTable(tableName);
        }
    }

    /**
     * Tests fix for BUG#7033 - PreparedStatements don't encode Big5 (and other multibyte) character sets correctly in static SQL strings.
     * 
     * @throws Exception
     */
    @Test
    public void testBug7033() throws Exception {
        Connection big5Conn = null;
        Statement big5Stmt = null;
        PreparedStatement big5PrepStmt = null;

        String testString = "\u5957 \u9910";

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "Big5");

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

    /**
     * Tests fix for Bug#8812, DBMD.getIndexInfo() returning inverted values for 'NON_UNIQUE' column.
     * 
     * @throws Exception
     */
    @Test
    public void testBug8812() throws Exception {
        String tableName = "testBug8812";

        try {
            createTable(tableName, "(field1 INT, field2 INT, INDEX(field1), UNIQUE INDEX(field2))");

            DatabaseMetaData dbmd = this.conn.getMetaData();
            this.rs = dbmd.getIndexInfo(this.conn.getCatalog(), null, tableName, true, false);
            assertTrue(this.rs.next()); // there should be one row that meets this requirement
            assertEquals(this.rs.getBoolean("NON_UNIQUE"), false);

            this.rs = dbmd.getIndexInfo(this.conn.getCatalog(), null, tableName, false, false);
            assertTrue(this.rs.next()); // there should be two rows that meets this requirement
            assertEquals(this.rs.getBoolean("NON_UNIQUE"), false);
            assertTrue(this.rs.next());
            assertEquals(this.rs.getBoolean("NON_UNIQUE"), true);

        } finally {
            dropTable(tableName);
        }
    }

    /**
     * Tests fix for BUG#8800 - supportsMixedCase*Identifiers() returns wrong value on servers running on case-sensitive filesystems.
     * 
     * @throws Exception
     */
    @Test
    public void testBug8800() throws Exception {
        assertEquals(((com.mysql.cj.jdbc.JdbcConnection) this.conn).lowerCaseTableNames(), !this.conn.getMetaData().supportsMixedCaseIdentifiers());
        assertEquals(((com.mysql.cj.jdbc.JdbcConnection) this.conn).lowerCaseTableNames(), !this.conn.getMetaData().supportsMixedCaseQuotedIdentifiers());

    }

    /**
     * Tests fix for BUG#8792 - DBMD.supportsResultSetConcurrency() not returning true for forward-only/read-only result sets (we obviously support this).
     * 
     * @throws Exception
     */
    @Test
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
            assertTrue(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
        }

        try {
            assertTrue(dbmd.supportsResultSetConcurrency(ResultSet.TYPE_SCROLL_INSENSITIVE, Integer.MIN_VALUE));
            fail("Exception should've been raised for bogus concurrency value");
        } catch (SQLException sqlEx) {
            assertTrue(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
        }

        try {
            assertTrue(dbmd.supportsResultSetConcurrency(Integer.MIN_VALUE, Integer.MIN_VALUE));
            fail("Exception should've been raised for bogus concurrency value");
        } catch (SQLException sqlEx) {
            assertTrue(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx.getSQLState()));
        }
    }

    /**
     * Tests fix for BUG#8803, 'DATA_TYPE' column from DBMD.getBestRowIdentifier() causes ArrayIndexOutOfBoundsException when accessed (and in fact, didn't
     * return any value).
     * 
     * @throws Exception
     */
    @Test
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
     * Tests fix for BUG#9320 - PreparedStatement.getMetaData() inserts blank row in database under certain conditions when not using server-side prepared
     * statements.
     * 
     * @throws Exception
     */
    @Test
    public void testBug9320() throws Exception {
        createTable("testBug9320", "(field1 int)");

        testAbsenceOfMetadataForQuery("INSERT INTO testBug9320 VALUES (?)");
        testAbsenceOfMetadataForQuery("UPDATE testBug9320 SET field1=?");
        testAbsenceOfMetadataForQuery("DELETE FROM testBug9320 WHERE field1=?");
    }

    /**
     * Tests fix for BUG#9778, DBMD.getTables() shouldn't return tables if views are asked for, even if the database version doesn't support views.
     * 
     * @throws Exception
     */
    @Test
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
     * Tests fix for BUG#9769 - Should accept null for procedureNamePattern, even though it isn't JDBC compliant, for legacy's sake.
     * 
     * @throws Exception
     */
    @Test
    public void testBug9769() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        Connection con = getConnectionWithProps(props);
        try {

            con.getMetaData().getProcedures(con.getCatalog(), "%", null);

            // TODO: Check results

        } finally {
            if (con != null) {
                con.close();
            }
        }

        // FIXME: Other methods to test
        // getColumns();
        // getTablePrivileges();
        // getTables();

    }

    /**
     * Tests fix for BUG#9917 - Should accept null for catalog in DBMD methods, even though it's not JDBC-compliant for legacy's sake.
     * 
     * @throws Exception
     */
    @Test
    public void testBug9917() throws Exception {
        String dbname = "testBug9917db";
        String tableName = "testBug9917table";
        try {
            // test defaults
            this.stmt.executeUpdate("DROP DATABASE IF EXISTS " + dbname);
            this.stmt.executeUpdate("CREATE DATABASE " + dbname);

            boolean defaultDbConfig = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).getPropertySet().getBooleanProperty(PropertyKey.nullDatabaseMeansCurrent)
                    .getValue();
            assertEquals(false, defaultDbConfig);

            // we use the table name which also exists in `mysql' database
            createTable(dbname + "." + tableName, "(field1 int)");
            createTable(tableName, "(field1 int)");
            String currentDb = ((JdbcConnection) this.conn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                    .getValue() == DatabaseTerm.SCHEMA ? this.conn.getSchema() : this.conn.getCatalog();

            // default 'false' means 'any database'
            // we should get at least two rows here
            this.rs = this.conn.getMetaData().getTables(null, null, tableName, null);
            int totalCnt = 0;
            int expectedCnt = 0;
            while (this.rs.next()) {
                String currDb = ((JdbcConnection) this.conn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                        .getValue() == DatabaseTerm.SCHEMA ? this.rs.getString("TABLE_SCHEM") : this.rs.getString("TABLE_CAT");
                if (currentDb.equalsIgnoreCase(currDb) || dbname.equalsIgnoreCase(currDb)) {
                    expectedCnt++;
                }
                totalCnt++;
            }
            assertEquals(2, expectedCnt);
            assertTrue(totalCnt >= 2);
            this.rs.close();

            // 'true' means only current database to be checked
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "true");
            Connection con = getConnectionWithProps(props);
            try {
                this.rs = con.getMetaData().getTables(null, null, tableName, null);
                while (this.rs.next()) {
                    String currDb = ((JdbcConnection) this.conn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                            .getValue() == DatabaseTerm.SCHEMA ? this.rs.getString("TABLE_SCHEM") : this.rs.getString("TABLE_CAT");
                    assertEquals(currentDb, currDb);
                }

            } finally {
                if (con != null) {
                    con.close();
                }
            }

        } finally {
            this.stmt.executeUpdate("DROP DATABASE IF EXISTS " + dbname);
        }

        // FIXME: Other methods to test
        //
        // getBestRowIdentifier()
        // getColumns()
        // getCrossReference()
        // getExportedKeys()
        // getImportedKeys()
        // getIndexInfo()
        // getPrimaryKeys()
        // getProcedures()
    }

    /**
     * Tests fix for BUG#11575 -- DBMD.storesLower/Mixed/UpperIdentifiers() reports incorrect values for servers deployed on Windows.
     * 
     * @throws Exception
     */
    @Test
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
     * Tests fix for BUG#11781, foreign key information that is quoted is parsed incorrectly.
     * 
     * @throws Exception
     */
    @Test
    public void testBug11781() throws Exception {
        createTable("`app tab`", "( C1 int(11) NULL, C2 int(11) NULL, INDEX NEWINX (C1), INDEX NEWINX2 (C1, C2))", "InnoDB");

        this.stmt.executeUpdate("ALTER TABLE `app tab` ADD CONSTRAINT APPFK FOREIGN KEY (C1) REFERENCES `app tab` (C1)");

        /*
         * this.rs = this.conn.getMetaData().getCrossReference(
         * this.conn.getCatalog(), null, "app tab", this.conn.getCatalog(),
         * null, "app tab");
         */

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        for (boolean useIS : new boolean[] { false, true }) {
            for (String databaseTerm : new String[] { "CATALOG", "SCHEMA" }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), databaseTerm);

                System.out.println("useInformationSchema=" + useIS + ", databaseTerm=" + databaseTerm);

                Connection con = getConnectionWithProps(props);

                String db = databaseTerm.contentEquals("SCHEMA") ? con.getSchema() : con.getCatalog();
                this.rs = ((com.mysql.cj.jdbc.DatabaseMetaData) con.getMetaData()).extractForeignKeyFromCreateTable(db, "app tab");
                assertTrue(this.rs.next(), "must return a row");

                assertEquals(("comment; APPFK(`C1`) REFER `" + db + "`/ `app tab` (`C1`)").toUpperCase(), this.rs.getString(3).toUpperCase());

                this.rs.close();

                this.rs = databaseTerm.contentEquals("SCHEMA") ? con.getMetaData().getImportedKeys(null, con.getSchema(), "app tab")
                        : con.getMetaData().getImportedKeys(con.getCatalog(), null, "app tab");

                assertTrue(this.rs.next());

                this.rs = databaseTerm.contentEquals("SCHEMA") ? con.getMetaData().getExportedKeys(null, con.getSchema(), "app tab")
                        : con.getMetaData().getExportedKeys(con.getCatalog(), null, "app tab");

                assertTrue(this.rs.next());

            }
        }

    }

    /**
     * Tests fix for BUG#12970 - java.sql.Types.OTHER returned for binary and varbinary columns.
     * 
     * @throws Exception
     */
    @Test
    public void testBug12970() throws Exception {
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

    /**
     * Tests fix for BUG#12975 - OpenOffice expects DBMD.supportsIEF() to return "true" if foreign keys are supported by the datasource, even though this method
     * also covers support for check constraints, which MySQL _doesn't_ have.
     * 
     * @throws Exception
     */
    @Test
    public void testBug12975() throws Exception {
        assertEquals(false, this.conn.getMetaData().supportsIntegrityEnhancementFacility());

        Connection overrideConn = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.overrideSupportsIntegrityEnhancementFacility.getKeyName(), "true");

            overrideConn = getConnectionWithProps(props);
            assertEquals(true, overrideConn.getMetaData().supportsIntegrityEnhancementFacility());
        } finally {
            if (overrideConn != null) {
                overrideConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#13277 - RSMD for generated keys has NPEs when a connection is referenced.
     * 
     * @throws Exception
     */
    @Test
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
     * Tests BUG13601 (which doesn't seem to be present in 3.1.11, but we'll leave it in here for regression's-sake).
     * 
     * @throws Exception
     */
    @Test
    public void testBug13601() throws Exception {

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

    /**
     * Tests fix for BUG#14815 - DBMD.getColumns() doesn't return TABLE_NAME correctly.
     * 
     * @throws Exception
     */
    @Test
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
     */
    @Test
    public void testBug15854() throws Exception {
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

    /**
     * Tests fix for BUG#16277 - Invalid classname returned for RSMD.getColumnClassName() for BIGINT type.
     * 
     * @throws Exception
     */
    @Test
    public void testBug16277() throws Exception {
        createTable("testBug16277", "(field1 BIGINT, field2 BIGINT UNSIGNED)");
        ResultSetMetaData rsmd = this.stmt.executeQuery("SELECT field1, field2 FROM testBug16277").getMetaData();
        assertEquals("java.lang.Long", rsmd.getColumnClassName(1));
        assertEquals("java.math.BigInteger", rsmd.getColumnClassName(2));
    }

    /**
     * Tests fix for BUG#18554 - Aliased column names where length of name > 251 are corrupted.
     * 
     * @throws Exception
     */
    @Test
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
        int i = ((com.mysql.cj.jdbc.ConnectionImpl) this.conn).getSession().getServerSession().getCharsetSettings()
                .getMaxBytesPerChar(CharsetMappingWrapper.getStaticJavaEncodingForMysqlCharset(
                        ((NativeCharsetSettings) ((com.mysql.cj.jdbc.JdbcConnection) this.conn).getSession().getServerSession().getCharsetSettings())
                                .getServerDefaultCharset()));
        if (i == 1) {
            // This is INT field but still processed in
            // ResultsetMetaData.getColumnDisplaySize
            assertEquals(20, rsmd.getColumnDisplaySize(1));
        }

        assertEquals(false, rsmd.isDefinitelyWritable(1));
        assertEquals(true, rsmd.isReadOnly(1));
        assertEquals(false, rsmd.isWritable(1));
    }

    @Test
    public void testSupportsCorrelatedSubqueries() throws Exception {
        DatabaseMetaData dbmd = this.conn.getMetaData();

        assertEquals(true, dbmd.supportsCorrelatedSubqueries());
    }

    @Test
    public void testSupportesGroupByUnrelated() throws Exception {
        DatabaseMetaData dbmd = this.conn.getMetaData();

        assertEquals(true, dbmd.supportsGroupByUnrelated());
    }

    /**
     * Tests fix for BUG#21267, ParameterMetaData throws NullPointerException when prepared SQL actually has a syntax error
     * 
     * @throws Exception
     */
    @Test
    public void testBug21267() throws Exception {
        createTable("bug21267", "(`Col1` int(11) NOT NULL,`Col2` varchar(45) default NULL,`Col3` varchar(45) default NULL,PRIMARY KEY  (`Col1`))");

        this.pstmt = this.conn.prepareStatement("SELECT Col1, Col2,Col4 FROM bug21267 WHERE Col1=?");
        this.pstmt.setInt(1, 1);

        java.sql.ParameterMetaData psMeta = this.pstmt.getParameterMetaData();

        try {
            assertEquals(0, psMeta.getParameterType(1));
        } catch (SQLException sqlEx) {
            assertEquals(MysqlErrorNumbers.SQL_STATE_DRIVER_NOT_CAPABLE, sqlEx.getSQLState());
        }

        this.pstmt.close();

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.generateSimpleParameterMetadata.getKeyName(), "true");

        this.pstmt = getConnectionWithProps(props).prepareStatement("SELECT Col1, Col2,Col4 FROM bug21267 WHERE Col1=?");

        psMeta = this.pstmt.getParameterMetaData();

        assertEquals(Types.VARCHAR, psMeta.getParameterType(1));
    }

    /**
     * Tests fix for BUG#21544 - When using information_schema for metadata, COLUMN_SIZE for getColumns() is not clamped to range
     * of java.lang.Integer as is the case when not using information_schema, thus leading to a truncation exception that
     * isn't present when not using information_schema.
     * 
     * @throws Exception
     */
    @Test
    public void testBug21544() throws Exception {
        createTable("testBug21544", "(foo_id INT NOT NULL, stuff LONGTEXT, PRIMARY KEY (foo_id))", "INNODB");

        Connection infoSchemConn = null;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        props.setProperty(PropertyKey.jdbcCompliantTruncation.getKeyName(), "false");
        props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "true");

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
     * Tests fix for BUG#22613 - DBMD.getColumns() does not return expected COLUMN_SIZE for the SET type (fixed to be consistent with the ODBC driver)
     * 
     * @throws Exception
     */
    @Test
    public void testBug22613() throws Exception {
        createTable("bug22613",
                "( s set('a','bc','def','ghij') default NULL, t enum('a', 'ab', 'cdef'), s2 SET('1','2','3','4','1585','ONE','TWO','Y','N','THREE'))");

        checkMetadataForBug22613(this.conn);

        Connection infoSchemConn = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");

            infoSchemConn = getConnectionWithProps(props);

            checkMetadataForBug22613(infoSchemConn);
        } finally {
            if (infoSchemConn != null) {
                infoSchemConn.close();
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
     * Fix for BUG#22628 - Driver.getPropertyInfo() throws NullPointerException for URL that only specifies host and/or port.
     * 
     * @throws Exception
     */
    @Test
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

            this.pstmt = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement(query);
            rsmd = this.pstmt.getMetaData();

            assertNull(rsmd);
        } finally {
            if (this.pstmt != null) {
                this.pstmt.close();
            }
        }
    }

    @Test
    public void testRSMDToStringFromDBMD() throws Exception {
        this.rs = this.conn.getMetaData().getTypeInfo();

        this.rs.getMetaData().toString(); // used to cause NPE
    }

    @Test
    public void testCharacterSetForDBMD() throws Exception {
        System.out.println("testCharacterSetForDBMD:");
        String quoteChar = this.conn.getMetaData().getIdentifierQuoteString();

        String tableName = quoteChar + "\u00e9\u0074\u00e9" + quoteChar;
        createTable(tableName, "(field1 int)");
        this.rs = this.conn.getMetaData().getTables(this.conn.getCatalog(), null, "%", new String[] { "TABLE" });
        while (this.rs.next()) {
            System.out.println(this.rs.getString("TABLE_NAME") + " -> " + new String(this.rs.getBytes("TABLE_NAME"), "UTF-8"));
        }
        this.rs = this.conn.getMetaData().getTables(this.conn.getCatalog(), null, tableName, new String[] { "TABLE" });
        assertEquals(true, this.rs.next());
        System.out.println(this.rs.getString("TABLE_NAME"));
        System.out.println(new String(this.rs.getBytes("TABLE_NAME"), "UTF-8"));
    }

    /**
     * Tests fix for BUG#18258 - Nonexistent catalog/database causes SQLException to be raised, rather than returning empty result set.
     * 
     * @throws Exception
     */
    @Test
    public void testBug18258() throws Exception {
        String bogusDatabaseName = "abcdefghijklmnopqrstuvwxyz";
        this.conn.getMetaData().getTables(bogusDatabaseName, "%", "%", new String[] { "TABLE", "VIEW" });
        this.conn.getMetaData().getColumns(bogusDatabaseName, "%", "%", "%");
        this.conn.getMetaData().getProcedures(bogusDatabaseName, "%", "%");
    }

    /**
     * Tests fix for BUG#23303 - DBMD.getSchemas() doesn't return a TABLE_CATALOG column.
     * 
     * @throws Exception
     */
    @Test
    public void testBug23303() throws Exception {
        this.rs = this.conn.getMetaData().getSchemas();
        this.rs.findColumn("TABLE_CATALOG");
    }

    /**
     * Tests fix for BUG#23304 - DBMD using "show" and DBMD using information_schema do not return results consistent with each other.
     * 
     * (note this fix only addresses the inconsistencies, not the issue that the driver is treating schemas differently than some users expect.
     * 
     * We will revisit this behavior when there is full support for schemas in MySQL).
     * 
     * @throws Exception
     */
    @Test
    public void testBug23304() throws Exception {
        Connection connShow = null;
        Connection connInfoSchema = null;

        ResultSet rsShow = null;
        ResultSet rsInfoSchema = null;

        try {
            Properties noInfoSchemaProps = new Properties();
            noInfoSchemaProps.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            noInfoSchemaProps.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            noInfoSchemaProps.setProperty(PropertyKey.useInformationSchema.getKeyName(), "false");

            Properties infoSchemaProps = new Properties();
            infoSchemaProps.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            infoSchemaProps.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            infoSchemaProps.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
            infoSchemaProps.setProperty(PropertyKey.dumpQueriesOnException.getKeyName(), "true");

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

            StringBuilder sb = new StringBuilder("(");
            sb.append("field01 int primary key not null, ");
            sb.append("field02 int unsigned, ");
            sb.append("field03 tinyint, ");
            sb.append("field04 tinyint unsigned, ");
            sb.append("field05 smallint, ");
            sb.append("field06 smallint unsigned, ");
            sb.append("field07 mediumint, ");
            sb.append("field08 mediumint unsigned, ");
            sb.append("field09 bigint, ");
            sb.append("field10 bigint unsigned, ");
            sb.append("field11 float, ");
            sb.append("field12 float unsigned, ");
            sb.append("field13 double, ");
            sb.append("field14 double unsigned, ");
            sb.append("field15 decimal, ");
            sb.append("field16 decimal unsigned, ");
            sb.append("field17 char(32), ");
            sb.append("field18 varchar(32), ");
            sb.append("field19 binary(32), ");
            sb.append("field20 varbinary(16384), ");
            sb.append("field21 tinyblob, ");
            sb.append("field22 blob, ");
            sb.append("field23 mediumblob, ");
            sb.append("field24 longblob, ");
            sb.append("field25 tinytext, ");
            sb.append("field26 text, ");
            sb.append("field27 mediumtext, ");
            sb.append("field28 longtext, ");
            sb.append("field29 date, ");
            sb.append("field30 time, ");
            sb.append("field31 datetime, ");
            sb.append("field32 timestamp, ");
            sb.append("field33 year, ");
            if (versionMeetsMinimum(5, 7)) {
                sb.append("field34 json, ");
            }
            sb.append("field35 boolean, ");
            sb.append("field36 bit, ");
            sb.append("field37 bit(64), ");
            sb.append("field38 enum('a','b', 'c' ), ");
            sb.append("field39 set('d', 'e', 'f' ), ");

            sb.append("field40 geometry, ");
            sb.append("field41 POINT, ");
            sb.append("field42 LINESTRING, ");
            sb.append("field43 POLYGON, ");
            sb.append("field44 MULTIPOINT, ");
            sb.append("field45 MULTILINESTRING, ");
            sb.append("field46 MULTIPOLYGON, ");
            sb.append("field47 GEOMETRYCOLLECTION ");
            if (versionMeetsMinimum(8, 0, 5)) {
                sb.append(", field48 GEOMCOLLECTION ");
            }

            sb.append(")");
            createTable("t_testBug23304", sb.toString());

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
            assertTrue(actual == null, "Expected null result set, actual was not null.");
            return;
        }
        assertFalse(actual == null, "Expected non-null actual result set.");

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
                            || ((expected.getString(i + 1) == null || expected.getString(i + 1).length() == 0) && actual.getObject(i + 1) == null)) {
                        continue; // known bug with SHOW FULL COLUMNS, and we
                                 // can't distinguish between null and ''
                                 // for a default
                    }

                    if ("CHAR_OCTET_LENGTH".equals(metadataExpected.getColumnName(i + 1))) {
                        if (((com.mysql.cj.jdbc.ConnectionImpl) this.conn).getSession().getServerSession().getCharsetSettings()
                                .getMaxBytesPerChar(CharsetMappingWrapper
                                        .getStaticJavaEncodingForMysqlCharset(((NativeCharsetSettings) ((com.mysql.cj.jdbc.JdbcConnection) this.conn)
                                                .getSession().getServerSession().getCharsetSettings()).getServerDefaultCharset())) > 1) {
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
     * Tests fix for BUG#25624 - Whitespace surrounding storage/size specifiers in stored procedure declaration causes NumberFormatException to be thrown when
     * calling stored procedure.
     * 
     * @throws Exception
     */
    @Test
    public void testBug25624() throws Exception {
        //
        // we changed up the parameters to get coverage of the fixes,
        // also note that whitespace _is_ significant in the DDL...
        //
        createProcedure("testBug25624", "(in _par1 decimal( 10 , 2 ) , in _par2 varchar( 4 )) BEGIN select 1; END");

        this.conn.prepareCall("{call testBug25624(?,?)}").close();
    }

    /**
     * Tests fix for BUG#27867 - Schema objects with identifiers other than the connection character aren't retrieved correctly in ResultSetMetadata.
     * 
     * @throws Exception
     */
    @Test
    public void testBug27867() throws Exception {
        String gbkColumnName = "\u00e4\u00b8\u00ad\u00e6\u2013\u2021\u00e6\u00b5\u2039\u00e8\u00af\u2022";
        createTable("ColumnNameEncoding",
                "(`" + gbkColumnName + "` varchar(1) default NULL, `ASCIIColumn` varchar(1) default NULL" + ")ENGINE=MyISAM DEFAULT CHARSET=utf8");

        this.rs = this.stmt.executeQuery("SELECT * FROM ColumnNameEncoding");
        java.sql.ResultSetMetaData tblMD = this.rs.getMetaData();

        assertEquals(gbkColumnName, tblMD.getColumnName(1));
        assertEquals("ASCIIColumn", tblMD.getColumnName(2));
    }

    /**
     * Fixed BUG#27915 - DatabaseMetaData.getColumns() doesn't contain SCOPE_* or IS_AUTOINCREMENT columns.
     * 
     * @throws Exception
     */
    @Test
    public void testBug27915() throws Exception {
        createTable("testBug27915", "(field1 int not null primary key auto_increment, field2 int)");
        DatabaseMetaData dbmd = this.conn.getMetaData();

        this.rs = dbmd.getColumns(this.conn.getCatalog(), null, "testBug27915", "%");
        this.rs.next();

        checkBug27915();

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        this.rs = getConnectionWithProps(props).getMetaData().getColumns(this.conn.getCatalog(), null, "testBug27915", "%");
        this.rs.next();

        checkBug27915();
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
     * Tests fix for BUG#27916 - UNSIGNED types not reported via DBMD.getTypeInfo(), and capitalization of types is not consistent between DBMD.getColumns(),
     * RSMD.getColumnTypeName() and DBMD.getTypeInfo().
     * 
     * This fix also ensures that the precision of UNSIGNED MEDIUMINT and UNSIGNED BIGINT is reported correctly via DBMD.getColumns().
     * 
     * Second fix ensures that list values of ENUM and SET types containing 'unsigned' are not taken in account.
     * 
     * @throws Exception
     */
    @Test
    public void testBug27916() throws Exception {
        createTable("testBug27916",
                "(field1 TINYINT UNSIGNED, field2 SMALLINT UNSIGNED, field3 INT UNSIGNED, field4 INTEGER UNSIGNED, field5 MEDIUMINT UNSIGNED, field6 BIGINT UNSIGNED)");

        ResultSetMetaData rsmd = this.stmt.executeQuery("SELECT * FROM testBug27916").getMetaData();

        HashMap<String, Object> typeNameToPrecision = new HashMap<>();
        this.rs = this.conn.getMetaData().getTypeInfo();

        while (this.rs.next()) {
            typeNameToPrecision.put(this.rs.getString("TYPE_NAME"), this.rs.getObject("PRECISION"));
        }

        this.rs = this.conn.getMetaData().getColumns(this.conn.getCatalog(), null, "testBug27916", "%");

        for (int i = 0; i < rsmd.getColumnCount(); i++) {
            this.rs.next();
            String typeName = this.rs.getString("TYPE_NAME");

            assertEquals(typeName, rsmd.getColumnTypeName(i + 1));
            assertEquals(this.rs.getInt("COLUMN_SIZE"), rsmd.getPrecision(i + 1), typeName);
            assertEquals(new Integer(rsmd.getPrecision(i + 1)), typeNameToPrecision.get(typeName), typeName);
        }

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "false");
        ArrayList<String> types = new ArrayList<>();
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
                assertTrue(types.contains(typeName), typeName);
            }
            this.rs.close();
            this.rs = dbmd.getColumns("mysql", null, "proc", "%");
            while (this.rs.next()) {
                String typeName = this.rs.getString("TYPE_NAME");
                assertTrue(types.contains(typeName), typeName);
            }
            this.rs.close();
            PropConn.close();
            props.clear();

            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
            PropConn = getConnectionWithProps(props);
            dbmd = PropConn.getMetaData();

            this.rs = dbmd.getColumns("mysql", null, "time_zone_transition", "%");
            while (this.rs.next()) {
                String typeName = this.rs.getString("TYPE_NAME");
                assertTrue(types.contains(typeName), typeName);
            }
            this.rs.close();
            this.rs = dbmd.getColumns("mysql", null, "proc", "%");
            while (this.rs.next()) {
                String typeName = this.rs.getString("TYPE_NAME");
                assertTrue(types.contains(typeName), typeName);
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

    @Test
    public void testBug20491() throws Exception {
        this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE '%char%'");
        while (this.rs.next()) {
            System.out.println(this.rs.getString(1) + " = " + this.rs.getString(2));
        }
        this.rs.close();

        String[] fields = { "field1_ae_\u00e4", "field2_ue_\u00fc", "field3_oe_\u00f6", "field4_sz_\u00df" };

        createTable("tst", "(`" + fields[0] + "` int(10) unsigned NOT NULL default '0', `" + fields[1] + "` varchar(45) default '', `" + fields[2]
                + "` varchar(45) default '', `" + fields[3] + "` varchar(45) default '', PRIMARY KEY  (`" + fields[0] + "`))");

        // demonstrate that these are all in the Cp1252 encoding

        for (int i = 0; i < fields.length; i++) {
            try {
                assertEquals(fields[i], new String(fields[i].getBytes("Cp1252"), "Cp1252"));
            } catch (AssertionFailedError afEr) {
                if (i == 3) {
                    // If we're on a mac, we're out of luck swe can't store this in the filesystem...

                    if (!Constants.OS_NAME.startsWith("Mac")) {
                        throw afEr;
                    }
                }
            }
        }

        @SuppressWarnings("unused")
        byte[] asBytes = fields[0].getBytes("utf-8");

        DatabaseMetaData md = this.conn.getMetaData();

        this.rs = md.getColumns(this.dbName, "%", "tst", "%");

        int j = 0;

        while (this.rs.next()) {
            try {
                assertEquals(fields[j++], this.rs.getString(4), "Wrong column name:" + this.rs.getString(4));
            } catch (AssertionFailedError afEr) {
                if (j == 3) {
                    // If we're on a mac, we're out of luck we can't store this in the filesystem...

                    if (!Constants.OS_NAME.startsWith("Mac")) {
                        throw afEr;
                    }
                }
            }
        }

        this.rs.close();

        this.rs = this.stmt.executeQuery("SELECT * FROM tst");

        ResultSetMetaData rsmd = this.rs.getMetaData();

        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
            try {
                assertEquals(fields[i - 1], rsmd.getColumnName(i), "Wrong column name:" + rsmd.getColumnName(i));
            } catch (AssertionFailedError afEr) {
                if (i - 1 == 3) {
                    // If we're on a mac, we're out of luck we can't store this in the filesystem...

                    if (!Constants.OS_NAME.startsWith("Mac")) {
                        throw afEr;
                    }
                }
            }
        }
    }

    /**
     * Tests fix for Bug#33594 - When cursor fetch is enabled, wrong metadata is returned from DBMD.
     * 
     * The fix is two parts.
     * 
     * First, when asking for the first column value twice from a cursor-fetched row, the driver didn't re-position, and thus the "next" column was returned.
     * 
     * Second, metadata statements and internal statements the driver uses shouldn't use cursor-based fetching at all, so we've ensured that internal statements
     * have their fetch size set to "0".
     * 
     * @throws Exception
     */
    @Test
    public void testBug33594() throws Exception {
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
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "false");
        props.setProperty(PropertyKey.useCursorFetch.getKeyName(), "false");
        props.setProperty(PropertyKey.defaultFetchSize.getKeyName(), "100");
        props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "true");
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
            props2.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props2.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props2.setProperty(PropertyKey.useInformationSchema.getKeyName(), "false");
            props2.setProperty(PropertyKey.useCursorFetch.getKeyName(), "true");
            props2.setProperty(PropertyKey.defaultFetchSize.getKeyName(), "100");
            props2.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "true");

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

    @Test
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

    @Test
    public void testNoSystemTablesReturned() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {
                    conn1 = getConnectionWithProps(props);

                    this.rs = dbMapsToSchema ? conn1.getMetaData().getTables("null", "information_schema", "%", new String[] { "SYSTEM VIEW" })
                            : conn1.getMetaData().getTables("information_schema", "null", "%", new String[] { "SYSTEM VIEW" });
                    assertTrue(this.rs.next());

                    this.rs = dbMapsToSchema ? conn1.getMetaData().getTables("null", "information_schema", "%", new String[] { "SYSTEM TABLE" })
                            : conn1.getMetaData().getTables("information_schema", "null", "%", new String[] { "SYSTEM TABLE" });
                    assertFalse(this.rs.next());

                    this.rs = dbMapsToSchema ? conn1.getMetaData().getTables("null", "information_schema", "%", new String[] { "TABLE" })
                            : conn1.getMetaData().getTables("information_schema", "null", "%", new String[] { "TABLE" });
                    assertFalse(this.rs.next());

                    this.rs = dbMapsToSchema ? conn1.getMetaData().getTables("null", "information_schema", "%", new String[] { "VIEW" })
                            : conn1.getMetaData().getTables("information_schema", "null", "%", new String[] { "VIEW" });
                    assertFalse(this.rs.next());

                    this.rs = dbMapsToSchema
                            ? conn1.getMetaData().getTables("null", "information_schema", "%", new String[] { "SYSTEM TABLE", "SYSTEM VIEW", "TABLE", "VIEW" })
                            : conn1.getMetaData().getTables("information_schema", "null", "%", new String[] { "SYSTEM TABLE", "SYSTEM VIEW", "TABLE", "VIEW" });
                    assertTrue(this.rs.next());

                    this.rs = dbMapsToSchema ? conn1.getMetaData().getColumns(null, "information_schema", "TABLES", "%")
                            : conn1.getMetaData().getColumns("information_schema", null, "TABLES", "%");
                    assertTrue(this.rs.next());
                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    @Test
    public void testABunchOfReturnTypes() throws Exception {
        checkABunchOfReturnTypesForConnection(this.conn);
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        checkABunchOfReturnTypesForConnection(getConnectionWithProps(props));
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
                Types.BIGINT, // 11. CARDINALITY int/long => When TYPE is tableIndexStatistic, then this is the number of rows
                // in the table; otherwise, it is the number of unique values in the index.
                Types.BIGINT, // 12. PAGES int/long => When TYPE is tableIndexStatistic then this is the number of pages used
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

    private final static Map<Integer, String> TYPES_MAP = new HashMap<>();

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
            assertEquals(expectedType, actualType, "Unexpected type in column " + (i + 1));
        }
    }

    /**
     * Bug #43714 - useInformationSchema with DatabaseMetaData.getExportedKeys() throws exception
     * 
     * @throws Exception
     */
    @Test
    public void testBug43714() throws Exception {
        Connection c_IS = null;
        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
            c_IS = getConnectionWithProps(props);
            DatabaseMetaData dbmd = c_IS.getMetaData();
            this.rs = dbmd.getExportedKeys("x", "y", "z");
        } finally {
            if (c_IS != null) {
                try {
                    c_IS.close();
                } catch (SQLException ex) {
                }
            }
        }
    }

    /**
     * Bug #41269 - DatabaseMetadata.getProcedureColumns() returns wrong value for column length
     * 
     * @throws Exception
     */
    @Test
    public void testBug41269() throws Exception {
        createProcedure("bug41269", "(in param1 int, out result varchar(197)) BEGIN select 1, ''; END");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "true");
        Connection con = getConnectionWithProps(props);
        try {
            ResultSet procMD = con.getMetaData().getProcedureColumns(null, null, "bug41269", "%");
            assertTrue(procMD.next());
            assertEquals(10, procMD.getInt(9), "Int param length");
            assertTrue(procMD.next());
            assertEquals(197, procMD.getInt(9), "String param length");
            assertFalse(procMD.next());
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    @Test
    public void testBug31187() throws Exception {
        createTable("testBug31187", "(field1 int)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "false");
        Connection nullCatConn = getConnectionWithProps(props);
        DatabaseMetaData dbmd = nullCatConn.getMetaData();
        ResultSet dbTblCols = dbmd.getColumns(null, null, "testBug31187", "%");

        boolean found = false;
        boolean dbMapsToSchema = ((JdbcConnection) nullCatConn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                .getValue() == DatabaseTerm.SCHEMA;

        while (dbTblCols.next()) {
            String db = dbMapsToSchema ? dbTblCols.getString("TABLE_SCHEM") : dbTblCols.getString("TABLE_CAT");
            String table = dbTblCols.getString("TABLE_NAME");
            boolean useLowerCaseTableNames = dbmd.storesLowerCaseIdentifiers();

            if (db.equals(dbMapsToSchema ? nullCatConn.getSchema() : nullCatConn.getCatalog())
                    && (((useLowerCaseTableNames && "testBug31187".equalsIgnoreCase(table)) || "testBug31187".equals(table)))) {
                found = true;
            }
        }

        assertTrue(found, "Didn't find any columns for table named 'testBug31187' in database " + this.conn.getCatalog());
    }

    @Test
    public void testBug44508() throws Exception {
        DatabaseMetaData dbmd = this.conn.getMetaData();

        this.rs = dbmd.getSuperTypes("", "", "");
        ResultSetMetaData rsmd = this.rs.getMetaData();

        assertEquals("TYPE_CAT", rsmd.getColumnName(1)); // Gives TABLE_CAT
        assertEquals("TYPE_SCHEM", rsmd.getColumnName(2)); // Gives TABLE_SCHEM
    }

    /**
     * Tests fix for BUG#52167 - Can't parse parameter list with special characters inside
     * 
     * @throws Exception
     */
    @Test
    public void testBug52167() throws Exception {
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
     * Tests fix for BUG#51912 - Passing NULL as cat. param to getProcedureColumns with nullCatalogMeansCurrent = false
     * 
     * @throws Exception
     */
    @Test
    public void testBug51912() throws Exception {
        Connection overrideConn = null;
        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "false");
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
     */
    @Test
    public void testBug38367() throws Exception {
        createProcedure("sptestBug38367",
                "(OUT nfact VARCHAR(100), IN ccuenta VARCHAR(100),\nOUT ffact VARCHAR(100),\nOUT fdoc VARCHAR(100))" + "\nBEGIN\nEND");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        Connection con = getConnectionWithProps(props);
        try {
            DatabaseMetaData dbMeta = con.getMetaData();
            this.rs = dbMeta.getProcedureColumns(con.getCatalog(), null, "sptestBug38367", null);
            while (this.rs.next()) {
                String columnName = this.rs.getString(4);
                Short columnNullable = new Short(this.rs.getShort(12));
                assertTrue(columnNullable.intValue() == java.sql.DatabaseMetaData.procedureNullable,
                        "Parameter " + columnName + " is not java.sql.DatabaseMetaData.procedureNullable.");
            }
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    /**
     * Tests fix for BUG#57808 - wasNull not set for DATE field with value 0000-00-00 in getDate() although zeroDateTimeBehavior is CONVERT_TO_NULL.
     * 
     * @throws Exception
     */
    @Test
    public void testBug57808() throws Exception {
        try {
            createTable("bug57808", "(ID INT(3) NOT NULL PRIMARY KEY, ADate DATE NOT NULL)");
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
            props.setProperty(PropertyKey.zeroDateTimeBehavior.getKeyName(), "CONVERT_TO_NULL");
            Connection conn1 = null;

            conn1 = getConnectionWithProps(props);
            this.stmt = conn1.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            this.stmt.executeUpdate("INSERT INTO bug57808(ID, ADate) VALUES(1, 0000-00-00)");

            this.rs = this.stmt.executeQuery("SELECT ID, ADate FROM bug57808 WHERE ID = 1");
            if (this.rs.first()) {
                Date theDate = this.rs.getDate("ADate");
                assertNull(theDate, "Original date was not NULL!");
                assertTrue(this.rs.wasNull(), "wasNull is FALSE");
            }
        } finally {
        }
    }

    /**
     * Tests fix for BUG#61150 - First call to SP fails with "No Database Selected"
     * The workaround introduced in DatabaseMetaData.getCallStmtParameterTypes to fix the bug in server where SHOW CREATE PROCEDURE was not respecting
     * lower-case table names is misbehaving when connection is not attached to database and on non-casesensitive OS.
     * 
     * @throws Exception
     */
    @Test
    public void testBug61150() throws Exception {
        StringBuilder newUrlToTestNoDB = new StringBuilder("jdbc:mysql://");
        newUrlToTestNoDB.append(getEncodedHostPortPairFromTestsuiteUrl()).append("/");

        Statement savedSt = this.stmt;

        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.remove(PropertyKey.DBNAME.getKeyName());
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
     * Tests fix for BUG#61332 - Check if "LIKE" or "=" is sent to server in I__S query when no wildcards are supplied for schema parameter.
     * 
     * @throws Exception
     */
    @Test
    public void testBug61332() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), QueryInterceptorBug61332.class.getName());

        createDatabase("dbbug61332");
        Connection testConn = getConnectionWithProps(props);

        try {
            createTable("dbbug61332.bug61332", "(c1 char(1))");
            DatabaseMetaData metaData = testConn.getMetaData();

            this.rs = ((JdbcConnection) this.conn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                    ? metaData.getColumns(null, "dbbug61332", "bug61332", null)
                    : metaData.getColumns("dbbug61332", null, "bug61332", null);
            this.rs.next();
        } finally {
        }
    }

    public static class QueryInterceptorBug61332 extends BaseQueryInterceptor {
        @Override
        public <T extends Resultset> T preProcess(Supplier<String> str, Query interceptedQuery) {
            String sql = str.get();
            if (interceptedQuery instanceof ClientPreparedStatement) {
                sql = ((ClientPreparedStatement) interceptedQuery).getPreparedSql();
                assertTrue(StringUtils.indexOfIgnoreCase(0, sql, "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?") > -1, "Failed on: " + sql);
            }
            return null;
        }
    }

    @Test
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
     */
    @Test
    public void testBug61203() throws Exception {
        Connection rootConn = null;
        Connection userConn = null;
        CallableStatement cStmt = null;

        try {
            Properties props = getPropertiesFromTestsuiteUrl();
            String dbname = props.getProperty(PropertyKey.DBNAME.getKeyName());
            if (dbname == null) {
                assertTrue(false, "No database selected");
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
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.noAccessToProcedureBodies.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        rootConn = getConnectionWithProps(props);
        props.setProperty(PropertyKey.USER.getKeyName(), "bug61203user");
        props.setProperty(PropertyKey.PASSWORD.getKeyName(), "foo");
        userConn = getConnectionWithProps(props);
        // 1.1.1. root call;
        callFunction(cStmt, rootConn);
        callProcedure(cStmt, rootConn);
        // 1.1.2. underprivileged user call;
        callFunction(cStmt, userConn);
        callProcedure(cStmt, userConn);

        // 1.2. no information schema
        props.clear();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.noAccessToProcedureBodies.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "false");
        rootConn = getConnectionWithProps(props);
        props.setProperty(PropertyKey.USER.getKeyName(), "bug61203user");
        props.setProperty(PropertyKey.PASSWORD.getKeyName(), "foo");
        userConn = getConnectionWithProps(props);
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
        assertEquals(cStmt.getInt(1), .001, 2f);
    }

    private void callProcedure(CallableStatement cStmt, Connection c) throws SQLException {
        cStmt = c.prepareCall("{CALL testbug61203pr(?,?,?)}");
        cStmt.setFloat(1, 2);
        cStmt.setInt(2, 1);
        cStmt.setInt(3, 1);
        cStmt.registerOutParameter(1, Types.INTEGER);
        cStmt.execute();
        assertEquals(cStmt.getInt(1), .001, 2f);
    }

    /**
     * Tests fix for BUG#63456 - MetaData precision is different when using UTF8 or Latin1 tables
     * 
     * @throws Exception
     */
    @Test
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
     */
    @Test
    public void testBug63800() throws Exception {
        try {
            Properties props = getPropertiesFromTestsuiteUrl();
            String dbname = props.getProperty(PropertyKey.DBNAME.getKeyName());
            assertFalse(StringUtils.isNullOrEmpty(dbname), "No database selected");

            for (boolean useIS : new boolean[] { false, true }) {
                for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                    props = new Properties();
                    props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
                    props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
                    props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                    props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());
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

                    System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);
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

        if (((JdbcConnection) con).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA) {
            String dbPattern = con.getSchema().substring(0, con.getSchema().length() - 1) + "%";
            this.rs = dmd.getVersionColumns(null, dbPattern, "testBug63800");
            assertFalse(this.rs.next(), "Schema pattern " + dbPattern + " should not be recognized.");
        } else {
            String dbPattern = con.getCatalog().substring(0, con.getCatalog().length() - 1) + "%";
            this.rs = dmd.getVersionColumns(dbPattern, null, "testBug63800");
            assertFalse(this.rs.next(), "Catalog pattern " + dbPattern + " should not be recognized.");
        }

        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue(this.rs.next(), "1 column must be found");
        assertEquals("f1", this.rs.getString(2), "Wrong column or single column not found");
        assertEquals(19, this.rs.getInt("COLUMN_SIZE"));

        if (versionMeetsMinimum(5, 6, 4)) {
            // fractional seconds are not supported in previous versions
            st.execute("DROP  TABLE IF EXISTS testBug63800");
            st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3))");
            dmd = con.getMetaData();
            this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
            assertTrue(this.rs.next(), "1 column must be found");
            assertEquals("f1", this.rs.getString(2), "Wrong column or single column not found");
            assertEquals(23, this.rs.getInt("COLUMN_SIZE"));
        }

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        if ("ON".equals(getMysqlVariable("explicit_defaults_for_timestamp"))) {
            assertFalse(this.rs.next(), "0 column must be found");
        } else {
            assertTrue(this.rs.next(), "1 column must be found");
            assertEquals("f1", this.rs.getString(2), "Wrong column or single column not found");
        }

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP DEFAULT CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertFalse(this.rs.next(), "0 column must be found");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP DEFAULT 0)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertFalse(this.rs.next(), "0 column must be found");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue(this.rs.next(), "1 column must be found");
        assertEquals("f1", this.rs.getString(2), "Wrong column or single column not found");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue(this.rs.next(), "1 column must be found");
        assertEquals("f1", this.rs.getString(2), "Wrong column or single column not found");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP NULL, f2 TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue(this.rs.next(), "1 column must be found");
        assertEquals("f2", this.rs.getString(2), "Wrong column or single column not found");

        // ALTER test
        st.execute("ALTER TABLE testBug63800 CHANGE COLUMN `f2` `f2` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00', "
                + "ADD COLUMN `f3` TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP  AFTER `f2`");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue(this.rs.next(), "1 column must be found");
        assertEquals("f3", this.rs.getString(2), "Wrong column or single column not found");
    }

    private void testDatetime(Connection con, Statement st, String dbname) throws SQLException {
        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP)");
        DatabaseMetaData dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue(this.rs.next(), "1 column must be found");
        assertEquals("f1", this.rs.getString(2), "Wrong column or single column not found");
        assertEquals(19, this.rs.getInt("COLUMN_SIZE"));

        if (versionMeetsMinimum(5, 6, 4)) {
            // fractional seconds are not supported in previous versions
            st.execute("DROP  TABLE IF EXISTS testBug63800");
            st.execute("CREATE TABLE testBug63800(f1 DATETIME(4) DEFAULT CURRENT_TIMESTAMP(4) ON UPDATE CURRENT_TIMESTAMP(4))");
            dmd = con.getMetaData();
            this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
            assertTrue(this.rs.next(), "1 column must be found");
            assertEquals("f1", this.rs.getString(2), "Wrong column or single column not found");
            assertEquals(24, this.rs.getInt("COLUMN_SIZE"));
        }

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertFalse(this.rs.next(), "0 column must be found");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME DEFAULT CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertFalse(this.rs.next(), "0 column must be found");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME DEFAULT 0)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertFalse(this.rs.next(), "0 column must be found");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME DEFAULT 0 ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue(this.rs.next(), "1 column must be found");
        assertEquals("f1", this.rs.getString(2), "Wrong column or single column not found");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        assertTrue(this.rs.next(), "1 column must be found");
        assertEquals("f1", this.rs.getString(2), "Wrong column or single column not found");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 DATETIME NULL, f2 DATETIME ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        int cnt = 0;
        while (this.rs.next()) {
            cnt++;
            assertEquals(1, cnt, "1 column must be found");
            assertEquals("f2", this.rs.getString(2), "Wrong column or single column not found");
        }

        // ALTER 1 test
        st.execute("ALTER TABLE testBug63800 CHANGE COLUMN `f2` `f2` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00', "
                + "ADD COLUMN `f3` DATETIME NULL ON UPDATE CURRENT_TIMESTAMP  AFTER `f2`");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        cnt = 0;
        while (this.rs.next()) {
            cnt++;
            assertEquals(1, cnt, "1 column must be found");
            assertEquals("f3", this.rs.getString(2), "Wrong column or single column not found");
        }

        // ALTER 2 test
        st.execute("ALTER TABLE testBug63800 CHANGE COLUMN `f2` `f2` TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        cnt = 0;
        while (this.rs.next()) {
            cnt++;
        }
        assertEquals(2, cnt, "2 column must be found");

        st.execute("DROP  TABLE IF EXISTS testBug63800");
        st.execute("CREATE TABLE testBug63800(f1 TIMESTAMP, f2 DATETIME ON UPDATE CURRENT_TIMESTAMP, f3 TIMESTAMP NULL ON UPDATE CURRENT_TIMESTAMP)");
        dmd = con.getMetaData();
        this.rs = dmd.getVersionColumns(dbname, dbname, "testBug63800");
        cnt = 0;
        while (this.rs.next()) {
            cnt++;
        }
        if ("ON".equals(getMysqlVariable("explicit_defaults_for_timestamp"))) {
            assertEquals(2, cnt, "2 column must be found");
        } else {
            assertEquals(3, cnt, "3 column must be found");
        }
    }

    /**
     * Tests fix for BUG#16436511 - getDriverName() returns a string with company name "MySQL-AB"
     * 
     * @throws Exception
     */
    @Test
    public void testBug16436511() throws Exception {
        DatabaseMetaData dbmd = this.conn.getMetaData();
        assertEquals("MySQL Connector/J", dbmd.getDriverName());
    }

    /**
     * Test fix for BUG#68098 - DatabaseMetaData.getIndexInfo sorts results incorrectly.
     * 
     * @throws Exception
     */
    @Test
    public void testBug68098() throws Exception {
        String[] testStepDescription = new String[] { "MySQL MetaData", "I__S MetaData" };
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        Connection connUseIS = getConnectionWithProps(props);
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
            this.rs = testDbMetaData.getIndexInfo(this.dbName, null, "testBug68098", false, false);
            int ind = 0;
            while (this.rs.next()) {
                assertEquals(expectedIndexesOrder[ind++], this.rs.getString("INDEX_NAME"), testStepDescription[i] + ", sort order is wrong");
            }
            this.rs.close();
        }

        connUseIS.close();
    }

    /**
     * Tests fix for BUG#65871 - DatabaseMetaData.getColumns() throws an MySQLSyntaxErrorException.
     * Delimited names of databases and tables are handled correctly now. The edge case is ANSI quoted identifiers with leading and trailing "`" symbols, for
     * example CREATE DATABASE "`dbname`". Methods like DatabaseMetaData.getColumns() allow parameters passed both in unquoted and quoted form, quoted form is
     * not JDBC-compliant but used by third party tools. So when you pass the identifier "`dbname`" in unquoted form (`dbname`) driver handles it as quoted by
     * "`" symbol. To handle such identifiers correctly a new behavior was added to pedantic mode (connection property pedantic=true), now if it set to true
     * methods like DatabaseMetaData.getColumns() treat all parameters as unquoted.
     * 
     * @throws Exception
     */
    @Test
    public void testBug65871() throws Exception {
        createTable("testbug65871_foreign",
                "(cpd_foreign_1_id int(8) not null, cpd_foreign_2_id int(8) not null," + "primary key (cpd_foreign_1_id, cpd_foreign_2_id)) ", "InnoDB");

        Connection pedanticConn = null;
        Connection pedanticConn_IS = null;
        Connection nonPedanticConn = null;
        Connection nonPedanticConn_IS = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

            props.setProperty(PropertyKey.sessionVariables.getKeyName(), "sql_mode=ansi");
            nonPedanticConn = getConnectionWithProps(props);

            props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
            nonPedanticConn_IS = getConnectionWithProps(props);

            props.setProperty(PropertyKey.pedantic.getKeyName(), "true");
            pedanticConn_IS = getConnectionWithProps(props);

            props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "false");
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
        boolean pedantic = ((MysqlConnection) conn1).getPropertySet().getBooleanProperty(PropertyKey.pedantic).getValue();

        testBug65871_testCatalog("db1`testbug65871", StringUtils.quoteIdentifier("db1`testbug65871", pedantic), conn1);

        testBug65871_testCatalog("db2`testbug65871", StringUtils.quoteIdentifier("db2`testbug65871", "\"", pedantic), conn1);

        testBug65871_testCatalog("`db3`testbug65871`", StringUtils.quoteIdentifier("`db3`testbug65871`", "\"", pedantic), conn1);
    }

    private void testBug65871_testCatalog(String unquotedDbName, String quotedDbName, Connection conn1) throws Exception {
        Statement st1 = null;

        try {
            st1 = conn1.createStatement();

            // 1. catalog
            st1.executeUpdate("DROP DATABASE IF EXISTS " + quotedDbName);
            st1.executeUpdate("CREATE DATABASE " + quotedDbName);
            this.rs = st1.executeQuery("show databases like '" + unquotedDbName + "'");
            assertTrue(this.rs.next(), "Database " + unquotedDbName + " (quoted " + quotedDbName + ") not found.");
            assertEquals(unquotedDbName, this.rs.getString(1));

            boolean pedantic = ((MysqlConnection) conn1).getPropertySet().getBooleanProperty(PropertyKey.pedantic).getValue();

            testBug65871_testTable(unquotedDbName, quotedDbName, "table1`testbug65871", StringUtils.quoteIdentifier("table1`testbug65871", pedantic), conn1,
                    st1);

            testBug65871_testTable(unquotedDbName, quotedDbName, "table2`testbug65871", StringUtils.quoteIdentifier("table2`testbug65871", "\"", pedantic),
                    conn1, st1);

            testBug65871_testTable(unquotedDbName, quotedDbName, "table3\"testbug65871", StringUtils.quoteIdentifier("table3\"testbug65871", "\"", pedantic),
                    conn1, st1);

            testBug65871_testTable(unquotedDbName, quotedDbName, "`table4`testbug65871`", StringUtils.quoteIdentifier("`table4`testbug65871`", "\"", pedantic),
                    conn1, st1);

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
                    + " foreign key (\"cpd_f\"\"oreign_1_id\", \"`cpd_f\"\"oreign_2_id`\")  references "
                    + (((JdbcConnection) st1.getConnection()).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                            .getValue() == DatabaseTerm.SCHEMA ? this.conn.getSchema() : this.conn.getCatalog())
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
            try {
                this.rs = ((com.mysql.cj.jdbc.DatabaseMetaData) conn1.getMetaData()).extractForeignKeyFromCreateTable(unquotedDbName, unquotedTableName);
                if (!this.rs.next()) {
                    failedTests.append("conn.getMetaData.extractForeignKeyFromCreateTable(unquotedDbName, unquotedTableName);\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.extractForeignKeyFromCreateTable(unquotedDbName, unquotedTableName);\n");
            }

            // 3. getColumns(...)
            try {
                boolean found = false;
                this.rs = ((JdbcConnection) conn1).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                        ? conn1.getMetaData().getColumns(null, unquotedDbName, unquotedTableName, "`B`EST`")
                        : conn1.getMetaData().getColumns(unquotedDbName, null, unquotedTableName, "`B`EST`");
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
                this.rs = ((JdbcConnection) conn1).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                        ? conn1.getMetaData().getBestRowIdentifier(null, unquotedDbName, unquotedTableName, DatabaseMetaData.bestRowNotPseudo, true)
                        : conn1.getMetaData().getBestRowIdentifier(unquotedDbName, null, unquotedTableName, DatabaseMetaData.bestRowNotPseudo, true);
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
                this.rs = ((JdbcConnection) conn1).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                        ? conn1.getMetaData().getCrossReference(null, this.conn.getCatalog(), "testbug65871_foreign", unquotedDbName, null, unquotedTableName)
                        : conn1.getMetaData().getCrossReference(this.conn.getCatalog(), null, "testbug65871_foreign", unquotedDbName, null, unquotedTableName);
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
                this.rs = ((JdbcConnection) conn1).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                        ? conn1.getMetaData().getExportedKeys(unquotedDbName, null, unquotedTableName)
                        : conn1.getMetaData().getExportedKeys(unquotedDbName, null, unquotedTableName);
                if (!this.rs.next()) {
                    failedTests.append("conn.getMetaData.getExportedKeys(unquotedDbName, null, unquotedTableName);\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.getExportedKeys(unquotedDbName, null, unquotedTableName);\n");
            }

            // 7. getImportedKeys(...)
            try {
                this.rs = ((JdbcConnection) conn1).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                        ? conn1.getMetaData().getImportedKeys(null, unquotedDbName, unquotedTableName)
                        : conn1.getMetaData().getImportedKeys(unquotedDbName, null, unquotedTableName);
                if (!this.rs.next()) {
                    failedTests.append("conn.getMetaData.getImportedKeys(unquotedDbName, null, unquotedTableName);\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.getImportedKeys(unquotedDbName, null, unquotedTableName);\n");
            }

            // 8. getIndexInfo(...)
            try {
                this.rs = ((JdbcConnection) conn1).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                        ? conn1.getMetaData().getIndexInfo(null, unquotedDbName, unquotedTableName, true, false)
                        : conn1.getMetaData().getIndexInfo(unquotedDbName, null, unquotedTableName, true, false);
                if (!this.rs.next()) {
                    failedTests.append("conn.getMetaData.getIndexInfo(unquotedDbName, null, unquotedTableName, true, false);\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.getIndexInfo(unquotedDbName, null, unquotedTableName, true, false);\n");
            }

            // 9. getPrimaryKeys(...)
            try {
                this.rs = ((JdbcConnection) conn1).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                        ? conn1.getMetaData().getPrimaryKeys(null, unquotedDbName, unquotedTableName)
                        : conn1.getMetaData().getPrimaryKeys(unquotedDbName, null, unquotedTableName);
                if (!this.rs.next()) {
                    failedTests.append("conn.getMetaData.getPrimaryKeys(unquotedDbName, null, unquotedTableName);\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.getPrimaryKeys(unquotedDbName, null, unquotedTableName);\n");
            }

            // 10. getTables(...)
            try {
                this.rs = ((JdbcConnection) conn1).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                        ? conn1.getMetaData().getTables(null, unquotedDbName, unquotedTableName, new String[] { "TABLE" })
                        : conn1.getMetaData().getTables(unquotedDbName, null, unquotedTableName, new String[] { "TABLE" });
                if (!this.rs.next()) {
                    failedTests.append("conn.getMetaData.getTables(unquotedDbName, null, unquotedTableName, new String[] {\"TABLE\"});\n");
                }
            } catch (Exception e) {
                failedTests.append("conn.getMetaData.getTables(unquotedDbName, null, unquotedTableName, new String[] {\"TABLE\"});\n");
            }

            // 11. getVersionColumns(...)
            try {
                this.rs = ((JdbcConnection) conn1).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA
                        ? conn1.getMetaData().getVersionColumns(null, unquotedDbName, unquotedTableName)
                        : conn1.getMetaData().getVersionColumns(unquotedDbName, null, unquotedTableName);
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
            boolean pedantic = ((MysqlConnection) conn1).getPropertySet().getBooleanProperty(PropertyKey.pedantic).getValue();
            throw new Exception("Failed tests for catalog " + quotedDbName + " and table " + quotedTableName + " ("
                    + (pedantic ? "pedantic mode" : "non-pedantic mode") + "):\n" + failedTests.toString());
        }
    }

    /**
     * Tests fix for BUG#69298 - Different outcome from DatabaseMetaData.getFunctions() when using I__S.
     * 
     * @throws Exception
     */
    @Test
    public void testBug69298() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "true");

        Connection testConn;

        createFunction("testBug69298_func", "(param_func INT) RETURNS INT COMMENT 'testBug69298_func comment' DETERMINISTIC RETURN 1");
        createProcedure("testBug69298_proc", "(IN param_proc INT) COMMENT 'testBug69298_proc comment' SELECT 1");

        // test with property useInformationSchema=false
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "false");
        testConn = getConnectionWithProps(props);
        assertFalse(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.useInformationSchema).getValue(),
                "Property useInformationSchema should be false");
        assertTrue(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.getProceduresReturnsFunctions).getValue(),
                "Property getProceduresReturnsFunctions should be true");
        checkGetFunctionsForBug69298("Std. Connection MetaData", testConn);
        checkGetFunctionColumnsForBug69298("Std. Connection MetaData", testConn);
        checkGetProceduresForBug69298("Std. Connection MetaData", testConn);
        checkGetProcedureColumnsForBug69298("Std. Connection MetaData", testConn);
        testConn.close();

        // test with property useInformationSchema=true
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        testConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.useInformationSchema).getValue(),
                "Property useInformationSchema should be true");
        assertTrue(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.getProceduresReturnsFunctions).getValue(),
                "Property getProceduresReturnsFunctions should be true");
        checkGetFunctionsForBug69298("Prop. useInfoSchema(1) MetaData", testConn);
        checkGetFunctionColumnsForBug69298("Prop. useInfoSchema(1) MetaData", testConn);
        checkGetProceduresForBug69298("Prop. useInfoSchema(1) MetaData", testConn);
        checkGetProcedureColumnsForBug69298("Prop. useInfoSchema(1) MetaData", testConn);
        testConn.close();

        // test with property useInformationSchema=false & getProceduresReturnsFunctions=false
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "false");
        props.setProperty(PropertyKey.getProceduresReturnsFunctions.getKeyName(), "false");
        testConn = getConnectionWithProps(props);
        assertFalse(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.useInformationSchema).getValue(),
                "Property useInformationSchema should be false");
        assertFalse(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.getProceduresReturnsFunctions).getValue(),
                "Property getProceduresReturnsFunctions should be false");
        checkGetFunctionsForBug69298("Prop. getProcRetFunc(0) MetaData", testConn);
        checkGetFunctionColumnsForBug69298("Prop. getProcRetFunc(0) MetaData", testConn);
        checkGetProceduresForBug69298("Prop. getProcRetFunc(0) MetaData", testConn);
        checkGetProcedureColumnsForBug69298("Prop. getProcRetFunc(0) MetaData", testConn);
        testConn.close();

        // test with property useInformationSchema=true & getProceduresReturnsFunctions=false
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        props.setProperty(PropertyKey.getProceduresReturnsFunctions.getKeyName(), "false");
        testConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.useInformationSchema).getValue(),
                "Property useInformationSchema should be true");
        assertFalse(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.getProceduresReturnsFunctions).getValue(),
                "Property getProceduresReturnsFunctions should be false");
        checkGetFunctionsForBug69298("Prop. useInfoSchema(1) + getProcRetFunc(0) MetaData", testConn);
        checkGetFunctionColumnsForBug69298("Prop. useInfoSchema(1) + getProcRetFunc(0) MetaData", testConn);
        checkGetProceduresForBug69298("Prop. useInfoSchema(1) + getProcRetFunc(0) MetaData", testConn);
        checkGetProcedureColumnsForBug69298("Prop. useInfoSchema(1) + getProcRetFunc(0) MetaData", testConn);
        testConn.close();
    }

    private void checkGetFunctionsForBug69298(String stepDescription, Connection testConn) throws Exception {
        DatabaseMetaData testDbMetaData = testConn.getMetaData();
        boolean dbMapsToSchema = ((JdbcConnection) testConn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                .getValue() == DatabaseTerm.SCHEMA;
        ResultSet functionsMD = testDbMetaData.getFunctions(null, null, "testBug69298_%");
        String sd = stepDescription + " getFunctions() ";

        assertTrue(functionsMD.next(), sd + "one row expected.");

        // function: testBug69298_func

        assertEquals(dbMapsToSchema ? "def" : testConn.getCatalog(), functionsMD.getString("FUNCTION_CAT"), sd + "-> FUNCTION_CAT");
        assertEquals(dbMapsToSchema ? testConn.getSchema() : null, functionsMD.getString("FUNCTION_SCHEM"), sd + "-> FUNCTION_SCHEM");
        assertEquals("testBug69298_func", functionsMD.getString("FUNCTION_NAME"), sd + "-> FUNCTION_NAME");
        assertEquals("testBug69298_func comment", functionsMD.getString("REMARKS"), sd + "-> REMARKS");
        assertEquals(DatabaseMetaData.functionNoTable, functionsMD.getShort("FUNCTION_TYPE"), sd + "-> FUNCTION_TYPE");
        assertEquals("testBug69298_func", functionsMD.getString("SPECIFIC_NAME"), sd + "-> SPECIFIC_NAME");

        assertFalse(functionsMD.next(), stepDescription + "no more rows expected.");
    }

    private void checkGetFunctionColumnsForBug69298(String stepDescription, Connection testConn) throws Exception {
        DatabaseMetaData testDbMetaData = testConn.getMetaData();
        boolean dbMapsToSchema = ((JdbcConnection) testConn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                .getValue() == DatabaseTerm.SCHEMA;
        ResultSet funcColsMD = testDbMetaData.getFunctionColumns(null, null, "testBug69298_%", "%");
        String sd = stepDescription + " getFunctionColumns() ";

        assertTrue(funcColsMD.next(), sd + "1st of 2 rows expected.");

        // function column: testBug69298_func return
        assertEquals(dbMapsToSchema ? "def" : testConn.getCatalog(), funcColsMD.getString("FUNCTION_CAT"), sd + "-> FUNCTION_CAT");
        assertEquals(dbMapsToSchema ? testConn.getSchema() : null, funcColsMD.getString("FUNCTION_SCHEM"), sd + "-> FUNCTION_SCHEM");
        assertEquals("testBug69298_func", funcColsMD.getString("FUNCTION_NAME"), sd + "-> FUNCTION_NAME");
        assertEquals("", funcColsMD.getString("COLUMN_NAME"), sd + "-> COLUMN_NAME");
        assertEquals(DatabaseMetaData.functionReturn, funcColsMD.getShort("COLUMN_TYPE"), sd + "-> COLUMN_TYPE");
        assertEquals(Types.INTEGER, funcColsMD.getInt("DATA_TYPE"), sd + "-> DATA_TYPE");
        assertEquals("INT", funcColsMD.getString("TYPE_NAME"), sd + "-> TYPE_NAME");
        assertEquals(10, funcColsMD.getInt("PRECISION"), sd + "-> PRECISION");
        assertEquals(10, funcColsMD.getInt("LENGTH"), sd + "-> LENGTH");
        assertEquals(0, funcColsMD.getShort("SCALE"), sd + "-> SCALE");
        assertEquals(10, funcColsMD.getShort("RADIX"), sd + "-> RADIX");
        assertEquals(DatabaseMetaData.functionNullable, funcColsMD.getShort("NULLABLE"), sd + "-> NULLABLE");
        assertEquals(null, funcColsMD.getString("REMARKS"), sd + "-> REMARKS");
        assertEquals(0, funcColsMD.getInt("CHAR_OCTET_LENGTH"), sd + "-> CHAR_OCTET_LENGTH");
        assertEquals(0, funcColsMD.getInt("ORDINAL_POSITION"), sd + "-> ORDINAL_POSITION");
        assertEquals("YES", funcColsMD.getString("IS_NULLABLE"), sd + "-> IS_NULLABLE");
        assertEquals("testBug69298_func", funcColsMD.getString("SPECIFIC_NAME"), sd + "-> SPECIFIC_NAME");

        assertTrue(funcColsMD.next(), sd + "2nd of 2 rows expected.");

        // function column: testBug69298_func.param_func
        assertEquals(dbMapsToSchema ? "def" : testConn.getCatalog(), funcColsMD.getString("FUNCTION_CAT"), sd + "-> FUNCTION_CAT");
        assertEquals(dbMapsToSchema ? testConn.getSchema() : null, funcColsMD.getString("FUNCTION_SCHEM"), sd + "-> FUNCTION_SCHEM");
        assertEquals("testBug69298_func", funcColsMD.getString("FUNCTION_NAME"), sd + "-> FUNCTION_NAME");
        assertEquals("param_func", funcColsMD.getString("COLUMN_NAME"), sd + "-> COLUMN_NAME");
        assertEquals(DatabaseMetaData.functionColumnIn, funcColsMD.getShort("COLUMN_TYPE"), sd + "-> COLUMN_TYPE");
        assertEquals(Types.INTEGER, funcColsMD.getInt("DATA_TYPE"), sd + "-> DATA_TYPE");
        assertEquals("INT", funcColsMD.getString("TYPE_NAME"), sd + "-> TYPE_NAME");
        assertEquals(10, funcColsMD.getInt("PRECISION"), sd + "-> PRECISION");
        assertEquals(10, funcColsMD.getInt("LENGTH"), sd + "-> LENGTH");
        assertEquals(0, funcColsMD.getShort("SCALE"), sd + "-> SCALE");
        assertEquals(10, funcColsMD.getShort("RADIX"), sd + "-> RADIX");
        assertEquals(DatabaseMetaData.functionNullable, funcColsMD.getShort("NULLABLE"), sd + "-> NULLABLE");
        assertEquals(null, funcColsMD.getString("REMARKS"), sd + "-> REMARKS");
        assertEquals(0, funcColsMD.getInt("CHAR_OCTET_LENGTH"), sd + "-> CHAR_OCTET_LENGTH");
        assertEquals(1, funcColsMD.getInt("ORDINAL_POSITION"), sd + "-> ORDINAL_POSITION");
        assertEquals("YES", funcColsMD.getString("IS_NULLABLE"), sd + "-> IS_NULLABLE");
        assertEquals("testBug69298_func", funcColsMD.getString("SPECIFIC_NAME"), sd + "-> SPECIFIC_NAME");

        assertFalse(funcColsMD.next(), sd + "no more rows expected.");
    }

    private void checkGetProceduresForBug69298(String stepDescription, Connection testConn) throws Exception {
        DatabaseMetaData testDbMetaData = testConn.getMetaData();
        boolean dbMapsToSchema = ((JdbcConnection) testConn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                .getValue() == DatabaseTerm.SCHEMA;
        ResultSet proceduresMD = testDbMetaData.getProcedures(null, null, "testBug69298_%");
        String sd = stepDescription + " getProcedures() ";
        boolean isGetProceduresReturnsFunctions = ((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.getProceduresReturnsFunctions)
                .getValue();

        if (isGetProceduresReturnsFunctions) {
            assertTrue(proceduresMD.next(), sd + "1st of 2 rows expected.");

            // function: testBug69298_func
            assertEquals(dbMapsToSchema ? "def" : testConn.getCatalog(), proceduresMD.getString("PROCEDURE_CAT"), sd + "-> PROCEDURE_CAT");
            assertEquals(dbMapsToSchema ? testConn.getSchema() : null, proceduresMD.getString("PROCEDURE_SCHEM"), sd + "-> PROCEDURE_SCHEM");
            assertEquals("testBug69298_func", proceduresMD.getString("PROCEDURE_NAME"), sd + "-> PROCEDURE_NAME");
            assertEquals("testBug69298_func comment", proceduresMD.getString("REMARKS"), sd + "-> REMARKS");
            assertEquals(DatabaseMetaData.procedureReturnsResult, proceduresMD.getShort("PROCEDURE_TYPE"), sd + "-> PROCEDURE_TYPE");
            assertEquals("testBug69298_func", proceduresMD.getString("SPECIFIC_NAME"), sd + "-> SPECIFIC_NAME");

            assertTrue(proceduresMD.next(), sd + "2nd of 2 rows expected.");
        } else {
            assertTrue(proceduresMD.next(), sd + "one row expected.");
        }

        // procedure: testBug69298_proc
        assertEquals(dbMapsToSchema ? "def" : testConn.getCatalog(), proceduresMD.getString("PROCEDURE_CAT"), sd + "-> PROCEDURE_CAT");
        assertEquals(dbMapsToSchema ? testConn.getSchema() : null, proceduresMD.getString("PROCEDURE_SCHEM"), sd + "-> PROCEDURE_SCHEM");
        assertEquals("testBug69298_proc", proceduresMD.getString("PROCEDURE_NAME"), sd + "-> PROCEDURE_NAME");
        assertEquals("testBug69298_proc comment", proceduresMD.getString("REMARKS"), sd + "-> REMARKS");
        assertEquals(DatabaseMetaData.procedureNoResult, proceduresMD.getShort("PROCEDURE_TYPE"), sd + "-> PROCEDURE_TYPE");
        assertEquals("testBug69298_proc", proceduresMD.getString("SPECIFIC_NAME"), sd + "-> SPECIFIC_NAME");

        assertFalse(proceduresMD.next(), stepDescription + "no more rows expected.");
    }

    private void checkGetProcedureColumnsForBug69298(String stepDescription, Connection testConn) throws Exception {
        DatabaseMetaData testDbMetaData = testConn.getMetaData();
        boolean dbMapsToSchema = ((JdbcConnection) testConn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                .getValue() == DatabaseTerm.SCHEMA;
        ResultSet procColsMD = testDbMetaData.getProcedureColumns(null, null, "testBug69298_%", "%");
        String sd = stepDescription + " getProcedureColumns() ";
        boolean isGetProceduresReturnsFunctions = ((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.getProceduresReturnsFunctions)
                .getValue();

        if (isGetProceduresReturnsFunctions) {
            assertTrue(procColsMD.next(), sd + "1st of 3 rows expected.");

            // function column: testBug69298_func return
            assertEquals(dbMapsToSchema ? "def" : testConn.getCatalog(), procColsMD.getString("PROCEDURE_CAT"), sd + "-> PROCEDURE_CAT");
            assertEquals(dbMapsToSchema ? testConn.getSchema() : null, procColsMD.getString("PROCEDURE_SCHEM"), sd + "-> PROCEDURE_SCHEM");
            assertEquals("testBug69298_func", procColsMD.getString("PROCEDURE_NAME"), sd + "-> PROCEDURE_NAME");
            assertEquals("", procColsMD.getString("COLUMN_NAME"), sd + "-> COLUMN_NAME");
            assertEquals(DatabaseMetaData.procedureColumnReturn, procColsMD.getShort("COLUMN_TYPE"), sd + "-> COLUMN_TYPE");
            assertEquals(Types.INTEGER, procColsMD.getInt("DATA_TYPE"), sd + "-> DATA_TYPE");
            assertEquals("INT", procColsMD.getString("TYPE_NAME"), sd + "-> TYPE_NAME");
            assertEquals(10, procColsMD.getInt("PRECISION"), sd + "-> PRECISION");
            assertEquals(10, procColsMD.getInt("LENGTH"), sd + "-> LENGTH");
            assertEquals(0, procColsMD.getShort("SCALE"), sd + "-> SCALE");
            assertEquals(10, procColsMD.getShort("RADIX"), sd + "-> RADIX");
            assertEquals(DatabaseMetaData.procedureNullable, procColsMD.getShort("NULLABLE"), sd + "-> NULLABLE");
            assertEquals(null, procColsMD.getString("REMARKS"), sd + "-> REMARKS");
            assertEquals(null, procColsMD.getString("COLUMN_DEF"), sd + "-> COLUMN_DEF");
            assertEquals(0, procColsMD.getInt("SQL_DATA_TYPE"), sd + "-> SQL_DATA_TYPE");
            assertEquals(0, procColsMD.getInt("SQL_DATETIME_SUB"), sd + "-> SQL_DATETIME_SUB");
            assertEquals(0, procColsMD.getInt("CHAR_OCTET_LENGTH"), sd + "-> CHAR_OCTET_LENGTH");
            assertEquals(0, procColsMD.getInt("ORDINAL_POSITION"), sd + "-> ORDINAL_POSITION");
            assertEquals("YES", procColsMD.getString("IS_NULLABLE"), sd + "-> IS_NULLABLE");
            assertEquals("testBug69298_func", procColsMD.getString("SPECIFIC_NAME"), sd + "-> SPECIFIC_NAME");

            assertTrue(procColsMD.next(), sd + "2nd of 3 rows expected.");

            // function column: testBug69298_func.param_func
            assertEquals(dbMapsToSchema ? "def" : testConn.getCatalog(), procColsMD.getString("PROCEDURE_CAT"), sd + "-> PROCEDURE_CAT");
            assertEquals(dbMapsToSchema ? testConn.getSchema() : null, procColsMD.getString("PROCEDURE_SCHEM"), sd + "-> PROCEDURE_SCHEM");
            assertEquals("testBug69298_func", procColsMD.getString("PROCEDURE_NAME"), sd + "-> PROCEDURE_NAME");
            assertEquals("param_func", procColsMD.getString("COLUMN_NAME"), sd + "-> COLUMN_NAME");
            assertEquals(DatabaseMetaData.procedureColumnIn, procColsMD.getShort("COLUMN_TYPE"), sd + "-> COLUMN_TYPE");
            assertEquals(Types.INTEGER, procColsMD.getInt("DATA_TYPE"), sd + "-> DATA_TYPE");
            assertEquals("INT", procColsMD.getString("TYPE_NAME"), sd + "-> TYPE_NAME");
            assertEquals(10, procColsMD.getInt("PRECISION"), sd + "-> PRECISION");
            assertEquals(10, procColsMD.getInt("LENGTH"), sd + "-> LENGTH");
            assertEquals(0, procColsMD.getShort("SCALE"), sd + "-> SCALE");
            assertEquals(10, procColsMD.getShort("RADIX"), sd + "-> RADIX");
            assertEquals(DatabaseMetaData.procedureNullable, procColsMD.getShort("NULLABLE"), sd + "-> NULLABLE");
            assertEquals(null, procColsMD.getString("REMARKS"), sd + "-> REMARKS");
            assertEquals(null, procColsMD.getString("COLUMN_DEF"), sd + "-> COLUMN_DEF");
            assertEquals(0, procColsMD.getInt("SQL_DATA_TYPE"), sd + "-> SQL_DATA_TYPE");
            assertEquals(0, procColsMD.getInt("SQL_DATETIME_SUB"), sd + "-> SQL_DATETIME_SUB");
            assertEquals(0, procColsMD.getInt("CHAR_OCTET_LENGTH"), sd + "-> CHAR_OCTET_LENGTH");
            assertEquals(1, procColsMD.getInt("ORDINAL_POSITION"), sd + "-> ORDINAL_POSITION");
            assertEquals("YES", procColsMD.getString("IS_NULLABLE"), sd + "-> IS_NULLABLE");
            assertEquals("testBug69298_func", procColsMD.getString("SPECIFIC_NAME"), sd + "-> SPECIFIC_NAME");

            assertTrue(procColsMD.next(), sd + "3rd of 3 rows expected.");
        } else {
            assertTrue(procColsMD.next(), sd + "one row expected.");
        }

        // procedure column: testBug69298_proc.param_proc
        assertEquals(dbMapsToSchema ? "def" : testConn.getCatalog(), procColsMD.getString("PROCEDURE_CAT"), sd + "-> PROCEDURE_CAT");
        assertEquals(dbMapsToSchema ? testConn.getSchema() : null, procColsMD.getString("PROCEDURE_SCHEM"), sd + "-> PROCEDURE_SCHEM");
        assertEquals("testBug69298_proc", procColsMD.getString("PROCEDURE_NAME"), sd + "-> PROCEDURE_NAME");
        assertEquals("param_proc", procColsMD.getString("COLUMN_NAME"), sd + "-> COLUMN_NAME");
        assertEquals(DatabaseMetaData.procedureColumnIn, procColsMD.getShort("COLUMN_TYPE"), sd + "-> COLUMN_TYPE");
        assertEquals(Types.INTEGER, procColsMD.getInt("DATA_TYPE"), sd + "-> DATA_TYPE");
        assertEquals("INT", procColsMD.getString("TYPE_NAME"), sd + "-> TYPE_NAME");
        assertEquals(10, procColsMD.getInt("PRECISION"), sd + "-> PRECISION");
        assertEquals(10, procColsMD.getInt("LENGTH"), sd + "-> LENGTH");
        assertEquals(0, procColsMD.getShort("SCALE"), sd + "-> SCALE");
        assertEquals(10, procColsMD.getShort("RADIX"), sd + "-> RADIX");
        assertEquals(DatabaseMetaData.procedureNullable, procColsMD.getShort("NULLABLE"), sd + "-> NULLABLE");
        assertEquals(null, procColsMD.getString("REMARKS"), sd + "-> REMARKS");
        assertEquals(null, procColsMD.getString("COLUMN_DEF"), sd + "-> COLUMN_DEF");
        assertEquals(0, procColsMD.getInt("SQL_DATA_TYPE"), sd + "-> SQL_DATA_TYPE");
        assertEquals(0, procColsMD.getInt("SQL_DATETIME_SUB"), sd + "-> SQL_DATETIME_SUB");
        assertEquals(0, procColsMD.getInt("CHAR_OCTET_LENGTH"), sd + "-> CHAR_OCTET_LENGTH");
        assertEquals(1, procColsMD.getInt("ORDINAL_POSITION"), sd + "-> ORDINAL_POSITION");
        assertEquals("YES", procColsMD.getString("IS_NULLABLE"), sd + "-> IS_NULLABLE");
        assertEquals("testBug69298_proc", procColsMD.getString("SPECIFIC_NAME"), sd + "-> SPECIFIC_NAME");

        assertFalse(procColsMD.next(), sd + "no more rows expected.");
    }

    /**
     * Tests fix for BUG#17248345 - GETFUNCTIONCOLUMNS() METHOD RETURNS COLUMNS OF PROCEDURE. (this happens when functions and procedures have a common name)
     * 
     * @throws Exception
     */
    @Test
    public void testBug17248345() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "true");

        Connection testConn;

        // create one stored procedure and one function with same name
        createProcedure("testBug17248345", "(IN proccol INT) SELECT 1");
        createFunction("testBug17248345", "(funccol INT) RETURNS INT DETERMINISTIC RETURN 1");

        // test with standard connection (getProceduresReturnsFunctions=true & useInformationSchema=false)
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "false");
        testConn = getConnectionWithProps(props);
        assertFalse(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.useInformationSchema).getValue(),
                "Property useInformationSchema should be false");
        assertTrue(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.getProceduresReturnsFunctions).getValue(),
                "Property getProceduresReturnsFunctions should be true");
        checkMetaDataInfoForBug17248345(testConn);
        testConn.close();

        // test with property useInformationSchema=true (getProceduresReturnsFunctions=true)
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        testConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.useInformationSchema).getValue(),
                "Property useInformationSchema should be true");
        assertTrue(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.getProceduresReturnsFunctions).getValue(),
                "Property getProceduresReturnsFunctions should be true");
        checkMetaDataInfoForBug17248345(testConn);
        testConn.close();

        // test with property getProceduresReturnsFunctions=false (useInformationSchema=false)
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "false");
        props.setProperty(PropertyKey.getProceduresReturnsFunctions.getKeyName(), "false");
        testConn = getConnectionWithProps(props);
        assertFalse(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.useInformationSchema).getValue(),
                "Property useInformationSchema should be false");
        assertFalse(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.getProceduresReturnsFunctions).getValue(),
                "Property getProceduresReturnsFunctions should be false");
        checkMetaDataInfoForBug17248345(testConn);
        testConn.close();

        // test with property useInformationSchema=true & getProceduresReturnsFunctions=false
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        props.setProperty(PropertyKey.getProceduresReturnsFunctions.getKeyName(), "false");
        testConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.useInformationSchema).getValue(),
                "Property useInformationSchema should be true");
        assertFalse(((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.getProceduresReturnsFunctions).getValue(),
                "Property getProceduresReturnsFunctions should be false");
        checkMetaDataInfoForBug17248345(testConn);
        testConn.close();
    }

    private void checkMetaDataInfoForBug17248345(Connection testConn) throws Exception {
        DatabaseMetaData testDbMetaData = testConn.getMetaData();
        ResultSet rsMD;
        boolean useInfoSchema = ((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.useInformationSchema).getValue();
        boolean getProcRetFunc = ((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.getProceduresReturnsFunctions).getValue();
        String stepDescription = "Prop. useInfoSchema(" + (useInfoSchema ? 1 : 0) + ") + getProcRetFunc(" + (getProcRetFunc ? 1 : 0) + "):";
        String sd;

        // getFunctions() must return 1 record.
        sd = stepDescription + " getFunctions() ";
        rsMD = testDbMetaData.getFunctions(null, null, "testBug17248345");
        assertTrue(rsMD.next(), sd + "one row expected.");
        assertEquals("testBug17248345", rsMD.getString("FUNCTION_NAME"), sd + " -> FUNCTION_NAME");
        assertFalse(rsMD.next(), sd + "no more rows expected.");

        // getFunctionColumns() must return 2 records (func return + func param).
        sd = stepDescription + " getFunctionColumns() ";
        rsMD = testDbMetaData.getFunctionColumns(null, null, "testBug17248345", "%");
        assertTrue(rsMD.next(), sd + "1st of 2 rows expected.");
        assertEquals("testBug17248345", rsMD.getString("FUNCTION_NAME"), sd + " -> FUNCTION_NAME");
        assertEquals("", rsMD.getString("COLUMN_NAME"), sd + " -> COLUMN_NAME");
        assertTrue(rsMD.next(), sd + "2nd of 2 rows expected.");
        assertEquals("testBug17248345", rsMD.getString("FUNCTION_NAME"), sd + " -> FUNCTION_NAME");
        assertEquals("funccol", rsMD.getString("COLUMN_NAME"), sd + " -> COLUMN_NAME");
        assertFalse(rsMD.next(), sd + "no more rows expected.");

        // getProcedures() must return 1 or 2 records, depending on if getProceduresReturnsFunctions is false or true
        // respectively. When exists a procedure and a function with same name, function is returned first.
        sd = stepDescription + " getProcedures() ";
        rsMD = testDbMetaData.getProcedures(null, null, "testBug17248345");
        if (getProcRetFunc) {
            assertTrue(rsMD.next(), sd + "1st of 2 rows expected.");
            assertEquals("testBug17248345", rsMD.getString("PROCEDURE_NAME"), sd + " -> PROCEDURE_NAME");
            assertTrue(rsMD.next(), sd + "2nd of 2 rows expected.");
        } else {
            assertTrue(rsMD.next(), sd + "one row expected.");
        }
        assertEquals("testBug17248345", rsMD.getString("PROCEDURE_NAME"), sd + " -> PROCEDURE_NAME");
        assertFalse(rsMD.next(), sd + "no more rows expected.");

        // getProcedureColumns() must return 1 or 3 records, depending on if getProceduresReturnsFunctions is false or
        // true respectively. When exists a procedure and a function with same name, function is returned first.
        sd = stepDescription + " getProcedureColumns() ";
        rsMD = testDbMetaData.getProcedureColumns(null, null, "testBug17248345", "%");
        if (getProcRetFunc) {
            assertTrue(rsMD.next(), sd + "1st of 3 rows expected.");
            assertEquals("testBug17248345", rsMD.getString("PROCEDURE_NAME"), sd + " -> PROCEDURE_NAME");
            assertEquals("", rsMD.getString("COLUMN_NAME"), sd + " -> COLUMN_NAME");
            assertTrue(rsMD.next(), sd + "2nd of 3 rows expected.");
            assertEquals("testBug17248345", rsMD.getString("PROCEDURE_NAME"), sd + " -> PROCEDURE_NAME");
            assertEquals("funccol", rsMD.getString("COLUMN_NAME"), sd + " -> COLUMN_NAME");
            assertTrue(rsMD.next(), sd + "3rd of 3 rows expected.");
        } else {
            assertTrue(rsMD.next(), sd + "one row expected.");
        }
        assertEquals("testBug17248345", rsMD.getString("PROCEDURE_NAME"), sd + " -> PROCEDURE_NAME");
        assertEquals("proccol", rsMD.getString("COLUMN_NAME"), sd + " -> COLUMN_NAME");
        assertFalse(rsMD.next(), sd + "no more rows expected.");
    }

    /**
     * Tests fix for BUG#69290 - JDBC Table type "SYSTEM TABLE" is used inconsistently.
     * 
     * Tests DatabaseMetaData.getTableTypes() and DatabaseMetaData.getTables() against schemas: mysql, information_schema, performance_schema, test.
     * 
     * @throws Exception
     */
    @Test
    public void testBug69290() throws Exception {
        String[] testStepDescription = new String[] { "MySQL MetaData", "I__S MetaData" };

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        Connection connUseIS = getConnectionWithProps(props);

        props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "false");
        Connection connUseISAndNullAll = getConnectionWithProps(props);

        props.remove(PropertyKey.useInformationSchema.getKeyName());
        Connection connNullAll = getConnectionWithProps(props);

        boolean dbMapsToSchema = ((JdbcConnection) this.conn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                .getValue() == DatabaseTerm.SCHEMA;

        final String testDb = dbMapsToSchema ? this.conn.getSchema() : this.conn.getCatalog();

        Connection[] testConnections = new Connection[] { this.conn, connUseIS };

        // check table types returned in getTableTypes()
        final List<String> tableTypes = Arrays.asList(new String[] { "LOCAL TEMPORARY", "SYSTEM TABLE", "SYSTEM VIEW", "TABLE", "VIEW" });

        for (int i = 0; i < testStepDescription.length; i++) {
            DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();
            this.rs = testDbMetaData.getTableTypes();

            int idx = 0;
            while (this.rs.next()) {
                String message = testStepDescription[i] + ", table type '" + this.rs.getString("TABLE_TYPE") + "'";
                assertFalse(idx >= tableTypes.size(), message + " not expected.");
                assertEquals(tableTypes.get(idx++), this.rs.getString("TABLE_TYPE"), message);
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
            this.rs = dbMapsToSchema ? testDbMetaData.getTables(null, "information_schema", "%", null)
                    : testDbMetaData.getTables("information_schema", null, "%", null);
            while (this.rs.next()) {
                assertEquals("SYSTEM VIEW", this.rs.getString("TABLE_TYPE"),
                        testStepDescription[i] + ", 'information_schema' catalog/schema, wrong table type for '" + this.rs.getString("TABLE_NAME") + "'.");
                countResults[i][0]++;
            }

            // check catalog/schema 'mysql'
            this.rs = dbMapsToSchema ? testDbMetaData.getTables(null, "mysql", "%", null) : testDbMetaData.getTables("mysql", null, "%", null);
            while (this.rs.next()) {
                assertEquals("SYSTEM TABLE", this.rs.getString("TABLE_TYPE"),
                        testStepDescription[i] + ", 'mysql' catalog/schema, wrong table type for '" + this.rs.getString("TABLE_NAME") + "'.");
                countResults[i][1]++;
            }

            // check catalog/schema 'performance_schema'
            this.rs = dbMapsToSchema ? testDbMetaData.getTables(null, "performance_schema", "%", null)
                    : testDbMetaData.getTables("performance_schema", null, "%", null);
            while (this.rs.next()) {
                assertEquals("SYSTEM TABLE", this.rs.getString("TABLE_TYPE"),
                        testStepDescription[i] + ", 'performance_schema' catalog/schema, wrong table type for '" + this.rs.getString("TABLE_NAME") + "'.");
                countResults[i][2]++;
            }

            // check catalog/schema '(test)'
            this.rs = dbMapsToSchema ? testDbMetaData.getTables(null, testDb, "testBug69290_%", null)
                    : testDbMetaData.getTables(testDb, null, "testBug69290_%", null);
            assertTrue(this.rs.next(), testStepDescription[i] + ", '" + testDb + "' catalog/schema, expected row from getTables().");
            assertEquals("TABLE", this.rs.getString("TABLE_TYPE"),
                    testStepDescription[i] + ", '" + testDb + "' catalog/schema, wrong table type for '" + this.rs.getString("TABLE_NAME") + "'.");
            assertTrue(this.rs.next(), testStepDescription[i] + ", '" + testDb + "' catalog/schema, expected row from getTables().");
            assertEquals("VIEW", this.rs.getString("TABLE_TYPE"),
                    testStepDescription[i] + ", '" + testDb + "' catalog/schema, wrong table type for '" + this.rs.getString("TABLE_NAME") + "'.");
        }

        // compare results count
        assertTrue(countResults[0][0] == countResults[1][0], "The number of results from getTables() MySQl(" + countResults[0][0] + ") and I__S("
                + countResults[1][0] + ") should be the same for 'information_schema' catalog/schema.");
        assertTrue(countResults[0][1] == countResults[1][1], "The number of results from getTables() MySQl(" + countResults[0][1] + ") and I__S("
                + countResults[1][1] + ") should be the same for 'mysql' catalog/schema.");
        assertTrue(countResults[0][2] == countResults[1][2], "The number of results from getTables() MySQl(" + countResults[0][2] + ") and I__S("
                + countResults[1][2] + ") should be the same for 'performance_schema' catalog/schema.");

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
                    assertEquals(tableType, this.rs.getString("TABLE_TYPE"), testStepDescription[i] + ", table type filter '" + tableType
                            + "', wrong table type for '" + this.rs.getString("TABLE_NAME") + "'.");
                    countResults[i][j]++;
                }
                j++;
            }
        }

        // compare results count
        int i = 0;
        for (String tableType : tableTypes) {
            assertTrue(countResults[0][i] == countResults[1][i], "The number of results from getTables() MySQl(" + countResults[0][i] + ") and I__S("
                    + countResults[1][i] + ") should be the same for '" + tableType + "' table type filter.");
            i++;
        }
    }

    /**
     * Tests fix for BUG#35115 - yearIsDateType=false has no effect on result's column type and class.
     * 
     * @throws Exception
     */
    @Test
    public void testBug35115() throws Exception {
        Connection testConnection = null;
        ResultSetMetaData rsMetaData = null;

        createTable("testBug35115", "(year YEAR)");
        this.stmt.executeUpdate("INSERT INTO testBug35115 VALUES ('2002'), ('2013')");

        /*
         * test connection with property 'yearIsDateType=false'
         */
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.yearIsDateType.getKeyName(), "false");
        testConnection = getConnectionWithProps(props);
        Statement st = testConnection.createStatement();
        this.rs = st.executeQuery("SELECT * FROM testBug35115");
        rsMetaData = this.rs.getMetaData();

        assertTrue(this.rs.next());
        assertEquals(Types.SMALLINT, rsMetaData.getColumnType(1), "YEAR columns should be treated as java.sql.Types.SMALLINT");
        assertEquals("YEAR", rsMetaData.getColumnTypeName(1), "YEAR columns should be identified as 'YEAR'");
        assertEquals(java.lang.Short.class.getName(), rsMetaData.getColumnClassName(1), "YEAR columns should be mapped to java.lang.Short");
        assertEquals(java.lang.Short.class.getName(), this.rs.getObject(1).getClass().getName(), "YEAR columns should be returned as java.lang.Short");

        testConnection.close();

        /*
         * test connection with property 'yearIsDateType=true'
         */
        props.setProperty(PropertyKey.yearIsDateType.getKeyName(), "true");
        testConnection = getConnectionWithProps(props);
        st = testConnection.createStatement();
        this.rs = st.executeQuery("SELECT * FROM testBug35115");
        rsMetaData = this.rs.getMetaData();

        assertTrue(this.rs.next());
        assertEquals(Types.DATE, rsMetaData.getColumnType(1), "YEAR columns should be treated as java.sql.Types.DATE");
        assertEquals("YEAR", rsMetaData.getColumnTypeName(1), "YEAR columns should be identified as 'YEAR'");
        assertEquals(java.sql.Date.class.getName(), rsMetaData.getColumnClassName(1), "YEAR columns should be mapped to java.sql.Date");
        assertEquals(java.sql.Date.class.getName(), this.rs.getObject(1).getClass().getName(), "YEAR columns should be returned as java.sql.Date");

        testConnection.close();
    }

    /**
     * Tests fix for BUG#68307 - getFunctionColumns() returns incorrect "COLUMN_TYPE" information. This is a JDBC4 feature.
     * 
     * @throws Exception
     */
    @Test
    public void testBug68307() throws Exception {
        createFunction("testBug68307_func", "(func_param_in INT) RETURNS INT DETERMINISTIC RETURN 1");

        createProcedure("testBug68307_proc", "(IN proc_param_in INT, OUT proc_param_out INT, INOUT proc_param_inout INT) SELECT 1");

        // test metadata from MySQL
        DatabaseMetaData testDbMetaData = this.conn.getMetaData();
        checkFunctionColumnTypeForBug68307("MySQL", testDbMetaData);
        checkProcedureColumnTypeForBug68307("MySQL", testDbMetaData);

        // test metadata from I__S
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        Connection connUseIS = getConnectionWithProps(props);
        testDbMetaData = connUseIS.getMetaData();
        checkFunctionColumnTypeForBug68307("I__S", testDbMetaData);
        checkProcedureColumnTypeForBug68307("I__S", testDbMetaData);
        connUseIS.close();
    }

    private void checkFunctionColumnTypeForBug68307(String testAgainst, DatabaseMetaData testDbMetaData) throws Exception {
        this.rs = testDbMetaData.getFunctionColumns(null, null, "testBug68307_%", "%");

        while (this.rs.next()) {
            String message = testAgainst + ", function <" + this.rs.getString("FUNCTION_NAME") + "." + this.rs.getString("COLUMN_NAME") + ">";
            if (this.rs.getString("COLUMN_NAME") == null || this.rs.getString("COLUMN_NAME").length() == 0) {
                assertEquals(DatabaseMetaData.functionReturn, this.rs.getShort("COLUMN_TYPE"), message);
            } else if (this.rs.getString("COLUMN_NAME").endsWith("_in")) {
                assertEquals(DatabaseMetaData.functionColumnIn, this.rs.getShort("COLUMN_TYPE"), message);
            } else if (this.rs.getString("COLUMN_NAME").endsWith("_inout")) {
                assertEquals(DatabaseMetaData.functionColumnInOut, this.rs.getShort("COLUMN_TYPE"), message);
            } else if (this.rs.getString("COLUMN_NAME").endsWith("_out")) {
                assertEquals(DatabaseMetaData.functionColumnOut, this.rs.getShort("COLUMN_TYPE"), message);
            } else {
                fail("Column '" + this.rs.getString("FUNCTION_NAME") + "." + this.rs.getString("COLUMN_NAME") + "' not expected within test case.");
            }
        }
    }

    private void checkProcedureColumnTypeForBug68307(String testAgainst, DatabaseMetaData testDbMetaData) throws Exception {
        this.rs = testDbMetaData.getProcedureColumns(null, null, "testBug68307_%", "%");

        while (this.rs.next()) {
            String message = testAgainst + ", procedure <" + this.rs.getString("PROCEDURE_NAME") + "." + this.rs.getString("COLUMN_NAME") + ">";
            if (this.rs.getString("COLUMN_NAME") == null || this.rs.getString("COLUMN_NAME").length() == 0) {
                assertEquals(DatabaseMetaData.procedureColumnReturn, this.rs.getShort("COLUMN_TYPE"), message);
            } else if (this.rs.getString("COLUMN_NAME").endsWith("_in")) {
                assertEquals(DatabaseMetaData.procedureColumnIn, this.rs.getShort("COLUMN_TYPE"), message);
            } else if (this.rs.getString("COLUMN_NAME").endsWith("_inout")) {
                assertEquals(DatabaseMetaData.procedureColumnInOut, this.rs.getShort("COLUMN_TYPE"), message);
            } else if (this.rs.getString("COLUMN_NAME").endsWith("_out")) {
                assertEquals(DatabaseMetaData.procedureColumnOut, this.rs.getShort("COLUMN_TYPE"), message);
            } else {
                fail("Column '" + this.rs.getString("FUNCTION_NAME") + "." + this.rs.getString("COLUMN_NAME") + "' not expected within test case.");
            }
        }
    }

    /**
     * Tests fix for BUG#44451 - getTables does not return resultset with expected columns.
     * 
     * @throws Exception
     */
    @Test
    public void testBug44451() throws Exception {
        String methodName;
        List<String> expectedFields;
        String[] testStepDescription = new String[] { "MySQL MetaData", "I__S MetaData" };
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        Connection connUseIS = getConnectionWithProps(props);
        Connection[] testConnections = new Connection[] { this.conn, connUseIS };

        methodName = "getClientInfoProperties()";
        expectedFields = Arrays.asList("NAME", "MAX_LEN", "DEFAULT_VALUE", "DESCRIPTION");
        for (int i = 0; i < testStepDescription.length; i++) {
            DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();
            this.rs = testDbMetaData.getClientInfoProperties();
            checkReturnedColumnsForBug44451(testStepDescription[i], methodName, expectedFields, this.rs);
            this.rs.close();
        }

        methodName = "getFunctions()";
        expectedFields = Arrays.asList("FUNCTION_CAT", "FUNCTION_SCHEM", "FUNCTION_NAME", "REMARKS", "FUNCTION_TYPE", "SPECIFIC_NAME");
        for (int i = 0; i < testStepDescription.length; i++) {
            DatabaseMetaData testDbMetaData = testConnections[i].getMetaData();
            this.rs = testDbMetaData.getFunctions(null, null, "%");
            checkReturnedColumnsForBug44451(testStepDescription[i], methodName, expectedFields, this.rs);
            this.rs.close();
        }

        connUseIS.close();
    }

    private void checkReturnedColumnsForBug44451(String stepDescription, String methodName, List<String> expectedFields, ResultSet resultSetToCheck)
            throws Exception {
        ResultSetMetaData rsMetaData = resultSetToCheck.getMetaData();
        int numberOfColumns = rsMetaData.getColumnCount();

        assertEquals(expectedFields.size(), numberOfColumns, stepDescription + ", wrong column count in method '" + methodName + "'.");
        for (int i = 0; i < numberOfColumns; i++) {
            int position = i + 1;
            assertEquals(expectedFields.get(i), rsMetaData.getColumnName(position),
                    stepDescription + ", wrong column at position '" + position + "' in method '" + methodName + "'.");
        }
        this.rs.close();
    }

    /**
     * Tests fix for BUG#20504139 - GETFUNCTIONCOLUMNS() AND GETPROCEDURECOLUMNS() RETURNS ERROR FOR VALID INPUTS.
     * 
     * @throws Exception
     */
    @Test
    public void testBug20504139() throws Exception {
        createFunction("testBug20504139f", "(namef CHAR(20)) RETURNS CHAR(50) DETERMINISTIC RETURN CONCAT('Hello, ', namef, '!')");
        createFunction("`testBug20504139``f`", "(namef CHAR(20)) RETURNS CHAR(50) DETERMINISTIC RETURN CONCAT('Hello, ', namef, '!')");
        createProcedure("testBug20504139p", "(INOUT namep CHAR(50)) SELECT  CONCAT('Hello, ', namep, '!') INTO namep");
        createProcedure("`testBug20504139``p`", "(INOUT namep CHAR(50)) SELECT  CONCAT('Hello, ', namep, '!') INTO namep");

        for (int testCase = 0; testCase < 8; testCase++) {// 3 props, 8 combinations: 2^3 = 8
            boolean usePedantic = (testCase & 1) == 1;
            boolean useInformationSchema = (testCase & 2) == 2;
            boolean useFuncsInProcs = (testCase & 4) == 4;

            String connProps = String.format("pedantic=%s,useInformationSchema=%s,getProceduresReturnsFunctions=%s", usePedantic, useInformationSchema,
                    useFuncsInProcs);
            System.out.printf("testBug20504139_%d: %s%n", testCase, connProps);

            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.pedantic.getKeyName(), "" + usePedantic);
            props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useInformationSchema);
            props.setProperty(PropertyKey.getProceduresReturnsFunctions.getKeyName(), "" + useFuncsInProcs);

            Connection testConn = getConnectionWithProps(props);
            DatabaseMetaData dbmd = testConn.getMetaData();
            boolean dbMapsToSchema = ((JdbcConnection) testConn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                    .getValue() == DatabaseTerm.SCHEMA;

            ResultSet testRs = null;

            try {
                /*
                 * test DatabaseMetadata.getProcedureColumns for function
                 */
                int i = 1;
                try {
                    for (String name : new String[] { "testBug20504139f", "testBug20504139`f" }) {
                        testRs = dbMapsToSchema ? dbmd.getProcedureColumns("", this.dbName, name, "%") : dbmd.getProcedureColumns(this.dbName, "", name, "%");

                        if (useFuncsInProcs) {
                            assertTrue(testRs.next());
                            assertEquals("", testRs.getString(4), testCase + "." + i + ". expected function column name (empty)");
                            assertEquals(DatabaseMetaData.procedureColumnReturn, testRs.getInt(5),
                                    testCase + "." + i + ". expected function column type (empty)");
                            assertTrue(testRs.next());
                            assertEquals("namef", testRs.getString(4), testCase + "." + i + ". expected function column name");
                            assertEquals(DatabaseMetaData.procedureColumnIn, testRs.getInt(5), testCase + "." + i + ". expected function column type (empty)");
                            assertFalse(testRs.next());
                        } else {
                            assertFalse(testRs.next());
                        }

                        testRs.close();
                        i++;
                    }
                } catch (SQLException e) {
                    assertFalse(e.getMessage().matches("FUNCTION `testBug20504139(:?`{2})?[fp]` does not exist"),
                            testCase + "." + i + ". failed to retrieve function columns, with getProcedureColumns(), from database meta data.");
                    throw e;
                }

                /*
                 * test DatabaseMetadata.getProcedureColumns for procedure
                 */
                i = 1;
                try {
                    for (String name : new String[] { "testBug20504139p", "testBug20504139`p" }) {
                        testRs = dbMapsToSchema ? dbmd.getProcedureColumns("", this.dbName, name, "%") : dbmd.getProcedureColumns(this.dbName, "", name, "%");

                        assertTrue(testRs.next());
                        assertEquals("namep", testRs.getString(4), testCase + ". expected procedure column name");
                        assertEquals(DatabaseMetaData.procedureColumnInOut, testRs.getInt(5), testCase + ". expected procedure column type (empty)");
                        assertFalse(testRs.next());

                        testRs.close();
                        i++;
                    }
                } catch (SQLException e) {
                    assertFalse(e.getMessage().matches("PROCEDURE `testBug20504139(:?`{2})?[fp]` does not exist"),
                            testCase + "." + i + ". failed to retrieve prodedure columns, with getProcedureColumns(), from database meta data.");
                    throw e;
                }

                /*
                 * test DatabaseMetadata.getFunctionColumns for function
                 */
                i = 1;
                try {
                    for (String name : new String[] { "testBug20504139f", "testBug20504139`f" }) {
                        testRs = dbMapsToSchema ? dbmd.getFunctionColumns("", this.dbName, name, "%") : dbmd.getFunctionColumns(this.dbName, "", name, "%");

                        assertTrue(testRs.next());
                        assertEquals("", testRs.getString(4), testCase + ". expected function column name (empty)");
                        assertEquals(DatabaseMetaData.functionReturn, testRs.getInt(5), testCase + ". expected function column type (empty)");
                        assertTrue(testRs.next());
                        assertEquals("namef", testRs.getString(4), testCase + ". expected function column name");
                        assertEquals(DatabaseMetaData.functionColumnIn, testRs.getInt(5), testCase + ". expected function column type (empty)");
                        assertFalse(testRs.next());

                        testRs.close();
                        i++;
                    }
                } catch (SQLException e) {
                    assertFalse(e.getMessage().matches("FUNCTION `testBug20504139(:?`{2})?[fp]` does not exist"),
                            testCase + "." + i + ". failed to retrieve function columns, with getFunctionColumns(), from database meta data.");
                    throw e;
                }

                /*
                 * test DatabaseMetadata.getFunctionColumns for procedure
                 */
                i = 1;
                try {
                    for (String name : new String[] { "testBug20504139p", "testBug20504139`p" }) {
                        testRs = dbMapsToSchema ? dbmd.getFunctionColumns("", this.dbName, name, "%") : dbmd.getFunctionColumns(this.dbName, "", name, "%");

                        assertFalse(testRs.next());

                        testRs.close();
                        i++;
                    }
                } catch (SQLException e) {
                    assertFalse(e.getMessage().matches("PROCEDURE `testBug20504139(:?`{2})?[fp]` does not exist"),
                            testCase + "." + i + ". failed to retrieve procedure columns, with getFunctionColumns(), from database meta data.");
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
     */
    @Test
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
            assertTrue(previousDb.compareTo(this.rs.getString(1)) < 0,
                    "'" + this.rs.getString(1) + "' is lexicographically lower than the previous catalog. Check the system output to see the catalogs list.");
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
     * @throws Exception
     */
    @Test
    public void testBug19803348() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.getProceduresReturnsFunctions.getKeyName(), "false");
        props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "false");

        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {
                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData dbmd = conn1.getMetaData();

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

                    this.rs = dbmd.getFunctions(null, null, "testBug19803348_%");
                    assertTrue(this.rs.next());
                    assertEquals(testDb1, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_f", this.rs.getString(3));
                    assertFalse(this.rs.next());

                    this.rs = dbmd.getFunctionColumns(null, null, "testBug19803348_%", "%");
                    assertTrue(this.rs.next());
                    assertEquals(testDb1, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_f", this.rs.getString(3));
                    assertEquals("", this.rs.getString(4));
                    assertTrue(this.rs.next());
                    assertEquals(testDb1, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_f", this.rs.getString(3));
                    assertEquals("d", this.rs.getString(4));
                    assertFalse(this.rs.next());

                    this.rs = dbmd.getProcedures(null, null, "testBug19803348_%");
                    assertTrue(this.rs.next());
                    assertEquals(testDb1, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_p", this.rs.getString(3));
                    assertFalse(this.rs.next());

                    this.rs = dbmd.getProcedureColumns(null, null, "testBug19803348_%", "%");
                    assertTrue(this.rs.next());
                    assertEquals(testDb1, this.rs.getString(dbMapsToSchema ? 2 : 1));
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

                    this.rs = dbmd.getFunctions(null, null, "testBug19803348_%");
                    assertTrue(this.rs.next());
                    assertEquals(testDb1, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_B_f", this.rs.getString(3));
                    assertTrue(this.rs.next());
                    assertEquals(testDb2, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_A_f", this.rs.getString(3));
                    assertFalse(this.rs.next());

                    this.rs = dbmd.getFunctionColumns(null, null, "testBug19803348_%", "%");
                    assertTrue(this.rs.next());
                    assertEquals(testDb1, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_B_f", this.rs.getString(3));
                    assertEquals("", this.rs.getString(4));
                    assertTrue(this.rs.next());
                    assertEquals(testDb1, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_B_f", this.rs.getString(3));
                    assertEquals("d", this.rs.getString(4));
                    assertTrue(this.rs.next());
                    assertEquals(testDb2, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_A_f", this.rs.getString(3));
                    assertEquals("", this.rs.getString(4));
                    assertTrue(this.rs.next());
                    assertEquals(testDb2, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_A_f", this.rs.getString(3));
                    assertEquals("d", this.rs.getString(4));
                    assertFalse(this.rs.next());

                    this.rs = dbmd.getProcedures(null, null, "testBug19803348_%");
                    assertTrue(this.rs.next());
                    assertEquals(testDb1, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_B_p", this.rs.getString(3));
                    assertTrue(this.rs.next());
                    assertEquals(testDb2, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_A_p", this.rs.getString(3));
                    assertFalse(this.rs.next());

                    this.rs = dbmd.getProcedureColumns(null, null, "testBug19803348_%", "%");
                    assertTrue(this.rs.next());
                    assertEquals(testDb1, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_B_p", this.rs.getString(3));
                    assertEquals("d", this.rs.getString(4));
                    assertTrue(this.rs.next());
                    assertEquals(testDb2, this.rs.getString(dbMapsToSchema ? 2 : 1));
                    assertEquals("testBug19803348_A_p", this.rs.getString(3));
                    assertEquals("d", this.rs.getString(4));
                    assertFalse(this.rs.next());

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    /**
     * Tests fix for BUG#20727196 - GETPROCEDURECOLUMNS() RETURNS EXCEPTION FOR FUNCTION WHICH RETURNS ENUM/SET TYPE.
     * 
     * @throws Exception
     */
    @Test
    public void testBug20727196() throws Exception {
        createFunction("testBug20727196_f1",
                "(p ENUM ('Yes', 'No')) RETURNS VARCHAR(10) DETERMINISTIC BEGIN RETURN IF(p='Yes', 'Yay!', if(p='No', 'Ney!', 'What?')); END");
        createFunction("testBug20727196_f2", "(p CHAR(1)) RETURNS ENUM ('Yes', 'No') DETERMINISTIC BEGIN RETURN IF(p='y', 'Yes', if(p='n', 'No', '?')); END");
        createFunction("testBug20727196_f3",
                "(p ENUM ('Yes', 'No')) RETURNS ENUM ('Yes', 'No') DETERMINISTIC BEGIN RETURN IF(p='Yes', 'Yes', if(p='No', 'No', '?')); END");
        createProcedure("testBug20727196_p1", "(p ENUM ('Yes', 'No')) BEGIN SELECT IF(p='Yes', 'Yay!', if(p='No', 'Ney!', 'What?')); END");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "true");
        props.setProperty(PropertyKey.getProceduresReturnsFunctions.getKeyName(), "false");
        for (boolean useInformationSchema : new boolean[] { false, true }) {
            props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useInformationSchema);

            Connection testConn = null;
            try {
                testConn = getConnectionWithProps(props);
                DatabaseMetaData dbmd = testConn.getMetaData();

                this.rs = dbmd.getFunctionColumns(null, null, "testBug20727196_%", "%");

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

                assertFalse(this.rs.next());

                this.rs = dbmd.getProcedureColumns(null, null, "testBug20727196_%", "%");

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
     */
    @Test
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
     */
    @Test
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
     * 
     * @throws Exception
     */
    @Test
    public void testBug23212347() throws Exception {
        boolean useSPS = false;
        do {
            String testCase = String.format("Case [SPS: %s]", useSPS ? "Y" : "N");
            createTable("testBug23212347", "(id INT)");

            Properties props = new Properties();
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), Boolean.toString(useSPS));
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

            Connection testConn = getConnectionWithProps(props);
            Statement testStmt = testConn.createStatement();
            testStmt.execute("INSERT INTO testBug23212347 VALUES (1)");

            this.pstmt = testConn.prepareStatement("SELECT * FROM testBug23212347 WHERE id = 1");
            this.rs = this.pstmt.executeQuery();
            assertTrue(this.rs.next(), testCase);
            assertEquals(1, this.rs.getInt(1), testCase);
            assertFalse(this.rs.next(), testCase);
            ResultSetMetaData rsmd = this.pstmt.getMetaData();
            assertEquals("id", rsmd.getColumnName(1), testCase);

            this.pstmt = testConn.prepareStatement("SELECT * FROM testBug23212347 WHERE id = ?");
            this.pstmt.setInt(1, 1);
            this.rs = this.pstmt.executeQuery();
            assertTrue(this.rs.next(), testCase);
            assertEquals(1, this.rs.getInt(1), testCase);
            assertFalse(this.rs.next());
            rsmd = this.pstmt.getMetaData();
            assertEquals("id", rsmd.getColumnName(1), testCase);
        } while (useSPS = !useSPS);
    }

    /**
     * Tests fix for Bug#73775 - DBMD.getProcedureColumns()/.getFunctionColumns() fail to filter by columnPattern
     * 
     * @throws Exception
     */
    @Test
    public void testBug73775() throws Exception {
        createFunction("testBug73775f", "(param1 CHAR(20), param2 CHAR(20)) RETURNS CHAR(40) DETERMINISTIC RETURN CONCAT(param1, param2)");
        createProcedure("testBug73775p", "(INOUT param1 CHAR(20), IN param2 CHAR(20)) BEGIN  SELECT CONCAT(param1, param2) INTO param1; END");

        boolean useIS = false;
        boolean inclFuncs = false;
        boolean dbMapsToSchema = false;
        do {
            final String testCase = String.format("Case: [useIS: %s, inclFuncs: %s, dbMapsToSchema: %s]", useIS ? "Y" : "N", inclFuncs ? "Y" : "N",
                    dbMapsToSchema ? "Y" : "N");

            final Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.useInformationSchema.getKeyName(), Boolean.toString(useIS));
            props.setProperty(PropertyKey.getProceduresReturnsFunctions.getKeyName(), Boolean.toString(inclFuncs));
            props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());
            final Connection testConn = getConnectionWithProps(props);
            final DatabaseMetaData dbmd = testConn.getMetaData();

            /*
             * Test getProcedureColumns()
             */
            this.rs = dbMapsToSchema ? dbmd.getProcedureColumns("", null, "testBug73775%", "%") : dbmd.getProcedureColumns(null, "", "testBug73775%", "%");
            if (inclFuncs) {
                assertTrue(this.rs.next(), testCase);
                assertEquals("testBug73775f", this.rs.getString(3), testCase);
                assertEquals("", this.rs.getString(4), testCase); // Function return param is always returned.
                assertEquals(DatabaseMetaData.procedureColumnReturn, this.rs.getInt(5));
                assertTrue(this.rs.next(), testCase);
                assertEquals("testBug73775f", this.rs.getString(3), testCase);
                assertEquals("param1", this.rs.getString(4), testCase);
                assertTrue(this.rs.next(), testCase);
                assertEquals("testBug73775f", this.rs.getString(3), testCase);
                assertEquals("param2", this.rs.getString(4), testCase);
            }
            assertTrue(this.rs.next(), testCase);
            assertEquals("testBug73775p", this.rs.getString(3), testCase);
            assertEquals("param1", this.rs.getString(4), testCase);
            assertTrue(this.rs.next(), testCase);
            assertEquals("testBug73775p", this.rs.getString(3), testCase);
            assertEquals("param2", this.rs.getString(4), testCase);
            assertFalse(this.rs.next(), testCase);

            for (String ptn : new String[] { "param1", "_____1", "%1", "p_r_m%1" }) {
                this.rs = dbMapsToSchema ? dbmd.getProcedureColumns("", null, "testBug73775%", ptn) : dbmd.getProcedureColumns(null, "", "testBug73775%", ptn);
                if (inclFuncs) {
                    assertTrue(this.rs.next());
                    assertEquals("testBug73775f", this.rs.getString(3), testCase);
                    assertEquals("", this.rs.getString(4), testCase); // Function return param is always returned.
                    assertEquals(DatabaseMetaData.procedureColumnReturn, this.rs.getInt(5));
                    assertTrue(this.rs.next(), testCase);
                    assertEquals("testBug73775f", this.rs.getString(3), testCase);
                    assertEquals("param1", this.rs.getString(4), testCase);
                }
                assertTrue(this.rs.next(), testCase);
                assertEquals("testBug73775p", this.rs.getString(3), testCase);
                assertEquals("param1", this.rs.getString(4), testCase);
                assertFalse(this.rs.next(), testCase);
            }

            for (String ptn : new String[] { "param2", "_____2", "%2", "p_r_m%2" }) {
                this.rs = dbMapsToSchema ? dbmd.getProcedureColumns("", null, "testBug73775%", ptn) : dbmd.getProcedureColumns(null, "", "testBug73775%", ptn);
                if (inclFuncs) {
                    assertTrue(this.rs.next());
                    assertEquals("testBug73775f", this.rs.getString(3), testCase);
                    assertEquals("", this.rs.getString(4), testCase); // Function return param is always returned.
                    assertEquals(DatabaseMetaData.procedureColumnReturn, this.rs.getInt(5));
                    assertTrue(this.rs.next(), testCase);
                    assertEquals("testBug73775f", this.rs.getString(3), testCase);
                    assertEquals("param2", this.rs.getString(4), testCase);
                }
                assertTrue(this.rs.next(), testCase);
                assertEquals("testBug73775p", this.rs.getString(3), testCase);
                assertEquals("param2", this.rs.getString(4), testCase);
                assertFalse(this.rs.next(), testCase);
            }

            this.rs = dbMapsToSchema ? dbmd.getProcedureColumns("", null, "testBug73775%", "") : dbmd.getProcedureColumns(null, "", "testBug73775%", "");
            if (inclFuncs) {
                assertTrue(this.rs.next(), testCase);
                assertEquals("testBug73775f", this.rs.getString(3), testCase);
                assertEquals("", this.rs.getString(4), testCase); // Function return param is always returned.
                assertEquals(DatabaseMetaData.procedureColumnReturn, this.rs.getInt(5), testCase);
            }
            assertFalse(this.rs.next(), testCase);

            /*
             * Test getFunctionColumns()
             */
            this.rs = dbMapsToSchema ? dbmd.getFunctionColumns("", null, "testBug73775%", "%") : dbmd.getFunctionColumns(null, "", "testBug73775%", "%");
            assertTrue(this.rs.next(), testCase);
            assertEquals("testBug73775f", this.rs.getString(3), testCase);
            assertEquals("", this.rs.getString(4), testCase); // Function return param is always returned.
            assertEquals(DatabaseMetaData.functionReturn, this.rs.getInt(5), testCase);
            assertTrue(this.rs.next(), testCase);
            assertEquals("testBug73775f", this.rs.getString(3), testCase);
            assertEquals("param1", this.rs.getString(4), testCase);
            assertTrue(this.rs.next(), testCase);
            assertEquals("testBug73775f", this.rs.getString(3), testCase);
            assertEquals("param2", this.rs.getString(4), testCase);
            assertFalse(this.rs.next(), testCase);

            for (String ptn : new String[] { "param1", "_____1", "%1", "p_r_m%1" }) {
                this.rs = dbMapsToSchema ? dbmd.getFunctionColumns("", null, "testBug73775%", ptn) : dbmd.getFunctionColumns(null, "", "testBug73775%", ptn);
                assertTrue(this.rs.next());
                assertEquals("testBug73775f", this.rs.getString(3), testCase);
                assertEquals("", this.rs.getString(4), testCase); // Function return param is always returned.
                assertEquals(DatabaseMetaData.functionReturn, this.rs.getInt(5), testCase);
                assertTrue(this.rs.next(), testCase);
                assertEquals("testBug73775f", this.rs.getString(3), testCase);
                assertEquals("param1", this.rs.getString(4), testCase);
                assertFalse(this.rs.next(), testCase);
            }

            for (String ptn : new String[] { "param2", "_____2", "%2", "p_r_m%2" }) {
                this.rs = dbMapsToSchema ? dbmd.getFunctionColumns("", null, "testBug73775%", ptn) : dbmd.getFunctionColumns(null, "", "testBug73775%", ptn);
                assertTrue(this.rs.next());
                assertEquals("testBug73775f", this.rs.getString(3), testCase);
                assertEquals("", this.rs.getString(4), testCase); // Function return param is always returned.
                assertEquals(DatabaseMetaData.functionReturn, this.rs.getInt(5), testCase);
                assertTrue(this.rs.next(), testCase);
                assertEquals("testBug73775f", this.rs.getString(3), testCase);
                assertEquals("param2", this.rs.getString(4), testCase);
                assertFalse(this.rs.next(), testCase);
            }

            this.rs = dbMapsToSchema ? dbmd.getFunctionColumns("", null, "testBug73775%", "") : dbmd.getFunctionColumns(null, "", "testBug73775%", "");
            assertTrue(this.rs.next(), testCase);
            assertEquals("testBug73775f", this.rs.getString(3), testCase);
            assertEquals("", this.rs.getString(4), testCase); // Function return param is always returned.
            assertEquals(DatabaseMetaData.functionReturn, this.rs.getInt(5), testCase);
            assertFalse(this.rs.next(), testCase);

            testConn.close();
        } while ((useIS = !useIS) || (inclFuncs = !inclFuncs) || (dbMapsToSchema = !dbMapsToSchema));
    }

    /**
     * Tests fix for BUG#87826 (26846249), MYSQL JDBC CONNECTOR/J DATABASEMETADATA NULL PATTERN HANDLING IS NON-COMPLIANT.
     * 
     * @throws Exception
     */
    @Test
    public void testBug87826() throws Exception {
        createTable("testBug87826", "(id INT)");
        createProcedure("bug87826", "(in param1 int, out result varchar(197)) BEGIN select 1, ''; END");

        final Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "true");

        for (String useIS : new String[] { "false", "true" }) {
            props.setProperty(PropertyKey.useInformationSchema.getKeyName(), useIS);
            Connection con = null;
            try {
                con = getConnectionWithProps(props);
                DatabaseMetaData md = con.getMetaData();

                ResultSet procMD = md.getProcedureColumns(null, null, "bug87826", null);
                assertTrue(procMD.next());
                assertEquals(10, procMD.getInt(9), "Int param length");
                assertTrue(procMD.next());
                assertEquals(197, procMD.getInt(9), "String param length");
                assertFalse(procMD.next());

                // at least one table should exist in current catalog
                this.rs = md.getTables(null, null, null, null);
                int cnt = 0;
                while (this.rs.next()) {
                    cnt++;
                }
                assertTrue(cnt > 0);

            } finally {
                if (con != null) {
                    con.close();
                }
            }
        }
    }

    /**
     * Tests fix for BUG#90887 (28034570), DATABASEMETADATAUSINGINFOSCHEMA#GETTABLES FAILS IF METHOD ARGUMENTS ARE NULL.
     * 
     * @throws Exception
     */
    @Test
    public void testBug90887() throws Exception {
        List<String> resNames = new ArrayList<>();

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "false");
        Connection con = getConnectionWithProps(props);
        DatabaseMetaData metaData = con.getMetaData();
        ResultSet res = metaData.getTables(null, null, null, null);
        while (res.next()) {
            resNames.add(res.getString("TABLE_NAME"));
        }

        assertTrue(resNames.size() > 0);

        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        con = getConnectionWithProps(props);
        metaData = con.getMetaData();
        res = metaData.getTables(null, null, null, null);
        while (res.next()) {
            resNames.remove(res.getString("TABLE_NAME"));
        }

        assertTrue(resNames.size() == 0);
    }

    /**
     * Tests fix for BUG#29186870, CONNECTOR/J REGRESSION: NOT RETURNING PRECISION GETPROCEDURECOLUMNS.
     * 
     * @throws Exception
     */
    @Test
    public void testBug29186870() throws Exception {
        Connection con = null;
        ResultSet rst = null;
        DatabaseMetaData dmd = null;

        Map<String, String> fieldToType = new HashMap<>();
        fieldToType.put("I", "INTEGER");
        fieldToType.put("D", "DATE");
        fieldToType.put("T", "TIME");
        fieldToType.put("DT", "DATETIME");
        fieldToType.put("TS", "TIMESTAMP");

        Map<String, Integer> fieldToExpectedLength = new HashMap<>();
        fieldToExpectedLength.put("I", 10);
        fieldToExpectedLength.put("D", 10);
        fieldToExpectedLength.put("T", 8);
        fieldToExpectedLength.put("DT", 19);
        fieldToExpectedLength.put("TS", 19);

        Map<String, Integer> fieldToExpectedPrecision = new HashMap<>();
        fieldToExpectedPrecision.put("I", 10);
        fieldToExpectedPrecision.put("D", 0);
        fieldToExpectedPrecision.put("T", 0);
        fieldToExpectedPrecision.put("DT", 0);
        fieldToExpectedPrecision.put("TS", 0);

        if (versionMeetsMinimum(5, 6, 4)) {
            // fractional seconds are not supported in previous versions
            fieldToType.put("T0", "TIME(0)");
            fieldToType.put("T3", "TIME(3)");
            fieldToType.put("T6", "TIME(6)");
            fieldToType.put("DT3", "DATETIME(3)");
            fieldToType.put("TS0", "TIMESTAMP(0)");
            fieldToType.put("TS6", "TIMESTAMP(6)");

            fieldToExpectedLength.put("T0", 8);
            fieldToExpectedLength.put("T3", 12);
            fieldToExpectedLength.put("T6", 15);
            fieldToExpectedLength.put("DT3", 23);
            fieldToExpectedLength.put("TS0", 19);
            fieldToExpectedLength.put("TS6", 26);

            fieldToExpectedPrecision.put("T0", 0);
            fieldToExpectedPrecision.put("T3", 3);
            fieldToExpectedPrecision.put("T6", 6);
            fieldToExpectedPrecision.put("DT3", 3);
            fieldToExpectedPrecision.put("TS0", 0);
            fieldToExpectedPrecision.put("TS6", 6);
        }

        String tname = "testBug29186870tab";
        String pname = "testBug29186870proc";
        String fname = "testBug29186870func";

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        try {
            for (String useIS : new String[] { "false", "true" }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), useIS);

                con = getConnectionWithProps(props);

                // 1. Test COLUMN_SIZE in getColumns()
                dmd = con.getMetaData();

                String str = "";
                for (String k : fieldToType.keySet()) {
                    if (str.length() > 0) {
                        str += ",";
                    }
                    str += k + " " + fieldToType.get(k);
                    if (fieldToType.get(k).startsWith("TIMESTAMP")) {
                        str += " NULL";
                    }

                }
                createTable(tname, "(" + str + ")");

                rst = dmd.getColumns(null, null, tname, null);

                assertNotNull(rst, "No records fetched");
                int cnt = 0;
                while (rst.next()) {
                    assertEquals(fieldToExpectedLength.get(rst.getString("COLUMN_NAME")), (Integer) rst.getInt("COLUMN_SIZE"));
                    cnt++;
                }
                assertEquals(fieldToType.size(), cnt);

                // 2. Test PRECISION and LENGTH in getProcedureColumns()
                str = "";
                for (String k : fieldToType.keySet()) {
                    if (str.length() > 0) {
                        str += ",";
                    }
                    str += "IN " + k + " " + fieldToType.get(k);
                }
                createProcedure(pname, "(" + str + ")" + "\n BEGIN\n SELECT I, DT, TS FROM TDT WHERE PDT = DT;" + "\n END");

                rst = dmd.getProcedureColumns(null, null, pname, null);

                assertNotNull(rst, "No records fetched");
                cnt = 0;
                while (rst.next()) {
                    String paramName = rst.getString("COLUMN_NAME");
                    assertEquals(fieldToExpectedPrecision.get(paramName), (Integer) rst.getInt("PRECISION"), paramName);
                    assertEquals(fieldToExpectedLength.get(paramName), (Integer) rst.getInt("LENGTH"), paramName);
                    cnt++;
                }
                assertEquals(fieldToType.size(), cnt);

                // 3. Test PRECISION and LENGTH in getFunctionColumns()
                str = "";
                for (String k : fieldToType.keySet()) {
                    if (str.length() > 0) {
                        str += ",";
                    }
                    str += k + " " + fieldToType.get(k);
                }
                createFunction(fname, "(" + str + ") RETURNS CHAR(1) DETERMINISTIC RETURN 'a'");

                rst = dmd.getFunctionColumns(null, null, fname, null);

                assertNotNull(rst, "No records fetched");
                cnt = 0;
                while (rst.next()) {
                    String paramName = rst.getString("COLUMN_NAME");
                    if (!StringUtils.isNullOrEmpty(paramName)) { // ignore the out parameter
                        assertEquals(fieldToExpectedPrecision.get(paramName), (Integer) rst.getInt("PRECISION"));
                        assertEquals(fieldToExpectedLength.get(paramName), (Integer) rst.getInt("LENGTH"));
                        cnt++;
                    }
                }
                assertEquals(fieldToType.size(), cnt);
            }
        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    /**
     * Tests fix for Bug#97413 (30477722), DATABASEMETADATA IS BROKEN AFTER SERVER WL#13528.
     * 
     * @throws Exception
     */
    @Test
    public void testBug97413() throws Exception {
        createTable("testBug97413",
                "(f1_1 TINYINT, f1_2 TINYINT UNSIGNED, f1_3 TINYINT(1), f1_4 TINYINT(1) UNSIGNED, f1_5 TINYINT(1) ZEROFILL, f1_6 TINYINT(1) UNSIGNED ZEROFILL,"
                        + " f2_1 SMALLINT, f2_2 SMALLINT UNSIGNED, f2_3 SMALLINT(1), f2_4 SMALLINT(1) UNSIGNED, f2_5 SMALLINT(1) ZEROFILL, f2_6 SMALLINT(1) UNSIGNED ZEROFILL,"
                        + " f3_1 MEDIUMINT, f3_2 MEDIUMINT UNSIGNED, f3_3 MEDIUMINT(1), f3_4 MEDIUMINT(1) UNSIGNED, f3_5 MEDIUMINT(1) ZEROFILL, f3_6 MEDIUMINT(1) UNSIGNED ZEROFILL,"
                        + " f4_1 INT, f4_2 INT UNSIGNED, f4_3 INT(1), f4_4 INT(1) UNSIGNED, f4_5 INT(1) ZEROFILL, f4_6 INT(1) UNSIGNED ZEROFILL,"
                        + " f5_1 BIGINT, f5_2 BIGINT UNSIGNED, f5_3 BIGINT(1), f5_4 BIGINT(1) UNSIGNED, f5_5 BIGINT(1) ZEROFILL, f5_6 BIGINT(1) UNSIGNED ZEROFILL,"
                        + " f6_1 FLOAT(7), f6_2 FLOAT(7) UNSIGNED, f6_3 FLOAT(10, 7), f6_4 FLOAT(10, 7) UNSIGNED, f6_5 FLOAT(10, 7) ZEROFILL, f6_6 FLOAT(10, 7) UNSIGNED ZEROFILL,"
                        + " f6_7 FLOAT(30), f6_8 FLOAT(30) UNSIGNED, f6_9 FLOAT(30) ZEROFILL, f6_10 FLOAT(30) UNSIGNED ZEROFILL,"
                        + " f7_1 REAL, f7_2 REAL UNSIGNED, f7_3 REAL(10, 7), f7_4 REAL(10, 7) UNSIGNED, f7_5 REAL(10, 7) ZEROFILL, f7_6 REAL(10, 7) UNSIGNED ZEROFILL,"
                        + " f8_1 DOUBLE, f8_2 DOUBLE UNSIGNED, f8_3 DOUBLE(12, 10), f8_4 DOUBLE(12, 10) UNSIGNED, f8_5 DOUBLE(12, 10) ZEROFILL, f8_6 DOUBLE(12, 10) UNSIGNED ZEROFILL"
                        + ")");

        int cnt1 = 0;
        int cnt2 = 0;
        int cnt3 = 0;
        int cnt4 = 0;
        if (versionMeetsMinimum(8, 0, 17)) {
            SQLWarning warn = this.stmt.getWarnings();
            assertNotNull(warn);
            while (warn != null) {
                if (warn.getMessage().startsWith("Integer display width is deprecated")) {
                    cnt1++;
                } else if (warn.getMessage().startsWith("Specifying number of digits for floating")) {
                    cnt2++;
                } else if (warn.getMessage().startsWith("The ZEROFILL attribute is deprecated")) {
                    cnt3++;
                } else if (warn.getMessage().startsWith("UNSIGNED for decimal and floating point data types is deprecated")) {
                    cnt4++;
                } else {
                    System.out.println(warn.getMessage());
                }
                warn = warn.getNextWarning();
            }
            assertEquals(20, cnt1);
            assertEquals(12, cnt2);
            assertEquals(18, cnt3);
            assertEquals(11, cnt4);
        }

        createProcedure("testBug97413p", "("
                + " INOUT f1_1 TINYINT, INOUT f1_2 TINYINT UNSIGNED, INOUT f1_3 TINYINT(1), INOUT f1_4 TINYINT(1) UNSIGNED, INOUT f1_5 TINYINT(1) ZEROFILL, INOUT f1_6 TINYINT(1) UNSIGNED ZEROFILL,"
                + " INOUT f2_1 SMALLINT, INOUT f2_2 SMALLINT UNSIGNED, INOUT f2_3 SMALLINT(1), INOUT f2_4 SMALLINT(1) UNSIGNED, INOUT f2_5 SMALLINT(1) ZEROFILL, INOUT f2_6 SMALLINT(1) UNSIGNED ZEROFILL,"
                + " INOUT f3_1 MEDIUMINT, INOUT f3_2 MEDIUMINT UNSIGNED, INOUT f3_3 MEDIUMINT(1), INOUT f3_4 MEDIUMINT(1) UNSIGNED, INOUT f3_5 MEDIUMINT(1) ZEROFILL, INOUT f3_6 MEDIUMINT(1) UNSIGNED ZEROFILL,"
                + " INOUT f4_1 INT, INOUT f4_2 INT UNSIGNED, INOUT f4_3 INT(1), INOUT f4_4 INT(1) UNSIGNED, INOUT f4_5 INT(1) ZEROFILL, INOUT f4_6 INT(1) UNSIGNED ZEROFILL,"
                + " INOUT f5_1 BIGINT, INOUT f5_2 BIGINT UNSIGNED, INOUT f5_3 BIGINT(1), INOUT f5_4 BIGINT(1) UNSIGNED, INOUT f5_5 BIGINT(1) ZEROFILL, INOUT f5_6 BIGINT(1) UNSIGNED ZEROFILL,"
                + " INOUT f6_1 FLOAT(7), INOUT f6_2 FLOAT(7) UNSIGNED, INOUT f6_3 FLOAT(10, 7), INOUT f6_4 FLOAT(10, 7) UNSIGNED, INOUT f6_5 FLOAT(10, 7) ZEROFILL, INOUT f6_6 FLOAT(10, 7) UNSIGNED ZEROFILL,"
                + " INOUT f6_7 FLOAT(30), INOUT f6_8 FLOAT(30) UNSIGNED, INOUT f6_9 FLOAT(30) ZEROFILL, INOUT f6_10 FLOAT(30) UNSIGNED ZEROFILL,"
                + " INOUT f7_1 REAL, INOUT f7_2 REAL UNSIGNED, INOUT f7_3 REAL(10, 7), INOUT f7_4 REAL(10, 7) UNSIGNED, INOUT f7_5 REAL(10, 7) ZEROFILL, INOUT f7_6 REAL(10, 7) UNSIGNED ZEROFILL,"
                + " INOUT f8_1 DOUBLE, INOUT f8_2 DOUBLE UNSIGNED, INOUT f8_3 DOUBLE(12, 10), INOUT f8_4 DOUBLE(12, 10) UNSIGNED, INOUT f8_5 DOUBLE(12, 10) ZEROFILL, INOUT f8_6 DOUBLE(12, 10) UNSIGNED ZEROFILL"
                + ") BEGIN SELECT CONCAT(f1_3, f1_4) INTO f1_1; END");

        createFunction("testBug97413f",
                "(f1_1 TINYINT, f1_2 TINYINT UNSIGNED, f1_3 TINYINT(1), f1_4 TINYINT(1) UNSIGNED, f1_5 TINYINT(1) ZEROFILL, f1_6 TINYINT(1) UNSIGNED ZEROFILL,"
                        + " f2_1 SMALLINT, f2_2 SMALLINT UNSIGNED, f2_3 SMALLINT(1), f2_4 SMALLINT(1) UNSIGNED, f2_5 SMALLINT(1) ZEROFILL, f2_6 SMALLINT(1) UNSIGNED ZEROFILL,"
                        + " f3_1 MEDIUMINT, f3_2 MEDIUMINT UNSIGNED, f3_3 MEDIUMINT(1), f3_4 MEDIUMINT(1) UNSIGNED, f3_5 MEDIUMINT(1) ZEROFILL, f3_6 MEDIUMINT(1) UNSIGNED ZEROFILL,"
                        + " f4_1 INT, f4_2 INT UNSIGNED, f4_3 INT(1), f4_4 INT(1) UNSIGNED, f4_5 INT(1) ZEROFILL, f4_6 INT(1) UNSIGNED ZEROFILL,"
                        + " f5_1 BIGINT, f5_2 BIGINT UNSIGNED, f5_3 BIGINT(1), f5_4 BIGINT(1) UNSIGNED, f5_5 BIGINT(1) ZEROFILL, f5_6 BIGINT(1) UNSIGNED ZEROFILL,"
                        + " f6_1 FLOAT(7), f6_2 FLOAT(7) UNSIGNED, f6_3 FLOAT(10, 7), f6_4 FLOAT(10, 7) UNSIGNED, f6_5 FLOAT(10, 7) ZEROFILL, f6_6 FLOAT(10, 7) UNSIGNED ZEROFILL,"
                        + " f6_7 FLOAT(30), f6_8 FLOAT(30) UNSIGNED, f6_9 FLOAT(30) ZEROFILL, f6_10 FLOAT(30) UNSIGNED ZEROFILL,"
                        + " f7_1 REAL, f7_2 REAL UNSIGNED, f7_3 REAL(10, 7), f7_4 REAL(10, 7) UNSIGNED, f7_5 REAL(10, 7) ZEROFILL, f7_6 REAL(10, 7) UNSIGNED ZEROFILL,"
                        + " f8_1 DOUBLE, f8_2 DOUBLE UNSIGNED, f8_3 DOUBLE(12, 10), f8_4 DOUBLE(12, 10) UNSIGNED, f8_5 DOUBLE(12, 10) ZEROFILL, f8_6 DOUBLE(12, 10) UNSIGNED ZEROFILL"
                        + ") RETURNS INT(6) DETERMINISTIC RETURN CONCAT(f1_1, f2_1)");

        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean useSSPS : new boolean[] { false, true }) {
                for (boolean tinyInt1isBit : new boolean[] { false, true }) {
                    for (boolean transformedBitIsBoolean : new boolean[] { false, true }) {
                        String errMsg = "useIS=" + useIS + ", useSSPS=" + useSSPS + ", tinyInt1isBit=" + tinyInt1isBit + ", transformedBitIsBoolean="
                                + transformedBitIsBoolean + "\n";
                        props.clear();
                        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
                        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
                        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                        props.setProperty(PropertyKey.tinyInt1isBit.getKeyName(), "" + tinyInt1isBit);
                        props.setProperty(PropertyKey.transformedBitIsBoolean.getKeyName(), "" + transformedBitIsBoolean);
                        Connection c1 = getConnectionWithProps(props);

                        /*
                         * DatabaseMetaData
                         */
                        ResultSet rs1 = c1.getMetaData().getColumns(c1.getCatalog(), null, "testBug97413", "f%");
                        new TestBug97413Columns(errMsg, tinyInt1isBit, transformedBitIsBoolean, rs1, "DATA_TYPE", "TYPE_NAME", null, "COLUMN_SIZE",
                                "DECIMAL_DIGITS").run();

                        /*
                         * ResultSetMetaData
                         */
                        this.rs = c1.createStatement().executeQuery("SELECT * FROM testBug97413");
                        ResultSetMetaData rm = this.rs.getMetaData();
                        new TestBug97413Columns(errMsg, tinyInt1isBit, transformedBitIsBoolean, rm).run();

                        /*
                         * Procedures
                         */
                        CallableStatement storedProc = c1
                                .prepareCall("{call testBug97413p(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                                        + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}");
                        ParameterMetaData pm = storedProc.getParameterMetaData();
                        new TestBug97413Columns(errMsg, tinyInt1isBit, transformedBitIsBoolean, pm).run();

                        rs1 = c1.getMetaData().getProcedureColumns(null, null, "testBug97413p", null);
                        new TestBug97413Columns(errMsg, tinyInt1isBit, transformedBitIsBoolean, rs1, "DATA_TYPE", "TYPE_NAME", "LENGTH", "PRECISION", "SCALE")
                                .run();

                        /*
                         * Functions
                         */
                        storedProc = c1.prepareCall("{call testBug97413f(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,"
                                + " ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}");
                        pm = storedProc.getParameterMetaData();
                        TestBug97413Columns testBug97413Columns = new TestBug97413Columns(errMsg, tinyInt1isBit, transformedBitIsBoolean, pm);
                        testBug97413Columns.testBug97413CheckMDColumn(Types.INTEGER, "INT", 0, 10, 0); // return type
                        testBug97413Columns.run();

                        rs1 = c1.getMetaData().getFunctionColumns(null, null, "testBug97413f", null);
                        testBug97413Columns = new TestBug97413Columns(errMsg, tinyInt1isBit, transformedBitIsBoolean, rs1, "DATA_TYPE", "TYPE_NAME", "LENGTH",
                                "PRECISION", "SCALE"); // return type
                        testBug97413Columns.testBug97413CheckMDColumn(Types.INTEGER, "INT", 10, 10, 0);
                        testBug97413Columns.run();

                        c1.close();
                    }
                }
            }
        }
    }

    private class TestBug97413Columns {
        String errMsg = null;
        boolean tinyInt1isBit = false;
        boolean transformedBitIsBoolean = false;
        ResultSet rset = null;
        ParameterMetaData pm = null;
        ResultSetMetaData rm = null;
        int id = 0;
        String tidField = null;
        String tnField = null;
        String widthField = null;
        String precisionField = null;
        String decimalField = null;

        TestBug97413Columns(String errMsg, boolean tinyInt1isBit, boolean transformedBitIsBoolean, ResultSet rset, String tidField, String tnField,
                String widthField, String precisionField, String decimalField) {
            this.errMsg = errMsg;
            this.tinyInt1isBit = tinyInt1isBit;
            this.transformedBitIsBoolean = transformedBitIsBoolean;
            this.rset = rset;
            this.tidField = tidField;
            this.tnField = tnField;
            this.widthField = widthField;
            this.precisionField = precisionField;
            this.decimalField = decimalField;
        }

        TestBug97413Columns(String errMsg, boolean tinyInt1isBit, boolean transformedBitIsBoolean, ParameterMetaData pm) {
            this.errMsg = errMsg;
            this.tinyInt1isBit = tinyInt1isBit;
            this.transformedBitIsBoolean = transformedBitIsBoolean;
            this.pm = pm;
        }

        TestBug97413Columns(String errMsg, boolean tinyInt1isBit, boolean transformedBitIsBoolean, ResultSetMetaData rm) {
            this.errMsg = errMsg;
            this.tinyInt1isBit = tinyInt1isBit;
            this.transformedBitIsBoolean = transformedBitIsBoolean;
            this.rm = rm;
        }

        void testBug97413CheckMDColumn(int dataType, String typeName, int width, int precision, int decimalDigits) throws Exception {
            if (this.rset != null) {
                assertTrue(this.rset.next());
                assertEquals(dataType, this.rset.getInt(this.tidField), this.errMsg + "Field name: " + this.rset.getString("COLUMN_NAME") + "\n");
                assertEquals(typeName, this.rset.getString(this.tnField), this.errMsg + "Field name: " + this.rset.getString("COLUMN_NAME") + "\n");
                if (this.widthField != null) {
                    assertEquals(width, this.rset.getInt(this.widthField), this.errMsg + "Field name: " + this.rset.getString("COLUMN_NAME") + "\n");
                }
                assertEquals(precision, this.rset.getInt(this.precisionField), this.errMsg + "Field name: " + this.rset.getString("COLUMN_NAME") + "\n");
                assertEquals(decimalDigits, this.rset.getInt(this.decimalField), this.errMsg + "Field name: " + this.rset.getString("COLUMN_NAME") + "\n");
            } else if (this.pm != null) {
                this.id++;
                assertEquals(dataType, this.pm.getParameterType(this.id), this.errMsg);
                assertEquals(typeName, this.pm.getParameterTypeName(this.id), this.errMsg);
                assertEquals(precision, this.pm.getPrecision(this.id), this.errMsg);
                assertEquals(decimalDigits, this.pm.getScale(this.id), this.errMsg);
            } else {
                this.id++;
                assertEquals(dataType, this.rm.getColumnType(this.id), this.errMsg);
                assertEquals(typeName, this.rm.getColumnTypeName(this.id), this.errMsg);
                assertEquals(precision, this.rm.getPrecision(this.id), this.errMsg);
                assertEquals(decimalDigits, this.rm.getScale(this.id), this.errMsg);
                assertEquals(precision, this.rm.getColumnDisplaySize(this.id), this.errMsg);
            }

        }

        void run() throws Exception {
            testBug97413CheckMDColumn(Types.TINYINT, "TINYINT", 3, 3, 0);
            testBug97413CheckMDColumn(Types.TINYINT, "TINYINT UNSIGNED", 3, 3, 0);

            testBug97413CheckMDColumn(this.tinyInt1isBit ? (this.transformedBitIsBoolean ? Types.BOOLEAN : Types.BIT) : Types.TINYINT,
                    this.tinyInt1isBit ? (this.transformedBitIsBoolean ? "BOOLEAN" : "BIT") : "TINYINT",
                    this.tinyInt1isBit ? (this.transformedBitIsBoolean ? 3 : 1) : 3, this.tinyInt1isBit ? (this.transformedBitIsBoolean ? 3 : 1) : 3, 0);
            testBug97413CheckMDColumn(Types.TINYINT, "TINYINT UNSIGNED", 3, 3, 0);
            testBug97413CheckMDColumn(Types.TINYINT, "TINYINT UNSIGNED", 3, 3, 0);
            testBug97413CheckMDColumn(Types.TINYINT, "TINYINT UNSIGNED", 3, 3, 0);

            testBug97413CheckMDColumn(Types.SMALLINT, "SMALLINT", 5, 5, 0);
            testBug97413CheckMDColumn(Types.SMALLINT, "SMALLINT UNSIGNED", 5, 5, 0);
            testBug97413CheckMDColumn(Types.SMALLINT, "SMALLINT", 5, 5, 0);
            testBug97413CheckMDColumn(Types.SMALLINT, "SMALLINT UNSIGNED", 5, 5, 0);
            testBug97413CheckMDColumn(Types.SMALLINT, "SMALLINT UNSIGNED", 5, 5, 0);
            testBug97413CheckMDColumn(Types.SMALLINT, "SMALLINT UNSIGNED", 5, 5, 0);

            testBug97413CheckMDColumn(Types.INTEGER, "MEDIUMINT", 7, 7, 0);
            testBug97413CheckMDColumn(Types.INTEGER, "MEDIUMINT UNSIGNED", 8, 8, 0);
            testBug97413CheckMDColumn(Types.INTEGER, "MEDIUMINT", 7, 7, 0);
            testBug97413CheckMDColumn(Types.INTEGER, "MEDIUMINT UNSIGNED", 8, 8, 0);
            testBug97413CheckMDColumn(Types.INTEGER, "MEDIUMINT UNSIGNED", 8, 8, 0);
            testBug97413CheckMDColumn(Types.INTEGER, "MEDIUMINT UNSIGNED", 8, 8, 0);

            testBug97413CheckMDColumn(Types.INTEGER, "INT", 10, 10, 0);
            testBug97413CheckMDColumn(Types.INTEGER, "INT UNSIGNED", 10, 10, 0);
            testBug97413CheckMDColumn(Types.INTEGER, "INT", 10, 10, 0);
            testBug97413CheckMDColumn(Types.INTEGER, "INT UNSIGNED", 10, 10, 0);
            testBug97413CheckMDColumn(Types.INTEGER, "INT UNSIGNED", 10, 10, 0);
            testBug97413CheckMDColumn(Types.INTEGER, "INT UNSIGNED", 10, 10, 0);

            testBug97413CheckMDColumn(Types.BIGINT, "BIGINT", 19, 19, 0);
            testBug97413CheckMDColumn(Types.BIGINT, "BIGINT UNSIGNED", 20, 20, 0);
            testBug97413CheckMDColumn(Types.BIGINT, "BIGINT", 19, 19, 0);
            testBug97413CheckMDColumn(Types.BIGINT, "BIGINT UNSIGNED", 20, 20, 0);
            testBug97413CheckMDColumn(Types.BIGINT, "BIGINT UNSIGNED", 20, 20, 0);
            testBug97413CheckMDColumn(Types.BIGINT, "BIGINT UNSIGNED", 20, 20, 0);

            testBug97413CheckMDColumn(Types.REAL, "FLOAT", 12, 12, 0);
            testBug97413CheckMDColumn(Types.REAL, "FLOAT UNSIGNED", 12, 12, 0);
            testBug97413CheckMDColumn(Types.REAL, "FLOAT", 10, 10, 7);
            testBug97413CheckMDColumn(Types.REAL, "FLOAT UNSIGNED", 10, 10, 7);
            testBug97413CheckMDColumn(Types.REAL, "FLOAT UNSIGNED", 10, 10, 7);
            testBug97413CheckMDColumn(Types.REAL, "FLOAT UNSIGNED", 10, 10, 7);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE", 22, 22, 0);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE UNSIGNED", 22, 22, 0);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE UNSIGNED", 22, 22, 0);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE UNSIGNED", 22, 22, 0);

            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE", 22, 22, 0);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE UNSIGNED", 22, 22, 0);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE", 10, 10, 7);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE UNSIGNED", 10, 10, 7);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE UNSIGNED", 10, 10, 7);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE UNSIGNED", 10, 10, 7);

            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE", 22, 22, 0);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE UNSIGNED", 22, 22, 0);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE", 12, 12, 10);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE UNSIGNED", 12, 12, 10);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE UNSIGNED", 12, 12, 10);
            testBug97413CheckMDColumn(Types.DOUBLE, "DOUBLE UNSIGNED", 12, 12, 10);
        }
    }

    /**
     * Tests fix for Bug#102076 (32329915), CONTRIBUTION: MYSQL JDBC DRIVER RESULTSET.GETLONG() THROWS NUMBEROUTOFRANGE.
     * 
     * @throws Exception
     */
    @Test
    public void testBug102076() throws Exception {
        createFunction("testBug102076f", "(x LONGBLOB, y LONGTEXT) RETURNS LONGBLOB DETERMINISTIC RETURN CONCAT(x, y)");
        createProcedure("testBug102076p", "(x LONGBLOB, y LONGTEXT)\nBEGIN\nSELECT 1;end\n");

        DatabaseMetaData dbmd = this.conn.getMetaData();

        this.rs = dbmd.getProcedureColumns(null, null, "testBug102076p", "%");
        assertTrue(this.rs.next());
        do {
            assertEquals(Integer.MAX_VALUE, this.rs.getLong(9)); // LENGTH
            assertEquals(Integer.MAX_VALUE, this.rs.getLong(17)); // CHAR_OCTET_LENGTH
        } while (this.rs.next());

        this.rs = dbmd.getFunctionColumns(null, null, "testBug102076f", "%");
        assertTrue(this.rs.next());
        do {
            assertEquals(Integer.MAX_VALUE, this.rs.getLong(9)); // LENGTH
            assertEquals(Integer.MAX_VALUE, this.rs.getLong(14)); // CHAR_OCTET_LENGTH
        } while (this.rs.next());

    }

    /**
     * Tests fix for Bug#95280 (29757140), DATABASEMETADATA.GETIMPORTEDKEYS RETURNS DOUBLE THE ROWS.
     * 
     * @throws Exception
     */
    @Test
    public void testBug95280() throws Exception {
        String databaseName1 = "dbBug95280_1";
        createDatabase(databaseName1);
        createTable(databaseName1 + ".table1",
                "(cat_id int not null auto_increment primary key, cat_name varchar(255) not null, cat_description text) ENGINE=InnoDB;");
        createTable(databaseName1 + ".table2",
                "(prd_id int not null auto_increment primary key, prd_name varchar(355) not null, prd_price decimal, cat_id int not null,"
                        + " FOREIGN KEY fk_cat(cat_id) REFERENCES table1(cat_id) ON UPDATE CASCADE ON DELETE RESTRICT) ENGINE=InnoDB;");

        String databaseName2 = "dbBug95280_2";
        createDatabase(databaseName2);
        createTable(databaseName2 + ".table1",
                "(cat_id int not null auto_increment primary key, cat_name varchar(255) not null, cat_description text) ENGINE=InnoDB;");
        createTable(databaseName2 + ".table2",
                "(prd_id int not null auto_increment primary key, prd_name varchar(355) not null, prd_price decimal, cat_id int not null,"
                        + " FOREIGN KEY fk_cat(cat_id) REFERENCES table1(cat_id) ON UPDATE CASCADE ON DELETE RESTRICT) ENGINE=InnoDB;");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        for (boolean useIS : new boolean[] { false, true }) {
            for (String databaseTerm : new String[] { "CATALOG", "SCHEMA" }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), databaseTerm);

                Connection con = getConnectionWithProps(props);
                DatabaseMetaData meta = con.getMetaData();

                this.rs = databaseTerm.contentEquals("SCHEMA") ? meta.getImportedKeys(null, databaseName1, "table2")
                        : meta.getImportedKeys(databaseName1, null, "table2");
                assertTrue(this.rs.next());
                assertEquals("table2", this.rs.getString("FKTABLE_NAME"));
                assertEquals("cat_id", this.rs.getString("FKCOLUMN_NAME"));
                assertEquals(1, this.rs.getInt("KEY_SEQ"));
                assertFalse(this.rs.next());
                con.close();
            }
        }
    }

    /**
     * Tests fix for Bug#104641 (33237255), DatabaseMetaData.getImportedKeys can return duplicated foreign keys.
     * 
     * @throws Exception
     */
    @Test
    public void testBug104641() throws Exception {
        String databaseName1 = "dbBug104641";
        createDatabase(databaseName1);
        createTable(databaseName1 + ".table1",
                "(`CREATED` datetime DEFAULT NULL,`ID` bigint NOT NULL AUTO_INCREMENT,`LRN_ID` bigint DEFAULT '0',`USERNAME` varchar(50) NOT NULL,"
                        + "PRIMARY KEY (`ID`),UNIQUE KEY `U_table1_LRN_ID` (`LRN_ID`),UNIQUE KEY `U_table1_USERNAME` (`USERNAME`) )");
        createTable(databaseName1 + ".table2",
                "(`AL_ID` varchar(50) DEFAULT NULL,`CREATED` datetime DEFAULT NULL,`ID` bigint NOT NULL AUTO_INCREMENT,`USER_ID` bigint DEFAULT NULL,"
                        + "PRIMARY KEY (`ID`),KEY `fk_table2_user_id` (`USER_ID`),KEY `index_al_id1` (`AL_ID`),"
                        + "CONSTRAINT `fk_table2_user_id` FOREIGN KEY (`USER_ID`) REFERENCES `table1` (`ID`) )");
        createTable(databaseName1 + ".table3",
                "(`AL_ID` varchar(50) DEFAULT NULL,`ID` bigint NOT NULL AUTO_INCREMENT,`USER_ID` bigint DEFAULT NULL,`LRN_ID` bigint DEFAULT '0',"
                        + "PRIMARY KEY (`ID`),KEY `fk_table3_LRN_ID` (`LRN_ID`),KEY `index_al_id2` (`AL_ID`),"
                        + "CONSTRAINT `fk_table3_LRN_ID` FOREIGN KEY `U_table1_LRN_ID` (`LRN_ID`) REFERENCES `table1` (`LRN_ID`) )");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        for (boolean useIS : new boolean[] { false, true }) {
            for (String databaseTerm : new String[] { "CATALOG", "SCHEMA" }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), databaseTerm);

                boolean dbTermIsSchema = databaseTerm.contentEquals("SCHEMA");

                String err = "useInformationSchema=" + useIS + ", databaseTerm=" + databaseTerm;
                Connection con = getConnectionWithProps(props);
                DatabaseMetaData meta = con.getMetaData();

                this.rs = dbTermIsSchema ? meta.getImportedKeys(null, databaseName1, "table2") : meta.getImportedKeys(databaseName1, null, "table2");
                assertTrue(this.rs.next(), err);
                assertEquals(dbTermIsSchema ? "def" : databaseName1, this.rs.getString("PKTABLE_CAT"), err);
                assertEquals(dbTermIsSchema ? databaseName1 : null, this.rs.getString("PKTABLE_SCHEM"), err);
                assertEquals(dbTermIsSchema ? "def" : databaseName1, this.rs.getString("FKTABLE_CAT"), err);
                assertEquals(dbTermIsSchema ? databaseName1 : null, this.rs.getString("FKTABLE_SCHEM"), err);
                assertEquals("table1", this.rs.getString("PKTABLE_NAME"), err);
                assertEquals("ID", this.rs.getString("PKCOLUMN_NAME"), err);
                assertEquals("table2", this.rs.getString("FKTABLE_NAME"), err);
                assertEquals("USER_ID", this.rs.getString("FKCOLUMN_NAME"), err);
                assertEquals(1, this.rs.getInt("KEY_SEQ"), err);
                assertEquals(1, this.rs.getInt("UPDATE_RULE"), err);
                assertEquals(1, this.rs.getInt("DELETE_RULE"), err);
                assertEquals("fk_table2_user_id", this.rs.getString("FK_NAME"), err);
                assertEquals(useIS ? "PRIMARY" : null, this.rs.getString("PK_NAME"), err);
                assertEquals(7, this.rs.getInt("DEFERRABILITY"), err);
                assertFalse(this.rs.next(), err);

                this.rs = dbTermIsSchema ? meta.getImportedKeys(null, databaseName1, "table3") : meta.getImportedKeys(databaseName1, null, "table3");
                assertTrue(this.rs.next(), err);
                assertEquals(dbTermIsSchema ? "def" : databaseName1, this.rs.getString("PKTABLE_CAT"), err);
                assertEquals(dbTermIsSchema ? databaseName1 : null, this.rs.getString("PKTABLE_SCHEM"), err);
                assertEquals(dbTermIsSchema ? "def" : databaseName1, this.rs.getString("FKTABLE_CAT"), err);
                assertEquals(dbTermIsSchema ? databaseName1 : null, this.rs.getString("FKTABLE_SCHEM"), err);
                assertEquals("table1", this.rs.getString("PKTABLE_NAME"), err);
                assertEquals("LRN_ID", this.rs.getString("PKCOLUMN_NAME"), err);
                assertEquals("table3", this.rs.getString("FKTABLE_NAME"), err);
                assertEquals("LRN_ID", this.rs.getString("FKCOLUMN_NAME"), err);
                assertEquals(1, this.rs.getInt("KEY_SEQ"), err);
                assertEquals(1, this.rs.getInt("UPDATE_RULE"), err);
                assertEquals(1, this.rs.getInt("DELETE_RULE"), err);
                assertEquals("fk_table3_LRN_ID", this.rs.getString("FK_NAME"), err);
                assertEquals(useIS ? "U_table1_LRN_ID" : null, this.rs.getString("PK_NAME"), err);
                assertEquals(7, this.rs.getInt("DEFERRABILITY"), err);
                assertFalse(this.rs.next(), err);

                con.close();
            }
        }
    }

    /**
     * Tests fix for Bug#33723611, getDefaultTransactionIsolation must return repeatable read.
     * 
     * @throws Exception
     */
    @Test
    public void testBug33723611() throws Exception {
        this.rs = this.stmt.executeQuery(versionMeetsMinimum(5, 7) ? "SELECT @@SESSION.transaction_isolation" : "SELECT @@SESSION.tx_isolation");
        assertTrue(this.rs.next());
        assertEquals("REPEATABLE-READ", this.rs.getString(1));

        assertEquals(Connection.TRANSACTION_REPEATABLE_READ, this.conn.getTransactionIsolation());

        DatabaseMetaData dbmd = this.conn.getMetaData();
        assertEquals(Connection.TRANSACTION_REPEATABLE_READ, dbmd.getDefaultTransactionIsolation());
    }

    /**
     * Tests fix for Bug#82084 (23743938), YEAR DATA TYPE RETURNS INCORRECT VALUE FOR JDBC GETCOLUMNTYPE().
     * 
     * @throws Exception
     */
    @Test
    public void testBug82084() throws Exception {
        createProcedure("testBug82084p", "(in param1 int, out result year) BEGIN select 1, ''; END");
        createTable("testBug82084", "(col_tiny SMALLINT, col_year year, col_date date)");
        this.stmt.executeUpdate("INSERT INTO testBug82084 VALUES(1,2006, '2006-01-01')");
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        for (boolean useIS : new Boolean[] { true, false }) {
            for (boolean yearIsDate : new Boolean[] { true, false }) {
                props.setProperty(PropertyKey.yearIsDateType.getKeyName(), "" + yearIsDate);
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                Connection con = getConnectionWithProps(props);

                DatabaseMetaData dbmd = con.getMetaData();
                this.rs = dbmd.getColumns(null, null, "testBug82084", "%year");
                assertTrue(this.rs.next());
                assertEquals(MysqlType.YEAR.getName(), this.rs.getString("TYPE_NAME"));
                assertEquals(yearIsDate ? Types.DATE : Types.SMALLINT, this.rs.getInt("DATA_TYPE"));

                this.rs = dbmd.getTypeInfo();
                while (this.rs.next()) {
                    if (this.rs.getString("TYPE_NAME").equals("YEAR")) {
                        assertEquals(yearIsDate ? Types.DATE : Types.SMALLINT, this.rs.getInt("DATA_TYPE"));
                        break;
                    }
                }

                ResultSet procMD = con.getMetaData().getProcedureColumns(null, null, "testBug82084p", "%result");
                assertTrue(procMD.next());
                assertEquals(MysqlType.YEAR.getName(), procMD.getString("TYPE_NAME"));
                assertEquals(yearIsDate ? Types.DATE : Types.SMALLINT, procMD.getInt("DATA_TYPE"));

                this.rs = con.createStatement().executeQuery("select * from testBug82084");
                ResultSetMetaData rsmd = this.rs.getMetaData();

                assertEquals(MysqlType.YEAR.getName(), rsmd.getColumnTypeName(2));
                if (yearIsDate) {
                    assertEquals(Types.DATE, rsmd.getColumnType(2));
                    assertEquals(java.sql.Date.class.getName(), rsmd.getColumnClassName(2));
                } else {
                    assertEquals(Types.SMALLINT, rsmd.getColumnType(2));
                    assertEquals(Short.class.getName(), rsmd.getColumnClassName(2));
                }

                con.close();
            }
        }
    }
}
