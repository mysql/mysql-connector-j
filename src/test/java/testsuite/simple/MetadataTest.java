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

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

import com.mysql.cj.Query;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.jdbc.DatabaseMetaDataUsingInfoSchema;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.util.StringUtils;

import testsuite.BaseQueryInterceptor;
import testsuite.BaseTestCase;

/**
 * Tests DatabaseMetaData methods.
 */
public class MetadataTest extends BaseTestCase {
    /**
     * Creates a new MetadataTest object.
     * 
     * @param name
     */
    public MetadataTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(MetadataTest.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testForeignKeys() throws SQLException {
        try {
            createTestTable();

            DatabaseMetaData dbmd = this.conn.getMetaData();
            this.rs = dbmd.getImportedKeys(null, null, "child");

            while (this.rs.next()) {
                String pkColumnName = this.rs.getString("PKCOLUMN_NAME");
                String fkColumnName = this.rs.getString("FKCOLUMN_NAME");
                assertTrue("Primary Key not returned correctly ('" + pkColumnName + "' != 'parent_id')", pkColumnName.equalsIgnoreCase("parent_id"));
                assertTrue("Foreign Key not returned correctly ('" + fkColumnName + "' != 'parent_id_fk')", fkColumnName.equalsIgnoreCase("parent_id_fk"));
            }

            this.rs.close();
            this.rs = dbmd.getExportedKeys(null, null, "parent");

            while (this.rs.next()) {
                String pkColumnName = this.rs.getString("PKCOLUMN_NAME");
                String fkColumnName = this.rs.getString("FKCOLUMN_NAME");
                String fkTableName = this.rs.getString("FKTABLE_NAME");
                assertTrue("Primary Key not returned correctly ('" + pkColumnName + "' != 'parent_id')", pkColumnName.equalsIgnoreCase("parent_id"));
                assertTrue("Foreign Key table not returned correctly for getExportedKeys ('" + fkTableName + "' != 'child')",
                        fkTableName.equalsIgnoreCase("child"));
                assertTrue("Foreign Key not returned correctly for getExportedKeys ('" + fkColumnName + "' != 'parent_id_fk')",
                        fkColumnName.equalsIgnoreCase("parent_id_fk"));
            }

            this.rs.close();

            this.rs = dbmd.getCrossReference(null, null, "cpd_foreign_3", null, null, "cpd_foreign_4");

            assertTrue(this.rs.next());

            String pkColumnName = this.rs.getString("PKCOLUMN_NAME");
            String pkTableName = this.rs.getString("PKTABLE_NAME");
            String fkColumnName = this.rs.getString("FKCOLUMN_NAME");
            String fkTableName = this.rs.getString("FKTABLE_NAME");
            String deleteAction = cascadeOptionToString(this.rs.getInt("DELETE_RULE"));
            String updateAction = cascadeOptionToString(this.rs.getInt("UPDATE_RULE"));

            assertEquals(pkColumnName, "cpd_foreign_1_id");
            assertEquals(pkTableName, "cpd_foreign_3");
            assertEquals(fkColumnName, "cpd_foreign_1_id");
            assertEquals(fkTableName, "cpd_foreign_4");
            assertEquals(updateAction, "CASCADE");

            // SHOW CREATE TABLE `cjtest_5_1`.`cpd_foreign_4` doesn't return ON DELETE rule while it was used in a table creation:
            //    CREATE TABLE cpd_foreign_4 (
            //                 cpd_foreign_1_id int(8) not null, cpd_foreign_2_id int(8) not null,
            //                 key(cpd_foreign_1_id), key(cpd_foreign_2_id),
            //                 primary key (cpd_foreign_1_id, cpd_foreign_2_id),
            //                 foreign key (cpd_foreign_1_id, cpd_foreign_2_id)
            //                 references cpd_foreign_3(cpd_foreign_1_id, cpd_foreign_2_id) ON DELETE RESTRICT ON UPDATE CASCADE
            //                 ) ENGINE = InnoDB
            // I_S returns a correct info, thus we have different results here 
            if (dbmd instanceof DatabaseMetaDataUsingInfoSchema) {
                assertEquals(deleteAction, "RESTRICT");
            } else {
                assertEquals(deleteAction, "NO ACTION");
            }

            this.rs.close();
            this.rs = null;
        } finally {
            if (this.rs != null) {
                this.rs.close();
                this.rs = null;
            }
            this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS parent");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_4");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_3");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_2");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_1");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS fktable2");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS fktable1");
        }

    }

    public void testGetPrimaryKeys() throws SQLException {
        try {
            createTable("multikey", "(d INT NOT NULL, b INT NOT NULL, a INT NOT NULL, c INT NOT NULL, PRIMARY KEY (d, b, a, c))");
            DatabaseMetaData dbmd = this.conn.getMetaData();
            this.rs = dbmd.getPrimaryKeys(this.conn.getCatalog(), "", "multikey");

            short[] keySeqs = new short[4];
            String[] columnNames = new String[4];
            int i = 0;

            while (this.rs.next()) {
                this.rs.getString("TABLE_NAME");
                columnNames[i] = this.rs.getString("COLUMN_NAME");

                this.rs.getString("PK_NAME");
                keySeqs[i] = this.rs.getShort("KEY_SEQ");
                i++;
            }

            if ((keySeqs[0] != 3) && (keySeqs[1] != 2) && (keySeqs[2] != 4) && (keySeqs[3] != 1)) {
                fail("Keys returned in wrong order");
            }
        } finally {
            if (this.rs != null) {
                try {
                    this.rs.close();
                } catch (SQLException sqlEx) {
                    /* ignore */
                }
            }
        }
    }

    private static String cascadeOptionToString(int option) {
        switch (option) {
            case DatabaseMetaData.importedKeyCascade:
                return "CASCADE";

            case DatabaseMetaData.importedKeySetNull:
                return "SET NULL";

            case DatabaseMetaData.importedKeyRestrict:
                return "RESTRICT";

            case DatabaseMetaData.importedKeyNoAction:
                return "NO ACTION";
        }

        return "SET DEFAULT";
    }

    private void createTestTable() throws SQLException {
        //Needed for previous runs that did not clean-up
        this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
        this.stmt.executeUpdate("DROP TABLE IF EXISTS parent");
        this.stmt.executeUpdate("DROP TABLE IF EXISTS multikey");
        this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_4");
        this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_3");
        this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_2");
        this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_1");
        this.stmt.executeUpdate("DROP TABLE IF EXISTS fktable2");
        this.stmt.executeUpdate("DROP TABLE IF EXISTS fktable1");

        createTable("parent", "(parent_id INT NOT NULL, PRIMARY KEY (parent_id))", "INNODB");
        createTable("child", "(child_id INT, parent_id_fk INT, INDEX par_ind (parent_id_fk), FOREIGN KEY (parent_id_fk) REFERENCES parent(parent_id)) ",
                "INNODB");

        // Test compound foreign keys
        try {
            createTable("cpd_foreign_1", "(id int(8) not null auto_increment primary key,name varchar(255) not null unique,key (id))", "InnoDB");
        } catch (SQLException sqlEx) {
            if (sqlEx.getMessage().indexOf("max key length") != -1) {
                createTable("cpd_foreign_1", "(id int(8) not null auto_increment primary key,name varchar(180) not null unique,key (id))", "InnoDB");
            }
        }

        createTable("cpd_foreign_2", "(id int(8) not null auto_increment primary key,key (id),name varchar(255)) ", "InnoDB");
        createTable("cpd_foreign_3",
                "(cpd_foreign_1_id int(8) not null,cpd_foreign_2_id int(8) not null,key(cpd_foreign_1_id),"
                        + "key(cpd_foreign_2_id),primary key (cpd_foreign_1_id, cpd_foreign_2_id),"
                        + "foreign key (cpd_foreign_1_id) references cpd_foreign_1(id),foreign key (cpd_foreign_2_id) references cpd_foreign_2(id)) ",
                "InnoDB");
        createTable("cpd_foreign_4",
                "(cpd_foreign_1_id int(8) not null,cpd_foreign_2_id int(8) not null,key(cpd_foreign_1_id),"
                        + "key(cpd_foreign_2_id),primary key (cpd_foreign_1_id, cpd_foreign_2_id),foreign key (cpd_foreign_1_id, cpd_foreign_2_id) "
                        + "references cpd_foreign_3(cpd_foreign_1_id, cpd_foreign_2_id) ON DELETE RESTRICT ON UPDATE CASCADE) ",
                "InnoDB");

        createTable("fktable1", "(TYPE_ID int not null, TYPE_DESC varchar(32), primary key(TYPE_ID))", "InnoDB");
        createTable("fktable2", "(KEY_ID int not null, COF_NAME varchar(32), PRICE float, TYPE_ID int, primary key(KEY_ID), "
                + "index(TYPE_ID), foreign key(TYPE_ID) references fktable1(TYPE_ID)) ", "InnoDB");
    }

    /**
     * Tests the implementation of metadata for views.
     * 
     * This test automatically detects whether or not the server it is running
     * against supports the creation of views.
     * 
     * @throws SQLException
     *             if the test fails.
     */
    public void testViewMetaData() throws SQLException {
        try {
            this.rs = this.conn.getMetaData().getTableTypes();

            while (this.rs.next()) {
                if ("VIEW".equalsIgnoreCase(this.rs.getString(1))) {

                    this.stmt.executeUpdate("DROP VIEW IF EXISTS vTestViewMetaData");
                    createTable("testViewMetaData", "(field1 INT)");
                    this.stmt.executeUpdate("CREATE VIEW vTestViewMetaData AS SELECT field1 FROM testViewMetaData");

                    ResultSet tablesRs = null;

                    try {
                        tablesRs = this.conn.getMetaData().getTables(this.conn.getCatalog(), null, "%ViewMetaData", new String[] { "TABLE", "VIEW" });
                        assertTrue(tablesRs.next());
                        assertTrue("testViewMetaData".equalsIgnoreCase(tablesRs.getString(3)));
                        assertTrue(tablesRs.next());
                        assertTrue("vTestViewMetaData".equalsIgnoreCase(tablesRs.getString(3)));

                    } finally {
                        if (tablesRs != null) {
                            tablesRs.close();
                        }
                    }

                    try {
                        tablesRs = this.conn.getMetaData().getTables(this.conn.getCatalog(), null, "%ViewMetaData", new String[] { "TABLE" });
                        assertTrue(tablesRs.next());
                        assertTrue("testViewMetaData".equalsIgnoreCase(tablesRs.getString(3)));
                        assertTrue(!tablesRs.next());
                    } finally {
                        if (tablesRs != null) {
                            tablesRs.close();
                        }
                    }
                    break;
                }
            }

        } finally {
            if (this.rs != null) {
                this.rs.close();
            }
            this.stmt.executeUpdate("DROP VIEW IF EXISTS vTestViewMetaData");
        }
    }

    /**
     * Tests detection of read-only fields.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testRSMDIsReadOnly() throws Exception {
        try {
            this.rs = this.stmt.executeQuery("SELECT 1");

            ResultSetMetaData rsmd = this.rs.getMetaData();

            assertTrue(rsmd.isReadOnly(1));

            try {
                createTable("testRSMDIsReadOnly", "(field1 INT)");
                this.stmt.executeUpdate("INSERT INTO testRSMDIsReadOnly VALUES (1)");

                this.rs = this.stmt.executeQuery("SELECT 1, field1 + 1, field1 FROM testRSMDIsReadOnly");
                rsmd = this.rs.getMetaData();

                assertTrue(rsmd.isReadOnly(1));
                assertTrue(rsmd.isReadOnly(2));
                assertTrue(!rsmd.isReadOnly(3));
            } finally {
            }
        } finally {
            if (this.rs != null) {
                this.rs.close();
            }
        }
    }

    public void testBitType() throws Exception {
        try {
            createTable("testBitType", "(field1 BIT, field2 BIT, field3 BIT)");
            this.stmt.executeUpdate("INSERT INTO testBitType VALUES (1, 0, NULL)");
            this.rs = this.stmt.executeQuery("SELECT field1, field2, field3 FROM testBitType");
            this.rs.next();

            assertTrue(((Boolean) this.rs.getObject(1)).booleanValue());
            assertTrue(!((Boolean) this.rs.getObject(2)).booleanValue());
            assertEquals(this.rs.getObject(3), null);

            System.out.println(this.rs.getObject(1) + ", " + this.rs.getObject(2) + ", " + this.rs.getObject(3));

            this.rs = this.conn.prepareStatement("SELECT field1, field2, field3 FROM testBitType").executeQuery();
            this.rs.next();

            assertTrue(((Boolean) this.rs.getObject(1)).booleanValue());
            assertTrue(!((Boolean) this.rs.getObject(2)).booleanValue());

            assertEquals(this.rs.getObject(3), null);
            byte[] asBytesTrue = this.rs.getBytes(1);
            byte[] asBytesFalse = this.rs.getBytes(2);
            byte[] asBytesNull = this.rs.getBytes(3);

            assertEquals(asBytesTrue[0], 1);
            assertEquals(asBytesFalse[0], 0);
            assertEquals(asBytesNull, null);

            createTable("testBitField", "(field1 BIT(9))");
            this.rs = this.stmt.executeQuery("SELECT field1 FROM testBitField");
            System.out.println(this.rs.getMetaData().getColumnClassName(1));
        } finally {
        }
    }

    public void testSupportsSelectForUpdate() throws Exception {
        boolean supportsForUpdate = this.conn.getMetaData().supportsSelectForUpdate();

        assertTrue(supportsForUpdate);
    }

    public void testTinyint1IsBit() throws Exception {
        String tableName = "testTinyint1IsBit";
        // Can't use 'BIT' or boolean
        createTable(tableName, "(field1 TINYINT(1))");
        this.stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (1)");

        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_tinyInt1isBit, "true");
        props.setProperty(PropertyDefinitions.PNAME_transformedBitIsBoolean, "true");
        Connection boolConn = getConnectionWithProps(props);

        this.rs = boolConn.createStatement().executeQuery("SELECT field1 FROM " + tableName);
        checkBitOrBooleanType(false);

        this.rs = boolConn.prepareStatement("SELECT field1 FROM " + tableName).executeQuery();
        checkBitOrBooleanType(false);

        this.rs = boolConn.getMetaData().getColumns(boolConn.getCatalog(), null, tableName, "field1");
        assertTrue(this.rs.next());

        assertEquals(Types.BOOLEAN, this.rs.getInt("DATA_TYPE"));

        assertEquals("BOOLEAN", this.rs.getString("TYPE_NAME"));

        props.clear();
        props.setProperty(PropertyDefinitions.PNAME_transformedBitIsBoolean, "false");
        props.setProperty(PropertyDefinitions.PNAME_tinyInt1isBit, "true");

        Connection bitConn = getConnectionWithProps(props);

        this.rs = bitConn.createStatement().executeQuery("SELECT field1 FROM " + tableName);
        checkBitOrBooleanType(true);

        this.rs = bitConn.prepareStatement("SELECT field1 FROM " + tableName).executeQuery();
        checkBitOrBooleanType(true);

        this.rs = bitConn.getMetaData().getColumns(boolConn.getCatalog(), null, tableName, "field1");
        assertTrue(this.rs.next());

        assertEquals(Types.BIT, this.rs.getInt("DATA_TYPE"));

        assertEquals("BIT", this.rs.getString("TYPE_NAME"));
    }

    private void checkBitOrBooleanType(boolean usingBit) throws SQLException {

        assertTrue(this.rs.next());
        assertEquals("java.lang.Boolean", this.rs.getObject(1).getClass().getName());
        if (!usingBit) {
            assertEquals(Types.BOOLEAN, this.rs.getMetaData().getColumnType(1));
        } else {
            assertEquals(Types.BIT, this.rs.getMetaData().getColumnType(1));
        }

        assertEquals("java.lang.Boolean", this.rs.getMetaData().getColumnClassName(1));
    }

    public void testResultSetMetaDataMethods() throws Exception {
        createTable("t1",
                "(c1 char(1) CHARACTER SET latin7 COLLATE latin7_general_cs, c2 char(10) CHARACTER SET latin7 COLLATE latin7_general_ci, g1 GEOMETRY)");

        this.rs = this.stmt.executeQuery("SELECT c1 as QQQ, c2, g1 FROM t1");

        assertThrows(SQLException.class, "Column index out of range.", new Callable<Void>() {
            @SuppressWarnings("synthetic-access")
            @Override
            public Void call() throws Exception {
                MetadataTest.this.rs.getMetaData().getColumnType(0);
                return null;
            }
        });
        assertThrows(SQLException.class, "Column index out of range.", new Callable<Void>() {
            @SuppressWarnings("synthetic-access")
            @Override
            public Void call() throws Exception {
                MetadataTest.this.rs.getMetaData().getColumnType(100);
                return null;
            }
        });

        assertEquals(Types.CHAR, this.rs.getMetaData().getColumnType(1));
        assertEquals("ISO-8859-13", ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(1));
        assertEquals("latin7", ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(1));
        assertEquals("QQQ", this.rs.getMetaData().getColumnLabel(1));
        assertEquals("c1", this.rs.getMetaData().getColumnName(1));
        assertTrue(this.rs.getMetaData().isCaseSensitive(1));
        assertFalse(this.rs.getMetaData().isCaseSensitive(2));
        assertTrue(this.rs.getMetaData().isCaseSensitive(3));
        assertFalse(this.rs.getMetaData().isCurrency(3));
        assertTrue(this.rs.getMetaData().isDefinitelyWritable(3));

        assertEquals(0, this.rs.getMetaData().getScale(1));

        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useOldAliasMetadataBehavior, "true");
        Connection con = getConnectionWithProps(props);

        this.rs = con.createStatement().executeQuery("SELECT c1 as QQQ, g1 FROM t1");
        assertEquals("QQQ", this.rs.getMetaData().getColumnLabel(1));
        assertEquals("QQQ", this.rs.getMetaData().getColumnName(1));

    }

    /**
     * Tests the implementation of Information Schema for primary keys.
     */
    public void testGetPrimaryKeysUsingInfoShcema() throws Exception {
        createTable("t1", "(c1 int(1) primary key)");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getPrimaryKeys(null, null, "t1");
            this.rs.next();
            assertEquals("t1", this.rs.getString("TABLE_NAME"));
            assertEquals("c1", this.rs.getString("COLUMN_NAME"));
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    /**
     * Tests the implementation of Information Schema for index info.
     */
    public void testGetIndexInfoUsingInfoSchema() throws Exception {
        createTable("t1", "(c1 int(1))");
        this.stmt.executeUpdate("CREATE INDEX index1 ON t1 (c1)");

        Connection conn1 = null;

        try {
            conn1 = getConnectionWithProps("useInformationSchema=true");
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getIndexInfo(conn1.getCatalog(), null, "t1", false, true);
            this.rs.next();
            assertEquals("t1", this.rs.getString("TABLE_NAME"));
            assertEquals("c1", this.rs.getString("COLUMN_NAME"));
            assertEquals("1", this.rs.getString("NON_UNIQUE"));
            assertEquals("index1", this.rs.getString("INDEX_NAME"));
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    /**
     * Tests the implementation of Information Schema for columns.
     */
    public void testGetColumnsUsingInfoSchema() throws Exception {
        createTable("t1", "(c1 char(1))");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        props.setProperty(PropertyDefinitions.PNAME_nullCatalogMeansCurrent, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getColumns(null, null, "t1", null);
            this.rs.next();
            assertEquals("t1", this.rs.getString("TABLE_NAME"));
            assertEquals("c1", this.rs.getString("COLUMN_NAME"));
            assertEquals("CHAR", this.rs.getString("TYPE_NAME"));
            assertEquals("1", this.rs.getString("COLUMN_SIZE"));
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    /**
     * Tests the implementation of Information Schema for tables.
     */
    public void testGetTablesUsingInfoSchema() throws Exception {
        createTable("`t1-1`", "(c1 char(1))");
        createTable("`t1-2`", "(c1 char(1))");
        createTable("`t2`", "(c1 char(1))");
        Set<String> tableNames = new HashSet<>();
        tableNames.add("t1-1");
        tableNames.add("t1-2");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            // pattern matching for table name
            this.rs = metaData.getTables(this.dbName, null, "t1-_", null);
            while (this.rs.next()) {
                assertTrue(tableNames.remove(this.rs.getString("TABLE_NAME")));
            }
            assertTrue(tableNames.isEmpty());
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    /**
     * Tests the implementation of Information Schema for column privileges.
     */
    public void testGetColumnPrivilegesUsingInfoSchema() throws Exception {

        if (!runTestIfSysPropDefined(PropertyDefinitions.SYSP_testsuite_cantGrant)) {
            Properties props = new Properties();

            props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
            props.setProperty(PropertyDefinitions.PNAME_nullCatalogMeansCurrent, "true");
            Connection conn1 = null;
            Statement stmt1 = null;
            String userHostQuoted = null;

            boolean grantFailed = true;

            try {
                conn1 = getConnectionWithProps(props);
                stmt1 = conn1.createStatement();
                createTable("t1", "(c1 int)");
                this.rs = stmt1.executeQuery("SELECT CURRENT_USER()");
                this.rs.next();
                String user = this.rs.getString(1);
                List<String> userHost = StringUtils.split(user, "@", false);
                if (userHost.size() < 2) {
                    fail("This test requires a JDBC URL with a user, and won't work with the anonymous user. "
                            + "You can skip this test by setting the system property " + PropertyDefinitions.SYSP_testsuite_cantGrant);
                }
                userHostQuoted = "'" + userHost.get(0) + "'@'" + userHost.get(1) + "'";

                try {
                    stmt1.executeUpdate("GRANT update (c1) on t1 to " + userHostQuoted);

                    grantFailed = false;

                } catch (SQLException sqlEx) {
                    fail("This testcase needs to be run with a URL that allows the user to issue GRANTs "
                            + " in the current database. You can skip this test by setting the system property \""
                            + PropertyDefinitions.SYSP_testsuite_cantGrant + "\".");
                }

                if (!grantFailed) {
                    DatabaseMetaData metaData = conn1.getMetaData();
                    this.rs = metaData.getColumnPrivileges(null, null, "t1", null);
                    this.rs.next();
                    assertEquals("t1", this.rs.getString("TABLE_NAME"));
                    assertEquals("c1", this.rs.getString("COLUMN_NAME"));
                    assertEquals(userHostQuoted, this.rs.getString("GRANTEE"));
                    assertEquals("UPDATE", this.rs.getString("PRIVILEGE"));
                }
            } finally {
                if (stmt1 != null) {

                    if (!grantFailed) {
                        stmt1.executeUpdate("REVOKE UPDATE (c1) ON t1 FROM " + userHostQuoted);
                    }

                    stmt1.close();
                }

                if (conn1 != null) {
                    conn1.close();
                }
            }
        }
    }

    /**
     * Tests the implementation of Information Schema for description
     * of stored procedures available in a catalog.
     */
    public void testGetProceduresUsingInfoSchema() throws Exception {
        createProcedure("sp1", "()\n BEGIN\nSELECT 1;end\n");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getProcedures(null, null, "sp1");
            this.rs.next();
            assertEquals("sp1", this.rs.getString("PROCEDURE_NAME"));
            assertEquals("1", this.rs.getString("PROCEDURE_TYPE"));
        } finally {
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    /**
     * Tests the implementation of Information Schema for foreign key.
     */
    public void testGetCrossReferenceUsingInfoSchema() throws Exception {
        this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
        this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
        this.stmt.executeUpdate("CREATE TABLE parent(id INT NOT NULL, PRIMARY KEY (id)) ENGINE=INNODB");
        this.stmt.executeUpdate(
                "CREATE TABLE child(id INT, parent_id INT, " + "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getCrossReference(null, null, "parent", null, null, "child");
            this.rs.next();
            assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
            assertEquals("id", this.rs.getString("PKCOLUMN_NAME"));
            assertEquals("child", this.rs.getString("FKTABLE_NAME"));
            assertEquals("parent_id", this.rs.getString("FKCOLUMN_NAME"));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
            this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    /**
     * Tests the implementation of Information Schema for foreign key.
     */
    public void testGetExportedKeysUsingInfoSchema() throws Exception {
        this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
        this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
        this.stmt.executeUpdate("CREATE TABLE parent(id INT NOT NULL, PRIMARY KEY (id)) ENGINE=INNODB");
        this.stmt.executeUpdate(
                "CREATE TABLE child(id INT, parent_id INT, " + "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getExportedKeys(null, null, "parent");
            this.rs.next();
            assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
            assertEquals("id", this.rs.getString("PKCOLUMN_NAME"));
            assertEquals("child", this.rs.getString("FKTABLE_NAME"));
            assertEquals("parent_id", this.rs.getString("FKCOLUMN_NAME"));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
            this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    /**
     * Tests the implementation of Information Schema for foreign key.
     */
    public void testGetImportedKeysUsingInfoSchema() throws Exception {
        this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
        this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
        this.stmt.executeUpdate("CREATE TABLE parent(id INT NOT NULL, PRIMARY KEY (id)) ENGINE=INNODB");
        this.stmt.executeUpdate(
                "CREATE TABLE child(id INT, parent_id INT, " + "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getImportedKeys(null, null, "child");
            this.rs.next();
            assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
            assertEquals("id", this.rs.getString("PKCOLUMN_NAME"));
            assertEquals("child", this.rs.getString("FKTABLE_NAME"));
            assertEquals("parent_id", this.rs.getString("FKCOLUMN_NAME"));
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
            this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    /**
     * WL#411 - Generated columns.
     * 
     * Test for new syntax and support in DatabaseMetaData.getColumns().
     * 
     * New syntax for CREATE TABLE, introduced in MySQL 5.7.6:
     * -col_name data_type [GENERATED ALWAYS] AS (expression) [VIRTUAL | STORED] [UNIQUE [KEY]] [COMMENT comment] [[NOT] NULL] [[PRIMARY] KEY]
     */
    public void testGeneratedColumns() throws Exception {
        if (!versionMeetsMinimum(5, 7, 6)) {
            return;
        }

        // Test GENERATED columns syntax.
        createTable("pythagorean_triple", "(side_a DOUBLE NULL, side_b DOUBLE NULL, "
                + "side_c_vir DOUBLE AS (SQRT(side_a * side_a + side_b * side_b)) VIRTUAL UNIQUE KEY COMMENT 'hypotenuse - virtual', "
                + "side_c_sto DOUBLE GENERATED ALWAYS AS (SQRT(POW(side_a, 2) + POW(side_b, 2))) STORED UNIQUE KEY COMMENT 'hypotenuse - stored' NOT NULL "
                + "PRIMARY KEY)");

        // Test data for generated columns.
        assertEquals(1, this.stmt.executeUpdate("INSERT INTO pythagorean_triple (side_a, side_b) VALUES (3, 4)"));
        this.rs = this.stmt.executeQuery("SELECT * FROM pythagorean_triple");
        assertTrue(this.rs.next());
        assertEquals(3d, this.rs.getDouble(1));
        assertEquals(4d, this.rs.getDouble(2));
        assertEquals(5d, this.rs.getDouble(3));
        assertEquals(5d, this.rs.getDouble(4));
        assertEquals(3d, this.rs.getDouble("side_a"));
        assertEquals(4d, this.rs.getDouble("side_b"));
        assertEquals(5d, this.rs.getDouble("side_c_sto"));
        assertEquals(5d, this.rs.getDouble("side_c_vir"));
        assertFalse(this.rs.next());

        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_nullCatalogMeansCurrent, "true");

        for (String useIS : new String[] { "false", "true" }) {
            Connection testConn = null;
            props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, useIS);

            testConn = getConnectionWithProps(props);
            DatabaseMetaData dbmd = testConn.getMetaData();

            String test = "Case [" + props.toString() + "]";

            // Test columns metadata.
            this.rs = dbmd.getColumns(null, null, "pythagorean_triple", "%");
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_a", this.rs.getString("COLUMN_NAME"));
            assertEquals(test, "YES", this.rs.getString("IS_NULLABLE"));
            assertEquals(test, "NO", this.rs.getString("IS_AUTOINCREMENT"));
            assertEquals(test, "NO", this.rs.getString("IS_GENERATEDCOLUMN"));
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_b", this.rs.getString("COLUMN_NAME"));
            assertEquals(test, "YES", this.rs.getString("IS_NULLABLE"));
            assertEquals(test, "NO", this.rs.getString("IS_AUTOINCREMENT"));
            assertEquals(test, "NO", this.rs.getString("IS_GENERATEDCOLUMN"));
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_c_vir", this.rs.getString("COLUMN_NAME"));
            assertEquals(test, "YES", this.rs.getString("IS_NULLABLE"));
            assertEquals(test, "NO", this.rs.getString("IS_AUTOINCREMENT"));
            assertEquals(test, "YES", this.rs.getString("IS_GENERATEDCOLUMN"));
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_c_sto", this.rs.getString("COLUMN_NAME"));
            assertEquals(test, "NO", this.rs.getString("IS_NULLABLE"));
            assertEquals(test, "NO", this.rs.getString("IS_AUTOINCREMENT"));
            assertEquals(test, "YES", this.rs.getString("IS_GENERATEDCOLUMN"));
            assertFalse(test, this.rs.next());

            // Test primary keys metadata.
            this.rs = dbmd.getPrimaryKeys(null, null, "pythagorean_triple");
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_c_sto", this.rs.getString("COLUMN_NAME"));
            assertEquals(test, "PRIMARY", this.rs.getString("PK_NAME"));
            assertFalse(test, this.rs.next());

            // Test indexes metadata.
            this.rs = dbmd.getIndexInfo(null, null, "pythagorean_triple", false, true);
            assertTrue(test, this.rs.next());
            assertEquals(test, "PRIMARY", this.rs.getString("INDEX_NAME"));
            assertEquals(test, "side_c_sto", this.rs.getString("COLUMN_NAME"));
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_c_sto", this.rs.getString("INDEX_NAME"));
            assertEquals(test, "side_c_sto", this.rs.getString("COLUMN_NAME"));
            assertTrue(test, this.rs.next());
            assertEquals(test, "side_c_vir", this.rs.getString("INDEX_NAME"));
            assertEquals(test, "side_c_vir", this.rs.getString("COLUMN_NAME"));
            assertFalse(test, this.rs.next());

            testConn.close();
        }
    }

    /**
     * Tests DatabaseMetaData.getSQLKeywords().
     * (Related to BUG#70701 - DatabaseMetaData.getSQLKeywords() doesn't match MySQL 5.6 reserved words)
     * 
     * This test checks the statically maintained keywords list.
     */
    public void testGetSqlKeywordsStatic() throws Exception {
        final String mysqlKeywords = "ACCESSIBLE,ADD,ANALYZE,ASC,BEFORE,CASCADE,CHANGE,CONTINUE,DATABASE,DATABASES,DAY_HOUR,DAY_MICROSECOND,DAY_MINUTE,"
                + "DAY_SECOND,DELAYED,DESC,DISTINCTROW,DIV,DUAL,ELSEIF,EMPTY,ENCLOSED,ESCAPED,EXIT,EXPLAIN,FIRST_VALUE,FLOAT4,FLOAT8,FORCE,FULLTEXT,GENERATED,"
                + "GROUPS,HIGH_PRIORITY,HOUR_MICROSECOND,HOUR_MINUTE,HOUR_SECOND,IF,IGNORE,INDEX,INFILE,INT1,INT2,INT3,INT4,INT8,IO_AFTER_GTIDS,"
                + "IO_BEFORE_GTIDS,ITERATE,JSON_TABLE,KEY,KEYS,KILL,LAG,LAST_VALUE,LEAD,LEAVE,LIMIT,LINEAR,LINES,LOAD,LOCK,LONG,LONGBLOB,LONGTEXT,LOOP,"
                + "LOW_PRIORITY,MASTER_BIND,MASTER_SSL_VERIFY_SERVER_CERT,MAXVALUE,MEDIUMBLOB,MEDIUMINT,MEDIUMTEXT,MIDDLEINT,MINUTE_MICROSECOND,MINUTE_SECOND,"
                + "NO_WRITE_TO_BINLOG,NTH_VALUE,NTILE,OPTIMIZE,OPTIMIZER_COSTS,OPTION,OPTIONALLY,OUTFILE,PERSIST,PERSIST_ONLY,PURGE,READ,READ_WRITE,REGEXP,"
                + "RENAME,REPEAT,REPLACE,REQUIRE,RESIGNAL,RESTRICT,RLIKE,SCHEMA,SCHEMAS,SECOND_MICROSECOND,SEPARATOR,SHOW,SIGNAL,SPATIAL,SQL_BIG_RESULT,"
                + "SQL_CALC_FOUND_ROWS,SQL_SMALL_RESULT,SSL,STARTING,STORED,STRAIGHT_JOIN,TERMINATED,TINYBLOB,TINYINT,TINYTEXT,UNDO,UNLOCK,UNSIGNED,USAGE,USE,"
                + "UTC_DATE,UTC_TIME,UTC_TIMESTAMP,VARBINARY,VARCHARACTER,VIRTUAL,WHILE,WRITE,XOR,YEAR_MONTH,ZEROFILL";

        if (!versionMeetsMinimum(8, 0, 11)) {
            Connection testConn = getConnectionWithProps("useInformationSchema=true");
            assertEquals("MySQL keywords don't match expected.", mysqlKeywords, testConn.getMetaData().getSQLKeywords());
            testConn.close();
        }

        Connection testConn = getConnectionWithProps("useInformationSchema=false"); // Required for MySQL 8.0.11 and above, otherwise returns dynamic keywords.
        assertEquals("MySQL keywords don't match expected.", mysqlKeywords, testConn.getMetaData().getSQLKeywords());
        testConn.close();
    }

    /**
     * Tests DatabaseMetaData.getSQLKeywords().
     * WL#10544, Update MySQL 8.0 keywords list.
     * 
     * This test checks the dynamically maintained keywords lists.
     */
    public void testGetSqlKeywordsDynamic() throws Exception {
        if (!versionMeetsMinimum(8, 0, 11)) {
            // Tested in testGetSqlKeywordsStatic();
            return;
        }

        /*
         * Setup test case.
         */
        // 1. Get list of SQL:2003 to exclude.
        Field dbmdSql2003Keywords = com.mysql.cj.jdbc.DatabaseMetaData.class.getDeclaredField("SQL2003_KEYWORDS");
        dbmdSql2003Keywords.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<String> sql2003ReservedWords = Collections.unmodifiableList((List<String>) dbmdSql2003Keywords.get(null));
        assertTrue("Failed to get field SQL2003_KEYWORDS from com.mysql.cj.jdbc.DatabaseMetaData",
                sql2003ReservedWords != null && !sql2003ReservedWords.isEmpty());

        // 2. Retrieve list of reserved words from server.
        final String keywordsQuery = "SELECT WORD FROM INFORMATION_SCHEMA.KEYWORDS WHERE RESERVED=1 ORDER BY WORD";
        List<String> mysqlReservedWords = new ArrayList<>();
        this.rs = this.stmt.executeQuery(keywordsQuery);
        while (this.rs.next()) {
            mysqlReservedWords.add(this.rs.getString(1));
        }
        assertTrue("Failed to retrieve reserved words from server.", !mysqlReservedWords.isEmpty());

        // 3. Find the difference mysqlReservedWords - sql2003ReservedWords and prepare the expected result.
        mysqlReservedWords.removeAll(sql2003ReservedWords);
        String expectedSqlKeywords = String.join(",", mysqlReservedWords);

        // Make sure the keywords cache is empty in DatabaseMetaDataUsingInfoSchema.
        Field dbmduisKeywordsCacheField = DatabaseMetaDataUsingInfoSchema.class.getDeclaredField("keywordsCache");
        dbmduisKeywordsCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<ServerVersion, String> dbmduisKeywordsCache = (Map<ServerVersion, String>) dbmduisKeywordsCacheField.get(null);
        assertNotNull("Failed to retrieve the field keywordsCache from com.mysql.cj.jdbc.DatabaseMetaDataUsingInfoSchema.", dbmduisKeywordsCache);
        dbmduisKeywordsCache.clear();
        assertTrue("Failed to clear the DatabaseMetaDataUsingInfoSchema keywords cache.", dbmduisKeywordsCache.isEmpty());

        /*
         * Check that keywords are retrieved from database and cached.
         */
        Properties props = new Properties();
        props.setProperty(PropertyDefinitions.PNAME_useInformationSchema, "true");
        props.setProperty(PropertyDefinitions.PNAME_queryInterceptors, TestGetSqlKeywordsDynamicQueryInterceptor.class.getName());

        // First call to DatabaseMetaData.getSQLKeywords() -> keywords are retrieved from database.
        Connection testConn = getConnectionWithProps(props);
        assertEquals("MySQL keywords don't match expected.", expectedSqlKeywords, testConn.getMetaData().getSQLKeywords());
        assertTrue("MySQL keywords weren't obtained from database.", TestGetSqlKeywordsDynamicQueryInterceptor.interceptedQueries.contains(keywordsQuery));
        assertTrue("Keywords for current server weren't properly cached.", dbmduisKeywordsCache.containsKey(((JdbcConnection) testConn).getServerVersion()));

        TestGetSqlKeywordsDynamicQueryInterceptor.interceptedQueries.clear();

        // Second call to DatabaseMetaData.getSQLKeywords(), using same connection -> keywords are retrieved from internal cache.
        assertEquals("MySQL keywords don't match expected.", expectedSqlKeywords, testConn.getMetaData().getSQLKeywords());
        assertFalse("MySQL keywords weren't obtained from cache.", TestGetSqlKeywordsDynamicQueryInterceptor.interceptedQueries.contains(keywordsQuery));
        assertTrue("Keywords for current server weren't properly cached.", dbmduisKeywordsCache.containsKey(((JdbcConnection) testConn).getServerVersion()));
        testConn.close();

        TestGetSqlKeywordsDynamicQueryInterceptor.interceptedQueries.clear();

        // Third call to DatabaseMetaData.getSQLKeywords(), using different connection -> keywords are retrieved from internal cache.
        testConn = getConnectionWithProps(props);
        assertEquals("MySQL keywords don't match expected.", expectedSqlKeywords, testConn.getMetaData().getSQLKeywords());
        assertFalse("MySQL keywords weren't obtained from cache.", TestGetSqlKeywordsDynamicQueryInterceptor.interceptedQueries.contains(keywordsQuery));
        assertTrue("Keywords for current server weren't properly cached.", dbmduisKeywordsCache.containsKey(((JdbcConnection) testConn).getServerVersion()));
        testConn.close();

        TestGetSqlKeywordsDynamicQueryInterceptor.interceptedQueries.clear();
    }

    public static class TestGetSqlKeywordsDynamicQueryInterceptor extends BaseQueryInterceptor {
        public static List<String> interceptedQueries = new ArrayList<>();

        @Override
        public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
            interceptedQueries.add(sql.get());
            return super.preProcess(sql, interceptedQuery);
        }
    }
}
