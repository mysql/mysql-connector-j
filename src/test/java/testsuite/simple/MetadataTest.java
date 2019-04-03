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
import com.mysql.cj.conf.PropertyDefinitions.DatabaseTerm;
import com.mysql.cj.conf.PropertyKey;
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

    public void testSupports() throws SQLException {
        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {
                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData dbmd = conn1.getMetaData();

                    assertEquals(dbMapsToSchema ? "CATALOG" : "database", dbmd.getCatalogTerm());
                    assertEquals(dbMapsToSchema ? "SCHEMA" : "", dbmd.getSchemaTerm());

                    assertEquals(!dbMapsToSchema, dbmd.supportsCatalogsInDataManipulation());
                    assertEquals(!dbMapsToSchema, dbmd.supportsCatalogsInIndexDefinitions());
                    assertEquals(!dbMapsToSchema, dbmd.supportsCatalogsInPrivilegeDefinitions());
                    assertEquals(!dbMapsToSchema, dbmd.supportsCatalogsInProcedureCalls());
                    assertEquals(!dbMapsToSchema, dbmd.supportsCatalogsInTableDefinitions());

                    assertEquals(dbMapsToSchema, dbmd.supportsSchemasInDataManipulation());
                    assertEquals(dbMapsToSchema, dbmd.supportsSchemasInIndexDefinitions());
                    assertEquals(dbMapsToSchema, dbmd.supportsSchemasInPrivilegeDefinitions());
                    assertEquals(dbMapsToSchema, dbmd.supportsSchemasInProcedureCalls());
                    assertEquals(dbMapsToSchema, dbmd.supportsSchemasInTableDefinitions());

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    public void testGetCatalogVsGetSchemas() throws SQLException {
        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                ResultSet rs1 = null;
                ResultSet rs2 = null;
                ResultSet rs3 = null;

                try {
                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData dbmd = conn1.getMetaData();

                    rs1 = dbmd.getSchemas();
                    rs2 = dbmd.getSchemas(this.dbName, this.dbName.substring(0, 3) + "%");
                    rs3 = dbmd.getCatalogs();

                    if (dbMapsToSchema) {
                        boolean found = false;
                        while (rs1.next()) {
                            assertEquals("def", rs1.getString("TABLE_CATALOG"));
                            if (this.dbName.equals(rs1.getString("TABLE_SCHEM"))) {
                                found = true;
                            }
                        }
                        assertTrue(found);

                        found = false;
                        while (rs2.next()) {
                            assertEquals("def", rs2.getString("TABLE_CATALOG"));
                            if (this.dbName.equals(rs2.getString("TABLE_SCHEM"))) {
                                found = true;
                            }
                        }
                        assertTrue(found);

                        assertFalse(rs3.next());
                    } else {
                        assertFalse(rs1.next());
                        assertFalse(rs2.next());

                        boolean found = false;
                        while (rs3.next()) {
                            if (this.dbName.equals(rs3.getString("TABLE_CAT"))) {
                                found = true;
                            }
                        }
                        assertTrue(found);
                    }

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    public void testForeignKeys() throws SQLException {
        String refDb = "test_cross_reference_db";
        try {
            //Needed for previous runs that did not clean-up
            this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS parent");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS multikey");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_4");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS " + refDb + ".cpd_foreign_3");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_2");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_1");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS fktable2");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS fktable1");

            createTable("parent", "(parent_id INT NOT NULL, PRIMARY KEY (parent_id))", "INNODB");
            createTable("child", "(child_id INT, parent_id_fk INT, INDEX par_ind (parent_id_fk), FOREIGN KEY (parent_id_fk) REFERENCES parent(parent_id)) ",
                    "INNODB");
            createDatabase(refDb);

            // Test compound foreign keys
            try {
                createTable("cpd_foreign_1", "(id int(8) not null auto_increment primary key,name varchar(255) not null unique,key (id))", "InnoDB");
            } catch (SQLException sqlEx) {
                if (sqlEx.getMessage().indexOf("max key length") != -1) {
                    createTable("cpd_foreign_1", "(id int(8) not null auto_increment primary key,name varchar(180) not null unique,key (id))", "InnoDB");
                }
            }

            createTable("cpd_foreign_2", "(id int(8) not null auto_increment primary key,key (id),name varchar(255)) ", "InnoDB");
            createTable(refDb + ".cpd_foreign_3",
                    "(cpd_foreign_1_id int(8) not null,cpd_foreign_2_id int(8) not null,key(cpd_foreign_1_id),"
                            + "key(cpd_foreign_2_id),primary key (cpd_foreign_1_id, cpd_foreign_2_id)," + "foreign key (cpd_foreign_1_id) references "
                            + this.dbName + ".cpd_foreign_1(id),foreign key (cpd_foreign_2_id) references " + this.dbName + ".cpd_foreign_2(id)) ",
                    "InnoDB");
            createTable("cpd_foreign_4",
                    "(cpd_foreign_1_id int(8) not null,cpd_foreign_2_id int(8) not null,key(cpd_foreign_1_id),"
                            + "key(cpd_foreign_2_id),primary key (cpd_foreign_1_id, cpd_foreign_2_id),foreign key (cpd_foreign_1_id, cpd_foreign_2_id) "
                            + "references " + refDb + ".cpd_foreign_3(cpd_foreign_1_id, cpd_foreign_2_id) ON DELETE RESTRICT ON UPDATE CASCADE) ",
                    "InnoDB");

            createTable("fktable1", "(TYPE_ID int not null, TYPE_DESC varchar(32), primary key(TYPE_ID))", "InnoDB");
            createTable("fktable2", "(KEY_ID int not null, COF_NAME varchar(32), PRICE float, TYPE_ID int, primary key(KEY_ID), "
                    + "index(TYPE_ID), foreign key(TYPE_ID) references fktable1(TYPE_ID)) ", "InnoDB");

            Properties props = new Properties();
            Connection conn1 = null;

            for (boolean useIS : new boolean[] { false, true }) {
                for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                    props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                    props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                    System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                    try {
                        conn1 = getConnectionWithProps(props);

                        String dbNamePattern = this.dbName.substring(0, this.dbName.length() - 1) + "%";
                        String refDbPattern = refDb.substring(0, refDb.length() - 1) + "%";

                        DatabaseMetaData dbmd = conn1.getMetaData();

                        if (dbMapsToSchema) {
                            this.rs = dbmd.getImportedKeys(null, this.dbName, "child");
                            assertTrue(this.rs.next());
                            this.rs = dbmd.getImportedKeys(null, dbNamePattern, "child");
                            assertFalse("Schema pattern " + dbNamePattern + " should not be recognized.", this.rs.next());
                        } else {
                            this.rs = dbmd.getImportedKeys(this.dbName, null, "child");
                            assertTrue(this.rs.next());
                            this.rs = dbmd.getImportedKeys(dbNamePattern, null, "child");
                            assertFalse("Catalog pattern " + dbNamePattern + " should not be recognized.", this.rs.next());
                        }

                        this.rs = dbmd.getImportedKeys(null, null, "child");

                        while (this.rs.next()) {
                            if (dbMapsToSchema) {
                                assertEquals("def", this.rs.getString("PKTABLE_CAT"));
                                assertEquals(this.dbName, this.rs.getString("PKTABLE_SCHEM"));
                                assertEquals("def", this.rs.getString("FKTABLE_CAT"));
                                assertEquals(this.dbName, this.rs.getString("FKTABLE_SCHEM"));
                            } else {
                                assertEquals(this.dbName, this.rs.getString("PKTABLE_CAT"));
                                assertNull(this.rs.getString("PKTABLE_SCHEM"));
                                assertEquals(this.dbName, this.rs.getString("FKTABLE_CAT"));
                                assertNull(this.rs.getString("FKTABLE_SCHEM"));
                            }
                            assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
                            assertEquals("parent_id", this.rs.getString("PKCOLUMN_NAME"));
                            assertEquals("child", this.rs.getString("FKTABLE_NAME"));
                            assertEquals("parent_id_fk", this.rs.getString("FKCOLUMN_NAME"));
                            assertEquals(1, this.rs.getShort("KEY_SEQ"));
                            assertEquals(DatabaseMetaData.importedKeyRestrict, this.rs.getShort("UPDATE_RULE"));
                            assertEquals(DatabaseMetaData.importedKeyRestrict, this.rs.getShort("DELETE_RULE"));
                            assertEquals("child_ibfk_1", this.rs.getString("FK_NAME"));
                            assertEquals(useIS ? "PRIMARY" : null, this.rs.getString("PK_NAME")); // PK_NAME is available only with I_S
                            assertEquals(DatabaseMetaData.importedKeyNotDeferrable, this.rs.getShort("DEFERRABILITY"));
                        }

                        this.rs.close();

                        if (dbMapsToSchema) {
                            this.rs = dbmd.getExportedKeys(null, this.dbName, "parent");
                            assertTrue(this.rs.next());
                            this.rs = dbmd.getExportedKeys(null, dbNamePattern, "parent");
                            assertFalse("Schema pattern " + dbNamePattern + " should not be recognized.", this.rs.next());
                        } else {
                            this.rs = dbmd.getExportedKeys(this.dbName, null, "parent");
                            assertTrue(this.rs.next());
                            this.rs = dbmd.getExportedKeys(dbNamePattern, null, "parent");
                            assertFalse("Catalog pattern " + dbNamePattern + " should not be recognized.", this.rs.next());
                        }

                        this.rs = dbmd.getExportedKeys(null, null, "parent");

                        while (this.rs.next()) {
                            if (dbMapsToSchema) {
                                assertEquals("def", this.rs.getString("PKTABLE_CAT"));
                                assertEquals(this.dbName, this.rs.getString("PKTABLE_SCHEM"));
                                assertEquals("def", this.rs.getString("FKTABLE_CAT"));
                                assertEquals(this.dbName, this.rs.getString("FKTABLE_SCHEM"));
                            } else {
                                assertEquals(this.dbName, this.rs.getString("PKTABLE_CAT"));
                                assertNull(this.rs.getString("PKTABLE_SCHEM"));
                                assertEquals(this.dbName, this.rs.getString("FKTABLE_CAT"));
                                assertNull(this.rs.getString("FKTABLE_SCHEM"));
                            }
                            assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
                            assertEquals("parent_id", this.rs.getString("PKCOLUMN_NAME"));

                            assertEquals("child", this.rs.getString("FKTABLE_NAME"));
                            assertEquals("parent_id_fk", this.rs.getString("FKCOLUMN_NAME"));

                            assertEquals(1, this.rs.getShort("KEY_SEQ"));
                            assertEquals(DatabaseMetaData.importedKeyRestrict, this.rs.getShort("UPDATE_RULE"));
                            assertEquals(DatabaseMetaData.importedKeyRestrict, this.rs.getShort("DELETE_RULE"));
                            assertEquals("child_ibfk_1", this.rs.getString("FK_NAME"));
                            assertEquals(useIS ? "PRIMARY" : null, this.rs.getString("PK_NAME")); // PK_NAME is available only with I_S
                            assertEquals(DatabaseMetaData.importedKeyNotDeferrable, this.rs.getShort("DEFERRABILITY"));
                        }

                        this.rs.close();

                        if (dbMapsToSchema) {
                            this.rs = dbmd.getCrossReference(null, refDb, "cpd_foreign_3", null, this.dbName, "cpd_foreign_4");
                            assertTrue(this.rs.next());
                            this.rs = dbmd.getCrossReference(null, refDbPattern, "cpd_foreign_3", null, dbNamePattern, "cpd_foreign_4");
                            assertFalse("Schema patterns " + refDbPattern + " and " + dbNamePattern + " should not be recognized.", this.rs.next());
                        } else {
                            this.rs = dbmd.getCrossReference(refDb, null, "cpd_foreign_3", this.dbName, null, "cpd_foreign_4");
                            assertTrue(this.rs.next());
                            this.rs = dbmd.getCrossReference(refDbPattern, null, "cpd_foreign_3", dbNamePattern, null, "cpd_foreign_4");
                            assertFalse("Catalog patterns " + refDbPattern + " and " + dbNamePattern + " should not be recognized.", this.rs.next());
                        }

                        this.rs = dbmd.getCrossReference(null, null, "cpd_foreign_3", null, null, "cpd_foreign_4");

                        assertTrue(this.rs.next());
                        if (dbMapsToSchema) {
                            assertEquals("def", this.rs.getString("PKTABLE_CAT"));
                            assertEquals(refDb, this.rs.getString("PKTABLE_SCHEM"));
                            assertEquals("def", this.rs.getString("FKTABLE_CAT"));
                            assertEquals(this.dbName, this.rs.getString("FKTABLE_SCHEM"));
                        } else {
                            assertEquals(refDb, this.rs.getString("PKTABLE_CAT"));
                            assertNull(this.rs.getString("PKTABLE_SCHEM"));
                            assertEquals(this.dbName, this.rs.getString("FKTABLE_CAT"));
                            assertNull(this.rs.getString("FKTABLE_SCHEM"));
                        }
                        assertEquals("cpd_foreign_3", this.rs.getString("PKTABLE_NAME"));
                        assertEquals("cpd_foreign_1_id", this.rs.getString("PKCOLUMN_NAME"));
                        assertEquals("cpd_foreign_4", this.rs.getString("FKTABLE_NAME"));
                        assertEquals("cpd_foreign_1_id", this.rs.getString("FKCOLUMN_NAME"));
                        assertEquals(1, this.rs.getInt("KEY_SEQ"));
                        assertEquals(DatabaseMetaData.importedKeyCascade, this.rs.getInt("UPDATE_RULE"));
                        assertEquals(DatabaseMetaData.importedKeyRestrict, this.rs.getInt("DELETE_RULE"));
                        assertEquals("cpd_foreign_4_ibfk_1", this.rs.getString("FK_NAME"));
                        assertEquals(useIS ? "PRIMARY" : null, this.rs.getString("PK_NAME")); // PK_NAME is available only with I_S
                        assertEquals(DatabaseMetaData.importedKeyNotDeferrable, this.rs.getInt("DEFERRABILITY"));

                        assertTrue(this.rs.next());
                        if (dbMapsToSchema) {
                            assertEquals("def", this.rs.getString("PKTABLE_CAT"));
                            assertEquals(refDb, this.rs.getString("PKTABLE_SCHEM"));
                            assertEquals("def", this.rs.getString("FKTABLE_CAT"));
                            assertEquals(this.dbName, this.rs.getString("FKTABLE_SCHEM"));
                        } else {
                            assertEquals(refDb, this.rs.getString("PKTABLE_CAT"));
                            assertNull(this.rs.getString("PKTABLE_SCHEM"));
                            assertEquals(this.dbName, this.rs.getString("FKTABLE_CAT"));
                            assertNull(this.rs.getString("FKTABLE_SCHEM"));
                        }
                        assertEquals("cpd_foreign_3", this.rs.getString("PKTABLE_NAME"));
                        assertEquals("cpd_foreign_2_id", this.rs.getString("PKCOLUMN_NAME"));
                        assertEquals("cpd_foreign_4", this.rs.getString("FKTABLE_NAME"));
                        assertEquals("cpd_foreign_2_id", this.rs.getString("FKCOLUMN_NAME"));
                        assertEquals(2, this.rs.getInt("KEY_SEQ"));
                        assertEquals(DatabaseMetaData.importedKeyCascade, this.rs.getInt("UPDATE_RULE"));
                        assertEquals(DatabaseMetaData.importedKeyRestrict, this.rs.getInt("DELETE_RULE"));
                        assertEquals("cpd_foreign_4_ibfk_1", this.rs.getString("FK_NAME"));
                        assertEquals(useIS ? "PRIMARY" : null, this.rs.getString("PK_NAME")); // PK_NAME is available only with I_S
                        assertEquals(DatabaseMetaData.importedKeyNotDeferrable, this.rs.getInt("DEFERRABILITY"));

                        assertFalse(this.rs.next());

                        this.rs.close();
                        this.rs = null;

                    } finally {
                        if (conn1 != null) {
                            conn1.close();
                        }
                    }
                }
            }

        } finally {
            if (this.rs != null) {
                this.rs.close();
                this.rs = null;
            }
            this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS parent");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_4");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS " + refDb + ".cpd_foreign_3");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_2");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS cpd_foreign_1");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS fktable2");
            this.stmt.executeUpdate("DROP TABLE IF EXISTS fktable1");
        }

    }

    public void testGetPrimaryKeys() throws SQLException {
        createTable("multikey", "(d INT NOT NULL, b INT NOT NULL, a INT NOT NULL, c INT NOT NULL, PRIMARY KEY (d, b, a, c))");

        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {
                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData dbmd = conn1.getMetaData();

                    if (dbMapsToSchema) {
                        String dbPattern = conn1.getSchema().substring(0, conn1.getSchema().length() - 1) + "%";
                        this.rs = dbmd.getPrimaryKeys("", dbPattern, "multikey"); //metaData.getIndexInfo(null, dbPattern, "t1", false, true);
                        assertFalse("Schema pattern " + dbPattern + " should not be recognized.", this.rs.next());
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = dbmd.getPrimaryKeys(dbPattern, null, "multikey"); //metaData.getIndexInfo(dbPattern, null, "t1", false, true);
                        assertFalse("Catalog pattern " + dbPattern + " should not be recognized.", this.rs.next());
                    }

                    this.rs = dbMapsToSchema ? dbmd.getPrimaryKeys("", conn1.getSchema(), "multikey") : dbmd.getPrimaryKeys(conn1.getCatalog(), "", "multikey");

                    short[] keySeqs = new short[4];
                    String[] columnNames = new String[4];
                    int i = 0;

                    while (this.rs.next()) {
                        if (dbMapsToSchema) {
                            assertEquals("def", this.rs.getString("TABLE_CAT"));
                            assertEquals(this.dbName, this.rs.getString("TABLE_SCHEM"));
                        } else {
                            assertEquals(this.dbName, this.rs.getString("TABLE_CAT"));
                            assertNull(this.rs.getString("TABLE_SCHEM"));
                        }
                        this.rs.getString("TABLE_NAME");
                        columnNames[i] = this.rs.getString("COLUMN_NAME");
                        keySeqs[i++] = this.rs.getShort("KEY_SEQ");
                        this.rs.getString("PK_NAME");
                    }

                    if ((keySeqs[0] != 3) && (keySeqs[1] != 2) && (keySeqs[2] != 4) && (keySeqs[3] != 1)) {
                        fail("Keys returned in wrong order");
                    }

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
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
        props.setProperty(PropertyKey.tinyInt1isBit.getKeyName(), "true");
        props.setProperty(PropertyKey.transformedBitIsBoolean.getKeyName(), "true");
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
        props.setProperty(PropertyKey.transformedBitIsBoolean.getKeyName(), "false");
        props.setProperty(PropertyKey.tinyInt1isBit.getKeyName(), "true");

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
        props.setProperty(PropertyKey.useOldAliasMetadataBehavior.getKeyName(), "true");
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
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
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

    public void testGetIndexInfo() throws Exception {
        createTable("t1", "(c1 int(1))");
        this.stmt.executeUpdate("CREATE INDEX index1 ON t1 (c1)");

        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {
                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData metaData = conn1.getMetaData();

                    if (dbMapsToSchema) {
                        String dbPattern = conn1.getSchema().substring(0, conn1.getSchema().length() - 1) + "%";
                        this.rs = metaData.getIndexInfo(null, dbPattern, "t1", false, true);
                        assertFalse("Schema pattern " + dbPattern + " should not be recognized.", this.rs.next());
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getIndexInfo(dbPattern, null, "t1", false, true);
                        assertFalse("Catalog pattern " + dbPattern + " should not be recognized.", this.rs.next());
                    }

                    this.rs = dbMapsToSchema ? metaData.getIndexInfo(null, conn1.getCatalog(), "t1", false, true)
                            : metaData.getIndexInfo(conn1.getCatalog(), null, "t1", false, true);
                    this.rs.next();
                    if (dbMapsToSchema) {
                        assertEquals("def", this.rs.getString("TABLE_CAT"));
                        assertEquals(this.dbName, this.rs.getString("TABLE_SCHEM"));
                    } else {
                        assertEquals(this.dbName, this.rs.getString("TABLE_CAT"));
                        assertNull(this.rs.getString("TABLE_SCHEM"));
                    }
                    assertEquals("t1", this.rs.getString("TABLE_NAME"));
                    assertTrue(this.rs.getBoolean("NON_UNIQUE"));
                    assertNull(this.rs.getString("INDEX_QUALIFIER"));
                    assertEquals("index1", this.rs.getString("INDEX_NAME"));
                    assertEquals(DatabaseMetaData.tableIndexOther, this.rs.getShort("TYPE"));
                    assertEquals(1, this.rs.getShort("ORDINAL_POSITION"));
                    assertEquals("c1", this.rs.getString("COLUMN_NAME"));
                    assertEquals("A", this.rs.getString("ASC_OR_DESC"));
                    assertEquals(0, this.rs.getLong("CARDINALITY"));
                    assertEquals(0, this.rs.getLong("PAGES"));
                    assertNull(this.rs.getString("FILTER_CONDITION"));

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }

    }

    /**
     * Tests the implementation of getColumns.
     */
    public void testGetColumns() throws Exception {
        createTable("t1", "(c1 char(1))");
        Properties props = new Properties();
        props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "true");

        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());
                Connection conn1 = null;
                try {
                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData metaData = conn1.getMetaData();
                    this.rs = metaData.getColumns(null, null, "t1", null);
                    this.rs.next();

                    if (dbMapsToSchema) {
                        assertEquals("def", this.rs.getString("TABLE_CAT"));
                        assertEquals(this.dbName, this.rs.getString("TABLE_SCHEM"));
                    } else {
                        assertEquals(this.dbName, this.rs.getString("TABLE_CAT"));
                        assertNull(this.rs.getString("TABLE_SCHEM"));
                    }

                    assertEquals("t1", this.rs.getString("TABLE_NAME"));
                    assertEquals("c1", this.rs.getString("COLUMN_NAME"));
                    assertEquals("CHAR", this.rs.getString("TYPE_NAME"));
                    assertEquals("1", this.rs.getString("COLUMN_SIZE"));

                    if (dbMapsToSchema) {
                        String dbPattern = conn1.getSchema().substring(0, conn1.getSchema().length() - 1) + "%";
                        this.rs = metaData.getColumns(null, dbPattern, "t1", null);
                        assertTrue("Schema pattern " + dbPattern + " should be recognized.", this.rs.next());
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getColumns(dbPattern, null, "t1", null);
                        assertFalse("Catalog pattern " + dbPattern + " should not be recognized.", this.rs.next());
                    }

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
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
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
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

    public void testGetTables() throws Exception {
        createTable("`t1-1`", "(c1 char(1)) COMMENT 'table1'");
        createTable("`t1-2`", "(c1 char(1))");
        createTable("`t2`", "(c1 char(1))");

        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {

                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData metaData = conn1.getMetaData();

                    this.rs = metaData.getTables(null, null, "t1-_", null);
                    testGetTables_checkResult(useIS, dbMapsToSchema);

                    this.rs = metaData.getTables(null, this.dbName.substring(0, 3) + "%", "t1-_", null);
                    testGetTables_checkResult(useIS, dbMapsToSchema);

                    this.rs = metaData.getTables(this.dbName, null, "t1-_", null);
                    testGetTables_checkResult(useIS, dbMapsToSchema);

                    if (dbMapsToSchema) {
                        String dbPattern = conn1.getSchema().substring(0, conn1.getSchema().length() - 1) + "%";
                        this.rs = metaData.getTables(null, dbPattern, "t1-_", null);
                        assertTrue("Schema pattern " + dbPattern + " should be recognized.", this.rs.next());
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getTables(dbPattern, null, "t1-_", null);
                        assertFalse("Catalog pattern " + dbPattern + " should not be recognized.", this.rs.next());
                    }

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    private void testGetTables_checkResult(boolean useIS, boolean dbMapsToSchema) throws Exception {
        assertTrue(this.rs.next());
        if (dbMapsToSchema) {
            assertEquals("def", this.rs.getString("TABLE_CAT"));
            assertEquals(this.dbName, this.rs.getString("TABLE_SCHEM"));
        } else {
            assertEquals(this.dbName, this.rs.getString("TABLE_CAT"));
            assertNull(this.rs.getString("TABLE_SCHEM"));
        }
        assertEquals("t1-1", this.rs.getString("TABLE_NAME"));
        assertEquals("TABLE", this.rs.getString("TABLE_TYPE"));
        assertEquals(useIS ? "table1" : "", this.rs.getString("REMARKS")); // Table comment is available only with I_S
        assertNull(this.rs.getString("TYPE_CAT"));
        assertNull(this.rs.getString("TYPE_SCHEM"));
        assertNull(this.rs.getString("TYPE_NAME"));
        assertNull(this.rs.getString("SELF_REFERENCING_COL_NAME"));
        assertNull(this.rs.getString("REF_GENERATION"));

        assertTrue(this.rs.next());
        if (dbMapsToSchema) {
            assertEquals("def", this.rs.getString("TABLE_CAT"));
            assertEquals(this.dbName, this.rs.getString("TABLE_SCHEM"));
        } else {
            assertEquals(this.dbName, this.rs.getString("TABLE_CAT"));
            assertNull(this.rs.getString("TABLE_SCHEM"));
        }
        assertEquals("t1-2", this.rs.getString("TABLE_NAME"));
        assertEquals("TABLE", this.rs.getString("TABLE_TYPE"));
        assertEquals("", this.rs.getString("REMARKS"));
        assertNull(this.rs.getString("TYPE_CAT"));
        assertNull(this.rs.getString("TYPE_SCHEM"));
        assertNull(this.rs.getString("TYPE_NAME"));
        assertNull(this.rs.getString("SELF_REFERENCING_COL_NAME"));
        assertNull(this.rs.getString("REF_GENERATION"));

        assertFalse(this.rs.next());
    }

    /**
     * Tests the implementation of column privileges metadata.
     */
    public void testGetColumnPrivileges() throws Exception {

        if (!runTestIfSysPropDefined(PropertyDefinitions.SYSP_testsuite_cantGrant)) {
            Properties props = new Properties();

            props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "true");
            Connection conn1 = null;
            Statement stmt1 = null;
            String userHostQuoted = null;

            for (boolean useIS : new boolean[] { false, true }) {
                for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                    props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                    props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

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
                            if (dbMapsToSchema) {
                                assertEquals("def", this.rs.getString("TABLE_CAT"));
                                assertEquals(this.dbName, this.rs.getString("TABLE_SCHEM"));
                            } else {
                                assertEquals(this.dbName, this.rs.getString("TABLE_CAT"));
                                assertNull(this.rs.getString("TABLE_SCHEM"));
                            }
                            assertEquals("t1", this.rs.getString("TABLE_NAME"));
                            assertEquals("c1", this.rs.getString("COLUMN_NAME"));
                            assertEquals(useIS ? userHostQuoted : userHost.get(0) + "@" + userHost.get(1), this.rs.getString("GRANTEE"));
                            assertEquals("UPDATE", this.rs.getString("PRIVILEGE"));

                            if (dbMapsToSchema) {
                                String dbPattern = conn1.getSchema().substring(0, conn1.getSchema().length() - 1) + "%";
                                this.rs = metaData.getColumnPrivileges(null, dbPattern, "t1", null);
                                assertFalse("Schema pattern " + dbPattern + " should not be recognized.", this.rs.next());
                            } else {
                                String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                                this.rs = metaData.getColumnPrivileges(dbPattern, null, "t1", null);
                                assertFalse("Catalog pattern " + dbPattern + " should not be recognized.", this.rs.next());
                            }

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
        }
    }

    public void testGetProcedures() throws Exception {
        createProcedure("sp1", "() COMMENT 'testGetProcedures comment1' \n BEGIN\nSELECT 1;end\n");

        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {

                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData metaData = conn1.getMetaData();

                    if (dbMapsToSchema) {
                        String dbPattern = conn1.getSchema().substring(0, conn1.getSchema().length() - 1) + "%";
                        this.rs = metaData.getProcedures(null, dbPattern, "sp1");
                        assertTrue("Schema pattern " + dbPattern + " should be recognized.", this.rs.next());
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getProcedures(dbPattern, null, "sp1");
                        assertFalse("Catalog pattern " + dbPattern + " should not be recognized.", this.rs.next());
                    }

                    this.rs = metaData.getProcedures(null, null, "sp1");
                    testGetProcedures_checkResult(dbMapsToSchema);

                    this.rs = metaData.getProcedures(null, this.dbName.substring(0, 3) + "%", "sp1");
                    testGetProcedures_checkResult(dbMapsToSchema);

                    this.rs = metaData.getProcedures(this.dbName, null, "sp1");
                    testGetProcedures_checkResult(dbMapsToSchema);

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    private void testGetProcedures_checkResult(boolean dbMapsToSchema) throws Exception {
        assertTrue(this.rs.next());
        if (dbMapsToSchema) {
            assertEquals("def", this.rs.getString("PROCEDURE_CAT"));
            assertEquals(this.dbName, this.rs.getString("PROCEDURE_SCHEM"));
        } else {
            assertEquals(this.dbName, this.rs.getString("PROCEDURE_CAT"));
            assertNull(this.rs.getString("PROCEDURE_SCHEM"));
        }
        assertEquals("sp1", this.rs.getString("PROCEDURE_NAME"));
        assertNull(this.rs.getString(4));
        assertNull(this.rs.getString(5));
        assertNull(this.rs.getString(6));
        assertEquals("testGetProcedures comment1", this.rs.getString("REMARKS"));
        assertEquals("1", this.rs.getString("PROCEDURE_TYPE"));
        assertEquals("sp1", this.rs.getString("SPECIFIC_NAME"));
    }

    public void testGetFunctions() throws Exception {
        createFunction("testGetFunctionsF", "(d INT) RETURNS INT DETERMINISTIC COMMENT 'testGetFunctions comment1' BEGIN RETURN d; END");

        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {

                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData metaData = conn1.getMetaData();

                    this.rs = metaData.getFunctions(null, null, "testGetFunctionsF");
                    testGetFunctions_checkResult(dbMapsToSchema);

                    this.rs = metaData.getFunctions(null, this.dbName.substring(0, 3) + "%", "testGetFunctionsF");
                    testGetFunctions_checkResult(dbMapsToSchema);

                    this.rs = metaData.getFunctions(this.dbName, null, "testGetFunctionsF");
                    testGetFunctions_checkResult(dbMapsToSchema);

                    if (dbMapsToSchema) {
                        String dbPattern = conn1.getSchema().substring(0, conn1.getSchema().length() - 1) + "%";
                        this.rs = metaData.getFunctions(null, dbPattern, "testGetFunctionsF");
                        assertTrue("Schema pattern " + dbPattern + " should be recognized.", this.rs.next());
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getFunctions(dbPattern, null, "testGetFunctionsF");
                        assertFalse("Catalog pattern " + dbPattern + " should not be recognized.", this.rs.next());
                    }

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    private void testGetFunctions_checkResult(boolean dbMapsToSchema) throws Exception {
        assertTrue(this.rs.next());
        if (dbMapsToSchema) {
            assertEquals("def", this.rs.getString("FUNCTION_CAT"));
            assertEquals(this.dbName, this.rs.getString("FUNCTION_SCHEM"));
        } else {
            assertEquals(this.dbName, this.rs.getString("FUNCTION_CAT"));
            assertNull(this.rs.getString("FUNCTION_SCHEM"));
        }
        assertEquals("testGetFunctionsF", this.rs.getString("FUNCTION_NAME"));
        assertEquals("testGetFunctions comment1", this.rs.getString("REMARKS"));
        assertEquals("1", this.rs.getString("FUNCTION_TYPE"));
        assertEquals("testGetFunctionsF", this.rs.getString("SPECIFIC_NAME"));
    }

    public void testGetProcedureColumns() throws Exception {
        createProcedure("testGetProcedureColumnsP", "(d INT) COMMENT 'testGetProcedureColumns comment1' \n BEGIN\nSELECT 1;end\n");

        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {

                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData metaData = conn1.getMetaData();

                    if (dbMapsToSchema) {
                        String dbPattern = conn1.getSchema().substring(0, conn1.getSchema().length() - 1) + "%";
                        this.rs = metaData.getProcedureColumns(null, dbPattern, "testGetProcedureColumnsP", "%");
                        assertTrue("Schema pattern " + dbPattern + " should be recognized.", this.rs.next());
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getProcedureColumns(dbPattern, null, "testGetProcedureColumnsP", "%");
                        assertFalse("Catalog pattern " + dbPattern + " should not be recognized.", this.rs.next());
                    }

                    this.rs = metaData.getProcedureColumns(null, null, "testGetProcedureColumnsP", "%");
                    testGetProcedureColumns_checkResult(dbMapsToSchema);

                    this.rs = metaData.getProcedureColumns(null, this.dbName.substring(0, 3) + "%", "testGetProcedureColumnsP", "%");
                    testGetProcedureColumns_checkResult(dbMapsToSchema);

                    this.rs = metaData.getProcedureColumns(this.dbName, null, "testGetProcedureColumnsP", "%");
                    testGetProcedureColumns_checkResult(dbMapsToSchema);

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    private void testGetProcedureColumns_checkResult(boolean dbMapsToSchema) throws Exception {
        assertTrue(this.rs.next());
        if (dbMapsToSchema) {
            assertEquals("def", this.rs.getString("PROCEDURE_CAT"));
            assertEquals(this.dbName, this.rs.getString("PROCEDURE_SCHEM"));
        } else {
            assertEquals(this.dbName, this.rs.getString("PROCEDURE_CAT"));
            assertNull(this.rs.getString("PROCEDURE_SCHEM"));
        }
        assertEquals("testGetProcedureColumnsP", this.rs.getString("PROCEDURE_NAME"));
        assertEquals("d", this.rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.procedureColumnIn, this.rs.getShort("COLUMN_TYPE"));
        assertEquals(Types.INTEGER, this.rs.getInt("DATA_TYPE"));
        assertEquals("INT", this.rs.getString("TYPE_NAME"));
        assertEquals(10, this.rs.getInt("PRECISION"));
        assertEquals(10, this.rs.getInt("LENGTH"));
        assertEquals(0, this.rs.getInt("SCALE"));
        assertEquals(10, this.rs.getInt("RADIX"));
        assertEquals(1, this.rs.getShort("NULLABLE"));
        assertNull(this.rs.getString("REMARKS"));
        assertNull(this.rs.getString("COLUMN_DEF"));
        assertNull(this.rs.getString("SQL_DATA_TYPE"));
        assertNull(this.rs.getString("SQL_DATETIME_SUB"));
        assertNull(this.rs.getString("CHAR_OCTET_LENGTH"));
        assertEquals(1, this.rs.getInt("ORDINAL_POSITION"));
        assertEquals("YES", this.rs.getString("IS_NULLABLE"));
        assertEquals("testGetProcedureColumnsP", this.rs.getString("SPECIFIC_NAME"));
        assertFalse(this.rs.next());
    }

    public void testGetFunctionColumns() throws Exception {
        createFunction("testGetFunctionColumnsF", "(d INT) RETURNS INT DETERMINISTIC COMMENT 'testGetFunctionColumnsF comment1' BEGIN RETURN d; END");

        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {

                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData metaData = conn1.getMetaData();

                    this.rs = metaData.getFunctionColumns(null, null, "testGetFunctionColumnsF", "%");
                    testGetFunctionColumns_checkResult(dbMapsToSchema);

                    this.rs = metaData.getFunctionColumns(null, this.dbName.substring(0, 3) + "%", "testGetFunctionColumnsF", "%");
                    testGetFunctionColumns_checkResult(dbMapsToSchema);

                    this.rs = metaData.getFunctionColumns(this.dbName, null, "testGetFunctionColumnsF", "%");
                    testGetFunctionColumns_checkResult(dbMapsToSchema);

                    if (dbMapsToSchema) {
                        String dbPattern = conn1.getSchema().substring(0, conn1.getSchema().length() - 1) + "%";
                        this.rs = metaData.getFunctionColumns(null, dbPattern, "testGetFunctionColumnsF", "%");
                        assertTrue("Schema pattern " + dbPattern + " should be recognized.", this.rs.next());
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getFunctionColumns(dbPattern, null, "testGetFunctionColumnsF", "%");
                        assertFalse("Catalog pattern " + dbPattern + " should not be recognized.", this.rs.next());
                    }

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    private void testGetFunctionColumns_checkResult(boolean dbMapsToSchema) throws Exception {
        assertTrue(this.rs.next());
        if (dbMapsToSchema) {
            assertEquals("def", this.rs.getString("FUNCTION_CAT"));
            assertEquals(this.dbName, this.rs.getString("FUNCTION_SCHEM"));
        } else {
            assertEquals(this.dbName, this.rs.getString("FUNCTION_CAT"));
            assertNull(this.rs.getString("FUNCTION_SCHEM"));
        }
        assertEquals("testGetFunctionColumnsF", this.rs.getString("FUNCTION_NAME"));
        assertEquals("", this.rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.functionReturn, this.rs.getShort("COLUMN_TYPE"));
        assertEquals(Types.INTEGER, this.rs.getInt("DATA_TYPE"));
        assertEquals("INT", this.rs.getString("TYPE_NAME"));
        assertEquals(10, this.rs.getInt("PRECISION"));
        assertEquals(10, this.rs.getInt("LENGTH"));
        assertEquals(0, this.rs.getInt("SCALE"));
        assertEquals(10, this.rs.getInt("RADIX"));
        assertEquals(1, this.rs.getShort("NULLABLE"));
        assertNull(this.rs.getString("REMARKS"));
        assertNull(this.rs.getString("CHAR_OCTET_LENGTH"));
        assertEquals(0, this.rs.getInt("ORDINAL_POSITION"));
        assertEquals("YES", this.rs.getString("IS_NULLABLE"));
        assertEquals("testGetFunctionColumnsF", this.rs.getString("SPECIFIC_NAME"));

        assertTrue(this.rs.next());
        if (dbMapsToSchema) {
            assertEquals("def", this.rs.getString("FUNCTION_CAT"));
            assertEquals(this.dbName, this.rs.getString("FUNCTION_SCHEM"));
        } else {
            assertEquals(this.dbName, this.rs.getString("FUNCTION_CAT"));
            assertNull(this.rs.getString("FUNCTION_SCHEM"));
        }
        assertEquals("testGetFunctionColumnsF", this.rs.getString("FUNCTION_NAME"));
        assertEquals("d", this.rs.getString("COLUMN_NAME"));
        assertEquals(DatabaseMetaData.functionColumnIn, this.rs.getShort("COLUMN_TYPE"));
        assertEquals(Types.INTEGER, this.rs.getInt("DATA_TYPE"));
        assertEquals("INT", this.rs.getString("TYPE_NAME"));
        assertEquals(10, this.rs.getInt("PRECISION"));
        assertEquals(10, this.rs.getInt("LENGTH"));
        assertEquals(0, this.rs.getInt("SCALE"));
        assertEquals(10, this.rs.getInt("RADIX"));
        assertEquals(1, this.rs.getShort("NULLABLE"));
        assertNull(this.rs.getString("REMARKS"));
        assertNull(this.rs.getString("CHAR_OCTET_LENGTH"));
        assertEquals(1, this.rs.getInt("ORDINAL_POSITION"));
        assertEquals("YES", this.rs.getString("IS_NULLABLE"));
        assertEquals("testGetFunctionColumnsF", this.rs.getString("SPECIFIC_NAME"));

        assertFalse(this.rs.next());
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
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
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

    public void testGetExportedKeys() throws Exception {
        this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
        this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
        createTable("parent", "(id INT NOT NULL, PRIMARY KEY (id)) ENGINE=INNODB");
        createTable("child", "(id INT, parent_id INT, FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");

        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {

                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData metaData = conn1.getMetaData();

                    this.rs = metaData.getExportedKeys(null, null, "parent");
                    testGetExportedKeys_checkResult(useIS, dbMapsToSchema);

                    this.rs = metaData.getExportedKeys(null, this.dbName, "parent");
                    testGetExportedKeys_checkResult(useIS, dbMapsToSchema);

                    this.rs = metaData.getExportedKeys(this.dbName, null, "parent");
                    testGetExportedKeys_checkResult(useIS, dbMapsToSchema);

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
        this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
        this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
    }

    private void testGetExportedKeys_checkResult(boolean useIS, boolean dbMapsToSchema) throws Exception {
        assertTrue(this.rs.next());
        if (dbMapsToSchema) {
            assertEquals("def", this.rs.getString("PKTABLE_CAT"));
            assertEquals(this.dbName, this.rs.getString("PKTABLE_SCHEM"));
            assertEquals("def", this.rs.getString("FKTABLE_CAT"));
            assertEquals(this.dbName, this.rs.getString("FKTABLE_SCHEM"));
        } else {
            assertEquals(this.dbName, this.rs.getString("PKTABLE_CAT"));
            assertNull(this.rs.getString("PKTABLE_SCHEM"));
            assertEquals(this.dbName, this.rs.getString("FKTABLE_CAT"));
            assertNull(this.rs.getString("FKTABLE_SCHEM"));
        }
        assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
        assertEquals("id", this.rs.getString("PKCOLUMN_NAME"));
        assertEquals("child", this.rs.getString("FKTABLE_NAME"));
        assertEquals("parent_id", this.rs.getString("FKCOLUMN_NAME"));
        assertEquals(1, this.rs.getShort("KEY_SEQ"));
        assertEquals(DatabaseMetaData.importedKeyRestrict, this.rs.getShort("UPDATE_RULE"));
        assertEquals(DatabaseMetaData.importedKeySetNull, this.rs.getShort("DELETE_RULE"));
        assertEquals("child_ibfk_1", this.rs.getString("FK_NAME"));
        assertEquals(useIS ? "PRIMARY" : null, this.rs.getString("PK_NAME")); // PK_NAME is available only with I_S
        assertEquals(DatabaseMetaData.importedKeyNotDeferrable, this.rs.getShort("DEFERRABILITY"));
    }

    public void testGetImportedKeys() throws Exception {
        createTable("parent", "(id INT NOT NULL, PRIMARY KEY (id)) ENGINE=INNODB");
        createTable("child", "(id INT, parent_id INT, FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");

        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {

                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData metaData = conn1.getMetaData();

                    this.rs = metaData.getImportedKeys(null, null, "child");
                    testGetImportedKeys_checkResult(useIS, dbMapsToSchema);

                    this.rs = metaData.getImportedKeys(null, this.dbName, "child");
                    testGetImportedKeys_checkResult(useIS, dbMapsToSchema);

                    this.rs = metaData.getImportedKeys(this.dbName, null, "child");
                    testGetImportedKeys_checkResult(useIS, dbMapsToSchema);

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    private void testGetImportedKeys_checkResult(boolean useIS, boolean dbMapsToSchema) throws Exception {
        assertTrue(this.rs.next());
        if (dbMapsToSchema) {
            assertEquals("def", this.rs.getString("PKTABLE_CAT"));
            assertEquals(this.dbName, this.rs.getString("PKTABLE_SCHEM"));
            assertEquals("def", this.rs.getString("FKTABLE_CAT"));
            assertEquals(this.dbName, this.rs.getString("FKTABLE_SCHEM"));
        } else {
            assertEquals(this.dbName, this.rs.getString("PKTABLE_CAT"));
            assertNull(this.rs.getString("PKTABLE_SCHEM"));
            assertEquals(this.dbName, this.rs.getString("FKTABLE_CAT"));
            assertNull(this.rs.getString("FKTABLE_SCHEM"));
        }
        assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
        assertEquals("id", this.rs.getString("PKCOLUMN_NAME"));
        assertEquals("child", this.rs.getString("FKTABLE_NAME"));
        assertEquals("parent_id", this.rs.getString("FKCOLUMN_NAME"));
        assertEquals(1, this.rs.getShort("KEY_SEQ"));
        assertEquals(DatabaseMetaData.importedKeyRestrict, this.rs.getShort("UPDATE_RULE"));
        assertEquals(DatabaseMetaData.importedKeySetNull, this.rs.getShort("DELETE_RULE"));
        assertEquals("child_ibfk_1", this.rs.getString("FK_NAME"));
        assertEquals(useIS ? "PRIMARY" : null, this.rs.getString("PK_NAME")); // PK_NAME is available only with I_S
        assertEquals(DatabaseMetaData.importedKeyNotDeferrable, this.rs.getShort("DEFERRABILITY"));
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
        props.setProperty(PropertyKey.nullDatabaseMeansCurrent.getKeyName(), "true");

        for (String useIS : new String[] { "false", "true" }) {
            Connection testConn = null;
            props.setProperty(PropertyKey.useInformationSchema.getKeyName(), useIS);

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
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestGetSqlKeywordsDynamicQueryInterceptor.class.getName());

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

    public void testGetTablePrivileges() throws Exception {
        String tableName = "testGetTablePrivileges";
        createTable(tableName, "(id INT NOT NULL, PRIMARY KEY (id)) ENGINE=INNODB");
        createUser("'testGTPUser'@'%'", "IDENTIFIED BY 'aha'");
        this.stmt.executeUpdate("grant SELECT on `" + this.dbName + "`.`testGetTablePrivileges` to 'testGTPUser'@'%'");

        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {

                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData metaData = conn1.getMetaData();

                    String tablePattern = "testGetTablePriv%";
                    if (!metaData.supportsMixedCaseIdentifiers()) {
                        tableName = tableName.toLowerCase();
                        tablePattern = tablePattern.toLowerCase();
                    }

                    this.rs = metaData.getTablePrivileges(null, null, tablePattern);
                    testGetTablePrivileges_checkResult(dbMapsToSchema, tableName);

                    this.rs = metaData.getTablePrivileges(null, this.dbName, tablePattern);
                    testGetTablePrivileges_checkResult(dbMapsToSchema, tableName);

                    this.rs = metaData.getTablePrivileges(this.dbName, null, tablePattern);
                    testGetTablePrivileges_checkResult(dbMapsToSchema, tableName);

                    if (dbMapsToSchema) {
                        String dbPattern = conn1.getSchema().substring(0, conn1.getSchema().length() - 1) + "%";
                        this.rs = metaData.getTablePrivileges(null, dbPattern, tablePattern);
                        assertTrue("Schema pattern " + dbPattern + " should be recognized.", this.rs.next());
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getTablePrivileges(dbPattern, null, tablePattern);
                        assertFalse("Catalog pattern " + dbPattern + " should not be recognized.", this.rs.next());
                    }

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    private void testGetTablePrivileges_checkResult(boolean dbMapsToSchema, String tableName) throws Exception {
        assertTrue(this.rs.next());
        if (dbMapsToSchema) {
            assertEquals("def", this.rs.getString("TABLE_CAT"));
            assertEquals(this.dbName, this.rs.getString("TABLE_SCHEM"));
        } else {
            assertEquals(this.dbName, this.rs.getString("TABLE_CAT"));
            assertNull(this.rs.getString("TABLE_SCHEM"));
        }
        assertEquals(tableName, this.rs.getString("TABLE_NAME"));
        assertTrue(this.rs.getString("GRANTOR").startsWith(mainConnectionUrl.getMainHost().getUser()));
        assertEquals("testGTPUser@%", this.rs.getString("GRANTEE"));
        assertEquals("SELECT", this.rs.getString("PRIVILEGE"));
        assertNull(this.rs.getString("IS_GRANTABLE"));
        assertFalse(this.rs.next());
    }

    public void testGetBestRowIdentifier() throws Exception {
        String tableName = "testGetBestRowIdentifier";
        createTable(tableName, "(field1 INT NOT NULL PRIMARY KEY)");

        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean dbMapsToSchema : new boolean[] { false, true }) {
                props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                props.setProperty(PropertyKey.databaseTerm.getKeyName(), dbMapsToSchema ? DatabaseTerm.SCHEMA.name() : DatabaseTerm.CATALOG.name());

                System.out.println("useIS=" + useIS + ", dbMapsToSchema=" + dbMapsToSchema);

                Connection conn1 = null;
                try {

                    conn1 = getConnectionWithProps(props);
                    DatabaseMetaData metaData = conn1.getMetaData();

                    this.rs = metaData.getBestRowIdentifier(null, null, tableName, DatabaseMetaData.bestRowNotPseudo, true);
                    testGetBestRowIdentifier_checkResult(this.rs);

                    this.rs = metaData.getBestRowIdentifier(null, this.dbName, tableName, DatabaseMetaData.bestRowNotPseudo, true);
                    testGetBestRowIdentifier_checkResult(this.rs);

                    this.rs = metaData.getBestRowIdentifier(this.dbName, null, tableName, DatabaseMetaData.bestRowNotPseudo, true);
                    testGetBestRowIdentifier_checkResult(this.rs);

                    if (dbMapsToSchema) {
                        String dbPattern = conn1.getSchema().substring(0, conn1.getSchema().length() - 1) + "%";
                        this.rs = metaData.getBestRowIdentifier(null, dbPattern, tableName, DatabaseMetaData.bestRowNotPseudo, true);
                        assertFalse("Schema pattern " + dbPattern + " should not be recognized.", this.rs.next());
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getBestRowIdentifier(dbPattern, null, tableName, DatabaseMetaData.bestRowNotPseudo, true);
                        assertFalse("Catalog pattern " + dbPattern + " should not be recognized.", this.rs.next());
                    }

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    private void testGetBestRowIdentifier_checkResult(ResultSet rs1) throws Exception {
        assertTrue(rs1.next());
        assertEquals(DatabaseMetaData.bestRowSession, rs1.getShort("SCOPE"));
        assertEquals("field1", rs1.getString("COLUMN_NAME"));
        assertEquals(Types.INTEGER, rs1.getInt("DATA_TYPE"));
        assertEquals("int", rs1.getString("TYPE_NAME"));
        assertEquals(11, rs1.getInt("COLUMN_SIZE"));
        assertEquals(11, rs1.getInt("BUFFER_LENGTH"));
        assertEquals(0, rs1.getShort("DECIMAL_DIGITS"));
        assertEquals(DatabaseMetaData.bestRowNotPseudo, rs1.getShort("PSEUDO_COLUMN"));
        assertFalse(rs1.next());
    }

}
