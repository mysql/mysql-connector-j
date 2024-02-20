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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

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

import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlType;
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

    @Test
    public void testSupports() throws SQLException {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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

    @Test
    public void testGetCatalogVsGetSchemas() throws SQLException {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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

    @Test
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
            props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
                            assertFalse(this.rs.next(), "Schema pattern " + dbNamePattern + " should not be recognized.");
                        } else {
                            this.rs = dbmd.getImportedKeys(this.dbName, null, "child");
                            assertTrue(this.rs.next());
                            this.rs = dbmd.getImportedKeys(dbNamePattern, null, "child");
                            assertFalse(this.rs.next(), "Catalog pattern " + dbNamePattern + " should not be recognized.");
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
                            assertFalse(this.rs.next(), "Schema pattern " + dbNamePattern + " should not be recognized.");
                        } else {
                            this.rs = dbmd.getExportedKeys(this.dbName, null, "parent");
                            assertTrue(this.rs.next());
                            this.rs = dbmd.getExportedKeys(dbNamePattern, null, "parent");
                            assertFalse(this.rs.next(), "Catalog pattern " + dbNamePattern + " should not be recognized.");
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
                            assertFalse(this.rs.next(), "Schema patterns " + refDbPattern + " and " + dbNamePattern + " should not be recognized.");
                        } else {
                            this.rs = dbmd.getCrossReference(refDb, null, "cpd_foreign_3", this.dbName, null, "cpd_foreign_4");
                            assertTrue(this.rs.next());
                            this.rs = dbmd.getCrossReference(refDbPattern, null, "cpd_foreign_3", dbNamePattern, null, "cpd_foreign_4");
                            assertFalse(this.rs.next(), "Catalog patterns " + refDbPattern + " and " + dbNamePattern + " should not be recognized.");
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

    @Test
    public void testGetPrimaryKeys() throws SQLException {
        createTable("multikey", "(d INT NOT NULL, b INT NOT NULL, a INT NOT NULL, c INT NOT NULL, PRIMARY KEY (d, b, a, c))");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
                        assertFalse(this.rs.next(), "Schema pattern " + dbPattern + " should not be recognized.");
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = dbmd.getPrimaryKeys(dbPattern, null, "multikey"); //metaData.getIndexInfo(dbPattern, null, "t1", false, true);
                        assertFalse(this.rs.next(), "Catalog pattern " + dbPattern + " should not be recognized.");
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

                    assertFalse(keySeqs[0] != 3 && keySeqs[1] != 2 && keySeqs[2] != 4 && keySeqs[3] != 1, "Keys returned in wrong order");

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
     * This test automatically detects whether or not the server it is running against supports the creation of views.
     *
     * @throws SQLException
     */
    @Test
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
     */
    @Test
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

    @Test
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

    @Test
    public void testSupportsSelectForUpdate() throws Exception {
        boolean supportsForUpdate = this.conn.getMetaData().supportsSelectForUpdate();

        assertTrue(supportsForUpdate);
    }

    @Test
    public void testTinyint1IsBit() throws Exception {
        String tableName = "testTinyint1IsBit";
        // Can't use 'BIT' or boolean
        createTable(tableName, "(field1 TINYINT(1))");
        this.stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (1)");

        Properties props = new Properties();
        for (boolean useIS : new boolean[] { false, true }) {
            for (boolean tinyInt1isBit : new boolean[] { true, true }) {
                for (boolean transformedBitIsBoolean : new boolean[] { false, true }) {
                    props.clear();
                    props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
                    props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
                    props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "" + useIS);
                    props.setProperty(PropertyKey.tinyInt1isBit.getKeyName(), "" + tinyInt1isBit);
                    props.setProperty(PropertyKey.transformedBitIsBoolean.getKeyName(), "" + transformedBitIsBoolean);
                    Connection boolConn = getConnectionWithProps(props);

                    this.rs = boolConn.createStatement().executeQuery("SELECT field1 FROM " + tableName);
                    checkBitOrBooleanType(!transformedBitIsBoolean);

                    this.rs = boolConn.prepareStatement("SELECT field1 FROM " + tableName).executeQuery();
                    checkBitOrBooleanType(!transformedBitIsBoolean);

                    this.rs = boolConn.getMetaData().getColumns(boolConn.getCatalog(), null, tableName, "field1");
                    assertTrue(this.rs.next());

                    assertEquals(transformedBitIsBoolean ? Types.BOOLEAN : Types.BIT, this.rs.getInt("DATA_TYPE"));
                    assertEquals(transformedBitIsBoolean ? "BOOLEAN" : "BIT", this.rs.getString("TYPE_NAME"));
                }
            }
        }
    }

    @Test
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

    @Test
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
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useOldAliasMetadataBehavior.getKeyName(), "true");
        Connection con = getConnectionWithProps(props);

        this.rs = con.createStatement().executeQuery("SELECT c1 as QQQ, g1 FROM t1");
        assertEquals("QQQ", this.rs.getMetaData().getColumnLabel(1));
        assertEquals("QQQ", this.rs.getMetaData().getColumnName(1));
    }

    /**
     * Tests the implementation of Information Schema for primary keys.
     *
     * @throws Exception
     */
    @Test
    public void testGetPrimaryKeysUsingInfoShcema() throws Exception {
        createTable("t1", "(c1 int(1) primary key)");
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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

    @Test
    public void testGetIndexInfo() throws Exception {
        createTable("t1", "(c1 int(1))");
        this.stmt.executeUpdate("CREATE INDEX index1 ON t1 (c1)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
                        assertFalse(this.rs.next(), "Schema pattern " + dbPattern + " should not be recognized.");
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getIndexInfo(dbPattern, null, "t1", false, true);
                        assertFalse(this.rs.next(), "Catalog pattern " + dbPattern + " should not be recognized.");
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
     *
     * @throws Exception
     */
    @Test
    public void testGetColumns() throws Exception {
        createTable("t1", "(c1 char(1))");
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
                        assertTrue(this.rs.next(), "Schema pattern " + dbPattern + " should be recognized.");
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getColumns(dbPattern, null, "t1", null);
                        assertFalse(this.rs.next(), "Catalog pattern " + dbPattern + " should not be recognized.");
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
     *
     * @throws Exception
     */
    @Test
    public void testGetTablesUsingInfoSchema() throws Exception {
        createTable("`t1-1`", "(c1 char(1))");
        createTable("`t1-2`", "(c1 char(1))");
        createTable("`t2`", "(c1 char(1))");
        Set<String> tableNames = new HashSet<>();
        tableNames.add("t1-1");
        tableNames.add("t1-2");
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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

    @Test
    public void testGetTables() throws Exception {
        createTable("`t1-1`", "(c1 char(1)) COMMENT 'table1'");
        createTable("`t1-2`", "(c1 char(1))");
        createTable("`t2`", "(c1 char(1))");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
                        assertTrue(this.rs.next(), "Schema pattern " + dbPattern + " should be recognized.");
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getTables(dbPattern, null, "t1-_", null);
                        assertFalse(this.rs.next(), "Catalog pattern " + dbPattern + " should not be recognized.");
                    }

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    @Test
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
     *
     * @throws Exception
     */
    @Test
    public void testGetColumnPrivileges() throws Exception {
        assumeFalse(isSysPropDefined(PropertyDefinitions.SYSP_testsuite_cantGrant),
                "This testcase needs to be run with a URL that allows the user to issue GRANTs "
                        + " in the current database. Aborted because the system property \"" + PropertyDefinitions.SYSP_testsuite_cantGrant + "\" is set.");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
                    assertFalse(userHost.size() < 2, "This test requires a JDBC URL with a user, and won't work with the anonymous user. "
                            + "You can skip this test by setting the system property " + PropertyDefinitions.SYSP_testsuite_cantGrant);
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
                            assertFalse(this.rs.next(), "Schema pattern " + dbPattern + " should not be recognized.");
                        } else {
                            String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                            this.rs = metaData.getColumnPrivileges(dbPattern, null, "t1", null);
                            assertFalse(this.rs.next(), "Catalog pattern " + dbPattern + " should not be recognized.");
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

    @Test
    public void testGetProcedures() throws Exception {
        createProcedure("sp1", "() COMMENT 'testGetProcedures comment1' \n BEGIN\nSELECT 1;end\n");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
                        assertTrue(this.rs.next(), "Schema pattern " + dbPattern + " should be recognized.");
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getProcedures(dbPattern, null, "sp1");
                        assertFalse(this.rs.next(), "Catalog pattern " + dbPattern + " should not be recognized.");
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

    @Test
    public void testGetFunctions() throws Exception {
        createFunction("testGetFunctionsF", "(d INT) RETURNS INT DETERMINISTIC COMMENT 'testGetFunctions comment1' BEGIN RETURN d; END");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
                        assertTrue(this.rs.next(), "Schema pattern " + dbPattern + " should be recognized.");
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getFunctions(dbPattern, null, "testGetFunctionsF");
                        assertFalse(this.rs.next(), "Catalog pattern " + dbPattern + " should not be recognized.");
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

    @Test
    public void testGetProcedureColumns() throws Exception {
        createProcedure("testGetProcedureColumnsP", "(d INT) COMMENT 'testGetProcedureColumns comment1' \n BEGIN\nSELECT 1;end\n");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
                        assertTrue(this.rs.next(), "Schema pattern " + dbPattern + " should be recognized.");
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getProcedureColumns(dbPattern, null, "testGetProcedureColumnsP", "%");
                        assertFalse(this.rs.next(), "Catalog pattern " + dbPattern + " should not be recognized.");
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

    @Test
    public void testGetFunctionColumns() throws Exception {
        createFunction("testGetFunctionColumnsF", "(d INT) RETURNS INT DETERMINISTIC COMMENT 'testGetFunctionColumnsF comment1' BEGIN RETURN d; END");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
                        assertTrue(this.rs.next(), "Schema pattern " + dbPattern + " should be recognized.");
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getFunctionColumns(dbPattern, null, "testGetFunctionColumnsF", "%");
                        assertFalse(this.rs.next(), "Catalog pattern " + dbPattern + " should not be recognized.");
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
     *
     * @throws Exception
     */
    @Test
    public void testGetCrossReferenceUsingInfoSchema() throws Exception {
        this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
        this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
        this.stmt.executeUpdate("CREATE TABLE parent(id INT NOT NULL, PRIMARY KEY (id)) ENGINE=INNODB");
        this.stmt.executeUpdate(
                "CREATE TABLE child(id INT, parent_id INT, " + "FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");
        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        Connection conn1 = null;
        try {
            conn1 = getConnectionWithProps(props);
            DatabaseMetaData metaData = conn1.getMetaData();
            this.rs = metaData.getCrossReference(null, null, "parent", null, null, "child");
            assertTrue(this.rs.next());
            assertEquals("parent", this.rs.getString("PKTABLE_NAME"));
            assertEquals("id", this.rs.getString("PKCOLUMN_NAME"));
            assertEquals("child", this.rs.getString("FKTABLE_NAME"));
            assertEquals("parent_id", this.rs.getString("FKCOLUMN_NAME"));
            assertFalse(this.rs.next());
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
            this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
            if (conn1 != null) {
                conn1.close();
            }
        }
    }

    @Test
    public void testGetExportedKeys() throws Exception {
        this.stmt.executeUpdate("DROP TABLE IF EXISTS child");
        this.stmt.executeUpdate("DROP TABLE If EXISTS parent");
        createTable("parent", "(id INT NOT NULL, PRIMARY KEY (id)) ENGINE=INNODB");
        createTable("child", "(id INT, parent_id INT, FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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

    @Test
    public void testGetImportedKeys() throws Exception {
        createTable("parent", "(id INT NOT NULL, PRIMARY KEY (id)) ENGINE=INNODB");
        createTable("child", "(id INT, parent_id INT, FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET NULL) ENGINE=INNODB");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
     * - col_name data_type [GENERATED ALWAYS] AS (expression) [VIRTUAL | STORED] [UNIQUE [KEY]] [COMMENT comment] [[NOT] NULL] [[PRIMARY] KEY]
     *
     * @throws Exception
     */
    @Test
    public void testGeneratedColumns() throws Exception {
        assumeTrue(versionMeetsMinimum(5, 7, 6), "MySQL 5.7.6+ is required to run this test.");

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
            assertTrue(this.rs.next(), test);
            assertEquals("side_a", this.rs.getString("COLUMN_NAME"), test);
            assertEquals("YES", this.rs.getString("IS_NULLABLE"), test);
            assertEquals("NO", this.rs.getString("IS_AUTOINCREMENT"), test);
            assertEquals("NO", this.rs.getString("IS_GENERATEDCOLUMN"), test);
            assertTrue(this.rs.next(), test);
            assertEquals("side_b", this.rs.getString("COLUMN_NAME"), test);
            assertEquals("YES", this.rs.getString("IS_NULLABLE"), test);
            assertEquals("NO", this.rs.getString("IS_AUTOINCREMENT"), test);
            assertEquals("NO", this.rs.getString("IS_GENERATEDCOLUMN"), test);
            assertTrue(this.rs.next(), test);
            assertEquals("side_c_vir", this.rs.getString("COLUMN_NAME"), test);
            assertEquals("YES", this.rs.getString("IS_NULLABLE"), test);
            assertEquals("NO", this.rs.getString("IS_AUTOINCREMENT"), test);
            assertEquals("YES", this.rs.getString("IS_GENERATEDCOLUMN"), test);
            assertTrue(this.rs.next(), test);
            assertEquals("side_c_sto", this.rs.getString("COLUMN_NAME"), test);
            assertEquals("NO", this.rs.getString("IS_NULLABLE"), test);
            assertEquals("NO", this.rs.getString("IS_AUTOINCREMENT"), test);
            assertEquals("YES", this.rs.getString("IS_GENERATEDCOLUMN"), test);
            assertFalse(this.rs.next(), test);

            // Test primary keys metadata.
            this.rs = dbmd.getPrimaryKeys(null, null, "pythagorean_triple");
            assertTrue(this.rs.next(), test);
            assertEquals("side_c_sto", this.rs.getString("COLUMN_NAME"), test);
            assertEquals("PRIMARY", this.rs.getString("PK_NAME"), test);
            assertFalse(this.rs.next(), test);

            // Test indexes metadata.
            this.rs = dbmd.getIndexInfo(null, null, "pythagorean_triple", false, true);
            assertTrue(this.rs.next(), test);
            assertEquals("PRIMARY", this.rs.getString("INDEX_NAME"), test);
            assertEquals("side_c_sto", this.rs.getString("COLUMN_NAME"), test);
            assertTrue(this.rs.next(), test);
            assertEquals("side_c_sto", this.rs.getString("INDEX_NAME"), test);
            assertEquals("side_c_sto", this.rs.getString("COLUMN_NAME"), test);
            assertTrue(this.rs.next(), test);
            assertEquals("side_c_vir", this.rs.getString("INDEX_NAME"), test);
            assertEquals("side_c_vir", this.rs.getString("COLUMN_NAME"), test);
            assertFalse(this.rs.next(), test);

            testConn.close();
        }
    }

    /**
     * Tests DatabaseMetaData.getSQLKeywords().
     * (Related to BUG#70701 - DatabaseMetaData.getSQLKeywords() doesn't match MySQL 5.6 reserved words)
     *
     * This test checks the statically maintained keywords list.
     *
     * @throws Exception
     */
    @Test
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

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), //
                versionMeetsMinimum(8, 0, 11) ? "false" // Required for MySQL 8.0.11 and above, otherwise returns dynamic keywords
                        : "true");

        Connection testConn = getConnectionWithProps(props);
        assertEquals(mysqlKeywords, testConn.getMetaData().getSQLKeywords(), "MySQL keywords don't match expected.");
        testConn.close();
    }

    /**
     * Tests DatabaseMetaData.getSQLKeywords().
     * WL#10544, Update MySQL 8.0 keywords list.
     *
     * This test checks the dynamically maintained keywords lists.
     *
     * @throws Exception
     */
    @Test
    public void testGetSqlKeywordsDynamic() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 11), "MySQL 8.0.11+ is required to run this test.");

        /*
         * Setup test case.
         */
        // 1. Get list of SQL:2003 to exclude.
        Field dbmdSql2003Keywords = com.mysql.cj.jdbc.DatabaseMetaData.class.getDeclaredField("SQL2003_KEYWORDS");
        dbmdSql2003Keywords.setAccessible(true);
        @SuppressWarnings("unchecked")
        List<String> sql2003ReservedWords = Collections.unmodifiableList((List<String>) dbmdSql2003Keywords.get(null));
        assertTrue(sql2003ReservedWords != null && !sql2003ReservedWords.isEmpty(),
                "Failed to get field SQL2003_KEYWORDS from com.mysql.cj.jdbc.DatabaseMetaData");

        // 2. Retrieve list of reserved words from server.
        final String keywordsQuery = "SELECT WORD FROM INFORMATION_SCHEMA.KEYWORDS WHERE RESERVED=1 ORDER BY WORD";
        List<String> mysqlReservedWords = new ArrayList<>();
        this.rs = this.stmt.executeQuery(keywordsQuery);
        while (this.rs.next()) {
            mysqlReservedWords.add(this.rs.getString(1));
        }
        assertTrue(!mysqlReservedWords.isEmpty(), "Failed to retrieve reserved words from server.");

        // 3. Find the difference mysqlReservedWords - sql2003ReservedWords and prepare the expected result.
        mysqlReservedWords.removeAll(sql2003ReservedWords);
        String expectedSqlKeywords = String.join(",", mysqlReservedWords);

        // Make sure the keywords cache is empty in DatabaseMetaDataUsingInfoSchema.
        Field dbmduisKeywordsCacheField = DatabaseMetaDataUsingInfoSchema.class.getDeclaredField("keywordsCache");
        dbmduisKeywordsCacheField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<ServerVersion, String> dbmduisKeywordsCache = (Map<ServerVersion, String>) dbmduisKeywordsCacheField.get(null);
        assertNotNull(dbmduisKeywordsCache, "Failed to retrieve the field keywordsCache from com.mysql.cj.jdbc.DatabaseMetaDataUsingInfoSchema.");
        dbmduisKeywordsCache.clear();
        assertTrue(dbmduisKeywordsCache.isEmpty(), "Failed to clear the DatabaseMetaDataUsingInfoSchema keywords cache.");

        /*
         * Check that keywords are retrieved from database and cached.
         */
        Properties props = new Properties();
        props.setProperty(PropertyKey.useInformationSchema.getKeyName(), "true");
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestGetSqlKeywordsDynamicQueryInterceptor.class.getName());

        // First call to DatabaseMetaData.getSQLKeywords() -> keywords are retrieved from database.
        Connection testConn = getConnectionWithProps(props);
        assertEquals(expectedSqlKeywords, testConn.getMetaData().getSQLKeywords(), "MySQL keywords don't match expected.");
        assertTrue(TestGetSqlKeywordsDynamicQueryInterceptor.interceptedQueries.contains(keywordsQuery), "MySQL keywords weren't obtained from database.");
        assertTrue(dbmduisKeywordsCache.containsKey(((JdbcConnection) testConn).getServerVersion()), "Keywords for current server weren't properly cached.");

        TestGetSqlKeywordsDynamicQueryInterceptor.interceptedQueries.clear();

        // Second call to DatabaseMetaData.getSQLKeywords(), using same connection -> keywords are retrieved from internal cache.
        assertEquals(expectedSqlKeywords, testConn.getMetaData().getSQLKeywords(), "MySQL keywords don't match expected.");
        assertFalse(TestGetSqlKeywordsDynamicQueryInterceptor.interceptedQueries.contains(keywordsQuery), "MySQL keywords weren't obtained from cache.");
        assertTrue(dbmduisKeywordsCache.containsKey(((JdbcConnection) testConn).getServerVersion()), "Keywords for current server weren't properly cached.");
        testConn.close();

        TestGetSqlKeywordsDynamicQueryInterceptor.interceptedQueries.clear();

        // Third call to DatabaseMetaData.getSQLKeywords(), using different connection -> keywords are retrieved from internal cache.
        testConn = getConnectionWithProps(props);
        assertEquals(expectedSqlKeywords, testConn.getMetaData().getSQLKeywords(), "MySQL keywords don't match expected.");
        assertFalse(TestGetSqlKeywordsDynamicQueryInterceptor.interceptedQueries.contains(keywordsQuery), "MySQL keywords weren't obtained from cache.");
        assertTrue(dbmduisKeywordsCache.containsKey(((JdbcConnection) testConn).getServerVersion()), "Keywords for current server weren't properly cached.");
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

    @Test
    public void testGetTablePrivileges() throws Exception {
        String tableName = "testGetTablePrivileges";
        createTable(tableName, "(id INT NOT NULL, PRIMARY KEY (id)) ENGINE=INNODB");
        createUser("'testGTPUser'@'%'", "IDENTIFIED BY 'aha'");
        this.stmt.executeUpdate("grant SELECT on `" + this.dbName + "`.`testGetTablePrivileges` to 'testGTPUser'@'%'");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
                        assertTrue(this.rs.next(), "Schema pattern " + dbPattern + " should be recognized.");
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getTablePrivileges(dbPattern, null, tablePattern);
                        assertFalse(this.rs.next(), "Catalog pattern " + dbPattern + " should not be recognized.");
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

    @Test
    public void testGetBestRowIdentifier() throws Exception {
        String tableName = "testGetBestRowIdentifier";
        createTable(tableName, "(field1 INT NOT NULL PRIMARY KEY)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
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
                        assertFalse(this.rs.next(), "Schema pattern " + dbPattern + " should not be recognized.");
                    } else {
                        String dbPattern = conn1.getCatalog().substring(0, conn1.getCatalog().length() - 1) + "%";
                        this.rs = metaData.getBestRowIdentifier(dbPattern, null, tableName, DatabaseMetaData.bestRowNotPseudo, true);
                        assertFalse(this.rs.next(), "Catalog pattern " + dbPattern + " should not be recognized.");
                    }

                } finally {
                    if (conn1 != null) {
                        conn1.close();
                    }
                }
            }
        }
    }

    @Test
    private void testGetBestRowIdentifier_checkResult(ResultSet rs1) throws Exception {
        assertTrue(rs1.next());
        assertEquals(DatabaseMetaData.bestRowSession, rs1.getShort("SCOPE"));
        assertEquals("field1", rs1.getString("COLUMN_NAME"));
        assertEquals(Types.INTEGER, rs1.getInt("DATA_TYPE"));
        assertEquals("int", rs1.getString("TYPE_NAME"));
        assertEquals(versionMeetsMinimum(8, 0, 19) ? 10 : 11, rs1.getInt("COLUMN_SIZE"));
        assertEquals(65535, rs1.getInt("BUFFER_LENGTH"));
        assertEquals(0, rs1.getShort("DECIMAL_DIGITS"));
        assertEquals(DatabaseMetaData.bestRowNotPseudo, rs1.getShort("PSEUDO_COLUMN"));
        assertFalse(rs1.next());
    }

    /**
     * WL#16174: Support for VECTOR data type
     *
     * This test checks that the type of the VECTOR column is reported back as MysqlType.VECTOR. VECTOR support was added in MySQL 9.0.0.
     *
     * @throws Exception
     */
    @Test
    public void testVectorColumnType() throws Exception {
        assumeTrue(versionMeetsMinimum(9, 0), "MySQL 9.0.0+ is needed to run this test.");
        createTable("testVectorColumnType", "(v VECTOR)");
        DatabaseMetaData md = this.conn.getMetaData();
        this.rs = md.getColumns(null, null, "testVectorColumnType", "v");
        this.rs.next();
        assertEquals(MysqlType.VECTOR.getName().toUpperCase(), this.rs.getString("TYPE_NAME").toUpperCase());
    }

    /**
     * WL#16174: Support for VECTOR data type
     *
     * This test checks that the result set metadata reports back the VECTOR column as MysqlType.VECTOR. VECTOR support was added in MySQL 9.0.0.
     *
     * @throws Exception
     */
    @Test
    public void testVectorResultSetType() throws Exception {
        assumeTrue(versionMeetsMinimum(9, 0), "MySQL 9.0.0+ is needed to run this test.");
        createTable("testVectorResultSetType", "(v VECTOR)");
        // 0xC3F5484014AE0F41 is the HEX representation for the vector [3.14000e+00,8.98000e+00]
        this.stmt.execute("INSERT INTO testVectorResultSetType VALUES(0xC3F5484014AE0F41)");
        this.rs = this.stmt.executeQuery("SELECT v FROM testVectorResultSetType");
        this.rs.next();
        ResultSetMetaData md = this.rs.getMetaData();
        assertEquals(MysqlType.VECTOR.getName().toUpperCase(), md.getColumnTypeName(1).toUpperCase());
    }

}
