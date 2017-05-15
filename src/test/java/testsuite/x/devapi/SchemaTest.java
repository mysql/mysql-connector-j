/*
  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.x.devapi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.xdevapi.Collection;
import com.mysql.cj.api.xdevapi.CreateTableStatement;
import com.mysql.cj.api.xdevapi.DatabaseObject.DbObjectStatus;
import com.mysql.cj.api.xdevapi.DatabaseObject.DbObjectType;
import com.mysql.cj.api.xdevapi.ForeignKeyDefinition.ChangeMode;
import com.mysql.cj.api.xdevapi.Row;
import com.mysql.cj.api.xdevapi.RowResult;
import com.mysql.cj.api.xdevapi.Schema;
import com.mysql.cj.api.xdevapi.SelectStatement;
import com.mysql.cj.api.xdevapi.Session;
import com.mysql.cj.api.xdevapi.Table;
import com.mysql.cj.api.xdevapi.Type;
import com.mysql.cj.api.xdevapi.ViewDDL.ViewAlgorithm;
import com.mysql.cj.api.xdevapi.ViewDDL.ViewCheckOption;
import com.mysql.cj.api.xdevapi.ViewDDL.ViewSqlSecurity;
import com.mysql.cj.api.xdevapi.ViewUpdate;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.x.core.DatabaseObjectDescription;
import com.mysql.cj.x.core.MysqlxSession;
import com.mysql.cj.x.core.XDevAPIError;
import com.mysql.cj.xdevapi.ColumnDef;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.ForeignKeyDef;
import com.mysql.cj.xdevapi.GeneratedColumnDef;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.SessionImpl;

public class SchemaTest extends DevApiBaseTestCase {
    @Before
    public void setupCollectionTest() {
        setupTestSession();
    }

    @After
    public void teardownCollectionTest() {
        destroyTestSession();
    }

    @Test
    public void testEquals() {
        if (!this.isSetForXTests) {
            return;
        }
        Schema otherDefaultSchema = this.session.getDefaultSchema();
        assertFalse(otherDefaultSchema == this.schema);
        assertTrue(otherDefaultSchema.equals(this.schema));
        assertTrue(this.schema.equals(otherDefaultSchema));
        assertFalse(this.schema.equals(this.session));

        Session otherSession = new SessionImpl(this.testProperties);
        Schema diffSessionSchema = otherSession.getDefaultSchema();
        assertEquals(this.schema.getName(), diffSessionSchema.getName());
        assertFalse(this.schema.equals(diffSessionSchema));
        assertFalse(diffSessionSchema.equals(this.schema));
        otherSession.close();
    }

    @Test
    public void testToString() {
        if (!this.isSetForXTests) {
            return;
        }
        // this will pass as long as the test database doesn't require identifier quoting
        assertEquals("Schema(" + getTestDatabase() + ")", this.schema.toString());
        Schema needsQuoted = this.session.getSchema("terrible'schema`name");
        assertEquals("Schema(`terrible'schema``name`)", needsQuoted.toString());
    }

    @Test
    public void testListCollections() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName1 = "test_list_collections1";
        String collName2 = "test_list_collections2";
        dropCollection(collName1);
        dropCollection(collName2);
        Collection coll1 = this.schema.createCollection(collName1);
        Collection coll2 = this.schema.createCollection(collName2);

        List<Collection> colls = this.schema.getCollections();
        assertTrue(colls.contains(coll1));
        assertTrue(colls.contains(coll2));

        colls = this.schema.getCollections("%ions2");
        assertFalse(colls.contains(coll1));
        assertTrue(colls.contains(coll2));
    }

    @Test
    public void testExists() {
        if (!this.isSetForXTests) {
            return;
        }
        assertEquals(DbObjectStatus.EXISTS, this.schema.existsInDatabase());
        Schema nonExistingSchema = this.session.getSchema(getTestDatabase() + "_SHOULD_NOT_EXIST_0xCAFEBABE");
        assertEquals(DbObjectStatus.NOT_EXISTS, nonExistingSchema.existsInDatabase());
    }

    @Test
    public void testCreateCollection() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "testCreateCollection";
        dropCollection(collName);
        Collection coll = this.schema.createCollection(collName);
        try {
            this.schema.createCollection(collName);
            fail("Exception should be thrown trying to create a collection that already exists");
        } catch (XDevAPIError ex) {
            // expected
            assertEquals(MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR, ex.getErrorCode());
        }
        Collection coll2 = this.schema.createCollection(collName, true);
        assertEquals(coll, coll2);
    }

    @Test
    public void testListTables() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "test_list_tables_collection";
        String tableName = "test_list_tables_table";
        String viewName = "test_list_tables_view";
        try {
            dropCollection(collName);
            sqlUpdate("drop view if exists " + viewName);
            sqlUpdate("drop table if exists " + tableName);

            Collection coll = this.schema.createCollection(collName);

            sqlUpdate("create table " + tableName + "(name varchar(32), age int, role int)");
            sqlUpdate("create view " + viewName + " as select name, age from " + tableName);

            Table table = this.schema.getTable(tableName);
            Table view = this.schema.getTable(viewName);

            List<Table> tables = this.schema.getTables();
            assertFalse(tables.contains(coll));
            assertTrue(tables.contains(table));
            assertTrue(tables.contains(view));

            tables = this.schema.getTables("%tables_t%");
            assertFalse(tables.contains(coll));
            assertTrue(tables.contains(table));
            assertFalse(tables.contains(view));

        } finally {
            dropCollection(collName);
            sqlUpdate("drop view if exists " + viewName);
            sqlUpdate("drop table if exists " + tableName);
        }

    }

    @Test
    public void testCreateTable() {
        if (!this.isSetForXTests) {
            return;
        }
        String tableName1 = "test_create_table1";
        String tableName2 = "test_create_table2";
        String tableLikeName = "test_create_table3";
        String tableAsName = "test_create_table4";
        sqlUpdate("drop table if exists " + tableAsName);
        sqlUpdate("drop table if exists " + tableAsName + "_check");
        sqlUpdate("drop table if exists " + tableLikeName);
        sqlUpdate("drop table if exists " + tableLikeName + "_check");
        sqlUpdate("drop table if exists " + tableName2);
        sqlUpdate("drop table if exists " + tableName2 + "_check");
        sqlUpdate("drop table if exists " + tableName1);
        sqlUpdate("drop table if exists " + tableName1 + "_check");

        assertThrows(WrongArgumentException.class, "Length parameter is not applicable to the JSON type of column 'f01'.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.createTable(tableName1).addColumn(new ColumnDef("f01", Type.JSON, 3)).execute();
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "Decimals parameter is not applicable to the INT type of column 'f01'.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.createTable(tableName1).addColumn(new ColumnDef("f01", Type.INT, 3).decimals(2)).execute();
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "Length must be specified before decimals for column 'f01'.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.createTable(tableName1).addColumn(new ColumnDef("f01", Type.DECIMAL).decimals(2)).execute();
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "UNSIGNED is not applicable to the VARCHAR type of column 'f01'.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.createTable(tableName1).addColumn(new ColumnDef("f01", Type.STRING, 3).unsigned()).execute();
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "G, PG, PG3 is not applicable to the SMALLINT type of column 'rating'.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.createTable(tableName1).addColumn(new ColumnDef("rating", Type.SMALLINT).values("G", "PG", "PG3")).execute();
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "BINARY is not applicable to the INT type of column 'f01'.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.createTable(tableName1).addColumn(new ColumnDef("f01", Type.INT, 3).binary()).execute();
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "CHARACTER SET is not applicable to the INT type of column 'f01'.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.createTable(tableName1).addColumn(new ColumnDef("f01", Type.INT, 3).charset("sjis")).execute();
                return null;
            }
        });
        assertThrows(WrongArgumentException.class, "COLLATE is not applicable to the INT type of column 'f01'.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.createTable(tableName1).addColumn(new ColumnDef("f01", Type.INT, 3).collation("sjis_japanese_ci")).execute();
                return null;
            }
        });

        // basics
        CreateTableStatement func = this.schema.createTable(tableName1) //
                .setDefaultCharset("utf8") //
                .setDefaultCollation("utf8_general_ci") //
                .addColumn(new ColumnDef("f01", Type.INT, 3).unsigned().notNull()) //
                .addColumn(new ColumnDef("f02", Type.BIT, 2).setDefault("1").comment("some comment")) //
                .addColumn(new ColumnDef("f03", Type.SMALLINT, 5).autoIncrement().uniqueIndex()) //
                .addColumn(new ColumnDef("f04", Type.BIGINT, 6)) //
                .addColumn(new ColumnDef("f05", Type.MEDIUMINT).unsigned()) //
                .addColumn(new ColumnDef("f06", Type.FLOAT, 10).decimals(2)) //
                .addColumn(new ColumnDef("f07", Type.DOUBLE, 5).decimals(2)) //
                .addColumn(new ColumnDef("f08", Type.JSON)) //
                .addColumn(new ColumnDef("f09", Type.STRING, 100).charset("sjis").collation("sjis_japanese_ci")) //
                .addColumn(new ColumnDef("f10", Type.STRING, 100).charset("sjis").binary()) //
                .addColumn(new ColumnDef("f11", Type.BYTES, 100)) //
                .addColumn(new ColumnDef("f12", Type.GEOMETRY)) //
                .addColumn(new ColumnDef("f13", Type.TIME).notNull().setDefault("'12:00'")) //
                .addColumn(new ColumnDef("f14", Type.DATE).notNull().setDefault("'2016-09-01'")) //
                .addColumn(new ColumnDef("f15", Type.DATETIME).notNull().setDefault("CURRENT_TIMESTAMP")) //
                .addColumn(new ColumnDef("f16", Type.TIMESTAMP).notNull().setDefault("CURRENT_TIMESTAMP")) //
                .addColumn(new ColumnDef("language_id", Type.TINYINT).unsigned().notNull()) //
                .addColumn(new GeneratedColumnDef("gen01", Type.DOUBLE, 5, "f04 / f05").decimals(2).notNull()) //
                .addPrimaryKey("f01", "f02") //
                .addIndex("lang_id", "language_id") //
                .addIndex("f05_idx", "f05") //
                .addUniqueIndex("f07_idx", "f07") //
                .addUniqueIndex("f09_idx", "f09") //
                .setInitialAutoIncrement(0) //
                .setComment("some table comment");
        System.out.println("Parsed to:");
        System.out.println(func);
        Table t1 = func.execute();

        String sql1 = " (f01 int(3) unsigned NOT NULL," //
                + " f02 bit(2) NOT NULL DEFAULT b'1' COMMENT 'some comment'," //
                + " f03 smallint(5) NOT NULL AUTO_INCREMENT," //
                + " f04 bigint(6) DEFAULT NULL," //
                + " f05 mediumint(8) unsigned DEFAULT NULL," + " f06 float(10,2) DEFAULT NULL," //
                + " f07 double(5,2) DEFAULT NULL," + " f08 json DEFAULT NULL," //
                + " f09 varchar(100) CHARACTER SET sjis DEFAULT NULL," //
                + " f10 varchar(100) CHARACTER SET sjis COLLATE sjis_bin DEFAULT NULL," //
                + " f11 varbinary(100) DEFAULT NULL," //
                + " f12 geometry DEFAULT NULL," //
                + " f13 time NOT NULL DEFAULT '12:00:00'," //
                + " f14 date NOT NULL DEFAULT '2016-09-01'," //
                + " f15 datetime NOT NULL DEFAULT CURRENT_TIMESTAMP," //
                + " f16 timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," //
                + " language_id tinyint(3) unsigned NOT NULL," //
                + " gen01 double(5,2) GENERATED ALWAYS AS (f04 / f05) VIRTUAL NOT NULL," //
                + " PRIMARY KEY (f01,f02)," //
                + " UNIQUE KEY f03 (f03)," //
                + " UNIQUE KEY f07_idx (f07)," //
                + " UNIQUE KEY f09_idx (f09)," //
                + " KEY f05_idx (f05)," //
                + " KEY lang_id (language_id)" //
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='some table comment'";

        checkCreatedTable(tableName1, sql1);

        // create table like
        Table tLike = this.schema.createTable(tableLikeName).like(tableName1).execute();
        assertNotEquals(t1, tLike);
        checkCreatedTable(tableLikeName, sql1);

        // complex types, foreign keys
        func = this.schema.createTable(tableName2) //
                .addColumn(new ColumnDef("film_id", Type.SMALLINT).primaryKey().autoIncrement().unsigned()) //
                .addColumn(new ColumnDef("title", Type.STRING, 255).notNull().uniqueIndex()) //
                .addColumn(new ColumnDef("language_id", Type.TINYINT).unsigned().notNull()) //
                .addColumn(new ColumnDef("original_language_id", Type.TINYINT).unsigned().setDefault(null)) //
                .addColumn(new ColumnDef("rental_duration", Type.TINYINT).unsigned().notNull().setDefault("3")) //
                .addColumn(new ColumnDef("rental_rate", Type.DECIMAL, 4).decimals(2).notNull().setDefault("4.99")) //
                .addColumn(new ColumnDef("length", Type.SMALLINT).unsigned().setDefault(null)) //
                .addColumn(new ColumnDef("replacement_cost", Type.DECIMAL, 5).decimals(2).notNull().setDefault("19.99")) //
                .addColumn(new ColumnDef("rating", Type.ENUM).values("G", "PG", "PG-13", "R", "NC-17").setDefault("'G'")) //
                .addColumn(
                        new ColumnDef("special_features", Type.SET).values("Trailers", "Commentaries", "Deleted Scenes", "Behind the Scenes").setDefault(null)) //
                .addColumn(new ColumnDef("last_update", Type.TIMESTAMP).notNull().setDefault("CURRENT_TIMESTAMP")) //
                .addIndex("idx_title", "title") //
                .addForeignKey("fk_film_language", new ForeignKeyDef().fields("language_id").refersTo(tableName1, "language_id")) //
                .addForeignKey("fk_film_language_original",
                        new ForeignKeyDef().fields("original_language_id").refersTo(tableName1, "language_id").onUpdate(ChangeMode.CASCADE));
        System.out.println("Parsed to:");
        System.out.println(func);
        Table t2 = func.execute();

        checkCreatedTable(tableName2, "(film_id smallint(5) unsigned NOT NULL AUTO_INCREMENT," //
                + " title varchar(255) NOT NULL," //
                + " language_id tinyint(3) unsigned NOT NULL," //
                + " original_language_id tinyint(3) unsigned DEFAULT NULL," //
                + " rental_duration tinyint(3) unsigned NOT NULL DEFAULT '3'," //
                + " rental_rate decimal(4,2) NOT NULL DEFAULT '4.99'," //
                + " length smallint(5) unsigned DEFAULT NULL," //
                + " replacement_cost decimal(5,2) NOT NULL DEFAULT '19.99'," //
                + " rating enum('G','PG','PG-13','R','NC-17') DEFAULT 'G'," //
                + " special_features set('Trailers','Commentaries','Deleted Scenes','Behind the Scenes') DEFAULT NULL," //
                + " last_update timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP," //
                + " PRIMARY KEY (film_id)," //
                + " UNIQUE KEY title (title)," //
                + " KEY idx_title (title)," //
                + " KEY fk_film_language (language_id)," //
                + " KEY fk_film_language_original (original_language_id)," //
                + " CONSTRAINT " + tableName2 + "_ibfk_1 FOREIGN KEY (language_id) REFERENCES " + tableName1 + " (language_id)," //
                + " CONSTRAINT " + tableName2 + "_ibfk_2 FOREIGN KEY (original_language_id) REFERENCES " + tableName1 + " (language_id) ON UPDATE CASCADE" //
                + ") ENGINE=InnoDB DEFAULT CHARSET=" + this.dbCharset);

        try {
            this.schema.createTable(tableName2).addColumn(new ColumnDef("id", Type.TINYINT).unsigned().notNull().primaryKey()).execute();
            fail("Exception should be thrown if trying to create a table that already exists");
        } catch (XDevAPIError ex) {
            // expected
            assertEquals(MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR, ex.getErrorCode());
        }
        Table t3 = this.schema.createTable(tableName2, true).addColumn(new ColumnDef("id", Type.TINYINT).unsigned().notNull().primaryKey()).execute();
        assertEquals(t2, t3);

        // create table as
        func = this.schema.createTable(tableAsName) //
                .addColumn(new ColumnDef("id", Type.SMALLINT).primaryKey().autoIncrement().unsigned()) //
                .addIndex("idx_title", "title") //
                .addForeignKey("fk_film_language", new ForeignKeyDef().fields("language_id").refersTo(tableName1, "language_id")) //
                .setDefaultCharset("utf8") //
                .setDefaultCollation("utf8_spanish_ci") //
                .setComment("with generated columns") //
                .as("SELECT film_id, title, language_id from " + tableName2);
        System.out.println("Parsed to:");
        System.out.println(func);
        func.execute();

        checkCreatedTable(tableAsName,
                "(id smallint(5) unsigned NOT NULL AUTO_INCREMENT," // 
                        + " film_id smallint(5) unsigned NOT NULL DEFAULT '0'," //
                        + " title varchar(255) CHARACTER SET " + this.dbCharset + " NOT NULL," //
                        + " language_id tinyint(3) unsigned NOT NULL," //
                        + " PRIMARY KEY (id)," //
                        + " KEY idx_title (title)," //
                        + " KEY fk_film_language (language_id)," //
                        + " CONSTRAINT " + tableAsName + "_ibfk_1 FOREIGN KEY (language_id) REFERENCES " + tableName1 + " (language_id)"
                        + ") ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_spanish_ci COMMENT='with generated columns'");

        sqlUpdate("drop table if exists " + tableAsName);
        sqlUpdate("drop table if exists " + tableAsName + "_check");
        sqlUpdate("drop table if exists " + tableLikeName);
        sqlUpdate("drop table if exists " + tableLikeName + "_check");
        sqlUpdate("drop table if exists " + tableName2);
        sqlUpdate("drop table if exists " + tableName2 + "_check");
        sqlUpdate("drop table if exists " + tableName1);
        sqlUpdate("drop table if exists " + tableName1 + "_check");
    }

    private void checkCreatedTable(String name, String ddl) {
        RowResult rows = this.session.sql("show create table " + name).execute();
        Row row = rows.next();
        String t1 = row.getString(1).substring(16 + name.length()); // skip CREATE TABLE `name` "

        // need to drop the original table first to avoid duplicate constraints
        sqlUpdate("drop table if exists " + name);

        sqlUpdate("CREATE TABLE " + name + "_check " + ddl);

        rows = this.session.sql("show create table " + name + "_check").execute();
        row = rows.next();
        String t2 = row.getString(1).substring(22 + name.length()); // skip CREATE TABLE `name_check` "

        System.out.println("Expected:");
        System.out.println(t2);
        System.out.println("Actual:");
        System.out.println(t1);
        assertEquals(t2, t1);

        // restore original table
        sqlUpdate("drop table if exists " + name + "_check ");
        sqlUpdate("CREATE TABLE " + name + " " + ddl);
    }

    @Test
    public void testViewDDL() {
        if (!this.isSetForXTests) {
            return;
        }

        try {
            sqlUpdate("drop table if exists collection_test_view_ddl");
            sqlUpdate("drop table if exists table_test_view_ddl");
            sqlUpdate("drop view if exists view_test_view_ddl");
            sqlUpdate("drop view if exists view_test_view_ddl_check");
            sqlUpdate("create table table_test_view_ddl (first_name varchar(32), last_name varchar(32), age int, descr JSON)");

            Collection col = this.schema.createCollection("collection_test_view_ddl");
            col.add("{\"first_name\": \"Clifford\", \"last_name\": \"Simak\"}").add("{\"first_name\": \"Arthur\", \"last_name\": \"Clarke\"}").execute();

            Table table = this.schema.getTable("table_test_view_ddl");
            assertEquals(DbObjectStatus.EXISTS, table.existsInDatabase());
            table.insert().values("Clifford", "Simak", "112", "{\"first_name\": \"Clifford\", \"last_name\": \"Simak\"}")
                    .values("Arthur", "Clarke", "99", "{\"first_name\": \"Arthur\", \"last_name\": \"Clarke\"}").execute();

            // 1. Create table view
            Table view = this.schema.createView("view_test_view_ddl", false)
                    .definedAs(table.select("concat(first_name, \" \", last_name) as name, age").orderBy("name ASC")).execute();

            assertEquals(DbObjectStatus.EXISTS, view.existsInDatabase());
            assertTrue(view.isView());

            // check that the view is in proper list
            assertTrue(this.schema.getTables().contains(view));
            assertFalse(this.schema.getCollections().contains(view));

            // 2. Retrieve the view
            Table view2 = this.schema.getTable("view_test_view_ddl", true);
            assertTrue(view2.isView());
            assertFalse(view == view2);
            assertTrue(view.equals(view2));

            RowResult rows = view2.select("*").execute();
            Row r = rows.next();
            assertEquals("Arthur Clarke", r.getString("name"));
            assertEquals(99, r.getInt("age"));
            r = rows.next();
            assertEquals("Clifford Simak", r.getString("name"));
            assertEquals(112, r.getInt("age"));

            // 3.1 Alter view with table.select, ensure that changes made to SelectStatement after calling definedAs() do not affect view (Bug#25438176)
            SelectStatement select = table.select("concat(first_name, \" \", last_name) as full_name").orderBy("full_name DESC"); // no .execute()!
            ViewUpdate vu = this.schema.alterView("view_test_view_ddl").definedAs(select);
            select = select.orderBy("full_name ASC");  // this will not affect the view definition
            Table view3 = vu.execute();

            assertTrue(view3.isView());
            rows = view3.select("*").execute();
            List<String> colNames = rows.getColumnNames();
            assertEquals(1, colNames.size());

            r = rows.next();
            assertEquals("Clifford Simak", r.getString("full_name"));
            r = rows.next();
            assertEquals("Arthur Clarke", r.getString("full_name"));

            // 3.2 Alter view with getting a COLLECTION_VIEW
            view3 = this.schema.alterView("view_test_view_ddl").definedAs(table.select("descr as doc")).execute();

            // check that the actual type is COLLECTION_VIEW
            assertTrue(getViewType(this.schema.getName(), "view_test_view_ddl") == DbObjectType.COLLECTION_VIEW);

            // checking the Table method
            assertTrue(view3.isView());

            // check that the view is in proper list
            assertTrue(this.schema.getTables().contains(view));
            assertFalse(this.schema.getCollections().contains(view));

            // 3.3 Alter view with renaming "doc" column (getting a VIEW)
            view3 = this.schema.alterView("view_test_view_ddl").columns("val").definedAs(table.select("descr as nodoc")).execute();

            // check that the actual type is VIEW
            assertTrue(getViewType(this.schema.getName(), "view_test_view_ddl") == DbObjectType.VIEW);

            // checking the Table method
            assertTrue(view3.isView());

            rows = view3.select("*").execute();
            colNames = rows.getColumnNames();
            assertEquals(1, colNames.size());
            assertEquals("val", colNames.get(0));

            // 4. Dropping the existing view
            this.schema.dropView("view_test_view_ddl").ifExists().execute();
            assertEquals(DbObjectStatus.NOT_EXISTS, this.schema.getTable("view_test_view_ddl").existsInDatabase());

            // Dropping the not existing view
            try {
                this.schema.dropView("notExistingViewDDL").execute();
                fail("Exception should be thrown if trying to drop a not existing view without ifExists() set.");
            } catch (XDevAPIError ex) {
                // expected
                assertEquals(MysqlErrorNumbers.ER_BAD_TABLE_ERROR, ex.getErrorCode());
            }

            this.schema.dropView("notExistingViewDDL").ifExists().execute(); // Notices are produced here but ignored
            assertEquals(DbObjectStatus.NOT_EXISTS, this.schema.getTable("notExistingViewDDL").existsInDatabase());

            // 5. Create collection view (views that have one column which is "doc JSON" should be categorized as "COLLECTION_VIEW")
            this.schema.createView("view_test_view_ddl", false).definedAs(table.select("descr as doc").orderBy("$.first_name ASC")).execute();
            view = this.schema.getTable("view_test_view_ddl", true);

            assertEquals(DbObjectStatus.EXISTS, view.existsInDatabase());
            assertTrue(view.isView());

            // check that the actual type is COLLECTION_VIEW
            assertTrue(getViewType(this.schema.getName(), "view_test_view_ddl") == DbObjectType.COLLECTION_VIEW);

            rows = view.select("*").execute();
            r = rows.next();
            DbDoc doc = r.getDbDoc("doc");
            assertEquals("Arthur", ((JsonString) doc.get("first_name")).getString());
            assertEquals("Clarke", ((JsonString) doc.get("last_name")).getString());
            r = rows.next();
            doc = r.getDbDoc("doc");
            assertEquals("Clifford", ((JsonString) doc.get("first_name")).getString());
            assertEquals("Simak", ((JsonString) doc.get("last_name")).getString());

            this.schema.dropView("view_test_view_ddl").ifExists().execute();

            // 6. Define all fields
            view = this.schema.createView("view_test_view_ddl", false).algorithm(ViewAlgorithm.MERGE).columns("n", "a")
                    .definedAs(table.select("concat(first_name, \" \", last_name) as name, age").orderBy("name ASC")).definer("root")
                    .security(ViewSqlSecurity.INVOKER).withCheckOption(ViewCheckOption.CASCADED).execute();
            rows = view.select("*").execute();
            r = rows.next();
            assertEquals("Arthur Clarke", r.getString("n"));
            assertEquals(99, r.getInt("a"));
            r = rows.next();
            assertEquals("Clifford Simak", r.getString("n"));
            assertEquals(112, r.getInt("a"));

            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.1"))) {
                this.session
                        .sql("CREATE ALGORITHM=MERGE DEFINER=`root`@`%` SQL SECURITY INVOKER VIEW `view_test_view_ddl_check` (`n`,`a`)"
                                + " AS select concat(`table_test_view_ddl`.`first_name`,' ',`table_test_view_ddl`.`last_name`) AS `name`,"
                                + "`table_test_view_ddl`.`age` AS `age` from `table_test_view_ddl`"
                                + " order by concat(`table_test_view_ddl`.`first_name`,' ',`table_test_view_ddl`.`last_name`) WITH CASCADED CHECK OPTION")
                        .execute();
            } else {
                this.session
                        .sql("CREATE ALGORITHM=MERGE DEFINER=`root`@`%` SQL SECURITY INVOKER VIEW `view_test_view_ddl_check`"
                                + " AS select concat(`table_test_view_ddl`.`first_name`,' ',`table_test_view_ddl`.`last_name`) AS `n`,"
                                + "`table_test_view_ddl`.`age` AS `a` from `table_test_view_ddl`"
                                + " order by concat(`table_test_view_ddl`.`first_name`,' ',`table_test_view_ddl`.`last_name`) WITH CASCADED CHECK OPTION")
                        .execute();
            }

            rows = this.session.sql("show create view `view_test_view_ddl`").execute();
            r = rows.next();
            String ddl = r.getString(1);

            rows = this.session.sql("show create view `view_test_view_ddl_check`").execute();
            r = rows.next();
            String ddlCheck = r.getString(1).replaceAll("view_test_view_ddl_check", "view_test_view_ddl");

            // TODO could cause problems on different server versions
            assertEquals(ddlCheck, ddl);

        } catch (Throwable t) {
            t.printStackTrace();
            throw t;
        } finally {
            sqlUpdate("drop table if exists collection_test_view_ddl");
            sqlUpdate("drop table if exists table_test_view_ddl");
            sqlUpdate("drop view if exists view_test_view_ddl");
            sqlUpdate("drop view if exists view_test_view_ddl_check");
        }
    }

    private DbObjectType getViewType(String schemaName, String view) {
        try {
            // check that the actual type is COLLECTION_VIEW
            Field f = SessionImpl.class.getDeclaredField("session");
            f.setAccessible(true);
            List<DatabaseObjectDescription> objects = ((MysqlxSession) f.get(this.session)).listObjects(schemaName, view);
            assertFalse(objects.isEmpty());
            // objects should contain exactly one element with matching this.name
            return objects.get(0).getObjectType();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * Tests fix for BUG#25575103, NPE FROM CREATETABLE() WHEN SOME OF THE INPUTS ARE NULL
     */
    @Test
    public void testBug25575103() {
        if (!this.isSetForXTests) {
            return;
        }

        assertThrows(XDevAPIError.class, "Parameter 'fkName' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("a", Type.INT, 3)).addForeignKey(null, null);
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'fkSpec' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("a", Type.INT, 3)).addForeignKey("some_fk", null);
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'name' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CreateTableStatement func = SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("a", Type.INT, 3)).addUniqueIndex(null,
                        (String[]) null);
                func.execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'column' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CreateTableStatement func = SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("a", Type.INT, 3)).addUniqueIndex("some_ui",
                        (String[]) null);
                func.execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'column' must not contain null values.", new Callable<Void>() {
            public Void call() throws Exception {
                CreateTableStatement func = SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("a", Type.INT, 3)).addUniqueIndex("some_ui", "",
                        null);
                func.execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'name' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CreateTableStatement func = SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("a", Type.INT, 3)).addIndex(null,
                        (String[]) null);
                func.execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'column' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CreateTableStatement func = SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("a", Type.INT, 3)).addIndex("", (String[]) null);
                func.execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'column' must not contain null values.", new Callable<Void>() {
            public Void call() throws Exception {
                CreateTableStatement func = SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("a", Type.INT, 3)).addIndex("", "", null);
                func.execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'pk' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CreateTableStatement func = SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("a", Type.INT, 3)).addPrimaryKey((String[]) null);
                func.execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'pk' must not contain null values.", new Callable<Void>() {
            public Void call() throws Exception {
                CreateTableStatement func = SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("a", Type.INT, 3)).addPrimaryKey("a", null);
                func.execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'colDef' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CreateTableStatement func = SchemaTest.this.schema.createTable("abc").addColumn(null);
                func.execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'columnName' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CreateTableStatement func = SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef(null, Type.INT, 3));
                func.execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'columnType' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CreateTableStatement func = SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("col", null, 3));
                func.execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'name' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                Table t1 = SchemaTest.this.schema.getCollectionAsTable(null);
                t1.insert("doc").values("{\"_id\":105}").execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'name' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                Table t1 = SchemaTest.this.schema.getTable(null);
                t1.existsInDatabase();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "ForeignKeyDefinition is incomplete, fields are empty.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("x", Type.STRING, 10)).addForeignKey("a", new ForeignKeyDef()).execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "ForeignKeyDefinition is incomplete, to-table isn't set.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("x", Type.STRING, 10)).addForeignKey("a", new ForeignKeyDef().fields("x"))
                        .execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "ForeignKeyDefinition is incomplete, to-columns are empty.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.createTable("abc").addColumn(new ColumnDef("x", Type.STRING, 10))
                        .addForeignKey("a", new ForeignKeyDef().fields("x").refersTo("cde", (String[]) null)).execute();
                return null;
            }
        });
    }

    /**
     * Tests fix for BUG#25575156, NPE FROM CREATEVIEW() WHEN SOME OF THE INPUTS ARE NULL
     */
    @Test
    public void testBug25575156() {
        if (!this.isSetForXTests) {
            return;
        }

        try {
            sqlUpdate("drop table if exists testBug25575156");
            this.schema.createCollection("testBug25575156", true);
            Table t1 = this.schema.getCollectionAsTable("testBug25575156");

            assertThrows(XDevAPIError.class, "Parameter 'columnStrLst' must not be null.", new Callable<Void>() {
                public Void call() throws Exception {
                    SchemaTest.this.schema.createView("view1", true).columns((String[]) null).definedAs(t1.select("*")).execute();
                    return null;
                }
            });

            assertThrows(XDevAPIError.class, "Parameter 'columnStrLst' must not contain null values.", new Callable<Void>() {
                public Void call() throws Exception {
                    SchemaTest.this.schema.createView("view1", true).columns("col1", null).definedAs(t1.select("*")).execute();
                    return null;
                }
            });

            assertThrows(XDevAPIError.class, "Parameter 'viewName' must not be null.", new Callable<Void>() {
                public Void call() throws Exception {
                    SchemaTest.this.schema.createView(null, true).columns("col1", "col2").definedAs(t1.select("c1, c2 as c1").where("c1>1")).execute();
                    return null;
                }
            });

            assertThrows(XDevAPIError.class, "Parameter 'selectStatement' must not be null.", new Callable<Void>() {
                public Void call() throws Exception {
                    SchemaTest.this.schema.createView("view1", false).columns("c1", "c3").definedAs(null).execute();// --> NPE
                    return null;
                }
            });
        } finally {
            sqlUpdate("drop table if exists testBug25575156");
        }
    }
}
