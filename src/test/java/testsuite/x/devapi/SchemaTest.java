/*
 * Copyright (c) 2015, 2021, Oracle and/or its affiliates.
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

package testsuite.x.devapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.Messages;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DatabaseObject;
import com.mysql.cj.xdevapi.DatabaseObject.DbObjectStatus;
import com.mysql.cj.xdevapi.Schema;
import com.mysql.cj.xdevapi.Schema.CreateCollectionOptions;
import com.mysql.cj.xdevapi.Schema.ModifyCollectionOptions;
import com.mysql.cj.xdevapi.Schema.Validation;
import com.mysql.cj.xdevapi.Schema.Validation.ValidationLevel;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionImpl;
import com.mysql.cj.xdevapi.SqlResult;
import com.mysql.cj.xdevapi.Table;

public class SchemaTest extends DevApiBaseTestCase {
    @BeforeEach
    public void setupCollectionTest() {
        assumeTrue(this.isSetForXTests, PropertyDefinitions.SYSP_testsuite_url_mysqlx + " must be set to run this test.");
        setupTestSession();
    }

    @AfterEach
    public void teardownCollectionTest() {
        destroyTestSession();
    }

    @Test
    public void testBasics() {
        assertEquals(this.schema, this.schema.getSchema());
        assertEquals(this.session, this.schema.getSession());
    }

    @Test
    public void testEquals() {
        Schema otherDefaultSchema = this.session.getDefaultSchema();
        assertFalse(otherDefaultSchema == this.schema);
        assertTrue(otherDefaultSchema.equals(this.schema));
        assertTrue(this.schema.equals(otherDefaultSchema));

        Session otherSession = new SessionImpl(this.testHostInfo);
        Schema diffSessionSchema = otherSession.getDefaultSchema();
        assertEquals(this.schema.getName(), diffSessionSchema.getName());
        assertFalse(this.schema.equals(diffSessionSchema));
        assertFalse(diffSessionSchema.equals(this.schema));
        otherSession.close();
    }

    @Test
    public void testToString() {
        // this will pass as long as the test database doesn't require identifier quoting
        assertEquals("Schema(" + getTestDatabase() + ")", this.schema.toString());
        Schema needsQuoted = this.session.getSchema("terrible'schema`name");
        assertEquals("Schema(`terrible'schema``name`)", needsQuoted.toString());
    }

    @Test
    public void testListCollections() {
        String collName1 = "test_list_collections1";
        String collName2 = "test_list_collections2";
        try {
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
        } finally {
            dropCollection(collName1);
            dropCollection(collName2);
        }
    }

    @Test
    public void testExists() {
        assertEquals(DbObjectStatus.EXISTS, this.schema.existsInDatabase());
        Schema nonExistingSchema = this.session.getSchema(getTestDatabase() + "_SHOULD_NOT_EXIST_0xCAFEBABE");
        assertEquals(DbObjectStatus.NOT_EXISTS, nonExistingSchema.existsInDatabase());
    }

    @Test
    public void testCreateCollection() {
        String collName = "testCreateCollection";
        try {
            dropCollection(collName);
            Collection coll = this.schema.createCollection(collName);
            try {
                this.schema.createCollection(collName);
                fail("Exception should be thrown trying to create a collection that already exists");
            } catch (XProtocolError ex) {
                // expected
                assertEquals(MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR, ex.getErrorCode());
            }
            try {
                this.schema.createCollection(collName, false);
                fail("Exception should be thrown trying to create a collection that already exists");
            } catch (XProtocolError ex) {
                // expected
                assertEquals(MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR, ex.getErrorCode());
            }
            Collection coll2 = this.schema.createCollection(collName, true);
            assertEquals(coll, coll2);
        } finally {
            dropCollection(collName);
        }
    }

    @Test
    public void testDropCollection() {
        String collName = "testDropCollection";
        dropCollection(collName);
        Collection coll = this.schema.getCollection(collName);
        assertEquals(DbObjectStatus.NOT_EXISTS, coll.existsInDatabase());

        // dropping non-existing collection should not fail
        this.schema.dropCollection(collName);

        coll = this.schema.createCollection(collName);
        assertEquals(DbObjectStatus.EXISTS, coll.existsInDatabase());
        this.schema.dropCollection(collName);

        // ensure that collection is dropped
        coll = this.schema.getCollection(collName);
        assertEquals(DbObjectStatus.NOT_EXISTS, coll.existsInDatabase());

        assertThrows(XProtocolError.class, "Parameter 'collectionName' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                SchemaTest.this.schema.dropCollection(null);
                return null;
            }
        });
    }

    @Test
    public void testListTables() {
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

            List<DatabaseObject> tables = new ArrayList<>();
            tables.addAll(this.schema.getTables());
            assertFalse(tables.contains(coll));
            assertTrue(tables.contains(table));
            assertTrue(tables.contains(view));

            tables = new ArrayList<>();
            tables.addAll(this.schema.getTables("%tables_t%"));
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
    public void testCreateCollectionWithOptions() {
        String collName1 = "testCreateCollection1";
        String collName2 = "testCreateCollection2";
        dropCollection(collName1);
        dropCollection(collName2);

        String sch1 = "{" + "\"id\":\"http://json-schema.org/geo\",\"$schema\":\"http://json-schema.org/draft-06/schema#\","
                + "\"description\":\"A geographical coordinate\",\"type\":\"object\",\"properties\":{\"latitude\":{\"type\":\"number\"},"
                + "\"longitude\":{\"type\":\"number\"}},\"required\":[\"latitude\",\"longitude\"]}";

        String sch2 = "{\"id\":\"http://json-schema.org/geo\",\"$schema\":\"http://json-schema.org/draft-06/schema#\","
                + "\"description\":\"The geographical coordinate\",\"type\":\"object\",\"properties\":{\"latitude\":{\"type\":\"number\"},"
                + "\"longitude\":{\"type\":\"number\"}},\"required\":[\"latitude\",\"longitude\"]}";

        String sch3 = "{\"id\":\"http://json-schema.org/idx\",\"$schema\":\"http://json-schema.org/draft-06/schema#\","
                + "\"type\":\"object\",\"properties\":{\"index\":{\"type\":\"number\"}},\"required\":[\"index\"]}";

        try {
            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.19"))) {
                // TSFR2b.1: Call createCollection with both level and schema options are set. Ensure that given values are set on server.
                Collection coll = this.schema.createCollection(collName1, //
                        new CreateCollectionOptions().setReuseExisting(false).setValidation(new Validation().setLevel(ValidationLevel.STRICT).setSchema(sch1)));
                SqlResult res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName1 + "`").execute();
                String def = res.next().getString(1);
                assertFalse(def.contains("NOT ENFORCED"));
                assertTrue(def.contains(sch1));
                coll.add("{\"latitude\": 20, \"longitude\": 30}").execute();

                Collection coll1 = this.schema.createCollection(collName1, //
                        new CreateCollectionOptions().setReuseExisting(true).setValidation(new Validation().setLevel(ValidationLevel.OFF).setSchema(sch1)));
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName1 + "`").execute();
                def = res.next().getString(1);
                assertFalse(def.contains("NOT ENFORCED"));
                assertTrue(def.contains(sch1));

                coll1 = this.schema.createCollection(collName1, //
                        new CreateCollectionOptions().setReuseExisting(true).setValidation(new Validation().setSchema(sch2)));
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName1 + "`").execute();
                def = res.next().getString(1);
                assertFalse(def.contains("NOT ENFORCED"));
                assertFalse(def.contains(sch2));
                assertTrue(def.contains(sch1));

                coll1 = this.schema.createCollection(collName1, //
                        new CreateCollectionOptions().setReuseExisting(true).setValidation(new Validation().setLevel(ValidationLevel.STRICT).setSchema(sch1)));
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName1 + "`").execute();
                def = res.next().getString(1);
                assertFalse(def.contains("NOT ENFORCED"));
                assertFalse(def.contains(sch2));
                assertTrue(def.contains(sch1));
                coll1.add("{\"latitude\": 30, \"longitude\": 40}").execute();

                // TSFR6d: Call createCollection(String collectionName) and createCollection(String collectionName, boolean reuseExisting) methods against server implementing WL#12965 and ensure they work as before.
                this.schema.createCollection(collName2);
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName2 + "`").execute();
                def = res.next().getString(1);
                assertTrue(def.contains("NOT ENFORCED"));
                dropCollection(collName2);

                this.schema.createCollection(collName2, new CreateCollectionOptions().setReuseExisting(false));
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName2 + "`").execute();
                def = res.next().getString(1);
                assertTrue(def.contains("NOT ENFORCED"));
                dropCollection(collName2);

                assertEquals(MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR, assertThrows(XProtocolError.class, new Callable<Void>() {
                    public Void call() throws Exception {
                        SchemaTest.this.schema.createCollection(collName1);
                        return null;
                    }
                }).getErrorCode());
                assertEquals(MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR, assertThrows(XProtocolError.class, new Callable<Void>() {
                    public Void call() throws Exception {
                        SchemaTest.this.schema.createCollection(collName1, false);
                        return null;
                    }
                }).getErrorCode());
                assertEquals(MysqlErrorNumbers.ER_TABLE_EXISTS_ERROR, assertThrows(XProtocolError.class, new Callable<Void>() {
                    public Void call() throws Exception {
                        SchemaTest.this.schema.createCollection(collName1, new CreateCollectionOptions().setReuseExisting(false));
                        return null;
                    }
                }).getErrorCode());
                Collection coll2 = this.schema.createCollection(collName1, true);
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName1 + "`").execute();
                def = res.next().getString(1);
                assertFalse(def.contains("NOT ENFORCED"));
                assertTrue(def.contains(sch1));

                coll2 = this.schema.createCollection(collName1, new CreateCollectionOptions().setReuseExisting(true));
                assertEquals(coll, coll2);
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName1 + "`").execute();
                def = res.next().getString(1);
                assertFalse(def.contains("NOT ENFORCED"));
                assertTrue(def.contains(sch1));

                // TSFR2b.2: Call createCollection with only schema option. Ensure the default level value is set on server.
                dropCollection(collName2);
                this.schema.createCollection(collName2, new CreateCollectionOptions().setValidation(new Validation().setSchema(sch1)));
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName2 + "`").execute();
                def = res.next().getString(1);
                if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.20"))) {
                    // After the Bug#30830962 fix when schema is set and level is omitted xplugin sets it to "strict"
                    assertFalse(def.contains("NOT ENFORCED"));
                } else {
                    assertTrue(def.contains("NOT ENFORCED"));
                }
                assertTrue(def.contains(sch1));

                // TSFR2b.3: Call createCollection with only level option. Ensure the default schema and the given level are set on server.
                dropCollection(collName2);
                this.schema.createCollection(collName2, new CreateCollectionOptions().setValidation(new Validation().setLevel(ValidationLevel.STRICT)));
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName2 + "`").execute();
                def = res.next().getString(1);
                assertFalse(def.contains("NOT ENFORCED"));
                assertTrue(def.contains("{\"type\":\"object\"}"));

                // TSFR5: Create collection with json schema and level `strict`. Try to insert document which doesn't match this schema, ensure that a server error being raised.
                assertThrows(XProtocolError.class, "ERROR 5180 \\(HY000\\) Document is not valid according to the schema assigned to collection.*",
                        new Callable<Void>() {
                            public Void call() throws Exception {
                                coll.add("{\"_id\": 1}").execute();
                                return null;
                            }
                        });

                // TSFR2a.1: Call modifyCollection with only level option. Ensure it's changed on server.
                this.schema.modifyCollection(collName1, new ModifyCollectionOptions().setValidation(new Validation().setLevel(ValidationLevel.OFF)));
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName1 + "`").execute();
                def = res.next().getString(1);
                assertTrue(def.contains("NOT ENFORCED"));
                assertTrue(def.contains(sch1));

                this.schema.modifyCollection(collName1, new ModifyCollectionOptions().setValidation(new Validation().setLevel(ValidationLevel.STRICT)));
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName1 + "`").execute();
                def = res.next().getString(1);
                assertFalse(def.contains("NOT ENFORCED"));
                assertTrue(def.contains(sch1));

                // TSFR2a.2: Call modifyCollection with only schema option. Ensure it's changed on server.
                this.schema.modifyCollection(collName1, new ModifyCollectionOptions().setValidation(new Validation().setSchema(sch2))); // sch2 is compatible with sch1
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName1 + "`").execute();
                def = res.next().getString(1);
                assertFalse(def.contains("NOT ENFORCED"));
                assertFalse(def.contains(sch1));
                assertTrue(def.contains(sch2));

                assertThrows(XProtocolError.class, "ERROR 5180 \\(HY000\\) Document is not valid according to the schema assigned to collection.*",
                        new Callable<Void>() {
                            public Void call() throws Exception {
                                SchemaTest.this.schema.modifyCollection(collName1,
                                        new ModifyCollectionOptions().setValidation(new Validation().setSchema(sch3))); // sch3 is incompatible with sch1
                                return null;
                            }
                        });

                // TSFR2a.3: Call modifyCollection with both level and schema options. Ensure they are changed on server.
                this.schema.modifyCollection(collName1,
                        new ModifyCollectionOptions().setValidation(new Validation().setLevel(ValidationLevel.OFF).setSchema(sch3)));
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName1 + "`").execute();
                def = res.next().getString(1);
                assertTrue(def.contains("NOT ENFORCED"));
                assertFalse(def.contains(sch2));
                assertTrue(def.contains(sch3));

                this.schema.modifyCollection(collName1,
                        new ModifyCollectionOptions().setValidation(new Validation().setLevel(ValidationLevel.STRICT).setSchema(sch1)));
                res = this.session.sql("SHOW CREATE TABLE `" + this.schema.getName() + "`.`" + collName1 + "`").execute();
                def = res.next().getString(1);
                assertFalse(def.contains("NOT ENFORCED"));
                assertFalse(def.contains(sch3));
                assertTrue(def.contains(sch1));

                // TSFR2a.4: Call modifyCollection with neither level nor schema options are set. Ensure Connector/J throws the XProtocolError
                assertThrows(XProtocolError.class, "ERROR 5020 \\(HY000\\) Arguments value used under \"validation\" must be an object with at least one field",
                        new Callable<Void>() {
                            public Void call() throws Exception {
                                SchemaTest.this.schema.modifyCollection(collName1, null);
                                return null;
                            }
                        });
                assertThrows(XProtocolError.class, "ERROR 5020 \\(HY000\\) Arguments value used under \"validation\" must be an object with at least one field",
                        new Callable<Void>() {
                            public Void call() throws Exception {
                                SchemaTest.this.schema.modifyCollection(collName1, new ModifyCollectionOptions());
                                return null;
                            }
                        });
                assertThrows(XProtocolError.class, "ERROR 5020 \\(HY000\\) Arguments value used under \"validation\" must be an object with at least one field",
                        new Callable<Void>() {
                            public Void call() throws Exception {
                                SchemaTest.this.schema.modifyCollection(collName1, new ModifyCollectionOptions().setValidation(new Validation()));
                                return null;
                            }
                        });

                // TSFR4: Try to create collection with an invalid json schema. Ensure that a server error being raised.
                assertThrows(XProtocolError.class, "ERROR 5182 \\(HY000\\) JSON validation schema .*", new Callable<Void>() {
                    public Void call() throws Exception {
                        SchemaTest.this.schema.createCollection("wrongSchema",
                                new CreateCollectionOptions().setValidation(new Validation()
                                        .setSchema("{\"id\": \"http://json-schema.org/geo\",\"$schema\":\"http://json-schema.org/draft-06/schema#\","
                                                + "\"description\":\"The geographical coordinate\",\"type\":\"object\",\"properties\":{\"latitude\":{" //
                                                + "\"type\":\"blablabla\"" // wrong type
                                                + "}},\"required\":[\"latitude\",\"foo\"]}")));
                        return null;
                    }
                });

            } else { // for old servers

                // TSFR6b: Call createCollection(collectionName, createCollectionOptions) method against server which doesn't support validation parameter for `create_collection` X Protocol command,
                // eg. MySQL 5.7. Ensure that server responds with error code 5015 and that Connector/J wraps it to WrongArgumentException with message
                // "The server doesn't support the requested operation. Please update the MySQL Server and or Client library".
                assertThrows(WrongArgumentException.class, Messages.getString("Schema.CreateCollection"), new Callable<Void>() {
                    public Void call() throws Exception {
                        SchemaTest.this.schema.createCollection(collName1,
                                new CreateCollectionOptions().setValidation(new Validation().setLevel(ValidationLevel.STRICT)
                                        .setSchema("{\"id\": \"http://json-schema.org/geo\",\"$schema\": \"http://json-schema.org/draft-06/schema#\","
                                                + "\"description\": \"A geographical coordinate\",\"type\": \"object\",\"properties\":"
                                                + "{\"latitude\": {\"type\": \"number\"},\"longitude\": {\"type\": \"number\"}},"
                                                + "\"required\": [\"latitude\", \"longitude\"]}")));
                        return null;
                    }
                });

                // TSFR6c: Call modifyCollection(String collectionName, ModifyCollectionOptions options) method against server which doesn't implement `modify_collection_options` X Protocol command,
                // eg. MySQL 5.7. Ensure that server responds with error code 5157 and that Connector/J wraps it to WrongArgumentException with message
                // "The server doesn't support the requested operation. Please update the MySQL Server and or Client library".
                assertThrows(WrongArgumentException.class, Messages.getString("Schema.CreateCollection"), new Callable<Void>() {
                    public Void call() throws Exception {
                        SchemaTest.this.schema.modifyCollection(collName1,
                                new ModifyCollectionOptions().setValidation(new Validation().setLevel(ValidationLevel.OFF)));
                        return null;
                    }
                });

                // TSFR6a: Call createCollection(String collectionName) and createCollection(String collectionName, boolean reuseExisting) methods against servers not implementing WL#12965 and ensure they work as before.
                // It's covered by testCreateCollection() test case.
            }
        } finally {
            dropCollection(collName1);
            dropCollection(collName2);
        }
    }
}
