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

import static com.mysql.cj.xdevapi.Expression.expr;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.AddResult;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DatabaseObject.DbObjectStatus;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DbDocImpl;
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.JsonArray;
import com.mysql.cj.xdevapi.JsonLiteral;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.Result;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.Schema;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.SqlResult;
import com.mysql.cj.xdevapi.XDevAPIError;

public class CollectionTest extends BaseCollectionTestCase {
    @Test
    public void testCount() {
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{'_id': '1', 'a':'a'}".replaceAll("'", "\"")).execute(); // Requires manual _id.
            this.collection.add("{'_id': '2', 'b':'b'}".replaceAll("'", "\"")).execute();
            this.collection.add("{'_id': '3', 'c':'c'}".replaceAll("'", "\"")).execute();
        } else {
            this.collection.add("{'a':'a'}".replaceAll("'", "\"")).execute();
            this.collection.add("{'b':'b'}".replaceAll("'", "\"")).execute();
            this.collection.add("{'c':'c'}".replaceAll("'", "\"")).execute();
        }
        assertEquals(3, this.collection.count());
        assertEquals(3, ((JsonNumber) this.collection.find().fields("COUNT(*) as cnt").execute().fetchOne().get("cnt")).getInteger().intValue());

        // test "not exists" message
        String collName = "testExists";
        dropCollection(collName);
        Collection coll = this.schema.getCollection(collName);
        assertThrows(XProtocolError.class, "Collection '" + collName + "' does not exist in schema '" + this.schema.getName() + "'", new Callable<Void>() {
            public Void call() throws Exception {
                coll.count();
                return null;
            }
        });
    }

    @Test
    public void testGetSchema() {
        String collName = "testExists";
        dropCollection(collName);
        Collection coll = this.schema.getCollection(collName);
        assertEquals(this.schema, coll.getSchema());
        this.schema.dropCollection(collName);
    }

    @Test
    public void testGetSession() {
        String collName = "testExists";
        dropCollection(collName);
        Collection coll = this.schema.getCollection(collName);
        assertEquals(this.session, coll.getSession());
        this.schema.dropCollection(collName);
    }

    @Test
    public void testExists() {
        String collName = "testExists";
        dropCollection(collName);
        Collection coll = this.schema.getCollection(collName);
        assertEquals(DbObjectStatus.NOT_EXISTS, coll.existsInDatabase());
        coll = this.schema.createCollection(collName);
        assertEquals(DbObjectStatus.EXISTS, coll.existsInDatabase());
        this.schema.dropCollection(collName);
    }

    @Test
    public void getNonExistentCollectionWithRequireExistsShouldThrow() {
        String collName = "testRequireExists";
        dropCollection(collName);
        assertThrows(WrongArgumentException.class, () -> this.schema.getCollection(collName, true));
    }

    @Test
    public void getNonExistentCollectionWithoutRequireExistsShouldNotThrow() {
        String collName = "testRequireExists";
        dropCollection(collName);
        this.schema.getCollection(collName, false);
    }

    @Test
    public void getExistentCollectionWithRequireExistsShouldNotThrow() {
        String collName = "testRequireExists";
        dropCollection(collName);
        this.schema.createCollection(collName);
        this.schema.getCollection(collName, true);
    }

    @Test
    public void createIndex() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4")), "MySQL 8.0.4+ is required to run this test.");

        /*
         * WL#11208 - DevAPI: Collection.createIndex
         */
        // FR1_1 Create an index on a single field.
        this.collection.createIndex("myIndex", new DbDocImpl().add("fields", new JsonArray()
                .addValue(new DbDocImpl().add("field", new JsonString().setValue("$.myField")).add("type", new JsonString().setValue("TEXT(200)")))));
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "t200", false, 200);
        this.collection.dropIndex("myIndex");

        // FR1_2 Create an index on a single field with all the possibles options.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(5)\", \"required\": true}]}");
        validateIndex("myIndex", this.collectionName, 1, false, true, false, "t5", false, 5);
        this.collection.dropIndex("myIndex");

        // FR1_3 Create an index on multiple fields.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(20)\"},"
                + " {\"field\": \"$.myField2\", \"type\": \"TEXT(10)\"}, {\"field\": \"$.myField3\", \"type\": \"INT\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "t20", false, 20);
        validateIndex("myIndex", this.collectionName, 2, false, false, false, "t10", false, 10);
        validateIndex("myIndex", this.collectionName, 3, false, false, false, "i", false, null);
        this.collection.dropIndex("myIndex");

        // FR1_4 Create an index on multiple fields with all the possibles options.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(20)\"},"
                + " {\"field\": \"$.myField2\", \"type\": \"TEXT(10)\", \"required\": true}, {\"field\": \"$.myField3\", \"type\": \"INT UNSIGNED\", \"required\": false}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "t20", false, 20);
        validateIndex("myIndex", this.collectionName, 2, false, true, false, "t10", false, 10);
        validateIndex("myIndex", this.collectionName, 3, false, false, false, "i_u", true, null);
        this.collection.dropIndex("myIndex");

        // FR1_5 Create an index using a geojson datatype field.
        this.collection.createIndex("myIndex",
                "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"GEOJSON\", \"required\": true}], \"type\":\"SPATIAL\"}");
        validateIndex("myIndex", this.collectionName, 1, false, true, false, "gj", false, 32);
        this.collection.dropIndex("myIndex");

        // FR1_6 Create an index using a geojson datatype field with all the possibles options.
        this.collection.createIndex("myIndex",
                "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"GEOJSON\", \"required\": true, \"options\": 2, \"srid\": 4326}], \"type\":\"SPATIAL\"}");
        validateIndex("myIndex", this.collectionName, 1, false, true, false, "gj", false, 32);
        this.collection.dropIndex("myIndex");

        // FR1_7 Create an index using a datetime field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"DATETIME\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "dd", false, null);
        this.collection.dropIndex("myIndex");

        // FR1_8 Create an index using a timestamp field.
        RowResult res = this.session.sql("select @@explicit_defaults_for_timestamp").execute();
        Row r = res.next();
        boolean explicitDefaultsForTimestamp = r.getBoolean(0);

        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TIMESTAMP\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, !explicitDefaultsForTimestamp, false, "ds", false, null);
        this.collection.dropIndex("myIndex");

        // FR1_9 Create an index using a time field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TIME\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "dt", false, null);
        this.collection.dropIndex("myIndex");

        // FR1_10 Create an index using a date field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"DATE\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "d", false, null);
        this.collection.dropIndex("myIndex");

        // FR1_11 Create an index using a numeric field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"NUMERIC UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "xn_u", true, null);
        this.collection.dropIndex("myIndex");

        // FR1_12 Create an index using a decimal field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"DECIMAL\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "xd", false, null);
        this.collection.dropIndex("myIndex");

        // FR1_13 Create an index using a double field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"DOUBLE UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "fd_u", true, null);
        this.collection.dropIndex("myIndex");

        // FR1_14 Create an index using a float field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"FLOAT UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "f_u", true, null);
        this.collection.dropIndex("myIndex");

        // FR1_15 Create an index using a real field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"REAL UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "fr_u", true, null);
        this.collection.dropIndex("myIndex");

        // FR1_16 Create an index using a bigint field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"BIGINT UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "ib_u", true, null);
        this.collection.dropIndex("myIndex");

        // FR1_17 Create an index using a integer field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"INTEGER UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "i_u", true, null);
        this.collection.dropIndex("myIndex");

        // FR1_18 Create an index using a mediumint field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"MEDIUMINT UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "im_u", true, null);
        this.collection.dropIndex("myIndex");

        // FR1_19 Create an index using a smallint field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"SMALLINT UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "is_u", true, null);
        this.collection.dropIndex("myIndex");

        // FR1_20 Create an index using a tinyint field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TINYINT UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "it_u", true, null);
        this.collection.dropIndex("myIndex");

        // FR5_2 Create an index with the name of an index that already exists.
        CollectionTest.this.collection.createIndex("myUniqueIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"INT\"}]}");
        assertThrows(XProtocolError.class, "ERROR 1061 \\(42000\\) Duplicate key name 'myUniqueIndex'", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myUniqueIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"INT\"}]}");
                return null;
            }
        });
        this.collection.dropIndex("myUniqueIndex");

        // FR5_4 Create an index where its definition is a JSON document but its structure is not valid.
        assertThrows(XDevAPIError.class, "Index definition does not contain fields.",
                () -> CollectionTest.this.collection.createIndex("myIndex", "{\"type\": \"INDEX\"}"));
        assertThrows(XDevAPIError.class, "Index definition 'fields' member must be an array of index fields.",
                () -> CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": 123}"));
        assertThrows(XDevAPIError.class, "Index field definition must be a JSON document.",
                () -> CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [123]}"));
        assertThrows(XDevAPIError.class, "Index field definition has no document path.",
                () -> CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [{\"type\": 123}]}"));
        assertThrows(XDevAPIError.class, "Index field 'field' member must be a string.",
                () -> CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": 123}]}"));
        assertThrows(XDevAPIError.class, "Index field definition has no field type.",
                () -> CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\"}]}"));
        assertThrows(XDevAPIError.class, "Index type must be a string.",
                () -> CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": 123}]}"));
        assertThrows(XDevAPIError.class, "Index field 'required' member must be boolean.", () -> CollectionTest.this.collection.createIndex("myIndex",
                "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(5)\", \"required\": \"yes\"}]}"));
        assertThrows(XDevAPIError.class, "Index field 'options' member must be integer.", () -> CollectionTest.this.collection.createIndex("myIndex",
                "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"GEOJSON\", \"options\": \"qqq\"}]}"));
        assertThrows(XDevAPIError.class, "Index field 'srid' member must be integer.", () -> CollectionTest.this.collection.createIndex("myIndex",
                "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"GEOJSON\", \"options\": 2, \"srid\": \"qqq\"}]}"));
        assertThrows(XDevAPIError.class, "Wrong index type 'SPTIAL'. Must be 'INDEX' or 'SPATIAL'.", () -> CollectionTest.this.collection.createIndex("myIndex",
                "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(5)\"}], \"type\":\"SPTIAL\"}"));
        assertThrows(XDevAPIError.class, "Index type must be a string.",
                () -> CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(5)\"}], \"type\":123}"));

        // FR5_6 Create a 'SPATIAL' index with "required" flag set to false.
        assertThrows(XProtocolError.class, "ERROR 5117 \\(HY000\\) GEOJSON index requires 'constraint.required: TRUE",
                () -> CollectionTest.this.collection.createIndex("myIndex",
                        "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"GEOJSON\", \"required\": false, \"options\": 2, \"srid\": 4326}], \"type\":\"SPATIAL\"}"));

        // FR5_8 Create an index specifying geojson options for non geojson data type.
        assertThrows(XDevAPIError.class, "Index field 'options' member should not be used for field types other than GEOJSON.",
                () -> CollectionTest.this.collection.createIndex("myIndex",
                        "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"TEXT(10)\", \"required\": true, \"options\": 2, \"srid\": 4326}]}"));
        assertThrows(XDevAPIError.class, "Index field 'srid' member should not be used for field types other than GEOJSON.",
                () -> CollectionTest.this.collection.createIndex("myIndex",
                        "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"TEXT(10)\", \"required\": true, \"srid\": 4326}]}"));

        // ET_2 Create an index specifying SPATIAL as the index type for a non spatial data type
        assertThrows(XProtocolError.class, "ERROR 3106 \\(HY000\\) 'Spatial index on virtual generated column' is not supported for generated columns.",
                () -> CollectionTest.this.collection.createIndex("myIndex",
                        "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(10)\"}], \"type\":\"SPATIAL\"}"));

        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.12"))) {
            this.collection.createIndex("myIndex",
                    "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"GEOJSON\", \"required\": true, \"options\": 2, \"srid\": 4326}],"
                            + " \"type\":\"INDEX\"}");
            validateIndex("myIndex", this.collectionName, 1, false, true, false, "gj", false, 32);
            this.collection.dropIndex("myIndex");
        } else {
            // ET_3 Create an index specifying INDEX as the index type for a spatial data type
            assertThrows(XProtocolError.class, "ERROR 1170 \\(42000\\) BLOB/TEXT column .+_gj_r_.+ used in key specification without a key length",
                    () -> CollectionTest.this.collection.createIndex("myIndex",
                            "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"GEOJSON\", \"required\": true, \"options\": 2, \"srid\": 4326}],"
                                    + " \"type\":\"INDEX\"}"));
        }

        // NPE checks
        assertThrows(XDevAPIError.class, "Parameter 'indexName' must not be null or empty.",
                () -> CollectionTest.this.collection.createIndex(null, "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(200)\"}]}"));
        assertThrows(XDevAPIError.class, "Parameter 'indexName' must not be null or empty.",
                () -> CollectionTest.this.collection.createIndex(" ", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(200)\"}]}"));
        assertThrows(XDevAPIError.class, "Parameter 'jsonIndexDefinition' must not be null or empty.",
                () -> CollectionTest.this.collection.createIndex("myIndex", (String) null));
        assertThrows(XDevAPIError.class, "Parameter 'jsonIndexDefinition' must not be null or empty.",
                () -> CollectionTest.this.collection.createIndex("myIndex", ""));
        assertThrows(XDevAPIError.class, "Parameter 'indexDefinition' must not be null or empty.",
                () -> CollectionTest.this.collection.createIndex("myIndex", (DbDoc) null));

        assertThrows(XDevAPIError.class, "The 'somekey' field is not allowed in indexDefinition.",
                () -> CollectionTest.this.collection.createIndex("myIndex", "{\"somekey\": 123}"));
        assertThrows(XDevAPIError.class, "The 'somefield' field is not allowed in indexField.",
                () -> CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [{\"somefield\": 123}]}"));
        assertThrows(XDevAPIError.class, "The 'unique' field is not allowed in indexDefinition.", () -> CollectionTest.this.collection.createIndex("myIndex",
                "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"INT\", \"required\": true}], \"unique\":true, \"type\":\"INDEX\"}"));

        // dropping non-existing index should not fail
        this.collection.dropIndex("non_existing_idx");
    }

    @Test
    public void createArrayIndex() throws Exception {
        /*
         * WL#12247 - DevAPI: indexing array fields
         */
        // TS.FR.4_1 - server not supporting array indexes; old-style indexes are created instead.
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.17"))) {
            this.collection.createIndex("myIndex", new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl()
                    .add("field", new JsonString().setValue("$.myField")).add("type", new JsonString().setValue("DATE")).add("array", JsonLiteral.TRUE))));
            validateIndex("myIndex", this.collectionName, 1, false, false, false, "d");
            this.collection.dropIndex("myIndex");

            this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"DATE\", \"array\": true}]}");
            validateIndex("myIndex", this.collectionName, 1, false, false, false, "d");
            this.collection.dropIndex("myIndex");

            return;
        }

        // MySQL 8.0.17 and above.
        String[] supArrTypes = new String[] { "BINARY(100)", "CHAR(100)", "DATE", "DATETIME", "TIME", "DECIMAL(10,2)", "SIGNED", "SIGNED INTEGER", "UNSIGNED",
                "UNSIGNED INTEGER" };
        String[] expArrTypes = new String[] { "binary(100)", "char(100)", "date", "datetime", "time", "decimal(10, 2)", "signed", "signed", "unsigned",
                "unsigned" };
        for (int i = 0; i < supArrTypes.length; i++) {
            String supArrType = supArrTypes[i];
            String expArrType = expArrTypes[i];

            // TS_1 - supported types in array indexes; empty collection.
            this.collection.createIndex("myIndex", new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl()
                    .add("field", new JsonString().setValue("$.myField")).add("type", new JsonString().setValue(supArrType)).add("array", JsonLiteral.TRUE))));
            validateIndex("myIndex", this.collectionName, 1, false, false, true, expArrType);
            this.collection.dropIndex("myIndex");

            this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"" + supArrType + "\", \"array\": true}]}");
            validateIndex("myIndex", this.collectionName, 1, false, false, true, expArrType);
            this.collection.dropIndex("myIndex");

            // TS_2 - supported types and 'required=false' in array indexes; empty collection.
            this.collection.createIndex("myIndex",
                    new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl().add("field", new JsonString().setValue("$.myField"))
                            .add("type", new JsonString().setValue(supArrType)).add("array", JsonLiteral.TRUE).add("required", JsonLiteral.FALSE))));
            validateIndex("myIndex", this.collectionName, 1, false, false, true, expArrType);
            this.collection.dropIndex("myIndex");

            this.collection.createIndex("myIndex",
                    "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"" + supArrType + "\", \"array\": true, \"required\": false}]}");
            validateIndex("myIndex", this.collectionName, 1, false, false, true, expArrType);
            this.collection.dropIndex("myIndex");

            // TS_3 - supported types and 'required=true' in array indexes; empty collection.
            assertThrows(XProtocolError.class, "ERROR 5017 \\(HY000\\) Unsupported argument specification for '\\$\\.myField'",
                    () -> this.collection.createIndex("myIndex",
                            new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl().add("field", new JsonString().setValue("$.myField"))
                                    .add("type", new JsonString().setValue(supArrType)).add("array", JsonLiteral.TRUE).add("required", JsonLiteral.TRUE)))));

            assertThrows(XProtocolError.class, "ERROR 5017 \\(HY000\\) Unsupported argument specification for '\\$\\.myField'",
                    () -> this.collection.createIndex("myIndex",
                            "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"" + supArrType + "\", \"array\": true, \"required\": true}]}"));
        }

        // TS_4 - multiple array indexes in same empty collection.
        this.collection.createIndex("myIndex1",
                new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl().add("field", new JsonString().setValue("$.myField"))
                        .add("type", new JsonString().setValue("CHAR(100)")).add("array", JsonLiteral.TRUE).add("required", JsonLiteral.FALSE))));
        this.collection.createIndex("myIndex2",
                new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl().add("field", new JsonString().setValue("$.myField"))
                        .add("type", new JsonString().setValue("CHAR(200)")).add("array", JsonLiteral.TRUE).add("required", JsonLiteral.FALSE))));
        validateIndex("myIndex1", this.collectionName, 1, false, false, true, "char(100)");
        validateIndex("myIndex2", this.collectionName, 1, false, false, true, "char(200)");
        this.collection.dropIndex("myIndex1");
        this.collection.dropIndex("myIndex2");

        this.collection.createIndex("myIndex1", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"CHAR(100)\", \"array\": true, \"required\": false}]}");
        this.collection.createIndex("myIndex2", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"CHAR(200)\", \"array\": true, \"required\": false}]}");
        validateIndex("myIndex1", this.collectionName, 1, false, false, true, "char(100)");
        validateIndex("myIndex2", this.collectionName, 1, false, false, true, "char(200)");
        this.collection.dropIndex("myIndex1");
        this.collection.dropIndex("myIndex2");

        // TS_5 - multiple array fields in same index.
        assertThrows(XProtocolError.class, "ERROR 1235 \\(42000\\) This version of MySQL doesn't yet support 'more than one multi-valued key part per index'",
                () -> this.collection.createIndex("myIndex",
                        new DbDocImpl().add("fields",
                                new JsonArray()
                                        .addValue(new DbDocImpl().add("field", new JsonString().setValue("$.myField1"))
                                                .add("type", new JsonString().setValue("CHAR(100)")).add("array", JsonLiteral.TRUE))
                                        .addValue(new DbDocImpl().add("field", new JsonString().setValue("$.myField2"))
                                                .add("type", new JsonString().setValue("CHAR(100)")).add("array", JsonLiteral.TRUE)))));

        assertThrows(XProtocolError.class, "ERROR 1235 \\(42000\\) This version of MySQL doesn't yet support 'more than one multi-valued key part per index'",
                () -> this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField1\", \"type\": \"CHAR(100)\", \"array\": true}, "
                        + "{\"field\": \"$.myField2\", \"type\": \"CHAR(100)\", \"array\": true}]}"));

        // TS_6 - supported index types that aren't supported in array fields; empty collection.
        String[] unsupArrTypes = new String[] { "INT", "INT UNSIGNED", "TINYINT", "TINYINT UNSIGNED", "SMALLINT", "SMALLINT UNSIGNED", "MEDIUMINT",
                "MEDIUMINT UNSIGNED", "INTEGER", "INTEGER UNSIGNED", "BIGINT", "BIGINT UNSIGNED", "REAL", "REAL UNSIGNED", "FLOAT", "FLOAT UNSIGNED", "DOUBLE",
                "DOUBLE UNSIGNED", "DECIMAL UNSIGNED", "NUMERIC", "NUMERIC UNSIGNED", "TIMESTAMP", "TEXT(100)", "GEOJSON" };
        for (int i = 0; i < unsupArrTypes.length; i++) {
            String unsupArrType = unsupArrTypes[i];

            assertThrows(XProtocolError.class,
                    "ERROR 5017 \\(HY000\\) Invalid or unsupported type specification for array index '" + unsupArrType.replace("(", "\\(").replace(")", "\\)")
                            + "'",
                    () -> this.collection.createIndex("myIndex",
                            new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl().add("field", new JsonString().setValue("$.myField"))
                                    .add("type", new JsonString().setValue(unsupArrType)).add("array", JsonLiteral.TRUE)))));

            assertThrows(XProtocolError.class,
                    "ERROR 5017 \\(HY000\\) Invalid or unsupported type specification for array index '" + unsupArrType.replace("(", "\\(").replace(")", "\\)")
                            + "'",
                    () -> this.collection.createIndex("myIndex",
                            "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"" + unsupArrType + "\", \"array\": true}]}"));
        }

        // TS_7 - supported index types in array indexes for numeric data; collection with numeric data.
        this.collection.add("{\"myField\": 1.23}").add("{\"myField\": [1, 2, 3]}").execute();
        supArrTypes = new String[] { "BINARY(100)", "CHAR(100)", "TIME", "DECIMAL(10,2)", "SIGNED", "SIGNED INTEGER", "UNSIGNED", "UNSIGNED INTEGER" };
        expArrTypes = new String[] { "binary(100)", "char(100)", "time", "decimal(10, 2)", "signed", "signed", "unsigned", "unsigned" };
        for (int i = 0; i < supArrTypes.length; i++) {
            String supArrType = supArrTypes[i];
            String expArrType = expArrTypes[i];

            this.collection.createIndex("myIndex", new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl()
                    .add("field", new JsonString().setValue("$.myField")).add("type", new JsonString().setValue(supArrType)).add("array", JsonLiteral.TRUE))));
            validateIndex("myIndex", this.collectionName, 1, false, false, true, expArrType);
            this.collection.dropIndex("myIndex");

            this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"" + supArrType + "\", \"array\": true}]}");
            validateIndex("myIndex", this.collectionName, 1, false, false, true, expArrType);
            this.collection.dropIndex("myIndex");
        }
        unsupArrTypes = new String[] { "DATE", "DATETIME" };
        for (int i = 0; i < unsupArrTypes.length; i++) {
            String unsupArrType = unsupArrTypes[i];

            assertThrows(XProtocolError.class, "ERROR 3751 \\(01000\\) Data truncated for functional index 'myIndex' at row 1",
                    () -> this.collection.createIndex("myIndex",
                            new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl().add("field", new JsonString().setValue("$.myField"))
                                    .add("type", new JsonString().setValue(unsupArrType)).add("array", JsonLiteral.TRUE)))));

            assertThrows(XProtocolError.class, "ERROR 3751 \\(01000\\) Data truncated for functional index 'myIndex' at row 1", () -> this.collection
                    .createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"" + unsupArrType + "\", \"array\": true}]}"));
        }
        this.collection.remove("true").execute();

        // TS_8 - supported index types in array indexes for string data; collection with string data.
        this.collection.add("{\"myField\": \"myData\"}").add("{\"myField\": [\"myData1\", \"myData2\", \"myData3\"]}").execute();
        supArrTypes = new String[] { "BINARY(100)", "CHAR(100)" };
        expArrTypes = new String[] { "binary(100)", "char(100)" };
        for (int i = 0; i < supArrTypes.length; i++) {
            String supArrType = supArrTypes[i];
            String expArrType = expArrTypes[i];

            this.collection.createIndex("myIndex", new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl()
                    .add("field", new JsonString().setValue("$.myField")).add("type", new JsonString().setValue(supArrType)).add("array", JsonLiteral.TRUE))));
            validateIndex("myIndex", this.collectionName, 1, false, false, true, expArrType);
            this.collection.dropIndex("myIndex");

            this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"" + supArrType + "\", \"array\": true}]}");
            validateIndex("myIndex", this.collectionName, 1, false, false, true, expArrType);
            this.collection.dropIndex("myIndex");
        }
        unsupArrTypes = new String[] { "DATE", "DATETIME", "TIME", "DECIMAL(10,2)", "SIGNED", "SIGNED INTEGER", "UNSIGNED", "UNSIGNED INTEGER" };
        for (int i = 0; i < unsupArrTypes.length; i++) {
            String unsupArrType = unsupArrTypes[i];

            String errMsg = i <= 2 ? "ERROR 3751 \\(01000\\) Data truncated for functional index 'myIndex' at row 1"
                    : "ERROR 3903 \\(22018\\) Invalid JSON value for CAST for functional index 'myIndex'\\.";
            assertThrows(XProtocolError.class, errMsg,
                    () -> this.collection.createIndex("myIndex",
                            new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl().add("field", new JsonString().setValue("$.myField"))
                                    .add("type", new JsonString().setValue(unsupArrType)).add("array", JsonLiteral.TRUE)))));

            assertThrows(XProtocolError.class, errMsg, () -> this.collection.createIndex("myIndex",
                    "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"" + unsupArrType + "\", \"array\": true}]}"));
        }
        this.collection.remove("true").execute();

        // TS_9 - supported index types in array indexes for datetime data; collection with datetime data.
        this.collection.add("{\"myField\": \"2019-01-01 05:20:50\"}").add("{\"myField\": [\"2019-01-01\", \"2018-12-31 22:33:44\", \"2018-01-01 01:02:03\"]}")
                .execute();
        supArrTypes = new String[] { "BINARY(100)", "CHAR(100)", "DATE", "DATETIME" };
        expArrTypes = new String[] { "binary(100)", "char(100)", "date", "datetime" };
        for (int i = 0; i < supArrTypes.length; i++) {
            String supArrType = supArrTypes[i];
            String expArrType = expArrTypes[i];

            this.collection.createIndex("myIndex", new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl()
                    .add("field", new JsonString().setValue("$.myField")).add("type", new JsonString().setValue(supArrType)).add("array", JsonLiteral.TRUE))));
            validateIndex("myIndex", this.collectionName, 1, false, false, true, expArrType);
            this.collection.dropIndex("myIndex");

            this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"" + supArrType + "\", \"array\": true}]}");
            validateIndex("myIndex", this.collectionName, 1, false, false, true, expArrType);
            this.collection.dropIndex("myIndex");
        }
        unsupArrTypes = new String[] { "TIME", "DECIMAL(10,2)", "SIGNED", "SIGNED INTEGER", "UNSIGNED", "UNSIGNED INTEGER" };
        for (int i = 0; i < unsupArrTypes.length; i++) {
            String unsupArrType = unsupArrTypes[i];

            String errMsg = i <= 0 ? "ERROR 3751 \\(01000\\) Data truncated for functional index 'myIndex' at row 1"
                    : "ERROR 3903 \\(22018\\) Invalid JSON value for CAST for functional index 'myIndex'\\.";
            assertThrows(XProtocolError.class, errMsg,
                    () -> this.collection.createIndex("myIndex",
                            new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl().add("field", new JsonString().setValue("$.myField"))
                                    .add("type", new JsonString().setValue(unsupArrType)).add("array", JsonLiteral.TRUE)))));

            assertThrows(XProtocolError.class, errMsg, () -> this.collection.createIndex("myIndex",
                    "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"" + unsupArrType + "\", \"array\": true}]}"));
        }
        this.collection.remove("true").execute();

        // TS_10 - supported index types in array indexes for time data; collection with time data.
        this.collection.add("{\"myField\": \"05:20:50\"}").add("{\"myField\": [\"01:01:01.555\", \"22:33:44.000000001\", \"23:59:59\"]}").execute();
        supArrTypes = new String[] { "BINARY(100)", "CHAR(100)", "TIME" };
        expArrTypes = new String[] { "binary(100)", "char(100)", "time" };
        for (int i = 0; i < supArrTypes.length; i++) {
            String supArrType = supArrTypes[i];
            String expArrType = expArrTypes[i];

            this.collection.createIndex("myIndex", new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl()
                    .add("field", new JsonString().setValue("$.myField")).add("type", new JsonString().setValue(supArrType)).add("array", JsonLiteral.TRUE))));
            validateIndex("myIndex", this.collectionName, 1, false, false, true, expArrType);
            this.collection.dropIndex("myIndex");

            this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"" + supArrType + "\", \"array\": true}]}");
            validateIndex("myIndex", this.collectionName, 1, false, false, true, expArrType);
            this.collection.dropIndex("myIndex");
        }
        unsupArrTypes = new String[] { "DATE", "DATETIME", "DECIMAL(10,2)", "SIGNED", "SIGNED INTEGER", "UNSIGNED", "UNSIGNED INTEGER" };
        for (int i = 0; i < unsupArrTypes.length; i++) {
            String unsupArrType = unsupArrTypes[i];

            String errMsg = i <= 1 ? "ERROR 3751 \\(01000\\) Data truncated for functional index 'myIndex' at row 1"
                    : "ERROR 3903 \\(22018\\) Invalid JSON value for CAST for functional index 'myIndex'\\.";
            assertThrows(XProtocolError.class, errMsg,
                    () -> this.collection.createIndex("myIndex",
                            new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl().add("field", new JsonString().setValue("$.myField"))
                                    .add("type", new JsonString().setValue(unsupArrType)).add("array", JsonLiteral.TRUE)))));

            assertThrows(XProtocolError.class, errMsg, () -> this.collection.createIndex("myIndex",
                    "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"" + unsupArrType + "\", \"array\": true}]}"));
        }
        this.collection.remove("true").execute();

        // TS.FR.3_1 - non-array indexes accept CHAR(n)
        this.collection.createIndex("myIndex", new DbDocImpl().add("fields", new JsonArray().addValue(new DbDocImpl()
                .add("field", new JsonString().setValue("$.myField")).add("type", new JsonString().setValue("CHAR(100)")).add("array", JsonLiteral.FALSE))));
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "c100", false, null);
        this.collection.dropIndex("myIndex");

        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"CHAR(100)\", \"array\": false}]}");
        validateIndex("myIndex", this.collectionName, 1, false, false, false, "c100", false, null);
        this.collection.dropIndex("myIndex");
    }

    private void validateIndex(String keyName, String collName, int sequence, boolean unique, boolean required, boolean array, String dataType)
            throws Exception {
        validateIndex(keyName, collName, sequence, unique, required, array, dataType, false, null);
    }

    private void validateIndex(String keyName, String collName, int sequence, boolean unique, boolean required, boolean array, String dataType,
            boolean unsigned, Integer length) throws Exception {
        boolean indexFound = false;

        SqlResult res = this.session.sql("show index from `" + collName + "`").execute();
        assertTrue(res.hasNext());

        for (Row row : res.fetchAll()) {
            if (keyName.equals(row.getString("Key_name"))) {
                if (sequence != row.getInt("Seq_in_index")) {
                    continue;
                }

                indexFound = true;
                assertEquals(collName.toUpperCase(), row.getString("Table").toUpperCase());
                assertEquals(unique ? "0" : "1", row.getString("Non_unique"));
                if (!array && row.getString("Column_name") != null) {
                    String[] columnNameTokens = row.getString("Column_name").toString().split("_");
                    assertEquals(dataType, unsigned ? columnNameTokens[1] + "_" + columnNameTokens[2] : columnNameTokens[1]);
                } else if (array && row.getString("Expression") != null) {
                    String expr = row.getString("Expression");
                    int typePos = expr.indexOf(" as ");
                    assertTrue(typePos >= 0, "Not an array index?");
                    expr = expr.substring(typePos + 4, expr.length() - 1);
                    assertTrue(expr.endsWith("array"));
                    expr = expr.substring(0, expr.indexOf(" array"));
                    assertEquals(dataType, expr);
                } else {
                    fail("Unexpected type of index");
                }

                //assertEquals("", row.getString("Collation")); // TODO enable when applicable
                assertEquals(length == null ? 0 : length.intValue(), row.getInt("Sub_part"));
                assertEquals(required ? "" : "YES", row.getString("Null"));
                break;
            }
        }

        if (!indexFound) {
            throw new Exception("Index not found.");
        }
    }

    private void validateArrayIndex(String keydName, String collName, int noFields) throws Exception {
        int indexFound = 0;
        boolean arrayExpr = false;

        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            SqlResult res = sess.sql("show index from `" + collName + "`").execute();
            assertTrue(res.hasNext());

            for (Row row : res.fetchAll()) {
                if (keydName.equals(row.getString("Key_name"))) {
                    indexFound++;
                    assertEquals(collName, row.getString("Table"));
                    String expr = row.getString("Expression");
                    System.out.println(expr);
                    if (expr != null) {
                        arrayExpr = true;
                    }
                }
            }
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }

        if ((indexFound != noFields) || (!arrayExpr)) {
            throw new Exception("Index not matching");
        }

    }

    /**
     * START testArrayIndexBasic tests
     * 
     * @throws Exception
     */
    @Test
    public void testArrayIndex001() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.17")), "MySQL 8.0.17+ is required to run this test.");

        int i = 0;
        String collname = "coll1";
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */

            coll.createIndex("intArrayIndex", "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"SIGNED INTEGER\", \"array\": true}]}");
            coll.createIndex("uintArrayIndex", "{\"fields\": [{\"field\": \"$.uintField\", \"type\": \"UNSIGNED INTEGER\", \"array\": true}]}");
            coll.createIndex("floatArrayIndex", "{\"fields\": [{\"field\": \"$.floatField\", \"type\": \"DECIMAL(10,2)\", \"array\": true}]}");
            coll.createIndex("dateArrayIndex", "{\"fields\": [{\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": true}]}");
            coll.createIndex("datetimeArrayIndex", "{\"fields\": [{\"field\": \"$.datetimeField\", \"type\": \"DATETIME\", \"array\": true}]}");
            coll.createIndex("timeArrayIndex", "{\"fields\": [{\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": true}]}");
            coll.createIndex("charArrayIndex", "{\"fields\": [{\"field\": \"$.charField\", \"type\": \"CHAR(256)\", \"array\": true}]}");
            coll.createIndex("binaryArrayIndex", "{\"fields\": [{\"field\": \"$.binaryField\", \"type\": \"BINARY(256)\", \"array\": true}]}");

            validateArrayIndex("intArrayIndex", "coll1", 1);
            validateArrayIndex("uintArrayIndex", "coll1", 1);
            validateArrayIndex("floatArrayIndex", "coll1", 1);
            validateArrayIndex("dateArrayIndex", "coll1", 1);
            validateArrayIndex("datetimeArrayIndex", "coll1", 1);
            validateArrayIndex("timeArrayIndex", "coll1", 1);
            validateArrayIndex("charArrayIndex", "coll1", 1);
            validateArrayIndex("binaryArrayIndex", "coll1", 1);

            coll.remove("true").execute();

            coll.add(
                    "{\"intField\" : [1,2,3], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"],\"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.4,53.6]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [11,12,3], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-29 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.1,52.9,53.0]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,34], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-7 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.7,53.6]}")
                    .execute();

            try {
                coll.add(
                        "{\"intField\" : \"[1,2,3]\", \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"],\"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.4,53.6]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("functional index"));
            }

            DocResult docs = coll.find(":intField in $.intField").bind("intField", 12).execute();
            i = 0;
            while (docs.hasNext()) {
                docs.next();
                i++;
            }

            assertTrue(i == 2);

            docs = coll.find(":uintField in $.uintField").bind("uintField", 52).execute();
            i = 0;
            while (docs.hasNext()) {
                docs.next();
                i++;
            }

            assertTrue(i == 3);

            docs = coll.find(":charField in $.charField").bind("charField", "abcd1").execute();
            i = 0;
            while (docs.hasNext()) {
                docs.next();
                i++;
            }

            assertTrue(i == 3);

            docs = coll.find(":binaryField in $.binaryField").bind("binaryField", "abcd1").execute();
            i = 0;
            while (docs.hasNext()) {
                docs.next();
                i++;
            }

            assertTrue(i == 3);

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    /**
     * START testArrayIndexBasic tests
     * 
     * @throws Exception
     */

    //@Test
    public void testArrayIndex002() throws Exception {
        System.out.println("testCreateIndexSanity");

        String collname = "coll1";
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */

            coll.createIndex("intArrayIndex", "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"SIGNED INTEGER\", \"array\": true, \"required\": true}]}");
            coll.createIndex("dateArrayIndex", "{\"fields\": [{\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": true, \"required\": true}]}");
            coll.createIndex("charArrayIndex", "{\"fields\": [{\"field\": \"$.charField\", \"type\": \"CHAR(256)\", \"array\": true, \"required\": true}]}");
            coll.createIndex("timeArrayIndex", "{\"fields\": [{\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": true, \"required\": true}]}");

            validateArrayIndex("intArrayIndex", "coll1", 1);
            validateArrayIndex("dateArrayIndex", "coll1", 1);
            validateArrayIndex("charArrayIndex", "coll1", 1);
            validateArrayIndex("timeArrayIndex", "coll1", 1);

            coll.add(
                    "{\"intField\" : [1,2,3], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [11,12,3], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,34], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                    .execute();

            try {
                coll.add(
                        "{\"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Document is missing a required field"));
            }

            try {
                coll.add(
                        "{\"intField\" : [1,2,3], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Document is missing a required field"));
            }

            try {
                coll.add(
                        "{\"intField\" : [11,12,3], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Document is missing a required field"));
            }

            try {
                coll.add(
                        "{\"intField\" : [11,12,3], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Document is missing a required field"));
            }

            try {
                coll.add(
                        "{\"intField\" : \"[12,23,34]\", \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("functional index"));
            }

            try {
                coll.add(
                        "{\"intField\" : 12, \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("functional index"));
            }

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void testArrayIndex003() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.17")), "MySQL 8.0.17+ is required to run this test.");

        String collname = "coll1";
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */

            try {
                coll.createIndex("multiArrayIndex",
                        "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"SIGNED INTEGER\", \"array\": true}, {\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": true}, {\"field\": \"$.charField\", \"type\": \"CHAR(256)\", \"array\": true}, {\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": true}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("This version of MySQL doesn't yet support"));
            }

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void testArrayIndex004() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.17")), "MySQL 8.0.17+ is required to run this test.");

        String collname = "coll1";
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */
            String indexString = "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"UNSIGNED INTEGER\", \"array\": true},"
                    + "{\"field\": \"$.charField\", \"type\": \"CHAR(255)\", \"array\": false},"
                    + "{\"field\": \"$.decimalField\", \"type\": \"DECIMAL\", \"array\": false},"
                    + "{\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": false},"
                    + "{\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": false},"
                    + "{\"field\": \"$.datetimeField\", \"type\": \"DATETIME\", \"array\": false}]}";

            coll.createIndex("multiArrayIndex", indexString);

            //coll.createIndex("multiArrayIndex", "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"INT\", \"array\": false}, {\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": false}, {\"field\": \"$.charField\", \"type\": \"CHAR(256)\", \"array\": true}, {\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": false}]}");

            validateArrayIndex("multiArrayIndex", "coll1", 6);

            coll.add(
                    "{\"intField\" : [12,23,34], \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : \"9999-12-31 23:59:59\", \"charField\" : \"abcd1\", \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : 51.2}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,25,34], \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : \"9999-12-31 23:59:59\", \"charField\" : \"abcd1\", \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : 51.2}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,35], \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : \"9999-12-31 23:59:59\", \"charField\" : \"abcd1\", \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : 51.2}")
                    .execute();
            coll.add(
                    "{\"intField\" : [18,23,34], \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : \"9999-12-31 23:59:59\", \"charField\" : \"abcd1\", \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : 51.2}")
                    .execute();

            try {
                coll.add(
                        "{\"intField\" : [12,23,34], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : \"10.30\"}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Incorrect date value"));
            }

            try {
                coll.add(
                        "{\"intField\" : 35, \"dateField\" : \"2019-1-1\", \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Incorrect time value"));
            }

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void testArrayIndex005() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.17")), "MySQL 8.0.17+ is required to run this test.");

        String collname = "coll1";
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */

            coll.createIndex("multiArrayIndex",
                    "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"INTEGER\", \"array\": false},"
                            + "{\"field\": \"$.charField\", \"type\": \"CHAR(255)\", \"array\": true},"
                            + "{\"field\": \"$.decimalField\", \"type\": \"DECIMAL\", \"array\": false},"
                            + "{\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": false},"
                            + "{\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": false},"
                            + "{\"field\": \"$.datetimeField\", \"type\": \"DATETIME\", \"array\": false}]}");

            validateArrayIndex("multiArrayIndex", "coll1", 6);

            coll.add(
                    "{\"intField\" : 12, \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : \"9999-12-31 23:59:59\", \"charField\" : [\"abcd1\", \"abcd2\", \"abcd3\", \"abcd4\"], \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : 51.2}")
                    .execute();
            coll.add(
                    "{\"intField\" : 12, \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : \"9999-12-31 23:59:59\", \"charField\" : [\"abcd1\", \"abcd2\", \"abcd3\", \"abcd4\"], \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : 51.2}")
                    .execute();
            coll.add(
                    "{\"intField\" : 12, \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : \"9999-12-31 23:59:59\", \"charField\" : [\"abcd1\", \"abcd2\", \"abcd3\", \"abcd4\"], \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : 51.2}")
                    .execute();
            coll.add(
                    "{\"intField\" : 18, \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : \"9999-12-31 23:59:59\", \"charField\" : [\"abcd1\", \"abcd2\", \"abcd3\", \"abcd4\"], \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : 51.2}")
                    .execute();

            try {
                coll.add(
                        "{\"intField\" : \"[12,23,34]\", \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Invalid JSON value for CAST to INTEGER from column json_extract at row"));
            }

            try {
                coll.add(
                        "{\"intField\" : 12, \"dateField\" : \"2019-1-1\", \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : \"10.30\"}")
                        .execute();
                // Behavior documented : assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Incorrect char value"));
            }

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void testArrayIndex006() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.17")), "MySQL 8.0.17+ is required to run this test.");

        String collname = "coll1";
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */

            coll.createIndex("multiArrayIndex",
                    "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"INTEGER\", \"array\": false},"
                            + "{\"field\": \"$.charField\", \"type\": \"CHAR(255)\", \"array\": false},"
                            + "{\"field\": \"$.decimalField\", \"type\": \"DECIMAL\", \"array\": true},"
                            + "{\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": false},"
                            + "{\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": false},"
                            + "{\"field\": \"$.datetimeField\", \"type\": \"DATETIME\", \"array\": false}]}");

            validateArrayIndex("multiArrayIndex", "coll1", 6);

            coll.add(
                    "{\"intField\" : 12, \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : \"9999-12-31 23:59:59\", \"charField\" : \"abcd1\", \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : [51.2, 57.6, 55.8]}")
                    .execute();
            coll.add(
                    "{\"intField\" : 12, \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : \"9999-12-31 23:59:59\", \"charField\" : \"abcd1\", \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : [51.2, 57.6, 55.8]}")
                    .execute();
            coll.add(
                    "{\"intField\" : 12, \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : \"9999-12-31 23:59:59\", \"charField\" : \"abcd1\", \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : [51.2, 57.6, 55.8]}")
                    .execute();
            coll.add(
                    "{\"intField\" : 18, \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : \"9999-12-31 23:59:59\", \"charField\" : \"abcd1\", \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : [51.2, 57.6, 55.8]}")
                    .execute();

            try {
                coll.add(
                        "{\"intField\" : \"[12,23,34]\", \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Invalid JSON value for CAST to INTEGER from column json_extract at row"));
            }

            try {
                coll.add(
                        "{\"intField\" : 12, \"dateField\" : \"2019-1-1\", \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Incorrect time value"));
            }

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void testArrayIndex007() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.17")), "MySQL 8.0.17+ is required to run this test.");

        String collname = "coll1";
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */

            coll.createIndex("multiArrayIndex",
                    "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"INTEGER\", \"array\": false},"
                            + "{\"field\": \"$.charField\", \"type\": \"CHAR(255)\", \"array\": false},"
                            + "{\"field\": \"$.decimalField\", \"type\": \"DECIMAL\", \"array\": false},"
                            + "{\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": false},"
                            + "{\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": false},"
                            + "{\"field\": \"$.datetimeField\", \"type\": \"DATETIME\", \"array\": true}]}");

            validateArrayIndex("multiArrayIndex", "coll1", 6);

            coll.add(
                    "{\"intField\" : 12, \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : \"abcd1\", \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : 51.2}")
                    .execute();
            coll.add(
                    "{\"intField\" : 12, \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : \"abcd1\", \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : 51.2}")
                    .execute();
            coll.add(
                    "{\"intField\" : 12, \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : \"abcd1\", \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : 51.2}")
                    .execute();
            coll.add(
                    "{\"intField\" : 18, \"uintField\" : [51,52,53], \"dateField\" : \"2019-1-1\", \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : \"abcd1\", \"binaryField\" : \"abcd1\", \"timeField\" : \"10.30\", \"decimalField\" : 51.2}")
                    .execute();

            try {
                coll.add(
                        "{\"intField\" : \"[12,23,34]\", \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Invalid JSON value for CAST to INTEGER from column json_extract at row"));
            }

            try {
                coll.add(
                        "{\"intField\" : 12, \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Incorrect date value"));
            }

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void testArrayIndex008() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.17")), "MySQL 8.0.17+ is required to run this test.");

        String collname = "coll1";
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */

            try {
                coll.createIndex("textArrayIndex", "{\"fields\": [{\"field\": \"$.textField\", \"type\": \"TEXT\", \"array\": true}]}");
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("index"));
            }

            try {
                coll.createIndex("boolArrayIndex", "{\"fields\": [{\"field\": \"$.boolField\", \"type\": \"BOOL\", \"array\": true}]}");
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("index"));
            }

            try {
                coll.createIndex("blobIndex", "{\"fields\": [{\"field\": \"$.blobField\", \"type\": \"BLOB\", \"array\": true}]}");
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("index"));
            }

            try {
                coll.createIndex("sintIndex", "{\"fields\": [{\"field\": \"$.sinField\", \"type\": \"SMALLINT\", \"array\": true}]}");
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("index"));
            }

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void testArrayIndex009() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.17")), "MySQL 8.0.17+ is required to run this test.");

        String collname = "coll1";
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */
            coll.createIndex("intArrayIndex", "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"UNSIGNED\", \"array\" : true}], \"type\" : \"INDEX\"}");
            coll.createIndex("dateArrayIndex", "{\"fields\": [{\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\" : true}], \"type\" : \"INDEX\"}");
            coll.createIndex("charArrayIndex", "{\"fields\": [{\"field\": \"$.charField\", \"type\": \"CHAR(255)\", \"array\" : true}], \"type\" : \"INDEX\"}");
            coll.createIndex("timeArrayIndex", "{\"fields\": [{\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\" : true}], \"type\" : \"INDEX\"}");

            validateArrayIndex("intArrayIndex", "coll1", 1);
            validateArrayIndex("dateArrayIndex", "coll1", 1);
            validateArrayIndex("charArrayIndex", "coll1", 1);
            validateArrayIndex("timeArrayIndex", "coll1", 1);

            coll.add(
                    "{\"intField\" : [1,2,3], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [11,12,3], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,34], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                    .execute();
            coll.add(
                    "{\"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [1,2,3], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [11,12,3], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [11,12,3], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"]}")
                    .execute();

            try {
                coll.add(
                        "{\"intField\" : \"[12,23,34]\", \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("functional index"));
            }

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void testArrayIndex010() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.17")), "MySQL 8.0.17+ is required to run this test.");

        String collname = "coll1";
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */

            try {
                coll.createIndex("multiArrayIndex1", "{\"fields\": [{\"field\": \"$.charField\", \"type\":\"CHAR(128)\", \"array\": true},"
                        + "{\"field\": \"$.binaryField\", \"type\":\"BINARY(128)\"}," + "{\"field\": \"$.intField\", \"type\":\"SIGNED INTEGER\"},"
                        + "{\"field\": \"$.intField2\", \"type\":\"SIGNED\"}," + "{\"field\": \"$.uintField\", \"type\":\"UNSIGNED\"},"
                        + "{\"field\": \"$.uintField2\", \"type\":\"UNSIGNED INTEGER\"}," + "{\"field\": \"$.dateField\", \"type\":\"DATE\"},"
                        + "{\"field\": \"$.datetimeField\", \"type\":\"DATETIME\"}," + "{\"field\": \"$.decimalField\", \"type\":\"DECIMAL(20,9)\"}" + "]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Invalid or unsupported type specification 'BINARY(128)'"));
            }

            coll.add("{\"intField\" : 1, \"dateField\" : \"2019-3-1\", \"charField\" : \"abcd1\", \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                    .execute();
            coll.add("{\"intField\" : 2, \"dateField\" : \"2019-5-1\", \"charField\" : \"abcd2\", \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                    .execute();
            coll.add("{\"intField\" : 3, \"dateField\" : \"2019-7-1\", \"charField\" : \"abcd3\", \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"]}")
                    .execute();

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void testArrayIndex011() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.17")), "MySQL 8.0.17+ is required to run this test.");

        String collname = "coll1";
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */

            coll.createIndex("intArrayIndex", "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"SIGNED INTEGER\", \"array\": true}]}");
            coll.createIndex("uintArrayIndex", "{\"fields\": [{\"field\": \"$.uintField\", \"type\": \"UNSIGNED INTEGER\", \"array\": true}]}");
            coll.createIndex("floatArrayIndex", "{\"fields\": [{\"field\": \"$.floatField\", \"type\": \"DECIMAL(10,2)\", \"array\": true}]}");
            coll.createIndex("dateArrayIndex", "{\"fields\": [{\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": true}]}");
            coll.createIndex("datetimeArrayIndex", "{\"fields\": [{\"field\": \"$.datetimeField\", \"type\": \"DATETIME\", \"array\": true}]}");
            coll.createIndex("timeArrayIndex", "{\"fields\": [{\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": true}]}");
            coll.createIndex("charArrayIndex", "{\"fields\": [{\"field\": \"$.charField\", \"type\": \"CHAR(256)\", \"array\": true}]}");
            coll.createIndex("binaryArrayIndex", "{\"fields\": [{\"field\": \"$.binaryField\", \"type\": \"BINARY(256)\", \"array\": true}]}");

            validateArrayIndex("intArrayIndex", "coll1", 1);
            validateArrayIndex("uintArrayIndex", "coll1", 1);
            validateArrayIndex("floatArrayIndex", "coll1", 1);
            validateArrayIndex("dateArrayIndex", "coll1", 1);
            validateArrayIndex("datetimeArrayIndex", "coll1", 1);
            validateArrayIndex("timeArrayIndex", "coll1", 1);
            validateArrayIndex("charArrayIndex", "coll1", 1);
            validateArrayIndex("binaryArrayIndex", "coll1", 1);

            coll.add(
                    "{\"intField\" : [], \"uintField\" : [], \"dateField\" : [], \"datetimeField\" : [], \"charField\" : [], \"binaryField\" : [],\"timeField\" : [], \"floatField\" : []}")
                    .execute();
            coll.add(
                    "{\"intField\" : [], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-29 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.1,52.9,53.0]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,34], \"uintField\" : [], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-7 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.7,53.6]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,34], \"uintField\" : [51,52,53], \"dateField\" : [], \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-7 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.7,53.6]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,34], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.7,53.6]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,34], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-7 23:59:59\"], \"charField\" : [], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.7,53.6]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,34], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-7 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.7,53.6]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,34], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-7 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [], \"floatField\" : [51.2,52.7,53.6]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,34], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-7 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : []}")
                    .execute();

            try {
                coll.add(
                        "{\"intField\" : [1,[2, 3],4], \"uintField\" : [51,[52, 53],54], \"dateField\" : [\"2019-1-1\", [\"2019-2-1\", \"2019-3-1\"], \"2019-4-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", [\"9999-12-31 23:59:59\"], \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", [\"abcd1\", \"abcd2\"], \"abcd4\"],\"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,[52.4],53.6]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Cannot store an array or an object in a scalar key part of the index"));
            }

            try {
                coll.add(
                        "{\"intField\" : \"\", \"uintField\" : [51,[52, 53],54], \"dateField\" : [\"2019-1-1\", [\"2019-2-1\", \"2019-3-1\"], \"2019-4-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"],\"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.4,53.6]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                if (mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.18"))) {
                    assertTrue(e.getMessage().contains("Cannot store an array or an object in a scalar key part of the index"));
                } else {
                    assertTrue(e.getMessage().contains("functional index"));
                }
            }

            try {
                coll.add(
                        "{\"intField\" : [1,5,4], \"dateField\" : \"\", \"datetimeField\" : \"\", \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"],\"timeField\" : \"\", \"floatField\" : [51.2,52.4,53.6]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("functional index"));
            }

            try {
                coll.add(
                        "{\"intField\" : [1,5,4], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\", \"2019-4-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : \"\", \"binaryField\" : \"\", \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.4,53.6]}")
                        .execute();
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("functional index"));
            }

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    /**
     * START testArrayIndexBasic tests
     * 
     * @throws Exception
     */
    @Test
    public void testArrayIndex012() throws Exception {
        System.out.println("testCreateIndexSanity");

        String collname = "coll1";
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */

            try {
                coll.createIndex("intArrayIndex", "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"SIGNED INTEGER\", \"array\": \"\"}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("uintArrayIndex", "{\"fields\": [{\"field\": \"$.uintField\", \"type\": \"UNSIGNED INTEGER\", \"array\": \"\"}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("floatArrayIndex", "{\"fields\": [{\"field\": \"$.floatField\", \"type\": \"DECIMAL(10,2)\", \"array\": \"\"}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("dateArrayIndex", "{\"fields\": [{\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": \"\"}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("datetimeArrayIndex", "{\"fields\": [{\"field\": \"$.datetimeField\", \"type\": \"DATETIME\", \"array\": \"\"}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("timeArrayIndex", "{\"fields\": [{\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": \"\"}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("charArrayIndex", "{\"fields\": [{\"field\": \"$.charField\", \"type\": \"CHAR(256)\", \"array\": \"\"}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("binaryArrayIndex", "{\"fields\": [{\"field\": \"$.binaryField\", \"type\": \"BINARY(256)\", \"array\": \"\"}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("intArrayIndex", "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"SIGNED INTEGER\", \"array\": null}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("uintArrayIndex", "{\"fields\": [{\"field\": \"$.uintField\", \"type\": \"UNSIGNED INTEGER\", \"array\": null}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("floatArrayIndex", "{\"fields\": [{\"field\": \"$.floatField\", \"type\": \"DECIMAL(10,2)\", \"array\": null}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("dateArrayIndex", "{\"fields\": [{\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": null}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("datetimeArrayIndex", "{\"fields\": [{\"field\": \"$.datetimeField\", \"type\": \"DATETIME\", \"array\": null}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("timeArrayIndex", "{\"fields\": [{\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": null}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("charArrayIndex", "{\"fields\": [{\"field\": \"$.charField\", \"type\": \"CHAR(256)\", \"array\": null}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("binaryArrayIndex", "{\"fields\": [{\"field\": \"$.binaryField\", \"type\": \"BINARY(256)\", \"array\": null}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("intArrayIndex", "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"SIGNED INTEGER\", \"array\": []}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("uintArrayIndex", "{\"fields\": [{\"field\": \"$.uintField\", \"type\": \"UNSIGNED INTEGER\", \"array\": []}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("floatArrayIndex", "{\"fields\": [{\"field\": \"$.floatField\", \"type\": \"DECIMAL(10,2)\", \"array\": []}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("dateArrayIndex", "{\"fields\": [{\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": []}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("datetimeArrayIndex", "{\"fields\": [{\"field\": \"$.datetimeField\", \"type\": \"DATETIME\", \"array\": []}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("timeArrayIndex", "{\"fields\": [{\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": []}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("charArrayIndex", "{\"fields\": [{\"field\": \"$.charField\", \"type\": \"CHAR(256)\", \"array\": []}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            try {
                coll.createIndex("binaryArrayIndex", "{\"fields\": [{\"field\": \"$.binaryField\", \"type\": \"BINARY(256)\", \"array\": []}]}");
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("Index field 'array' member must be boolean."));
            }

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void testArrayIndex013() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.17")), "MySQL 8.0.17+ is required to run this test.");

        String collname = "coll1";
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            coll.add(
                    "{\"intField\" : [1,2,3], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"],\"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.4,53.6]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [11,12,3], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-29 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.1,52.9,53.0]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,34], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-7 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.7,53.6]}")
                    .execute();

            /* create basic index */

            coll.createIndex("intArrayIndex", "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"SIGNED INTEGER\", \"array\": true}]}");
            coll.createIndex("uintArrayIndex", "{\"fields\": [{\"field\": \"$.uintField\", \"type\": \"UNSIGNED INTEGER\", \"array\": true}]}");
            coll.createIndex("floatArrayIndex", "{\"fields\": [{\"field\": \"$.floatField\", \"type\": \"DECIMAL(10,2)\", \"array\": true}]}");
            coll.createIndex("dateArrayIndex", "{\"fields\": [{\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": true}]}");
            coll.createIndex("datetimeArrayIndex", "{\"fields\": [{\"field\": \"$.datetimeField\", \"type\": \"DATETIME\", \"array\": true}]}");
            coll.createIndex("timeArrayIndex", "{\"fields\": [{\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": true}]}");
            coll.createIndex("charArrayIndex", "{\"fields\": [{\"field\": \"$.charField\", \"type\": \"CHAR(256)\", \"array\": true}]}");
            coll.createIndex("binaryArrayIndex", "{\"fields\": [{\"field\": \"$.binaryField\", \"type\": \"BINARY(256)\", \"array\": true}]}");

            validateArrayIndex("intArrayIndex", "coll1", 1);
            validateArrayIndex("uintArrayIndex", "coll1", 1);
            validateArrayIndex("floatArrayIndex", "coll1", 1);
            validateArrayIndex("dateArrayIndex", "coll1", 1);
            validateArrayIndex("datetimeArrayIndex", "coll1", 1);
            validateArrayIndex("timeArrayIndex", "coll1", 1);
            validateArrayIndex("charArrayIndex", "coll1", 1);
            validateArrayIndex("binaryArrayIndex", "coll1", 1);

            coll.remove("true").execute();

            coll.add(
                    "{\"intField\" : [1,2,3], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"],\"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.4,53.6]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [11,12,3], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-29 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.1,52.9,53.0]}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,34], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-7 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.7,53.6]}")
                    .execute();

            try {
                coll.add(
                        "{\"intField\" : \"[1,2,3]\", \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"],\"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.4,53.6]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("functional index"));
            }

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void testArrayIndex014() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.17")), "MySQL 8.0.17+ is required to run this test.");

        String collname = "coll1";
        DbDoc doc = null;
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */

            coll.createIndex("intArrayIndex", "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"SIGNED INTEGER\", \"array\": true}]}");
            coll.createIndex("uintArrayIndex", "{\"fields\": [{\"field\": \"$.uintField\", \"type\": \"UNSIGNED INTEGER\", \"array\": true}]}");
            coll.createIndex("floatArrayIndex", "{\"fields\": [{\"field\": \"$.floatField\", \"type\": \"DECIMAL(10,2)\", \"array\": true}]}");
            coll.createIndex("dateArrayIndex", "{\"fields\": [{\"field\": \"$.dateField\", \"type\": \"DATE\", \"array\": true}]}");
            coll.createIndex("datetimeArrayIndex", "{\"fields\": [{\"field\": \"$.datetimeField\", \"type\": \"DATETIME\", \"array\": true}]}");
            coll.createIndex("timeArrayIndex", "{\"fields\": [{\"field\": \"$.timeField\", \"type\": \"TIME\", \"array\": true}]}");
            coll.createIndex("charArrayIndex", "{\"fields\": [{\"field\": \"$.charField\", \"type\": \"CHAR(256)\", \"array\": true}]}");
            coll.createIndex("binaryArrayIndex", "{\"fields\": [{\"field\": \"$.binaryField\", \"type\": \"BINARY(256)\", \"array\": true}]}");

            validateArrayIndex("intArrayIndex", "coll1", 1);
            validateArrayIndex("uintArrayIndex", "coll1", 1);
            validateArrayIndex("floatArrayIndex", "coll1", 1);
            validateArrayIndex("dateArrayIndex", "coll1", 1);
            validateArrayIndex("datetimeArrayIndex", "coll1", 1);
            validateArrayIndex("timeArrayIndex", "coll1", 1);
            validateArrayIndex("charArrayIndex", "coll1", 1);
            validateArrayIndex("binaryArrayIndex", "coll1", 1);

            coll.remove("true").execute();

            coll.add(
                    "{\"intField\" : [1,2,3], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"],\"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.4,53.6],\"dateFieldWOI\" : \"2019-1-1\"}")
                    .execute();
            coll.add(
                    "{\"intField\" : [11,12,3], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-29 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.1,52.9,53.0],\"dateFieldWOI\" : \"2019-1-1\"}")
                    .execute();
            coll.add(
                    "{\"intField\" : [12,23,34], \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-7 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"timeField\" : [\"10.30\", \"7.30\", \"12.30\"], \"floatField\" : [51.2,52.7,53.6],\"dateFieldWOI\" : \"2019-2-1\"}")
                    .execute();

            try {
                coll.add(
                        "{\"intField\" : \"[1,2,3]\", \"uintField\" : [51,52,53], \"dateField\" : [\"2019-1-1\", \"2019-2-1\", \"2019-3-1\"], \"datetimeField\" : [\"9999-12-30 23:59:59\", \"9999-12-31 23:59:59\", \"9999-12-31 23:59:59\"], \"charField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"], \"binaryField\" : [\"abcd1\", \"abcd1\", \"abcd2\", \"abcd4\"],\"timeField\" : [\"10.30\", \"11.30\", \"12.30\"], \"floatField\" : [51.2,52.4,53.6]}")
                        .execute();
                assertTrue(false);
            } catch (Exception e) {
                System.out.println("ERROR : " + e.getMessage());
                assertTrue(e.getMessage().contains("functional index"));
            }

            DocResult docs = coll.find(":intField in $.intField").bind("intField", 12).execute();
            doc = null;
            int i = 0;
            while (docs.hasNext()) {
                doc = docs.next();
                i++;
            }

            assertTrue(i == 2);

            docs = coll.find(":uintField in $.uintField").bind("uintField", 52).execute();
            doc = null;
            i = 0;
            while (docs.hasNext()) {
                doc = docs.next();
                i++;
            }

            assertTrue(i == 3);

            docs = coll.find(":charField in $.charField").bind("charField", "abcd1").execute();
            doc = null;
            i = 0;
            while (docs.hasNext()) {
                doc = docs.next();
                i++;
            }

            assertTrue(i == 3);

            docs = coll.find(":binaryField in $.binaryField").bind("binaryField", "abcd1").execute();
            doc = null;
            i = 0;
            while (docs.hasNext()) {
                doc = docs.next();
                i++;
            }

            assertTrue(i == 3);

            docs = coll.find(":floatField in $.floatField").bind("floatField", 51.2).execute();
            doc = null;
            i = 0;
            while (docs.hasNext()) {
                doc = docs.next();
                i++;
            }

            System.out.println("Count = " + i);
            assertTrue(i == 2);

            docs = coll.find("CAST(CAST('2019-2-1' as DATE) as JSON) in $.dateField").execute();
            doc = null;
            i = 0;
            while (docs.hasNext()) {
                doc = docs.next();
                i++;
            }

            System.out.println("Count = " + i);
            assertTrue(i == 3);

            docs = coll.find("CAST(CAST('2019-2-1' as DATE) as JSON) not in $.dateField").execute();
            doc = null;
            i = 0;
            while (docs.hasNext()) {
                doc = docs.next();
                i++;
            }
            System.out.println("Using NOT IN");
            System.out.println("Count = " + i);
            //assertTrue(i == 0);

            docs = coll.find("'2019-1-1' not in $.dateFieldWOI").execute();
            doc = null;
            i = 0;
            while (docs.hasNext()) {
                doc = docs.next();
                System.out.println((((JsonString) doc.get("dateFieldWOI")).getString()));
                i++;
            }
            System.out.println("Using NOT IN Without Index");
            System.out.println("Count = " + i);

            docs = coll.find("CAST(CAST('2019-2-1' as DATE) as JSON) overlaps $.dateField").execute();
            doc = null;
            i = 0;
            while (docs.hasNext()) {
                doc = docs.next();
                i++;
            }

            System.out.println("Count = " + i);
            assertTrue(i == 3);

            docs = coll.find("CAST(CAST('2019-2-1' as DATE) as JSON) not overlaps $.dateField").execute();
            doc = null;
            i = 0;
            while (docs.hasNext()) {
                doc = docs.next();
                i++;
            }
            System.out.println("Using NOT OVERLAPS");
            System.out.println("Count = " + i);
            //assertTrue(i == 0);

            docs = coll.find("CAST(CAST(:datetimeField as DATETIME) as JSON) in $.datetimeField").bind("datetimeField", "9999-12-30 23:59:59").execute();
            doc = null;
            i = 0;
            while (docs.hasNext()) {
                doc = docs.next();
                i++;
            }

            System.out.println("Count = " + i);
            assertTrue(i == 2);

            docs = coll.find("CAST(CAST(:timeField as TIME) as JSON) in $.timeField").bind("timeField", "7.30").execute();
            doc = null;
            i = 0;
            while (docs.hasNext()) {
                doc = docs.next();
                i++;
            }

            System.out.println("Count = " + i);
            assertTrue(i == 1);

            //Integration scenarios between Index and Overlaps. Explicit casting added due to server Bug#29752056. NOT IN and NOT OVERLAPS doesn't require explicit casting
            docs = coll.find("CAST(CAST('2019-2-1' as DATE) as JSON) in $.dateField").execute();
            //System.out.println("Number of rows using IN with indexed array = "+docs.count());
            assertTrue(docs.count() == 3);

            docs = coll.find("'2019-2-1' not in $.dateField").execute();
            //System.out.println("Number of rows using NOT IN without casting = "+docs.count());
            assertTrue(docs.count() == 0);

            docs = coll.find("CAST(CAST('2019-2-1' as DATE) as JSON) overlaps $.dateField").execute();
            //System.out.println("Number of rows using OVERLAPS with indexed array = "+docs.count());
            assertTrue(docs.count() == 3);

            docs = coll.find("'2019-2-1' not overlaps $.dateField").execute();
            //System.out.println("Number of rows using NOT OVERLAPS without casting = "+docs.count());
            assertTrue(docs.count() == 0);

            //Integration scenarios for time
            docs = coll.find("CAST(CAST(:timeField as TIME) as JSON) in $.timeField").bind("timeField", "7.30").execute();
            assertTrue(docs.count() == 1);

            docs = coll.find(":timeField not in $.timeField").bind("timeField", "7.30").execute();
            assertTrue(docs.count() == 2);

            docs = coll.find("CAST(CAST(:timeField as TIME) as JSON) overlaps $.timeField").bind("timeField", "7.30").execute();
            assertTrue(docs.count() == 1);

            docs = coll.find(":timeField not overlaps $.timeField").bind("timeField", "7.30").execute();
            assertTrue(docs.count() == 2);

            //Integration scenarios for datetime
            docs = coll.find("CAST(CAST(:datetimeField as DATETIME) as JSON) in $.datetimeField").bind("datetimeField", "9999-12-30 23:59:59").execute();
            assertTrue(docs.count() == 2);

            docs = coll.find(":datetimeField NOT IN $.datetimeField").bind("datetimeField", "9999-12-30 23:59:59").execute();
            assertTrue(docs.count() == 1);

            docs = coll.find("CAST(CAST(:datetimeField as DATETIME) as JSON) OVERLAPS $.datetimeField").bind("datetimeField", "9999-12-30 23:59:59").execute();
            assertTrue(docs.count() == 2);

            docs = coll.find(":datetimeField NOT OVERLAPS $.datetimeField").bind("datetimeField", "9999-12-30 23:59:59").execute();
            assertTrue(docs.count() == 1);

            //Integration scenaris of Integer
            docs = coll.find(":intField not in $.intField").bind("intField", 12).execute();
            assertTrue(docs.count() == 1);

            docs = coll.find(":intField overlaps $.intField").bind("intField", 12).execute();
            assertTrue(docs.count() == 2);

            docs = coll.find(":intField not overlaps $.intField").bind("intField", 12).execute();
            assertTrue(docs.count() == 1);

            //Integration scenaris of unsigned integer
            docs = coll.find(":uintField not in $.uintField").bind("uintField", 52).execute();
            assertTrue(docs.count() == 0);

            docs = coll.find(":uintField overlaps $.uintField").bind("uintField", 52).execute();
            assertTrue(docs.count() == 3);

            docs = coll.find(":uintField not overlaps $.uintField").bind("uintField", 52).execute();
            assertTrue(docs.count() == 0);

            //Integration scenaris of character type
            docs = coll.find(":charField not in $.charField").bind("charField", "abcd1").execute();
            assertTrue(docs.count() == 0);

            docs = coll.find(":charField overlaps $.charField").bind("charField", "abcd1").execute();
            assertTrue(docs.count() == 3);

            docs = coll.find(":charField not overlaps $.charField").bind("charField", "abcd1").execute();
            assertTrue(docs.count() == 0);

            //Integration scenarios of binary type
            docs = coll.find(":binaryField not in $.binaryField").bind("binaryField", "abcd1").execute();
            assertTrue(docs.count() == 0);

            docs = coll.find(":binaryField overlaps $.binaryField").bind("binaryField", "abcd1").execute();
            assertTrue(docs.count() == 3);

            docs = coll.find(":binaryField not overlaps $.binaryField").bind("binaryField", "abcd1").execute();
            assertTrue(docs.count() == 0);

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void testArrayIndex015() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(this.baseUrl, ServerVersion.parseVersion("8.0.17")), "MySQL 8.0.17+ is required to run this test.");

        String collname = "coll1";
        DbDoc doc = null;
        Session sess = null;
        try {
            sess = new SessionFactory().getSession(this.baseUrl);
            Schema sch = sess.getDefaultSchema();
            sch.dropCollection(collname);
            Collection coll = sch.createCollection(collname, true);

            /* create basic index */

            coll.remove("true").execute();

            coll.add(
                    "{\"_id\":1,\"name\":\"decimalArray1\",\"decimalField1\":[835975.76,87349829932749.67,89248481498149882498141.12],\"decimalField2\":[835975.76,87349829932.839],\"decimalField3\":[835977.76,87349829932.839]}")
                    .execute();
            coll.add(
                    "{\"_id\":2,\"name\":\"dateArray1\",\"dateField1\" : [\"2017-12-12\",\"2018-12-12\",\"2019-12-12\"],\"dateField2\" : [\"2017-12-12\",\"2018-11-11\",\"2019-11-11\"],\"dateField3\" : [\"2017-12-12\",\"2018-10-10\",\"2019-10-10\"]}")
                    .execute();
            coll.add(
                    "{\"_id\":3,\"name\":\"timeArray1\",\"timeField1\" : [\"12:20\",\"11:20\",\"10:20\"], \"timeField2\" : [\"12:00\",\"11:00\",\"10:20\"], \"timeField3\" : [\"12:10\",\"11:10\",\"10:00\"]}")
                    .execute();
            coll.add(
                    "{\"_id\":4,\"name\":\"timestampArray1\",\"timestampField1\" : [\"2017-12-12 20:12:07\", \"2018-12-12 20:12:07\",\"2019-12-12 20:12:07\"], \"timestampField2\" : [\"2017-12-12 20:12:07\", \"2018-11-11 20:12:07\",\"2019-11-11 20:12:07\"], \"timestampField3\" : [\"2017-12-12 20:12:07\", \"2018-10-11 20:12:07\",\"2019-12-12 20:12:07\"]}")
                    .execute();
            coll.add(
                    "{\"_id\":5,\"name\":\"datetimeArray1\", \"datetimeField1\" : [\"2017-12-12 20:12:07\", \"2018-12-12 20:12:07\",\"2019-12-12 20:12:07\"], \"datetimeField2\" : [\"2017-12-12 20:12:07\", \"2018-11-11 20:12:07\",\"2019-11-11 20:12:07\"],\"datetimeField3\" : [\"2017-10-10 20:12:07\", \"2018-10-10 20:12:07\",\"2019-10-10 20:12:07\"]}")
                    .execute();
            coll.add(
                    "{\"_id\":6,\"name\":\"binaryArray1\", \"binaryField1\":[\"0xe240\",\"0x0001e240\"],\"binaryField2\":[\"0xe240\",\"0x0001e240\"],\"binaryField3\":[\"0xe240\",\"0x0001e240\"]}")
                    .execute();
            coll.add(
                    "{\"_id\":7,\"name\":\"dateArray2\",\"dateField1\" : [\"2017-12-12\",\"2018-12-12\",\"2019-12-12\"],\"dateField2\" : [\"2017-11-11\",\"2018-11-11\",\"2019-11-11\"],\"dateField3\" : [\"2017-10-10\",\"2018-10-10\",\"2019-10-10\"]}")
                    .execute();
            coll.add(
                    "{\"_id\":8,\"name\":\"timeArray2\",\"timeField1\" : [\"12:20\",\"11:20\",\"10:20\"], \"timeField2\" : [\"12:00\",\"11:00\",\"10:00\"], \"timeField3\" : [\"12:10\",\"11:10\",\"10:10\"]}")
                    .execute();
            coll.add(
                    "{\"_id\":9,\"name\":\"datetimeArray2\", \"datetimeField1\" : [\"2017-12-12 20:12:07\", \"2018-12-12 20:12:07\",\"2019-12-12 20:12:07\"], \"datetimeField2\" : [\"2017-11-11 20:12:07\", \"2018-11-11 20:12:07\",\"2019-11-11 20:12:07\"],\"datetimeField3\" : [\"2017-10-10 20:12:07\", \"2018-10-10 20:12:07\",\"2019-10-10 20:12:07\"]}")
                    .execute();
            coll.add(
                    "{\"_id\":10,\"name\":\"binaryArray2\", \"binaryField1\":[\"0xe240\",\"0x0001e240\"],\"binaryField2\":[\"0xe2040\",\"0x0001e2040\"],\"binaryField3\":[\"0xe02040\",\"0x0001e02040\"]}")
                    .execute();
            coll.add(
                    "{\"_id\":11,\"name\":\"charArray1\", \"charField1\":[\"char1\",\"char2\"],\"charField2\":[\"char1\",\"char3\"],\"charField3\":[\"char1\",\"char2\"]}")
                    .execute();
            coll.add(
                    "{\"_id\":12,\"name\":\"charArray2\", \"charField1\":[\"char1\",\"char2\"],\"charField2\":[\"char3\",\"char4\"],\"charField3\":[\"char5\",\"char6\"]}")
                    .execute();
            coll.add("{\"_id\":13,\"name\":\"intArray1\", \"intField1\":[-15,-25],\"intField2\":[-15,-20],\"intField3\":[-10,-20]}").execute();
            coll.add("{\"_id\":14,\"name\":\"intArray2\", \"intField1\":[-10,-20],\"intField2\":[-30,-40],\"intField3\":[-50,-60]}").execute();

            coll.add("{\"_id\":15,\"name\":\"uintArray1\", \"uintField1\":[15,25],\"uintField2\":[15,20],\"uintField3\":[10,20]}").execute();
            coll.add("{\"_id\":16,\"name\":\"uintArray2\", \"uintField1\":[10,20],\"uintField2\":[30,40],\"uintField3\":[50,60]}").execute();

            coll.createIndex("intArrayIndex1", "{\"fields\": [{\"field\": \"$.intField1\", \"type\": \"SIGNED INTEGER\", \"array\": true}]}");
            coll.createIndex("intArrayIndex2", "{\"fields\": [{\"field\": \"$.intField2\", \"type\": \"SIGNED INTEGER\", \"array\": true}]}");
            coll.createIndex("uintArrayIndex1", "{\"fields\": [{\"field\": \"$.uintField1\", \"type\": \"UNSIGNED INTEGER\", \"array\": true}]}");
            coll.createIndex("uintArrayIndex2", "{\"fields\": [{\"field\": \"$.uintField2\", \"type\": \"UNSIGNED INTEGER\", \"array\": true}]}");
            coll.createIndex("decimalArrayIndex1", "{\"fields\": [{\"field\": \"$.decimalField1\", \"type\": \"DECIMAL(65,2)\", \"array\": true}]}");
            coll.createIndex("decimalArrayIndex2", "{\"fields\": [{\"field\": \"$.decimalField2\", \"type\": \"DECIMAL(65,2)\", \"array\": true}]}");
            coll.createIndex("dateArrayIndex1", "{\"fields\": [{\"field\": \"$.dateField1\", \"type\": \"DATE\", \"array\": true}]}");
            coll.createIndex("dateArrayIndex2", "{\"fields\": [{\"field\": \"$.dateField2\", \"type\": \"DATE\", \"array\": true}]}");
            coll.createIndex("datetimeArrayIndex1", "{\"fields\": [{\"field\": \"$.datetimeField1\", \"type\": \"DATETIME\", \"array\": true}]}");
            coll.createIndex("datetimeArrayIndex2", "{\"fields\": [{\"field\": \"$.datetimeField2\", \"type\": \"DATETIME\", \"array\": true}]}");
            coll.createIndex("timeArrayIndex1", "{\"fields\": [{\"field\": \"$.timeField1\", \"type\": \"TIME\", \"array\": true}]}");
            coll.createIndex("timeArrayIndex2", "{\"fields\": [{\"field\": \"$.timeField2\", \"type\": \"TIME\", \"array\": true}]}");
            coll.createIndex("charArrayIndex1", "{\"fields\": [{\"field\": \"$.charField1\", \"type\": \"CHAR(256)\", \"array\": true}]}");
            coll.createIndex("charArrayIndex2", "{\"fields\": [{\"field\": \"$.charField2\", \"type\": \"CHAR(256)\", \"array\": true}]}");
            coll.createIndex("binaryArrayIndex1", "{\"fields\": [{\"field\": \"$.binaryField1\", \"type\": \"BINARY(256)\", \"array\": true}]}");
            coll.createIndex("binaryArrayIndex2", "{\"fields\": [{\"field\": \"$.binaryField2\", \"type\": \"BINARY(256)\", \"array\": true}]}");
            validateArrayIndex("intArrayIndex1", "coll1", 1);
            validateArrayIndex("uintArrayIndex1", "coll1", 1);
            validateArrayIndex("decimalArrayIndex1", "coll1", 1);
            validateArrayIndex("dateArrayIndex1", "coll1", 1);
            validateArrayIndex("datetimeArrayIndex1", "coll1", 1);
            validateArrayIndex("timeArrayIndex1", "coll1", 1);
            validateArrayIndex("charArrayIndex1", "coll1", 1);
            validateArrayIndex("binaryArrayIndex1", "coll1", 1);
            validateArrayIndex("intArrayIndex2", "coll1", 1);
            validateArrayIndex("uintArrayIndex2", "coll1", 1);
            validateArrayIndex("decimalArrayIndex2", "coll1", 1);
            validateArrayIndex("dateArrayIndex2", "coll1", 1);
            validateArrayIndex("datetimeArrayIndex2", "coll1", 1);
            validateArrayIndex("timeArrayIndex2", "coll1", 1);
            validateArrayIndex("charArrayIndex2", "coll1", 1);
            validateArrayIndex("binaryArrayIndex2", "coll1", 1);

            DocResult docs = coll.find("$.intField1 overlaps $.intField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("intArray1", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.intField1 not overlaps $.intField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("intArray2", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.uintField1 overlaps $.uintField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("uintArray1", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.uintField1 not overlaps $.uintField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("uintArray2", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.charField1 overlaps $.charField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("charArray1", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.charField1 not overlaps $.charField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("charArray2", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.decimalField1 overlaps $.decimalField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("decimalArray1", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.decimalField1 not overlaps $.decimalField2").execute();
            assertTrue(docs.count() == 0);

            docs = coll.find("$.binaryField1 overlaps $.binaryField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("binaryArray1", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.binaryField1 not overlaps $.binaryField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("binaryArray2", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.timeField1 overlaps $.timeField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("timeArray1", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.timeField1 not overlaps $.timeField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("timeArray2", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.datetimeField1 overlaps $.datetimeField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("datetimeArray1", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.datetimeField1 not overlaps $.datetimeField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("datetimeArray2", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.dateField1 overlaps $.dateField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("dateArray1", ((JsonString) doc.get("name")).getString());

            docs = coll.find("$.dateField1 not overlaps $.dateField2").execute();
            assertTrue(docs.count() == 1);
            doc = docs.next();
            assertEquals("dateArray2", ((JsonString) doc.get("name")).getString());

            sch.dropCollection(collname);
        } finally {
            if (sess != null) {
                sess.close();
                sess = null;
            }
        }
    }

    @Test
    public void testAsyncBind() throws Exception {
        int i = 0, maxrec = 10;
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf((100 + i) * 10)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(100 + i)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(1 + i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }

        CompletableFuture<AddResult> asyncRes = this.collection.add(jsonlist).executeAsync();
        asyncRes.get();

        // 1. Incorrect PlaceHolder Name in bind
        assertThrows(WrongArgumentException.class, "Unknown placeholder: F", () -> this.collection.find("F1 like :X").bind("F", "Field-1-%-3").executeAsync());

        // 2. PlaceHolder Name in small letter
        assertThrows(WrongArgumentException.class, "Unknown placeholder: x", () -> this.collection.find("F1 like :X").bind("x", "Field-1-%-3").executeAsync());

        // 3. No bind
        assertThrows(WrongArgumentException.class, "Placeholder 'X' is not bound", () -> this.collection.find("F1 like :X").executeAsync());

        // 4. bind 2 times
        assertThrows(WrongArgumentException.class, "Unknown placeholder: x",
                () -> this.collection.find("F1 like :X").bind("X", "Field-1-%-4").bind("x", "Field-1-%-3").bind("X", "Field-1-%-3").executeAsync());

        // 5. bind same variable 2 times (Success)
        CompletableFuture<DocResult> asyncDocs = this.collection.find("F1 like :X").bind("X", "Field-1-%-4").bind("X", "Field-1-%-3").executeAsync();

        DocResult docs = asyncDocs.get();
        DbDoc doc = docs.next();
        assertEquals((long) 103, (long) (((JsonNumber) doc.get("F3")).getInteger()));
        assertFalse(docs.hasNext());

        // 6. bind In different order (Success)
        asyncDocs = this.collection.find("F1 like :X and $.F3 =:Y and  $.F3 !=:Z").bind("Y", 103).bind("Z", 104).bind("X", "Field-1-%-3").executeAsync();
        docs = asyncDocs.get();
        doc = docs.next();
        assertEquals((long) 103, (long) (((JsonNumber) doc.get("F3")).getInteger()));
        assertFalse(docs.hasNext());

        // 7. Using same Bind Variables many times(Success)
        asyncDocs = this.collection.find("F1 like :F1 and $.F3 in (:F3,:F2,:F3) and  $.F2 =(:F3*10)").bind("F3", 103).bind("F2", 102).bind("F1", "Field-1-%-3")
                .executeAsync();
        docs = asyncDocs.get();
        doc = docs.next();
        assertEquals((long) 103, (long) (((JsonNumber) doc.get("F3")).getInteger()));
        assertFalse(docs.hasNext());

        // 1. Incorrect PlaceHolder Name in bind
        assertThrows(WrongArgumentException.class, "Unknown placeholder: F",
                () -> this.collection.modify("F1 like :X").set("$.F4", 1).bind("F", "Field-1-%-3").executeAsync());

        // 2. PlaceHolder Name in small letter
        assertThrows(WrongArgumentException.class, "Unknown placeholder: x",
                () -> this.collection.modify("F1 like :X").set("$.F4", 1).bind("x", "Field-1-%-3").executeAsync());

        // 3. No bind
        assertThrows(WrongArgumentException.class, "Placeholder 'X' is not bound", () -> this.collection.modify("F1 like :X").set("$.F4", 1).executeAsync());

        // 4. bind 2 times
        assertThrows(WrongArgumentException.class, "Unknown placeholder: x", () -> this.collection.modify("F1 like :X").set("$.F4", 1).bind("X", "Field-1-%-4")
                .bind("x", "Field-1-%-3").bind("X", "Field-1-%-3").executeAsync());

        // 5. bind same variable 2 times (Success)
        CompletableFuture<Result> asyncRes2 = this.collection.modify("F1 like :X").set("$.F4", -1).bind("X", "Field-1-%-4").bind("X", "Field-1-%-3")
                .executeAsync();
        Result res2 = asyncRes2.get();
        assertEquals(1, res2.getAffectedItemsCount());

        // 6. bind In different order (Success)
        asyncRes2 = this.collection.modify("F1 like :X and $.F3 =:Y and  $.F3 !=:Z").set("$.F4", -2).bind("Y", 103).bind("Z", 104).bind("X", "Field-1-%-3")
                .executeAsync();
        res2 = asyncRes2.get();
        assertEquals(1, res2.getAffectedItemsCount());

        // 7. Using same Bind Variables many times(Success)
        asyncRes2 = this.collection.modify("F1 like :F1 and $.F3 in (:F3,:F2,:F3) and  $.F2 =(:F3*10)").set("$.F4", -3).bind("F3", 103).bind("F2", 102)
                .bind("F1", "Field-1-%-3").executeAsync();
        res2 = asyncRes2.get();
        assertEquals(1, res2.getAffectedItemsCount());

        asyncRes2 = this.collection.modify("$.F3 = :X").set("$.F4", 1).bind("X", 101).sort("$.F1 asc").executeAsync();
        res2 = asyncRes2.get();
        assertEquals(1, res2.getAffectedItemsCount());

        asyncRes2 = this.collection.modify("$.F3 = :X+:Y+:X+:Y").set("$.F4", -4).bind("X", 50).bind("Y", 2).sort("$.F1 asc").executeAsync();
        res2 = asyncRes2.get();
        assertEquals(1, res2.getAffectedItemsCount());

        // 1. Incorrect PlaceHolder Name in bind
        assertThrows(WrongArgumentException.class, "Unknown placeholder: F",
                () -> this.collection.remove("F1 like :X").bind("F", "Field-1-%-3").executeAsync());

        // 2. PlaceHolder Name in small letter
        assertThrows(WrongArgumentException.class, "Unknown placeholder: x",
                () -> this.collection.remove("F1 like :X").bind("x", "Field-1-%-3").executeAsync());

        // 3. No bind
        assertThrows(WrongArgumentException.class, "Placeholder 'X' is not bound", () -> this.collection.remove("F1 like :X").executeAsync());

        // 4. bind 2 times
        assertThrows(WrongArgumentException.class, "Unknown placeholder: x",
                () -> this.collection.remove("F1 like :X").bind("X", "Field-1-%-4").bind("x", "Field-1-%-3").bind("X", "Field-1-%-3").executeAsync());

        // 5. bind same variable 2 times (Success)
        asyncRes2 = this.collection.remove("F1 like :X").bind("X", "Field-1-%-4").bind("X", "Field-1-%-0").executeAsync();
        res2 = asyncRes2.get();
        assertEquals(1, res2.getAffectedItemsCount());

        // 6. bind In different order (Success)
        asyncRes2 = this.collection.remove("F1 like :X and $.F3 =:Y and  $.F3 !=:Z").bind("Y", 101).bind("Z", 104).bind("X", "Field-1-%-1").executeAsync();
        res2 = asyncRes2.get();
        assertEquals(1, res2.getAffectedItemsCount());

        // 7. Using same Bind Variables many times(Success)
        asyncRes2 = this.collection.remove("F1 like :F1 and $.F3 in (:F3,:F2,:F3) and  $.F2 =(:F3*10)").bind("F3", 102).bind("F2", 107)
                .bind("F1", "Field-1-%-2").executeAsync();
        res2 = asyncRes2.get();
        assertEquals(1, res2.getAffectedItemsCount());
    }

    @Test
    public void testFetchOneFetchAllAsync() throws Exception {
        int i = 0, maxrec = 10;
        CompletableFuture<DocResult> asyncDocs = null;
        List<DbDoc> rowDoc = null;
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf((100 + i) * 10)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(100 + i)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(1 + i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }

        CompletableFuture<AddResult> asyncRes = this.collection.add(jsonlist).executeAsync();
        asyncRes.get();

        // fetchAll() after fetchOne (Error)
        asyncDocs = this.collection.find("F1 like :X and $.F3 <=:Y and  $.F3 !=:Z").bind("Y", 105).bind("Z", 105).bind("X", "Field-1-%").orderBy("F1 asc")
                .executeAsync();
        DocResult docs1 = asyncDocs.get();
        DbDoc doc = docs1.fetchOne();
        assertThrows(WrongArgumentException.class, "Cannot fetchAll\\(\\) after starting iteration", () -> docs1.fetchAll());

        i = 0;
        while (doc != null) {
            assertEquals((long) (100 + i), (long) (((JsonNumber) doc.get("F3")).getInteger()));
            i = i + 1;
            doc = docs1.fetchOne();
        }
        assertThrows(WrongArgumentException.class, "Cannot fetchAll\\(\\) after starting iteration", () -> docs1.fetchAll());

        // fetchAll() after next (Error)
        asyncDocs = this.collection.find("F1 like :X and $.F3 <=:Y and  $.F3 !=:Z").bind("Y", 105).bind("Z", 105).bind("X", "Field-1-%").orderBy("F1 asc")
                .executeAsync();
        DocResult docs2 = asyncDocs.get();
        doc = docs2.next();
        assertThrows(WrongArgumentException.class, "Cannot fetchAll\\(\\) after starting iteration", () -> docs2.fetchAll());
        i = 0;
        do {
            assertEquals((long) (100 + i), (long) (((JsonNumber) doc.get("F3")).getInteger()));
            i = i + 1;
            doc = docs2.next();
        } while (docs2.hasNext());
        assertThrows(WrongArgumentException.class, "Cannot fetchAll\\(\\) after starting iteration", () -> docs2.fetchAll());

        // fetchOne() and next(Success)
        asyncDocs = this.collection.find("F1 like :X and $.F3 <=:Y and  $.F3 !=:Z").bind("Y", 108).bind("Z", 108).bind("X", "Field-1-%").orderBy("F3 asc")
                .executeAsync();
        DocResult docs3 = asyncDocs.get();
        i = 0;
        while (docs3.hasNext()) {
            if (i % 2 == 1) {
                doc = docs3.next();
            } else {
                doc = docs3.fetchOne();
            }
            assertEquals((long) (100 + i), (long) (((JsonNumber) doc.get("F3")).getInteger()));
            i = i + 1;

        }
        assertThrows(WrongArgumentException.class, "Cannot fetchAll\\(\\) after starting iteration", () -> docs3.fetchAll());

        //fetchAll (Success)
        asyncDocs = this.collection.find("F1 like :X and $.F3 <=:Y and  $.F3 !=:Z").bind("Y", 105).bind("Z", 105).bind("X", "Field-1-%").orderBy("F1 asc")
                .executeAsync();
        DocResult docs = asyncDocs.get();
        rowDoc = docs.fetchAll();
        assertEquals((long) 5, (long) rowDoc.size());
        for (i = 0; i < rowDoc.size(); i++) {
            doc = rowDoc.get(i);
            assertEquals((long) (100 + i), (long) (((JsonNumber) doc.get("F3")).getInteger()));
        }
        doc = docs.fetchOne();
    }

    @SuppressWarnings({ "deprecation", "unchecked" })
    @Test
    public void testCollectionAddModifyRemoveAsync() throws Exception {
        int i = 0;
        int NUMBER_OF_QUERIES = 5000;
        DbDoc doc = null;
        AddResult res = null;
        SqlResult res1 = null;
        CompletableFuture<DocResult> asyncDocs = null;
        DocResult docs = null;
        DbDoc newDoc1 = new DbDocImpl();
        newDoc1.add("_id", new JsonString().setValue(String.valueOf(1000)));
        newDoc1.add("F1", new JsonString().setValue("Field-1-Data-1"));
        newDoc1.add("F2", new JsonNumber().setValue(String.valueOf(1000)));
        newDoc1.add("T", new JsonNumber().setValue(String.valueOf(10)));

        DbDoc newDoc2 = new DbDocImpl();
        newDoc2.add("_id", new JsonString().setValue(String.valueOf(1000)));

        // add(), modify(),find(),  remove() Success And Failure
        List<Object> futures = new ArrayList<>();
        for (i = 0; i < NUMBER_OF_QUERIES; ++i) {
            if (i % 10 == 0) {
                futures.add(this.collection.add(newDoc1).executeAsync());
            } else if (i % 10 == 1) {
                futures.add(this.collection.find("NON_EXISTING_FUNCTION1()").fields("$._id as _id, $.F1 as F1, $.F2 as F2, $.F3 as F3").executeAsync()); //Error
            } else if (i % 10 == 2) {
                futures.add(this.collection.find("$.F2 = 1000").fields("$._id as _id, $.F1 as F1, $.F2 as F2, $.T as T").executeAsync());
            } else if (i % 10 == 3) {
                futures.add(this.collection.add(newDoc2).executeAsync()); //Error
            } else if (i % 10 == 4) {
                futures.add(this.collection.modify("$.F2 = 1000").change("$.T", expr("$.T+1")).sort("$.F3 desc").executeAsync());
            } else if (i % 10 == 5) {
                futures.add(this.collection.modify("$.F2 = 1000").change("$.T", expr("NON_EXISTING_FUNCTION2()")).sort("$.F3 desc").executeAsync());//Error
            } else if (i % 10 == 6) {
                futures.add(this.collection.remove("NON_EXISTING_FUNCTION3()=:PARAM").bind("PARAM", 1000).orderBy("$.F3 desc").executeAsync());//Error
            } else if (i % 10 == 7) {
                futures.add(this.session.sql(
                        "SELECT JSON_OBJECT('_id', JSON_EXTRACT(doc,'$._id'),'F1', JSON_EXTRACT(doc,'$.F1'),'F2', JSON_EXTRACT(doc,'$.F2'),'T', JSON_EXTRACT(doc,'$.T')) AS doc FROM `"
                                + this.collectionName + "` WHERE (JSON_EXTRACT(doc,'$.F2') = 1000) ")
                        .executeAsync());
            } else if (i % 10 == 8) {
                futures.add(this.session.sql("select non_existingfun() /* loop : " + i + "*/").executeAsync());//Error
            } else {
                futures.add(this.collection.remove("$.F2 = :PARAM").bind("PARAM", 1000).orderBy("$.F3 desc").executeAsync());
            }
        }

        for (i = 0; i < NUMBER_OF_QUERIES; ++i) {
            int i1 = i;
            if (i % 10 == 0) {
                res = ((CompletableFuture<AddResult>) futures.get(i)).get();
                assertEquals(1, res.getAffectedItemsCount());
            } else if (i % 10 == 1) {
                assertThrows(ExecutionException.class, ".*FUNCTION " + this.schema.getName() + ".NON_EXISTING_FUNCTION1 does not exist.*",
                        () -> ((CompletableFuture<DocResult>) futures.get(i1)).get());
            } else if (i % 10 == 2) {
                docs = ((CompletableFuture<DocResult>) futures.get(i)).get();
                assertTrue(docs.hasNext());
                doc = docs.next();
                assertEquals((long) (10), (long) (((JsonNumber) doc.get("T")).getInteger()));
                assertFalse(docs.hasNext());
            } else if (i % 10 == 3) {
                assertThrows(ExecutionException.class, ".*Document contains a field value that is not unique but required to be.*",
                        () -> ((CompletableFuture<AddResult>) futures.get(i1)).get());
            } else if (i % 10 == 4) {
                Result res2 = ((CompletableFuture<AddResult>) futures.get(i)).get();
                assertEquals(1, res2.getAffectedItemsCount());
            } else if (i % 10 == 5) {
                assertThrows(ExecutionException.class, ".*FUNCTION " + this.schema.getName() + ".NON_EXISTING_FUNCTION2 does not exist.*",
                        () -> ((CompletableFuture<Result>) futures.get(i1)).get());
            } else if (i % 10 == 6) {
                assertThrows(ExecutionException.class, ".*FUNCTION " + this.schema.getName() + ".NON_EXISTING_FUNCTION3 does not exist.*",
                        () -> ((CompletableFuture<Result>) futures.get(i1)).get());
            } else if (i % 10 == 7) {
                res1 = ((CompletableFuture<SqlResult>) futures.get(i)).get();
                res1.next();
                assertFalse(res1.hasNext());
            } else if (i % 10 == 8) {
                assertThrows(ExecutionException.class, ".*FUNCTION " + this.schema.getName() + ".non_existingfun does not exist.*",
                        () -> ((CompletableFuture<SqlResult>) futures.get(i1)).get());
            } else {
                Result res2 = ((CompletableFuture<AddResult>) futures.get(i)).get();
                assertEquals(1, res2.getAffectedItemsCount());
            }
        }

        if ((NUMBER_OF_QUERIES - 1) % 10 < 9) {
            assertEquals((1), this.collection.count());
            asyncDocs = this.collection.find("$.T = :X ").bind("X", 10).fields(expr("{'cnt':count($.T)}")).executeAsync();
            docs = asyncDocs.get();
            doc = docs.next();
            assertEquals((long) (1), (long) (((JsonNumber) doc.get("cnt")).getInteger()));
        } else {
            assertEquals((0), this.collection.count());
        }
    }
}
