/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DatabaseObject.DbObjectStatus;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DbDocImpl;
import com.mysql.cj.xdevapi.JsonArray;
import com.mysql.cj.xdevapi.JsonLiteral;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.SqlResult;
import com.mysql.cj.xdevapi.XDevAPIError;

public class CollectionTest extends BaseCollectionTestCase {
    @Test
    public void testCount() {
        if (!this.isSetForXTests) {
            return;
        }

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
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "testExists";
        dropCollection(collName);
        Collection coll = this.schema.getCollection(collName);
        assertEquals(this.schema, coll.getSchema());
        this.schema.dropCollection(collName);
    }

    @Test
    public void testGetSession() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "testExists";
        dropCollection(collName);
        Collection coll = this.schema.getCollection(collName);
        assertEquals(this.session, coll.getSession());
        this.schema.dropCollection(collName);
    }

    @Test
    public void testExists() {
        if (!this.isSetForXTests) {
            return;
        }
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
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "testRequireExists";
        dropCollection(collName);
        assertThrows(WrongArgumentException.class, () -> this.schema.getCollection(collName, true));
    }

    @Test
    public void getNonExistentCollectionWithoutRequireExistsShouldNotThrow() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "testRequireExists";
        dropCollection(collName);
        this.schema.getCollection(collName, false);
    }

    @Test
    public void getExistentCollectionWithRequireExistsShouldNotThrow() {
        if (!this.isSetForXTests) {
            return;
        }
        String collName = "testRequireExists";
        dropCollection(collName);
        this.schema.createCollection(collName);
        this.schema.getCollection(collName, true);
    }

    @Test
    public void createIndex() throws Exception {
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4"))) {
            return;
        }

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
        if (!this.isSetForXTests) {
            return;
        }

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
                    if (typePos >= 0) {
                        expr = expr.substring(typePos + 4, expr.length() - 1);
                        assertTrue(expr.endsWith("array"));
                        expr = expr.substring(0, expr.indexOf(" array"));
                        assertEquals(dataType, expr);
                    } else {
                        fail("Not an array index?");
                    }
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
}
