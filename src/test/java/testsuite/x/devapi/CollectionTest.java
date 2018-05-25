/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;

import org.junit.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DatabaseObject.DbObjectStatus;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DbDocImpl;
import com.mysql.cj.xdevapi.JsonArray;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.Row;
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

    @Test(expected = WrongArgumentException.class)
    public void getNonExistentCollectionWithRequireExistsShouldThrow() {
        if (!this.isSetForXTests) {
            throw new WrongArgumentException("Throw WrongArgumentException as expected, but test was ignored because of missed configuration.");
        }
        String collName = "testRequireExists";
        dropCollection(collName);
        this.schema.getCollection(collName, true);
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

        // FR1_1 Create an index on a single field.
        this.collection.createIndex("myIndex", new DbDocImpl().add("fields", new JsonArray()
                .addValue(new DbDocImpl().add("field", new JsonString().setValue("$.myField")).add("type", new JsonString().setValue("TEXT(200)")))));
        validateIndex("myIndex", this.collectionName, "t200", false, false, false, 1, 200);
        this.collection.dropIndex("myIndex");

        // FR1_2 Create an index on a single field with all the possibles options.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(5)\", \"required\": true}]}");
        validateIndex("myIndex", this.collectionName, "t5", false, true, false, 1, 5);
        this.collection.dropIndex("myIndex");

        // FR1_3 Create an index on multiple fields.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(20)\"},"
                + " {\"field\": \"$.myField2\", \"type\": \"TEXT(10)\"}, {\"field\": \"$.myField3\", \"type\": \"INT\"}]}");
        validateIndex("myIndex", this.collectionName, "t20", false, false, false, 1, 20);
        validateIndex("myIndex", this.collectionName, "t10", false, false, false, 2, 10);
        validateIndex("myIndex", this.collectionName, "i", false, false, false, 3, null);
        this.collection.dropIndex("myIndex");

        // FR1_4 Create an index on multiple fields with all the possibles options.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(20)\"},"
                + " {\"field\": \"$.myField2\", \"type\": \"TEXT(10)\", \"required\": true}, {\"field\": \"$.myField3\", \"type\": \"INT UNSIGNED\", \"required\": false}]}");
        validateIndex("myIndex", this.collectionName, "t20", false, false, false, 1, 20);
        validateIndex("myIndex", this.collectionName, "t10", false, true, false, 2, 10);
        validateIndex("myIndex", this.collectionName, "i_u", false, false, true, 3, null);
        this.collection.dropIndex("myIndex");

        // FR1_5 Create an index using a geojson datatype field.
        this.collection.createIndex("myIndex",
                "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"GEOJSON\", \"required\": true}], \"type\":\"SPATIAL\"}");
        validateIndex("myIndex", this.collectionName, "gj", false, true, false, 1, 32);
        this.collection.dropIndex("myIndex");

        // FR1_6 Create an index using a geojson datatype field with all the possibles options.
        this.collection.createIndex("myIndex",
                "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"GEOJSON\", \"required\": true, \"options\": 2, \"srid\": 4326}], \"type\":\"SPATIAL\"}");
        validateIndex("myIndex", this.collectionName, "gj", false, true, false, 1, 32);
        this.collection.dropIndex("myIndex");

        // FR1_7 Create an index using a datetime field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"DATETIME\"}]}");
        validateIndex("myIndex", this.collectionName, "dd", false, false, false, 1, null);
        this.collection.dropIndex("myIndex");

        // FR1_8 Create an index using a timestamp field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TIMESTAMP\"}]}");
        validateIndex("myIndex", this.collectionName, "ds", false, false, false, 1, null);
        this.collection.dropIndex("myIndex");

        // FR1_9 Create an index using a time field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TIME\"}]}");
        validateIndex("myIndex", this.collectionName, "dt", false, false, false, 1, null);
        this.collection.dropIndex("myIndex");

        // FR1_10 Create an index using a date field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"DATE\"}]}");
        validateIndex("myIndex", this.collectionName, "d", false, false, false, 1, null);
        this.collection.dropIndex("myIndex");

        // FR1_11 Create an index using a numeric field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"NUMERIC UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, "xn_u", false, false, true, 1, null);
        this.collection.dropIndex("myIndex");

        // FR1_12 Create an index using a decimal field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"DECIMAL\"}]}");
        validateIndex("myIndex", this.collectionName, "xd", false, false, false, 1, null);
        this.collection.dropIndex("myIndex");

        // FR1_13 Create an index using a double field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"DOUBLE UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, "fd_u", false, false, true, 1, null);
        this.collection.dropIndex("myIndex");

        // FR1_14 Create an index using a float field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"FLOAT UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, "f_u", false, false, true, 1, null);
        this.collection.dropIndex("myIndex");

        // FR1_15 Create an index using a real field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"REAL UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, "fr_u", false, false, true, 1, null);
        this.collection.dropIndex("myIndex");

        // FR1_16 Create an index using a bigint field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"BIGINT UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, "ib_u", false, false, true, 1, null);
        this.collection.dropIndex("myIndex");

        // FR1_17 Create an index using a integer field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"INTEGER UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, "i_u", false, false, true, 1, null);
        this.collection.dropIndex("myIndex");

        // FR1_18 Create an index using a mediumint field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"MEDIUMINT UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, "im_u", false, false, true, 1, null);
        this.collection.dropIndex("myIndex");

        // FR1_19 Create an index using a smallint field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"SMALLINT UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, "is_u", false, false, true, 1, null);
        this.collection.dropIndex("myIndex");

        // FR1_20 Create an index using a tinyint field.
        this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TINYINT UNSIGNED\"}]}");
        validateIndex("myIndex", this.collectionName, "it_u", false, false, true, 1, null);
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
        assertThrows(XDevAPIError.class, "Index definition does not contain fields.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", "{\"type\": \"INDEX\"}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Index definition 'fields' member must be an array of index fields.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": 123}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Index field definition must be a JSON document.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [123]}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Index field definition has no document path.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [{\"type\": 123}]}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Index field 'field' member must be a string.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": 123}]}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Index field definition has no field type.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\"}]}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Index type must be a string.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": 123}]}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Index field 'required' member must be boolean.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex",
                        "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(5)\", \"required\": \"yes\"}]}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Index field 'options' member must be integer.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex",
                        "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"GEOJSON\", \"options\": \"qqq\"}]}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Index field 'srid' member must be integer.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex",
                        "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"GEOJSON\", \"options\": 2, \"srid\": \"qqq\"}]}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Wrong index type 'SPTIAL'. Must be 'INDEX' or 'SPATIAL'.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(5)\"}], \"type\":\"SPTIAL\"}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Index type must be a string.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(5)\"}], \"type\":123}");
                return null;
            }
        });

        // FR5_6 Create a 'SPATIAL' index with "required" flag set to false.
        assertThrows(XProtocolError.class, "ERROR 5117 \\(HY000\\) GEOJSON index requires 'constraint.required: TRUE", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex",
                        "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"GEOJSON\", \"required\": false, \"options\": 2, \"srid\": 4326}], \"type\":\"SPATIAL\"}");
                return null;
            }
        });

        // FR5_8 Create an index specifying geojson options for non geojson data type.
        assertThrows(XDevAPIError.class, "Index field 'options' member should not be used for field types other than GEOJSON.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex",
                        "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"TEXT(10)\", \"required\": true, \"options\": 2, \"srid\": 4326}]}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Index field 'srid' member should not be used for field types other than GEOJSON.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex",
                        "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"TEXT(10)\", \"required\": true, \"srid\": 4326}]}");
                return null;
            }
        });

        // ET_2 Create an index specifying SPATIAL as the index type for a non spatial data type
        assertThrows(XProtocolError.class, "ERROR 3106 \\(HY000\\) 'Spatial index on virtual generated column' is not supported for generated columns.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        CollectionTest.this.collection.createIndex("myIndex",
                                "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(10)\"}], \"type\":\"SPATIAL\"}");
                        return null;
                    }
                });

        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.12"))) {
            this.collection.createIndex("myIndex",
                    "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"GEOJSON\", \"required\": true, \"options\": 2, \"srid\": 4326}],"
                            + " \"type\":\"INDEX\"}");
            validateIndex("myIndex", this.collectionName, "gj", false, true, false, 1, 32);
            this.collection.dropIndex("myIndex");
        } else {
            // ET_3 Create an index specifying INDEX as the index type for a spatial data type
            assertThrows(XProtocolError.class, "ERROR 1170 \\(42000\\) BLOB/TEXT column .+_gj_r_.+ used in key specification without a key length",
                    new Callable<Void>() {
                        public Void call() throws Exception {
                            CollectionTest.this.collection.createIndex("myIndex",
                                    "{\"fields\": [{\"field\": \"$.myGeoJsonField\", \"type\": \"GEOJSON\", \"required\": true, \"options\": 2, \"srid\": 4326}],"
                                            + " \"type\":\"INDEX\"}");
                            return null;
                        }
                    });
        }

        // NPE checks
        assertThrows(XDevAPIError.class, "Parameter 'indexName' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex(null, "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(200)\"}]}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'indexName' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex(" ", "{\"fields\": [{\"field\": \"$.myField\", \"type\": \"TEXT(200)\"}]}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'jsonIndexDefinition' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", (String) null);
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'jsonIndexDefinition' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", "");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'indexDefinition' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", (DbDoc) null);
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "The 'somekey' field is not allowed in indexDefinition.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", "{\"somekey\": 123}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "The 'somefield' field is not allowed in indexField.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex", "{\"fields\": [{\"somefield\": 123}]}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "The 'unique' field is not allowed in indexDefinition.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionTest.this.collection.createIndex("myIndex",
                        "{\"fields\": [{\"field\": \"$.intField\", \"type\": \"INT\", \"required\": true}], \"unique\":true, \"type\":\"INDEX\"}");
                return null;
            }
        });

        // dropping non-existing index should not fail
        this.collection.dropIndex("non_existing_idx");
    }

    private void validateIndex(String keydName, String collName, String dataType, boolean unique, boolean required, boolean isUnsigned, int sequence,
            Integer length) throws Exception {
        boolean indexFound = false;

        SqlResult res = this.session.sql("show index from `" + collName + "`").execute();
        assertTrue(res.hasNext());

        for (Row row : res.fetchAll()) {
            if (keydName.equals(row.getString("Key_name"))) {
                if (sequence != row.getInt("Seq_in_index")) {
                    continue;
                }

                indexFound = true;
                assertEquals(collName.toUpperCase(), row.getString("Table").toUpperCase());
                assertEquals(unique ? "0" : "1", row.getString("Non_unique"));
                String[] columnNameTokens = row.getString("Column_name").toString().split("_");
                assertEquals(dataType, isUnsigned ? columnNameTokens[1] + "_" + columnNameTokens[2] : columnNameTokens[1]);

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
