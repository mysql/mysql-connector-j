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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.AddResult;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.Result;
import com.mysql.cj.xdevapi.XDevAPIError;

public class CollectionAddTest extends BaseCollectionTestCase {
    @Before
    @Override
    public void setupCollectionTest() {
        super.setupCollectionTest();
    }

    @After
    @Override
    public void teardownCollectionTest() {
        super.teardownCollectionTest();
    }

    @Test
    public void testBasicAddString() {
        if (!this.isSetForXTests) {
            return;
        }
        String json = "{'firstName':'Frank', 'middleName':'Lloyd', 'lastName':'Wright'}".replaceAll("'", "\"");
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            json = json.replace("{", "{\"_id\": \"1\", "); // Inject an _id.
        }
        AddResult res = this.collection.add(json).execute();
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            assertTrue(res.getGeneratedIds().get(0).matches("[a-f0-9]{28}"));
        } else {
            assertEquals(0, res.getGeneratedIds().size());
        }

        DocResult docs = this.collection.find("firstName like '%Fra%'").execute();
        DbDoc d = docs.next();
        JsonString val = (JsonString) d.get("lastName");
        assertEquals("Wright", val.getString());
    }

    @Test
    public void testBasicAddStringArray() {
        if (!this.isSetForXTests) {
            return;
        }
        this.collection.add("{\"_id\": 1}", "{\"_id\": 2}").execute();
        assertEquals(true, this.collection.find("_id = 1").execute().hasNext());
        assertEquals(true, this.collection.find("_id = 2").execute().hasNext());
        assertEquals(false, this.collection.find("_id = 3").execute().hasNext());

        this.collection.add(new String[] { "{\"_id\": 3}", "{\"_id\": 4}" }).execute();
        assertEquals(true, this.collection.find("_id = 1").execute().hasNext());
        assertEquals(true, this.collection.find("_id = 2").execute().hasNext());
        assertEquals(true, this.collection.find("_id = 3").execute().hasNext());
        assertEquals(true, this.collection.find("_id = 4").execute().hasNext());
        assertEquals(false, this.collection.find("_id = 5").execute().hasNext());
    }

    @Test
    public void testBasicAddDoc() {
        if (!this.isSetForXTests) {
            return;
        }
        DbDoc doc = this.collection.newDoc().add("firstName", new JsonString().setValue("Georgia"));
        doc.add("middleName", new JsonString().setValue("Totto"));
        doc.add("lastName", new JsonString().setValue("O'Keeffe"));
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            doc.add("_id", new JsonString().setValue("1")); // Inject an _id.
        }
        AddResult res = this.collection.add(doc).execute();
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            assertTrue(res.getGeneratedIds().get(0).matches("[a-f0-9]{28}"));
        } else {
            assertEquals(0, res.getGeneratedIds().size());
        }

        DocResult docs = this.collection.find("lastName like 'O\\'Kee%'").execute();
        DbDoc d = docs.next();
        JsonString val = (JsonString) d.get("lastName");
        assertEquals("O'Keeffe", val.getString());
    }

    @Test
    public void testBasicAddDocArray() {
        if (!this.isSetForXTests) {
            return;
        }

        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion(("8.0.5")))) {
            AddResult res1 = this.collection.add(this.collection.newDoc().add("f1", new JsonString().setValue("doc1")),
                    this.collection.newDoc().add("f1", new JsonString().setValue("doc2"))).execute();
            assertTrue(res1.getGeneratedIds().get(0).matches("[a-f0-9]{28}"));
        } else {
            AddResult res1 = this.collection
                    .add(this.collection.newDoc().add("_id", new JsonString().setValue("1")).add("f1", new JsonString().setValue("doc1")),
                            this.collection.newDoc().add("_id", new JsonString().setValue("2")).add("f1", new JsonString().setValue("doc2")))
                    .execute(); // Inject _ids.
            assertEquals(0, res1.getGeneratedIds().size());
        }

        DocResult docs = this.collection.find("f1 like 'doc%'").execute();
        assertEquals(2, docs.count());

        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion(("8.0.5")))) {
            AddResult res2 = this.collection.add(new DbDoc[] { this.collection.newDoc().add("f1", new JsonString().setValue("doc3")),
                    this.collection.newDoc().add("f1", new JsonString().setValue("doc4")) }).execute();
            assertTrue(res2.getGeneratedIds().get(0).matches("[a-f0-9]{28}"));
        } else {
            AddResult res2 = this.collection
                    .add(new DbDoc[] { this.collection.newDoc().add("_id", new JsonString().setValue("3")).add("f1", new JsonString().setValue("doc3")),
                            this.collection.newDoc().add("_id", new JsonString().setValue("4")).add("f1", new JsonString().setValue("doc4")) })
                    .execute();
            assertEquals(0, res2.getGeneratedIds().size());
        }

        docs = this.collection.find("f1 like 'doc%'").execute();
        assertEquals(4, docs.count());
    }

    @Test
    @Ignore("needs implemented")
    public void testBasicAddMap() {
        if (!this.isSetForXTests) {
            return;
        }
        Map<String, Object> doc = new HashMap<>();
        doc.put("x", 1);
        doc.put("y", "this is y");
        doc.put("z", new BigDecimal("44.22"));
        AddResult res = this.collection.add(doc).execute();
        assertTrue(res.getGeneratedIds().get(0).matches("[a-f0-9]{28}"));

        DocResult docs = this.collection.find("z >= 44.22").execute();
        DbDoc d = docs.next();
        JsonString val = (JsonString) d.get("y");
        assertEquals("this is y", val.getString());
    }

    @Test
    public void testAddWithAssignedId() {
        if (!this.isSetForXTests) {
            return;
        }
        String json1 = "{'_id': 'Id#1', 'name': 'assignedId'}".replaceAll("'", "\"");
        String json2 = "{'name': 'autoId'}".replaceAll("'", "\"");
        AddResult res;
        int expectedAssignedIds;
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            res = this.collection.add(json1).execute();
            assertThrows(XProtocolError.class, "ERROR 5115 \\(HY000\\) Document is missing a required field", () -> this.collection.add(json2).execute());
            expectedAssignedIds = 0;
        } else {
            res = this.collection.add(json1).add(json2).execute();
            expectedAssignedIds = 1;
        }

        List<String> ids = res.getGeneratedIds();
        assertEquals(expectedAssignedIds, ids.size());

        for (String strId : ids) { // Although the _id="Id#1" is not returned in getGeneratedIds(), it may be in a future version from some other method.
            DocResult docs = this.collection.find("_id == '" + strId + "'").execute();
            DbDoc d = docs.next();
            JsonString val = (JsonString) d.get("name");
            if (strId.equals("Id#1")) {
                assertEquals("assignedId", val.getString());
            } else {
                assertEquals("autoId", val.getString());
            }
        }
    }

    @Test
    public void testChainedAdd() {
        if (!this.isSetForXTests) {
            return;
        }
        String json = "{'_id': 1}".replaceAll("'", "\"");
        this.collection.add(json).add(json.replaceAll("1", "2")).execute();

        assertEquals(true, this.collection.find("_id = 1").execute().hasNext());
        assertEquals(true, this.collection.find("_id = 2").execute().hasNext());
        assertEquals(false, this.collection.find("_id = 3").execute().hasNext());
    }

    @Test
    public void testAddLargeDocument() {
        if (!this.isSetForXTests) {
            return;
        }
        int docSize = 255 * 1024;
        StringBuilder b = new StringBuilder("{\"_id\": \"large_doc\", \"large_field\":\"");
        for (int i = 0; i < docSize; ++i) {
            b.append('.');
        }
        String s = b.append("\"}").toString();
        this.collection.add(s).execute();

        DocResult docs = this.collection.find().execute();
        DbDoc d = docs.next();
        assertEquals(docSize, ((JsonString) d.get("large_field")).getString().length());
    }

    @Test
    public void testAddNoDocs() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        Result res = this.collection.add(new DbDoc[] {}).execute();
        assertEquals(0, res.getAffectedItemsCount());
        assertEquals(0, res.getWarningsCount());

        CompletableFuture<AddResult> f = this.collection.add(new DbDoc[] {}).executeAsync();
        res = f.get();
        assertEquals(0, res.getAffectedItemsCount());
        assertEquals(0, res.getWarningsCount());
    }

    @Test
    public void testAddOrReplaceOne() {
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
            return;
        }
        this.collection.add("{\"_id\": \"id1\", \"a\": 1}").execute();

        // new _id
        Result res = this.collection.addOrReplaceOne("id2", this.collection.newDoc().add("a", new JsonNumber().setValue("2")));
        assertEquals(1, res.getAffectedItemsCount());
        assertEquals(2, this.collection.count());
        assertTrue(this.collection.find("a = 1").execute().hasNext());
        assertTrue(this.collection.find("a = 2").execute().hasNext());

        // existing _id
        res = this.collection.addOrReplaceOne("id1", this.collection.newDoc().add("a", new JsonNumber().setValue("3")));
        assertEquals(2, res.getAffectedItemsCount());
        assertEquals(2, this.collection.count());
        assertFalse(this.collection.find("a = 1").execute().hasNext());
        assertTrue(this.collection.find("a = 2").execute().hasNext());
        assertTrue(this.collection.find("a = 3").execute().hasNext());

        // existing _id in a new document
        res = this.collection.addOrReplaceOne("id1", "{\"_id\": \"id1\", \"a\": 4}");
        assertEquals(2, res.getAffectedItemsCount());
        assertEquals(2, this.collection.count());
        assertTrue(this.collection.find("a = 2").execute().hasNext());
        assertFalse(this.collection.find("a = 3").execute().hasNext());
        assertTrue(this.collection.find("a = 4").execute().hasNext());

        // a new document with _id field that doesn't match id parameter
        assertThrows(XDevAPIError.class, "Document already has an _id that doesn't match to id parameter", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionAddTest.this.collection.addOrReplaceOne("id2",
                        CollectionAddTest.this.collection.newDoc().add("_id", new JsonString().setValue("id111")));
                return null;
            }
        });

        // null document
        assertThrows(XDevAPIError.class, "Parameter 'doc' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionAddTest.this.collection.addOrReplaceOne("id2", (DbDoc) null);
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'jsonString' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionAddTest.this.collection.addOrReplaceOne("id2", (String) null);
                return null;
            }
        });

        // null id parameter
        assertThrows(XDevAPIError.class, "Parameter 'id' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionAddTest.this.collection.addOrReplaceOne(null,
                        CollectionAddTest.this.collection.newDoc().add("_id", new JsonString().setValue("id111")));
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'id' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionAddTest.this.collection.addOrReplaceOne(null, "{\"_id\": \"id100\", \"a\": 100}");
                return null;
            }
        });

    }

    /**
     * Tests fix for Bug#21914769, NPE WHEN TRY TO EXECUTE INVALID JSON STRING.
     */
    @Test
    public void testBug21914769() {
        if (!this.isSetForXTests) {
            return;
        }

        assertThrows(WrongArgumentException.class, "Invalid whitespace character ']'.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionAddTest.this.collection.add("{\"_id\":\"1004\",\"F1\": ] }").execute();
                return null;
            }
        });

    }

    /**
     * Test for Bug#92264 (28594434), JSONPARSER PUTS UNNECESSARY MAXIMUM LIMIT ON JSONNUMBER TO 10 DIGITS.
     * 
     * @throws Exception
     */
    @Test
    public void testBug92264() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        this.collection.add("{\"_id\":\"1\",\"dataCreated\": 1546300800000}").execute();

        DocResult docs = this.collection.find("dataCreated = 1546300800000").execute();
        assertTrue(docs.hasNext());
        DbDoc doc = docs.next();
        assertEquals("1546300800000", doc.get("dataCreated").toString());
        assertEquals(new BigDecimal("1546300800000"), ((JsonNumber) doc.get("dataCreated")).getBigDecimal());
    }
}
