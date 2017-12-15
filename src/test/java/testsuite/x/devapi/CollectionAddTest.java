/*
 * Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.
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

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mysql.cj.api.xdevapi.AddResult;
import com.mysql.cj.api.xdevapi.DocResult;
import com.mysql.cj.api.xdevapi.Result;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.x.core.XDevAPIError;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonString;

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
        AddResult res = this.collection.add(json).execute();
        assertTrue(res.getDocumentIds().get(0).matches("[a-f0-9]{32}"));
        assertTrue(res.getDocumentId().matches("[a-f0-9]{32}"));

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
        DbDoc doc = new DbDoc().add("firstName", new JsonString().setValue("Georgia"));
        doc.add("middleName", new JsonString().setValue("Totto"));
        doc.add("lastName", new JsonString().setValue("O'Keeffe"));
        AddResult res = this.collection.add(doc).execute();
        assertTrue(res.getDocumentIds().get(0).matches("[a-f0-9]{32}"));
        assertTrue(res.getDocumentId().matches("[a-f0-9]{32}"));

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
        AddResult res1 = this.collection.add(new DbDoc().add("f1", new JsonString().setValue("doc1")), new DbDoc().add("f1", new JsonString().setValue("doc2")))
                .execute();
        assertTrue(res1.getDocumentIds().get(0).matches("[a-f0-9]{32}"));
        assertThrows(XDevAPIError.class, "Method getDocumentId\\(\\) is allowed only for a single document add\\(\\) result.", new Callable<Void>() {
            public Void call() throws Exception {
                res1.getDocumentId();
                return null;
            }
        });

        DocResult docs = this.collection.find("f1 like 'doc%'").execute();
        assertEquals(2, docs.count());

        AddResult res2 = this.collection
                .add(new DbDoc[] { new DbDoc().add("f1", new JsonString().setValue("doc3")), new DbDoc().add("f1", new JsonString().setValue("doc4")) })
                .execute();
        assertTrue(res2.getDocumentIds().get(0).matches("[a-f0-9]{32}"));
        assertThrows(XDevAPIError.class, "Method getDocumentId\\(\\) is allowed only for a single document add\\(\\) result.", new Callable<Void>() {
            public Void call() throws Exception {
                res2.getDocumentId();
                return null;
            }
        });

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
        assertTrue(res.getDocumentIds().get(0).matches("[a-f0-9]{32}"));
        assertTrue(res.getDocumentId().matches("[a-f0-9]{32}"));

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
        AddResult res = this.collection.add(json1).add(json2).execute();

        List<String> ids = res.getDocumentIds();
        assertEquals(2, ids.size());

        assertThrows(XDevAPIError.class, "Method getDocumentId\\(\\) is allowed only for a single document add\\(\\) result.", new Callable<Void>() {
            public Void call() throws Exception {
                res.getDocumentId();
                return null;
            }
        });

        for (String strId : ids) {
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
    public void testAddNoDocs() {
        if (!this.isSetForXTests) {
            return;
        }
        Result res = this.collection.add(new DbDoc[] {}).execute();
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
        Result res = this.collection.addOrReplaceOne("id2", new DbDoc().add("a", new JsonNumber().setValue("2")));
        assertEquals(1, res.getAffectedItemsCount());
        assertEquals(2, this.collection.count());
        assertTrue(this.collection.find("a = 1").execute().hasNext());
        assertTrue(this.collection.find("a = 2").execute().hasNext());

        // existing _id
        res = this.collection.addOrReplaceOne("id1", new DbDoc().add("a", new JsonNumber().setValue("3")));
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
                CollectionAddTest.this.collection.addOrReplaceOne("id2", new DbDoc().add("_id", new JsonString().setValue("id111")));
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
                CollectionAddTest.this.collection.addOrReplaceOne(null, new DbDoc().add("_id", new JsonString().setValue("id111")));
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
}
