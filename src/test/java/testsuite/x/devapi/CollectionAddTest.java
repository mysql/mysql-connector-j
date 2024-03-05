/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
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

package testsuite.x.devapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.AddResult;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DbDocImpl;
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.JsonArray;
import com.mysql.cj.xdevapi.JsonLiteral;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.Result;
import com.mysql.cj.xdevapi.XDevAPIError;

public class CollectionAddTest extends BaseCollectionTestCase {

    @Test
    public void testBasicAddString() {
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
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
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

        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
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
    @Disabled("Collection.add(Map<String, ?> doc) is not implemented yet.")
    public void testBasicAddMap() {
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
        String json = "{'_id': 1}".replaceAll("'", "\"");
        this.collection.add(json).add(json.replaceAll("1", "2")).execute();

        assertEquals(true, this.collection.find("_id = 1").execute().hasNext());
        assertEquals(true, this.collection.find("_id = 2").execute().hasNext());
        assertEquals(false, this.collection.find("_id = 3").execute().hasNext());
    }

    @Test
    public void testAddLargeDocument() {
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
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3")), "MySQL 8.0.3+ is required to run this test.");

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
        assertThrows(XDevAPIError.class, "Replacement document has an _id that is different than the matched document\\.", () -> {
            CollectionAddTest.this.collection.addOrReplaceOne("id2", CollectionAddTest.this.collection.newDoc().add("_id", new JsonString().setValue("id111")));
            return null;
        });

        // null document
        assertThrows(XDevAPIError.class, "Parameter 'doc' must not be null.", () -> {
            CollectionAddTest.this.collection.addOrReplaceOne("id2", (DbDoc) null);
            return null;
        });
        assertThrows(XDevAPIError.class, "Parameter 'jsonString' must not be null.", () -> {
            CollectionAddTest.this.collection.addOrReplaceOne("id2", (String) null);
            return null;
        });

        // null id parameter
        assertThrows(XDevAPIError.class, "Parameter 'id' must not be null.", () -> {
            CollectionAddTest.this.collection.addOrReplaceOne(null, CollectionAddTest.this.collection.newDoc().add("_id", new JsonString().setValue("id111")));
            return null;
        });
        assertThrows(XDevAPIError.class, "Parameter 'id' must not be null.", () -> {
            CollectionAddTest.this.collection.addOrReplaceOne(null, "{\"_id\": \"id100\", \"a\": 100}");
            return null;
        });
    }

    /**
     * Tests fix for Bug#21914769, NPE WHEN TRY TO EXECUTE INVALID JSON STRING.
     */
    @Test
    public void testBug21914769() {
        assertThrows(WrongArgumentException.class, "Invalid whitespace character ']'.", () -> {
            CollectionAddTest.this.collection.add("{\"_id\":\"1004\",\"F1\": ] }").execute();
            return null;
        });
    }

    /**
     * Test for Bug#92264 (28594434), JSONPARSER PUTS UNNECESSARY MAXIMUM LIMIT ON JSONNUMBER TO 10 DIGITS.
     *
     * @throws Exception
     */
    @Test
    public void testBug92264() throws Exception {
        this.collection.add("{\"_id\":\"1\",\"dataCreated\": 1546300800000}").execute();

        DocResult docs = this.collection.find("dataCreated = 1546300800000").execute();
        assertTrue(docs.hasNext());
        DbDoc doc = docs.next();
        assertEquals("1546300800000", doc.get("dataCreated").toString());
        assertEquals(new BigDecimal("1546300800000"), ((JsonNumber) doc.get("dataCreated")).getBigDecimal());
    }

    /**
     * Test for Bug92819 (28834959), EXPRPARSER THROWS WRONGARGUMENTEXCEPTION WHEN PARSING EMPTY JSON ARRAY.
     */
    @Test
    public void testBug92819() {
        this.collection.add("{\"_id\":\"1\",\"emptyArray\": []}").execute();
        DocResult docs = this.collection.find("_id = '1'").execute();
        assertTrue(docs.hasNext());
        DbDoc doc = docs.next();
        assertEquals("[]", doc.get("emptyArray").toString());
    }

    @Test
    public void testCollectionAddBasic() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, maxrec = 100;

        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonString().setValue("Field-2-Data-" + i));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(300 + i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        /* add(DbDoc doc) */
        DbDoc newDoc = new DbDocImpl();
        newDoc.add("_id", new JsonString().setValue(String.valueOf(maxrec + 1000)));
        newDoc.add("F1", new JsonString().setValue("Field-1-Data-" + maxrec));
        newDoc.add("F2", new JsonString().setValue("Field-2-Data-" + maxrec));
        newDoc.add("F3", new JsonNumber().setValue(String.valueOf(300 + maxrec)));
        this.collection.add(newDoc).execute();

        /* add(String jsonString) */
        String json = "{'_id':'" + (maxrec + 1000 + 1) + "','F1':'Field-1-Data-" + (maxrec + 1) + "','F2':'Field-2-Data-" + (maxrec + 1) + "','F3':"
                + (300 + maxrec + 1) + "}";
        json = json.replaceAll("'", "\"");
        this.collection.add(json).execute();

        /* No _Id Field and chained add() */
        json = "{'F1': 'Field-1-Data-9999','F2': 'Field-2-Data-9999','F3': 'Field-3-Data-9999'}".replaceAll("'", "\"");
        this.collection.add(json).add(json.replaceAll("9", "8")).execute();

        assertEquals(maxrec + 4, this.collection.count());
        DocResult docs = this.collection.find("$._id = '1000'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        DbDoc doc = null;
        doc = docs.next();
        assertEquals("1000", ((JsonString) doc.get("_id")).getString());
        System.out.println("ID :" + ((JsonString) doc.get("_id")).getString());
        System.out.println("F1 :" + ((JsonString) doc.get("f1")).getString());
        System.out.println("F2 :" + ((JsonString) doc.get("f2")).getString());
        System.out.println("F3 :" + ((JsonNumber) doc.get("f3")).getInteger());
    }

    @Test
    public void testCollectionAddStrings() throws Exception {
        DbDoc doc = null;
        DocResult docs = null;
        String json = "";

        json = "{'_id':'1001','F1':'{Open Brace','F2':'}Close Brace','F3':'$Dollor Sign'}".replaceAll("'", "\"");
        this.collection.add(json).execute();
        json = "{'_id':'1002','F1':'{Open and }Close Brace','F2':'}Close and {Open Brace','F3':'$Dollor and << Shift Sign'}".replaceAll("'", "\"");
        this.collection.add(json).execute();
        json = "{'_id':'1003','F1':'{{2Open and }}2Close Brace','F2':'}}2Close and {{2Open Brace','F3':'$.Dollor dot and $$2Dollor'}".replaceAll("'", "\"");
        this.collection.add(json).execute();
        json = "{'_id':'1004','F1':'{{{3Open and }}}3Close Brace','F2':'}}}3Close and {{{3Open Brace','F3':'$.Dollor dot and ,Comma'}".replaceAll("'", "\"");
        this.collection.add(json).execute();

        json = "{'_id':'1005','F1':'[Square Open','F2':']Square Close','F3':'$.Dollor dot and :Colon'}".replaceAll("'", "\"");
        this.collection.add(json).execute();

        json = "{'_id':'1006','F1':'[Square Open ]Square Close','F2':']Square Close [Square Open','F3':'$.,:{[}] '}".replaceAll("'", "\"");
        this.collection.add(json).execute();

        /* find with Condition */
        docs = this.collection.find("$.F1 Like '{{2%}%2%'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        doc = docs.next();
        assertEquals("{{2Open and }}2Close Brace", ((JsonString) doc.get("f1")).getString());
        assertEquals("}}2Close and {{2Open Brace", ((JsonString) doc.get("f2")).getString());
        assertEquals("$.Dollor dot and $$2Dollor", ((JsonString) doc.get("f3")).getString());
        assertFalse(docs.hasNext());

        docs = this.collection.find("$.F1 Like '[%]%'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        doc = docs.next();
        assertEquals("[Square Open ]Square Close", ((JsonString) doc.get("f1")).getString());
        assertEquals("]Square Close [Square Open", ((JsonString) doc.get("f2")).getString());
        assertEquals("$.,:{[}] ", ((JsonString) doc.get("f3")).getString());
        assertFalse(docs.hasNext());

        docs = this.collection.find("$.F3 Like '$%]%'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        doc = docs.next();
        assertEquals("[Square Open ]Square Close", ((JsonString) doc.get("f1")).getString());
        assertEquals("]Square Close [Square Open", ((JsonString) doc.get("f2")).getString());
        assertEquals("$.,:{[}] ", ((JsonString) doc.get("f3")).getString());
        assertFalse(docs.hasNext());
    }

    @Test
    public void testCollectionAddBigKeys() throws Exception {
        int i = 0, j = 0;
        int maxkey = 10;
        int maxrec = 5;
        int keylen = 1024;
        String key_sub = buildString(keylen, 'X');
        String data_sub = "Data";

        /* Insert maxrec records with maxkey (key,value) pairs with key length=keylen */
        String key, data, query;
        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc = new DbDocImpl();
            newDoc.add("_id", new JsonNumber().setValue(String.valueOf(i)));
            for (j = 0; j < maxkey; j++) {
                key = key_sub + j;
                data = data_sub + j;
                newDoc.add(key, new JsonString().setValue(data));
            }
            this.collection.add(newDoc).execute();
            newDoc = null;
        }
        assertEquals(maxrec, this.collection.count());

        /* Fetch all keys */
        query = "$._id as _id";
        for (j = 0; j < maxkey; j++) {
            key = key_sub + j;
            query = query + ",$." + key + " as " + key;
        }
        DocResult docs = this.collection.find().orderBy("$._id").fields(query).execute();
        DbDoc doc = null;
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            for (j = 0; j < maxkey; j++) {
                key = key_sub + j;
                data = data_sub + j;
                assertEquals(data, ((JsonString) doc.get(key)).getString());
            }
            i++;
        }
        assertEquals(maxrec, i);
    }

    @Test
    public void testCollectionAddBigKeyData() throws Exception {
        int i = 0, j = 0;
        int maxkey = 10;
        int maxrec = 5;
        int keylen = 10;
        int datalen = 1 * 5;
        String key_sub = buildString(keylen, 'X');
        String data_sub = buildString(datalen, 'X');

        /* Insert maxrec records with maxkey (key,value) pairs with key length=keylen and datalength=datalen */
        String key, data, query;
        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc = new DbDocImpl();
            newDoc.add("_id", new JsonNumber().setValue(String.valueOf(i)));
            for (j = 0; j < maxkey; j++) {
                key = key_sub + j;
                data = data_sub + j;
                newDoc.add(key, new JsonString().setValue(data));
            }
            this.collection.add(newDoc).execute();
            newDoc = null;
        }
        assertEquals(maxrec, this.collection.count());

        /* Fetch all keys */
        query = "$._id as _id";
        for (j = 0; j < maxkey; j++) {
            key = key_sub + j;
            query = query + ",$." + key + " as " + key;
        }
        DocResult docs = this.collection.find().orderBy("$._id").fields(query).execute();
        DbDoc doc = null;
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            for (j = 0; j < maxkey; j++) {
                key = key_sub + j;
                data = data_sub + j;
                assertEquals(data, ((JsonString) doc.get(key)).getString());
            }
            i++;
        }
        assertEquals(maxrec, i);
    }

    @Test
    public void testCollectionAddBigKeyDataString() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0;
        int maxrec = 5;
        int datalen = 1 * 5;
        String longdata = "";
        String json = "";

        /* Insert maxrec (key,value) pairs with datalength=datalen */
        AddResult res = null;
        for (i = 0; i < maxrec; i++) {
            json = "{\"F1\":\"Field-6-Data-" + i + "\",\"F2\":\"";
            longdata = buildString(datalen + i, 'X');
            json = json + longdata + "\"}";
            res = this.collection.add(json).add(json.replaceAll("6", "7")).add(json.replaceAll("6", "8")).execute();
            System.out.println("getGeneratedIds: " + res.getGeneratedIds());

        }
        assertEquals(maxrec * 3, this.collection.count());

        /* Fetch all keys */
        DocResult docs = this.collection.find("$.F1 like '%-6-%'").orderBy("$.F2 asc").fields("$._id as _id, $.F1 as fld1, $.F2 as fld2").execute();
        DbDoc doc = null;
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            longdata = buildString(datalen + i, 'X');
            assertEquals(longdata, ((JsonString) doc.get("fld2")).getString());
            i++;
        }
        assertEquals(maxrec, i);
    }

    @Test
    public void testCollectionAddManyKeys() throws Exception {
        int i = 0, j = 0;
        int maxkey = 500;
        int maxrec = 5;
        String key_sub = "keyname_";
        String data_sub = "Data";

        /* Insert maxrec each with maxkey number of (key,value) pairs */
        String key, data, query;
        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc = new DbDocImpl();
            newDoc.add("_id", new JsonNumber().setValue(String.valueOf(i)));
            for (j = 0; j < maxkey; j++) {
                key = key_sub + j;
                data = data_sub + j;
                newDoc.add(key, new JsonString().setValue(data));
            }
            this.collection.add(newDoc).execute();
            newDoc = null;
        }
        assertEquals(maxrec, this.collection.count());

        /* Fetch all keys */
        query = "$._id as _id";
        for (j = 0; j < maxkey; j++) {
            key = key_sub + j;
            query = query + ",$." + key + " as " + key;
        }
        DocResult docs = this.collection.find().orderBy("$._id").fields(query).execute();
        i = 0;
        while (docs.hasNext()) {
            docs.next();
            i++;
        }
        assertEquals(maxrec, i);

        /* fetch maxrec-1 records */
        docs = this.collection.find("$._id < " + (maxrec - 1)).orderBy("$._id").fields("$._id as _id, $." + key_sub + (maxkey - 1) + " as Key1").execute();
        i = 0;
        while (docs.hasNext()) {
            docs.next();
            i++;
        }
        assertEquals(maxrec - 1, i);
    }

    @Test
    public void testCollectionAddManyRecords() throws Exception {
        int i = 0, maxrec = 10;

        /* add(DbDoc[] docs) -> Insert maxrec number of records in ne execution */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonNumber().setValue(String.valueOf(i)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(300 + i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals(maxrec, this.collection.count());
        DocResult docs = this.collection.find("$._id >= 0").orderBy("$._id").fields("$._id as _id, $.F1 as f1, $.F2 as f2").execute();
        DbDoc doc = null;
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals((long) i, (long) ((JsonNumber) doc.get("_id")).getInteger());
            i++;
        }
        assertEquals(maxrec, i);
    }

    /**/
    @Test
    public void testCollectionAddArray() throws Exception {
        int i = 0, j = 0, k = 0, maxrec = 5, arraySize = 9;

        /* add(DbDoc[] docs) -> Array data */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            JsonArray jarray1 = new JsonArray();
            JsonArray jarray2 = new JsonArray();
            JsonArray jarray3 = new JsonArray();
            JsonArray jarray4 = new JsonArray();
            JsonArray jarray5 = new JsonArray();
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonNumber().setValue(String.valueOf(i)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            for (j = 0; j < arraySize; j++) {
                jarray1.addValue(new JsonNumber().setValue(String.valueOf(j)));
            }
            newDoc2.add("ARR_INT", jarray1);

            for (j = 0; j < arraySize; j++) {
                jarray2.addValue(new JsonString().setValue("Data-" + j));
            }
            newDoc2.add("ARR_STR", jarray2);

            for (j = 0; j < arraySize; j++) {
                if (j % 3 == 2) {
                    jarray3.addValue(JsonLiteral.FALSE);
                } else if (j % 3 == 1) {
                    jarray3.addValue(JsonLiteral.TRUE);
                } else {
                    jarray3.addValue(JsonLiteral.NULL);
                }
            }
            newDoc2.add("ARR_LIT", jarray3);

            for (j = 0; j < arraySize; j++) {
                JsonArray subarray = new JsonArray();

                for (k = 0; k < 5; k++) {
                    subarray.addValue(new JsonNumber().setValue(String.valueOf(j)));
                }
                jarray4.addValue(subarray);
                subarray = null;
            }
            newDoc2.add("ARR_ARR", jarray4);

            for (j = 0; j < arraySize; j++) {
                if (j % 3 == 2) {
                    jarray5.addValue(JsonLiteral.FALSE);
                } else if (j % 3 == 1) {
                    jarray5.addValue(new JsonString().setValue("Data-" + j));
                } else {
                    jarray5.addValue(new JsonNumber().setValue(String.valueOf(j)));
                }

            }
            newDoc2.add("ARR_MIX", jarray5);
            this.collection.add(newDoc2).execute();
            jsonlist[i] = newDoc2;
            newDoc2 = null;
            jarray1 = null;
            jarray2 = null;
            jarray3 = null;
            jarray4 = null;
            jarray5 = null;
        }
        //coll.add(jsonlist).execute();
        jsonlist = null;
        assertEquals(maxrec, this.collection.count());
        DocResult docs = this.collection.find("$._id >= 0").orderBy("$._id").fields("$._id as _id, $.F1 as f1, $.F2 as f2").execute();
        DbDoc doc = null;
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals((long) i, (long) ((JsonNumber) doc.get("_id")).getInteger());
            i++;
        }
        assertEquals(maxrec, i);
    }

    @Test
    public void testGetGeneratedIds() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        AddResult res = null;
        DbDoc doc = null;
        DocResult docs = null;
        int i = 0;

        //One record using String
        String json = "{\"FLD1\":\"Data1\"}";
        res = this.collection.add(json).execute();
        List<String> docIds = res.getGeneratedIds();
        assertTrue(docIds.get(0).matches("[a-f0-9]{28}"));
        assertEquals(1, this.collection.count());
        assertEquals(1, docIds.size());

        //More than One record using String
        json = "{\"FLD1\":\"Data2\"}";
        res = this.collection.add(json).add("{}").add("{\"_id\":\"id1\"}").add("{\"FLD1\":\"Data3\"}").execute();
        docIds = res.getGeneratedIds();
        assertEquals(5, this.collection.count());
        assertEquals(3, docIds.size());

        //More than One record using String, and single add()
        json = "{\"FLD1\":\"Data15\"}";
        res = this.collection.add(json, "{}", "{\"_id\":\"id2\"}", "{\"FLD1\":\"Data16\"}").execute();
        docIds = res.getGeneratedIds();
        assertEquals(9, this.collection.count());
        assertEquals(3, docIds.size());

        //One record using DbDoc
        DbDoc newDoc2 = new DbDocImpl();
        newDoc2.add("FLD1", new JsonString().setValue("Data4"));
        res = this.collection.add(newDoc2).execute();
        docIds = res.getGeneratedIds();
        assertEquals(10, this.collection.count());
        assertEquals(1, docIds.size());
        assertTrue(docIds.get(0).matches("[a-f0-9]{28}"));

        //More Than One record using DbDoc
        newDoc2.clear();
        newDoc2.add("FLD1", new JsonString().setValue("Data5"));
        DbDoc newDoc3 = new DbDocImpl();
        newDoc3.add("FLD1", new JsonString().setValue("Data6"));
        res = this.collection.add(newDoc2).add(newDoc3).execute();
        docIds = res.getGeneratedIds();
        assertEquals(12, this.collection.count());
        assertEquals(2, docIds.size());
        assertTrue(docIds.get(0).compareTo(docIds.get(1)) < 0);

        //One record using DbDoc[]
        DbDoc[] jsonlist1 = new DbDocImpl[1];
        newDoc2.clear();
        newDoc2.add("FLD1", new JsonString().setValue("Data7"));
        jsonlist1[0] = newDoc2;
        res = this.collection.add(jsonlist1).execute();
        docIds = res.getGeneratedIds();
        assertEquals(13, this.collection.count());
        assertEquals(1, docIds.size());
        assertTrue(docIds.get(0).matches("[a-f0-9]{28}"));

        //More Than One record using DbDoc[]
        DbDoc[] jsonlist = new DbDocImpl[5];
        for (i = 0; i < 5; i++) {
            DbDoc newDoc = new DbDocImpl();
            newDoc.add("FLD1", new JsonString().setValue("Data" + (i + 8)));
            if (i % 2 == 0) {
                newDoc.add("_id", new JsonString().setValue("id-" + (i + 8)));
            }
            jsonlist[i] = newDoc;
            newDoc = null;
        }
        res = this.collection.add(jsonlist).execute();
        docIds = res.getGeneratedIds();
        assertEquals(18, this.collection.count());
        assertEquals(2, docIds.size());

        json = "{}";
        res = this.collection.add(json).execute();
        docIds = res.getGeneratedIds();
        assertTrue(docIds.get(0).matches("[a-f0-9]{28}"));
        assertEquals(19, this.collection.count());
        assertEquals(1, docIds.size());

        //Verify that when _id is provided by client, getGeneratedIds() will return empty
        res = this.collection.add("{\"_id\":\"00001273834abcdfe\",\"FLD1\":\"Data1\",\"name\":\"name1\"}",
                "{\"_id\":\"000012738uyie98rjdeje\",\"FLD2\":\"Data2\",\"name\":\"name1\"}",
                "{\"_id\":\"00001273y834uhf489fe\",\"FLD3\":\"Data3\",\"name\":\"name1\"}").execute();
        docIds = res.getGeneratedIds();
        assertEquals(22, this.collection.count());
        assertEquals(0, docIds.size());

        res = this.collection.add("{\"_id\":null,\"FLD1\":\"nulldata\"}").execute();
        docIds = res.getGeneratedIds();
        assertEquals(23, this.collection.count());
        assertEquals(0, docIds.size());
        docs = this.collection.find("$.FLD1 == 'nulldata'").execute();
        doc = docs.next();
        assertEquals("null", ((JsonLiteral) doc.get("_id")).toString());

        //Try inserting duplicate _ids. User should get error
        assertThrows(XProtocolError.class, "ERROR 5116 \\(HY000\\) Document contains a field value that is not unique but required to be",
                () -> this.collection.add("{\"_id\":\"abcd1234\",\"FLD1\":\"Data1\"}").add("{\"_id\":\"abcd1234\",\"FLD1\":\"Data2\"}").execute());
    }

}
