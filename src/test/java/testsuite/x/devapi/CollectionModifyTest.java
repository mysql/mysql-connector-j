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

import static com.mysql.cj.xdevapi.Expression.expr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.util.concurrent.Callable;

import org.junit.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.xdevapi.AddResult;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DbDocImpl;
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.JsonArray;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonParser;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.Result;
import com.mysql.cj.xdevapi.XDevAPIError;

/**
 * @todo
 */
public class CollectionModifyTest extends BaseCollectionTestCase {

    @Test
    public void testSet() {
        if (!this.isSetForXTests) {
            return;
        }

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{}").execute();
        }

        this.collection.modify("true").set("a", "Value for a").execute();
        this.collection.modify("1 == 1").set("b", "Value for b").execute();
        this.collection.modify("false").set("c", "Value for c").execute();
        this.collection.modify("0 == 1").set("d", "Value for d").execute();

        DocResult res = this.collection.find("a = 'Value for a'").execute();
        DbDoc jd = res.next();
        assertEquals("Value for a", ((JsonString) jd.get("a")).getString());
        assertEquals("Value for b", ((JsonString) jd.get("b")).getString());
        assertNull(jd.get("c"));
        assertNull(jd.get("d"));
    }

    @Test
    public void testUnset() {
        if (!this.isSetForXTests) {
            return;
        }
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"x\":\"100\", \"y\":\"200\", \"z\":1}").execute(); // Requires manual _id.
            this.collection.add("{\"_id\": \"2\", \"a\":\"100\", \"b\":\"200\", \"c\":1}").execute();
        } else {
            this.collection.add("{\"x\":\"100\", \"y\":\"200\", \"z\":1}").execute();
            this.collection.add("{\"a\":\"100\", \"b\":\"200\", \"c\":1}").execute();
        }

        this.collection.modify("true").unset("$.x").unset("$.y").execute();
        this.collection.modify("true").unset("$.a", "$.b").execute();

        DocResult res = this.collection.find().execute();
        DbDoc jd = res.next();
        assertNull(jd.get("x"));
        assertNull(jd.get("y"));
        assertNull(jd.get("a"));
        assertNull(jd.get("b"));
    }

    @Test
    public void testReplace() {
        if (!this.isSetForXTests) {
            return;
        }
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"x\":100}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{\"x\":100}").execute();
        }
        this.collection.modify("true").change("$.x", "99").execute();

        DocResult res = this.collection.find().execute();
        DbDoc jd = res.next();
        assertEquals("99", ((JsonString) jd.get("x")).getString());
    }

    @Test
    public void testArrayAppend() {
        if (!this.isSetForXTests) {
            return;
        }

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"x\":[8,16,32]}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{\"x\":[8,16,32]}").execute();
        }
        this.collection.modify("true").arrayAppend("$.x", "64").execute();

        DocResult res = this.collection.find().execute();
        DbDoc jd = res.next();
        JsonArray xArray = (JsonArray) jd.get("x");
        assertEquals(new Integer(8), ((JsonNumber) xArray.get(0)).getInteger());
        assertEquals(new Integer(16), ((JsonNumber) xArray.get(1)).getInteger());
        assertEquals(new Integer(32), ((JsonNumber) xArray.get(2)).getInteger());
        // TODO: better arrayAppend() overloads?
        assertEquals("64", ((JsonString) xArray.get(3)).getString());
        assertEquals(4, xArray.size());
    }

    @Test
    public void testArrayInsert() {
        if (!this.isSetForXTests) {
            return;
        }

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"x\":[1,2]}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{\"x\":[1,2]}").execute();
        }
        this.collection.modify("true").arrayInsert("$.x[1]", 43).execute();
        // same as append
        this.collection.modify("true").arrayInsert("$.x[3]", 44).execute();

        DocResult res = this.collection.find().execute();
        DbDoc jd = res.next();
        JsonArray xArray = (JsonArray) jd.get("x");
        assertEquals(new Integer(1), ((JsonNumber) xArray.get(0)).getInteger());
        assertEquals(new Integer(43), ((JsonNumber) xArray.get(1)).getInteger());
        assertEquals(new Integer(2), ((JsonNumber) xArray.get(2)).getInteger());
        assertEquals(new Integer(44), ((JsonNumber) xArray.get(3)).getInteger());
        assertEquals(4, xArray.size());
    }

    @Test
    public void testJsonModify() {
        if (!this.isSetForXTests) {
            return;
        }

        DbDoc nestedDoc = new DbDocImpl().add("z", new JsonNumber().setValue("100"));
        DbDoc doc = new DbDocImpl().add("x", new JsonNumber().setValue("3")).add("y", nestedDoc);

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"x\":1, \"y\":1}").execute(); // Requires manual _id.
            this.collection.add("{\"_id\": \"2\", \"x\":2, \"y\":2}").execute();
            this.collection.add(doc.add("_id", new JsonString().setValue("3"))).execute(); // Inject an _id.
            this.collection.add("{\"_id\": \"4\", \"x\":4, \"m\":1}").execute();
        } else {
            this.collection.add("{\"x\":1, \"y\":1}").execute();
            this.collection.add("{\"x\":2, \"y\":2}").execute();
            this.collection.add(doc).execute();
            this.collection.add("{\"x\":4, \"m\":1}").execute();
        }

        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionModifyTest.this.collection.modify(null).set("y", nestedDoc).execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionModifyTest.this.collection.modify(" ").set("y", nestedDoc).execute();
                return null;
            }
        });

        this.collection.modify("y = 1").set("y", nestedDoc).execute();
        this.collection.modify("y = :n").set("y", nestedDoc).bind("n", 2).execute();

        this.collection.modify("x = 1").set("m", 1).execute();
        this.collection.modify("true").change("$.m", nestedDoc).execute();

        assertEquals(1, this.collection.find("x = :x").bind("x", 1).execute().count());
        assertEquals(0, this.collection.find("y = :y").bind("y", 2).execute().count());
        assertEquals(3, this.collection.find("y = {\"z\": 100}").execute().count());
        assertEquals(2, this.collection.find("m = {\"z\": 100}").execute().count());

        // TODO check later whether it's possible; for now placeholders are of Scalar type only
        //assertEquals(1, this.collection.find("y = :y").bind("y", nestedDoc).execute().count());

        // literal won't match JSON docs
        assertEquals(0, this.collection.find("y = :y").bind("y", "{\"z\": 100}").execute().count());

        DocResult res = this.collection.find().execute();
        while (res.hasNext()) {
            DbDoc jd = res.next();
            if (jd.get("y") != null) {
                assertEquals(nestedDoc.toString(), ((DbDoc) jd.get("y")).toString());
            }
            if (jd.get("m") != null) {
                assertEquals(nestedDoc.toString(), ((DbDoc) jd.get("m")).toString());
            }
        }
    }

    @Test
    public void testArrayModify() {
        if (!this.isSetForXTests) {
            return;
        }

        JsonArray xArray = new JsonArray().addValue(new JsonString().setValue("a")).addValue(new JsonNumber().setValue("1"));
        DbDoc doc = new DbDocImpl().add("x", new JsonNumber().setValue("3")).add("y", xArray);

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"x\":1, \"y\":[\"b\", 2]}").execute(); // Requires manual _id.
            this.collection.add("{\"_id\": \"2\", \"x\":2, \"y\":22}").execute();
            this.collection.add(doc.add("_id", new JsonString().setValue("3"))).execute(); // Inject an _id.
        } else {
            this.collection.add("{\"x\":1, \"y\":[\"b\", 2]}").execute();
            this.collection.add("{\"x\":2, \"y\":22}").execute();
            this.collection.add(doc).execute();
        }

        this.collection.modify("true").arrayInsert("$.y[1]", 44).execute();
        this.collection.modify("x = 2").change("$.y", xArray).execute();
        this.collection.modify("x = 3").set("y", xArray).execute();

        DocResult res = this.collection.find().execute();
        while (res.hasNext()) {
            DbDoc jd = res.next();
            if (((JsonNumber) jd.get("x")).getInteger() == 1) {
                assertEquals((new JsonArray().addValue(new JsonString().setValue("b")).addValue(new JsonNumber().setValue("44"))
                        .addValue(new JsonNumber().setValue("2"))).toString(), (jd.get("y")).toString());
            } else {
                assertEquals(xArray.toString(), jd.get("y").toString());
            }
        }

    }

    /**
     * Tests fix for BUG#24471057, UPDATE FAILS WHEN THE NEW VALUE IS OF TYPE DBDOC WHICH HAS ARRAY IN IT.
     * 
     * @throws Exception
     *             if the test fails.
     */
    @Test
    public void testBug24471057() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        String docStr = "{\"B\" : 2, \"ID\" : 1, \"KEY\" : [1]}";
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            docStr = docStr.replace("{", "{\"_id\": \"1\", "); // Inject an _id.
        }
        DbDoc doc1 = JsonParser.parseDoc(new StringReader(docStr));

        AddResult res = this.collection.add(doc1).execute();
        this.collection.modify("ID=1").set("$.B", doc1).execute();

        // expected doc
        DbDoc doc2 = JsonParser.parseDoc(new StringReader(docStr));
        doc2.put("B", doc1);
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            doc2.put("_id", new JsonString().setValue(res.getGeneratedIds().get(0)));
        }
        DocResult docs = this.collection.find().execute();
        DbDoc doc = docs.next();
        assertEquals(doc2.toString(), doc.toString());

        // DbDoc as an array member
        DbDoc doc3 = JsonParser.parseDoc(new StringReader(docStr));
        ((JsonArray) doc1.get("KEY")).add(doc3);
        this.collection.modify("ID=1").set("$.B", doc1).execute();

        // expected doc
        doc2.put("B", doc1);
        docs = this.collection.find().execute();
        doc = docs.next();
        assertEquals(doc2.toString(), doc.toString());

    }

    @Test
    public void testMergePatch() throws Exception {
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
            return;
        }

        // 1. Update the name and zip code of match
        this.collection.add("{\"_id\": \"1\", \"name\": \"Alice\", \"address\": {\"zip\": \"12345\", \"street\": \"32 Main str\"}}").execute();
        this.collection.add("{\"_id\": \"2\", \"name\": \"Bob\", \"address\": {\"zip\": \"325226\", \"city\": \"San Francisco\", \"street\": \"42 2nd str\"}}")
                .execute();

        this.collection.modify("_id = :id").patch(JsonParser.parseDoc("{\"name\": \"Joe\", \"address\": {\"zip\":\"91234\"}}")).bind("id", "1").execute();

        DocResult docs = this.collection.find().orderBy("$._id").execute();
        assertTrue(docs.hasNext());
        assertEquals(JsonParser.parseDoc("{\"_id\": \"1\", \"name\": \"Joe\", \"address\": {\"zip\": \"91234\", \"street\": \"32 Main str\"}}").toString(),
                docs.next().toString());
        assertTrue(docs.hasNext());
        assertEquals(JsonParser
                .parseDoc("{\"_id\": \"2\", \"name\": \"Bob\", \"address\": {\"zip\": \"325226\", \"city\": \"San Francisco\", \"street\": \"42 2nd str\"}}")
                .toString(), docs.next().toString());
        assertFalse(docs.hasNext());

        // Delete the address field of match
        this.collection.modify("_id = :id").patch("{\"address\": null}").bind("id", "1").execute();

        docs = this.collection.find().orderBy("$._id").execute();
        assertTrue(docs.hasNext());
        assertEquals(JsonParser.parseDoc("{\"_id\": \"1\", \"name\": \"Joe\"}").toString(), docs.next().toString());
        assertTrue(docs.hasNext());
        assertEquals(JsonParser
                .parseDoc("{\"_id\": \"2\", \"name\": \"Bob\", \"address\": {\"zip\": \"325226\", \"city\": \"San Francisco\", \"street\": \"42 2nd str\"}}")
                .toString(), docs.next().toString());
        assertFalse(docs.hasNext());

        String id = "a6f4b93e1a264a108393524f29546a8c";
        this.collection.add("{\"_id\" : \"" + id + "\"," //
                + "\"title\" : \"AFRICAN EGG\"," //
                + "\"description\" : \"A Fast-Paced Documentary of a Pastry Chef And a Dentist who must Pursue a Forensic Psychologist in The Gulf of Mexico\"," //
                + "\"releaseyear\" : 2006," //
                + "\"language\" : \"English\"," //
                + "\"duration\" : 130," //
                + "\"rating\" : \"G\"," //
                + "\"genre\" : \"Science fiction\"," //
                + "\"actors\" : [" //
                + "    {\"name\" : \"MILLA PECK\"," //
                + "     \"country\" : \"Mexico\"," //
                + "     \"birthdate\": \"12 Jan 1984\"}," //
                + "    {\"name\" : \"VAL BOLGER\"," //
                + "     \"country\" : \"Botswana\"," //
                + "     \"birthdate\": \"26 Jul 1975\" }," //
                + "    {\"name\" : \"SCARLETT BENING\"," //
                + "     \"country\" : \"Syria\"," //
                + "     \"birthdate\": \"16 Mar 1978\" }" //
                + "    ]," //
                + "\"additionalinfo\" : {" //
                + "    \"director\" : {" //
                + "        \"name\": \"Sharice Legaspi\"," //
                + "        \"age\":57," //
                + "        \"awards\": [" //
                + "            {\"award\": \"Best Movie\"," //
                + "             \"movie\": \"THE EGG\"," //
                + "             \"year\": 2002}," //
                + "            {\"award\": \"Best Special Effects\"," //
                + "             \"movie\": \"AFRICAN EGG\"," //
                + "             \"year\": 2006}" //
                + "            ]" //
                + "        }," //
                + "    \"writers\" : [\"Rusty Couturier\", \"Angelic Orduno\", \"Carin Postell\"]," //
                + "    \"productioncompanies\" : [\"Qvodrill\", \"Indigoholdings\"]" //
                + "    }" //
                + "}").execute();

        // Adding a new field to multiple documents
        this.collection.modify("language = :lang").patch("{\"translations\": [\"Spanish\"]}").bind("lang", "English").execute();
        docs = this.collection.find("_id = :id").bind("id", id).limit(1).execute();
        assertTrue(docs.hasNext());
        DbDoc doc = docs.next();
        assertNotNull(doc.get("translations"));
        JsonArray arr = (JsonArray) doc.get("translations");
        assertEquals(1, arr.size());
        assertEquals("Spanish", ((JsonString) arr.get(0)).getString());

        this.collection.modify("additionalinfo.director.name = :director").patch("{\"additionalinfo\": {\"musicby\": \"Sakila D\" }}")
                .bind("director", "Sharice Legaspi").execute();
        docs = this.collection.find("_id = :id").bind("id", id).limit(1).execute();
        assertTrue(docs.hasNext());
        doc = docs.next();
        assertNotNull(doc.get("additionalinfo"));
        DbDoc doc2 = (DbDoc) doc.get("additionalinfo");
        assertNotNull(doc2.get("musicby"));
        assertEquals("Sakila D", ((JsonString) doc2.get("musicby")).getString());

        this.collection.modify("additionalinfo.director.name = :director").patch("{\"additionalinfo\": {\"director\": {\"country\": \"France\"}}}")
                .bind("director", "Sharice Legaspi").execute();
        docs = this.collection.find("_id = :id").bind("id", id).limit(1).execute();
        assertTrue(docs.hasNext());
        doc = docs.next();
        assertNotNull(doc.get("additionalinfo"));
        assertNotNull(((DbDoc) doc.get("additionalinfo")).get("director"));
        doc2 = (DbDoc) ((DbDoc) doc.get("additionalinfo")).get("director");
        assertNotNull(doc2.get("country"));
        assertEquals("France", ((JsonString) doc2.get("country")).getString());

        // Replacing/Updating a field's value in multiple documents
        this.collection.modify("language = :lang").patch("{\"translations\": [\"Spanish\", \"Italian\"]}").bind("lang", "English").execute();
        docs = this.collection.find("_id = :id").bind("id", id).limit(1).execute();
        assertTrue(docs.hasNext());
        doc = docs.next();
        assertNotNull(doc.get("translations"));
        arr = (JsonArray) doc.get("translations");
        assertEquals(2, arr.size());
        assertEquals("Spanish", ((JsonString) arr.get(0)).getString());
        assertEquals("Italian", ((JsonString) arr.get(1)).getString());

        this.collection.modify("additionalinfo.director.name = :director").patch("{\"additionalinfo\": {\"musicby\": \"The Sakila\" }}")
                .bind("director", "Sharice Legaspi").execute();
        docs = this.collection.find("_id = :id").bind("id", id).limit(1).execute();
        assertTrue(docs.hasNext());
        doc = docs.next();
        assertNotNull(doc.get("additionalinfo"));
        doc2 = (DbDoc) doc.get("additionalinfo");
        assertNotNull(doc2.get("musicby"));
        assertEquals("The Sakila", ((JsonString) doc2.get("musicby")).getString());

        this.collection.modify("additionalinfo.director.name = :director").patch("{\"additionalinfo\": {\"director\": {\"country\": \"Canada\"}}}")
                .bind("director", "Sharice Legaspi").execute();
        docs = this.collection.find("_id = :id").bind("id", id).limit(1).execute();
        assertTrue(docs.hasNext());
        doc = docs.next();
        assertNotNull(doc.get("additionalinfo"));
        assertNotNull(((DbDoc) doc.get("additionalinfo")).get("director"));
        doc2 = (DbDoc) ((DbDoc) doc.get("additionalinfo")).get("director");
        assertNotNull(doc2.get("country"));
        assertEquals("Canada", ((JsonString) doc2.get("country")).getString());

        // Removing a field from multiple documents:
        this.collection.modify("language = :lang").patch("{\"translations\": null}").bind("lang", "English").execute();
        docs = this.collection.find("_id = :id").bind("id", id).limit(1).execute();
        assertTrue(docs.hasNext());
        doc = docs.next();
        assertNull(doc.get("translations"));

        this.collection.modify("additionalinfo.director.name = :director").patch("{\"additionalinfo\": {\"musicby\": null }}")
                .bind("director", "Sharice Legaspi").execute();
        docs = this.collection.find("_id = :id").bind("id", id).limit(1).execute();
        assertTrue(docs.hasNext());
        doc = docs.next();
        assertNotNull(doc.get("additionalinfo"));
        doc2 = (DbDoc) doc.get("additionalinfo");
        assertNull(doc2.get("musicby"));

        this.collection.modify("additionalinfo.director.name = :director").patch("{\"additionalinfo\": {\"director\": {\"country\": null}}}")
                .bind("director", "Sharice Legaspi").execute();
        docs = this.collection.find("_id = :id").bind("id", id).limit(1).execute();
        assertTrue(docs.hasNext());
        doc = docs.next();
        assertNotNull(doc.get("additionalinfo"));
        assertNotNull(((DbDoc) doc.get("additionalinfo")).get("director"));
        doc2 = (DbDoc) ((DbDoc) doc.get("additionalinfo")).get("director");
        assertNull(doc2.get("country"));

        // Using expressions

        this.collection.modify("_id = :id").patch("{\"zip\": address.zip-300000, \"street\": CONCAT($.name, '''s street: ', $.address.street)}").bind("id", "2")
                .execute();

        this.collection.modify("_id = :id").patch("{\"city\": UPPER($.address.city)}").bind("id", "2").execute();

        docs = this.collection.find("_id = :id").bind("id", "2").limit(1).execute();
        assertTrue(docs.hasNext());
        doc = docs.next();
        assertEquals(25226, ((JsonNumber) doc.get("zip")).getBigDecimal().intValue());
        assertEquals("Bob's street: 42 2nd str", ((JsonString) doc.get("street")).getString());
        assertEquals("SAN FRANCISCO", ((JsonString) doc.get("city")).getString());
        doc2 = (DbDoc) doc.get("address");
        assertNotNull(doc2);
        assertEquals("325226", ((JsonString) doc2.get("zip")).getString());
        assertEquals("42 2nd str", ((JsonString) doc2.get("street")).getString());
        assertEquals("San Francisco", ((JsonString) doc2.get("city")).getString());
    }

    /**
     * Tests fix for BUG#27185332, WL#11210:ERROR IS THROWN WHEN NESTED EMPTY DOCUMENTS ARE INSERTED TO COLLECTION.
     * 
     * @throws Exception
     *             if the test fails.
     */
    @Test
    public void testBug27185332() throws Exception {
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
            return;
        }

        DbDoc doc = JsonParser.parseDoc("{\"_id\": \"qqq\", \"nullfield\": {}, \"theme\": {         }}");
        assertEquals("qqq", ((JsonString) doc.get("_id")).getString());
        assertEquals(new DbDocImpl(), doc.get("nullfield"));
        assertEquals(new DbDocImpl(), doc.get("theme"));

        String id = "qqq";
        this.collection.add("{\"_id\" : \"" + id + "\"," //
                + "\"title\" : \"THE TITLE\"," //
                + "\"description\" : \"Author's story\"," //
                + "\"releaseyear\" : 2012," //
                + "\"language\" : \"English\"," //
                + "\"theme\" : \"fiction\"," //
                + "\"roles\" : ["//
                + "    {\"name\" : \"Role 1\"," //
                + "     \"country\" : [\"Thailand\", \"Singapore\"]," //
                + "     \"birthdate\": \"12 Jan 1984\"}," //
                + "    {\"name\" : \"Role 2\"," //
                + "     \"country\" : \"Bali\"," //
                + "     \"birthdate\": \"26 Jul 1975\" }," //
                + "    {\"name\" : \"Role 3\"," //
                + "     \"country\" : \"Doha\"," //
                + "     \"birthdate\": \"16 Mar 1978\" }" //
                + "    ]," //
                + "\"additionalinfo\" : {" //
                + "    \"author\" : {" //
                + "        \"name\": \"John Gray\"," //
                + "        \"age\":57," //
                + "        \"awards\": [" //
                + "            {\"award\": \"Best Writer\"," //
                + "             \"book\": \"MAFM WAFV\"," //
                + "             \"year\": 2002}," //
                + "            {\"award\": \"Best Imagination\"," //
                + "             \"book\": \"THE TITLE\"," //
                + "             \"year\": 2006}" //
                + "            ]" //
                + "        }," //
                + "    \"otherwriters\" : [\"Rusty Couturier\", \"Angelic Orduno\", \"Carin Postell\"]," //
                + "    \"production\" : [\"Qvodrill\", \"Indigoholdings\"]" //
                + "    }" //
                + "}").execute();
        this.collection.modify("true").patch("{\"nullfield\": { \"nested\": null}}").execute();
        DocResult docs = this.collection.find("_id = :id").bind("id", id).execute();
        assertTrue(docs.hasNext());
        doc = docs.next(); //   <---- Error at this line
        assertNotNull(doc.get("nullfield"));
        assertEquals(new DbDocImpl(), doc.get("nullfield"));
    }

    @Test
    public void testReplaceOne() {
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
            return;
        }

        Result res = this.collection.replaceOne("someId", "{\"_id\":\"someId\",\"a\":3}");
        assertEquals(0, res.getAffectedItemsCount());

        this.collection.add("{\"_id\":\"existingId\",\"a\":1}").execute();

        res = this.collection.replaceOne("existingId", new DbDocImpl().add("a", new JsonNumber().setValue("2")));
        assertEquals(1, res.getAffectedItemsCount());

        DbDoc doc = this.collection.getOne("existingId");
        assertNotNull(doc);
        assertEquals(new Integer(2), ((JsonNumber) doc.get("a")).getInteger());

        res = this.collection.replaceOne("notExistingId", "{\"_id\":\"existingId\",\"a\":3}");
        assertEquals(0, res.getAffectedItemsCount());

        res = this.collection.replaceOne("", "{\"_id\":\"existingId\",\"a\":3}");
        assertEquals(0, res.getAffectedItemsCount());

        /*
         * FR5.1 The id of the document must remain immutable:
         * 
         * Use a collection with some documents
         * Fetch a document
         * Modify _id: _new_id_ and modify any other field of the document
         * Call replaceOne() giving original ID and modified document: expect affected = 1
         * Fetch the document again, ensure other document modifications took place
         * Ensure no document with _new_id_ was added to the collection
         */
        this.collection.remove("1=1").execute();
        assertEquals(0, this.collection.count());
        this.collection.add("{\"_id\":\"id1\",\"a\":1}").execute();

        doc = this.collection.getOne("id1");
        assertNotNull(doc);
        ((JsonString) doc.get("_id")).setValue("id2");
        ((JsonNumber) doc.get("a")).setValue("2");
        res = this.collection.replaceOne("id1", doc);
        assertEquals(1, res.getAffectedItemsCount());

        doc = this.collection.getOne("id1");
        assertNotNull(doc);
        assertEquals("id1", ((JsonString) doc.get("_id")).getString());
        assertEquals(new Integer(2), ((JsonNumber) doc.get("a")).getInteger());

        doc = this.collection.getOne("id2");
        assertNull(doc);

        /*
         * FR5.2 The id of the document must remain immutable:
         * 
         * Use a collection with some documents
         * Fetch a document
         * Unset _id and modify any other field of the document
         * Call replaceOne() giving original ID and modified document: expect affected = 1
         * Fetch the document again, ensure other document modifications took place
         * Ensure the number of documents in the collection is unaltered
         */
        doc = this.collection.getOne("id1");
        assertNotNull(doc);
        doc.remove("_id");
        ((JsonNumber) doc.get("a")).setValue("3");
        res = this.collection.replaceOne("id1", doc);
        assertEquals(1, res.getAffectedItemsCount());

        doc = this.collection.getOne("id1");
        assertNotNull(doc);
        assertEquals("id1", ((JsonString) doc.get("_id")).getString());
        assertEquals(new Integer(3), ((JsonNumber) doc.get("a")).getInteger());
        assertEquals(1, this.collection.count());

        // null document
        assertThrows(XDevAPIError.class, "Parameter 'doc' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionModifyTest.this.collection.replaceOne("id1", (DbDoc) null);
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'jsonString' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionModifyTest.this.collection.replaceOne("id2", (String) null);
                return null;
            }
        });

        // null id parameter
        assertThrows(XDevAPIError.class, "Parameter 'id' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionModifyTest.this.collection.replaceOne(null, new DbDocImpl().add("a", new JsonNumber().setValue("2")));
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Parameter 'id' must not be null.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionModifyTest.this.collection.replaceOne(null, "{\"_id\": \"id100\", \"a\": 100}");
                return null;
            }
        });

        assertNull(this.collection.getOne(null));

    }

    /**
     * Tests fix for BUG#27226293, JSONNUMBER.GETINTEGER() & NUMBERFORMATEXCEPTION.
     */
    @Test
    public void testBug27226293() {
        if (!this.isSetForXTests) {
            return;
        }

        this.collection.add("{ \"_id\" : \"doc1\" , \"name\" : \"bob\" , \"age\": 45 }").execute();

        DocResult result = this.collection.find("name = 'bob'").execute();
        DbDoc doc = result.fetchOne();
        assertEquals(new Integer(45), ((JsonNumber) doc.get("age")).getInteger());

        // After fixing server Bug#88230 (MySQL 8.0.4) the next operation returns
        // decimal age value 46.0 instead of integer 46
        this.collection.modify("name='bob'").set("age", expr("$.age + 1")).execute();
        doc = this.collection.find("name='bob'").execute().fetchOne();
        int age = ((JsonNumber) doc.get("age")).getInteger();
        assertEquals(46, age);
    }
}
