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

import static com.mysql.cj.xdevapi.Expression.expr;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.StringReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.xdevapi.AddResult;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DbDocImpl;
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.JsonArray;
import com.mysql.cj.xdevapi.JsonLiteral;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonParser;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.JsonValue;
import com.mysql.cj.xdevapi.ModifyStatement;
import com.mysql.cj.xdevapi.ModifyStatementImpl;
import com.mysql.cj.xdevapi.Result;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.XDevAPIError;

/**
 * @todo
 */
public class CollectionModifyTest extends BaseCollectionTestCase {

    @Test
    public void testSet() {
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

        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", () -> {
            CollectionModifyTest.this.collection.modify(null).set("y", nestedDoc).execute();
            return null;
        });

        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", () -> {
            CollectionModifyTest.this.collection.modify(" ").set("y", nestedDoc).execute();
            return null;
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
                assertEquals(new JsonArray().addValue(new JsonString().setValue("b")).addValue(new JsonNumber().setValue("44"))
                        .addValue(new JsonNumber().setValue("2")).toString(), jd.get("y").toString());
            } else {
                assertEquals(xArray.toString(), jd.get("y").toString());
            }
        }
    }

    /**
     * Tests fix for BUG#24471057, UPDATE FAILS WHEN THE NEW VALUE IS OF TYPE DBDOC WHICH HAS ARRAY IN IT.
     *
     * @throws Exception
     */
    @Test
    public void testBug24471057() throws Exception {
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
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3")), "MySQL 8.0.3+ is required to run this test.");

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
     */
    @Test
    public void testBug27185332() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3")), "MySQL 8.0.3+ is required to run this test.");

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
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3")), "MySQL 8.0.3+ is required to run this test.");

        Result res = this.collection.replaceOne("someId", "{\"_id\":\"someId\",\"a\":3}");
        assertEquals(0, res.getAffectedItemsCount());

        this.collection.add("{\"_id\":\"existingId\",\"a\":1}").execute();

        res = this.collection.replaceOne("existingId", new DbDocImpl().add("a", new JsonNumber().setValue("2")));
        assertEquals(1, res.getAffectedItemsCount());

        DbDoc doc = this.collection.getOne("existingId");
        assertNotNull(doc);
        assertEquals(2, ((JsonNumber) doc.get("a")).getInteger());

        // Original behavior changed by Bug#32770013.
        assertThrows(XDevAPIError.class, "Replacement document has an _id that is different than the matched document\\.", () -> {
            CollectionModifyTest.this.collection.replaceOne("nonExistingId", "{\"_id\":\"existingId\",\"a\":3}");
            return null;
        });

        // Original behavior changed by Bug#32770013.
        assertThrows(XDevAPIError.class, "Replacement document has an _id that is different than the matched document\\.", () -> {
            CollectionModifyTest.this.collection.replaceOne("", "{\"_id\":\"existingId\",\"a\":3}");
            return null;
        });

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
        this.collection.remove("true").execute();
        assertEquals(0, this.collection.count());
        this.collection.add("{\"_id\":\"id1\",\"a\":1}").execute();

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
        assertThrows(XDevAPIError.class, "Parameter 'doc' must not be null.", () -> {
            CollectionModifyTest.this.collection.replaceOne("id1", (DbDoc) null);
            return null;
        });
        assertThrows(XDevAPIError.class, "Parameter 'jsonString' must not be null.", () -> {
            CollectionModifyTest.this.collection.replaceOne("id2", (String) null);
            return null;
        });

        // null id parameter
        assertThrows(XDevAPIError.class, "Parameter 'id' must not be null.", () -> {
            CollectionModifyTest.this.collection.replaceOne(null, new DbDocImpl().add("a", new JsonNumber().setValue("2")));
            return null;
        });
        assertThrows(XDevAPIError.class, "Parameter 'id' must not be null.", () -> {
            CollectionModifyTest.this.collection.replaceOne(null, "{\"_id\": \"id100\", \"a\": 100}");
            return null;
        });

        assertNull(this.collection.getOne(null));
    }

    /**
     * Tests fix for BUG#27226293, JSONNUMBER.GETINTEGER() & NUMBERFORMATEXCEPTION.
     */
    @Test
    public void testBug27226293() {
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

    @Test
    public void testPreparedStatements() {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14")), "MySQL 8.0.14+ is required to run this test.");

        try {
            // Prepare test data.
            testPreparedStatementsResetData();

            SessionFactory sf = new SessionFactory();

            /*
             * Test common usage.
             */
            Session testSession = sf.getSession(this.testProperties);

            int sessionThreadId = getThreadId(testSession);
            assertPreparedStatementsCount(sessionThreadId, 0, 1);
            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

            Collection testCol1 = testSession.getDefaultSchema().getCollection(this.collectionName + "_1");
            Collection testCol2 = testSession.getDefaultSchema().getCollection(this.collectionName + "_2");
            Collection testCol3 = testSession.getDefaultSchema().getCollection(this.collectionName + "_3");
            Collection testCol4 = testSession.getDefaultSchema().getCollection(this.collectionName + "_4");

            // Initialize several ModifyStatement objects.
            ModifyStatement testModify1 = testCol1.modify("true").set("ord", expr("$.ord * 10")); // Modify all.
            ModifyStatement testModify2 = testCol2.modify("$.ord >= :n").set("ord", expr("$.ord * 10")); // Criteria with one placeholder.
            ModifyStatement testModify3 = testCol3.modify("$.ord >= :n AND $.ord <= :n + 1").set("ord", expr("$.ord * 10")); // Criteria with same placeholder repeated.
            ModifyStatement testModify4 = testCol4.modify("$.ord >= :n AND $.ord <= :m").set("ord", expr("$.ord * 10")); // Criteria with multiple placeholders.

            assertPreparedStatementsCountsAndId(testSession, 0, testModify1, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testModify2, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testModify3, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testModify4, 0, -1);

            // A. Set binds: 1st execute -> non-prepared.
            assertTestPreparedStatementsResult(testModify1.execute(), 4, testCol1.getName(), 10, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 0, testModify1, 0, -1);
            assertTestPreparedStatementsResult(testModify2.bind("n", 2).execute(), 3, testCol2.getName(), 1, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 0, testModify2, 0, -1);
            assertTestPreparedStatementsResult(testModify3.bind("n", 2).execute(), 2, testCol3.getName(), 1, 20, 30, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testModify3, 0, -1);
            assertTestPreparedStatementsResult(testModify4.bind("n", 2).bind("m", 3).execute(), 2, testCol4.getName(), 1, 20, 30, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testModify4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);
            testPreparedStatementsResetData();

            // B. Set sort resets execution count: 1st execute -> non-prepared.
            assertTestPreparedStatementsResult(testModify1.sort("$._id").execute(), 4, testCol1.getName(), 10, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 0, testModify1, 0, -1);
            assertTestPreparedStatementsResult(testModify2.sort("$._id").execute(), 3, testCol2.getName(), 1, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 0, testModify2, 0, -1);
            assertTestPreparedStatementsResult(testModify3.sort("$._id").execute(), 2, testCol3.getName(), 1, 20, 30, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testModify3, 0, -1);
            assertTestPreparedStatementsResult(testModify4.sort("$._id").execute(), 2, testCol4.getName(), 1, 20, 30, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testModify4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);
            testPreparedStatementsResetData();

            // C. Set binds reuse statement: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testModify1.execute(), 4, testCol1.getName(), 10, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 1, testModify1, 1, 1);
            assertTestPreparedStatementsResult(testModify2.bind("n", 3).execute(), 2, testCol2.getName(), 1, 2, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 2, testModify2, 2, 1);
            assertTestPreparedStatementsResult(testModify3.bind("n", 3).execute(), 2, testCol3.getName(), 1, 2, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 3, testModify3, 3, 1);
            assertTestPreparedStatementsResult(testModify4.bind("m", 4).execute(), 3, testCol4.getName(), 1, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 4, 4, 0);
            testPreparedStatementsResetData();

            // D. Set binds reuse statement: 3rd execute -> execute.
            assertTestPreparedStatementsResult(testModify1.execute(), 4, testCol1.getName(), 10, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify1, 1, 2);
            assertTestPreparedStatementsResult(testModify2.bind("n", 4).execute(), 1, testCol2.getName(), 1, 2, 3, 40);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify2, 2, 2);
            assertTestPreparedStatementsResult(testModify3.bind("n", 1).execute(), 2, testCol3.getName(), 10, 20, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify3, 3, 2);
            assertTestPreparedStatementsResult(testModify4.bind("m", 2).execute(), 1, testCol4.getName(), 1, 20, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify4, 4, 2);

            assertPreparedStatementsStatusCounts(testSession, 4, 8, 0);
            testPreparedStatementsResetData();

            // E. Set new values deallocates and resets execution count: 1st execute -> deallocate + non-prepared.
            assertTestPreparedStatementsResult(testModify1.set("ord", expr("$.ord * 100")).execute(), 4, testCol1.getName(), 100, 200, 300, 400);
            assertPreparedStatementsCountsAndId(testSession, 3, testModify1, 0, -1);
            assertTestPreparedStatementsResult(testModify2.set("ord", expr("$.ord * 100")).execute(), 1, testCol2.getName(), 1, 2, 3, 400);
            assertPreparedStatementsCountsAndId(testSession, 2, testModify2, 0, -1);
            assertTestPreparedStatementsResult(testModify3.set("ord", expr("$.ord * 100")).execute(), 2, testCol3.getName(), 100, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 1, testModify3, 0, -1);
            assertTestPreparedStatementsResult(testModify4.set("ord", expr("$.ord * 100")).execute(), 1, testCol4.getName(), 1, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testModify4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 4, 8, 4);
            testPreparedStatementsResetData();

            // F. No Changes: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testModify1.execute(), 4, testCol1.getName(), 100, 200, 300, 400);
            assertPreparedStatementsCountsAndId(testSession, 1, testModify1, 1, 1);
            assertTestPreparedStatementsResult(testModify2.execute(), 1, testCol2.getName(), 1, 2, 3, 400);
            assertPreparedStatementsCountsAndId(testSession, 2, testModify2, 2, 1);
            assertTestPreparedStatementsResult(testModify3.execute(), 2, testCol3.getName(), 100, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 3, testModify3, 3, 1);
            assertTestPreparedStatementsResult(testModify4.execute(), 1, testCol4.getName(), 1, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 8, 12, 4);
            testPreparedStatementsResetData();

            // G. Set limit for the first time deallocates and re-prepares: 1st execute -> re-prepare + execute.
            assertTestPreparedStatementsResult(testModify1.limit(1).execute(), 1, testCol1.getName(), 100, 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify1, 1, 1);
            assertTestPreparedStatementsResult(testModify2.limit(1).execute(), 1, testCol2.getName(), 1, 2, 3, 400);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify2, 2, 1);
            assertTestPreparedStatementsResult(testModify3.limit(1).execute(), 1, testCol3.getName(), 100, 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify3, 3, 1);
            assertTestPreparedStatementsResult(testModify4.limit(1).execute(), 1, testCol4.getName(), 1, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 12, 16, 8);
            testPreparedStatementsResetData();

            // H. Set limit reuse prepared statement: 2nd execute -> execute.
            assertTestPreparedStatementsResult(testModify1.limit(2).execute(), 2, testCol1.getName(), 100, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify1, 1, 2);
            assertTestPreparedStatementsResult(testModify2.limit(2).execute(), 1, testCol2.getName(), 1, 2, 3, 400);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify2, 2, 2);
            assertTestPreparedStatementsResult(testModify3.limit(2).execute(), 2, testCol3.getName(), 100, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify3, 3, 2);
            assertTestPreparedStatementsResult(testModify4.limit(2).execute(), 1, testCol4.getName(), 1, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify4, 4, 2);

            assertPreparedStatementsStatusCounts(testSession, 12, 20, 8);
            testPreparedStatementsResetData();

            // I. Set sort deallocates and resets execution count, set limit has no effect: 1st execute -> deallocate + non-prepared.
            assertTestPreparedStatementsResult(testModify1.sort("$._id").limit(1).execute(), 1, testCol1.getName(), 100, 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 3, testModify1, 0, -1);
            assertTestPreparedStatementsResult(testModify2.sort("$._id").limit(1).execute(), 1, testCol2.getName(), 1, 2, 3, 400);
            assertPreparedStatementsCountsAndId(testSession, 2, testModify2, 0, -1);
            assertTestPreparedStatementsResult(testModify3.sort("$._id").limit(1).execute(), 1, testCol3.getName(), 100, 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 1, testModify3, 0, -1);
            assertTestPreparedStatementsResult(testModify4.sort("$._id").limit(1).execute(), 1, testCol4.getName(), 1, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testModify4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 12, 20, 12);
            testPreparedStatementsResetData();

            // J. Set limit reuse statement: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testModify1.limit(2).execute(), 2, testCol1.getName(), 100, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 1, testModify1, 1, 1);
            assertTestPreparedStatementsResult(testModify2.limit(2).execute(), 1, testCol2.getName(), 1, 2, 3, 400);
            assertPreparedStatementsCountsAndId(testSession, 2, testModify2, 2, 1);
            assertTestPreparedStatementsResult(testModify3.limit(2).execute(), 2, testCol3.getName(), 100, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 3, testModify3, 3, 1);
            assertTestPreparedStatementsResult(testModify4.limit(2).execute(), 1, testCol4.getName(), 1, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testModify4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 16, 24, 12);
            testPreparedStatementsResetData();

            testSession.close();
            assertPreparedStatementsCount(sessionThreadId, 0, 10); // Prepared statements won't live past the closing of the session.

            /*
             * Test falling back onto non-prepared statements.
             */
            testSession = sf.getSession(this.testProperties);
            int origMaxPrepStmtCount = this.session.sql("SELECT @@max_prepared_stmt_count").execute().fetchOne().getInt(0);

            try {
                // Allow preparing only one more statement.
                this.session.sql("SET GLOBAL max_prepared_stmt_count = ?").bind(getPreparedStatementsCount() + 1).execute();

                sessionThreadId = getThreadId(testSession);
                assertPreparedStatementsCount(sessionThreadId, 0, 1);
                assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

                testCol1 = testSession.getDefaultSchema().getCollection(this.collectionName + "_1");
                testCol2 = testSession.getDefaultSchema().getCollection(this.collectionName + "_2");

                testModify1 = testCol1.modify("true").set("ord", expr("$.ord * 10"));
                testModify2 = testCol2.modify("true").set("ord", expr("$.ord * 10"));

                // 1st execute -> don't prepare.
                assertTestPreparedStatementsResult(testModify1.execute(), 4, testCol1.getName(), 10, 20, 30, 40);
                assertPreparedStatementsCountsAndId(testSession, 0, testModify1, 0, -1);
                assertTestPreparedStatementsResult(testModify2.execute(), 4, testCol2.getName(), 10, 20, 30, 40);
                assertPreparedStatementsCountsAndId(testSession, 0, testModify2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);
                testPreparedStatementsResetData();

                // 2nd execute -> prepare + execute.
                assertTestPreparedStatementsResult(testModify1.execute(), 4, testCol1.getName(), 10, 20, 30, 40);
                assertPreparedStatementsCountsAndId(testSession, 1, testModify1, 1, 1);
                assertTestPreparedStatementsResult(testModify2.execute(), 4, testCol2.getName(), 10, 20, 30, 40); // Fails preparing, execute as non-prepared.
                assertPreparedStatementsCountsAndId(testSession, 1, testModify2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 2, 1, 0); // Failed prepare also counts.
                testPreparedStatementsResetData();

                // 3rd execute -> execute.
                assertTestPreparedStatementsResult(testModify1.execute(), 4, testCol1.getName(), 10, 20, 30, 40);
                assertPreparedStatementsCountsAndId(testSession, 1, testModify1, 1, 2);
                assertTestPreparedStatementsResult(testModify2.execute(), 4, testCol2.getName(), 10, 20, 30, 40); // Execute as non-prepared.
                assertPreparedStatementsCountsAndId(testSession, 1, testModify2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 2, 2, 0);
                testPreparedStatementsResetData();

                testSession.close();
                assertPreparedStatementsCount(sessionThreadId, 0, 10); // Prepared statements won't live past the closing of the session.
            } finally {
                this.session.sql("SET GLOBAL max_prepared_stmt_count = ?").bind(origMaxPrepStmtCount).execute();
            }
        } finally {
            for (int i = 0; i < 4; i++) {
                dropCollection(this.collectionName + "_" + (i + 1));
            }
        }
    }

    private void testPreparedStatementsResetData() {
        for (int i = 0; i < 4; i++) {
            Collection col = this.session.getDefaultSchema().createCollection(this.collectionName + "_" + (i + 1), true);
            col.remove("true").execute();
            col.add("{\"_id\":\"1\", \"ord\": 1}", "{\"_id\":\"2\", \"ord\": 2}", "{\"_id\":\"3\", \"ord\": 3}", "{\"_id\":\"4\", \"ord\": 4}").execute();
        }
    }

    @SuppressWarnings("hiding")
    private void assertTestPreparedStatementsResult(Result res, int expectedAffectedItemsCount, String collectionName, int... expectedValues) {
        assertEquals(expectedAffectedItemsCount, res.getAffectedItemsCount());
        DocResult docRes = this.schema.getCollection(collectionName).find().execute();
        assertEquals(expectedValues.length, docRes.count());
        for (int v : expectedValues) {
            assertEquals(v, ((JsonNumber) docRes.next().get("ord")).getInteger().intValue());
        }
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testDeprecateWhere() throws Exception {
        this.collection.add("{\"_id\":\"1\", \"ord\": 1}", "{\"_id\":\"2\", \"ord\": 2}", "{\"_id\":\"3\", \"ord\": 3}", "{\"_id\":\"4\", \"ord\": 4}",
                "{\"_id\":\"5\", \"ord\": 5}", "{\"_id\":\"6\", \"ord\": 6}", "{\"_id\":\"7\", \"ord\": 7}", "{\"_id\":\"8\", \"ord\": 8}").execute();

        ModifyStatement testModify = this.collection.modify("$.ord <= 2");

        assertTrue(testModify.getClass().getMethod("where", String.class).isAnnotationPresent(Deprecated.class));

        assertEquals(2, testModify.set("$.one", "1").execute().getAffectedItemsCount());
        assertEquals(4, ((ModifyStatementImpl) testModify).where("$.ord > 4").set("$.two", "2").execute().getAffectedItemsCount());
    }

    @Test
    public void testCollectionModifyBasic() throws Exception {
        int i = 0, maxrec = 30, recCnt = 0;
        DbDoc doc = null;
        Result res = null;
        String s1 = buildString(10, 'X');
        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(10 * (i + 1))));
            if (i % 3 == 2) {
                newDoc2.add("F3", JsonLiteral.TRUE);
            } else if (i % 3 == 1) {
                newDoc2.add("F3", JsonLiteral.NULL);
            } else {
                newDoc2.add("F3", JsonLiteral.FALSE);
            }
            newDoc2.add("tmp1", new JsonString().setValue("tempdata-" + i));
            newDoc2.add("tmp2", new JsonString().setValue("tempForChange-" + i));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals(maxrec, this.collection.count());

        /* fetch all */
        DocResult docs = this.collection.find("CAST($.F2 as SIGNED)> 0").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(String.valueOf(i + 1000), ((JsonString) doc.get("_id")).getString());
            assertEquals((long) (10 * (i + 1)), (long) ((JsonNumber) doc.get("f2")).getInteger());
            i++;
        }
        assertEquals(maxrec, i);

        /* Modify using empty Condition */
        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.",
                () -> CollectionModifyTest.this.collection.modify("").set("$.F1", "Data_New").execute());

        /* Modify using null Condition */
        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.",
                () -> CollectionModifyTest.this.collection.modify(null).set("$.F1", "Data_New").execute());

        /* Modify with true Condition using Set */
        res = this.collection.modify("true").set("$.F1", "Data_True").execute();
        assertEquals(maxrec, res.getAffectedItemsCount());
        docs = this.collection.find("$.F1 Like '%True'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        recCnt = count_data(docs);
        assertEquals(maxrec, recCnt);

        res = this.collection.modify("1 == 1").set("$.F1", "Data_New").execute();
        assertEquals(maxrec, res.getAffectedItemsCount());
        docs = this.collection.find("$.F1 Like '%New'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        recCnt = count_data(docs);
        assertEquals(maxrec, recCnt);

        /* Modify with a false Condition */
        res = this.collection.modify("false").set("$.F1", "False_Data").execute();
        assertEquals(0, res.getAffectedItemsCount());
        // Modify with a Condition which results to false
        res = this.collection.modify("0 == 1").set("$.F1", "False_Data").execute();
        assertEquals(0, res.getAffectedItemsCount());

        /* Un Set */
        // Test UnSet with condition
        docs = this.collection.find("$.tmp1 Like 'tempdata%'").fields("$._id as _id, $.tmp1 as tp").execute();
        recCnt = count_data(docs);
        // Total Rec with $.tmp1 Like 'tempdata%'
        assertEquals(maxrec, recCnt);
        res = this.collection.modify("CAST($._id as SIGNED) % 2").unset("$.tmp1").execute();
        assertEquals(maxrec / 2, res.getAffectedItemsCount());
        docs = this.collection.find("$.tmp1 Like 'tempdata%'").fields("$._id as _id, $.tmp1 as tp").execute();
        recCnt = count_data(docs);
        // Total records after Unset(with condition)
        assertEquals(maxrec / 2, recCnt);
        // Test true condition with unset
        res = this.collection.modify("1 == 1").unset("$.tmp1").execute();
        assertEquals(maxrec / 2, res.getAffectedItemsCount());
        docs = this.collection.find("$.tmp1 Like 'tempdata%'").fields("$._id as _id, $.tmp1 as tp").execute();
        recCnt = count_data(docs);
        // Total records after Unset(without condition)
        assertEquals(0, recCnt);

        /* Test for Change().unset tmp2 for half of the total records.Call change without condition. */
        res = this.collection.modify("CAST($._id as SIGNED) % 2").unset("$.tmp2").execute();
        assertEquals(maxrec / 2, res.getAffectedItemsCount());
        docs = this.collection.find("$.tmp2 Like 'tempForChange%'").fields("$._id as _id, $.tmp2 as tp").execute();
        recCnt = count_data(docs);
        // Total records after Unset(with condition)
        assertEquals(maxrec / 2, recCnt);

        // Test for Change()
        res = this.collection.modify("true").change("$.tmp2", "Changedata").execute();
        assertEquals(maxrec / 2, res.getAffectedItemsCount());
        docs = this.collection.find("$.tmp2 Like 'Changedata'").fields("$._id as _id, $.tmp2 as tp").execute();
        recCnt = count_data(docs);
        // Total records Changed after modify().change(without condition)
        assertEquals(maxrec / 2, recCnt);

        // Test for set () after unset
        res = this.collection.modify("true").set("$.tmp2", "Changedata1").execute();
        assertEquals(maxrec, res.getAffectedItemsCount());
        docs = this.collection.find("$.tmp2 Like 'Changedata1'").fields("$._id as _id, $.tmp2 as tp").execute();
        recCnt = count_data(docs);
        // Total Records Set when Half of the records were unset
        assertEquals(maxrec, recCnt);

        // Test for set () after unset All
        res = this.collection.modify("true").unset("$.tmp2").execute();
        assertEquals(maxrec, res.getAffectedItemsCount());
        docs = this.collection.find("$.tmp2 IS Not NULL").fields("$._id as _id, $.tmp2 as tp").execute();
        recCnt = count_data(docs);
        // Total Records After unsetting all
        assertEquals(0, recCnt);

        res = this.collection.modify("1 == 1").set("$.tmp2", "Changedata3").execute();
        assertEquals(maxrec, res.getAffectedItemsCount());
        docs = this.collection.find("$.tmp2 Like 'Changedata3'").fields("$._id as _id, $.tmp2 as tp").execute();
        recCnt = count_data(docs);
        // Total Records Set when All the records were unset
        assertEquals(maxrec, recCnt);

        // Modify with Condition using Set
        res = this.collection.modify("$._id = '1001' and  $.F1 Like '%New' ").set("$.F1", s1).execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("$.F1 = '" + s1 + "'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        recCnt = count_data(docs);
        assertEquals(1, recCnt);
    }

    @Test
    public void testCollectionModifySortLimit() throws Exception {
        int i = 0, maxrec = 30, recCnt = 0;
        DbDoc doc = null;
        Result res = null;
        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1001)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(10 * (i + 1))));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(1 + i)));
            newDoc2.add("tmp1", new JsonString().setValue("tempdata-" + i));
            newDoc2.add("tmp2", new JsonNumber().setValue(String.valueOf(-1)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals(maxrec, this.collection.count());

        /* fetch all */
        DocResult docs = this.collection.find("CAST($.F3 as SIGNED)>= 0").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(String.valueOf(i + 1001), ((JsonString) doc.get("_id")).getString());
            assertEquals((long) (10 * (i + 1)), (long) ((JsonNumber) doc.get("f2")).getInteger());
            i++;
        }
        assertEquals(maxrec, i);

        /* With Sort and limit */
        res = this.collection.modify("$.F3 < 10 and  $.F1 Like 'Field-1%' and CAST($.F3 as SIGNED) > 2").set("$.tmp1", "UpdData").sort("$.F1 asc").limit(5)
                .execute();
        assertEquals(5, res.getAffectedItemsCount());
        docs = this.collection.find("$.tmp1 = 'UpdData'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        i = 2;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(String.valueOf(i + 1001), ((JsonString) doc.get("_id")).getString());
            assertEquals((long) (i + 1), (long) ((JsonNumber) doc.get("f3")).getInteger());
            i++;
        }

        // Unset with Condition using Set With Sort and limit
        res = this.collection.modify("$.F3 < 10 and  $.F1 Like 'Field-1%' and CAST($.F3 as SIGNED) > 2").unset("$.tmp1").sort("$.F1 asc").limit(5).execute();
        assertEquals(5, res.getAffectedItemsCount());
        // Unset keys which is already unset
        res = this.collection.modify("$.F3 < 11 and  $.F1 Like 'Field-1%' and CAST($.F3 as SIGNED) > 2").unset("$.tmp1").sort("$.F1 asc").limit(6).execute();
        assertEquals(1, res.getAffectedItemsCount());
        // set keys which is already unset
        res = this.collection.modify("$.F3 < 11 and  $.F1 Like 'Field-1%' and CAST($.F3 as SIGNED) > 2").set("$.tmp1", "UpdData").sort("$.F1 asc").limit(6)
                .execute();
        assertEquals(6, res.getAffectedItemsCount());

        // set keys which is already unset
        res = this.collection.modify("$.F3 < 11 and  $.F1 Like 'Field-1%' and CAST($.F3 as SIGNED) > 2").set("$.F2", -2147483648).sort("$.F1 asc").limit(6)
                .execute();
        assertEquals(6, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F2 as SIGNED) = -2147483648").fields("$._id as _id, $.F2 as f2").execute();
        recCnt = count_data(docs);
        assertEquals(6, recCnt);
        res = this.collection.modify("CAST($.F3 as SIGNED) < 11 and  $.F1 Like 'Field-1%' and CAST($.F3 as SIGNED) > 2").set("$.tmp2", 2147483647)
                .sort("$.F1 asc").limit(6).execute();
        assertEquals(6, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F2 as SIGNED) = (CAST($.tmp2 as SIGNED)*-1)-1").fields("$._id as _id, $.F2 as f2").execute();
        recCnt = count_data(docs);
        assertEquals(6, recCnt);
        docs = this.collection.find("(CAST($.F2 as SIGNED) * -1) - 1 = CAST($.tmp2 as SIGNED)").fields("$._id as _id, $.F2 as f2").execute();
        recCnt = count_data(docs);
        assertEquals(6, recCnt);

        res = this.collection.modify("CAST($.F3 as SIGNED) < 11 and  $.F1 Like 'Field-1%'").set("$.tmp2", 9999).set("$.F2", 9999).sort("$.F1 asc").execute();
        assertEquals(10, res.getAffectedItemsCount());

        /* set,unset,change together */
        //  res = coll.modify("CAST($.F3 as SIGNED) < 11").set("$.tmp2",$.tmp2+1).change("$.F1","concat($.F1,'Rajesh')").unset("$.tmp1").sort("$.F1 asc").execute();
        res = this.collection.modify("CAST($.F3 as SIGNED) < 11").set("$.tmp2", 9898).change("$.F1", "'Rajesh'").unset("$.tmp1").sort("$.F1 asc").execute();
        assertEquals(10, res.getAffectedItemsCount());

        docs = this.collection.find("CAST($.tmp2 as SIGNED) = 9898").fields("$._id as _id, $.F2 as f2").execute();
        recCnt = count_data(docs);
        assertEquals(10, recCnt);

        docs = this.collection.find("$.F1 like '%Rajesh'''").fields("$._id as _id, $.F1 as f1").execute();
        recCnt = count_data(docs);
        assertEquals(10, recCnt);

        res = this.collection.modify("CAST($.F3 as SIGNED) < 11").unset("$.tmp1").set("$.tmp2", 9897).change("$.F1", "Rajesh").sort("$.F1 asc").execute();
        assertEquals(10, res.getAffectedItemsCount());

        res = this.collection.modify("true").unset("$.tmp1").set("$.tmp2", 9897).change("$.F1", "Rajesh").sort("$.F1 asc").limit(5).execute();
        assertEquals(5, res.getAffectedItemsCount());

        res = this.collection.modify("false").unset("$.tmp1").set("$.tmp2", 9897).change("$.F1", "Rajesh").sort("$.F1 asc").limit(5).execute();
        assertEquals(0, res.getAffectedItemsCount());
    }

    @Test
    @Disabled("$.F4 = 9223372036854775807 condition without quotes bind() not supported with modify.")
    public void testCollectionModifyBind() throws Exception {
        int i = 0, maxrec = 10, recCnt = 0;
        Result res = null;
        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];
        long l1 = Long.MAX_VALUE, l2 = Long.MIN_VALUE, l3 = 2147483647;
        System.out.println("l = ===" + l1);

        double d1 = 100.4567;
        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(d1 + i)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(l1 - i)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(l2 + i)));
            newDoc2.add("F5", new JsonNumber().setValue(String.valueOf(l3 + i)));
            newDoc2.add("F6", new JsonString().setValue(2000 + i + "-02-" + (i * 2 + 10)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();
        assertEquals(maxrec, this.collection.count());

        /* find */
        DocResult docs = this.collection.find("CAST($.F3 as SIGNED)=2147483649").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3+0 as f3").execute();
        recCnt = count_data(docs);
        assertEquals(maxrec, recCnt);

        //  /*
        res = this.collection.modify("$.F4 = ?").set("$.F4", 1).bind(new Object[] { l2 }).sort("$.F1 asc").execute();
        assertEquals(1, res.getAffectedItemsCount());
        //  */
    }

    /*
     * Using Big int and Double
     */
    @Test
    public void testCollectionModifyDataTypes() throws Exception {
        int i = 0, maxrec = 10, recCnt = 0;
        DbDoc doc = null;
        Result res = null;
        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];
        long l1 = Long.MAX_VALUE, l2 = Long.MIN_VALUE, l3 = 2147483647;

        double d1 = 100.4567;
        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(d1 + i)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(l1 - i)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(l2 + i)));
            newDoc2.add("F5", new JsonNumber().setValue(String.valueOf(l3 + i)));
            newDoc2.add("F6", new JsonString().setValue(2000 + i + "-02-" + (i * 2 + 10)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();
        assertEquals(maxrec, this.collection.count());

        /* find without Condition */
        DocResult docs = this.collection.find("CAST($.F5 as SIGNED)=2147483649").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3+0 as f3").execute();
        recCnt = count_data(docs);
        assertEquals(1, recCnt);

        /* condition on Double */
        res = this.collection.modify("CAST($.F2 as DECIMAL(10,4)) =" + d1).set("$.F1", "UpdData1").sort("CAST($.F2 as DECIMAL(10,4))").execute();
        assertEquals(1, res.getAffectedItemsCount());

        docs = this.collection.find("$.F1 = 'UpdData1'").fields("$._id as _id, $.F2 as f2").execute();
        doc = docs.next();
        assertEquals(new BigDecimal(String.valueOf(d1)), ((JsonNumber) doc.get("f2")).getBigDecimal());
        assertEquals(String.valueOf(1000), ((JsonString) doc.get("_id")).getString());

        /* condition on Big Int */
        res = this.collection.modify("CAST($.F3 as SIGNED) =" + l1).set("$.F1", "UpdData2").sort("CAST($.F3 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());

        docs = this.collection.find("$.F1 = 'UpdData2'").fields("$._id as _id, $.F3 as f3").execute();
        doc = docs.next();
        assertEquals(new BigDecimal(String.valueOf(l1)), ((JsonNumber) doc.get("f3")).getBigDecimal());
        assertEquals(String.valueOf(1000), ((JsonString) doc.get("_id")).getString());

        /* condition on Big Int */
        res = this.collection.modify("CAST($.F5 as SIGNED) >= " + l3 + " and  CAST($.F5 as SIGNED) < " + l1 + " and CAST($.F5 as SIGNED) > " + l2)
                .set("$.F1", "AllUpd").sort("CAST($.F5 as SIGNED)").execute();
        assertEquals(maxrec, res.getAffectedItemsCount());

        docs = this.collection.find("$.F1 = 'AllUpd'").fields("$._id as _id, $.F5 as f5").orderBy(" CAST($.F5 as SIGNED)").execute();

        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(String.valueOf(i + 1000), ((JsonString) doc.get("_id")).getString());
            assertEquals(new BigDecimal(String.valueOf(i + l3)), ((JsonNumber) doc.get("f5")).getBigDecimal());
            i++;
        }
        assertEquals(maxrec, i);

        /* condition on Double */
        res = this.collection.modify("CAST($.F5 as SIGNED) =" + l3 + "+" + 3).set("$.F1", "UpdData3").sort("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());

        docs = this.collection.find("$.F1 = 'UpdData3'").fields("$._id as _id, $.F5 as f5").execute();
        doc = docs.next();
        assertEquals(new BigDecimal(String.valueOf(3 + l3)), ((JsonNumber) doc.get("f5")).getBigDecimal());
        assertEquals(String.valueOf(1000 + 3), ((JsonString) doc.get("_id")).getString());

        /* condition on Double */
        res = this.collection.modify("CAST($.F5 as SIGNED) - 3 =" + l3).set("$.F1", "UpdData4").sort("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());

        docs = this.collection.find("$.F1 = 'UpdData4'").fields("$._id as _id, $.F5 as f5").execute();
        doc = docs.next();
        assertEquals(new BigDecimal(String.valueOf(3 + l3)), ((JsonNumber) doc.get("f5")).getBigDecimal());
        assertEquals(String.valueOf(1000 + 3), ((JsonString) doc.get("_id")).getString());

        /* condition on date */
        res = this.collection.modify("$.F6 + interval 6 day = '2007-03-02' ").set("$.F1", "UpdData5").sort("$.F6").execute();
        assertEquals(1, res.getAffectedItemsCount());

        docs = this.collection.find("$.F1 = 'UpdData5'").fields("$._id as _id, $.F6 as f6").execute();
        doc = docs.next();
        assertEquals("2007-02-24", ((JsonString) doc.get("f6")).getString());
        assertEquals(String.valueOf(1000 + 7), ((JsonString) doc.get("_id")).getString());
        assertFalse(docs.hasNext());
    }

    /*
     * Update Using Expressions
     */
    @Test
    public void testCollectionModifyExpr() throws Exception {
        int i = 0, maxrec = 10;
        DbDoc doc = null;
        Result res = null;
        DocResult docs = null;
        DbDoc[] jsonlist = new DbDocImpl[maxrec];
        long l1 = Long.MAX_VALUE, l2 = Long.MIN_VALUE, l3 = 2147483647;
        double d1 = 100.4567;
        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(d1 + i)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(l1 - i)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(l2 + i)));
            newDoc2.add("F5", new JsonNumber().setValue(String.valueOf(l3 + i)));
            newDoc2.add("F6", new JsonString().setValue(2000 + i + "-02-" + (i * 2 + 10)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();
        assertEquals(maxrec, this.collection.count());

        /* condition on Double */
        res = this.collection.modify("CAST($.F2 as DECIMAL(10,4)) =" + d1).set("$.F1", expr("concat('data',$.F1,'UpdData1')"))
                .sort("CAST($.F2 as DECIMAL(10,4))").execute();
        assertEquals(1, res.getAffectedItemsCount());

        docs = this.collection.find("$.F1 like 'data%UpdData1'").fields("$._id as _id, $.F2 as f2").execute();
        doc = docs.next();
        assertEquals(new BigDecimal(String.valueOf(d1)), ((JsonNumber) doc.get("f2")).getBigDecimal());
        assertEquals(String.valueOf(1000), ((JsonString) doc.get("_id")).getString());

        res = this.collection.modify("CAST($.F2 as DECIMAL(10,4)) =" + d1).set("$.F6", expr("$.F6 + interval 6 day")).sort("CAST($.F2 as DECIMAL(10,4))")
                .execute();
        assertEquals(1, res.getAffectedItemsCount());

        docs = this.collection.find("$.F6 + interval 6 day = '2000-02-22'").fields("$._id as _id, $.F6 as f6").execute();
        doc = docs.next();
        assertEquals("2000-02-16", ((JsonString) doc.get("f6")).getString());
        assertEquals(String.valueOf(1000), ((JsonString) doc.get("_id")).getString());

        res = this.collection.modify("$.F6= '2004-02-18'").set("$.F6", expr("$.F6 + interval 11 day")).set("$.F1", "NewData")
                .sort("CAST($.F2 as DECIMAL(10,4))").execute();
        assertEquals(1, res.getAffectedItemsCount());

        docs = this.collection.find("$.F1 = 'NewData'").fields("$._id as _id, $.F6 as f6").execute();
        doc = docs.next();
        assertEquals("2004-02-29", ((JsonString) doc.get("f6")).getString());
        assertEquals(String.valueOf(1004), ((JsonString) doc.get("_id")).getString());

        /* condition on Big Int */
        res = this.collection.modify("CAST($.F3 as SIGNED) =" + l1).set("$.F3", expr("CAST($.F3 as SIGNED)  -1")).sort("CAST($.F3 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());

        res = this.collection.modify("CAST($.F3 as SIGNED) + 1  =" + l1).set("$.F3", expr("CAST($.F3 as SIGNED) + 1")).sort("CAST($.F3 as SIGNED)").execute();
        assertEquals(2, res.getAffectedItemsCount());

        docs = this.collection.find("CAST($.F3 as SIGNED)=" + l1).fields("$._id as _id, $.F3 as f3").orderBy("$._id asc").execute();
        doc = docs.next();
        assertEquals(new BigDecimal(String.valueOf(l1)), ((JsonNumber) doc.get("f3")).getBigDecimal());
        assertEquals(String.valueOf(1000), ((JsonString) doc.get("_id")).getString());

        doc = docs.next();
        assertEquals(new BigDecimal(String.valueOf(l1)), ((JsonNumber) doc.get("f3")).getBigDecimal());
        assertEquals(String.valueOf(1001), ((JsonString) doc.get("_id")).getString());
        assertFalse(docs.hasNext());

        /* condition on Big Int.Compex Expression */
        res = this.collection.modify("CAST($.F4 as SIGNED) < 0").set("$.F1", "Abcd")
                .set("$.F4", expr("((CAST($.F4 as SIGNED) + CAST($.F3 as SIGNED)) * 1)/1.1 + 1 ")).execute();
        assertEquals(maxrec, res.getAffectedItemsCount());

        res = this.collection.modify("true").set("$.F1", expr("concat('data',$.F1,'UpdData1')")).sort("CAST($.F2 as DECIMAL(10,4))").execute();
        assertEquals(10, res.getAffectedItemsCount());

        res = this.collection.modify("false").set("$.F1", "Abcd").set("$.F4", expr("((CAST($.F4 as SIGNED) + CAST($.F3 as SIGNED)) * 1)/1.1 + 1 ")).execute();
        assertEquals(0, res.getAffectedItemsCount());
    }

    @Test
    public void testCollectionModifyArray() throws Exception {
        int i = 0, j = 0, maxrec = 8, arraySize = 30;
        int lStr = 1024 * 800;
        JsonArray yArray = null;
        DbDoc doc = null;
        DocResult docs = null;
        Result res = null;
        String s1 = buildString(lStr, 'X');
        long l3 = 2147483647;
        double d1 = 1000.1234;

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i + 1)));

            JsonArray jarray = new JsonArray();
            for (j = 0; j < arraySize; j++) {
                jarray.addValue(new JsonNumber().setValue(String.valueOf(l3 + j + i)));
            }
            newDoc2.add("ARR1", jarray);

            JsonArray karray = new JsonArray();
            for (j = 0; j < arraySize; j++) {
                karray.addValue(new JsonNumber().setValue(String.valueOf(d1 + j + i)));
            }
            newDoc2.add("ARR2", karray);
            JsonArray larray = new JsonArray();
            for (j = 0; j < arraySize; j++) {
                larray.addValue(new JsonString().setValue("St_" + i + "_" + j));
            }
            newDoc2.add("ARR3", larray);
            this.collection.add(newDoc2).execute();
            newDoc2 = null;
            jarray = null;
        }
        assertEquals(maxrec, this.collection.count());

        //Update Array data using expr
        res = this.collection.modify("$.F1 = 1").change("$.ARR1[1]", expr("$.ARR1[1] / $.ARR1[1]")).sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());

        docs = this.collection.find("CAST($.ARR1[1] as SIGNED) = 1").orderBy("$._id").execute();
        doc = docs.next();
        assertEquals((long) 1, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertFalse(docs.hasNext());

        /* Unset Array element */
        res = this.collection.modify("CAST($.F1 as SIGNED) = 1").unset("$.ARR1[1]").sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("$.F1 = 1").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR1");
        assertEquals(arraySize - 1, yArray.size());
        assertEquals(new BigDecimal("2147483647"), ((JsonNumber) yArray.get(0)).getBigDecimal());
        assertEquals(new BigDecimal("2147483649"), ((JsonNumber) yArray.get(1)).getBigDecimal());
        assertFalse(docs.hasNext());

        /* set Array element */
        res = this.collection.modify("CAST($.F1 as SIGNED) = 1").set("$.ARR1[1]", 90).sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("$.F1 = 1").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR1");
        assertEquals(arraySize - 1, yArray.size());
        assertEquals(new BigDecimal("2147483647"), ((JsonNumber) yArray.get(0)).getBigDecimal());
        assertEquals(new BigDecimal("90"), ((JsonNumber) yArray.get(1)).getBigDecimal());
        assertFalse(docs.hasNext());

        /* set Array element */
        res = this.collection.modify("CAST($.F1 as SIGNED) = 1").set("$.ARR1[2]", 91).sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("$.F1 = 1").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR1");
        assertEquals(arraySize - 1, yArray.size());
        assertEquals(new BigDecimal("2147483647"), ((JsonNumber) yArray.get(0)).getBigDecimal());
        assertEquals(new BigDecimal("90"), ((JsonNumber) yArray.get(1)).getBigDecimal());
        assertEquals(new BigDecimal("91"), ((JsonNumber) yArray.get(2)).getBigDecimal());
        assertFalse(docs.hasNext());

        /* set Array element (String) */
        res = this.collection.modify("CAST($.F1 as SIGNED) = 1").set("$.ARR3[1]", s1).sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("length($.ARR3[1]) = " + lStr).orderBy("$._id").execute();
        doc = docs.next();
        assertEquals((long) 1, (long) ((JsonNumber) doc.get("F1")).getInteger());
        yArray = (JsonArray) doc.get("ARR3");
        assertEquals(arraySize, yArray.size());
        assertEquals("St_0_0", ((JsonString) yArray.get(0)).getString());
        assertEquals("St_0_2", ((JsonString) yArray.get(2)).getString());
        assertFalse(docs.hasNext());

        /* set Array element (String) */
        res = this.collection.modify("CAST($.F1 as SIGNED) = 1").set("$.ARR3[1]", expr("concat($.ARR3[1], $.ARR3[1])")).sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("length($.ARR3[1]) = " + lStr * 2).orderBy("$._id").execute();
        doc = docs.next();
        assertEquals((long) 1, (long) ((JsonNumber) doc.get("F1")).getInteger());
        yArray = (JsonArray) doc.get("ARR3");
        assertEquals(arraySize, yArray.size());
        assertEquals("St_0_0", ((JsonString) yArray.get(0)).getString());
        assertEquals("St_0_2", ((JsonString) yArray.get(2)).getString());
        assertFalse(docs.hasNext());

        /* Change Array elements of all rows (String) */
        res = this.collection.modify("CAST($.F1 as SIGNED) >= 1").set("$.ARR3[1]", expr("concat($.ARR3[1], '" + s1 + "')")).sort("$._id").execute();
        assertEquals(maxrec, res.getAffectedItemsCount());
        docs = this.collection.find("length($.ARR3[1]) > " + lStr).orderBy("$._id").fields(expr("{'cnt':count($._id)}")).execute();
        doc = docs.next();
        assertEquals((long) maxrec, (long) ((JsonNumber) doc.get("cnt")).getInteger());
        assertFalse(docs.hasNext());

        /* Unset Array element(String) */
        res = this.collection.modify("$.F1 = 1").unset("$.ARR3[1]").sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F1 as SIGNED) = 1").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR3");
        assertEquals(arraySize - 1, yArray.size());
        assertEquals("St_0_0", ((JsonString) yArray.get(0)).getString());
        assertEquals("St_0_2", ((JsonString) yArray.get(1)).getString());
        assertFalse(docs.hasNext());

        /* Unset Array element(String) */
        res = this.collection.modify("CAST($.F1 as SIGNED) > 1").unset("$.ARR3[1]").sort("$._id").execute();
        assertEquals(maxrec - 1, res.getAffectedItemsCount());
        docs = this.collection.find("length($.ARR3[1]) < " + lStr).orderBy("$._id").fields(expr("{'cnt':count($._id)}")).execute();
        doc = docs.next();
        assertEquals((long) maxrec, (long) ((JsonNumber) doc.get("cnt")).getInteger());
        assertFalse(docs.hasNext());
    }

    /* ArrayAppend() for int double and string */
    @Test
    public void testCollectionModifyArrayAppend() throws Exception {
        int i = 0, j = 0, maxrec = 8, arraySize = 30;
        int lStr = 10;
        JsonArray yArray = null;
        DbDoc doc = null;
        DocResult docs = null;
        Result res = null;
        String s1 = buildString(lStr, '.');
        long l3 = 2147483647;
        double d1 = 1000.1234;

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i + 1)));

            JsonArray jarray = new JsonArray();
            for (j = 0; j < arraySize; j++) {
                jarray.addValue(new JsonNumber().setValue(String.valueOf(l3 + j + i)));
            }
            newDoc2.add("ARR1", jarray);

            JsonArray karray = new JsonArray();
            for (j = 0; j < arraySize; j++) {
                karray.addValue(new JsonNumber().setValue(String.valueOf(d1 + j + i)));
            }
            newDoc2.add("ARR2", karray);
            JsonArray larray = new JsonArray();
            for (j = 0; j < arraySize; j++) {
                larray.addValue(new JsonString().setValue("St_" + i + "_" + j));
            }
            newDoc2.add("ARR3", larray);
            this.collection.add(newDoc2).execute();
            newDoc2 = null;
            jarray = null;
        }
        assertEquals(maxrec, this.collection.count());

        // Append 1 number in the array (ARR1) where $.F1 = 1
        res = this.collection.modify("$.F1 = 1").arrayAppend("$.ARR1", -1).sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.ARR1[" + arraySize + "] as SIGNED) = -1").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR1");
        assertEquals(arraySize + 1, yArray.size());
        assertEquals((long) 1, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertFalse(docs.hasNext());

        // Append 3 numbers in the array (ARR1) where $.F1 = 1
        res = this.collection.modify("CAST($.F1 as SIGNED) = 1").arrayAppend("$.ARR1", -2).arrayAppend("$.ARR1", -3).arrayAppend("$.ARR1", -4).sort("$._id")
                .execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.ARR1[" + arraySize + "] as SIGNED) = -1").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR1");
        assertEquals(arraySize + 4, yArray.size());
        assertEquals((long) 1, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertFalse(docs.hasNext());

        // Append 1 number in the array (ARR2) where $.F1 = 1
        res = this.collection.modify("CAST($.F1 as SIGNED) = 1").arrayAppend("$.ARR2", -4321.4321).sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.ARR2[" + arraySize + "] as DECIMAL(10,4)) = -4321.4321").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR2");
        assertEquals(arraySize + 1, yArray.size());
        assertEquals((long) 1, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertFalse(docs.hasNext());

        // Append 3 number in the array (ARR2) where $.F1 = 1
        res = this.collection.modify("CAST($.F1 as SIGNED) = 1").arrayAppend("$.ARR2", 4321.1234).arrayAppend("$.ARR2", 4321.9847)
                .arrayAppend("$.ARR2", -4321.9888).sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.ARR2[" + arraySize + "] as  DECIMAL(10,4)) =  -4321.4321").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR2");
        assertEquals(arraySize + 4, yArray.size());
        assertEquals((long) 1, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertFalse(docs.hasNext());

        // Append 1 String in the array (ARR3) where $.F1 = 1
        res = this.collection.modify("CAST($.F1 as SIGNED) = 1").arrayAppend("$.ARR3", s1).sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("$.ARR3[" + arraySize + "] = '" + s1 + "'").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR3");
        assertEquals(arraySize + 1, yArray.size());
        assertEquals((long) 1, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertFalse(docs.hasNext());

        // Append 5 Strings in the array (ARR3) where $.F1 = 1
        res = this.collection.modify("CAST($.F1 as SIGNED) = 1").arrayAppend("$.ARR3", s1 + "1").arrayAppend("$.ARR3", s1 + "2").arrayAppend("$.ARR3", s1 + "3")
                .arrayAppend("$.ARR3", s1 + "4").arrayAppend("$.ARR3", s1 + "5").sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("$.ARR3[" + arraySize + "] = '" + s1 + "'").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR3");
        assertEquals(arraySize + 6, yArray.size());
        assertEquals((long) 1, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertFalse(docs.hasNext());
    }

    /* ArrayInsert() for int , double and string */
    @Test
    public void testCollectionModifyArrayInsert() throws Exception {
        int i = 0, j = 0, maxrec = 8, arraySize = 30;
        int lStr = 10;
        JsonArray yArray = null;
        DbDoc doc = null;
        DocResult docs = null;
        Result res = null;
        String s1 = buildString(lStr, '.');
        long l3 = 2147483647;
        double d1 = 1000.1234;

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i + 1)));

            JsonArray jarray = new JsonArray();
            for (j = 0; j < arraySize; j++) {
                jarray.addValue(new JsonNumber().setValue(String.valueOf(l3 + j + i)));
            }
            newDoc2.add("ARR1", jarray);

            JsonArray karray = new JsonArray();
            for (j = 0; j < arraySize; j++) {
                karray.addValue(new JsonNumber().setValue(String.valueOf(d1 + j + i)));
            }
            newDoc2.add("ARR2", karray);
            JsonArray larray = new JsonArray();
            for (j = 0; j < arraySize; j++) {
                larray.addValue(new JsonString().setValue("St_" + i + "_" + j));
            }
            newDoc2.add("ARR3", larray);
            this.collection.add(newDoc2).execute();
            newDoc2 = null;
            jarray = null;
        }
        assertEquals(maxrec, this.collection.count());

        // Insert to a aposistion > arraySize Shld Work same as Append
        // Insert 1 number in the array (ARR1) after position arraySize where $.F1 = 1
        res = this.collection.modify("CAST($.F1 as SIGNED) = 1").arrayInsert("$.ARR1[" + arraySize * 2 + "]", -1).sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.ARR1[" + arraySize + "] as SIGNED) = -1").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR1");
        assertEquals(arraySize + 1, yArray.size());
        assertEquals((long) 1, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertFalse(docs.hasNext());

        // Insert 3 numbers in the array (ARR1) where $.F1 = 2
        res = this.collection.modify("CAST($.F1 as SIGNED) = 2").arrayInsert("$.ARR1[0]", -2).arrayInsert("$.ARR1[1]", -3).arrayInsert("$.ARR1[2]", -4)
                .sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.ARR1[0] as SIGNED) = -2").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR1");
        assertEquals((long) (arraySize + 3), (long) yArray.size());
        assertEquals((long) 2, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertEquals((long) -2, (long) ((JsonNumber) yArray.get(0)).getInteger());
        assertEquals((long) -3, (long) ((JsonNumber) yArray.get(1)).getInteger());
        assertEquals((long) -4, (long) ((JsonNumber) yArray.get(2)).getInteger());
        assertFalse(docs.hasNext());

        // Insert 3 numbers in the array (ARR1) where $.F1 = 3
        res = this.collection.modify("CAST($.F1 as SIGNED) = 3").arrayInsert("$.ARR1[2]", -4).arrayInsert("$.ARR1[1]", -3).arrayInsert("$.ARR1[0]", -2)
                .sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.ARR1[2] as SIGNED) = -3").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR1");
        assertEquals(arraySize + 3, yArray.size());
        assertEquals(3, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertEquals((long) -2, (long) ((JsonNumber) yArray.get(0)).getInteger());
        assertEquals((long) -3, (long) ((JsonNumber) yArray.get(2)).getInteger());
        assertEquals((long) -4, (long) ((JsonNumber) yArray.get(4)).getInteger());
        assertFalse(docs.hasNext());

        // Insert 1 number in the array (ARR2) where $.F1 = 1
        res = this.collection.modify("CAST($.F1 as SIGNED) = 1").arrayInsert("$.ARR2[1]", -4321.4321).sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.ARR2[1] as DECIMAL(10,4)) = -4321.4321").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR2");
        assertEquals((long) (arraySize + 1), (long) yArray.size());
        assertEquals((long) 1, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertFalse(docs.hasNext());

        // Insert 3 number in the array (ARR2) where $.F1 = 2
        res = this.collection.modify("CAST($.F1 as SIGNED) = 2").arrayInsert("$.ARR2[2]", 4321.1234).arrayInsert("$.ARR2[0]", 4321.9847)
                .arrayInsert("$.ARR2[1]", -4321.9888).sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.ARR2[0] as  DECIMAL(10,4)) =  4321.9847").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR2");
        assertEquals(arraySize + 3, yArray.size());
        assertEquals((long) 2, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertEquals(new BigDecimal(String.valueOf("4321.1234")), ((JsonNumber) yArray.get(4)).getBigDecimal());
        assertEquals(new BigDecimal(String.valueOf("4321.9847")), ((JsonNumber) yArray.get(0)).getBigDecimal());
        assertEquals(new BigDecimal(String.valueOf("-4321.9888")), ((JsonNumber) yArray.get(1)).getBigDecimal());
        assertFalse(docs.hasNext());

        // Insert 1 String in the array (ARR3) where $.F1 = 1
        res = this.collection.modify("CAST($.F1 as SIGNED) = 1").arrayInsert("$.ARR3[1]", s1).sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("$.ARR3[1]  = '" + s1 + "'").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR3");
        assertEquals(arraySize + 1, yArray.size());
        assertEquals((long) 1, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertFalse(docs.hasNext());

        // Insert 3 Strings in the array (ARR3) where $.F1 = 2
        res = this.collection.modify("CAST($.F1 as SIGNED) = 2").arrayInsert("$.ARR3[1]", s1).arrayInsert("$.ARR3[2]", s1).arrayInsert("$.ARR3[0]", "")
                .sort("$._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("$.ARR3[0]  = ''").orderBy("$._id").execute();
        doc = docs.next();
        yArray = (JsonArray) doc.get("ARR3");
        assertEquals(arraySize + 3, yArray.size());
        assertEquals((long) 2, (long) ((JsonNumber) doc.get("F1")).getInteger());
        assertFalse(docs.hasNext());

        // Insert 3 Strings in the array (ARR3) using an empty condition
        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", () -> {
            CollectionModifyTest.this.collection.modify(" ").arrayInsert("$.ARR3[1]", s1).arrayInsert("$.ARR3[2]", s1).arrayInsert("$.ARR3[0]", "")
                    .sort("$._id").execute();
            return null;
        });

        // Insert 3 Strings in the array (ARR3) using null condition
        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", () -> {
            CollectionModifyTest.this.collection.modify(null).arrayInsert("$.ARR3[1]", s1).arrayInsert("$.ARR3[2]", s1).arrayInsert("$.ARR3[0]", "")
                    .sort("$._id").execute();
            return null;
        });
    }

    @Test
    public void testCollectionModifyAsync() throws Exception {
        int i = 0, j = 0, maxrec = 10;
        DbDoc doc = null;
        AddResult res = null;
        Result res2 = null;
        CompletableFuture<AddResult> asyncRes = null;
        CompletableFuture<Result> asyncRes2 = null;
        CompletableFuture<DocResult> asyncDocs = null;
        DocResult docs = null;
        double d = 100.123;

        /* add().executeAsync() maxrec num of records */
        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(d + i)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(100 + i)));
            newDoc2.add("T", new JsonNumber().setValue(String.valueOf(i)));
            asyncRes = this.collection.add(newDoc2).executeAsync();
            res = asyncRes.get();
            assertEquals(1, res.getAffectedItemsCount());
            newDoc2 = null;
        }

        assertEquals(maxrec, this.collection.count());

        asyncDocs = this.collection.find("F3 >= ? and F3 < ?").bind(100, 100006).fields(expr("{'cnt':count($.F1)}")).executeAsync();
        docs = asyncDocs.get();
        doc = docs.next();
        assertEquals((long) maxrec, (long) ((JsonNumber) doc.get("cnt")).getInteger());

        /* Simple Update with executeAsync */
        asyncRes2 = this.collection.modify("$.F3 > 100").unset("$.T").sort("$.F3 desc").executeAsync();
        res2 = asyncRes2.get();
        assertEquals(maxrec - 1, res2.getAffectedItemsCount());

        asyncRes2 = this.collection.modify("$.F3 >= 100").change("$.T", expr("10000+1")).sort("$.F3 desc").executeAsync();
        res2 = asyncRes2.get();
        assertEquals(1, res2.getAffectedItemsCount());

        asyncDocs = this.collection.find("$.T >= ? ").bind(10000).fields(expr("{'cnt':count($.T)}")).executeAsync();
        docs = asyncDocs.get();
        doc = docs.next();
        assertEquals((long) 1, (long) ((JsonNumber) doc.get("cnt")).getInteger());

        asyncRes2 = this.collection.modify("$.F3 >= 100").unset("$.T").sort("$.F3 desc").executeAsync();
        res2 = asyncRes.get();
        assertEquals(1, res2.getAffectedItemsCount());

        asyncRes2 = this.collection.modify("$.F3 >= 100").set("$.T", expr("10000+3")).sort("$.F3 desc").executeAsync();
        res2 = asyncRes2.get();
        assertEquals(maxrec, res2.getAffectedItemsCount());

        asyncDocs = this.collection.find("$.T >= ? ").bind(10000).fields(expr("{'cnt':count($.T)}")).executeAsync();
        docs = asyncDocs.get();
        doc = docs.next();
        assertEquals((long) maxrec, (long) ((JsonNumber) doc.get("cnt")).getInteger());

        CompletableFuture<?> futures[] = new CompletableFuture<?>[501];
        //List <Object>futures =  new ArrayList<Object>();
        j = 0;
        for (i = 0; i < 500; ++i, j++) {
            if (j >= maxrec) {
                j = 0;
            }
            futures[i] = this.collection.modify("$.F3 = " + (100 + j)).change("$.T", i).executeAsync();
        }
        for (i = 0; i < 500; ++i, j++) {
            // res = ((CompletableFuture<Result>) futures.get(i)).get();
            res2 = (Result) futures[i].get();
            assertEquals(1, res2.getAffectedItemsCount());
        }

        futures[i] = this.collection.modify("true").change("$.T", -1).executeAsync();

        // wait for them all to finish
        CompletableFuture.allOf(futures).get();

        asyncDocs = this.collection.find("$.T = ? ").bind(-1).fields(expr("{'cnt':count($.T)}")).executeAsync();
        docs = asyncDocs.get();
        doc = docs.next();
        assertEquals((long) maxrec, (long) ((JsonNumber) doc.get("cnt")).getInteger());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCollectionModifyAsyncMany() throws Exception {
        int i = 0, maxrec = 10;
        int NUMBER_OF_QUERIES = 1000;
        DbDoc doc = null;
        AddResult res = null;
        Result res2 = null;
        CompletableFuture<AddResult> asyncRes = null;
        CompletableFuture<DocResult> asyncDocs = null;
        DocResult docs = null;
        double d = 100.123;

        /* add().executeAsync() maxrec num of records */
        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(d + i)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(1000 + i)));
            newDoc2.add("T", new JsonNumber().setValue(String.valueOf(i)));
            asyncRes = this.collection.add(newDoc2).executeAsync();
            res = asyncRes.get();
            assertEquals(1, res.getAffectedItemsCount());
            newDoc2 = null;
        }

        assertEquals(maxrec, this.collection.count());

        List<Object> futures = new ArrayList<>();
        for (i = 0; i < NUMBER_OF_QUERIES; ++i) {
            if (i % 3 == 0) {
                futures.add(this.collection.modify("$.F3 % 2 = 0 ").change("$.T", expr("1000000+" + i)).sort("$.F3 desc").executeAsync());
            } else if (i % 3 == 1) {
                futures.add(this.collection.modify("$.F3 = " + (1000 + i)).change("$.T", expr("NON_EXISTING_FUNCTION()")).sort("$.F3 desc").executeAsync());//Error
            } else {
                futures.add(this.collection.modify("$.F3 % 2 = 1 ").change("$.T", expr("$.F3+" + i)).sort("$.F3 desc").executeAsync());
            }
        }

        for (i = 0; i < NUMBER_OF_QUERIES; ++i) {
            if (i % 3 == 0) {
                res2 = ((CompletableFuture<AddResult>) futures.get(i)).get();
                assertEquals(maxrec / 2, res2.getAffectedItemsCount());
            } else if (i % 3 == 1) {
                int i1 = i;
                assertThrows(ExecutionException.class, ".*FUNCTION " + this.schema.getName() + ".NON_EXISTING_FUNCTION does not exist.*",
                        () -> ((CompletableFuture<Result>) futures.get(i1)).get());
            } else {
                res2 = ((CompletableFuture<Result>) futures.get(i)).get();
                assertEquals(maxrec / 2, res2.getAffectedItemsCount());
            }
        }

        asyncDocs = this.collection.find("$.T > :X ").bind("X", 1000000).fields(expr("{'cnt':count($.T)}")).executeAsync();
        docs = asyncDocs.get();
        doc = docs.next();
        assertEquals((long) maxrec / 2, (long) ((JsonNumber) doc.get("cnt")).getInteger());

        asyncDocs = this.collection.find("$.T > :X and $.T < :Y").bind("X", 1000).bind("Y", 1000000).fields(expr("{'cnt':count($.T)}")).executeAsync();
        docs = asyncDocs.get();
        doc = docs.next();
        assertEquals((long) maxrec / 2, (long) ((JsonNumber) doc.get("cnt")).getInteger());
    }

    /**
     * Tests fix for Bug#107510 (Bug#34259416), Empty string given to set() from Collection.modify() replaces full document.
     *
     * @throws Exception
     */
    @Test
    public void testBug107510() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5")), "MySQL 8.0.5+ is required to run this test.");

        this.collection.add("{\"bug\": \"testBug107510\"}").execute();
        DbDoc doc = this.collection.find().execute().fetchOne();
        assertEquals("testBug107510", ((JsonString) doc.get("bug")).getString());

        // .set()
        assertThrows(CJException.class, "Parameter 'docPath' must not be null or empty\\.", () -> {
            this.collection.modify("true").set("", JsonParser.parseDoc("{\"bug\": \"testBug34259416\"}")).execute();
            return null;
        });
        assertThrows(CJException.class, "Parameter 'docPath' must not be null or empty\\.", () -> {
            this.collection.modify("true").set(null, JsonParser.parseDoc("{\"bug\": \"testBug34259416\"}")).execute();
            return null;
        });
        doc = this.collection.find().execute().fetchOne();
        assertEquals("testBug107510", ((JsonString) doc.get("bug")).getString());

        this.collection.modify("true").set("$", JsonParser.parseDoc("{\"bug\": \"testBug34259416\"}")).execute();
        doc = this.collection.find().execute().fetchOne();
        assertEquals("testBug34259416", ((JsonString) doc.get("bug")).getString());

        this.collection.modify("true").set("$.type", "BUG1").execute();
        doc = this.collection.find().execute().fetchOne();
        assertEquals("testBug34259416", ((JsonString) doc.get("bug")).getString());
        assertEquals("BUG1", ((JsonString) doc.get("type")).getString());

        this.collection.modify("true").set(".type", "BUG2").execute();
        doc = this.collection.find().execute().fetchOne();
        assertEquals("testBug34259416", ((JsonString) doc.get("bug")).getString());
        assertEquals("BUG2", ((JsonString) doc.get("type")).getString());

        this.collection.modify("true").set("type", "BUG3").execute();
        doc = this.collection.find().execute().fetchOne();
        assertEquals("testBug34259416", ((JsonString) doc.get("bug")).getString());
        assertEquals("BUG3", ((JsonString) doc.get("type")).getString());

        // .unset()
        assertThrows(CJException.class, "Parameter 'docPath' must not be null or empty\\.", () -> {
            this.collection.modify("true").unset("").execute();
            return null;
        });
        assertThrows(CJException.class, "Parameter 'docPath' must not be null or empty\\.", () -> {
            this.collection.modify("true").unset((String) null).execute();
            return null;
        });
        assertThrows(CJException.class, "Parameter 'docPath' must not be null or empty\\.", () -> {
            this.collection.modify("true").unset((String[]) null).execute();
            return null;
        });

        // .change()
        assertThrows(CJException.class, "Parameter 'docPath' must not be null or empty\\.", () -> {
            this.collection.modify("true").change("", "").execute();
            return null;
        });
        assertThrows(CJException.class, "Parameter 'docPath' must not be null or empty\\.", () -> {
            this.collection.modify("true").change(null, "").execute();
            return null;
        });

        // .arrayAppend()
        assertThrows(CJException.class, "Parameter 'docPath' must not be null or empty\\.", () -> {
            this.collection.modify("true").arrayAppend("", "").execute();
            return null;
        });
        assertThrows(CJException.class, "Parameter 'docPath' must not be null or empty\\.", () -> {
            this.collection.modify("true").arrayAppend(null, "").execute();
            return null;
        });

        // .arrayInsert()
        assertThrows(CJException.class, "Parameter 'docPath' must not be null or empty\\.", () -> {
            this.collection.modify("true").arrayInsert("", "").execute();
            return null;
        });
        assertThrows(CJException.class, "Parameter 'docPath' must not be null or empty\\.", () -> {
            this.collection.modify("true").arrayInsert(null, "").execute();
            return null;
        });
    }

    /**
     * Tests fix for Bug#33637993, Loss of backslashes in data after modify api is used.
     *
     * @throws Exception
     */
    @Test
    public void testBug33637993() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0 is required to run this test.");

        Result res;
        String testData = "(%d) foo\nbar\\nbaz\\u003D|\"\""; // Use "(%d)" as an increment to force different documents each step.

        // Test document with a string element.
        DbDoc doc = new DbDocImpl().add("_id", new JsonString().setValue("1")).add("str", new JsonString().setValue(String.format(testData, 0)));
        String expected = "{\"_id\":\"1\",\"str\":\"(0) foo\\nbar\\\\nbaz\\\\u003D|\\\"\\\"\"}";

        // Add test document.
        res = this.collection.add(doc).execute();
        assertEquals(1, res.getAffectedItemsCount());
        assertEquals(expected, this.collection.find("_id = \"1\"").execute().fetchOne().toString());

        // Modify using .set(DbDoc)
        res = this.collection.modify("_id = \"1\"").set("str", String.format(testData, 1)).execute();
        assertEquals(1, res.getAffectedItemsCount());
        assertEquals(expected.replace("(0)", "(1)"), this.collection.find("_id = \"1\"").execute().fetchOne().toString());

        // Modify using .patch(DbDoc)
        DbDoc docEdit = new DbDocImpl().add("str", new JsonString().setValue(String.format(testData, 2)));
        res = this.collection.modify("_id = \"1\"").patch(docEdit).execute();
        assertEquals(1, res.getAffectedItemsCount());
        assertEquals(expected.replace("(0)", "(2)"), this.collection.find("_id = \"1\"").execute().fetchOne().toString());

        // Modify using .patch(String) --> Escape sequences are processed by the JSON parser, so result is different.
        expected = "{\"_id\":\"1\",\"str\":\"(3) foo\\nbar\\nbaz=|\\\"\"}";
        res = this.collection.modify("_id = \"1\"").patch("{\"str\": \"" + String.format(testData, 3) + "\"}").execute();
        assertEquals(1, res.getAffectedItemsCount());
        assertEquals(expected, this.collection.find("_id = \"1\"").execute().fetchOne().toString());

        // Test document with an array element.
        doc = new DbDocImpl().add("_id", new JsonString().setValue("2")).add("arr",
                new JsonArray().addValue(new JsonString().setValue(String.format(testData, 0))));
        expected = "{\"_id\":\"2\",\"arr\":[\"(0) foo\\nbar\\\\nbaz\\\\u003D|\\\"\\\"\"]}";
        String expectedPart = expected.substring(expected.indexOf("foo"), expected.indexOf(']'));
        String resAsStr;
        int p = -1;

        // Add test document.
        res = this.collection.add(doc).execute();
        assertEquals(1, res.getAffectedItemsCount());
        assertEquals(expected, this.collection.find("_id = \"2\"").execute().fetchOne().toString());

        // Modify using .arrayInsert()
        res = this.collection.modify("_id = \"2\"").arrayInsert("arr[1]", String.format(testData, 1)).execute();
        assertEquals(1, res.getAffectedItemsCount());
        resAsStr = this.collection.find("_id = \"2\"").execute().fetchOne().toString();
        assertTrue((p = resAsStr.indexOf(expectedPart, p + 1)) > 0);
        assertTrue((p = resAsStr.indexOf(expectedPart, p + 1)) > 0);
        assertFalse((p = resAsStr.indexOf(expectedPart, p + 1)) > 0);
        p = -1;

        // Modify using .arrayInsert()
        res = this.collection.modify("_id = \"2\"").arrayAppend("arr", String.format(testData, 2)).execute();
        assertEquals(1, res.getAffectedItemsCount());
        resAsStr = this.collection.find("_id = \"2\"").execute().fetchOne().toString();
        assertTrue((p = resAsStr.indexOf(expectedPart, p + 1)) > 0);
        assertTrue((p = resAsStr.indexOf(expectedPart, p + 1)) > 0);
        assertTrue((p = resAsStr.indexOf(expectedPart, p + 1)) > 0);
        assertFalse((p = resAsStr.indexOf(expectedPart, p + 1)) > 0);

        // Final strings (within array) check.
        doc = this.collection.find("_id = \"2\"").execute().fetchOne();
        JsonArray arr = (JsonArray) doc.get("arr");
        assertEquals(3, arr.size());
        for (JsonValue v : arr) {
            assertEquals(5, ((JsonString) v).toFormattedString().indexOf(expectedPart));
        }
    }

}
