/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.mysqlx.devapi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.DocResult;
import com.mysql.cj.x.json.DbDoc;
import com.mysql.cj.x.json.JsonArray;
import com.mysql.cj.x.json.JsonNumber;
import com.mysql.cj.x.json.JsonString;

/**
 * @todo
 */
public class CollectionModifyTest extends CollectionTest {
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
    public void testSet() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.collection.add("{}").execute();

        this.collection.modify().set("x", "Value for x").execute();

        DocResult res = this.collection.find("x = 'Value for x'").execute();
        DbDoc jd = res.next();
        assertEquals("Value for x", ((JsonString) jd.get("x")).getString());
    }

    @Test
    public void testUnset() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.collection.add("{\"x\":\"100\", \"y\":\"200\", \"z\":1}").execute();
        this.collection.add("{\"a\":\"100\", \"b\":\"200\", \"c\":1}").execute();

        this.collection.modify().unset("$.x").unset("$.y").execute();
        this.collection.modify().unset("$.a", "$.b").execute();

        DocResult res = this.collection.find().execute();
        DbDoc jd = res.next();
        assertNull(jd.get("x"));
        assertNull(jd.get("y"));
        assertNull(jd.get("a"));
        assertNull(jd.get("b"));
    }

    @Test
    public void testReplace() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.collection.add("{\"x\":100}").execute();
        this.collection.modify().change("$.x", "99").execute();

        DocResult res = this.collection.find().execute();
        DbDoc jd = res.next();
        assertEquals("99", ((JsonString) jd.get("x")).getString());
    }

    @Test
    public void testArrayAppend() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.collection.add("{\"x\":[8,16,32]}").execute();
        this.collection.modify().arrayAppend("$.x", "64").execute();

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
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.collection.add("{\"x\":[1,2]}").execute();
        this.collection.modify().arrayInsert("$.x[1]", 43).execute();
        // same as append
        this.collection.modify().arrayInsert("$.x[3]", 44).execute();

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
        if (!this.isSetForMySQLxTests) {
            return;
        }

        DbDoc nestedDoc = new DbDoc().add("z", new JsonNumber().setValue("100"));
        DbDoc doc = new DbDoc().add("x", new JsonNumber().setValue("3")).add("y", nestedDoc);

        this.collection.add("{\"x\":1, \"y\":1}").execute();
        this.collection.add("{\"x\":2, \"y\":2}").execute();
        this.collection.add(doc).execute();
        this.collection.add("{\"x\":4, \"m\":1}").execute();

        this.collection.modify("y = 1").set("y", nestedDoc).execute();
        this.collection.modify("y = :n").set("y", nestedDoc).bind("n", 2).execute();

        this.collection.modify("x = 1").set("m", 1).execute();
        this.collection.modify().change("$.m", nestedDoc).execute();

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
        if (!this.isSetForMySQLxTests) {
            return;
        }

        JsonArray xArray = new JsonArray().addValue(new JsonString().setValue("a")).addValue(new JsonNumber().setValue("1"));
        DbDoc doc = new DbDoc().add("x", new JsonNumber().setValue("3")).add("y", xArray);

        this.collection.add("{\"x\":1, \"y\":[\"b\", 2]}").execute();
        this.collection.add("{\"x\":2, \"y\":22}").execute();
        this.collection.add(doc).execute();

        this.collection.modify().arrayInsert("$.y[1]", 44).execute();
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

}
