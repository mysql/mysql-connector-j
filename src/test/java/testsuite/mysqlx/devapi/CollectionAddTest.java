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
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.mysql.cj.api.x.DocResult;
import com.mysql.cj.api.x.Result;
import com.mysql.cj.x.json.DbDoc;
import com.mysql.cj.x.json.JsonString;

public class CollectionAddTest extends CollectionTest {
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
        if (!this.isSetForMySQLxTests) {
            return;
        }
        String json = "{'firstName':'Frank', 'middleName':'Lloyd', 'lastName':'Wright'}".replaceAll("'", "\"");
        Result res = this.collection.add(json).execute();
        assertTrue(res.getLastDocumentIds().get(0).matches("[a-f0-9]{32}"));

        DocResult docs = this.collection.find("firstName like '%Fra%'").execute();
        DbDoc d = docs.next();
        JsonString val = (JsonString) d.get("lastName");
        assertEquals("Wright", val.getString());
    }

    @Test
    public void testBasicAddStringArray() {
        if (!this.isSetForMySQLxTests) {
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
        if (!this.isSetForMySQLxTests) {
            return;
        }
        DbDoc doc = new DbDoc().add("firstName", new JsonString().setValue("Georgia"));
        doc.add("middleName", new JsonString().setValue("Totto"));
        doc.add("lastName", new JsonString().setValue("O'Keeffe"));
        Result res = this.collection.add(doc).execute();
        assertTrue(res.getLastDocumentIds().get(0).matches("[a-f0-9]{32}"));

        DocResult docs = this.collection.find("lastName like 'O\\'Kee%'").execute();
        DbDoc d = docs.next();
        JsonString val = (JsonString) d.get("lastName");
        assertEquals("O'Keeffe", val.getString());
    }

    @Test
    public void testBasicAddDocArray() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        Result res = this.collection.add(new DbDoc().add("f1", new JsonString().setValue("doc1")), new DbDoc().add("f1", new JsonString().setValue("doc2")))
                .execute();
        assertTrue(res.getLastDocumentIds().get(0).matches("[a-f0-9]{32}"));
        DocResult docs = this.collection.find("f1 like 'doc%'").execute();
        assertEquals(2, docs.count());

        res = this.collection
                .add(new DbDoc[] { new DbDoc().add("f1", new JsonString().setValue("doc3")), new DbDoc().add("f1", new JsonString().setValue("doc4")) })
                .execute();
        assertTrue(res.getLastDocumentIds().get(0).matches("[a-f0-9]{32}"));
        docs = this.collection.find("f1 like 'doc%'").execute();
        assertEquals(4, docs.count());
    }

    @Test
    @Ignore("needs implemented")
    public void testBasicAddMap() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        Map<String, Object> doc = new HashMap<>();
        doc.put("x", 1);
        doc.put("y", "this is y");
        doc.put("z", new BigDecimal("44.22"));
        Result res = this.collection.add(doc).execute();
        assertTrue(res.getLastDocumentIds().get(0).matches("[a-f0-9]{32}"));

        DocResult docs = this.collection.find("z >= 44.22").execute();
        DbDoc d = docs.next();
        JsonString val = (JsonString) d.get("y");
        assertEquals("this is y", val.getString());
    }

    @Test
    public void testAddWithAssignedId() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        String json = "{'_id': 'Id#1', 'name': '<unknown>'}".replaceAll("'", "\"");
        Result res = this.collection.add(json).execute();
        assertEquals(0, res.getLastDocumentIds().size());

        DocResult docs = this.collection.find("_id == 'Id#1'").execute();
        DbDoc d = docs.next();
        JsonString val = (JsonString) d.get("name");
        assertEquals("<unknown>", val.getString());
    }

    @Test
    public void testChainedAdd() {
        if (!this.isSetForMySQLxTests) {
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
        if (!this.isSetForMySQLxTests) {
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
        if (!this.isSetForMySQLxTests) {
            return;
        }
        Result res = this.collection.add(new DbDoc[] {}).execute();
        assertEquals(0, res.getAffectedItemsCount());
        assertEquals(null, res.getAutoIncrementValue());
        assertEquals(0, res.getWarningsCount());
    }
}
