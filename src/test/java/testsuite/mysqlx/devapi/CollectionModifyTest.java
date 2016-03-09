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

        this.collection.modify().unset("$.x").unset("$.y").execute();

        DocResult res = this.collection.find().execute();
        DbDoc jd = res.next();
        assertNull(jd.get("x"));
        assertNull(jd.get("y"));
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
}
