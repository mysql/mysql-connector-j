/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.FetchedDocs;
import com.mysql.cj.x.json.JsonDoc;

/**
 * @todo
 */
public class CollectionFindTest extends CollectionTest {
    @Before
    @Override
    public void setupCollectionTest() {
        super.setupCollectionTest();
    }

    @After
    @Override
    public void teardownCollectionTest() {
        // super.teardownCollectionTest();
    }

    @Test
    public void testLimitOffset() {
        this.collection.add("{}").execute();
        this.collection.add("{}").execute();
        this.collection.add("{}").execute();
        this.collection.add("{}").execute();

        // limit 1, order by ID, save the first ID
        FetchedDocs docs = this.collection.find().orderBy("@._id").limit(1).execute();
        assertTrue(docs.hasNext());
        String firstId = ((JsonDoc) docs.next()).get("_id").getValue();
        assertTrue(firstId.matches("[a-f0-9]{32}"));
        assertFalse(docs.hasNext());

        // limit 3, offset 1, order by ID, make sure we don't see the first ID
        docs = this.collection.find().orderBy("@._id").limit(3).skip(1).execute();
        assertTrue(docs.hasNext());
        assertNotEquals(firstId, ((JsonDoc) docs.next()).get("_id").getValue());
        assertTrue(docs.hasNext());
        assertNotEquals(firstId, ((JsonDoc) docs.next()).get("_id").getValue());
        assertTrue(docs.hasNext());
        assertNotEquals(firstId, ((JsonDoc) docs.next()).get("_id").getValue());
        assertFalse(docs.hasNext());
    }

    @Test
    public void testNumericExpressions() {
        this.collection.add("{\"x\":\"1\", \"y\":\"2\"}").execute();

        FetchedDocs docs;

        docs = this.collection.find("@.x + @.y = 3").execute();
        docs.next();
        docs = this.collection.find("@.y - @.x = 1").execute();
        docs.next();
        docs = this.collection.find("@.y = @.x * 2").execute();
        docs.next();
        docs = this.collection.find("@.x = @.y / 2").execute();
        docs.next();
        docs = this.collection.find("@.x = 3 % @.y").execute();
        docs.next();
        docs = this.collection.find("@.x != @.y").execute();
        docs.next();
        docs = this.collection.find("@.x < @.y").execute();
        docs.next();
        docs = this.collection.find("@.x <= @.y").execute();
        docs.next();
        docs = this.collection.find("@.y > @.x").execute();
        docs.next();
        docs = this.collection.find("@.y >= @.x").execute();
        docs.next();
        docs = this.collection.find("@.y > 1.9 and @.y < 2.1").execute();
        docs.next();
    }

    @Test
    public void testBitwiseExpressions() {
        this.collection.add("{\"x1\":\"31\", \"x2\":\"13\", \"x3\":\"8\", \"x4\":\"18446744073709551614\"}").execute();

        FetchedDocs docs;

        docs = this.collection.find("@.x1 = 29 | 15").execute();
        docs.next();
        docs = this.collection.find("@.x2 = 29 & 15").execute();
        docs.next();
        docs = this.collection.find("@.x3 = 11 ^ 3").execute();
        docs.next();
        docs = this.collection.find("@.x3 = 1 << 3").execute();
        docs.next();
        docs = this.collection.find("@.x3 = 16 >> 1").execute();
        docs.next();
        docs = this.collection.find("@.x4 = ~1").execute();
        docs.next();
    }
}
