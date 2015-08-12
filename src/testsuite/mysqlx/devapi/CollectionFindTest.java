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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.FetchedDocs;
import com.mysql.cj.x.json.JsonDoc;
import com.mysql.cj.x.json.JsonNumber;
import com.mysql.cj.x.json.JsonString;

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
        super.teardownCollectionTest();
    }

    @Test
    public void testProjection() {
        // TODO: the "1" is coming back from the server as a string. checking with xplugin team if this is ok
        this.collection.add("{\"_id\":\"the_id\",\"g\":1}").execute();

        FetchedDocs docs = this.collection.find().fields("@._id as _id, @.g as g, 1 + 1 as q").execute();
        JsonDoc doc = docs.next();
        assertEquals("the_id", ((JsonString) doc.get("_id")).getString());
        assertEquals("1", ((JsonString) doc.get("g")).getString());
        assertEquals(new Integer(2), ((JsonNumber) doc.get("q")).getInteger());
    }

    @Test
    public void testDocumentProjection() {
        // use a document as a projection
        this.collection.add("{\"_id\":\"the_id\",\"g\":1}").execute();

        FetchedDocs docs = this.collection.find().fields("{'_id':@._id, 'q':1 + 1, 'g2':-20*@.g}").execute();
        JsonDoc doc = docs.next();
        assertEquals("the_id", ((JsonString) doc.get("_id")).getString());
        assertEquals(new Integer(-20), ((JsonNumber) doc.get("g2")).getInteger());
        assertEquals(new Integer(2), ((JsonNumber) doc.get("q")).getInteger());
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
        String firstId = ((JsonString) docs.next().get("_id")).getString();
        assertTrue(firstId.matches("[a-f0-9]{32}"));
        assertFalse(docs.hasNext());

        // limit 3, offset 1, order by ID, make sure we don't see the first ID
        docs = this.collection.find().orderBy("@._id").limit(3).skip(1).execute();
        assertTrue(docs.hasNext());
        assertNotEquals(firstId, ((JsonString) docs.next().get("_id")).getString());
        assertTrue(docs.hasNext());
        assertNotEquals(firstId, ((JsonString) docs.next().get("_id")).getString());
        assertTrue(docs.hasNext());
        assertNotEquals(firstId, ((JsonString) docs.next().get("_id")).getString());
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

    @Test
    public void testIntervalExpressions() {
        this.collection.add("{\"aDate\":\"2000-01-01\", \"aDatetime\":\"2000-01-01 12:00:01\"}").execute();

        FetchedDocs docs;

        docs = this.collection.find("@.aDatetime + interval 1000000 microsecond = '2000-01-01 12:00:02'").execute();
        docs.next();
        docs = this.collection.find("@.aDatetime + interval 1 second = '2000-01-01 12:00:02'").execute();
        docs.next();
        docs = this.collection.find("@.aDatetime + interval 2 minute = '2000-01-01 12:02:01'").execute();
        docs.next();
        docs = this.collection.find("@.aDatetime + interval 4 hour = '2000-01-01 16:00:01'").execute();
        docs.next();
        docs = this.collection.find("@.aDate + interval 10 day = '2000-01-11'").execute();
        docs.next();
        docs = this.collection.find("@.aDate + interval 2 week = '2000-01-15'").execute();
        docs.next();
        docs = this.collection.find("@.aDate - interval 2 month = '1999-11-01'").execute();
        docs.next();
        docs = this.collection.find("@.aDate + interval 2 quarter = '2000-07-01'").execute();
        docs.next();
        docs = this.collection.find("@.aDate - interval 1 year = '1999-01-01'").execute();
        docs.next();
        docs = this.collection.find("@.aDatetime + interval '3.1000000' second_microsecond = '2000-01-01 12:00:05'").execute();
        docs.next();
        docs = this.collection.find("@.aDatetime + interval '1:1.1' minute_microsecond = '2000-01-01 12:01:02.100000'").execute();
        docs.next();
        docs = this.collection.find("@.aDatetime + interval '1:1' minute_second = '2000-01-01 12:01:02'").execute();
        docs.next();
        docs = this.collection.find("@.aDatetime + interval '1:1:1.1' hour_microsecond = '2000-01-01 13:01:02.100000'").execute();
        docs.next();
        docs = this.collection.find("@.aDatetime + interval '1:1:1' hour_second = '2000-01-01 13:01:02'").execute();
        docs.next();
        docs = this.collection.find("@.aDatetime + interval '1:1' hour_minute = '2000-01-01 13:01:01'").execute();
        docs.next();
        docs = this.collection.find("@.aDatetime + interval '2 3:4:5.600' day_microsecond = '2000-01-03 15:04:06.600000'").execute();
        docs.next();
        docs = this.collection.find("@.aDatetime + interval '2 3:4:5' day_second = '2000-01-03 15:04:06'").execute();
        docs.next();
        docs = this.collection.find("@.aDatetime + interval '2 3:4' day_minute = '2000-01-03 15:04:01'").execute();
        docs.next();
        docs = this.collection.find("@.aDatetime + interval '2 3' day_hour = '2000-01-03 15:00:01'").execute();
        docs.next();
        docs = this.collection.find("@.aDate + interval '2-3' year_month = '2002-04-01'").execute();
        docs.next();
    }

    @Test
    // these are important to test the "operator" (BETWEEN/REGEXP/etc) to function representation in the protocol
    public void testIlriExpressions() {
        this.collection.add("{\"a\":\"some text with 5432\", \"b\":\"100\", \"c\":true}").execute();

        FetchedDocs docs;

        // TODO: this is a known problem on the server with JSON value types (with IS NULL too)
        // docs = this.collection.find("@.c IS TRUE AND @.c IS NOT FALSE").execute();
        // docs.next();

        docs = this.collection.find("@.b IN (1,100) AND @.b NOT IN (2, 200)").execute();
        docs.next();

        docs = this.collection.find("@.a LIKE '%5432' AND @.a NOT LIKE '%xxx'").execute();
        docs.next();

        docs = this.collection.find("@.b between 99 and 101 and @.b not between 101 and 102").execute();
        docs.next();

        docs = this.collection.find("@.a REGEXP '5432'").execute();
        docs.next();
        docs = this.collection.find("@.a NOT REGEXP '5432'").execute();
        assertFalse(docs.hasNext());
    }

    // TODO: test rest of expressions
}
