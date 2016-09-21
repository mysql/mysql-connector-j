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

import static com.mysql.cj.api.x.Expression.expr;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.api.x.DocResult;
import com.mysql.cj.api.x.Row;
import com.mysql.cj.api.x.Table;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.mysqlx.MysqlxError;
import com.mysql.cj.x.json.DbDoc;
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
        try {
            super.teardownCollectionTest();
        } catch (Exception ex) {
            // expected-to-fail tests may destroy the connection, don't penalize them here
            System.err.println("Exception during teardown:");
            ex.printStackTrace();
        }
    }

    @Test
    public void testProjection() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        // TODO: the "1" is coming back from the server as a string. checking with xplugin team if this is ok
        this.collection.add("{\"_id\":\"the_id\",\"g\":1}").execute();

        DocResult docs = this.collection.find().fields("$._id as _id, $.g as g, 1 + 1 as q").execute();
        DbDoc doc = docs.next();
        assertEquals("the_id", ((JsonString) doc.get("_id")).getString());
        assertEquals(new Integer(1), ((JsonNumber) doc.get("g")).getInteger());
        assertEquals(new Integer(2), ((JsonNumber) doc.get("q")).getInteger());

        // multiple projection strings
        docs = this.collection.find().fields("$._id as _id", "$.g as g", "1 + 1 as q").execute();
        doc = docs.next();
        assertEquals("the_id", ((JsonString) doc.get("_id")).getString());
        assertEquals(new Integer(1), ((JsonNumber) doc.get("g")).getInteger());
        assertEquals(new Integer(2), ((JsonNumber) doc.get("q")).getInteger());
    }

    @Test
    public void testDocumentProjection() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        // use a document as a projection
        this.collection.add("{\"_id\":\"the_id\",\"g\":1}").execute();

        DocResult docs = this.collection.find().fields(expr("{'_id':$._id, 'q':1 + 1, 'g2':-20*$.g}")).execute();
        DbDoc doc = docs.next();
        assertEquals("the_id", ((JsonString) doc.get("_id")).getString());
        assertEquals(new Integer(-20), ((JsonNumber) doc.get("g2")).getInteger());
        assertEquals(new Integer(2), ((JsonNumber) doc.get("q")).getInteger());
    }

    /**
     * MYSQLCONNJ-618
     */
    @Test
    public void outOfRange() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        try {
            this.collection.add("{}").execute();
            DocResult docs = this.collection.find().fields(expr("{'X':1-cast(pow(2,63) as signed)}")).execute();
            docs.next(); // we are getting valid data from xplugin before the error, need this call to force the error
            fail("Statement should raise an error");
        } catch (MysqlxError err) {
            assertEquals(MysqlErrorNumbers.ER_DATA_OUT_OF_RANGE, err.getErrorCode());
        }
    }

    /**
     * Test that {@link DocResult} implements {@link java.lang.Iterable}.
     */
    @Test
    public void testIterable() {
        if (!this.isSetForMySQLxTests) {
            return;
        }

        this.collection.add("{}").execute();
        this.collection.add("{}").execute();
        this.collection.add("{}").execute();
        DocResult docs = this.collection.find().execute();
        int numDocs = 0;
        for (DbDoc d : docs) {
            if (d != null) {
                numDocs++;
            }
        }
        assertEquals(3, numDocs);
    }

    @Test
    public void basicCollectionAsTable() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.collection.add("{\"xyz\":1}").execute();
        Table coll = this.schema.getCollectionAsTable(this.collection.getName());
        Row r = coll.select("doc").execute().next();
        DbDoc doc = r.getDbDoc("doc");
        assertEquals(new Integer(1), ((JsonNumber) doc.get("xyz")).getInteger());
    }

    @Test
    public void testLimitOffset() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.collection.add("{}").execute();
        this.collection.add("{}").execute();
        this.collection.add("{}").execute();
        this.collection.add("{}").execute();

        // limit 1, order by ID, save the first ID
        DocResult docs = this.collection.find().orderBy("$._id").limit(1).execute();
        assertTrue(docs.hasNext());
        String firstId = ((JsonString) docs.next().get("_id")).getString();
        assertTrue(firstId.matches("[a-f0-9]{32}"));
        assertFalse(docs.hasNext());

        // limit 3, offset 1, order by ID, make sure we don't see the first ID
        docs = this.collection.find().orderBy("$._id").limit(3).skip(1).execute();
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
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.collection.add("{\"x\":1, \"y\":2}").execute();

        DocResult docs;

        docs = this.collection.find("$.x + $.y = 3").execute();
        docs.next();
        docs = this.collection.find("$.y - $.x = 1").execute();
        docs.next();
        docs = this.collection.find("$.y = $.x * 2").execute();
        docs.next();
        docs = this.collection.find("$.x = $.y / 2").execute();
        docs.next();
        docs = this.collection.find("$.x = 3 % $.y").execute();
        docs.next();
        docs = this.collection.find("$.x != $.y").execute();
        docs.next();
        docs = this.collection.find("$.x < $.y").execute();
        docs.next();
        docs = this.collection.find("$.x <= $.y").execute();
        docs.next();
        docs = this.collection.find("$.y > $.x").execute();
        docs.next();
        docs = this.collection.find("$.y >= $.x").execute();
        docs.next();
        docs = this.collection.find("$.y > 1.9 and $.y < 2.1").execute();
        docs.next();
    }

    @Test
    public void testBitwiseExpressions() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.collection.add("{\"x1\":31, \"x2\":13, \"x3\":8, \"x4\":\"18446744073709551614\"}").execute();

        DocResult docs;

        docs = this.collection.find("$.x1 = 29 | 15").execute();
        docs.next();
        docs = this.collection.find("$.x2 = 29 & 15").execute();
        docs.next();
        docs = this.collection.find("$.x3 = 11 ^ 3").execute();
        docs.next();
        docs = this.collection.find("$.x3 = 1 << 3").execute();
        docs.next();
        docs = this.collection.find("$.x3 = 16 >> 1").execute();
        docs.next();
        // lack of JSON_UNQUOTE() workaround
        docs = this.collection.find("cast($.x4 as unsigned) = ~1").execute();
        docs.next();
    }

    @Test
    public void testIntervalExpressions() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.collection.add("{\"aDate\":\"2000-01-01\", \"aDatetime\":\"2000-01-01 12:00:01\"}").execute();

        DocResult docs;

        docs = this.collection.find("$.aDatetime + interval 1000000 microsecond = '2000-01-01 12:00:02'").execute();
        docs.next();
        docs = this.collection.find("$.aDatetime + interval 1 second = '2000-01-01 12:00:02'").execute();
        docs.next();
        docs = this.collection.find("$.aDatetime + interval 2 minute = '2000-01-01 12:02:01'").execute();
        docs.next();
        docs = this.collection.find("$.aDatetime + interval 4 hour = '2000-01-01 16:00:01'").execute();
        docs.next();
        docs = this.collection.find("$.aDate + interval 10 day = '2000-01-11'").execute();
        docs.next();
        docs = this.collection.find("$.aDate + interval 2 week = '2000-01-15'").execute();
        docs.next();
        docs = this.collection.find("$.aDate - interval 2 month = '1999-11-01'").execute();
        docs.next();
        docs = this.collection.find("$.aDate + interval 2 quarter = '2000-07-01'").execute();
        docs.next();
        docs = this.collection.find("$.aDate - interval 1 year = '1999-01-01'").execute();
        docs.next();
        docs = this.collection.find("$.aDatetime + interval '3.1000000' second_microsecond = '2000-01-01 12:00:05'").execute();
        docs.next();
        docs = this.collection.find("$.aDatetime + interval '1:1.1' minute_microsecond = '2000-01-01 12:01:02.100000'").execute();
        docs.next();
        docs = this.collection.find("$.aDatetime + interval '1:1' minute_second = '2000-01-01 12:01:02'").execute();
        docs.next();
        docs = this.collection.find("$.aDatetime + interval '1:1:1.1' hour_microsecond = '2000-01-01 13:01:02.100000'").execute();
        docs.next();
        docs = this.collection.find("$.aDatetime + interval '1:1:1' hour_second = '2000-01-01 13:01:02'").execute();
        docs.next();
        docs = this.collection.find("$.aDatetime + interval '1:1' hour_minute = '2000-01-01 13:01:01'").execute();
        docs.next();
        docs = this.collection.find("$.aDatetime + interval '2 3:4:5.600' day_microsecond = '2000-01-03 15:04:06.600000'").execute();
        docs.next();
        docs = this.collection.find("$.aDatetime + interval '2 3:4:5' day_second = '2000-01-03 15:04:06'").execute();
        docs.next();
        docs = this.collection.find("$.aDatetime + interval '2 3:4' day_minute = '2000-01-03 15:04:01'").execute();
        docs.next();
        docs = this.collection.find("$.aDatetime + interval '2 3' day_hour = '2000-01-03 15:00:01'").execute();
        docs.next();
        docs = this.collection.find("$.aDate + interval '2-3' year_month = '2002-04-01'").execute();
        docs.next();
    }

    @Test
    // these are important to test the "operator" (BETWEEN/REGEXP/etc) to function representation in the protocol
    public void testIlriExpressions() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.collection.add("{\"a\":\"some text with 5432\", \"b\":\"100\", \"c\":true}").execute();

        DocResult docs;

        // TODO: this is a known problem on the server with JSON value types (with IS NULL too)
        // docs = this.collection.find("$.c IS TRUE AND $.c IS NOT FALSE").execute();
        // docs.next();

        docs = this.collection.find("$.b IN (1,100) AND $.b NOT IN (2, 200)").execute();
        docs.next();

        docs = this.collection.find("$.a LIKE '%5432' AND $.a NOT LIKE '%xxx'").execute();
        docs.next();

        docs = this.collection.find("$.b between 99 and 101 and $.b not between 101 and 102").execute();
        docs.next();

        docs = this.collection.find("$.a REGEXP '5432'").execute();
        docs.next();
        docs = this.collection.find("$.a NOT REGEXP '5432'").execute();
        assertFalse(docs.hasNext());
    }

    @Test
    public void cast() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.collection.add("{\"x\":100}").execute();

        DbDoc d = this.collection.find().fields("CAST($.x as SIGNED) as x").execute().next();
        assertEquals(new Integer(100), ((JsonNumber) d.get("x")).getInteger());
    }

    @Test
    public void testOrderBy() {
        if (!this.isSetForMySQLxTests) {
            return;
        }
        this.collection.add("{\"_id\":1, \"x\":20, \"y\":22}").execute();
        this.collection.add("{\"_id\":2, \"x\":20, \"y\":21}").execute();
        this.collection.add("{\"_id\":3, \"x\":10, \"y\":40}").execute();
        this.collection.add("{\"_id\":4, \"x\":10, \"y\":50}").execute();

        DocResult docs = this.collection.find().orderBy("$.x, $.y").execute();
        assertTrue(docs.hasNext());
        assertEquals(new Integer(3), ((JsonNumber) docs.next().get("_id")).getInteger());
        assertTrue(docs.hasNext());
        assertEquals(new Integer(4), ((JsonNumber) docs.next().get("_id")).getInteger());
        assertTrue(docs.hasNext());
        assertEquals(new Integer(2), ((JsonNumber) docs.next().get("_id")).getInteger());
        assertTrue(docs.hasNext());
        assertEquals(new Integer(1), ((JsonNumber) docs.next().get("_id")).getInteger());
        assertFalse(docs.hasNext());

        // multiple SortExprStr
        docs = this.collection.find().sort("$.x", "$.y").execute();
        assertTrue(docs.hasNext());
        assertEquals(new Integer(3), ((JsonNumber) docs.next().get("_id")).getInteger());
        assertTrue(docs.hasNext());
        assertEquals(new Integer(4), ((JsonNumber) docs.next().get("_id")).getInteger());
        assertTrue(docs.hasNext());
        assertEquals(new Integer(2), ((JsonNumber) docs.next().get("_id")).getInteger());
        assertTrue(docs.hasNext());
        assertEquals(new Integer(1), ((JsonNumber) docs.next().get("_id")).getInteger());
        assertFalse(docs.hasNext());
    }

    // TODO: test rest of expressions
}
