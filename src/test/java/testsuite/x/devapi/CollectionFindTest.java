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
import static org.hamcrest.CoreMatchers.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.Statement;
import com.mysql.cj.xdevapi.Table;

/**
 * @todo
 */
public class CollectionFindTest extends BaseCollectionTestCase {

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
        if (!this.isSetForXTests) {
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
        if (!this.isSetForXTests) {
            return;
        }
        // use a document as a projection
        this.collection.add("{\"_id\":\"the_id\",\"g\":1}").execute();

        DocResult docs = this.collection.find().fields(expr("{'_id':$._id, 'q':1 + 1, 'g2':-20*$.g}")).execute();
        DbDoc doc = docs.next();
        assertEquals("the_id", ((JsonString) doc.get("_id")).getString());
        assertEquals(-20, ((JsonNumber) doc.get("g2")).getBigDecimal().intValue());
        assertEquals(new Integer(2), ((JsonNumber) doc.get("q")).getInteger());
    }

    /**
     * MYSQLCONNJ-618
     */
    @Test
    public void outOfRange() {
        if (!this.isSetForXTests) {
            return;
        }
        try {
            this.collection.add("{\"_id\": \"1\"}").execute();
            DocResult docs = this.collection.find().fields(expr("{'X':1-cast(pow(2,63) as signed)}")).execute();
            docs.next(); // we are getting valid data from xplugin before the error, need this call to force the error
            fail("Statement should raise an error");
        } catch (XProtocolError err) {
            assertEquals(MysqlErrorNumbers.ER_DATA_OUT_OF_RANGE, err.getErrorCode());
        }
    }

    /**
     * Test that {@link DocResult} implements {@link java.lang.Iterable}.
     */
    @Test
    public void testIterable() {
        if (!this.isSetForXTests) {
            return;
        }

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\"}").execute(); // Requires manual _id.
            this.collection.add("{\"_id\": \"2\"}").execute();
            this.collection.add("{\"_id\": \"3\"}").execute();
        } else {
            this.collection.add("{}").execute();
            this.collection.add("{}").execute();
            this.collection.add("{}").execute();
        }
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
        if (!this.isSetForXTests) {
            return;
        }
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"xyz\":1}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{\"xyz\":1}").execute();
        }
        Table coll = this.schema.getCollectionAsTable(this.collection.getName());
        Row r = coll.select("doc").execute().next();
        DbDoc doc = r.getDbDoc("doc");
        assertEquals(new Integer(1), ((JsonNumber) doc.get("xyz")).getInteger());
    }

    @Test
    public void testLimitOffset() {
        if (!this.isSetForXTests) {
            return;
        }
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\"}").execute(); // Requires manual _id.
            this.collection.add("{\"_id\": \"2\"}").execute();
            this.collection.add("{\"_id\": \"3\"}").execute();
            this.collection.add("{\"_id\": \"4\"}").execute();
        } else {
            this.collection.add("{}").execute();
            this.collection.add("{}").execute();
            this.collection.add("{}").execute();
            this.collection.add("{}").execute();
        }

        // limit 1, order by ID, save the first ID
        DocResult docs = this.collection.find().orderBy("$._id").limit(1).execute();
        assertTrue(docs.hasNext());
        String firstId = ((JsonString) docs.next().get("_id")).getString();
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            assertTrue(firstId.matches("[a-f0-9]{28}"));
        }
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
        if (!this.isSetForXTests) {
            return;
        }

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"x\":1, \"y\":2}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{\"x\":1, \"y\":2}").execute();
        }

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
        if (!this.isSetForXTests) {
            return;
        }

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"x1\":31, \"x2\":13, \"x3\":8, \"x4\":\"18446744073709551614\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{\"x1\":31, \"x2\":13, \"x3\":8, \"x4\":\"18446744073709551614\"}").execute();
        }

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
        if (!this.isSetForXTests) {
            return;
        }
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"aDate\":\"2000-01-01\", \"aDatetime\":\"2000-01-01 12:00:01\"}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{\"aDate\":\"2000-01-01\", \"aDatetime\":\"2000-01-01 12:00:01\"}").execute();
        }

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
        if (!this.isSetForXTests) {
            return;
        }
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"a\":\"some text with 5432\", \"b\":\"100\", \"c\":true}").execute(); // Requires manual _id.
        } else {
            this.collection.add("{\"a\":\"some text with 5432\", \"b\":\"100\", \"c\":true}").execute();
        }

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

        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.2"))) {

            // cont_in

            docs = this.collection.find("$.b IN [100,101,102]").execute();
            assertEquals(1, docs.count());

            docs = this.collection.find("'some text with 5432' in $.a").execute();
            assertEquals(1, docs.count());

            docs = this.collection.find("1 in [1, 2, 4]").execute();
            assertEquals(1, docs.count());

            docs = this.collection.find("5 in [1, 2, 4]").execute();
            assertEquals(0, docs.count());

            docs = this.collection.find("{'a': 2} in {'a': 1, 'b': 2}").execute();
            assertEquals(0, docs.count());

            docs = this.collection.find("{'a': 1} in {'a': 1, 'b': 2}").execute();
            assertEquals(1, docs.count());

            docs = this.collection.find("{'a': 1} in {'a': 1, 'b': 2}").execute();
            assertEquals(1, docs.count());

            // not_cont_in

            docs = this.collection.find("3 not in [1, 2, 4]").execute();
            assertEquals(1, docs.count());

            docs = this.collection.find("'some text with 5432' not in $.a").execute();
            assertEquals(0, docs.count());

            docs = this.collection.find("'qqq' not in $.a").execute();
            assertEquals(1, docs.count());

            docs = this.collection.find("{'a': 1} not in {'a': 1, 'b': 2}").execute();
            assertEquals(0, docs.count());

            docs = this.collection.find("{'a': 2} not in {'a': 1, 'b': 2} AND $.b NOT IN (100, 200)").execute();
            assertEquals(0, docs.count());
        }
    }

    @Test
    public void cast() {
        if (!this.isSetForXTests) {
            return;
        }
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"x\":100}").execute();
        } else {
            this.collection.add("{\"x\":100}").execute();
        }

        DbDoc d = this.collection.find().fields("CAST($.x as SIGNED) as x").execute().next();
        assertEquals(new Integer(100), ((JsonNumber) d.get("x")).getInteger());
    }

    @Test
    public void testOrderBy() {
        if (!this.isSetForXTests) {
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

    @Test
    public void testCollectionRowLocks() throws Exception {
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3"))) {
            return;
        }

        this.collection.add("{\"_id\":\"1\", \"a\":1}").execute();
        this.collection.add("{\"_id\":\"2\", \"a\":1}").execute();
        this.collection.add("{\"_id\":\"3\", \"a\":1}").execute();

        Session session1 = null;
        Session session2 = null;

        try {
            session1 = new SessionFactory().getSession(this.testProperties);
            Collection col1 = session1.getDefaultSchema().getCollection(this.collectionName);
            session2 = new SessionFactory().getSession(this.testProperties);
            Collection col2 = session2.getDefaultSchema().getCollection(this.collectionName);

            // test1: Shared Lock
            session1.startTransaction();
            col1.find("_id = '1'").lockShared().execute();

            session2.startTransaction();
            col2.find("_id = '2'").lockShared().execute(); // should return immediately

            CompletableFuture<DocResult> res1 = col2.find("_id = '1'").lockShared().executeAsync(); // should return immediately
            res1.get(5, TimeUnit.SECONDS);
            assertTrue(res1.isDone());

            session1.rollback();
            session2.rollback();

            // test2: Shared Lock after Exclusive
            session1.startTransaction();
            col1.find("_id = '1'").lockExclusive().execute();

            session2.startTransaction();
            col2.find("_id = '2'").lockShared().execute(); // should return immediately
            CompletableFuture<DocResult> res2 = col2.find("_id = '1'").lockShared().executeAsync(); // session2 blocks
            assertThrows(TimeoutException.class, new Callable<Void>() {
                public Void call() throws Exception {
                    res2.get(5, TimeUnit.SECONDS);
                    return null;
                }
            });

            session1.rollback(); // session2 should unblock now
            res2.get(5, TimeUnit.SECONDS);
            assertTrue(res2.isDone());
            session2.rollback();

            // test3: Exclusive after Shared
            session1.startTransaction();
            col1.find("_id = '1'").lockShared().execute();
            col1.find("_id = '3'").lockShared().execute();

            session2.startTransaction();
            col2.find("_id = '2'").lockExclusive().execute(); // should return immediately
            col2.find("_id = '3'").lockShared().execute(); // should return immediately
            CompletableFuture<DocResult> res3 = col2.find("_id = '1'").lockExclusive().executeAsync(); // session2 should block
            assertThrows(TimeoutException.class, new Callable<Void>() {
                public Void call() throws Exception {
                    res3.get(5, TimeUnit.SECONDS);
                    return null;
                }
            });

            session1.rollback(); // session2 should unblock now
            res3.get(5, TimeUnit.SECONDS);
            assertTrue(res3.isDone());
            session2.rollback();

            // test4: Exclusive after Exclusive
            session1.startTransaction();
            col1.find("_id = '1'").lockExclusive().execute();

            session2.startTransaction();
            col2.find("_id = '2'").lockExclusive().execute(); // should return immediately
            CompletableFuture<DocResult> res4 = col2.find("_id = '1'").lockExclusive().executeAsync(); // session2 should block
            assertThrows(TimeoutException.class, new Callable<Void>() {
                public Void call() throws Exception {
                    res4.get(5, TimeUnit.SECONDS);
                    return null;
                }
            });

            session1.rollback(); // session2 should unblock now
            res4.get(5, TimeUnit.SECONDS);
            assertTrue(res4.isDone());
            session2.rollback();

        } finally {
            if (session1 != null) {
                session1.close();
            }
            if (session2 != null) {
                session2.close();
            }
        }

    }

    @Test
    public void testCollectionRowLockOptions() throws Exception {
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            return;
        }

        Function<DocResult, List<String>> asStringList = rr -> rr.fetchAll().stream().map(d -> ((JsonString) d.get("_id")).getString())
                .collect(Collectors.toList());

        this.collection.add("{\"_id\":\"1\", \"a\":1}").add("{\"_id\":\"2\", \"a\":1}").add("{\"_id\":\"3\", \"a\":1}").execute();

        Session session1 = null;
        Session session2 = null;

        try {
            session1 = new SessionFactory().getSession(this.testProperties);
            Collection col1 = session1.getDefaultSchema().getCollection(this.collectionName);
            session2 = new SessionFactory().getSession(this.testProperties);
            Collection col2 = session2.getDefaultSchema().getCollection(this.collectionName);
            DocResult res;
            CompletableFuture<DocResult> futRes;

            /*
             * 1. Shared Lock in both sessions.
             */

            // session2.lockShared() returns data immediately.
            session1.startTransaction();
            col1.find("_id = '1'").lockShared().execute();

            session2.startTransaction();
            res = col2.find("_id < '3'").lockShared().execute();
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), hasItems("1", "2"));
            session2.rollback();

            session2.startTransaction();
            futRes = col2.find("_id < '3'").lockShared().executeAsync();
            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("1", "2"));
            session2.rollback();

            session1.rollback();

            // session2.lockShared(NOWAIT) returns data immediately.
            session1.startTransaction();
            col1.find("_id = '1'").lockShared().execute();

            session2.startTransaction();
            res = col2.find("_id < '3'").lockShared(Statement.LockContention.NOWAIT).execute();
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("1", "2"));
            session2.rollback();

            session2.startTransaction();
            futRes = col2.find("_id < '3'").lockShared(Statement.LockContention.NOWAIT).executeAsync();
            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("1", "2"));
            session2.rollback();

            session1.rollback();

            // session2.lockShared(SKIP_LOCK) returns data immediately.
            session1.startTransaction();
            col1.find("_id = '1'").lockShared().execute();

            session2.startTransaction();
            res = col2.find("_id < '3'").lockShared(Statement.LockContention.SKIP_LOCKED).execute();
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("1", "2"));
            session2.rollback();

            session2.startTransaction();
            futRes = col2.find("_id < '3'").lockShared(Statement.LockContention.SKIP_LOCKED).executeAsync();
            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("1", "2"));
            session2.rollback();

            session1.rollback();

            /*
             * 2. Shared Lock in first session and exclusive lock in second.
             */

            // session2.lockExclusive() blocks until session1 ends.
            session1.startTransaction();
            col1.find("_id = '1'").lockShared().execute();

            // session2.startTransaction();
            // res = col2.find("_id < '3'").lockExclusive().execute(); (Can't test)
            // session2.rollback();

            session2.startTransaction();
            futRes = col2.find("_id < '3'").lockExclusive().executeAsync();
            final CompletableFuture<DocResult> fr1 = futRes;
            assertThrows(TimeoutException.class, () -> fr1.get(3, TimeUnit.SECONDS));

            session1.rollback(); // Unlocks session2.

            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("1", "2"));
            session2.rollback();

            // session2.lockExclusive(NOWAIT) should return locking error.
            session1.startTransaction();
            col1.find("_id = '1'").lockShared().execute();

            session2.startTransaction();
            assertThrows(XProtocolError.class,
                    "ERROR 3572 \\(HY000\\) Statement aborted because lock\\(s\\) could not be acquired immediately and NOWAIT is set\\.",
                    () -> col2.find("_id < '3'").lockExclusive(Statement.LockContention.NOWAIT).execute());
            session2.rollback();

            session2.startTransaction();
            futRes = col2.find("_id < '3'").lockExclusive(Statement.LockContention.NOWAIT).executeAsync();
            final CompletableFuture<DocResult> fr2 = futRes;
            assertThrows(ExecutionException.class,
                    ".*XProtocolError: ERROR 3572 \\(HY000\\) Statement aborted because lock\\(s\\) could not be acquired immediately and NOWAIT is set\\.",
                    () -> fr2.get(3, TimeUnit.SECONDS));
            session2.rollback();

            session1.rollback();

            // session2.lockExclusive(SKIP_LOCK) should return (unlocked) data immediately.
            session1.startTransaction();
            col1.find("_id = '1'").lockShared().execute();

            session2.startTransaction();
            res = col2.find("_id < '3'").lockExclusive(Statement.LockContention.SKIP_LOCKED).execute();
            assertEquals(1, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("2"));
            session2.rollback();

            session2.startTransaction();
            futRes = col2.find("_id < '3'").lockExclusive(Statement.LockContention.SKIP_LOCKED).executeAsync();
            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(1, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("2"));
            session2.rollback();

            session1.rollback();

            /*
             * 3. Exclusive Lock in first session and shared lock in second.
             */

            // session2.lockShared() blocks until session1 ends.
            session1.startTransaction();
            col1.find("_id = '1'").lockExclusive().execute();

            // session2.startTransaction();
            // res = col2.find("_id < '3'").lockShared().execute(); (Can't test)
            // session2.rollback();

            session2.startTransaction();
            futRes = col2.find("_id < '3'").lockShared().executeAsync();
            final CompletableFuture<DocResult> fr3 = futRes;
            assertThrows(TimeoutException.class, () -> fr3.get(3, TimeUnit.SECONDS));

            session1.rollback(); // Unlocks session2.

            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("1", "2"));
            session2.rollback();

            // session2.lockShared(NOWAIT) should return locking error.
            session1.startTransaction();
            col1.find("_id = '1'").lockExclusive().execute();

            session2.startTransaction();
            assertThrows(XProtocolError.class,
                    "ERROR 3572 \\(HY000\\) Statement aborted because lock\\(s\\) could not be acquired immediately and NOWAIT is set\\.",
                    () -> col2.find("_id < '3'").lockShared(Statement.LockContention.NOWAIT).execute());
            session2.rollback();

            session2.startTransaction();
            futRes = col2.find("_id < '3'").lockShared(Statement.LockContention.NOWAIT).executeAsync();
            final CompletableFuture<DocResult> fr4 = futRes;
            assertThrows(ExecutionException.class,
                    ".*XProtocolError: ERROR 3572 \\(HY000\\) Statement aborted because lock\\(s\\) could not be acquired immediately and NOWAIT is set\\.",
                    () -> fr4.get(3, TimeUnit.SECONDS));
            session2.rollback();

            session1.rollback();

            // session2.lockShared(SKIP_LOCK) should return (unlocked) data immediately.
            session1.startTransaction();
            col1.find("_id = '1'").lockExclusive().execute();

            session2.startTransaction();
            res = col2.find("_id < '3'").lockShared(Statement.LockContention.SKIP_LOCKED).execute();
            assertEquals(1, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("2"));
            session2.rollback();

            session2.startTransaction();
            futRes = col2.find("_id < '3'").lockShared(Statement.LockContention.SKIP_LOCKED).executeAsync();
            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(1, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("2"));
            session2.rollback();

            session1.rollback();

            /*
             * 4. Exclusive Lock in both sessions.
             */

            // session2.lockExclusive() blocks until session1 ends.
            session1.startTransaction();
            col1.find("_id = '1'").lockExclusive().execute();

            // session2.startTransaction();
            // res = col2.find("_id < '3'").lockExclusive().execute(); (Can't test)
            // session2.rollback();

            session2.startTransaction();
            futRes = col2.find("_id < '3'").lockExclusive().executeAsync();
            final CompletableFuture<DocResult> fr5 = futRes;
            assertThrows(TimeoutException.class, () -> fr5.get(3, TimeUnit.SECONDS));

            session1.rollback(); // Unlocks session2.

            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(2, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("1", "2"));
            session2.rollback();

            // session2.lockExclusive(NOWAIT) should return locking error.
            session1.startTransaction();
            col1.find("_id = '1'").lockExclusive().execute();

            session2.startTransaction();
            assertThrows(XProtocolError.class,
                    "ERROR 3572 \\(HY000\\) Statement aborted because lock\\(s\\) could not be acquired immediately and NOWAIT is set\\.",
                    () -> col2.find("_id < '3'").lockExclusive(Statement.LockContention.NOWAIT).execute());
            session2.rollback();

            session2.startTransaction();
            futRes = col2.find("_id < '3'").lockExclusive(Statement.LockContention.NOWAIT).executeAsync();
            final CompletableFuture<DocResult> fr6 = futRes;
            assertThrows(ExecutionException.class,
                    ".*XProtocolError: ERROR 3572 \\(HY000\\) Statement aborted because lock\\(s\\) could not be acquired immediately and NOWAIT is set\\.",
                    () -> fr6.get(3, TimeUnit.SECONDS));
            session2.rollback();

            session1.rollback();

            // session2.lockExclusive(SKIP_LOCK) should return (unlocked) data immediately.
            session1.startTransaction();
            col1.find("_id = '1'").lockExclusive().execute();

            session2.startTransaction();
            res = col2.find("_id < '3'").lockExclusive(Statement.LockContention.SKIP_LOCKED).execute();
            assertEquals(1, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("2"));
            session2.rollback();

            session2.startTransaction();
            futRes = col2.find("_id < '3'").lockExclusive(Statement.LockContention.SKIP_LOCKED).executeAsync();
            res = futRes.get(3, TimeUnit.SECONDS);
            assertTrue(futRes.isDone());
            assertEquals(1, asStringList.apply(res).size());
            assertThat(asStringList.apply(res), CoreMatchers.hasItems("2"));
            session2.rollback();

            session1.rollback();
        } finally {
            if (session1 != null) {
                session1.close();
            }
            if (session2 != null) {
                session2.close();
            }
        }
    }

    @Test
    public void getOne() {
        if (!this.isSetForXTests) {
            return;
        }

        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"a\":1}").execute();
            this.collection.add("{\"_id\": \"2\", \"a\":2}").execute();
        } else {
            this.collection.add("{\"a\":1}").execute();
            this.collection.add("{\"a\":2}").execute();
        }
        this.collection.add("{\"_id\":\"existingId\",\"a\":3}").execute();

        DbDoc doc = this.collection.getOne("existingId");
        assertNotNull(doc);
        assertEquals(new Integer(3), ((JsonNumber) doc.get("a")).getInteger());

        doc = this.collection.getOne("NotExistingId");
        assertNull(doc);

        doc = this.collection.getOne(null);
        assertNull(doc);
    }
}
