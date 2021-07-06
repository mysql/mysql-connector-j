/*
 * Copyright (c) 2015, 2021, Oracle and/or its affiliates.
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.AddResult;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DbDocImpl;
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.FindStatement;
import com.mysql.cj.xdevapi.FindStatementImpl;
import com.mysql.cj.xdevapi.JsonArray;
import com.mysql.cj.xdevapi.JsonLiteral;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.Result;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.SessionImpl;
import com.mysql.cj.xdevapi.SqlResult;
import com.mysql.cj.xdevapi.Statement;
import com.mysql.cj.xdevapi.Table;
import com.mysql.cj.xdevapi.Warning;

/**
 * @todo
 */
public class CollectionFindTest extends BaseCollectionTestCase {

    @Test
    public void testProjection() {
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
        try {
            this.collection.add("{\"_id\": \"1\"}").execute();
            DocResult docs = this.collection.find().fields(expr(
                    mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.13")) ? "{'X':cast(pow(2,63) as signed)+1}" : "{'X':1-cast(pow(2,63) as signed)}"))
                    .execute();
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

    @SuppressWarnings("deprecation")
    @Test
    public void testLimitOffset() {
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
        docs = this.collection.find().orderBy("$._id").limit(3).offset(1).execute();
        assertTrue(docs.hasNext());
        assertNotEquals(firstId, ((JsonString) docs.next().get("_id")).getString());
        assertTrue(docs.hasNext());
        assertNotEquals(firstId, ((JsonString) docs.next().get("_id")).getString());
        assertTrue(docs.hasNext());
        assertNotEquals(firstId, ((JsonString) docs.next().get("_id")).getString());
        assertFalse(docs.hasNext());

        // Test deprecated skip(n) alias for offset
        // limit 1, offset 1, order by ID, make sure we don't see the first ID
        docs = this.collection.find().orderBy("$._id").limit(1).skip(1).execute();
        assertTrue(docs.hasNext());
        assertNotEquals(firstId, ((JsonString) docs.next().get("_id")).getString());
        assertFalse(docs.hasNext());
    }

    @Test
    public void testNumericExpressions() {
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
        docs = this.collection.find("cast($.x4 as unsigned) = ~1").execute();
        docs.next();
    }

    @Test
    public void testIntervalExpressions() {
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

            docs = this.collection.find("CAST(JSON_UNQUOTE($.b) AS JSON) IN [100,101,102]").execute();
            assertEquals(1, docs.count());

            docs = this.collection.find("$.b IN ['100','101','102']").execute();
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
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.3")), "MySQL 8.0.3+ is required to run this test.");

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
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5")), "MySQL 8.0.5+ is required to run this test.");

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

    @Test
    public void testGroupingQuery() {
        this.collection.add("{\"_id\": \"01\", \"name\":\"Mamie\", \"age\":11, \"something\":0}").execute();
        this.collection.add("{\"_id\": \"02\", \"name\":\"Eulalia\", \"age\":11, \"something\":0}").execute();
        this.collection.add("{\"_id\": \"03\", \"name\":\"Polly\", \"age\":12, \"something\":0}").execute();
        this.collection.add("{\"_id\": \"04\", \"name\":\"Rufus\", \"age\":12, \"something\":0}").execute();
        this.collection.add("{\"_id\": \"05\", \"name\":\"Cassidy\", \"age\":13, \"something\":0}").execute();
        this.collection.add("{\"_id\": \"06\", \"name\":\"Olympia\", \"age\":14, \"something\":0}").execute();
        this.collection.add("{\"_id\": \"07\", \"name\":\"Lev\", \"age\":14, \"something\":0}").execute();
        this.collection.add("{\"_id\": \"08\", \"name\":\"Tierney\", \"age\":15, \"something\":0}").execute();
        this.collection.add("{\"_id\": \"09\", \"name\":\"Octavia\", \"age\":15, \"something\":0}").execute();
        this.collection.add("{\"_id\": \"10\", \"name\":\"Vesper\", \"age\":16, \"something\":0}").execute();
        this.collection.add("{\"_id\": \"11\", \"name\":\"Caspian\", \"age\":17, \"something\":0}").execute();
        this.collection.add("{\"_id\": \"12\", \"name\":\"Romy\", \"age\":17, \"something\":0}").execute();

        // Result:
        // age_group | cnt
        // 11        | 2   <-- filtered out by where
        // 12        | 2   <-- filtered out by limit
        // 13        | 1   <-- filtered out by having
        // 14        | 2   * second row in result
        // 15        | 2   * first row in result
        // 16        | 1   <-- filtered out by having
        // 17        | 2   <-- filtered out by offset
        DocResult res = this.collection.find("age > 11 and 1 < 2 and 40 between 30 and 900") //
                .fields("age as age_group, count(name) as cnt, something as something") //
                .groupBy("something, age") //
                .having("count(name) > 1") //
                .orderBy("age desc") //
                .limit(2).offset(1) //
                .execute();

        assertEquals(2, res.count());

        DbDoc doc = res.fetchOne();
        assertEquals(15, ((JsonNumber) doc.get("age_group")).getInteger().intValue());
        assertEquals(2, ((JsonNumber) doc.get("cnt")).getInteger().intValue());
        assertEquals(0, ((JsonNumber) doc.get("something")).getInteger().intValue());

        doc = res.fetchOne();
        assertEquals(14, ((JsonNumber) doc.get("age_group")).getInteger().intValue());
        assertEquals(2, ((JsonNumber) doc.get("cnt")).getInteger().intValue());
        assertEquals(0, ((JsonNumber) doc.get("something")).getInteger().intValue());
    }

    /**
     * Test for Bug#21921956, X DEVAPI: EXPRESSION PARSE ERROR WITH UNARY OPERATOR.
     */
    @Test
    public void testBug21921956() {
        this.collection.add("{\"_id\": \"1004\", \"F1\": 123}").execute();

        DocResult res = this.collection.find().fields(expr("{'X':4<< -(1-2)}")).execute();
        assertTrue(res.hasData());
        DbDoc doc = res.fetchOne();
        assertEquals(8, ((JsonNumber) doc.get("X")).getInteger().intValue());

        res = this.collection.find().fields(expr("{'X':4<< +(3-2)}")).execute();
        assertTrue(res.hasData());
        doc = res.fetchOne();
        assertEquals(8, ((JsonNumber) doc.get("X")).getInteger().intValue());

        res = this.collection.find().fields(expr("{'X':4<< !(2-2)}")).execute();
        assertTrue(res.hasData());
        doc = res.fetchOne();
        assertEquals(8, ((JsonNumber) doc.get("X")).getInteger().intValue());

        res = this.collection.find("F1 = -(-123)").execute();
        if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
            // TODO is probably a bug in MySQL 5.7 server:
            // the above request generates query like:
            // SELECT doc FROM `cjtest_5_1`.`CollectionTest-617` WHERE (JSON_EXTRACT(doc,'$.F1') = (--123))
            // which returns no records with MySQL 5.7.24 while returns a document with MySQL 8.0.14
            assertTrue(res.hasData());
            doc = res.fetchOne();
            assertEquals("1004", ((JsonString) doc.get("_id")).getString());
        }
    }

    @Test
    public void testPreparedStatements() {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14")), "MySQL 8.0.14+ is required to run this test.");

        // Prepare test data.
        this.collection.add("{\"_id\":\"1\", \"ord\": 1}", "{\"_id\":\"2\", \"ord\": 2}", "{\"_id\":\"3\", \"ord\": 3}", "{\"_id\":\"4\", \"ord\": 4}",
                "{\"_id\":\"5\", \"ord\": 5}", "{\"_id\":\"6\", \"ord\": 6}", "{\"_id\":\"7\", \"ord\": 7}", "{\"_id\":\"8\", \"ord\": 8}").execute();

        SessionFactory sf = new SessionFactory();

        /*
         * Test common usage.
         */
        Session testSession = sf.getSession(this.testProperties);

        int sessionThreadId = getThreadId(testSession);
        assertPreparedStatementsCount(sessionThreadId, 0, 1);
        assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

        Collection testCol = testSession.getDefaultSchema().getCollection(this.collectionName);

        // Initialize several FindStatement objects.
        FindStatement testFind1 = testCol.find(); // Find all.
        FindStatement testFind2 = testCol.find("$.ord >= :n"); // Criteria with one placeholder.
        FindStatement testFind3 = testCol.find("$.ord >= :n AND $.ord <= :n + 3"); // Criteria with same placeholder repeated.
        FindStatement testFind4 = testCol.find("$.ord >= :n AND $.ord <= :m"); // Criteria with multiple placeholders.

        assertPreparedStatementsCountsAndId(testSession, 0, testFind1, 0, -1);
        assertPreparedStatementsCountsAndId(testSession, 0, testFind2, 0, -1);
        assertPreparedStatementsCountsAndId(testSession, 0, testFind3, 0, -1);
        assertPreparedStatementsCountsAndId(testSession, 0, testFind4, 0, -1);

        assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

        // A. Set binds: 1st execute -> non-prepared.
        assertTestPreparedStatementsResult(testFind1.execute(), 1, 8);
        assertPreparedStatementsCountsAndId(testSession, 0, testFind1, 0, -1);
        assertTestPreparedStatementsResult(testFind2.bind("n", 2).execute(), 2, 8);
        assertPreparedStatementsCountsAndId(testSession, 0, testFind2, 0, -1);
        assertTestPreparedStatementsResult(testFind3.bind("n", 2).execute(), 2, 5);
        assertPreparedStatementsCountsAndId(testSession, 0, testFind3, 0, -1);
        assertTestPreparedStatementsResult(testFind4.bind("n", 2).bind("m", 5).execute(), 2, 5);
        assertPreparedStatementsCountsAndId(testSession, 0, testFind4, 0, -1);

        assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

        // B. Set sort resets execution count: 1st execute -> non-prepared.
        assertTestPreparedStatementsResult(testFind1.sort("$._id").execute(), 1, 8);
        assertPreparedStatementsCountsAndId(testSession, 0, testFind1, 0, -1);
        assertTestPreparedStatementsResult(testFind2.sort("$._id").execute(), 2, 8);
        assertPreparedStatementsCountsAndId(testSession, 0, testFind2, 0, -1);
        assertTestPreparedStatementsResult(testFind3.sort("$._id").execute(), 2, 5);
        assertPreparedStatementsCountsAndId(testSession, 0, testFind3, 0, -1);
        assertTestPreparedStatementsResult(testFind4.sort("$._id").execute(), 2, 5);
        assertPreparedStatementsCountsAndId(testSession, 0, testFind4, 0, -1);

        assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

        // C. Set binds reuse statement: 2nd execute -> prepare + execute.
        assertTestPreparedStatementsResult(testFind1.execute(), 1, 8);
        assertPreparedStatementsCountsAndId(testSession, 1, testFind1, 1, 1);
        assertTestPreparedStatementsResult(testFind2.bind("n", 3).execute(), 3, 8);
        assertPreparedStatementsCountsAndId(testSession, 2, testFind2, 2, 1);
        assertTestPreparedStatementsResult(testFind3.bind("n", 3).execute(), 3, 6);
        assertPreparedStatementsCountsAndId(testSession, 3, testFind3, 3, 1);
        assertTestPreparedStatementsResult(testFind4.bind("m", 6).execute(), 2, 6);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind4, 4, 1);

        assertPreparedStatementsStatusCounts(testSession, 4, 4, 0);

        // D. Set binds reuse statement: 3rd execute -> execute.
        assertTestPreparedStatementsResult(testFind1.execute(), 1, 8);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind1, 1, 2);
        assertTestPreparedStatementsResult(testFind2.bind("n", 4).execute(), 4, 8);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind2, 2, 2);
        assertTestPreparedStatementsResult(testFind3.bind("n", 4).execute(), 4, 7);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind3, 3, 2);
        assertTestPreparedStatementsResult(testFind4.bind("n", 3).bind("m", 7).execute(), 3, 7);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind4, 4, 2);

        assertPreparedStatementsStatusCounts(testSession, 4, 8, 0);

        // E. Set sort deallocates and resets execution count: 1st execute -> deallocate + non-prepared.
        assertTestPreparedStatementsResult(testFind1.sort("$._id").execute(), 1, 8);
        assertPreparedStatementsCountsAndId(testSession, 3, testFind1, 0, -1);
        assertTestPreparedStatementsResult(testFind2.sort("$._id").bind("n", 4).execute(), 4, 8);
        assertPreparedStatementsCountsAndId(testSession, 2, testFind2, 0, -1);
        assertTestPreparedStatementsResult(testFind3.sort("$._id").bind("n", 4).execute(), 4, 7);
        assertPreparedStatementsCountsAndId(testSession, 1, testFind3, 0, -1);
        assertTestPreparedStatementsResult(testFind4.sort("$._id").bind("n", 3).bind("m", 7).execute(), 3, 7);
        assertPreparedStatementsCountsAndId(testSession, 0, testFind4, 0, -1);

        assertPreparedStatementsStatusCounts(testSession, 4, 8, 4);

        // F. No Changes: 2nd execute -> prepare + execute.
        assertTestPreparedStatementsResult(testFind1.execute(), 1, 8);
        assertPreparedStatementsCountsAndId(testSession, 1, testFind1, 1, 1);
        assertTestPreparedStatementsResult(testFind2.bind("n", 4).execute(), 4, 8);
        assertPreparedStatementsCountsAndId(testSession, 2, testFind2, 2, 1);
        assertTestPreparedStatementsResult(testFind3.bind("n", 4).execute(), 4, 7);
        assertPreparedStatementsCountsAndId(testSession, 3, testFind3, 3, 1);
        assertTestPreparedStatementsResult(testFind4.bind("n", 3).bind("m", 7).execute(), 3, 7);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind4, 4, 1);

        assertPreparedStatementsStatusCounts(testSession, 8, 12, 4);

        // G. Set limit for the first time deallocates and re-prepares: 1st execute -> re-prepare + execute.
        assertTestPreparedStatementsResult(testFind1.limit(2).execute(), 1, 2);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind1, 1, 1);
        assertTestPreparedStatementsResult(testFind2.limit(2).execute(), 4, 5);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind2, 2, 1);
        assertTestPreparedStatementsResult(testFind3.limit(2).execute(), 4, 5);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind3, 3, 1);
        assertTestPreparedStatementsResult(testFind4.limit(2).execute(), 3, 4);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind4, 4, 1);

        assertPreparedStatementsStatusCounts(testSession, 12, 16, 8);

        // H. Set limit and offset reuse prepared statement: 2nd execute -> execute.
        assertTestPreparedStatementsResult(testFind1.limit(1).offset(1).execute(), 2, 2);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind1, 1, 2);
        assertTestPreparedStatementsResult(testFind2.limit(1).offset(1).execute(), 5, 5);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind2, 2, 2);
        assertTestPreparedStatementsResult(testFind3.limit(1).offset(1).execute(), 5, 5);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind3, 3, 2);
        assertTestPreparedStatementsResult(testFind4.limit(1).offset(1).execute(), 4, 4);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind4, 4, 2);

        assertPreparedStatementsStatusCounts(testSession, 12, 20, 8);

        // I. Set sort deallocates and resets execution count, set limit and bind has no effect: 1st execute -> deallocate + non-prepared.
        assertTestPreparedStatementsResult(testFind1.sort("$._id").limit(2).execute(), 2, 3);
        assertPreparedStatementsCountsAndId(testSession, 3, testFind1, 0, -1);
        assertTestPreparedStatementsResult(testFind2.sort("$._id").limit(2).bind("n", 4).execute(), 5, 6);
        assertPreparedStatementsCountsAndId(testSession, 2, testFind2, 0, -1);
        assertTestPreparedStatementsResult(testFind3.sort("$._id").limit(2).bind("n", 4).execute(), 5, 6);
        assertPreparedStatementsCountsAndId(testSession, 1, testFind3, 0, -1);
        assertTestPreparedStatementsResult(testFind4.sort("$._id").limit(2).bind("n", 3).bind("m", 7).execute(), 4, 5);
        assertPreparedStatementsCountsAndId(testSession, 0, testFind4, 0, -1);

        assertPreparedStatementsStatusCounts(testSession, 12, 20, 12);

        // J. Set offset reuse statement: 2nd execute -> prepare + execute.
        assertTestPreparedStatementsResult(testFind1.offset(0).execute(), 1, 2);
        assertPreparedStatementsCountsAndId(testSession, 1, testFind1, 1, 1);
        assertTestPreparedStatementsResult(testFind2.offset(0).execute(), 4, 5);
        assertPreparedStatementsCountsAndId(testSession, 2, testFind2, 2, 1);
        assertTestPreparedStatementsResult(testFind3.offset(0).execute(), 4, 5);
        assertPreparedStatementsCountsAndId(testSession, 3, testFind3, 3, 1);
        assertTestPreparedStatementsResult(testFind4.offset(0).execute(), 3, 4);
        assertPreparedStatementsCountsAndId(testSession, 4, testFind4, 4, 1);

        assertPreparedStatementsStatusCounts(testSession, 16, 24, 12);

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

            testCol = testSession.getDefaultSchema().getCollection(this.collectionName);

            testFind1 = testCol.find();
            testFind2 = testCol.find();

            // 1st execute -> don't prepare.
            assertTestPreparedStatementsResult(testFind1.execute(), 1, 8);
            assertPreparedStatementsCountsAndId(testSession, 0, testFind1, 0, -1);
            assertTestPreparedStatementsResult(testFind2.execute(), 1, 8);
            assertPreparedStatementsCountsAndId(testSession, 0, testFind2, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

            // 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testFind1.execute(), 1, 8);
            assertPreparedStatementsCountsAndId(testSession, 1, testFind1, 1, 1);
            assertTestPreparedStatementsResult(testFind2.execute(), 1, 8); // Fails preparing, execute as non-prepared.
            assertPreparedStatementsCountsAndId(testSession, 1, testFind2, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 2, 1, 0); // Failed prepare also counts.

            // 3rd execute -> execute.
            assertTestPreparedStatementsResult(testFind1.execute(), 1, 8);
            assertPreparedStatementsCountsAndId(testSession, 1, testFind1, 1, 2);
            assertTestPreparedStatementsResult(testFind2.execute(), 1, 8); // Execute as non-prepared.
            assertPreparedStatementsCountsAndId(testSession, 1, testFind2, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 2, 2, 0);

            testSession.close();
            assertPreparedStatementsCount(sessionThreadId, 0, 10); // Prepared statements won't live past the closing of the session.
        } finally {
            this.session.sql("SET GLOBAL max_prepared_stmt_count = ?").bind(origMaxPrepStmtCount).execute();
        }
    }

    private void assertTestPreparedStatementsResult(DocResult res, int expectedMin, int expectedMax) {
        for (DbDoc d : res.fetchAll()) {
            assertEquals(expectedMin++, ((JsonNumber) d.get("ord")).getInteger().intValue());
        }
        assertEquals(expectedMax, expectedMin - 1);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void testDeprecateWhere() throws Exception {
        this.collection.add("{\"_id\":\"1\", \"ord\": 1}", "{\"_id\":\"2\", \"ord\": 2}", "{\"_id\":\"3\", \"ord\": 3}", "{\"_id\":\"4\", \"ord\": 4}",
                "{\"_id\":\"5\", \"ord\": 5}", "{\"_id\":\"6\", \"ord\": 6}", "{\"_id\":\"7\", \"ord\": 7}", "{\"_id\":\"8\", \"ord\": 8}").execute();

        FindStatement testFind = this.collection.find("$.ord <= 2");

        assertTrue(testFind.getClass().getMethod("where", String.class).isAnnotationPresent(Deprecated.class));

        assertEquals(2, testFind.execute().count());
        assertEquals(4, ((FindStatementImpl) testFind).where("$.ord > 4").execute().count());
    }

    @Test
    public void testOverlaps() {
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.17"))) {
            // TSFR6
            assertThrows(XProtocolError.class, "ERROR 5150 \\(HY000\\) Invalid operator overlaps",
                    () -> this.collection.find("[1, 2, 3] OVERLAPS $.list").execute());
            return;
        }

        this.collection.add("{\"_id\": \"01\", \"list\":[2,3], \"age\":21, \"overlaps\":true}").execute();
        this.collection.add("{\"_id\": \"02\", \"list\":[3,4], \"age\":22, \"overlaps\":false}").execute();
        this.collection.add("{\"_id\": \"03\", \"list\":[5,6], \"age\":23, \"overlaps\":false}").execute();

        // TSFR1
        DocResult res = this.collection.find("[1, 2, 3] OVERLAPS $.list").orderBy("_id").execute();
        assertEquals(2, res.count());
        DbDoc doc = res.fetchOne();
        assertEquals("01", ((JsonString) doc.get("_id")).getString());
        doc = res.fetchOne();
        assertEquals("02", ((JsonString) doc.get("_id")).getString());

        res = this.collection.find("$.list OVERLAPS [4]").orderBy("_id").execute();
        assertEquals(1, res.count());
        doc = res.fetchOne();
        assertEquals("02", ((JsonString) doc.get("_id")).getString());

        res = this.collection.find("[1, 2, 3] NOT OVERLAPS $.list").execute();
        assertEquals(1, res.count());
        doc = res.fetchOne();
        assertEquals("03", ((JsonString) doc.get("_id")).getString());

        res = this.collection.find("$.list NOT OVERLAPS [4]").orderBy("_id").execute();
        doc = res.fetchOne();
        assertEquals("01", ((JsonString) doc.get("_id")).getString());
        doc = res.fetchOne();
        assertEquals("03", ((JsonString) doc.get("_id")).getString());

        // TSFR2
        res = this.collection.find("$.list OverLaps [1, 2]").execute();
        assertEquals(1, res.count());
        doc = res.fetchOne();
        assertEquals("01", ((JsonString) doc.get("_id")).getString());

        res = this.collection.find("$.list noT overLAPS [1, 2]").orderBy("_id").execute();
        assertEquals(2, res.count());
        doc = res.fetchOne();
        assertEquals("02", ((JsonString) doc.get("_id")).getString());
        doc = res.fetchOne();
        assertEquals("03", ((JsonString) doc.get("_id")).getString());

        res = this.collection.find("[3] overlaps $.list").orderBy("_id").execute();
        assertEquals(2, res.count());
        doc = res.fetchOne();
        assertEquals("01", ((JsonString) doc.get("_id")).getString());
        doc = res.fetchOne();
        assertEquals("02", ((JsonString) doc.get("_id")).getString());

        res = this.collection.find("[3] Not Overlaps $.list").execute();
        assertEquals(1, res.count());
        doc = res.fetchOne();
        assertEquals("03", ((JsonString) doc.get("_id")).getString());

        // TSFR3
        // C/J lexer mistakes if ident equals to reserved word, thus escaped idents are required in this case.
        res = this.collection.find().fields("$.`overlaps` as `overlaps`").orderBy("_id").execute();
        assertEquals(3, res.count());
        doc = res.fetchOne();
        assertTrue(((JsonLiteral) doc.get("overlaps")).equals(JsonLiteral.TRUE));
        doc = res.fetchOne();
        assertTrue(((JsonLiteral) doc.get("overlaps")).equals(JsonLiteral.FALSE));
        doc = res.fetchOne();
        assertTrue(((JsonLiteral) doc.get("overlaps")).equals(JsonLiteral.FALSE));

        // TSFR4
        assertThrows(WrongArgumentException.class, "Cannot find atomic expression at token pos: 0", () -> this.collection.find("overlaps $.list").execute());
        assertThrows(WrongArgumentException.class, "No more tokens when expecting one at token pos 4", () -> this.collection.find("$.list OVERLAPS").execute());
        assertThrows(WrongArgumentException.class, "Cannot find atomic expression at token pos: 0", () -> this.collection.find("overlaps").execute());
        assertThrows(WrongArgumentException.class, "Cannot find atomic expression at token pos: 1", () -> this.collection.find("NOT overlaps").execute());
        assertThrows(WrongArgumentException.class, "No more tokens when expecting one at token pos 5",
                () -> this.collection.find("$.list NOT OVERLAPS").execute());
        assertThrows(WrongArgumentException.class, "Cannot find atomic expression at token pos: 1", () -> this.collection.find("not overlaps").execute());

        // TSFR5
        res = this.collection.find("[1, 2, 3] OVERLAPS $.age").execute();
        assertEquals(0, res.count());

        res = this.collection.find("['21', '2', '3'] OVERLAPS $.age").execute();
        assertEquals(0, res.count());
    }

    @Test
    public void testCollectionFindInSanity() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, maxrec = 10;
        DbDoc doc = null;
        DocResult docs = null;
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(10 * (i + 1) + 0.1234)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(i + 1)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(10000 - i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        /* find without Condition */
        docs = this.collection.find().fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3,$.F2/10 as tmp1,1/2 as tmp2").orderBy("$.F3").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            i++;
        }
        assertEquals((maxrec), i);

        /* find with single element IN which uses json_contains */
        docs = this.collection.find("'1001' in $._id").execute();
        doc = docs.next();
        assertEquals("1001", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with multiple IN which uses json_contains */
        String findCond = "";
        for (i = 1; i < maxrec; i++) {
            findCond = findCond + "'";
            findCond = findCond + String.valueOf(i + 1000) + "' not in $._id";
            if (i != maxrec - 1) {
                findCond = findCond + " and ";
            }
        }

        docs = this.collection.find(findCond).execute();
        doc = docs.next();
        assertEquals("1000", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with single IN for string with orderBy */
        docs = this.collection.find("'Field-1-Data-2' in $.F1").orderBy("CAST($.F4 as SIGNED)").execute();
        doc = docs.next();
        assertEquals(String.valueOf(1002), (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with single IN for numeric with orderBy */
        docs = this.collection.find("10000 in $.F4").orderBy("CAST($.F4 as SIGNED)").execute();
        doc = docs.next();
        assertEquals(String.valueOf(1000), (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with single IN for float with orderBy */
        docs = this.collection.find("20.1234 in $.F2").orderBy("CAST($.F4 as SIGNED)").execute();
        doc = docs.next();
        assertEquals(String.valueOf(1001), (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* Testing with table */
        Table table = this.schema.getCollectionAsTable(this.collectionName);

        /* find with single IN for string */
        RowResult rows = table.select("doc->$._id as _id").where("'1001' in doc->$._id").execute();
        Row r = rows.next();
        assertEquals(r.getString("_id"), "\"1001\"");
        assertFalse(rows.hasNext());

        /* find with multiple IN in single select */
        findCond = "";
        for (i = 1; i < maxrec; i++) {
            findCond = findCond + "'";
            findCond = findCond + String.valueOf(i + 1000) + "' not in doc->$._id";
            if (i != maxrec - 1) {
                findCond = findCond + " and ";
            }
        }

        /* find with single IN for string */
        rows = table.select("doc->$._id as _id").where(findCond).execute();
        r = rows.next();
        assertEquals(r.getString("_id"), "\"1000\"");
        assertFalse(rows.hasNext());

        /* find with single IN for float */
        rows = table.select("doc->$._id as _id").where("20.1234 in doc->$.F2").execute();
        r = rows.next();
        assertEquals(r.getString("_id"), "\"1001\"");
        assertFalse(rows.hasNext());

        /* find with single IN for string */
        rows = table.select("doc->$._id as _id").where("'Field-1-Data-2' in doc->$.F1").execute();
        r = rows.next();
        assertEquals(r.getString("_id"), "\"1002\"");
        assertFalse(rows.hasNext());

        /* find with single IN for numeric */
        rows = table.select("doc->$._id as _id").where("10000 in doc->$.F4").execute();
        r = rows.next();
        assertEquals(r.getString("_id"), "\"1000\"");
        assertFalse(rows.hasNext());
    }

    @Test
    public void testCollectionFindInValidArray() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, j = 0, maxrec = 8, minArraySize = 3;
        DbDoc doc = null;
        DocResult docs = null;
        long l3 = 2147483647;
        double d1 = 1000.1234;

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i + 1)));

            JsonArray jarray = new JsonArray();
            for (j = 0; j < (minArraySize + i); j++) {
                jarray.addValue(new JsonNumber().setValue(String.valueOf((l3 + j + i))));
            }
            newDoc2.add("ARR1", jarray);

            JsonArray karray = new JsonArray();
            for (j = 0; j < (minArraySize + i); j++) {
                karray.addValue(new JsonNumber().setValue(String.valueOf((d1 + j + i))));
            }
            newDoc2.add("ARR2", karray);
            JsonArray larray = new JsonArray();
            for (j = 0; j < (minArraySize + i); j++) {
                larray.addValue(new JsonString().setValue("St_" + i + "_" + j));
            }
            newDoc2.add("ARR3", larray);
            this.collection.add(newDoc2).execute();
            newDoc2 = null;
            jarray = null;
        }

        assertEquals((maxrec), this.collection.count());

        /* find with single IN in array */
        docs = this.collection.find("2147483647 in $.ARR1").execute();
        doc = docs.next();
        assertEquals("1000", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with array IN array */
        docs = this.collection.find("[2147483647, 2147483648, 2147483649] in $.ARR1").execute();
        doc = docs.next();
        assertEquals("1000", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with array IN array with orderBy */
        docs = this.collection.find("[2147483648, 2147483648, 2147483649] in $.ARR1").orderBy("_id").execute();
        doc = docs.next();
        assertEquals("1000", (((JsonString) doc.get("_id")).getString()));
        doc = docs.next();
        assertEquals("1001", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with NULL IN array */
        docs = this.collection.find("NULL in $.ARR1").execute();
        assertFalse(docs.hasNext());
    }

    @Test
    public void testCollectionFindInValidMax() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, j = 0, maxFld = 100;
        DbDoc doc = null;
        DocResult docs = null;
        String json = "";
        this.session.startTransaction();

        //Max depth N records

        //Insert and Find max array
        int maxdepth = 10;//97
        json = "{\"_id\":\"1002\",\"XYZ\":1111";
        for (j = 0; j < maxFld; j++) {
            json = json + ",\"ARR" + j + "\":[";
            for (i = 0; i < maxdepth; i++) {
                json = json + i + ",[";
            }
            json = json + i;
            for (i = maxdepth - 1; i >= 0; i--) {
                json = json + "]," + i;
            }
            json = json + "]";
        }
        json = json + "}";
        this.collection.add(json).execute();

        json = "{\"_id\":\"1003\",\"XYZ\":2222";
        //maxdepth = 4;
        for (j = 0; j < maxFld; j++) {
            json = json + ",\"DATAX" + j + "\":";
            for (i = 0; i < maxdepth; i++) {
                json = json + "{\"D" + i + "\":";
            }
            json = json + maxdepth;
            for (i = maxdepth - 1; i >= 0; i--) {
                json = json + "}";
            }
        }
        json = json + "}";
        this.collection.add(json).execute();

        // Both arrays and many {}
        //maxdepth = 4;
        json = "{\"_id\":\"1001\",\"XYZ\":3333";
        for (j = 0; j < maxFld; j++) {
            json = json + ",\"ARR" + j + "\":[";
            for (i = 0; i < maxdepth; i++) {
                json = json + i + ",[";
            }
            json = json + i;
            for (i = maxdepth - 1; i >= 0; i--) {
                json = json + "]," + i;
            }
            json = json + "]";
        }

        for (j = 0; j < maxFld; j++) {
            json = json + ",\"DATAX" + j + "\":";
            for (i = 0; i < maxdepth; i++) {
                json = json + "{\"D" + i + "\":";
            }
            json = json + maxdepth;
            for (i = maxdepth - 1; i >= 0; i--) {
                json = json + "}";
            }
        }
        json = json + "}";
        this.collection.add(json).execute();

        /* find with single IN in array with max depth */
        docs = this.collection.find("10 in $.ARR0").orderBy("_id").execute();
        doc = docs.next();
        assertEquals("1001", (((JsonString) doc.get("_id")).getString()));
        doc = docs.next();
        assertEquals("1002", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        json = "";
        json = json + "[";
        for (i = 0; i < maxdepth; i++) {
            json = json + i + ",[";
        }
        json = json + i;
        for (i = maxdepth - 1; i >= 0; i--) {
            json = json + "]," + i;
        }
        json = json + "] in $.ARR0";

        /* find with single IN in array's max depth - 1 element */
        docs = this.collection.find(json).orderBy("_id").execute();
        doc = docs.next();
        assertEquals("1001", (((JsonString) doc.get("_id")).getString()));
        doc = docs.next();
        assertEquals("1002", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        json = "";
        for (i = 0; i < maxdepth; i++) {
            json = json + "{\"D" + i + "\":";
        }
        json = json + maxdepth;
        for (i = maxdepth - 1; i >= 0; i--) {
            json = json + "}";
        }

        json = json + " in $.DATAX0";

        /* find with single IN in Document */
        docs = this.collection.find(json).orderBy("_id").execute();
        doc = docs.next();
        assertEquals("1001", (((JsonString) doc.get("_id")).getString()));
        doc = docs.next();
        assertEquals("1003", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        Table table = this.schema.getCollectionAsTable(this.collectionName);

        json = "";
        json = json + "[";
        for (i = 0; i < maxdepth; i++) {
            json = json + i + ",[";
        }
        json = json + i;
        for (i = maxdepth - 1; i >= 0; i--) {
            json = json + "]," + i;
        }
        json = json + "] in doc->$.ARR0";

        /* find with single IN in max depth Document */
        RowResult rows = table.select("doc->$._id as _id").where(json).execute();
        Row r = rows.next();
        assertEquals(r.getString("_id"), "\"1001\"");
        r = rows.next();
        assertEquals(r.getString("_id"), "\"1002\"");
        assertFalse(rows.hasNext());

        json = "";
        for (i = 0; i < maxdepth; i++) {
            json = json + "{\"D" + i + "\":";
        }
        json = json + maxdepth;
        for (i = maxdepth - 1; i >= 0; i--) {
            json = json + "}";
        }

        json = json + " in doc->$.DATAX0";

        /* find with single IN in max depth Document */
        rows = table.select("doc->$._id as _id").where(json).execute();
        r = rows.next();
        assertEquals(r.getString("_id"), "\"1001\"");
        r = rows.next();
        assertEquals(r.getString("_id"), "\"1003\"");
        assertFalse(rows.hasNext());

        rows = table.select("doc->$._id as _id").where("10 in doc->$.ARR0").execute();
        r = rows.next();
        assertEquals(r.getString("_id"), "\"1001\"");
        r = rows.next();
        assertEquals(r.getString("_id"), "\"1002\"");
        assertFalse(rows.hasNext());
    }

    @Test
    public void testCollectionFindInValidFunction() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        DbDoc doc = null;
        DocResult docs = null;

        this.collection.add("{\"_id\": \"1001\", \"ARR\":[1,1,2], \"ARR1\":[\"name1\", \"name2\", \"name3\"]}").execute();
        this.collection.add("{\"_id\": \"1002\", \"ARR\":[1,2,3], \"ARR1\":[\"name4\", \"name5\", \"name6\"]}").execute();
        this.collection.add("{\"_id\": \"1003\", \"ARR\":[1,4,5], \"ARR1\":[\"name1\", \"name1\", \"name5\"]}").execute();

        docs = this.collection.find("[1,1,3] in $.ARR").execute();
        doc = docs.next();
        assertEquals("1002", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("[2,5] in $.ARR").execute();
        assertFalse(docs.hasNext());

        docs = this.collection.find("(1+2) in (1, 2, 3)").execute();
        doc = docs.next();

        docs = this.collection.find("concat('name', '6') in ('name1', 'name2', 'name6')").execute();
        doc = docs.next();

        Table tabNew = this.schema.getCollectionAsTable(this.collectionName);

        RowResult rows = tabNew.select("doc->$._id as _id").where("(1+2) in (1, 2, 3)").execute();
        rows.next();

        assertThrows(XProtocolError.class, "ERROR 5154 \\(HY000\\) CONT_IN expression requires operator that produce a JSON value\\.",
                () -> tabNew.select("doc->$._id as _id").where("(1+2) in [1, 2, 3]").execute());

        assertThrows(XProtocolError.class, "ERROR 5154 \\(HY000\\) CONT_IN expression requires operator that produce a JSON value\\.",
                () -> tabNew.select("doc->$._id as _id").where("(1+2) in doc->$.ARR").execute());

        assertThrows(XProtocolError.class, "ERROR 5154 \\(HY000\\) CONT_IN expression requires function that produce a JSON value\\.",
                () -> this.collection.find("concat('name', '6') in ['name1', 'name2', 'name6']").execute());

        assertThrows(XProtocolError.class, "ERROR 5154 \\(HY000\\) CONT_IN expression requires operator that produce a JSON value\\.",
                () -> this.collection.find("(1+2) in $.ARR").execute());

        assertThrows(XProtocolError.class, "ERROR 5154 \\(HY000\\) CONT_IN expression requires function that produce a JSON value\\.",
                () -> this.collection.find("concat('name', '6') in $.ARR1").execute());
    }

    @Test
    public void testCollectionFindInValidMix() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, j = 0, maxrec = 8, minArraySize = 3;
        DbDoc doc = null;
        DocResult docs = null;

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i + 1)));

            JsonArray jarray = new JsonArray();
            for (j = 0; j < (minArraySize + i); j++) {
                jarray.addValue(new JsonString().setValue("Field-1-Data-" + i));
            }
            newDoc2.add("ARR1", jarray);

            this.collection.add(newDoc2).execute();
            newDoc2 = null;
            jarray = null;
        }

        assertEquals((maxrec), this.collection.count());

        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1100)));
            newDoc2.add("ARR1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonString().setValue("10-15-201" + i));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        /* find with IN for key having mixture of value and array */
        docs = this.collection.find("\"10-15-2017\" in $.F2").execute();
        doc = docs.next();
        assertEquals("1107", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with NULL IN in key having mix of array and string */
        docs = this.collection.find("NULL in $.ARR1").execute();
        assertFalse(docs.hasNext());
    }

    @Test
    public void testCollectionFindInInvalid() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, j = 0, maxrec = 8, minArraySize = 3;
        DocResult docs = null;
        String json = "";

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i + 1)));

            JsonArray jarray = new JsonArray();
            for (j = 0; j < (minArraySize + i); j++) {
                jarray.addValue(new JsonString().setValue("Field-1-Data-" + i));
            }
            newDoc2.add("ARR1", jarray);

            this.collection.add(newDoc2).execute();
            newDoc2 = null;
            jarray = null;
        }

        assertEquals((maxrec), this.collection.count());

        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1100)));
            newDoc2.add("ARR1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonString().setValue("10-15-201" + i));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        json = "{\"_id\":\"1201\",\"XYZ\":2222, \"DATAX\":{\"D1\":1, \"D2\":2, \"D3\":3}}";
        this.collection.add(json).execute();

        /* find with invalid IN in document */
        try {
            docs = this.collection.find("{\"D1\":3, \"D2\":2, \"D3\":3} in $.DATAX").execute();
            assertFalse(docs.hasNext());
        } catch (XProtocolError Ex) {
            Ex.printStackTrace();
            if (Ex.getErrorCode() != MysqlErrorNumbers.ER_BAD_NULL_ERROR) {
                throw Ex;
            }
        }

        /* find with IN that does not match */
        docs = this.collection.find("\"2222\" in $.XYZ").execute();
        assertFalse(docs.hasNext());

        /* find with NULL IN */
        docs = this.collection.find("NULL in $.ARR1").execute();
        assertFalse(docs.hasNext());

        /* find with NULL IN */
        docs = this.collection.find("NULL in $.DATAX").execute();
        assertFalse(docs.hasNext());

        /* find with IN for non existant key */
        docs = this.collection.find("\"ABC\" in $.nonexistant").execute();
        assertFalse(docs.hasNext());
    }

    @Test
    public void testCollectionFindInUpdate() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, maxrec = 10;
        DbDoc doc = null;
        DocResult docs = null;

        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(10 * (i + 1) + 0.1234)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(i + 1)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(10000 - i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        /* find without Condition */
        docs = this.collection.find().fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3,$.F2/10 as tmp1,1/2 as tmp2").orderBy("$.F3").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            i++;
        }
        assertEquals((maxrec), i);

        /* modify with single IN */
        Result res = this.collection.modify("'1001' in $._id").set("$.F1", "Data_New").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("'1001' in $._id").execute();
        doc = docs.next();
        assertEquals("Data_New", (((JsonString) doc.get("F1")).getString()));
        assertFalse(docs.hasNext());

        /* find with = Condition and fetchAll() keyword */
        String findCond = "";
        for (i = 1; i < maxrec; i++) {
            findCond = findCond + "'";
            findCond = findCond + String.valueOf(i + 1000) + "' not in $._id";
            if (i != maxrec - 1) {
                findCond = findCond + " and ";
            }
        }

        /* modify with multiple IN */
        res = this.collection.modify(findCond).set("$.F1", "Data_New_1").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find(findCond).execute();
        doc = docs.next();
        assertEquals("Data_New_1", (((JsonString) doc.get("F1")).getString()));
        assertFalse(docs.hasNext());

        /* modify with single IN and sort */
        res = this.collection.modify("10000 in $.F4").set("$.F1", "Data_New_2").sort("CAST($.F4 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("10000 in $.F4").orderBy("CAST($.F4 as SIGNED)").execute();
        doc = docs.next();
        assertEquals("Data_New_2", (((JsonString) doc.get("F1")).getString()));
        assertFalse(docs.hasNext());

        /* modify with single IN and sort */
        res = this.collection.modify("20.1234 in $.F2").set("$.F1", "Data_New_3").sort("CAST($.F4 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("20.1234 in $.F2").orderBy("CAST($.F4 as SIGNED)").execute();
        doc = docs.next();
        assertEquals("Data_New_3", (((JsonString) doc.get("F1")).getString()));
        assertFalse(docs.hasNext());

        Table table = this.schema.getCollectionAsTable(this.collectionName);

        /* update with single IN */
        String toUpdate = String.format("JSON_REPLACE(doc, \"$.F1\", \"Data_New_4\")");
        res = table.update().set("doc", expr(toUpdate)).where("'1001' in doc->$._id").execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        RowResult rows = table.select("doc->$.F1 as F1").where("'1001' in doc->$._id").execute();
        Row r = rows.next();
        assertEquals(r.getString("F1"), "\"Data_New_4\"");
        assertFalse(rows.hasNext());

        findCond = "";
        for (i = 1; i < maxrec; i++) {
            findCond = findCond + "'";
            findCond = findCond + String.valueOf(i + 1000) + "' not in doc->$._id";
            if (i != maxrec - 1) {
                findCond = findCond + " and ";
            }
        }

        /* update with multiple IN */
        toUpdate = String.format("JSON_REPLACE(doc, \"$.F1\", \"Data_New_5\")");
        res = table.update().set("doc", expr(toUpdate)).where(findCond).execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        rows = table.select("doc->$.F1 as F1").where(findCond).execute();
        r = rows.next();
        assertEquals(r.getString("F1"), "\"Data_New_5\"");
        assertFalse(rows.hasNext());

        /* update with single IN for float */
        toUpdate = String.format("JSON_REPLACE(doc, \"$.F1\", \"Data_New_6\")");
        res = table.update().set("doc", expr(toUpdate)).where("20.1234 in doc->$.F2").execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        rows = table.select("doc->$.F1 as F1").where("20.1234 in doc->$.F2").execute();
        r = rows.next();
        assertEquals(r.getString("F1"), "\"Data_New_6\"");
        assertFalse(rows.hasNext());

        /* update with single IN for int */
        toUpdate = String.format("JSON_REPLACE(doc, \"$.F1\", \"Data_New_7\")");
        res = table.update().set("doc", expr(toUpdate)).where("10000 in doc->$.F4").execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        rows = table.select("doc->$.F1 as F1").where("10000 in doc->$.F4").execute();
        r = rows.next();
        assertEquals(r.getString("F1"), "\"Data_New_7\"");
        assertFalse(rows.hasNext());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCollectionFindInDelete() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, maxrec = 10;
        DocResult docs = null;
        Result res = null;

        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(10 * (i + 1) + 0.1234)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(i + 1)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(10000 + i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        /* find without Condition */
        docs = this.collection.find().fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3,$.F2/10 as tmp1,1/2 as tmp2").orderBy("$.F3").execute();
        i = 0;
        while (docs.hasNext()) {
            docs.next();
            i++;
        }
        assertEquals((maxrec), i);

        /* remove with single IN */
        res = this.collection.remove("'1001' in $._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("'1001' in $._id").execute();
        assertFalse(docs.hasNext());

        /* remove with mulltiple IN */
        String findCond = "";
        for (i = 1; i < maxrec; i++) {
            findCond = findCond + "'";
            findCond = findCond + String.valueOf(i + 1000) + "' not in $._id";
            if (i != maxrec - 1) {
                findCond = findCond + " and ";
            }
        }

        res = this.collection.remove(findCond).execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find(findCond).execute();
        assertFalse(docs.hasNext());

        /* remove with single IN */
        res = this.collection.remove("10004 in $.F4").orderBy("CAST($.F4 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("10004 in $.F4").orderBy("CAST($.F4 as SIGNED)").execute();
        assertFalse(docs.hasNext());

        /* remove with single IN for float */
        res = this.collection.remove("30.1234 in $.F2").orderBy("CAST($.F4 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("30.1234 in $.F2").orderBy("CAST($.F4 as SIGNED)").execute();
        assertFalse(docs.hasNext());

        res = this.collection.remove("true").execute();

        jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(10 * (i + 1) + 0.1234)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(i + 1)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(10000 + i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        Table table = this.schema.getCollectionAsTable(this.collectionName);

        /* delete with single IN */
        res = table.delete().where("'1001' in doc->$._id").execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        RowResult rows = table.select("doc->$.F1 as _id").where("'1001' in doc->$._id").execute();
        assertFalse(rows.hasNext());

        /* delete with multiple IN */
        findCond = "";
        for (i = 1; i < maxrec; i++) {
            findCond = findCond + "'";
            findCond = findCond + String.valueOf(i + 1000) + "' not in doc->$._id";
            if (i != maxrec - 1) {
                findCond = findCond + " and ";
            }
        }

        res = table.delete().where(findCond).execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        rows = table.select("doc->$.F1 as _id").where(findCond).execute();
        assertFalse(rows.hasNext());

        /* delete with single IN for float */
        res = table.delete().where("30.1234 in doc->$.F2").execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        rows = table.select("doc->$.F1 as F1").where("30.1234 in doc->$.F2").execute();
        assertFalse(rows.hasNext());

        /* delete with single IN for int */
        res = table.delete().where("10004 in doc->$.F4").execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        rows = table.select("doc->$.F1 as F1").where("10004 in doc->$.F4").execute();
        assertFalse(rows.hasNext());
    }

    @Test
    public void testCollectionFindOverlapsSanity() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, maxrec = 10;
        DbDoc doc = null;
        DocResult docs = null;

        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(10 * (i + 1) + 0.1234)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(i + 1)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(10000 - i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        /* find without Condition */
        docs = this.collection.find().fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3,$.F2/10 as tmp1,1/2 as tmp2").orderBy("$.F3").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            i++;
        }
        assertEquals((maxrec), i);

        /* find with single element OVERLAPS which uses json_overlaps */
        docs = this.collection.find("'1001' overlaps $._id").execute();
        doc = docs.next();
        assertEquals("1001", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with multiple OVERLAPS which uses json_overlaps */
        String findCond = "";
        for (i = 1; i < maxrec; i++) {
            findCond = findCond + "'";
            findCond = findCond + String.valueOf(i + 1000) + "' not overlaps $._id";
            if (i != maxrec - 1) {
                findCond = findCond + " and ";
            }
        }

        docs = this.collection.find(findCond).execute();
        doc = docs.next();
        assertEquals("1000", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with single OVERLAPS for string with orderBy */
        docs = this.collection.find("'Field-1-Data-2' overlaps $.F1").orderBy("CAST($.F4 as SIGNED)").execute();
        doc = docs.next();
        assertEquals(String.valueOf(1002), (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with single OVERLAPS for numeric with orderBy */
        docs = this.collection.find("10000 overlaps $.F4").orderBy("CAST($.F4 as SIGNED)").execute();
        doc = docs.next();
        assertEquals(String.valueOf(1000), (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with single OVERLAPS for float with orderBy */
        docs = this.collection.find("20.1234 overlaps $.F2").orderBy("CAST($.F4 as SIGNED)").execute();
        doc = docs.next();
        assertEquals(String.valueOf(1001), (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* Testing with table */
        Table table = this.schema.getCollectionAsTable(this.collectionName);

        /* find with single OVERLAPS for string */
        RowResult rows = table.select("doc->$._id as _id").where("'1001' overlaps doc->$._id").execute();
        Row r = rows.next();
        assertEquals(r.getString("_id"), "\"1001\"");
        assertFalse(rows.hasNext());

        /* find with multiple OVERLAPS in single select */
        findCond = "";
        for (i = 1; i < maxrec; i++) {
            findCond = findCond + "'";
            findCond = findCond + String.valueOf(i + 1000) + "' not overlaps doc->$._id";
            if (i != maxrec - 1) {
                findCond = findCond + " and ";
            }
        }

        /* find with single OVERLAPS for string */
        rows = table.select("doc->$._id as _id").where(findCond).execute();
        r = rows.next();
        assertEquals(r.getString("_id"), "\"1000\"");
        assertFalse(rows.hasNext());

        /* find with single OVERLAPS for float */
        rows = table.select("doc->$._id as _id").where("20.1234 overlaps doc->$.F2").execute();
        r = rows.next();
        assertEquals(r.getString("_id"), "\"1001\"");
        assertFalse(rows.hasNext());

        /* find with single OVERLAPS for string */
        rows = table.select("doc->$._id as _id").where("'Field-1-Data-2' overlaps doc->$.F1").execute();
        r = rows.next();
        assertEquals(r.getString("_id"), "\"1002\"");
        assertFalse(rows.hasNext());

        /* find with single OVERLAPS for numeric */
        rows = table.select("doc->$._id as _id").where("10000 overlaps doc->$.F4").execute();
        r = rows.next();
        assertEquals(r.getString("_id"), "\"1000\"");
        assertFalse(rows.hasNext());
    }

    @Test
    public void testCollectionFindOverlaps() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        try {
            int i = 0, j = 0, maxrec = 8, minArraySize = 3;
            DbDoc doc = null;
            DocResult docs = null;
            long l3 = 2147483647;
            double d1 = 1000.1234;

            for (i = 0; i < maxrec; i++) {
                DbDoc newDoc2 = new DbDocImpl();
                newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
                newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i + 1)));

                JsonArray jarray = new JsonArray();
                for (j = 0; j < (minArraySize + i); j++) {
                    jarray.addValue(new JsonNumber().setValue(String.valueOf((l3 + j + i))));
                }
                newDoc2.add("ARR1", jarray);

                JsonArray karray = new JsonArray();
                for (j = 0; j < (minArraySize + i); j++) {
                    karray.addValue(new JsonNumber().setValue(String.valueOf((d1 + j + i))));
                }
                newDoc2.add("ARR2", karray);
                JsonArray larray = new JsonArray();
                for (j = 0; j < (minArraySize + i); j++) {
                    larray.addValue(new JsonString().setValue("St_" + i + "_" + j));
                }
                newDoc2.add("ARR3", larray);
                this.collection.add(newDoc2).execute();
                newDoc2 = null;
                jarray = null;
            }

            assertEquals((maxrec), this.collection.count());

            /* find with single OVERLAPS in array */
            docs = this.collection.find("2147483647 overlaps $.ARR1").execute();
            doc = docs.next();
            assertEquals("1000", (((JsonString) doc.get("_id")).getString()));
            assertFalse(docs.hasNext());

            Table table = this.schema.getCollectionAsTable(this.collectionName);
            RowResult rows = table.select("doc->$._id as _id").where("2147483647 overlaps $.ARR1").execute();
            Row r = rows.next();
            assertEquals("\"1000\"", r.getString("_id"));
            assertFalse(rows.hasNext());

            /* find with array OVERLAPS array */
            docs = this.collection.find("[2147483647, 2147483648, 2147483649] overlaps $.ARR1").execute();
            doc = docs.next();
            assertEquals("1000", (((JsonString) doc.get("_id")).getString()));
            doc = docs.next();
            assertEquals("1001", (((JsonString) doc.get("_id")).getString()));
            doc = docs.next();
            assertEquals("1002", (((JsonString) doc.get("_id")).getString()));
            assertFalse(docs.hasNext());

            rows = table.select("doc->$._id as _id").where("[2147483647, 2147483648, 2147483649] overlaps $.ARR1").execute();
            r = rows.next();
            assertEquals("\"1000\"", r.getString("_id"));
            r = rows.next();
            assertEquals("\"1001\"", r.getString("_id"));
            r = rows.next();
            assertEquals("\"1002\"", r.getString("_id"));
            assertFalse(rows.hasNext());

            /* find with array OVERLAPS array with orderBy */
            docs = this.collection.find("[2147483648, 2147483648, 2147483649] overlaps $.ARR1").orderBy("_id").execute();
            doc = docs.next();
            assertEquals("1000", (((JsonString) doc.get("_id")).getString()));
            doc = docs.next();
            assertEquals("1001", (((JsonString) doc.get("_id")).getString()));
            doc = docs.next();
            assertEquals("1002", (((JsonString) doc.get("_id")).getString()));
            assertFalse(docs.hasNext());

            /* */
            docs = this.collection.find("[!false && true] OVERLAPS [true]").execute();
            assertEquals(maxrec, docs.count());

            /* Overlaps with NULL */
            docs = this.collection.find("NULL overlaps $.ARR1").execute();
            assertFalse(docs.hasNext());

            rows = table.select("doc->$._id as _id").where("NULL overlaps $.ARR1").execute();
            assertFalse(rows.hasNext());

            docs = this.collection.find("$.ARR1 overlaps null").execute();
            assertFalse(docs.hasNext());

            rows = table.select("doc->$._id as _id").where("$.ARR1 overlaps NULL").execute();
            assertFalse(rows.hasNext());

            /* Not Overlaps with NULL */
            docs = this.collection.find("NULL not overlaps $.ARR1").execute();
            assertTrue(docs.hasNext());
            assertEquals(maxrec, docs.count());

            rows = table.select("doc->$._id as _id").where("NULL not overlaps $.ARR1").execute();
            assertTrue(rows.hasNext());
            assertEquals(maxrec, docs.count());

            docs = this.collection.find("$.ARR1 not overlaps null").execute();
            assertTrue(docs.hasNext());
            assertEquals(maxrec, docs.count());

            rows = table.select("doc->$._id as _id").where("$.ARR1 not overlaps null").execute();
            assertTrue(rows.hasNext());
            assertEquals(maxrec, docs.count());

            /* Test OVERLAPS/NOT OVERLAPS with empty array - Expected to pass, though the array is empty but still valid */
            docs = this.collection.find("[] Overlaps $.ARR1").execute(); //checking the case insensitivity as well
            assertFalse(docs.hasNext());

            rows = table.select().where("[] ovErlaps $.ARR1").execute();
            assertFalse(rows.hasNext());

            docs = this.collection.find("$.ARR1 overlapS []").execute(); //checking the case insensitivity as well
            assertFalse(docs.hasNext());

            rows = table.select().where("$.ARR1 ovErlaps []").execute();
            assertFalse(rows.hasNext());

            docs = this.collection.find("[] not overlaps $.ARR1").execute();
            assertTrue(docs.hasNext());
            assertEquals(maxrec, docs.count());

            rows = table.select().where("[] not overlaps $.ARR1").execute();
            assertTrue(rows.hasNext());
            assertEquals(maxrec, docs.count());

            docs = this.collection.find("$.ARR1 not oveRlaps []").execute(); //checking the case insensitivity as well
            assertTrue(docs.hasNext());
            assertEquals(maxrec, docs.count());

            rows = table.select().where("$.ARR1 not overlaps []").execute();
            assertTrue(rows.hasNext());
            assertEquals(maxrec, docs.count());

            /* When the right number of operands are not provided - error should be thrown */
            assertThrows(WrongArgumentException.class, "Cannot find atomic expression at token pos: 0",
                    () -> this.collection.find("overlaps $.ARR1").execute());
            assertThrows(WrongArgumentException.class, "No more tokens when expecting one at token pos 4",
                    () -> this.collection.find("$.ARR1 OVERLAPS").execute());
            assertThrows(WrongArgumentException.class, "Cannot find atomic expression at token pos: 0", () -> this.collection.find("OVERLAPS").execute());
            assertThrows(WrongArgumentException.class, "Cannot find atomic expression at token pos: 1",
                    () -> this.collection.find("not overlaps $.ARR1").execute());
            assertThrows(WrongArgumentException.class, "No more tokens when expecting one at token pos 5",
                    () -> this.collection.find("$.ARR1 NOT OVERLAPS").execute());
            assertThrows(WrongArgumentException.class, "Cannot find atomic expression at token pos: 1", () -> this.collection.find("not OVERLAPS").execute());

            final Table table1 = this.schema.getCollectionAsTable(this.collectionName);

            assertThrows(WrongArgumentException.class, "Cannot find atomic expression at token pos: 0",
                    () -> table1.select().where("overlaps $.ARR1").execute());
            assertThrows(WrongArgumentException.class, "No more tokens when expecting one at token pos 4",
                    () -> table1.select().where("$.ARR1 OVERLAPS").execute());
            assertThrows(WrongArgumentException.class, "Cannot find atomic expression at token pos: 0", () -> table1.select().where("OVERLAPS").execute());
            assertThrows(WrongArgumentException.class, "Cannot find atomic expression at token pos: 1",
                    () -> table1.select().where("not overlaps $.ARR1").execute());
            assertThrows(WrongArgumentException.class, "No more tokens when expecting one at token pos 5",
                    () -> table1.select().where("$.ARR1 NOT OVERLAPS").execute());
            assertThrows(WrongArgumentException.class, "Cannot find atomic expression at token pos: 1", () -> table1.select().where("not OVERLAPS").execute());

            /* invalid criteria, e.g. .find("[1, 2, 3] OVERLAPS $.age") . where $.age is atomic value */
            dropCollection("coll2");
            Collection coll2 = this.schema.createCollection("coll2", true);
            coll2.add("{ \"_id\": \"1\", \"name\": \"nonjson\", \"age\": \"50\",\"arrayField\":[1,[7]]}").execute();
            //The below command should give exception, but X-plugin doesn't return any error
            docs = coll2.find("[1,2,3] overlaps $.age").execute();
            assertEquals(0, docs.count());
            docs = coll2.find("arrayField OVERLAPS [7]").execute();
            assertEquals(0, docs.count());

            docs = coll2.find("arrayField[1] OVERLAPS [7]").execute();
            assertEquals(1, docs.count());

            table = this.schema.getCollectionAsTable("coll2");
            rows = table.select().where("[1,2,3] overlaps $.age").execute();
            assertEquals(0, rows.count());

            /* Test with empty spaces */
            dropCollection("coll3");
            Collection coll3 = this.schema.createCollection("coll3", true);
            coll3.add("{ \"_id\":1, \"name\": \"Record1\",\"list\":[\"\"], \"age\":15, \"intList\":[1,2,3] }").execute();//List contains an array without any space
            coll3.add("{ \"_id\":2, \"name\": \"overlaps\",\"list\":[\" \"],\"age\":24}").execute();//List contains an array with space
            coll3.add("{ \"_id\":3, \"overlaps\": \"overlaps\",\"age\":30}").execute();
            docs = coll3.find("[''] OVERLAPS $.list").execute();
            assertEquals(1, docs.count());
            assertEquals(new Integer(1), ((JsonNumber) docs.next().get("_id")).getInteger());

            table = this.schema.getCollectionAsTable("coll3");
            rows = table.select("doc->$._id as _id").where("[''] overlaps $.list").execute();
            r = rows.next();
            assertEquals("1", r.getString("_id"));

            docs = coll3.find("[' '] OVERLAPS $.list").execute();
            assertEquals(1, docs.count());
            assertEquals(new Integer(2), ((JsonNumber) docs.next().get("_id")).getInteger());

            rows = table.select("doc->$._id as _id").where("[' '] overlaps $.list").execute();
            r = rows.next();
            assertEquals("2", r.getString("_id"));

            docs = coll3.find("'overlaps' OVERLAPS $.name").execute();
            assertEquals(1, docs.count());
            assertEquals(new Integer(2), ((JsonNumber) docs.next().get("_id")).getInteger());

            rows = table.select("doc->$._id as _id").where("'overlaps' overlaps $.name").execute();
            r = rows.next();
            assertEquals(1, docs.count());
            assertEquals("2", r.getString("_id"));

            docs = coll3.find("[3] OVERLAPS $.intList").execute();
            assertEquals(1, docs.count());

            rows = table.select().where("[3] overlaps $.intList").execute();
            assertEquals(1, rows.count());

            /* Escape the keyword, to use it as identifier */
            docs = coll3.find("`overlaps` OVERLAPS $.`overlaps`").execute();
            assertEquals(1, docs.count());

            rows = table.select().where("'overlaps' overlaps $.`overlaps`").execute();
            assertEquals(1, rows.count());

            docs = coll3.find("$.`overlaps` OVERLAPS `overlaps`").execute();
            assertEquals(1, docs.count());

            rows = table.select().where("$.`overlaps` overlaps 'overlaps'").execute();
            assertEquals(1, rows.count());

            dropCollection("coll4");
            Collection coll4 = this.schema.createCollection("coll4", true);
            coll4.add("{\"overlaps\":{\"one\":1, \"two\":2, \"three\":3},\"list\":{\"one\":1, \"two\":2, \"three\":3},\"name\":\"one\"}").execute();
            coll4.add("{\"overlaps\":{\"one\":1, \"two\":2, \"three\":3},\"list\":{\"four\":4, \"five\":5, \"six\":6},\"name\":\"two\"}").execute();
            coll4.add("{\"overlaps\":{\"one\":1, \"three\":3, \"five\":5},\"list\":{\"two\":2, \"four\":4, \"six\":6},\"name\":\"three\"}").execute();
            coll4.add("{\"overlaps\":{\"one\":1, \"three\":3, \"five\":5},\"list\":{\"three\":3, \"six\":9, \"nine\":9},\"name\":\"four\"}").execute();
            coll4.add("{\"overlaps\":{\"one\":1, \"three\":3, \"five\":5},\"list\":{\"three\":6, \"six\":12, \"nine\":18},\"name\":\"five\"}").execute();
            coll4.add("{\"overlaps\":{\"one\":[1,2,3]}, \"list\":{\"one\":[3,4,5]}, \"name\":\"six\"}").execute();
            coll4.add("{\"overlaps\":{\"one\":[1,2,3]}, \"list\":{\"one\":[1,2,3]}, \"name\":\"seven\"}").execute();

            docs = coll4.find("`overlaps` OVERLAPS `list`").execute();
            assertEquals(3, docs.count());
            doc = docs.fetchOne();
            assertEquals("one", (((JsonString) doc.get("name")).getString()));
            doc = docs.fetchOne();
            assertEquals("four", (((JsonString) doc.get("name")).getString()));
            doc = docs.fetchOne();
            assertEquals("seven", (((JsonString) doc.get("name")).getString()));

            table = this.schema.getCollectionAsTable("coll4");
            rows = table.select("doc->$.name as name").where("$.`overlaps` OVERLAPS $.`list`").execute();
            assertEquals(3, rows.count());
            r = rows.next();
            assertEquals("\"one\"", r.getString("name"));
        } finally {
            dropCollection("coll4");
            dropCollection("coll3");
            dropCollection("coll2");
        }
    }

    @Test
    public void testCollectionFindOverlapsWithExpr() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        DbDoc doc = null;
        DocResult docs = null;

        this.collection.add("{\"_id\": \"1001\", \"ARR\":[1,1,2], \"ARR1\":[\"name1\", \"name2\", \"name3\"]}").execute();
        this.collection.add("{\"_id\": \"1002\", \"ARR\":[1,2,3], \"ARR1\":[\"name4\", \"name5\", \"name6\"]}").execute();
        this.collection.add("{\"_id\": \"1003\", \"ARR\":[1,4,5], \"ARR1\":[\"name1\", \"name1\", \"name5\"]}").execute();

        docs = this.collection.find("[1,1,3] overlaps $.ARR").execute();
        doc = docs.next();
        assertEquals("1001", (((JsonString) doc.get("_id")).getString()));
        doc = docs.next();
        assertEquals("1002", (((JsonString) doc.get("_id")).getString()));
        doc = docs.next();
        assertEquals("1003", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("[2,5] overlaps $.ARR").execute();
        doc = docs.next();
        assertEquals("1001", (((JsonString) doc.get("_id")).getString()));
        doc = docs.next();
        assertEquals("1002", (((JsonString) doc.get("_id")).getString()));
        doc = docs.next();
        assertEquals("1003", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("1 overlaps [1, 2, 3]").execute();
        assertEquals(3, docs.count());

        docs = this.collection.find("'name6' overlaps ['name1', 'name2', 'name6']").execute();
        assertEquals(3, docs.count());

        docs = this.collection.find("cast((1+6) AS JSON) OVERLAPS [2,3,7]").execute();
        assertEquals(3, docs.count());

        docs = this.collection.find("[(1+2)] overlaps [1, 2, 3]").execute();
        assertEquals(3, docs.count());

        docs = this.collection.find("[concat('name', '6')] overlaps ['name1', 'name2', 'name6']").execute();
        assertEquals(3, docs.count());

        docs = this.collection.find("[CURDATE()] overlaps ['2019-16-05','2018-16-05']").execute();
        assertEquals(0, docs.count());

        docs = this.collection.find("true overlaps [2]").execute();
        assertEquals(0, docs.count());

        assertThrows(XProtocolError.class, "ERROR 5154 \\(HY000\\) OVERLAPS expression requires operator that produce a JSON value\\.",
                () -> this.collection.find("[1,2,3] overlaps $.ARR overlaps [2]").execute());

        assertThrows(XProtocolError.class, "ERROR 5154 \\(HY000\\) OVERLAPS expression requires operator that produce a JSON value\\.",
                () -> this.collection.find("(1+2) overlaps [1, 2, 3]").execute());

        assertThrows(XProtocolError.class, "ERROR 5154 \\(HY000\\) OVERLAPS expression requires function that produce a JSON value\\.",
                () -> this.collection.find("concat('name', '6') overlaps ['name1', 'name2', 'name6']").execute());

        assertThrows(XProtocolError.class, "ERROR 5154 \\(HY000\\) OVERLAPS expression requires operator that produce a JSON value\\.",
                () -> this.collection.find("(1+2) overlaps $.ARR").execute());

        assertThrows(XProtocolError.class, "ERROR 5154 \\(HY000\\) OVERLAPS expression requires function that produce a JSON value\\.",
                () -> this.collection.find("concat('name', '6') overlaps $.ARR1").execute());

        Table tabNew = this.schema.getCollectionAsTable(this.collectionName);
        RowResult rows = tabNew.select("doc->$._id as _id").where("[(1+2)] overlaps [1, 2, 3]").execute();
        rows.next();
        assertEquals(3, rows.count());

        rows = tabNew.select("doc->$._id as _id").where("[concat('name', '6')] overlaps ['name1', 'name2', 'name6']").execute();
        rows.next();
        assertEquals(3, rows.count());

        assertThrows(XProtocolError.class, "ERROR 5154 \\(HY000\\) OVERLAPS expression requires function that produce a JSON value\\.",
                () -> tabNew.select("doc->$._id as _id").where("expr(1+2) overlaps [1, 2, 3]").execute());

        assertThrows(XProtocolError.class, "ERROR 5154 \\(HY000\\) OVERLAPS expression requires operator that produce a JSON value\\.",
                () -> tabNew.select("doc->$._id as _id").where("(1+2) overlaps doc->$.ARR").execute());
    }

    @Test
    public void testCollectionFindOverlapsValidMix() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, j = 0, maxrec = 8, minArraySize = 3;
        DbDoc doc = null;
        DocResult docs = null;

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i + 1)));

            JsonArray jarray = new JsonArray();
            for (j = 0; j < (minArraySize + i); j++) {
                jarray.addValue(new JsonString().setValue("Field-1-Data-" + i));
            }
            newDoc2.add("ARR1", jarray);

            this.collection.add(newDoc2).execute();
            newDoc2 = null;
            jarray = null;
        }

        assertEquals((maxrec), this.collection.count());

        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1100)));
            newDoc2.add("ARR1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonString().setValue("10-15-201" + i));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        /* find with OVERLAPS for key having mixture of value and array */
        docs = this.collection.find("\"10-15-2017\" OVERLAPS $.F2").execute();
        doc = docs.next();
        assertEquals("1107", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("JSON_TYPE($.ARR1) = 'ARRAY' AND \"Field-1-Data-0\" OVERLAPS $.ARR1").execute();
        doc = docs.next();
        assertEquals("1000", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with NULL OVERLAPS in key having mix of array and string */
        docs = this.collection.find("NULL OVERLAPS $.ARR1").execute();
        assertFalse(docs.hasNext());
    }

    @Test
    public void testCollModifyTabUpdateWithOverlaps() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, maxrec = 10;
        DbDoc doc = null;
        DocResult docs = null;
        Result res = null;

        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(10 * (i + 1) + 0.1234)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(i + 1)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(10000 - i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        /* find without Condition */
        docs = this.collection.find().fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3,$.F2/10 as tmp1,1/2 as tmp2").orderBy("$.F3").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            i++;
        }
        assertEquals((maxrec), i);

        /* modify with single OVERLAPS */
        res = this.collection.modify("'1001' overlaps $._id").set("$.F1", "Data_New").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("'1001' overlaps $._id").execute();
        doc = docs.next();
        assertEquals("Data_New", (((JsonString) doc.get("F1")).getString()));
        assertFalse(docs.hasNext());

        /* find with = Condition and fetchAll() keyword */
        String findCond = "";
        for (i = 1; i < maxrec; i++) {
            findCond = findCond + "'";
            findCond = findCond + String.valueOf(i + 1000) + "' not overlaps $._id";
            if (i != maxrec - 1) {
                findCond = findCond + " and ";
            }
        }

        /* modify with multiple overlaps */
        res = this.collection.modify(findCond).set("$.F1", "Data_New_1").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find(findCond).execute();
        doc = docs.next();
        assertEquals("Data_New_1", (((JsonString) doc.get("F1")).getString()));
        assertFalse(docs.hasNext());

        /* modify with single overlaps and sort */
        res = this.collection.modify("10000 overlaps $.F4").set("$.F1", "Data_New_2").sort("CAST($.F4 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("10000 overlaps $.F4").orderBy("CAST($.F4 as SIGNED)").execute();
        doc = docs.next();
        assertEquals("Data_New_2", (((JsonString) doc.get("F1")).getString()));
        assertFalse(docs.hasNext());

        /* modify with single overlaps and sort */
        res = this.collection.modify("20.1234 overlaps $.F2").set("$.F1", "Data_New_3").sort("CAST($.F4 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("20.1234 overlaps $.F2").orderBy("CAST($.F4 as SIGNED)").execute();
        doc = docs.next();
        assertEquals("Data_New_3", (((JsonString) doc.get("F1")).getString()));
        assertFalse(docs.hasNext());

        Table table = this.schema.getCollectionAsTable(this.collectionName);

        /* update with single OVERLAPS */
        String toUpdate = String.format("JSON_REPLACE(doc, \"$.F1\", \"Data_New_4\")");
        res = table.update().set("doc", expr(toUpdate)).where("'1001' overlaps doc->$._id").execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        RowResult rows = table.select("doc->$.F1 as F1").where("'1001' overlaps doc->$._id").execute();
        Row r = rows.next();
        assertEquals(r.getString("F1"), "\"Data_New_4\"");
        assertFalse(rows.hasNext());

        findCond = "";
        for (i = 1; i < maxrec; i++) {
            findCond = findCond + "'";
            findCond = findCond + String.valueOf(i + 1000) + "' not overlaps doc->$._id";
            if (i != maxrec - 1) {
                findCond = findCond + " and ";
            }
        }

        /* update with multiple OVERLAPS */
        toUpdate = String.format("JSON_REPLACE(doc, \"$.F1\", \"Data_New_5\")");
        res = table.update().set("doc", expr(toUpdate)).where(findCond).execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        rows = table.select("doc->$.F1 as F1").where(findCond).execute();
        r = rows.next();
        assertEquals(r.getString("F1"), "\"Data_New_5\"");
        assertFalse(rows.hasNext());

        /* update with single OVERLAPS for float */
        toUpdate = String.format("JSON_REPLACE(doc, \"$.F1\", \"Data_New_6\")");
        res = table.update().set("doc", expr(toUpdate)).where("20.1234 overlaps doc->$.F2").execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        rows = table.select("doc->$.F1 as F1").where("20.1234 overlaps doc->$.F2").execute();
        r = rows.next();
        assertEquals(r.getString("F1"), "\"Data_New_6\"");
        assertFalse(rows.hasNext());

        /* update with single OVERLAPS for int */
        toUpdate = String.format("JSON_REPLACE(doc, \"$.F1\", \"Data_New_7\")");
        res = table.update().set("doc", expr(toUpdate)).where("10000 overlaps doc->$.F4").execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        rows = table.select("doc->$.F1 as F1").where("10000 overlaps doc->$.F4").execute();
        r = rows.next();
        assertEquals(r.getString("F1"), "\"Data_New_7\"");
        assertFalse(rows.hasNext());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCollRemoveTabDeleteWithOverlaps() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, maxrec = 10;
        DocResult docs = null;
        Result res = null;

        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(10 * (i + 1) + 0.1234)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(i + 1)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(10000 + i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        /* find without Condition */
        docs = this.collection.find().fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3,$.F2/10 as tmp1,1/2 as tmp2").orderBy("$.F3").execute();
        i = 0;
        while (docs.hasNext()) {
            docs.next();
            i++;
        }
        assertEquals((maxrec), i);

        /* remove with single OVERLAPS */
        res = this.collection.remove("'1001' overlaps $._id").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("'1001' overlaps $._id").execute();
        assertFalse(docs.hasNext());

        /* remove with mulltiple OVERLAPS */
        String findCond = "";
        for (i = 1; i < maxrec; i++) {
            findCond = findCond + "'";
            findCond = findCond + String.valueOf(i + 1000) + "' not overlaps $._id";
            if (i != maxrec - 1) {
                findCond = findCond + " and ";
            }
        }

        res = this.collection.remove(findCond).execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find(findCond).execute();
        assertFalse(docs.hasNext());

        /* remove with single OVERLAPS */
        res = this.collection.remove("10004 overlaps $.F4").orderBy("CAST($.F4 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("10004 overlaps $.F4").orderBy("CAST($.F4 as SIGNED)").execute();
        assertFalse(docs.hasNext());

        /* remove with single OVERLAPS for float */
        res = this.collection.remove("30.1234 overlaps $.F2").orderBy("CAST($.F4 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("30.1234 overlaps $.F2").orderBy("CAST($.F4 as SIGNED)").execute();
        assertFalse(docs.hasNext());

        res = this.collection.remove("true").execute();

        jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(10 * (i + 1) + 0.1234)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(i + 1)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(10000 + i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        Table table = this.schema.getCollectionAsTable(this.collectionName);

        /* delete with single OVERLAPS */
        res = table.delete().where("'1001' overlaps doc->$._id").execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        RowResult rows = table.select("doc->$.F1 as _id").where("'1001' overlaps doc->$._id").execute();
        assertFalse(rows.hasNext());

        /* delete with multiple OVERLAPS */
        findCond = "";
        for (i = 1; i < maxrec; i++) {
            findCond = findCond + "'";
            findCond = findCond + String.valueOf(i + 1000) + "' not overlaps doc->$._id";
            if (i != maxrec - 1) {
                findCond = findCond + " and ";
            }
        }

        res = table.delete().where(findCond).execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        rows = table.select("doc->$.F1 as _id").where(findCond).execute();
        assertFalse(rows.hasNext());

        /* delete with single OVERLAPS for float */
        res = table.delete().where("30.1234 overlaps doc->$.F2").execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        rows = table.select("doc->$.F1 as F1").where("30.1234 overlaps doc->$.F2").execute();
        assertFalse(rows.hasNext());

        /* delete with single OVERLAPS for int */
        res = table.delete().where("10004 overlaps doc->$.F4").execute();
        assertEquals(res.getAffectedItemsCount(), 1);
        rows = table.select("doc->$.F1 as F1").where("10004 overlaps doc->$.F4").execute();
        assertFalse(rows.hasNext());
    }

    /* 64k length key */
    @Test
    public void testCollectionFindStress_002() throws Exception {
        int i = 0, maxrec = 5;
        int maxLen = 1024 * 64 - 1;
        SqlResult res1 = null;
        Session tmpSess = null;
        Row r = null;
        int defXPackLen = 0;
        int defPackLen = 0;
        try {
            tmpSess = new SessionFactory().getSession(this.baseUrl);
            res1 = tmpSess.sql("show variables like 'mysqlx_max_allowed_packet'").execute();
            r = res1.next();
            defXPackLen = Integer.parseInt(r.getString("Value"));
            res1 = tmpSess.sql("show variables like 'max_allowed_packet'").execute();
            r = res1.next();
            defPackLen = Integer.parseInt(r.getString("Value"));

            tmpSess.sql("set Global mysqlx_max_allowed_packet=128*1024*1024 ").execute();
            tmpSess.sql("set Global max_allowed_packet=128*1024*1024 ").execute();

            String s1 = "";

            /* max+1 key length --> Expect error */
            s1 = buildString(maxLen + 1, 'q');
            DbDoc[] jsonlist = new DbDocImpl[maxrec];
            for (i = 0; i < maxrec; i++) {
                DbDoc newDoc2 = new DbDocImpl();
                newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1 + 1000)));
                newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i + 1)));
                newDoc2.add(s1, new JsonString().setValue("Data_1" + i));
                jsonlist[i] = newDoc2;
                newDoc2 = null;
            }

            assertThrows(XProtocolError.class, "ERROR 3151 \\(22032\\) The JSON object contains a key name that is too long\\.",
                    () -> this.collection.add(jsonlist).execute());

            /* With Max Keysize */
            s1 = buildString(maxLen - 1, 'q');
            for (i = 0; i < maxrec; i++) {
                DbDoc newDoc2 = new DbDocImpl();
                newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1 + 1000)));
                newDoc2.add(s1 + "1", new JsonNumber().setValue(String.valueOf(i + 1)));
                newDoc2.add(s1 + "2", new JsonString().setValue("Data_1" + i));
                //jsonlist[i]=newDoc2;
                this.collection.add(newDoc2).execute();
                newDoc2 = null;
            }
            //coll.add(jsonlist).execute();

            DocResult docs0 = this.collection.find("$._id= '1001'").fields("$._id as _id, $." + s1 + "1 as " + s1 + "X, $." + s1 + "2 as " + s1 + "Y")
                    .execute();
            DbDoc doc0 = docs0.next();
            assertEquals(String.valueOf(1 + 1000), (((JsonString) doc0.get("_id")).getString()));

        } finally {
            if (tmpSess != null) {
                tmpSess.sql("set Global mysqlx_max_allowed_packet=" + defXPackLen).execute();
                tmpSess.sql("set Global max_allowed_packet=" + defPackLen).execute();
                tmpSess.close();
            }
        }
    }

    /* Large Data */
    @Test
    //@Ignore("Wait for 1M Data issue Fix in Plugin")
    public void testCollectionFindStress_003() throws Exception {
        int i = 0, maxrec = 5;
        int maxLen = 1024 * 1024 + 4;
        SqlResult res1 = null;
        Session tmpSess = null;
        Row r = null;
        int defPackLen = 0;
        int defXPackLen = 0;

        try {
            tmpSess = new SessionFactory().getSession(this.baseUrl);
            res1 = tmpSess.sql("show variables like 'mysqlx_max_allowed_packet'").execute();
            r = res1.next();
            defXPackLen = Integer.parseInt(r.getString("Value"));

            res1 = tmpSess.sql("show variables like 'max_allowed_packet'").execute();
            r = res1.next();
            defPackLen = Integer.parseInt(r.getString("Value"));

            tmpSess.sql("set Global mysqlx_max_allowed_packet=128*1024*1024 ").execute();
            tmpSess.sql("set Global max_allowed_packet=128*1024*1024 ").execute();
            ((SessionImpl) this.session).getSession().getProtocol().setMaxAllowedPacket(128 * 1024 * 1024);

            String s1 = "";

            /* maxLen Data length */
            s1 = buildString(maxLen + 1, 'q');
            for (i = 0; i < maxrec; i++) {
                DbDoc newDoc2 = new DbDocImpl();
                newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1 + 1000)));
                newDoc2.add("F1", new JsonString().setValue(s1 + i));
                newDoc2.add("F2", new JsonString().setValue(s1 + i));
                this.collection.add(newDoc2).execute();
                newDoc2 = null;
            }

            DocResult docs0 = this.collection.find("$._id= '1001'").fields("$._id as _id, $.F1 as f1, $.F2 as f2").execute();
            DbDoc doc0 = docs0.next();
            assertEquals(String.valueOf(1 + 1000), (((JsonString) doc0.get("_id")).getString()));
        } finally {
            if (tmpSess != null) {
                tmpSess.sql("set Global mysqlx_max_allowed_packet=" + defXPackLen).execute();
                tmpSess.sql("set Global max_allowed_packet=" + defPackLen).execute();
                tmpSess.close();
            }
        }
    }

    /*
     * Many Keys
     * Issue : Hangs when maxKey > 7K
     */
    @Test
    public void testCollectionFindStress_004() throws Exception {
        int i = 0, j = 0, maxrec = 5;
        int maxKey = 1024 * 8;
        String key, key_sub = "key_", query;
        String data, data_sub = "data_";
        DocResult docs = null;
        DbDoc doc = null;
        String json = "";
        for (i = 0; i < maxrec; i++) {
            StringBuilder b = new StringBuilder("{'_id':'" + (1000 + i) + "'");
            for (j = 0; j < maxKey; j++) {
                key = key_sub + j;
                data = data_sub + i + "_" + j;
                b.append(",'").append(key).append("':'").append(data).append("'");
            }

            json = b.append("}").toString();
            json = json.replaceAll("'", "\"");
            this.collection.add(json).execute();
            b = null;
        }

        /* Inserting maxKey (key,data) pair */
        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1 + 2000)));
            for (j = 0; j < maxKey; j++) {
                key = key_sub + j;
                data = data_sub + i + "_" + j;
                newDoc2.add(key, new JsonString().setValue(data));
            }

            this.collection.add(newDoc2).execute();
            newDoc2 = null;
        }

        assertEquals((maxrec * 2), this.collection.count());
        /* Select All Keys */
        query = "$._id as _id";
        for (j = 0; j < maxKey; j++) {
            key = key_sub + j;
            query = query + ",$." + key + " as " + key;
        }
        docs = this.collection.find("$._id= '1001'").fields(query).orderBy(key_sub + (maxKey - 1)).execute();
        doc = docs.next();
        assertEquals(String.valueOf(1001), (((JsonString) doc.get("_id")).getString()));
        assertEquals(data_sub + "1_" + (maxKey - 1), (((JsonString) doc.get(key_sub + (maxKey - 1))).getString()));
    }

    /**
     * Bigint,Double, Date data CAST Operator
     * 
     * @throws Exception
     */
    @Test
    public void testCollectionFindDatatypes() throws Exception {
        int i = 0, maxrec = 10;
        DbDoc doc = null;
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
            newDoc2.add("F6", new JsonString().setValue((2000 + i) + "-02-" + (i * 2 + 10)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        /* Compare Big Int */
        DocResult docs = this.collection.find("CAST($.F3 as SIGNED) =" + l1).fields("$._id as _id, $.F3 as f3, $.F3 as f3").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(l1, (((JsonNumber) doc.get("f3")).getBigDecimal().longValue()));
            i++;
        }
        assertEquals((1), i);

        /* Compare Big Int */
        docs = this.collection.find("CAST($.F5 as SIGNED) =" + l3 + "+" + 3).fields("$._id as _id, $.F1 as f1, $.F5 as f5").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(l3 + 3, (((JsonNumber) doc.get("f5")).getBigDecimal().longValue()));
            i++;
        }
        assertEquals((1), i);

        /* CAST in Order By */
        docs = this.collection.find("CAST($.F5 as SIGNED) < " + l3 + "+" + 5).fields("$._id as _id, $.F1 as f1, $.F5 as f5").orderBy("CAST($.F5 as SIGNED) asc")
                .execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(l3 + i, (((JsonNumber) doc.get("f5")).getBigDecimal().longValue()));
            i++;
        }
        assertEquals((5), i);

        docs = this.collection.find("CAST($.F4 as SIGNED) < " + l2 + "+" + 5).fields("$._id as _id, $.F1 as f1, $.F4 as f4")
                .orderBy("CAST($.F4 as SIGNED) desc").execute();
        i = 4;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(l2 + i, (((JsonNumber) doc.get("f4")).getBigDecimal().longValue()));
            i--;
        }
        assertEquals((-1), i);

        /* Compare Double */
        docs = this.collection.find("CAST($.F2 as DECIMAL(10,4)) =" + d1).fields("$._id as _id, $.F1 as f1, $.F2 as f2").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(d1, (((JsonNumber) doc.get("f2")).getBigDecimal().doubleValue()));
            i++;
        }
        assertEquals((1), i);
    }

    /* OPerators =,!=,<,>,<=,>= IN, NOT IN,Like , Not Like, Between, REGEXP,NOT REGEXP , interval,|,&,^,<<,>>,~ */
    @Test
    public void testCollectionFindBasic() throws Exception {
        int i = 0, maxrec = 10;
        DbDoc doc = null;
        DocResult docs = null;

        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(10 * (i + 1))));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(i + 1)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(10000 - i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        /* find without Condition */
        docs = this.collection.find().fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3,$.F2/10 as tmp1,1/2 as tmp2").orderBy("$.F3").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals((long) (i + 1), (long) (((JsonNumber) doc.get("f3")).getInteger()));
            assertEquals((((JsonNumber) doc.get("f3")).getInteger()), (((JsonNumber) doc.get("tmp1")).getInteger()));
            assertEquals(new BigDecimal("0.500000000"), (((JsonNumber) doc.get("tmp2")).getBigDecimal()));

            i++;
        }
        assertEquals((maxrec), i);

        /* find with = Condition and fetchAll() keyword */
        docs = this.collection.find("$._id = '1001'").execute();
        doc = docs.next();
        assertEquals("1001", (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with order by and condition */
        docs = this.collection.find("$.F3 > 1").orderBy("CAST($.F4 as SIGNED)").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            i++;
            assertEquals(String.valueOf(1000 + maxrec - i), (((JsonString) doc.get("_id")).getString()));
            assertEquals((long) (10000 - maxrec + i), (long) (((JsonNumber) doc.get("F4")).getInteger()));

        }
        assertEquals(i, (maxrec - 1));

        /* find with order by and limit with condition */
        docs = this.collection.find("$._id > 1001").orderBy("CAST($.F4 as SIGNED)").limit(1).execute();
        doc = docs.next();
        assertEquals(String.valueOf(1000 + maxrec - 1), (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());

        /* find with order by limit and offset with condition */
        docs = this.collection.find("$.F3 > 2").orderBy("CAST($.F4 as SIGNED)").fields("$._id as _id, $.F2/$.F3 as f1,$.F4 as f4").limit(10).offset(2)
                .execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            i++;
            assertEquals(String.valueOf(1000 + maxrec - i - 2), (((JsonString) doc.get("_id")).getString()));
            assertEquals((long) (10000 - maxrec + i + 2), (long) (((JsonNumber) doc.get("f4")).getInteger()));
            assertEquals((long) 10, (long) (((JsonNumber) doc.get("f1")).getInteger()));
        }
        assertEquals(i, (maxrec - 4));
    }

    @Test
    public void testCollectionFindGroupBy() throws Exception {
        int i = 0, j = 0, maxrec = 10, grpcnt = 50;
        DbDoc doc = null;

        DbDoc[] jsonlist = new DbDocImpl[maxrec];
        for (j = 1; j <= grpcnt; j++) {
            for (i = 0; i < maxrec; i++) {
                DbDoc newDoc2 = new DbDocImpl();
                newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + (1000 * j))));
                newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(10 * (i + 1))));
                newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(i + 1)));
                newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(100 * (i + 1))));
                newDoc2.add("F4", new JsonString().setValue(buildString(8192 + i, 'X')));
                jsonlist[i] = newDoc2;
                newDoc2 = null;
            }
            this.collection.add(jsonlist).execute();
        }
        assertEquals((maxrec * grpcnt), this.collection.count());

        /* find with groupBy (Sum) with Having Clause */
        DocResult docs = this.collection.find().fields("sum($.F1) as sum_f1, sum($.F2) as sum_f2, sum($.F3) as sum_f3,Max($.F4) as max_f4 ").groupBy("$.F1")
                .having("MIn($.F1) > 10").orderBy("sum($.F1)").execute();
        i = 1;
        while (docs.hasNext()) {
            i++;
            doc = docs.next();
            assertEquals((long) (i * grpcnt * 10), (long) (((JsonNumber) doc.get("sum_f1")).getInteger()));
            assertEquals((long) (i * grpcnt), (long) (((JsonNumber) doc.get("sum_f2")).getInteger()));
            assertEquals((long) (i * grpcnt * 100), (long) (((JsonNumber) doc.get("sum_f3")).getInteger()));
            //assertEquals((buildString(10+i,'X')), (((JsonString) doc.get("max_f4")).getString()));
        }
        assertEquals((maxrec), i);

        /* find with groupBy (Max) with Having Clause on String */
        docs = this.collection.find().fields("max($.F1) as max_f1, max($.F2) as max_f2, max($.F3) as max_f3,max($.F4) as max_f4 ").groupBy("$.F4")
                .having("max($.F1) > 20").execute();
        i = 2;
        while (docs.hasNext()) {
            i++;
            doc = docs.next();
            assertEquals(String.valueOf(i * 10), (((JsonString) doc.get("max_f1")).getString()));
            assertEquals(String.valueOf(i), (((JsonString) doc.get("max_f2")).getString()));
            assertEquals(String.valueOf(i * 100), (((JsonString) doc.get("max_f3")).getString()));
            //assertEquals((buildString(10+i,'X')), (((JsonString) doc.get("max_f4")).getString()));
        }
        assertEquals((maxrec), i);

        docs = this.collection.find().fields("max($.F1) as max_f1, max($.F2) as max_f2, max($.F3) as max_f3,max($.F4) as max_f4max_f4").groupBy("$.F4")
                .having("max($.F1) > 20").orderBy("$.F4").execute();
        //docs = coll.find().fields("max($.F4) as max_f4,$.F4 as f4").groupBy("$.F4").having("max($.F1) > 20").orderBy("$.F4").execute();
        i = 2;
        while (docs.hasNext()) {
            i++;
            doc = docs.next();
            assertEquals(String.valueOf(i * 10), (((JsonString) doc.get("max_f1")).getString()));
            assertEquals(String.valueOf(i), (((JsonString) doc.get("max_f2")).getString()));
            assertEquals(String.valueOf(i * 100), (((JsonString) doc.get("max_f3")).getString()));
            //assertEquals((buildString(10+i,'X')), (((JsonString) doc.get("max_f4")).getString()));
        }
        assertEquals((maxrec), i);

        docs = this.collection.find().fields("max($.F1) as max_f1, max($.F2) as max_f2, max($.F3) as max_f3,max($.F4) as max_f4max_f4").groupBy("$.F4")
                .having("max($.F1) > 20").orderBy("$.F4").limit(1).offset(1).execute();
        doc = docs.next();
        assertEquals(String.valueOf(40), (((JsonString) doc.get("max_f1")).getString()));
        assertEquals(String.valueOf(4), (((JsonString) doc.get("max_f2")).getString()));
        assertEquals(String.valueOf(400), (((JsonString) doc.get("max_f3")).getString()));
        assertFalse(docs.hasNext());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCollectionFindSkipWarning() throws Exception {
        int i = 0, maxrec = 10;
        DbDoc doc = null;
        DocResult docs = null;

        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(10 * (i + 1))));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(i + 1)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(10000 - i)));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        /* find with order by and condition */
        docs = this.collection.find("$.F3 > 1").orderBy("CAST($.F4 as SIGNED)").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            i++;
            assertEquals(String.valueOf(1000 + maxrec - i), (((JsonString) doc.get("_id")).getString()));
            assertEquals((long) (10000 - maxrec + i), (long) (((JsonNumber) doc.get("F4")).getInteger()));

        }
        assertEquals(i, (maxrec - 1));

        /* find with order by and limit with condition */
        docs = this.collection.find("$._id > 1001").orderBy("CAST($.F4 as SIGNED)").limit(1).skip(2).execute();

        doc = docs.next();
        assertEquals(String.valueOf(1000 + maxrec - 3), (((JsonString) doc.get("_id")).getString()));
        assertFalse(docs.hasNext());
    }

    /* OPerators =,!=,<,>,<=,>= IN, NOT IN,Like , Not Like, Between, REGEXP,NOT REGEXP , interval,|,&,^,<<,>>,~ */
    /* REGEXP,NOT REGEXP,LIKE, NOT LIKE, */
    @Test
    public void testCollectionFindWithStringComparison() throws Exception {
        int i = 0, maxrec = 10;
        int SLen = 1024;
        DbDoc doc = null;

        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1 + 1000)));
            newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(10 * (i + 1))));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(i + 1)));
            newDoc2.add("F3", new JsonString().setValue(buildString(SLen + i + 1, 'q') + buildString(1 + i, 'X') + buildString(SLen + i + 1, '#')));
            //    newDoc2.add("F3", new JsonString().setValue("?????"));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        /* find With REGEXP Condition */
        DocResult docs = this.collection.find("$.F3  REGEXP 'q'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(buildString(SLen + i + 1, 'q') + buildString(1 + i, 'X') + buildString(SLen + i + 1, '#'), (((JsonString) doc.get("f3")).getString()));
            i++;
        }
        assertEquals((maxrec), i);

        /* find With REGEXP Condition */
        docs = this.collection.find("$.F3 REGEXP 'qXX#'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        doc = docs.next();
        assertEquals(buildString(SLen + 2, 'q') + buildString(2, 'X') + buildString(SLen + 2, '#'), (((JsonString) doc.get("f3")).getString()));
        assertFalse(docs.hasNext());

        /* find With Not REGEXP Condition */
        docs = this.collection.find("$.F3 NOT REGEXP 'qX*#'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        i = 0;
        assertFalse(docs.hasNext());

        /* find With REGEXP Condition */
        docs = this.collection.find("$.F3 REGEXP 'qXXXX#'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        doc = docs.next();
        assertEquals(buildString(SLen + 4, 'q') + buildString(4, 'X') + buildString(SLen + 4, '#'), (((JsonString) doc.get("f3")).getString()));
        assertFalse(docs.hasNext());

        /* find With Like Condition */
        docs = this.collection.find("$.F3  like '%q_X_#%'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").orderBy("CAST($.F2 as SIGNED)")
                .execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(buildString(SLen + i + 1, 'q') + buildString(1 + i, 'X') + buildString(SLen + i + 1, '#'), (((JsonString) doc.get("f3")).getString()));
            i++;
        }
        assertEquals(3, i);

        /* find With Not Like Condition */
        docs = this.collection.find("$.F3 Like '%qX#%'").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").execute();
        doc = docs.next();
        assertEquals(buildString(SLen + 1, 'q') + buildString(1, 'X') + buildString(SLen + 1, '#'), (((JsonString) doc.get("f3")).getString()));
        assertFalse(docs.hasNext());

        /* find With Like and NOT REGEXP Condition */
        docs = this.collection.find("$.F3 NOT REGEXP 'qqX##' and $.F3  like '%q_X_#%' ").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3")
                .orderBy("CAST($.F2 as SIGNED)").execute();
        i = 0;
        while (docs.hasNext()) {
            i++;
            doc = docs.next();
            assertEquals(buildString(SLen + i + 1, 'q') + buildString(1 + i, 'X') + buildString(SLen + i + 1, '#'), (((JsonString) doc.get("f3")).getString()));

        }
        assertEquals(2, i);

        /* find With Like , NOT REGEXP and between Condition */
        docs = this.collection.find("$.F3 NOT REGEXP 'qqX##' and $.F3  like '%q_X_#%' and $.F1 between 21 and 31")
                .fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3").orderBy("CAST($.F2 as SIGNED)").execute();
        doc = docs.next();
        assertEquals(buildString(SLen + 3, 'q') + buildString(3, 'X') + buildString(SLen + 3, '#'), (((JsonString) doc.get("f3")).getString()));
        assertEquals((long) 30, (long) (((JsonNumber) doc.get("f1")).getInteger()));
        assertFalse(docs.hasNext());
    }

    /* |,&,^,<<,>>,~ */
    @Test
    public void testCollectionFindWithBitOperation() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, maxrec = 10;
        int SLen = 1;
        DbDoc doc = null;

        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1 + 1000)));
            newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i + 1)));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf((int) Math.pow(2, (i + 1)))));
            newDoc2.add("F3", new JsonString().setValue(buildString(SLen + i, 'q')));
            //    newDoc2.add("F3", new JsonString().setValue("?????"));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        /* find With bitwise | Condition */
        DocResult docs = this.collection.find("CAST($.F2 as SIGNED) | pow(2,$.F1) = $.F2 ")
                .fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3 , $.F2 | pow(2,$.F1) as tmp").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(String.valueOf(i + 1 + 1000), (((JsonString) doc.get("_id")).getString()));
            assertEquals((long) (i + 1), (long) (((JsonNumber) doc.get("f1")).getInteger()));
            assertEquals((long) ((int) Math.pow(2, (i + 1))), (long) (((JsonNumber) doc.get("f2")).getInteger()));
            assertEquals(buildString(SLen + i, 'q'), (((JsonString) doc.get("f3")).getString()));
            assertEquals((long) Math.pow(2, (i + 1)), (long) (((JsonNumber) doc.get("tmp")).getInteger()));
            i++;
        }
        assertEquals((maxrec), i);

        /* find With bitwise & Condition */
        docs = this.collection.find("CAST($.F2 as SIGNED) & 64 ").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3 , $.F2 & 64 as tmp").execute();
        doc = docs.next();
        assertEquals((long) 64, (long) (((JsonNumber) doc.get("f2")).getInteger()));
        assertEquals((long) 64, (long) (((JsonNumber) doc.get("tmp")).getInteger()));
        assertFalse(docs.hasNext());

        /* find With bitwise | Condition */
        docs = this.collection.find("CAST($.F2 as SIGNED) | $.F1 = 37 ").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3 ,$.F2 | $.F1  as tmp")
                .execute();
        doc = docs.next();
        assertEquals((long) 32, (long) (((JsonNumber) doc.get("f2")).getInteger()));
        assertEquals((long) 37, (long) (((JsonNumber) doc.get("tmp")).getInteger()));
        assertFalse(docs.hasNext());

        /* find With bitwise << Condition */
        docs = this.collection.find("CAST($.F2 as SIGNED) = 1<<4 ").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3 ,  1<<4  as tmp").execute();
        doc = docs.next();
        assertEquals((long) 16, (long) (((JsonNumber) doc.get("f2")).getInteger()));
        assertEquals((long) 16, (long) (((JsonNumber) doc.get("tmp")).getInteger()));
        assertFalse(docs.hasNext());

        /* find With bitwise >> Condition */
        docs = this.collection.find("CAST($.F2 as SIGNED) = 32>>4 ").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3 , 32>>4  as tmp").execute();
        doc = docs.next();
        assertEquals((long) 2, (long) (((JsonNumber) doc.get("f2")).getInteger()));
        assertEquals((long) 2, (long) (((JsonNumber) doc.get("tmp")).getInteger()));
        assertFalse(docs.hasNext());

        /* find With bitwise ^ Condition */
        docs = this.collection.find("CAST($.F2 as SIGNED) ^ 1 = 17").fields("$._id as _id,$.F2 as f2, $.F2 ^ 1 as tmp").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals((long) 16, (long) (((JsonNumber) doc.get("f2")).getInteger()));
            assertEquals((long) 17, (long) (((JsonNumber) doc.get("tmp")).getInteger()));
            i++;
        }
        assertFalse(docs.hasNext());
        this.collection.add("{\"x1\":\"31\", \"x2\":\"13\", \"x3\":\"8\", \"x4\":\"18446744073709551614\"}").execute();

        /* find With bitwise ~ Condition **********FAILING************ */
        docs = this.collection.find("~16 = ~CAST($.F2 as SIGNED)").fields("$._id as _id,$.F2 as f2, ~1 as tmp").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals((long) 16, (long) (((JsonNumber) doc.get("f2")).getInteger()));
            //assertEquals(17, (((JsonNumber) doc.get("tmp")).getInteger()));
            i++;
        }
        assertFalse(docs.hasNext());
    }

    /* interval, In, NOT IN */
    @Test
    public void testCollectionFindWithIntervalOperation() throws Exception {
        int i = 0, maxrec = 10;
        int SLen = 1;
        DbDoc doc = null;

        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1 + 1000)));
            newDoc2.add("dt", new JsonString().setValue((2000 + i) + "-02-" + (i * 2 + 10)));
            newDoc2.add("dtime", new JsonString().setValue((2000 + i) + "-02-01 12:" + (i + 10) + ":01"));
            newDoc2.add("str", new JsonString().setValue(buildString(SLen + i, 'q')));
            newDoc2.add("ival", new JsonNumber().setValue(String.valueOf((int) Math.pow(2, (i + 1)))));
            if (maxrec - 1 == i) {
                newDoc2.add("ival", new JsonNumber().setValue(String.valueOf(-2147483648)));
            }
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();
        assertEquals((maxrec), this.collection.count());

        /* find With bitwise | Condition */
        DocResult docs = this.collection.find("CAST($.ival as SIGNED)>1 ")
                .fields("$._id as _id, $.dt as f1, $.dtime as f2, $.str as f3 , $.ival  as f4,$.dt - interval 25 day as tmp").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(String.valueOf(i + 1 + 1000), (((JsonString) doc.get("_id")).getString()));
            assertEquals(((2000 + i) + "-02-" + (i * 2 + 10)), (((JsonString) doc.get("f1")).getString()));
            assertEquals((2000 + i) + "-02-01 12:" + (i + 10) + ":01", (((JsonString) doc.get("f2")).getString()));
            assertEquals(buildString(SLen + i, 'q'), (((JsonString) doc.get("f3")).getString()));
            assertEquals((long) ((int) Math.pow(2, (i + 1))), (long) (((JsonNumber) doc.get("f4")).getInteger()));
            i++;
        }

        assertEquals((maxrec - 1), i);
        /* find With bitwise interval Condition */
        docs = this.collection.find("$.dt + interval 6 day = '2007-03-02' ").fields("$._id as _id, $.dt as f1 ").execute();
        doc = docs.next();
        assertEquals("2007-02-24", (((JsonString) doc.get("f1")).getString()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("$.dt + interval 1 month = '2006-03-22' ").fields("$._id as _id, $.dt as f1 ").execute();
        doc = docs.next();
        assertEquals("2006-02-22", (((JsonString) doc.get("f1")).getString()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("$.dt + interval 1 year = '2010-02-28' ").fields("$._id as _id, $.dt as f1 ").execute();
        doc = docs.next();
        assertEquals("2009-02-28", (((JsonString) doc.get("f1")).getString()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("$.dt - interval 1 year = '2008-02-28' ").fields("$._id as _id, $.dt as f1 ").execute();
        doc = docs.next();
        assertEquals("2009-02-28", (((JsonString) doc.get("f1")).getString()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("$.dt - interval 25 day = '2007-01-30' ").fields("$._id as _id, $.dt as f1 ").execute();
        doc = docs.next();
        assertEquals("2007-02-24", (((JsonString) doc.get("f1")).getString()));
        assertFalse(docs.hasNext());

        /* Between */
        docs = this.collection.find("CAST($.ival as SIGNED) between 65 and 128 ").fields("$._id as _id, $.ival as f1 ").execute();
        doc = docs.next();
        assertEquals((long) 128, (long) (((JsonNumber) doc.get("f1")).getInteger()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("$.dt between '2006-01-28' and '2007-02-01' ").fields("$._id as _id, $.dt as f1 ").execute();
        doc = docs.next();
        assertEquals("2006-02-22", (((JsonString) doc.get("f1")).getString()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("CAST($.ival as SIGNED) <0 ").fields("$._id as _id, $.ival as f1 ").execute();
        doc = docs.next();
        assertEquals((long) -2147483648, (long) (((JsonNumber) doc.get("f1")).getInteger()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("(CAST($.ival as SIGNED) between 9 and 31) or (CAST($.ival as SIGNED) between 65 and 128)")
                .fields("$._id as _id, $.ival as f1 ").orderBy("CAST($.ival as SIGNED) asc").execute();
        doc = docs.next();
        assertEquals((long) 16, (long) (((JsonNumber) doc.get("f1")).getInteger()));
        doc = docs.next();
        assertEquals((long) 128, (long) (((JsonNumber) doc.get("f1")).getInteger()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("(CAST($.ival as SIGNED) between 9 and 31) and (CAST($.ival as SIGNED) between 65 and 128)")
                .fields("$._id as _id, $.ival as f1 ").orderBy("CAST($.ival as SIGNED)").execute();
        assertFalse(docs.hasNext());

        docs = this.collection.find("CAST($.ival as SIGNED) in (20,NULL,31.5,'17',16,CAST($.ival as SIGNED)+1) ").fields("$._id as _id, $.ival as f1 ")
                .execute();
        doc = docs.next();
        assertEquals((long) 16, (long) (((JsonNumber) doc.get("f1")).getInteger()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("(CAST($.ival as SIGNED) in (16,32,4,256,512)) and ($.dt - interval 25 day = '2007-01-30' ) and  ($.ival not in(2,4))")
                .fields("$._id as _id, $.ival as f1 ").execute();
        doc = docs.next();
        assertEquals((long) 256, (long) (((JsonNumber) doc.get("f1")).getInteger()));
        assertFalse(docs.hasNext());
    }

    /**
     * Issue : in orderBy all values are treated as string
     * : bind() with Map Fails
     * 
     * @throws Exception
     */
    @Test
    public void testCollectionFindWithBind() throws Exception {
        int i = 0, maxrec = 15;
        int SLen = 500;
        DbDoc doc = null;
        DbDoc[] jsonlist = new DbDocImpl[maxrec];

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1 + 1000)));
            newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i + 1)));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf((int) Math.pow(2, (i + 1)))));
            newDoc2.add("F3", new JsonString().setValue(buildString(SLen + i, 'q')));
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();

        assertEquals((maxrec), this.collection.count());

        /* find all */
        DocResult docs = this.collection.find("CAST($.F2 as SIGNED) > ? ").bind(new Object[] { 1 }).fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3")
                .execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(String.valueOf(i + 1 + 1000), (((JsonString) doc.get("_id")).getString()));
            assertEquals((long) (i + 1), (long) (((JsonNumber) doc.get("f1")).getInteger()));
            assertEquals((long) ((int) Math.pow(2, (i + 1))), (long) (((JsonNumber) doc.get("f2")).getInteger()));
            //    assertEquals(buildString(SLen+i,'q'), (((JsonString) doc.get("f3")).getString()));
            i++;
        }
        assertEquals((maxrec), i);

        docs = this.collection.find("CAST($.F2 as SIGNED) = ?").bind(new Object[] { 32 }).execute();
        doc = docs.next();
        assertEquals((long) 32, (long) (((JsonNumber) doc.get("F2")).getInteger()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("CAST($.F2 as SIGNED) between ? and ?").bind(new Object[] { 10, 17 }).execute();
        doc = docs.next();
        assertEquals((long) 16, (long) (((JsonNumber) doc.get("F2")).getInteger()));
        assertFalse(docs.hasNext());

        docs = this.collection.find("CAST($.F2 as SIGNED) in(?,?,?,?,?,?,?,?,?,?,?+1,?-1)").bind(new Object[] { 1, 3, 5, 7, 9, 11, 13, 15, 17, 19, 31, 129 })
                .orderBy("CAST($.F2 as SIGNED)").execute();
        doc = docs.next();
        assertEquals((long) 32, (long) (((JsonNumber) doc.get("F2")).getInteger()));
        doc = docs.next();
        assertEquals((long) 128, (long) (((JsonNumber) doc.get("F2")).getInteger()));
        assertFalse(docs.hasNext());

        Object[] tmp = new Object[maxrec];
        String q = "CAST($.F2 as SIGNED) in(";
        for (i = 0; i < maxrec; i++) {
            if (i > 0) {
                q = q + ",";
            }
            q = q + "?";
            tmp[i] = (int) Math.pow(2, (i + 1));
        }
        q = q + ")";

        docs = this.collection.find(q).bind(tmp).orderBy("CAST($.F2 as SIGNED) asc").execute();
        //    docs= coll.find(q).bind(tmp).orderBy("$.F2").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals((long) Math.pow(2, i + 1), (long) (((JsonNumber) doc.get("F2")).getInteger()));
            i++;
        }
        assertEquals(maxrec, i);
        assertFalse(docs.hasNext());
        //tmp =null;
        tmp = new Object[maxrec];
        q = "$.F3 in(";
        for (i = 0; i < maxrec; i++) {
            if (i > 0) {
                q = q + ",";
            }
            q = q + "?";
            tmp[i] = buildString(SLen + i, 'q');
        }
        q = q + ")";

        docs = this.collection.find(q).bind(tmp).fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3 ").orderBy("$.F3 asc").execute();
        //docs= coll.find("$.F3 not in('dd')").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3 as f3 ").orderBy("$.F3 asc").execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals(String.valueOf(i + 1 + 1000), (((JsonString) doc.get("_id")).getString()));
            assertEquals((long) (i + 1), (long) (((JsonNumber) doc.get("f1")).getInteger()));
            assertEquals((long) ((int) Math.pow(2, (i + 1))), (long) (((JsonNumber) doc.get("f2")).getInteger()));
            i++;
        }
        assertEquals((maxrec), i);

        /* With Map */
        Map<String, Object> params = new HashMap<>();
        params.put("thePlaceholder", 32);
        params.put("thePlaceholder2", 2);
        docs = this.collection.find("CAST($.F2 as SIGNED) = :thePlaceholder or CAST($.F2 as SIGNED) = :thePlaceholder2")
                .fields("$._id as _id, $.F1 as f1, $.F2 as f2").bind(params).orderBy("CAST($.F2 as SIGNED) desc").execute();
        doc = docs.next();
        assertEquals(String.valueOf(1005), (((JsonString) doc.get("_id")).getString()));
        assertEquals((long) (32), (long) (((JsonNumber) doc.get("f2")).getInteger()));
        doc = docs.next();
        assertEquals(String.valueOf(1001), (((JsonString) doc.get("_id")).getString()));
        assertEquals((long) (2), (long) (((JsonNumber) doc.get("f2")).getInteger()));

        assertFalse(docs.hasNext());

        q = "";
        params.clear();
        for (i = 0; i < maxrec; i++) {
            params.put("thePlaceholder" + i, (int) Math.pow(2, (i + 1)));
            if (i > 0) {
                q = q + " or ";
            }
            q = q + "CAST($.F2 as SIGNED) =:thePlaceholder" + i + " ";
        }

        docs = this.collection.find(q).fields("$._id as _id, $.F1 as f1, $.F2 as f2").bind(params).execute();
        i = 0;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals((long) ((int) Math.pow(2, (i + 1))), (long) (((JsonNumber) doc.get("f2")).getInteger()));
            i++;
        }
        assertEquals((maxrec), i);
    }

    @Test
    public void testCollectionFindArray() throws Exception {
        int i = 0, j = 0, maxrec = 8, minArraySize = 3;
        JsonArray yArray = null;
        DbDoc doc = null;
        DocResult ddoc = null;
        long l3 = 2147483647;
        double d1 = 1000.1234;

        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonNumber().setValue(String.valueOf(i + 1)));

            JsonArray jarray = new JsonArray();
            for (j = 0; j < (minArraySize + i); j++) {
                jarray.addValue(new JsonNumber().setValue(String.valueOf((l3 + j + i))));
            }
            newDoc2.add("ARR1", jarray);

            JsonArray karray = new JsonArray();
            for (j = 0; j < (minArraySize + i); j++) {
                karray.addValue(new JsonNumber().setValue(String.valueOf((d1 + j + i))));
            }
            newDoc2.add("ARR2", karray);
            JsonArray larray = new JsonArray();
            for (j = 0; j < (minArraySize + i); j++) {
                larray.addValue(new JsonString().setValue("St_" + i + "_" + j));
            }
            newDoc2.add("ARR3", larray);
            this.collection.add(newDoc2).execute();
            newDoc2 = null;
            jarray = null;
        }

        assertEquals((maxrec), this.collection.count());

        ddoc = this.collection.find("CAST($.F1 as SIGNED) > 1").orderBy("$._id").execute();
        i = 1;
        while (ddoc.hasNext()) {
            doc = ddoc.next();
            yArray = (JsonArray) doc.get("ARR1");
            assertEquals(minArraySize + i, yArray.size());
            for (j = 0; j < yArray.size(); j++) {
                assertEquals(new BigDecimal(String.valueOf(l3 + j + i)), (((JsonNumber) yArray.get(j)).getBigDecimal()));
            }
            i++;
        }
        assertEquals((maxrec), i);

        /* Condition On Array(int) field */
        ddoc = this.collection.find("CAST($.ARR1[1] as SIGNED) > 2147483650").orderBy("CAST($.ARR1[1] as SIGNED)").execute();
        i = 0;
        while (ddoc.hasNext()) {
            doc = ddoc.next();
            assertEquals((long) (i + 4), (long) (((JsonNumber) doc.get("F1")).getInteger()));
            i++;
        }
        assertEquals((maxrec - 3), i);

        /* Condition On Array(String) field */
        ddoc = this.collection.find("$.ARR3[1] = 'St_3_1'").orderBy("$.ARR3[1]").execute();
        doc = ddoc.next();
        assertEquals((long) (4), (long) (((JsonNumber) doc.get("F1")).getInteger()));
        assertFalse(ddoc.hasNext());

        /* Multiple Condition On Array(String) field */
        ddoc = this.collection.find("$.ARR3[1] in ('St_3_1','St_2_1','St_4_1')and $.ARR3[2] in ('St_3_2','St_4_2')").orderBy("$.ARR3[1]").execute();
        doc = ddoc.next();
        assertEquals((long) (4), (long) (((JsonNumber) doc.get("F1")).getInteger()));
        doc = ddoc.next();
        assertEquals((long) (5), (long) (((JsonNumber) doc.get("F1")).getInteger()));
        assertFalse(ddoc.hasNext());
    }

    /**
     * Checks getWarningsCount and getWarnings APIs
     * 
     * @throws Exception
     */
    @Test
    public void testGetWarningsFromCollection() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        String collname = "coll1";
        Collection coll = null;
        Warning w = null;
        DocResult docs = null;
        int i = 0;
        try {
            this.session.sql("set  max_error_count=20000").execute();
            dropCollection(collname);
            coll = this.schema.createCollection(collname, true);
            for (i = 1; i <= 10; i++) {
                if (i % 2 == 0) {
                    coll.add("{\"X\":" + i + ",\"Y\":" + (i + 1000) + "}").execute();
                } else {
                    coll.add("{\"X\":0,\"Y\":0}").execute();
                }
            }
            docs = coll.find().fields("1/$.X as col1,1/$.Y as col2").execute();
            assertEquals(10, docs.getWarningsCount());
            i = 0;
            for (Iterator<Warning> warn = docs.getWarnings(); warn.hasNext();) {
                w = warn.next();
                assertEquals("Division by 0", w.getMessage());
                assertEquals(2, w.getLevel());
                assertEquals(1365, w.getCode());
                i++;
            }
            this.schema.dropCollection(collname);
            coll = this.schema.createCollection(collname, true);
            coll.add("{\"X\":1}").execute();
            String s = "";
            for (i = 1; i <= 10000; i++) {
                if (i > 1) {
                    s = s + ",";
                }
                if (i % 2 == 0) {
                    s = s + "1/$.X as col" + i;
                } else {
                    s = s + "$.X/0 as col1" + i;
                }
            }
            docs = coll.find().fields(s).execute();
            assertEquals(5000, docs.getWarningsCount());
            i = 0;
            for (Iterator<Warning> warn = docs.getWarnings(); warn.hasNext();) {
                w = warn.next();
                assertEquals("Division by 0", w.getMessage());
                assertEquals(2, w.getLevel());
                assertEquals(1365, w.getCode());
                i++;
            }
            this.schema.dropCollection(collname);
        } finally {
            if (this.session != null) {
                this.session.close();
            }
        }
    }

    @Test
    public void testCollectionFindAsyncMany() throws Exception {
        int i = 0, maxrec = 10;
        int NUMBER_OF_QUERIES = 1000;
        DbDoc doc = null;
        AddResult res = null;
        CompletableFuture<AddResult> asyncRes = null;
        DocResult docs = null;

        double d = 100.123;

        /* add().executeAsync() maxrec num of records */
        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(d + i)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(100 + i)));

            asyncRes = this.collection.add(newDoc2).executeAsync();
            res = asyncRes.get();

            assertEquals(1, res.getAffectedItemsCount());
            newDoc2 = null;
        }

        assertEquals((maxrec), this.collection.count());

        List<CompletableFuture<DocResult>> futures = new ArrayList<>();
        for (i = 0; i < NUMBER_OF_QUERIES; ++i) {
            if (i % 3 == 0) {
                futures.add(this.collection.find("F1  like '%Field%-5'").fields("$._id as _id, $.F1 as F1, $.F2 as F2, $.F3 as F3").executeAsync());
            } else if (i % 3 == 1) {
                futures.add(this.collection.find("NON_EXISTING_FUNCTION()").fields("$._id as _id, $.F1 as F1, $.F2 as F2, $.F3 as F3").executeAsync()); //Error
            } else {
                futures.add(this.collection.find("F3 = ?").bind(106).executeAsync());
            }
        }

        for (i = 0; i < NUMBER_OF_QUERIES; ++i) {
            if (i % 3 == 0) {
                docs = futures.get(i).get();
                doc = docs.next();
                assertEquals((long) 105, (long) (((JsonNumber) doc.get("F3")).getInteger()));
                assertEquals("1005", (((JsonString) doc.get("_id")).getString()));
                assertFalse(docs.hasNext());
            } else if (i % 3 == 1) {
                final int i1 = i;
                assertThrows(ExecutionException.class, "com.mysql.cj.protocol.x.XProtocolError: ERROR 1305 \\(42000\\) FUNCTION " + this.schema.getName()
                        + ".NON_EXISTING_FUNCTION does not exist", () -> futures.get(i1).get());
            } else {
                docs = futures.get(i).get();
                doc = docs.next();
                assertEquals((long) 106, (long) (((JsonNumber) doc.get("F3")).getInteger()));
                assertEquals("1006", (((JsonString) doc.get("_id")).getString()));
                assertFalse(docs.hasNext());
            }
        }

        final CompletableFuture<DocResult> asyncDocs = this.collection.find("F3 > ? and F3 < ?").bind(102, 106).fields(expr("{'_id':$._id,'X':sleep(1)}"))
                .executeAsync();
        assertThrows(java.util.concurrent.TimeoutException.class, () -> asyncDocs.get(2, TimeUnit.SECONDS));
    }

    @Test
    public void testCollectionFindAsyncExt() throws Exception {
        int i = 0, maxrec = 10;
        DbDoc doc = null;
        AddResult res = null;
        CompletableFuture<AddResult> asyncRes = null;
        CompletableFuture<DocResult> asyncDocs = null;
        DocResult docs = null;
        try {
            double d = 100.123;
            DbDoc[] jsonlist = new DbDocImpl[maxrec];

            /* add().executeAsync() with JsonList,DbDoc and JsonString */
            for (i = 0; i < maxrec; i++) {
                DbDoc newDoc2 = new DbDocImpl();
                newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
                newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
                newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(d + i)));
                newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(100 + i)));
                jsonlist[i] = newDoc2;
                newDoc2 = null;
            }

            asyncRes = this.collection.add(jsonlist).executeAsync();
            res = asyncRes.get();
            assertEquals(maxrec, res.getAffectedItemsCount());

            /* With Bind,fields and orderBy */
            asyncDocs = this.collection.find("$.F3  < ? and $.F3 > ? and $.F3 != ?").bind(105, 101, 103).fields("$._id as _id, $.F1 as f1, $.F3 as f3")
                    .orderBy("$.F3 asc").executeAsync();
            docs = asyncDocs.get();
            doc = docs.next();
            assertEquals((long) 102, (long) (((JsonNumber) doc.get("f3")).getInteger()));
            assertEquals("1002", (((JsonString) doc.get("_id")).getString()));
            doc = docs.next();
            assertEquals((long) 104, (long) (((JsonNumber) doc.get("f3")).getInteger()));
            assertEquals("1004", (((JsonString) doc.get("_id")).getString()));
            assertFalse(docs.hasNext());

            /* find with groupBy with Having Clause */
            asyncDocs = this.collection.find("$.F3 > ? and $.F3 < ?").fields("max($.F1) as max_f1, sum($.F2) as max_f2, sum($.F3) as max_f3 ").groupBy("$.F3")
                    .having("max($.F3) > 105 and max($.F3) < 107 ").bind(104, 108).executeAsync();
            docs = asyncDocs.get();
            doc = docs.next();
            assertEquals("Field-1-Data-6", (((JsonString) doc.get("max_f1")).getString()));
            assertEquals(new BigDecimal(String.valueOf(d + 6)), (((JsonNumber) doc.get("max_f2")).getBigDecimal()));
            assertEquals((long) (106), (long) (((JsonNumber) doc.get("max_f3")).getInteger()));
            assertFalse(docs.hasNext());

            sqlUpdate("drop function if exists abcd");
            sqlUpdate(
                    "CREATE FUNCTION abcd (`p1 col1` CHAR(20)) RETURNS ENUM('YES','NO')  COMMENT 'Sample Function abcd'  DETERMINISTIC RETURN  IF(EXISTS(SELECT 1 ), 'YES', 'NO' )");

            /* execute Function */
            asyncDocs = this.collection.find("$.F1 like ? and $.F1  not like ? and $.F1 not like ?")
                    .bind(new Object[] { ("%Fie%-2"), ("%Fie%-1"), ("%Fie%-3") }).fields(expr("{'_id':$._id,'F3':$.F3,'X': abcd('S')}")).executeAsync();
            docs = asyncDocs.get();
            doc = docs.next();
            assertEquals((long) 102, (long) (((JsonNumber) doc.get("F3")).getInteger()));
            assertEquals("1002", (((JsonString) doc.get("_id")).getString()));
            assertEquals("YES", (((JsonString) doc.get("X")).getString()));
            assertFalse(docs.hasNext());

            /* Map */
            Map<String, Object> params = new HashMap<>();
            params.put("namedParam10000", "%Fie%-2");
            params.put("namedParam10001", "%Fie%-3");
            params.put("namedParam10002", 102);
            asyncDocs = this.collection.find("($.F1 like :namedParam10000 OR $.F1 not like :namedParam10001) and $.F3  = :namedParam10002").bind(params)
                    .fields(expr("{'_id':$._id,'F3':$.F3,'X': abcd('S')}")).executeAsync();
            docs = asyncDocs.get();
            doc = docs.next();
            assertEquals((long) 102, (long) (((JsonNumber) doc.get("F3")).getInteger()));
            assertEquals("1002", (((JsonString) doc.get("_id")).getString()));
            assertEquals("YES", (((JsonString) doc.get("X")).getString()));
            assertFalse(docs.hasNext());
        } finally {
            sqlUpdate("drop function if exists abcd");

        }
    }
}
