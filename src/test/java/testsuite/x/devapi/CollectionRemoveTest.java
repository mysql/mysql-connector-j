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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DbDoc;
import com.mysql.cj.xdevapi.DbDocImpl;
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.JsonArray;
import com.mysql.cj.xdevapi.JsonNumber;
import com.mysql.cj.xdevapi.JsonString;
import com.mysql.cj.xdevapi.RemoveStatement;
import com.mysql.cj.xdevapi.RemoveStatementImpl;
import com.mysql.cj.xdevapi.Result;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.XDevAPIError;

/**
 * @todo
 */
public class CollectionRemoveTest extends BaseCollectionTestCase {

    @Test
    public void deleteAll() {
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\"}").execute(); // Requires manual _id.
            this.collection.add("{\"_id\": \"2\"}").execute();
            this.collection.add("{\"_id\": \"3\"}").execute();
        } else {
            this.collection.add("{}").execute();
            this.collection.add("{}").execute();
            this.collection.add("{}").execute();
        }

        assertEquals(3, this.collection.count());

        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", () -> {
            CollectionRemoveTest.this.collection.remove(null).execute();
            return null;
        });

        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", () -> {
            CollectionRemoveTest.this.collection.remove(" ").execute();
            return null;
        });

        this.collection.remove("false").execute();
        assertEquals(3, this.collection.count());

        this.collection.remove("0 == 1").execute();
        assertEquals(3, this.collection.count());

        this.collection.remove("true").execute();
        assertEquals(0, this.collection.count());
    }

    @Test
    public void deleteSome() {
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\"}").execute(); // Requires manual _id.
            this.collection.add("{\"_id\": \"2\"}").execute();
            this.collection.add("{\"_id\": \"3\", \"x\":22}").execute();
        } else {
            this.collection.add("{}").execute();
            this.collection.add("{}").execute();
            this.collection.add("{\"x\":22}").execute();
        }

        assertEquals(3, this.collection.count());
        this.collection.remove("$.x = 22").sort("x", "x").execute();
        assertEquals(2, this.collection.count());
    }

    @Test
    public void removeOne() {
        if (!mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.5"))) {
            this.collection.add("{\"_id\": \"1\", \"x\":1}").execute(); // Requires manual _id.
            this.collection.add("{\"_id\": \"2\", \"x\":2}").execute();
        } else {
            this.collection.add("{\"x\":1}").execute();
            this.collection.add("{\"x\":2}").execute();
        }
        this.collection.add("{\"_id\":\"existingId\",\"x\":3}").execute();

        assertEquals(3, this.collection.count());
        assertTrue(this.collection.find("x = 3").execute().hasNext());

        Result res = this.collection.removeOne("existingId");
        assertEquals(1, res.getAffectedItemsCount());

        assertEquals(2, this.collection.count());
        assertFalse(this.collection.find("x = 3").execute().hasNext());

        res = this.collection.removeOne("notExistingId");
        assertEquals(0, res.getAffectedItemsCount());

        assertEquals(2, this.collection.count());
        assertFalse(this.collection.find("x = 3").execute().hasNext());

        res = this.collection.removeOne(null);
        assertEquals(0, res.getAffectedItemsCount());

        assertEquals(2, this.collection.count());
        assertFalse(this.collection.find("x = 3").execute().hasNext());
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

            // Initialize several RemoveStatement objects.
            RemoveStatement testRemove1 = testCol1.remove("true"); // Remove all.
            RemoveStatement testRemove2 = testCol2.remove("$.ord >= :n"); // Criteria with one placeholder.
            RemoveStatement testRemove3 = testCol3.remove("$.ord >= :n AND $.ord <= :n + 1"); // Criteria with same placeholder repeated.
            RemoveStatement testRemove4 = testCol4.remove("$.ord >= :n AND $.ord <= :m"); // Criteria with multiple placeholders.

            assertPreparedStatementsCountsAndId(testSession, 0, testRemove1, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testRemove2, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testRemove3, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testRemove4, 0, -1);

            // A. Set binds: 1st execute -> non-prepared.
            assertTestPreparedStatementsResult(testRemove1.execute(), 4, testCol1.getName());
            assertPreparedStatementsCountsAndId(testSession, 0, testRemove1, 0, -1);
            assertTestPreparedStatementsResult(testRemove2.bind("n", 2).execute(), 3, testCol2.getName(), 1);
            assertPreparedStatementsCountsAndId(testSession, 0, testRemove2, 0, -1);
            assertTestPreparedStatementsResult(testRemove3.bind("n", 2).execute(), 2, testCol3.getName(), 1, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testRemove3, 0, -1);
            assertTestPreparedStatementsResult(testRemove4.bind("n", 2).bind("m", 3).execute(), 2, testCol4.getName(), 1, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testRemove4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);
            testPreparedStatementsResetData();

            // B. Set sort resets execution count: 1st execute -> non-prepared.
            assertTestPreparedStatementsResult(testRemove1.sort("$._id").execute(), 4, testCol1.getName());
            assertPreparedStatementsCountsAndId(testSession, 0, testRemove1, 0, -1);
            assertTestPreparedStatementsResult(testRemove2.sort("$._id").execute(), 3, testCol2.getName(), 1);
            assertPreparedStatementsCountsAndId(testSession, 0, testRemove2, 0, -1);
            assertTestPreparedStatementsResult(testRemove3.sort("$._id").execute(), 2, testCol3.getName(), 1, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testRemove3, 0, -1);
            assertTestPreparedStatementsResult(testRemove4.sort("$._id").execute(), 2, testCol4.getName(), 1, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testRemove4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);
            testPreparedStatementsResetData();

            // C. Set binds reuse statement: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testRemove1.execute(), 4, testCol1.getName());
            assertPreparedStatementsCountsAndId(testSession, 1, testRemove1, 1, 1);
            assertTestPreparedStatementsResult(testRemove2.bind("n", 3).execute(), 2, testCol2.getName(), 1, 2);
            assertPreparedStatementsCountsAndId(testSession, 2, testRemove2, 2, 1);
            assertTestPreparedStatementsResult(testRemove3.bind("n", 3).execute(), 2, testCol3.getName(), 1, 2);
            assertPreparedStatementsCountsAndId(testSession, 3, testRemove3, 3, 1);
            assertTestPreparedStatementsResult(testRemove4.bind("m", 4).execute(), 3, testCol4.getName(), 1);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 4, 4, 0);
            testPreparedStatementsResetData();

            // D. Set binds reuse statement: 3rd execute -> execute.
            assertTestPreparedStatementsResult(testRemove1.execute(), 4, testCol1.getName());
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove1, 1, 2);
            assertTestPreparedStatementsResult(testRemove2.bind("n", 4).execute(), 1, testCol2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove2, 2, 2);
            assertTestPreparedStatementsResult(testRemove3.bind("n", 1).execute(), 2, testCol3.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove3, 3, 2);
            assertTestPreparedStatementsResult(testRemove4.bind("m", 2).execute(), 1, testCol4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove4, 4, 2);

            assertPreparedStatementsStatusCounts(testSession, 4, 8, 0);
            testPreparedStatementsResetData();

            // E. Set sort deallocates and resets execution count: 1st execute -> deallocate + non-prepared.
            assertTestPreparedStatementsResult(testRemove1.sort("$._id").execute(), 4, testCol1.getName());
            assertPreparedStatementsCountsAndId(testSession, 3, testRemove1, 0, -1);
            assertTestPreparedStatementsResult(testRemove2.sort("$._id").execute(), 1, testCol2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 2, testRemove2, 0, -1);
            assertTestPreparedStatementsResult(testRemove3.sort("$._id").execute(), 2, testCol3.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 1, testRemove3, 0, -1);
            assertTestPreparedStatementsResult(testRemove4.sort("$._id").execute(), 1, testCol4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testRemove4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 4, 8, 4);
            testPreparedStatementsResetData();

            // F. No Changes: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testRemove1.execute(), 4, testCol1.getName());
            assertPreparedStatementsCountsAndId(testSession, 1, testRemove1, 1, 1);
            assertTestPreparedStatementsResult(testRemove2.execute(), 1, testCol2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 2, testRemove2, 2, 1);
            assertTestPreparedStatementsResult(testRemove3.execute(), 2, testCol3.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 3, testRemove3, 3, 1);
            assertTestPreparedStatementsResult(testRemove4.execute(), 1, testCol4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 8, 12, 4);
            testPreparedStatementsResetData();

            // G. Set limit for the first time deallocates and re-prepares: 1st execute -> re-prepare + execute.
            assertTestPreparedStatementsResult(testRemove1.limit(1).execute(), 1, testCol1.getName(), 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove1, 1, 1);
            assertTestPreparedStatementsResult(testRemove2.limit(1).execute(), 1, testCol2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove2, 2, 1);
            assertTestPreparedStatementsResult(testRemove3.limit(1).execute(), 1, testCol3.getName(), 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove3, 3, 1);
            assertTestPreparedStatementsResult(testRemove4.limit(1).execute(), 1, testCol4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 12, 16, 8);
            testPreparedStatementsResetData();

            // H. Set limit reuse prepared statement: 2nd execute -> execute.
            assertTestPreparedStatementsResult(testRemove1.limit(2).execute(), 2, testCol1.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove1, 1, 2);
            assertTestPreparedStatementsResult(testRemove2.limit(2).execute(), 1, testCol2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove2, 2, 2);
            assertTestPreparedStatementsResult(testRemove3.limit(2).execute(), 2, testCol3.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove3, 3, 2);
            assertTestPreparedStatementsResult(testRemove4.limit(2).execute(), 1, testCol4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove4, 4, 2);

            assertPreparedStatementsStatusCounts(testSession, 12, 20, 8);
            testPreparedStatementsResetData();

            // I. Set sort deallocates and resets execution count, set limit has no effect: 1st execute -> deallocate + non-prepared.
            assertTestPreparedStatementsResult(testRemove1.sort("$._id").limit(1).execute(), 1, testCol1.getName(), 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 3, testRemove1, 0, -1);
            assertTestPreparedStatementsResult(testRemove2.sort("$._id").limit(1).execute(), 1, testCol2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 2, testRemove2, 0, -1);
            assertTestPreparedStatementsResult(testRemove3.sort("$._id").limit(1).execute(), 1, testCol3.getName(), 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 1, testRemove3, 0, -1);
            assertTestPreparedStatementsResult(testRemove4.sort("$._id").limit(1).execute(), 1, testCol4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testRemove4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 12, 20, 12);
            testPreparedStatementsResetData();

            // J. Set limit reuse statement: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testRemove1.limit(2).execute(), 2, testCol1.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 1, testRemove1, 1, 1);
            assertTestPreparedStatementsResult(testRemove2.limit(2).execute(), 1, testCol2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 2, testRemove2, 2, 1);
            assertTestPreparedStatementsResult(testRemove3.limit(2).execute(), 2, testCol3.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 3, testRemove3, 3, 1);
            assertTestPreparedStatementsResult(testRemove4.limit(2).execute(), 1, testCol4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testRemove4, 4, 1);

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

                testRemove1 = testCol1.remove("true");
                testRemove2 = testCol2.remove("true");

                // 1st execute -> don't prepare.
                assertTestPreparedStatementsResult(testRemove1.execute(), 4, testCol1.getName());
                assertPreparedStatementsCountsAndId(testSession, 0, testRemove1, 0, -1);
                assertTestPreparedStatementsResult(testRemove2.execute(), 4, testCol2.getName());
                assertPreparedStatementsCountsAndId(testSession, 0, testRemove2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);
                testPreparedStatementsResetData();

                // 2nd execute -> prepare + execute.
                assertTestPreparedStatementsResult(testRemove1.execute(), 4, testCol1.getName());
                assertPreparedStatementsCountsAndId(testSession, 1, testRemove1, 1, 1);
                assertTestPreparedStatementsResult(testRemove2.execute(), 4, testCol2.getName()); // Fails preparing, execute as non-prepared.
                assertPreparedStatementsCountsAndId(testSession, 1, testRemove2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 2, 1, 0); // Failed prepare also counts.
                testPreparedStatementsResetData();

                // 3rd execute -> execute.
                assertTestPreparedStatementsResult(testRemove1.execute(), 4, testCol1.getName());
                assertPreparedStatementsCountsAndId(testSession, 1, testRemove1, 1, 2);
                assertTestPreparedStatementsResult(testRemove2.execute(), 4, testCol2.getName()); // Execute as non-prepared.
                assertPreparedStatementsCountsAndId(testSession, 1, testRemove2, 0, -1);

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

        RemoveStatement testRemove = this.collection.remove("$.ord <= 2");

        assertTrue(testRemove.getClass().getMethod("where", String.class).isAnnotationPresent(Deprecated.class));

        assertEquals(2, testRemove.execute().getAffectedItemsCount());
        assertEquals(4, ((RemoveStatementImpl) testRemove).where("$.ord > 4").execute().getAffectedItemsCount());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCollectionRemoveBasic() throws Exception {
        int i = 0, j = 0, maxrec = 100, recCnt = 0, arraySize = 30;
        Result res = null;
        DocResult docs = null;
        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];
        long l1 = Long.MAX_VALUE, l2 = Long.MIN_VALUE;
        double d1 = 100.4567;
        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(d1 + i)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(l1 - i)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(l2 + i)));
            newDoc2.add("F5", new JsonNumber().setValue(String.valueOf(1 + i)));
            newDoc2.add("F6", new JsonString().setValue(2000 + i + "-02-" + (i * 2 + 10)));
            JsonArray jarray = new JsonArray();
            for (j = 0; j < arraySize; j++) {
                jarray.addValue(new JsonString().setValue("String-" + i + "-" + j));
            }
            newDoc2.add("ARR1", jarray);
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();
        assertEquals(maxrec, this.collection.count());

        /* find without Condition */
        docs = this.collection.find("$.F4<0").fields("$._id as _id, $.F1 as f1, $.F2 as f2").execute();
        recCnt = count_data(docs);
        assertEquals(maxrec, recCnt);

        /* remove with condition */
        res = this.collection.remove("CAST($.F5 as SIGNED) = 1").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F5 as SIGNED) = 1 ").fields("$._id as _id, $.F1 as f1, $.F2 as f2").execute();
        assertFalse(docs.hasNext());

        /* remove with condition, limit and orderBy */
        res = this.collection.remove("CAST($.F5 as SIGNED) < 10").limit(1).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F5 as SIGNED) = 2 ").fields("$._id as _id, $.F1 as f1, $.F2 as f2, $.F3+0 as f3").execute();
        assertFalse(docs.hasNext());

        /* remove with condition on string */
        res = this.collection.remove("$.F1 = 'Field-1-Data-2'").limit(10).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("$.F1 = 'Field-1-Data-2'").fields("$._id as _id, $.F1 as f1, $.F2 as f2").execute();
        assertFalse(docs.hasNext());

        /* remove with condition on BigInt */
        res = this.collection.remove("CAST($.F3 as SIGNED) = " + (l1 - 3)).limit(10).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F3 as SIGNED) = " + (l1 - 3)).fields("$._id as _id, $.F1 as f1, $.F2 as f2").execute();
        assertFalse(docs.hasNext());

        /* remove with condition on Double */
        res = this.collection.remove("CAST($.F2 as DECIMAL(10,5)) = " + (d1 + 4)).limit(10).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F2 as SIGNED) = " + (d1 + 4)).fields("$._id as _id, $.F1 as f1, $.F2 as f2").execute();
        assertFalse(docs.hasNext());

        /* remove with condition on Array */
        res = this.collection.remove("$.ARR1[1]  like 'String-5-1' OR $.ARR1[0]  like 'String-5-0' AND $.ARR1[2]  like 'String-5-2'").limit(10)
                .orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("$.ARR1[1]  like 'String-5-%'").fields("$._id as _id, $.F1 as f1, $.F2 as f2").execute();
        assertFalse(docs.hasNext());

        /* Try to remove non-existing row with condition on Array */
        res = this.collection.remove("$.ARR1[1]  like 'String-5-1' OR $.ARR1[0]  like 'String-5-0' AND $.ARR1[2]  like 'String-5-2'").limit(10)
                .orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(0, res.getAffectedItemsCount());

        /* remove with condition on Array */
        res = this.collection.remove("$.ARR1[1] like concat(substr($.ARR1[0],1,7),'6','-1')").limit(10).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("$.ARR1[1]  like 'String-6-%'").fields("$._id as _id, $.F1 as f1, $.F2 as f2").execute();
        assertFalse(docs.hasNext());

        /* remove with null condition */
        i = (int) this.collection.count();
        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", () -> this.collection.remove(null).execute());

        /* remove with empty condition */
        i = (int) this.collection.count();
        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", () -> this.collection.remove(" ").execute());

        /* remove All with a true condition */
        i = (int) this.collection.count();
        res = this.collection.remove("true").limit(maxrec * 10).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(i, res.getAffectedItemsCount());
        docs = this.collection.find("$.ARR1[1]  like 'S%'").fields("$._id as _id, $.F1 as f1, $.F2 as f2").execute();
        assertFalse(docs.hasNext());

        /* remove with a false condition */
        i = (int) this.collection.count();
        res = this.collection.remove("false").limit(maxrec * 10).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(0, res.getAffectedItemsCount());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testCollectionRemoveBindComplex() throws Exception {
        assumeTrue(mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.0")), "MySQL 8.0+ is required to run this test.");

        int i = 0, j = 0, maxrec = 20, arraySize = 3;
        DbDoc doc = null;
        Result res = null;
        DocResult docs = null;
        /* add(DbDoc[] docs) */
        DbDoc[] jsonlist = new DbDocImpl[maxrec];
        long l1 = Long.MAX_VALUE, l2 = Long.MIN_VALUE;
        double d1 = 100.4567;
        for (i = 0; i < maxrec; i++) {
            DbDoc newDoc2 = new DbDocImpl();
            newDoc2.add("_id", new JsonString().setValue(String.valueOf(i + 1000)));
            newDoc2.add("F1", new JsonString().setValue("Field-1-Data-" + i));
            newDoc2.add("F2", new JsonNumber().setValue(String.valueOf(d1 + i)));
            newDoc2.add("F3", new JsonNumber().setValue(String.valueOf(l1 - i)));
            newDoc2.add("F4", new JsonNumber().setValue(String.valueOf(l2 + i)));
            newDoc2.add("F5", new JsonNumber().setValue(String.valueOf(1 + i)));
            newDoc2.add("F6", new JsonString().setValue(2000 + i + "-02-" + (i * 2 + 10)));
            JsonArray jarray = new JsonArray();
            for (j = 0; j < arraySize; j++) {
                if (j == 1) {
                    jarray.addValue(new JsonString().setValue("String-" + j));
                } else {
                    jarray.addValue(new JsonString().setValue("String-" + i + "-" + j));
                }
            }
            newDoc2.add("ARR1", jarray);
            jsonlist[i] = newDoc2;
            newDoc2 = null;
        }
        this.collection.add(jsonlist).execute();
        /*
         * coll.createIndex("index1",true).field(".F1","TEXT(128)",true).execute();
         * coll.createIndex("index2",true).field(".ARR1["+(arraySize-1)+"]","TEXT(256)",true).execute();
         * coll.createIndex("index3",true).field(".F5","INT",true).execute();
         * coll.createIndex("index4",true).field(".F3","BIGINT",true).execute();
         */

        this.collection.createIndex("index1", "{\"fields\": [{\"field\": \"$.F1\", \"type\": \"TEXT(120)\", \"required\": true}],  \"type\" : \"INDEX\"}");
        this.collection.createIndex("index2",
                "{\"fields\": [{\"field\": \"$.ARR1[" + (arraySize - 1) + "]\", \"type\": \"TEXT(120)\", \"required\": true}],  \"type\" : \"INDEX\"}");
        this.collection.createIndex("index3", "{\"fields\": [{\"field\": \"$.F5\", \"type\": \"INT\", \"required\": true}],  \"type\" : \"INDEX\"}");
        this.collection.createIndex("index4", "{\"fields\": [{\"field\": \"$.F3\", \"type\": \"BIGINT\", \"required\": true}],  \"type\" : \"INDEX\"}");

        assertEquals(maxrec, this.collection.count());

        assertThrows(XProtocolError.class, "ERROR 5115 \\(HY000\\) Document is missing a required field",
                () -> this.collection.modify("CAST($.F5 as SIGNED) = 1").unset("$.ARR1[0]").sort("$._id").execute()); //dropping an empty string as collection

        // With Named parameter
        docs = this.collection.find("CAST($.F5 as SIGNED) > :A  AND CAST($.F5 as SIGNED) < :B").bind("B", 2).bind("A", -1).orderBy(" CAST($.F5 as SIGNED) asc ")
                .fields("$.F5 as F5").execute();
        i = 1;
        while (docs.hasNext()) {
            doc = docs.next();
            assertEquals((long) i, (long) ((JsonNumber) doc.get("F5")).getInteger());
        }

        /* Named Param */
        res = this.collection.remove("CAST($.F5 as SIGNED) > :A  AND CAST($.F5 as SIGNED) < :B AND CAST($.F5 as SIGNED) > :C").bind("B", 2).bind("C", -2)
                .bind("A", -1).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F5 as SIGNED)  <? ").bind(2).fields("$.F5 as F5").execute();
        assertFalse(docs.hasNext());

        /* Array */
        res = this.collection.remove("CAST($.F5 as SIGNED) > ? and CAST($.F5 as SIGNED) < ?").bind(new Object[] { -1, 3 }).orderBy("CAST($.F5 as SIGNED)")
                .execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F5 as SIGNED)  <? ").bind(3).fields("$.F5 as F5").execute();
        assertFalse(docs.hasNext());

        /* Map */
        Map<String, Object> params = new HashMap<>();
        params.put("namedParam10001", -1);
        params.put("namedParam10000", 4);

        res = this.collection.remove("CAST($.F5 as SIGNED) < :namedParam10000 and CAST($.F5 as SIGNED) > :namedParam10001").bind(params)
                .orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F5 as SIGNED)  <? ").bind(4).fields("$.F5 as F5").execute();
        assertFalse(docs.hasNext());

        res = this.collection.remove("CAST($.F5 as SIGNED)  < ?").bind(5).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F5 as SIGNED)  <? ").bind(5).fields("$.F5 as F5").execute();
        assertFalse(docs.hasNext());

        res = this.collection.remove("CAST($.F5 as SIGNED)  in (?,?,?,?,?,?)").bind(5, -3, -5, 4, l1, l2).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F5 as SIGNED)  <? ").bind(5).fields("$.F5 as F5").execute();
        assertFalse(docs.hasNext());

        /* Named Param with large param name */
        res = this.collection
                .remove("CAST($.F5 as SIGNED) > :" + buildString(1000, 'Y') + "  AND CAST($.F5 as SIGNED) <= :B AND CAST($.F5 as SIGNED) > :"
                        + buildString(1001, 'Y'))
                .bind("B", 6).bind(buildString(1001, 'Y'), -2).bind(buildString(1000, 'Y'), -1).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F5 as SIGNED)  <? ").bind(6).fields("$.F5 as F5").execute();
        assertFalse(docs.hasNext());

        /* List */
        List<Object> ldata = new ArrayList<>();
        ldata.add(-1);
        ldata.add(7);
        ldata.add(8);
        ldata.add(-2147483648);
        res = this.collection.remove("CAST($.F5 as SIGNED) > ?  AND CAST($.F5 as SIGNED) <= ?  AND CAST($.F5 as SIGNED) < ? AND CAST($.F5 as SIGNED) > ?")
                .bind(ldata).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F5 as SIGNED)  <? ").bind(7).fields("$.F5 as F5").execute();
        assertFalse(docs.hasNext());

        /* STRESSSSS */
        /* Many List Params Bug#21832388 */
        ldata.clear();
        int maxBind = 20;
        String q = "";

        for (i = 0; i < maxBind; i++) {
            ldata.add(i + 9);
            if (i > 0) {
                q = q + " and ";
            }
            q = q + "CAST($.F5 as SIGNED) < ?";
        }

        res = this.collection.remove(q).bind(ldata).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F5 as SIGNED)  <? ").bind(8).fields("$.F5 as F5").execute();
        assertFalse(docs.hasNext());

        /* Many Map variables */
        params.clear();
        q = "";
        params.clear();
        for (i = 0; i < maxBind; i++) {
            params.put("thePlaceholderName" + i, i + 10);
            if (i > 0) {
                q = q + " AND ";
            }
            q = q + "CAST($.F5 as SIGNED) < :thePlaceholderName" + i + " ";
        }

        res = this.collection.remove(q).bind(params).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F5 as SIGNED)  <? ").bind(9).fields("$.F5 as F5").execute();
        assertFalse(docs.hasNext());

        /* Many Array variables */
        Object[] adata = new Object[maxBind];
        q = "";
        params.clear();

        for (i = 0; i < maxBind; i++) {
            adata[i] = i + 11;
            if (i > 0) {
                q = q + " AND ";
            }
            q = q + "CAST($.F5 as SIGNED) < ? ";
        }

        res = this.collection.remove(q).bind(adata).orderBy("CAST($.F5 as SIGNED)").execute();
        assertEquals(1, res.getAffectedItemsCount());
        docs = this.collection.find("CAST($.F5 as SIGNED)  <? ").bind(10).fields("$.F5 as F5").execute();
        assertFalse(docs.hasNext());

        this.collection.dropIndex("index4");
        this.collection.dropIndex("index3");
        this.collection.dropIndex("index2");
        this.collection.dropIndex("index1");
        this.schema.dropCollection("invalidColl");//dropping an invalid collection

        assertThrows(XProtocolError.class, "Parameter 'collectionName' must not be null.", () -> {
            this.schema.dropCollection(null);
            return null;
        });
        try {
            this.schema.dropCollection(null);
        } catch (Exception e) {
            System.out.println("ERROR : " + e.getMessage());
            assertTrue(e.getMessage().contains("must not be null"));
        }

        // successfully dropped an invalid and null collection

        assertThrows(XProtocolError.class, "ERROR 5113 \\(HY000\\) Invalid collection name", () -> {
            this.schema.dropCollection("");//dropping an empty string as collection
            return null;
        });

        assertThrows(XProtocolError.class, "ERROR 1103 \\(42000\\) Incorrect table name ' '", () -> {
            this.schema.dropCollection(" ");
            return null;
        });
    }

}
