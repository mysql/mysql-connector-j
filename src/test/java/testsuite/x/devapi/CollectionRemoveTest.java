/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;

import org.junit.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.xdevapi.Collection;
import com.mysql.cj.xdevapi.DocResult;
import com.mysql.cj.xdevapi.JsonNumber;
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

        assertEquals(3, this.collection.count());

        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionRemoveTest.this.collection.remove(null).execute();
                return null;
            }
        });

        assertThrows(XDevAPIError.class, "Parameter 'criteria' must not be null or empty.", new Callable<Void>() {
            public Void call() throws Exception {
                CollectionRemoveTest.this.collection.remove(" ").execute();
                return null;
            }
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
        if (!this.isSetForXTests) {
            return;
        }

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
        if (!this.isSetForXTests) {
            return;
        }

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
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
            return;
        }

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
        if (!this.isSetForXTests) {
            return;
        }

        this.collection.add("{\"_id\":\"1\", \"ord\": 1}", "{\"_id\":\"2\", \"ord\": 2}", "{\"_id\":\"3\", \"ord\": 3}", "{\"_id\":\"4\", \"ord\": 4}",
                "{\"_id\":\"5\", \"ord\": 5}", "{\"_id\":\"6\", \"ord\": 6}", "{\"_id\":\"7\", \"ord\": 7}", "{\"_id\":\"8\", \"ord\": 8}").execute();

        RemoveStatement testRemove = this.collection.remove("$.ord <= 2");

        assertTrue(testRemove.getClass().getMethod("where", String.class).isAnnotationPresent(Deprecated.class));

        assertEquals(2, testRemove.execute().getAffectedItemsCount());
        assertEquals(4, ((RemoveStatementImpl) testRemove).where("$.ord > 4").execute().getAffectedItemsCount());
    }
}
