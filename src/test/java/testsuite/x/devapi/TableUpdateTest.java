/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates. All rights reserved.
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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.xdevapi.Result;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.Table;
import com.mysql.cj.xdevapi.UpdateStatement;

/**
 * @todo
 */
public class TableUpdateTest extends BaseTableTestCase {
    @Test
    public void testUpdates() {
        if (!this.isSetForXTests) {
            return;
        }
        try {
            sqlUpdate("drop table if exists updates");
            sqlUpdate("drop view if exists updatesView");
            sqlUpdate("create table updates (_id varchar(32), name varchar(20), birthday date, age int)");
            sqlUpdate("create view updatesView as select _id, name, age from updates");

            sqlUpdate("insert into updates values ('1', 'Sakila', '2000-05-27', 14)");
            sqlUpdate("insert into updates values ('2', 'Shakila', '2001-06-26', 13)");

            Table table = this.schema.getTable("updates");
            table.update().set("name", expr("concat(name, '-updated')")).set("age", expr("age + 1")).where("name == 'Sakila'").execute();

            Table view = this.schema.getTable("updatesView");
            view.update().set("name", expr("concat(name, '-updated')")).set("age", expr("age + 3")).where("name == 'Shakila'").orderBy("age", "name").execute();

            RowResult rows = table.select("name, age").where("_id == :theId").bind("theId", 1).execute();
            Row r = rows.next();
            assertEquals("Sakila-updated", r.getString(0));
            assertEquals(15, r.getInt(1));
            assertFalse(rows.hasNext());

            rows = table.select("name, age").where("_id == :theId").bind("theId", 2).execute();
            r = rows.next();
            assertEquals("Shakila-updated", r.getString(0));
            assertEquals(16, r.getInt(1));
            assertFalse(rows.hasNext());
        } finally {
            sqlUpdate("drop table if exists updates");
            sqlUpdate("drop view if exists updatesView");
        }
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

            Table testTbl1 = testSession.getDefaultSchema().getTable("testPrepareUpdate_1");
            Table testTbl2 = testSession.getDefaultSchema().getTable("testPrepareUpdate_2");
            Table testTbl3 = testSession.getDefaultSchema().getTable("testPrepareUpdate_3");
            Table testTbl4 = testSession.getDefaultSchema().getTable("testPrepareUpdate_4");

            // Initialize several UpdateStatement objects.
            UpdateStatement testUpdate1 = testTbl1.update().where("true").set("ord", expr("ord * 10")); // Update all.
            UpdateStatement testUpdate2 = testTbl2.update().where("ord >= :n").set("ord", expr("ord * 10")); // Criteria with one placeholder.
            UpdateStatement testupdate3 = testTbl3.update().where("ord >= :n AND ord <= :n + 1").set("ord", expr("ord * 10")); // Criteria with same placeholder repeated.
            UpdateStatement testUpdate4 = testTbl4.update().where("ord >= :n AND ord <= :m").set("ord", expr("ord * 10")); // Criteria with multiple placeholders.

            assertPreparedStatementsCountsAndId(testSession, 0, testUpdate1, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testUpdate2, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testupdate3, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testUpdate4, 0, -1);

            // A. Set binds: 1st execute -> non-prepared.
            assertTestPreparedStatementsResult(testUpdate1.execute(), 4, testTbl1.getName(), 10, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 0, testUpdate1, 0, -1);
            assertTestPreparedStatementsResult(testUpdate2.bind("n", 2).execute(), 3, testTbl2.getName(), 1, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 0, testUpdate2, 0, -1);
            assertTestPreparedStatementsResult(testupdate3.bind("n", 2).execute(), 2, testTbl3.getName(), 1, 20, 30, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testupdate3, 0, -1);
            assertTestPreparedStatementsResult(testUpdate4.bind("n", 2).bind("m", 3).execute(), 2, testTbl4.getName(), 1, 20, 30, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testUpdate4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);
            testPreparedStatementsResetData();

            // B. Set orderBy resets execution count: 1st execute -> non-prepared.
            assertTestPreparedStatementsResult(testUpdate1.orderBy("id").execute(), 4, testTbl1.getName(), 10, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 0, testUpdate1, 0, -1);
            assertTestPreparedStatementsResult(testUpdate2.orderBy("id").execute(), 3, testTbl2.getName(), 1, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 0, testUpdate2, 0, -1);
            assertTestPreparedStatementsResult(testupdate3.orderBy("id").execute(), 2, testTbl3.getName(), 1, 20, 30, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testupdate3, 0, -1);
            assertTestPreparedStatementsResult(testUpdate4.orderBy("id").execute(), 2, testTbl4.getName(), 1, 20, 30, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testUpdate4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);
            testPreparedStatementsResetData();

            // C. Set binds reuse statement: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testUpdate1.execute(), 4, testTbl1.getName(), 10, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 1, testUpdate1, 1, 1);
            assertTestPreparedStatementsResult(testUpdate2.bind("n", 3).execute(), 2, testTbl2.getName(), 1, 2, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 2, testUpdate2, 2, 1);
            assertTestPreparedStatementsResult(testupdate3.bind("n", 3).execute(), 2, testTbl3.getName(), 1, 2, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 3, testupdate3, 3, 1);
            assertTestPreparedStatementsResult(testUpdate4.bind("m", 4).execute(), 3, testTbl4.getName(), 1, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 4, testUpdate4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 4, 4, 0);
            testPreparedStatementsResetData();

            // D. Set binds reuse statement: 3rd execute -> execute.
            assertTestPreparedStatementsResult(testUpdate1.execute(), 4, testTbl1.getName(), 10, 20, 30, 40);
            assertPreparedStatementsCountsAndId(testSession, 4, testUpdate1, 1, 2);
            assertTestPreparedStatementsResult(testUpdate2.bind("n", 4).execute(), 1, testTbl2.getName(), 1, 2, 3, 40);
            assertPreparedStatementsCountsAndId(testSession, 4, testUpdate2, 2, 2);
            assertTestPreparedStatementsResult(testupdate3.bind("n", 1).execute(), 2, testTbl3.getName(), 10, 20, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testupdate3, 3, 2);
            assertTestPreparedStatementsResult(testUpdate4.bind("m", 2).execute(), 1, testTbl4.getName(), 1, 20, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testUpdate4, 4, 2);

            assertPreparedStatementsStatusCounts(testSession, 4, 8, 0);
            testPreparedStatementsResetData();

            // E. Set new values deallocates and resets execution count: 1st execute -> deallocate + non-prepared.
            assertTestPreparedStatementsResult(testUpdate1.set("ord", expr("ord * 100")).execute(), 4, testTbl1.getName(), 100, 200, 300, 400);
            assertPreparedStatementsCountsAndId(testSession, 3, testUpdate1, 0, -1);
            assertTestPreparedStatementsResult(testUpdate2.set("ord", expr("ord * 100")).execute(), 1, testTbl2.getName(), 1, 2, 3, 400);
            assertPreparedStatementsCountsAndId(testSession, 2, testUpdate2, 0, -1);
            assertTestPreparedStatementsResult(testupdate3.set("ord", expr("ord * 100")).execute(), 2, testTbl3.getName(), 100, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 1, testupdate3, 0, -1);
            assertTestPreparedStatementsResult(testUpdate4.set("ord", expr("ord * 100")).execute(), 1, testTbl4.getName(), 1, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testUpdate4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 4, 8, 4);
            testPreparedStatementsResetData();

            // F. No Changes: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testUpdate1.execute(), 4, testTbl1.getName(), 100, 200, 300, 400);
            assertPreparedStatementsCountsAndId(testSession, 1, testUpdate1, 1, 1);
            assertTestPreparedStatementsResult(testUpdate2.execute(), 1, testTbl2.getName(), 1, 2, 3, 400);
            assertPreparedStatementsCountsAndId(testSession, 2, testUpdate2, 2, 1);
            assertTestPreparedStatementsResult(testupdate3.execute(), 2, testTbl3.getName(), 100, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 3, testupdate3, 3, 1);
            assertTestPreparedStatementsResult(testUpdate4.execute(), 1, testTbl4.getName(), 1, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testUpdate4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 8, 12, 4);
            testPreparedStatementsResetData();

            // G. Set limit for the first time deallocates and re-prepares: 1st execute -> re-prepare + execute.
            assertTestPreparedStatementsResult(testUpdate1.limit(1).execute(), 1, testTbl1.getName(), 100, 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testUpdate1, 1, 1);
            assertTestPreparedStatementsResult(testUpdate2.limit(1).execute(), 1, testTbl2.getName(), 1, 2, 3, 400);
            assertPreparedStatementsCountsAndId(testSession, 4, testUpdate2, 2, 1);
            assertTestPreparedStatementsResult(testupdate3.limit(1).execute(), 1, testTbl3.getName(), 100, 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testupdate3, 3, 1);
            assertTestPreparedStatementsResult(testUpdate4.limit(1).execute(), 1, testTbl4.getName(), 1, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testUpdate4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 12, 16, 8);
            testPreparedStatementsResetData();

            // H. Set limit reuse prepared statement: 2nd execute -> execute.
            assertTestPreparedStatementsResult(testUpdate1.limit(2).execute(), 2, testTbl1.getName(), 100, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testUpdate1, 1, 2);
            assertTestPreparedStatementsResult(testUpdate2.limit(2).execute(), 1, testTbl2.getName(), 1, 2, 3, 400);
            assertPreparedStatementsCountsAndId(testSession, 4, testUpdate2, 2, 2);
            assertTestPreparedStatementsResult(testupdate3.limit(2).execute(), 2, testTbl3.getName(), 100, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testupdate3, 3, 2);
            assertTestPreparedStatementsResult(testUpdate4.limit(2).execute(), 1, testTbl4.getName(), 1, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testUpdate4, 4, 2);

            assertPreparedStatementsStatusCounts(testSession, 12, 20, 8);
            testPreparedStatementsResetData();

            // I. Set orderBy deallocates and resets execution count, set limit has no effect: 1st execute -> deallocate + non-prepared.
            assertTestPreparedStatementsResult(testUpdate1.orderBy("id").limit(1).execute(), 1, testTbl1.getName(), 100, 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 3, testUpdate1, 0, -1);
            assertTestPreparedStatementsResult(testUpdate2.orderBy("id").limit(1).execute(), 1, testTbl2.getName(), 1, 2, 3, 400);
            assertPreparedStatementsCountsAndId(testSession, 2, testUpdate2, 0, -1);
            assertTestPreparedStatementsResult(testupdate3.orderBy("id").limit(1).execute(), 1, testTbl3.getName(), 100, 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 1, testupdate3, 0, -1);
            assertTestPreparedStatementsResult(testUpdate4.orderBy("id").limit(1).execute(), 1, testTbl4.getName(), 1, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testUpdate4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 12, 20, 12);
            testPreparedStatementsResetData();

            // J. Set limit reuse statement: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testUpdate1.limit(2).execute(), 2, testTbl1.getName(), 100, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 1, testUpdate1, 1, 1);
            assertTestPreparedStatementsResult(testUpdate2.limit(2).execute(), 1, testTbl2.getName(), 1, 2, 3, 400);
            assertPreparedStatementsCountsAndId(testSession, 2, testUpdate2, 2, 1);
            assertTestPreparedStatementsResult(testupdate3.limit(2).execute(), 2, testTbl3.getName(), 100, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 3, testupdate3, 3, 1);
            assertTestPreparedStatementsResult(testUpdate4.limit(2).execute(), 1, testTbl4.getName(), 1, 200, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testUpdate4, 4, 1);

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

                testTbl1 = testSession.getDefaultSchema().getTable("testPrepareUpdate_1");
                testTbl2 = testSession.getDefaultSchema().getTable("testPrepareUpdate_2");

                testUpdate1 = testTbl1.update().where("true").set("ord", expr("ord * 10"));
                testUpdate2 = testTbl2.update().where("true").set("ord", expr("ord * 10"));

                // 1st execute -> don't prepare.
                assertTestPreparedStatementsResult(testUpdate1.execute(), 4, testTbl1.getName(), 10, 20, 30, 40);
                assertPreparedStatementsCountsAndId(testSession, 0, testUpdate1, 0, -1);
                assertTestPreparedStatementsResult(testUpdate2.execute(), 4, testTbl2.getName(), 10, 20, 30, 40);
                assertPreparedStatementsCountsAndId(testSession, 0, testUpdate2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);
                testPreparedStatementsResetData();

                // 2nd execute -> prepare + execute.
                assertTestPreparedStatementsResult(testUpdate1.execute(), 4, testTbl1.getName(), 10, 20, 30, 40);
                assertPreparedStatementsCountsAndId(testSession, 1, testUpdate1, 1, 1);
                assertTestPreparedStatementsResult(testUpdate2.execute(), 4, testTbl2.getName(), 10, 20, 30, 40); // Fails preparing, execute as non-prepared.
                assertPreparedStatementsCountsAndId(testSession, 1, testUpdate2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 2, 1, 0); // Failed prepare also counts.
                testPreparedStatementsResetData();

                // 3rd execute -> execute.
                assertTestPreparedStatementsResult(testUpdate1.execute(), 4, testTbl1.getName(), 10, 20, 30, 40);
                assertPreparedStatementsCountsAndId(testSession, 1, testUpdate1, 1, 2);
                assertTestPreparedStatementsResult(testUpdate2.execute(), 4, testTbl2.getName(), 10, 20, 30, 40); // Execute as non-prepared.
                assertPreparedStatementsCountsAndId(testSession, 1, testUpdate2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 2, 2, 0);
                testPreparedStatementsResetData();

                testSession.close();
                assertPreparedStatementsCount(sessionThreadId, 0, 10); // Prepared statements won't live past the closing of the session.
            } finally {
                this.session.sql("SET GLOBAL max_prepared_stmt_count = ?").bind(origMaxPrepStmtCount).execute();
            }
        } finally {
            for (int i = 0; i < 4; i++) {
                sqlUpdate("DROP TABLE IF EXISTS testPrepareUpdate_" + (i + 1));
            }
        }
    }

    private void testPreparedStatementsResetData() {
        for (int i = 0; i < 4; i++) {
            sqlUpdate("CREATE TABLE IF NOT EXISTS testPrepareUpdate_" + (i + 1) + " (id INT PRIMARY KEY, ord INT)");
            sqlUpdate("TRUNCATE TABLE testPrepareUpdate_" + (i + 1));
            sqlUpdate("INSERT INTO testPrepareUpdate_" + (i + 1) + " VALUES (1, 1), (2, 2), (3, 3), (4, 4)");
        }
    }

    private void assertTestPreparedStatementsResult(Result res, int expectedAffectedItemsCount, String tableName, int... expectedValues) {
        assertEquals(expectedAffectedItemsCount, res.getAffectedItemsCount());
        RowResult rowRes = this.schema.getTable(tableName).select("ord").execute();
        assertEquals(expectedValues.length, rowRes.count());
        for (int v : expectedValues) {
            assertEquals(v, rowRes.next().getInt("ord"));
        }
    }
}
