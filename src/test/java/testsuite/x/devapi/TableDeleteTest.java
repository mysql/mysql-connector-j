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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

import com.mysql.cj.ServerVersion;
import com.mysql.cj.xdevapi.DeleteStatement;
import com.mysql.cj.xdevapi.Result;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.Table;

/**
 * @todo
 */
public class TableDeleteTest extends BaseTableTestCase {
    @Test
    public void testDelete() {
        assumeTrue(this.isSetForXTests);

        try {
            sqlUpdate("drop table if exists testDelete");
            sqlUpdate("drop view if exists testDeleteView");
            sqlUpdate("create table testDelete (_id varchar(32), name varchar(20), birthday date, age int)");
            sqlUpdate("create view testDeleteView as select _id, age from testDelete");

            sqlUpdate("insert into testDelete values ('1', 'Sakila', '2000-05-27', 14)");
            sqlUpdate("insert into testDelete values ('2', 'Shakila', '2001-06-26', 13)");
            sqlUpdate("insert into testDelete values ('3', 'Shakila', '2002-06-26', 12)");

            Table table = this.schema.getTable("testDelete");
            assertEquals(3, table.count());
            table.delete().orderBy("age", "name").where("age == 13").execute();
            assertEquals(2, table.count());

            Table view = this.schema.getTable("testDeleteView");
            assertEquals(2, view.count());
            view.delete().where("age == 12").executeAsync();
            assertEquals(1, view.count());

            table.delete().where("age = :age").bind("age", 14).execute();
            assertEquals(0, table.count());
        } finally {
            sqlUpdate("drop table if exists testDelete");
            sqlUpdate("drop view if exists testDeleteView");
        }
    }

    @Test
    public void testPreparedStatements() {
        assumeTrue(this.isSetForXTests && mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14")));

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

            Table testTbl1 = testSession.getDefaultSchema().getTable("testPrepareDelete_1");
            Table testTbl2 = testSession.getDefaultSchema().getTable("testPrepareDelete_2");
            Table testTbl3 = testSession.getDefaultSchema().getTable("testPrepareDelete_3");
            Table testTbl4 = testSession.getDefaultSchema().getTable("testPrepareDelete_4");

            // Initialize several DeleteStatement objects.
            DeleteStatement testDelete1 = testTbl1.delete().where("true"); // Delete all.
            DeleteStatement testDelete2 = testTbl2.delete().where("ord >= :n"); // Criteria with one placeholder.
            DeleteStatement testDelete3 = testTbl3.delete().where("ord >= :n AND ord <= :n + 1"); // Criteria with same placeholder repeated.
            DeleteStatement testDelete4 = testTbl4.delete().where("ord >= :n AND ord <= :m"); // Criteria with multiple placeholders.

            assertPreparedStatementsCountsAndId(testSession, 0, testDelete1, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testDelete2, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testDelete3, 0, -1);
            assertPreparedStatementsCountsAndId(testSession, 0, testDelete4, 0, -1);

            // A. Set binds: 1st execute -> non-prepared.
            assertTestPreparedStatementsResult(testDelete1.execute(), 4, testTbl1.getName());
            assertPreparedStatementsCountsAndId(testSession, 0, testDelete1, 0, -1);
            assertTestPreparedStatementsResult(testDelete2.bind("n", 2).execute(), 3, testTbl2.getName(), 1);
            assertPreparedStatementsCountsAndId(testSession, 0, testDelete2, 0, -1);
            assertTestPreparedStatementsResult(testDelete3.bind("n", 2).execute(), 2, testTbl3.getName(), 1, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testDelete3, 0, -1);
            assertTestPreparedStatementsResult(testDelete4.bind("n", 2).bind("m", 3).execute(), 2, testTbl4.getName(), 1, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testDelete4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);
            testPreparedStatementsResetData();

            // B. Set orderBy resets execution count: 1st execute -> non-prepared.
            assertTestPreparedStatementsResult(testDelete1.orderBy("id").execute(), 4, testTbl1.getName());
            assertPreparedStatementsCountsAndId(testSession, 0, testDelete1, 0, -1);
            assertTestPreparedStatementsResult(testDelete2.orderBy("id").execute(), 3, testTbl2.getName(), 1);
            assertPreparedStatementsCountsAndId(testSession, 0, testDelete2, 0, -1);
            assertTestPreparedStatementsResult(testDelete3.orderBy("id").execute(), 2, testTbl3.getName(), 1, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testDelete3, 0, -1);
            assertTestPreparedStatementsResult(testDelete4.orderBy("id").execute(), 2, testTbl4.getName(), 1, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testDelete4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);
            testPreparedStatementsResetData();

            // C. Set binds reuse statement: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testDelete1.execute(), 4, testTbl1.getName());
            assertPreparedStatementsCountsAndId(testSession, 1, testDelete1, 1, 1);
            assertTestPreparedStatementsResult(testDelete2.bind("n", 3).execute(), 2, testTbl2.getName(), 1, 2);
            assertPreparedStatementsCountsAndId(testSession, 2, testDelete2, 2, 1);
            assertTestPreparedStatementsResult(testDelete3.bind("n", 3).execute(), 2, testTbl3.getName(), 1, 2);
            assertPreparedStatementsCountsAndId(testSession, 3, testDelete3, 3, 1);
            assertTestPreparedStatementsResult(testDelete4.bind("m", 4).execute(), 3, testTbl4.getName(), 1);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 4, 4, 0);
            testPreparedStatementsResetData();

            // D. Set binds reuse statement: 3rd execute -> execute.
            assertTestPreparedStatementsResult(testDelete1.execute(), 4, testTbl1.getName());
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete1, 1, 2);
            assertTestPreparedStatementsResult(testDelete2.bind("n", 4).execute(), 1, testTbl2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete2, 2, 2);
            assertTestPreparedStatementsResult(testDelete3.bind("n", 1).execute(), 2, testTbl3.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete3, 3, 2);
            assertTestPreparedStatementsResult(testDelete4.bind("m", 2).execute(), 1, testTbl4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete4, 4, 2);

            assertPreparedStatementsStatusCounts(testSession, 4, 8, 0);
            testPreparedStatementsResetData();

            // E. Set orderBy deallocates and resets execution count: 1st execute -> deallocate + non-prepared.
            assertTestPreparedStatementsResult(testDelete1.orderBy("id").execute(), 4, testTbl1.getName());
            assertPreparedStatementsCountsAndId(testSession, 3, testDelete1, 0, -1);
            assertTestPreparedStatementsResult(testDelete2.orderBy("id").execute(), 1, testTbl2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 2, testDelete2, 0, -1);
            assertTestPreparedStatementsResult(testDelete3.orderBy("id").execute(), 2, testTbl3.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 1, testDelete3, 0, -1);
            assertTestPreparedStatementsResult(testDelete4.orderBy("id").execute(), 1, testTbl4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testDelete4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 4, 8, 4);
            testPreparedStatementsResetData();

            // F. No Changes: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testDelete1.execute(), 4, testTbl1.getName());
            assertPreparedStatementsCountsAndId(testSession, 1, testDelete1, 1, 1);
            assertTestPreparedStatementsResult(testDelete2.execute(), 1, testTbl2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 2, testDelete2, 2, 1);
            assertTestPreparedStatementsResult(testDelete3.execute(), 2, testTbl3.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 3, testDelete3, 3, 1);
            assertTestPreparedStatementsResult(testDelete4.execute(), 1, testTbl4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 8, 12, 4);
            testPreparedStatementsResetData();

            // G. Set limit for the first time deallocates and re-prepares: 1st execute -> re-prepare + execute.
            assertTestPreparedStatementsResult(testDelete1.limit(1).execute(), 1, testTbl1.getName(), 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete1, 1, 1);
            assertTestPreparedStatementsResult(testDelete2.limit(1).execute(), 1, testTbl2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete2, 2, 1);
            assertTestPreparedStatementsResult(testDelete3.limit(1).execute(), 1, testTbl3.getName(), 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete3, 3, 1);
            assertTestPreparedStatementsResult(testDelete4.limit(1).execute(), 1, testTbl4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete4, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 12, 16, 8);
            testPreparedStatementsResetData();

            // H. Set limit reuse prepared statement: 2nd execute -> execute.
            assertTestPreparedStatementsResult(testDelete1.limit(2).execute(), 2, testTbl1.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete1, 1, 2);
            assertTestPreparedStatementsResult(testDelete2.limit(2).execute(), 1, testTbl2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete2, 2, 2);
            assertTestPreparedStatementsResult(testDelete3.limit(2).execute(), 2, testTbl3.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete3, 3, 2);
            assertTestPreparedStatementsResult(testDelete4.limit(2).execute(), 1, testTbl4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete4, 4, 2);

            assertPreparedStatementsStatusCounts(testSession, 12, 20, 8);
            testPreparedStatementsResetData();

            // I. Set sort deallocates and resets execution count, set limit has no effect: 1st execute -> deallocate + non-prepared.
            assertTestPreparedStatementsResult(testDelete1.orderBy("id").limit(1).execute(), 1, testTbl1.getName(), 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 3, testDelete1, 0, -1);
            assertTestPreparedStatementsResult(testDelete2.orderBy("id").limit(1).execute(), 1, testTbl2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 2, testDelete2, 0, -1);
            assertTestPreparedStatementsResult(testDelete3.orderBy("id").limit(1).execute(), 1, testTbl3.getName(), 2, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 1, testDelete3, 0, -1);
            assertTestPreparedStatementsResult(testDelete4.orderBy("id").limit(1).execute(), 1, testTbl4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 0, testDelete4, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 12, 20, 12);
            testPreparedStatementsResetData();

            // J. Set limit reuse statement: 2nd execute -> prepare + execute.
            assertTestPreparedStatementsResult(testDelete1.limit(2).execute(), 2, testTbl1.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 1, testDelete1, 1, 1);
            assertTestPreparedStatementsResult(testDelete2.limit(2).execute(), 1, testTbl2.getName(), 1, 2, 3);
            assertPreparedStatementsCountsAndId(testSession, 2, testDelete2, 2, 1);
            assertTestPreparedStatementsResult(testDelete3.limit(2).execute(), 2, testTbl3.getName(), 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 3, testDelete3, 3, 1);
            assertTestPreparedStatementsResult(testDelete4.limit(2).execute(), 1, testTbl4.getName(), 1, 3, 4);
            assertPreparedStatementsCountsAndId(testSession, 4, testDelete4, 4, 1);

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

                testTbl1 = testSession.getDefaultSchema().getTable("testPrepareDelete_1");
                testTbl2 = testSession.getDefaultSchema().getTable("testPrepareDelete_2");

                testDelete1 = testTbl1.delete().where("true");
                testDelete2 = testTbl2.delete().where("true");

                // 1st execute -> don't prepare.
                assertTestPreparedStatementsResult(testDelete1.execute(), 4, testTbl1.getName());
                assertPreparedStatementsCountsAndId(testSession, 0, testDelete1, 0, -1);
                assertTestPreparedStatementsResult(testDelete2.execute(), 4, testTbl2.getName());
                assertPreparedStatementsCountsAndId(testSession, 0, testDelete2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);
                testPreparedStatementsResetData();

                // 2nd execute -> prepare + execute.
                assertTestPreparedStatementsResult(testDelete1.execute(), 4, testTbl1.getName());
                assertPreparedStatementsCountsAndId(testSession, 1, testDelete1, 1, 1);
                assertTestPreparedStatementsResult(testDelete2.execute(), 4, testTbl2.getName()); // Fails preparing, execute as non-prepared.
                assertPreparedStatementsCountsAndId(testSession, 1, testDelete2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 2, 1, 0); // Failed prepare also counts.
                testPreparedStatementsResetData();

                // 3rd execute -> execute.
                assertTestPreparedStatementsResult(testDelete1.execute(), 4, testTbl1.getName());
                assertPreparedStatementsCountsAndId(testSession, 1, testDelete1, 1, 2);
                assertTestPreparedStatementsResult(testDelete2.execute(), 4, testTbl2.getName()); // Execute as non-prepared.
                assertPreparedStatementsCountsAndId(testSession, 1, testDelete2, 0, -1);

                assertPreparedStatementsStatusCounts(testSession, 2, 2, 0);
                testPreparedStatementsResetData();

                testSession.close();
                assertPreparedStatementsCount(sessionThreadId, 0, 10); // Prepared statements won't live past the closing of the session.
            } finally {
                this.session.sql("SET GLOBAL max_prepared_stmt_count = ?").bind(origMaxPrepStmtCount).execute();
            }
        } finally {
            for (int i = 0; i < 4; i++) {
                sqlUpdate("DROP TABLE IF EXISTS testPrepareDelete_" + (i + 1));
            }
        }
    }

    private void testPreparedStatementsResetData() {
        for (int i = 0; i < 4; i++) {
            sqlUpdate("CREATE TABLE IF NOT EXISTS testPrepareDelete_" + (i + 1) + " (id INT PRIMARY KEY, ord INT)");
            sqlUpdate("TRUNCATE TABLE testPrepareDelete_" + (i + 1));
            sqlUpdate("INSERT INTO testPrepareDelete_" + (i + 1) + " VALUES (1, 1), (2, 2), (3, 3), (4, 4)");
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
