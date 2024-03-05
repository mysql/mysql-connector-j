/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
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

package testsuite.regression;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import testsuite.BaseTestCase;

/**
 * Tests SubQueries
 */
public class SubqueriesRegressionTest extends BaseTestCase {

    private final static int REPETITIONS = 100;

    @BeforeEach
    public void setUp() throws Exception {
        createTables();
    }

    @Test
    public void testSubQuery1() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {

            this.rs = this.stmt.executeQuery(
                    "select t3.colA from t3, t1 where t3.colA = 'bbbb' and t3.colB = t1.colA and exists (select 'X' from t2 where t2.colB = t1.colB)");
            assertTrue(this.rs.next());
            assertTrue("bbbb".equals(this.rs.getString(1)));
            assertTrue(!this.rs.next());
        }
    }

    @Test
    public void testSubQuery2() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {

            this.rs = this.stmt
                    .executeQuery("select t3.colA from t3, t1 where t3.colA = 'bbbb' and t3.colB = t1.colA and exists (select 'X' from t2 where t2.colB = 2)");
            assertTrue(this.rs.next());
            assertTrue("bbbb".equals(this.rs.getString(1)));
            assertTrue(!this.rs.next());
        }
    }

    @Test
    public void testSubQuery3() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {

            this.rs = this.stmt.executeQuery("select * from t1 where t1.colA = 'efgh' and exists (select 'X' from t2 where t2.colB = t1.colB)");
            assertTrue(this.rs.next());
            assertTrue("efgh".equals(this.rs.getString(1)));
            assertTrue("2".equals(this.rs.getString(2)));
            assertTrue(!this.rs.next());
        }
    }

    @Test
    public void testSubQuery4() throws Exception {
        // not really a subquery, but we want to have this in our testsuite
        for (int i = 0; i < REPETITIONS; i++) {
            this.rs = this.stmt.executeQuery("select colA, '' from t2 union select colA, colB from t3");

            assertTrue(this.rs.next());
            assertTrue("type1".equals(this.rs.getString(1)));
            assertTrue("".equals(this.rs.getString(2)));

            assertTrue(this.rs.next());
            assertTrue("type2".equals(this.rs.getString(1)));
            assertTrue("".equals(this.rs.getString(2)));

            assertTrue(this.rs.next());
            assertTrue("type3".equals(this.rs.getString(1)));
            assertTrue("".equals(this.rs.getString(2)));

            assertTrue(this.rs.next());
            assertTrue("aaaa".equals(this.rs.getString(1)));
            assertTrue("abcd".equals(this.rs.getString(2)), "'" + this.rs.getString(2) + "' != expected of 'abcd'");

            assertTrue(this.rs.next());
            assertTrue("bbbb".equals(this.rs.getString(1)));
            assertTrue("efgh".equals(this.rs.getString(2)));

            assertTrue(this.rs.next());
            assertTrue("cccc".equals(this.rs.getString(1)));
            assertTrue("ijkl".equals(this.rs.getString(2)), "'" + this.rs.getString(2) + "' != expected of 'ijkl'");

            assertTrue(!this.rs.next());
        }
    }

    @Test
    public void testSubQuery5() throws Exception {
        for (int i = 0; i < REPETITIONS; i++) {

            this.rs = this.stmt.executeQuery("select t1.colA from t1, t4 where t4.colA = t1.colA and exists (select 'X' from t2 where t2.colA = t4.colB)");
            assertTrue(this.rs.next());
            assertTrue("abcd".equals(this.rs.getString(1)));
            assertTrue(this.rs.next());
            assertTrue("efgh".equals(this.rs.getString(1)));
            assertTrue(this.rs.next());
            assertTrue("ijkl".equals(this.rs.getString(1)));
            assertTrue(!this.rs.next());

        }
    }

    private void createTables() throws Exception {
        createTable("t1", "(colA varchar(10), colB decimal(3,0))");
        createTable("t2", "(colA varchar(10), colB varchar(10))");
        createTable("t3", "(colA varchar(10), colB varchar(10))");
        createTable("t4", "(colA varchar(10), colB varchar(10))");
        this.stmt.executeUpdate("insert into t1 values ('abcd', 1), ('efgh', 2), ('ijkl', 3)");
        this.stmt.executeUpdate("insert into t2 values ('type1', '1'), ('type2', '2'), ('type3', '3')");
        this.stmt.executeUpdate("insert into t3 values ('aaaa', 'abcd'), ('bbbb', 'efgh'), ('cccc', 'ijkl')");
        this.stmt.executeUpdate("insert into t4 values ('abcd', 'type1'), ('efgh', 'type2'), ('ijkl', 'type3')");
    }

}
