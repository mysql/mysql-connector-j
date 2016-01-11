/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.regression;

import testsuite.BaseTestCase;

/**
 * Tests SubQueries on MySQL > 4.1
 */
public class SubqueriesRegressionTest extends BaseTestCase {
    private final static int REPETITIONS = 100;

    /**
     */
    public SubqueriesRegressionTest(String name) {
        super(name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#setUp()
     */
    @Override
    public void setUp() throws Exception {
        super.setUp();

        createTables();
    }

    /*
     * (non-Javadoc)
     * 
     * @see junit.framework.TestCase#tearDown()
     */
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(SubqueriesRegressionTest.class);
    }

    public void testSubQuery1() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            for (int i = 0; i < REPETITIONS; i++) {

                this.rs = this.stmt.executeQuery(
                        "select t3.colA from t3, t1 where t3.colA = 'bbbb' and t3.colB = t1.colA and exists (select 'X' from t2 where t2.colB = t1.colB)");
                assertTrue(this.rs.next());
                assertTrue("bbbb".equals(this.rs.getString(1)));
                assertTrue(!this.rs.next());
            }
        }
    }

    public void testSubQuery2() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            for (int i = 0; i < REPETITIONS; i++) {

                this.rs = this.stmt.executeQuery(
                        "select t3.colA from t3, t1 where t3.colA = 'bbbb' and t3.colB = t1.colA and exists (select 'X' from t2 where t2.colB = 2)");
                assertTrue(this.rs.next());
                assertTrue("bbbb".equals(this.rs.getString(1)));
                assertTrue(!this.rs.next());

            }
        }
    }

    public void testSubQuery3() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            for (int i = 0; i < REPETITIONS; i++) {

                this.rs = this.stmt.executeQuery("select * from t1 where t1.colA = 'efgh' and exists (select 'X' from t2 where t2.colB = t1.colB)");
                assertTrue(this.rs.next());
                assertTrue("efgh".equals(this.rs.getString(1)));
                assertTrue("2".equals(this.rs.getString(2)));
                assertTrue(!this.rs.next());

            }
        }
    }

    public void testSubQuery4() throws Exception {
        // not really a subquery, but we want to have this in our testsuite
        if (versionMeetsMinimum(4, 1)) {
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
                assertTrue("'" + this.rs.getString(2) + "' != expected of 'abcd'", "abcd".equals(this.rs.getString(2)));

                assertTrue(this.rs.next());
                assertTrue("bbbb".equals(this.rs.getString(1)));
                assertTrue("efgh".equals(this.rs.getString(2)));

                assertTrue(this.rs.next());
                assertTrue("cccc".equals(this.rs.getString(1)));
                assertTrue("'" + this.rs.getString(2) + "' != expected of 'ijkl'", "ijkl".equals(this.rs.getString(2)));

                assertTrue(!this.rs.next());
            }
        }
    }

    public void testSubQuery5() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
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
