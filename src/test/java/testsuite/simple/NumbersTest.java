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

package testsuite.simple;

import java.sql.SQLException;

import testsuite.BaseTestCase;

public class NumbersTest extends BaseTestCase {
    private static final long TEST_BIGINT_VALUE = 6147483647L;

    /**
     * Creates a new NumbersTest object.
     * 
     * @param name
     */
    public NumbersTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(NumbersTest.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createTestTable();
    }

    public void testNumbers() throws SQLException {
        this.rs = this.stmt.executeQuery("SELECT * from number_test");

        while (this.rs.next()) {
            long minBigInt = this.rs.getLong(1);
            long maxBigInt = this.rs.getLong(2);
            long testBigInt = this.rs.getLong(3);
            assertTrue("Minimum bigint not stored correctly", (minBigInt == Long.MIN_VALUE));
            assertTrue("Maximum bigint not stored correctly", (maxBigInt == Long.MAX_VALUE));
            assertTrue("Test bigint not stored correctly", (TEST_BIGINT_VALUE == testBigInt));
        }
    }

    private void createTestTable() throws SQLException {
        createTable("number_test", "(minBigInt bigint, maxBigInt bigint, testBigInt bigint)");
        this.stmt.executeUpdate(
                "INSERT INTO number_test (minBigInt,maxBigInt,testBigInt) values (" + Long.MIN_VALUE + "," + Long.MAX_VALUE + "," + TEST_BIGINT_VALUE + ")");
    }
}
