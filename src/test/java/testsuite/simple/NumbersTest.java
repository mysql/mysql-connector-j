/*
 * Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.
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
