/*
 * Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.
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

public class TransactionTest extends BaseTestCase {
    private static final double DOUBLE_CONST = 25.4312;

    private static final double EPSILON = .0000001;

    /**
     * Creates a new TransactionTest object.
     * 
     * @param name
     */
    public TransactionTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(TransactionTest.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    public void testTransaction() throws SQLException {
        try {
            createTable("trans_test", "(id INT NOT NULL PRIMARY KEY, decdata DOUBLE)", "InnoDB");
            this.conn.setAutoCommit(false);
            this.stmt.executeUpdate("INSERT INTO trans_test (id, decdata) VALUES (1, 1.0)");
            this.conn.rollback();
            this.rs = this.stmt.executeQuery("SELECT * from trans_test");

            boolean hasResults = this.rs.next();
            assertTrue("Results returned, rollback to empty table failed", (hasResults != true));
            this.stmt.executeUpdate("INSERT INTO trans_test (id, decdata) VALUES (2, " + DOUBLE_CONST + ")");
            this.conn.commit();
            this.rs = this.stmt.executeQuery("SELECT * from trans_test where id=2");
            hasResults = this.rs.next();
            assertTrue("No rows in table after INSERT", hasResults);

            double doubleVal = this.rs.getDouble(2);
            double delta = Math.abs(DOUBLE_CONST - doubleVal);
            assertTrue("Double value returned != " + DOUBLE_CONST, (delta < EPSILON));
        } finally {
            this.conn.setAutoCommit(true);
        }
    }
}
