/*
   Copyright (C) 2002 MySQL AB
   
      This program is free software; you can redistribute it and/or modify
      it under the terms of the GNU General Public License as published by
      the Free Software Foundation; either version 2 of the License, or
      (at your option) any later version.
   
      This program is distributed in the hope that it will be useful,
      but WITHOUT ANY WARRANTY; without even the implied warranty of
      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
      GNU General Public License for more details.
   
      You should have received a copy of the GNU General Public License
      along with this program; if not, write to the Free Software
      Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
      
 */
package testsuite.simple;

import java.sql.SQLException;

import testsuite.BaseTestCase;


/** 
 *
 * @author  Mark Matthews
 * @version $Id$
 */
public class TransactionTest
    extends BaseTestCase {

    //~ Instance/static variables .............................................

    private static final double DOUBLE_CONST = 25.4312;
    private static final double EPSILON = .0000001;

    //~ Constructors ..........................................................

    /**
     * Creates a new TransactionTest object.
     * 
     * @param name DOCUMENT ME!
     */
    public TransactionTest(String name) {
        super(name);
    }

    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @param args DOCUMENT ME!
     */
    public static void main(String[] args) {
        new TransactionTest("testTransaction").run();
    }

    /**
     * DOCUMENT ME!
     * 
     * @throws Exception DOCUMENT ME!
     */
    public void setUp()
               throws Exception {
        super.setUp();
        createTestTable();
    }

    /**
     * DOCUMENT ME!
     * 
     * @throws SQLException DOCUMENT ME!
     */
    public void testTransaction()
                         throws SQLException {

        try {
            conn.setAutoCommit(false);
            stmt.executeUpdate(
                    "INSERT INTO trans_test (id, decdata) VALUES (1, 1.0)");
            conn.rollback();
            rs = stmt.executeQuery("SELECT * from trans_test");

            boolean hasResults = rs.next();
            assertTrue("Results returned, rollback to empty table failed", 
                       (hasResults != true));
            stmt.executeUpdate(
                    "INSERT INTO trans_test (id, decdata) VALUES (2, "
                    + DOUBLE_CONST + ")");
            conn.commit();
            rs = stmt.executeQuery("SELECT * from trans_test where id=2");
            hasResults = rs.next();
            assertTrue("No rows in table after INSERT", hasResults);

            double doubleVal = rs.getDouble(2);
            double delta = Math.abs(DOUBLE_CONST - doubleVal);
            assertTrue("Double value returned != " + DOUBLE_CONST, 
                       (delta < EPSILON));
        } finally {
            conn.setAutoCommit(true);
        }
    }

    private void createTestTable()
                          throws SQLException {

        //
        // Catch the error, the table might exist
        //
        try {
            stmt.executeUpdate("DROP TABLE trans_test");
        } /* ignore */ catch (SQLException sqlEx) {
            ;
        }

        stmt.executeUpdate(
                "CREATE TABLE trans_test (id INT NOT NULL PRIMARY KEY, decdata DOUBLE) TYPE=InnoDB");
    }
}