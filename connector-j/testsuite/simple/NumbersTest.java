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
public class NumbersTest
    extends BaseTestCase {

    //~ Instance/static variables .............................................

    private static final long TEST_BIGINT_VALUE = 6147483647L;

    //~ Constructors ..........................................................

    /**
     * Creates a new NumbersTest object.
     * 
     * @param name DOCUMENT ME!
     */
    public NumbersTest(String name) {
        super(name);
    }

    //~ Methods ...............................................................

    /**
     * DOCUMENT ME!
     * 
     * @param args DOCUMENT ME!
     */
    public static void main(String[] args) {
        new NumbersTest("testNumbers").run();
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
    public void testNumbers()
                     throws SQLException {
        rs = stmt.executeQuery("SELECT * from number_test");

        while (rs.next()) {

            long minBigInt = rs.getLong(1);
            long maxBigInt = rs.getLong(2);
            long testBigInt = rs.getLong(3);
            assertTrue("Minimum bigint not stored correctly", 
                       (minBigInt == Long.MIN_VALUE));
            assertTrue("Maximum bigint not stored correctly", 
                       (maxBigInt == Long.MAX_VALUE));
            assertTrue("Test bigint not stored correctly", 
                       (TEST_BIGINT_VALUE == testBigInt));
        }
    }

    private void createTestTable()
                          throws SQLException {

        try {
            stmt.executeUpdate("DROP TABLE number_test");
        } /* ignore */ catch (SQLException sqlEx) {
            ;
        }

        stmt.executeUpdate(
                "CREATE TABLE number_test (minBigInt bigint, maxBigInt bigint, testBigInt bigint)");
        stmt.executeUpdate(
                "INSERT INTO number_test (minBigInt,maxBigInt,testBigInt) values ("
                + Long.MIN_VALUE + "," + Long.MAX_VALUE + ","
                + TEST_BIGINT_VALUE + ")");
    }
}