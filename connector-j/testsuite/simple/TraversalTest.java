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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import testsuite.BaseTestCase;


/** 
 *
 * @author  Administrator
 * @version 
 */
public class TraversalTest
    extends BaseTestCase
{

    //~ Constructors ..........................................................

    public TraversalTest(String name)
    {
        super(name);
    }

    //~ Methods ...............................................................

    public static void main(String[] args)
    {
        new TraversalTest("testTransaction").run();
    }

    public void setUp()
               throws Exception
    {
        super.setUp();
        createTestTable();
    }

    public void testTraversal()
                       throws SQLException
    {

        Statement scrollableStmt = null;

        try {
            scrollableStmt = conn.createStatement(
                                     ResultSet.TYPE_SCROLL_SENSITIVE, 
                                     ResultSet.CONCUR_READ_ONLY);
            rs             = scrollableStmt.executeQuery(
                                     "SELECT * FROM TRAVERSAL ORDER BY pos");

            int count      = 0;
            rs.beforeFirst();

            boolean forwardOk = true;

            while (rs.next()) {

                int pos = rs.getInt("POS");

                // test case-sensitive column names
                pos = rs.getInt("pos");
                pos = rs.getInt("Pos");
                pos = rs.getInt("POs");
                pos = rs.getInt("PoS");
                pos = rs.getInt("pOS");
                pos = rs.getInt("pOs");
                pos = rs.getInt("poS");

                if (pos != count) {
                    forwardOk = false;
                }

                count++;
            }

            assertTrue("Only traversed " + count + " / 100 rows", forwardOk);

            boolean isAfterLast = rs.isAfterLast();
            assertTrue("ResultSet.isAfterLast() failed", isAfterLast);
            rs.afterLast();

            // Scroll backwards
            count = 99;

            boolean reverseOk = true;

            while (rs.previous()) {

                int pos = rs.getInt("pos");

                if (pos != count) {
                    reverseOk = false;
                }
			
                count--;
            }

            assertTrue("ResultSet.previous() failed", reverseOk);

            boolean isFirst = rs.isFirst();
            assertTrue("ResultSet.isFirst() failed", isFirst);
            rs.absolute(50);

            int pos = rs.getInt("pos");
            assertTrue("ResultSet.absolute() failed", pos == 49);

            boolean onResultSet = rs.absolute(200);
            assertTrue("ResultSet.absolute() to point off result set failed", 
                       onResultSet == false);
            onResultSet = rs.absolute(100);
            assertTrue("ResultSet.absolute() from off rs to on rs failed", 
                       onResultSet);
            onResultSet = rs.absolute(-99);
            assertTrue("ResultSet.absolute(-99) failed", onResultSet);
            assertTrue("ResultSet absolute(-99) failed", rs.getInt(1) == 1);
        } finally {

            if (scrollableStmt != null) {

                try {
                    scrollableStmt.close();
                } catch (SQLException sqlEx) {
                }
            }
        }
    }

    private void createTestTable()
                          throws SQLException
    {

        //
        // Catch the error, the table might exist
        //
        try {
            stmt.executeUpdate("DROP TABLE TRAVERSAL");
        } catch (SQLException SQLE) {
        }

        stmt.executeUpdate(
                "CREATE TABLE TRAVERSAL (pos int PRIMARY KEY, stringdata CHAR(32))");

        for (int i = 0; i < 100; i++) {
            stmt.executeUpdate(
                    "INSERT INTO TRAVERSAL VALUES (" + i + 
                    ", 'StringData')");
        }
    }
}