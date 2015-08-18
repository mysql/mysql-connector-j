/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import testsuite.BaseTestCase;

/**
 * Tests result set traversal methods.
 */
public class TraversalTest extends BaseTestCase {

    /**
     * Creates a new TraversalTest object.
     * 
     * @param name
     */
    public TraversalTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(TraversalTest.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createTestTable();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE TRAVERSAL");
        } catch (SQLException SQLE) {
        }
        super.tearDown();
    }

    public void testTraversal() throws SQLException {

        Statement scrollableStmt = null;

        try {
            scrollableStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            this.rs = scrollableStmt.executeQuery("SELECT * FROM TRAVERSAL ORDER BY pos");

            // Test isFirst()
            if (this.rs.first()) {
                assertTrue("ResultSet.isFirst() failed", this.rs.isFirst());
                this.rs.relative(-1);
                assertTrue("ResultSet.isBeforeFirst() failed", this.rs.isBeforeFirst());
            }

            // Test isLast()
            if (this.rs.last()) {
                assertTrue("ResultSet.isLast() failed", this.rs.isLast());
                this.rs.relative(1);
                assertTrue("ResultSet.isAfterLast() failed", this.rs.isAfterLast());
            }

            int count = 0;
            this.rs.beforeFirst();

            boolean forwardOk = true;

            while (this.rs.next()) {

                int pos = this.rs.getInt("POS");

                // test case-sensitive column names
                pos = this.rs.getInt("pos");
                pos = this.rs.getInt("Pos");
                pos = this.rs.getInt("POs");
                pos = this.rs.getInt("PoS");
                pos = this.rs.getInt("pOS");
                pos = this.rs.getInt("pOs");
                pos = this.rs.getInt("poS");

                if (pos != count) {
                    forwardOk = false;
                }

                assertTrue("ResultSet.getRow() failed.", pos == (this.rs.getRow() - 1));

                count++;

            }

            assertTrue("Only traversed " + count + " / 100 rows", forwardOk);

            boolean isAfterLast = this.rs.isAfterLast();
            assertTrue("ResultSet.isAfterLast() failed", isAfterLast);
            this.rs.afterLast();

            // Scroll backwards
            count = 99;

            boolean reverseOk = true;

            while (this.rs.previous()) {

                int pos = this.rs.getInt("pos");

                if (pos != count) {
                    reverseOk = false;
                }

                count--;
            }

            assertTrue("ResultSet.previous() failed", reverseOk);

            boolean isBeforeFirst = this.rs.isBeforeFirst();
            assertTrue("ResultSet.isBeforeFirst() failed", isBeforeFirst);

            this.rs.next();
            boolean isFirst = this.rs.isFirst();
            assertTrue("ResultSet.isFirst() failed", isFirst);

            // Test absolute positioning
            this.rs.absolute(50);
            int pos = this.rs.getInt("pos");
            assertTrue("ResultSet.absolute() failed", pos == 49);

            // Test relative positioning
            this.rs.relative(-1);
            pos = this.rs.getInt("pos");
            assertTrue("ResultSet.relative(-1) failed", pos == 48);

            // Test bogus absolute index
            boolean onResultSet = this.rs.absolute(200);
            assertTrue("ResultSet.absolute() to point off result set failed", onResultSet == false);
            onResultSet = this.rs.absolute(100);
            assertTrue("ResultSet.absolute() from off this.rs to on this.rs failed", onResultSet);

            onResultSet = this.rs.absolute(-99);
            assertTrue("ResultSet.absolute(-99) failed", onResultSet);
            assertTrue("ResultSet absolute(-99) failed", this.rs.getInt(1) == 1);
        } finally {

            if (scrollableStmt != null) {

                try {
                    scrollableStmt.close();
                } catch (SQLException sqlEx) {
                    // ignore
                }
            }
        }
    }

    private void createTestTable() throws SQLException {
        //
        // Catch the error, the table might exist
        //
        try {
            this.stmt.executeUpdate("DROP TABLE TRAVERSAL");
        } catch (SQLException SQLE) {
            // ignore
        }

        this.stmt.executeUpdate("CREATE TABLE TRAVERSAL (pos int PRIMARY KEY, stringdata CHAR(32))");

        for (int i = 0; i < 100; i++) {
            this.stmt.executeUpdate("INSERT INTO TRAVERSAL VALUES (" + i + ", 'StringData')");
        }
    }
}