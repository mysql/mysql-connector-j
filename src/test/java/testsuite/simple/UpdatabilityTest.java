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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.mysql.cj.jdbc.exceptions.NotUpdatable;

import testsuite.BaseTestCase;

/**
 * Tests for updatable result sets
 */
public class UpdatabilityTest extends BaseTestCase {
    /**
     * Creates a new UpdatabilityTest object.
     * 
     * @param name
     */
    public UpdatabilityTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(UpdatabilityTest.class);
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        createTestTable();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            this.stmt.executeUpdate("DROP TABLE UPDATABLE");
        } catch (SQLException SQLE) {
        }
        super.tearDown();
    }

    /**
     * Tests if aliased tables work as updatable result sets.
     * 
     * @throws Exception
     *             if an error occurs
     */
    public void testAliasedTables() throws Exception {
        Statement scrollableStmt = null;

        try {
            scrollableStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            this.rs = scrollableStmt.executeQuery("SELECT pos1 AS p1, pos2 AS P2, char_field AS cf FROM UPDATABLE AS UPD LIMIT 1");
            this.rs.next();
            this.rs.close();
            this.rs = null;

            scrollableStmt.close();
            scrollableStmt = null;
        } finally {
            if (this.rs != null) {
                try {
                    this.rs.close();
                } catch (SQLException sqlEx) {
                    // ignore
                }

                this.rs = null;
            }

            if (scrollableStmt != null) {
                try {
                    scrollableStmt.close();
                } catch (SQLException sqlEx) {
                    // ignore
                }

                scrollableStmt = null;
            }
        }
    }

    /**
     * Tests that the driver does not let you update result sets that come from
     * tables that don't have primary keys
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public void testBogusTable() throws SQLException {
        this.stmt.executeUpdate("DROP TABLE IF EXISTS BOGUS_UPDATABLE");
        this.stmt.executeUpdate("CREATE TABLE BOGUS_UPDATABLE (field1 int)");

        Statement scrollableStmt = null;

        try {
            scrollableStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            this.rs = scrollableStmt.executeQuery("SELECT * FROM BOGUS_UPDATABLE");

            try {
                this.rs.moveToInsertRow();
                fail("ResultSet.moveToInsertRow() should not succeed on non-updatable table");
            } catch (NotUpdatable noUpdate) {
                // ignore
            }
        } finally {
            if (scrollableStmt != null) {
                try {
                    scrollableStmt.close();
                } catch (SQLException sqlEx) {
                    // ignore
                }
            }

            this.stmt.executeUpdate("DROP TABLE IF EXISTS BOGUS_UPDATABLE");
        }
    }

    /**
     * Tests that the driver does not let you update result sets that come from
     * queries that haven't selected all primary keys
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public void testMultiKeyTable() throws SQLException {
        this.stmt.executeUpdate("DROP TABLE IF EXISTS MULTI_UPDATABLE");
        this.stmt.executeUpdate("CREATE TABLE MULTI_UPDATABLE (field1 int NOT NULL, field2 int NOT NULL, PRIMARY KEY (field1, field2))");

        Statement scrollableStmt = null;

        try {
            scrollableStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            this.rs = scrollableStmt.executeQuery("SELECT field1 FROM MULTI_UPDATABLE");

            try {
                this.rs.moveToInsertRow();
                fail("ResultSet.moveToInsertRow() should not succeed on query that does not select all primary keys");
            } catch (NotUpdatable noUpdate) {
                // ignore
            }
        } finally {
            if (scrollableStmt != null) {
                try {
                    scrollableStmt.close();
                } catch (SQLException sqlEx) {
                    // ignore
                }
            }

            this.stmt.executeUpdate("DROP TABLE IF EXISTS MULTI_UPDATABLE");
        }
    }

    public void testUpdatability() throws SQLException {
        Statement scrollableStmt = null;

        try {
            scrollableStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            this.rs = scrollableStmt.executeQuery("SELECT * FROM UPDATABLE ORDER BY pos1");

            this.rs.getMetaData().getColumnCount();

            while (this.rs.next()) {
                int rowPos = this.rs.getInt(1);
                this.rs.updateString(3, "New Data" + (100 - rowPos));
                this.rs.updateRow();
            }

            //
            // Insert a new row
            //
            this.rs.moveToInsertRow();
            this.rs.updateInt(1, 400);
            this.rs.updateInt(2, 400);
            this.rs.updateString(3, "New Data" + (100 - 400));
            this.rs.insertRow();

            // Test moveToCurrentRow
            int rememberedPosition = this.rs.getRow();
            this.rs.moveToInsertRow();
            this.rs.moveToCurrentRow();
            assertTrue("ResultSet.moveToCurrentRow() failed", this.rs.getRow() == rememberedPosition);
            this.rs.close();
            this.rs = scrollableStmt.executeQuery("SELECT * FROM UPDATABLE ORDER BY pos1");

            boolean dataGood = true;

            while (this.rs.next()) {
                int rowPos = this.rs.getInt(1);

                if (!this.rs.getString(3).equals("New Data" + (100 - rowPos))) {
                    dataGood = false;
                }
            }

            assertTrue("Updates failed", dataGood);

            // move back, and change the primary key
            // This should work
            int newPrimaryKeyId = 99999;
            this.rs.absolute(1);
            this.rs.updateInt(1, newPrimaryKeyId);
            this.rs.updateRow();

            int savedPrimaryKeyId = this.rs.getInt(1);
            assertTrue("Updated primary key does not match", (newPrimaryKeyId == savedPrimaryKeyId));

            // Check cancelRowUpdates()
            this.rs.absolute(1);

            int primaryKey = this.rs.getInt(1);
            int originalValue = this.rs.getInt(2);
            this.rs.updateInt(2, -3);
            this.rs.cancelRowUpdates();

            int newValue = this.rs.getInt(2);
            assertTrue("ResultSet.cancelRowUpdates() failed", newValue == originalValue);

            // Now check refreshRow()
            // Check cancelRowUpdates()
            this.rs.absolute(1);
            primaryKey = this.rs.getInt(1);
            this.stmt.executeUpdate("UPDATE UPDATABLE SET char_field='foo' WHERE pos1=" + primaryKey);
            this.rs.refreshRow();
            assertTrue("ResultSet.refreshRow failed", this.rs.getString("char_field").equals("foo"));

            // Now check deleteRow()
            this.rs.last();

            int oldLastRow = this.rs.getRow();
            this.rs.deleteRow();
            this.rs.last();
            assertTrue("ResultSet.deleteRow() failed", this.rs.getRow() == (oldLastRow - 1));
            this.rs.close();

            /*
             * FIXME: Move to regression
             * 
             * scrollableStmt.executeUpdate("DROP TABLE IF EXISTS test");
             * scrollableStmt.executeUpdate("CREATE TABLE test (ident INTEGER
             * PRIMARY KEY, name TINYTEXT, expiry DATETIME default null)");
             * scrollableStmt.executeUpdate("INSERT INTO test SET ident=1,
             * name='original'");
             * 
             * //Select to get a resultset to work on ResultSet this.rs =
             * this.stmt.executeQuery("SELECT ident, name, expiry FROM test");
             * 
             * //Check that the expiry field was null before we did our update
             * this.rs.first();
             * 
             * java.sql.Date before = this.rs.getDate("expiry");
             * 
             * if (this.rs.wasNull()) { System.out.println("Expiry was correctly
             * SQL null before update"); }
             * 
             * //Update a different field this.rs.updateString("name",
             * "Updated"); this.rs.updateRow();
             * 
             * //Test to see if field has been altered java.sql.Date after =
             * this.rs.getDate(3);
             * 
             * if (this.rs.wasNull()) System.out.println("Bug disproved - expiry
             * SQL null after update"); else System.out.println("Bug proved -
             * expiry corrupted to '" + after + "'");
             */
        } finally {
            if (scrollableStmt != null) {
                try {
                    scrollableStmt.close();
                } catch (SQLException sqlEx) {
                }
            }
        }
    }

    private void createTestTable() throws SQLException {
        //
        // Catch the error, the table might exist
        //
        try {
            this.stmt.executeUpdate("DROP TABLE UPDATABLE");
        } catch (SQLException SQLE) {
        }

        this.stmt.executeUpdate("CREATE TABLE UPDATABLE (pos1 int not null, pos2 int not null, char_field VARCHAR(32), PRIMARY KEY (pos1, pos2))");

        for (int i = 0; i < 100; i++) {
            this.stmt.executeUpdate("INSERT INTO UPDATABLE VALUES (" + i + ", " + i + ",'StringData" + i + "')");
        }
    }
}
