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
public class UpdatabilityTest
    extends BaseTestCase
{

    //~ Constructors ..........................................................

    public UpdatabilityTest(String name)
    {
        super(name);
    }

    //~ Methods ...............................................................

    public static void main(String[] args)
    {
        new UpdatabilityTest("testUpdatability").run();
    }

    public void setUp()
               throws Exception
    {
        super.setUp();
        createTestTable();
    }

    public void testUpdatability()
                       throws SQLException
    {

        Statement scrollableStmt = null;

        try {
        	scrollableStmt = conn.createStatement(
                                     ResultSet.TYPE_SCROLL_INSENSITIVE, 
                                     ResultSet.CONCUR_UPDATABLE);
                                     
        	rs = scrollableStmt.executeQuery("SELECT * FROM UPDATABLE ORDER BY pos1");
            
            int numCols = rs.getMetaData().getColumnCount();

            while (rs.next()) {
                int rowPos = rs.getInt(1);
                rs.updateString(3, "New Data" + (100 - rowPos));
                rs.updateRow();
            
            }

            //
            // Insert a new row
            //
            
            rs.moveToInsertRow();
            rs.updateInt(1, 400);
            rs.updateInt(2, 400);
            rs.updateString(3, "New Data" + (100 - 400));
            rs.insertRow();
            rs.close();
            
            rs = scrollableStmt.executeQuery("SELECT * FROM UPDATABLE ORDER BY pos1");

            boolean dataGood = true;
          
            while (rs.next()) {
                int rowPos = rs.getInt(1);

                if (!rs.getString(3).equals("New Data" + (100 - rowPos))) {
                    dataGood = false;
                }
            }

			assertTrue("Updates failed", dataGood);
			
            

			// move back, and change the primary key
			// This should work
			
			int newPrimaryKeyId = 99999;
			
            rs.absolute(1);
            rs.updateInt(1, newPrimaryKeyId);
            rs.updateRow();
            
            int savedPrimaryKeyId = rs.getInt(1);
            
            assertTrue("Updated primary key does not match", (newPrimaryKeyId == savedPrimaryKeyId));
            
            rs.close();
            
          
/*
           FIXME: Move to regression
           
            scrollableStmt.executeUpdate("DROP TABLE IF EXISTS test");
            scrollableStmt.executeUpdate("CREATE TABLE test (ident INTEGER PRIMARY KEY, name TINYTEXT, expiry DATETIME default null)");
            scrollableStmt.executeUpdate("INSERT INTO test SET ident=1, name='original'");

            //Select to get a resultset to work on
            ResultSet rs = stmt.executeQuery("SELECT ident, name, expiry FROM test");

            //Check that the expiry field was null before we did our update
            rs.first();

            java.sql.Date before = rs.getDate("expiry");

            if (rs.wasNull()) {
                System.out.println("Expiry was correctly SQL null before update");
            }

            //Update a different field
            rs.updateString("name", "Updated");
            rs.updateRow();

            //Test to see if field has been altered
            java.sql.Date after = rs.getDate(3);

            if (rs.wasNull())
                System.out.println("Bug disproved - expiry SQL null after update");
            else
                System.out.println("Bug proved - expiry corrupted to '" + 
                                   after + "'");
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

    private void createTestTable()
                          throws SQLException
    {

       //
            // Catch the error, the table might exist
            //
            try {
                stmt.executeUpdate("DROP TABLE UPDATABLE");
            } catch (SQLException SQLE) {
            }

            stmt.executeUpdate("CREATE TABLE UPDATABLE (pos1 int not null, pos2 int not null, char_field VARCHAR(32), PRIMARY KEY (pos1, pos2))");

            for (int i = 0; i < 100; i++) {
                stmt.executeUpdate("INSERT INTO UPDATABLE VALUES (" + i + 
                                   ", " + i + ",'StringData" + i + "')");
            }
    }
}