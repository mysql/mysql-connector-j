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
import java.sql.DatabaseMetaData;
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
public class MetadataTest
    extends BaseTestCase
{

    //~ Instance/static variables .............................................

   

    //~ Constructors ..........................................................

    public MetadataTest(String name)
    {
        super(name);
    }

    //~ Methods ...............................................................

    public static void main(String[] args)
    {
        new MetadataTest("testForeignKeys").run();
    }

    public void setUp()
               throws Exception
    {
        super.setUp();
        createTestTable();
    }

    public void testForeignKeys()
                         throws SQLException
    {

        DatabaseMetaData dbmd = conn.getMetaData();
        rs = dbmd.getImportedKeys(null, null, "child");

        while (rs.next()) {
        	String pkColumnName = rs.getString("PKCOLUMN_NAME");
        	String fkColumnName = rs.getString("FKCOLUMN_NAME");
        	
        	assertTrue("Primary Key not returned correctly ('" + pkColumnName + "' != 'id')", pkColumnName.equalsIgnoreCase("id"));
        	assertTrue("Foreign Key not returned correctly ('" + fkColumnName + "' != 'parent_id')", fkColumnName.equalsIgnoreCase("parent_id"));
        }

        rs.close();
        rs = dbmd.getExportedKeys(null, null, "parent");

        while (rs.next()) {
        	String pkColumnName = rs.getString("PKCOLUMN_NAME");
        	String fkColumnName = rs.getString("FKCOLUMN_NAME");
        	
        	assertTrue("Primary Key not returned correctly ('" + pkColumnName + "' != 'id')", pkColumnName.equalsIgnoreCase("id"));
        	assertTrue("Foreign Key not returned correctly ('" + fkColumnName + "' != 'parent_id')", fkColumnName.equalsIgnoreCase("parent_id"));
        }


        rs.close();
    }

    private void createTestTable()
                          throws SQLException
    {

        try {
            stmt.executeUpdate("DROP TABLE parent");
            stmt.executeUpdate("DROP TABLE child");
        } catch (SQLException sqlEx) {
        }

        stmt.executeUpdate(
                "CREATE TABLE parent(id INT NOT NULL, PRIMARY KEY (id)) TYPE=INNODB");
        stmt.executeUpdate(
                "CREATE TABLE child(id INT, parent_id INT, INDEX par_ind (parent_id), FOREIGN KEY (parent_id) REFERENCES parent(id)) TYPE=INNODB");
    }
}