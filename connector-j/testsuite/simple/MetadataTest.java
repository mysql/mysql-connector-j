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

    //~ Constructors ..........................................................

    public MetadataTest(String name)
    {
        super(name);
    }

    //~ Methods ...............................................................

    public static void main(String[] args)
    {
    	new MetadataTest("testGetPrimaryKeys").run();
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
            assertTrue("Primary Key not returned correctly ('" + 
                       pkColumnName + "' != 'parent_id')", 
                       pkColumnName.equalsIgnoreCase("parent_id"));
            assertTrue("Foreign Key not returned correctly ('" + 
                       fkColumnName + "' != 'parent_id_fk')", 
                       fkColumnName.equalsIgnoreCase("parent_id_fk"));
        }

        rs.close();
        rs = dbmd.getExportedKeys(null, null, "parent");

        while (rs.next()) {

            String pkColumnName = rs.getString("PKCOLUMN_NAME");
            String fkColumnName = rs.getString("FKCOLUMN_NAME");
            String fkTableName  = rs.getString("FKTABLE_NAME");
            
            assertTrue("Primary Key not returned correctly ('" + 
                       pkColumnName + "' != 'parent_id')", 
                       pkColumnName.equalsIgnoreCase("parent_id"));
                       
                       
            assertTrue("Foreign Key table not returned correctly for getExportedKeys ('" + 
                       fkTableName + "' != 'child')", 
                       fkTableName.equalsIgnoreCase("child"));
                       
            assertTrue("Foreign Key not returned correctly for getExportedKeys ('" + 
                       fkColumnName + "' != 'parent_id_fk')", 
                       fkColumnName.equalsIgnoreCase("parent_id_fk"));
        }

        rs.close();
        
        rs = dbmd.getCrossReference(null, null, "parent", null, null, "child");
    }

    public void testGetPrimaryKeys()
                            throws SQLException
    {

        try {

            DatabaseMetaData dbmd = conn.getMetaData();
            rs = dbmd.getPrimaryKeys(conn.getCatalog(), "", "multikey");
			short[] keySeqs = new short[4];
			String[] columnNames = new String[4];
			int i = 0;
			
            while (rs.next()) {
            	String tableName = rs.getString("TABLE_NAME");
            	columnNames[i] = rs.getString("COLUMN_NAME");
            	String pkName = rs.getString("PK_NAME");
            	keySeqs[i] = rs.getShort("KEY_SEQ");
            	i++;
            }
            
            if (keySeqs[0] != 3 &&
            	keySeqs[1] != 2 &&
            	keySeqs[2] != 4 &&
            	keySeqs[4] != 1) {
            		fail("Keys returned in wrong order");
            }
            
        } finally {

            if (rs != null) {

                try {
                    rs.close();
                } catch (SQLException sqlEx) { /* ignore */
                }
            }
        }
    }

    private void createTestTable()
                          throws SQLException
    {

        try {
            stmt.executeUpdate("DROP TABLE parent");
            stmt.executeUpdate("DROP TABLE child");
            stmt.executeUpdate("DROP TABLE multikey");
        } catch (SQLException sqlEx) {
        }

        stmt.executeUpdate(
                "CREATE TABLE parent(parent_id INT NOT NULL, PRIMARY KEY (parent_id)) TYPE=INNODB");
        stmt.executeUpdate(
                "CREATE TABLE child(child_id INT, parent_id_fk INT, INDEX par_ind (parent_id_fk), FOREIGN KEY (parent_id_fk) REFERENCES parent(parent_id)) TYPE=INNODB");
        stmt.executeUpdate(
        		"CREATE TABLE multikey(d INT NOT NULL, b INT NOT NULL, a INT NOT NULL, c INT NOT NULL, PRIMARY KEY (d, b, a, c))");
        		
    }
}