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
 
package testsuite.perf;

import java.sql.PreparedStatement;

import testsuite.BaseTestCase;

/**
 * Simple performance testing unit test.
 * 
 * @author Mark Matthews
 */
public class LoadStorePerfTest extends BaseTestCase {

	/**
	 * Constructor for LoadStorePerfTest.
	 * @param name
	 */
	public LoadStorePerfTest(String name) {
       super(name);
	}

	public static void main(String[] args) throws Exception {
        new LoadStorePerfTest("test100Transactions").run();
	}
    
	/**
	 * @see junit.framework.TestCase#setUp()
	 */
	public void setUp() throws Exception {
		super.setUp();
        
        stmt.executeUpdate("DROP TABLE IF EXISTS perfLoadStore");
        stmt.executeUpdate("CREATE TABLE perfLoadStore (priKey INT NOT NULL AUTO_INCREMENT, charField char(20), PRIMARY KEY (priKey))");
        stmt.executeUpdate("INSERT INTO perfLoadStore (charField) VALUES ('blah')");
        
	}

	/**
	 * @see junit.framework.TestCase#tearDown()
	 */
	public void tearDown() throws Exception {
        stmt.executeUpdate("DROP TABLE IF EXISTS perfLoadStore");
        
		super.tearDown();
	}
    
    public void test100Transactions() throws Exception {
        PreparedStatement pStmt = conn.prepareStatement("UPDATE perfLoadStore SET priKey=?, charField=? where priKey=?");
        long begin = System.currentTimeMillis();
        
        for (int i = 0; i < 1000; i++) {
            conn.setAutoCommit(false);
            rs = stmt.executeQuery("SELECT COUNT(*) FROM perfLoadStore WHERE priKey=1");
            
            while (rs.next()) {
                 int key = rs.getInt(1);
            }
            
            rs.close();
            
            rs = stmt.executeQuery("SELECT priKey, charField FROM perfLoadStore");
            
            
            
            while (rs.next()) {
                int key = rs.getInt(1);
                String field = rs.getString(2);
                
                pStmt.setInt(1, key);
                pStmt.setInt(3, key);
                pStmt.setString(2, field);
                pStmt.executeUpdate();
                
            }
            
            rs.close();
            
            conn.commit();
            conn.setAutoCommit(true);   
            
        }
        
        pStmt.close();
        
        long end = System.currentTimeMillis();
        
        long timeElapsed = (end - begin) / 1000;
        
        double tps = 10000/ timeElapsed;
        
        System.out.println(tps);
    }
            

}
