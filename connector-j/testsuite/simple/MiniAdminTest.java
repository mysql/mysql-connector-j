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

import com.mysql.jdbc.MiniAdmin;

import testsuite.BaseTestCase;

/**
 * Testsuite for MiniAdmin functionality.
 * 
 * @author Mark Matthews
 */
public class MiniAdminTest extends BaseTestCase {

    /** 
     * The system property that must exist to run the shutdown
     * test
     */
    
    private static final String SHUTDOWN_PROP = 
        "com.mysql.jdbc.testsuite.MiniAdminTest.runShutdown";
        
    /**
     * Creates a new test case
     * 
     * @param name the test to run
     */
    public MiniAdminTest(String name) {
        super(name);
    }
    
    /**
     * Runs tests for this testsuite.
     * 
     * @param args ignored
     * @throws Exception if an error occurs.
     */
	public static void main(String[] args) throws Exception {
        new MiniAdminTest("testUrlConstructor").run();
        new MiniAdminTest("testShutdown").run();
	}
    
    /**
     * Tests whether or not you can construct a MiniAdmin
     * with a JDBC URL.
     * 
     * @throws Exception if an error occurs
     */
    public void testUrlConstructor() throws Exception {
        new MiniAdmin(dbUrl);
    }
    
    /**
     * Tests whether or not you can shutdown the server with
     * MiniAdmin.
     * 
     * Only runs if SHUTDOWN_PROP is defined.
     * 
     * @throws Exception if an error occurs
     */
    public void testShutdown() throws Exception {
       if (runTestIfSysPropDefined(SHUTDOWN_PROP)) {
            new MiniAdmin(conn).shutdown();
        }
    }
}
