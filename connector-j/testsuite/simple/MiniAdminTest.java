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

    public MiniAdminTest(String name) {
        super(name);
    }
    
    /**
     * Runs tests for this testsuite.
     */
	public static void main(String[] args) throws Exception {
        new MiniAdminTest("testUrlConstructor").run();
        new MiniAdminTest("testShutdown").run();
	}
    
    /**
     * Tests whether or not you can construct a MiniAdmin
     * with a JDBC URL.
     */
    public void testUrlConstructor() throws Exception {
        new MiniAdmin(dbUrl);
    }
    
    /**
     * Tests whether or not you can shutdown the server with
     * MiniAdmin
     */
    public void testShutdown() throws Exception {
        new MiniAdmin(conn).shutdown();
    }
}
