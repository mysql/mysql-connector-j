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

import testsuite.BaseTestCase;

/**
 * Tests SSL functionality in the driver.
 * 
 * @author Mark Matthews
 */
public class SSLTest extends BaseTestCase {

	/**
	 * Constructor for SSLTest.
	 * @param name the name of the test to run.
	 */
	public SSLTest(String name) {
        super(name);
        
        System.setProperty("javax.net.debug", "all");
        
        StringBuffer sslUrl = new StringBuffer(dbUrl);
        
        if (dbUrl.indexOf("?") == -1) {
            sslUrl.append("?");
        } else {
            sslUrl.append("&");
        }
        
        sslUrl.append("useSSL=true");
            
		
	}

    /**
     * Runs all test cases
     * 
     * @param args ignored
     */
	public static void main(String[] args) {
        new SSLTest("testConnect").run();
	}
    
    /**
     * Tests SSL Connection
     * 
     * @throws Exception if an error occurs
     */
    public void testConnect() throws Exception {
        
        System.out.println("<<<<<<<<<<< Look for SSL debug output >>>>>>>>>>>");
    }
}
