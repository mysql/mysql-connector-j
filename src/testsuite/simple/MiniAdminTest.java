/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

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

import com.mysql.jdbc.MiniAdmin;

/**
 * Testsuite for MiniAdmin functionality.
 * 
 * @author Mark Matthews
 */
public class MiniAdminTest extends BaseTestCase {
	// ~ Static fields/initializers
	// ---------------------------------------------

	/**
	 * The system property that must exist to run the shutdown test
	 */
	private static final String SHUTDOWN_PROP = "com.mysql.jdbc.testsuite.MiniAdminTest.runShutdown";

	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * Creates a new test case
	 * 
	 * @param name
	 *            the test to run
	 */
	public MiniAdminTest(String name) {
		super(name);
	}

	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(MiniAdminTest.class);
	}

	/**
	 * Tests whether or not you can shutdown the server with MiniAdmin.
	 * 
	 * Only runs if SHUTDOWN_PROP is defined.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void testShutdown() throws Exception {
		if (runTestIfSysPropDefined(SHUTDOWN_PROP)) {
			new MiniAdmin(this.conn).shutdown();
		}
	}

	/**
	 * Tests whether or not you can construct a MiniAdmin with a JDBC URL.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void testUrlConstructor() throws Exception {
		new MiniAdmin(dbUrl);
	}
}
