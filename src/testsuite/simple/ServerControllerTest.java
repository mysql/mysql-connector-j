/*
 Copyright (C) 2002-2004 MySQL AB

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

import com.mysql.jdbc.util.ServerController;

/**
 * Demonstrates usage of the ServerController class.
 * 
 * @author Mark Matthews
 * @version $Id: ServerControllerTest.java,v 1.1.2.1 2005/05/13 18:58:37
 *          mmatthews Exp $
 */
public class ServerControllerTest extends BaseTestCase {

	private String baseDir;

	/**
	 * Creates a ServerControllerTest testcase.
	 * 
	 * @param name
	 *            the name of the test to run.
	 */
	public ServerControllerTest(String name) {
		super(name);

		this.baseDir = System
				.getProperty("com.mysql.jdbc.test.ServerController.basedir");
	}

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(ServerControllerTest.class);
	}

	/**
	 * Demonstrates usage of the ServerController class.
	 * 
	 * This test is only run if the property
	 * 'com.mysql.jdbc.test.ServerController.basedir' is set.
	 * 
	 * @throws Exception
	 *             if an error occurs.
	 */
	public void testServerController() throws Exception {

		if (this.baseDir != null) {
			System.out.println("Starting server @ " + this.baseDir);

			ServerController controller = new ServerController(this.baseDir);
			System.out.println(controller.start());
			System.out.println("Hit enter to stop server....");
			System.in.read();
			controller.stop(true);

		}
	}
}
