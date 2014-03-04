/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

import testsuite.BaseTestCase;

/**
 * Tests SSL functionality in the driver.
 * 
 * @author Mark Matthews
 */
public class SSLTest extends BaseTestCase {
	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * Constructor for SSLTest.
	 * 
	 * @param name
	 *            the name of the test to run.
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

	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(SSLTest.class);
	}

	/**
	 * Tests SSL Connection
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void testConnect() throws Exception {
		System.out.println("<<<<<<<<<<< Look for SSL debug output >>>>>>>>>>>");
	}
}
