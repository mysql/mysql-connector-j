/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

import com.mysql.cj.core.admin.ServerController;
import com.mysql.cj.core.conf.PropertyDefinitions;

import testsuite.BaseTestCase;

/**
 * Demonstrates usage of the ServerController class.
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

        this.baseDir = System.getProperty(PropertyDefinitions.SYSP_testsuite_serverController_basedir);
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
