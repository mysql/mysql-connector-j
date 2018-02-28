/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package testsuite.simple;

import com.mysql.cj.admin.ServerController;
import com.mysql.cj.conf.PropertyDefinitions;

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
