/*
 * Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.
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

import testsuite.BaseTestCase;

/**
 * Tests SSL functionality in the driver.
 */
public class SSLTest extends BaseTestCase {
    /**
     * Constructor for SSLTest.
     * 
     * @param name
     *            the name of the test to run.
     */
    public SSLTest(String name) {
        super(name);

        System.setProperty("javax.net.debug", "all");

        StringBuilder sslUrl = new StringBuilder(dbUrl);

        if (dbUrl.indexOf("?") == -1) {
            sslUrl.append("?");
        } else {
            sslUrl.append("&");
        }

        sslUrl.append("useSSL=true");
    }

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
