/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.simple.jdbc42;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.mysql.jdbc.Driver;
import com.mysql.jdbc.MySQLConnection;

import testsuite.BaseTestCase;

public class ConnectionTest extends BaseTestCase {
    public ConnectionTest(String name) {
        super(name);
    }

    /**
     * Test for Driver.acceptsURL() behavior clarification:
     * - acceptsURL() throws SQLException if URL is null.
     */
    public void testDriverAcceptsURLNullArgument() {
        assertThrows(SQLException.class, "The url cannot be null", new Callable<Void>() {
            public Void call() throws Exception {
                Driver mysqlDriver = new Driver();
                mysqlDriver.acceptsURL(null);
                return null;
            }
        });
    }

    /**
     * Test for Driver.connect() behavior clarifications:
     * - connect() throws SQLException if URL is null.
     */
    public void testDriverConnectNullArgument() throws Exception {
        assertThrows(SQLException.class, "The url cannot be null", new Callable<Void>() {
            public Void call() throws Exception {
                Driver mysqlDriver = new Driver();
                mysqlDriver.connect(null, null);
                return null;
            }
        });

        assertThrows(SQLException.class, "The url cannot be null", new Callable<Void>() {
            public Void call() throws Exception {
                DriverManager.getConnection(null);
                return null;
            }
        });
    }

    /**
     * Test for Driver.connect() behavior clarifications:
     * - connect() properties precedence is implementation-defined.
     */
    public void testDriverConnectPropertiesPrecedence() throws Exception {
        assertThrows(SQLException.class, "Access denied for user 'dummy'@'localhost' \\(using password: YES\\)", new Callable<Void>() {
            public Void call() throws Exception {
                DriverManager.getConnection(BaseTestCase.dbUrl, "dummy", "dummy");
                return null;
            }
        });

        // make sure the connection string doesn't contain 'maxRows'
        String testUrl = BaseTestCase.dbUrl;
        int b = testUrl.indexOf("maxRows");
        if (b != -1) {
            int e = testUrl.indexOf('&', b);
            if (e == -1) {
                e = testUrl.length();
                b--;
            } else {
                e++;
            }
            testUrl = testUrl.substring(0, b) + testUrl.substring(e, testUrl.length());
        }

        Properties props = new Properties();
        props.setProperty("maxRows", "123");

        // Default property value.
        MySQLConnection testConn = (MySQLConnection) DriverManager.getConnection(testUrl);
        assertEquals(-1, testConn.getMaxRows());
        testConn = (MySQLConnection) DriverManager.getConnection(testUrl, new Properties());
        assertEquals(-1, testConn.getMaxRows());

        // Property in properties only.
        testConn = (MySQLConnection) DriverManager.getConnection(testUrl, props);
        assertEquals(123, testConn.getMaxRows());

        testUrl += (testUrl.indexOf('?') == -1 ? "?" : "&") + "maxRows=321";

        // Property in URL only.
        testConn = (MySQLConnection) DriverManager.getConnection(testUrl);
        assertEquals(321, testConn.getMaxRows());
        testConn = (MySQLConnection) DriverManager.getConnection(testUrl, new Properties());
        assertEquals(321, testConn.getMaxRows());

        // Property in both.
        testConn = (MySQLConnection) DriverManager.getConnection(testUrl, props);
        assertEquals(123, testConn.getMaxRows());
    }

    /**
     * Test for REF_CURSOR support checking.
     */
    public void testSupportsRefCursors() throws Exception {
        assertFalse(this.conn.getMetaData().supportsRefCursors());
    }
}
