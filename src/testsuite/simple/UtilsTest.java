/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;

import testsuite.BaseTestCase;

import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.ConnectionProperties;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.ResultSetImpl;
import com.mysql.jdbc.Statement;
import com.mysql.jdbc.StatementImpl;
import com.mysql.jdbc.Util;

public class UtilsTest extends BaseTestCase {
    /**
     * Creates a new UtilsTest.
     * 
     * @param name
     *            the name of the test
     */
    public UtilsTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(UtilsTest.class);
    }

    /**
     * Tests Util.isJdbcInterface()
     * 
     * @throws Exception
     */
    public void testIsJdbcInterface() throws Exception {
        // Classes directly or indirectly implementing JDBC interfaces.
        assertTrue(Util.isJdbcInterface(PreparedStatement.class));
        assertTrue(Util.isJdbcInterface(StatementImpl.class));
        assertTrue(Util.isJdbcInterface(Statement.class));
        assertTrue(Util.isJdbcInterface(ResultSetImpl.class));
        Statement s = (Statement) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class<?>[] { Statement.class }, new InvocationHandler() {
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                return null;
            }
        });
        assertTrue(Util.isJdbcInterface(s.getClass()));

        // Classes not implementing JDBC interfaces.
        assertFalse(Util.isJdbcInterface(Util.class));
        assertFalse(Util.isJdbcInterface(UtilsTest.class));

    }

    /**
     * Tests Util.isJdbcPackage()
     * 
     * @throws Exception
     */
    public void testIsJdbcPackage() throws Exception {
        // JDBC packages.
        assertTrue(Util.isJdbcPackage("java.sql"));
        assertTrue(Util.isJdbcPackage("javax.sql"));
        assertTrue(Util.isJdbcPackage("javax.sql.rowset"));
        assertTrue(Util.isJdbcPackage("com.mysql.jdbc"));
        assertTrue(Util.isJdbcPackage("com.mysql.jdbc"));
        assertTrue(Util.isJdbcPackage("com.mysql.jdbc.jdbc2.optional"));

        // Non-JDBC packages.
        assertFalse(Util.isJdbcPackage("java"));
        assertFalse(Util.isJdbcPackage("java.lang"));
        assertFalse(Util.isJdbcPackage("com"));
        assertFalse(Util.isJdbcPackage("com.mysql"));
    }

    /**
     * Tests Util.isJdbcPackage()
     * 
     * @throws Exception
     */
    public void testGetImplementedInterfaces() throws Exception {
        Class<?>[] ifaces;
        ifaces = Util.getImplementedInterfaces(Statement.class);
        assertEquals(1, ifaces.length);
        assertEquals(ifaces[0], java.sql.Statement.class);

        ifaces = Util.getImplementedInterfaces(StatementImpl.class);
        assertEquals(1, ifaces.length);
        assertEquals(ifaces[0], Statement.class);

        ifaces = Util.getImplementedInterfaces(ConnectionImpl.class);
        assertEquals(3, ifaces.length);
        List<Class<?>> ifacesList = Arrays.asList(ifaces);
        for (Class<?> clazz : new Class<?>[] { MySQLConnection.class, Serializable.class, ConnectionProperties.class }) {
            assertTrue(ifacesList.contains(clazz));
        }
    }
}