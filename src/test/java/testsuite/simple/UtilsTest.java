/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;

import com.mysql.cj.jdbc.ClientPreparedStatement;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcStatement;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.ha.MultiHostConnectionProxy;
import com.mysql.cj.jdbc.result.ResultSetImpl;
import com.mysql.cj.util.Util;

import testsuite.BaseTestCase;

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
        assertTrue(Util.isJdbcInterface(ClientPreparedStatement.class));
        assertTrue(Util.isJdbcInterface(StatementImpl.class));
        assertTrue(Util.isJdbcInterface(JdbcStatement.class));
        assertTrue(Util.isJdbcInterface(ResultSetImpl.class));
        JdbcStatement s = (JdbcStatement) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class<?>[] { JdbcStatement.class },
                new InvocationHandler() {
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
        assertTrue(Util.isJdbcPackage("com.mysql.cj.jdbc"));
        assertTrue(Util.isJdbcPackage("com.mysql.cj.jdbc.admin"));
        assertTrue(Util.isJdbcPackage("com.mysql.cj.jdbc.exceptions"));
        assertTrue(Util.isJdbcPackage("com.mysql.cj.jdbc.ha"));
        assertTrue(Util.isJdbcPackage("com.mysql.cj.jdbc.interceptors"));
        assertTrue(Util.isJdbcPackage("com.mysql.cj.jdbc.jxm"));
        assertTrue(Util.isJdbcPackage("com.mysql.cj.jdbc.util"));

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
        ifaces = Util.getImplementedInterfaces(JdbcStatement.class);
        assertEquals(2, ifaces.length);
        assertEquals(ifaces[0], java.sql.Statement.class);

        ifaces = Util.getImplementedInterfaces(StatementImpl.class);
        assertEquals(1, ifaces.length);
        assertEquals(ifaces[0], JdbcStatement.class);

        ifaces = Util.getImplementedInterfaces(ConnectionImpl.class);
        assertEquals(3, ifaces.length);
        List<Class<?>> ifacesList = Arrays.asList(ifaces);
        for (Class<?> clazz : new Class<?>[] { JdbcConnection.class, Serializable.class }) {
            assertTrue(ifacesList.contains(clazz));
        }
    }

    /**
     * Tests Util.getPackageName()
     */
    public void testGetPackageName() {
        assertEquals(MultiHostConnectionProxy.class.getPackage().getName(), Util.getPackageName(MultiHostConnectionProxy.class));
        assertEquals(JdbcConnection.class.getPackage().getName(), Util.getPackageName(this.conn.getClass().getInterfaces()[0]));
    }
}
