/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package testsuite.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serializable;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

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
     * Tests Util.isJdbcInterface()
     */
    @Test
    public void testIsJdbcInterface() {
        // Classes directly or indirectly implementing JDBC interfaces.
        assertTrue(Util.isJdbcInterface(ClientPreparedStatement.class));
        assertTrue(Util.isJdbcInterface(StatementImpl.class));
        assertTrue(Util.isJdbcInterface(JdbcStatement.class));
        assertTrue(Util.isJdbcInterface(ResultSetImpl.class));
        JdbcStatement s = (JdbcStatement) Proxy.newProxyInstance(this.getClass().getClassLoader(), new Class<?>[] { JdbcStatement.class },
                (proxy, method, args) -> null);
        assertTrue(Util.isJdbcInterface(s.getClass()));

        // Classes not implementing JDBC interfaces.
        assertFalse(Util.isJdbcInterface(Util.class));
        assertFalse(Util.isJdbcInterface(UtilsTest.class));
    }

    /**
     * Tests Util.isJdbcPackage()
     */
    @Test
    public void testIsJdbcPackage() {
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
     */
    @Test
    public void testGetImplementedInterfaces() {
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
    @Test
    public void testGetPackageName() {
        assertEquals(MultiHostConnectionProxy.class.getPackage().getName(), Util.getPackageName(MultiHostConnectionProxy.class));
        assertEquals(JdbcConnection.class.getPackage().getName(), Util.getPackageName(this.conn.getClass().getInterfaces()[0]));
    }

}
