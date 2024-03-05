/*
 * Copyright (c) 2002, 2024, Oracle and/or its affiliates.
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.PrintWriter;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.Name;
import javax.naming.NameParser;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;
import javax.sql.DataSource;
import javax.sql.PooledConnection;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.conf.BooleanPropertyDefinition;
import com.mysql.cj.conf.EnumPropertyDefinition;
import com.mysql.cj.conf.IntegerPropertyDefinition;
import com.mysql.cj.conf.LongPropertyDefinition;
import com.mysql.cj.conf.MemorySizePropertyDefinition;
import com.mysql.cj.conf.PropertyDefinition;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.StringPropertyDefinition;
import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;
import com.mysql.cj.jdbc.MysqlDataSource;
import com.mysql.cj.jdbc.MysqlXADataSource;

import testsuite.BaseTestCase;
import testsuite.MockJndiContextFactory;

public class DataSourceTest extends BaseTestCase {

    private Context ctx;

    /**
     * Sets up this test, binding a DataSource into JNDI, using a mock in-memory JNDI provider.
     *
     * @throws Exception
     */
    @BeforeEach
    public void setUp() throws Exception {
        /*
         * This code is separated from the rest of the test since you normally would NOT register a JDBC driver in your code. It would likely be configured into
         * your naming and directory service using some GUI.
         */
        MysqlDataSource ds;
        Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, MockJndiContextFactory.class.getName());
        this.ctx = new InitialContext(env);
        assertNotNull(this.ctx, "Naming Context not created");
        ds = new com.mysql.cj.jdbc.MysqlDataSource();
        ds.setUrl(dbUrl); // from BaseTestCase
        this.ctx.bind("_test", ds);
    }

    /**
     * Un-binds the DataSource and closes the context
     *
     * @throws Exception
     */
    @AfterEach
    public void tearDown() throws Exception {
        this.ctx.unbind("_test");
        this.ctx.close();
    }

    /**
     * Tests that we can get a connection from the DataSource bound in JNDI during test setup
     *
     * @throws Exception
     */
    @Test
    public void testDataSource() throws Exception {
        NameParser nameParser = this.ctx.getNameParser("");
        Name datasourceName = nameParser.parse("_test");
        Object obj = this.ctx.lookup(datasourceName);
        DataSource boundDs = null;

        if (obj instanceof DataSource) {
            boundDs = (DataSource) obj;
        } else if (obj instanceof Reference) {
            //
            // For some reason, this comes back as a Reference instance under CruiseControl !?
            //
            Reference objAsRef = (Reference) obj;
            ObjectFactory factory = (ObjectFactory) Class.forName(objAsRef.getFactoryClassName()).newInstance();
            boundDs = (DataSource) factory.getObjectInstance(objAsRef, datasourceName, this.ctx, new Hashtable<>());
        }

        assertNotNull(boundDs, "Datasource not bound");

        if (boundDs instanceof MysqlDataSource) {
            ((MysqlDataSource) boundDs).getStringProperty(PropertyKey.sslMode.getKeyName()).setValue("DISABLED");
            ((MysqlDataSource) boundDs).getBooleanProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName()).setValue(true);
        }

        Connection con = boundDs.getConnection();
        assertNotNull(con, "Connection can not be obtained from data source");
        con.close();
    }

    /**
     * Tests whether Connection.changeUser() (and thus pooled connections) restore character set information correctly.
     *
     * @throws Exception
     */
    @Test
    public void testChangeUserAndCharsets() throws Exception {
        MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();
        ds.setURL(BaseTestCase.dbUrl);
        ds.getStringProperty(PropertyKey.sslMode.getKeyName()).setValue("DISABLED");
        ds.getBooleanProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName()).setValue(true);
        ds.getProperty(PropertyKey.characterEncoding).setValue("utf-8");
        PooledConnection pooledConnection = ds.getPooledConnection();

        Connection connToMySQL = pooledConnection.getConnection();
        this.rs = connToMySQL.createStatement().executeQuery("SELECT @@character_set_results");
        assertTrue(this.rs.next());
        assertNull(this.rs.getString(1));

        this.rs = connToMySQL.createStatement().executeQuery("SHOW SESSION VARIABLES LIKE 'character_set_client'");
        assertTrue(this.rs.next());
        assertTrue(this.rs.getString(2).startsWith("utf8")); // Because of utf8mb4.

        connToMySQL.close();

        connToMySQL = pooledConnection.getConnection();
        this.rs = connToMySQL.createStatement().executeQuery("SELECT @@character_set_results");
        assertTrue(this.rs.next());
        assertNull(this.rs.getString(1));

        this.rs = connToMySQL.createStatement().executeQuery("SHOW SESSION VARIABLES LIKE 'character_set_client'");
        assertTrue(this.rs.next());
        assertTrue(this.rs.getString(2).startsWith("utf8")); // Because of utf8mb4.

        pooledConnection.getConnection().close();
    }

    /**
     * Tests whether XADataSources can be bound into JNDI
     *
     * @throws Exception
     */
    @Test
    public void testXADataSource() throws Exception {
        MysqlXADataSource ds = new MysqlXADataSource();
        ds.setUrl(dbUrl);

        String name = "XA";
        this.ctx.rebind(name, ds);

        Object result = this.ctx.lookup(name);

        assertNotNull(result, "XADataSource not bound into JNDI");
    }

    @Test
    public void testPropertyGettersSetters() throws Exception {
        com.mysql.cj.jdbc.MysqlDataSource ds = new com.mysql.cj.jdbc.MysqlDataSource();

        String testStr = "Test value";
        int testInt = 42;
        long testLong = 42L;

        // standard properties
        assertEquals("MySQL Connector/J Data Source", ds.getDescription());
        ds.setDescription(testStr);
        assertEquals(testStr, ds.getDescription());

        assertEquals(0, ds.getLoginTimeout());
        ds.setLoginTimeout(testInt);
        // TODO assertEquals(testInt, ds.getLoginTimeout());

        assertNull(ds.getLogWriter());
        PrintWriter pw = new PrintWriter(File.createTempFile("testPropertyGettersSettersLog", "tmp"));
        ds.setLogWriter(pw);
        assertEquals(pw, ds.getLogWriter());

        assertEquals(3306, ds.getPort());
        ds.setPort(3307);
        assertEquals(3307, ds.getPort());

        assertEquals(3307, ds.getPortNumber());
        ds.setPortNumber(3308);
        assertEquals(3308, ds.getPortNumber());

        // TODO ds.getReference();
        // TODO ds.setPropertiesViaRef(ref);

        assertEquals("", ds.getServerName());
        ds.setServerName("test.server.name");
        assertEquals("test.server.name", ds.getServerName());

        assertEquals("jdbc:mysql://test.server.name:3308/", ds.getUrl());
        ds.setUrl("http://192.168.1.1/");
        assertEquals("http://192.168.1.1/", ds.getUrl());

        assertEquals("http://192.168.1.1/", ds.getURL());
        ds.setURL("http://10.0.0.1");
        assertEquals("http://10.0.0.1", ds.getURL());

        //assertNull(ds.getUser());
        //ds.setUser("testUser");
        //assertEquals("testUser", ds.getUser());

        // instrumented properties
        for (PropertyDefinition<?> def : PropertyDefinitions.PROPERTY_KEY_TO_PROPERTY_DEFINITION.values()) {
            if (def.getCategory().equals(PropertyDefinitions.CATEGORY_XDEVAPI)) {
                continue;
            }
            String pname = def.hasCcAlias() ? def.getCcAlias() : def.getName();
            String gname = "get" + pname.substring(0, 1).toUpperCase() + pname.substring(1);
            String sname = "set" + pname.substring(0, 1).toUpperCase() + pname.substring(1);

            Method getter = ds.getClass().getMethod(gname, new Class<?>[] {});
            Object res1 = getter.invoke(ds, new Object[] {});
            assertEquals(def.getDefaultValue() + "", res1 + "", gname + ": ");

            Method setter = null;

            if (def instanceof StringPropertyDefinition) {
                setter = ds.getClass().getMethod(sname, new Class<?>[] { String.class });
                setter.invoke(ds, new Object[] { testStr });
                assertEquals(testStr, getter.invoke(ds, new Object[] {}), sname + ": ");

            } else if (def instanceof BooleanPropertyDefinition) {
                Boolean testBool = !((Boolean) def.getDefaultValue());
                setter = ds.getClass().getMethod(sname, new Class<?>[] { Boolean.TYPE });
                setter.invoke(ds, new Object[] { testBool });
                assertEquals(testBool, getter.invoke(ds, new Object[] {}), sname + ": ");

            } else if (def instanceof IntegerPropertyDefinition) {
                setter = ds.getClass().getMethod(sname, new Class<?>[] { Integer.TYPE });
                setter.invoke(ds, new Object[] { testInt });
                assertEquals(testInt, getter.invoke(ds, new Object[] {}), sname + ": ");

            } else if (def instanceof LongPropertyDefinition) {
                setter = ds.getClass().getMethod(sname, new Class<?>[] { Long.TYPE });
                setter.invoke(ds, new Object[] { testLong });
                assertEquals(testLong, getter.invoke(ds, new Object[] {}), sname + ": ");

            } else if (def instanceof MemorySizePropertyDefinition) {
                setter = ds.getClass().getMethod(sname, new Class<?>[] { Integer.TYPE });
                setter.invoke(ds, new Object[] { testInt });
                assertEquals(testInt, getter.invoke(ds, new Object[] {}), sname + ": ");

            } else if (def instanceof EnumPropertyDefinition<?>) {
                String testEnum = null;
                for (String val : def.getAllowableValues()) {
                    if (!val.equals(def.getDefaultValue())) {
                        testEnum = val;
                        break;
                    }
                }
                setter = ds.getClass().getMethod(sname, new Class<?>[] { String.class });
                setter.invoke(ds, new Object[] { testEnum });
                assertEquals(testEnum, getter.invoke(ds, new Object[] {}), sname + ": ");

            } else {
                fail("Unknown " + def.getName() + " property type.");
            }
        }
    }

    @Test
    public void testUrlEscaping() {
        MysqlDataSource testDataSource = new MysqlDataSource();
        testDataSource.setServerName("connectorj.mysql.com");
        testDataSource.setDatabaseName("mysql?connector/j");
        assertEquals("jdbc:mysql://connectorj.mysql.com:3306/mysql%3Fconnector%2Fj", testDataSource.getUrl());

        testDataSource.setServerName("connectorj.mysql.com:12345/fakeDB?foo=");
        testDataSource.setDatabaseName("goodDB");
        assertEquals("jdbc:mysql://connectorj.mysql.com%3A12345%2FfakeDB%3Ffoo%3D:3306/goodDB", testDataSource.getUrl());
    }

}
