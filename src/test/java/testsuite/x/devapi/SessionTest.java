/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

package testsuite.x.devapi;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.FileNotFoundException;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.Constants;
import com.mysql.cj.CoreSession;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJPacketTooBigException;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.Client;
import com.mysql.cj.xdevapi.Client.ClientProperty;
import com.mysql.cj.xdevapi.ClientFactory;
import com.mysql.cj.xdevapi.ClientImpl;
import com.mysql.cj.xdevapi.ClientImpl.PooledXProtocol;
import com.mysql.cj.xdevapi.FindStatement;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.Schema;
import com.mysql.cj.xdevapi.SelectStatement;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.SessionImpl;
import com.mysql.cj.xdevapi.SqlMultiResult;
import com.mysql.cj.xdevapi.SqlResult;
import com.mysql.cj.xdevapi.SqlStatement;
import com.mysql.cj.xdevapi.XDevAPIError;

import testsuite.UnreliableSocketFactory;

public class SessionTest extends DevApiBaseTestCase {
    @BeforeEach
    public void setupSessionTest() {
        setupTestSession();
    }

    @AfterEach
    public void teardownSessionTest() {
        if (this.isSetForXTests) {
            this.createdTestSchemas.forEach(schemaName -> {
                try {
                    this.session.dropSchema(schemaName);
                } catch (XProtocolError x) {
                    // ignored
                }
            });
            destroyTestSession();
        }
    }

    private List<String> createdTestSchemas = new ArrayList<>();

    /**
     * Create a random schema name. The schema will be dropped upon test cleanup.
     * 
     * @return a string
     */
    private String getRandomTestSchemaName() {
        String n = "cj_test_schema_no_" + new Random().nextInt(1000);
        this.createdTestSchemas.add(n);
        return n;
    }

    @Test
    public void urlWithDefaultSchema() {
        if (!this.isSetForXTests) {
            return;
        }

        try {
            // Create user with mysql_native_password authentication plugin as it can be used with any of the authentication mechanisms.
            this.session.sql("CREATE USER IF NOT EXISTS 'testUserN'@'%' IDENTIFIED WITH mysql_native_password BY 'testUserN'").execute();
            this.session.sql("GRANT SELECT ON *.* TO 'testUserN'@'%'").execute();

            final String testSchemaName = getRandomTestSchemaName();
            this.session.createSchema(testSchemaName);

            final SessionFactory testSessionFactory = new SessionFactory();
            final String testUriPattern = "mysqlx://testUserN:testUserN@%s:%s/%s?xdevapi.auth=%s";

            // Check if the default schema is correctly sent when using different authentication mechanisms.
            String[] authMechs = mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4")) ? new String[] { "PLAIN", "MYSQL41", "SHA256_MEMORY" }
                    : new String[] { "PLAIN", "MYSQL41" };
            for (String authMech : authMechs) {
                final String testCase = "Testing default schema provided in authentication mecanism '" + authMech + "'.";

                // Test using a connection String.
                final String testUri = String.format(testUriPattern, getTestHost(), getTestPort(), testSchemaName, authMech);

                Session testSession = testSessionFactory.getSession(testUri);
                assertTrue(testSession.getUri().contains("/" + testSchemaName + "?"), testCase);
                assertEquals(testSchemaName, testSession.getDefaultSchemaName(), testCase);
                assertNotNull(testSession.getDefaultSchema(), testCase);
                assertEquals(testSchemaName, testSession.getDefaultSchema().getName(), testCase);
                assertEquals(testSchemaName, testSession.sql("SELECT database()").execute().fetchOne().getString(0), testCase);
                testSession.close();

                // Test using a properties map.
                final Properties testProps = new Properties();
                testProps.setProperty(PropertyKey.USER.getKeyName(), "testUserN");
                testProps.setProperty(PropertyKey.PASSWORD.getKeyName(), "testUserN");
                testProps.setProperty(PropertyKey.HOST.getKeyName(), getTestHost());
                testProps.setProperty(PropertyKey.PORT.getKeyName(), String.valueOf(getTestPort()));
                testProps.setProperty(PropertyKey.DBNAME.getKeyName(), testSchemaName);
                testProps.setProperty(PropertyKey.xdevapiAuth.getKeyName(), authMech);

                testSession = testSessionFactory.getSession(testProps);
                assertTrue(testSession.getUri().contains("/" + testSchemaName + "?"), testCase);
                assertEquals(testSchemaName, testSession.getDefaultSchemaName(), testCase);
                assertNotNull(testSession.getDefaultSchema(), testCase);
                assertEquals(testSchemaName, testSession.getDefaultSchema().getName(), testCase);
                assertEquals(testSchemaName, testSession.sql("SELECT database()").execute().fetchOne().getString(0), testCase);
                testSession.close();
            }
        } finally {
            this.session.sql("DROP USER IF EXISTS testUserN").execute();
        }
    }

    @Test
    public void urlWithoutDefaultSchema() {
        if (!this.isSetForXTests) {
            return;
        }

        try {
            // Create user with mysql_native_password authentication plugin as it can be used with any of the authentication mechanisms.
            this.session.sql("CREATE USER IF NOT EXISTS 'testUserN'@'%' IDENTIFIED WITH mysql_native_password BY 'testUserN'").execute();
            this.session.sql("GRANT SELECT ON *.* TO 'testUserN'@'%'").execute();

            final SessionFactory testSessionFactory = new SessionFactory();
            final String testUriPattern1 = "mysqlx://testUserN:testUserN@%s:%s/?xdevapi.auth=%s";
            final String testUriPattern2 = "mysqlx://testUserN:testUserN@%s:%s?xdevapi.auth=%s";
            final String testUriPattern3 = "mysqlx://testUserN:testUserN@address=(host=%s)(port=%s)(xdevapi.auth=%s)";

            // Check if not setting a default schema works correctly when using different authentication mechanisms.
            String[] authMechs = mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4")) ? new String[] { "PLAIN", "MYSQL41", "SHA256_MEMORY" }
                    : new String[] { "PLAIN", "MYSQL41" };
            for (String authMech : authMechs) {
                for (String testUriPattern : new String[] { testUriPattern1, testUriPattern2, testUriPattern3 }) {
                    // Test using a connection String.
                    final String testUri = String.format(testUriPattern, getTestHost(), getTestPort(), authMech);
                    final String testCase = "Testing no default schema with authentication mecanism '" + authMech + "' and URI '" + testUri + "'.";

                    Session testSession = testSessionFactory.getSession(testUri);
                    assertTrue(testSession.getUri().contains("/?"), testCase);
                    assertEquals("", testSession.getDefaultSchemaName(), testCase);
                    assertNull(testSession.getDefaultSchema(), testCase);
                    assertNull(testSession.sql("SELECT database()").execute().fetchOne().getString(0), testCase);
                    testSession.close();
                }

                // Test using a properties map.
                final String testCase = "Testing no default schema with authentication mecanism '" + authMech + "'.";
                final Properties testProps = new Properties();
                testProps.setProperty(PropertyKey.USER.getKeyName(), "testUserN");
                testProps.setProperty(PropertyKey.PASSWORD.getKeyName(), "testUserN");
                testProps.setProperty(PropertyKey.HOST.getKeyName(), getTestHost());
                testProps.setProperty(PropertyKey.PORT.getKeyName(), String.valueOf(getTestPort()));
                testProps.setProperty(PropertyKey.xdevapiAuth.getKeyName(), authMech);

                Session testSession = testSessionFactory.getSession(testProps);
                assertTrue(testSession.getUri().contains("/?"), testCase);
                assertEquals("", testSession.getDefaultSchemaName(), testCase);
                assertNull(testSession.getDefaultSchema(), testCase);
                assertNull(testSession.sql("SELECT database()").execute().fetchOne().getString(0), testCase);
                testSession.close();
            }
        } finally {
            this.session.sql("DROP USER IF EXISTS testUserN").execute();
        }
    }

    @Test
    public void invalidDefaultSchema() {
        if (!this.isSetForXTests) {
            return;
        }

        try {
            // Create user with mysql_native_password authentication plugin as it can be used with any of the authentication mechanisms.
            this.session.sql("CREATE USER IF NOT EXISTS 'testUserN'@'%' IDENTIFIED WITH mysql_native_password BY 'testUserN'").execute();
            this.session.sql("GRANT SELECT ON *.* TO 'testUserN'@'%'").execute();

            final String testSchemaName = getRandomTestSchemaName();

            final SessionFactory testSessionFactory = new SessionFactory();
            final String testUriPattern = "mysqlx://testUserN:testUserN@%s:%s/%s?xdevapi.auth=%s";

            // Check if the default schema is correctly sent when using different authentication mechanisms.
            String[] authMechs = mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.4")) ? new String[] { "PLAIN", "MYSQL41", "SHA256_MEMORY" }
                    : new String[] { "PLAIN", "MYSQL41" };
            for (String authMech : authMechs) {
                final String testCase = "Testing missing default schema provided in authentication mecanism '" + authMech + "'.";

                // Test using a connection String.
                final String testUri = String.format(testUriPattern, getTestHost(), getTestPort(), testSchemaName, authMech);

                assertThrows(testCase, XProtocolError.class, "ERROR \\d{4} \\(HY000\\) Unknown database '" + testSchemaName + "'", () -> {
                    testSessionFactory.getSession(testUri);
                    return null;
                });

                // Test using a properties map.
                final Properties testProps = new Properties();
                testProps.setProperty(PropertyKey.USER.getKeyName(), "testUserN");
                testProps.setProperty(PropertyKey.PASSWORD.getKeyName(), "testUserN");
                testProps.setProperty(PropertyKey.HOST.getKeyName(), getTestHost());
                testProps.setProperty(PropertyKey.PORT.getKeyName(), String.valueOf(getTestPort()));
                testProps.setProperty(PropertyKey.DBNAME.getKeyName(), testSchemaName);
                testProps.setProperty(PropertyKey.xdevapiAuth.getKeyName(), authMech);

                assertThrows(testCase, XProtocolError.class, "ERROR \\d{4} \\(HY000\\) Unknown database '" + testSchemaName + "'", () -> {
                    testSessionFactory.getSession(testUri);
                    return null;
                });
            }
        } finally {
            this.session.sql("DROP USER IF EXISTS testUserN").execute();
        }
    }

    @Test
    public void createDropSchema() {
        if (!this.isSetForXTests) {
            return;
        }
        String testSchemaName = getRandomTestSchemaName();
        Schema newSchema = this.session.createSchema(testSchemaName);
        assertTrue(this.session.getSchemas().contains(newSchema));
        this.session.dropSchema(testSchemaName);
        assertFalse(this.session.getSchemas().contains(newSchema));
    }

    @Test
    public void createAndReuseExistingSchema() {
        if (!this.isSetForXTests) {
            return;
        }
        String testSchemaName = getRandomTestSchemaName();
        Schema newSchema = this.session.createSchema(testSchemaName);
        assertTrue(this.session.getSchemas().contains(newSchema));
        Schema reusedSchema = this.session.createSchema(testSchemaName, true);
        assertTrue(this.session.getSchemas().contains(reusedSchema));
    }

    @Test
    public void listSchemas() {
        if (!this.isSetForXTests) {
            return;
        }
        List<Schema> schemas = this.session.getSchemas();
        // we should have visibility of at least these two
        Schema infoSchema = this.session.getSchema("information_schema");
        assertTrue(schemas.contains(infoSchema));
        Schema testSchema = this.session.getSchema(getTestDatabase());
        assertTrue(schemas.contains(testSchema));
    }

    @Test
    public void createExistingSchemaError() {
        if (!this.isSetForXTests) {
            return;
        }
        String testSchemaName = getRandomTestSchemaName();
        Schema newSchema = this.session.createSchema(testSchemaName);
        assertTrue(this.session.getSchemas().contains(newSchema));
        try {
            this.session.createSchema(testSchemaName);
            fail("Attempt to create a schema with the name of an existing schema should fail");
        } catch (XProtocolError err) {
            assertEquals(MysqlErrorNumbers.ER_DB_CREATE_EXISTS, err.getErrorCode());
        }
    }

    /**
     * Test the client-side enforcing of server `mysqlx_max_allowed_packet'.
     */
    @Test
    public void errorOnPacketTooBig() {
        if (!this.isSetForXTests) {
            return;
        }
        try {
            SqlStatement stmt = this.session.sql("select @@mysqlx_max_allowed_packet");
            SqlResult res = stmt.execute();
            Row r = res.next();
            long mysqlxMaxAllowedPacket = r.getLong(0);

            long size = 100 + mysqlxMaxAllowedPacket;
            StringBuilder b = new StringBuilder();
            for (int i = 0; i < size; ++i) {
                b.append('a');
            }
            String s = b.append("\"}").toString();
            this.session.dropSchema(s);
            fail("Large packet should cause an exception");
        } catch (CJPacketTooBigException ex) {
            // expected
        }
    }

    /**
     * Tests fix for Bug#21690043, CONNECT FAILS WHEN PASSWORD IS BLANK.
     */
    @Test
    public void testBug21690043() {
        if (!this.isSetForXTests) {
            return;
        }

        try {
            this.session.sql("CREATE USER 'bug21690043user1'@'%' IDENTIFIED WITH mysql_native_password").execute();
            this.session.sql("GRANT SELECT ON *.* TO 'bug21690043user1'@'%'").execute();

            Properties props = new Properties();
            props.putAll(this.testProperties);
            props.setProperty("user", "bug21690043user1");
            props.setProperty("password", "");
            new SessionFactory().getSession(props);
        } catch (Throwable t) {
            throw t;
        } finally {
            this.session.sql("DROP USER 'bug21690043user1'@'%'").execute();
        }
    }

    @Test
    public void basicSql() {
        if (!this.isSetForXTests) {
            return;
        }
        SqlStatement stmt = this.session.sql("select 1,2,3 from dual");
        SqlResult res = stmt.execute();
        assertTrue(res.hasData());
        Row r = res.next();
        assertEquals("1", r.getString(0));
        assertEquals("2", r.getString(1));
        assertEquals("3", r.getString(2));
        assertEquals("1", r.getString("1"));
        assertEquals("2", r.getString("2"));
        assertEquals("3", r.getString("3"));
        assertFalse(res.hasNext());

        assertThrows(XDevAPIError.class, "Method getAutoIncrementValue\\(\\) is allowed only for insert statements.", new Callable<Void>() {
            public Void call() throws Exception {
                assertEquals(null, res.getAutoIncrementValue());
                return null;
            }
        });
    }

    @Test
    public void sqlUpdate() {
        if (!this.isSetForXTests) {
            return;
        }
        SqlStatement stmt = this.session.sql("set @cjTestVar = 1");
        SqlResult res = stmt.execute();
        assertFalse(res.hasData());
        assertEquals(0, res.getAffectedItemsCount());
        assertEquals(null, res.getAutoIncrementValue());
        assertEquals(0, res.getWarningsCount());
        assertFalse(res.getWarnings().hasNext());

        // TODO SqlUpdateResult throws FeatureNotAvailableException("Not a multi-result");
        //res.nextResult();

        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.fetchAll();
                return null;
            }
        });
        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.next();
                return null;
            }
        });
        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.hasNext();
                return null;
            }
        });
        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.getColumnCount();
                return null;
            }
        });
        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.getColumns();
                return null;
            }
        });
        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.getColumnNames();
                return null;
            }
        });
        assertThrows(FeatureNotAvailableException.class, "No data", new Callable<Void>() {
            public Void call() throws Exception {
                res.count();
                return null;
            }
        });
    }

    @Test
    public void sqlArguments() {
        if (!this.isSetForXTests) {
            return;
        }
        SqlStatement stmt = this.session.sql("select ? as a, 40 + ? as b, ? as c");
        SqlResult res = stmt.bind(1).bind(2).bind(3).execute();
        Row r = res.next();
        assertEquals("1", r.getString("a"));
        assertEquals("42", r.getString("b"));
        assertEquals("3", r.getString("c"));
    }

    @Test
    public void basicMultipleResults() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop procedure if exists basicMultipleResults");
        sqlUpdate("create procedure basicMultipleResults() begin explain select 1; explain select 2; end");
        SqlStatement stmt = this.session.sql("call basicMultipleResults()");
        SqlResult res = stmt.execute();
        assertTrue(res.hasData());
        /* Row r = */ res.next();
        assertFalse(res.hasNext());
        assertTrue(res.nextResult());
        assertTrue(res.hasData());
        assertFalse(res.nextResult());
        assertFalse(res.nextResult());
        assertFalse(res.nextResult());
    }

    @Test
    public void smartBufferMultipleResults() {
        if (!this.isSetForXTests) {
            return;
        }
        sqlUpdate("drop procedure if exists basicMultipleResults");
        sqlUpdate("create procedure basicMultipleResults() begin explain select 1; explain select 2; end");
        SqlStatement stmt = this.session.sql("call basicMultipleResults()");
        /* SqlResult res = */ stmt.execute();
        // execute another statement, should work fine
        this.session.sql("call basicMultipleResults()");
        this.session.sql("call basicMultipleResults()");
        this.session.sql("call basicMultipleResults()");
    }

    @Test
    public void sqlInsertAutoIncrementValue() {
        if (!this.isSetForXTests) {
            return;
        }

        sqlUpdate("drop table if exists lastInsertId");
        sqlUpdate("create table lastInsertId (id int not null primary key auto_increment, name varchar(20) not null)");

        SqlStatement stmt = this.session.sql("insert into lastInsertId values (null, 'a')");
        SqlResult res = stmt.execute();

        assertFalse(res.hasData());
        assertEquals(1, res.getAffectedItemsCount());
        assertEquals(0, res.getWarningsCount());
        assertFalse(res.getWarnings().hasNext());
        assertEquals(new Long(1), res.getAutoIncrementValue());
    }

    /**
     * Tests fix for Bug #27652379, NPE FROM GETSESSION(PROPERTIES) WHEN HOST PARAMETER IS GIVEN IN SMALL LETTER.
     * 
     * @throws Exception
     */
    @Test
    public void testBug27652379() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        Properties props = new Properties();

        // Upper case keys.
        props.clear();
        props.setProperty("HOST", getTestHost());
        props.setProperty("PORT", String.valueOf(getTestPort()));
        props.setProperty("USER", getTestUser());
        props.setProperty("PASSWORD", getTestPassword());
        props.setProperty("DBNAME", getTestDatabase());

        Session testSession = this.fact.getSession(props);
        assertEquals(getTestDatabase(), testSession.getDefaultSchemaName());
        testSession.close();

        testSession = this.fact.getSession(new Properties(props));
        assertEquals(getTestDatabase(), testSession.getDefaultSchemaName());
        testSession.close();

        // Lower case keys.
        props.clear();
        props.setProperty("host", getTestHost());
        props.setProperty("port", String.valueOf(getTestPort()));
        props.setProperty("user", getTestUser());
        props.setProperty("password", getTestPassword());
        props.setProperty("dbname", getTestDatabase());

        testSession = this.fact.getSession(props);
        assertEquals(getTestDatabase(), testSession.getDefaultSchemaName());
        testSession.close();

        testSession = this.fact.getSession(new Properties(props));
        assertEquals(getTestDatabase(), testSession.getDefaultSchemaName());
        testSession.close();

        // Random case keys.
        props.clear();
        props.setProperty("HOst", getTestHost());
        props.setProperty("poRT", String.valueOf(getTestPort()));
        props.setProperty("uSEr", getTestUser());
        props.setProperty("PassworD", getTestPassword());
        props.setProperty("DbNaMe", getTestDatabase());

        testSession = this.fact.getSession(props);
        assertEquals(getTestDatabase(), testSession.getDefaultSchemaName());
        testSession.close();

        testSession = this.fact.getSession(new Properties(props));
        assertEquals(getTestDatabase(), testSession.getDefaultSchemaName());
        testSession.close();
    }

    /**
     * Tests fix for BUG#23045604 - XSESSION.GETURI() RETURNS NPE.
     */
    @Test
    public void testBug23045604() {
        if (!this.isSetForXTests) {
            return;
        }

        String url = this.baseUrl;
        if (!url.contains("?")) {
            url += "?";
        }
        Session sess = this.fact.getSession(url + makeParam(PropertyKey.serverTimezone, "Asia/Calcutta") + makeParam(PropertyKey.serverConfigCacheFactory, ""));

        String uri = sess.getUri();
        assertTrue(uri.contains("serverTimezone=Asia/Calcutta"));
        assertTrue(uri.contains("serverConfigCacheFactory="));
        assertFalse(uri.contains(","));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testPooledSessions() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        final ClientFactory cf = new ClientFactory();
        final String url = this.baseUrl;
        final Properties props = new Properties();

        /*
         * UT8/1: Verify that setting an incorrect value in the client options defined in the Pooling options in the HLS throw an exception with the expected
         * message.
         */
        // pooling.enabled
        props.clear();
        assertThrows(XDevAPIError.class, "Client option 'pooling\\.enabled' does not support value 'sure'\\.", new Callable<Void>() {
            public Void call() throws Exception {
                props.setProperty(ClientProperty.POOLING_ENABLED.getKeyName(), "sure");
                cf.getClient(url, props);
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Client option 'pooling\\.enabled' does not support value 'sure'\\.", new Callable<Void>() {
            public Void call() throws Exception {
                cf.getClient(url, "{\"pooling\": {\"enabled\": \"sure\"}}");
                return null;
            }
        });
        // pooling.maxSize
        props.clear();
        assertThrows(XDevAPIError.class, "Client option 'pooling\\.maxSize' does not support value '0'\\.", new Callable<Void>() {
            public Void call() throws Exception {
                props.setProperty(ClientProperty.POOLING_MAX_SIZE.getKeyName(), "0");
                cf.getClient(url, props);
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Client option 'pooling\\.maxSize' does not support value '0'\\.", new Callable<Void>() {
            public Void call() throws Exception {
                cf.getClient(url, "{\"pooling\": {\"maxSize\": 0}}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Client option 'pooling\\.maxSize' does not support value 'one'\\.", new Callable<Void>() {
            public Void call() throws Exception {
                cf.getClient(url, "{\"pooling\": {\"maxSize\": \"one\"}}");
                return null;
            }
        });
        // pooling.maxIdleTime
        props.clear();
        assertThrows(XDevAPIError.class, "Client option 'pooling\\.maxIdleTime' does not support value '-1'\\.", new Callable<Void>() {
            public Void call() throws Exception {
                props.setProperty(ClientProperty.POOLING_MAX_IDLE_TIME.getKeyName(), "-1");
                cf.getClient(url, props);
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Client option 'pooling\\.maxIdleTime' does not support value '-1'\\.", new Callable<Void>() {
            public Void call() throws Exception {
                cf.getClient(url, "{\"pooling\": {\"maxIdleTime\": -1}}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Client option 'pooling\\.maxIdleTime' does not support value 'one'\\.", new Callable<Void>() {
            public Void call() throws Exception {
                cf.getClient(url, "{\"pooling\": {\"maxIdleTime\": \"one\"}}");
                return null;
            }
        });
        // pooling.queueTimeout
        props.clear();
        assertThrows(XDevAPIError.class, "Client option 'pooling\\.queueTimeout' does not support value '-1'\\.", new Callable<Void>() {
            public Void call() throws Exception {
                props.setProperty(ClientProperty.POOLING_QUEUE_TIMEOUT.getKeyName(), "-1");
                cf.getClient(url, props);
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Client option 'pooling\\.queueTimeout' does not support value '-1'\\.", new Callable<Void>() {
            public Void call() throws Exception {
                cf.getClient(url, "{\"pooling\": {\"queueTimeout\": -1}}");
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Client option 'pooling\\.queueTimeout' does not support value 'one'\\.", new Callable<Void>() {
            public Void call() throws Exception {
                cf.getClient(url, "{\"pooling\": {\"queueTimeout\": \"one\"}}");
                return null;
            }
        });
        // Unknown pooling option.
        props.clear();
        assertThrows(XDevAPIError.class, "Client option 'pooling\\.foo' is not recognized as valid\\.", new Callable<Void>() {
            public Void call() throws Exception {
                props.setProperty("pooling.foo", "bar");
                cf.getClient(url, props);
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Client option 'pooling\\.foo' is not recognized as valid\\.", new Callable<Void>() {
            public Void call() throws Exception {
                cf.getClient(url, "{\"pooling\": {\"foo\": \"bar\"}}");
                return null;
            }
        });
        // Unknown clientProps option.
        props.clear();
        assertThrows(XDevAPIError.class, "Client option 'foo' is not recognized as valid\\.", new Callable<Void>() {
            public Void call() throws Exception {
                props.setProperty("foo", "bar");
                cf.getClient(url, props);
                return null;
            }
        });
        assertThrows(XDevAPIError.class, "Client option 'foo' is not recognized as valid\\.", new Callable<Void>() {
            public Void call() throws Exception {
                cf.getClient(url, "{\"foo\": {\"bar\": \"baz\"}}");
                return null;
            }
        });

        /*
         * UT9/1: Verify that when no pooling properties passed their values set to defaults: pooling.maxSize=25, pooling.maxIdleTime=0, pooling.queueTimeout=0.
         */
        props.clear();
        testPooledSessions_checkClientProperties(cf.getClient(this.baseUrl, props), 25, 0, 0);
        testPooledSessions_checkClientProperties(cf.getClient(this.baseUrl, (Properties) null), 25, 0, 0);
        testPooledSessions_checkClientProperties(cf.getClient(this.baseUrl, "{}"), 25, 0, 0);
        testPooledSessions_checkClientProperties(cf.getClient(this.baseUrl, (String) null), 25, 0, 0);

        /*
         * UT9/2: Verify that when all pooling properties passed via Properties object the Client is configured according to their values.
         */
        props.setProperty(ClientProperty.POOLING_ENABLED.getKeyName(), "true");
        props.setProperty(ClientProperty.POOLING_MAX_SIZE.getKeyName(), "5");
        props.setProperty(ClientProperty.POOLING_MAX_IDLE_TIME.getKeyName(), "6");
        props.setProperty(ClientProperty.POOLING_QUEUE_TIMEOUT.getKeyName(), "7");
        testPooledSessions_checkClientProperties(cf.getClient(this.baseUrl, props), 5, 6, 7);

        /*
         * UT9/3: Verify that when all pooling properties passed via json string ({"pooling" : {"enabled" : true, "maxSize" : 8, "maxIdleTime" : 9,
         * "queueTimeout" : 10} }) the Client is configured according to their values.
         */
        testPooledSessions_checkClientProperties(
                cf.getClient(this.baseUrl, "{\"pooling\" : {\"enabled\" : true, \"maxSize\" : 8, \"maxIdleTime\" : 9, \"queueTimeout\" : 10} }"), 8, 9, 10);

        /*
         * UT3/2: Start a client with pooling enabled, call Client.getSession() twice and verify that the objects returned are different and the internal
         * connection instances are different too.
         */
        Client cli0 = cf.getClient(this.baseUrl, "{\"pooling\": {\"enabled\": true}}");
        Session s0 = cli0.getSession();
        Session s1 = cli0.getSession();
        assertNotEquals(s0, s1);

        Field fProtocol = CoreSession.class.getDeclaredField("protocol");
        fProtocol.setAccessible(true);
        assertNotEquals(fProtocol.get(((SessionImpl) s0).getSession()), fProtocol.get(((SessionImpl) s1).getSession()));
        cli0.close();

        /*
         * UT3/3: Start a client with pooling enabled, call Client.getSession() twice, closing the first session before getting the second one, and verify that
         * the objects returned are different and that the internal connection instances are the same.
         */
        cli0 = cf.getClient(this.baseUrl, "{\"pooling\": {\"enabled\": true}}");
        s0 = cli0.getSession();
        s0.close();
        s1 = cli0.getSession();
        s1.sql("SELECT 1").execute();
        assertNotEquals(s0, s1);
        assertEquals(fProtocol.get(((SessionImpl) s0).getSession()), fProtocol.get(((SessionImpl) s1).getSession()));
        cli0.close();

        /*
         * UT3/4: Start a client with pooling disabled, call Client.getSession() twice and verify that the objects returned are different and the internal
         * connection instances are different too.
         */
        cli0 = cf.getClient(this.baseUrl, "{\"pooling\": {\"enabled\": false}}");
        s0 = cli0.getSession();
        s1 = cli0.getSession();
        assertNotEquals(s0, s1);
        assertNotEquals(fProtocol.get(((SessionImpl) s0).getSession()), fProtocol.get(((SessionImpl) s1).getSession()));
        cli0.close();

        /*
         * UT3/5: Start a client with pooling disabled, call Client.getSession() twice, closing the first session before getting the second one, and verify that
         * the objects returned are different and that the internal connection instances are different too.
         */
        cli0 = cf.getClient(this.baseUrl, "{\"pooling\": {\"enabled\": false}}");
        s0 = cli0.getSession();
        s0.close();
        s1 = cli0.getSession();
        assertNotEquals(s0, s1);
        assertNotEquals(fProtocol.get(((SessionImpl) s0).getSession()), fProtocol.get(((SessionImpl) s1).getSession()));
        cli0.close();

        /*
         * UT5/1: Having a full pool and queueTimeout = 0, verify that a new Client.getSession() waits until a Session is released.
         * UT6/1: Verify that that a client object can not open/create more sessions that the specified in the maxSize option.
         */
        props.setProperty(ClientProperty.POOLING_MAX_SIZE.getKeyName(), "3");
        props.setProperty(ClientProperty.POOLING_MAX_IDLE_TIME.getKeyName(), "1000");
        props.setProperty(ClientProperty.POOLING_QUEUE_TIMEOUT.getKeyName(), "1000");
        Client cli1 = cf.getClient(this.baseUrl, props);
        s1 = cli1.getSession();
        Session s2 = cli1.getSession();
        Session s3 = cli1.getSession();
        testPooledSessions_assertFailureTimeout(cli1, 1000, 2000, XDevAPIError.class, "Session can not be obtained within 1000 milliseconds.");
        cli1.close();

        /*
         * UT12/1: Having a pool with a single connection, close the Session, get another Session with Client.getSession(). Verify that the received Session
         * object is new and uses the same internal connection (MysqlxSession) instance.
         */
        cli0 = cf.getClient(this.baseUrl, "{\"pooling\": {\"enabled\": true, \"maxSize\" : 1}}");
        s0 = cli0.getSession();
        s0.close();
        s1 = cli0.getSession();
        s1.sql("SELECT 1").execute();
        assertNotEquals(s0, s1);
        assertEquals(fProtocol.get(((SessionImpl) s0).getSession()), fProtocol.get(((SessionImpl) s1).getSession()));
        cli0.close();

        /*
         * UT12/2: Having a pool with a number of connections greater than 1 and lower than maxPoolSize, close on Session, get another Session with
         * Client.getSession(). Verify that the received Session object is new and uses the same internal connection (MysqlxSession) instance.
         */
        cli0 = cf.getClient(this.baseUrl, "{\"pooling\": {\"enabled\": true, \"maxSize\" : 3}}");
        s0 = cli0.getSession();
        s0.close();
        s1 = cli0.getSession();
        s1.sql("SELECT 1").execute();
        assertNotEquals(s0, s1);
        assertEquals(fProtocol.get(((SessionImpl) s0).getSession()), fProtocol.get(((SessionImpl) s1).getSession()));
        cli0.close();

        /*
         * UT12/3: Having a full pool with all sessions in active state, close one Session, get another Session with Client.getSession(). Verify that the
         * received Session object is new and uses the same internal connection (MysqlxSession) instance.
         */
        cli0 = cf.getClient(this.baseUrl, "{\"pooling\": {\"enabled\": true, \"maxSize\" : 3}}");
        s0 = cli0.getSession();
        s1 = cli0.getSession();
        s2 = cli0.getSession();
        s0.close();
        s3 = cli0.getSession();
        s3.sql("SELECT 1").execute();
        assertNotEquals(s0, s3);
        assertEquals(fProtocol.get(((SessionImpl) s0).getSession()), fProtocol.get(((SessionImpl) s3).getSession()));
        cli0.close();

        /*
         * UT11/2: Having a pool with a single idle connection and maxIdleTime = n, verify that after n milliseconds a Client.getSession() call will remove all
         * expired sessions from pool and return a new Session object that uses a new MysqlxSession instance.
         */
        cli0 = cf.getClient(this.baseUrl, "{\"pooling\": {\"enabled\": true, \"maxSize\" : 3, \"maxIdleTime\" : 1000}}");
        s0 = cli0.getSession();
        s0.close();
        Thread.sleep(2000);
        s1 = cli0.getSession();
        assertNotEquals(s0, s1);
        assertNotEquals(fProtocol.get(((SessionImpl) s0).getSession()), fProtocol.get(((SessionImpl) s1).getSession()));

        Field fIdleSessions = ClientImpl.class.getDeclaredField("idleProtocols");
        fIdleSessions.setAccessible(true);
        assertTrue(((BlockingQueue<PooledXProtocol>) fIdleSessions.get(cli0)).isEmpty());

        /*
         * UT4/1: Verify that all idle and active sessions are closed after Client.close() call.
         */
        cli0 = cf.getClient(this.baseUrl, "{\"pooling\": {\"enabled\": true, \"maxSize\" : 3}}");
        s0 = cli0.getSession();
        s1 = cli0.getSession();
        s0.close();

        Field fActiveSessions = ClientImpl.class.getDeclaredField("activeProtocols");
        fActiveSessions.setAccessible(true);
        assertEquals(1, ((BlockingQueue<PooledXProtocol>) fIdleSessions.get(cli0)).size());
        assertEquals(1, ((Set<WeakReference<PooledXProtocol>>) fActiveSessions.get(cli0)).size());

        cli0.close();
        assertEquals(0, ((BlockingQueue<PooledXProtocol>) fIdleSessions.get(cli0)).size());
        assertEquals(0, ((Set<WeakReference<PooledXProtocol>>) fActiveSessions.get(cli0)).size());

        final Session ses = s1;
        assertThrows(CJCommunicationsException.class, new Callable<Void>() {
            public Void call() throws Exception {
                ses.getSchemas();
                return null;
            }
        });

        /*
         * UT4/2: Verify that after Client was closed the Client.getSession() throws an XDevAPIError with the message "Client is closed."
         */
        Client cli2 = cf.getClient(this.baseUrl, "{\"pooling\": {\"enabled\": true}}");
        cli2.close();
        assertThrows(XDevAPIError.class, "Client is closed.", new Callable<Void>() {
            public Void call() throws Exception {
                cli2.getSession();
                return null;
            }
        });

        /*
         * UT11/1: Having a pool with a single active and maxIdleTime = 0, verify that if closing the session then after any long inactivity time a new Session
         * object returned by Client.getSession() uses the same internal MysqlxSession object.
         */
        props.setProperty(ClientProperty.POOLING_MAX_SIZE.getKeyName(), "2");
        props.setProperty(ClientProperty.POOLING_MAX_IDLE_TIME.getKeyName(), "0");
        props.setProperty(ClientProperty.POOLING_QUEUE_TIMEOUT.getKeyName(), "20000");
        Client cli3 = cf.getClient(this.baseUrl, props);
        s0 = cli3.getSession();
        s1 = cli3.getSession();
        assertEquals(2, ((Set<WeakReference<PooledXProtocol>>) fActiveSessions.get(cli3)).size());
        s0.close();
        Thread.sleep(10000);
        s2 = cli3.getSession();
        s2.sql("SELECT 1").execute();
        assertNotEquals(s0, s2);
        assertEquals(fProtocol.get(((SessionImpl) s0).getSession()), fProtocol.get(((SessionImpl) s2).getSession()));
        cli3.close();

        /*
         * UT5/1: Having a full pool and queueTimeout = 0, verify that a new Client.getSession() waits until a Session is released.
         */
        props.setProperty(ClientProperty.POOLING_MAX_SIZE.getKeyName(), "2");
        props.setProperty(ClientProperty.POOLING_MAX_IDLE_TIME.getKeyName(), "1000");
        props.setProperty(ClientProperty.POOLING_QUEUE_TIMEOUT.getKeyName(), "0");
        cli3 = cf.getClient(this.baseUrl, props);
        s1 = cli3.getSession();
        Session s6 = cli3.getSession();

        Thread t = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(10000);
                    s6.close();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

            }
        });

        long begin = System.currentTimeMillis();
        t.start();
        s1 = cli3.getSession();
        long end = System.currentTimeMillis() - begin;
        s1.sql("SELECT 1").execute();
        assertTrue(end >= 10000, "Expected wait time 10000 ms but was " + end);
        assertEquals(fProtocol.get(((SessionImpl) s1).getSession()), fProtocol.get(((SessionImpl) s6).getSession()));

        /*
         * TS10_2 Verify that RuntimProperty objects are reset to initial values when Session returned to pool.
         */
        assertEquals(2, ((Set<WeakReference<PooledXProtocol>>) fActiveSessions.get(cli3)).size());
        ((SessionImpl) s1).getSession().getPropertySet().getStringProperty(PropertyKey.connectionAttributes).setValue("orig:s1");
        s1.close();
        assertEquals(1, ((Set<WeakReference<PooledXProtocol>>) fActiveSessions.get(cli3)).size());
        s2 = cli3.getSession();
        s2.sql("SELECT 1").execute();
        assertEquals(fProtocol.get(((SessionImpl) s1).getSession()), fProtocol.get(((SessionImpl) s2).getSession()));
        assertNotEquals("orig:s1", ((SessionImpl) s1).getSession().getPropertySet().getStringProperty(PropertyKey.connectionAttributes).getValue());
        assertEquals(((SessionImpl) s1).getSession().getPropertySet().getStringProperty(PropertyKey.connectionAttributes).getInitialValue(),
                ((SessionImpl) s1).getSession().getPropertySet().getStringProperty(PropertyKey.connectionAttributes).getValue());

        /*
         * UT10/1: Get the max sessions allowed in the pool, create the same variables and temp tables in all the sessions,
         * save ((SessionImpl) sN).getSession().getServerSession().getThreadId() values.
         * Then close all the sessions and get a new one: the variables and the temp tables must not exist,
         * ((SessionImpl) sN).getSession().getServerSession().getThreadId() must return values equal to saved ones.
         */

        cli0 = cf.getClient(this.baseUrl, "{\"pooling\": {\"enabled\": true, \"maxSize\" : 2}}");
        s0 = cli0.getSession();
        s1 = cli0.getSession();
        long id0 = ((SessionImpl) s0).getSession().getServerSession().getThreadId();
        long id1 = ((SessionImpl) s1).getSession().getServerSession().getThreadId();

        s0.sql("SET @a='s0'").execute();
        s0.sql("CREATE TEMPORARY TABLE testpooledsessionstmps0(x int)").execute();

        s1.sql("SET @a='s1'").execute();
        s1.sql("CREATE TEMPORARY TABLE testpooledsessionstmps1(x int)").execute();

        SqlResult res = s0.sql("SELECT @a as a").execute();
        assertTrue(res.hasNext());
        assertEquals("s0", res.next().getString(0));
        res = s0.sql("SHOW CREATE TABLE testpooledsessionstmps0").execute();
        assertTrue(res.hasNext());
        assertEquals("testpooledsessionstmps0", res.next().getString(0));

        res = s1.sql("SELECT @a as a").execute();
        assertTrue(res.hasNext());
        assertEquals("s1", res.next().getString(0));
        res = s1.sql("SHOW CREATE TABLE testpooledsessionstmps1").execute();
        assertTrue(res.hasNext());
        assertEquals("testpooledsessionstmps1", res.next().getString(0));

        s0.close();
        s1.close();

        Session s0_new = cli0.getSession();
        Session s1_new = cli0.getSession();

        assertEquals(id0, ((SessionImpl) s0_new).getSession().getServerSession().getThreadId());
        assertEquals(id1, ((SessionImpl) s1_new).getSession().getServerSession().getThreadId());

        res = s0_new.sql("SELECT @a as a").execute();
        assertTrue(res.hasNext());
        assertNull(res.next().getString(0));

        assertThrows(XProtocolError.class, ".*testpooledsessionstmps0' doesn't exist", new Callable<Void>() {
            public Void call() throws Exception {
                s0_new.sql("SHOW CREATE TABLE testpooledsessionstmps0").execute();
                return null;
            }
        });

        res = s1_new.sql("SELECT @a as a").execute();
        assertTrue(res.hasNext());
        assertNull(res.next().getString(0));

        assertThrows(XProtocolError.class, ".*testpooledsessionstmps1' doesn't exist", new Callable<Void>() {
            public Void call() throws Exception {
                s1_new.sql("SHOW CREATE TABLE testpooledsessionstmps1").execute();
                return null;
            }
        });

        cli0.close();
    }

    private void testPooledSessions_checkClientProperties(Client cli, int maxSize, int maxIdleTime, int queueTimeout) throws Exception {
        Field f = ClientImpl.class.getDeclaredField("maxSize");
        f.setAccessible(true);
        assertEquals(maxSize, f.get(cli));

        f = ClientImpl.class.getDeclaredField("maxIdleTime");
        f.setAccessible(true);
        assertEquals(maxIdleTime, f.get(cli));

        f = ClientImpl.class.getDeclaredField("queueTimeout");
        f.setAccessible(true);
        assertEquals(queueTimeout, f.get(cli));
    }

    private <EX extends Throwable> void testPooledSessions_assertFailureTimeout(Client cli, int expLowLimit, int expUpLimit, Class<EX> throwable,
            String message) {
        long begin = System.currentTimeMillis();
        assertThrows(throwable, message, () -> cli.getSession());
        long end = System.currentTimeMillis() - begin;
        assertTrue(end >= expLowLimit && end < expUpLimit, "Expected: " + expLowLimit + ".." + expUpLimit + ". Got " + end);
    }

    @Test
    public void testBug28616573() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }

        RowResult res = this.session.sql(
                "select @@global.mysqlx_max_connections, VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Mysqlx_worker_threads_active'")
                .execute();
        Row r = res.next();
        int mysqlxMaxConnections = r.getInt(0);
        int mysqlWorkerThreadsActive = Integer.parseInt(r.getString(1));
        this.session.sql("SET @@global.mysqlx_max_connections=" + (mysqlWorkerThreadsActive + 2)).execute(); // allow only 2 additional connections

        Properties props = new Properties();
        props.setProperty(ClientProperty.POOLING_ENABLED.getKeyName(), "true");
        props.setProperty(ClientProperty.POOLING_MAX_SIZE.getKeyName(), "2");
        props.setProperty(ClientProperty.POOLING_MAX_IDLE_TIME.getKeyName(), "2000");
        props.setProperty(ClientProperty.POOLING_QUEUE_TIMEOUT.getKeyName(), "2000");

        try {
            ClientFactory cf = new ClientFactory();
            Client cli1 = cf.getClient(this.baseUrl, props);
            Client cli2 = cf.getClient(this.baseUrl, props);

            Session sess1 = cli1.getSession(); // new connection #1
            sess1.sql("SELECT 1").execute();
            sess1.close();
            sess1 = cli1.getSession(); // reuse connection #1
            Session sess2 = cli1.getSession(); // new connection #2
            sess2.sql("SELECT 1").execute();

            assertThrows(CJCommunicationsException.class, "Unable to connect to any of the target hosts\\.", cli2::getSession);

            cli1.close();
            cli2.close();

        } finally {
            this.session.sql("SET @@global.mysqlx_max_connections=" + mysqlxMaxConnections).execute();
        }
    }

    @Test
    public void testBug28606708() throws Exception {
        if (!this.isSetForXTests || !isServerRunningOnWindows()) {
            return;
        }

        for (String path : new String[] { null, "\\\\.\\pipe\\MySQL80" }) {
            String url = this.baseUrl + makeParam(PropertyKey.socketFactory, "com.mysql.cj.protocol.NamedPipeSocketFactory");
            if (path != null) {
                url += makeParam(PropertyKey.PATH, path);
            }
            try {
                this.fact.getSession(url);
                fail("The named-pipe connection attempt must fail with " + (path != null ? path : "default") + " path.");
            } catch (Exception e) {
                Throwable cause = e.getCause();
                if (cause != null) {
                    if (cause instanceof CJCommunicationsException && cause.getCause() != null && cause.getCause() instanceof FileNotFoundException
                            && ((path == null ? "\\\\.\\pipe\\MySQL" : path) + " (The system cannot find the file specified)")
                                    .equals(cause.getCause().getMessage())) {
                        continue;
                    } else if (cause instanceof XProtocolError
                            && "ASSERTION FAILED: Unknown message type: 10 (server messages mapping: null)".equals(cause.getMessage())) {
                        // if named pipes are enabled on server then we expect this error because the pipe is bound to legacy protocol
                        continue;
                    }
                }
                throw e;
            }
        }
    }

    @Test
    public void testSessionAttributes() throws Exception {
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.16"))) {
            return;
        }
        ClientFactory cf = new ClientFactory();
        Map<String, String> userAttributes = new HashMap<>();

        // TSFR1/TSFR2/TSFR3 Create a Session with xdevapi.connection-attributes in the connection string, verify that
        // the session is successfully established. Create a Session using a connection string containing the properties
        // as listed below, verify that the session is successfully established and the server contains the defined session attributes.
        //    xdevapi.connection-attributes=[key1=value1]
        //    xdevapi.connection-attributes=[key1=value1,key2=value2]
        //    xdevapi.connection-attributes=key1=value1
        //    xdevapi.connection-attributes=key1=value1,key2=value2
        userAttributes.clear();
        userAttributes.put("key1", "value1");

        String baseUrlLocal = this.baseUrl + (this.baseUrl.contains("?") ? "&" : "?");

        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1=value1", true), userAttributes);
        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1=value1]", true), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1=value1", true), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1=value1]", true), userAttributes);

        userAttributes.put("key2", "value2");

        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1=value1,key2=value2", true), userAttributes);
        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1=value1,key2=value2]", true),
                userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1=value1,key2=value2", true), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1=value1,key2=value2]", true), userAttributes);

        // TSFR4/TSFR5/TSFR6 Create a Session without xdevapi.connection-attributes in the connection string, verify that all predefined attributes
        // exist and contain the correct values. Verify that only connection attributes starting with "_" were set for current session.
        userAttributes.clear();
        testSessionAttributes_checkSession(this.baseUrl, userAttributes);
        testSessionAttributes_checkClient(this.baseUrl, userAttributes);

        // TSFR7 Create a Session using a connection string containing the properties as listed below, verify that a WrongArgumentException exception is thrown
        // with the message Key names in "xdevapi.connection-attributes" cannot start with "_".
        //    xdevapi.connection-attributes=[_key1=value1]
        //    xdevapi.connection-attributes=[key1=value1,_key2=value2]
        //    xdevapi.connection-attributes=_key1=value1
        //    xdevapi.connection-attributes=key1=value1,_key2=value2
        assertThrows(WrongArgumentException.class, "Key names in \"xdevapi.connection-attributes\" cannot start with \"_\".", () -> {
            this.fact.getSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[_key1=value1]", true));
            return null;
        });
        assertThrows(WrongArgumentException.class, "Key names in \"xdevapi.connection-attributes\" cannot start with \"_\".", () -> {
            Client cli1 = cf.getClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[_key1=value1]", true), new Properties());
            cli1.getSession();
            return null;
        });

        assertThrows(WrongArgumentException.class, "Key names in \"xdevapi.connection-attributes\" cannot start with \"_\".", () -> {
            this.fact.getSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1=value1,_key2=value2]", true));
            return null;
        });
        assertThrows(WrongArgumentException.class, "Key names in \"xdevapi.connection-attributes\" cannot start with \"_\".", () -> {
            Client cli1 = cf.getClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1=value1,_key2=value2]", true), new Properties());
            cli1.getSession();
            return null;
        });

        assertThrows(WrongArgumentException.class, "Key names in \"xdevapi.connection-attributes\" cannot start with \"_\".", () -> {
            this.fact.getSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "_key1=value1", true));
            return null;
        });
        assertThrows(WrongArgumentException.class, "Key names in \"xdevapi.connection-attributes\" cannot start with \"_\".", () -> {
            Client cli1 = cf.getClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "_key1=value1", true), new Properties());
            cli1.getSession();
            return null;
        });

        assertThrows(WrongArgumentException.class, "Key names in \"xdevapi.connection-attributes\" cannot start with \"_\".", () -> {
            this.fact.getSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1=value1,_key2=value2", true));
            return null;
        });
        assertThrows(WrongArgumentException.class, "Key names in \"xdevapi.connection-attributes\" cannot start with \"_\".", () -> {
            Client cli1 = cf.getClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1=value1,_key2=value2", true), new Properties());
            cli1.getSession();
            return null;
        });

        // TSFR9 Create a Session using a connection string containing the properties as listed below, verify
        // that the user-defined connection attribute key1 has an empty value.
        //    xdevapi.connection-attributes=[key1]
        //    xdevapi.connection-attributes=[key1,key2=value2]
        //    xdevapi.connection-attributes=[key1=]
        //    xdevapi.connection-attributes=[key1=,key2=value2]
        //    xdevapi.connection-attributes=key1
        //    xdevapi.connection-attributes=key1,key2=value2
        //    xdevapi.connection-attributes=key1=
        //    xdevapi.connection-attributes=key1=,key2=value2
        userAttributes.clear();
        userAttributes.put("key1", "");

        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1]", true), userAttributes);
        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1=]", true), userAttributes);
        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1", true), userAttributes);
        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1=", true), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1]", true), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1=]", true), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1", true), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1=", true), userAttributes);

        userAttributes.put("key2", "value2");

        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1,key2=value2]", true), userAttributes);
        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1=,key2=value2]", true), userAttributes);
        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1,key2=value2", true), userAttributes);
        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1=,key2=value2", true), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1,key2=value2]", true), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[key1=,key2=value2]", true), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1,key2=value2", true), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1=,key2=value2", true), userAttributes);

        // TSFR10 Create a Session with xdevapi.connection-attributes=false in the connection string, verify
        // that no connection attribute was set for current session.
        Session s10 = this.fact.getSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "false", true));
        SqlResult res = s10.sql("SELECT * FROM performance_schema.session_connect_attrs WHERE processlist_id = CONNECTION_ID()").execute();
        assertFalse(res.hasNext(), "Expected no connection attributes.");
        s10.close();

        Client c10 = cf.getClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "false", true), new Properties());
        s10 = c10.getSession();
        res = s10.sql("SELECT * FROM performance_schema.session_connect_attrs WHERE processlist_id = CONNECTION_ID()").execute();
        assertFalse(res.hasNext(), "Expected no connection attributes.");
        s10.close();
        c10.close();

        // TSFR11 Create a Session using a connection string containing the properties as listed below, verify
        // that only the client-defined connection attributes were set for the current session.
        //    xdevapi.connection-attributes
        //    xdevapi.connection-attributes=
        //    xdevapi.connection-attributes=true
        //    xdevapi.connection-attributes=[]
        userAttributes.clear();

        testSessionAttributes_checkSession(baseUrlLocal + PropertyKey.xdevapiConnectionAttributes.getKeyName(), userAttributes);
        testSessionAttributes_checkSession(baseUrlLocal + PropertyKey.xdevapiConnectionAttributes.getKeyName() + "=", userAttributes);
        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "true", true), userAttributes);
        testSessionAttributes_checkSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[]", true), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + PropertyKey.xdevapiConnectionAttributes.getKeyName(), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + PropertyKey.xdevapiConnectionAttributes.getKeyName() + "=", userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "true", true), userAttributes);
        testSessionAttributes_checkClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "[]", true), userAttributes);

        // TSFR13 Create a Session with xdevapi.connection-attributes=[key1=value1,key1=value2] in the connection string, verify that
        // a WrongArgumentException exception is thrown with the message Duplicate key "key1" used in "xdevapi.connection-attributes".
        assertThrows(WrongArgumentException.class, "Duplicate key \"key1\" used in \"xdevapi.connection-attributes\".", () -> {
            this.fact.getSession(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1=value1,key1=value2", true));
            return null;
        });
        assertThrows(WrongArgumentException.class, "Duplicate key \"key1\" used in \"xdevapi.connection-attributes\".", () -> {
            Client cli1 = cf.getClient(baseUrlLocal + makeParam(PropertyKey.xdevapiConnectionAttributes, "key1=value1,key1=value2", true), new Properties());
            cli1.getSession();
            return null;
        });
    }

    private void testSessionAttributes_checkSession(String url, Map<String, String> userAttributes) {
        Session s = this.fact.getSession(url);
        try {
            testSessionAttributes_checkSession(s, userAttributes);
        } finally {
            s.close();
        }
    }

    private void testSessionAttributes_checkSession(Session s, Map<String, String> userAttributes) {
        Map<String, Integer> matchedCounts = new HashMap<>();
        Map<String, String> matchValues = new HashMap<>();
        matchValues.put("_platform", Constants.OS_ARCH);
        matchValues.put("_os", Constants.OS_NAME + "-" + Constants.OS_VERSION);
        matchValues.put("_client_name", Constants.CJ_NAME);
        matchValues.put("_client_version", Constants.CJ_VERSION);
        matchValues.put("_client_license", Constants.CJ_LICENSE);
        matchValues.put("_runtime_version", Constants.JVM_VERSION);
        matchValues.put("_runtime_vendor", Constants.JVM_VENDOR);
        matchValues.putAll(userAttributes);

        SqlResult res = s.sql("SELECT * FROM performance_schema.session_connect_attrs WHERE processlist_id = CONNECTION_ID()").execute();
        while (res.hasNext()) {
            Row r = res.next();
            String key = r.getString(1);
            String val = r.getString(2);
            if (!matchValues.containsKey(key)) {
                fail("Unexpected connection attribute key:  " + key);
            }
            Integer cnt = matchedCounts.get(key);
            matchedCounts.put(key, cnt == null ? 1 : cnt++);

            // when client sends an empty string as an attribute value the NULL value is stored to performance_schema.session_connect_attrs
            String expected = matchValues.get(key);
            if (expected.equals("")) {
                expected = null;
            }
            assertEquals(expected, val);
        }
        for (String key : matchValues.keySet()) {
            if (!matchedCounts.containsKey(key)) {
                fail("Incorrect number of entries for key \"" + key + "\": 0");
            } else if (matchedCounts.get(key) != 1) {
                fail("Incorrect number of entries for key \"" + key + "\": " + matchedCounts.get(key));
            }
        }
    }

    private void testSessionAttributes_checkClient(String url, Map<String, String> userAttributes) throws Exception {
        ClientFactory cf = new ClientFactory();
        Client c = cf.getClient(url, new Properties());

        try {
            Session s0 = c.getSession();
            testSessionAttributes_checkSession(s0, userAttributes);
            s0.close(); // return to pool

            // check that pooled session set the same attributes
            Session s1 = c.getSession(); // get it from pool
            s1.sql("SELECT 1").execute();
            assertNotEquals(s0, s1);
            Field fProtocol = CoreSession.class.getDeclaredField("protocol");
            fProtocol.setAccessible(true);
            assertEquals(fProtocol.get(((SessionImpl) s0).getSession()), fProtocol.get(((SessionImpl) s1).getSession()));
            testSessionAttributes_checkSession(s1, userAttributes);
        } finally {
            c.close();
        }
    }

    @Test
    public void testPreparedStatementsCleanup() {
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
            return;
        }

        try {
            // Prepare test data.
            this.schema.createCollection("testPrepStmtClean", true).add("{\"_id\":\"1\"}").execute();

            SessionFactory sf = new SessionFactory();

            /*
             * Test common usage.
             */
            Session testSession = sf.getSession(this.testProperties);

            int sessionThreadId = getThreadId(testSession);
            assertPreparedStatementsCount(sessionThreadId, 0, 1);
            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

            // Initialize several *Statement objects.
            FindStatement testFind1 = testSession.getDefaultSchema().getCollection("testPrepStmtClean").find();
            SelectStatement testSelect1 = testSession.getDefaultSchema().getCollectionAsTable("testPrepStmtClean").select("_id");
            FindStatement testFind2 = testSession.getDefaultSchema().getCollection("testPrepStmtClean").find();
            SelectStatement testSelect2 = testSession.getDefaultSchema().getCollectionAsTable("testPrepStmtClean").select("_id");

            // 1st execute -> don't prepare.
            testFind1.execute();
            assertPreparedStatementsCountsAndId(testSession, 0, testFind1, 0, -1);
            testSelect1.execute();
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect1, 0, -1);
            testFind2.execute();
            assertPreparedStatementsCountsAndId(testSession, 0, testFind2, 0, -1);
            testSelect2.execute();
            assertPreparedStatementsCountsAndId(testSession, 0, testSelect2, 0, -1);

            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

            // 2nd execute -> prepare + execute.
            testFind1.execute();
            assertPreparedStatementsCountsAndId(testSession, 1, testFind1, 1, 1);
            testSelect1.execute();
            assertPreparedStatementsCountsAndId(testSession, 2, testSelect1, 2, 1);
            testFind2.execute();
            assertPreparedStatementsCountsAndId(testSession, 3, testFind2, 3, 1);
            testSelect2.execute();
            assertPreparedStatementsCountsAndId(testSession, 4, testSelect2, 4, 1);

            assertPreparedStatementsStatusCounts(testSession, 4, 4, 0);
            assertPreparedStatementsCount(sessionThreadId, 4, 1);

            /*
             * The following verifications are non-deterministic as System.gc() only hints the JVM to perform a garbage collection. This approach allows some
             * time for the JVM to execute the GC. In case of failure the repeats or wait times may have to be adjusted.
             * The test can be deleted entirely if no reasonable setup can be found.
             */

            // Nullify first statement.
            testFind1 = null;
            System.gc();
            int psCount, countdown = 10;
            do {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
                testSession.sql("SELECT 1").execute();
                psCount = getPreparedStatementsCount(sessionThreadId);
            } while (psCount != 3 && --countdown > 0);
            assertPreparedStatementsStatusCounts(testSession, 4, 4, 1);
            assertPreparedStatementsCount(sessionThreadId, 3, 1);

            // Nullify second and third statements.
            testSelect1 = null;
            testFind2 = null;
            System.gc();
            countdown = 10;
            do {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
                testSession.sql("SELECT 1").execute();
                psCount = getPreparedStatementsCount(sessionThreadId);
            } while (psCount != 1 && --countdown > 0);
            assertPreparedStatementsStatusCounts(testSession, 4, 4, 3);
            assertPreparedStatementsCount(sessionThreadId, 1, 1);

            // Nullify last statement.
            testSelect2 = null;
            System.gc();
            countdown = 10;
            do {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
                testSession.sql("SELECT 1").execute();
                psCount = getPreparedStatementsCount(sessionThreadId);
            } while (psCount != 0 && --countdown > 0);
            assertPreparedStatementsStatusCounts(testSession, 4, 4, 4);
            assertPreparedStatementsCount(sessionThreadId, 0, 1);

            testSession.close();
            assertPreparedStatementsCount(sessionThreadId, 0, 1);
        } finally {
            this.schema.dropCollection("testPrepStmtClean");
        }
    }

    @Test
    public void testPreparedStatementsPooledConnections() {
        if (!this.isSetForXTests || !mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.14"))) {
            return;
        }

        Properties props = new Properties();
        props.setProperty(ClientProperty.POOLING_ENABLED.getKeyName(), "true");
        props.setProperty(ClientProperty.POOLING_MAX_SIZE.getKeyName(), "1");

        try {
            this.schema.createCollection("testPrepStmtPooling", true).add("{\"_id\":\"1\"}").execute();

            ClientFactory cf = new ClientFactory();
            Client testClient = cf.getClient(this.baseUrl, props);

            Session testSession = testClient.getSession();
            int sessionThreadId = getThreadId(testSession);
            assertPreparedStatementsCount(sessionThreadId, 0, 1);

            FindStatement testFind = testSession.getDefaultSchema().getCollection("testPrepStmtPooling").find();

            // 1st execute -> don't prepare.
            testFind.execute();
            assertPreparedStatementsCountsAndId(testSession, 0, testFind, 0, -1);
            assertPreparedStatementsStatusCounts(testSession, 0, 0, 0);

            // 2nd execute -> prepare + execute.
            testFind.execute();
            assertPreparedStatementsCountsAndId(testSession, 1, testFind, 1, 1);
            assertPreparedStatementsStatusCounts(testSession, 1, 1, 0);

            assertPreparedStatementsCount(sessionThreadId, 1, 1);

            // Prepared statements won't live past closing the session, or returning it to the pool.
            testSession.close();
            assertPreparedStatementsCount(sessionThreadId, 0, 10);

            testSession = testClient.getSession();
            sessionThreadId = getThreadId(testSession);
            assertPreparedStatementsCount(sessionThreadId, 0, 1);

            // The underlying connection object in testFind is the same as the one returned from the pool to the new session.
            assertThrows(XProtocolError.class, "ERROR 5110 \\(HY000\\) Statement with ID=1 was not prepared", testFind::execute); // This exec attempt counts.
            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.16"))) { // Mysqlx.Session.Reset doesn't clear PS counters.
                assertPreparedStatementsStatusCounts(testSession, 1, 2, 0);
            } else {
                assertPreparedStatementsStatusCounts(testSession, 0, 1, 0);
            }

            testFind = testSession.getDefaultSchema().getCollection("testPrepStmtPooling").find();

            // 1st execute -> don't prepare.
            testFind.execute();
            assertPreparedStatementsCountsAndId(testSession, 0, testFind, 0, -1);
            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.16"))) { // Mysqlx.Session.Reset doesn't clear PS counters.
                assertPreparedStatementsStatusCounts(testSession, 1, 2, 0);
            } else {
                assertPreparedStatementsStatusCounts(testSession, 0, 1, 0);
            }
            // 2nd execute -> prepare + execute.
            testFind.execute();
            assertPreparedStatementsCountsAndId(testSession, 1, testFind, 1, 1);
            if (mysqlVersionMeetsMinimum(ServerVersion.parseVersion("8.0.16"))) { // Mysqlx.Session.Reset doesn't clear PS counters.
                assertPreparedStatementsStatusCounts(testSession, 2, 3, 0);
            } else {
                assertPreparedStatementsStatusCounts(testSession, 1, 2, 0);
            }

            assertPreparedStatementsCount(sessionThreadId, 1, 1);

            // Prepared statements won't live past closing the client and its sessions.
            testClient.close();
            assertPreparedStatementsCount(sessionThreadId, 0, 10);

            assertThrows(CJCommunicationsException.class, "Unable to write message", testFind::execute);
        } finally {
            this.schema.dropCollection("testPrepStmtPooling");
        }
    }

    @Test
    public void testBug23721537() throws Exception {
        if (!this.isSetForXTests) {
            return;
        }
        try {
            sqlUpdate("drop table if exists testBug23721537");
            sqlUpdate("create table testBug23721537 (id int, name varchar(20) not null)");
            sqlUpdate("insert into testBug23721537 values (0, 'a')");

            this.session.sql("drop procedure if exists newproc").execute();
            this.session.sql(
                    "create procedure newproc (in p1 int,in p2 char(20)) begin select 1; update testBug23721537 set name='b' where id=0; select 2; select 3; end;")
                    .execute();

            /* sync execution */
            SqlResult res1 = this.session.sql("call newproc(?,?)").bind(10).bind("X").execute();

            assertTrue(res1 instanceof SqlMultiResult);
            assertTrue(res1.hasData());
            assertTrue(res1.hasNext());
            Row r = res1.next();
            assertEquals(1, r.getInt(0));
            assertFalse(res1.hasNext());

            SqlResult res2 = this.session.sql("select 10").execute(); // res1 should finish streaming here

            assertTrue(res1.nextResult());
            assertTrue(res1.hasData());
            assertTrue(res1.hasNext());
            r = res1.next();
            assertEquals(2, r.getInt(0));
            assertFalse(res1.hasNext());

            assertTrue(res1.nextResult());
            assertTrue(res1.hasData());
            assertTrue(res1.hasNext());
            r = res1.next();
            assertEquals(3, r.getInt(0));
            assertFalse(res1.hasNext());

            assertFalse(res1.nextResult());

            //
            assertTrue(res2.hasData());
            assertTrue(res2.hasNext());
            r = res2.next();
            assertEquals(10, r.getInt(0));
            assertFalse(res2.hasNext());
            assertFalse(res2.nextResult());

            /* async execution */
            res1 = this.session.sql("call newproc(?,?)").bind(10).bind("X").executeAsync().get();

            assertTrue(res1.hasData());
            r = res1.next();
            assertEquals(1, r.getInt(0));
            assertFalse(res1.hasNext());

            res2 = this.session.sql("select 10").executeAsync().get(); // res1 should finish streaming here

            assertTrue(res1.nextResult());
            assertTrue(res1.hasData());
            assertTrue(res1.hasNext());
            r = res1.next();
            assertEquals(2, r.getInt(0));
            assertFalse(res1.hasNext());

            assertTrue(res1.nextResult());
            assertTrue(res1.hasData());
            assertTrue(res1.hasNext());
            r = res1.next();
            assertEquals(3, r.getInt(0));
            assertFalse(res1.hasNext());

            assertFalse(res1.nextResult());

            //
            assertTrue(res2.hasData());
            assertTrue(res2.hasNext());
            r = res2.next();
            assertEquals(10, r.getInt(0));
            assertFalse(res2.hasNext());
            assertFalse(res2.nextResult());

        } finally {
            sqlUpdate("drop table if exists testBug23721537");
            this.session.sql("drop procedure if exists newproc").execute();
        }
    }

    @Test
    public void basicSessionFailoverRandomSort() {
        if (!this.isSetForXTests) {
            return;
        }
        UnreliableSocketFactory.flushAllStaticData();

        final String testUriPattern = "mysqlx://%s:%s@[%s]/%s?" + PropertyKey.xdevapiConnectTimeout.getKeyName() + "=100&"
                + PropertyKey.socketFactory.getKeyName() + "=" + UnreliableSocketFactory.class.getName();
        String testHosts = IntStream.range(1, 6).mapToObj(i -> "host" + i).peek(h -> UnreliableSocketFactory.mapHost(h, getTestHost()))
                .map(h -> h + ":" + getTestPort()).collect(Collectors.joining(","));
        String testUri = String.format(testUriPattern, getTestUser(), getTestPassword(), testHosts, getTestDatabase());

        Set<String> downHosts = new HashSet<>();
        for (int i = 1; i <= 5; i++) {
            Session testSession = this.fact.getSession(testUri);
            assertTrue(UnreliableSocketFactory.isConnected());
            testSession.close();

            String lastHost = UnreliableSocketFactory.getHostFromLastConnection();
            for (String h : downHosts) {
                assertNotEquals(h, lastHost);
            }
            downHosts.add(lastHost);
            UnreliableSocketFactory.downHost(lastHost.substring(1));
        }

        // None of the hosts is available by now.
        assertThrows(CJCommunicationsException.class, "Unable to connect to any of the target hosts\\.", () -> {
            this.fact.getSession(testUri);
            return null;
        });

        UnreliableSocketFactory.dontDownHost("host3");

        UnreliableSocketFactory.flushConnectionAttempts();
        Session testSession = this.fact.getSession(testUri);
        assertTrue(UnreliableSocketFactory.isConnected());
        testSession.close();
        assertEquals(UnreliableSocketFactory.getHostConnectedStatus("host3"), UnreliableSocketFactory.getHostFromLastConnection());
    }

    @Test
    public void basicSessionFailoverByPriorities() {
        if (!this.isSetForXTests) {
            return;
        }
        UnreliableSocketFactory.flushAllStaticData();

        final String testUriPattern = "mysqlx://%s:%s@[%s]/%s?" + PropertyKey.xdevapiConnectTimeout.getKeyName() + "=100&"
                + PropertyKey.socketFactory.getKeyName() + "=" + UnreliableSocketFactory.class.getName();
        String testHosts = IntStream.range(1, 6).mapToObj(i -> "host" + i).peek(h -> UnreliableSocketFactory.mapHost(h, getTestHost()))
                .map(h -> "(address=" + h + ":" + getTestPort() + ",priority=%d)").collect(Collectors.joining(","));
        String testUriPatternPriorities = String.format(testUriPattern, getTestUser(), getTestPassword(), testHosts, getTestDatabase());
        String testUri = String.format(testUriPatternPriorities, 60, 80, 100, 20, 40);
        int[] hostsOrder = new int[] { 3, 2, 1, 5, 4 };

        for (int i = 0; i < hostsOrder.length; i++) {
            int h = hostsOrder[i];

            Session testSession = this.fact.getSession(testUri);
            assertTrue(UnreliableSocketFactory.isConnected());
            testSession.close();

            String lastHost = UnreliableSocketFactory.getHostFromLastConnection();
            assertEquals(UnreliableSocketFactory.getHostConnectedStatus("host" + h), lastHost);
            List<String> connectionAttempts = UnreliableSocketFactory.getHostsFromAllConnections();
            for (int a = 0; a < i; a++) {
                assertEquals(UnreliableSocketFactory.getHostFailedStatus("host" + hostsOrder[a]), connectionAttempts.get(a));
            }
            UnreliableSocketFactory.downHost("host" + h);
            UnreliableSocketFactory.flushConnectionAttempts();
        }

        // None of the hosts is available by now.
        assertThrows(CJCommunicationsException.class, "Unable to connect to any of the target hosts\\.", () -> {
            this.fact.getSession(testUri);
            return null;
        });

        UnreliableSocketFactory.dontDownHost("host" + hostsOrder[1]);

        UnreliableSocketFactory.flushConnectionAttempts();
        Session testSession = this.fact.getSession(testUri);
        assertTrue(UnreliableSocketFactory.isConnected());
        testSession.close();
        List<String> connectionAttempts = UnreliableSocketFactory.getHostsFromAllConnections();
        assertEquals(2, connectionAttempts.size());
        assertEquals(UnreliableSocketFactory.getHostFailedStatus("host" + hostsOrder[0]), connectionAttempts.get(0));
        assertEquals(UnreliableSocketFactory.getHostConnectedStatus("host" + hostsOrder[1]), connectionAttempts.get(1));
    }

    @Test
    public void pooledSessionFailoverRandomSortAndPooling() {
        if (!this.isSetForXTests) {
            return;
        }
        UnreliableSocketFactory.flushAllStaticData();

        final String testUriPattern = "mysqlx://%s:%s@[%s]/%s?" + PropertyKey.xdevapiConnectTimeout.getKeyName() + "=100&"
                + PropertyKey.socketFactory.getKeyName() + "=" + UnreliableSocketFactory.class.getName();
        String testHosts = IntStream.range(1, 6).mapToObj(i -> "host" + i).peek(h -> UnreliableSocketFactory.mapHost(h, getTestHost()))
                .map(h -> h + ":" + getTestPort()).collect(Collectors.joining(","));
        String testUri = String.format(testUriPattern, getTestUser(), getTestPassword(), testHosts, getTestDatabase());

        final ClientFactory cf = new ClientFactory();
        Client client = cf.getClient(testUri, "{\"pooling\" : {\"enabled\" : true, \"maxSize\" : 10} }");

        Set<String> downHosts = new HashSet<>();
        for (int i = 1; i <= 5; i++) {
            Session testSession = client.getSession();
            assertTrue(UnreliableSocketFactory.isConnected());
            testSession.close(); // Pool this connection.

            String lastHost = UnreliableSocketFactory.getHostFromLastConnection();
            for (String h : downHosts) {
                assertNotEquals(h, lastHost);
            }
            downHosts.add(lastHost);
            UnreliableSocketFactory.downHost(lastHost.substring(1));
            UnreliableSocketFactory.flushConnectionAttempts();
        }

        // None of the hosts is available by now.
        assertThrows(CJCommunicationsException.class, "Unable to connect to any of the target hosts\\.", () -> {
            client.getSession();
            return null;
        });

        UnreliableSocketFactory.dontDownHost("host3");

        UnreliableSocketFactory.flushConnectionAttempts();
        Session testSession = client.getSession();
        assertTrue(UnreliableSocketFactory.isConnected());
        testSession.close();
        assertEquals(UnreliableSocketFactory.getHostConnectedStatus("host3"), UnreliableSocketFactory.getHostFromLastConnection());

        UnreliableSocketFactory.flushConnectionAttempts();
        Session testSession1 = client.getSession(); // Pick previous connection from the pool. Doesn't count as new connections.
        assertEquals(0, UnreliableSocketFactory.getHostsFromAllConnections().size());
        Session testSession2 = client.getSession(); // Create a new connection.
        assertTrue(UnreliableSocketFactory.isConnected());
        testSession1.close();
        testSession2.close();
        assertEquals(UnreliableSocketFactory.getHostConnectedStatus("host3"), UnreliableSocketFactory.getHostFromLastConnection());

        client.close();
    }

    @Test
    public void pooledSessionFailoverRandomSortAndNoPooling() {
        if (!this.isSetForXTests) {
            return;
        }
        UnreliableSocketFactory.flushAllStaticData();

        final String testUriPattern = "mysqlx://%s:%s@[%s]/%s?" + PropertyKey.xdevapiConnectTimeout.getKeyName() + "=100&"
                + PropertyKey.socketFactory.getKeyName() + "=" + UnreliableSocketFactory.class.getName();
        String testHosts = IntStream.range(1, 6).mapToObj(i -> "host" + i).peek(h -> UnreliableSocketFactory.mapHost(h, getTestHost()))
                .map(h -> h + ":" + getTestPort()).collect(Collectors.joining(","));
        String testUri = String.format(testUriPattern, getTestUser(), getTestPassword(), testHosts, getTestDatabase());

        final ClientFactory cf = new ClientFactory();
        Client client = cf.getClient(testUri, "{\"pooling\" : {\"enabled\" : true, \"maxSize\" : 10} }");

        Set<String> downHosts = new HashSet<>();
        for (int i = 1; i <= 5; i++) {
            client.getSession(); // Don't pool this connection.
            assertTrue(UnreliableSocketFactory.isConnected());

            String lastHost = UnreliableSocketFactory.getHostFromLastConnection();
            for (String h : downHosts) {
                assertNotEquals(h, lastHost);
            }
            downHosts.add(lastHost);
            UnreliableSocketFactory.downHost(lastHost.substring(1));
            UnreliableSocketFactory.flushConnectionAttempts();
        }

        // None of the hosts is available by now.
        assertThrows(CJCommunicationsException.class, "Unable to connect to any of the target hosts\\.", () -> {
            client.getSession();
            return null;
        });

        UnreliableSocketFactory.dontDownHost("host3");

        UnreliableSocketFactory.flushConnectionAttempts();
        client.getSession();
        assertTrue(UnreliableSocketFactory.isConnected());
        assertEquals(UnreliableSocketFactory.getHostConnectedStatus("host3"), UnreliableSocketFactory.getHostFromLastConnection());

        UnreliableSocketFactory.flushConnectionAttempts();
        client.getSession();
        assertTrue(UnreliableSocketFactory.isConnected());
        assertEquals(UnreliableSocketFactory.getHostConnectedStatus("host3"), UnreliableSocketFactory.getHostFromLastConnection());

        client.close();
    }

    @Test
    public void pooledSessionFailoverByPrioritiesAndPooling() {
        if (!this.isSetForXTests) {
            return;
        }
        UnreliableSocketFactory.flushAllStaticData();

        final String testUriPattern = "mysqlx://%s:%s@[%s]/%s?" + PropertyKey.xdevapiConnectTimeout.getKeyName() + "=100&"
                + PropertyKey.socketFactory.getKeyName() + "=" + UnreliableSocketFactory.class.getName();
        String testHosts = IntStream.range(1, 6).mapToObj(i -> "host" + i).peek(h -> UnreliableSocketFactory.mapHost(h, getTestHost()))
                .map(h -> "(address=" + h + ":" + getTestPort() + ",priority=%d)").collect(Collectors.joining(","));
        String testUriPatternPriorities = String.format(testUriPattern, getTestUser(), getTestPassword(), testHosts, getTestDatabase());
        String testUri = String.format(testUriPatternPriorities, 60, 80, 100, 20, 40);
        int[] hostsOrder = new int[] { 3, 2, 1, 5, 4 };

        final ClientFactory cf = new ClientFactory();
        Client client = cf.getClient(testUri, "{\"pooling\" : {\"enabled\" : true, \"maxSize\" : 10} }");

        for (int i = 0; i < hostsOrder.length; i++) {
            int h = hostsOrder[i];

            Session testSession = client.getSession();
            assertTrue(UnreliableSocketFactory.isConnected());
            testSession.close(); // Pool this connection.

            String lastHost = UnreliableSocketFactory.getHostFromLastConnection();
            assertEquals(UnreliableSocketFactory.getHostConnectedStatus("host" + h), lastHost);
            List<String> connectionAttempts = UnreliableSocketFactory.getHostsFromAllConnections();
            assertEquals(i == 0 ? 1 : 2, connectionAttempts.size());
            for (int a = 0; a < connectionAttempts.size() - 1; a++) {
                assertEquals(UnreliableSocketFactory.getHostFailedStatus("host" + hostsOrder[i - 1 + a]), connectionAttempts.get(a));
            }
            UnreliableSocketFactory.downHost("host" + h);
            UnreliableSocketFactory.flushConnectionAttempts();
        }

        // None of the hosts is available by now.
        assertThrows(CJCommunicationsException.class, "Unable to connect to any of the target hosts\\.", () -> {
            client.getSession();
            return null;
        });
        // Final connection tried the last known to be good host (host4) and then the remaining hosts by their priority
        List<String> connectionAttempts = UnreliableSocketFactory.getHostsFromAllConnections();
        for (int i = 0; i < hostsOrder.length; i++) {
            assertEquals(UnreliableSocketFactory.getHostFailedStatus("host" + hostsOrder[(i + 4) % 5]), connectionAttempts.get(i));
        }

        UnreliableSocketFactory.dontDownHost("host" + hostsOrder[1]);

        UnreliableSocketFactory.flushConnectionAttempts();
        Session testSession = client.getSession();
        assertTrue(UnreliableSocketFactory.isConnected());
        testSession.close();
        connectionAttempts = UnreliableSocketFactory.getHostsFromAllConnections();
        assertEquals(2, connectionAttempts.size());
        assertEquals(UnreliableSocketFactory.getHostFailedStatus("host" + hostsOrder[0]), connectionAttempts.get(0));
        assertEquals(UnreliableSocketFactory.getHostConnectedStatus("host" + hostsOrder[1]), connectionAttempts.get(1));

        UnreliableSocketFactory.flushConnectionAttempts();
        Session testSession1 = client.getSession(); // Pick previous connection from the pool. Doesn't count as new connections.
        assertEquals(0, UnreliableSocketFactory.getHostsFromAllConnections().size());
        Session testSession2 = client.getSession(); // Create a new connection.
        assertTrue(UnreliableSocketFactory.isConnected());
        testSession1.close();
        testSession2.close();
        connectionAttempts = UnreliableSocketFactory.getHostsFromAllConnections();
        assertEquals(1, connectionAttempts.size());
        assertEquals(UnreliableSocketFactory.getHostConnectedStatus("host" + hostsOrder[1]), connectionAttempts.get(0));

        client.close();
    }

    @Test
    public void pooledSessionFailoverByPrioritiesAndNoPooling() {
        if (!this.isSetForXTests) {
            return;
        }
        UnreliableSocketFactory.flushAllStaticData();

        final String testUriPattern = "mysqlx://%s:%s@[%s]/%s?" + PropertyKey.xdevapiConnectTimeout.getKeyName() + "=100&"
                + PropertyKey.socketFactory.getKeyName() + "=" + UnreliableSocketFactory.class.getName();
        String testHosts = IntStream.range(1, 6).mapToObj(i -> "host" + i).peek(h -> UnreliableSocketFactory.mapHost(h, getTestHost()))
                .map(h -> "(address=" + h + ":" + getTestPort() + ",priority=%d)").collect(Collectors.joining(","));
        String testUriPatternPriorities = String.format(testUriPattern, getTestUser(), getTestPassword(), testHosts, getTestDatabase());
        String testUri = String.format(testUriPatternPriorities, 60, 80, 100, 20, 40);
        int[] hostsOrder = new int[] { 3, 2, 1, 5, 4 };

        final ClientFactory cf = new ClientFactory();
        Client client = cf.getClient(testUri, "{\"pooling\" : {\"enabled\" : true, \"maxSize\" : 10} }");

        for (int i = 0; i < hostsOrder.length; i++) {
            int h = hostsOrder[i];

            client.getSession(); // Don't pool this connection.
            assertTrue(UnreliableSocketFactory.isConnected());

            String lastHost = UnreliableSocketFactory.getHostFromLastConnection();
            assertEquals(UnreliableSocketFactory.getHostConnectedStatus("host" + h), lastHost);
            List<String> connectionAttempts = UnreliableSocketFactory.getHostsFromAllConnections();
            assertEquals(i == 0 ? 1 : 2, connectionAttempts.size());
            for (int a = 0; a < connectionAttempts.size() - 1; a++) {
                assertEquals(UnreliableSocketFactory.getHostFailedStatus("host" + hostsOrder[i - 1 + a]), connectionAttempts.get(a));
            }
            UnreliableSocketFactory.downHost("host" + h);
            UnreliableSocketFactory.flushConnectionAttempts();
        }

        // None of the hosts is available by now.
        assertThrows(CJCommunicationsException.class, "Unable to connect to any of the target hosts\\.", () -> {
            client.getSession();
            return null;
        });
        // Final connection tried the last known to be good host (host4) and then the remaining hosts by their priority
        List<String> connectionAttempts = UnreliableSocketFactory.getHostsFromAllConnections();
        for (int i = 0; i < hostsOrder.length; i++) {
            assertEquals(UnreliableSocketFactory.getHostFailedStatus("host" + hostsOrder[(i + 4) % 5]), connectionAttempts.get(i));
        }

        UnreliableSocketFactory.dontDownHost("host" + hostsOrder[1]);

        UnreliableSocketFactory.flushConnectionAttempts();
        client.getSession();
        assertTrue(UnreliableSocketFactory.isConnected());
        connectionAttempts = UnreliableSocketFactory.getHostsFromAllConnections();
        assertEquals(2, connectionAttempts.size());
        assertEquals(UnreliableSocketFactory.getHostFailedStatus("host" + hostsOrder[0]), connectionAttempts.get(0));
        assertEquals(UnreliableSocketFactory.getHostConnectedStatus("host" + hostsOrder[1]), connectionAttempts.get(1));

        UnreliableSocketFactory.flushConnectionAttempts();
        client.getSession();
        assertTrue(UnreliableSocketFactory.isConnected());
        connectionAttempts = UnreliableSocketFactory.getHostsFromAllConnections();
        assertEquals(1, connectionAttempts.size());
        assertEquals(UnreliableSocketFactory.getHostConnectedStatus("host" + hostsOrder[1]), connectionAttempts.get(0));

        client.close();
    }
}
