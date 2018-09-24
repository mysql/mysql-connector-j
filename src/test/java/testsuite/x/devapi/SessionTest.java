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

package testsuite.x.devapi;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mysql.cj.CoreSession;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJPacketTooBigException;
import com.mysql.cj.exceptions.FeatureNotAvailableException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.protocol.x.XProtocolError;
import com.mysql.cj.xdevapi.Client;
import com.mysql.cj.xdevapi.Client.ClientProperty;
import com.mysql.cj.xdevapi.ClientFactory;
import com.mysql.cj.xdevapi.ClientImpl;
import com.mysql.cj.xdevapi.ClientImpl.PooledXProtocol;
import com.mysql.cj.xdevapi.Row;
import com.mysql.cj.xdevapi.RowResult;
import com.mysql.cj.xdevapi.Schema;
import com.mysql.cj.xdevapi.Session;
import com.mysql.cj.xdevapi.SessionFactory;
import com.mysql.cj.xdevapi.SessionImpl;
import com.mysql.cj.xdevapi.SqlResult;
import com.mysql.cj.xdevapi.SqlStatement;
import com.mysql.cj.xdevapi.XDevAPIError;

public class SessionTest extends DevApiBaseTestCase {
    @Before
    public void setupCollectionTest() {
        setupTestSession();
    }

    @After
    public void teardownCollectionTest() {
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
     */
    private String getRandomTestSchemaName() {
        String n = "cj_test_schema_no_" + new Random().nextInt(1000);
        this.createdTestSchemas.add(n);
        return n;
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
     * 
     * @throws Exception
     *             if the test fails.
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
        assertTrue("Expected wait time 10000 ms but was " + end, end >= 10000);
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
         * TODO: Add when xplugin is ready
         * UT10/1: Get the max sessions allowed in the pool, create the same variables and temp tables in all the sessions. Then close all the sessions and get
         * a new one: the variables and the temp tables must not exist.
         */
        cli0 = cf.getClient(this.baseUrl, "{\"pooling\": {\"enabled\": true, \"maxSize\" : 2}}");
        s0 = cli0.getSession();
        s1 = cli0.getSession();

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
        assertTrue("Expected: " + expLowLimit + ".." + expUpLimit + ". Got " + end, end >= expLowLimit && end < expUpLimit);
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

            assertThrows(CJCommunicationsException.class, "Cannot read packet header", () -> cli2.getSession());

            cli1.close();
            cli2.close();

        } finally {
            this.session.sql("SET @@global.mysqlx_max_connections=" + mysqlxMaxConnections).execute();
        }
    }
}
