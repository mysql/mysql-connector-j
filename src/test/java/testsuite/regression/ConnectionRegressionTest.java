/*
 * Copyright (c) 2002, 2019, Oracle and/or its affiliates. All rights reserved.
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

package testsuite.regression;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.security.cert.CertificateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.net.ssl.SSLContext;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.Constants;
import com.mysql.cj.Messages;
import com.mysql.cj.MysqlConnection;
import com.mysql.cj.NativeSession;
import com.mysql.cj.Query;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.Session;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.PropertyDefinitions.DatabaseTerm;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyDefinitions.ZeroDatetimeBehavior;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.url.ReplicationConnectionUrl;
import com.mysql.cj.exceptions.ClosedOnExpiredPasswordException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.PasswordExpiredException;
import com.mysql.cj.exceptions.PropertyNotModifiableException;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.ClientPreparedStatement;
import com.mysql.cj.jdbc.ConnectionGroupManager;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.MysqlConnectionPoolDataSource;
import com.mysql.cj.jdbc.MysqlDataSource;
import com.mysql.cj.jdbc.MysqlPooledConnection;
import com.mysql.cj.jdbc.MysqlXAConnection;
import com.mysql.cj.jdbc.MysqlXADataSource;
import com.mysql.cj.jdbc.MysqlXid;
import com.mysql.cj.jdbc.NonRegisteringDriver;
import com.mysql.cj.jdbc.SuspendableXAConnection;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.mysql.cj.jdbc.exceptions.MysqlDataTruncation;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.ha.LoadBalanceExceptionChecker;
import com.mysql.cj.jdbc.ha.LoadBalancedConnectionProxy;
import com.mysql.cj.jdbc.ha.RandomBalanceStrategy;
import com.mysql.cj.jdbc.ha.ReplicationConnection;
import com.mysql.cj.jdbc.ha.ReplicationConnectionGroup;
import com.mysql.cj.jdbc.ha.ReplicationConnectionGroupManager;
import com.mysql.cj.jdbc.ha.ReplicationConnectionProxy;
import com.mysql.cj.jdbc.ha.SequentialBalanceStrategy;
import com.mysql.cj.jdbc.integration.jboss.MysqlValidConnectionChecker;
import com.mysql.cj.jdbc.jmx.ReplicationGroupManagerMBean;
import com.mysql.cj.log.Log;
import com.mysql.cj.log.ProfilerEvent;
import com.mysql.cj.log.ProfilerEventImpl;
import com.mysql.cj.log.StandardLogger;
import com.mysql.cj.protocol.AuthenticationPlugin;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.MessageReader;
import com.mysql.cj.protocol.MessageSender;
import com.mysql.cj.protocol.PacketReceivedTimeHolder;
import com.mysql.cj.protocol.PacketSentTimeHolder;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.StandardSocketFactory;
import com.mysql.cj.protocol.a.DebugBufferingPacketReader;
import com.mysql.cj.protocol.a.DebugBufferingPacketSender;
import com.mysql.cj.protocol.a.MultiPacketReader;
import com.mysql.cj.protocol.a.NativePacketHeader;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.protocol.a.NativeProtocol;
import com.mysql.cj.protocol.a.SimplePacketReader;
import com.mysql.cj.protocol.a.SimplePacketSender;
import com.mysql.cj.protocol.a.TimeTrackingPacketReader;
import com.mysql.cj.protocol.a.TimeTrackingPacketSender;
import com.mysql.cj.protocol.a.authentication.CachingSha2PasswordPlugin;
import com.mysql.cj.protocol.a.authentication.MysqlNativePasswordPlugin;
import com.mysql.cj.protocol.a.authentication.MysqlOldPasswordPlugin;
import com.mysql.cj.protocol.a.authentication.Sha256PasswordPlugin;
import com.mysql.cj.util.LogUtils;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;

import testsuite.BaseQueryInterceptor;
import testsuite.BaseTestCase;
import testsuite.BufferingLogger;
import testsuite.UnreliableSocketFactory;

/**
 * Regression tests for Connections
 */
public class ConnectionRegressionTest extends BaseTestCase {
    /**
     * @param name
     *            the name of the testcase
     */
    public ConnectionRegressionTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(ConnectionRegressionTest.class);
    }

    public void testBug1914() throws Exception {
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), BIGINT)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), BINARY)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), BIT)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), CHAR)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), DATE)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), DECIMAL)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), DOUBLE)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), FLOAT)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), INTEGER)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), LONGVARBINARY)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), LONGVARCHAR)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), TIME)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), TIMESTAMP)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), TINYINT)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), VARBINARY)}"));
        System.out.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), VARCHAR)}"));
    }

    /**
     * Tests fix for BUG#3554 - Not specifying database in URL causes
     * MalformedURL exception.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testBug3554() throws Exception {
        try {
            new NonRegisteringDriver().connect("jdbc:mysql://localhost:3306/?user=root&password=root", new Properties());
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage().indexOf("Malformed") == -1);
        }
    }

    public void testBug3790() throws Exception {
        String field2OldValue = "foo";
        String field2NewValue = "bar";
        int field1OldValue = 1;

        Connection conn1 = null;
        Connection conn2 = null;
        Statement stmt1 = null;
        Statement stmt2 = null;
        ResultSet rs2 = null;

        Properties props = new Properties();

        try {
            createTable("testBug3790", "(field1 INT NOT NULL PRIMARY KEY, field2 VARCHAR(32)) ", "InnoDB");
            this.stmt.executeUpdate("INSERT INTO testBug3790 VALUES (" + field1OldValue + ", '" + field2OldValue + "')");

            conn1 = getConnectionWithProps(props); // creates a new connection
            conn2 = getConnectionWithProps(props); // creates another new
            // connection
            conn1.setAutoCommit(false);
            conn2.setAutoCommit(false);

            stmt1 = conn1.createStatement();
            stmt1.executeUpdate("UPDATE testBug3790 SET field2 = '" + field2NewValue + "' WHERE field1=" + field1OldValue);
            conn1.commit();

            stmt2 = conn2.createStatement();

            rs2 = stmt2.executeQuery("SELECT field1, field2 FROM testBug3790");

            assertTrue(rs2.next());
            assertTrue(rs2.getInt(1) == field1OldValue);
            assertTrue(rs2.getString(2).equals(field2NewValue));
        } finally {
            if (rs2 != null) {
                rs2.close();
            }

            if (stmt2 != null) {
                stmt2.close();
            }

            if (stmt1 != null) {
                stmt1.close();
            }

            if (conn1 != null) {
                conn1.close();
            }

            if (conn2 != null) {
                conn2.close();
            }
        }
    }

    /**
     * Tests if the driver configures character sets correctly for 4.1.x
     * servers. Requires that the 'admin connection' is configured, as this test
     * needs to create/drop databases.
     * 
     * @throws Exception
     *             if an error occurs
     */
    public void testCollation41() throws Exception {
        if (isAdminConnectionConfigured()) {
            Map<String, String> charsetsAndCollations = getCharacterSetsAndCollations();
            charsetsAndCollations.remove("latin7"); // Maps to multiple Java
            // charsets
            charsetsAndCollations.remove("ucs2"); // can't be used as a
            // connection charset

            for (String charsetName : charsetsAndCollations.keySet()) {
                Connection charsetConn = null;
                Statement charsetStmt = null;

                try {
                    //String collationName = charsetsAndCollations.get(charsetName);

                    Properties props = new Properties();
                    props.setProperty(PropertyKey.characterEncoding.getKeyName(), charsetName);

                    System.out.println("Testing character set " + charsetName);

                    charsetConn = getAdminConnectionWithProps(props);

                    charsetStmt = charsetConn.createStatement();

                    charsetStmt.executeUpdate("DROP DATABASE IF EXISTS testCollation41");
                    charsetStmt.executeUpdate("DROP TABLE IF EXISTS testCollation41");
                    charsetStmt.executeUpdate("CREATE DATABASE testCollation41 DEFAULT CHARACTER SET " + charsetName);
                    charsetStmt.close();

                    charsetConn.setCatalog("testCollation41");

                    // We've switched catalogs, so we need to recreate the
                    // statement to pick this up...
                    charsetStmt = charsetConn.createStatement();

                    StringBuilder createTableCommand = new StringBuilder("CREATE TABLE testCollation41(field1 VARCHAR(255), field2 INT)");

                    charsetStmt.executeUpdate(createTableCommand.toString());

                    charsetStmt.executeUpdate("INSERT INTO testCollation41 VALUES ('abc', 0)");

                    int updateCount = charsetStmt.executeUpdate("UPDATE testCollation41 SET field2=1 WHERE field1='abc'");
                    assertTrue(updateCount == 1);
                } finally {
                    if (charsetStmt != null) {
                        charsetStmt.executeUpdate("DROP TABLE IF EXISTS testCollation41");
                        charsetStmt.executeUpdate("DROP DATABASE IF EXISTS testCollation41");
                        charsetStmt.close();
                    }

                    if (charsetConn != null) {
                        charsetConn.close();
                    }
                }
            }
        }
    }

    /**
     * Tests setReadOnly() being reset during failover
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testSetReadOnly() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");

        String sepChar = "?";

        if (BaseTestCase.dbUrl.indexOf("?") != -1) {
            sepChar = "&";
        }

        Connection reconnectableConn = DriverManager.getConnection(BaseTestCase.dbUrl + sepChar + "autoReconnect=true", props);

        this.rs = reconnectableConn.createStatement().executeQuery("SELECT CONNECTION_ID()");
        this.rs.next();

        String connectionId = this.rs.getString(1);

        reconnectableConn.setReadOnly(true);

        boolean isReadOnly = reconnectableConn.isReadOnly();

        Connection killConn = getConnectionWithProps((Properties) null);

        killConn.createStatement().executeUpdate("KILL " + connectionId);
        Thread.sleep(2000);

        SQLException caughtException = null;

        int numLoops = 8;

        while (caughtException == null && numLoops > 0) {
            numLoops--;

            try {
                reconnectableConn.createStatement().executeQuery("SELECT 1");
            } catch (SQLException sqlEx) {
                caughtException = sqlEx;
            }
        }

        System.out.println("Executing statement on reconnectable connection...");

        this.rs = reconnectableConn.createStatement().executeQuery("SELECT CONNECTION_ID()");
        this.rs.next();
        assertTrue("Connection is not a reconnected-connection", !connectionId.equals(this.rs.getString(1)));

        try {
            reconnectableConn.createStatement().executeQuery("SELECT 1");
        } catch (SQLException sqlEx) {
            // ignore
        }

        this.rs = reconnectableConn.createStatement().executeQuery("SELECT 1");

        assertTrue(reconnectableConn.isReadOnly() == isReadOnly);
    }

    private Map<String, String> getCharacterSetsAndCollations() throws Exception {
        Map<String, String> charsetsToLoad = new HashMap<>();

        try {
            this.rs = this.stmt.executeQuery("SHOW character set");

            while (this.rs.next()) {
                charsetsToLoad.put(this.rs.getString("Charset"), this.rs.getString("Default collation"));
            }

            //
            // These don't have mappings in Java...
            //
            charsetsToLoad.remove("swe7");
            charsetsToLoad.remove("hp8");
            charsetsToLoad.remove("dec8");
            charsetsToLoad.remove("koi8u");
            charsetsToLoad.remove("keybcs2");
            charsetsToLoad.remove("geostd8");
            charsetsToLoad.remove("armscii8");
        } finally {
            if (this.rs != null) {
                this.rs.close();
            }
        }

        return charsetsToLoad;
    }

    /**
     * Tests fix for BUG#4334, port #'s not being picked up for
     * failover/autoreconnect.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testBug4334() throws Exception {
        if (isAdminConnectionConfigured()) {
            Connection adminConnection = null;

            try {
                adminConnection = getAdminConnection();

                int bogusPortNumber = 65534;

                HostInfo defaultHost = mainConnectionUrl.getMainHost();

                String host = defaultHost.getHost();
                int port = defaultHost.getPort();
                String database = defaultHost.getDatabase();
                String user = defaultHost.getUser();
                String password = defaultHost.getPassword();

                StringBuilder newUrlToTestPortNum = new StringBuilder("jdbc:mysql://");

                if (host != null) {
                    newUrlToTestPortNum.append(host);
                }

                newUrlToTestPortNum.append(":").append(port);
                newUrlToTestPortNum.append(",");

                if (host != null) {
                    newUrlToTestPortNum.append(host);
                }

                newUrlToTestPortNum.append(":").append(bogusPortNumber);
                newUrlToTestPortNum.append("/");

                if (database != null) {
                    newUrlToTestPortNum.append(database);
                }

                if ((user != null) || (password != null)) {
                    newUrlToTestPortNum.append("?");

                    if (user != null) {
                        newUrlToTestPortNum.append("user=").append(user);

                        if (password != null) {
                            newUrlToTestPortNum.append("&");
                        }
                    }

                    if (password != null) {
                        newUrlToTestPortNum.append("password=").append(password);
                    }
                }

                Properties autoReconnectProps = new Properties();
                autoReconnectProps.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");

                System.out.println(newUrlToTestPortNum);

                //
                // First test that port #'s are being correctly picked up
                //
                // We do this by looking at the error message that is returned
                //
                Connection portNumConn = DriverManager.getConnection(newUrlToTestPortNum.toString(), autoReconnectProps);
                Statement portNumStmt = portNumConn.createStatement();
                this.rs = portNumStmt.executeQuery("SELECT connection_id()");
                this.rs.next();

                killConnection(adminConnection, this.rs.getString(1));

                try {
                    portNumStmt.executeQuery("SELECT connection_id()");
                } catch (SQLException sqlEx) {
                    // we expect this one
                }

                try {
                    portNumStmt.executeQuery("SELECT connection_id()");
                } catch (SQLException sqlEx) {
                    assertTrue(sqlEx.getMessage().toLowerCase().indexOf("connection refused") != -1);
                }

                //
                // Now make sure failover works
                //
                StringBuilder newUrlToTestFailover = new StringBuilder("jdbc:mysql://");

                if (host != null) {
                    newUrlToTestFailover.append(host);
                }

                newUrlToTestFailover.append(":").append(port);
                newUrlToTestFailover.append(",");

                if (host != null) {
                    newUrlToTestFailover.append(host);
                }

                newUrlToTestFailover.append(":").append(bogusPortNumber);
                newUrlToTestFailover.append("/");

                if (database != null) {
                    newUrlToTestFailover.append(database);
                }

                if ((user != null) || (password != null)) {
                    newUrlToTestFailover.append("?");

                    if (user != null) {
                        newUrlToTestFailover.append("user=").append(user);

                        if (password != null) {
                            newUrlToTestFailover.append("&");
                        }
                    }

                    if (password != null) {
                        newUrlToTestFailover.append("password=").append(password);
                    }
                }

                Connection failoverConn = DriverManager.getConnection(newUrlToTestFailover.toString(), autoReconnectProps);
                Statement failoverStmt = failoverConn.createStatement();
                this.rs = failoverStmt.executeQuery("SELECT connection_id()");
                this.rs.next();

                killConnection(adminConnection, this.rs.getString(1));

                try {
                    failoverStmt.executeQuery("SELECT connection_id()");
                } catch (SQLException sqlEx) {
                    // we expect this one
                }

                this.rs = failoverStmt.executeQuery("SELECT connection_id()");
            } finally {
                if (adminConnection != null) {
                    adminConnection.close();
                }
            }
        }
    }

    private static void killConnection(Connection adminConn, String threadId) throws SQLException {
        adminConn.createStatement().execute("KILL " + threadId);
    }

    /**
     * Tests fix for BUG#6966, connections starting up failed-over (due to down
     * master) never retry master.
     * 
     * @throws Exception
     *             if the test fails...Note, test is timing-dependent, but
     *             should work in most cases.
     */
    public void testBug6966() throws Exception {
        Properties props = getPropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
        props.setProperty(PropertyKey.socketFactory.getKeyName(), "testsuite.UnreliableSocketFactory");

        String host = props.getProperty(PropertyKey.HOST.getKeyName());
        String port = props.getProperty(PropertyKey.PORT.getKeyName());

        props.remove(PropertyKey.HOST.getKeyName());

        props.setProperty(PropertyKey.queriesBeforeRetryMaster.getKeyName(), "50");
        props.setProperty(PropertyKey.maxReconnects.getKeyName(), "1");

        UnreliableSocketFactory.mapHost("master", host);
        UnreliableSocketFactory.mapHost("slave", host);
        UnreliableSocketFactory.downHost("master");

        Connection failoverConnection = null;

        try {
            failoverConnection = getConnectionWithProps("jdbc:mysql://master:" + port + ",slave:" + port + "/", props);
            failoverConnection.setAutoCommit(false);

            String originalConnectionId = getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT CONNECTION_ID()").toString();

            for (int i = 0; i < 50; i++) {
                failoverConnection.createStatement().execute("SELECT 1");
            }

            UnreliableSocketFactory.dontDownHost("master");

            failoverConnection.setAutoCommit(true);

            String newConnectionId = getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT CONNECTION_ID()").toString();

            assertEquals("/master", UnreliableSocketFactory.getHostFromLastConnection());
            assertFalse(newConnectionId.equals(originalConnectionId));

            failoverConnection.createStatement().execute("SELECT 1");
        } finally {
            UnreliableSocketFactory.flushAllStaticData();

            if (failoverConnection != null) {
                failoverConnection.close();
            }
        }
    }

    /**
     * Test fix for BUG#7952 -- Infinite recursion when 'falling back' to master
     * in failover configuration.
     * 
     * @throws Exception
     *             if the tests fails.
     */
    public void testBug7952() throws Exception {
        String host = getEncodedHostPortPairFromTestsuiteUrl() + "," + getEncodedHostPortPairFromTestsuiteUrl();
        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
        props.setProperty(PropertyKey.queriesBeforeRetryMaster.getKeyName(), "10");
        props.setProperty(PropertyKey.maxReconnects.getKeyName(), "1");

        Connection failoverConnection = null;
        Connection killerConnection = getConnectionWithProps((String) null);

        try {
            failoverConnection = getConnectionWithProps("jdbc:mysql://" + host + "/", props);
            failoverConnection.setAutoCommit(false);

            String failoverConnectionId = getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT CONNECTION_ID()").toString();

            System.out.println("Connection id: " + failoverConnectionId);

            killConnection(killerConnection, failoverConnectionId);

            Thread.sleep(3000); // This can take some time....

            try {
                failoverConnection.createStatement().executeQuery("SELECT 1");
            } catch (SQLException sqlEx) {
                assertTrue("08S01".equals(sqlEx.getSQLState()));
            }

            ((com.mysql.cj.jdbc.JdbcConnection) failoverConnection).setFailedOver(true);

            failoverConnection.setAutoCommit(true);

            String failedConnectionId = getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT CONNECTION_ID()").toString();
            System.out.println("Failed over connection id: " + failedConnectionId);

            ((com.mysql.cj.jdbc.JdbcConnection) failoverConnection).setFailedOver(true);

            for (int i = 0; i < 30; i++) {
                failoverConnection.setAutoCommit(true);
                System.out.println(getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT CONNECTION_ID()"));
                // failoverConnection.createStatement().executeQuery("SELECT
                // 1");
                failoverConnection.setAutoCommit(true);
            }

            String fallbackConnectionId = getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT CONNECTION_ID()").toString();
            System.out.println("fallback connection id: " + fallbackConnectionId);

            /*
             * long begin = System.currentTimeMillis();
             * 
             * failoverConnection.setAutoCommit(true);
             * 
             * long end = System.currentTimeMillis();
             * 
             * assertTrue("Probably didn't try failing back to the
             * master....check test", (end - begin) > 500);
             * 
             * failoverConnection.createStatement().executeQuery("SELECT 1");
             */
        } finally {
            if (failoverConnection != null) {
                failoverConnection.close();
            }
        }
    }

    /**
     * Tests fix for BUG#7607 - MS932, SHIFT_JIS and Windows_31J not recog. as
     * aliases for sjis.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug7607() throws Exception {
        Connection ms932Conn = null, cp943Conn = null, shiftJisConn = null, windows31JConn = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "MS932");

            ms932Conn = getConnectionWithProps(props);

            this.rs = ms932Conn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_set_client'");
            assertTrue(this.rs.next());
            String encoding = this.rs.getString(2);
            assertEquals("cp932", encoding.toLowerCase(Locale.ENGLISH));

            this.rs = ms932Conn.createStatement().executeQuery("SELECT 'abc'");
            assertTrue(this.rs.next());

            String charsetToCheck = "ms932";

            assertEquals(charsetToCheck,
                    ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(1).toLowerCase(Locale.ENGLISH));

            try {
                ms932Conn.createStatement().executeUpdate("drop table if exists testBug7607");
                ms932Conn.createStatement().executeUpdate("create table testBug7607 (sortCol int, col1 varchar(100) ) character set sjis");
                ms932Conn.createStatement().executeUpdate("insert into testBug7607 values(1, 0x835C)"); // standard
                // sjis
                ms932Conn.createStatement().executeUpdate("insert into testBug7607 values(2, 0x878A)"); // NEC
                // kanji

                this.rs = ms932Conn.createStatement().executeQuery("SELECT col1 FROM testBug7607 ORDER BY sortCol ASC");
                assertTrue(this.rs.next());
                String asString = this.rs.getString(1);
                assertTrue("\u30bd".equals(asString));

                assertTrue(this.rs.next());
                asString = this.rs.getString(1);
                assertEquals("\u3231", asString);
            } finally {
                ms932Conn.createStatement().executeUpdate("drop table if exists testBug7607");
            }

            props = new Properties();
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "SHIFT_JIS");

            shiftJisConn = getConnectionWithProps(props);

            this.rs = shiftJisConn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_set_client'");
            assertTrue(this.rs.next());
            encoding = this.rs.getString(2);
            assertTrue("sjis".equalsIgnoreCase(encoding));

            this.rs = shiftJisConn.createStatement().executeQuery("SELECT 'abc'");
            assertTrue(this.rs.next());

            String charSetUC = ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(1).toUpperCase(Locale.US);

            props = new Properties();
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "WINDOWS-31J");

            windows31JConn = getConnectionWithProps(props);

            this.rs = windows31JConn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_set_client'");
            assertTrue(this.rs.next());
            encoding = this.rs.getString(2);

            assertEquals("cp932", encoding.toLowerCase(Locale.ENGLISH));

            this.rs = windows31JConn.createStatement().executeQuery("SELECT 'abc'");
            assertTrue(this.rs.next());

            assertEquals("windows-31j".toLowerCase(Locale.ENGLISH),
                    ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(1).toLowerCase(Locale.ENGLISH));

            props = new Properties();
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "CP943");

            cp943Conn = getConnectionWithProps(props);

            this.rs = cp943Conn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_set_client'");
            assertTrue(this.rs.next());
            encoding = this.rs.getString(2);
            assertTrue("sjis".equalsIgnoreCase(encoding));

            this.rs = cp943Conn.createStatement().executeQuery("SELECT 'abc'");
            assertTrue(this.rs.next());

            charSetUC = ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(1).toUpperCase(Locale.US);

            assertEquals("CP943", charSetUC);

        } finally {
            if (ms932Conn != null) {
                ms932Conn.close();
            }

            if (shiftJisConn != null) {
                shiftJisConn.close();
            }

            if (windows31JConn != null) {
                windows31JConn.close();
            }

            if (cp943Conn != null) {
                cp943Conn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#9206, can not use 'UTF-8' for characterSetResults
     * configuration property.
     */
    public void testBug9206() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.characterSetResults.getKeyName(), "UTF-8");
        getConnectionWithProps(props).close();
    }

    /**
     * These two charsets have different names depending on version of MySQL
     * server.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testNewCharsetsConfiguration() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "EUC_KR");
        getConnectionWithProps(props).close();

        props = new Properties();
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "KOI8_R");
        getConnectionWithProps(props).close();
    }

    /**
     * Tests fix for BUG#10144 - Memory leak in ServerPreparedStatement if
     * serverPrepare() fails.
     */

    public void testBug10144() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.emulateUnsupportedPstmts.getKeyName(), "false");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");

        Connection bareConn = getConnectionWithProps(props);

        int currentOpenStatements = ((com.mysql.cj.jdbc.JdbcConnection) bareConn).getActiveStatementCount();

        try {
            bareConn.prepareStatement("Boo!");
            fail("Should not've been able to prepare that one!");
        } catch (SQLException sqlEx) {
            assertEquals(currentOpenStatements, ((com.mysql.cj.jdbc.JdbcConnection) bareConn).getActiveStatementCount());
        } finally {
            bareConn.close();
        }
    }

    /**
     * Tests fix for BUG#10496 - SQLException is thrown when using property
     * "characterSetResults"
     */
    public void testBug10496() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "WINDOWS-31J");
        props.setProperty(PropertyKey.characterSetResults.getKeyName(), "WINDOWS-31J");
        getConnectionWithProps(props).close();

        props = new Properties();
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "EUC_JP");
        props.setProperty(PropertyKey.characterSetResults.getKeyName(), "EUC_JP");
        getConnectionWithProps(props).close();
    }

    /**
     * Tests fix for BUG#11259, autoReconnect ping causes exception on
     * connection startup.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug11259() throws Exception {
        Connection dsConn = null;
        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
            dsConn = getConnectionWithProps(props);
        } finally {
            if (dsConn != null) {
                dsConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#11879 -- ReplicationConnection won't switch to slave,
     * throws "Catalog can't be null" exception.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug11879() throws Exception {
        if (runMultiHostTests()) {
            Connection replConn = null;

            try {
                replConn = getMasterSlaveReplicationConnection();
                replConn.setReadOnly(true);
                replConn.setReadOnly(false);
            } finally {
                if (replConn != null) {
                    replConn.close();
                }
            }
        }
    }

    /**
     * Tests fix for BUG#11976 - maxPerformance.properties mis-spells
     * "elideSetAutoCommits".
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug11976() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.useConfigs.getKeyName(), "maxPerformance");

        Connection maxPerfConn = getConnectionWithProps(props);
        assertEquals(true, ((JdbcConnection) maxPerfConn).getPropertySet().getBooleanProperty(PropertyKey.elideSetAutoCommits).getValue().booleanValue());
    }

    /**
     * Tests fix for BUG#12218, properties shared between master and slave with
     * replication connection.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug12218() throws Exception {
        if (runMultiHostTests()) {
            Connection replConn = null;

            HostInfo hostInfo = mainConnectionUrl.getMainHost();
            Properties props = new Properties();
            props.setProperty(PropertyKey.USER.getKeyName(), hostInfo.getUser());
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), hostInfo.getPassword());
            props = appendRequiredProperties(props);

            String replUrl = String.format("%1$s//address=(host=%2$s)(port=%3$d),address=(host=%2$s)(port=%3$d)(isSlave=true)/%4$s",
                    ConnectionUrl.Type.REPLICATION_CONNECTION.getScheme(), getEncodedHostFromTestsuiteUrl(), getPortFromTestsuiteUrl(), hostInfo.getDatabase());

            try {
                replConn = DriverManager.getConnection(replUrl, props);
                assertTrue(
                        !((ReplicationConnection) replConn).getMasterConnection().hasSameProperties(((ReplicationConnection) replConn).getSlavesConnection()));
            } finally {
                if (replConn != null) {
                    replConn.close();
                }
            }
        }
    }

    /**
     * Tests fix for BUG#12229 - explainSlowQueries hangs with server-side
     * prepared statements.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug12229() throws Exception {
        createTable("testBug12229", "(`int_field` integer )");
        this.stmt.executeUpdate("insert into testBug12229 values (123456),(1)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.profileSQL.getKeyName(), "true");
        props.setProperty(PropertyKey.slowQueryThresholdMillis.getKeyName(), "0");
        props.setProperty(PropertyKey.logSlowQueries.getKeyName(), "true");
        props.setProperty(PropertyKey.explainSlowQueries.getKeyName(), "true");
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");

        Connection explainConn = getConnectionWithProps(props);

        this.pstmt = explainConn.prepareStatement("SELECT `int_field` FROM `testBug12229` WHERE `int_field` = ?");
        this.pstmt.setInt(1, 1);

        this.rs = this.pstmt.executeQuery();
        assertTrue(this.rs.next());

        this.rs = this.pstmt.executeQuery();
        assertTrue(this.rs.next());

        this.rs = this.pstmt.executeQuery();
        assertTrue(this.rs.next());
    }

    /**
     * Tests fix for BUG#12752 - Cp1251 incorrectly mapped to win1251 for
     * servers newer than 4.0.x.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug12752() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "Cp1251");
        getConnectionWithProps(props).close();
    }

    /**
     * Tests fix for BUG#12753, sessionVariables=....=...., doesn't work as it's
     * tokenized incorrectly.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug12753() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sessionVariables.getKeyName(), "sql_mode=ansi");

        Connection sessionConn = null;

        try {
            sessionConn = getConnectionWithProps(props);

            String sqlMode = getMysqlVariable(sessionConn, "sql_mode");
            assertTrue(sqlMode.indexOf("ANSI") != -1);
        } finally {
            if (sessionConn != null) {
                sessionConn.close();
                sessionConn = null;
            }
        }
    }

    /**
     * Tests fix for BUG#13048 - maxQuerySizeToLog is not respected.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug13048() throws Exception {

        Connection profileConn = null;
        PrintStream oldErr = System.err;

        try {
            ByteArrayOutputStream bOut = new ByteArrayOutputStream();
            System.setErr(new PrintStream(bOut));

            Properties props = new Properties();
            props.setProperty(PropertyKey.profileSQL.getKeyName(), "true");
            props.setProperty(PropertyKey.maxQuerySizeToLog.getKeyName(), "2");
            props.setProperty(PropertyKey.logger.getKeyName(), StandardLogger.class.getName());

            profileConn = getConnectionWithProps(props);

            StringBuilder queryBuf = new StringBuilder("SELECT '");

            for (int i = 0; i < 500; i++) {
                queryBuf.append("a");
            }

            queryBuf.append("'");

            this.rs = profileConn.createStatement().executeQuery(queryBuf.toString());
            this.rs.close();

            String logString = new String(bOut.toString("ISO8859-1"));
            assertTrue(logString.indexOf("... (truncated)") != -1);

            bOut = new ByteArrayOutputStream();
            System.setErr(new PrintStream(bOut));

            this.rs = profileConn.prepareStatement(queryBuf.toString()).executeQuery();
            logString = new String(bOut.toString("ISO8859-1"));

            assertTrue(logString.indexOf("... (truncated)") != -1);
        } finally {
            System.setErr(oldErr);

            if (profileConn != null) {
                profileConn.close();
            }

            if (this.rs != null) {
                ResultSet toClose = this.rs;
                this.rs = null;
                toClose.close();
            }
        }
    }

    /**
     * Tests fix for BUG#13453 - can't use & or = in URL configuration values
     * (we now allow you to use www-form-encoding).
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug13453() throws Exception {
        StringBuilder urlBuf = new StringBuilder(dbUrl);

        if (dbUrl.indexOf('?') == -1) {
            urlBuf.append('?');
        } else {
            urlBuf.append('&');
        }
        // %25 := '%'; %26 := '&'; %3d := '=';
        urlBuf.append("sessionVariables=@testBug13453%3D'%25%26+%3D'");

        Connection encodedConn = null;

        try {
            encodedConn = DriverManager.getConnection(urlBuf.toString(), null);

            this.rs = encodedConn.createStatement().executeQuery("SELECT @testBug13453");
            assertTrue(this.rs.next());
            assertEquals("%& =", this.rs.getString(1));
        } finally {
            if (this.rs != null) {
                this.rs.close();
                this.rs = null;
            }

            if (encodedConn != null) {
                encodedConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#15065 - Usage advisor complains about unreferenced
     * columns, even though they've been referenced.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug15065() throws Exception {
        createTable("testBug15065", "(field1 int)");

        this.stmt.executeUpdate("INSERT INTO testBug15065 VALUES (1)");

        Connection advisorConn = null;
        Statement advisorStmt = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.useUsageAdvisor.getKeyName(), "true");
            props.setProperty(PropertyKey.logger.getKeyName(), StandardLogger.class.getName());

            advisorConn = getConnectionWithProps(props);
            advisorStmt = advisorConn.createStatement();

            Method[] getMethods = ResultSet.class.getMethods();

            PrintStream oldErr = System.err;

            try {
                ByteArrayOutputStream bOut = new ByteArrayOutputStream();
                System.setErr(new PrintStream(bOut));

                HashMap<String, String> methodsToSkipMap = new HashMap<>();

                // Needs an actual URL
                methodsToSkipMap.put("getURL", null);

                // Java6 JDBC4.0 methods we don't implement
                methodsToSkipMap.put("getNCharacterStream", null);
                methodsToSkipMap.put("getNClob", null);
                methodsToSkipMap.put("getNString", null);
                methodsToSkipMap.put("getRowId", null);
                methodsToSkipMap.put("getSQLXML", null);

                // int doesn't convert to these types
                methodsToSkipMap.put("getDate", null);
                methodsToSkipMap.put("getTime", null);
                methodsToSkipMap.put("getTimestamp", null);
                methodsToSkipMap.put("getCharacterStream", null);
                methodsToSkipMap.put("getUnicodeStream", null);
                methodsToSkipMap.put("getAsciiStream", null);
                methodsToSkipMap.put("getBinaryStream", null);

                for (int j = 0; j < 2; j++) {
                    for (int i = 0; i < getMethods.length; i++) {
                        String methodName = getMethods[i].getName();

                        if (methodName.startsWith("get") && !methodsToSkipMap.containsKey(methodName)) {
                            Class<?>[] parameterTypes = getMethods[i].getParameterTypes();

                            if (parameterTypes.length == 1 && parameterTypes[0] == Integer.TYPE) {
                                if (j == 0) {
                                    this.rs = advisorStmt.executeQuery("SELECT COUNT(*) FROM testBug15065");
                                } else {
                                    this.rs = advisorConn.prepareStatement("SELECT COUNT(*) FROM testBug15065").executeQuery();
                                }

                                this.rs.next();

                                try {

                                    getMethods[i].invoke(this.rs, new Object[] { new Integer(1) });
                                } catch (InvocationTargetException invokeEx) {
                                    // we don't care about bad values, just that the column gets "touched"
                                    if (!invokeEx.getCause().getClass().isAssignableFrom(java.sql.SQLException.class)
                                            && !invokeEx.getCause().getClass().getName().equals("com.mysql.jdbc.NotImplemented")
                                            && !invokeEx.getCause().getClass().getName().equals("java.sql.SQLFeatureNotSupportedException")) {
                                        throw invokeEx;
                                    }
                                }

                                this.rs.close();
                                this.rs = null;
                            }
                        }
                    }
                }

                String logOut = bOut.toString("ISO8859-1");

                if (logOut.indexOf(".Level") != -1) {
                    return; // we ignore for warnings
                }

                assertTrue("Usage advisor complained about columns:\n\n" + logOut, logOut.indexOf("columns") == -1);
            } finally {
                System.setErr(oldErr);
            }
        } finally {
            if (advisorConn != null) {
                advisorConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#15544, no "dos" character set in MySQL > 4.1.0
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug15544() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "Cp437");
        Connection dosConn = null;

        try {
            dosConn = getConnectionWithProps(props);
        } finally {
            if (dosConn != null) {
                dosConn.close();
            }
        }
    }

    public void testCSC5765() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "utf8");
        props.setProperty(PropertyKey.characterSetResults.getKeyName(), "utf8");
        props.setProperty(PropertyKey.connectionCollation.getKeyName(), "utf8_bin");

        Connection utf8Conn = null;

        try {
            utf8Conn = getConnectionWithProps(props);
            this.rs = utf8Conn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_%'");
            while (this.rs.next()) {
                System.out.println(this.rs.getString(1) + " = " + this.rs.getString(2));
            }

            this.rs = utf8Conn.createStatement().executeQuery("SHOW VARIABLES LIKE 'collation_%'");
            while (this.rs.next()) {
                System.out.println(this.rs.getString(1) + " = " + this.rs.getString(2));
            }
        } finally {
            if (utf8Conn != null) {
                utf8Conn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#15570 - ReplicationConnection incorrectly copies state,
     * doesn't transfer connection context correctly when transitioning between
     * the same read-only states.
     * 
     * (note, this test will fail if the test user doesn't have permission to
     * "USE 'mysql'".
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug15570() throws Exception {
        Connection replConn = null;

        try {
            replConn = getMasterSlaveReplicationConnection();
            boolean dbMapsToSchema = ((JdbcConnection) replConn).getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                    .getValue() == DatabaseTerm.SCHEMA;

            int masterConnectionId = Integer.parseInt(getSingleIndexedValueWithQuery(replConn, 1, "SELECT CONNECTION_ID()").toString());

            replConn.setReadOnly(false);

            assertEquals(masterConnectionId, Integer.parseInt(getSingleIndexedValueWithQuery(replConn, 1, "SELECT CONNECTION_ID()").toString()));

            String currentDb = dbMapsToSchema ? replConn.getSchema() : replConn.getCatalog();

            if (dbMapsToSchema) {
                replConn.setSchema(currentDb);
                assertEquals(currentDb, replConn.getSchema());
            } else {
                replConn.setCatalog(currentDb);
                assertEquals(currentDb, replConn.getCatalog());
            }

            replConn.setReadOnly(true);

            int slaveConnectionId = Integer.parseInt(getSingleIndexedValueWithQuery(replConn, 1, "SELECT CONNECTION_ID()").toString());

            // The following test is okay for now, as the chance of MySQL wrapping the connection id counter during our testsuite is very small.
            // As per Bug#21286268 fix a Replication connection first initializes the Slaves sub-connection, then the Masters.
            assertTrue("Master id " + masterConnectionId + " is not newer than slave id " + slaveConnectionId, masterConnectionId > slaveConnectionId);

            assertEquals(currentDb, dbMapsToSchema ? replConn.getSchema() : replConn.getCatalog());

            String newDb = "mysql";

            if (dbMapsToSchema) {
                replConn.setSchema(newDb);
                assertEquals(newDb, replConn.getSchema());
            } else {
                replConn.setCatalog(newDb);
                assertEquals(newDb, replConn.getCatalog());
            }

            replConn.setReadOnly(true);
            assertEquals(newDb, dbMapsToSchema ? replConn.getSchema() : replConn.getCatalog());

            replConn.setReadOnly(false);
            assertEquals(masterConnectionId, Integer.parseInt(getSingleIndexedValueWithQuery(replConn, 1, "SELECT CONNECTION_ID()").toString()));
        } finally {
            if (replConn != null) {
                replConn.close();
            }
        }
    }

    /**
     * Tests bug where downed slave caused round robin load balance not to cycle
     * back to first host in the list.
     * 
     * @throws Exception
     *             if the test fails...Note, test is timing-dependent, but
     *             should work in most cases.
     */
    public void testBug23281() throws Exception {
        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "false");
        props.setProperty(PropertyKey.failOverReadOnly.getKeyName(), "false");
        props.setProperty(PropertyKey.connectTimeout.getKeyName(), "5000");

        String host = getEncodedHostPortPairFromTestsuiteUrl();

        removeHostRelatedProps(props);

        StringBuilder newHostBuf = new StringBuilder();

        newHostBuf.append(host);

        newHostBuf.append(",");
        // newHostBuf.append(host);
        newHostBuf.append("192.0.2.1"); // non-exsitent machine from RFC3330 test network
        newHostBuf.append(":65532"); // make sure the slave fails

        Connection failoverConnection = null;

        try {
            failoverConnection = getConnectionWithProps("jdbc:mysql://" + newHostBuf.toString() + "/", props);

            String originalConnectionId = getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT CONNECTION_ID()").toString();

            System.out.println(originalConnectionId);

            Connection nextConnection = getConnectionWithProps("jdbc:mysql://" + newHostBuf.toString() + "/", props);

            String nextId = getSingleIndexedValueWithQuery(nextConnection, 1, "SELECT CONNECTION_ID()").toString();

            System.out.println(nextId);

        } finally {
            if (failoverConnection != null) {
                failoverConnection.close();
            }
        }
    }

    /**
     * Tests to insure proper behavior for BUG#24706.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug24706() throws Exception {
        // 'elideSetAutoCommits' feature was turned off due to Server Bug#66884. See also ConnectionPropertiesImpl#getElideSetAutoCommits().
        // TODO Turn this test back on as soon as the server bug is fixed. Consider making it version specific.
        boolean ignoreTest = true;
        if (ignoreTest) {
            return;
        }

        Properties props = new Properties();
        props.setProperty(PropertyKey.elideSetAutoCommits.getKeyName(), "true");
        props.setProperty(PropertyKey.logger.getKeyName(), BufferingLogger.class.getName());
        props.setProperty(PropertyKey.profileSQL.getKeyName(), "true");
        Connection c = null;

        BufferingLogger.startLoggingToBuffer();

        try {
            c = getConnectionWithProps(props);
            c.setAutoCommit(true);
            c.createStatement().execute("SELECT 1");
            c.setAutoCommit(true);
            c.setAutoCommit(false);
            c.createStatement().execute("SELECT 1");
            c.setAutoCommit(false);

            // We should only see _one_ "set autocommit=" sent to the server

            String log = BufferingLogger.getBuffer().toString();
            int searchFrom = 0;
            int count = 0;
            int found = 0;

            while ((found = log.indexOf("SET autocommit=", searchFrom)) != -1) {
                searchFrom = found + 1;
                count++;
            }

            // The SELECT doesn't actually start a transaction, so being pedantic the driver issues SET autocommit=0 again in this case.
            assertEquals(2, count);
        } finally {
            BufferingLogger.dropBuffer();

            if (c != null) {
                c.close();
            }
        }
    }

    /**
     * Tests fix for BUG#25514 - Timer instance used for
     * Statement.setQueryTimeout() created per-connection, rather than per-VM,
     * causing memory leak.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug25514() throws Exception {

        for (int i = 0; i < 10; i++) {
            getConnectionWithProps((Properties) null).close();
        }

        ThreadGroup root = Thread.currentThread().getThreadGroup().getParent();

        while (root.getParent() != null) {
            root = root.getParent();
        }

        int numThreadsNamedTimer = findNamedThreadCount(root, "Timer");

        if (numThreadsNamedTimer == 0) {
            numThreadsNamedTimer = findNamedThreadCount(root, "MySQL Statement Cancellation Timer");
        }

        // Notice that this seems impossible to test on JDKs prior to 1.5, as there is no reliable way to find the TimerThread, so we have to rely on new JDKs
        // for this test.
        assertTrue("More than one timer for cancel was created", numThreadsNamedTimer <= 1);
    }

    private int findNamedThreadCount(ThreadGroup group, String nameStart) {

        int count = 0;

        int numThreads = group.activeCount();
        Thread[] threads = new Thread[numThreads * 2];
        numThreads = group.enumerate(threads, false);

        for (int i = 0; i < numThreads; i++) {
            if (threads[i].getName().startsWith(nameStart)) {
                count++;
            }
        }

        int numGroups = group.activeGroupCount();
        ThreadGroup[] groups = new ThreadGroup[numGroups * 2];
        numGroups = group.enumerate(groups, false);

        for (int i = 0; i < numGroups; i++) {
            count += findNamedThreadCount(groups[i], nameStart);
        }

        return count;
    }

    /**
     * Ensures that we don't miss getters/setters for driver properties defined in
     * PropertyDefinitions so that names given in documentation work with
     * DataSources which will use JavaBean-style names and reflection to set the
     * values (and often fail silently! when the method isn't available).
     * 
     * @throws Exception
     */
    public void testBug23626() throws Exception {

        List<String> propertyNames = new ArrayList<>();
        // Standard DataSource methods
        propertyNames.add("databaseName");
        propertyNames.add("description");
        propertyNames.add("password");
        propertyNames.add("portNumber");
        propertyNames.add("serverName");
        propertyNames.add("user");
        // propertyNames.add("dataSourceName"); // TODO not supported
        // propertyNames.add("networkProtocol"); // TODO not supported
        // propertyNames.add("roleName"); // TODO not supported

        DriverPropertyInfo[] dpi = new NonRegisteringDriver().getPropertyInfo(dbUrl, null);
        for (int i = 0; i < dpi.length; i++) {
            String propertyName = dpi[i].name;
            PropertyKey key = PropertyKey.fromValue(propertyName);
            if (key != null) {
                switch (key) {
                    case USER:
                    case PASSWORD:
                    case DBNAME:
                    case PORT:
                    case HOST:
                        continue;

                    default:
                        if (key.getCcAlias() != null) {
                            propertyName = key.getCcAlias();
                        }
                        break;
                }
            }
            propertyNames.add(propertyName);
        }

        testBug23626ForClass(MysqlDataSource.class, propertyNames);
        testBug23626ForClass(MysqlXADataSource.class, propertyNames);

        // TODO Standard Connection Pool Properties are not supported
        //        maxStatements   int     The total number of statements that the pool should keep open. 0 (zero) indicates that caching of statements is disabled.
        //        initialPoolSize int     The number of physical connections the pool should contain when it is created
        //        minPoolSize     int     The number of physical connections the pool should keep available at all times. 0 (zero) indicates that connections should be created as needed.
        //        maxPoolSize     int     The maximum number of physical connections that the pool should contain. 0 (zero) indicates no maximum size.
        //        maxIdleTime     int     The number of seconds that a physical connection should remain unused in the pool before the connection is closed. 0 (zero) indicates no limit.
        //        propertyCycle   int     The interval, in seconds, that the pool should wait before enforcing the current policy defined by the values of the above connection pool properties
        testBug23626ForClass(MysqlConnectionPoolDataSource.class, propertyNames);
    }

    private void testBug23626ForClass(Class<?> clazz, List<String> propertyNames) throws Exception {
        StringBuilder missingSettersBuf = new StringBuilder();
        StringBuilder missingGettersBuf = new StringBuilder();

        Class<?>[][] argTypes = { new Class<?>[] { String.class }, new Class<?>[] { Integer.TYPE }, new Class<?>[] { Long.TYPE },
                new Class<?>[] { Boolean.TYPE } };

        for (String propertyName : propertyNames) {
            StringBuilder mutatorName = new StringBuilder("set");
            mutatorName.append(Character.toUpperCase(propertyName.charAt(0)));
            mutatorName.append(propertyName.substring(1));

            StringBuilder accessorName = new StringBuilder("get");
            accessorName.append(Character.toUpperCase(propertyName.charAt(0)));
            accessorName.append(propertyName.substring(1));

            try {
                clazz.getMethod(accessorName.toString(), (Class[]) null);
            } catch (NoSuchMethodException nsme) {
                missingGettersBuf.append(accessorName.toString());
                missingGettersBuf.append("\n");
            }

            boolean foundMethod = false;

            for (int j = 0; j < argTypes.length; j++) {
                try {
                    clazz.getMethod(mutatorName.toString(), argTypes[j]);
                    foundMethod = true;
                    break;
                } catch (NoSuchMethodException nsme) {

                }
            }

            if (!foundMethod) {
                missingSettersBuf.append(mutatorName);
                missingSettersBuf.append("\n");
            }
        }

        assertEquals("Missing setters for listed configuration properties.", "", missingSettersBuf.toString());
        assertEquals("Missing getters for listed configuration properties.", "", missingSettersBuf.toString());

    }

    /**
     * Tests fix for BUG#25545 - Client flags not sent correctly during
     * handshake when using SSL.
     * 
     * Requires test certificates from testsuite/ssl-test-certs to be installed
     * on the server being tested.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug25545() throws Exception {
        createProcedure("testBug25545", "() BEGIN SELECT 1; END");

        String trustStorePath = "src/test/config/ssl-test-certs/ca-truststore";

        System.setProperty("javax.net.ssl.keyStore", trustStorePath);
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStore", trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        Connection sslConn = null;

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
            props.setProperty(PropertyKey.requireSSL.getKeyName(), "true");

            sslConn = getConnectionWithProps(props);
            sslConn.prepareCall("{ call testBug25545()}").execute();
        } finally {
            if (sslConn != null) {
                sslConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#36948 - Trying to use trustCertificateKeyStoreUrl
     * causes an IllegalStateException.
     * 
     * Requires test certificates from testsuite/ssl-test-certs to be installed
     * on the server being tested.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug36948() throws Exception {

        Connection _conn = null;

        try {
            String hostSpec = getEncodedHostPortPairFromTestsuiteUrl();
            Properties props = getHostFreePropertiesFromTestsuiteUrl();
            String db = props.getProperty(PropertyKey.DBNAME.getKeyName(), "test");
            props.remove(PropertyKey.useSSL.getKeyName());
            props.remove(PropertyKey.requireSSL.getKeyName());
            props.remove(PropertyKey.verifyServerCertificate.getKeyName());
            props.remove(PropertyKey.trustCertificateKeyStoreUrl.getKeyName());
            props.remove(PropertyKey.trustCertificateKeyStoreType.getKeyName());
            props.remove(PropertyKey.trustCertificateKeyStorePassword.getKeyName());

            final String url = "jdbc:mysql://" + hostSpec + "/" + db + "?useSSL=true&requireSSL=true&verifyServerCertificate=true"
                    + "&trustCertificateKeyStoreUrl=file:src/test/config/ssl-test-certs/ca-truststore&trustCertificateKeyStoreType=JKS"
                    + "&trustCertificateKeyStorePassword=password";

            _conn = DriverManager.getConnection(url, props);
        } finally {
            if (_conn != null) {
                _conn.close();
            }
        }

    }

    /**
     * Tests fix for BUG#27655 - getTransactionIsolation() uses
     * "SHOW VARIABLES LIKE" which is very inefficient on MySQL-5.0+
     * 
     * @throws Exception
     */
    public void testBug27655() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.profileSQL.getKeyName(), "true");
        props.setProperty(PropertyKey.logger.getKeyName(), BufferingLogger.class.getName());
        BufferingLogger.startLoggingToBuffer();

        Connection loggedConn = null;

        try {
            loggedConn = getConnectionWithProps(props);
            loggedConn.getTransactionIsolation();

            String s = versionMeetsMinimum(8, 0, 3) ? "transaction_isolation" : "tx_isolation";
            assertEquals(-1, BufferingLogger.getBuffer().toString().indexOf("SHOW VARIABLES LIKE '" + s + "'"));
        } finally {
            BufferingLogger.dropBuffer();
            if (loggedConn != null) {
                loggedConn.close();
            }
        }
    }

    /**
     * Tests fix for issue where a failed-over connection would let an
     * application call setReadOnly(false), when that call should be ignored
     * until the connection is reconnected to a writable master.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testFailoverReadOnly() throws Exception {
        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
        props.setProperty(PropertyKey.queriesBeforeRetryMaster.getKeyName(), "0");
        props.setProperty(PropertyKey.secondsBeforeRetryMaster.getKeyName(), "0"); // +^ enable fall back to primary as soon as possible

        Connection failoverConn = null;

        Statement failoverStmt = null;

        try {
            failoverConn = getConnectionWithProps(getMasterSlaveUrl(), props);

            failoverStmt = failoverConn.createStatement();

            String masterConnectionId = getSingleIndexedValueWithQuery(failoverConn, 1, "SELECT connection_id()").toString();

            this.stmt.execute("KILL " + masterConnectionId);

            // die trying, so we get the next host
            for (int i = 0; i < 100; i++) {
                try {
                    failoverStmt.executeQuery("SELECT 1");
                } catch (SQLException sqlEx) {
                    break;
                }
            }

            String slaveConnectionId = getSingleIndexedValueWithQuery(failoverConn, 1, "SELECT connection_id()").toString();

            assertTrue("Didn't get a new physical connection", !masterConnectionId.equals(slaveConnectionId));

            failoverConn.setReadOnly(false); // this should be ignored

            assertTrue(failoverConn.isReadOnly());

            this.stmt.execute("KILL " + slaveConnectionId); // we can't issue this on our own connection :p

            // die trying, so we get the next host
            for (int i = 0; i < 100; i++) {
                try {
                    failoverStmt.executeQuery("SELECT 1");
                } catch (SQLException sqlEx) {
                    break;
                }
            }

            String newMasterId = getSingleIndexedValueWithQuery(failoverConn, 1, "SELECT connection_id()").toString();

            assertTrue("Didn't get a new physical connection", !slaveConnectionId.equals(newMasterId));

            failoverConn.setReadOnly(false);

            assertFalse(failoverConn.isReadOnly());
        } finally {
            if (failoverStmt != null) {
                failoverStmt.close();
            }

            if (failoverConn != null) {
                failoverConn.close();
            }
        }
    }

    public void testPropertiesDescriptionsKeys() throws Exception {
        DriverPropertyInfo[] dpi = new NonRegisteringDriver().getPropertyInfo(dbUrl, null);

        for (int i = 0; i < dpi.length; i++) {
            String description = dpi[i].description;
            String propertyName = dpi[i].name;

            if (description.indexOf("Missing error message for key '") != -1 || description.startsWith("!")) {
                fail("Missing message for configuration property " + propertyName);
            }

            if (description.length() < 10) {
                fail("Suspiciously short description for configuration property " + propertyName);
            }
        }
    }

    public void testBug29106() throws Exception {
        ClassLoader cl = Thread.currentThread().getContextClassLoader();
        Class<?> checkerClass = cl.loadClass("com.mysql.cj.jdbc.integration.jboss.MysqlValidConnectionChecker");
        ((MysqlValidConnectionChecker) checkerClass.newInstance()).isValidConnection(this.conn);
    }

    public void testBug29852() throws Exception {
        Connection lbConn = getLoadBalancedConnection();
        assertTrue(!lbConn.getClass().getName().startsWith("com.mysql.cj.jdbc"));
        lbConn.close();
    }

    /**
     * Test of a new feature to fix BUG 22643, specifying a "validation query"
     * in your connection pool that starts with "slash-star ping slash-star"
     * _exactly_ will cause the driver to " + instead send a ping to the server
     * (much lighter weight), and when using a ReplicationConnection or a
     * LoadBalancedConnection, will send the ping across all active connections.
     * 
     * @throws Exception
     */
    public void testBug22643() throws Exception {
        checkPingQuery(this.conn);

        Connection replConnection = getMasterSlaveReplicationConnection();

        try {
            checkPingQuery(replConnection);
        } finally {
            if (replConnection != null) {
                replConnection.close();
            }
        }

        Connection lbConn = getLoadBalancedConnection();

        try {
            checkPingQuery(lbConn);
        } finally {
            if (lbConn != null) {
                lbConn.close();
            }
        }
    }

    private void checkPingQuery(Connection c) throws SQLException {
        // Yes, I know we're sending 2, and looking for 1 that's part of the test, since we don't _really_ send the query to the server!
        String aPingQuery = "/* ping */ SELECT 2";
        Statement pingStmt = c.createStatement();
        PreparedStatement pingPStmt = null;

        this.rs = pingStmt.executeQuery(aPingQuery);
        assertTrue(this.rs.next());
        assertEquals(this.rs.getInt(1), 1);

        assertTrue(pingStmt.execute(aPingQuery));
        this.rs = pingStmt.getResultSet();
        assertTrue(this.rs.next());
        assertEquals(this.rs.getInt(1), 1);

        pingPStmt = c.prepareStatement(aPingQuery);

        assertTrue(pingPStmt.execute());
        this.rs = pingPStmt.getResultSet();
        assertTrue(this.rs.next());
        assertEquals(this.rs.getInt(1), 1);

        this.rs = pingPStmt.executeQuery();
        assertTrue(this.rs.next());
        assertEquals(this.rs.getInt(1), 1);

    }

    public void testBug31053() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.connectTimeout.getKeyName(), "2000");
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), "random");

        Connection lbConn = getLoadBalancedConnection(2, "localhost:23", props);

        lbConn.setAutoCommit(false);

        for (int i = 0; i < 10; i++) {
            lbConn.commit();
        }
    }

    public void testBug32877() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.connectTimeout.getKeyName(), "2000");
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), "bestResponseTime");

        Connection lbConn = getLoadBalancedConnection(1, "localhost:23", props);

        lbConn.setAutoCommit(false);

        long begin = System.currentTimeMillis();

        for (int i = 0; i < 4; i++) {
            lbConn.commit();
        }

        assertTrue(System.currentTimeMillis() - begin < 10000);
    }

    /**
     * Tests fix for BUG#33734 - NullPointerException when using client-side
     * prepared statements and enabling caching of prepared statements (only
     * present in nightly builds of 5.1).
     * 
     * @throws Exception
     */
    public void testBug33734() throws Exception {
        Connection testConn = getConnectionWithProps("cachePrepStmts=true,useServerPrepStmts=false");
        try {
            testConn.prepareStatement("SELECT 1");
        } finally {
            testConn.close();
        }
    }

    /** 34703 [NEW]: isValild() aborts Connection on timeout */

    public void testBug34703() throws Exception {

        Method isValid = java.sql.Connection.class.getMethod("isValid", new Class<?>[] { Integer.TYPE });

        Connection newConn = getConnectionWithProps((Properties) null);
        isValid.invoke(newConn, new Object[] { new Integer(1) });
        Thread.sleep(2000);
        assertTrue(((Boolean) isValid.invoke(newConn, new Object[] { new Integer(0) })).booleanValue());
    }

    public void testBug34937() throws Exception {
        com.mysql.cj.jdbc.MysqlConnectionPoolDataSource ds = new com.mysql.cj.jdbc.MysqlConnectionPoolDataSource();
        StringBuilder urlBuf = new StringBuilder();
        urlBuf.append(getMasterSlaveUrl());
        urlBuf.append("?");
        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        String key = null;

        Enumeration<Object> keyEnum = props.keys();

        while (keyEnum.hasMoreElements()) {
            key = (String) keyEnum.nextElement();
            urlBuf.append(key);
            urlBuf.append("=");
            urlBuf.append(props.get(key));
            urlBuf.append("&");
        }

        String url = urlBuf.toString();
        url = "jdbc:mysql:replication:" + url.substring(url.indexOf("jdbc:mysql:") + "jdbc:mysql:".length());
        ds.setURL(url);
        Connection replConn = ds.getPooledConnection().getConnection();

        boolean readOnly = false;

        for (int i = 0; i < 10; i++) {
            this.rs = replConn.createStatement().executeQuery("SELECT 1");
            assertTrue(this.rs.next());
            this.rs = replConn.prepareStatement("SELECT 1").executeQuery();
            assertTrue(this.rs.next());
            readOnly = !readOnly;
            replConn.setReadOnly(readOnly);
        }
    }

    public void testBug35660() throws Exception {

        Connection lbConn = getLoadBalancedConnection(null);
        Connection lbConn2 = getLoadBalancedConnection(null);

        try {
            assertEquals(this.conn, this.conn);
            assertEquals(lbConn, lbConn);
            assertFalse(lbConn.equals(this.conn));
            assertFalse(lbConn.equals(lbConn2));
        } finally {
            lbConn.close();
            lbConn2.close();
        }
    }

    public void testBug37570() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "utf-8");
        props.setProperty(PropertyKey.passwordCharacterEncoding.getKeyName(), "utf-8");

        // TODO enable for usual connection?
        Connection adminConn = getAdminConnectionWithProps(props);

        if (adminConn != null) {

            String unicodePassword = "\u0430\u0431\u0432"; // Cyrillic string
            String user = "bug37570";
            Statement adminStmt = adminConn.createStatement();

            adminStmt.executeUpdate("create user '" + user + "'@'127.0.0.1' identified by 'foo'");
            adminStmt.executeUpdate("grant usage on *.* to '" + user + "'@'127.0.0.1'");
            adminStmt.executeUpdate("update mysql.user set password=PASSWORD('" + unicodePassword + "') where user = '" + user + "'");
            adminStmt.executeUpdate("flush privileges");

            try {
                ((JdbcConnection) adminConn).changeUser(user, unicodePassword);
            } catch (SQLException sqle) {
                assertTrue("Connection with non-latin1 password failed", false);
            }

        }
    }

    public void testUnreliableSocketFactory() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), "bestResponseTime");
        Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);
        assertNotNull("Connection should not be null", this.conn);

        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        // both connections are live now
        UnreliableSocketFactory.downHost("first");
        UnreliableSocketFactory.downHost("second");
        try {
            conn2.createStatement().execute("SELECT 1");
            fail("Should hang here.");
        } catch (SQLException sqlEx) {
            assertEquals("08S01", sqlEx.getSQLState());
        }
    }

    public void testReplicationConnectionGroupHostManagement() throws Exception {
        String replicationGroup1 = "rg1";

        Properties props = new Properties();
        props.setProperty(PropertyKey.replicationConnectionGroup.getKeyName(), replicationGroup1);
        props.setProperty(PropertyKey.retriesAllDown.getKeyName(), "3");
        props.setProperty(PropertyKey.loadBalanceHostRemovalGracePeriod.getKeyName(), "1");
        ReplicationConnection conn2 = this.getUnreliableReplicationConnection(new String[] { "first", "second", "third" }, props);
        assertNotNull("Connection should not be null", this.conn);
        conn2.setAutoCommit(false);
        String port = getPort(props);
        String firstHost = "first:" + port;
        String secondHost = "second:" + port;
        String thirdHost = "third:" + port;

        // "first" should be master, "second" and "third" should be slaves.
        assertEquals(1, ReplicationConnectionGroupManager.getConnectionCountWithHostAsMaster(replicationGroup1, firstHost));
        assertEquals(0, ReplicationConnectionGroupManager.getConnectionCountWithHostAsSlave(replicationGroup1, firstHost));

        // remove "third" from slave pool:
        conn2.removeSlave(thirdHost);

        assertEquals(0, ReplicationConnectionGroupManager.getConnectionCountWithHostAsMaster(replicationGroup1, thirdHost));
        assertEquals(0, ReplicationConnectionGroupManager.getConnectionCountWithHostAsSlave(replicationGroup1, thirdHost));

        // add "third" back into slave pool:
        conn2.addSlaveHost(thirdHost);

        assertEquals(0, ReplicationConnectionGroupManager.getConnectionCountWithHostAsMaster(replicationGroup1, thirdHost));
        assertEquals(1, ReplicationConnectionGroupManager.getConnectionCountWithHostAsSlave(replicationGroup1, thirdHost));

        conn2.setReadOnly(false);

        assertEquals(0, ReplicationConnectionGroupManager.getNumberOfMasterPromotion(replicationGroup1));

        // failover to "second" as master
        ReplicationConnectionGroupManager.promoteSlaveToMaster(replicationGroup1, secondHost);
        assertEquals(1, ReplicationConnectionGroupManager.getNumberOfMasterPromotion(replicationGroup1));

        // "first" is still a master:
        assertEquals(1, ReplicationConnectionGroupManager.getConnectionCountWithHostAsMaster(replicationGroup1, firstHost));
        assertEquals(0, ReplicationConnectionGroupManager.getConnectionCountWithHostAsSlave(replicationGroup1, firstHost));
        assertEquals(1, ReplicationConnectionGroupManager.getConnectionCountWithHostAsMaster(replicationGroup1, secondHost));
        assertEquals(0, ReplicationConnectionGroupManager.getConnectionCountWithHostAsSlave(replicationGroup1, secondHost));

        ReplicationConnectionGroupManager.removeMasterHost(replicationGroup1, firstHost);

        conn2.createStatement().execute("SELECT 1");
        assertFalse(conn2.isClosed());

        conn2.commit();

        // validate that queries are successful:
        conn2.createStatement().execute("SELECT 1");
        assertTrue(conn2.isHostMaster(secondHost));

        // master is now offline
        UnreliableSocketFactory.downHost("second");
        try {
            Statement lstmt = conn2.createStatement();
            lstmt.execute("SELECT 1");
            fail("Should fail here due to closed connection");
        } catch (SQLException sqlEx) {
            assertEquals("08S01", sqlEx.getSQLState());
        }

    }

    public void testReplicationConnectionHostManagement() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.retriesAllDown.getKeyName(), "3");
        props.setProperty(PropertyKey.loadBalanceHostRemovalGracePeriod.getKeyName(), "1");

        ReplicationConnection conn2 = this.getUnreliableReplicationConnection(new String[] { "first", "second", "third" }, props);
        conn2.setAutoCommit(false);
        String port = getPort(props);
        String firstHost = "first:" + port;
        String secondHost = "second:" + port;
        String thirdHost = "third:" + port;

        // "first" should be master, "second" and "third" should be slaves.
        assertTrue(conn2.isHostMaster(firstHost));
        assertTrue(conn2.isHostSlave(secondHost));
        assertTrue(conn2.isHostSlave(thirdHost));
        assertFalse(conn2.isHostSlave(firstHost));
        assertFalse(conn2.isHostMaster(secondHost));
        assertFalse(conn2.isHostMaster(thirdHost));

        // remove "third" from slave pool:
        conn2.removeSlave(thirdHost);
        assertFalse(conn2.isHostSlave(thirdHost));
        assertFalse(conn2.isHostMaster(thirdHost));

        // add "third" back into slave pool:
        conn2.addSlaveHost(thirdHost);
        assertTrue(conn2.isHostSlave(thirdHost));
        assertFalse(conn2.isHostMaster(thirdHost));
        conn2.setReadOnly(false);

        // failover to "second" as master, "first"
        // can still be used:
        conn2.promoteSlaveToMaster(secondHost);
        assertTrue(conn2.isHostMaster(firstHost));
        assertFalse(conn2.isHostSlave(firstHost));
        assertFalse(conn2.isHostSlave(secondHost));
        assertTrue(conn2.isHostMaster(secondHost));
        assertTrue(conn2.isHostSlave(thirdHost));
        assertFalse(conn2.isHostMaster(thirdHost));

        conn2.removeMasterHost(firstHost);

        // "first" should no longer be used:
        conn2.promoteSlaveToMaster(secondHost);
        assertFalse(conn2.isHostMaster(firstHost));
        assertFalse(conn2.isHostSlave(firstHost));
        assertFalse(conn2.isHostSlave(secondHost));
        assertTrue(conn2.isHostMaster(secondHost));
        assertTrue(conn2.isHostSlave(thirdHost));
        assertFalse(conn2.isHostMaster(thirdHost));

        conn2.createStatement().execute("SELECT 1");
        assertFalse(conn2.isClosed());

        // check that we're waiting until transaction boundary to fail over.
        // assertTrue(conn2.hasPendingNewMaster());
        assertFalse(conn2.isClosed());
        conn2.commit();
        assertFalse(conn2.isClosed());
        assertTrue(conn2.isHostMaster(secondHost));
        assertFalse(conn2.isClosed());
        assertTrue(conn2.isMasterConnection());
        assertFalse(conn2.isClosed());

        // validate that queries are successful:
        conn2.createStatement().execute("SELECT 1");
        assertTrue(conn2.isHostMaster(secondHost));

        // master is now offline
        UnreliableSocketFactory.downHost("second");
        try {
            Statement lstmt = conn2.createStatement();
            lstmt.execute("SELECT 1");
            fail("Should fail here due to closed connection");
        } catch (SQLException sqlEx) {
            assertEquals("08S01", sqlEx.getSQLState());
        }

        UnreliableSocketFactory.dontDownHost("second");
        try {
            // won't work now even though master is back up connection has already been implicitly closed when a new master host cannot be found:
            Statement lstmt = conn2.createStatement();
            lstmt.execute("SELECT 1");
            fail("Will fail because inability to find new master host implicitly closes connection.");
        } catch (SQLException e) {
            assertEquals("08003", e.getSQLState());
        }

    }

    public void testReplicationConnectWithNoMaster() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.retriesAllDown.getKeyName(), "3");
        props.setProperty(PropertyKey.allowMasterDownConnections.getKeyName(), "true");

        Set<String> downedHosts = new HashSet<>();
        downedHosts.add("first");

        ReplicationConnection conn2 = this.getUnreliableReplicationConnection(new String[] { "first", "second", "third" }, props, downedHosts);
        assertTrue(conn2.isReadOnly());
        assertFalse(conn2.isMasterConnection());
        try {
            conn2.createStatement().execute("SELECT 1");
        } catch (SQLException e) {
            fail("Should not fail to execute SELECT statements!");
        }
        UnreliableSocketFactory.flushAllStaticData();
        conn2.setReadOnly(false);
        assertFalse(conn2.isReadOnly());
        assertTrue(conn2.isMasterConnection());
        try {
            conn2.createStatement().execute("DROP TABLE IF EXISTS testRepTable");
            conn2.createStatement().execute("CREATE TABLE testRepTable (a INT)");
            conn2.createStatement().execute("INSERT INTO testRepTable VALUES (1)");
            conn2.createStatement().execute("DROP TABLE IF EXISTS testRepTable");

        } catch (SQLException e) {
            fail("Should not fail to execute CREATE/INSERT/DROP statements.");
        }
    }

    public void testReplicationConnectWithMultipleMasters() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.retriesAllDown.getKeyName(), "3");

        Set<MockConnectionConfiguration> configs = new HashSet<>();
        MockConnectionConfiguration first = new MockConnectionConfiguration("first", "slave", null, false);
        MockConnectionConfiguration second = new MockConnectionConfiguration("second", "master", null, false);
        MockConnectionConfiguration third = new MockConnectionConfiguration("third", "master", null, false);

        configs.add(first);
        configs.add(second);
        configs.add(third);

        ReplicationConnection conn2 = this.getUnreliableReplicationConnection(configs, props);
        assertFalse(conn2.isReadOnly());
        assertTrue(conn2.isMasterConnection());
        assertTrue(conn2.isHostSlave(first.getHostPortPair()));
        assertTrue(conn2.isHostMaster(second.getHostPortPair()));
        assertTrue(conn2.isHostMaster(third.getHostPortPair()));

    }

    public void testReplicationConnectionMemory() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.retriesAllDown.getKeyName(), "3");
        String replicationGroup = "memoryGroup";
        props.setProperty(PropertyKey.replicationConnectionGroup.getKeyName(), replicationGroup);
        props.setProperty(PropertyKey.loadBalanceHostRemovalGracePeriod.getKeyName(), "1");

        Set<MockConnectionConfiguration> configs = new HashSet<>();
        MockConnectionConfiguration first = new MockConnectionConfiguration("first", "slave", null, false);
        MockConnectionConfiguration second = new MockConnectionConfiguration("second", "master", null, false);
        MockConnectionConfiguration third = new MockConnectionConfiguration("third", "slave", null, false);

        configs.add(first);
        configs.add(second);
        configs.add(third);

        ReplicationConnection conn2 = this.getUnreliableReplicationConnection(configs, props);

        ReplicationConnectionGroupManager.promoteSlaveToMaster(replicationGroup, first.getHostPortPair());
        ReplicationConnectionGroupManager.removeMasterHost(replicationGroup, second.getHostPortPair());
        ReplicationConnectionGroupManager.addSlaveHost(replicationGroup, second.getHostPortPair());

        conn2.setReadOnly(false);

        assertFalse(conn2.isReadOnly());
        assertTrue(conn2.isMasterConnection());
        assertTrue(conn2.isHostMaster(first.getHostPortPair()));
        assertTrue(conn2.isHostSlave(second.getHostPortPair()));
        assertTrue(conn2.isHostSlave(third.getHostPortPair()));

        // make sure state changes made are reflected in new connections:

        ReplicationConnection conn3 = this.getUnreliableReplicationConnection(configs, props);

        conn3.setReadOnly(false);

        assertFalse(conn3.isReadOnly());
        assertTrue(conn3.isMasterConnection());
        assertTrue(conn3.isHostMaster(first.getHostPortPair()));
        assertTrue(conn3.isHostSlave(second.getHostPortPair()));
        assertTrue(conn3.isHostSlave(third.getHostPortPair()));

    }

    public void testReplicationJMXInterfaces() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.retriesAllDown.getKeyName(), "3");
        String replicationGroup = "testReplicationJMXInterfaces";
        props.setProperty(PropertyKey.replicationConnectionGroup.getKeyName(), replicationGroup);
        props.setProperty(PropertyKey.ha_enableJMX.getKeyName(), "true");

        Set<MockConnectionConfiguration> configs = new HashSet<>();
        MockConnectionConfiguration first = new MockConnectionConfiguration("first", "slave", null, false);
        MockConnectionConfiguration second = new MockConnectionConfiguration("second", "master", null, false);
        MockConnectionConfiguration third = new MockConnectionConfiguration("third", "slave", null, false);

        configs.add(first);
        configs.add(second);
        configs.add(third);

        ReplicationConnection conn2 = this.getUnreliableReplicationConnection(configs, props);

        ReplicationGroupManagerMBean bean = getReplicationMBean();

        assertEquals(1, bean.getActiveLogicalConnectionCount(replicationGroup));
        assertEquals(1, bean.getTotalLogicalConnectionCount(replicationGroup));
        assertEquals(0, bean.getSlavePromotionCount(replicationGroup));
        assertEquals(1, bean.getActiveMasterHostCount(replicationGroup));
        assertEquals(2, bean.getActiveSlaveHostCount(replicationGroup));
        bean.removeSlaveHost(replicationGroup, first.getHostPortPair());
        assertFalse(bean.getSlaveHostsList(replicationGroup).contains(first.getHostPortPair()));
        assertEquals(1, bean.getActiveSlaveHostCount(replicationGroup));
        conn2.close();
        assertEquals(0, bean.getActiveLogicalConnectionCount(replicationGroup));
        conn2 = this.getUnreliableReplicationConnection(configs, props);
        assertEquals(1, bean.getActiveLogicalConnectionCount(replicationGroup));
        assertEquals(2, bean.getTotalLogicalConnectionCount(replicationGroup));
        assertEquals(1, bean.getActiveSlaveHostCount(replicationGroup));
        assertEquals(1, bean.getActiveMasterHostCount(replicationGroup));
        bean.promoteSlaveToMaster(replicationGroup, third.getHostPortPair());
        assertEquals(2, bean.getActiveMasterHostCount(replicationGroup));
        assertEquals(0, bean.getActiveSlaveHostCount(replicationGroup));
        // confirm this works when no group filter is specified:
        bean.addSlaveHost(null, first.getHostPortPair());
        assertEquals(1, bean.getActiveSlaveHostCount(replicationGroup));
        assertEquals(2, bean.getActiveMasterHostCount(replicationGroup));
        bean.removeMasterHost(replicationGroup, second.getHostPortPair());
        assertEquals(1, bean.getActiveSlaveHostCount(replicationGroup));
        assertEquals(1, bean.getActiveMasterHostCount(replicationGroup));

        ReplicationConnection conn3 = this.getUnreliableReplicationConnection(configs, props);

        assertEquals(2, bean.getActiveLogicalConnectionCount(replicationGroup));
        assertEquals(3, bean.getTotalLogicalConnectionCount(replicationGroup));

        assertTrue(bean.getMasterHostsList(replicationGroup).contains(third.getHostPortPair()));
        assertFalse(bean.getMasterHostsList(replicationGroup).contains(first.getHostPortPair()));
        assertFalse(bean.getMasterHostsList(replicationGroup).contains(second.getHostPortPair()));

        assertFalse(bean.getSlaveHostsList(replicationGroup).contains(third.getHostPortPair()));
        assertTrue(bean.getSlaveHostsList(replicationGroup).contains(first.getHostPortPair()));
        assertFalse(bean.getSlaveHostsList(replicationGroup).contains(second.getHostPortPair()));

        assertTrue(bean.getMasterHostsList(replicationGroup).contains(conn3.getMasterConnection().getHost()));
        assertTrue(bean.getSlaveHostsList(replicationGroup).contains(conn3.getSlavesConnection().getHost()));

        assertTrue(bean.getRegisteredConnectionGroups().contains(replicationGroup));

    }

    private ReplicationGroupManagerMBean getReplicationMBean() throws Exception {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        ObjectName mbeanName = new ObjectName("com.mysql.cj.jdbc.jmx:type=ReplicationGroupManager");
        return MBeanServerInvocationHandler.newProxyInstance(mbs, mbeanName, ReplicationGroupManagerMBean.class, false);

    }

    public void testBug43421() throws Exception {

        Properties props = new Properties();
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), "bestResponseTime");

        Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);

        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        // both connections are live now
        UnreliableSocketFactory.downHost("second");
        UnreliableSocketFactory.downHost("first");
        try {
            conn2.createStatement().execute("/* ping */");
            fail("Pings will not succeed when one host is down and using loadbalance w/o global blacklist.");
        } catch (SQLException sqlEx) {
        }

        UnreliableSocketFactory.flushAllStaticData();
        props = new Properties();
        props.setProperty(PropertyKey.loadBalanceBlacklistTimeout.getKeyName(), "200");
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), "bestResponseTime");

        conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);

        assertNotNull("Connection should not be null", this.conn);

        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        // both connections are live now
        UnreliableSocketFactory.downHost("second");
        try {
            conn2.createStatement().execute("/* ping */");
        } catch (SQLException sqlEx) {
            fail("Pings should succeed even though host is down.");
        }
    }

    public void testBug48442() throws Exception {

        Properties props = new Properties();
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), "random");
        Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);

        assertNotNull("Connection should not be null", conn2);
        conn2.setAutoCommit(false);
        UnreliableSocketFactory.downHost("second");
        int hc = 0;
        try {
            conn2.createStatement().execute("SELECT 1");
        } catch (SQLException e) {
            conn2.createStatement().execute("SELECT 1");
        }
        hc = conn2.hashCode();
        conn2.commit();
        UnreliableSocketFactory.dontDownHost("second");
        UnreliableSocketFactory.downHost("first");
        try {
            conn2.commit();
        } catch (SQLException e) {
        }
        assertTrue(hc == conn2.hashCode());

    }

    public void testBug45171() throws Exception {
        List<Statement> statementsToTest = new LinkedList<>();
        statementsToTest.add(this.conn.createStatement());
        statementsToTest.add(((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement("SELECT 1"));
        statementsToTest.add(((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS));
        statementsToTest.add(((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement("SELECT 1", new int[0]));
        statementsToTest.add(((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement("SELECT 1", new String[0]));
        statementsToTest.add(((com.mysql.cj.jdbc.JdbcConnection) this.conn).serverPrepareStatement("SELECT 1"));
        statementsToTest.add(((com.mysql.cj.jdbc.JdbcConnection) this.conn).serverPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS));
        statementsToTest.add(((com.mysql.cj.jdbc.JdbcConnection) this.conn).serverPrepareStatement("SELECT 1", new int[0]));
        statementsToTest.add(((com.mysql.cj.jdbc.JdbcConnection) this.conn).serverPrepareStatement("SELECT 1", new String[0]));

        for (Statement toTest : statementsToTest) {
            assertEquals(toTest.getResultSetType(), ResultSet.TYPE_FORWARD_ONLY);
            assertEquals(toTest.getResultSetConcurrency(), ResultSet.CONCUR_READ_ONLY);
        }

    }

    /**
     * Tests fix for BUG#44587, provide last packet sent/received timing in all
     * connection failure errors.
     */
    public void testBug44587() throws Exception {
        Exception e = null;
        String msg = ExceptionFactory.createLinkFailureMessageBasedOnHeuristics(((MysqlConnection) this.conn).getPropertySet(),
                ((MysqlConnection) this.conn).getSession().getServerSession(), new PacketSentTimeHolder() {
                    @Override
                    public long getPreviousPacketSentTime() {
                        return System.currentTimeMillis() - 1000;
                    }

                    @Override
                    public long getLastPacketSentTime() {
                        return System.currentTimeMillis() - 1000;
                    }
                }, new PacketReceivedTimeHolder() {
                    @Override
                    public long getLastPacketReceivedTime() {
                        return System.currentTimeMillis() - 2000;
                    }
                }, e);
        assertTrue(containsMessage(msg, "CommunicationsException.ServerPacketTimingInfo"));
    }

    /**
     * Tests fix for BUG#45419, ensure that time is not converted to seconds
     * before being reported as milliseconds.
     */
    public void testBug45419() throws Exception {
        Exception e = null;
        String msg = ExceptionFactory.createLinkFailureMessageBasedOnHeuristics(((MysqlConnection) this.conn).getPropertySet(),
                ((MysqlConnection) this.conn).getSession().getServerSession(), new PacketSentTimeHolder() {
                    @Override
                    public long getPreviousPacketSentTime() {
                        return System.currentTimeMillis() - 1000;
                    }

                    @Override
                    public long getLastPacketSentTime() {
                        return System.currentTimeMillis() - 1000;
                    }
                }, new PacketReceivedTimeHolder() {
                    @Override
                    public long getLastPacketReceivedTime() {
                        return System.currentTimeMillis() - 2000;
                    }
                }, e);
        Matcher m = Pattern.compile("([\\d\\,\\.]+)", Pattern.MULTILINE).matcher(msg);
        assertTrue(m.find());
        assertTrue(Long.parseLong(m.group(0).replaceAll("[,.]", "")) >= 2000);
        assertTrue(Long.parseLong(m.group(1).replaceAll("[,.]", "")) >= 1000);
    }

    public static boolean containsMessage(String msg, String key) {
        String[] expectedFragments = Messages.getString(key).split("\\{\\d\\}");
        for (int i = 0; i < expectedFragments.length; i++) {
            if (msg.indexOf(expectedFragments[i]) < 0) {
                return false;
            }
        }
        return true;
    }

    public void testBug46637() throws Exception {
        String hostname = getPortFreeHostname(null);
        UnreliableSocketFactory.flushAllStaticData();
        UnreliableSocketFactory.downHost(hostname);

        try {
            Connection noConn = getConnectionWithProps("socketFactory=testsuite.UnreliableSocketFactory");
            noConn.close();
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage().indexOf("has not received") != -1);
        } finally {
            UnreliableSocketFactory.flushAllStaticData();
        }
    }

    public void testBug32216() throws Exception {
        checkBug32216("www.mysql.com", "12345", "my_database");
        checkBug32216("www.mysql.com", null, "my_database");
    }

    private void checkBug32216(String host, String port, String dbname) throws SQLException {

        StringBuilder url = new StringBuilder("jdbc:mysql://");
        url.append(host);

        if (port != null) {
            url.append(':');
            url.append(port);
        }

        url.append('/');
        url.append(dbname);

        HostInfo connectionHost = ConnectionUrl.getConnectionUrlInstance(url.toString(), null).getMainHost();

        assertEquals("hostname not equal", host, connectionHost.getHost());
        if (port != null) {
            assertEquals("port not equal", port, String.valueOf(connectionHost.getPort()));
        } else {
            assertEquals("port default incorrect", "3306", String.valueOf(connectionHost.getPort()));
        }

        assertEquals("dbname not equal", dbname, connectionHost.getDatabase());
    }

    public void testBug44324() throws Exception {
        createTable("bug44324", "(Id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, SomeVChar VARCHAR(10)) ENGINE=MyISAM;");

        try {
            this.stmt.executeUpdate("INSERT INTO bug44324 values (null, 'Some text much longer than 10 characters')");
        } catch (MysqlDataTruncation sqlEx) {
            assertTrue(0 != sqlEx.getErrorCode());
        }

    }

    public void testBug46925() throws Exception {
        MysqlXADataSource xads1 = new MysqlXADataSource();
        MysqlXADataSource xads2 = new MysqlXADataSource();

        Xid txid = new MysqlXid(new byte[] { 0x1 }, new byte[] { 0xf }, 3306);

        xads1.getProperty(PropertyKey.pinGlobalTxToPhysicalConnection).setValue(true);
        xads1.setUrl(dbUrl);

        xads2.getProperty(PropertyKey.pinGlobalTxToPhysicalConnection).setValue(true);
        xads2.setUrl(dbUrl);

        XAConnection c1 = xads1.getXAConnection();
        assertTrue(c1 instanceof SuspendableXAConnection);
        // start a transaction on one connection
        c1.getXAResource().start(txid, XAResource.TMNOFLAGS);
        c1.getXAResource().end(txid, XAResource.TMSUCCESS);

        XAConnection c2 = xads2.getXAConnection();
        assertTrue(c2 instanceof SuspendableXAConnection);
        // prepare on another one. Since we are using a "pinned" connection we should have the same "currentXAConnection" for both SuspendableXAConnection
        c2.getXAResource().prepare(txid); // this will fail without the fix.
        c2.getXAResource().commit(txid, false);
    }

    public void testBug47494() throws Exception {
        try {
            getConnectionWithProps("jdbc:mysql://localhost:9999/test?socketFactory=testsuite.regression.ConnectionRegressionTest$PortNumberSocketFactory");
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getCause() instanceof IOException);
        }

        try {
            getConnectionWithProps("jdbc:mysql://:9999/test?socketFactory=testsuite.regression.ConnectionRegressionTest$PortNumberSocketFactory");
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getCause() instanceof IOException);
        }

        try {
            getConnectionWithProps("jdbc:mysql://:9999,:9999/test?socketFactory=testsuite.regression.ConnectionRegressionTest$PortNumberSocketFactory");
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getCause() instanceof IOException);
        }

        try {
            getConnectionWithProps(
                    "jdbc:mysql://localhost:9999,localhost:9999/test?socketFactory=testsuite.regression.ConnectionRegressionTest$PortNumberSocketFactory");
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getCause() instanceof IOException);
        }
    }

    public static class PortNumberSocketFactory extends StandardSocketFactory {

        public PortNumberSocketFactory() {

        }

        @Override
        public <T extends Closeable> T connect(String hostname, int portNumber, PropertySet props, int loginTimeout) throws IOException {
            assertEquals(9999, portNumber);

            throw new IOException();
        }

    }

    public void testBug48486() throws Exception {

        String hostSpec = getEncodedHostPortPairFromTestsuiteUrl();
        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        String database = props.getProperty(PropertyKey.DBNAME.getKeyName());
        props.remove(PropertyKey.DBNAME.getKeyName());

        StringBuilder configs = new StringBuilder();
        for (@SuppressWarnings("rawtypes")
        Map.Entry entry : props.entrySet()) {
            configs.append(entry.getKey());
            configs.append("=");
            configs.append(entry.getValue());
            configs.append("&");
        }

        String newUrl = String.format("jdbc:mysql:loadbalance://%s,%s/%s?%s", hostSpec, hostSpec, database, configs.toString());

        MysqlConnectionPoolDataSource ds = new MysqlConnectionPoolDataSource();
        ds.setUrl(newUrl);

        Connection c = ds.getPooledConnection().getConnection();
        this.rs = c.createStatement().executeQuery("SELECT 1");
        this.rs = c.prepareStatement("SELECT 1").executeQuery();
    }

    public void testBug48605() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), "random");
        props.setProperty(PropertyKey.selfDestructOnPingMaxOperations.getKeyName(), "5");
        final Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);

        assertNotNull("Connection should not be null", conn2);
        conn2.setAutoCommit(false);
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        conn2.commit();
        // after commit we may be using a different connection, make sure the number of executions on this also reaches the defined limit.
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");

        assertThrows(SQLException.class, "Ping or validation failed because configured connection lifetime exceeded\\.", new Callable<Void>() {
            public Void call() throws Exception {
                conn2.createStatement().execute("/* ping */ SELECT 1");
                return null;
            }
        });

        assertTrue(conn2.isClosed());

        assertThrows(SQLException.class, "No operations allowed after connection closed.*", new Callable<Void>() {
            public Void call() throws Exception {
                conn2.createStatement().execute("SELECT 1");
                return null;
            }
        });
    }

    public void testBug49700() throws Exception {
        Connection c = getConnectionWithProps("sessionVariables=@foo='bar'");
        assertEquals("bar", getSingleIndexedValueWithQuery(c, 1, "SELECT @foo"));
        ((com.mysql.cj.jdbc.JdbcConnection) c).resetServerState();
        assertEquals("bar", getSingleIndexedValueWithQuery(c, 1, "SELECT @foo"));
    }

    public void testBug51266() throws Exception {
        Properties props = new Properties();
        Set<String> downedHosts = new HashSet<>();
        downedHosts.add("first");

        // this loop will hang on the first unreliable host if the bug isn't
        // fixed.
        for (int i = 0; i < 20; i++) {
            getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props, downedHosts).close();
        }
    }

    // Tests fix for Bug#51643 - connection chosen by load balancer "sticks" to statements that live past commit()/rollback().

    public void testBug51643() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), SequentialBalanceStrategy.class.getName());

        Connection lbConn = getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);
        try {
            PreparedStatement cPstmt = lbConn.prepareStatement("SELECT connection_id()");
            PreparedStatement serverPstmt = lbConn.prepareStatement("SELECT connection_id()");
            Statement plainStmt = lbConn.createStatement();

            lbConn.setAutoCommit(false);
            this.rs = cPstmt.executeQuery();
            this.rs.next();
            String cPstmtConnId = this.rs.getString(1);

            this.rs = serverPstmt.executeQuery();
            this.rs.next();
            String serverPstmtConnId = this.rs.getString(1);

            this.rs = plainStmt.executeQuery("SELECT connection_id()");
            this.rs.next();
            String plainStmtConnId = this.rs.getString(1);
            lbConn.commit();
            lbConn.setAutoCommit(false);

            this.rs = cPstmt.executeQuery();
            this.rs.next();
            String cPstmtConnId2 = this.rs.getString(1);
            assertFalse(cPstmtConnId2.equals(cPstmtConnId));

            this.rs = serverPstmt.executeQuery();
            this.rs.next();
            String serverPstmtConnId2 = this.rs.getString(1);
            assertFalse(serverPstmtConnId2.equals(serverPstmtConnId));

            this.rs = plainStmt.executeQuery("SELECT connection_id()");
            this.rs.next();
            String plainStmtConnId2 = this.rs.getString(1);
            assertFalse(plainStmtConnId2.equals(plainStmtConnId));
        } finally {
            lbConn.close();
        }
    }

    public void testBug51783() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), ForcedLoadBalanceStrategy.class.getName());
        props.setProperty(PropertyKey.loadBalanceBlacklistTimeout.getKeyName(), "5000");
        props.setProperty(PropertyKey.loadBalancePingTimeout.getKeyName(), "100");
        props.setProperty(PropertyKey.loadBalanceValidateConnectionOnSwapServer.getKeyName(), "true");

        String portNumber = String.valueOf(getPortFromTestsuiteUrl());

        ForcedLoadBalanceStrategy.forceFutureServer("first:" + portNumber, -1);
        Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);
        conn2.setAutoCommit(false);
        conn2.createStatement().execute("SELECT 1");
        ForcedLoadBalanceStrategy.forceFutureServer("second:" + portNumber, -1);
        UnreliableSocketFactory.downHost("second");
        try {
            conn2.commit(); // will be on second after this
            assertTrue("Connection should be closed", conn2.isClosed());
        } catch (SQLException e) {
            fail("Should not error because failure to get another server.");
        }
        conn2.close();

        props = new Properties();
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), ForcedLoadBalanceStrategy.class.getName());
        props.setProperty(PropertyKey.loadBalanceBlacklistTimeout.getKeyName(), "5000");
        props.setProperty(PropertyKey.loadBalancePingTimeout.getKeyName(), "100");
        props.setProperty(PropertyKey.loadBalanceValidateConnectionOnSwapServer.getKeyName(), "false");
        ForcedLoadBalanceStrategy.forceFutureServer("first:" + portNumber, -1);
        conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);
        conn2.setAutoCommit(false);
        conn2.createStatement().execute("SELECT 1");
        ForcedLoadBalanceStrategy.forceFutureServer("second:" + portNumber, 1);
        UnreliableSocketFactory.downHost("second");
        try {
            conn2.commit(); // will be on second after this
            assertFalse("Connection should not be closed, should be able to connect to first", conn2.isClosed());
        } catch (SQLException e) {
            fail("Should not error because failure to get another server.");
        }
    }

    public static class ForcedLoadBalanceStrategy extends RandomBalanceStrategy {

        private static String forcedFutureServer = null;
        private static int forceFutureServerTimes = 0;

        public static void forceFutureServer(String host, int times) {
            forcedFutureServer = host;
            forceFutureServerTimes = times;
        }

        public static void dontForceFutureServer() {
            forcedFutureServer = null;
            forceFutureServerTimes = 0;
        }

        @Override
        public com.mysql.cj.jdbc.ConnectionImpl pickConnection(InvocationHandler proxy, List<String> configuredHosts,
                Map<String, JdbcConnection> liveConnections, long[] responseTimes, int numRetries) throws SQLException {
            if (forcedFutureServer == null || forceFutureServerTimes == 0 || !configuredHosts.contains(forcedFutureServer)) {
                return super.pickConnection(proxy, configuredHosts, liveConnections, responseTimes, numRetries);
            }
            if (forceFutureServerTimes > 0) {
                forceFutureServerTimes--;
            }
            ConnectionImpl conn = (ConnectionImpl) liveConnections.get(forcedFutureServer);

            if (conn == null) {
                conn = ((LoadBalancedConnectionProxy) proxy).createConnectionForHost(forcedFutureServer);

            }
            return conn;
        }
    }

    public void testAutoCommitLB() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), CountingReBalanceStrategy.class.getName());
        props.setProperty(PropertyKey.loadBalanceAutoCommitStatementThreshold.getKeyName(), "3");

        Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);
        conn2.setAutoCommit(true);
        CountingReBalanceStrategy.resetTimesRebalanced();
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 2");
        assertEquals(0, CountingReBalanceStrategy.getTimesRebalanced());
        conn2.createStatement().execute("SELECT 3");
        assertEquals(1, CountingReBalanceStrategy.getTimesRebalanced());
        conn2.setAutoCommit(false);
        CountingReBalanceStrategy.resetTimesRebalanced();
        assertEquals(0, CountingReBalanceStrategy.getTimesRebalanced());
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 2");
        conn2.createStatement().execute("SELECT 3");
        assertEquals(0, CountingReBalanceStrategy.getTimesRebalanced());
        conn2.close();

        props.remove(PropertyKey.loadBalanceAutoCommitStatementThreshold.getKeyName());
        conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);
        conn2.setAutoCommit(true);
        CountingReBalanceStrategy.resetTimesRebalanced();
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 2");
        conn2.createStatement().execute("SELECT 3");
        assertEquals(0, CountingReBalanceStrategy.getTimesRebalanced());
        conn2.setAutoCommit(false);
        CountingReBalanceStrategy.resetTimesRebalanced();
        assertEquals(0, CountingReBalanceStrategy.getTimesRebalanced());
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 2");
        conn2.createStatement().execute("SELECT 3");
        assertEquals(0, CountingReBalanceStrategy.getTimesRebalanced());
        conn2.close();

        props.setProperty(PropertyKey.loadBalanceAutoCommitStatementThreshold.getKeyName(), "3");
        props.setProperty(PropertyKey.loadBalanceAutoCommitStatementRegex.getKeyName(), ".*2.*");
        conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);
        conn2.setAutoCommit(true);
        CountingReBalanceStrategy.resetTimesRebalanced();
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 2");
        conn2.createStatement().execute("SELECT 3");
        conn2.createStatement().execute("SELECT 2");
        assertEquals(0, CountingReBalanceStrategy.getTimesRebalanced());
        conn2.createStatement().execute("SELECT 2");
        assertEquals(1, CountingReBalanceStrategy.getTimesRebalanced());
        conn2.close();

    }

    public static class CountingReBalanceStrategy extends RandomBalanceStrategy {

        private static int rebalancedTimes = 0;

        public static int getTimesRebalanced() {
            return rebalancedTimes;
        }

        public static void resetTimesRebalanced() {
            rebalancedTimes = 0;
        }

        @Override
        public com.mysql.cj.jdbc.ConnectionImpl pickConnection(InvocationHandler proxy, List<String> configuredHosts,
                Map<String, JdbcConnection> liveConnections, long[] responseTimes, int numRetries) throws SQLException {
            rebalancedTimes++;
            return super.pickConnection(proxy, configuredHosts, liveConnections, responseTimes, numRetries);
        }
    }

    public void testBug56429() throws Exception {
        Properties props = getPropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
        props.setProperty(PropertyKey.socketFactory.getKeyName(), "testsuite.UnreliableSocketFactory");

        String host = props.getProperty(PropertyKey.HOST.getKeyName());
        String port = props.getProperty(PropertyKey.PORT.getKeyName());

        props.remove(PropertyKey.HOST.getKeyName());

        props.setProperty(PropertyKey.queriesBeforeRetryMaster.getKeyName(), "50");
        props.setProperty(PropertyKey.maxReconnects.getKeyName(), "1");

        UnreliableSocketFactory.mapHost("master", host);
        UnreliableSocketFactory.mapHost("slave", host);

        Connection failoverConnection = null;

        try {
            failoverConnection = getConnectionWithProps("jdbc:mysql://master:" + port + ",slave:" + port + "/", props);

            String userHost = getSingleIndexedValueWithQuery(1, "SELECT USER()").toString();
            String[] userParts = userHost.split("@");

            this.rs = this.stmt.executeQuery("SHOW PROCESSLIST");

            int startConnCount = 0;

            while (this.rs.next()) {
                if (this.rs.getString("User").equals(userParts[0]) && this.rs.getString("Host").equals(userParts[1])) {
                    startConnCount++;
                }
            }

            assert (startConnCount > 0);

            failoverConnection.setAutoCommit(false); // this will fail if state
                                                    // not copied over

            for (int i = 0; i < 20; i++) {

                failoverConnection.commit();
            }

            this.rs = this.stmt.executeQuery("SHOW PROCESSLIST");

            int endConnCount = 0;

            while (this.rs.next()) {
                if (this.rs.getString("User").equals(userParts[0]) && this.rs.getString("Host").equals(userParts[1])) {
                    endConnCount++;
                }
            }

            assert (endConnCount > 0);

            if (endConnCount - startConnCount >= 20) {
                // this may be bogus if run on a real system, we should probably look to see they're coming from this testsuite?
                fail("We're leaking connections even when not failed over");
            }
        } finally {
            if (failoverConnection != null) {
                failoverConnection.close();
            }
        }
    }

    public void testBug56955() throws Exception {
        assertEquals("JKS", ((MysqlConnection) this.conn).getPropertySet().getStringProperty(PropertyKey.trustCertificateKeyStoreType).getStringValue());
        assertEquals("JKS", ((MysqlConnection) this.conn).getPropertySet().getStringProperty(PropertyKey.clientCertificateKeyStoreType).getStringValue());
    }

    public void testBug58706() throws Exception {
        Properties props = getPropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
        props.setProperty(PropertyKey.socketFactory.getKeyName(), "testsuite.UnreliableSocketFactory");

        String host = props.getProperty(PropertyKey.HOST.getKeyName());
        String port = props.getProperty(PropertyKey.PORT.getKeyName());

        props.remove(PropertyKey.HOST.getKeyName());

        props.setProperty(PropertyKey.queriesBeforeRetryMaster.getKeyName(), "0");
        props.setProperty(PropertyKey.secondsBeforeRetryMaster.getKeyName(), "1");
        props.setProperty(PropertyKey.failOverReadOnly.getKeyName(), "false");

        UnreliableSocketFactory.mapHost("master", host);
        UnreliableSocketFactory.mapHost("slave", host);

        Connection failoverConnection = null;

        try {
            failoverConnection = getConnectionWithProps("jdbc:mysql://master:" + port + ",slave:" + port + "/", props);
            failoverConnection.setAutoCommit(false);

            assertEquals("/master", UnreliableSocketFactory.getHostFromLastConnection());

            for (int i = 0; i < 50; i++) {
                this.rs = failoverConnection.createStatement().executeQuery("SELECT 1");
            }

            UnreliableSocketFactory.downHost("master");

            try {
                this.rs = failoverConnection.createStatement().executeQuery("SELECT 1"); // this should fail and trigger failover
                fail("Expected exception");
            } catch (SQLException sqlEx) {
                assertEquals("08S01", sqlEx.getSQLState());
            }

            failoverConnection.setAutoCommit(true);
            assertEquals("/slave", UnreliableSocketFactory.getHostFromLastConnection());
            assertTrue(!failoverConnection.isReadOnly());
            this.rs = failoverConnection.createStatement().executeQuery("SELECT 1");
            this.rs = failoverConnection.createStatement().executeQuery("SELECT 1");
            UnreliableSocketFactory.dontDownHost("master");
            Thread.sleep(2000);
            failoverConnection.setAutoCommit(true);
            this.rs = failoverConnection.createStatement().executeQuery("SELECT 1");
            assertEquals("/master", UnreliableSocketFactory.getHostFromLastConnection());
            this.rs = failoverConnection.createStatement().executeQuery("SELECT 1");
        } finally {
            UnreliableSocketFactory.flushAllStaticData();

            if (failoverConnection != null) {
                failoverConnection.close();
            }
        }
    }

    public void testStatementComment() throws Exception {
        Connection c = getConnectionWithProps("autoGenerateTestcaseScript=true,logger=StandardLogger");
        PrintStream oldErr = System.err;

        try {
            ByteArrayOutputStream bOut = new ByteArrayOutputStream();
            PrintStream printStream = new PrintStream(bOut);
            System.setErr(printStream);

            ((com.mysql.cj.jdbc.JdbcConnection) c).setStatementComment("Hi there");
            c.setAutoCommit(false);

            c.createStatement().execute("SELECT 1");
            c.commit();
            c.rollback();
            Pattern pattern = Pattern.compile("Hi");
            String loggedData = new String(bOut.toByteArray());
            Matcher matcher = pattern.matcher(loggedData);
            int count = 0;
            while (matcher.find()) {
                count++;
            }

            assertEquals(4, count);
        } finally {
            System.setErr(oldErr);
        }
    }

    public void testReconnectWithCachedConfig() throws Exception {
        Connection rConn = getConnectionWithProps("autoReconnect=true,initialTimeout=2,maxReconnects=3,cacheServerConfiguration=true,elideSetAutoCommits=true");
        String threadId = getSingleIndexedValueWithQuery(rConn, 1, "select connection_id()").toString();
        killConnection(this.conn, threadId);
        boolean detectedDeadConn = false;

        for (int i = 0; i < 100; i++) {
            try {
                rConn.createStatement().executeQuery("SELECT 1");
            } catch (SQLException sqlEx) {
                detectedDeadConn = true;
                break;
            }
        }

        assertTrue(detectedDeadConn);
        rConn.prepareStatement("SELECT 1").execute();

        Connection rConn2 = getConnectionWithProps(
                "autoReconnect=true,initialTimeout=2,maxReconnects=3,cacheServerConfiguration=true,elideSetAutoCommits=true");
        rConn2.prepareStatement("SELECT 1").execute();

    }

    public void testBug61201() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.sessionVariables.getKeyName(), "FOREIGN_KEY_CHECKS=0");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "latin1");
        props.setProperty(PropertyKey.profileSQL.getKeyName(), "true");

        Connection varConn = getConnectionWithProps(props);
        varConn.close();
    }

    @SuppressWarnings("resource")
    public void testChangeUser() throws Exception {
        Properties props = getPropertiesFromTestsuiteUrl();

        Connection testConn = getConnectionWithProps(props);
        Statement testStmt = testConn.createStatement();

        for (int i = 0; i < 500; i++) {
            ((com.mysql.cj.jdbc.JdbcConnection) testConn).changeUser(props.getProperty(PropertyKey.USER.getKeyName()),
                    props.getProperty(PropertyKey.PASSWORD.getKeyName()));

            if (i % 10 == 0) {
                try {
                    ((com.mysql.cj.jdbc.JdbcConnection) testConn).changeUser("bubba", props.getProperty(PropertyKey.PASSWORD.getKeyName()));
                } catch (SQLException sqlEx) {
                    sqlEx.printStackTrace();
                    assertTrue(testConn.isClosed());
                    testConn = getConnectionWithProps(props);
                    testStmt = testConn.createStatement();
                }
            }

            testStmt.execute("SELECT 1");
        }
        testConn.close();
    }

    public void testChangeUserNoDb() throws Exception {
        String databaseName = "testchangeusernodb";

        this.stmt.executeUpdate("DROP DATABASE IF EXISTS " + databaseName);

        Properties props = getPropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.createDatabaseIfNotExist.getKeyName(), "true");
        props.setProperty(PropertyKey.DBNAME.getKeyName(), databaseName);
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        Connection con = getConnectionWithProps(props);

        this.rs = this.stmt.executeQuery("show databases like '" + databaseName + "'");
        if (this.rs.next()) {
            assertEquals(databaseName, this.rs.getString(1));
        } else {
            fail("Database " + databaseName + " is not found.");
        }

        ((com.mysql.cj.jdbc.JdbcConnection) con).changeUser(props.getProperty(PropertyKey.USER.getKeyName()),
                props.getProperty(PropertyKey.PASSWORD.getKeyName()));

        this.rs = con.createStatement().executeQuery("select DATABASE()");
        assertTrue(this.rs.next());
        assertEquals(databaseName, this.rs.getString(1));

        con.close();
    }

    public void testChangeUserClosedConn() throws Exception {
        Properties props = getPropertiesFromTestsuiteUrl();
        Connection newConn = getConnectionWithProps((Properties) null);

        try {
            newConn.close();
            ((com.mysql.cj.jdbc.JdbcConnection) newConn).changeUser(props.getProperty(PropertyKey.USER.getKeyName()),
                    props.getProperty(PropertyKey.PASSWORD.getKeyName()));
            fail("Expected SQL Exception");
        } catch (SQLException ex) {
            // expected
            if (!ex.getClass().getName().endsWith("SQLNonTransientConnectionException")) {
                throw ex;
            }
        } finally {
            newConn.close();
        }
    }

    public void testBug63284() throws Exception {
        Properties props = getPropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
        props.setProperty(PropertyKey.socketFactory.getKeyName(), "testsuite.UnreliableSocketFactory");

        String host = props.getProperty(PropertyKey.HOST.getKeyName());
        String port = props.getProperty(PropertyKey.PORT.getKeyName());

        props.remove(PropertyKey.HOST.getKeyName());

        props.setProperty(PropertyKey.queriesBeforeRetryMaster.getKeyName(), "50");
        props.setProperty(PropertyKey.maxReconnects.getKeyName(), "1");

        UnreliableSocketFactory.mapHost("master", host);
        UnreliableSocketFactory.mapHost("slave", host);

        Connection failoverConnection1 = null;
        Connection failoverConnection2 = null;

        try {
            failoverConnection1 = getConnectionWithProps("jdbc:mysql://master:" + port + ",slave:" + port + "/", props);

            failoverConnection2 = getConnectionWithProps("jdbc:mysql://master:" + port + ",slave:" + port + "/", props);

            assertTrue(((com.mysql.cj.jdbc.JdbcConnection) failoverConnection1).isMasterConnection());

            // Two different Connection objects should not equal each other:
            assertFalse(failoverConnection1.equals(failoverConnection2));

            int hc = failoverConnection1.hashCode();

            UnreliableSocketFactory.downHost("master");

            for (int i = 0; i < 3; i++) {
                try {
                    failoverConnection1.createStatement().execute("SELECT 1");
                } catch (SQLException e) {
                    // do nothing, expect SQLException when failing over initially goal here is to ensure valid connection against a slave
                }
            }
            // ensure we're now connected to the slave
            assertFalse(((com.mysql.cj.jdbc.JdbcConnection) failoverConnection1).isMasterConnection());

            // ensure that hashCode() result is persistent across failover events when proxy state changes
            assertEquals(hc, failoverConnection1.hashCode());
        } finally {
            if (failoverConnection1 != null) {
                failoverConnection1.close();
            }
            if (failoverConnection2 != null) {
                failoverConnection2.close();
            }
        }
    }

    public void testDefaultPlugin() throws Exception {
        if (!versionMeetsMinimum(5, 5, 7)) {
            return;
        }
        Connection testConn = null;
        Properties props = new Properties();

        props.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), "");
        try {
            testConn = getConnectionWithProps(props);
            assertTrue("Exception is expected due to incorrect defaultAuthenticationPlugin value", false);
        } catch (SQLException sqlEx) {
            assertTrue(true);
        } finally {
            if (testConn != null) {
                testConn.close();
            }
        }

        props.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), "mysql_native_password");
        try {
            testConn = getConnectionWithProps(props);
            assertTrue("Exception is expected due to incorrect defaultAuthenticationPlugin value (mechanism name instead of class name)", false);
        } catch (SQLException sqlEx) {
            assertTrue(true);
        } finally {
            if (testConn != null) {
                testConn.close();
            }
        }

        props.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
        try {
            testConn = getConnectionWithProps(props);
            assertTrue("Exception is expected due to defaultAuthenticationPlugin value is not listed", false);
        } catch (SQLException sqlEx) {
            assertTrue(true);
        } finally {
            if (testConn != null) {
                testConn.close();
            }
        }

        props.setProperty(PropertyKey.authenticationPlugins.getKeyName(), "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
        props.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
        try {
            testConn = getConnectionWithProps(props);
            assertTrue(true);
        } catch (SQLException sqlEx) {
            assertTrue("Exception is not expected due to defaultAuthenticationPlugin value is correctly listed", false);
        } finally {
            if (testConn != null) {
                testConn.close();
            }
        }
    }

    public void testDisabledPlugins() throws Exception {
        if (!versionMeetsMinimum(5, 5, 7)) {
            return;
        }
        Connection testConn = null;
        Properties props = new Properties();

        props.setProperty(PropertyKey.disabledAuthenticationPlugins.getKeyName(), "mysql_native_password");
        try {
            testConn = getConnectionWithProps(props);
            assertTrue("Exception is expected due to disabled defaultAuthenticationPlugin", false);
        } catch (SQLException sqlEx) {
            assertTrue(true);
        } finally {
            if (testConn != null) {
                testConn.close();
            }
        }

        props.setProperty(PropertyKey.disabledAuthenticationPlugins.getKeyName(), MysqlNativePasswordPlugin.class.getName());
        try {
            testConn = getConnectionWithProps(props);
            assertTrue("Exception is expected due to disabled defaultAuthenticationPlugin", false);
        } catch (SQLException sqlEx) {
            assertTrue(true);
        } finally {
            if (testConn != null) {
                testConn.close();
            }
        }

        props.setProperty(PropertyKey.authenticationPlugins.getKeyName(), "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
        props.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
        props.setProperty(PropertyKey.disabledAuthenticationPlugins.getKeyName(), "auth_test_plugin");
        try {
            testConn = getConnectionWithProps(props);
            assertTrue("Exception is expected due to disabled defaultAuthenticationPlugin", false);
        } catch (SQLException sqlEx) {
            assertTrue(true);
        } finally {
            if (testConn != null) {
                testConn.close();
            }
        }

        props.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), MysqlNativePasswordPlugin.class.getName());
        props.setProperty(PropertyKey.authenticationPlugins.getKeyName(), "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
        props.setProperty(PropertyKey.disabledAuthenticationPlugins.getKeyName(), "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
        try {
            testConn = getConnectionWithProps(props);
            assertTrue(true);
        } catch (SQLException sqlEx) {
            assertTrue("Exception is not expected due to disabled plugin is not default", false);
        } finally {
            if (testConn != null) {
                testConn.close();
            }
        }
    }

    public void testAuthTestPlugin() throws Exception {
        if (!versionMeetsMinimum(5, 5, 7)) {
            return;
        }
        boolean install_plugin_in_runtime = false;
        try {

            // install plugin if required
            this.rs = this.stmt.executeQuery(
                    "select (PLUGIN_LIBRARY LIKE 'auth_test_plugin%') as `TRUE`" + " FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='test_plugin_server'");
            if (this.rs.next()) {
                if (!this.rs.getBoolean(1)) {
                    install_plugin_in_runtime = true;
                }
            } else {
                install_plugin_in_runtime = true;
            }

            if (install_plugin_in_runtime) {
                String ext = Constants.OS_NAME.toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
                this.stmt.executeUpdate("INSTALL PLUGIN test_plugin_server SONAME 'auth_test_plugin" + ext + "'");
            }

            String dbname = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.DBNAME.getKeyName());
            assertFalse("No database selected", StringUtils.isNullOrEmpty(dbname));

            // create proxy users
            createUser("'wl5851user'@'%'", "identified WITH test_plugin_server AS 'plug_dest'");
            createUser("'plug_dest'@'%'", "IDENTIFIED BY 'foo'");
            this.stmt.executeUpdate("GRANT PROXY ON 'plug_dest'@'%' TO 'wl5851user'@'%'");
            this.stmt.executeUpdate("delete from mysql.db where user='plug_dest'");
            this.stmt.executeUpdate(
                    "insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '"
                            + dbname + "', 'plug_dest', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
            this.stmt.executeUpdate(
                    "insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', 'information\\_schema', 'plug_dest', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
            this.stmt.executeUpdate("flush privileges");

            Properties props = new Properties();
            props.setProperty(PropertyKey.USER.getKeyName(), "wl5851user");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "plug_dest");
            props.setProperty(PropertyKey.authenticationPlugins.getKeyName(), "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");

            Connection testConn = null;
            Statement testSt = null;
            ResultSet testRs = null;
            try {
                testConn = getConnectionWithProps(props);
                testSt = testConn.createStatement();
                testRs = testSt.executeQuery("select USER(),CURRENT_USER()");
                testRs.next();
                assertEquals("wl5851user", testRs.getString(1).split("@")[0]);
                assertEquals("plug_dest", testRs.getString(2).split("@")[0]);

            } finally {
                if (testRs != null) {
                    testRs.close();
                }
                if (testSt != null) {
                    testSt.close();
                }
                if (testConn != null) {
                    testConn.getWarnings();
                    testConn.close();
                }
            }

        } finally {
            if (install_plugin_in_runtime) {
                this.stmt.executeUpdate("UNINSTALL PLUGIN test_plugin_server");
            }
        }
    }

    public void testTwoQuestionsPlugin() throws Exception {
        if (!versionMeetsMinimum(5, 5, 7)) {
            return;
        }
        boolean install_plugin_in_runtime = false;
        try {

            // install plugin if required
            this.rs = this.stmt.executeQuery("select PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='two_questions'");
            if (this.rs.next()) {
                if (!this.rs.getString(1).equals("ACTIVE")) {
                    fail("The 'two_questions' plugin is preinstalled but not active.");
                }
            } else {
                install_plugin_in_runtime = true;
            }

            if (install_plugin_in_runtime) {
                String ext = Constants.OS_NAME.toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
                this.stmt.executeUpdate("INSTALL PLUGIN two_questions SONAME 'auth" + ext + "'");
            }

            String dbname = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.DBNAME.getKeyName());
            assertFalse("No database selected", StringUtils.isNullOrEmpty(dbname));

            createUser("'wl5851user2'@'%'", "identified WITH two_questions AS 'two_questions_password'");
            this.stmt.executeUpdate("delete from mysql.db where user='wl5851user2'");
            this.stmt.executeUpdate(
                    "insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '"
                            + dbname + "', 'wl5851user2', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
            this.stmt.executeUpdate(
                    "insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', 'information\\_schema', 'wl5851user2', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
            this.stmt.executeUpdate("flush privileges");

            Properties props = new Properties();
            props.setProperty(PropertyKey.USER.getKeyName(), "wl5851user2");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "two_questions_password");
            props.setProperty(PropertyKey.authenticationPlugins.getKeyName(), "testsuite.regression.ConnectionRegressionTest$TwoQuestionsPlugin");

            Connection testConn = null;
            Statement testSt = null;
            ResultSet testRs = null;
            try {
                testConn = getConnectionWithProps(props);
                testSt = testConn.createStatement();
                testRs = testSt.executeQuery("select USER(),CURRENT_USER()");
                testRs.next();
                assertEquals("wl5851user2", testRs.getString(1).split("@")[0]);

            } finally {
                if (testRs != null) {
                    testRs.close();
                }
                if (testSt != null) {
                    testSt.close();
                }
                if (testConn != null) {
                    testConn.getWarnings();
                    testConn.close();
                }
            }

        } finally {
            if (install_plugin_in_runtime) {
                this.stmt.executeUpdate("UNINSTALL PLUGIN two_questions");
            }
        }
    }

    public void testThreeAttemptsPlugin() throws Exception {
        if (!versionMeetsMinimum(5, 5, 7)) {
            return;
        }
        boolean install_plugin_in_runtime = false;
        try {

            // install plugin if required
            this.rs = this.stmt.executeQuery("select PLUGIN_STATUS FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='three_attempts'");
            if (this.rs.next()) {
                if (!this.rs.getString(1).equals("ACTIVE")) {
                    fail("The 'three_attempts' plugin is preinstalled but not active.");
                }
            } else {
                install_plugin_in_runtime = true;
            }

            if (install_plugin_in_runtime) {
                String ext = Constants.OS_NAME.toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
                this.stmt.executeUpdate("INSTALL PLUGIN three_attempts SONAME 'auth" + ext + "'");
            }

            String dbname = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.DBNAME.getKeyName());
            assertFalse("No database selected", StringUtils.isNullOrEmpty(dbname));

            createUser("'wl5851user3'@'%'", "identified WITH three_attempts AS 'three_attempts_password'");
            this.stmt.executeUpdate("delete from mysql.db where user='wl5851user3'");
            this.stmt.executeUpdate(
                    "insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '"
                            + dbname + "', 'wl5851user3', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
            this.stmt.executeUpdate(
                    "insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', 'information\\_schema', 'wl5851user3', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
            this.stmt.executeUpdate("flush privileges");

            Properties props = new Properties();
            props.setProperty(PropertyKey.USER.getKeyName(), "wl5851user3");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "three_attempts_password");
            props.setProperty(PropertyKey.authenticationPlugins.getKeyName(), "testsuite.regression.ConnectionRegressionTest$ThreeAttemptsPlugin");

            Connection testConn = null;
            Statement testSt = null;
            ResultSet testRs = null;
            try {
                testConn = getConnectionWithProps(props);
                testSt = testConn.createStatement();
                testRs = testSt.executeQuery("select USER(),CURRENT_USER()");
                testRs.next();
                assertEquals("wl5851user3", testRs.getString(1).split("@")[0]);

            } finally {
                if (testRs != null) {
                    testRs.close();
                }
                if (testSt != null) {
                    testSt.close();
                }
                if (testConn != null) {
                    testConn.getWarnings();
                    testConn.close();
                }
            }

        } catch (Exception ex) {
            ex.printStackTrace();
            throw ex;
        } finally {
            if (install_plugin_in_runtime) {
                this.stmt.executeUpdate("UNINSTALL PLUGIN three_attempts");
            }
        }
    }

    public static class AuthTestPlugin implements AuthenticationPlugin<NativePacketPayload> {

        private String password = null;

        public void destroy() {
            this.password = null;
        }

        public String getProtocolPluginName() {
            return "auth_test_plugin";
        }

        public boolean requiresConfidentiality() {
            return false;
        }

        public boolean isReusable() {
            return true;
        }

        public void setAuthenticationParameters(String user, String password) {
            this.password = password;
        }

        public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
            toServer.clear();
            NativePacketPayload bresp = new NativePacketPayload(StringUtils.getBytes(this.password));
            toServer.add(bresp);
            return true;
        }

        public void reset() {
        }

    }

    public static class TwoQuestionsPlugin implements AuthenticationPlugin<NativePacketPayload> {

        private String password = null;

        public void destroy() {
            this.password = null;
        }

        public String getProtocolPluginName() {
            return "dialog";
        }

        public boolean requiresConfidentiality() {
            return false;
        }

        public boolean isReusable() {
            return true;
        }

        public void setAuthenticationParameters(String user, String password) {
            this.password = password;
        }

        public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
            toServer.clear();
            if ((fromServer.getByteBuffer()[0] & 0xff) == 4) {
                NativePacketPayload bresp = new NativePacketPayload(StringUtils.getBytes(this.password));
                toServer.add(bresp);
            } else {
                NativePacketPayload bresp = new NativePacketPayload(StringUtils.getBytes("yes, of course"));
                toServer.add(bresp);
            }
            return true;
        }

        public void reset() {
        }

    }

    public static class ThreeAttemptsPlugin implements AuthenticationPlugin<NativePacketPayload> {

        private String password = null;
        private int counter = 0;

        public void destroy() {
            this.password = null;
            this.counter = 0;
        }

        public String getProtocolPluginName() {
            return "dialog";
        }

        public boolean requiresConfidentiality() {
            return false;
        }

        public boolean isReusable() {
            return true;
        }

        public void setAuthenticationParameters(String user, String password) {
            this.password = password;
        }

        public boolean nextAuthenticationStep(NativePacketPayload fromServer, List<NativePacketPayload> toServer) {
            toServer.clear();
            this.counter++;
            if ((fromServer.getByteBuffer()[0] & 0xff) == 4) {
                NativePacketPayload bresp = new NativePacketPayload(StringUtils.getBytes(this.counter > 2 ? this.password : "wrongpassword" + this.counter));
                toServer.add(bresp);
            } else {
                NativePacketPayload bresp = new NativePacketPayload(fromServer.getByteBuffer());
                toServer.add(bresp);
            }
            return true;
        }

        public void reset() {
        }

    }

    public void testOldPasswordPlugin() throws Exception {

        if (!versionMeetsMinimum(5, 5, 7) || versionMeetsMinimum(5, 7, 5)) {
            // As of 5.7.5, support for mysql_old_password is removed.
            System.out.println("testOldPasswordPlugin was skipped: This test is only run for 5.5.7 - 5.7.4 server versions.");
            return;
        }

        Connection testConn = null;

        try {
            this.stmt.executeUpdate("SET @current_secure_auth = @@global.secure_auth");
            this.stmt.executeUpdate("SET GLOBAL secure_auth= off");

            createUser("'bug64983user1'@'%'", "IDENTIFIED WITH mysql_old_password");
            this.stmt.executeUpdate("set password for 'bug64983user1'@'%' = OLD_PASSWORD('pwd')");
            this.stmt.executeUpdate("grant all on *.* to 'bug64983user1'@'%'");

            createUser("'bug64983user2'@'%'", "IDENTIFIED WITH mysql_old_password");
            this.stmt.executeUpdate("set password for 'bug64983user2'@'%' = OLD_PASSWORD('')");
            this.stmt.executeUpdate("grant all on *.* to 'bug64983user2'@'%'");

            createUser("'bug64983user3'@'%'", "IDENTIFIED WITH mysql_old_password");
            this.stmt.executeUpdate("grant all on *.* to 'bug64983user3'@'%'");

            this.stmt.executeUpdate("flush privileges");

            Properties props = new Properties();

            // connect with default plugin
            props.setProperty(PropertyKey.USER.getKeyName(), "bug64983user1");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");
            testConn = getConnectionWithProps(props);
            ResultSet testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user1", testRs.getString(1).split("@")[0]);
            testConn.close();

            props.setProperty(PropertyKey.USER.getKeyName(), "bug64983user2");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "");
            testConn = getConnectionWithProps(props);
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user2", testRs.getString(1).split("@")[0]);
            testConn.close();

            props.setProperty(PropertyKey.USER.getKeyName(), "bug64983user3");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "");
            testConn = getConnectionWithProps(props);
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user3", testRs.getString(1).split("@")[0]);
            testConn.close();

            // connect with MysqlOldPasswordPlugin plugin
            props.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), MysqlOldPasswordPlugin.class.getName());

            props.setProperty(PropertyKey.USER.getKeyName(), "bug64983user1");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");
            testConn = getConnectionWithProps(props);
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user1", testRs.getString(1).split("@")[0]);
            testConn.close();

            props.setProperty(PropertyKey.USER.getKeyName(), "bug64983user2");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "");
            testConn = getConnectionWithProps(props);
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user2", testRs.getString(1).split("@")[0]);
            testConn.close();

            props.setProperty(PropertyKey.USER.getKeyName(), "bug64983user3");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "");
            testConn = getConnectionWithProps(props);
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user3", testRs.getString(1).split("@")[0]);

            // changeUser
            ((JdbcConnection) testConn).changeUser("bug64983user1", "pwd");
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user1", testRs.getString(1).split("@")[0]);

            ((JdbcConnection) testConn).changeUser("bug64983user2", "");
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user2", testRs.getString(1).split("@")[0]);

            ((JdbcConnection) testConn).changeUser("bug64983user3", "");
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user3", testRs.getString(1).split("@")[0]);

        } finally {
            try {
                this.stmt.executeUpdate("SET GLOBAL secure_auth = @current_secure_auth");

                if (testConn != null) {
                    testConn.close();
                }
            } catch (Exception ex) {
                System.err.println("Exception during cleanup:");
                ex.printStackTrace();
            }
        }

    }

    public void testAuthCleartextPlugin() throws Exception {
        if (!versionMeetsMinimum(5, 5, 7)) {
            return;
        }
        boolean install_plugin_in_runtime = false;
        try {

            // install plugin if required
            this.rs = this.stmt.executeQuery("select (PLUGIN_LIBRARY LIKE 'auth_test_plugin%') as `TRUE`"
                    + " FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='cleartext_plugin_server'");
            if (this.rs.next()) {
                if (!this.rs.getBoolean(1)) {
                    install_plugin_in_runtime = true;
                }
            } else {
                install_plugin_in_runtime = true;
            }

            if (install_plugin_in_runtime) {
                String ext = Constants.OS_NAME.toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
                this.stmt.executeUpdate("INSTALL PLUGIN cleartext_plugin_server SONAME 'auth_test_plugin" + ext + "'");
            }

            String dbname = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.DBNAME.getKeyName());
            assertFalse("No database selected", StringUtils.isNullOrEmpty(dbname));

            // create proxy users
            createUser("'wl5735user'@'%'", "identified WITH cleartext_plugin_server AS ''");
            this.stmt.executeUpdate("delete from mysql.db where user='wl5735user'");
            this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, "
                    + "Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,"
                    + "Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '" + dbname
                    + "', 'wl5735user', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
            this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, "
                    + "Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,"
                    + "Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES "
                    + "('%', 'information\\_schema', 'wl5735user', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', "
                    + "'Y', 'Y', 'N', 'N')");
            this.stmt.executeUpdate("flush privileges");

            Properties props = new Properties();
            props.setProperty(PropertyKey.USER.getKeyName(), "wl5735user");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "");
            props.setProperty(PropertyKey.useSSL.getKeyName(), "false");

            Connection testConn = null;
            Statement testSt = null;
            ResultSet testRs = null;
            try {
                testConn = getConnectionWithProps(props);
                fail("SQLException expected due to SSL connection is required");
            } catch (Exception e) {
                assertEquals("SSL connection required for plugin 'mysql_clear_password'. Check if \"useSSL\" is set to \"true\".", e.getMessage());
            } finally {
                if (testConn != null) {
                    testConn.getWarnings();
                    testConn.close();
                }
            }

            try {
                String trustStorePath = "src/test/config/ssl-test-certs/ca-truststore";
                System.setProperty("javax.net.ssl.keyStore", trustStorePath);
                System.setProperty("javax.net.ssl.keyStorePassword", "password");
                System.setProperty("javax.net.ssl.trustStore", trustStorePath);
                System.setProperty("javax.net.ssl.trustStorePassword", "password");
                props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                testConn = getConnectionWithProps(props);

                assertTrue("SSL connection isn't actually established!", ((MysqlConnection) testConn).getSession().isSSLEstablished());

                testSt = testConn.createStatement();
                testRs = testSt.executeQuery("select USER(),CURRENT_USER()");
                testRs.next();

                assertEquals("wl5735user", testRs.getString(1).split("@")[0]);
                assertEquals("wl5735user", testRs.getString(2).split("@")[0]);

            } finally {
                if (testRs != null) {
                    testRs.close();
                }
                if (testSt != null) {
                    testSt.close();
                }
                if (testConn != null) {
                    testConn.getWarnings();
                    testConn.close();
                }
            }

        } finally {
            if (install_plugin_in_runtime) {
                this.stmt.executeUpdate("UNINSTALL PLUGIN cleartext_plugin_server");
            }
        }
    }

    /**
     * This test requires two server instances:
     * 1) main test server pointed by com.mysql.cj.testsuite.url variable
     * configured without RSA encryption support (sha256_password_private_key_path,
     * sha256_password_public_key_path, caching_sha2_password_private_key_path and
     * caching_sha2_password_public_key_path config options are unset).
     * 2) additional server instance pointed by com.mysql.cj.testsuite.url.openssl
     * variable configured with default-authentication-plugin=sha256_password
     * and RSA encryption enabled.
     * 
     * To run this test please add this variable to ant call:
     * -Dcom.mysql.cj.testsuite.url.openssl=jdbc:mysql://localhost:3307/test?user=root&password=pwd
     * 
     * @throws Exception
     */
    public void testSha256PasswordPlugin() throws Exception {
        String trustStorePath = "src/test/config/ssl-test-certs/ca-truststore";
        System.setProperty("javax.net.ssl.keyStore", trustStorePath);
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStore", trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        /*
         * test against server without RSA support
         */
        if (versionMeetsMinimum(5, 6, 5)) {
            if (!pluginIsActive(this.stmt, "sha256_password")) {
                fail("sha256_password required to run this test");
            }

            // newer GPL servers, like 8.0.4+, are using OpenSSL and can use RSA encryption, while old ones compiled with yaSSL cannot
            boolean gplWithRSA = allowsRsa(this.stmt);

            try {
                if (!versionMeetsMinimum(8, 0, 5)) {
                    this.stmt.executeUpdate("SET @current_old_passwords = @@global.old_passwords");
                }
                createUser("'wl5602user'@'%'", "identified WITH sha256_password");
                this.stmt.executeUpdate("grant all on *.* to 'wl5602user'@'%'");
                createUser("'wl5602nopassword'@'%'", "identified WITH sha256_password");
                this.stmt.executeUpdate("grant all on *.* to 'wl5602nopassword'@'%'");
                if (!versionMeetsMinimum(8, 0, 5)) {
                    this.stmt.executeUpdate("SET GLOBAL old_passwords= 2");
                    this.stmt.executeUpdate("SET SESSION old_passwords= 2");
                }
                this.stmt.executeUpdate(versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'wl5602user'@'%' IDENTIFIED BY 'pwd'"
                        : "set password for 'wl5602user'@'%' = PASSWORD('pwd')");
                this.stmt.executeUpdate("flush privileges");

                final Properties propsNoRetrieval = new Properties();
                propsNoRetrieval.setProperty(PropertyKey.USER.getKeyName(), "wl5602user");
                propsNoRetrieval.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                final Properties propsNoRetrievalNoPassword = new Properties();
                propsNoRetrievalNoPassword.setProperty(PropertyKey.USER.getKeyName(), "wl5602nopassword");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.PASSWORD.getKeyName(), "");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                final Properties propsAllowRetrieval = new Properties();
                propsAllowRetrieval.setProperty(PropertyKey.USER.getKeyName(), "wl5602user");
                propsAllowRetrieval.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");
                propsAllowRetrieval.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                final Properties propsAllowRetrievalNoPassword = new Properties();
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.USER.getKeyName(), "wl5602nopassword");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.PASSWORD.getKeyName(), "");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                // 1. without SSL
                // SQLException expected due to server doesn't recognize Public Key Retrieval packet
                assertThrows(SQLException.class, "Public Key Retrieval is not allowed", new Callable<Void>() {
                    public Void call() throws Exception {
                        getConnectionWithProps(propsNoRetrieval);
                        return null;
                    }
                });

                if (gplWithRSA) {
                    assertCurrentUser(null, propsAllowRetrieval, "wl5602user", false);
                } else {
                    assertThrows(SQLException.class, "Access denied for user 'wl5602user'.*", new Callable<Void>() {
                        public Void call() throws Exception {
                            getConnectionWithProps(propsAllowRetrieval);
                            return null;
                        }
                    });
                }
                assertCurrentUser(null, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(null, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 2. with serverRSAPublicKeyFile specified
                // SQLException expected due to server doesn't recognize RSA encrypted payload
                propsNoRetrieval.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                propsAllowRetrieval.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");

                assertThrows(SQLException.class, "Access denied for user 'wl5602user'.*", new Callable<Void>() {
                    public Void call() throws Exception {
                        getConnectionWithProps(propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Access denied for user 'wl5602user'.*", new Callable<Void>() {
                    public Void call() throws Exception {
                        getConnectionWithProps(propsAllowRetrieval);
                        return null;
                    }
                });

                assertCurrentUser(null, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(null, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 3. over SSL
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");

                assertCurrentUser(null, propsNoRetrieval, "wl5602user", true);
                assertCurrentUser(null, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(null, propsAllowRetrieval, "wl5602user", true);
                assertCurrentUser(null, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // over SSL with client-default Sha256PasswordPlugin
                propsNoRetrieval.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), Sha256PasswordPlugin.class.getName());
                propsNoRetrievalNoPassword.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), Sha256PasswordPlugin.class.getName());
                propsAllowRetrieval.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), Sha256PasswordPlugin.class.getName());
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), Sha256PasswordPlugin.class.getName());

                assertCurrentUser(null, propsNoRetrieval, "wl5602user", true);
                assertCurrentUser(null, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(null, propsAllowRetrieval, "wl5602user", true);
                assertCurrentUser(null, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

            } finally {
                this.stmt.executeUpdate("flush privileges");
                if (!versionMeetsMinimum(8, 0, 5)) {
                    this.stmt.executeUpdate("SET GLOBAL old_passwords = @current_old_passwords");
                }
            }
        }

        /*
         * test against server with RSA support
         */
        if (this.sha256Conn != null && ((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 6, 5)) {

            if (!pluginIsActive(this.sha256Stmt, "sha256_password")) {
                fail("sha256_password required to run this test");
            }
            if (!allowsRsa(this.sha256Stmt)) {
                fail("RSA encryption must be enabled on " + sha256Url + " to run this test");
            }

            try {
                // create user with long password and sha256_password auth
                if (!((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET @current_old_passwords = @@global.old_passwords");
                }
                createUser(this.sha256Stmt, "'wl5602user'@'%'", "identified WITH sha256_password");
                this.sha256Stmt.executeUpdate("grant all on *.* to 'wl5602user'@'%'");
                createUser(this.sha256Stmt, "'wl5602nopassword'@'%'", "identified WITH sha256_password");
                this.sha256Stmt.executeUpdate("grant all on *.* to 'wl5602nopassword'@'%'");
                if (!((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET GLOBAL old_passwords= 2");
                    this.sha256Stmt.executeUpdate("SET SESSION old_passwords= 2");
                }
                this.sha256Stmt.executeUpdate(
                        ((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'wl5602user'@'%' IDENTIFIED BY 'pwd'"
                                : "set password for 'wl5602user'@'%' = PASSWORD('pwd')");
                this.sha256Stmt.executeUpdate("flush privileges");

                final Properties propsNoRetrieval = new Properties();
                propsNoRetrieval.setProperty(PropertyKey.USER.getKeyName(), "wl5602user");
                propsNoRetrieval.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");

                final Properties propsNoRetrievalNoPassword = new Properties();
                propsNoRetrievalNoPassword.setProperty(PropertyKey.USER.getKeyName(), "wl5602nopassword");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.PASSWORD.getKeyName(), "");

                final Properties propsAllowRetrieval = new Properties();
                propsAllowRetrieval.setProperty(PropertyKey.USER.getKeyName(), "wl5602user");
                propsAllowRetrieval.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");
                propsAllowRetrieval.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

                final Properties propsAllowRetrievalNoPassword = new Properties();
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.USER.getKeyName(), "wl5602nopassword");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.PASSWORD.getKeyName(), "");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

                // 1. with client-default MysqlNativePasswordPlugin
                propsNoRetrieval.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), MysqlNativePasswordPlugin.class.getName());
                propsAllowRetrieval.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), MysqlNativePasswordPlugin.class.getName());

                // 1.1. RSA
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                assertThrows(SQLException.class, "Public Key Retrieval is not allowed", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrieval);
                        return null;
                    }
                });

                assertCurrentUser(sha256Url, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(sha256Url, propsAllowRetrieval, "wl5602user", false);
                assertCurrentUser(sha256Url, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 1.2. over SSL
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");

                assertCurrentUser(sha256Url, propsNoRetrieval, "wl5602user", true);
                assertCurrentUser(sha256Url, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(sha256Url, propsAllowRetrieval, "wl5602user", true);
                assertCurrentUser(sha256Url, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 2. with client-default Sha256PasswordPlugin
                propsNoRetrieval.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), Sha256PasswordPlugin.class.getName());
                propsNoRetrievalNoPassword.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), Sha256PasswordPlugin.class.getName());
                propsAllowRetrieval.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), Sha256PasswordPlugin.class.getName());
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), Sha256PasswordPlugin.class.getName());

                // 2.1. RSA
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                assertThrows(SQLException.class, "Public Key Retrieval is not allowed", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrieval);
                        return null;
                    }
                });

                assertCurrentUser(sha256Url, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(sha256Url, propsAllowRetrieval, "wl5602user", false);
                assertCurrentUser(sha256Url, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 2.2. over SSL
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");

                assertCurrentUser(sha256Url, propsNoRetrieval, "wl5602user", true);
                assertCurrentUser(sha256Url, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(sha256Url, propsAllowRetrieval, "wl5602user", false);
                assertCurrentUser(sha256Url, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 3. with serverRSAPublicKeyFile specified
                propsNoRetrieval.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                propsAllowRetrieval.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");

                // 3.1. RSA
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                assertCurrentUser(sha256Url, propsNoRetrieval, "wl5602user", false);
                assertCurrentUser(sha256Url, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(sha256Url, propsAllowRetrieval, "wl5602user", false);
                assertCurrentUser(sha256Url, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 3.2. Runtime setServerRSAPublicKeyFile must be denied
                final Connection c2 = getConnectionWithProps(sha256Url, propsNoRetrieval);
                assertThrows(PropertyNotModifiableException.class, "Dynamic change of ''serverRSAPublicKeyFile'' is not allowed.", new Callable<Void>() {
                    public Void call() throws Exception {
                        ((JdbcConnection) c2).getPropertySet().getProperty(PropertyKey.serverRSAPublicKeyFile)
                                .setValue("src/test/config/ssl-test-certs/mykey.pub");
                        return null;
                    }
                });
                c2.close();

                // 3.3. Runtime setAllowPublicKeyRetrieval must be denied
                final Connection c3 = getConnectionWithProps(sha256Url, propsNoRetrieval);
                assertThrows(PropertyNotModifiableException.class, "Dynamic change of ''allowPublicKeyRetrieval'' is not allowed.", new Callable<Void>() {
                    public Void call() throws Exception {
                        ((JdbcConnection) c3).getPropertySet().getProperty(PropertyKey.allowPublicKeyRetrieval).setValue(true);
                        return null;
                    }
                });
                c3.close();

                // 3.4. over SSL
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");

                assertCurrentUser(sha256Url, propsNoRetrieval, "wl5602user", true);
                assertCurrentUser(sha256Url, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(sha256Url, propsAllowRetrieval, "wl5602user", true);
                assertCurrentUser(sha256Url, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 4. with wrong serverRSAPublicKeyFile specified
                propsNoRetrieval.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "unexistant/dummy.pub");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "unexistant/dummy.pub");
                propsAllowRetrieval.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "unexistant/dummy.pub");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "unexistant/dummy.pub");

                // 4.1. RSA
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                propsNoRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                propsAllowRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrievalNoPassword);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrievalNoPassword);
                        return null;
                    }
                });

                propsNoRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrievalNoPassword);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrievalNoPassword);
                        return null;
                    }
                });

                // 4.2. over SSL
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");

                propsNoRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                propsAllowRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrievalNoPassword);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrievalNoPassword);
                        return null;
                    }
                });

                propsNoRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrievalNoPassword);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrievalNoPassword);
                        return null;
                    }
                });

            } finally {
                if (!((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET GLOBAL old_passwords = @current_old_passwords");
                }
            }

        }
    }

    private void assertCurrentUser(String url, Properties props, String expectedUser, boolean sslRequired) throws SQLException {
        Connection connection = url == null ? getConnectionWithProps(props) : getConnectionWithProps(url, props);
        if (sslRequired) {
            assertTrue("SSL connection isn't actually established!", ((MysqlConnection) connection).getSession().isSSLEstablished());
        }
        Statement st = connection.createStatement();
        ResultSet rset = st.executeQuery("select USER(),CURRENT_USER()");
        rset.next();
        assertEquals(expectedUser, rset.getString(1).split("@")[0]);
        assertEquals(expectedUser, rset.getString(2).split("@")[0]);
        connection.close();
    }

    private boolean pluginIsActive(Statement st, String plugin) throws SQLException {
        ResultSet rset = st.executeQuery("select (PLUGIN_STATUS='ACTIVE') as `TRUE` from INFORMATION_SCHEMA.PLUGINS where PLUGIN_NAME='" + plugin + "'");
        boolean pluginIsActive = false;
        if (rset.next()) {
            pluginIsActive = rset.getBoolean(1);
        }
        return pluginIsActive;
    }

    private boolean allowsRsa(Statement st) throws SQLException {
        boolean allowsRSA = false;
        ResultSet rset = st.executeQuery("SHOW STATUS LIKE 'Rsa_public_key'");
        if (rset.next()) {
            String key = rset.getString(2);
            if (key != null) {
                String value = rset.getString(2);
                allowsRSA = (value != null && value.length() > 0);
            }
        }
        return allowsRSA;
    }

    public void testBug36662() throws Exception {

        try {
            String tz1 = TimeUtil.getCanonicalTimezone("MEST", null);
            assertNotNull(tz1);
        } catch (Exception e1) {
            String mes1 = e1.getMessage();
            mes1 = mes1.substring(mes1.lastIndexOf("The timezones that 'MEST' maps to are:") + 39);
            try {
                String tz2 = TimeUtil.getCanonicalTimezone("CEST", null);
                assertEquals(mes1, tz2);
            } catch (Exception e2) {
                String mes2 = e2.getMessage();
                mes2 = mes2.substring(mes2.lastIndexOf("The timezones that 'CEST' maps to are:") + 39);
                assertEquals(mes1, mes2);
            }
        }
    }

    public void testBug37931() throws Exception {

        Connection _conn = null;
        Properties props = new Properties();
        props.setProperty(PropertyKey.characterSetResults.getKeyName(), "ISO88591");

        try {
            _conn = getConnectionWithProps(props);
            assertTrue("This point should not be reached.", false);
        } catch (Exception e) {
            assertEquals("Can't map ISO88591 given for characterSetResults to a supported MySQL encoding.", e.getMessage());
        } finally {
            if (_conn != null) {
                _conn.close();
            }
        }

        props.setProperty(PropertyKey.characterSetResults.getKeyName(), "null");

        try {
            _conn = getConnectionWithProps(props);

            Statement _stmt = _conn.createStatement();
            ResultSet _rs = _stmt.executeQuery("show variables where variable_name='character_set_results'");
            if (_rs.next()) {
                String res = _rs.getString(2);
                if (res == null || "NULL".equalsIgnoreCase(res) || res.length() == 0) {
                    assertTrue(true);
                } else {
                    assertTrue(false);
                }
            }
        } finally {
            if (_conn != null) {
                _conn.close();
            }
        }
    }

    public void testBug64205() throws Exception {
        Properties props = getPropertiesFromTestsuiteUrl();
        String dbname = props.getProperty(PropertyKey.DBNAME.getKeyName());
        if (dbname == null) {
            assertTrue("No database selected", false);
        }

        props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "EUC_JP");

        Connection testConn = null;
        Statement testSt = null;
        ResultSet testRs = null;
        try {
            testConn = getConnectionWithProps(props);
            testSt = testConn.createStatement();
            testRs = testSt.executeQuery("SELECT * FROM `" + dbname + "`.`\u307b\u3052\u307b\u3052`");
        } catch (SQLException e1) {
            if (e1.getClass().getName().endsWith("SQLSyntaxErrorException")) {
                assertEquals("Table '" + dbname + ".\u307B\u3052\u307b\u3052' doesn't exist", e1.getMessage());
            } else if (e1.getErrorCode() == MysqlErrorNumbers.ER_FILE_NOT_FOUND) {
                // this could happen on Windows with 5.5 and 5.6 servers where BUG#14642248 exists
                assertTrue(e1.getMessage().contains("Can't find file"));
            } else {
                throw e1;
            }

            testSt.close();
            testConn.close();

            try {
                props.setProperty(PropertyKey.characterSetResults.getKeyName(), "SJIS");
                testConn = getConnectionWithProps(props);
                testSt = testConn.createStatement();
                testSt.execute("SET lc_messages = 'ru_RU'");
                testRs = testSt.executeQuery("SELECT * FROM `" + dbname + "`.`\u307b\u3052\u307b\u3052`");
            } catch (SQLException e2) {
                if (e2.getClass().getName().endsWith("SQLSyntaxErrorException")) {
                    assertEquals("\u0422\u0430\u0431\u043b\u0438\u0446\u0430 '" + dbname
                            + ".\u307b\u3052\u307b\u3052' \u043d\u0435 \u0441\u0443\u0449\u0435\u0441\u0442\u0432\u0443\u0435\u0442", e2.getMessage());
                } else if (e2.getErrorCode() == MysqlErrorNumbers.ER_FILE_NOT_FOUND) {
                    // this could happen on Windows with 5.5 and 5.6 servers where BUG#14642248 exists
                    assertTrue("File not found error message should be russian but is this one: " + e2.getMessage(),
                            e2.getMessage().indexOf("\u0444\u0430\u0439\u043b") > -1);
                } else {
                    throw e2;
                }
            }

        } finally {
            if (testRs != null) {
                testRs.close();
            }
            if (testSt != null) {
                testSt.close();
            }
            if (testConn != null) {
                testConn.close();
            }
        }

        // also test with explicit characterSetResults and cacheServerConfiguration
        try {
            props.setProperty(PropertyKey.characterSetResults.getKeyName(), "EUC_JP");
            props.setProperty(PropertyKey.cacheServerConfiguration.getKeyName(), "true");
            testConn = getConnectionWithProps(props);
            testSt = testConn.createStatement();
            testRs = testSt.executeQuery("SELECT * FROM `" + dbname + "`.`\u307b\u3052\u307b\u3052`");
            fail("Exception should be thrown for attemping to query non-existing table");
        } catch (SQLException e1) {
            if (e1.getClass().getName().endsWith("SQLSyntaxErrorException")) {
                assertEquals("Table '" + dbname + ".\u307B\u3052\u307b\u3052' doesn't exist", e1.getMessage());
            } else if (e1.getErrorCode() == MysqlErrorNumbers.ER_FILE_NOT_FOUND) {
                // this could happen on Windows with 5.5 and 5.6 servers where BUG#14642248 exists
                assertTrue(e1.getMessage().contains("Can't find file"));
            } else {
                throw e1;
            }
        } finally {
            testConn.close();
        }
        props.remove(PropertyKey.cacheServerConfiguration.getKeyName());

        // Error messages may also be received after the handshake but before connection initialization is complete. This tests the interpretation of
        // errors thrown during this time window using a SatementInterceptor that throws an Exception while setting the session variables.
        // Start by getting the Latin1 version of the error to compare later.
        String latin1ErrorMsg = "";
        int latin1ErrorLen = 0;
        try {
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "Latin1");
            props.setProperty(PropertyKey.characterSetResults.getKeyName(), "Latin1");
            props.setProperty(PropertyKey.sessionVariables.getKeyName(), "lc_messages=ru_RU");
            props.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestBug64205QueryInterceptor.class.getName());
            testConn = getConnectionWithProps(props);
            fail("Exception should be trown for syntax error, caused by the exception interceptor");
        } catch (Exception e) {
            latin1ErrorMsg = e.getMessage();
            latin1ErrorLen = latin1ErrorMsg.length();
        }
        // Now compare with results when using a proper encoding.
        try {
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "EUC_JP");
            props.setProperty(PropertyKey.characterSetResults.getKeyName(), "EUC_JP");
            props.setProperty(PropertyKey.sessionVariables.getKeyName(), "lc_messages=ru_RU");
            props.setProperty(PropertyKey.queryInterceptors.getKeyName(), TestBug64205QueryInterceptor.class.getName());
            testConn = getConnectionWithProps(props);
            fail("Exception should be trown for syntax error, caused by the exception interceptor");
        } catch (SQLException e) {
            // There should be the Russian version of this error message, correctly encoded. A mis-interpretation, e.g. decoding as latin1, would return a
            // wrong message with the wrong size.
            assertEquals(29 + dbname.length(), e.getMessage().length());
            assertFalse(latin1ErrorMsg.equals(e.getMessage()));
            assertFalse(latin1ErrorLen == e.getMessage().length());
        } finally {
            testConn.close();
        }

    }

    public static class TestBug64205QueryInterceptor extends BaseQueryInterceptor {
        private JdbcConnection connection;

        @Override
        public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
            this.connection = (JdbcConnection) conn;
            return super.init(conn, props, log);
        }

        @Override
        public <M extends Message> M postProcess(M queryPacket, M originalResponsePacket) {
            String sql = StringUtils.toString(queryPacket.getByteBuffer(), 1, (queryPacket.getPosition() - 1));
            if (sql.contains("lc_messages=ru_RU")) {
                try {
                    this.connection.createStatement()
                            .executeQuery("SELECT * FROM `"
                                    + (this.connection.getPropertySet().<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm)
                                            .getValue() == DatabaseTerm.SCHEMA ? this.connection.getSchema() : this.connection.getCatalog())
                                    + "`.`\u307b\u3052\u307b\u3052`");
                } catch (Exception e) {
                    throw ExceptionFactory.createException(e.getMessage(), e);
                }
            }
            return originalResponsePacket;
        }
    }

    public void testIsLocal() throws Exception {
        boolean normalState = ((ConnectionImpl) this.conn).isServerLocal();

        if (normalState) {
            Properties props = new Properties();
            props.setProperty(PropertyKey.socketFactory.getKeyName(), NonLocalSocketFactory.class.getName());
            props.setProperty(PropertyKey.useSSL.getKeyName(), "false");

            boolean isLocal = ((ConnectionImpl) getConnectionWithProps(props)).isServerLocal();

            assertFalse(isLocal == normalState);
        }
    }

    /**
     * Tests fix for BUG#57662, Incorrect Query Duration When useNanosForElapsedTime Enabled
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug57662() throws Exception {

        createTable("testBug57662", "(x VARCHAR(10) NOT NULL DEFAULT '')");
        Connection conn_is = null;
        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.profileSQL.getKeyName(), "true");
            props.setProperty(PropertyKey.useNanosForElapsedTime.getKeyName(), "true");
            props.setProperty(PropertyKey.logger.getKeyName(), "testsuite.simple.TestBug57662Logger");
            conn_is = getConnectionWithProps(props);
            this.rs = conn_is.getMetaData().getColumns(null, null, "testBug57662", "%");

            assertFalse(((testsuite.simple.TestBug57662Logger) ((ConnectionImpl) conn_is).getSession().getLog()).hasNegativeDurations);

        } finally {
            if (conn_is != null) {
                conn_is.close();
            }
        }

    }

    public void testBug14563127() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), ForcedLoadBalanceStrategy.class.getName());
        props.setProperty(PropertyKey.loadBalanceBlacklistTimeout.getKeyName(), "5000");
        props.setProperty(PropertyKey.loadBalancePingTimeout.getKeyName(), "100");
        props.setProperty(PropertyKey.loadBalanceValidateConnectionOnSwapServer.getKeyName(), "true");

        String portNumber = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.PORT.getKeyName());

        if (portNumber == null) {
            portNumber = "3306";
        }

        ForcedLoadBalanceStrategy.forceFutureServer("first:" + portNumber, -1);
        Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);
        conn2.setAutoCommit(false);
        conn2.createStatement().execute("SELECT 1");

        // make sure second is added to active connections cache:
        ForcedLoadBalanceStrategy.forceFutureServer("second:" + portNumber, -1);
        conn2.commit();

        // switch back to first:
        ForcedLoadBalanceStrategy.forceFutureServer("first:" + portNumber, -1);
        conn2.commit();

        // kill second while still in cache:
        UnreliableSocketFactory.downHost("second");

        // force second host to be selected next time:
        ForcedLoadBalanceStrategy.forceFutureServer("second:" + portNumber, 1);

        try {
            conn2.commit(); // will be on second after this
            assertTrue("Connection should not be closed", !conn2.isClosed());
        } catch (SQLException e) {
            fail("Should not error because failure to select another server.");
        }
        conn2.close();

    }

    /**
     * Tests fix for BUG#11237 useCompression=true and LOAD DATA LOCAL INFILE SQL Command
     * 
     * @throws Exception
     *             if any errors occur
     */
    public void testBug11237() throws Exception {
        this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'max_allowed_packet'");
        this.rs.next();
        if (this.rs.getInt(2) < 4 + 1024 * 1024 * 16 - 1) {
            fail("You need to increase max_allowed_packet to at least " + (4 + 1024 * 1024 * 16 - 1) + " before running this test!");
        }

        int requiredSize = 1024 * 1024 * 300;
        int fieldLength = 1023;
        int loops = requiredSize / 2 / (fieldLength + 1);

        File testFile = File.createTempFile("cj-testloaddata", ".dat");
        testFile.deleteOnExit();

        // TODO: following cleanup doesn't work correctly during concurrent execution of testsuite 
        // cleanupTempFiles(testFile, "cj-testloaddata");

        BufferedOutputStream bOut = new BufferedOutputStream(new FileOutputStream(testFile));

        for (int i = 0; i < loops; i++) {
            for (int j = 0; j < fieldLength; j++) {
                bOut.write("a".getBytes()[0]);
            }
            bOut.write("\t".getBytes()[0]);
            for (int j = 0; j < fieldLength; j++) {
                bOut.write("b".getBytes()[0]);
            }
            bOut.write("\n".getBytes()[0]);
        }

        bOut.flush();
        bOut.close();

        createTable("testBug11237", "(field1 VARCHAR(1024), field2 VARCHAR(1024))");

        StringBuilder fileNameBuf = null;

        if (File.separatorChar == '\\') {
            fileNameBuf = new StringBuilder();

            String fileName = testFile.getAbsolutePath();
            int fileNameLength = fileName.length();

            for (int i = 0; i < fileNameLength; i++) {
                char c = fileName.charAt(i);

                if (c == '\\') {
                    fileNameBuf.append("/");
                } else {
                    fileNameBuf.append(c);
                }
            }
        } else {
            fileNameBuf = new StringBuilder(testFile.getAbsolutePath());
        }

        Properties props = new Properties();
        props.setProperty(PropertyKey.allowLoadLocalInfile.getKeyName(), "true");
        props.setProperty(PropertyKey.useCompression.getKeyName(), "true");
        Connection conn1 = getConnectionWithProps(props);
        Statement stmt1 = conn1.createStatement();

        int updateCount = stmt1.executeUpdate("LOAD DATA LOCAL INFILE '" + fileNameBuf.toString() + "' INTO TABLE testBug11237 CHARACTER SET "
                + CharsetMapping.getMysqlCharsetForJavaEncoding(
                        ((MysqlConnection) this.conn).getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue(),
                        ((JdbcConnection) conn1).getServerVersion()));

        assertTrue(updateCount == loops);
    }

    public void testStackOverflowOnMissingInterceptor() throws Exception {
        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.queryInterceptors.getKeyName(), "fooBarBaz");

            getConnectionWithProps(props).close();
        } catch (Exception e) {
        }
    }

    public void testExpiredPassword() throws Exception {
        if (!versionMeetsMinimum(5, 6, 10)) {
            return;
        }
        Connection testConn = null;
        Statement testSt = null;
        ResultSet testRs = null;

        Properties urlProps = getPropertiesFromTestsuiteUrl();
        String dbname = urlProps.getProperty(PropertyKey.DBNAME.getKeyName());

        try {

            createUser("'must_change1'@'%'", "IDENTIFIED BY 'aha'");
            this.stmt.executeUpdate("grant all on `" + dbname + "`.* to 'must_change1'@'%'");
            createUser("'must_change2'@'%'", "IDENTIFIED BY 'aha'");
            this.stmt.executeUpdate("grant all on `" + dbname + "`.* to 'must_change2'@'%'");

            // TODO workaround for Bug#77732, should be fixed in 5.7.9
            if (versionMeetsMinimum(5, 7, 6) && !versionMeetsMinimum(8, 0, 5)) {
                this.stmt.executeUpdate("GRANT SELECT ON `performance_schema`.`session_variables` TO 'must_change1'@'%'");
                this.stmt.executeUpdate("GRANT SELECT ON `performance_schema`.`session_variables` TO 'must_change2'@'%'");
            }

            this.stmt.executeUpdate(versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'must_change1'@'%', 'must_change2'@'%' PASSWORD EXPIRE"
                    : "ALTER USER 'must_change1'@'%' PASSWORD EXPIRE, 'must_change2'@'%' PASSWORD EXPIRE");

            Properties props = new Properties();

            // ALTER USER can be prepared as of 5.6.8 (BUG#14646014)
            if (versionMeetsMinimum(5, 6, 8)) {
                props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
                testConn = getConnectionWithProps(props);

                this.pstmt = testConn.prepareStatement(versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'must_change1'@'%', 'must_change2'@'%' PASSWORD EXPIRE"
                        : "ALTER USER 'must_change1'@'%' PASSWORD EXPIRE, 'must_change2'@'%' PASSWORD EXPIRE");
                this.pstmt.executeUpdate();
                this.pstmt.close();

                this.pstmt = testConn.prepareStatement(versionMeetsMinimum(5, 7, 6) ? "ALTER USER ?, 'must_change2'@'%' PASSWORD EXPIRE"
                        : "ALTER USER ? PASSWORD EXPIRE, 'must_change2'@'%' PASSWORD EXPIRE");
                this.pstmt.setString(1, "must_change1");
                this.pstmt.executeUpdate();
                this.pstmt.close();

                testConn.close();
            }

            props.setProperty(PropertyKey.USER.getKeyName(), "must_change1");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "aha");

            try {
                testConn = getConnectionWithProps(props);
                fail("SQLException expected due to password expired");
            } catch (PasswordExpiredException | ClosedOnExpiredPasswordException | SQLException e1) {

                if ((e1 instanceof SQLException && (((SQLException) e1).getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD
                        || ((SQLException) e1).getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD_LOGIN)) || e1 instanceof PasswordExpiredException
                        || e1 instanceof ClosedOnExpiredPasswordException) {

                    props.setProperty(PropertyKey.disconnectOnExpiredPasswords.getKeyName(), "false");
                    try {
                        testConn = getConnectionWithProps(props);
                        testSt = testConn.createStatement();
                        testRs = testSt.executeQuery("SHOW VARIABLES LIKE 'disconnect_on_expired_password'");
                        testRs.close();
                        fail("SQLException expected due to password expired");

                    } catch (PasswordExpiredException | ClosedOnExpiredPasswordException | SQLException e3) {
                        if (e3 instanceof SQLException && (((SQLException) e3).getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD_LOGIN)
                                || e3 instanceof ClosedOnExpiredPasswordException) {
                            testSt.close();
                            testConn = getConnectionWithProps(props);
                            testSt = testConn.createStatement();
                        }
                        if (testSt == null) {
                            testSt = testConn.createStatement();
                        }
                        testSt.executeUpdate(versionMeetsMinimum(5, 7, 6) ? "ALTER USER USER() IDENTIFIED BY 'newpwd'" : "SET PASSWORD = PASSWORD('newpwd')");
                        testConn.close();

                        props.setProperty(PropertyKey.USER.getKeyName(), "must_change1");
                        props.setProperty(PropertyKey.PASSWORD.getKeyName(), "newpwd");
                        props.setProperty(PropertyKey.disconnectOnExpiredPasswords.getKeyName(), "true");
                        testConn = getConnectionWithProps(props);
                        testSt = testConn.createStatement();
                        testRs = testSt.executeQuery("SHOW VARIABLES LIKE 'disconnect_on_expired_password'");
                        assertTrue(testRs.next());

                        // change user
                        try {
                            ((JdbcConnection) testConn).changeUser("must_change2", "aha");
                            fail("SQLException expected due to password expired");

                        } catch (PasswordExpiredException | ClosedOnExpiredPasswordException | SQLException e4) {
                            if (e4 instanceof SQLException
                                    && (((SQLException) e4).getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD
                                            || ((SQLException) e4).getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD_LOGIN)
                                    || e4 instanceof PasswordExpiredException || e4 instanceof ClosedOnExpiredPasswordException) {
                                props.setProperty(PropertyKey.disconnectOnExpiredPasswords.getKeyName(), "false");
                                testConn = getConnectionWithProps(props);

                                try {
                                    ((JdbcConnection) testConn).changeUser("must_change2", "aha");
                                    testSt = testConn.createStatement();
                                    testRs = testSt.executeQuery("SHOW VARIABLES LIKE 'disconnect_on_expired_password'");
                                    fail("SQLException expected due to password expired");

                                } catch (PasswordExpiredException | ClosedOnExpiredPasswordException | SQLException e5) {
                                    if (e5 instanceof SQLException && ((SQLException) e5).getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD_LOGIN
                                            || e5 instanceof ClosedOnExpiredPasswordException) {
                                        testConn = getConnectionWithProps(props);
                                        testSt = testConn.createStatement();
                                    }
                                    testSt = testConn.createStatement();
                                    testSt.executeUpdate(
                                            versionMeetsMinimum(5, 7, 6) ? "ALTER USER USER() IDENTIFIED BY 'newpwd'" : "SET PASSWORD = PASSWORD('newpwd')");
                                    testConn.close();

                                    props.setProperty(PropertyKey.USER.getKeyName(), "must_change2");
                                    props.setProperty(PropertyKey.PASSWORD.getKeyName(), "newpwd");
                                    props.setProperty(PropertyKey.disconnectOnExpiredPasswords.getKeyName(), "true");
                                    testConn = getConnectionWithProps(props);
                                    testSt = testConn.createStatement();
                                    testRs = testSt.executeQuery("SHOW VARIABLES LIKE 'disconnect_on_expired_password'");
                                    assertTrue(testRs.next());

                                }

                            } else {
                                throw e4;
                            }
                        }

                    }

                } else {
                    throw e1;
                }

            }

        } finally {
            if (testRs != null) {
                testRs.close();
            }
            if (testSt != null) {
                testSt.close();
            }
            if (testConn != null) {
                testConn.close();
            }
        }

    }

    /**
     * Tests connection attributes
     * 
     * @throws Exception
     */
    public void testConnectionAttributes() throws Exception {
        if (versionMeetsMinimum(5, 6)) {
            testConnectionAttributes(dbUrl);
        }
        if (this.sha256Conn != null && ((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 6, 5)) {
            testConnectionAttributes(sha256Url);
        }

    }

    private void testConnectionAttributes(String url) throws Exception {
        if (!versionMeetsMinimum(5, 6)) {
            return;
        }
        Properties props = new Properties();
        props.setProperty(PropertyKey.connectionAttributes.getKeyName(), "first:one,again:two");
        props.setProperty(PropertyKey.USER.getKeyName(), getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.USER.getKeyName()));
        Connection attConn = super.getConnectionWithProps(url, props);
        ResultSet rslt = attConn.createStatement()
                .executeQuery("SELECT * FROM performance_schema.session_connect_attrs WHERE processlist_id = CONNECTION_ID()");
        Map<String, Integer> matchedCounts = new HashMap<>();

        // disabling until standard values are defined and implemented
        // matchedCounts.put("_os", 0);
        // matchedCounts.put("_platform", 0);
        matchedCounts.put("_runtime_version", 0);
        matchedCounts.put("_runtime_vendor", 0);
        matchedCounts.put("_client_version", 0);
        matchedCounts.put("_client_license", 0);
        matchedCounts.put("_client_name", 0);
        matchedCounts.put("first", 0);
        matchedCounts.put("again", 0);

        while (rslt.next()) {
            String key = rslt.getString(2);
            String val = rslt.getString(3);
            if (!matchedCounts.containsKey(key)) {
                fail("Unexpected connection attribute key:  " + key);
            }
            matchedCounts.put(key, matchedCounts.get(key) + 1);
            if (key.equals("_runtime_version")) {
                assertEquals(Constants.JVM_VERSION, val);
            } else if (key.equals("_os")) {
                assertEquals(Constants.OS_NAME, val);
            } else if (key.equals("_platform")) {
                assertEquals(Constants.OS_ARCH, val);
            } else if (key.equals("_runtime_vendor")) {
                assertEquals(Constants.JVM_VENDOR, val);
            } else if (key.equals("_client_version")) {
                assertEquals(Constants.CJ_VERSION, val);
            } else if (key.equals("_client_license")) {
                assertEquals(Constants.CJ_LICENSE, val);
            } else if (key.equals("_client_name")) {
                assertEquals(Constants.CJ_NAME, val);
            } else if (key.equals("first")) {
                assertEquals("one", val);
            } else if (key.equals("again")) {
                assertEquals("two", val);
            }
        }

        rslt.close();
        attConn.close();

        for (String key : matchedCounts.keySet()) {
            if (matchedCounts.get(key) != 1) {
                fail("Incorrect number of entries for key \"" + key + "\": " + matchedCounts.get(key));
            }
        }

        props.setProperty(PropertyKey.connectionAttributes.getKeyName(), "none");
        attConn = super.getConnectionWithProps(url, props);
        rslt = attConn.createStatement().executeQuery("SELECT * FROM performance_schema.session_connect_attrs WHERE processlist_id = CONNECTION_ID()");
        if (rslt.next()) {
            fail("Expected no connection attributes.");
        }

    }

    /**
     * Tests fix for BUG#16224249 - Deadlock on concurrently used LoadBalancedMySQLConnection
     * 
     * @throws Exception
     */
    public void testBug16224249() throws Exception {
        String hostSpec = getEncodedHostPortPairFromTestsuiteUrl();
        int port = getPortFromTestsuiteUrl();

        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        String database = props.getProperty(PropertyKey.DBNAME.getKeyName());
        props.remove(PropertyKey.DBNAME.getKeyName());

        StringBuilder configs = new StringBuilder();
        for (@SuppressWarnings("rawtypes")
        Map.Entry entry : props.entrySet()) {
            configs.append(entry.getKey());
            configs.append("=");
            configs.append(entry.getValue());
            configs.append("&");
        }

        String loadbalanceUrl = String.format("jdbc:mysql:loadbalance://%s,%s/%s?%s", hostSpec, hostSpec, database, configs.toString());
        String failoverUrl = String.format("jdbc:mysql://%s,%s/%s?%s", hostSpec, "127.0.0.1:" + port, database, configs.toString());

        Connection[] loadbalancedconnection = new Connection[] { new NonRegisteringDriver().connect(loadbalanceUrl, null),
                new NonRegisteringDriver().connect(loadbalanceUrl, null), new NonRegisteringDriver().connect(loadbalanceUrl, null) };

        Connection[] failoverconnection = new Connection[] { new NonRegisteringDriver().connect(failoverUrl, null),
                new NonRegisteringDriver().connect(failoverUrl, null), new NonRegisteringDriver().connect(failoverUrl, null) };

        // WebLogic-style test
        Class<?> mysqlCls = null;
        Class<?> jcls = failoverconnection[0].getClass(); // the driver-level connection, a Proxy in this case...
        ClassLoader jcl = jcls.getClassLoader();
        if (jcl != null) {
            mysqlCls = jcl.loadClass(JdbcConnection.class.getName());
        } else {
            mysqlCls = Class.forName(JdbcConnection.class.getName(), true, null);
        }

        if ((mysqlCls != null) && (mysqlCls.isAssignableFrom(jcls))) {
            Method abort = mysqlCls.getMethod("abortInternal", new Class<?>[] {});
            boolean hasAbortMethod = abort != null;
            assertTrue("abortInternal() method should be found for connection class " + jcls, hasAbortMethod);
        } else {
            fail(JdbcConnection.class.getName() + " interface IS NOT ASSIGNABE from connection class " + jcls);
        }
        //-------------

        // Concurrent test
        System.out.println("Warming up");
        for (int i = 0; i < failoverconnection.length; i++) {
            this.stmt = failoverconnection[i].createStatement();
            this.pstmt = failoverconnection[i].prepareStatement("SELECT 1 FROM DUAL");
            for (int j = 0; j < 10000; j++) {
                this.rs = this.pstmt.executeQuery();
                this.rs = this.stmt.executeQuery("SELECT 1 FROM DUAL");
            }
        }

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(12);

        ScheduledFuture<?> f1 = scheduler.schedule(new PollTask(failoverconnection[0], 1), 500, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> f2 = scheduler.schedule(new PollTask(failoverconnection[1], 2), 500, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> f3 = scheduler.schedule(new PollTask(failoverconnection[2], 3), 500, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> f4 = scheduler.schedule(new PollTask(loadbalancedconnection[0], 4), 500, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> f5 = scheduler.schedule(new PollTask(loadbalancedconnection[1], 5), 500, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> f6 = scheduler.schedule(new PollTask(loadbalancedconnection[2], 6), 500, TimeUnit.MILLISECONDS);

        ScheduledFuture<?> f7 = scheduler.schedule(new CancelTask(failoverconnection[0], 7), 600, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> f8 = scheduler.schedule(new CancelTask(failoverconnection[1], 8), 600, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> f9 = scheduler.schedule(new CancelTask(failoverconnection[2], 9), 600, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> f10 = scheduler.schedule(new CancelTask(loadbalancedconnection[0], 10), 600, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> f11 = scheduler.schedule(new CancelTask(loadbalancedconnection[1], 11), 600, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> f12 = scheduler.schedule(new CancelTask(loadbalancedconnection[2], 12), 600, TimeUnit.MILLISECONDS);

        try {
            while (f1.get(5, TimeUnit.SECONDS) != null || f2.get(5, TimeUnit.SECONDS) != null || f3.get(5, TimeUnit.SECONDS) != null
                    || f4.get(5, TimeUnit.SECONDS) != null || f5.get(5, TimeUnit.SECONDS) != null || f6.get(5, TimeUnit.SECONDS) != null
                    || f7.get(5, TimeUnit.SECONDS) != null || f8.get(5, TimeUnit.SECONDS) != null || f9.get(5, TimeUnit.SECONDS) != null
                    || f10.get(5, TimeUnit.SECONDS) != null || f11.get(5, TimeUnit.SECONDS) != null || f12.get(5, TimeUnit.SECONDS) != null) {
                System.out.println("waiting");
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }

        if (this.testServerPrepStmtDeadlockCounter < 12) {
            Map<Thread, StackTraceElement[]> tr = Thread.getAllStackTraces();
            for (StackTraceElement[] el : tr.values()) {
                System.out.println();
                for (StackTraceElement stackTraceElement : el) {
                    System.out.println(stackTraceElement);
                }
            }
        }

        for (int i = 0; i < failoverconnection.length; i++) {
            try {
                this.rs = failoverconnection[i].createStatement().executeQuery("SELECT 1");
            } catch (Exception e1) {
                try {
                    this.rs = failoverconnection[i].createStatement().executeQuery("SELECT 1");
                    fail("Connection should be explicitly closed.");
                } catch (Exception e2) {
                    assertTrue(true);
                }
            }
        }

        scheduler.shutdown();

    }

    /**
     * Tests fix for BUG#68763, ReplicationConnection.isMasterConnection() returns false always
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug68763() throws Exception {
        ReplicationConnection replConn = null;

        replConn = (ReplicationConnection) getMasterSlaveReplicationConnection();
        replConn.setReadOnly(true);
        assertFalse("isMasterConnection() should be false for slave connection", replConn.isMasterConnection());
        replConn.setReadOnly(false);
        assertTrue("isMasterConnection() should be true for master connection", replConn.isMasterConnection());
    }

    /**
     * Tests fix for BUG#68733, ReplicationConnection does not ping all underlying
     * active physical connections to slaves.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug68733() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), ForcedLoadBalanceStrategy.class.getName());
        props.setProperty(PropertyKey.loadBalancePingTimeout.getKeyName(), "100");
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
        props.setProperty(PropertyKey.retriesAllDown.getKeyName(), "1");

        String portNumber = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.PORT.getKeyName());

        if (portNumber == null) {
            portNumber = "3306";
        }

        ForcedLoadBalanceStrategy.forceFutureServer("slave1:" + portNumber, -1);
        // throw Exception if slave2 gets ping
        UnreliableSocketFactory.downHost("slave2");

        ReplicationConnection conn2 = this.getUnreliableReplicationConnection(new String[] { "master", "slave1", "slave2" }, props);
        assertTrue("Is not actually on master!", conn2.isMasterConnection());

        conn2.setAutoCommit(false);

        conn2.commit();
        // go to slaves:
        conn2.setReadOnly(true);

        // should succeed, as slave2 has not yet been activated:
        conn2.createStatement().execute("/* ping */ SELECT 1");
        // allow connections to slave2:
        UnreliableSocketFactory.dontDownHost("slave2");
        // force next re-balance to slave2:
        ForcedLoadBalanceStrategy.forceFutureServer("slave2:" + portNumber, -1);
        // re-balance:
        conn2.commit();
        // down slave1 (active but not selected slave connection):
        UnreliableSocketFactory.downHost("slave1");
        // should succeed, as slave2 is currently selected:
        conn2.createStatement().execute("/* ping */ SELECT 1");

        // make all hosts available
        UnreliableSocketFactory.flushAllStaticData();

        // peg connection to slave2:
        ForcedLoadBalanceStrategy.forceFutureServer("slave2:" + portNumber, -1);
        conn2.commit();

        this.rs = conn2.createStatement().executeQuery("SELECT CONNECTION_ID()");
        this.rs.next();
        int slave2id = this.rs.getInt(1);

        // peg connection to slave1 now:
        ForcedLoadBalanceStrategy.forceFutureServer("slave1:" + portNumber, -1);
        conn2.commit();

        // this is a really hacky way to confirm ping was processed
        // by an inactive load-balanced connection, but we lack COM_PING
        // counters on the server side, and need to create infrastructure
        // to capture what's being sent by the driver separately.

        Thread.sleep(2000);
        conn2.createStatement().execute("/* ping */ SELECT 1");
        this.rs = conn2.createStatement().executeQuery("SELECT time FROM information_schema.processlist WHERE id = " + slave2id);
        this.rs.next();
        assertTrue("Processlist should be less than 2 seconds due to ping", this.rs.getInt(1) < 2);

        // peg connection to slave2:
        ForcedLoadBalanceStrategy.forceFutureServer("slave2:" + portNumber, -1);
        conn2.commit();
        // leaving connection tied to slave2, bring slave2 down and slave1 up:
        UnreliableSocketFactory.downHost("slave2");

        try {
            conn2.createStatement().execute("/* ping */ SELECT 1");
            fail("Expected failure because current slave connection is down.");
        } catch (SQLException e) {
        }

        conn2.close();

        ForcedLoadBalanceStrategy.forceFutureServer("slave1:" + portNumber, -1);
        UnreliableSocketFactory.flushAllStaticData();
        conn2 = this.getUnreliableReplicationConnection(new String[] { "master", "slave1", "slave2" }, props);
        conn2.setAutoCommit(false);
        // go to slaves:
        conn2.setReadOnly(true);

        // on slave1 now:
        conn2.commit();

        ForcedLoadBalanceStrategy.forceFutureServer("slave2:" + portNumber, -1);
        // on slave2 now:
        conn2.commit();

        // disable master:
        UnreliableSocketFactory.downHost("master");

        // ping should succeed, because we're still attached to slaves:
        conn2.createStatement().execute("/* ping */ SELECT 1");

        // bring master back up:
        UnreliableSocketFactory.dontDownHost("master");

        // get back to master, confirm it's recovered:
        conn2.commit();
        conn2.createStatement().execute("/* ping */ SELECT 1");
        try {
            conn2.setReadOnly(false);
        } catch (SQLException e) {
        }

        conn2.commit();

        // take down both slaves:
        UnreliableSocketFactory.downHost("slave1");
        UnreliableSocketFactory.downHost("slave2");

        assertTrue(conn2.isMasterConnection());
        // should succeed, as we're still on master:
        conn2.createStatement().execute("/* ping */ SELECT 1");

        UnreliableSocketFactory.dontDownHost("slave1");
        UnreliableSocketFactory.dontDownHost("slave2");
        UnreliableSocketFactory.downHost("master");

        try {
            conn2.createStatement().execute("/* ping */ SELECT 1");
            fail("should have failed because master is offline");
        } catch (SQLException e) {

        }

        UnreliableSocketFactory.dontDownHost("master");
        conn2.createStatement().execute("SELECT 1");
        // continue on slave2:
        conn2.setReadOnly(true);

        // should succeed, as slave2 is up:
        conn2.createStatement().execute("/* ping */ SELECT 1");

        UnreliableSocketFactory.downHost("slave2");

        try {
            conn2.createStatement().execute("/* ping */ SELECT 1");
            fail("should have failed because slave2 is offline and the active chosen connection.");
        } catch (SQLException e) {
        }

        conn2.close();
    }

    protected int testServerPrepStmtDeadlockCounter = 0;

    class PollTask implements Runnable {

        private Connection c;
        private int num = 0;

        private Statement st1 = null;
        private PreparedStatement pst1 = null;

        PollTask(Connection cn, int n) throws SQLException {
            this.c = cn;
            this.num = n;

            this.st1 = this.c.createStatement();
            this.pst1 = this.c.prepareStatement("SELECT 1 FROM DUAL");
        }

        public void run() {
            System.out.println(this.num + ". Start polling at " + new Date().getTime());
            boolean connectionClosed = false;

            for (int i = 0; i < 20000; i++) {
                try {
                    this.st1.executeQuery("SELECT 1 FROM DUAL").close();
                    this.pst1.executeQuery().close();
                } catch (Exception ex1) {
                    if (!connectionClosed) {
                        System.out.println(this.num + "." + i + " " + ex1.getMessage());
                        connectionClosed = true;
                    } else {
                        break;
                    }
                }
            }

            ConnectionRegressionTest.this.testServerPrepStmtDeadlockCounter++;
            System.out.println(this.num + ". Done!");
        }

    }

    class CancelTask implements Runnable {

        private Connection c;
        private int num = 0;

        CancelTask(Connection cn, int n) throws SQLException {
            this.c = cn;
            this.num = n;
        }

        public void run() {
            System.out.println(this.num + ". Start cancelling at " + new Date().getTime());

            if (Proxy.isProxyClass(this.c.getClass())) {
                try {
                    if (this.num == 7 || this.num == 10) {
                        Proxy.getInvocationHandler(this.c).invoke(this.c, Connection.class.getMethod("close", new Class<?>[] {}), null);
                    } else if (this.num == 8 || this.num == 11) {
                        Proxy.getInvocationHandler(this.c).invoke(this.c, JdbcConnection.class.getMethod("abortInternal", new Class<?>[] {}), null);
                    } else if (this.num == 9 || this.num == 12) {
                        Proxy.getInvocationHandler(this.c).invoke(this.c, JdbcConnection.class.getMethod("abort", new Class<?>[] { Executor.class }),
                                new Object[] { new ThreadPerTaskExecutor() });
                    }

                    ConnectionRegressionTest.this.testServerPrepStmtDeadlockCounter++;
                    System.out.println(this.num + ". Done!");
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }

    }

    class ThreadPerTaskExecutor implements Executor {
        public void execute(Runnable r) {
            new Thread(r).start();
        }
    }

    /**
     * Tests fix for BUG#68400 useCompression=true and connect to server, zip native method cause out of memory
     * 
     * @throws Exception
     *             if any errors occur
     */
    public void testBug68400() throws Exception {
        Field f = com.mysql.cj.jdbc.AbandonedConnectionCleanupThread.class.getDeclaredField("connectionFinalizerPhantomRefs");
        f.setAccessible(true);
        Set<?> connectionTrackingSet = (Set<?>) f.get(com.mysql.cj.jdbc.AbandonedConnectionCleanupThread.class);

        Field referentField = java.lang.ref.Reference.class.getDeclaredField("referent");
        referentField.setAccessible(true);

        createTable("testBug68400", "(x VARCHAR(255) NOT NULL DEFAULT '')");
        String s1 = "a very very very very very very very very very very very very very very very very very very very very very very very very large string "
                + "to ensure compression enabled";
        this.stmt.executeUpdate("insert into testBug68400 values ('" + s1 + "')");

        Properties props = new Properties();
        props.setProperty(PropertyKey.useCompression.getKeyName(), "true");
        props.setProperty(PropertyKey.connectionAttributes.getKeyName(), "testBug68400:true");

        testMemLeakBatch(props, connectionTrackingSet, referentField, 0, 0, s1, "testBug68400:true");
        testMemLeakBatch(props, connectionTrackingSet, referentField, 0, 1, s1, "testBug68400:true");
        testMemLeakBatch(props, connectionTrackingSet, referentField, 0, 2, s1, "testBug68400:true");

        System.out.println("Done.");

    }

    /**
     * @param props
     * @param connectionType
     *            0-ConnectionImpl, 1-LoadBalancedConnection, 2-FailoverConnection, 3-ReplicationConnection
     * @param finType
     *            0 - none, 1 - close(), 2 - abortInternal()
     * @throws Exception
     */
    private void testMemLeakBatch(Properties props, Set<?> connectionTrackingSet, Field referentField, int connectionType, int finType, String s1,
            String attributeValue) throws Exception {

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        int connectionNumber = 0;

        String[] typeNames = new String[] { "ConnectionImpl", "LoadBalancedConnection", "FailoverConnection", "ReplicationConnection" };

        System.out.println("\n" + typeNames[connectionType] + ", " + (finType == 0 ? "nullification" : (finType == 1 ? "close()" : "abortInternal()")));

        // 1. Create 100 connections with "testBug68400:true" attribute
        for (int j = 0; j < 20; j++) {
            switch (connectionType) {
                case 1:
                    //load-balanced connection
                    connection = getLoadBalancedConnection(props);
                    break;
                case 2:
                    //failover connection
                    Properties baseprops = getPropertiesFromTestsuiteUrl();
                    baseprops.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
                    baseprops.setProperty(PropertyKey.socketFactory.getKeyName(), "testsuite.UnreliableSocketFactory");

                    String host = props.getProperty(PropertyKey.HOST.getKeyName());
                    String port = props.getProperty(PropertyKey.PORT.getKeyName());

                    baseprops.remove(PropertyKey.HOST.getKeyName());

                    baseprops.setProperty(PropertyKey.queriesBeforeRetryMaster.getKeyName(), "50");
                    baseprops.setProperty(PropertyKey.maxReconnects.getKeyName(), "1");

                    UnreliableSocketFactory.mapHost("master", host);
                    UnreliableSocketFactory.mapHost("slave", host);

                    baseprops.putAll(props);

                    connection = getConnectionWithProps("jdbc:mysql://master:" + port + ",slave:" + port + "/", baseprops);
                    break;
                case 3:
                    //ReplicationConnection;
                    Properties replProps = new Properties();
                    replProps.putAll(props);
                    replProps.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), ForcedLoadBalanceStrategy.class.getName());
                    replProps.setProperty(PropertyKey.loadBalancePingTimeout.getKeyName(), "100");
                    replProps.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");

                    connection = this.getUnreliableReplicationConnection(new String[] { "master", "slave1", "slave2" }, replProps);

                    break;
                default:
                    connection = getConnectionWithProps(props);
                    break;
            }

            statement = connection.createStatement();
            resultSet = statement.executeQuery("select /* a very very very very very very very very very very very very very very very very very very very "
                    + "very very very very very large string to ensure compression enabled */ x from testBug68400");
            if (resultSet.next()) {
                String s2 = resultSet.getString(1);
                assertEquals(s1, s2);
            }
            if (resultSet != null) {
                resultSet.close();
            }
            statement.close();
            if (finType == 1) {
                connection.close();
            } else if (finType == 2) {
                ((com.mysql.cj.jdbc.JdbcConnection) connection).abortInternal();
            }
            connection = null;
        }

        // 2. Count connections before GC
        System.out.println("SET: " + connectionTrackingSet.size());

        connectionNumber = countTestConnections(connectionTrackingSet, referentField, false, attributeValue);
        System.out.println("Test related connections in SET before GC: " + connectionNumber);

        // 3. Run GC
        Runtime.getRuntime().gc();

        // 4. Sleep to ensure abandoned connection clean up occurred
        Thread.sleep(2000);

        // 5. Count connections before GC
        connectionNumber = countTestConnections(connectionTrackingSet, referentField, true, attributeValue);
        System.out.println("Test related connections in SET after GC: " + connectionNumber);
        System.out.println("SET: " + connectionTrackingSet.size());

        assertEquals(
                "No connection with \"" + attributeValue
                        + "\" connection attribute should exist in AbandonedConnectionCleanupThread.connectionFinalizerPhantomRefs map after GC",
                0, connectionNumber);
    }

    private int countTestConnections(Set<?> connectionTrackingSet, Field referentField, boolean show, String attributValue) throws Exception {
        int connectionNumber = 0;
        for (Object o1 : connectionTrackingSet) {
            com.mysql.cj.jdbc.JdbcConnection ctmp = (com.mysql.cj.jdbc.JdbcConnection) referentField.get(o1);
            String atts = null;
            try {
                if (ctmp != null) {
                    atts = ctmp.getPropertySet().getStringProperty(PropertyKey.connectionAttributes).getValue();
                    if (atts != null && atts.equals(attributValue)) {
                        connectionNumber++;
                        if (show) {
                            System.out.println(ctmp.toString());
                        }
                    }
                }
            } catch (NullPointerException e) {
                System.out.println("NullPointerException: \n" + ctmp + "\n" + atts);
            }
        }
        return connectionNumber;
    }

    /**
     * Tests fix for BUG#17251955, ARRAYINDEXOUTOFBOUNDSEXCEPTION ON LONG MULTI-BYTE DB/USER NAMES
     * 
     * @throws Exception
     */
    public void testBug17251955() throws Exception {
        Connection c1 = null;
        Statement st1 = null;
        Connection c2 = null;
        Properties props = new Properties();
        String url = "jdbc:mysql://" + getEncodedHostPortPairFromTestsuiteUrl();

        try {
            props.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");
            c1 = getConnectionWithProps(props);
            st1 = c1.createStatement();
            createDatabase(st1, "`\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8`");
            createUser(st1, "'\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8'@'%'", "identified by 'msandbox'");
            st1.execute("grant all on `\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8`.* to '\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8'@'%'");

            props = getHostFreePropertiesFromTestsuiteUrl();
            props.setProperty(PropertyKey.USER.getKeyName(), "\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8");
            props.setProperty(PropertyKey.PASSWORD.getKeyName(), "msandbox");
            props.remove(PropertyKey.DBNAME.getKeyName());
            c2 = DriverManager.getConnection(url + "/\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8", props);
            this.rs = c2.createStatement().executeQuery("select 1");
            c2.close();

        } catch (SQLException e) {
            assertFalse("e.getCause() instanceof java.lang.ArrayIndexOutOfBoundsException", e.getCause() instanceof java.lang.ArrayIndexOutOfBoundsException);

            props.setProperty(PropertyKey.USER.getKeyName(), "\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8");
            c2 = DriverManager.getConnection(url + "/\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8", props);
            this.rs = c2.createStatement().executeQuery("select 1");
            c2.close();
        } finally {
            if (c2 != null) {
                c2.close();
            }
            if (st1 != null) {
                dropUser(st1, "'\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8'@'%'");
                dropDatabase(st1, "`\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8`");
                st1.close();
            }
            if (c1 != null) {
                c1.close();
            }
        }
    }

    /**
     * Tests fix for BUG#69506 - XAER_DUPID error code is not returned when a duplicate XID is offered in Java.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug69506() throws Exception {
        MysqlXADataSource dataSource = new MysqlXADataSource();

        dataSource.setUrl(dbUrl);

        XAConnection testXAConn1 = dataSource.getXAConnection();
        XAConnection testXAConn2 = dataSource.getXAConnection();

        Xid duplicateXID = new MysqlXid("1".getBytes(), "1".getBytes(), 1);

        testXAConn1.getXAResource().start(duplicateXID, 0);

        try {
            testXAConn2.getXAResource().start(duplicateXID, 0);
            fail("XAException was expected.");
        } catch (XAException e) {
            assertEquals("Wrong error code retured for duplicated XID.", XAException.XAER_DUPID, e.errorCode);
        }
    }

    /**
     * Tests fix for BUG#69746, ResultSet closed after Statement.close() when dontTrackOpenResources=true
     * active physical connections to slaves.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug69746() throws Exception {
        Connection testConnection;
        Statement testStatement;
        ResultSet testResultSet;

        /*
         * Test explicit closes
         */
        testConnection = getConnectionWithProps("dontTrackOpenResources=true");
        testStatement = testConnection.createStatement();
        testResultSet = testStatement.executeQuery("SELECT 1");

        assertFalse("Connection should not be closed.", testConnection.isClosed());
        assertFalse("Statement should not be closed.", isStatementClosedForTestBug69746(testStatement));
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet));

        testConnection.close();

        assertTrue("Connection should be closed.", testConnection.isClosed());
        assertFalse("Statement should not be closed.", isStatementClosedForTestBug69746(testStatement));
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet));

        testStatement.close();

        assertTrue("Connection should be closed.", testConnection.isClosed());
        assertTrue("Statement should be closed.", isStatementClosedForTestBug69746(testStatement));
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet));

        testResultSet.close();

        assertTrue("Connection should be closed.", testConnection.isClosed());
        assertTrue("Statement should be closed.", isStatementClosedForTestBug69746(testStatement));
        assertTrue("ResultSet should be closed.", isResultSetClosedForTestBug69746(testResultSet));

        /*
         * Test implicit closes
         */
        // Prepare test objects
        createProcedure("testBug69746_proc", "() BEGIN SELECT 1; SELECT 2; SELECT 3; END");
        createTable("testBug69746_tbl", "(fld1 INT NOT NULL AUTO_INCREMENT, fld2 INT, PRIMARY KEY(fld1))");

        testConnection = getConnectionWithProps("dontTrackOpenResources=true");
        testStatement = testConnection.createStatement();
        testResultSet = testStatement.executeQuery("SELECT 1");

        // 1. Statement.execute() & Statement.getMoreResults()
        this.rs = testStatement.executeQuery("CALL testBug69746_proc");
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet));

        ResultSet testResultSet2 = testStatement.getResultSet();
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet));
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet2));

        testStatement.getMoreResults();
        ResultSet testResultSet3 = testStatement.getResultSet();
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet));
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet2));
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet3));

        testStatement.getMoreResults(Statement.KEEP_CURRENT_RESULT);
        ResultSet testResultSet4 = testStatement.getResultSet();
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet));
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet2));
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet3));
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet4));

        testStatement.getMoreResults(Statement.CLOSE_ALL_RESULTS);
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet));
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet2));
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet3));
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet4));

        // 2. Statement.executeBatch()
        testStatement.addBatch("INSERT INTO testBug69746_tbl (fld2) VALUES (1)");
        testStatement.addBatch("INSERT INTO testBug69746_tbl (fld2) VALUES (2)");
        testStatement.executeBatch();
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet));

        // 3. Statement.executeQuery()
        this.rs = testStatement.executeQuery("SELECT 2");
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet));

        // 4. Statement.executeUpdate()
        testStatement.executeUpdate("INSERT INTO testBug69746_tbl (fld2) VALUES (3)");
        assertFalse("ResultSet should not be closed.", isResultSetClosedForTestBug69746(testResultSet));

        testResultSet.close();
        testResultSet2.close();
        testResultSet3.close();
        testResultSet4.close();
        testStatement.close();
        testConnection.close();
    }

    private boolean isStatementClosedForTestBug69746(Statement statement) {
        try {
            statement.getResultSet();
        } catch (SQLException ex) {
            return ex.getMessage().equalsIgnoreCase(Messages.getString("Statement.AlreadyClosed"));
        }
        return false;
    }

    private boolean isResultSetClosedForTestBug69746(ResultSet resultSet) {
        try {
            resultSet.first();
        } catch (SQLException ex) {
            return ex.getMessage().equalsIgnoreCase(Messages.getString("ResultSet.Operation_not_allowed_after_ResultSet_closed_144"));
        }
        return false;
    }

    /**
     * This test requires additional server instance configured with
     * default-authentication-plugin=sha256_password and RSA encryption enabled.
     * 
     * To run this test please add this variable to ant call:
     * -Dcom.mysql.cj.testsuite.url.openssl=jdbc:mysql://localhost:3307/test?user=root&password=pwd
     * 
     * @throws Exception
     */
    public void testLongAuthResponsePayload() throws Exception {

        NativeSession sha256Sess;
        if (this.sha256Conn != null && (sha256Sess = (NativeSession) ((JdbcConnection) this.sha256Conn).getSession()).versionMeetsMinimum(5, 6, 6)) {
            Properties props = new Properties();
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

            // check that sha256_password plugin is available
            if (!pluginIsActive(this.sha256Stmt, "sha256_password")) {
                fail("sha256_password required to run this test");
            }

            try {
                // create user with long password and sha256_password auth
                String pwd = sha256Sess.versionMeetsMinimum(8, 0, 4) || sha256Sess.versionMeetsMinimum(5, 7, 21) && !sha256Sess.versionMeetsMinimum(8, 0, 0)
                        || sha256Sess.versionMeetsMinimum(5, 6, 39) && !sha256Sess.versionMeetsMinimum(5, 7, 0)
                                ? "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeaaaaaaaaaabbbbbbbbbbccccccccccdddddddddd"
                                : "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee"
                                        + "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee"
                                        + "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee";

                if (!sha256Sess.versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET @current_old_passwords = @@global.old_passwords");
                }
                createUser(this.sha256Stmt, "'wl6134user'@'%'", "identified WITH sha256_password");
                this.sha256Stmt.executeUpdate("grant all on *.* to 'wl6134user'@'%'");
                if (!sha256Sess.versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET GLOBAL old_passwords= 2");
                    this.sha256Stmt.executeUpdate("SET SESSION old_passwords= 2");
                }
                this.sha256Stmt.executeUpdate(((MysqlConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 7, 6)
                        ? "ALTER USER 'wl6134user'@'%' IDENTIFIED BY '" + pwd + "'"
                        : "set password for 'wl6134user'@'%' = PASSWORD('" + pwd + "')");
                this.sha256Stmt.executeUpdate("flush privileges");

                props.setProperty(PropertyKey.USER.getKeyName(), "wl6134user");
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), pwd);
                props.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), Sha256PasswordPlugin.class.getName());
                props.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                Connection testConn = null;
                try {
                    testConn = DriverManager.getConnection(sha256Url, props);
                    fail("SQLException expected due to password is too long for RSA encryption");
                } catch (Exception e) {
                    assertTrue(e.getMessage().startsWith("Data must not be longer than"));
                } finally {
                    if (testConn != null) {
                        testConn.close();
                    }
                }

                try {
                    String trustStorePath = "src/test/config/ssl-test-certs/ca-truststore";
                    System.setProperty("javax.net.ssl.keyStore", trustStorePath);
                    System.setProperty("javax.net.ssl.keyStorePassword", "password");
                    System.setProperty("javax.net.ssl.trustStore", trustStorePath);
                    System.setProperty("javax.net.ssl.trustStorePassword", "password");

                    props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                    props.setProperty(PropertyKey.requireSSL.getKeyName(), "true");
                    props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");
                    assertCurrentUser(sha256Url, props, "wl6134user", true);

                } catch (Exception e) {
                    throw e;
                } finally {
                    if (testConn != null) {
                        testConn.close();
                    }
                }
            } finally {
                if (!sha256Sess.versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET GLOBAL old_passwords = @current_old_passwords");
                }
            }
        }
    }

    /**
     * Tests fix for Bug#69452 - Memory size connection property doesn't support large values well
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug69452() throws Exception {
        String[][] testMemUnits = new String[][] { { "k", "kb", "kB", "K", "Kb", "KB" }, { "m", "mb", "mB", "M", "Mb", "MB" },
                { "g", "gb", "gB", "G", "Gb", "GB" } };
        JdbcConnection connWithMemProps;
        long[] memMultiplier = new long[] { 1024, 1024 * 1024, 1024 * 1024 * 1024 };

        for (int i = 0; i < testMemUnits.length; i++) {
            for (int j = 0; j < testMemUnits[i].length; j++) {
                // testing with memory values under 2GB because higher values aren't supported.
                connWithMemProps = (com.mysql.cj.jdbc.JdbcConnection) getConnectionWithProps(
                        String.format("blobSendChunkSize=1.2%1$s,largeRowSizeThreshold=1.4%1$s,locatorFetchBufferSize=1.6%1$s", testMemUnits[i][j]));

                // test values of property 'blobSendChunkSize'
                assertEquals("Memory unit '" + testMemUnits[i][j] + "'; property 'blobSendChunkSize'", (int) (memMultiplier[i] * 1.2),
                        connWithMemProps.getPropertySet().getMemorySizeProperty(PropertyKey.blobSendChunkSize).getValue().intValue());

                // test values of property 'largeRowSizeThreshold'
                assertEquals("Memory unit '" + testMemUnits[i][j] + "'; property 'largeRowSizeThreshold'", "1.4" + testMemUnits[i][j],
                        connWithMemProps.getPropertySet().getMemorySizeProperty(PropertyKey.largeRowSizeThreshold).getStringValue());
                assertEquals("Memory unit '" + testMemUnits[i][j] + "'; property 'largeRowSizeThreshold'", (int) (memMultiplier[i] * 1.4),
                        connWithMemProps.getPropertySet().getMemorySizeProperty(PropertyKey.largeRowSizeThreshold).getValue().intValue());

                // test values of property 'locatorFetchBufferSize'
                assertEquals("Memory unit '" + testMemUnits[i][j] + "'; property 'locatorFetchBufferSize'", (int) (memMultiplier[i] * 1.6),
                        connWithMemProps.getPropertySet().getMemorySizeProperty(PropertyKey.locatorFetchBufferSize).getValue().intValue());

                connWithMemProps.close();
            }
        }
    }

    /**
     * Tests fix for Bug#69777 - Setting maxAllowedPacket below 8203 makes blobSendChunkSize negative.
     * 
     * @throws Exception
     *             if any errors occur
     */
    public void testBug69777() throws Exception {
        final int maxPacketSizeThreshold = 8203; // ServerPreparedStatement.BLOB_STREAM_READ_BUF_SIZE + 11

        // test maxAllowedPacket below threshold and useServerPrepStmts=true
        assertThrows(SQLException.class, "Connection setting too low for 'maxAllowedPacket'.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps("useServerPrepStmts=true,maxAllowedPacket=" + (maxPacketSizeThreshold - 1)).close();
                return null;
            }
        });

        assertThrows(SQLException.class, "Connection setting too low for 'maxAllowedPacket'.*", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps("useServerPrepStmts=true,maxAllowedPacket=" + maxPacketSizeThreshold).close();
                return null;
            }
        });

        // the following instructions should execute without any problem

        // test maxAllowedPacket above threshold and useServerPrepStmts=true
        getConnectionWithProps("useServerPrepStmts=true,maxAllowedPacket=" + (maxPacketSizeThreshold + 1)).close();

        // test maxAllowedPacket below threshold and useServerPrepStmts=false
        getConnectionWithProps("useServerPrepStmts=false,maxAllowedPacket=" + (maxPacketSizeThreshold - 1)).close();

        // test maxAllowedPacket on threshold and useServerPrepStmts=false
        getConnectionWithProps("useServerPrepStmts=false,maxAllowedPacket=" + maxPacketSizeThreshold).close();

        // test maxAllowedPacket above threshold and useServerPrepStmts=false
        getConnectionWithProps("useServerPrepStmts=false,maxAllowedPacket=" + (maxPacketSizeThreshold + 1)).close();
    }

    /**
     * Tests fix for BUG#69579 - DriverManager.setLoginTimeout not honored.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug69579() throws Exception {
        // Mock Server that accepts network connections and does nothing with them, for connection timeout testing.
        class MockServer implements Runnable {
            private ServerSocket serverSocket = null;

            int initialize() throws IOException {
                this.serverSocket = new ServerSocket(0);
                return this.serverSocket.getLocalPort();
            }

            void releaseResources() {
                System.out.println("Start releasing mock server resources.");
                if (this.serverSocket != null) {
                    try {
                        this.serverSocket.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

            public void run() {
                if (this.serverSocket == null) {
                    throw new Error("Mock server not initialized.");
                }
                Socket clientSocket = null;
                try {
                    while ((clientSocket = this.serverSocket.accept()) != null) {
                        System.out.println("Client socket accepted: [" + clientSocket.toString() + "]");
                    }
                } catch (IOException e) {
                    System.out.println("Shutting down mock server.");
                } finally {
                    if (clientSocket != null) {
                        try {
                            clientSocket.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
        }

        ExecutorService executor = Executors.newCachedThreadPool();
        MockServer mockServer = new MockServer();
        int serverPort = 0;
        try {
            serverPort = mockServer.initialize();
        } catch (IOException e1) {
            fail("Failed to initialize a mock server.");
        }
        final String testURL = "jdbc:mysql://localhost:" + serverPort;
        Connection testConn = null;
        final int oldLoginTimeout = DriverManager.getLoginTimeout();
        final int loginTimeout = 3;
        final int testTimeout = loginTimeout * 2;
        long timestamp = System.currentTimeMillis();

        try {
            DriverManager.setLoginTimeout(loginTimeout);

            executor.execute(mockServer);

            Future<Connection> future = executor.submit(new Callable<Connection>() {
                @SuppressWarnings("synthetic-access")
                public Connection call() throws Exception {
                    return getConnectionWithProps(testURL, "");
                }
            });

            testConn = future.get(testTimeout, TimeUnit.SECONDS);
            testConn.close();

            fail("The connection attempt should have timed out.");

        } catch (InterruptedException e) {
            e.printStackTrace();
            fail("Failed to establish a connection with mock server.");

        } catch (ExecutionException e) {
            if (e.getCause() instanceof SQLException) {
                e.printStackTrace();
                assertTrue(e.getCause().getMessage().startsWith("Communications link failure")
                        || e.getCause().getMessage().equals(Messages.getString("Connection.LoginTimeout")));

                assertEquals("Login timeout should have occured in (secs.):", loginTimeout, (System.currentTimeMillis() - timestamp) / 1000);
            } else {
                fail("Failed to establish a connection with mock server.");
            }

        } catch (TimeoutException e) {
            fail("Time expired for connection attempt.");

        } finally {
            DriverManager.setLoginTimeout(oldLoginTimeout);
            mockServer.releaseResources();
            executor.shutdownNow();
        }
    }

    /**
     * Tests fix for Bug#71038, Add an option for custom collations detection
     * 
     * @throws Exception
     */
    public void testBug71038() throws Exception {
        Properties p = new Properties();
        p.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        p.setProperty(PropertyKey.detectCustomCollations.getKeyName(), "false");
        p.setProperty(PropertyKey.queryInterceptors.getKeyName(), Bug71038QueryInterceptor.class.getName());

        JdbcConnection c = (JdbcConnection) getConnectionWithProps(p);
        Bug71038QueryInterceptor si = (Bug71038QueryInterceptor) c.getQueryInterceptorsInstances().get(0);
        assertTrue("SHOW COLLATION was issued when detectCustomCollations=false", si.cnt == 0);
        c.close();

        p.setProperty(PropertyKey.detectCustomCollations.getKeyName(), "true");
        p.setProperty(PropertyKey.queryInterceptors.getKeyName(), Bug71038QueryInterceptor.class.getName());

        c = (JdbcConnection) getConnectionWithProps(p);
        si = (Bug71038QueryInterceptor) c.getQueryInterceptorsInstances().get(0);
        assertTrue("SHOW COLLATION wasn't issued when detectCustomCollations=true", si.cnt > 0);
        c.close();
    }

    /**
     * Counts the number of issued "SHOW COLLATION" statements.
     */
    public static class Bug71038QueryInterceptor extends BaseQueryInterceptor {
        int cnt = 0;

        @Override
        public <M extends Message> M preProcess(M queryPacket) {
            String sql = StringUtils.toString(queryPacket.getByteBuffer(), 1, (queryPacket.getPosition() - 1));
            if (sql.contains("SHOW COLLATION")) {
                this.cnt++;
            }
            return null;
        }
    }

    /**
     * Internal method for tests to get a replication connection with a
     * single master host to the test URL.
     */
    private ReplicationConnection getTestReplicationConnectionNoSlaves(String masterHost) throws Exception {
        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        List<HostInfo> masterHosts = new ArrayList<>();
        masterHosts.add(mainConnectionUrl.getHostOrSpawnIsolated(masterHost));
        List<HostInfo> slaveHosts = new ArrayList<>(); // empty
        Map<String, String> properties = new HashMap<>();
        props.stringPropertyNames().stream().forEach(k -> properties.put(k, props.getProperty(k)));
        ReplicationConnection replConn = ReplicationConnectionProxy.createProxyInstance(new ReplicationConnectionUrl(masterHosts, slaveHosts, properties));
        return replConn;
    }

    /**
     * Test that we remain on the master when:
     * - the connection is not in read-only mode
     * - no slaves are configured
     * - a new slave is added
     */
    public void testReplicationConnectionNoSlavesRemainOnMaster() throws Exception {
        Properties props = getPropertiesFromTestsuiteUrl();
        String masterHost = props.getProperty(PropertyKey.HOST.getKeyName()) + ":" + props.getProperty(PropertyKey.PORT.getKeyName());
        ReplicationConnection replConn = getTestReplicationConnectionNoSlaves(masterHost);
        Statement s = replConn.createStatement();
        ResultSet rs1 = s.executeQuery("select CONNECTION_ID()");
        assertTrue(rs1.next());
        int masterConnectionId = rs1.getInt(1);
        rs1.close();
        s.close();

        // add a slave and make sure we are NOT on a new connection
        replConn.addSlaveHost(masterHost);
        s = replConn.createStatement();
        rs1 = s.executeQuery("select CONNECTION_ID()");
        assertTrue(rs1.next());
        assertEquals(masterConnectionId, rs1.getInt(1));
        assertFalse(replConn.isReadOnly());
        rs1.close();
        s.close();
    }

    public void testReplicationConnectionNoSlavesBasics() throws Exception {
        // create a replication connection with only a master, get the
        // connection id for later use
        Properties props = getPropertiesFromTestsuiteUrl();
        String masterHost = props.getProperty(PropertyKey.HOST.getKeyName()) + ":" + props.getProperty(PropertyKey.PORT.getKeyName());
        ReplicationConnection replConn = getTestReplicationConnectionNoSlaves(masterHost);
        replConn.setAutoCommit(false);
        Statement s = replConn.createStatement();
        ResultSet rs1 = s.executeQuery("select CONNECTION_ID()");
        assertTrue(rs1.next());
        int masterConnectionId = rs1.getInt(1);
        assertFalse(replConn.isReadOnly());
        rs1.close();
        s.close();

        // make sure we are still on the same connection after going
        // to read-only mode. There are no slaves, so no other
        // connections are possible
        replConn.setReadOnly(true);
        assertTrue(replConn.isReadOnly());
        assertTrue(replConn.getCurrentConnection().isReadOnly());
        s = replConn.createStatement();
        try {
            s.executeUpdate("truncate non_existing_table");
            fail("executeUpdate should not be allowed in read-only mode");
        } catch (SQLException ex) {
            assertEquals(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, ex.getSQLState());
        }
        try {
            s.execute("truncate non_existing_table");
            fail("executeUpdate should not be allowed in read-only mode");
        } catch (SQLException ex) {
            assertEquals(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, ex.getSQLState());
        }
        rs1 = s.executeQuery("select CONNECTION_ID()");
        assertTrue(rs1.next());
        assertEquals(masterConnectionId, rs1.getInt(1));
        rs1.close();
        s.close();

        // add a slave and make sure we are on a new connection
        replConn.addSlaveHost(masterHost);
        s = replConn.createStatement();
        rs1 = s.executeQuery("select CONNECTION_ID()");
        assertTrue(rs1.next());
        assertTrue(rs1.getInt(1) != masterConnectionId);
        rs1.close();
        s.close();

        // switch back to master
        replConn.setReadOnly(false);
        s = replConn.createStatement();
        rs1 = s.executeQuery("select CONNECTION_ID()");
        assertFalse(replConn.isReadOnly());
        assertFalse(replConn.getCurrentConnection().isReadOnly());
        assertTrue(rs1.next());
        assertEquals(masterConnectionId, rs1.getInt(1));
        rs1.close();
        s.close();

        // removing the slave should switch back to the master
        replConn.setReadOnly(true);
        replConn.removeSlave(masterHost);
        replConn.commit();
        s = replConn.createStatement();
        rs1 = s.executeQuery("select CONNECTION_ID()");
        // should be maintained even though we're back on the master
        assertTrue(replConn.isReadOnly());
        assertTrue(replConn.getCurrentConnection().isReadOnly());
        assertTrue(rs1.next());
        assertEquals(masterConnectionId, rs1.getInt(1));
        rs1.close();
        s.close();
    }

    /**
     * Tests fix for Bug#71850 - init() is called twice on exception interceptors
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug71850() throws Exception {
        assertThrows(Exception.class, "ExceptionInterceptor.init\\(\\) called 1 time\\(s\\)", new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(
                        "exceptionInterceptors=testsuite.regression.ConnectionRegressionTest$TestBug71850ExceptionInterceptor," + "user=unexistent_user");
                return null;
            }
        });
    }

    public static class TestBug71850ExceptionInterceptor implements ExceptionInterceptor {

        private int counter = 0;

        public ExceptionInterceptor init(Properties props, Log log) {
            this.counter++;
            return this;
        }

        public void destroy() {
        }

        public SQLException interceptException(Exception sqlEx) {

            return new SQLException("ExceptionInterceptor.init() called " + this.counter + " time(s)");
        }

    }

    /**
     * Tests fix for BUG#67803 - XA commands sent twice to MySQL server
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug67803() throws Exception {
        MysqlXADataSource dataSource = new MysqlXADataSource();
        dataSource.setUrl(dbUrl);
        dataSource.getProperty(PropertyKey.useCursorFetch).setValue(true);
        dataSource.getProperty(PropertyKey.defaultFetchSize).setValue(50);
        dataSource.getProperty(PropertyKey.useServerPrepStmts).setValue(true);
        dataSource.getProperty(PropertyKey.exceptionInterceptors).setValue("testsuite.regression.ConnectionRegressionTest$TestBug67803ExceptionInterceptor");

        XAConnection testXAConn1 = dataSource.getXAConnection();
        testXAConn1.getXAResource().start(new MysqlXid("2".getBytes(), "2".getBytes(), 1), 0);
    }

    public static class TestBug67803ExceptionInterceptor implements ExceptionInterceptor {

        public ExceptionInterceptor init(Properties props, Log log) {
            return this;
        }

        public void destroy() {
        }

        public SQLException interceptException(Exception sqlEx) {
            if (!(sqlEx instanceof SQLException)) {
                return SQLError.createSQLException("SQLException expected, but got " + sqlEx.getClass().getName(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        sqlEx, null);
            }
            if (((SQLException) sqlEx).getErrorCode() == 1295
                    || sqlEx.getMessage().contains("This command is not supported in the prepared statement protocol yet")) {
                // SQLException will not be re-thrown if emulateUnsupportedPstmts=true, thus throw RuntimeException to fail the test
                throw new RuntimeException(sqlEx);
            }
            return (SQLException) sqlEx;
        }

    }

    /**
     * Test for Bug#72712 - SET NAMES issued unnecessarily.
     * 
     * Using a statement interceptor, ensure that SET NAMES is not
     * called if the encoding requested by the client application
     * matches that of character_set_server.
     * 
     * Also test that character_set_results is not set unnecessarily.
     */
    public void testBug72712() throws Exception {
        // this test is only run when character_set_server=latin1
        if (!((MysqlConnection) this.conn).getSession().getServerSession().getServerVariable("character_set_server").equals("latin1")) {
            return;
        }

        Properties p = new Properties();
        p.setProperty(PropertyKey.characterEncoding.getKeyName(), "cp1252");
        p.setProperty(PropertyKey.characterSetResults.getKeyName(), "cp1252");
        p.setProperty(PropertyKey.queryInterceptors.getKeyName(), Bug72712QueryInterceptor.class.getName());

        getConnectionWithProps(p);
        // exception will be thrown from the statement interceptor if any SET statements are issued
    }

    /**
     * Statement interceptor used to implement preceding test.
     */
    public static class Bug72712QueryInterceptor extends BaseQueryInterceptor {
        @Override
        public <T extends Resultset> T preProcess(Supplier<String> str, Query interceptedQuery) {
            String sql = str.get();
            if (sql.contains("SET NAMES") || sql.contains("character_set_results") && !(sql.contains("SHOW VARIABLES") || sql.contains("SELECT  @@"))) {
                throw ExceptionFactory.createException("Wrongt statement issued: " + sql);
            }
            return null;
        }
    }

    /**
     * Test for Bug#62577 - XA connection fails with ClassCastException
     */
    public void testBug62577() throws Exception {
        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        String hostSpec = getEncodedHostPortPairFromTestsuiteUrl();
        String database = props.getProperty(PropertyKey.DBNAME.getKeyName());
        props.remove(PropertyKey.DBNAME.getKeyName());

        StringBuilder configs = new StringBuilder();
        for (@SuppressWarnings("rawtypes")
        Map.Entry entry : props.entrySet()) {
            configs.append(entry.getKey());
            configs.append("=");
            configs.append(entry.getValue());
            configs.append("&");
        }
        String cfg1 = configs.toString();

        configs.append(PropertyKey.pinGlobalTxToPhysicalConnection);
        configs.append("=");
        configs.append("true");
        String cfg2 = configs.toString();

        // load-balance
        testBug62577TestUrl(String.format("jdbc:mysql:loadbalance://%s,%s/%s?%s", hostSpec, hostSpec, database, cfg1));
        testBug62577TestUrl(String.format("jdbc:mysql:loadbalance://%s,%s/%s?%s", hostSpec, hostSpec, database, cfg2));
        // failover
        testBug62577TestUrl(String.format("jdbc:mysql://%s,%s/%s?%s", hostSpec, hostSpec, database, cfg1));
        testBug62577TestUrl(String.format("jdbc:mysql://%s,%s/%s?%s", hostSpec, hostSpec, database, cfg2));
    }

    private void testBug62577TestUrl(String url) throws Exception {
        MysqlXADataSource dataSource = new MysqlXADataSource();
        dataSource.setUrl(url);
        XAConnection xaConn = dataSource.getXAConnection();
        Statement st = xaConn.getConnection().createStatement();
        this.rs = st.executeQuery("SELECT 1;");
        xaConn.close();
    }

    /**
     * Test fix for Bug#18869381 - CHANGEUSER() FOR SHA USER RESULTS IN NULLPOINTEREXCEPTION
     * 
     * This test requires additional server instance configured with
     * default-authentication-plugin=sha256_password and RSA encryption enabled.
     * 
     * To run this test please add this variable to ant call:
     * -Dcom.mysql.cj.testsuite.url.openssl=jdbc:mysql://localhost:3307/test?user=root&password=pwd
     * 
     * @throws Exception
     */
    public void testBug18869381() throws Exception {

        if (this.sha256Conn != null && ((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 6, 6)) {

            if (!pluginIsActive(this.sha256Stmt, "sha256_password")) {
                fail("sha256_password required to run this test");
            }

            try {
                if (!((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET @current_old_passwords = @@global.old_passwords");
                }
                createUser(this.sha256Stmt, "'bug18869381user1'@'%'", "identified WITH sha256_password");
                this.sha256Stmt.executeUpdate("grant all on *.* to 'bug18869381user1'@'%'");
                createUser(this.sha256Stmt, "'bug18869381user2'@'%'", "identified WITH sha256_password");
                this.sha256Stmt.executeUpdate("grant all on *.* to 'bug18869381user2'@'%'");
                createUser(this.sha256Stmt, "'bug18869381user3'@'%'", "identified WITH mysql_native_password");
                this.sha256Stmt.executeUpdate("grant all on *.* to 'bug18869381user3'@'%'");
                this.sha256Stmt.executeUpdate(
                        ((MysqlConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'bug18869381user3'@'%' IDENTIFIED BY 'pwd3'"
                                : "set password for 'bug18869381user3'@'%' = PASSWORD('pwd3')");
                if (!((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET GLOBAL old_passwords= 2");
                    this.sha256Stmt.executeUpdate("SET SESSION old_passwords= 2");
                }
                this.sha256Stmt.executeUpdate(((MysqlConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 7, 6)
                        ? "ALTER USER 'bug18869381user1'@'%' IDENTIFIED BY 'LongLongLongLongLongLongLongLongLongLongLongLongPwd1'"
                        : "set password for 'bug18869381user1'@'%' = PASSWORD('LongLongLongLongLongLongLongLongLongLongLongLongPwd1')");
                this.sha256Stmt.executeUpdate(
                        ((MysqlConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'bug18869381user2'@'%' IDENTIFIED BY 'pwd2'"
                                : "set password for 'bug18869381user2'@'%' = PASSWORD('pwd2')");
                this.sha256Stmt.executeUpdate("flush privileges");

                Properties props = new Properties();
                props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

                props.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), MysqlNativePasswordPlugin.class.getName());
                props.setProperty(PropertyKey.useCompression.getKeyName(), "false");
                testBug18869381WithProperties(props);
                props.setProperty(PropertyKey.useCompression.getKeyName(), "true");
                testBug18869381WithProperties(props);

                props.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), Sha256PasswordPlugin.class.getName());
                props.setProperty(PropertyKey.useCompression.getKeyName(), "false");
                testBug18869381WithProperties(props);
                props.setProperty(PropertyKey.useCompression.getKeyName(), "true");
                testBug18869381WithProperties(props);

                props.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                props.setProperty(PropertyKey.useCompression.getKeyName(), "false");
                testBug18869381WithProperties(props);
                props.setProperty(PropertyKey.useCompression.getKeyName(), "true");
                testBug18869381WithProperties(props);

                String trustStorePath = "src/test/config/ssl-test-certs/ca-truststore";
                System.setProperty("javax.net.ssl.keyStore", trustStorePath);
                System.setProperty("javax.net.ssl.keyStorePassword", "password");
                System.setProperty("javax.net.ssl.trustStore", trustStorePath);
                System.setProperty("javax.net.ssl.trustStorePassword", "password");
                props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                props.setProperty(PropertyKey.useCompression.getKeyName(), "false");
                testBug18869381WithProperties(props);
                props.setProperty(PropertyKey.useCompression.getKeyName(), "true");
                testBug18869381WithProperties(props);

            } finally {
                if (!((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET GLOBAL old_passwords = @current_old_passwords");
                }
            }
        }
    }

    private void testBug18869381WithProperties(Properties props) throws Exception {
        Connection testConn = null;
        Statement testSt = null;
        ResultSet testRs = null;

        try {
            testConn = getConnectionWithProps(sha256Url, props);

            ((JdbcConnection) testConn).changeUser("bug18869381user1", "LongLongLongLongLongLongLongLongLongLongLongLongPwd1");
            testSt = testConn.createStatement();
            testRs = testSt.executeQuery("select USER(),CURRENT_USER()");
            testRs.next();
            assertEquals("bug18869381user1", testRs.getString(1).split("@")[0]);
            assertEquals("bug18869381user1", testRs.getString(2).split("@")[0]);
            testSt.close();

            ((JdbcConnection) testConn).changeUser("bug18869381user2", "pwd2");
            testSt = testConn.createStatement();
            testRs = testSt.executeQuery("select USER(),CURRENT_USER()");
            testRs.next();
            assertEquals("bug18869381user2", testRs.getString(1).split("@")[0]);
            assertEquals("bug18869381user2", testRs.getString(2).split("@")[0]);
            testSt.close();

            ((JdbcConnection) testConn).changeUser("bug18869381user3", "pwd3");
            testSt = testConn.createStatement();
            testRs = testSt.executeQuery("select USER(),CURRENT_USER()");
            testRs.next();
            assertEquals("bug18869381user3", testRs.getString(1).split("@")[0]);
            assertEquals("bug18869381user3", testRs.getString(2).split("@")[0]);

        } finally {
            if (testConn != null) {
                testConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#73053 - Endless loop in MysqlIO.clearInputStream due to Linux kernel bug.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug73053() throws Exception {
        /*
         * Test reported issue using a Socket implementation that simulates the buggy behavior.
         */
        try {
            Connection testConn = getConnectionWithProps("socketFactory=testsuite.regression.ConnectionRegressionTest$TestBug73053SocketFactory");
            Statement testStmt = testConn.createStatement();
            this.rs = testStmt.executeQuery("SELECT 1");
            testStmt.close();
            testConn.close();
        } catch (SQLException e) {
            e.printStackTrace();
            fail("No SQLException should be thrown.");
        }

        /*
         * Test the re-implementation of the method that was reported to fail - MysqlIO.clearInputStream() in a normal situation were there actually are bytes
         * to clear out. When running multi-queries with streaming results, if not all results are consumed then the socket has to be cleared out when closing
         * the statement, thus calling MysqlIO.clearInputStream() and effectively discard unread data.
         */
        try {
            Connection testConn = getConnectionWithProps("allowMultiQueries=true");

            Statement testStmt = testConn.createStatement();
            testStmt.setFetchSize(Integer.MIN_VALUE); // set for streaming results

            ResultSet testRS = testStmt.executeQuery("SELECT 1; SELECT 2; SELECT 3; SELECT 4");

            assertTrue(testRS.next());
            assertEquals(1, testRS.getInt(1));

            assertTrue(testStmt.getMoreResults());
            testStmt.getResultSet();

            testStmt.close();
            testConn.close();
        } catch (SQLException e) {
            fail("No SQLException should be thrown.");
        }

        /*
         * Test another scenario that may be able to reproduce the bug, as reported by some (never effectively verified though).
         */
        try {
            final int timeout = 10000;
            final String query = "SELECT SLEEP(15)";

            // 1. run a very slow query in a different thread
            Executors.newSingleThreadExecutor().execute(new Runnable() {
                public void run() {
                    try {
                        // set socketTimeout so this thread doesn't hang if no exception is thrown after killing the connection at server side
                        Connection testConn = getConnectionWithProps("socketTimeout=" + timeout);
                        Statement testStmt = testConn.createStatement();
                        try {
                            testStmt.execute(query);
                        } catch (SQLException e) {
                            assertEquals("Can not read response from server. Expected to read 4 bytes, read 0 bytes before connection was unexpectedly lost.",
                                    e.getCause().getMessage());
                        }
                        testStmt.close();
                        testConn.close();
                    } catch (SQLException e) {
                        fail("No SQLException should be thrown.");
                    }
                }
            });

            // 2. kill the connection running the slow query, at server side, to make sure the driver doesn't hang after its killed
            final long timestamp = System.currentTimeMillis();
            long elapsedTime = 0;

            boolean run = true;
            while (run) {
                this.rs = this.stmt.executeQuery("SHOW PROCESSLIST");
                while (this.rs.next()) {
                    if (query.equals(this.rs.getString(8))) {
                        this.stmt.execute("KILL CONNECTION " + this.rs.getInt(1));
                        run = false;
                        break;
                    }
                }
                if (run) {
                    Thread.sleep(250);
                }
                elapsedTime = System.currentTimeMillis() - timestamp;

                // allow it 10% more time to reach the socketTimeout threshold
                if (elapsedTime > timeout * 1.1) {
                    fail("Failed to kill the connection at server side.");
                }
            }
        } catch (SQLException e) {
            fail("No SQLException should be thrown.");
        }
    }

    public static class TestBug73053SocketFactory extends StandardSocketFactory {
        @SuppressWarnings("unchecked")
        @Override
        public <T extends Closeable> T connect(String hostname, int portNumber, PropertySet props, int loginTimeout) throws IOException {
            return (T) (this.rawSocket = new ConnectionRegressionTest.TestBug73053SocketWrapper(super.connect(hostname, portNumber, props, loginTimeout)));
        }
    }

    private static class TestBug73053SocketWrapper extends Socket {
        final Socket underlyingSocket;

        public TestBug73053SocketWrapper(Socket underlyingSocket) {
            this.underlyingSocket = underlyingSocket;
            try {
                this.underlyingSocket.setSoTimeout(100);
            } catch (SocketException e) {
                fail("Failed preparing custom Socket");
            }
        }

        @Override
        public void connect(SocketAddress endpoint) throws IOException {
            this.underlyingSocket.connect(endpoint);
        }

        @Override
        public void connect(SocketAddress endpoint, int timeout) throws IOException {
            this.underlyingSocket.connect(endpoint, timeout);
        }

        @Override
        public void bind(SocketAddress bindpoint) throws IOException {
            this.underlyingSocket.bind(bindpoint);
        }

        @Override
        public InetAddress getInetAddress() {
            return this.underlyingSocket.getInetAddress();
        }

        @Override
        public InetAddress getLocalAddress() {
            return this.underlyingSocket.getLocalAddress();
        }

        @Override
        public int getPort() {
            return this.underlyingSocket.getPort();
        }

        @Override
        public int getLocalPort() {
            return this.underlyingSocket.getLocalPort();
        }

        @Override
        public SocketAddress getRemoteSocketAddress() {
            return this.underlyingSocket.getRemoteSocketAddress();
        }

        @Override
        public SocketAddress getLocalSocketAddress() {
            return this.underlyingSocket.getLocalSocketAddress();
        }

        @Override
        public SocketChannel getChannel() {
            return this.underlyingSocket.getChannel();
        }

        @Override
        public InputStream getInputStream() throws IOException {
            return new ConnectionRegressionTest.TestBug73053InputStreamWrapper(this.underlyingSocket.getInputStream());
        }

        @Override
        public OutputStream getOutputStream() throws IOException {
            return this.underlyingSocket.getOutputStream();
        }

        @Override
        public void setTcpNoDelay(boolean on) throws SocketException {
            this.underlyingSocket.setTcpNoDelay(on);
        }

        @Override
        public boolean getTcpNoDelay() throws SocketException {
            return this.underlyingSocket.getTcpNoDelay();
        }

        @Override
        public void setSoLinger(boolean on, int linger) throws SocketException {
            this.underlyingSocket.setSoLinger(on, linger);
        }

        @Override
        public int getSoLinger() throws SocketException {
            return this.underlyingSocket.getSoLinger();
        }

        @Override
        public void sendUrgentData(int data) throws IOException {
            this.underlyingSocket.sendUrgentData(data);
        }

        @Override
        public void setOOBInline(boolean on) throws SocketException {
            this.underlyingSocket.setOOBInline(on);
        }

        @Override
        public boolean getOOBInline() throws SocketException {
            return this.underlyingSocket.getOOBInline();
        }

        @Override
        public synchronized void setSoTimeout(int timeout) throws SocketException {
            this.underlyingSocket.setSoTimeout(timeout);
        }

        @Override
        public synchronized int getSoTimeout() throws SocketException {
            return this.underlyingSocket.getSoTimeout();
        }

        @Override
        public synchronized void setSendBufferSize(int size) throws SocketException {
            this.underlyingSocket.setSendBufferSize(size);
        }

        @Override
        public synchronized int getSendBufferSize() throws SocketException {
            return this.underlyingSocket.getSendBufferSize();
        }

        @Override
        public synchronized void setReceiveBufferSize(int size) throws SocketException {
            this.underlyingSocket.setReceiveBufferSize(size);
        }

        @Override
        public synchronized int getReceiveBufferSize() throws SocketException {
            return this.underlyingSocket.getReceiveBufferSize();
        }

        @Override
        public void setKeepAlive(boolean on) throws SocketException {
            this.underlyingSocket.setKeepAlive(on);
        }

        @Override
        public boolean getKeepAlive() throws SocketException {
            return this.underlyingSocket.getKeepAlive();
        }

        @Override
        public void setTrafficClass(int tc) throws SocketException {
            this.underlyingSocket.setTrafficClass(tc);
        }

        @Override
        public int getTrafficClass() throws SocketException {
            return this.underlyingSocket.getTrafficClass();
        }

        @Override
        public void setReuseAddress(boolean on) throws SocketException {
            this.underlyingSocket.setReuseAddress(on);
        }

        @Override
        public boolean getReuseAddress() throws SocketException {
            return this.underlyingSocket.getReuseAddress();
        }

        @Override
        public synchronized void close() throws IOException {
            this.underlyingSocket.close();
        }

        @Override
        public void shutdownInput() throws IOException {
            this.underlyingSocket.shutdownInput();
        }

        @Override
        public void shutdownOutput() throws IOException {
            this.underlyingSocket.shutdownOutput();
        }

        @Override
        public String toString() {
            return this.underlyingSocket.toString();
        }

        @Override
        public boolean isConnected() {
            return this.underlyingSocket.isConnected();
        }

        @Override
        public boolean isBound() {
            return this.underlyingSocket.isBound();
        }

        @Override
        public boolean isClosed() {
            return this.underlyingSocket.isClosed();
        }

        @Override
        public boolean isInputShutdown() {
            return this.underlyingSocket.isInputShutdown();
        }

        @Override
        public boolean isOutputShutdown() {
            return this.underlyingSocket.isOutputShutdown();
        }

        @Override
        public void setPerformancePreferences(int connectionTime, int latency, int bandwidth) {
            this.underlyingSocket.setPerformancePreferences(connectionTime, latency, bandwidth);
        }

        @Override
        public int hashCode() {
            return this.underlyingSocket.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return this.underlyingSocket.equals(obj);
        }
    }

    private static class TestBug73053InputStreamWrapper extends InputStream {
        final InputStream underlyingInputStream;
        int loopCount = 0;

        public TestBug73053InputStreamWrapper(InputStream underlyingInputStream) {
            this.underlyingInputStream = underlyingInputStream;
        }

        @Override
        public int read() throws IOException {
            this.loopCount = 0;
            return this.underlyingInputStream.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            this.loopCount = 0;
            return this.underlyingInputStream.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            try {
                int readCount = this.underlyingInputStream.read(b, off, len);
                this.loopCount = 0;
                return readCount;
            } catch (SocketTimeoutException e) {
                this.loopCount++;
                if (this.loopCount > 10) {
                    fail("Probable infinite loop at MySQLIO.clearInputStream().");
                }
                return -1;
            }
        }

        @Override
        public long skip(long n) throws IOException {
            return this.underlyingInputStream.skip(n);
        }

        @Override
        public int available() throws IOException {
            // In some older Linux kernels the underlying system call may return 1 when actually no bytes are available in a CLOSE_WAIT state socket, even if EOF
            // has been reached.
            int available = this.underlyingInputStream.available();
            return available == 0 ? 1 : available;
        }

        @Override
        public void close() throws IOException {
            this.underlyingInputStream.close();
        }

        @Override
        public synchronized void mark(int readlimit) {
            this.underlyingInputStream.mark(readlimit);
        }

        @Override
        public synchronized void reset() throws IOException {
            this.underlyingInputStream.reset();
        }

        @Override
        public boolean markSupported() {
            return this.underlyingInputStream.markSupported();
        }

        @Override
        public int hashCode() {
            return this.underlyingInputStream.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return this.underlyingInputStream.equals(obj);
        }

        @Override
        public String toString() {
            return this.underlyingInputStream.toString();
        }
    }

    /**
     * Tests fix for BUG#19354014 - CHANGEUSER() CALL RESULTS IN "PACKETS OUT OF ORDER" ERROR
     * 
     * @throws Exception
     */
    public void testBug19354014() throws Exception {
        if (!versionMeetsMinimum(5, 5, 7)) {
            return;
        }
        Connection con = null;
        createUser("'bug19354014user'@'%'", "identified WITH mysql_native_password");
        this.stmt.executeUpdate("grant all on *.* to 'bug19354014user'@'%'");
        this.stmt.executeUpdate(versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'bug19354014user'@'%' IDENTIFIED BY 'pwd'"
                : "set password for 'bug19354014user'@'%' = PASSWORD('pwd')");
        this.stmt.executeUpdate("flush privileges");

        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.useCompression.getKeyName(), "true");

            con = getConnectionWithProps(props);
            ((JdbcConnection) con).changeUser("bug19354014user", "pwd");
        } finally {
            this.stmt.executeUpdate("flush privileges");

            if (con != null) {
                con.close();
            }
        }
    }

    /**
     * Tests fix for Bug#75168 - loadBalanceExceptionChecker interface cannot work using JDBC4/JDK7
     * 
     * @throws Exception
     */
    public void testBug75168() throws Exception {
        final Properties props = new Properties();
        props.setProperty(PropertyKey.loadBalanceExceptionChecker.getKeyName(), Bug75168LoadBalanceExceptionChecker.class.getName());
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), Bug75168QueryInterceptor.class.getName());

        Connection connTest = getLoadBalancedConnection(2, null, props); // get a load balancing connection with two default servers
        for (int i = 0; i < 3; i++) {
            Statement stmtTest = null;
            try {
                stmtTest = connTest.createStatement();
                stmtTest.execute("SELECT * FROM nonexistent_table");
                fail("'Table doesn't exist' exception was expected.");
            } catch (SQLException e) {
                assertTrue("'Table doesn't exist' exception was expected.", e.getMessage().endsWith("nonexistent_table' doesn't exist"));
            } finally {
                if (stmtTest != null) {
                    stmtTest.close();
                }
            }
        }
        connTest.close();

        boolean stop = false;
        do {
            connTest = getLoadBalancedConnection(2, null, props); // get a load balancing connection with two default servers
            for (int i = 0; i < 3; i++) {
                PreparedStatement pstmtTest = null;
                try {
                    pstmtTest = connTest.prepareStatement("SELECT * FROM nonexistent_table");
                    pstmtTest.execute();
                    fail("'Table doesn't exist' exception was expected.");
                } catch (SQLException e) {
                    assertTrue("'Table doesn't exist' exception was expected.", e.getMessage().endsWith("nonexistent_table' doesn't exist"));
                } finally {
                    if (pstmtTest != null) {
                        pstmtTest.close();
                    }
                }
            }
            connTest.close();

            // do it again with server prepared statements
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
        } while (stop = !stop);
    }

    public static class Bug75168LoadBalanceExceptionChecker implements LoadBalanceExceptionChecker {
        public void init(Properties props) {
        }

        public void destroy() {
        }

        public boolean shouldExceptionTriggerFailover(Throwable ex) {
            return ex.getMessage().endsWith("nonexistent_table' doesn't exist");
        }
    }

    public static class Bug75168QueryInterceptor extends BaseQueryInterceptor {
        static Connection previousConnection = null;

        private JdbcConnection connection;

        @Override
        public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
            this.connection = (JdbcConnection) conn;
            return this;
        }

        @Override
        public void destroy() {
            this.connection = null;
            if (previousConnection == null) {
                fail("Test testBug75168 didn't run as expected.");
            }
        }

        @Override
        public <T extends Resultset> T preProcess(Supplier<String> str, Query interceptedQuery) {
            String sql = str.get();
            if (sql == null) {
                sql = "";
            }
            if (sql.length() == 0 && interceptedQuery instanceof ClientPreparedStatement) {
                try {
                    sql = ((ClientPreparedStatement) interceptedQuery).asSql();
                } catch (SQLException ex) {
                    throw ExceptionFactory.createException(ex.getMessage(), ex);
                }
            }
            if (sql.indexOf("nonexistent_table") >= 0) {
                assertTrue("Different connection expected.", !this.connection.equals(previousConnection));
                previousConnection = this.connection;
            }
            return null;
        }
    }

    /**
     * Tests fix for BUG#71084 - Wrong java.sql.Date stored if client and server time zones differ
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug71084() throws Exception {
        createTable("testBug71084", "(id INT, dt DATE)");

        Properties connProps = new Properties();
        testBug71084AssertCase(connProps, "GMT+2", "GMT+6", null, "1998-05-21", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-6", "GMT+2", null, "1998-05-21", "1998-05-21", "1998-05-21 0:00:00");
    }

    private void testBug71084AssertCase(Properties connProps, String clientTZ, String serverTZ, String targetTZ, String insertDate, String expectedStoredDate,
            String expectedRetrievedDate) throws Exception {
        final TimeZone defaultTZ = TimeZone.getDefault();
        final boolean useTargetCal = targetTZ != null;
        final Properties testExtraProperties = new Properties();

        this.stmt.execute("DELETE FROM testBug71084");

        try {
            TimeZone.setDefault(TimeZone.getTimeZone(clientTZ));

            SimpleDateFormat longDateFrmt = TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd HH:mm:ss", null, TimeZone.getDefault());
            SimpleDateFormat shortDateFrmt = TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd", null, TimeZone.getDefault());

            Calendar targetCal = null;
            String targetCalMsg = null;
            if (useTargetCal) {
                targetCal = Calendar.getInstance(TimeZone.getTimeZone(targetTZ));
                targetCalMsg = " (Calendar methods)";
            } else {
                targetCalMsg = " (non-Calendar methods)";
            }

            Date dateIn = insertDate.length() == 10 ? shortDateFrmt.parse(insertDate) : longDateFrmt.parse(insertDate);
            String expectedDateInDB = expectedStoredDate;
            Date expectedDateInRS = longDateFrmt.parse(expectedRetrievedDate);
            String expectedDateInDBNoConv = shortDateFrmt.format(dateIn);
            /* Date expectedDateInRSNoConv = */ shortDateFrmt.parse(expectedDateInDBNoConv);

            int id = 0;
            for (Entry<Object, Object> prop : testExtraProperties.entrySet()) {
                id++;

                String key = (String) prop.getKey();
                String value = (String) prop.getValue();
                Properties connPropsLocal = new Properties();
                String propsList = "...";

                connPropsLocal.putAll(connProps);
                if (key.length() > 0) {
                    connPropsLocal.setProperty(key, value);
                }
                for (Object k : connPropsLocal.keySet()) {
                    propsList += "," + (String) k;
                }

                connPropsLocal.setProperty(PropertyKey.serverTimezone.getKeyName(), serverTZ);

                Connection testConn = getConnectionWithProps(connPropsLocal);

                PreparedStatement testPstmt = testConn.prepareStatement("INSERT INTO testBug71084 VALUES (?, ?)");
                testPstmt.setInt(1, id);
                if (useTargetCal) {
                    testPstmt.setDate(2, new java.sql.Date(dateIn.getTime()), targetCal);
                } else {
                    testPstmt.setDate(2, new java.sql.Date(dateIn.getTime()));
                }
                testPstmt.execute();
                testPstmt.close();

                Statement testStmt = testConn.createStatement();
                // Get date value from database: Column `dt` - allowing time zone conversion by returning it as is; Column `dtStr` - preventing time zone
                // conversion by returning it as String and invalidating the date format so that no automatic conversion can ever happen.
                ResultSet restRs = testStmt.executeQuery("SELECT dt, CONCAT('$', dt) AS dtStr FROM testBug71084 WHERE id = " + id);
                restRs.next();
                java.sql.Date dateOut = useTargetCal ? restRs.getDate(1, targetCal) : restRs.getDate(1);
                String dateInDB = restRs.getString(2).substring(1);
                restRs.close();
                testStmt.close();

                testConn.close();

                assertEquals(id + ". [" + propsList + "] Date stored" + targetCalMsg, expectedDateInDB, dateInDB);
                assertEquals(id + ". [" + propsList + "] Date retrieved" + targetCalMsg, longDateFrmt.format(expectedDateInRS), longDateFrmt.format(dateOut));
            }
        } finally {
            TimeZone.setDefault(defaultTZ);
        }
    }

    /**
     * Tests fix for BUG#20685022 - SSL CONNECTION TO MYSQL 5.7.6 COMMUNITY SERVER FAILS
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20685022() throws Exception {
        if (!isCommunityEdition()) {
            return;
        }

        final Properties props = new Properties();

        /*
         * case 1: non verifying server certificate
         */
        props.clear();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.requireSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");

        getConnectionWithProps(props);

        /*
         * case 2: verifying server certificate using key store provided by connection properties
         */
        props.clear();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.requireSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "true");
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");

        getConnectionWithProps(props);

        /*
         * case 3: verifying server certificate using key store provided by system properties
         */
        props.clear();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.requireSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "true");

        String trustStorePath = "src/test/config/ssl-test-certs/ca-truststore";
        System.setProperty("javax.net.ssl.keyStore", trustStorePath);
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStore", trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        getConnectionWithProps(props);
    }

    /**
     * Tests fix for BUG#75592 - "SHOW VARIABLES WHERE" is expensive.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug75592() throws Exception {
        if (versionMeetsMinimum(5, 0, 3)) {

            JdbcConnection con = (JdbcConnection) getConnectionWithProps("queryInterceptors=" + Bug75592QueryInterceptor.class.getName());

            // reference values
            Map<String, String> serverVariables = new HashMap<>();
            this.rs = con.createStatement().executeQuery("SHOW VARIABLES");
            while (this.rs.next()) {
                serverVariables.put(this.rs.getString(1), this.rs.getString(2));
            }

            // fix the renaming of "tx_isolation" to "transaction_isolation" that is made in NativeSession.loadServerVariables().
            if (!serverVariables.containsKey("transaction_isolation") && serverVariables.containsKey("tx_isolation")) {
                serverVariables.put("transaction_isolation", serverVariables.remove("tx_isolation"));
            }
            Session session = con.getSession();

            // check values from "select @@var..."
            assertEquals(serverVariables.get("auto_increment_increment"), session.getServerSession().getServerVariable("auto_increment_increment"));
            assertEquals(serverVariables.get("character_set_client"), session.getServerSession().getServerVariable("character_set_client"));
            assertEquals(serverVariables.get("character_set_connection"), session.getServerSession().getServerVariable("character_set_connection"));

            // we override character_set_results sometimes when configuring client charsets, thus need to check against actual value
            if (session.getServerSession().getServerVariable(ServerSession.LOCAL_CHARACTER_SET_RESULTS) == null) {
                assertEquals("", serverVariables.get("character_set_results"));
            } else {
                assertEquals(serverVariables.get("character_set_results"),
                        session.getServerSession().getServerVariable(ServerSession.LOCAL_CHARACTER_SET_RESULTS));
            }

            assertEquals(serverVariables.get("character_set_server"), session.getServerSession().getServerVariable("character_set_server"));
            assertEquals(serverVariables.get("init_connect"), session.getServerSession().getServerVariable("init_connect"));
            assertEquals(serverVariables.get("interactive_timeout"), session.getServerSession().getServerVariable("interactive_timeout"));
            assertEquals(serverVariables.get("license"), session.getServerSession().getServerVariable("license"));
            assertEquals(serverVariables.get("lower_case_table_names"), session.getServerSession().getServerVariable("lower_case_table_names"));
            assertEquals(serverVariables.get("max_allowed_packet"), session.getServerSession().getServerVariable("max_allowed_packet"));
            assertEquals(serverVariables.get("net_write_timeout"), session.getServerSession().getServerVariable("net_write_timeout"));
            if (!con.getServerVersion().meetsMinimum(new ServerVersion(8, 0, 3))) {
                assertEquals(serverVariables.get("query_cache_size"), session.getServerSession().getServerVariable("query_cache_size"));
                assertEquals(serverVariables.get("query_cache_type"), session.getServerSession().getServerVariable("query_cache_type"));
            }

            // not necessarily contains STRICT_TRANS_TABLES
            for (String sm : serverVariables.get("sql_mode").split(",")) {
                if (!sm.equals("STRICT_TRANS_TABLES")) {
                    assertTrue(session.getServerSession().getServerVariable("sql_mode").contains(sm));
                }
            }

            assertEquals(serverVariables.get("system_time_zone"), session.getServerSession().getServerVariable("system_time_zone"));
            assertEquals(serverVariables.get("time_zone"), session.getServerSession().getServerVariable("time_zone"));
            assertEquals(serverVariables.get("transaction_isolation"), session.getServerSession().getServerVariable("transaction_isolation"));
            assertEquals(serverVariables.get("wait_timeout"), session.getServerSession().getServerVariable("wait_timeout"));
            if (!versionMeetsMinimum(5, 5, 0)) {
                assertEquals(serverVariables.get("language"), session.getServerSession().getServerVariable("language"));
            }
        }
    }

    /**
     * Statement interceptor for preceding testBug75592().
     */
    public static class Bug75592QueryInterceptor extends BaseQueryInterceptor {
        @Override
        public <T extends Resultset> T preProcess(Supplier<String> str, Query interceptedQuery) {
            String sql = str.get();
            if (sql.contains("SHOW VARIABLES WHERE")) {
                throw ExceptionFactory.createException("'SHOW VARIABLES WHERE' statement issued: " + sql);
            }
            return null;
        }
    }

    /**
     * Tests fix for BUG#62452 - NPE thrown in JDBC4MySQLPooledException when statement is closed.
     * 
     * @throws Exception
     */
    public void testBug62452() throws Exception {
        PooledConnection con = null;

        MysqlConnectionPoolDataSource pds = new MysqlConnectionPoolDataSource();
        pds.setUrl(dbUrl);
        con = pds.getPooledConnection();
        assertTrue(con instanceof MysqlPooledConnection);
        testBug62452WithConnection(con);

        MysqlXADataSource xads = new MysqlXADataSource();
        xads.setUrl(dbUrl);

        xads.getProperty(PropertyKey.pinGlobalTxToPhysicalConnection).setValue(false);
        con = xads.getXAConnection();
        assertTrue(con instanceof MysqlXAConnection);
        testBug62452WithConnection(con);

        xads.getProperty(PropertyKey.pinGlobalTxToPhysicalConnection).setValue(true);
        con = xads.getXAConnection();
        assertTrue(con instanceof SuspendableXAConnection);
        testBug62452WithConnection(con);

    }

    private void testBug62452WithConnection(PooledConnection con) throws Exception {
        this.pstmt = con.getConnection().prepareStatement("SELECT 1");
        this.rs = this.pstmt.executeQuery();
        con.close();

        // If PooledConnection is already closed by some reason a NullPointerException was thrown on the next line
        // because the closed connection has nulled out the list that it synchronises on when the closed event is fired.
        this.pstmt.close();
    }

    /**
     * Tests fix for BUG#20825727 - CONNECT FAILURE WHEN TRY TO CONNECT SHA USER WITH DIFFERENT CHARSET.
     * 
     * This test runs through all authentication plugins when one of the following server requirements is met:
     * 1. Default connection string points to a server configured with both SSL *and* RSA encryption.
     * or
     * 2. Default connection string points to a server configured with SSL enabled but no RSA encryption *and* the property
     * com.mysql.cj.testsuite.url.openssl points to an additional server configured with
     * default-authentication-plugin=sha256_password and RSA encryption.
     * 
     * If none of the servers has SSL and RSA encryption enabled then only 'mysql_native_password' and 'mysql_old_password' plugins are tested.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20825727() throws Exception {
        if (!versionMeetsMinimum(5, 5, 7)) {
            return;
        }

        final String[] testDbUrls;
        Properties props = new Properties();
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        if (this.sha256Conn != null && ((MysqlConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 5, 7)) {
            testDbUrls = new String[] { BaseTestCase.dbUrl, sha256Url };
        } else {
            testDbUrls = new String[] { BaseTestCase.dbUrl };
        }

        for (String testDbUrl : testDbUrls) {
            JdbcConnection testConn = (JdbcConnection) getConnectionWithProps(testDbUrl, props);
            Statement testStmt = testConn.createStatement();

            this.rs = testStmt.executeQuery("SELECT @@GLOBAL.HAVE_SSL = 'YES' AS have_ssl");
            final boolean sslEnabled = this.rs.next() && this.rs.getBoolean(1);

            this.rs = testStmt.executeQuery("SHOW STATUS LIKE '%Rsa_public_key%'");
            final boolean rsaEnabled = this.rs.next() && this.rs.getString(1).length() > 0;

            System.out.println();
            System.out.println("* Testing URL: " + testDbUrl + " [SSL enabled: " + sslEnabled + "]  [RSA enabled: " + rsaEnabled + "]");
            System.out.println("******************************************************************************************************************************"
                    + "*************");
            System.out.printf("%-25s : %-25s : %s : %-25s : %-18s : %-18s [%s]%n", "Connection Type", "Auth. Plugin", "pwd ", "Encoding Prop.",
                    "Encoding Value", "Server Encoding", "TstRes");
            System.out.println("------------------------------------------------------------------------------------------------------------------------------"
                    + "-------------");

            boolean clearTextPluginInstalled = false;
            boolean secureAuthChanged = false;
            try {
                String[] plugins;

                // install cleartext plugin if required
                this.rs = testStmt.executeQuery(
                        "SELECT (PLUGIN_LIBRARY LIKE 'auth_test_plugin%') FROM INFORMATION_SCHEMA.PLUGINS" + " WHERE PLUGIN_NAME='cleartext_plugin_server'");
                if (!this.rs.next() || !this.rs.getBoolean(1)) {
                    String ext = System.getProperty(PropertyDefinitions.SYSP_os_name).toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
                    testStmt.execute("INSTALL PLUGIN cleartext_plugin_server SONAME 'auth_test_plugin" + ext + "'");
                    clearTextPluginInstalled = true;
                }

                if (testConn.getSession().versionMeetsMinimum(5, 7, 5)) {
                    // mysql_old_password plugin not supported
                    plugins = new String[] { "cleartext_plugin_server,-1", "mysql_native_password,0", "sha256_password,2" };
                } else if (testConn.getSession().versionMeetsMinimum(5, 6, 6)) {
                    plugins = new String[] { "cleartext_plugin_server,-1", "mysql_native_password,0", "mysql_old_password,1", "sha256_password,2" };

                    // temporarily disable --secure-auth mode to allow old format passwords
                    testStmt.executeUpdate("SET @current_secure_auth = @@global.secure_auth");
                    testStmt.executeUpdate("SET @@global.secure_auth = off");
                    secureAuthChanged = true;
                } else {
                    // sha256_password plugin not supported
                    plugins = new String[] { "cleartext_plugin_server,-1", "mysql_native_password,0", "mysql_old_password,1" };
                }

                final String simplePwd = "my\tpass word";
                final String complexPwd = "my\tp\u00e4ss w\u263ard";

                for (String encoding : new String[] { "", "UTF-8", "ISO-8859-1", "US-ASCII" }) {
                    for (String plugin : plugins) {

                        String pluginName = plugin.split(",")[0];
                        int pwdHashingMethod = Integer.parseInt(plugin.split(",")[1]);

                        String testStep = "";
                        try {
                            testStep = "create user";
                            testBug20825727CreateUser(testDbUrl, "testBug20825727", simplePwd, encoding, pluginName, pwdHashingMethod);
                            testStep = "login with simple password";
                            testBug20825727TestLogin(testDbUrl, testConn.getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue(),
                                    sslEnabled, rsaEnabled, "testBug20825727", simplePwd, encoding, pluginName);

                            testStep = "change password";
                            testBug20825727ChangePassword(testDbUrl, "testBug20825727", complexPwd, encoding, pluginName, pwdHashingMethod);
                            testStep = "login with complex password";
                            testBug20825727TestLogin(testDbUrl, testConn.getPropertySet().getStringProperty(PropertyKey.characterEncoding).getValue(),
                                    sslEnabled, rsaEnabled, "testBug20825727", complexPwd, encoding, pluginName);
                        } catch (SQLException e) {
                            e.printStackTrace();
                            fail("Failed at '" + testStep + "' using encoding '" + encoding + "' and plugin '" + pluginName
                                    + "'. See also system output for more details.");
                        } finally {
                            try {
                                dropUser(testStmt, "'testBug20825727'@'%'");
                            } catch (Exception e) {
                            }
                        }
                    }
                }
            } finally {
                if (clearTextPluginInstalled) {
                    testStmt.executeUpdate("UNINSTALL PLUGIN cleartext_plugin_server");
                }
                if (secureAuthChanged) {
                    testStmt.executeUpdate("SET @@global.secure_auth = @current_secure_auth");
                }

                testStmt.close();
                testConn.close();
            }
        }
    }

    private void testBug20825727CreateUser(String testDbUrl, String user, String password, String encoding, String pluginName, int pwdHashingMethod)
            throws SQLException {
        JdbcConnection testConn = null;
        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            if (encoding.length() > 0) {
                props.setProperty(PropertyKey.characterEncoding.getKeyName(), encoding);
            }
            testConn = (JdbcConnection) getConnectionWithProps(testDbUrl, props);
            Statement testStmt = testConn.createStatement();

            if (testConn.getSession().versionMeetsMinimum(5, 7, 6)) {
                testStmt.execute("CREATE USER '" + user + "'@'%' IDENTIFIED WITH " + pluginName + " BY '" + password + "'");
            } else if (pwdHashingMethod >= 0) {
                // for mysql_native_password, mysql_old_password and sha256_password plugins
                testStmt.execute("CREATE USER '" + user + "'@'%' IDENTIFIED WITH " + pluginName);
                if (!testConn.getSession().versionMeetsMinimum(8, 0, 5)) {
                    testStmt.execute("SET @@session.old_passwords = " + pwdHashingMethod);
                }
                testStmt.execute("SET PASSWORD FOR '" + user + "'@'%' = PASSWORD('" + password + "')");
                if (!testConn.getSession().versionMeetsMinimum(8, 0, 5)) {
                    testStmt.execute("SET @@session.old_passwords = @@global.old_passwords");
                }
            } else {
                // for cleartext_plugin_server plugin
                testStmt.execute("CREATE USER '" + user + "'@'%' IDENTIFIED WITH " + pluginName + " AS '" + password + "'");
            }
            testStmt.execute("GRANT ALL ON *.* TO '" + user + "'@'%'");
            testStmt.close();
        } finally {
            if (testConn != null) {
                testConn.close();
            }
        }
    }

    private void testBug20825727ChangePassword(String testDbUrl, String user, String password, String encoding, String pluginName, int pwdHashingMethod)
            throws SQLException {
        JdbcConnection testConn = null;
        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            if (encoding.length() > 0) {
                props.setProperty(PropertyKey.characterEncoding.getKeyName(), encoding);
            }
            testConn = (JdbcConnection) getConnectionWithProps(testDbUrl, props);
            Statement testStmt = testConn.createStatement();

            if (testConn.getSession().versionMeetsMinimum(5, 7, 6)) {
                testStmt.execute("ALTER USER '" + user + "'@'%' IDENTIFIED BY '" + password + "'");
            } else if (pwdHashingMethod >= 0) {
                // for mysql_native_password, mysql_old_password and sha256_password plugins
                if (!testConn.getSession().versionMeetsMinimum(8, 0, 5)) {
                    testStmt.execute("SET @@session.old_passwords = " + pwdHashingMethod);
                }
                testStmt.execute("SET PASSWORD FOR '" + user + "'@'%' = PASSWORD('" + password + "')");
                if (!testConn.getSession().versionMeetsMinimum(8, 0, 5)) {
                    testStmt.execute("SET @@session.old_passwords = @@global.old_passwords");
                }
            } else {
                // for cleartext_plugin_server plugin
                dropUser(testStmt, "'" + user + "'@'%'");
                testStmt.execute("CREATE USER '" + user + "'@'%' IDENTIFIED WITH " + pluginName + " AS '" + password + "'");
                testStmt.execute("GRANT ALL ON *.* TO '" + user + "'@'%'");
            }
            testStmt.close();
        } finally {
            if (testConn != null) {
                testConn.close();
            }
        }
    }

    private void testBug20825727TestLogin(final String testDbUrl, String defaultServerEncoding, boolean sslEnabled, boolean rsaEnabled, String user,
            String password, String encoding, String pluginName) throws SQLException {

        final Properties props = new Properties();
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        final JdbcConnection testBaseConn = (JdbcConnection) getConnectionWithProps(testDbUrl, props);
        final boolean pwdIsComplex = !Charset.forName("US-ASCII").newEncoder().canEncode(password);

        for (String encProp : encoding.length() == 0 ? new String[] { "*none*" }
                : new String[] { PropertyKey.characterEncoding.getKeyName(), PropertyKey.passwordCharacterEncoding.getKeyName() }) {
            for (int testCase = 1; testCase <= 4; testCase++) {

                props.setProperty(PropertyKey.USER.getKeyName(), user);
                props.setProperty(PropertyKey.PASSWORD.getKeyName(), password);
                if (encoding.length() > 0) {
                    props.setProperty(encProp, encoding);
                }

                String testCaseMsg = "*none*";
                switch (testCase) {
                    case 1:
                        /*
                         * Test with an SSL disabled connection.
                         * Can't be used with plugins 'cleartext_plugin_server' and 'sha256_password'.
                         */
                        if (pluginName.equals("cleartext_plugin_server") || pluginName.equals("sha256_password")) {
                            continue;
                        }
                        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
                        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                        props.setProperty(PropertyKey.requireSSL.getKeyName(), "false");
                        testCaseMsg = "Non-SSL/Non-RSA";
                        break;

                    case 2:
                        /*
                         * Test with an SSL enabled connection.
                         */
                        if (!sslEnabled) {
                            continue;
                        }
                        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "false");
                        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                        props.setProperty(PropertyKey.requireSSL.getKeyName(), "true");
                        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");
                        testCaseMsg = "SSL";
                        break;

                    case 3:
                        /*
                         * Test with an RSA encryption enabled connection, using public key retrieved from server.
                         * Requires additional server instance pointed by 'com.mysql.cj.testsuite.url.openssl'.
                         * Can't be used with plugin 'cleartext_plugin_server'.
                         */
                        if (pluginName.equals("cleartext_plugin_server") || !rsaEnabled) {
                            continue;
                        }
                        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
                        testCaseMsg = "RSA [pubkey-retrieval]";
                        break;

                    case 4:
                        /*
                         * Test with an RSA encryption enabled connection, using public key pointed by the property 'serverRSAPublicKeyFile'.
                         * Requires additional server instance pointed by 'com.mysql.cj.testsuite.url.openssl'.
                         * Can't be used with plugin 'cleartext_plugin_server'.
                         */
                        if (pluginName.equals("cleartext_plugin_server") || !rsaEnabled) {
                            continue;
                        }
                        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "false");
                        props.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                        testCaseMsg = "RSA [pubkey-file]";
                        break;
                }

                boolean testShouldPass = true;
                if (pwdIsComplex) {
                    // if no encoding is specifically defined then our default password encoding ('UTF-8') and server's encoding must coincide
                    testShouldPass = encoding.length() > 0 || defaultServerEncoding.equalsIgnoreCase("UTF-8");

                    if (!testBaseConn.getSession().versionMeetsMinimum(5, 7, 6) && pluginName.equals("cleartext_plugin_server")) {
                        // 'cleartext_plugin_server' from servers below version 5.7.6 requires UTF-8 encoding
                        testShouldPass = encoding.equals("UTF-8") || (encoding.length() == 0 && defaultServerEncoding.equals("UTF-8"));
                    }
                }

                System.out.printf("%-25s : %-25s : %s : %-25s : %-18s : %-18s [%s]%n", testCaseMsg, pluginName, pwdIsComplex ? "cplx" : "smpl", encProp,
                        encoding.length() == 0 ? "-" : encoding, defaultServerEncoding, testShouldPass);

                Connection testConn = null;
                try {
                    if (testShouldPass) {
                        testConn = getConnectionWithProps(testDbUrl, props);
                        Statement testStmt = testConn.createStatement();

                        this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                        assertTrue(this.rs.next());
                        if (!this.rs.getString(1).startsWith(user) || !this.rs.getString(2).startsWith(user)) {
                            fail("Unexpected failure in test case '" + testCaseMsg + "' using encoding '" + encoding + "' in property '" + encProp + "'.");
                        }
                        this.rs.close();
                        testStmt.close();
                    } else {
                        assertThrows(SQLException.class, "Access denied for user 'testBug20825727'@.*", new Callable<Void>() {
                            @SuppressWarnings("synthetic-access")
                            public Void call() throws Exception {
                                getConnectionWithProps(testDbUrl, props);
                                return null;
                            }
                        });
                    }
                } finally {
                    if (testConn != null) {
                        try {
                            testConn.close();
                        } catch (SQLException e) {
                        }
                    }
                }
            }
        }

        testBaseConn.close();
    }

    /**
     * Tests fix for BUG#75670 - Connection fails with "Public Key Retrieval is not allowed" for native auth.
     * 
     * Requires additional server instance pointed by com.mysql.cj.testsuite.url.openssl variable configured with
     * default-authentication-plugin=sha256_password and RSA encryption enabled.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug75670() throws Exception {
        if (this.sha256Conn != null && ((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 6, 6)) {

            if (!pluginIsActive(this.sha256Stmt, "sha256_password")) {
                fail("sha256_password required to run this test");
            }

            try {
                if (!((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET @current_old_passwords = @@global.old_passwords");
                }

                createUser(this.sha256Stmt, "'bug75670user'@'%'", ""); // let --default-authentication-plugin option force sha256_password
                this.rs = this.sha256Stmt.executeQuery("SELECT plugin FROM mysql.user WHERE user='bug75670user'");
                assertTrue(this.rs.next());
                assertEquals("Wrong default authentication plugin (check test conditions):", "sha256_password", this.rs.getString(1));

                if (((MysqlConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 7, 6)) {
                    createUser(this.sha256Stmt, "'bug75670user_mnp'@'%'", "IDENTIFIED WITH mysql_native_password BY 'bug75670user_mnp'");
                    createUser(this.sha256Stmt, "'bug75670user_sha'@'%'", "IDENTIFIED WITH sha256_password BY 'bug75670user_sha'");
                } else {
                    if (!((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 5)) {
                        this.sha256Stmt.execute("SET @@session.old_passwords = 0");
                    }
                    createUser(this.sha256Stmt, "'bug75670user_mnp'@'%'", "IDENTIFIED WITH mysql_native_password");
                    this.sha256Stmt.execute("SET PASSWORD FOR 'bug75670user_mnp'@'%' = PASSWORD('bug75670user_mnp')");
                    if (!((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 5)) {
                        this.sha256Stmt.execute("SET @@session.old_passwords = 2");
                    }
                    createUser(this.sha256Stmt, "'bug75670user_sha'@'%'", "IDENTIFIED WITH sha256_password");
                    this.sha256Stmt.execute("SET PASSWORD FOR 'bug75670user_sha'@'%' = PASSWORD('bug75670user_sha')");
                }
                this.sha256Stmt.execute("GRANT ALL ON *.* TO 'bug75670user_mnp'@'%'");
                this.sha256Stmt.execute("GRANT ALL ON *.* TO 'bug75670user_sha'@'%'");

                System.out.println();
                System.out.printf("%-25s : %-18s : %-25s : %-25s : %s%n", "DefAuthPlugin", "AllowPubKeyRet", "User", "Passwd", "Test result");
                System.out.println("----------------------------------------------------------------------------------------------------"
                        + "------------------------------");

                for (Class<?> defAuthPlugin : new Class<?>[] { MysqlNativePasswordPlugin.class, Sha256PasswordPlugin.class }) {
                    for (String user : new String[] { "bug75670user_mnp", "bug75670user_sha" }) {
                        for (String pwd : new String[] { user, "wrong*pwd", "" }) {
                            for (boolean allowPubKeyRetrieval : new boolean[] { true, false }) {
                                final Connection testConn;
                                Statement testStmt;

                                boolean expectedPubKeyRetrievalFail = (user.endsWith("_sha")
                                        || user.endsWith("_mnp") && defAuthPlugin.equals(Sha256PasswordPlugin.class)) && !allowPubKeyRetrieval
                                        && pwd.length() > 0;
                                boolean expectedAccessDeniedFail = !user.equals(pwd);
                                System.out.printf("%-25s : %-18s : %-25s : %-25s : %s%n", defAuthPlugin.getSimpleName(), allowPubKeyRetrieval, user, pwd,
                                        expectedPubKeyRetrievalFail ? "Fail [Pub. Key retrieval]" : expectedAccessDeniedFail ? "Fail [Access denied]" : "Ok");

                                final Properties props = new Properties();
                                props.setProperty(PropertyKey.USER.getKeyName(), user);
                                props.setProperty(PropertyKey.PASSWORD.getKeyName(), pwd);
                                props.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), defAuthPlugin.getName());
                                props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), Boolean.toString(allowPubKeyRetrieval));
                                props.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                                if (expectedPubKeyRetrievalFail) {
                                    // connection will fail due to public key retrieval failure
                                    assertThrows(SQLException.class, "Public Key Retrieval is not allowed", new Callable<Void>() {
                                        @SuppressWarnings("synthetic-access")
                                        public Void call() throws Exception {
                                            getConnectionWithProps(sha256Url, props);
                                            return null;
                                        }
                                    });

                                } else if (expectedAccessDeniedFail) {
                                    // connection will fail due to wrong password
                                    assertThrows(SQLException.class, "Access denied for user '" + user + "'@.*", new Callable<Void>() {
                                        @SuppressWarnings("synthetic-access")
                                        public Void call() throws Exception {
                                            getConnectionWithProps(sha256Url, props);
                                            return null;
                                        }
                                    });

                                } else {
                                    // connection will succeed
                                    testConn = getConnectionWithProps(sha256Url, props);
                                    testStmt = testConn.createStatement();
                                    this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                                    assertTrue(this.rs.next());
                                    assertTrue(this.rs.getString(1).startsWith(user));
                                    assertTrue(this.rs.getString(2).startsWith(user));
                                    this.rs.close();
                                    testStmt.close();

                                    // change user using same credentials will succeed
                                    System.out.printf("%25s : %-18s : %-25s : %-25s : %s%n", "| ChangeUser (same)", allowPubKeyRetrieval, user, pwd, "Ok");
                                    ((JdbcConnection) testConn).changeUser(user, user);
                                    testStmt = testConn.createStatement();
                                    this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                                    assertTrue(this.rs.next());
                                    assertTrue(this.rs.getString(1).startsWith(user));
                                    assertTrue(this.rs.getString(2).startsWith(user));
                                    this.rs.close();
                                    testStmt.close();

                                    // change user using different credentials
                                    final String swapUser = user.indexOf("_sha") == -1 ? "bug75670user_sha" : "bug75670user_mnp";
                                    expectedPubKeyRetrievalFail = (swapUser.endsWith("_sha")
                                            || swapUser.endsWith("_mnp") && defAuthPlugin.equals(Sha256PasswordPlugin.class)) && !allowPubKeyRetrieval;
                                    System.out.printf("%25s : %-18s : %-25s : %-25s : %s%n", "| ChangeUser (diff)", allowPubKeyRetrieval, swapUser, swapUser,
                                            expectedPubKeyRetrievalFail ? "Fail [Pub. Key retrieval]" : "Ok");

                                    if (expectedPubKeyRetrievalFail) {
                                        // change user will fail due to public key retrieval failure
                                        assertThrows(SQLException.class, "Public Key Retrieval is not allowed", new Callable<Void>() {
                                            public Void call() throws Exception {
                                                ((JdbcConnection) testConn).changeUser(swapUser, swapUser);
                                                return null;
                                            }
                                        });
                                    } else {
                                        // change user will succeed
                                        ((JdbcConnection) testConn).changeUser(swapUser, swapUser);
                                        testStmt = testConn.createStatement();
                                        this.rs = testStmt.executeQuery("SELECT USER(), CURRENT_USER()");
                                        assertTrue(this.rs.next());
                                        assertTrue(this.rs.getString(1).startsWith(swapUser));
                                        assertTrue(this.rs.getString(2).startsWith(swapUser));
                                        this.rs.close();
                                    }

                                    testConn.close();
                                }
                            }
                        }
                    }
                }
            } finally {
                if (!((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET GLOBAL old_passwords = @current_old_passwords");
                }
            }
        }
    }

    /**
     * Tests fix for Bug#16634180 - LOCK WAIT TIMEOUT EXCEEDED CAUSES SQLEXCEPTION, SHOULD CAUSE SQLTRANSIENTEXCEPTION
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug16634180() throws Exception {

        createTable("testBug16634180", "(pk integer primary key, val integer)", "InnoDB");
        this.stmt.executeUpdate("insert into testBug16634180 values(0,0)");

        Connection c1 = null;
        Connection c2 = null;

        try {
            c1 = getConnectionWithProps(new Properties());
            c1.setAutoCommit(false);
            Statement s1 = c1.createStatement();
            s1.executeUpdate("update testBug16634180 set val=val+1 where pk=0");

            c2 = getConnectionWithProps(new Properties());
            c2.setAutoCommit(false);
            Statement s2 = c2.createStatement();
            try {
                s2.executeUpdate("update testBug16634180 set val=val+1 where pk=0");
                fail("ER_LOCK_WAIT_TIMEOUT should be thrown.");
            } catch (SQLTransientException ex) {
                assertEquals(MysqlErrorNumbers.ER_LOCK_WAIT_TIMEOUT, ex.getErrorCode());
                assertEquals(MysqlErrorNumbers.SQL_STATE_ROLLBACK_SERIALIZATION_FAILURE, ex.getSQLState());
                assertEquals("Lock wait timeout exceeded; try restarting transaction", ex.getMessage());
            }
        } finally {
            if (c1 != null) {
                c1.close();
            }
            if (c2 != null) {
                c2.close();
            }
        }
    }

    /**
     * Tests fix for Bug#21934573 - FABRIC CODE INVOLVED IN THREAD DEADLOCK.
     * (Duplicate Bug#78710 (21966391) - Deadlock on ReplicationConnection and ReplicationConnectionGroup when failover)
     * 
     * Two threads with different Fabric connections using the same server group (and consequently the same {@link ReplicationConnectionGroup}) may hit a
     * deadlock when one executes a failover procedure and the other, simultaneously, calls a method that acquires a lock on the {@link ReplicationConnection}
     * instance monitor.
     * 
     * This happens when, in one thread, a Fabric connection (performing the failover) and while owning a lock on {@link ReplicationConnectionGroup},
     * sequentially tries to lock the object monitor from each {@link ReplicationConnection} belonging to the same {@link ReplicationConnectionGroup}, in the
     * attempt of updating their servers lists by calling the synchronized methods {@link ReplicationConnection#removeMasterHost(String)},
     * {@link ReplicationConnection#addSlaveHost(String)}, {@link ReplicationConnection#removeSlaveHost(String)} or
     * {@link ReplicationConnection#promoteSlaveToMaster(String)} while, at the same time, a second thread is executing one of the synchronized methods from the
     * {@link ReplicationConnection} instance, such as {@link ReplicationConnection#close()} or {@link ReplicationConnection#doPing()} (*), in one of those
     * connections. Later on, the second thread, eventually initiates a failover procedure too and hits the lock on {@link ReplicationConnectionGroup} owned by
     * the first thread. The first thread, at the same time, requires that the lock on {@link ReplicationConnection} is released by the second thread to be able
     * to complete the failover procedure is has initiated before.
     * (*) Executing a query may trigger this too via locking on {@link LoadBalancedConnectionProxy}.
     * 
     * This test simulates the way Fabric connections operate when they need to synchronize the list of servers from a {@link ReplicationConnection} with the
     * Fabric's server group. In that operation we, like Fabric connections, use an {@link ExceptionInterceptor} that ends up changing the
     * {@link ReplicationConnection}s from a given {@link ReplicationConnectionGroup}.
     * 
     * This test is unable to cover the failing scenario since the fix in the main code was also reproduced here, with the addition of the {@link ReentrantLock}
     * {@code singleSynchWorkerMonitor} in the {@link TestBug21934573ExceptionInterceptor} the same way as in {@link ErrorReportingExceptionInterceptor}. The
     * way to reproduce it and observe the deadlock happening is by setting the connection property {@code __useReplConnGroupLocks__} to {@code False}.
     * 
     * WARNING! If this test fails there is no guarantee that the JVM will remain stable and won't affect any other tests. It is imperative that this test
     * passes to ensure other tests results.
     */
    public void testBug21934573() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.exceptionInterceptors.getKeyName(), TestBug21934573ExceptionInterceptor.class.getName());
        props.setProperty(PropertyKey.replicationConnectionGroup.getKeyName(), "deadlock");
        props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "true");
        props.setProperty("__useReplConnGroupLocks__", "true"); // Set this to 'false' to observe the deadlock.

        final Connection connA = getMasterSlaveReplicationConnection(props);
        final Connection connB = getMasterSlaveReplicationConnection(props);

        for (final Connection testConn : new Connection[] { connA, connB }) {
            new Thread(new Runnable() {
                public void run() {
                    try {
                        // Lock on testConn to emulate runtime locking behavior of Repl/LB connections.
                        synchronized (testConn) {
                            testConn.createStatement().executeQuery("SELECT column FROM table");
                        }
                    } catch (Exception e) {
                    }
                }
            }, testConn.getClass().getSimpleName() + "@" + Integer.toHexString(System.identityHashCode(testConn)) + "_thread").start();
        }

        TestBug21934573ExceptionInterceptor.initialized = true;

        // Let the two concurrent threads run concurrently for 2secs, at the most, before checking if they hit a deadlock situation.
        // Wait two times 1sec as TestBug21934573ExceptionInterceptor.mainThreadLock.notify() should be called twice (once per secondary thread).
        synchronized (TestBug21934573ExceptionInterceptor.mainThreadLock) {
            TestBug21934573ExceptionInterceptor.mainThreadLock.wait(1000);
            TestBug21934573ExceptionInterceptor.mainThreadLock.wait(1000);
        }

        int deadlockCount = 0;
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        long[] threadIds = threadMXBean.findMonitorDeadlockedThreads();
        if (threadIds != null) {
            ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds, Integer.MAX_VALUE);
            for (ThreadInfo ti : threadInfos) {
                System.out.println();
                System.out.println(ti);
                System.out.println("Stack trace:");
                for (StackTraceElement ste : ti.getStackTrace()) {
                    System.out.println("   " + ste);
                }
                if (ti.getThreadName().equals("early_syncing_thread") || ti.getThreadName().equals("late_syncing_thread")) {
                    deadlockCount++;
                }
            }
            if (deadlockCount == 2) {// Acquire the connection's monitor to mimic the behavior of other synchronized methods (like close() or doPing()).
                fail("Deadlock detected. WARNING: this failure may lead to JVM instability.");
            } else {
                fail("Unexpected deadlock detected. Consult system output for more details. WARNING: this failure may lead to JVM instability.");
            }
        }
    }

    /*
     * Mimics the behavior of ErrorReportingExceptionInterceptor/FabricMySQLConnectionProxy.syncGroupServersToReplicationConnectionGroup() but actuates on any
     * SQLException (not only communication related exceptions) and calls directly methods changing servers lists from ReplicationConnectionGroup.
     */
    public static class TestBug21934573ExceptionInterceptor implements ExceptionInterceptor {
        static boolean initialized = false;
        static Object mainThreadLock = new Object();
        private static boolean threadIsWaiting = false;
        private static final Set<String> replConnGroupLocks = Collections.synchronizedSet(new HashSet<String>());

        private boolean useSyncGroupServersLock = true;

        public ExceptionInterceptor init(Properties props, Log log) {
            if (props.containsKey("__useReplConnGroupLocks__")) {
                this.useSyncGroupServersLock = Boolean.parseBoolean(props.getProperty("__useReplConnGroupLocks__"));
            }
            return this;
        }

        public void destroy() {
        }

        public Exception interceptException(Exception sqlEx) {
            if (!initialized) {
                return sqlEx;
            }

            // Make sure both threads execute the code after the synchronized block concurrently.
            synchronized (TestBug21934573ExceptionInterceptor.class) {
                if (threadIsWaiting) {
                    TestBug21934573ExceptionInterceptor.class.notify();
                } else {
                    threadIsWaiting = true;
                    try {
                        TestBug21934573ExceptionInterceptor.class.wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

            ReplicationConnectionGroup replConnGrp = ReplicationConnectionGroupManager.getConnectionGroup("deadlock");
            if (!this.useSyncGroupServersLock || replConnGroupLocks.add(replConnGrp.getGroupName())) {
                try {
                    System.out.println("Emulating syncing state in: " + replConnGrp + " on thread " + Thread.currentThread().getName() + ".");
                    replConnGrp.removeMasterHost("localhost:1234");
                    replConnGrp.addSlaveHost("localhost:1234");
                    replConnGrp.removeSlaveHost("localhost:1234", false);
                    replConnGrp.promoteSlaveToMaster("localhost:1234");
                } catch (SQLException ex) {
                    throw new RuntimeException(ex);
                } finally {
                    if (this.useSyncGroupServersLock) {
                        replConnGroupLocks.remove(replConnGrp.getGroupName());
                    }
                }
            } else {
                System.out.println("Giving up syncing state on thread " + Thread.currentThread() + ". Let the other thread do it!");
            }

            synchronized (TestBug21934573ExceptionInterceptor.mainThreadLock) {
                TestBug21934573ExceptionInterceptor.mainThreadLock.notify();
            }
            return null;
        }
    }

    /**
     * Tests fix for BUG#21947042, PREFER TLS WHERE SUPPORTED BY MYSQL SERVER.
     * 
     * Requires test certificates from testsuite/ssl-test-certs to be installed
     * on the server being tested.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug21947042() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", "");
        System.setProperty("javax.net.ssl.trustStorePassword", "");

        Connection sslConn = null;
        Properties props = new Properties();

        // 1. No explicit useSSL
        sslConn = getConnectionWithProps(props);
        assertFalse(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.PREFERRED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());

        testBug21947042_PrintCipher(sslConn);
        testBug21947042_PrintVersion(sslConn);
        sslConn.close();

        // 2. Explicit useSSL=false
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.DISABLED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertFalse(((MysqlConnection) sslConn).getSession().isSSLEstablished());

        testBug21947042_PrintCipher(sslConn);
        testBug21947042_PrintVersion(sslConn);
        sslConn.close();

        props.setProperty(PropertyKey.useSSL.getKeyName(), "no");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.DISABLED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertFalse(((MysqlConnection) sslConn).getSession().isSSLEstablished());

        testBug21947042_PrintCipher(sslConn);
        testBug21947042_PrintVersion(sslConn);
        sslConn.close();

        // 2.1. Explicit useSSL=false, requireSSL=true
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.requireSSL.getKeyName(), "true");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.DISABLED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertFalse(((MysqlConnection) sslConn).getSession().isSSLEstablished());

        testBug21947042_PrintCipher(sslConn);
        testBug21947042_PrintVersion(sslConn);
        sslConn.close();

        props.setProperty(PropertyKey.useSSL.getKeyName(), "no");
        props.setProperty(PropertyKey.requireSSL.getKeyName(), "yes");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.DISABLED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertFalse(((MysqlConnection) sslConn).getSession().isSSLEstablished());

        testBug21947042_PrintCipher(sslConn);
        testBug21947042_PrintVersion(sslConn);
        sslConn.close();
        props.remove(PropertyKey.requireSSL.getKeyName());

        // 3. Explicit useSSL=true
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        sslConn = getConnectionWithProps(props);
        assertFalse(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.PREFERRED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());

        testBug21947042_PrintCipher(sslConn);
        testBug21947042_PrintVersion(sslConn);
        sslConn.close();

        props.setProperty(PropertyKey.useSSL.getKeyName(), "yes");
        sslConn = getConnectionWithProps(props);
        assertFalse(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.PREFERRED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());

        testBug21947042_PrintCipher(sslConn);
        testBug21947042_PrintVersion(sslConn);
        sslConn.close();

        // 3.1. Explicit useSSL=true, requireSSL=true
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.requireSSL.getKeyName(), "true");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.REQUIRED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());

        testBug21947042_PrintCipher(sslConn);
        testBug21947042_PrintVersion(sslConn);
        sslConn.close();

        props.setProperty(PropertyKey.useSSL.getKeyName(), "yes");
        props.setProperty(PropertyKey.requireSSL.getKeyName(), "yes");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.REQUIRED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());

        testBug21947042_PrintCipher(sslConn);
        testBug21947042_PrintVersion(sslConn);
        sslConn.close();

        // 4. Explicit useSSL=true, verifyServerCertificate=true, no trust store
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "true");
        assertThrows(SQLException.class, new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "yes");
        assertThrows(SQLException.class, new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        // 5. Explicit useSSL=true, verifyServerCertificate=true
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "true");
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.VERIFY_CA, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());

        testBug21947042_PrintCipher(sslConn);
        testBug21947042_PrintVersion(sslConn);
        sslConn.close();

        props.setProperty(PropertyKey.useSSL.getKeyName(), "yes");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "yes");
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.VERIFY_CA, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());

        testBug21947042_PrintCipher(sslConn);
        testBug21947042_PrintVersion(sslConn);
        sslConn.close();
    }

    private void testBug21947042_PrintCipher(Connection con) throws Exception {
        ResultSet rset = con.createStatement().executeQuery("SHOW STATUS LIKE 'ssl_cipher'");
        assertTrue(rset.next());
        String cipher = rset.getString(2);
        System.out.println("ssl_cipher=" + cipher);
    }

    private void testBug21947042_PrintVersion(Connection con) throws Exception {
        ResultSet rset = con.createStatement().executeQuery("SHOW STATUS LIKE 'ssl_version'");
        assertTrue(rset.next());
        String version = rset.getString(2);
        System.out.println("ssl_version=" + version);
    }

    /**
     * Tests fix for Bug#56100 - Replication driver routes DML statements to read-only slaves.
     */
    public void testBug56100() throws Exception {
        final String port = getPort(null);
        final String hostMaster = "master:" + port;
        final String hostSlave = "slave:" + port;

        final Properties props = new Properties();
        props.setProperty(PropertyKey.queryInterceptors.getKeyName(), Bug56100QueryInterceptor.class.getName());

        final ReplicationConnection testConn = getUnreliableReplicationConnection(new String[] { "master", "slave" }, props);

        assertTrue(testConn.isHostMaster(hostMaster));
        assertTrue(testConn.isHostSlave(hostSlave));

        // verify that current connection is 'master'
        assertTrue(testConn.isMasterConnection());

        final Statement testStmt1 = testConn.createStatement();
        testBug56100AssertHost(testStmt1, "master");

        // set connection to read-only state and verify that current connection is 'slave' now
        testConn.setReadOnly(true);
        assertFalse(testConn.isMasterConnection());

        final Statement testStmt2 = testConn.createStatement();
        testBug56100AssertHost(testStmt1, "slave");
        testBug56100AssertHost(testStmt2, "slave");

        // set connection to read/write state and verify that current connection is 'master' again
        testConn.setReadOnly(false);
        assertTrue(testConn.isMasterConnection());

        final Statement testStmt3 = testConn.createStatement();
        testBug56100AssertHost(testStmt1, "master");
        testBug56100AssertHost(testStmt2, "master");
        testBug56100AssertHost(testStmt3, "master");

        // let Connection.close() also close open statements
        testConn.close();

        assertThrows(SQLException.class, "No operations allowed after statement closed.", new Callable<Void>() {
            public Void call() throws Exception {
                testStmt1.execute("SELECT 'Bug56100'");
                return null;
            }
        });

        assertThrows(SQLException.class, "No operations allowed after statement closed.", new Callable<Void>() {
            public Void call() throws Exception {
                testStmt2.execute("SELECT 'Bug56100'");
                return null;
            }
        });

        assertThrows(SQLException.class, "No operations allowed after statement closed.", new Callable<Void>() {
            public Void call() throws Exception {
                testStmt3.execute("SELECT 'Bug56100'");
                return null;
            }
        });
    }

    private void testBug56100AssertHost(Statement testStmt, String expectedHost) throws SQLException {
        this.rs = testStmt.executeQuery("SELECT '<HOST_NAME>'");
        assertTrue(this.rs.next());
        assertEquals(expectedHost, this.rs.getString(1));
        this.rs.close();
    }

    public static class Bug56100QueryInterceptor extends BaseQueryInterceptor {
        private JdbcConnection connection;

        @Override
        public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
            this.connection = (JdbcConnection) conn;
            return this;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends Resultset> T preProcess(Supplier<String> str, Query interceptedQuery) {
            String sql = str.get();
            if (sql.contains("<HOST_NAME>")) {
                try {
                    return (T) ((Statement) interceptedQuery).executeQuery(sql.replace("<HOST_NAME>", this.connection.getHost()));
                } catch (SQLException ex) {
                    throw ExceptionFactory.createException(ex.getMessage(), ex);
                }
            }

            return super.preProcess(() -> {
                return sql;
            }, interceptedQuery);
        }

        @Override
        public void destroy() {
            this.connection = null;
        }
    }

    /**
     * Tests fix for WL#8196, Support for TLSv1.2 Protocol.
     * 
     * This test requires community server (preferably compiled with yaSSL) in -Dcom.mysql.cj.testsuite.url and commercial server (with OpenSSL) in
     * -Dcom.mysql.cj.testsuite.url.openssl
     * 
     * Test certificates from test/config/ssl-test-certs must be installed on both servers.
     */
    public void testTLSVersion() throws Exception {
        // Find out which TLS protocol versions are supported by this JVM.
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, null, null);
        List<String> jvmSupportedProtocols = Arrays.asList(sslContext.createSSLEngine().getSupportedProtocols());

        final String[] testDbUrls;
        Properties props = new Properties();
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.requireSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");

        if (this.sha256Conn != null && ((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 5, 7)) {
            testDbUrls = new String[] { BaseTestCase.dbUrl, sha256Url };
        } else {
            testDbUrls = new String[] { BaseTestCase.dbUrl };
        }

        for (String testDbUrl : testDbUrls) {
            System.out.println(testDbUrl);
            System.out.println("JVM version: " + System.getProperty(PropertyDefinitions.SYSP_java_version));
            System.out.println("JVM supports TLS protocols: " + jvmSupportedProtocols);
            Connection sslConn = getConnectionWithProps(testDbUrl, props);
            assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());
            System.out.println("MySQL version: " + ((MysqlConnection) sslConn).getSession().getServerSession().getServerVersion());
            this.rs = sslConn.createStatement().executeQuery("SHOW STATUS LIKE 'ssl_version'");
            assertTrue(this.rs.next());
            String tlsVersionUsed = this.rs.getString(2);
            System.out.println("TLS version used: " + tlsVersionUsed);

            if (((JdbcConnection) sslConn).getSession().versionMeetsMinimum(5, 7, 10)) {
                this.rs = sslConn.createStatement().executeQuery("SHOW GLOBAL VARIABLES LIKE 'tls_version'");
                assertTrue(this.rs.next());
                List<String> serverSupportedProtocols = Arrays.asList(this.rs.getString(2).trim().split("\\s*,\\s*"));
                String highestCommonTlsVersion = "";
                for (String p : new String[] { "TLSv1.2", "TLSv1.1", "TLSv1" }) {
                    if (jvmSupportedProtocols.contains(p) && serverSupportedProtocols.contains(p)) {
                        highestCommonTlsVersion = p;
                        break;
                    }
                }
                System.out.println("Server supports TLS protocols: " + serverSupportedProtocols);
                System.out.println("Highest common TLS protocol: " + highestCommonTlsVersion);

                assertEquals(highestCommonTlsVersion, tlsVersionUsed);
            } else {
                assertEquals("TLSv1", tlsVersionUsed);
            }
            System.out.println();

            sslConn.close();
        }
    }

    /**
     * Tests fix for Bug#87379. This allows TLS version to be overridden through a new configuration option - enabledTLSProtocols. When set to some combination
     * of TLSv1, TLSv1.1, or TLSv1.2 (comma-separated, no spaces), the default behavior restricting the TLS version based on JRE and MySQL Server version is
     * bypassed to enable or restrict specific TLS versions.
     * 
     * This test requires community server (preferably compiled with yaSSL) in -Dcom.mysql.cj.testsuite.url and commercial server (with OpenSSL) in
     * -Dcom.mysql.cj.testsuite.url.openssl
     * 
     * Test certificates from testsuite/ssl-test-certs must be installed on both servers.
     */
    public void testEnableTLSVersion() throws Exception {
        // Find out which TLS protocol versions are supported by this JVM.
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, null, null);
        List<String> jvmSupportedProtocols = Arrays.asList(sslContext.createSSLEngine().getSupportedProtocols());

        final String[] testDbUrls;
        Properties props = new Properties();
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.requireSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");

        if (this.sha256Conn != null && ((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 5, 7)) {
            testDbUrls = new String[] { BaseTestCase.dbUrl, sha256Url };
        } else {
            testDbUrls = new String[] { BaseTestCase.dbUrl };
        }

        for (String testDbUrl : testDbUrls) {
            System.out.println(testDbUrl);
            System.out.println("JVM version: " + System.getProperty(PropertyDefinitions.SYSP_java_version));
            System.out.println("JVM supports TLS protocols: " + jvmSupportedProtocols);
            Connection sslConn = getConnectionWithProps(testDbUrl, props);
            assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());
            System.out.println("MySQL version: " + ((MysqlConnection) sslConn).getSession().getServerSession().getServerVersion());
            List<String> commonSupportedProtocols = new ArrayList<>();
            if (((JdbcConnection) sslConn).getSession().versionMeetsMinimum(5, 7, 10)) {
                this.rs = sslConn.createStatement().executeQuery("SHOW GLOBAL VARIABLES LIKE 'tls_version'");
                assertTrue(this.rs.next());
                List<String> serverSupportedProtocols = Arrays.asList(this.rs.getString(2).trim().split("\\s*,\\s*"));
                System.out.println("Server supports TLS protocols: " + serverSupportedProtocols);
                commonSupportedProtocols.addAll(serverSupportedProtocols);
                commonSupportedProtocols.retainAll(jvmSupportedProtocols);
            } else {
                commonSupportedProtocols.add("TLSv1");
            }

            String[] testingProtocols = { "TLSv1.2", "TLSv1.1", "TLSv1" };
            for (String protocol : testingProtocols) {
                Properties testProps = new Properties();
                testProps.putAll(props);
                testProps.setProperty(PropertyKey.enabledTLSProtocols.getKeyName(), protocol);
                System.out.println("Testing " + protocol + " expecting connection: " + commonSupportedProtocols.contains(protocol));
                try {
                    Connection tlsConn = getConnectionWithProps(testDbUrl, testProps);
                    if (!commonSupportedProtocols.contains(protocol)) {
                        fail("Expected to fail connection with " + protocol + " due to lack of jvm/server support.");
                    }
                    ResultSet rset = tlsConn.createStatement().executeQuery("SHOW STATUS LIKE 'ssl_version'");
                    assertTrue(rset.next());
                    String tlsVersion = rset.getString(2);
                    assertEquals(protocol, tlsVersion);
                    tlsConn.close();
                } catch (Exception e) {
                    if (commonSupportedProtocols.contains(protocol)) {
                        e.printStackTrace();
                        fail("Expected to be able to connect with " + protocol + " protocol, but failed.");
                    }
                }
            }
            System.out.println();
            sslConn.close();
        }
    }

    /**
     * Tests fix for Bug#56122 - JDBC4 functionality failure when using replication connections.
     */
    public void testBug56122() throws Exception {
        for (final Connection testConn : new Connection[] { this.conn, getFailoverConnection(), getLoadBalancedConnection(),
                getMasterSlaveReplicationConnection() }) {
            testConn.createClob();
            testConn.createBlob();
            testConn.createNClob();
            testConn.createSQLXML();
            testConn.isValid(12345);
            testConn.setClientInfo(new Properties());
            testConn.setClientInfo("NAME", "VALUE");
            testConn.getClientInfo();
            testConn.getClientInfo("CLIENT");
            assertThrows(SQLFeatureNotSupportedException.class, new Callable<Void>() {
                public Void call() throws Exception {
                    testConn.createArrayOf("A_TYPE", null);
                    return null;
                }
            });
            assertThrows(SQLFeatureNotSupportedException.class, new Callable<Void>() {
                public Void call() throws Exception {
                    testConn.createStruct("A_TYPE", null);
                    return null;
                }
            });
        }
    }

    /**
     * Tests fix for Bug#21286268 - CONNECTOR/J REPLICATION USE MASTER IF SLAVE IS UNAVAILABLE.
     */
    public void testBug21286268() throws Exception {
        final String MASTER = "master";
        final String SLAVE = "slave";

        final String MASTER_OK = UnreliableSocketFactory.getHostConnectedStatus(MASTER);
        final String MASTER_FAIL = UnreliableSocketFactory.getHostFailedStatus(MASTER);
        final String SLAVE_OK = UnreliableSocketFactory.getHostConnectedStatus(SLAVE);
        final String SLAVE_FAIL = UnreliableSocketFactory.getHostFailedStatus(SLAVE);

        final String[] hosts = new String[] { MASTER, SLAVE };
        final Properties props = new Properties();
        props.setProperty(PropertyKey.connectTimeout.getKeyName(), "100");
        props.setProperty(PropertyKey.retriesAllDown.getKeyName(), "2"); // Failed connection attempts will show up twice.
        final Set<String> downedHosts = new HashSet<>();
        Connection testConn = null;

        /*
         * Initialization case 1: Masters and Slaves up.
         */
        downedHosts.clear();
        UnreliableSocketFactory.flushAllStaticData();

        testConn = getUnreliableReplicationConnection(hosts, props, downedHosts);
        assertConnectionsHistory(SLAVE_OK, MASTER_OK);
        testBug21286268AssertConnectedToAndReadOnly(testConn, MASTER, false);

        /*
         * Initialization case 2a: Masters up and Slaves down (readFromMasterWhenNoSlaves=false).
         */
        props.setProperty(PropertyKey.readFromMasterWhenNoSlaves.getKeyName(), "false");
        downedHosts.clear();
        downedHosts.add(SLAVE);
        UnreliableSocketFactory.flushAllStaticData();

        assertThrows(SQLException.class, "(?s)Communications link failure.*", new Callable<Void>() {
            @SuppressWarnings("synthetic-access")
            public Void call() throws Exception {
                getUnreliableReplicationConnection(hosts, props, downedHosts);
                return null;
            }
        });
        assertConnectionsHistory(SLAVE_FAIL);
        props.remove(PropertyKey.readFromMasterWhenNoSlaves.getKeyName());

        /*
         * Initialization case 2b: Masters up and Slaves down (allowSlaveDownConnections=true).
         */
        props.setProperty(PropertyKey.allowSlaveDownConnections.getKeyName(), "true");
        downedHosts.clear();
        downedHosts.add(SLAVE);
        UnreliableSocketFactory.flushAllStaticData();

        testConn = getUnreliableReplicationConnection(hosts, props, downedHosts);
        assertConnectionsHistory(SLAVE_FAIL, MASTER_OK);
        testBug21286268AssertConnectedToAndReadOnly(testConn, MASTER, false);
        props.remove(PropertyKey.allowSlaveDownConnections.getKeyName());

        /*
         * Initialization case 3a: Masters down and Slaves up (allowSlaveDownConnections=false).
         */
        props.setProperty(PropertyKey.allowSlaveDownConnections.getKeyName(), "false");
        downedHosts.clear();
        downedHosts.add(MASTER);
        UnreliableSocketFactory.flushAllStaticData();

        assertThrows(SQLException.class, "(?s)Communications link failure.*", new Callable<Void>() {
            @SuppressWarnings("synthetic-access")
            public Void call() throws Exception {
                getUnreliableReplicationConnection(hosts, props, downedHosts);
                return null;
            }
        });
        assertConnectionsHistory(SLAVE_OK, MASTER_FAIL, MASTER_FAIL);
        props.remove(PropertyKey.allowSlaveDownConnections.getKeyName());

        /*
         * Initialization case 3b: Masters down and Slaves up (allowMasterDownConnections=true).
         */
        props.setProperty(PropertyKey.allowMasterDownConnections.getKeyName(), "true");
        downedHosts.clear();
        downedHosts.add(MASTER);
        UnreliableSocketFactory.flushAllStaticData();

        testConn = getUnreliableReplicationConnection(hosts, props, downedHosts);
        assertConnectionsHistory(SLAVE_OK, MASTER_FAIL, MASTER_FAIL);
        testBug21286268AssertConnectedToAndReadOnly(testConn, SLAVE, true);
        props.remove(PropertyKey.allowMasterDownConnections.getKeyName());

        /*
         * Initialization case 4: Masters down and Slaves down (allowMasterDownConnections=[false|true] + allowSlaveDownConnections=[false|true]).
         */
        for (int tst = 0; tst < 4; tst++) {
            boolean allowMasterDownConnections = (tst & 0x1) != 0;
            boolean allowSlaveDownConnections = (tst & 0x2) != 0;

            String testCase = String.format("Case: %d [ %s | %s ]", tst, allowMasterDownConnections ? "alwMstDn" : "-",
                    allowSlaveDownConnections ? "alwSlvDn" : "-");
            System.out.println(testCase);

            props.setProperty(PropertyKey.allowMasterDownConnections.getKeyName(), Boolean.toString(allowMasterDownConnections));
            props.setProperty(PropertyKey.allowSlaveDownConnections.getKeyName(), Boolean.toString(allowSlaveDownConnections));
            downedHosts.clear();
            downedHosts.add(MASTER);
            downedHosts.add(SLAVE);
            UnreliableSocketFactory.flushAllStaticData();

            assertThrows(SQLException.class, "(?s)Communications link failure.*", new Callable<Void>() {
                @SuppressWarnings("synthetic-access")
                public Void call() throws Exception {
                    getUnreliableReplicationConnection(hosts, props, downedHosts);
                    return null;
                }
            });
            if (allowSlaveDownConnections) {
                assertConnectionsHistory(SLAVE_FAIL, SLAVE_FAIL, MASTER_FAIL, MASTER_FAIL);
            } else {
                assertConnectionsHistory(SLAVE_FAIL, SLAVE_FAIL);
            }
            props.remove(PropertyKey.allowMasterDownConnections.getKeyName());
            props.remove(PropertyKey.allowSlaveDownConnections.getKeyName());
        }

        /*
         * Run-time case 1: Switching between masters and slaves.
         */
        downedHosts.clear();
        UnreliableSocketFactory.flushAllStaticData();

        // Use Masters.
        testConn = getUnreliableReplicationConnection(hosts, props, downedHosts);
        assertConnectionsHistory(SLAVE_OK, MASTER_OK);
        testBug21286268AssertConnectedToAndReadOnly(testConn, MASTER, false);

        // Use Slaves.
        testConn.setReadOnly(true);
        testBug21286268AssertConnectedToAndReadOnly(testConn, SLAVE, true);

        // Use Masters.
        testConn.setReadOnly(false);
        testBug21286268AssertConnectedToAndReadOnly(testConn, MASTER, false);

        /*
         * Run-time case 2a: Running with Masters down (Masters connection doesn't recover).
         */
        downedHosts.clear();
        UnreliableSocketFactory.flushAllStaticData();

        // Use Masters.
        testConn = getUnreliableReplicationConnection(hosts, props, downedHosts);
        testBug21286268AssertConnectedToAndReadOnly(testConn, MASTER, false);

        // Master server down.
        UnreliableSocketFactory.downHost(MASTER);

        // Use Slaves.
        testConn.setReadOnly(true);
        testBug21286268AssertConnectedToAndReadOnly(testConn, SLAVE, true);
        assertConnectionsHistory(SLAVE_OK, MASTER_OK); // No changes so far.

        // Use Masters.
        testConn.setReadOnly(false);
        assertConnectionsHistory(SLAVE_OK, MASTER_OK, MASTER_FAIL, MASTER_FAIL); // Failed re-initializing Masters.

        {
            final Connection localTestConn = testConn;
            assertThrows(SQLException.class, "(?s)No operations allowed after connection closed.*", new Callable<Void>() {
                public Void call() throws Exception {
                    localTestConn.createStatement().execute("SELECT 1");
                    return null;
                }
            });
        }
        assertConnectionsHistory(SLAVE_OK, MASTER_OK, MASTER_FAIL, MASTER_FAIL); // No changes so far.

        /*
         * Run-time case 2b: Running with Masters down (Masters connection recover in time).
         */
        downedHosts.clear();
        UnreliableSocketFactory.flushAllStaticData();

        // Use Masters.
        testConn = getUnreliableReplicationConnection(hosts, props, downedHosts);
        assertConnectionsHistory(SLAVE_OK, MASTER_OK);
        testBug21286268AssertConnectedToAndReadOnly(testConn, MASTER, false);

        // Find Masters conn ID.
        long connId = ((MysqlConnection) testConn).getSession().getThreadId();

        // Master server down.
        UnreliableSocketFactory.downHost(MASTER);
        this.stmt.execute("KILL CONNECTION " + connId); // Actually kill the Masters connection at server side.

        // Use Slaves.
        testConn.setReadOnly(true);
        testBug21286268AssertConnectedToAndReadOnly(testConn, SLAVE, true);

        // Master server up.
        UnreliableSocketFactory.dontDownHost(MASTER);

        // Use Masters.
        testConn.setReadOnly(false);
        assertConnectionsHistory(SLAVE_OK, MASTER_OK, MASTER_OK); // Masters connection re-initialized.
        testBug21286268AssertConnectedToAndReadOnly(testConn, MASTER, false);

        /*
         * Run-time case 3a: Running with Slaves down (readFromMasterWhenNoSlaves=false).
         */
        props.setProperty(PropertyKey.readFromMasterWhenNoSlaves.getKeyName(), "false");
        downedHosts.clear();
        UnreliableSocketFactory.flushAllStaticData();

        // Use Masters.
        testConn = getUnreliableReplicationConnection(hosts, props, downedHosts);
        assertConnectionsHistory(SLAVE_OK, MASTER_OK);
        testBug21286268AssertConnectedToAndReadOnly(testConn, MASTER, false);

        // Find Slaves conn ID.
        testConn.setReadOnly(true);
        connId = ((MysqlConnection) testConn).getSession().getThreadId();
        testBug21286268AssertConnectedToAndReadOnly(testConn, SLAVE, true);
        testConn.setReadOnly(false);
        testBug21286268AssertConnectedToAndReadOnly(testConn, MASTER, false);

        // Slave server down.
        UnreliableSocketFactory.downHost(SLAVE);
        this.stmt.execute("KILL CONNECTION " + connId); // Actually kill the Slaves connection at server side.

        // Use Slaves.
        testConn.setReadOnly(true);
        assertConnectionsHistory(SLAVE_OK, MASTER_OK, SLAVE_FAIL, SLAVE_FAIL); // Failed re-initializing Slaves.

        {
            final Connection localTestConn = testConn;
            assertThrows(SQLException.class, "(?s)No operations allowed after connection closed.*", new Callable<Void>() {
                public Void call() throws Exception {
                    localTestConn.createStatement().execute("SELECT 1");
                    return null;
                }
            });
        }
        assertConnectionsHistory(SLAVE_OK, MASTER_OK, SLAVE_FAIL, SLAVE_FAIL); // No changes so far.

        // Retry using Slaves. Will fail indefinitely.
        {
            final Connection localTestConn = testConn;
            assertThrows(SQLException.class, "(?s)Communications link failure.*", new Callable<Void>() {
                public Void call() throws Exception {
                    localTestConn.setReadOnly(true);
                    return null;
                }
            });
        }
        assertConnectionsHistory(SLAVE_OK, MASTER_OK, SLAVE_FAIL, SLAVE_FAIL, SLAVE_FAIL, SLAVE_FAIL); // Failed connecting to Slaves.

        /*
         * Run-time case 3b: Running with Slaves down (readFromMasterWhenNoSlaves=true).
         */
        props.setProperty(PropertyKey.readFromMasterWhenNoSlaves.getKeyName(), "true");
        downedHosts.clear();
        UnreliableSocketFactory.flushAllStaticData();

        // Use Masters.
        testConn = getUnreliableReplicationConnection(hosts, props, downedHosts);
        assertConnectionsHistory(SLAVE_OK, MASTER_OK);
        testBug21286268AssertConnectedToAndReadOnly(testConn, MASTER, false);

        // Find Slaves conn ID.
        testConn.setReadOnly(true);
        connId = ((MysqlConnection) testConn).getSession().getThreadId();
        testBug21286268AssertConnectedToAndReadOnly(testConn, SLAVE, true);
        testConn.setReadOnly(false);
        testBug21286268AssertConnectedToAndReadOnly(testConn, MASTER, false);

        // Slave server down.
        UnreliableSocketFactory.downHost(SLAVE);
        this.stmt.execute("KILL CONNECTION " + connId); // Actually kill the Slaves connection at server side.

        // Use Slaves.
        testConn.setReadOnly(true);
        assertConnectionsHistory(SLAVE_OK, MASTER_OK, SLAVE_FAIL, SLAVE_FAIL); // Failed re-initializing Slaves.

        {
            final Connection localTestConn = testConn;
            assertThrows(SQLException.class, "(?s)No operations allowed after connection closed.*", new Callable<Void>() {
                public Void call() throws Exception {
                    localTestConn.createStatement().execute("SELECT 1");
                    return null;
                }
            });
        }
        assertConnectionsHistory(SLAVE_OK, MASTER_OK, SLAVE_FAIL, SLAVE_FAIL); // No changes so far.

        // Retry using Slaves. Will fall-back to Masters as read-only.
        testConn.setReadOnly(true);
        assertConnectionsHistory(SLAVE_OK, MASTER_OK, SLAVE_FAIL, SLAVE_FAIL, SLAVE_FAIL, SLAVE_FAIL); // Failed connecting to Slaves, failed-over to Masters.
        testBug21286268AssertConnectedToAndReadOnly(testConn, MASTER, true);

        // Use Masters.
        testConn.setReadOnly(false);
        testBug21286268AssertConnectedToAndReadOnly(testConn, MASTER, false);

        // Slave server up.
        UnreliableSocketFactory.dontDownHost(SLAVE);

        // Use Slaves.
        testConn.setReadOnly(true);
        assertConnectionsHistory(SLAVE_OK, MASTER_OK, SLAVE_FAIL, SLAVE_FAIL, SLAVE_FAIL, SLAVE_FAIL, SLAVE_OK); // Slaves connection re-initialized.
        testBug21286268AssertConnectedToAndReadOnly(testConn, SLAVE, true);
        props.remove(PropertyKey.readFromMasterWhenNoSlaves.getKeyName());
    }

    private void testBug21286268AssertConnectedToAndReadOnly(Connection testConn, String expectedHost, boolean expectedReadOnly) throws SQLException {
        this.rs = testConn.createStatement().executeQuery("SELECT 1");
        assertEquals(expectedHost, ((JdbcConnection) testConn).getHost());
        assertEquals(expectedReadOnly, testConn.isReadOnly());
    }

    /**
     * Tests fix for Bug#77171 - On every connect getting sql_mode from server creates unnecessary exception.
     * 
     * This fix is a refactoring on ConnectorImpl.initializePropsFromServer() to improve performance when processing the SQL_MODE value. No behavior was
     * changed. This test guarantees that nothing was broken in these matters, for the relevant MySQL versions, after this fix.
     */
    public void testBug77171() throws Exception {
        String sqlMode = getMysqlVariable("sql_mode");
        sqlMode = removeSqlMode("ANSI_QUOTES", sqlMode);
        sqlMode = removeSqlMode("NO_BACKSLASH_ESCAPES", sqlMode);
        String newSqlMode = sqlMode;
        if (sqlMode.length() > 0) {
            sqlMode += ",";
        }

        Properties props = new Properties();
        props.put("sessionVariables", "sql_mode='" + newSqlMode + "'");
        Connection testConn = getConnectionWithProps(props);
        assertFalse(((JdbcConnection) testConn).getSession().getServerSession().useAnsiQuotedIdentifiers());
        assertFalse(((JdbcConnection) testConn).getSession().getServerSession().isNoBackslashEscapesSet());
        testConn.close();

        props.clear();
        newSqlMode = sqlMode + "ANSI_QUOTES";
        props.put("sessionVariables", "sql_mode='" + newSqlMode + "'");
        testConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) testConn).getSession().getServerSession().useAnsiQuotedIdentifiers());
        assertFalse(((JdbcConnection) testConn).getSession().getServerSession().isNoBackslashEscapesSet());
        testConn.close();

        props.clear();
        newSqlMode = sqlMode + "NO_BACKSLASH_ESCAPES";
        props.put("sessionVariables", "sql_mode='" + newSqlMode + "'");
        testConn = getConnectionWithProps(props);
        assertFalse(((JdbcConnection) testConn).getSession().getServerSession().useAnsiQuotedIdentifiers());
        assertTrue(((JdbcConnection) testConn).getSession().getServerSession().isNoBackslashEscapesSet());
        testConn.close();

        props.clear();
        newSqlMode = sqlMode + "ANSI_QUOTES,NO_BACKSLASH_ESCAPES";
        props.put("sessionVariables", "sql_mode='" + newSqlMode + "'");
        testConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) testConn).getSession().getServerSession().useAnsiQuotedIdentifiers());
        assertTrue(((JdbcConnection) testConn).getSession().getServerSession().isNoBackslashEscapesSet());
        testConn.close();
    }

    /**
     * Tests fix for Bug#22730682 - ARRAYINDEXOUTOFBOUNDSEXCEPTION FROM CONNECTIONGROUPMANAGER.REMOVEHOST().
     * 
     * This bug was caused by an incorrect array handling when removing an host from a load balanced connection group, with the option to affect existing
     * connections.
     */
    public void testBug22730682() throws Exception {
        final String currentHost = mainConnectionUrl.getMainHost().getHostPortPair();
        final String dummyHost = "bug22730682:12345";

        final Properties props = new Properties();
        Connection testConn;

        final String lbConnGroup1 = "Bug22730682LB1";
        props.setProperty(PropertyKey.loadBalanceConnectionGroup.getKeyName(), lbConnGroup1);
        testConn = getLoadBalancedConnection(3, dummyHost, props);
        assertEquals(2, ConnectionGroupManager.getActiveHostCount(lbConnGroup1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup1).contains(dummyHost));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup1).contains(currentHost));
        ConnectionGroupManager.removeHost(lbConnGroup1, dummyHost);
        assertEquals(1, ConnectionGroupManager.getActiveHostCount(lbConnGroup1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup1).contains(currentHost));
        testConn.close();

        final String lbConnGroup2 = "Bug22730682LB2";
        props.setProperty(PropertyKey.loadBalanceConnectionGroup.getKeyName(), lbConnGroup2);
        testConn = getLoadBalancedConnection(3, dummyHost, props);
        assertEquals(2, ConnectionGroupManager.getActiveHostCount(lbConnGroup2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup2).contains(dummyHost));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup2).contains(currentHost));
        ConnectionGroupManager.removeHost(lbConnGroup2, dummyHost, true);
        assertEquals(1, ConnectionGroupManager.getActiveHostCount(lbConnGroup2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup2).contains(currentHost));
        testConn.close();
    }

    /**
     * Tests fix for Bug#22848249 - LOADBALANCECONNECTIONGROUPMANAGER.REMOVEHOST() NOT WORKING AS EXPECTED.
     * 
     * Tests a sequence of additions and removals of hosts from a load-balanced connection group.
     */
    public void testBug22848249() throws Exception {
        /*
         * Remove and add hosts to the connection group, other than the one from the active underlying connection.
         * Changes affecting active l/b connections.
         */
        subTestBug22848249A();

        /*
         * Remove and add hosts to the connection group, including the host from the active underlying connection.
         * Changes affecting active l/b connections.
         */
        subTestBug22848249B();

        /*
         * Remove hosts from the connection group with changes not affecting active l/b connections.
         */
        subTestBug22848249C();
        /*
         * Add hosts to the connection group with changes not affecting active l/b connections.
         */
        subTestBug22848249D();
    }

    /*
     * Tests removing and adding hosts (excluding the host from the underlying physical connection) to the connection group with the option to propagate
     * changes to all active load-balanced connections.
     */
    private void subTestBug22848249A() throws Exception {
        final String defaultHost = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.HOST.getKeyName());
        final String defaultPort = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.PORT.getKeyName());
        final String host1 = "first";
        final String host2 = "second";
        final String host3 = "third";
        final String host4 = "fourth";
        final String hostPort1 = host1 + ":" + defaultPort;
        final String hostPort2 = host2 + ":" + defaultPort;
        final String hostPort3 = host3 + ":" + defaultPort;
        final String hostPort4 = host4 + ":" + defaultPort;
        final String lbConnGroup = "Bug22848249A";

        System.out.println("testBug22848249A:");
        System.out.println("********************************************************************************");

        Properties props = new Properties();
        props.setProperty(PropertyKey.loadBalanceHostRemovalGracePeriod.getKeyName(), "0");
        props.setProperty(PropertyKey.loadBalanceConnectionGroup.getKeyName(), lbConnGroup);
        Connection testConn = getUnreliableLoadBalancedConnection(new String[] { host1, host2, host3 }, props);
        testConn.setAutoCommit(false);

        String connectedHost = ((JdbcConnection) testConn).getHost();
        assertConnectionsHistory(UnreliableSocketFactory.getHostConnectedStatus(connectedHost));

        assertEquals(3, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3));

        /*
         * The l/b connection won't be able to use removed unused hosts.
         */

        // Remove a non-connected host: host2 or host3.
        String removedHost = connectedHost.equals(host3) ? host2 : host3;
        String removedHostPort = removedHost + ":" + defaultPort;
        ConnectionGroupManager.removeHost(lbConnGroup, removedHostPort, true);
        assertEquals(connectedHost, ((JdbcConnection) testConn).getHost()); // Still connected to the initital host.
        assertEquals(2, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2) ^ removedHostPort.equals(hostPort2)); // Only one can be true.
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3) ^ removedHostPort.equals(hostPort3));

        // Force some transaction boundaries while checking that the removed host is never used.
        int connectionSwaps = 0;
        for (int i = 0; i < 100; i++) {
            testConn.rollback();
            String newConnectedHost = ((JdbcConnection) testConn).getHost();
            assertFalse(newConnectedHost.equals(removedHost));
            if (!connectedHost.equals(newConnectedHost)) {
                connectedHost = newConnectedHost;
                connectionSwaps++;
            }
        }
        System.out.println("\t1. Swapped connections " + connectionSwaps + " times out of 100, without hitting the removed host(s).");
        assertTrue(connectionSwaps > 0); // Non-deterministic, but something must be wrong if there are no swaps after 100 transaction boundaries.
        assertFalse(UnreliableSocketFactory.getHostsFromAllConnections().contains(UnreliableSocketFactory.getHostConnectedStatus(removedHost)));

        /*
         * The l/b connection will be able to use a host added back to the connection group.
         */

        // Add back the previously removed host.
        ConnectionGroupManager.addHost(lbConnGroup, removedHostPort, true);
        assertEquals(3, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3));

        // Force transaction boundaries until the new host is selected or a limit number of attempts is reached.
        String newHost = removedHost;
        connectionSwaps = 0;
        int attemptsLeft = 100;
        while (!(connectedHost = ((JdbcConnection) testConn).getHost()).equals(newHost)) {
            testConn.rollback();
            String newConnectedHost = ((JdbcConnection) testConn).getHost();
            if (!connectedHost.equals(newConnectedHost)) {
                connectedHost = newConnectedHost;
                connectionSwaps++;
            }
            if (--attemptsLeft == 0) {
                fail("Failed to swap to the newly added host after 100 transaction boundaries and " + connectionSwaps + " connection swaps.");
            }
        }
        System.out.println("\t2. Swapped connections " + connectionSwaps + " times before hitting the new host.");
        assertTrue(connectionSwaps > 0); // Non-deterministic, but something must be wrong if there are no swaps after 100 transaction boundaries.
        assertTrue(UnreliableSocketFactory.getHostsFromAllConnections().contains(UnreliableSocketFactory.getHostConnectedStatus(newHost)));

        /*
         * The l/b connection will be able to use new hosts added to the connection group.
         */

        // Add a completely new host.
        UnreliableSocketFactory.mapHost(host4, defaultHost);
        ConnectionGroupManager.addHost(lbConnGroup, hostPort4, true);
        assertEquals(4, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort4));

        // Force transaction boundaries until the new host is selected or a limit number of attempts is reached.
        newHost = host4;
        connectionSwaps = 0;
        attemptsLeft = 100;
        while (!(connectedHost = ((JdbcConnection) testConn).getHost()).equals(newHost)) {
            testConn.rollback();
            String newConnectedHost = ((JdbcConnection) testConn).getHost();
            if (!connectedHost.equals(newConnectedHost)) {
                connectedHost = newConnectedHost;
                connectionSwaps++;
            }
            if (--attemptsLeft == 0) {
                fail("Failed to swap to the newly added host after 100 transaction boundaries and " + connectionSwaps + " connection swaps.");
            }
        }
        System.out.println("\t3. Swapped connections " + connectionSwaps + " times before hitting the new host.");
        assertTrue(connectionSwaps > 0); // Non-deterministic, but something must be wrong if there are no swaps after 100 transaction boundaries.
        assertTrue(UnreliableSocketFactory.getHostsFromAllConnections().contains(UnreliableSocketFactory.getHostConnectedStatus(newHost)));

        /*
         * The l/b connection won't be able to use any number of removed hosts (excluding the current active host).
         */

        // Remove any two hosts, other than the one used in the active connection.
        String removedHost1 = connectedHost.equals(host2) ? host1 : host2;
        String removedHostPort1 = removedHost1 + ":" + defaultPort;
        ConnectionGroupManager.removeHost(lbConnGroup, removedHostPort1, true);
        String removedHost2 = connectedHost.equals(host4) ? host3 : host4;
        String removedHostPort2 = removedHost2 + ":" + defaultPort;
        ConnectionGroupManager.removeHost(lbConnGroup, removedHostPort2, true);
        assertEquals(connectedHost, ((JdbcConnection) testConn).getHost()); // Still connected to the same host.
        assertEquals(2, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1) ^ removedHostPort1.equals(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2) ^ removedHostPort1.equals(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3) ^ removedHostPort2.equals(hostPort3));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort4) ^ removedHostPort2.equals(hostPort4));

        // Force some transaction boundaries while checking that the removed hosts are never used.
        connectionSwaps = 0;
        for (int i = 0; i < 100; i++) {
            testConn.rollback();
            String newConnectedHost = ((JdbcConnection) testConn).getHost();
            assertFalse(newConnectedHost.equals(removedHost1));
            assertFalse(newConnectedHost.equals(removedHost2));
            if (!connectedHost.equals(newConnectedHost)) {
                connectedHost = newConnectedHost;
                connectionSwaps++;
            }
        }
        System.out.println("\t4. Swapped connections " + connectionSwaps + " times out of 100, without hitting the removed host(s).");
        assertTrue(connectionSwaps > 0); // Non-deterministic, but something must be wrong if there are no swaps after 100 transaction boundaries.

        // Make sure the connection is working fine.
        this.rs = testConn.createStatement().executeQuery("SELECT 'testBug22848249'");
        assertTrue(this.rs.next());
        assertEquals("testBug22848249", this.rs.getString(1));
        testConn.close();
    }

    /*
     * Tests removing and adding hosts (including the host from the underlying physical connection) to the connection group with the option to propagate
     * changes to all active load-balanced connections.
     */
    private void subTestBug22848249B() throws Exception {
        final String defaultHost = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.HOST.getKeyName());
        final String defaultPort = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.PORT.getKeyName());
        final String host1 = "first";
        final String host2 = "second";
        final String host3 = "third";
        final String host4 = "fourth";
        final String hostPort1 = host1 + ":" + defaultPort;
        final String hostPort2 = host2 + ":" + defaultPort;
        final String hostPort3 = host3 + ":" + defaultPort;
        final String hostPort4 = host4 + ":" + defaultPort;
        final String lbConnGroup = "Bug22848249B";

        System.out.println("testBug22848249B:");
        System.out.println("********************************************************************************");

        Properties props = new Properties();
        props.setProperty(PropertyKey.loadBalanceHostRemovalGracePeriod.getKeyName(), "0");
        props.setProperty(PropertyKey.loadBalanceConnectionGroup.getKeyName(), lbConnGroup);
        Connection testConn = getUnreliableLoadBalancedConnection(new String[] { host1, host2, host3 }, props);
        testConn.setAutoCommit(false);

        String connectedHost = ((JdbcConnection) testConn).getHost();
        assertConnectionsHistory(UnreliableSocketFactory.getHostConnectedStatus(connectedHost));

        assertEquals(3, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3));

        /*
         * The l/b connection won't be able to use removed hosts.
         * Underlying connection is invalidated after removing the host currently being used.
         */

        // Remove the connected host.
        String removedHost = connectedHost;
        String removedHostPort = removedHost + ":" + defaultPort;
        ConnectionGroupManager.removeHost(lbConnGroup, removedHostPort, true);
        assertFalse(((JdbcConnection) testConn).getHost().equals(connectedHost)); // No longer connected to the removed host.
        assertEquals(2, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1) ^ removedHostPort.equals(hostPort1)); // Only one can be true.
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2) ^ removedHostPort.equals(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3) ^ removedHostPort.equals(hostPort3));

        // Force some transaction boundaries while checking that the removed host is never used again.
        UnreliableSocketFactory.flushConnectionAttempts();
        int connectionSwaps = 0;
        for (int i = 0; i < 100; i++) {
            testConn.rollback();
            String newConnectedHost = ((JdbcConnection) testConn).getHost();
            assertFalse(newConnectedHost.equals(removedHost));
            if (!connectedHost.equals(newConnectedHost)) {
                connectedHost = newConnectedHost;
                connectionSwaps++;
            }
        }
        System.out.println("\t1. Swapped connections " + connectionSwaps + " times out of 100, without hitting the removed host(s).");
        assertTrue(connectionSwaps > 0); // Non-deterministic, but something must be wrong if there are no swaps after 100 transaction boundaries.
        assertFalse(UnreliableSocketFactory.getHostsFromAllConnections().contains(UnreliableSocketFactory.getHostConnectedStatus(removedHost)));

        /*
         * The l/b connection will be able to use a host added back to the connection group.
         */

        // Add back the previously removed host.
        ConnectionGroupManager.addHost(lbConnGroup, removedHostPort, true);
        assertEquals(3, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3));

        // Force transaction boundaries until the new host is selected or a limit number of attempts is reached.
        String newHost = removedHost;
        connectionSwaps = 0;
        int attemptsLeft = 100;
        while (!(connectedHost = ((JdbcConnection) testConn).getHost()).equals(newHost)) {
            testConn.rollback();
            String newConnectedHost = ((JdbcConnection) testConn).getHost();
            if (!connectedHost.equals(newConnectedHost)) {
                connectedHost = newConnectedHost;
                connectionSwaps++;
            }
            if (--attemptsLeft == 0) {
                fail("Failed to swap to the newly added host after 100 transaction boundaries and " + connectionSwaps + " connection swaps.");
            }
        }
        System.out.println("\t2. Swapped connections " + connectionSwaps + " times before hitting the new host.");
        assertTrue(connectionSwaps > 0); // Non-deterministic, but something must be wrong if there are no swaps after 100 transaction boundaries.
        assertTrue(UnreliableSocketFactory.getHostsFromAllConnections().contains(UnreliableSocketFactory.getHostConnectedStatus(newHost)));

        /*
         * The l/b connection will be able to use new hosts added to the connection group.
         */

        // Add a completely new host.
        UnreliableSocketFactory.mapHost(host4, defaultHost);
        ConnectionGroupManager.addHost(lbConnGroup, hostPort4, true);
        assertEquals(4, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort4));

        // Force transaction boundaries until the new host is selected or a limit number of attempts is reached.
        newHost = host4;
        connectionSwaps = 0;
        attemptsLeft = 100;
        while (!(connectedHost = ((JdbcConnection) testConn).getHost()).equals(newHost)) {
            testConn.rollback();
            String newConnectedHost = ((JdbcConnection) testConn).getHost();
            if (!connectedHost.equals(newConnectedHost)) {
                connectedHost = newConnectedHost;
                connectionSwaps++;
            }
            if (--attemptsLeft == 0) {
                fail("Failed to swap to the newly added host after 100 transaction boundaries and " + connectionSwaps + " connection swaps.");
            }
        }
        System.out.println("\t3. Swapped connections " + connectionSwaps + " times before hitting the new host.");
        assertTrue(connectionSwaps > 0); // Non-deterministic, but something must be wrong if there are no swaps after 100 transaction boundaries.
        assertTrue(UnreliableSocketFactory.getHostsFromAllConnections().contains(UnreliableSocketFactory.getHostConnectedStatus(newHost)));

        /*
         * The l/b connection won't be able to use any number of removed hosts (including the current active host).
         * Underlying connection is invalidated after removing the host currently being used.
         */

        // Remove two hosts, one of them is from the active connection.
        String removedHost1 = connectedHost.equals(host1) ? host1 : host2;
        String removedHostPort1 = removedHost1 + ":" + defaultPort;
        ConnectionGroupManager.removeHost(lbConnGroup, removedHostPort1, true);
        String removedHost2 = connectedHost.equals(host3) ? host3 : host4;
        String removedHostPort2 = removedHost2 + ":" + defaultPort;
        ConnectionGroupManager.removeHost(lbConnGroup, removedHostPort2, true);
        assertFalse(((JdbcConnection) testConn).getHost().equals(removedHost1)); // Not connected to the first removed host.
        assertFalse(((JdbcConnection) testConn).getHost().equals(removedHost2)); // Not connected to the second removed host.
        assertEquals(2, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1) ^ removedHostPort1.equals(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2) ^ removedHostPort1.equals(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3) ^ removedHostPort2.equals(hostPort3));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort4) ^ removedHostPort2.equals(hostPort4));

        // Force some transaction boundaries while checking that the removed hosts are never used.
        connectionSwaps = 0;
        for (int i = 0; i < 100; i++) {
            testConn.rollback();
            String newConnectedHost = ((JdbcConnection) testConn).getHost();
            assertFalse(newConnectedHost.equals(removedHost1));
            assertFalse(newConnectedHost.equals(removedHost2));
            if (!connectedHost.equals(newConnectedHost)) {
                connectedHost = newConnectedHost;
                connectionSwaps++;
            }
        }
        System.out.println("\t4. Swapped connections " + connectionSwaps + " times out of 100, without hitting the removed host(s).");
        assertTrue(connectionSwaps > 0); // Non-deterministic, but something must be wrong if there are no swaps after 100 transaction boundaries.

        // Make sure the connection is working fine.
        this.rs = testConn.createStatement().executeQuery("SELECT 'testBug22848249'");
        assertTrue(this.rs.next());
        assertEquals("testBug22848249", this.rs.getString(1));
        testConn.close();
    }

    /*
     * Tests removing hosts from the connection group without affecting current active connections.
     */
    private void subTestBug22848249C() throws Exception {
        final String defaultPort = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.PORT.getKeyName());
        final String host1 = "first";
        final String host2 = "second";
        final String host3 = "third";
        final String host4 = "fourth";
        final String hostPort1 = host1 + ":" + defaultPort;
        final String hostPort2 = host2 + ":" + defaultPort;
        final String hostPort3 = host3 + ":" + defaultPort;
        final String hostPort4 = host4 + ":" + defaultPort;
        final String lbConnGroup = "Bug22848249C";

        System.out.println("testBug22848249C:");
        System.out.println("********************************************************************************");

        /*
         * Initial connection will be able to use all hosts, even after removed from the connection group.
         */
        Properties props = new Properties();
        props.setProperty(PropertyKey.loadBalanceHostRemovalGracePeriod.getKeyName(), "0");
        props.setProperty(PropertyKey.loadBalanceConnectionGroup.getKeyName(), lbConnGroup);
        Connection testConn = getUnreliableLoadBalancedConnection(new String[] { host1, host2, host3, host4 }, props);
        testConn.setAutoCommit(false);

        String connectedHost = ((JdbcConnection) testConn).getHost();
        assertConnectionsHistory(UnreliableSocketFactory.getHostConnectedStatus(connectedHost));

        assertEquals(4, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort4));

        // Remove two hosts, one of them is from the active connection.
        String removedHost1 = connectedHost.equals(host1) ? host1 : host2;
        String removedHostPort1 = removedHost1 + ":" + defaultPort;
        ConnectionGroupManager.removeHost(lbConnGroup, removedHostPort1, false);
        String removedHost2 = connectedHost.equals(host3) ? host3 : host4;
        String removedHostPort2 = removedHost2 + ":" + defaultPort;
        ConnectionGroupManager.removeHost(lbConnGroup, removedHostPort2, false);
        assertEquals(connectedHost, ((JdbcConnection) testConn).getHost()); // Still connected to the same host.
        assertEquals(2, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1) ^ removedHostPort1.equals(hostPort1)); // Only one can be true.
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2) ^ removedHostPort1.equals(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3) ^ removedHostPort2.equals(hostPort3));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort4) ^ removedHostPort2.equals(hostPort4));

        // Force some transaction boundaries and check that all hosts are being used.
        int connectionSwaps = 0;
        Set<String> hostsUsed = new HashSet<>();
        for (int i = 0; i < 100 && hostsUsed.size() < 4; i++) {
            testConn.rollback();
            String newConnectedHost = ((JdbcConnection) testConn).getHost();
            if (!connectedHost.equals(newConnectedHost)) {
                hostsUsed.add(newConnectedHost);
                connectedHost = newConnectedHost;
                connectionSwaps++;
            }
        }
        System.out.println("\t1. Swapped connections " + connectionSwaps + " times out of 100 or before using all hosts.");
        assertTrue(connectionSwaps > 0); // Non-deterministic, but something must be wrong if there are no swaps after 100 transaction boundaries.
        assertEquals(4, hostsUsed.size());

        // Make sure the connection is working fine.
        this.rs = testConn.createStatement().executeQuery("SELECT 'testBug22848249'");
        assertTrue(this.rs.next());
        assertEquals("testBug22848249", this.rs.getString(1));
        testConn.close();

        /*
         * New connection wont be able to use the previously removed hosts.
         */
        testConn = getUnreliableLoadBalancedConnection(new String[] { host1, host2, host3, host4 }, props);
        testConn.setAutoCommit(false);

        connectedHost = ((JdbcConnection) testConn).getHost();
        assertConnectionsHistory(UnreliableSocketFactory.getHostConnectedStatus(connectedHost));

        assertFalse(((JdbcConnection) testConn).getHost().equals(removedHost1)); // Not connected to the removed host.
        assertFalse(((JdbcConnection) testConn).getHost().equals(removedHost2)); // Not connected to the removed host.
        assertEquals(2, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1) ^ removedHostPort1.equals(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2) ^ removedHostPort1.equals(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3) ^ removedHostPort2.equals(hostPort3));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort4) ^ removedHostPort2.equals(hostPort4));

        // Force some transaction boundaries while checking that the removed hosts are never used.
        connectionSwaps = 0;
        for (int i = 0; i < 100; i++) {
            testConn.rollback();
            String newConnectedHost = ((JdbcConnection) testConn).getHost();
            assertFalse(newConnectedHost.equals(removedHost1));
            assertFalse(newConnectedHost.equals(removedHost2));
            if (!connectedHost.equals(newConnectedHost)) {
                connectedHost = newConnectedHost;
                connectionSwaps++;
            }
        }
        System.out.println("\t2. Swapped connections " + connectionSwaps + " times out of 100, without hitting the removed host(s).");
        assertTrue(connectionSwaps > 0); // Non-deterministic, but something must be wrong if there are no swaps after 100 transaction boundaries.

        // Make sure the connection is working fine.
        this.rs = testConn.createStatement().executeQuery("SELECT 'testBug22848249'");
        assertTrue(this.rs.next());
        assertEquals("testBug22848249", this.rs.getString(1));
        testConn.close();
    }

    /*
     * Tests adding hosts from the connection group without affecting current active connections.
     */
    private void subTestBug22848249D() throws Exception {
        final String defaultHost = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.HOST.getKeyName());
        final String defaultPort = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.PORT.getKeyName());
        final String host1 = "first";
        final String host2 = "second";
        final String host3 = "third";
        final String host4 = "fourth";
        final String hostPort1 = host1 + ":" + defaultPort;
        final String hostPort2 = host2 + ":" + defaultPort;
        final String hostPort3 = host3 + ":" + defaultPort;
        final String hostPort4 = host4 + ":" + defaultPort;
        final String lbConnGroup = "Bug22848249D";

        System.out.println("testBug22848249D:");
        System.out.println("********************************************************************************");

        /*
         * Initial connection will be able to use only the hosts available when it was initialized, even after adding new ones to the connection group.
         */
        Properties props = new Properties();
        props.setProperty(PropertyKey.loadBalanceHostRemovalGracePeriod.getKeyName(), "0");
        props.setProperty(PropertyKey.loadBalanceConnectionGroup.getKeyName(), lbConnGroup);
        Connection testConn = getUnreliableLoadBalancedConnection(new String[] { host1, host2 }, props);
        testConn.setAutoCommit(false);

        String connectedHost = ((JdbcConnection) testConn).getHost();
        assertConnectionsHistory(UnreliableSocketFactory.getHostConnectedStatus(connectedHost));

        assertEquals(2, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2));

        // Add two hosts.
        UnreliableSocketFactory.mapHost(host3, defaultHost);
        ConnectionGroupManager.addHost(lbConnGroup, hostPort3, false);
        UnreliableSocketFactory.mapHost(host4, defaultHost);
        ConnectionGroupManager.addHost(lbConnGroup, hostPort4, false);
        assertEquals(4, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort4));

        // Force some transaction boundaries and check that the new hosts aren't used.
        int connectionSwaps = 0;
        for (int i = 0; i < 100; i++) {
            testConn.rollback();
            String newConnectedHost = ((JdbcConnection) testConn).getHost();
            assertFalse(newConnectedHost.equals(host3));
            assertFalse(newConnectedHost.equals(host4));
            if (!connectedHost.equals(newConnectedHost)) {
                connectedHost = newConnectedHost;
                connectionSwaps++;
            }
        }
        System.out.println("\t1. Swapped connections " + connectionSwaps + " times out of 100, without hitting the newly added host(s).");
        assertTrue(connectionSwaps > 0); // Non-deterministic, but something must be wrong if there are no swaps after 100 transaction boundaries.

        // Make sure the connection is working fine.
        this.rs = testConn.createStatement().executeQuery("SELECT 'testBug22848249'");
        assertTrue(this.rs.next());
        assertEquals("testBug22848249", this.rs.getString(1));
        testConn.close();

        /*
         * New connection will be able to use all hosts.
         */
        testConn = getUnreliableLoadBalancedConnection(new String[] { host1, host2, host3, host4 }, props);
        testConn.setAutoCommit(false);

        connectedHost = ((JdbcConnection) testConn).getHost();
        assertConnectionsHistory(UnreliableSocketFactory.getHostConnectedStatus(connectedHost));

        assertEquals(4, ConnectionGroupManager.getActiveHostCount(lbConnGroup));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort1));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort2));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort3));
        assertTrue(ConnectionGroupManager.getActiveHostLists(lbConnGroup).contains(hostPort4));

        // Force some transaction boundaries while checking that the removed hosts are never used.
        connectionSwaps = 0;
        Set<String> hostsUsed = new HashSet<>();
        for (int i = 0; i < 100 && hostsUsed.size() < 4; i++) {
            testConn.rollback();
            String newConnectedHost = ((JdbcConnection) testConn).getHost();
            if (!connectedHost.equals(newConnectedHost)) {
                hostsUsed.add(newConnectedHost);
                connectedHost = newConnectedHost;
                connectionSwaps++;
            }
        }
        System.out.println("\t2. Swapped connections " + connectionSwaps + " times out of 100 or before using all hosts.");
        assertTrue(connectionSwaps > 0); // Non-deterministic, but something must be wrong if there are no swaps after 100 transaction boundaries.
        assertEquals(4, hostsUsed.size());

        // Make sure the connection is working fine.
        this.rs = testConn.createStatement().executeQuery("SELECT 'testBug22848249'");
        assertTrue(this.rs.next());
        assertEquals("testBug22848249", this.rs.getString(1));
        testConn.close();
    }

    /**
     * Tests fix for Bug#22678872 - NPE DURING UPDATE WITH FABRIC.
     * 
     * Although the bug was reported against a Fabric connection, it can't be systematically reproduced there. A deep analysis revealed that the bug occurs due
     * to a defect in the dynamic hosts management of replication connections, specifically when one or both of the internal hosts lists (masters and/or slaves)
     * becomes empty. As such, the bug is reproducible and tested resorting to replication connections and dynamic hosts management of replication connections
     * only.
     * This test reproduces the relevant steps involved in the original stack trace, originated in the FabricMySQLConnectionProxy.getActiveConnection() code:
     * - The replication connections are initialized with the same properties as in a Fabric connection.
     * - Hosts are removed using the same options as in a Fabric connection.
     * - The method tested after any host change is Connection.setAutoCommit(), which is the method that triggered the original NPE.
     */
    public void testBug22678872() throws Exception {
        final Properties connProps = getPropertiesFromTestsuiteUrl();
        final String host = connProps.getProperty(PropertyKey.HOST.getKeyName(), "localhost");
        final String port = connProps.getProperty(PropertyKey.PORT.getKeyName(), "3306");
        final String hostPortPair = host + ":" + port;
        final String database = connProps.getProperty(PropertyKey.DBNAME.getKeyName());
        final String username = connProps.getProperty(PropertyKey.USER.getKeyName());
        final String password = connProps.getProperty(PropertyKey.PASSWORD.getKeyName(), "");
        final String serverTimezone = connProps.getProperty(PropertyKey.serverTimezone.getKeyName());

        final Map<String, String> props = new HashMap<>();
        props.put(PropertyKey.USER.getKeyName(), username);
        props.put(PropertyKey.PASSWORD.getKeyName(), password);
        props.put(PropertyKey.DBNAME.getKeyName(), database);
        props.put(PropertyKey.useSSL.getKeyName(), "false");
        props.put(PropertyKey.loadBalanceHostRemovalGracePeriod.getKeyName(), "0"); // Speed up the test execution.
        // Replicate the properties used in FabricMySQLConnectionProxy.getActiveConnection().
        props.put(PropertyKey.retriesAllDown.getKeyName(), "1");
        props.put(PropertyKey.allowMasterDownConnections.getKeyName(), "true");
        props.put(PropertyKey.allowSlaveDownConnections.getKeyName(), "true");
        props.put(PropertyKey.readFromMasterWhenNoSlaves.getKeyName(), "true");
        if (serverTimezone != null) {
            props.put(PropertyKey.serverTimezone.getKeyName(), serverTimezone);
        }

        ConnectionUrl replConnectionUrl = new ReplicationConnectionUrl(Collections.<HostInfo>emptyList(), Collections.<HostInfo>emptyList(), props);

        String replConnGroup = "";
        final List<HostInfo> emptyHostsList = Collections.emptyList();
        final List<HostInfo> singleHostList = Collections.singletonList(replConnectionUrl.getHostOrSpawnIsolated(hostPortPair));

        /*
         * Case A:
         * - Initialize a replication connection with masters and slaves lists empty.
         */
        replConnGroup = "Bug22678872A";
        props.put(PropertyKey.replicationConnectionGroup.getKeyName(), replConnGroup);
        assertThrows(SQLException.class, "A replication connection cannot be initialized without master hosts and slave hosts, simultaneously\\.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        ReplicationConnectionProxy.createProxyInstance(new ReplicationConnectionUrl(emptyHostsList, emptyHostsList, props));
                        return null;
                    }
                });

        /*
         * Case B:
         * - Initialize a replication connection with one master and no slaves.
         * - Then remove the master and add it back as a slave, followed by a promotion to master.
         */
        replConnGroup = "Bug22678872B";
        props.put(PropertyKey.replicationConnectionGroup.getKeyName(), replConnGroup);
        final ReplicationConnection testConnB = ReplicationConnectionProxy
                .createProxyInstance(new ReplicationConnectionUrl(singleHostList, emptyHostsList, props));
        assertTrue(testConnB.isMasterConnection());  // Connected to a master host.
        assertFalse(testConnB.isReadOnly());
        testConnB.setAutoCommit(false); // This was the method that triggered the original NPE. 
        ReplicationConnectionGroupManager.removeMasterHost(replConnGroup, hostPortPair, false);
        assertThrows(SQLException.class, "The replication connection is an inconsistent state due to non existing hosts in both its internal hosts lists\\.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        testConnB.setAutoCommit(false); // JDBC interface method throws SQLException.
                        return null;
                    }
                });
        assertThrows(IllegalStateException.class,
                "The replication connection is an inconsistent state due to non existing hosts in both its internal hosts lists\\.", new Callable<Void>() {
                    public Void call() throws Exception {
                        testConnB.isMasterConnection(); // Some Connector/J internal methods don't throw compatible exceptions. They have to be wrapped.
                        return null;
                    }
                });

        ReplicationConnectionGroupManager.addSlaveHost(replConnGroup, hostPortPair);
        assertFalse(testConnB.isMasterConnection());  // Connected to a slave host.
        assertTrue(testConnB.isReadOnly());
        testConnB.setAutoCommit(false);

        ReplicationConnectionGroupManager.promoteSlaveToMaster(replConnGroup, hostPortPair);
        assertTrue(testConnB.isMasterConnection());  // Connected to a master host.
        assertFalse(testConnB.isReadOnly());
        testConnB.setAutoCommit(false);
        testConnB.close();

        /*
         * Case C:
         * - Initialize a replication connection with no masters and one slave.
         * - Then remove the slave and add it back, followed by a promotion to master.
         */
        replConnGroup = "Bug22678872C";
        props.put(PropertyKey.replicationConnectionGroup.getKeyName(), replConnGroup);
        final ReplicationConnection testConnC = ReplicationConnectionProxy
                .createProxyInstance(new ReplicationConnectionUrl(emptyHostsList, singleHostList, props));
        assertFalse(testConnC.isMasterConnection());  // Connected to a slave host.
        assertTrue(testConnC.isReadOnly());
        testConnC.setAutoCommit(false);

        ReplicationConnectionGroupManager.removeSlaveHost(replConnGroup, hostPortPair, true);
        assertThrows(SQLException.class, "The replication connection is an inconsistent state due to non existing hosts in both its internal hosts lists\\.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        testConnC.setAutoCommit(false);
                        return null;
                    }
                });

        ReplicationConnectionGroupManager.addSlaveHost(replConnGroup, hostPortPair);
        assertFalse(testConnC.isMasterConnection());  // Connected to a slave host.
        assertTrue(testConnC.isReadOnly());
        testConnC.setAutoCommit(false);

        ReplicationConnectionGroupManager.promoteSlaveToMaster(replConnGroup, hostPortPair);
        assertTrue(testConnC.isMasterConnection()); // Connected to a master host ...
        assertTrue(testConnC.isReadOnly()); // ... but the connection is read-only because it was initialized with no masters.
        testConnC.setAutoCommit(false);
        testConnC.close();

        /*
         * Case D:
         * - Initialize a replication connection with one master and one slave.
         * - Then remove the master host, followed by removing the slave host.
         * - Finally add the slave host back and promote it to master.
         */
        replConnGroup = "Bug22678872D";
        props.put(PropertyKey.replicationConnectionGroup.getKeyName(), replConnGroup);
        final ReplicationConnection testConnD = ReplicationConnectionProxy
                .createProxyInstance(new ReplicationConnectionUrl(singleHostList, singleHostList, props));
        assertTrue(testConnD.isMasterConnection());  // Connected to a master host.
        assertFalse(testConnD.isReadOnly());
        testConnD.setAutoCommit(false);

        ReplicationConnectionGroupManager.removeMasterHost(replConnGroup, hostPortPair, false);
        assertFalse(testConnD.isMasterConnection());  // Connected to a slave host.
        assertTrue(testConnD.isReadOnly());
        testConnD.setAutoCommit(false);

        ReplicationConnectionGroupManager.removeSlaveHost(replConnGroup, hostPortPair, true);
        assertThrows(SQLException.class, "The replication connection is an inconsistent state due to non existing hosts in both its internal hosts lists\\.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        testConnD.setAutoCommit(false);
                        return null;
                    }
                });

        ReplicationConnectionGroupManager.addSlaveHost(replConnGroup, hostPortPair);
        assertFalse(testConnD.isMasterConnection());  // Connected to a slave host.
        assertTrue(testConnD.isReadOnly());
        testConnD.setAutoCommit(false);

        ReplicationConnectionGroupManager.promoteSlaveToMaster(replConnGroup, hostPortPair);
        assertTrue(testConnD.isMasterConnection());  // Connected to a master host.
        assertFalse(testConnD.isReadOnly());
        testConnD.setAutoCommit(false);
        testConnD.close();

        /*
         * Case E:
         * - Initialize a replication connection with one master and one slave.
         * - Set read-only.
         * - Then remove the slave host, followed by removing the master host.
         * - Finally add the slave host back and promote it to master.
         */
        replConnGroup = "Bug22678872E";
        props.put(PropertyKey.replicationConnectionGroup.getKeyName(), replConnGroup);
        final ReplicationConnection testConnE = ReplicationConnectionProxy
                .createProxyInstance(new ReplicationConnectionUrl(singleHostList, singleHostList, props));
        assertTrue(testConnE.isMasterConnection());  // Connected to a master host.
        assertFalse(testConnE.isReadOnly());
        testConnE.setAutoCommit(false);

        testConnE.setReadOnly(true);
        assertFalse(testConnE.isMasterConnection());  // Connected to a slave host.
        assertTrue(testConnE.isReadOnly());
        testConnE.setAutoCommit(false);

        ReplicationConnectionGroupManager.removeSlaveHost(replConnGroup, hostPortPair, true);
        assertTrue(testConnE.isMasterConnection());  // Connected to a master host...
        assertTrue(testConnE.isReadOnly()); // ... but the connection is read-only because that's how it was previously set.
        testConnE.setAutoCommit(false);

        ReplicationConnectionGroupManager.removeMasterHost(replConnGroup, hostPortPair, false);
        assertThrows(SQLException.class, "The replication connection is an inconsistent state due to non existing hosts in both its internal hosts lists\\.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        testConnE.setAutoCommit(false);
                        return null;
                    }
                });

        ReplicationConnectionGroupManager.addSlaveHost(replConnGroup, hostPortPair);
        assertFalse(testConnE.isMasterConnection());  // Connected to a slave host.
        assertTrue(testConnE.isReadOnly());
        testConnE.setAutoCommit(false);

        ReplicationConnectionGroupManager.promoteSlaveToMaster(replConnGroup, hostPortPair);
        assertTrue(testConnE.isMasterConnection());  // Connected to a master host...
        assertTrue(testConnE.isReadOnly()); // ... but the connection is read-only because that's how it was previously set.
        testConnE.setAutoCommit(false);
        testConnE.close();

        /*
         * Case F:
         * - Initialize a replication connection with one master and one slave.
         * - Then remove the slave host, followed by removing the master host.
         * - Finally add the slave host back and promote it to master.
         */
        replConnGroup = "Bug22678872F";
        props.put(PropertyKey.replicationConnectionGroup.getKeyName(), replConnGroup);
        final ReplicationConnection testConnF = ReplicationConnectionProxy
                .createProxyInstance(new ReplicationConnectionUrl(singleHostList, singleHostList, props));
        assertTrue(testConnF.isMasterConnection());  // Connected to a master host.
        assertFalse(testConnF.isReadOnly());
        testConnF.setAutoCommit(false);

        ReplicationConnectionGroupManager.removeSlaveHost(replConnGroup, hostPortPair, true);
        assertTrue(testConnF.isMasterConnection());  // Connected to a master host.
        assertFalse(testConnF.isReadOnly());
        testConnF.setAutoCommit(false);

        ReplicationConnectionGroupManager.removeMasterHost(replConnGroup, hostPortPair, false);
        assertThrows(SQLException.class, "The replication connection is an inconsistent state due to non existing hosts in both its internal hosts lists\\.",
                new Callable<Void>() {
                    public Void call() throws Exception {
                        testConnF.setAutoCommit(false);
                        return null;
                    }
                });

        ReplicationConnectionGroupManager.addSlaveHost(replConnGroup, hostPortPair);
        assertFalse(testConnF.isMasterConnection());  // Connected to a slave host.
        assertTrue(testConnF.isReadOnly());
        testConnF.setAutoCommit(false);

        ReplicationConnectionGroupManager.promoteSlaveToMaster(replConnGroup, hostPortPair);
        assertTrue(testConnF.isMasterConnection());  // Connected to a master host.
        assertFalse(testConnF.isReadOnly());
        testConnF.setAutoCommit(false);
        testConnF.close();

        /*
         * Case G:
         * This covers one corner case where the attribute ReplicationConnectionProxy.currentConnection can still be null even when there are known hosts. It
         * results from a combination of empty hosts lists with downed hosts:
         * - Start with one host in each list.
         * - Switch to the slaves connection (set read-only).
         * - Remove the master host.
         * - Make the slave only unavailable.
         * - Promote the slave host to master.
         * - (At this point the active connection is "null")
         * - Finally bring up the host again and check the connection status.
         */
        // Use the UnreliableSocketFactory to control when the host must be downed.
        props.remove(PropertyKey.replicationConnectionGroup.getKeyName());
        props.put(PropertyKey.socketFactory.getKeyName(), "testsuite.UnreliableSocketFactory");
        replConnectionUrl = new ReplicationConnectionUrl(Collections.<HostInfo>emptyList(), Collections.<HostInfo>emptyList(), props);

        final String newHost = "bug22678872";
        final String newHostPortPair = newHost + ":" + port;
        final String hostConnected = UnreliableSocketFactory.getHostConnectedStatus(newHost);
        final String hostNotConnected = UnreliableSocketFactory.getHostFailedStatus(newHost);
        final List<HostInfo> newSingleHostList = Collections.singletonList(replConnectionUrl.getHostOrSpawnIsolated(newHostPortPair));
        UnreliableSocketFactory.flushAllStaticData();
        UnreliableSocketFactory.mapHost(newHost, host);

        replConnGroup = "Bug22678872G";
        props.put(PropertyKey.replicationConnectionGroup.getKeyName(), replConnGroup);
        final ReplicationConnection testConnG = ReplicationConnectionProxy
                .createProxyInstance(new ReplicationConnectionUrl(newSingleHostList, newSingleHostList, props));
        assertTrue(testConnG.isMasterConnection()); // Connected to a master host.
        assertFalse(testConnG.isReadOnly());
        testConnG.setAutoCommit(false);

        testBug22678872CheckConnectionsHistory(hostConnected, hostConnected); // Two successful connections.

        testConnG.setReadOnly(true);
        assertFalse(testConnG.isMasterConnection()); // Connected to a slave host.
        assertTrue(testConnG.isReadOnly());
        testConnG.setAutoCommit(false);

        ReplicationConnectionGroupManager.removeMasterHost(replConnGroup, newHostPortPair, false);
        assertFalse(testConnG.isMasterConnection()); // Connected to a slave host.
        assertTrue(testConnG.isReadOnly());
        testConnG.setAutoCommit(false);

        UnreliableSocketFactory.downHost(newHost); // The host (currently a slave) goes down before being promoted to master.
        assertThrows(SQLException.class, "(?s)Communications link failure.*", new Callable<Void>() {
            public Void call() throws Exception {
                testConnG.promoteSlaveToMaster(newHostPortPair);
                return null;
            }
        });

        testBug22678872CheckConnectionsHistory(hostNotConnected); // One failed connection attempt.

        assertFalse(testConnG.isMasterConnection()); // Actually not connected, but the promotion to master succeeded. 
        assertThrows(SQLException.class, "The connection is unusable at the current state\\. There may be no hosts to connect to or all hosts this "
                + "connection knows may be down at the moment\\.", new Callable<Void>() {
                    public Void call() throws Exception {
                        testConnG.setAutoCommit(false);
                        return null;
                    }
                });

        testBug22678872CheckConnectionsHistory(hostNotConnected); // Another failed connection attempt.

        assertThrows(SQLException.class, "(?s)Communications link failure.*", new Callable<Void>() {
            public Void call() throws Exception {
                testConnG.setReadOnly(false); // Triggers a reconnection that fails. The read-only state change is canceled by the exception.
                return null;
            }
        }); // This throws a comm failure because it tried to connect to the existing server and failed. The internal read-only state didn't change.

        testBug22678872CheckConnectionsHistory(hostNotConnected); // Another failed connection attempt.

        UnreliableSocketFactory.dontDownHost(newHost); // The host (currently a master) is up again.
        testConnG.setAutoCommit(false); // Triggers a reconnection that succeeds.

        testBug22678872CheckConnectionsHistory(hostConnected); // One successful connection.

        assertTrue(testConnG.isMasterConnection()); // Connected to a master host...
        assertTrue(testConnG.isReadOnly()); // ... but the connection is read-only because that's how it was previously set.
        testConnG.setAutoCommit(false);

        testConnG.close();
    }

    private void testBug22678872CheckConnectionsHistory(String... expectedConnectionsHistory) {
        assertConnectionsHistory(expectedConnectionsHistory);
        assertEquals(UnreliableSocketFactory.getHostsFromAllConnections().size(), expectedConnectionsHistory.length);
        UnreliableSocketFactory.flushConnectionAttempts();
    }

    /**
     * Tests fix for Bug#77649 - URL start with word "address",JDBC can't parse the "host:port" Correctly.
     */
    public void testBug77649() throws Exception {
        String host = getHostFromTestsuiteUrl();
        int port = getPortFromTestsuiteUrl();

        String[] hosts = new String[] { getEncodedHostFromTestsuiteUrl(), "address", "address.somewhere", "addressing", "addressing.somewhere" };

        UnreliableSocketFactory.flushAllStaticData();
        for (int i = 1; i < hosts.length; i++) { // Don't map the first host.
            UnreliableSocketFactory.mapHost(hosts[i], host);
        }

        Properties props = getHostFreePropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.socketFactory.getKeyName(), UnreliableSocketFactory.class.getName());
        for (String h : hosts) {
            getConnectionWithProps(String.format("jdbc:mysql://%s:%s", h, port), props).close();
            getConnectionWithProps(String.format("jdbc:mysql://address=(protocol=tcp)(host=%s)(port=%s)", h, port), props).close();
        }
    }

    /**
     * Tests fix for Bug#74711 - FORGOTTEN WORKAROUND FOR BUG#36326.
     * 
     * This test requires a server started with the options '--query_cache_type=1' and '--query_cache_size=N', (N > 0).
     */
    public void testBug74711() throws Exception {
        if (!((MysqlConnection) this.conn).getSession().getServerSession().isQueryCacheEnabled()) {
            System.err.println("Warning! testBug77411() requires a server supporting a query cache.");
            return;
        }
        this.rs = this.stmt.executeQuery("SELECT @@global.query_cache_type, @@global.query_cache_size");
        this.rs.next();
        if (!"ON".equalsIgnoreCase(this.rs.getString(1)) || "0".equals(this.rs.getString(2))) {
            System.err
                    .println("Warning! testBug77411() requires a server started with the options '--query_cache_type=1' and '--query_cache_size=N', (N > 0).");
            return;
        }

        boolean useLocTransSt = false;
        boolean useElideSetAC = false;
        do {
            final String testCase = String.format("Case: [LocTransSt: %s, ElideAC: %s ]", useLocTransSt ? "Y" : "N", useElideSetAC ? "Y" : "N");
            final Properties props = new Properties();
            props.setProperty(PropertyKey.useLocalTransactionState.getKeyName(), Boolean.toString(useLocTransSt));
            props.setProperty(PropertyKey.elideSetAutoCommits.getKeyName(), Boolean.toString(useElideSetAC));
            Connection testConn = getConnectionWithProps(props);

            assertEquals(testCase, useLocTransSt,
                    ((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.useLocalTransactionState).getValue().booleanValue());
            assertEquals(testCase, useElideSetAC,
                    ((JdbcConnection) testConn).getPropertySet().getBooleanProperty(PropertyKey.elideSetAutoCommits).getValue().booleanValue());

            testConn.close();
        } while ((useLocTransSt = !useLocTransSt) || (useElideSetAC = !useElideSetAC));
    }

    /**
     * Tests fix for Bug#75209 - Set useLocalTransactionState may result in partially committed transaction.
     */
    public void testBug75209() throws Exception {
        createTable("testBug75209", "(id INT PRIMARY KEY)", "InnoDB");

        boolean useLocTransSt = false;
        final Properties props = new Properties();
        do {
            this.stmt.executeUpdate("TRUNCATE TABLE testBug75209");
            this.stmt.executeUpdate("INSERT INTO testBug75209 VALUES (1)");

            final String testCase = String.format("Case: [LocTransSt: %s]", useLocTransSt ? "Y" : "N");

            props.setProperty(PropertyKey.useLocalTransactionState.getKeyName(), Boolean.toString(useLocTransSt));
            final Connection testConn = getConnectionWithProps(props);
            testConn.setAutoCommit(false);

            final Statement testStmt = testConn.createStatement();
            try {
                assertEquals(testCase, 1, testStmt.executeUpdate("INSERT INTO testBug75209 VALUES(2)"));

                // This triggers Duplicate-key exception
                testStmt.executeUpdate("INSERT INTO testBug75209 VALUES(2)");
                fail(testCase + ": SQLException expected here!");
            } catch (Exception e) {
                testConn.rollback();
            }
            testStmt.close();

            testConn.setAutoCommit(true);
            testConn.close();

            this.rs = this.stmt.executeQuery("SELECT COUNT(*) FROM testBug75209");
            assertTrue(this.rs.next());
            assertEquals(testCase, 1, this.rs.getInt(1));
        } while (useLocTransSt = !useLocTransSt);
    }

    /**
     * Tests fix for Bug#75615 - Incorrect implementation of Connection.setNetworkTimeout().
     * 
     * Note: this test exploits a non deterministic race condition. Usually the failure was observed under 10 consecutive executions, as such the siginficant
     * part of the test is run up to 25 times.
     */
    Future<?> testBug75615Future = null;

    public void testBug75615() throws Exception {
        // Main use case: although this could cause an exception due to a race condition in MysqlIO.mysqlConnection it is silently swallowed within the running
        // thread.
        final Connection testConn1 = getConnectionWithProps("");
        testConn1.setNetworkTimeout(Executors.newSingleThreadExecutor(), 1000);
        testConn1.close();

        // Main use case simulation: this simulates the above by capturing an eventual exeption in the main thread. This is where this test would actually fail.
        // This part is repeated several times to increase the chance of hitting the reported bug.
        for (int i = 0; i < 25; i++) {
            final ExecutorService execService = Executors.newSingleThreadExecutor();
            final Connection testConn2 = getConnectionWithProps("");
            testConn2.setNetworkTimeout(new Executor() {
                public void execute(Runnable command) {
                    // Attach the future to the parent object so that it can track the exception in the main thread.
                    ConnectionRegressionTest.this.testBug75615Future = execService.submit(command);
                }
            }, 1000);
            testConn2.close();
            try {
                this.testBug75615Future.get();
            } catch (ExecutionException e) {
                e.getCause().printStackTrace();
                fail("Exception thrown in the thread that was setting the network timeout: " + e.getCause());
            }
            execService.shutdownNow();
        }

        // Test the expected exception on null executor.
        assertThrows(SQLException.class, "Executor can not be null", new Callable<Void>() {
            public Void call() throws Exception {
                Connection testConn = getConnectionWithProps("");
                testConn.setNetworkTimeout(null, 1000);
                testConn.close();
                return null;
            }
        });
    }

    /**
     * Tests fix for Bug#70785 - MySQL Connector/J inconsistent init state for autocommit.
     */
    public void testBug70785() throws Exception {
        // Make sure that both client and server have autocommit turned on.
        assertTrue(this.conn.getAutoCommit());
        this.rs = this.stmt.executeQuery("SELECT @@session.autocommit");
        this.rs.next();
        assertTrue(this.rs.getBoolean(1));

        if (!versionMeetsMinimum(5, 5)) {
            return;
        }
        this.rs = this.stmt.executeQuery("SELECT @@global.init_connect");
        this.rs.next();
        String originalInitConnect = this.rs.getString(1);
        this.stmt.execute("SET @@global.init_connect='SET @testBug70785=1'"); // Server variable init_connect cannot be empty for this test.

        this.rs = this.stmt.executeQuery("SELECT @@global.autocommit");
        this.rs.next();
        boolean originalAutoCommit = this.rs.getBoolean(1);
        boolean autoCommit = originalAutoCommit;

        int n = 0;
        try {
            do {
                this.stmt.execute("SET @@global.autocommit=" + (autoCommit ? 1 : 0));

                boolean cacheServerConf = false;
                boolean useLocTransSt = false;
                boolean elideSetAutoCommit = false;
                do {
                    final String testCase = String.format("Case: [AutoCommit: %s, CacheSrvConf: %s, LocTransSt: %s, ElideSetAC: %s ]", autoCommit ? "Y" : "N",
                            cacheServerConf ? "Y" : "N", useLocTransSt ? "Y" : "N", elideSetAutoCommit ? "Y" : "N");
                    final Properties props = new Properties();
                    props.setProperty(PropertyKey.cacheServerConfiguration.getKeyName(), Boolean.toString(cacheServerConf));
                    props.setProperty(PropertyKey.useLocalTransactionState.getKeyName(), Boolean.toString(useLocTransSt));
                    props.setProperty(PropertyKey.elideSetAutoCommits.getKeyName(), Boolean.toString(elideSetAutoCommit));

                    if (cacheServerConf) {
                        n++;
                    }
                    String uniqueUrl = dbUrl + "&testBug70785=" + n; // Make sure that the first connection will be a cache miss and the second a cache hit.
                    Connection testConn1 = getConnectionWithProps(uniqueUrl, props);
                    Connection testConn2 = getConnectionWithProps(uniqueUrl, props);

                    assertTrue(testCase, testConn1.getAutoCommit());
                    this.rs = testConn1.createStatement().executeQuery("SELECT @@session.autocommit");
                    this.rs.next();
                    assertTrue(testCase, this.rs.getBoolean(1));

                    assertTrue(testCase, testConn2.getAutoCommit());
                    this.rs = testConn2.createStatement().executeQuery("SELECT @@session.autocommit");
                    this.rs.next();
                    assertTrue(testCase, this.rs.getBoolean(1));

                    testConn1.close();
                    testConn2.close();
                } while ((cacheServerConf = !cacheServerConf) || (useLocTransSt = !useLocTransSt) || (elideSetAutoCommit = !elideSetAutoCommit));
            } while ((autoCommit = !autoCommit) != originalAutoCommit);
        } finally {
            this.stmt.execute("SET @@global.init_connect='" + originalInitConnect + "'");
            this.stmt.execute("SET @@global.autocommit=" + (originalAutoCommit ? 1 : 0));
        }
    }

    /**
     * This test requires two server instances:
     * 1) main test server pointed to by the com.mysql.cj.testsuite.url variable configured without RSA encryption support
     * (sha256_password_private_key_path, sha256_password_public_key_path, caching_sha2_password_private_key_path and
     * caching_sha2_password_public_key_path config options are unset).
     * 2) additional server instance pointed to by the com.mysql.cj.testsuite.url.openssl variable configured with
     * default-authentication-plugin=sha256_password, RSA encryption enabled, and server configuration options
     * "caching_sha2_password_private_key_path" and "caching_sha2_password_public_key_path" set to the same values
     * as "sha256_password_private_key_path" and "sha256_password_public_key_path" respectively.
     * 
     * To run this test, please add this variable to the ant call:
     * -Dcom.mysql.cj.testsuite.url.openssl=jdbc:mysql://localhost:3307/test?user=root&password=pwd
     * 
     * @throws Exception
     */
    public void testCachingSha2PasswordPlugin() throws Exception {
        String trustStorePath = "src/test/config/ssl-test-certs/ca-truststore";
        System.setProperty("javax.net.ssl.keyStore", trustStorePath);
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStore", trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        /*
         * test against server without RSA support
         */
        if (versionMeetsMinimum(8, 0, 3)) {
            if (!pluginIsActive(this.stmt, "caching_sha2_password")) {
                fail("caching_sha2_password required to run this test");
            }

            // newer GPL servers, like 8.0.4+, are using OpenSSL and can use RSA encryption, while old ones compiled with yaSSL cannot
            boolean gplWithRSA = allowsRsa(this.stmt);

            try {
                if (!versionMeetsMinimum(8, 0, 5)) {
                    this.stmt.executeUpdate("SET @current_old_passwords = @@global.old_passwords");
                }
                createUser("'wl11060user'@'%'", "identified WITH caching_sha2_password");
                this.stmt.executeUpdate("grant all on *.* to 'wl11060user'@'%'");
                createUser("'wl11060nopassword'@'%'", "identified WITH caching_sha2_password");
                this.stmt.executeUpdate("grant all on *.* to 'wl11060nopassword'@'%'");
                if (!versionMeetsMinimum(8, 0, 5)) {
                    this.stmt.executeUpdate("SET GLOBAL old_passwords= 2");
                    this.stmt.executeUpdate("SET SESSION old_passwords= 2");
                }
                this.stmt.executeUpdate(versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'wl11060user'@'%' IDENTIFIED BY 'pwd'"
                        : "set password for 'wl11060user'@'%' = PASSWORD('pwd')");
                this.stmt.executeUpdate("flush privileges");

                final Properties propsNoRetrieval = new Properties();
                propsNoRetrieval.setProperty(PropertyKey.USER.getKeyName(), "wl11060user");
                propsNoRetrieval.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                final Properties propsNoRetrievalNoPassword = new Properties();
                propsNoRetrievalNoPassword.setProperty(PropertyKey.USER.getKeyName(), "wl11060nopassword");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.PASSWORD.getKeyName(), "");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                final Properties propsAllowRetrieval = new Properties();
                propsAllowRetrieval.setProperty(PropertyKey.USER.getKeyName(), "wl11060user");
                propsAllowRetrieval.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");
                propsAllowRetrieval.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                final Properties propsAllowRetrievalNoPassword = new Properties();
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.USER.getKeyName(), "wl11060nopassword");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.PASSWORD.getKeyName(), "");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                // 1. without SSL
                // SQLException expected due to server doesn't recognize Public Key Retrieval packet
                assertThrows(SQLException.class, "Public Key Retrieval is not allowed", new Callable<Void>() {
                    public Void call() throws Exception {
                        getConnectionWithProps(propsNoRetrieval);
                        return null;
                    }
                });
                if (gplWithRSA) {
                    assertCurrentUser(null, propsAllowRetrieval, "wl11060user", false);
                } else {
                    assertThrows(SQLException.class, "Access denied for user 'wl11060user'.*", new Callable<Void>() {
                        public Void call() throws Exception {
                            getConnectionWithProps(propsAllowRetrieval);
                            return null;
                        }
                    });
                }

                assertCurrentUser(null, propsNoRetrievalNoPassword, "wl11060nopassword", false);
                assertCurrentUser(null, propsAllowRetrievalNoPassword, "wl11060nopassword", false);

                // 2. with serverRSAPublicKeyFile specified
                // SQLException expected due to server not recognizing RSA encrypted payload
                propsNoRetrieval.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                propsAllowRetrieval.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");

                this.stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 

                assertThrows(SQLException.class, "Access denied for user 'wl11060user'.*", new Callable<Void>() {
                    public Void call() throws Exception {
                        getConnectionWithProps(propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Access denied for user 'wl11060user'.*", new Callable<Void>() {
                    public Void call() throws Exception {
                        getConnectionWithProps(propsAllowRetrieval);
                        return null;
                    }
                });

                assertCurrentUser(null, propsNoRetrievalNoPassword, "wl11060nopassword", false);
                assertCurrentUser(null, propsAllowRetrievalNoPassword, "wl11060nopassword", false);

                // 3. over SSL
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");

                this.stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertCurrentUser(null, propsNoRetrieval, "wl11060user", true);
                assertCurrentUser(null, propsNoRetrievalNoPassword, "wl11060nopassword", false);

                this.stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertCurrentUser(null, propsAllowRetrieval, "wl11060user", true);
                assertCurrentUser(null, propsAllowRetrievalNoPassword, "wl11060nopassword", false);

                // over SSL with client-default CachingSha2PasswordPlugin
                propsNoRetrieval.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), CachingSha2PasswordPlugin.class.getName());
                propsNoRetrievalNoPassword.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), CachingSha2PasswordPlugin.class.getName());
                propsAllowRetrieval.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), CachingSha2PasswordPlugin.class.getName());
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), CachingSha2PasswordPlugin.class.getName());

                this.stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertCurrentUser(null, propsNoRetrieval, "wl11060user", true);
                assertCurrentUser(null, propsNoRetrievalNoPassword, "wl11060nopassword", false);

                this.stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertCurrentUser(null, propsAllowRetrieval, "wl11060user", true);
                assertCurrentUser(null, propsAllowRetrievalNoPassword, "wl11060nopassword", false);

                // 4. without SSL but now we hit the cached scramble
                propsNoRetrieval.clear();
                propsNoRetrieval.setProperty(PropertyKey.USER.getKeyName(), "wl11060user");
                propsNoRetrieval.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                propsAllowRetrieval.clear();
                propsAllowRetrieval.setProperty(PropertyKey.USER.getKeyName(), "wl11060user");
                propsAllowRetrieval.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");
                propsAllowRetrieval.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                assertCurrentUser(null, propsNoRetrieval, "wl11060user", false); // note that is was failing on step 1
                assertCurrentUser(null, propsAllowRetrieval, "wl11060user", false); // note that is was failing on step 1

            } finally {
                this.stmt.executeUpdate("flush privileges");
                if (!versionMeetsMinimum(8, 0, 5)) {
                    this.stmt.executeUpdate("SET GLOBAL old_passwords = @current_old_passwords");
                }
            }
        }

        /*
         * test against server with RSA support
         */
        if (this.sha256Conn != null && ((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 3)) {

            if (!pluginIsActive(this.sha256Stmt, "caching_sha2_password")) {
                fail("caching_sha2_password required to run this test");
            }
            if (!allowsRsa(this.sha256Stmt)) {
                fail("RSA encryption must be enabled on " + sha256Url + " to run this test");
            }

            try {
                // create user with long password and caching_sha2_password auth
                if (!((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET @current_old_passwords = @@global.old_passwords");
                }
                createUser(this.sha256Stmt, "'wl11060user'@'%'", "identified WITH caching_sha2_password");
                this.sha256Stmt.executeUpdate("grant all on *.* to 'wl11060user'@'%'");
                createUser(this.sha256Stmt, "'wl11060nopassword'@'%'", "identified WITH caching_sha2_password");
                this.sha256Stmt.executeUpdate("grant all on *.* to 'wl11060nopassword'@'%'");
                if (!((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET GLOBAL old_passwords= 2");
                    this.sha256Stmt.executeUpdate("SET SESSION old_passwords= 2");
                }
                this.sha256Stmt.executeUpdate(
                        ((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'wl11060user'@'%' IDENTIFIED BY 'pwd'"
                                : "set password for 'wl11060user'@'%' = PASSWORD('pwd')");
                this.sha256Stmt.executeUpdate("flush privileges");

                final Properties propsNoRetrieval = new Properties();
                propsNoRetrieval.setProperty(PropertyKey.USER.getKeyName(), "wl11060user");
                propsNoRetrieval.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");

                final Properties propsNoRetrievalNoPassword = new Properties();
                propsNoRetrievalNoPassword.setProperty(PropertyKey.USER.getKeyName(), "wl11060nopassword");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.PASSWORD.getKeyName(), "");

                final Properties propsAllowRetrieval = new Properties();
                propsAllowRetrieval.setProperty(PropertyKey.USER.getKeyName(), "wl11060user");
                propsAllowRetrieval.setProperty(PropertyKey.PASSWORD.getKeyName(), "pwd");
                propsAllowRetrieval.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

                final Properties propsAllowRetrievalNoPassword = new Properties();
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.USER.getKeyName(), "wl11060nopassword");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.PASSWORD.getKeyName(), "");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

                // 1. with client-default MysqlNativePasswordPlugin
                propsNoRetrieval.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), MysqlNativePasswordPlugin.class.getName());
                propsAllowRetrieval.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), MysqlNativePasswordPlugin.class.getName());

                // 1.1. RSA
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                assertThrows(SQLException.class, "Public Key Retrieval is not allowed", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrieval);
                        return null;
                    }
                });

                assertCurrentUser(sha256Url, propsNoRetrievalNoPassword, "wl11060nopassword", false);
                assertCurrentUser(sha256Url, propsAllowRetrieval, "wl11060user", false);
                assertCurrentUser(sha256Url, propsAllowRetrievalNoPassword, "wl11060nopassword", false);

                // 1.2. over SSL
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");

                this.sha256Stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertCurrentUser(sha256Url, propsNoRetrieval, "wl11060user", true);
                assertCurrentUser(sha256Url, propsNoRetrievalNoPassword, "wl11060nopassword", false);

                this.sha256Stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertCurrentUser(sha256Url, propsAllowRetrieval, "wl11060user", true);
                assertCurrentUser(sha256Url, propsAllowRetrievalNoPassword, "wl11060nopassword", false);

                // 2. with client-default CachingSha2PasswordPlugin
                propsNoRetrieval.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), CachingSha2PasswordPlugin.class.getName());
                propsNoRetrievalNoPassword.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), CachingSha2PasswordPlugin.class.getName());
                propsAllowRetrieval.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), CachingSha2PasswordPlugin.class.getName());
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.defaultAuthenticationPlugin.getKeyName(), CachingSha2PasswordPlugin.class.getName());

                // 2.1. RSA
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                assertCurrentUser(sha256Url, propsNoRetrieval, "wl11060user", false); // wl11060user scramble is cached now, thus authenticated successfully

                this.sha256Stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertThrows(SQLException.class, "Public Key Retrieval is not allowed", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrieval); // now, with full authentication, it's failed
                        return null;
                    }
                });
                assertCurrentUser(sha256Url, propsNoRetrievalNoPassword, "wl11060nopassword", false);

                this.sha256Stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertCurrentUser(sha256Url, propsAllowRetrieval, "wl11060user", false);
                assertCurrentUser(sha256Url, propsAllowRetrievalNoPassword, "wl11060nopassword", false);

                // 2.2. over SSL
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");

                this.sha256Stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertCurrentUser(sha256Url, propsNoRetrieval, "wl11060user", true);
                assertCurrentUser(sha256Url, propsNoRetrievalNoPassword, "wl11060nopassword", false);

                this.sha256Stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertCurrentUser(sha256Url, propsAllowRetrieval, "wl11060user", false);
                assertCurrentUser(sha256Url, propsAllowRetrievalNoPassword, "wl11060nopassword", false);

                // 3. with serverRSAPublicKeyFile specified
                propsNoRetrieval.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                propsAllowRetrieval.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "src/test/config/ssl-test-certs/mykey.pub");

                // 3.1. RSA
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                this.sha256Stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertCurrentUser(sha256Url, propsNoRetrieval, "wl11060user", false);
                assertCurrentUser(sha256Url, propsNoRetrievalNoPassword, "wl11060nopassword", false);

                this.sha256Stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertCurrentUser(sha256Url, propsAllowRetrieval, "wl11060user", false);
                assertCurrentUser(sha256Url, propsAllowRetrievalNoPassword, "wl11060nopassword", false);

                // 3.2. Runtime setServerRSAPublicKeyFile must be denied 
                final Connection c2 = getConnectionWithProps(sha256Url, propsNoRetrieval);
                assertThrows(PropertyNotModifiableException.class, "Dynamic change of ''serverRSAPublicKeyFile'' is not allowed.", new Callable<Void>() {
                    public Void call() throws Exception {
                        ((JdbcConnection) c2).getPropertySet().getProperty(PropertyKey.serverRSAPublicKeyFile)
                                .setValue("src/test/config/ssl-test-certs/mykey.pub");
                        return null;
                    }
                });
                c2.close();

                // 3.3. Runtime setAllowPublicKeyRetrieval must be denied 
                final Connection c3 = getConnectionWithProps(sha256Url, propsNoRetrieval);
                assertThrows(PropertyNotModifiableException.class, "Dynamic change of ''allowPublicKeyRetrieval'' is not allowed.", new Callable<Void>() {
                    public Void call() throws Exception {
                        ((JdbcConnection) c3).getPropertySet().getProperty(PropertyKey.allowPublicKeyRetrieval).setValue(true);
                        return null;
                    }
                });
                c3.close();

                // 3.4. over SSL
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");

                this.sha256Stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertCurrentUser(sha256Url, propsNoRetrieval, "wl11060user", true);
                assertCurrentUser(sha256Url, propsNoRetrievalNoPassword, "wl11060nopassword", false);

                this.sha256Stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 
                assertCurrentUser(sha256Url, propsAllowRetrieval, "wl11060user", true);
                assertCurrentUser(sha256Url, propsAllowRetrievalNoPassword, "wl11060nopassword", false);

                // 4. with wrong serverRSAPublicKeyFile specified
                propsNoRetrieval.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "unexistant/dummy.pub");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "unexistant/dummy.pub");
                propsAllowRetrieval.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "unexistant/dummy.pub");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.serverRSAPublicKeyFile.getKeyName(), "unexistant/dummy.pub");

                // 4.1. RSA
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "false");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "false");

                propsNoRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                propsAllowRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "false");

                this.sha256Stmt.executeUpdate("flush privileges"); // to ensure that we'll go through the full authentication 

                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrievalNoPassword);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrievalNoPassword);
                        return null;
                    }
                });

                propsNoRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrievalNoPassword);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrievalNoPassword);
                        return null;
                    }
                });

                // 4.2. over SSL
                propsNoRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.useSSL.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.useSSL.getKeyName(), "true");

                propsNoRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                propsAllowRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "false");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "false");

                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrievalNoPassword);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrievalNoPassword);
                        return null;
                    }
                });

                propsNoRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                propsNoRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                propsAllowRetrieval.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                propsAllowRetrievalNoPassword.setProperty(PropertyKey.paranoid.getKeyName(), "true");
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsNoRetrievalNoPassword);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256Url, propsAllowRetrievalNoPassword);
                        return null;
                    }
                });

            } finally {
                if (!((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(8, 0, 5)) {
                    this.sha256Stmt.executeUpdate("SET GLOBAL old_passwords = @current_old_passwords");
                }
            }

        }
    }

    /**
     * Tests fix for Bug#88242 - autoReconnect and socketTimeout JDBC option makes wrong order of client packet.
     * 
     * The wrong behavior may not be observed in all systems or configurations. It seems to be easier to reproduce when SSL is enabled. Without it, the data
     * packets flow faster and desynchronization occurs rarely, which is the root cause for this problem.
     */
    public void testBug88242() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
        props.setProperty(PropertyKey.socketTimeout.getKeyName(), "1500");

        Connection testConn = getConnectionWithProps(props);
        PreparedStatement ps = testConn.prepareStatement("SELECT ?, SLEEP(?)");

        int key = 0;
        for (int i = 0; i < 5; i++) {
            // Execute a query that runs faster than the socket timeout limit.
            ps.setInt(1, ++key);
            ps.setInt(2, 0);
            try {
                ResultSet rset = ps.executeQuery();
                assertTrue(rset.next());
                assertEquals(key, rset.getInt(1));
            } catch (SQLException e) {
                e.printStackTrace();
                fail("Exception [" + e.getClass().getName() + ": " + e.getMessage() + "] caught when no exception was expected.");
            }

            // Execute a query that runs slower than the socket timeout limit.
            ps.setInt(1, ++key);
            ps.setInt(2, 2);
            final PreparedStatement localPstmt = ps;
            assertThrows("Communications link failure.*", SQLException.class, new Callable<Void>() {
                public Void call() throws Exception {
                    localPstmt.executeQuery();
                    return null;
                }
            });
        }

        testConn.close();
    }

    /**
     * Tests fix for Bug#88232 - c/J does not rollback transaction when autoReconnect=true.
     * 
     * This is essentially a duplicate of Bug#88242, but observed in a different use case.
     */
    public void testBug88232() throws Exception {
        createTable("testBug88232", "(id INT)", "INNODB");

        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.autoReconnect.getKeyName(), "true");
        props.setProperty(PropertyKey.socketTimeout.getKeyName(), "2000");
        props.setProperty(PropertyKey.cacheServerConfiguration.getKeyName(), "true");
        props.setProperty(PropertyKey.useLocalSessionState.getKeyName(), "true");

        final Connection testConn = getConnectionWithProps(props);
        final Statement testStmt = testConn.createStatement();

        try {
            /*
             * Step 1: Insert data in a interrupted (by socket timeout exception) transaction.
             */
            testStmt.execute("START TRANSACTION");
            testStmt.execute("INSERT INTO testBug88232 VALUES (1)");
            assertThrows("Communications link failure.*", SQLException.class, new Callable<Void>() {
                public Void call() throws Exception {
                    testStmt.executeQuery("SELECT SLEEP(3)"); // Throws exception due to socket timeout. Transaction should be rolled back or canceled.
                    return null;
                }
            });
            // Check data using a different connection: table should be empty.
            this.rs = this.stmt.executeQuery("SELECT * FROM testBug88232");
            assertFalse(this.rs.next());

            /*
             * Step 2: Insert data in a new transaction and commit.
             */
            testStmt.execute("START TRANSACTION"); // Reconnects and causes implicit commit in previous transaction if not rolled back.
            testStmt.executeUpdate("INSERT INTO testBug88232 VALUES (2)");
            testStmt.execute("COMMIT");

            // Check data using a different connection: only 2nd record should be present.
            this.rs = this.stmt.executeQuery("SELECT * FROM testBug88232");
            assertTrue(this.rs.next());
            assertEquals(2, this.rs.getInt(1));
            assertFalse(this.rs.next());
        } finally {
            testConn.createStatement().execute("ROLLBACK"); // Make sure the table testBug88232 is unlocked in case of failure, otherwise it can't be deleted.
            testConn.close();
        }
    }

    /**
     * Tests fix for Bug#27131768 - NULL POINTER EXCEPTION IN CONNECTION.
     */
    public void testBug27131768() throws Exception {
        Properties props = new Properties();
        props.setProperty("useServerPrepStmts", "true");
        props.setProperty("useInformationSchema", "true");
        props.setProperty("useCursorFetch", "true");
        props.setProperty("defaultFetchSize", "3");

        Connection testConn = getConnectionWithProps(props);
        testConn.createStatement().executeQuery("SELECT 1");
        testConn.close();
    }

    /**
     * Tests fix for Bug#79612 (22362474), CONNECTION ATTRIBUTES LOST WHEN CONNECTING WITHOUT DEFAULT DATABASE.
     */
    public void testBug79612() throws Exception {
        // The case with database present in URL is covered by testConnectionAttributes() test case.
        // Testing without database here.
        if (versionMeetsMinimum(5, 6)) {
            testConnectionAttributes(getNoDbUrl(dbUrl));
        }
        if (this.sha256Conn != null && ((JdbcConnection) this.sha256Conn).getSession().versionMeetsMinimum(5, 6, 5)) {
            testConnectionAttributes(getNoDbUrl(sha256Url));
        }
    }

    /**
     * Tests fix for Bug#88227 (27029657), Connector/J 5.1.44 cannot be used against MySQL 5.7.20 without warnings.
     */
    public void testBug88227() throws Exception {
        java.sql.Connection testConn = getConnectionWithProps("statementInterceptors=" + Bug88227QueryInterceptor.class.getName());
        Bug88227QueryInterceptor.mayHaveWarnings = false;
        testConn.getTransactionIsolation();
        testConn.isReadOnly();
        testConn.close();
    }

    public static class Bug88227QueryInterceptor extends BaseQueryInterceptor {
        public static boolean mayHaveWarnings = true;

        @Override
        public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
            assertFalse("Unexpected [SHOW WARNINGS] was issued", sql.get().contains("SHOW WARNINGS"));
            return super.preProcess(sql, interceptedQuery);
        }

        @Override
        public <T extends Resultset> T postProcess(Supplier<String> sql, Query interceptedQuery, T originalResultSet, ServerSession serverSession) {
            if (!mayHaveWarnings) {
                assertEquals("Warnings while executing [" + sql + "]", 0, ((NativeSession) interceptedQuery.getSession()).getProtocol().getWarningCount());
            }
            return super.postProcess(sql, interceptedQuery, originalResultSet, serverSession);
        }
    }

    /**
     * Tests fix for Bug#26819691, SETTING PACKETDEBUGBUFFERSIZE=0 RESULTS IN CONNECTION FAILURE.
     */
    public void testBug26819691() throws Exception {
        assertThrows(SQLException.class, "The connection property 'packetDebugBufferSize' only accepts integer values in the range of 1 - 2147483647, "
                + "the value '0' exceeds this range\\.", new Callable<Void>() {
                    public Void call() throws Exception {
                        getConnectionWithProps("packetDebugBufferSize=0,enablePacketDebug=true");
                        return null;
                    }
                });

        getConnectionWithProps("packetDebugBufferSize=1,enablePacketDebug=true").close();
    }

    /**
     * Tests fix for Bug#86741 (26314325), Multi-Host connection with autocommit=0 getAutoCommit maybe wrong.
     */
    public void testBug86741() throws Exception {
        this.rs = this.stmt.executeQuery("SELECT @@global.autocommit");
        assertTrue(this.rs.next());
        int prevAutocommit = this.rs.getInt(1);
        this.stmt.execute("SET GLOBAL autocommit=0");
        try {
            Connection testConn;

            testConn = getConnectionWithProps("");
            assertTrue("Wrong connection autocommit state", testConn.getAutoCommit());
            this.rs = testConn.createStatement().executeQuery("SELECT @@global.autocommit, @@session.autocommit");
            this.rs.next();
            assertEquals("Wrong @@global.autocommit", 0, this.rs.getInt(1));
            assertEquals("Wrong @@session.autocommit", 1, this.rs.getInt(2));
            testConn.close();

            testConn = getFailoverConnection();
            assertTrue("Wrong connection autocommit state", testConn.getAutoCommit());
            this.rs = testConn.createStatement().executeQuery("SELECT @@global.autocommit, @@session.autocommit");
            this.rs.next();
            assertEquals("Wrong @@global.autocommit", 0, this.rs.getInt(1));
            assertEquals("Wrong @@session.autocommit", 1, this.rs.getInt(2));
            testConn.close();

            testConn = getLoadBalancedConnection();
            assertTrue("Wrong connection autocommit state", testConn.getAutoCommit());
            this.rs = testConn.createStatement().executeQuery("SELECT @@global.autocommit, @@session.autocommit");
            this.rs.next();
            assertEquals("Wrong @@global.autocommit", 0, this.rs.getInt(1));
            assertEquals("Wrong @@session.autocommit", 1, this.rs.getInt(2));
            testConn.close();

            testConn = getMasterSlaveReplicationConnection();
            assertTrue("Wrong connection autocommit state", testConn.getAutoCommit());
            this.rs = testConn.createStatement().executeQuery("SELECT @@global.autocommit, @@session.autocommit");
            this.rs.next();
            assertEquals("Wrong @@global.autocommit", 0, this.rs.getInt(1));
            assertEquals("Wrong @@session.autocommit", 1, this.rs.getInt(2));
            testConn.close();
        } finally {
            this.stmt.execute("SET GLOBAL autocommit=" + prevAutocommit);
        }
    }

    /**
     * Tests fix for Bug#90753 (27977617), WAIT_TIMEOUT EXCEEDED MESSAGE NOT TRIGGERED.
     */
    public void testBug90753() throws Exception {
        String initialWaitTimeout = getMysqlVariable("wait_timeout");
        String initialInteractiveTimeout = getMysqlVariable("interactive_timeout");
        int seconds = 2;

        Properties props = new Properties();
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");

        try {
            getConnectionWithProps(props).createStatement().executeUpdate("SET @@global.wait_timeout=" + seconds + ", @@global.interactive_timeout=" + seconds);

            Connection testConn = getConnectionWithProps(props);
            ResultSet rslt = testConn.createStatement().executeQuery("SELECT @@wait_timeout, @@interactive_timeout");
            rslt.next();
            System.out.println("wait_timeout: " + rslt.getString(1));
            System.out.println("interactive_timeout: " + rslt.getString(1));
            Thread.sleep(1500 * seconds);

            assertThrows(CommunicationsException.class,
                    "The last packet successfully received from the server was .+ milliseconds ago.+"
                            + "The last packet sent successfully to the server was .+ milliseconds ago.+"
                            + "is longer than the server configured value of 'wait_timeout'.+",
                    new Callable<Void>() {
                        public Void call() throws Exception {
                            testConn.createStatement().executeQuery("SELECT 1");
                            return null;
                        }
                    });

        } finally {
            getConnectionWithProps(props).createStatement()
                    .executeUpdate("SET @@global.wait_timeout=" + initialWaitTimeout + ", @@global.interactive_timeout=" + initialInteractiveTimeout);
        }

    }

    /**
     * Tests fix for BUG#26089880, GETCONNECTION("MYSQLX://..") RETURNS NON-X PROTOCOL CONNECTION.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testBug26089880() throws Exception {
        assertThrows(SQLException.class, "No suitable driver found for mysqlx://localhost:33060/test\\?user=usr&password=pwd", new Callable<Void>() {
            public Void call() throws Exception {
                DriverManager.getConnection("mysqlx://localhost:33060/test?user=usr&password=pwd", null);
                return null;
            }
        });
    }

    /**
     * Tests fix for BUG#87600 (26724154), CONNECTOR THROWS 'MALFORMED DATABASE URL' ON NON MYSQL CONNECTION-URLS.
     * 
     * @throws Exception
     *             if an error ocurrs.
     */
    public void testBug87600() throws Exception {
        assertThrows(SQLException.class, "No suitable driver found for jdbc:oracle:thin:@127.0.0.1:1521:xe", new Callable<Void>() {
            public Void call() throws Exception {
                DriverManager.getConnection("jdbc:oracle:thin:@127.0.0.1:1521:xe", null);
                return null;
            }
        });
    }

    /**
     * Tests fix for BUG#91421 (28246270), ALLOWED VALUES FOR ZERODATETIMEBEHAVIOR ARE INCOMPATIBLE WITH NETBEANS.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testBug91421() throws Exception {
        Properties props = new Properties();

        props.setProperty(PropertyKey.zeroDateTimeBehavior.getKeyName(), "exception"); // legacy EXCEPTION alias
        JdbcConnection con = (JdbcConnection) getConnectionWithProps(props);
        assertEquals(ZeroDatetimeBehavior.EXCEPTION, con.getPropertySet().<ZeroDatetimeBehavior>getEnumProperty(PropertyKey.zeroDateTimeBehavior).getValue());

        props.setProperty(PropertyKey.zeroDateTimeBehavior.getKeyName(), "round"); // legacy ROUND alias
        con = (JdbcConnection) getConnectionWithProps(props);
        assertEquals(ZeroDatetimeBehavior.ROUND, con.getPropertySet().<ZeroDatetimeBehavior>getEnumProperty(PropertyKey.zeroDateTimeBehavior).getValue());

        props.setProperty(PropertyKey.zeroDateTimeBehavior.getKeyName(), "convertToNull"); // legacy CONVERT_TO_NULL alias
        con = (JdbcConnection) getConnectionWithProps(props);
        assertEquals(ZeroDatetimeBehavior.CONVERT_TO_NULL,
                con.getPropertySet().<ZeroDatetimeBehavior>getEnumProperty(PropertyKey.zeroDateTimeBehavior).getValue());

        con = (JdbcConnection) getConnectionWithProps("jdbc:mysql://(port=" + getPortFromTestsuiteUrl() + ",user=" + mainConnectionUrl.getDefaultUser()
                + ",password=" + mainConnectionUrl.getDefaultPassword() + ",zeroDateTimeBehavior=convertToNull)/" + this.dbName,
                appendRequiredProperties(null));
        assertEquals(ZeroDatetimeBehavior.CONVERT_TO_NULL,
                con.getPropertySet().<ZeroDatetimeBehavior>getEnumProperty(PropertyKey.zeroDateTimeBehavior).getValue());
    }

    /**
     * Tests fix for BUG#28150662, CONNECTOR/J 8 MALFORMED DATABASE URL EXCEPTION WHIT CORRECT URL STRING.
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testBug28150662() throws Exception {
        HostInfo hostInfo = mainConnectionUrl.getMainHost();
        List<String> connStr = new ArrayList<>();
        connStr.add(dbUrl + "&sessionVariables=sql_mode='IGNORE_SPACE,ANSI',FOREIGN_KEY_CHECKS=0&connectionCollation=utf8mb4_unicode_ci");
        connStr.add(dbUrl + "&connectionCollation=utf8mb4_unicode_ci&sessionVariables=sql_mode='IGNORE_SPACE,ANSI',FOREIGN_KEY_CHECKS=0");
        connStr.add(String.format(
                "jdbc:mysql://address=(host=%1$s)(port=%2$d)(connectionCollation=utf8mb4_unicode_ci)(sessionVariables=sql_mode='IGNORE_SPACE,ANSI',FOREIGN_KEY_CHECKS=0)(user=%3$s)(password=%4$s)/%5$s",
                getEncodedHostFromTestsuiteUrl(), getPortFromTestsuiteUrl(), hostInfo.getUser(), hostInfo.getPassword(), hostInfo.getDatabase()));
        connStr.add(String.format(
                "jdbc:mysql://(host=%1$s,port=%2$d,connectionCollation=utf8mb4_unicode_ci,sessionVariables=sql_mode='IGNORE_SPACE%3$sANSI'%3$sFOREIGN_KEY_CHECKS=0,user=%4$s,password=%5$s)/%6$s",
                getEncodedHostFromTestsuiteUrl(), getPortFromTestsuiteUrl(), "%2C", hostInfo.getUser(), hostInfo.getPassword(), hostInfo.getDatabase()));

        for (String cs : connStr) {
            JdbcConnection c1 = (JdbcConnection) getConnectionWithProps(cs, appendRequiredProperties(null));

            assertEquals("utf8mb4_unicode_ci", c1.getPropertySet().getStringProperty(PropertyKey.connectionCollation).getValue());
            String foreignKeyChecks = getMysqlVariable(c1, "FOREIGN_KEY_CHECKS");
            assertTrue("OFF".equalsIgnoreCase(foreignKeyChecks) || "0".equals(foreignKeyChecks));

            assertEquals("sql_mode='IGNORE_SPACE,ANSI',FOREIGN_KEY_CHECKS=0", c1.getPropertySet().getStringProperty(PropertyKey.sessionVariables).getValue());
            String sqlMode = getMysqlVariable(c1, "sql_mode");
            assertTrue(sqlMode.indexOf("ANSI") != -1);
            assertTrue(sqlMode.indexOf("IGNORE_SPACE") != -1);
        }
    }

    /**
     * Tests fix for BUG#27102307, CHANGE USESSL AND VERIFYSERVERCERTIFICATE TO SSLMODE OPTION.
     * 
     * @throws Exception
     *             if an error ocurrs.
     */
    public void testBug27102307() throws Exception {
        // Basic SSL properties translation is tested in testBug21947042(). Testing only missing variants here.
        System.setProperty("javax.net.ssl.trustStore", "");
        System.setProperty("javax.net.ssl.trustStorePassword", "");

        Connection sslConn = null;
        Properties props = new Properties();

        // 1. Explicit sslMode, no explicit useSSL
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.toString());
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.DISABLED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertFalse(((MysqlConnection) sslConn).getSession().isSSLEstablished());
        sslConn.close();

        // 2. Explicit sslMode, explicit useSSL=false
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.toString());
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.REQUIRED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());
        sslConn.close();

        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.toString());
        props.setProperty(PropertyKey.useSSL.getKeyName(), "no");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.REQUIRED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());
        sslConn.close();

        // 3. Explicit sslMode, explicit useSSL=true
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.toString());
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.DISABLED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertFalse(((MysqlConnection) sslConn).getSession().isSSLEstablished());
        sslConn.close();

        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.toString());
        props.setProperty(PropertyKey.useSSL.getKeyName(), "yes");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.DISABLED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertFalse(((MysqlConnection) sslConn).getSession().isSSLEstablished());
        sslConn.close();

        // 4. Explicit sslMode=REQUIRED, explicit useSSL=true, verifyServerCertificate=true, no trust store
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.toString());
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "true");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.REQUIRED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());
        sslConn.close();

        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.REQUIRED.toString());
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "yes");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.REQUIRED, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());
        sslConn.close();

        // 5. Explicit sslMode=VERIFY_CA, explicit useSSL=true, verifyServerCertificate=false, no trust store
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.VERIFY_CA.toString());
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");
        assertThrows(SQLException.class, new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.VERIFY_CA.toString());
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "no");
        assertThrows(SQLException.class, new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        // 5. Explicit sslMode=VERIFY_CA, explicit useSSL=false, verifyServerCertificate=false, with trust store
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.VERIFY_CA.toString());
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        sslConn = getConnectionWithProps(props);
        assertTrue(((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).isExplicitlySet());
        assertEquals(SslMode.VERIFY_CA, ((JdbcConnection) sslConn).getPropertySet().getEnumProperty(PropertyKey.sslMode).getValue());
        assertTrue(((MysqlConnection) sslConn).getSession().isSSLEstablished());
        sslConn.close();

        // 6. Explicit sslMode=VERIFY_IDENTITY, explicit useSSL=true, verifyServerCertificate=false, no trust store
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.VERIFY_IDENTITY.toString());
        props.setProperty(PropertyKey.useSSL.getKeyName(), "true");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");
        assertThrows(SQLException.class, new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        });

        // 7. Explicit sslMode=VERIFY_IDENTITY, explicit useSSL=false, verifyServerCertificate=false, with trust store
        // The server certificate used in this test has "CN=MySQL Connector/J Server" thus expecting identity check failure
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.VERIFY_IDENTITY.toString());
        props.setProperty(PropertyKey.useSSL.getKeyName(), "false");
        props.setProperty(PropertyKey.verifyServerCertificate.getKeyName(), "false");
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStoreType.getKeyName(), "JKS");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        Throwable cause = null;
        try {
            sslConn = getConnectionWithProps(props);
            fail("CertificateException expected but not thrown.");
        } catch (Throwable t) {
            cause = t.getCause();
            while (cause != null && !(cause instanceof CertificateException)) {
                cause = cause.getCause();
            }
            assertTrue("CertificateException expected", cause != null && cause instanceof CertificateException);
            assertTrue(cause.getMessage()
                    .contains("Server certificate identity check failed. The certificate Common Name 'MySQL Connector/J Server' does not match"));
        }
    }

    /**
     * Test fix for Bug#89948 (27658489), Batched statements are not committed for useLocalTransactionState=true.
     */
    public void testBug89948() throws Exception {
        createTable("testBug89948", "(id INT PRIMARY KEY)");

        boolean resetConn = false;
        boolean allowMQ = false;
        boolean rwBatchStmts = false;
        boolean useLTS = false;
        boolean useLSS = false;

        do {
            final String testCase = String.format("Case: [resetConn: %s, allowMQ: %s, rwBatchStmts: %s, useLTS: %s, useLSS: %s ]", resetConn ? "Y" : "N",
                    allowMQ ? "Y" : "N", rwBatchStmts ? "Y" : "N", useLTS ? "Y" : "N", useLSS ? "Y" : "N");

            Properties props = new Properties();
            props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), Boolean.toString(allowMQ));
            props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), Boolean.toString(rwBatchStmts));
            props.setProperty(PropertyKey.useLocalTransactionState.getKeyName(), Boolean.toString(useLTS));
            props.setProperty(PropertyKey.useLocalSessionState.getKeyName(), Boolean.toString(useLTS));

            Connection testConn = getConnectionWithProps(props);
            testConn.setAutoCommit(false);

            this.pstmt = testConn.prepareStatement("INSERT INTO testBug89948 VALUES (?)");
            for (int i = 1; i <= 10; i++) {
                this.pstmt.setInt(1, i);
                this.pstmt.addBatch();
            }
            this.pstmt.executeBatch();
            testConn.commit();
            this.pstmt.close();

            testBug89948Check(testCase, 10, 0);

            if (resetConn) {
                testConn.close();
                testConn = getConnectionWithProps(props);
                testConn.setAutoCommit(false);
            }

            this.pstmt = testConn.prepareStatement("UPDATE testBug89948 SET id = id + 100 WHERE id = ?");
            for (int i = 1; i <= 10; i++) {
                this.pstmt.setInt(1, i);
                this.pstmt.addBatch();
            }
            this.pstmt.executeBatch();
            testConn.commit();
            this.pstmt.close();

            testBug89948Check(testCase, 10, 100);

            if (resetConn) {
                testConn.close();
                testConn = getConnectionWithProps(props);
                testConn.setAutoCommit(false);
            }

            this.pstmt = testConn.prepareStatement("DELETE FROM testBug89948 WHERE id % 100 = ?"); // match N or (100 + N)
            for (int i = 1; i <= 10; i++) {
                this.pstmt.setInt(1, i);
                this.pstmt.addBatch();
            }
            this.pstmt.executeBatch();
            testConn.commit();
            this.pstmt.close();

            testBug89948Check(testCase, 0, 0);

            testConn.close();

            this.stmt.executeUpdate("TRUNCATE TABLE testBug89948");
        } while ((resetConn = !resetConn) || (rwBatchStmts = !rwBatchStmts) || (useLTS = !useLTS) || (useLSS = !useLSS));
    }

    private void testBug89948Check(String testCase, int expectedCount, int idOffset) throws Exception {
        // Run this query in a different connection.
        this.rs = this.stmt.executeQuery("SELECT * FROM testBug89948");
        int c = 0;
        while (this.rs.next()) {
            c++;
            assertEquals(testCase, idOffset + c, this.rs.getInt(1));
        }
        assertEquals(testCase, expectedCount, c);
    }

    /**
     * Tests fix for Bug#91317 (28207422), Wrong defaults on collation mappings.
     */
    public void testBug91317() throws Exception {
        Map<String, String> defaultCollations = new HashMap<>();

        // Compare server-side and client-side collation defaults.
        this.rs = this.stmt.executeQuery("SELECT COLLATION_NAME, CHARACTER_SET_NAME, ID FROM INFORMATION_SCHEMA.COLLATIONS WHERE IS_DEFAULT = 'Yes'");
        while (this.rs.next()) {
            String collationName = this.rs.getString(1);
            String charsetName = this.rs.getString(2);
            int collationId = this.rs.getInt(3);
            int mappedCollationId = CharsetMapping.CHARSET_NAME_TO_COLLATION_INDEX.get(charsetName);

            defaultCollations.put(charsetName, collationName);

            // Default collation for 'utf8mb4' is 'utf8mb4_0900_ai_ci' in MySQL 8.0.1 and above, 'utf8mb4_general_ci' in the others.
            if ("utf8mb4".equalsIgnoreCase(charsetName) && !versionMeetsMinimum(8, 0, 1)) {
                mappedCollationId = 45;
            }

            assertEquals(collationId, mappedCollationId);
            assertEquals(collationName, CharsetMapping.COLLATION_INDEX_TO_COLLATION_NAME[mappedCollationId]);
        }

        ServerVersion sv = ((JdbcConnection) this.conn).getServerVersion();

        // Check `collation_connection` for each one of the known character sets.
        this.rs = this.stmt.executeQuery("SELECT character_set_name FROM information_schema.character_sets");
        int csCount = 0;
        while (this.rs.next()) {
            csCount++;
            String cs = this.rs.getString(1);

            // The following cannot be set as client_character_set
            // (https://dev.mysql.com/doc/refman/8.0/en/charset-connection.html#charset-connection-impermissible-client-charset)
            if (cs.equalsIgnoreCase("ucs2") || cs.equalsIgnoreCase("utf16") || cs.equalsIgnoreCase("utf16le") || cs.equalsIgnoreCase("utf32")) {
                continue;
            }

            String javaEnc = CharsetMapping.getJavaEncodingForMysqlCharset(cs);
            String charsetForJavaEnc = CharsetMapping.getMysqlCharsetForJavaEncoding(javaEnc, sv);
            String expectedCollation = defaultCollations.get(charsetForJavaEnc);

            if ("UTF-8".equalsIgnoreCase(javaEnc)) {
                // UTF-8 is the exception. This encoding is converted to MySQL charset 'utf8mb4' instead of 'utf8', and its corresponding collation.
                expectedCollation = versionMeetsMinimum(8, 0, 1) ? "utf8mb4_0900_ai_ci" : "utf8mb4_general_ci";
            }

            Connection testConn = getConnectionWithProps("characterEncoding=" + javaEnc);
            ResultSet testRs = testConn.createStatement().executeQuery("SHOW VARIABLES LIKE 'collation_connection'");
            assertTrue(testRs.next());
            assertEquals(expectedCollation, testRs.getString(2));
            testConn.close();
        }
        // Assert that some charsets were tested.
        assertTrue(csCount > 35); // There are 39 charsets in MySQL 5.5.61, 40 in MySQL 5.6.41 and 41 in MySQL 5.7.23 and above, but these numbers can vary.
    }

    /**
     * Tests fix for Bug#25642226, CHANGEUSER() NOT SETTING THE DATABASE PROPERLY WITH SHA USER.
     */
    public void testBug25642226() throws Exception {
        testBug25642226Task(dbUrl, "\u4F5C\u4F5C\u4F5C");
        testBug25642226Task(sha256Url, "\u4F5C\u4F5C\u4F5C");
    }

    private void testBug25642226Task(String url, String pwd) throws Exception {
        final Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "REQUIRED");
        props.setProperty(PropertyKey.trustCertificateKeyStoreUrl.getKeyName(), "file:src/test/config/ssl-test-certs/ca-truststore");
        props.setProperty(PropertyKey.trustCertificateKeyStorePassword.getKeyName(), "password");
        props.setProperty(PropertyKey.characterEncoding.getKeyName(), "UTF-8");

        Connection c1 = getConnectionWithProps(url, props);
        Connection c2 = null;
        Session sess = ((JdbcConnection) c1).getSession();

        if (!sess.versionMeetsMinimum(5, 6, 5)) {
            System.out.println("Skipped. Requires MySQL 5.6.5+ server.");
            c1.close();
            return;
        }

        Statement s1 = c1.createStatement();
        if (!pluginIsActive(s1, "sha256_password")) {
            c1.close();
            fail("sha256_password required to run this test");
        }

        this.rs = s1.executeQuery("select database()");
        this.rs.next();
        String origDb = this.rs.getString(1);
        System.out.println("URL [" + url + "]");
        System.out.println("1. Original database [" + origDb + "]");

        try {
            // create user with required password and sha256_password auth
            if (!sess.versionMeetsMinimum(8, 0, 5)) {
                s1.executeUpdate("SET @current_old_passwords = @@global.old_passwords");
                s1.executeUpdate("SET GLOBAL old_passwords= 2");
                s1.executeUpdate("SET SESSION old_passwords= 2");
            }
            createUser(s1, "'Bug25642226u1'@'%'", "identified WITH sha256_password");
            s1.executeUpdate("grant all on *.* to 'Bug25642226u1'@'%'");
            s1.executeUpdate(sess.versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'Bug25642226u1'@'%' IDENTIFIED BY '" + pwd + "'"
                    : "set password for 'Bug25642226u1'@'%' = PASSWORD('" + pwd + "')");
            s1.executeUpdate("flush privileges");

            c2 = getConnectionWithProps(url, props);
            Statement s2 = c2.createStatement();

            ((JdbcConnection) c2).changeUser("Bug25642226u1", pwd);
            this.rs = s2.executeQuery("select database()");
            this.rs.next();
            System.out.println("2. Database after sha256 changeUser [" + this.rs.getString(1) + "]");
            assertEquals(origDb, this.rs.getString(1)); // was returning null for database name
            this.rs = s2.executeQuery("show tables"); // was failing with exception

            // create user with required password and caching_sha2_password auth
            if (sess.versionMeetsMinimum(8, 0, 3)) {
                if (!pluginIsActive(s1, "caching_sha2_password")) {
                    fail("caching_sha2_password required to run this test");
                }
                // create user with required password and sha256_password auth
                createUser(s1, "'Bug25642226u2'@'%'", "identified WITH caching_sha2_password");
                s1.executeUpdate("grant all on *.* to 'Bug25642226u2'@'%'");
                s1.executeUpdate(sess.versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'Bug25642226u2'@'%' IDENTIFIED BY '" + pwd + "'"
                        : "set password for 'Bug25642226u2'@'%' = PASSWORD('" + pwd + "')");
                s1.executeUpdate("flush privileges");

                ((JdbcConnection) c2).changeUser("Bug25642226u2", pwd);
                this.rs = s2.executeQuery("select database()");
                this.rs.next();
                System.out.println("3. Database after sha2 changeUser [" + this.rs.getString(1) + "]");
                assertEquals(origDb, this.rs.getString(1)); // was returning null for database name
                this.rs = s2.executeQuery("show tables"); // was failing with exception
            }

            c2.close();

        } finally {
            s1.executeUpdate("flush privileges");
            if (!sess.versionMeetsMinimum(8, 0, 5)) {
                s1.executeUpdate("SET GLOBAL old_passwords = @current_old_passwords");
            }
            c1.close();

            if (c2 != null) {
                c2.close();
            }
        }
    }

    /**
     * Tests fix for BUG#92625 (28731795), CONTRIBUTION: FIX OBSERVED NPE IN CLEARINPUTSTREAM.
     * 
     * @throws Exception
     */
    public void testBug92625() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
        Connection con = getConnectionWithProps(props);

        ((NativeSession) ((JdbcConnection) con).getSession()).getProtocol().getSocketConnection().forceClose();
        assertThrows(CommunicationsException.class, new Callable<Void>() {
            public Void call() throws Exception {
                ((JdbcConnection) con).serverPrepareStatement("SELECT 1");
                return null;
            }
        });
    }

    /**
     * Tests fix for BUG#25642021, CHANGEUSER() FAILS WHEN ENABLEPACKETDEBUG=TRUE.
     * 
     * @throws Exception
     */
    public void testBug25642021() throws Exception {
        Properties props = getPropertiesFromTestsuiteUrl();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.enablePacketDebug.getKeyName(), "true");
        props.setProperty(PropertyKey.maintainTimeStats.getKeyName(), "true");

        Connection newConn = getConnectionWithProps(props);
        ((JdbcConnection) newConn).changeUser(props.getProperty(PropertyKey.USER.getKeyName()), props.getProperty(PropertyKey.PASSWORD.getKeyName()));

        // check that decorators are still in place
        NativeProtocol p = ((NativeSession) ((JdbcConnection) newConn).getSession()).getProtocol();
        MessageSender<NativePacketPayload> sender = p.getPacketSender();
        MessageReader<NativePacketHeader, NativePacketPayload> reader = p.getPacketReader();

        assertEquals(DebugBufferingPacketSender.class, sender.getClass());
        assertEquals(TimeTrackingPacketSender.class, sender.undecorate().getClass());
        assertEquals(SimplePacketSender.class, sender.undecorate().undecorate().getClass());
        assertEquals(SimplePacketSender.class, sender.undecorate().undecorate().undecorate().getClass());

        assertEquals(MultiPacketReader.class, reader.getClass());
        assertEquals(DebugBufferingPacketReader.class, reader.undecorate().getClass());
        assertEquals(TimeTrackingPacketReader.class, reader.undecorate().undecorate().getClass());
        assertEquals(SimplePacketReader.class, reader.undecorate().undecorate().undecorate().getClass());
        assertEquals(SimplePacketReader.class, reader.undecorate().undecorate().undecorate().undecorate().getClass());
    }

    /**
     * Tests fix for BUG#93007 (28860051), LoadBalancedConnectionProxy.getGlobalBlacklist bug.
     * 
     * @throws Exception
     */
    public void testBug93007() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), ForcedLoadBalanceStrategy.class.getName());
        props.setProperty(PropertyKey.loadBalanceBlacklistTimeout.getKeyName(), "5000");
        props.setProperty(PropertyKey.loadBalancePingTimeout.getKeyName(), "100");
        props.setProperty(PropertyKey.loadBalanceValidateConnectionOnSwapServer.getKeyName(), "true");

        String portNumber = getPropertiesFromTestsuiteUrl().getProperty(PropertyKey.PORT.getKeyName());

        if (portNumber == null) {
            portNumber = "3306";
        }

        ForcedLoadBalanceStrategy.forceFutureServer("first:" + portNumber, -1);
        Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);
        conn2.setAutoCommit(false);
        conn2.createStatement().execute("SELECT 1");

        LoadBalancedConnectionProxy h = (LoadBalancedConnectionProxy) Proxy.getInvocationHandler(conn2);

        Map<String, Long> blackList = h.getGlobalBlacklist();
        assertTrue(blackList.size() == 0);

        // make sure second is added to active connections cache:
        ForcedLoadBalanceStrategy.forceFutureServer("second:" + portNumber, -1);
        conn2.commit();

        // switch back to first:
        ForcedLoadBalanceStrategy.forceFutureServer("first:" + portNumber, -1);
        conn2.commit();

        // kill second while still in cache:
        UnreliableSocketFactory.downHost("second");

        // force second host to be selected next time:
        ForcedLoadBalanceStrategy.forceFutureServer("second:" + portNumber, 1);

        try {
            conn2.commit(); // will be on second after this
            assertTrue("Connection should not be closed", !conn2.isClosed());

            blackList = h.getGlobalBlacklist();
            assertTrue(blackList.size() > 0);
            assertNotNull(blackList.get("second:" + portNumber));

        } catch (SQLException e) {
            fail("Should not error because failure to select another server.");
        }
        conn2.close();

    }

    /**
     * Tests fix for Bug#29329326, PLEASE AVOID SHOW PROCESSLIST IF POSSIBLE.
     * 
     * @throws Exception
     */
    public void testBug29329326() throws Exception {
        Properties p = new Properties();
        p.setProperty(PropertyKey.queryInterceptors.getKeyName(), Bug29329326QueryInterceptor.class.getName());

        JdbcConnection c = (JdbcConnection) getConnectionWithProps(p);
        Bug29329326QueryInterceptor qi = (Bug29329326QueryInterceptor) c.getQueryInterceptorsInstances().get(0);
        assertTrue("SHOW PROCESSLIST was issued during connection establishing", qi.cnt == 0);

        ((com.mysql.cj.jdbc.ConnectionImpl) c).isServerLocal();

        String ps = ((MysqlConnection) c).getSession().getServerSession().getServerVariable("performance_schema");
        if (versionMeetsMinimum(5, 6, 0) // performance_schema.threads in MySQL 5.5 does not contain PROCESSLIST_HOST column
                && ps != null && ("1".contentEquals(ps) || "ON".contentEquals(ps))) {
            assertTrue("SHOW PROCESSLIST was issued by isServerLocal()", qi.cnt == 0);
        } else {
            assertTrue("SHOW PROCESSLIST wasn't issued by isServerLocal()", qi.cnt > 0);
        }
    }

    /**
     * Counts the number of issued "SHOW PROCESSLIST" statements.
     */
    public static class Bug29329326QueryInterceptor extends BaseQueryInterceptor {
        int cnt = 0;

        @Override
        public <M extends Message> M preProcess(M queryPacket) {
            String sql = StringUtils.toString(queryPacket.getByteBuffer(), 1, (queryPacket.getPosition() - 1));
            if (sql.contains("SHOW PROCESSLIST")) {
                this.cnt++;
            }
            return null;
        }
    }

    /**
     * Test fix for Bug#41172 (11750577), PROFILEREVENT.PACK() THROWS ARRAYINDEXOUTOFBOUNDSEXCEPTION.
     * 
     * @throws Exception
     */
    public void testBug41172() throws Exception {
        byte eventType = ProfilerEvent.TYPE_FETCH;
        String host = "host1";
        String db = "db1";
        long connId = Long.MAX_VALUE;
        int stId = Integer.MAX_VALUE;
        int rsId = Integer.MAX_VALUE;
        long duration = Long.MAX_VALUE;
        String durationUnits = "ms";
        Throwable t = new Throwable();
        String mess = "message1";

        ProfilerEvent pe1 = new ProfilerEventImpl(eventType, host, db, connId, stId, rsId, duration, durationUnits, t, mess);

        byte[] buf1 = pe1.pack();
        ProfilerEvent pe2 = ProfilerEventImpl.unpack(buf1);

        assertEquals(eventType, pe2.getEventType());
        assertEquals(host, pe2.getHostName());
        assertEquals(db, pe2.getDatabase());
        assertEquals(connId, pe2.getConnectionId());
        assertEquals(stId, pe2.getStatementId());
        assertEquals(rsId, pe2.getResultSetId());
        assertEquals(duration, pe2.getEventDuration());
        assertEquals(durationUnits, pe2.getDurationUnits());
        assertEquals(LogUtils.findCallingClassAndMethod(t), pe2.getEventCreationPointAsString());
        assertEquals(mess, pe2.getMessage());
    }

    /**
     * Test fix for Bug#74690 (20010454), PROFILEREVENT HOSTNAME HAS NO GETTER().
     * 
     * @throws Exception
     */
    public void testBug74690() throws Exception {
        byte eventType = ProfilerEvent.TYPE_FETCH;
        String host = "host1";
        String db = "db1";
        long connId = Long.MAX_VALUE;
        int stId = Integer.MAX_VALUE;
        int rsId = Integer.MAX_VALUE;
        long duration = Long.MAX_VALUE;
        String durationUnits = "ms";
        Throwable t = new Throwable();
        String mess = "message1";

        ProfilerEvent pe1 = new ProfilerEventImpl(eventType, host, db, connId, stId, rsId, duration, durationUnits, t, mess);
        assertEquals(eventType, pe1.getEventType());
        assertEquals(host, pe1.getHostName());
        assertEquals(db, pe1.getDatabase());
        assertEquals(connId, pe1.getConnectionId());
        assertEquals(stId, pe1.getStatementId());
        assertEquals(rsId, pe1.getResultSetId());
        assertEquals(duration, pe1.getEventDuration());
        assertEquals(durationUnits, pe1.getDurationUnits());
        assertEquals(LogUtils.findCallingClassAndMethod(t), pe1.getEventCreationPointAsString());
        assertEquals(mess, pe1.getMessage());

        ProfilerEvent pe2 = new ProfilerEventImpl(eventType, null, null, connId, stId, rsId, duration, null, null, null);
        assertEquals("", pe2.getHostName());
        assertEquals("", pe2.getDatabase());
        assertEquals("", pe2.getDurationUnits());
        assertEquals(LogUtils.findCallingClassAndMethod(null), pe1.getEventCreationPointAsString());
        assertEquals("", pe2.getMessage());

    }

    /**
     * Test fix for Bug#70677 (17640628), CONNECTOR J WITH PROFILESQL - LOG CONTAINS LOTS OF STACKTRACE DATA.
     * 
     * @throws Exception
     */
    public void testBug70677() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.profileSQL.getKeyName(), "true");
        props.setProperty(PropertyKey.logger.getKeyName(), BufferingLogger.class.getName());
        BufferingLogger.startLoggingToBuffer();

        Connection loggedConn = null;

        try {
            loggedConn = getConnectionWithProps(props);
            Statement st = loggedConn.createStatement();
            st.executeQuery("SELECT 70677");

            assertTrue(BufferingLogger.getBuffer().toString().indexOf("SELECT 70677") > 0);
            assertEquals(-1, BufferingLogger.getBuffer().toString().indexOf("** BEGIN NESTED EXCEPTION **"));
        } finally {
            BufferingLogger.dropBuffer();
            if (loggedConn != null) {
                loggedConn.close();
            }
        }
    }
}
