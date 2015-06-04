/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.regression;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import testsuite.BaseStatementInterceptor;
import testsuite.BaseTestCase;
import testsuite.UnreliableSocketFactory;

import com.mysql.jdbc.AuthenticationPlugin;
import com.mysql.jdbc.Buffer;
import com.mysql.jdbc.CharsetMapping;
import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.ConnectionProperties;
import com.mysql.jdbc.Driver;
import com.mysql.jdbc.ExceptionInterceptor;
import com.mysql.jdbc.LoadBalanceExceptionChecker;
import com.mysql.jdbc.LoadBalancingConnectionProxy;
import com.mysql.jdbc.Messages;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.MysqlDataTruncation;
import com.mysql.jdbc.MysqlErrorNumbers;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.RandomBalanceStrategy;
import com.mysql.jdbc.ReplicationConnection;
import com.mysql.jdbc.ReplicationConnectionGroupManager;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.StandardSocketFactory;
import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.TimeUtil;
import com.mysql.jdbc.Util;
import com.mysql.jdbc.exceptions.MySQLNonTransientConnectionException;
import com.mysql.jdbc.integration.jboss.MysqlValidConnectionChecker;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlXid;
import com.mysql.jdbc.jdbc2.optional.SuspendableXAConnection;
import com.mysql.jdbc.jmx.ReplicationGroupManagerMBean;
import com.mysql.jdbc.log.StandardLogger;

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
     *             if an error ocurrs.
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
        if (versionMeetsMinimum(4, 1) && isAdminConnectionConfigured()) {
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
                    props.put("characterEncoding", charsetName);

                    System.out.println("Testing character set " + charsetName);

                    charsetConn = getAdminConnectionWithProps(props);

                    charsetStmt = charsetConn.createStatement();

                    charsetStmt.executeUpdate("DROP DATABASE IF EXISTS testCollation41");
                    charsetStmt.executeUpdate("DROP TABLE IF EXISTS testCollation41");

                    charsetStmt.executeUpdate("CREATE DATABASE testCollation41 DEFAULT CHARACTER SET " + charsetName);
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
        props.put("autoReconnect", "true");

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

        reconnectableConn.createStatement().executeQuery("SELECT 1");

        assertTrue(reconnectableConn.isReadOnly() == isReadOnly);
    }

    private Map<String, String> getCharacterSetsAndCollations() throws Exception {
        Map<String, String> charsetsToLoad = new HashMap<String, String>();

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

                NonRegisteringDriver driver = new NonRegisteringDriver();

                Properties oldProps = driver.parseURL(BaseTestCase.dbUrl, null);

                String host = driver.host(oldProps);
                int port = driver.port(oldProps);
                String database = oldProps.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
                String user = oldProps.getProperty(NonRegisteringDriver.USER_PROPERTY_KEY);
                String password = oldProps.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY);

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
                autoReconnectProps.put("autoReconnect", "true");

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

                failoverStmt.executeQuery("SELECT connection_id()");
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
        Properties props = new Driver().parseURL(BaseTestCase.dbUrl, null);
        props.setProperty("autoReconnect", "true");
        props.setProperty("socketFactory", "testsuite.UnreliableSocketFactory");

        Properties urlProps = new NonRegisteringDriver().parseURL(dbUrl, null);

        String host = urlProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
        String port = urlProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

        props.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);
        props.remove(NonRegisteringDriver.NUM_HOSTS_PROPERTY_KEY);
        props.remove(NonRegisteringDriver.HOST_PROPERTY_KEY + ".1");
        props.remove(NonRegisteringDriver.PORT_PROPERTY_KEY + ".1");

        props.setProperty("queriesBeforeRetryMaster", "50");
        props.setProperty("maxReconnects", "1");

        UnreliableSocketFactory.mapHost("master", host);
        UnreliableSocketFactory.mapHost("slave", host);
        UnreliableSocketFactory.downHost("master");

        Connection failoverConnection = null;

        try {
            failoverConnection = getConnectionWithProps("jdbc:mysql://master:" + port + ",slave:" + port + "/", props);
            failoverConnection.setAutoCommit(false);

            String originalConnectionId = getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT CONNECTION_ID()").toString();

            for (int i = 0; i < 50; i++) {
                failoverConnection.createStatement().executeQuery("SELECT 1");
            }

            UnreliableSocketFactory.dontDownHost("master");

            failoverConnection.setAutoCommit(true);

            String newConnectionId = getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT CONNECTION_ID()").toString();

            assertEquals("/master", UnreliableSocketFactory.getHostFromLastConnection());
            assertFalse(newConnectionId.equals(originalConnectionId));

            failoverConnection.createStatement().executeQuery("SELECT 1");
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
        Properties props = new Driver().parseURL(BaseTestCase.dbUrl, null);
        props.setProperty("autoReconnect", "true");

        String host = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);

        if (!NonRegisteringDriver.isHostPropertiesList(host)) {
            String port = props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");

            host = host + ":" + port;
        }

        host = host + "," + host;

        props.remove("PORT");
        props.remove("HOST");

        props.setProperty("queriesBeforeRetryMaster", "10");
        props.setProperty("maxReconnects", "1");

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

            ((com.mysql.jdbc.Connection) failoverConnection).setFailedOver(true);

            failoverConnection.setAutoCommit(true);

            String failedConnectionId = getSingleIndexedValueWithQuery(failoverConnection, 1, "SELECT CONNECTION_ID()").toString();
            System.out.println("Failed over connection id: " + failedConnectionId);

            ((com.mysql.jdbc.Connection) failoverConnection).setFailedOver(true);

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
        if (versionMeetsMinimum(4, 1)) {
            Connection ms932Conn = null, cp943Conn = null, shiftJisConn = null, windows31JConn = null;

            try {
                Properties props = new Properties();
                props.setProperty("characterEncoding", "MS932");

                ms932Conn = getConnectionWithProps(props);

                this.rs = ms932Conn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_set_client'");
                assertTrue(this.rs.next());
                String encoding = this.rs.getString(2);
                if (!versionMeetsMinimum(5, 0, 3) && !versionMeetsMinimum(4, 1, 11)) {
                    assertEquals("sjis", encoding.toLowerCase(Locale.ENGLISH));
                } else {
                    assertEquals("cp932", encoding.toLowerCase(Locale.ENGLISH));
                }

                this.rs = ms932Conn.createStatement().executeQuery("SELECT 'abc'");
                assertTrue(this.rs.next());

                String charsetToCheck = "ms932";

                assertEquals(charsetToCheck, ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(1).toLowerCase(Locale.ENGLISH));

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

                    // Can't be fixed unless server is fixed,
                    // this is fixed in 4.1.7.

                    assertTrue(this.rs.next());
                    asString = this.rs.getString(1);
                    assertEquals("\u3231", asString);
                } finally {
                    ms932Conn.createStatement().executeUpdate("drop table if exists testBug7607");
                }

                props = new Properties();
                props.setProperty("characterEncoding", "SHIFT_JIS");

                shiftJisConn = getConnectionWithProps(props);

                this.rs = shiftJisConn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_set_client'");
                assertTrue(this.rs.next());
                encoding = this.rs.getString(2);
                assertTrue("sjis".equalsIgnoreCase(encoding));

                this.rs = shiftJisConn.createStatement().executeQuery("SELECT 'abc'");
                assertTrue(this.rs.next());

                String charSetUC = ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(1).toUpperCase(Locale.US);

                if (isRunningOnJdk131()) {
                    assertEquals("WINDOWS-31J", charSetUC);
                } else {
                    // assertEquals("SHIFT_JIS", charSetUC);
                }

                props = new Properties();
                props.setProperty("characterEncoding", "WINDOWS-31J");

                windows31JConn = getConnectionWithProps(props);

                this.rs = windows31JConn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_set_client'");
                assertTrue(this.rs.next());
                encoding = this.rs.getString(2);

                if (!versionMeetsMinimum(5, 0, 3) && !versionMeetsMinimum(4, 1, 11)) {
                    assertEquals("sjis", encoding.toLowerCase(Locale.ENGLISH));
                } else {
                    assertEquals("cp932", encoding.toLowerCase(Locale.ENGLISH));
                }

                this.rs = windows31JConn.createStatement().executeQuery("SELECT 'abc'");
                assertTrue(this.rs.next());

                if (!versionMeetsMinimum(4, 1, 11)) {
                    assertEquals("sjis".toLowerCase(Locale.ENGLISH), ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(1)
                            .toLowerCase(Locale.ENGLISH));
                } else {
                    assertEquals("windows-31j".toLowerCase(Locale.ENGLISH), ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(1)
                            .toLowerCase(Locale.ENGLISH));
                }

                props = new Properties();
                props.setProperty("characterEncoding", "CP943");

                cp943Conn = getConnectionWithProps(props);

                this.rs = cp943Conn.createStatement().executeQuery("SHOW VARIABLES LIKE 'character_set_client'");
                assertTrue(this.rs.next());
                encoding = this.rs.getString(2);
                assertTrue("sjis".equalsIgnoreCase(encoding));

                this.rs = cp943Conn.createStatement().executeQuery("SELECT 'abc'");
                assertTrue(this.rs.next());

                charSetUC = ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(1).toUpperCase(Locale.US);

                if (isRunningOnJdk131()) {
                    assertEquals("WINDOWS-31J", charSetUC);
                } else {
                    assertEquals("CP943", charSetUC);
                }

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
    }

    /**
     * In some case Connector/J's round-robin function doesn't work.
     * 
     * I had 2 mysqld, node1 "localhost:3306" and node2 "localhost:3307".
     * 
     * 1. node1 is up, node2 is up
     * 
     * 2. java-program connect to node1 by using properties
     * "autoRecconect=true",
     * "roundRobinLoadBalance=true","failOverReadOnly=false".
     * 
     * 3. node1 is down, node2 is up
     * 
     * 4. java-program execute a query and fail, but Connector/J's round-robin
     * fashion failover work and if java-program retry a query it can succeed
     * (connection is change to node2 by Connector/j)
     * 
     * 5. node1 is up, node2 is up
     * 
     * 6. node1 is up, node2 is down
     * 
     * 7. java-program execute a query, but this time Connector/J doesn't work
     * althought node1 is up and usable.
     * 
     * 
     * @throws Exception
     */

    /*
     * FIXME: This test is no longer valid with random selection of hosts public
     * void testBug8643() throws Exception { if (runMultiHostTests()) {
     * Properties defaultProps = getMasterSlaveProps();
     * 
     * defaultProps.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);
     * defaultProps.remove(NonRegisteringDriver.PORT_PROPERTY_KEY);
     * 
     * defaultProps.put("autoReconnect", "true");
     * defaultProps.put("roundRobinLoadBalance", "true");
     * defaultProps.put("failOverReadOnly", "false");
     * 
     * Connection con = null; try { con =
     * DriverManager.getConnection(getMasterSlaveUrl(), defaultProps); Statement
     * stmt1 = con.createStatement();
     * 
     * ResultSet rs1 = stmt1 .executeQuery("show variables like 'port'");
     * rs1.next();
     * 
     * rs1 = stmt1.executeQuery("select connection_id()"); rs1.next(); String
     * originalConnectionId = rs1.getString(1); this.stmt.executeUpdate("kill "
     * + originalConnectionId);
     * 
     * int numLoops = 8;
     * 
     * SQLException caughtException = null;
     * 
     * while (caughtException == null && numLoops > 0) { numLoops--;
     * 
     * try { rs1 = stmt1.executeQuery("show variables like 'port'"); } catch
     * (SQLException sqlEx) { caughtException = sqlEx; } }
     * 
     * assertNotNull(caughtException);
     * 
     * // failover and retry rs1 =
     * stmt1.executeQuery("show variables like 'port'");
     * 
     * rs1.next(); assertTrue(!((com.mysql.jdbc.Connection) con)
     * .isMasterConnection());
     * 
     * rs1 = stmt1.executeQuery("select connection_id()"); rs1.next(); String
     * nextConnectionId = rs1.getString(1);
     * assertTrue(!nextConnectionId.equals(originalConnectionId));
     * 
     * this.stmt.executeUpdate("kill " + nextConnectionId);
     * 
     * numLoops = 8;
     * 
     * caughtException = null;
     * 
     * while (caughtException == null && numLoops > 0) { numLoops--;
     * 
     * try { rs1 = stmt1.executeQuery("show variables like 'port'"); } catch
     * (SQLException sqlEx) { caughtException = sqlEx; } }
     * 
     * assertNotNull(caughtException);
     * 
     * // failover and retry rs1 =
     * stmt1.executeQuery("show variables like 'port'");
     * 
     * rs1.next(); assertTrue(((com.mysql.jdbc.Connection) con)
     * .isMasterConnection());
     * 
     * } finally { if (con != null) { try { con.close(); } catch (Exception e) {
     * e.printStackTrace(); } } } } }
     */

    /**
     * Tests fix for BUG#9206, can not use 'UTF-8' for characterSetResults
     * configuration property.
     */
    public void testBug9206() throws Exception {
        Properties props = new Properties();
        props.setProperty("characterSetResults", "UTF-8");
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
        props.setProperty("useUnicode", "true");
        props.setProperty("characterEncoding", "EUC_KR");
        getConnectionWithProps(props).close();

        props = new Properties();
        props.setProperty("useUnicode", "true");
        props.setProperty("characterEncoding", "KOI8_R");
        getConnectionWithProps(props).close();
    }

    /**
     * Tests fix for BUG#10144 - Memory leak in ServerPreparedStatement if
     * serverPrepare() fails.
     */

    public void testBug10144() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            Properties props = new Properties();
            props.setProperty("emulateUnsupportedPstmts", "false");
            props.setProperty("useServerPrepStmts", "true");

            Connection bareConn = getConnectionWithProps(props);

            int currentOpenStatements = ((com.mysql.jdbc.Connection) bareConn).getActiveStatementCount();

            try {
                bareConn.prepareStatement("Boo!");
                fail("Should not've been able to prepare that one!");
            } catch (SQLException sqlEx) {
                assertEquals(currentOpenStatements, ((com.mysql.jdbc.Connection) bareConn).getActiveStatementCount());
            } finally {
                if (bareConn != null) {
                    bareConn.close();
                }
            }
        }
    }

    /**
     * Tests fix for BUG#10496 - SQLException is thrown when using property
     * "characterSetResults"
     */
    public void testBug10496() throws Exception {
        if (versionMeetsMinimum(5, 0, 3)) {
            Properties props = new Properties();
            props.setProperty("useUnicode", "true");
            props.setProperty("characterEncoding", "WINDOWS-31J");
            props.setProperty("characterSetResults", "WINDOWS-31J");
            getConnectionWithProps(props).close();

            props = new Properties();
            props.setProperty("useUnicode", "true");
            props.setProperty("characterEncoding", "EUC_JP");
            props.setProperty("characterSetResults", "EUC_JP");
            getConnectionWithProps(props).close();
        }
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
            props.setProperty("autoReconnect", "true");
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
        if (isRunningOnJdk131()) {
            return; // test not valid on JDK-1.3.1
        }

        if (!versionMeetsMinimum(6, 0)) {
            return; // server status is broken until MySQL-6.0
        }

        Properties props = new Properties();
        props.setProperty("useConfigs", "maxPerformance");

        Connection maxPerfConn = getConnectionWithProps(props);
        assertEquals(true, ((com.mysql.jdbc.Connection) maxPerfConn).getElideSetAutoCommits());
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

            try {
                replConn = getMasterSlaveReplicationConnection();
                assertTrue(!((MySQLConnection) ((ReplicationConnection) replConn).getMasterConnection()).hasSameProperties(((ReplicationConnection) replConn)
                        .getSlavesConnection()));
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
        props.put("profileSQL", "true");
        props.put("slowQueryThresholdMillis", "0");
        props.put("logSlowQueries", "true");
        props.put("explainSlowQueries", "true");
        props.put("useServerPrepStmts", "true");

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
        props.setProperty("characterEncoding", "Cp1251");
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
        if (versionMeetsMinimum(4, 1)) {
            Properties props = new Properties();
            props.setProperty("sessionVariables", "sql_mode=ansi");

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
            props.setProperty("profileSQL", "true");
            props.setProperty("maxQuerySizeToLog", "2");
            props.setProperty("logger", "com.mysql.jdbc.log.StandardLogger");

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

        urlBuf.append("sessionVariables=@testBug13453='%25%26+%3D'");

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
        if (isRunningOnJdk131()) {
            return; // test not valid on JDK-1.3.1
        }

        createTable("testBug15065", "(field1 int)");

        this.stmt.executeUpdate("INSERT INTO testBug15065 VALUES (1)");

        Connection advisorConn = null;
        Statement advisorStmt = null;

        try {
            Properties props = new Properties();
            props.setProperty("useUsageAdvisor", "true");
            props.setProperty("logger", "com.mysql.jdbc.log.StandardLogger");

            advisorConn = getConnectionWithProps(props);
            advisorStmt = advisorConn.createStatement();

            Method[] getMethods = ResultSet.class.getMethods();

            PrintStream oldErr = System.err;

            try {
                ByteArrayOutputStream bOut = new ByteArrayOutputStream();
                System.setErr(new PrintStream(bOut));

                HashMap<String, String> methodsToSkipMap = new HashMap<String, String>();

                // Needs an actual URL
                methodsToSkipMap.put("getURL", null);

                // Java6 JDBC4.0 methods we don't implement
                methodsToSkipMap.put("getNCharacterStream", null);
                methodsToSkipMap.put("getNClob", null);
                methodsToSkipMap.put("getNString", null);
                methodsToSkipMap.put("getRowId", null);
                methodsToSkipMap.put("getSQLXML", null);

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
                                    // we don't care about bad values, just that
                                    // the
                                    // column gets "touched"
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
        props.setProperty("characterEncoding", "Cp437");
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
        if (isRunningOnJdk131()) {
            return; // test not valid on JDK-1.3.1
        }

        Properties props = new Properties();
        props.setProperty("useUnicode", "true");
        props.setProperty("characterEncoding", "utf8");
        props.setProperty("characterSetResults", "utf8");
        props.setProperty("connectionCollation", "utf8_bin");

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

            int masterConnectionId = Integer.parseInt(getSingleIndexedValueWithQuery(replConn, 1, "SELECT CONNECTION_ID()").toString());

            replConn.setReadOnly(false);

            assertEquals(masterConnectionId, Integer.parseInt(getSingleIndexedValueWithQuery(replConn, 1, "SELECT CONNECTION_ID()").toString()));

            String currentCatalog = replConn.getCatalog();

            replConn.setCatalog(currentCatalog);
            assertEquals(currentCatalog, replConn.getCatalog());

            replConn.setReadOnly(true);

            int slaveConnectionId = Integer.parseInt(getSingleIndexedValueWithQuery(replConn, 1, "SELECT CONNECTION_ID()").toString());

            // The following test is okay for now, as the chance
            // of MySQL wrapping the connection id counter during our
            // testsuite is very small.

            assertTrue("Slave id " + slaveConnectionId + " is not newer than master id " + masterConnectionId, slaveConnectionId > masterConnectionId);

            assertEquals(currentCatalog, replConn.getCatalog());

            String newCatalog = "mysql";

            replConn.setCatalog(newCatalog);
            assertEquals(newCatalog, replConn.getCatalog());

            replConn.setReadOnly(true);
            assertEquals(newCatalog, replConn.getCatalog());

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
        Properties props = new Driver().parseURL(BaseTestCase.dbUrl, null);
        props.setProperty("autoReconnect", "false");
        props.setProperty("roundRobinLoadBalance", "true");
        props.setProperty("failoverReadOnly", "false");

        if (!isRunningOnJdk131()) {
            props.setProperty("connectTimeout", "5000");
        }

        String host = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);

        if (!NonRegisteringDriver.isHostPropertiesList(host)) {
            String port = props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");

            host = host + ":" + port;
        }

        props.remove("PORT");
        props.remove("HOST");

        StringBuilder newHostBuf = new StringBuilder();

        newHostBuf.append(host);

        newHostBuf.append(",");
        // newHostBuf.append(host);
        newHostBuf.append("192.0.2.1"); // non-exsitent machine from RFC3330
                                        // test network
        newHostBuf.append(":65532"); // make sure the slave fails

        props.remove("PORT");
        props.remove("HOST");

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
        if (!versionMeetsMinimum(6, 0)) {
            return; // server status isn't there to support this feature
        }

        Properties props = new Properties();
        props.setProperty("elideSetAutoCommits", "true");
        props.setProperty("logger", "StandardLogger");
        props.setProperty("profileSQL", "true");
        Connection c = null;

        StandardLogger.startLoggingToBuffer();

        try {
            c = getConnectionWithProps(props);
            c.setAutoCommit(true);
            c.createStatement().execute("SELECT 1");
            c.setAutoCommit(true);
            c.setAutoCommit(false);
            c.createStatement().execute("SELECT 1");
            c.setAutoCommit(false);

            // We should only see _one_ "set autocommit=" sent to the server

            String log = StandardLogger.getBuffer().toString();
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
            StandardLogger.dropBuffer();

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
     * Ensures that we don't miss getters/setters for driver properties in
     * ConnectionProperties so that names given in documentation work with
     * DataSources which will use JavaBean-style names and reflection to set the
     * values (and often fail silently! when the method isn't available).
     * 
     * @throws Exception
     */
    public void testBug23626() throws Exception {
        Class<?> clazz = this.conn.getClass();

        DriverPropertyInfo[] dpi = new NonRegisteringDriver().getPropertyInfo(dbUrl, null);
        StringBuilder missingSettersBuf = new StringBuilder();
        StringBuilder missingGettersBuf = new StringBuilder();

        Class<?>[][] argTypes = { new Class[] { String.class }, new Class[] { Integer.TYPE }, new Class[] { Long.TYPE }, new Class[] { Boolean.TYPE } };

        for (int i = 0; i < dpi.length; i++) {

            String propertyName = dpi[i].name;

            if (propertyName.equals("HOST") || propertyName.equals("PORT") || propertyName.equals("DBNAME") || propertyName.equals("user")
                    || propertyName.equals("password")) {
                continue;
            }

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
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        if (isRunningOnJdk131()) {
            return;
        }

        createProcedure("testBug25545", "() BEGIN SELECT 1; END");

        String trustStorePath = "src/testsuite/ssl-test-certs/test-cert-store";

        System.setProperty("javax.net.ssl.keyStore", trustStorePath);
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStore", trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        Connection sslConn = null;

        try {
            Properties props = new Properties();
            props.setProperty("useSSL", "true");
            props.setProperty("requireSSL", "true");
            if (Util.getJVMVersion() < 8 && versionMeetsMinimum(5, 7, 6) && isCommunityEdition()) {
                props.setProperty("enabledSSLCipherSuites", SSL_CIPHERS_FOR_576);
            }

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

            Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
            String host = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY, "localhost");
            String port = props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");
            String db = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY, "test");

            String hostSpec = host;

            if (!NonRegisteringDriver.isHostPropertiesList(host)) {
                hostSpec = host + ":" + port;
            }

            final boolean sslRequirementsFor576 = Util.getJVMVersion() < 8 && versionMeetsMinimum(5, 7, 6) && isCommunityEdition();
            final String url = "jdbc:mysql://" + hostSpec + "/" + db + "?useSSL=true&requireSSL=true&verifyServerCertificate=true"
                    + "&trustCertificateKeyStoreUrl=file:src/testsuite/ssl-test-certs/test-cert-store&trustCertificateKeyStoreType=JKS"
                    + "&trustCertificateKeyStorePassword=password" + (sslRequirementsFor576 ? "&enabledSSLCipherSuites=" + SSL_CIPHERS_FOR_576 : "");

            _conn = DriverManager.getConnection(url, (String) this.getPropertiesFromTestsuiteUrl().get("user"), (String) this.getPropertiesFromTestsuiteUrl()
                    .get("password"));

            assertTrue(true);
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
        props.setProperty("profileSQL", "true");
        props.setProperty("logger", "StandardLogger");
        StandardLogger.startLoggingToBuffer();

        Connection loggedConn = null;

        try {
            loggedConn = getConnectionWithProps(props);
            loggedConn.getTransactionIsolation();

            if (versionMeetsMinimum(4, 0, 3)) {
                assertEquals(-1, StandardLogger.getBuffer().toString().indexOf("SHOW VARIABLES LIKE 'tx_isolation'"));
            }
        } finally {
            StandardLogger.dropBuffer();
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
        Properties props = getMasterSlaveProps();
        props.setProperty("autoReconnect", "true");
        props.setProperty("queriesBeforeRetryMaster", "0");
        props.setProperty("secondsBeforeRetryMaster", "0"); // +^ enable fall back to primary as soon as possible

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
        Class<?> checkerClass = cl.loadClass("com.mysql.jdbc.integration.jboss.MysqlValidConnectionChecker");
        ((MysqlValidConnectionChecker) checkerClass.newInstance()).isValidConnection(this.conn);
    }

    public void testBug29852() throws Exception {
        Connection lbConn = getLoadBalancedConnection();
        assertTrue(!lbConn.getClass().getName().startsWith("com.mysql.jdbc"));
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
        props.setProperty("connectTimeout", "2000");
        props.setProperty("loadBalanceStrategy", "random");

        Connection lbConn = getLoadBalancedConnection(2, "localhost:23", props);

        lbConn.setAutoCommit(false);

        for (int i = 0; i < 10; i++) {
            lbConn.commit();
        }
    }

    public void testBug32877() throws Exception {
        Properties props = new Properties();
        props.setProperty("connectTimeout", "2000");
        props.setProperty("loadBalanceStrategy", "bestResponseTime");

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
        if (!com.mysql.jdbc.Util.isJdbc4()) {
            return;
        }

        Method isValid = java.sql.Connection.class.getMethod("isValid", new Class[] { Integer.TYPE });

        Connection newConn = getConnectionWithProps((Properties) null);
        isValid.invoke(newConn, new Object[] { new Integer(1) });
        Thread.sleep(2000);
        assertTrue(((Boolean) isValid.invoke(newConn, new Object[] { new Integer(0) })).booleanValue());
    }

    public void testBug34937() throws Exception {
        com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource ds = new com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource();
        StringBuilder urlBuf = new StringBuilder();
        urlBuf.append(getMasterSlaveUrl());
        urlBuf.append("?");
        Properties props = getMasterSlaveProps();
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
        props.setProperty("characterEncoding", "utf-8");
        props.setProperty("passwordCharacterEncoding", "utf-8");

        Connection adminConn = getAdminConnectionWithProps(props);

        if (adminConn != null) {

            String unicodePassword = "\u0430\u0431\u0432"; // Cyrillic string
            String user = "bug37570";
            Statement adminStmt = adminConn.createStatement();

            adminStmt.executeUpdate("grant usage on *.* to '" + user + "'@'127.0.0.1' identified by 'foo'");
            adminStmt.executeUpdate("update mysql.user set password=PASSWORD('" + unicodePassword + "') where user = '" + user + "'");
            adminStmt.executeUpdate("flush privileges");

            try {
                ((MySQLConnection) adminConn).changeUser(user, unicodePassword);
            } catch (SQLException sqle) {
                assertTrue("Connection with non-latin1 password failed", false);
            }

        }
    }

    public void testUnreliableSocketFactory() throws Exception {
        Properties props = new Properties();
        props.setProperty("loadBalanceStrategy", "bestResponseTime");
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
        props.setProperty("replicationConnectionGroup", replicationGroup1);
        props.setProperty("retriesAllDown", "3");
        ReplicationConnection conn2 = this.getUnreliableReplicationConnection(new String[] { "first", "second", "third" }, props);
        assertNotNull("Connection should not be null", this.conn);
        conn2.setAutoCommit(false);
        String port = getPort(props, new NonRegisteringDriver());
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
        props.setProperty("retriesAllDown", "3");

        ReplicationConnection conn2 = this.getUnreliableReplicationConnection(new String[] { "first", "second", "third" }, props);
        conn2.setAutoCommit(false);
        String port = getPort(props, new NonRegisteringDriver());
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
            conn2.createStatement().execute("SELECT 1");
            fail("Will fail because inability to find new master host implicitly closes connection.");
        } catch (SQLException e) {
            assertEquals("08003", e.getSQLState());
        }

    }

    public void testReplicationConnectWithNoMaster() throws Exception {
        Properties props = new Properties();
        props.setProperty("retriesAllDown", "3");
        props.setProperty("allowMasterDownConnections", "true");

        Set<String> downedHosts = new HashSet<String>();
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
        props.setProperty("retriesAllDown", "3");

        Set<MockConnectionConfiguration> configs = new HashSet<MockConnectionConfiguration>();
        MockConnectionConfiguration first = new MockConnectionConfiguration("first", "slave", null, false);
        MockConnectionConfiguration second = new MockConnectionConfiguration("second", "master", null, false);
        MockConnectionConfiguration third = new MockConnectionConfiguration("third", "master", null, false);

        configs.add(first);
        configs.add(second);
        configs.add(third);

        ReplicationConnection conn2 = this.getUnreliableReplicationConnection(configs, props);
        assertFalse(conn2.isReadOnly());
        assertTrue(conn2.isMasterConnection());
        assertTrue(conn2.isHostSlave(first.getAddress()));
        assertTrue(conn2.isHostMaster(second.getAddress()));
        assertTrue(conn2.isHostMaster(third.getAddress()));

    }

    public void testReplicationConnectionMemory() throws Exception {
        Properties props = new Properties();
        props.setProperty("retriesAllDown", "3");
        String replicationGroup = "memoryGroup";
        props.setProperty("replicationConnectionGroup", replicationGroup);

        Set<MockConnectionConfiguration> configs = new HashSet<MockConnectionConfiguration>();
        MockConnectionConfiguration first = new MockConnectionConfiguration("first", "slave", null, false);
        MockConnectionConfiguration second = new MockConnectionConfiguration("second", "master", null, false);
        MockConnectionConfiguration third = new MockConnectionConfiguration("third", "slave", null, false);

        configs.add(first);
        configs.add(second);
        configs.add(third);

        ReplicationConnection conn2 = this.getUnreliableReplicationConnection(configs, props);

        ReplicationConnectionGroupManager.promoteSlaveToMaster(replicationGroup, first.getAddress());
        ReplicationConnectionGroupManager.removeMasterHost(replicationGroup, second.getAddress());
        ReplicationConnectionGroupManager.addSlaveHost(replicationGroup, second.getAddress());

        conn2.setReadOnly(false);

        assertFalse(conn2.isReadOnly());
        assertTrue(conn2.isMasterConnection());
        assertTrue(conn2.isHostMaster(first.getAddress()));
        assertTrue(conn2.isHostSlave(second.getAddress()));
        assertTrue(conn2.isHostSlave(third.getAddress()));

        // make sure state changes made are reflected in new connections:

        ReplicationConnection conn3 = this.getUnreliableReplicationConnection(configs, props);

        conn3.setReadOnly(false);

        assertFalse(conn3.isReadOnly());
        assertTrue(conn3.isMasterConnection());
        assertTrue(conn3.isHostMaster(first.getAddress()));
        assertTrue(conn3.isHostSlave(second.getAddress()));
        assertTrue(conn3.isHostSlave(third.getAddress()));

    }

    public void testReplicationJMXInterfaces() throws Exception {
        Properties props = new Properties();
        props.setProperty("retriesAllDown", "3");
        String replicationGroup = "testReplicationJMXInterfaces";
        props.setProperty("replicationConnectionGroup", replicationGroup);
        props.setProperty("replicationEnableJMX", "true");

        Set<MockConnectionConfiguration> configs = new HashSet<MockConnectionConfiguration>();
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
        bean.removeSlaveHost(replicationGroup, first.getAddress());
        assertFalse(bean.getSlaveHostsList(replicationGroup).contains(first.getAddress()));
        assertEquals(1, bean.getActiveSlaveHostCount(replicationGroup));
        conn2.close();
        assertEquals(0, bean.getActiveLogicalConnectionCount(replicationGroup));
        conn2 = this.getUnreliableReplicationConnection(configs, props);
        assertEquals(1, bean.getActiveLogicalConnectionCount(replicationGroup));
        assertEquals(2, bean.getTotalLogicalConnectionCount(replicationGroup));
        assertEquals(1, bean.getActiveSlaveHostCount(replicationGroup));
        assertEquals(1, bean.getActiveMasterHostCount(replicationGroup));
        bean.promoteSlaveToMaster(replicationGroup, third.getAddress());
        assertEquals(2, bean.getActiveMasterHostCount(replicationGroup));
        assertEquals(0, bean.getActiveSlaveHostCount(replicationGroup));
        // confirm this works when no group filter is specified:
        bean.addSlaveHost(null, first.getAddress());
        assertEquals(1, bean.getActiveSlaveHostCount(replicationGroup));
        assertEquals(2, bean.getActiveMasterHostCount(replicationGroup));
        bean.removeMasterHost(replicationGroup, second.getAddress());
        assertEquals(1, bean.getActiveSlaveHostCount(replicationGroup));
        assertEquals(1, bean.getActiveMasterHostCount(replicationGroup));

        ReplicationConnection conn3 = this.getUnreliableReplicationConnection(configs, props);

        assertEquals(2, bean.getActiveLogicalConnectionCount(replicationGroup));
        assertEquals(3, bean.getTotalLogicalConnectionCount(replicationGroup));

        assertTrue(bean.getMasterHostsList(replicationGroup).contains(third.getAddress()));
        assertFalse(bean.getMasterHostsList(replicationGroup).contains(first.getAddress()));
        assertFalse(bean.getMasterHostsList(replicationGroup).contains(second.getAddress()));

        assertFalse(bean.getSlaveHostsList(replicationGroup).contains(third.getAddress()));
        assertTrue(bean.getSlaveHostsList(replicationGroup).contains(first.getAddress()));
        assertFalse(bean.getSlaveHostsList(replicationGroup).contains(second.getAddress()));

        assertTrue(bean.getMasterHostsList(replicationGroup).contains(conn3.getMasterConnection().getHost()));
        assertTrue(bean.getSlaveHostsList(replicationGroup).contains(conn3.getSlavesConnection().getHost()));

        assertTrue(bean.getRegisteredConnectionGroups().contains(replicationGroup));

    }

    private ReplicationGroupManagerMBean getReplicationMBean() throws Exception {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

        ObjectName mbeanName = new ObjectName("com.mysql.jdbc.jmx:type=ReplicationGroupManager");
        return (ReplicationGroupManagerMBean) MBeanServerInvocationHandler.newProxyInstance(mbs, mbeanName, ReplicationGroupManagerMBean.class, false);

    }

    public void testBug43421() throws Exception {

        Properties props = new Properties();
        props.setProperty("loadBalanceStrategy", "bestResponseTime");

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
        props.setProperty("globalBlacklistTimeout", "200");
        props.setProperty("loadBalanceStrategy", "bestResponseTime");

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
        props.setProperty("loadBalanceStrategy", "random");
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
        List<Statement> statementsToTest = new LinkedList<Statement>();
        statementsToTest.add(this.conn.createStatement());
        statementsToTest.add(((com.mysql.jdbc.Connection) this.conn).clientPrepareStatement("SELECT 1"));
        statementsToTest.add(((com.mysql.jdbc.Connection) this.conn).clientPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS));
        statementsToTest.add(((com.mysql.jdbc.Connection) this.conn).clientPrepareStatement("SELECT 1", new int[0]));
        statementsToTest.add(((com.mysql.jdbc.Connection) this.conn).clientPrepareStatement("SELECT 1", new String[0]));
        statementsToTest.add(((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement("SELECT 1"));
        statementsToTest.add(((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS));
        statementsToTest.add(((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement("SELECT 1", new int[0]));
        statementsToTest.add(((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement("SELECT 1", new String[0]));

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
        String msg = SQLError.createLinkFailureMessageBasedOnHeuristics((MySQLConnection) this.conn, System.currentTimeMillis() - 1000,
                System.currentTimeMillis() - 2000, e);
        assertTrue(containsMessage(msg, "CommunicationsException.ServerPacketTimingInfo"));
    }

    /**
     * Tests fix for BUG#45419, ensure that time is not converted to seconds
     * before being reported as milliseconds.
     */
    public void testBug45419() throws Exception {
        Exception e = null;
        String msg = SQLError.createLinkFailureMessageBasedOnHeuristics((MySQLConnection) this.conn, System.currentTimeMillis() - 1000,
                System.currentTimeMillis() - 2000, e);
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
        NonRegisteringDriver driver = new NonRegisteringDriver();
        Properties props = new Properties();
        copyBasePropertiesIntoProps(props, driver);
        String hostname = getPortFreeHostname(props, driver);
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
        NonRegisteringDriver driver = new NonRegisteringDriver();

        StringBuilder url = new StringBuilder("jdbc:mysql://");
        url.append(host);

        if (port != null) {
            url.append(':');
            url.append(port);
        }

        url.append('/');
        url.append(dbname);

        Properties result = driver.parseURL(url.toString(), new Properties());

        assertEquals("hostname not equal", host, result.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY));
        if (port != null) {
            assertEquals("port not equal", port, result.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY));
        } else {
            assertEquals("port default incorrect", "3306", result.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY));
        }

        assertEquals("dbname not equal", dbname, result.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY));
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

        xads1.setPinGlobalTxToPhysicalConnection(true);
        xads1.setUrl(dbUrl);

        xads2.setPinGlobalTxToPhysicalConnection(true);
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
            getConnectionWithProps("jdbc:mysql://localhost:9999,localhost:9999/test?socketFactory=testsuite.regression.ConnectionRegressionTest$PortNumberSocketFactory");
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getCause() instanceof IOException);
        }
    }

    public static class PortNumberSocketFactory extends StandardSocketFactory {

        public PortNumberSocketFactory() {

        }

        @Override
        public Socket connect(String hostname, int portNumber, Properties props) throws SocketException, IOException {
            assertEquals(9999, portNumber);

            throw new IOException();
        }

    }

    public void testBug48486() throws Exception {

        Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
        String host = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY, "localhost");
        String port = props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");

        String hostSpec = host;

        if (!NonRegisteringDriver.isHostPropertiesList(host)) {
            hostSpec = host + ":" + port;
        }

        String database = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
        removeHostRelatedProps(props);
        props.remove(NonRegisteringDriver.DBNAME_PROPERTY_KEY);

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
        c.createStatement().executeQuery("SELECT 1");
        c.prepareStatement("SELECT 1").executeQuery();
    }

    public void testBug48605() throws Exception {
        Properties props = new Properties();
        props.setProperty("loadBalanceStrategy", "random");
        props.setProperty("selfDestructOnPingMaxOperations", "5");
        Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[] { "first", "second" }, props);

        assertNotNull("Connection should not be null", conn2);
        conn2.setAutoCommit(false);
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        conn2.createStatement().execute("SELECT 1");
        conn2.commit();
        try {
            conn2.createStatement().execute("/* ping */ SELECT 1");
            // don't care about this - we want the SQLExceptions passed up early
            // for ping failures, rather
            // than waiting until commit/rollback and pickNewConnection().
        } catch (SQLException e) {
        }
        assertTrue(conn2.isClosed());
        try {
            conn2.createStatement().execute("SELECT 1");
            fail("Should throw Exception, connection is closed.");
        } catch (SQLException e) {
        }
    }

    public void testBug49700() throws Exception {
        Connection c = getConnectionWithProps("sessionVariables=@foo='bar'");
        assertEquals("bar", getSingleIndexedValueWithQuery(c, 1, "SELECT @foo"));
        ((com.mysql.jdbc.Connection) c).resetServerState();
        assertEquals("bar", getSingleIndexedValueWithQuery(c, 1, "SELECT @foo"));
    }

    public void testBug51266() throws Exception {
        Properties props = new Properties();
        props.setProperty("roundRobinLoadBalance", "true"); // shouldn't be
                                                            // needed, but used
                                                            // in reported bug,
                                                            // it's removed by
                                                            // the driver
        Set<String> downedHosts = new HashSet<String>();
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
        props.setProperty("loadBalanceStrategy", "com.mysql.jdbc.SequentialBalanceStrategy");

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
        props.setProperty("loadBalanceStrategy", ForcedLoadBalanceStrategy.class.getName());
        props.setProperty("loadBalanceBlacklistTimeout", "5000");
        props.setProperty("loadBalancePingTimeout", "100");
        props.setProperty("loadBalanceValidateConnectionOnSwapServer", "true");

        String portNumber = new NonRegisteringDriver().parseURL(dbUrl, null).getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

        if (portNumber == null) {
            portNumber = "3306";
        }

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
        props.setProperty("loadBalanceStrategy", ForcedLoadBalanceStrategy.class.getName());
        props.setProperty("loadBalanceBlacklistTimeout", "5000");
        props.setProperty("loadBalancePingTimeout", "100");
        props.setProperty("loadBalanceValidateConnectionOnSwapServer", "false");
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
        public com.mysql.jdbc.ConnectionImpl pickConnection(LoadBalancingConnectionProxy proxy, List<String> configuredHosts,
                Map<String, ConnectionImpl> liveConnections, long[] responseTimes, int numRetries) throws SQLException {
            if (forcedFutureServer == null || forceFutureServerTimes == 0 || !configuredHosts.contains(forcedFutureServer)) {
                return super.pickConnection(proxy, configuredHosts, liveConnections, responseTimes, numRetries);
            }
            if (forceFutureServerTimes > 0) {
                forceFutureServerTimes--;
            }
            ConnectionImpl conn = liveConnections.get(forcedFutureServer);

            if (conn == null) {
                conn = proxy.createConnectionForHost(forcedFutureServer);

            }
            return conn;
        }

        @Override
        public void destroy() {
            super.destroy();

        }

        @Override
        public void init(com.mysql.jdbc.Connection conn, Properties props) throws SQLException {
            super.init(conn, props);

        }

    }

    public void testAutoCommitLB() throws Exception {
        Properties props = new Properties();
        props.setProperty("loadBalanceStrategy", CountingReBalanceStrategy.class.getName());
        props.setProperty("loadBalanceAutoCommitStatementThreshold", "3");

        String portNumber = new NonRegisteringDriver().parseURL(dbUrl, null).getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

        if (portNumber == null) {
            portNumber = "3306";
        }

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

        props.remove("loadBalanceAutoCommitStatementThreshold");
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

        props.setProperty("loadBalanceAutoCommitStatementThreshold", "3");
        props.setProperty("loadBalanceAutoCommitStatementRegex", ".*2.*");
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
        public com.mysql.jdbc.ConnectionImpl pickConnection(LoadBalancingConnectionProxy proxy, List<String> configuredHosts,
                Map<String, ConnectionImpl> liveConnections, long[] responseTimes, int numRetries) throws SQLException {
            rebalancedTimes++;
            return super.pickConnection(proxy, configuredHosts, liveConnections, responseTimes, numRetries);

        }

        @Override
        public void destroy() {
            super.destroy();
        }

        @Override
        public void init(com.mysql.jdbc.Connection conn, Properties props) throws SQLException {
            super.init(conn, props);
        }

    }

    public void testBug56429() throws Exception {
        Properties props = new Driver().parseURL(BaseTestCase.dbUrl, null);
        props.setProperty("autoReconnect", "true");
        props.setProperty("socketFactory", "testsuite.UnreliableSocketFactory");

        Properties urlProps = new NonRegisteringDriver().parseURL(BaseTestCase.dbUrl, null);

        String host = urlProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
        String port = urlProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

        props.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);
        props.remove(NonRegisteringDriver.NUM_HOSTS_PROPERTY_KEY);
        props.remove(NonRegisteringDriver.HOST_PROPERTY_KEY + ".1");
        props.remove(NonRegisteringDriver.PORT_PROPERTY_KEY + ".1");

        props.setProperty("queriesBeforeRetryMaster", "50");
        props.setProperty("maxReconnects", "1");

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

            if (endConnCount - startConnCount >= 20) { // this may be bogus if run on a real system, we should probably look to see they're coming from this
                                                       // testsuite?
                fail("We're leaking connections even when not failed over");
            }
        } finally {
            if (failoverConnection != null) {
                failoverConnection.close();
            }
        }
    }

    public void testBug56955() throws Exception {
        assertEquals("JKS", ((com.mysql.jdbc.Connection) this.conn).getTrustCertificateKeyStoreType());
        assertEquals("JKS", ((com.mysql.jdbc.Connection) this.conn).getClientCertificateKeyStoreType());
    }

    public void testBug57262() throws Exception {
        Properties props = new Properties();
        props.setProperty("characterEncoding", "utf-8");
        props.setProperty("useUnicode", "true");
        props.setProperty("useOldUTF8Behavior", "true");

        Connection c = getConnectionWithProps(props);
        ResultSet r = c.createStatement().executeQuery("SHOW SESSION VARIABLES LIKE 'character_set_connection'");
        r.next();
        assertEquals("latin1", r.getString(2));
    }

    public void testBug58706() throws Exception {
        Properties props = new Driver().parseURL(BaseTestCase.dbUrl, null);
        props.setProperty("autoReconnect", "true");
        props.setProperty("socketFactory", "testsuite.UnreliableSocketFactory");

        Properties urlProps = new NonRegisteringDriver().parseURL(dbUrl, null);

        String host = urlProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
        String port = urlProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

        props.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);
        props.remove(NonRegisteringDriver.NUM_HOSTS_PROPERTY_KEY);
        props.remove(NonRegisteringDriver.HOST_PROPERTY_KEY + ".1");
        props.remove(NonRegisteringDriver.PORT_PROPERTY_KEY + ".1");

        props.setProperty("queriesBeforeRetryMaster", "0");
        props.setProperty("secondsBeforeRetryMaster", "1");
        props.setProperty("failOverReadOnly", "false");

        UnreliableSocketFactory.mapHost("master", host);
        UnreliableSocketFactory.mapHost("slave", host);

        Connection failoverConnection = null;

        try {
            failoverConnection = getConnectionWithProps("jdbc:mysql://master:" + port + ",slave:" + port + "/", props);
            failoverConnection.setAutoCommit(false);

            assertEquals("/master", UnreliableSocketFactory.getHostFromLastConnection());

            for (int i = 0; i < 50; i++) {
                failoverConnection.createStatement().executeQuery("SELECT 1");
            }

            UnreliableSocketFactory.downHost("master");

            try {
                failoverConnection.createStatement().executeQuery("SELECT 1"); // this should fail and trigger failover
                fail("Expected exception");
            } catch (SQLException sqlEx) {
                assertEquals("08S01", sqlEx.getSQLState());
            }

            failoverConnection.setAutoCommit(true);
            assertEquals("/slave", UnreliableSocketFactory.getHostFromLastConnection());
            assertTrue(!failoverConnection.isReadOnly());
            failoverConnection.createStatement().executeQuery("SELECT 1");
            failoverConnection.createStatement().executeQuery("SELECT 1");
            UnreliableSocketFactory.dontDownHost("master");
            Thread.sleep(2000);
            failoverConnection.setAutoCommit(true);
            failoverConnection.createStatement().executeQuery("SELECT 1");
            assertEquals("/master", UnreliableSocketFactory.getHostFromLastConnection());
            failoverConnection.createStatement().executeQuery("SELECT 1");
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

            ((com.mysql.jdbc.Connection) c).setStatementComment("Hi there");
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
        rConn.prepareStatement("SELECT 1").executeQuery();

        Connection rConn2 = getConnectionWithProps("autoReconnect=true,initialTimeout=2,maxReconnects=3,cacheServerConfiguration=true,elideSetAutoCommits=true");
        rConn2.prepareStatement("SELECT 1").executeQuery();

    }

    public void testBug61201() throws Exception {
        Properties props = new Properties();
        props.setProperty("sessionVariables", "FOREIGN_KEY_CHECKS=0");
        props.setProperty("characterEncoding", "latin1");
        props.setProperty("profileSQL", "true");

        Connection varConn = getConnectionWithProps(props);
        varConn.close();
    }

    public void testChangeUser() throws Exception {
        Properties props = getPropertiesFromTestsuiteUrl();

        Connection testConn = getConnectionWithProps(props);
        Statement testStmt = testConn.createStatement();

        for (int i = 0; i < 500; i++) {
            ((com.mysql.jdbc.Connection) testConn).changeUser(props.getProperty(NonRegisteringDriver.USER_PROPERTY_KEY),
                    props.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY));

            if (i % 10 == 0) {
                try {
                    ((com.mysql.jdbc.Connection) testConn).changeUser("bubba", props.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY));
                } catch (SQLException sqlEx) {
                    if (versionMeetsMinimum(5, 6, 13)) {
                        assertTrue(testConn.isClosed());
                        testConn = getConnectionWithProps(props);
                        testStmt = testConn.createStatement();
                    }
                }
            }

            testStmt.executeQuery("SELECT 1");
        }
        testConn.close();
    }

    public void testChangeUserClosedConn() throws Exception {
        Properties props = getPropertiesFromTestsuiteUrl();
        Connection newConn = getConnectionWithProps((Properties) null);

        try {
            newConn.close();
            ((com.mysql.jdbc.Connection) newConn).changeUser(props.getProperty(NonRegisteringDriver.USER_PROPERTY_KEY),
                    props.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY));
            fail("Expected SQL Exception");
        } catch (SQLException ex) {
            // expected
            if (!ex.getClass().getName().endsWith("MySQLNonTransientConnectionException")) {
                throw ex;
            }
        } finally {
            newConn.close();
        }
    }

    public void testBug63284() throws Exception {
        Properties props = new Driver().parseURL(BaseTestCase.dbUrl, null);
        props.setProperty("autoReconnect", "true");
        props.setProperty("socketFactory", "testsuite.UnreliableSocketFactory");

        Properties urlProps = new NonRegisteringDriver().parseURL(BaseTestCase.dbUrl, null);

        String host = urlProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
        String port = urlProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

        props.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);
        props.remove(NonRegisteringDriver.NUM_HOSTS_PROPERTY_KEY);
        props.remove(NonRegisteringDriver.HOST_PROPERTY_KEY + ".1");
        props.remove(NonRegisteringDriver.PORT_PROPERTY_KEY + ".1");

        props.setProperty("queriesBeforeRetryMaster", "50");
        props.setProperty("maxReconnects", "1");

        UnreliableSocketFactory.mapHost("master", host);
        UnreliableSocketFactory.mapHost("slave", host);

        Connection failoverConnection1 = null;
        Connection failoverConnection2 = null;

        try {
            failoverConnection1 = getConnectionWithProps("jdbc:mysql://master:" + port + ",slave:" + port + "/", props);

            failoverConnection2 = getConnectionWithProps("jdbc:mysql://master:" + port + ",slave:" + port + "/", props);

            assert (((com.mysql.jdbc.Connection) failoverConnection1).isMasterConnection());

            // Two different Connection objects should not equal each other:
            assert (!failoverConnection1.equals(failoverConnection2));

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
            assert (!((com.mysql.jdbc.Connection) failoverConnection1).isMasterConnection());

            // ensure that hashCode() result is persistent across failover events when proxy state changes
            assert (failoverConnection1.hashCode() == hc);
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
        if (versionMeetsMinimum(5, 5, 7)) {

            Connection testConn = null;
            Properties props = new Properties();

            props.setProperty("defaultAuthenticationPlugin", "");
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

            props.setProperty("defaultAuthenticationPlugin", "mysql_native_password");
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

            props.setProperty("defaultAuthenticationPlugin", "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
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

            props.setProperty("authenticationPlugins", "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
            props.setProperty("defaultAuthenticationPlugin", "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
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
    }

    public void testDisabledPlugins() throws Exception {
        if (versionMeetsMinimum(5, 5, 7)) {

            Connection testConn = null;
            Properties props = new Properties();

            props.setProperty("disabledAuthenticationPlugins", "mysql_native_password");
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

            props.setProperty("disabledAuthenticationPlugins", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");
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

            props.setProperty("authenticationPlugins", "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
            props.setProperty("defaultAuthenticationPlugin", "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
            props.setProperty("disabledAuthenticationPlugins", "auth_test_plugin");
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

            props.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");
            props.setProperty("authenticationPlugins", "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
            props.setProperty("disabledAuthenticationPlugins", "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
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
    }

    public void testAuthTestPlugin() throws Exception {
        if (versionMeetsMinimum(5, 5, 7)) {

            boolean install_plugin_in_runtime = false;
            try {

                // install plugin if required
                this.rs = this.stmt.executeQuery("select (PLUGIN_LIBRARY LIKE 'auth_test_plugin%') as `TRUE`"
                        + " FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='test_plugin_server'");
                if (this.rs.next()) {
                    if (!this.rs.getBoolean(1)) {
                        install_plugin_in_runtime = true;
                    }
                } else {
                    install_plugin_in_runtime = true;
                }

                if (install_plugin_in_runtime) {
                    String ext = System.getProperty("os.name").toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
                    this.stmt.executeUpdate("INSTALL PLUGIN test_plugin_server SONAME 'auth_test_plugin" + ext + "'");
                }

                Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
                String dbname = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
                if (dbname == null) {
                    assertTrue("No database selected", false);
                }

                // create proxy users
                this.stmt.executeUpdate("grant usage on *.* to 'wl5851user'@'%' identified WITH test_plugin_server AS 'plug_dest'");
                this.stmt.executeUpdate("grant usage on *.* to 'plug_dest'@'%' IDENTIFIED BY 'foo'");
                this.stmt.executeUpdate("GRANT PROXY ON 'plug_dest'@'%' TO 'wl5851user'@'%'");
                this.stmt.executeUpdate("delete from mysql.db where user='plug_dest'");
                this.stmt
                        .executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '"
                                + dbname + "', 'plug_dest', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
                this.stmt
                        .executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', 'information\\_schema', 'plug_dest', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
                this.stmt.executeUpdate("flush privileges");

                props = new Properties();
                props.setProperty("user", "wl5851user");
                props.setProperty("password", "plug_dest");
                props.setProperty("authenticationPlugins", "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");

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
                        testConn.close();
                    }
                }

            } finally {
                this.stmt.executeUpdate("drop user 'wl5851user'@'%'");
                this.stmt.executeUpdate("drop user 'plug_dest'@'%'");
                if (install_plugin_in_runtime) {
                    this.stmt.executeUpdate("UNINSTALL PLUGIN test_plugin_server");
                }
            }
        }
    }

    public void testTwoQuestionsPlugin() throws Exception {
        if (versionMeetsMinimum(5, 5, 7)) {

            boolean install_plugin_in_runtime = false;
            try {

                // install plugin if required
                this.rs = this.stmt.executeQuery("select (PLUGIN_LIBRARY LIKE 'two_questions%') as `TRUE`"
                        + " FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='two_questions'");
                if (this.rs.next()) {
                    if (!this.rs.getBoolean(1)) {
                        install_plugin_in_runtime = true;
                    }
                } else {
                    install_plugin_in_runtime = true;
                }

                if (install_plugin_in_runtime) {
                    String ext = System.getProperty("os.name").toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
                    this.stmt.executeUpdate("INSTALL PLUGIN two_questions SONAME 'auth" + ext + "'");
                }

                Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
                String dbname = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
                if (dbname == null) {
                    assertTrue("No database selected", false);
                }

                this.stmt.executeUpdate("grant usage on *.* to 'wl5851user2'@'%' identified WITH two_questions AS 'two_questions_password'");
                this.stmt.executeUpdate("delete from mysql.db where user='wl5851user2'");
                this.stmt
                        .executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '"
                                + dbname + "', 'wl5851user2', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
                this.stmt
                        .executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', 'information\\_schema', 'wl5851user2', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
                this.stmt.executeUpdate("flush privileges");

                props = new Properties();
                props.setProperty("user", "wl5851user2");
                props.setProperty("password", "two_questions_password");
                props.setProperty("authenticationPlugins", "testsuite.regression.ConnectionRegressionTest$TwoQuestionsPlugin");

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
                        testConn.close();
                    }
                }

            } finally {
                this.stmt.executeUpdate("drop user 'wl5851user2'@'%'");
                if (install_plugin_in_runtime) {
                    this.stmt.executeUpdate("UNINSTALL PLUGIN two_questions");
                }
            }
        }
    }

    public void testThreeAttemptsPlugin() throws Exception {
        if (versionMeetsMinimum(5, 5, 7)) {

            boolean install_plugin_in_runtime = false;
            try {

                // install plugin if required
                this.rs = this.stmt.executeQuery("select (PLUGIN_LIBRARY LIKE 'three_attempts%') as `TRUE`"
                        + " FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='three_attempts'");
                if (this.rs.next()) {
                    if (!this.rs.getBoolean(1)) {
                        install_plugin_in_runtime = true;
                    }
                } else {
                    install_plugin_in_runtime = true;
                }

                if (install_plugin_in_runtime) {
                    String ext = System.getProperty("os.name").toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
                    this.stmt.executeUpdate("INSTALL PLUGIN three_attempts SONAME 'auth" + ext + "'");
                }

                Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
                String dbname = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
                if (dbname == null) {
                    assertTrue("No database selected", false);
                }

                this.stmt.executeUpdate("grant usage on *.* to 'wl5851user3'@'%' identified WITH three_attempts AS 'three_attempts_password'");
                this.stmt.executeUpdate("delete from mysql.db where user='wl5851user3'");
                this.stmt
                        .executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '"
                                + dbname + "', 'wl5851user3', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
                this.stmt
                        .executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', 'information\\_schema', 'wl5851user3', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
                this.stmt.executeUpdate("flush privileges");

                props = new Properties();
                props.setProperty("user", "wl5851user3");
                props.setProperty("password", "three_attempts_password");
                props.setProperty("authenticationPlugins", "testsuite.regression.ConnectionRegressionTest$ThreeAttemptsPlugin");

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
                        testConn.close();
                    }
                }

            } finally {
                this.stmt.executeUpdate("drop user 'wl5851user3'@'%'");
                if (install_plugin_in_runtime) {
                    this.stmt.executeUpdate("UNINSTALL PLUGIN three_attempts");
                }
            }
        }
    }

    public static class AuthTestPlugin implements AuthenticationPlugin {

        private String password = null;

        public void init(com.mysql.jdbc.Connection conn1, Properties props) throws SQLException {
        }

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

        public boolean nextAuthenticationStep(Buffer fromServer, List<Buffer> toServer) throws SQLException {
            toServer.clear();
            Buffer bresp = new Buffer(StringUtils.getBytes(this.password));
            toServer.add(bresp);
            return true;
        }

    }

    public static class TwoQuestionsPlugin implements AuthenticationPlugin {

        private String password = null;

        public void init(com.mysql.jdbc.Connection conn1, Properties props) throws SQLException {
        }

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

        public boolean nextAuthenticationStep(Buffer fromServer, List<Buffer> toServer) throws SQLException {
            toServer.clear();
            if ((fromServer.getByteBuffer()[0] & 0xff) == 4) {
                Buffer bresp = new Buffer(StringUtils.getBytes(this.password));
                toServer.add(bresp);
            } else {
                Buffer bresp = new Buffer(StringUtils.getBytes("yes, of course"));
                toServer.add(bresp);
            }
            return true;
        }

    }

    public static class ThreeAttemptsPlugin implements AuthenticationPlugin {

        private String password = null;
        private int counter = 0;

        public void init(com.mysql.jdbc.Connection conn1, Properties props) throws SQLException {
            this.counter = 0;
        }

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

        public boolean nextAuthenticationStep(Buffer fromServer, List<Buffer> toServer) throws SQLException {
            toServer.clear();
            this.counter++;
            if ((fromServer.getByteBuffer()[0] & 0xff) == 4) {
                Buffer bresp = new Buffer(StringUtils.getBytes(this.counter > 2 ? this.password : "wrongpassword" + this.counter));
                toServer.add(bresp);
            } else {
                Buffer bresp = new Buffer(fromServer.getByteBuffer());
                toServer.add(bresp);
            }
            return true;
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

            this.stmt.executeUpdate("CREATE USER 'bug64983user1'@'%' IDENTIFIED WITH mysql_old_password");
            this.stmt.executeUpdate("set password for 'bug64983user1'@'%' = OLD_PASSWORD('pwd')");
            this.stmt.executeUpdate("grant all on *.* to 'bug64983user1'@'%'");

            this.stmt.executeUpdate("CREATE USER 'bug64983user2'@'%' IDENTIFIED WITH mysql_old_password");
            this.stmt.executeUpdate("set password for 'bug64983user2'@'%' = OLD_PASSWORD('')");
            this.stmt.executeUpdate("grant all on *.* to 'bug64983user2'@'%'");

            this.stmt.executeUpdate("CREATE USER 'bug64983user3'@'%' IDENTIFIED WITH mysql_old_password");
            this.stmt.executeUpdate("grant all on *.* to 'bug64983user3'@'%'");

            this.stmt.executeUpdate("flush privileges");

            Properties props = new Properties();

            // connect with default plugin
            props.setProperty("user", "bug64983user1");
            props.setProperty("password", "pwd");
            testConn = getConnectionWithProps(props);
            ResultSet testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user1", testRs.getString(1).split("@")[0]);
            testConn.close();

            props.setProperty("user", "bug64983user2");
            props.setProperty("password", "");
            testConn = getConnectionWithProps(props);
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user2", testRs.getString(1).split("@")[0]);
            testConn.close();

            props.setProperty("user", "bug64983user3");
            props.setProperty("password", "");
            testConn = getConnectionWithProps(props);
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user3", testRs.getString(1).split("@")[0]);
            testConn.close();

            // connect with MysqlOldPasswordPlugin plugin
            props.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.MysqlOldPasswordPlugin");

            props.setProperty("user", "bug64983user1");
            props.setProperty("password", "pwd");
            testConn = getConnectionWithProps(props);
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user1", testRs.getString(1).split("@")[0]);
            testConn.close();

            props.setProperty("user", "bug64983user2");
            props.setProperty("password", "");
            testConn = getConnectionWithProps(props);
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user2", testRs.getString(1).split("@")[0]);
            testConn.close();

            props.setProperty("user", "bug64983user3");
            props.setProperty("password", "");
            testConn = getConnectionWithProps(props);
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user3", testRs.getString(1).split("@")[0]);

            // changeUser
            ((MySQLConnection) testConn).changeUser("bug64983user1", "pwd");
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user1", testRs.getString(1).split("@")[0]);

            ((MySQLConnection) testConn).changeUser("bug64983user2", "");
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user2", testRs.getString(1).split("@")[0]);

            ((MySQLConnection) testConn).changeUser("bug64983user3", "");
            testRs = testConn.createStatement().executeQuery("select USER()");
            testRs.next();
            assertEquals("bug64983user3", testRs.getString(1).split("@")[0]);

        } finally {
            try {
                this.stmt.executeUpdate("SET GLOBAL secure_auth = @current_secure_auth");
                this.stmt.executeUpdate("drop user 'bug64983user1'@'%'");
                this.stmt.executeUpdate("drop user 'bug64983user2'@'%'");
                this.stmt.executeUpdate("drop user 'bug64983user3'@'%'");

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
        if (versionMeetsMinimum(5, 5, 7)) {

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
                    String ext = System.getProperty("os.name").toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
                    this.stmt.executeUpdate("INSTALL PLUGIN cleartext_plugin_server SONAME 'auth_test_plugin" + ext + "'");
                }

                Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
                String dbname = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
                if (dbname == null) {
                    assertTrue("No database selected", false);
                }

                // create proxy users
                this.stmt.executeUpdate("grant usage on *.* to 'wl5735user'@'%' identified WITH cleartext_plugin_server AS ''");
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

                props = new Properties();
                props.setProperty("user", "wl5735user");
                props.setProperty("password", "");

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
                        testConn.close();
                    }
                }

                try {
                    String trustStorePath = "src/testsuite/ssl-test-certs/test-cert-store";
                    System.setProperty("javax.net.ssl.keyStore", trustStorePath);
                    System.setProperty("javax.net.ssl.keyStorePassword", "password");
                    System.setProperty("javax.net.ssl.trustStore", trustStorePath);
                    System.setProperty("javax.net.ssl.trustStorePassword", "password");
                    props.setProperty("useSSL", "true");
                    if (Util.getJVMVersion() < 8 && versionMeetsMinimum(5, 7, 6) && isCommunityEdition()) {
                        props.setProperty("enabledSSLCipherSuites", SSL_CIPHERS_FOR_576);
                    }
                    testConn = getConnectionWithProps(props);

                    assertTrue("SSL connection isn't actually established!", ((MySQLConnection) testConn).getIO().isSSLEstablished());

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
                        testConn.close();
                    }
                }

            } finally {
                this.stmt.executeUpdate("drop user 'wl5735user'@'%'");
                if (install_plugin_in_runtime) {
                    this.stmt.executeUpdate("UNINSTALL PLUGIN cleartext_plugin_server");
                }
            }
        }
    }

    /**
     * This test requires two server instances:
     * 1) main test server pointed by com.mysql.jdbc.testsuite.url variable
     * configured without RSA encryption support
     * 2) additional server instance pointed by com.mysql.jdbc.testsuite.url.sha256default
     * variable configured with default-authentication-plugin=sha256_password
     * and RSA encryption enabled.
     * 
     * To run this test please add this variable to ant call:
     * -Dcom.mysql.jdbc.testsuite.url.sha256default=jdbc:mysql://localhost:3307/test?user=root&password=pwd
     * 
     * @throws Exception
     */
    public void testSha256PasswordPlugin() throws Exception {
        String trustStorePath = "src/testsuite/ssl-test-certs/test-cert-store";
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
            if (allowsRsa(this.stmt)) {
                fail("RSA encryption must be disabled on " + System.getProperty("com.mysql.jdbc.testsuite.url") + " to run this test");
            }

            try {
                this.stmt.executeUpdate("SET @current_old_passwords = @@global.old_passwords");
                this.stmt.executeUpdate("grant all on *.* to 'wl5602user'@'%' identified WITH sha256_password");
                this.stmt.executeUpdate("grant all on *.* to 'wl5602nopassword'@'%' identified WITH sha256_password");
                this.stmt.executeUpdate("SET GLOBAL old_passwords= 2");
                this.stmt.executeUpdate("SET SESSION old_passwords= 2");
                this.stmt.executeUpdate(versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'wl5602user'@'%' IDENTIFIED BY 'pwd'"
                        : "set password for 'wl5602user'@'%' = PASSWORD('pwd')");
                this.stmt.executeUpdate("flush privileges");

                final Properties propsNoRetrieval = new Properties();
                propsNoRetrieval.setProperty("user", "wl5602user");
                propsNoRetrieval.setProperty("password", "pwd");

                final Properties propsNoRetrievalNoPassword = new Properties();
                propsNoRetrievalNoPassword.setProperty("user", "wl5602nopassword");
                propsNoRetrievalNoPassword.setProperty("password", "");

                final Properties propsAllowRetrieval = new Properties();
                propsAllowRetrieval.setProperty("user", "wl5602user");
                propsAllowRetrieval.setProperty("password", "pwd");
                propsAllowRetrieval.setProperty("allowPublicKeyRetrieval", "true");

                final Properties propsAllowRetrievalNoPassword = new Properties();
                propsAllowRetrievalNoPassword.setProperty("user", "wl5602nopassword");
                propsAllowRetrievalNoPassword.setProperty("password", "");
                propsAllowRetrievalNoPassword.setProperty("allowPublicKeyRetrieval", "true");

                // 1. without SSL
                // SQLException expected due to server doesn't recognize Public Key Retrieval packet
                assertThrows(SQLException.class, "Public Key Retrieval is not allowed", new Callable<Void>() {
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

                // 2. with serverRSAPublicKeyFile specified
                // SQLException expected due to server doesn't recognize RSA encrypted payload
                propsNoRetrieval.setProperty("serverRSAPublicKeyFile", "src/testsuite/ssl-test-certs/mykey.pub");
                propsNoRetrievalNoPassword.setProperty("serverRSAPublicKeyFile", "src/testsuite/ssl-test-certs/mykey.pub");
                propsAllowRetrieval.setProperty("serverRSAPublicKeyFile", "src/testsuite/ssl-test-certs/mykey.pub");
                propsAllowRetrievalNoPassword.setProperty("serverRSAPublicKeyFile", "src/testsuite/ssl-test-certs/mykey.pub");

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
                propsNoRetrieval.setProperty("useSSL", "true");
                propsNoRetrievalNoPassword.setProperty("useSSL", "true");
                propsAllowRetrieval.setProperty("useSSL", "true");
                propsAllowRetrievalNoPassword.setProperty("useSSL", "true");
                if (Util.getJVMVersion() < 8 && versionMeetsMinimum(5, 7, 6) && isCommunityEdition()) {
                    propsNoRetrieval.setProperty("enabledSSLCipherSuites", SSL_CIPHERS_FOR_576);
                    propsNoRetrievalNoPassword.setProperty("enabledSSLCipherSuites", SSL_CIPHERS_FOR_576);
                    propsAllowRetrieval.setProperty("enabledSSLCipherSuites", SSL_CIPHERS_FOR_576);
                    propsAllowRetrievalNoPassword.setProperty("enabledSSLCipherSuites", SSL_CIPHERS_FOR_576);
                }

                assertCurrentUser(null, propsNoRetrieval, "wl5602user", true);
                assertCurrentUser(null, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(null, propsAllowRetrieval, "wl5602user", true);
                assertCurrentUser(null, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // over SSL with client-default Sha256PasswordPlugin
                propsNoRetrieval.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.Sha256PasswordPlugin");
                propsNoRetrievalNoPassword.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.Sha256PasswordPlugin");
                propsAllowRetrieval.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.Sha256PasswordPlugin");
                propsAllowRetrievalNoPassword.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.Sha256PasswordPlugin");

                assertCurrentUser(null, propsNoRetrieval, "wl5602user", true);
                assertCurrentUser(null, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(null, propsAllowRetrieval, "wl5602user", true);
                assertCurrentUser(null, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

            } finally {
                this.stmt.executeUpdate("drop user 'wl5602user'@'%'");
                this.stmt.executeUpdate("drop user 'wl5602nopassword'@'%'");
                this.stmt.executeUpdate("flush privileges");
                this.stmt.executeUpdate("SET GLOBAL old_passwords = @current_old_passwords");
            }

        }

        /*
         * test against server with RSA support
         */
        final String sha256defaultDbUrl = System.getProperty("com.mysql.jdbc.testsuite.url.sha256default");
        if (sha256defaultDbUrl != null) {

            Properties props = new Properties();
            props.setProperty("allowPublicKeyRetrieval", "true");

            Connection c1 = getConnectionWithProps(sha256defaultDbUrl, props);
            Statement s1 = c1.createStatement();
            if (!pluginIsActive(s1, "sha256_password")) {
                fail("sha256_password required to run this test");
            }
            if (!allowsRsa(s1)) {
                fail("RSA encryption must be enabled on " + sha256defaultDbUrl + " to run this test");
            }

            try {
                // create user with long password and sha256_password auth
                s1.executeUpdate("SET @current_old_passwords = @@global.old_passwords");
                s1.executeUpdate("grant all on *.* to 'wl5602user'@'%' identified WITH sha256_password");
                s1.executeUpdate("grant all on *.* to 'wl5602nopassword'@'%' identified WITH sha256_password");
                s1.executeUpdate("SET GLOBAL old_passwords= 2");
                s1.executeUpdate("SET SESSION old_passwords= 2");
                s1.executeUpdate(((MySQLConnection) c1).versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'wl5602user'@'%' IDENTIFIED BY 'pwd'"
                        : "set password for 'wl5602user'@'%' = PASSWORD('pwd')");
                s1.executeUpdate("flush privileges");

                final Properties propsNoRetrieval = new Properties();
                propsNoRetrieval.setProperty("user", "wl5602user");
                propsNoRetrieval.setProperty("password", "pwd");

                final Properties propsNoRetrievalNoPassword = new Properties();
                propsNoRetrievalNoPassword.setProperty("user", "wl5602nopassword");
                propsNoRetrievalNoPassword.setProperty("password", "");

                final Properties propsAllowRetrieval = new Properties();
                propsAllowRetrieval.setProperty("user", "wl5602user");
                propsAllowRetrieval.setProperty("password", "pwd");
                propsAllowRetrieval.setProperty("allowPublicKeyRetrieval", "true");

                final Properties propsAllowRetrievalNoPassword = new Properties();
                propsAllowRetrievalNoPassword.setProperty("user", "wl5602nopassword");
                propsAllowRetrievalNoPassword.setProperty("password", "");
                propsAllowRetrievalNoPassword.setProperty("allowPublicKeyRetrieval", "true");

                // 1. with client-default MysqlNativePasswordPlugin
                propsNoRetrieval.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");
                propsAllowRetrieval.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");

                // 1.1. RSA
                propsNoRetrieval.setProperty("useSSL", "false");
                propsAllowRetrieval.setProperty("useSSL", "false");

                assertThrows(SQLException.class, "Public Key Retrieval is not allowed", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsNoRetrieval);
                        return null;
                    }
                });

                assertCurrentUser(sha256defaultDbUrl, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(sha256defaultDbUrl, propsAllowRetrieval, "wl5602user", false);
                assertCurrentUser(sha256defaultDbUrl, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 1.2. over SSL
                propsNoRetrieval.setProperty("useSSL", "true");
                propsNoRetrievalNoPassword.setProperty("useSSL", "true");
                propsAllowRetrieval.setProperty("useSSL", "true");
                propsAllowRetrievalNoPassword.setProperty("useSSL", "true");

                assertCurrentUser(sha256defaultDbUrl, propsNoRetrieval, "wl5602user", true);
                assertCurrentUser(sha256defaultDbUrl, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(sha256defaultDbUrl, propsAllowRetrieval, "wl5602user", true);
                assertCurrentUser(sha256defaultDbUrl, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 2. with client-default Sha256PasswordPlugin
                propsNoRetrieval.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.Sha256PasswordPlugin");
                propsNoRetrievalNoPassword.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.Sha256PasswordPlugin");
                propsAllowRetrieval.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.Sha256PasswordPlugin");
                propsAllowRetrievalNoPassword.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.Sha256PasswordPlugin");

                // 2.1. RSA
                propsNoRetrieval.setProperty("useSSL", "false");
                propsNoRetrievalNoPassword.setProperty("useSSL", "false");
                propsAllowRetrieval.setProperty("useSSL", "false");
                propsAllowRetrievalNoPassword.setProperty("useSSL", "false");

                assertThrows(SQLException.class, "Public Key Retrieval is not allowed", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsNoRetrieval);
                        return null;
                    }
                });

                assertCurrentUser(sha256defaultDbUrl, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(sha256defaultDbUrl, propsAllowRetrieval, "wl5602user", false);
                assertCurrentUser(sha256defaultDbUrl, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 2.2. over SSL
                propsNoRetrieval.setProperty("useSSL", "true");
                propsNoRetrievalNoPassword.setProperty("useSSL", "true");
                propsAllowRetrieval.setProperty("useSSL", "true");
                propsAllowRetrievalNoPassword.setProperty("useSSL", "true");

                assertCurrentUser(sha256defaultDbUrl, propsNoRetrieval, "wl5602user", true);
                assertCurrentUser(sha256defaultDbUrl, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(sha256defaultDbUrl, propsAllowRetrieval, "wl5602user", false);
                assertCurrentUser(sha256defaultDbUrl, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 3. with serverRSAPublicKeyFile specified
                propsNoRetrieval.setProperty("serverRSAPublicKeyFile", "src/testsuite/ssl-test-certs/mykey.pub");
                propsNoRetrievalNoPassword.setProperty("serverRSAPublicKeyFile", "src/testsuite/ssl-test-certs/mykey.pub");
                propsAllowRetrieval.setProperty("serverRSAPublicKeyFile", "src/testsuite/ssl-test-certs/mykey.pub");
                propsAllowRetrievalNoPassword.setProperty("serverRSAPublicKeyFile", "src/testsuite/ssl-test-certs/mykey.pub");

                // 3.1. RSA
                propsNoRetrieval.setProperty("useSSL", "false");
                propsNoRetrievalNoPassword.setProperty("useSSL", "false");
                propsAllowRetrieval.setProperty("useSSL", "false");
                propsAllowRetrievalNoPassword.setProperty("useSSL", "false");

                assertCurrentUser(sha256defaultDbUrl, propsNoRetrieval, "wl5602user", false);
                assertCurrentUser(sha256defaultDbUrl, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(sha256defaultDbUrl, propsAllowRetrieval, "wl5602user", false);
                assertCurrentUser(sha256defaultDbUrl, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 3.2. Runtime setServerRSAPublicKeyFile must be denied 
                final Connection c2 = getConnectionWithProps(sha256defaultDbUrl, propsNoRetrieval);
                assertThrows(SQLException.class, "Dynamic change of ''serverRSAPublicKeyFile'' is not allowed.", new Callable<Void>() {
                    public Void call() throws Exception {
                        ((ConnectionProperties) c2).setServerRSAPublicKeyFile("src/testsuite/ssl-test-certs/mykey.pub");
                        return null;
                    }
                });
                c2.close();

                // 3.3. Runtime setAllowPublicKeyRetrieval must be denied 
                final Connection c3 = getConnectionWithProps(sha256defaultDbUrl, propsNoRetrieval);
                assertThrows(SQLException.class, "Dynamic change of ''allowPublicKeyRetrieval'' is not allowed.", new Callable<Void>() {
                    public Void call() throws Exception {
                        ((ConnectionProperties) c3).setAllowPublicKeyRetrieval(true);
                        return null;
                    }
                });
                c3.close();

                // 3.4. over SSL
                propsNoRetrieval.setProperty("useSSL", "true");
                propsNoRetrievalNoPassword.setProperty("useSSL", "true");
                propsAllowRetrieval.setProperty("useSSL", "true");
                propsAllowRetrievalNoPassword.setProperty("useSSL", "true");

                assertCurrentUser(sha256defaultDbUrl, propsNoRetrieval, "wl5602user", true);
                assertCurrentUser(sha256defaultDbUrl, propsNoRetrievalNoPassword, "wl5602nopassword", false);
                assertCurrentUser(sha256defaultDbUrl, propsAllowRetrieval, "wl5602user", true);
                assertCurrentUser(sha256defaultDbUrl, propsAllowRetrievalNoPassword, "wl5602nopassword", false);

                // 4. with wrong serverRSAPublicKeyFile specified
                propsNoRetrieval.setProperty("serverRSAPublicKeyFile", "unexistant/dummy.pub");
                propsNoRetrievalNoPassword.setProperty("serverRSAPublicKeyFile", "unexistant/dummy.pub");
                propsAllowRetrieval.setProperty("serverRSAPublicKeyFile", "unexistant/dummy.pub");
                propsAllowRetrievalNoPassword.setProperty("serverRSAPublicKeyFile", "unexistant/dummy.pub");

                // 4.1. RSA
                propsNoRetrieval.setProperty("useSSL", "false");
                propsNoRetrievalNoPassword.setProperty("useSSL", "false");
                propsAllowRetrieval.setProperty("useSSL", "false");
                propsAllowRetrievalNoPassword.setProperty("useSSL", "false");

                propsNoRetrieval.setProperty("paranoid", "false");
                propsNoRetrievalNoPassword.setProperty("paranoid", "false");
                propsAllowRetrieval.setProperty("paranoid", "false");
                propsAllowRetrievalNoPassword.setProperty("paranoid", "false");
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsNoRetrievalNoPassword);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsAllowRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsAllowRetrievalNoPassword);
                        return null;
                    }
                });

                propsNoRetrieval.setProperty("paranoid", "true");
                propsNoRetrievalNoPassword.setProperty("paranoid", "true");
                propsAllowRetrieval.setProperty("paranoid", "true");
                propsAllowRetrievalNoPassword.setProperty("paranoid", "true");
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsNoRetrievalNoPassword);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsAllowRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsAllowRetrievalNoPassword);
                        return null;
                    }
                });

                // 4.2. over SSL
                propsNoRetrieval.setProperty("useSSL", "true");
                propsNoRetrievalNoPassword.setProperty("useSSL", "true");
                propsAllowRetrieval.setProperty("useSSL", "true");
                propsAllowRetrievalNoPassword.setProperty("useSSL", "true");

                propsNoRetrieval.setProperty("paranoid", "false");
                propsNoRetrievalNoPassword.setProperty("paranoid", "false");
                propsAllowRetrieval.setProperty("paranoid", "false");
                propsAllowRetrievalNoPassword.setProperty("paranoid", "false");
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsNoRetrievalNoPassword);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsAllowRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key 'unexistant/dummy.pub'.*", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsAllowRetrievalNoPassword);
                        return null;
                    }
                });

                propsNoRetrieval.setProperty("paranoid", "true");
                propsNoRetrievalNoPassword.setProperty("paranoid", "true");
                propsAllowRetrieval.setProperty("paranoid", "true");
                propsAllowRetrievalNoPassword.setProperty("paranoid", "true");
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsNoRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsNoRetrievalNoPassword);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsAllowRetrieval);
                        return null;
                    }
                });
                assertThrows(SQLException.class, "Unable to read public key ", new Callable<Void>() {
                    @SuppressWarnings("synthetic-access")
                    public Void call() throws Exception {
                        getConnectionWithProps(sha256defaultDbUrl, propsAllowRetrievalNoPassword);
                        return null;
                    }
                });

            } finally {
                if (c1 != null) {
                    if (s1 != null) {
                        s1.executeUpdate("drop user 'wl5602user'@'%'");
                        s1.executeUpdate("drop user 'wl5602nopassword'@'%'");
                        s1.executeUpdate("flush privileges");
                        s1.executeUpdate("SET GLOBAL old_passwords = @current_old_passwords");
                        s1.close();
                    }
                    c1.close();
                }
            }
        }
    }

    private void assertCurrentUser(String url, Properties props, String expectedUser, boolean sslRequired) throws SQLException {
        Connection connection = url == null ? getConnectionWithProps(props) : getConnectionWithProps(url, props);
        if (sslRequired) {
            assertTrue("SSL connection isn't actually established!", ((MySQLConnection) connection).getIO().isSSLEstablished());
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
        props.setProperty("characterSetResults", "ISO88591");

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

        props.setProperty("characterSetResults", "null");

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
        if (versionMeetsMinimum(5, 5, 0)) {
            Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
            String dbname = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
            if (dbname == null) {
                assertTrue("No database selected", false);
            }

            props = new Properties();
            props.setProperty("characterEncoding", "EUC_JP");

            Connection testConn = null;
            Statement testSt = null;
            ResultSet testRs = null;
            try {
                testConn = getConnectionWithProps(props);
                testSt = testConn.createStatement();
                testRs = testSt.executeQuery("SELECT * FROM `" + dbname + "`.`\u307b\u3052\u307b\u3052`");
            } catch (SQLException e1) {
                if (e1.getClass().getName().endsWith("MySQLSyntaxErrorException")) {
                    assertEquals("Table '" + dbname + ".\u307B\u3052\u307b\u3052' doesn't exist", e1.getMessage());
                } else if (e1.getErrorCode() == MysqlErrorNumbers.ER_FILE_NOT_FOUND) {
                    // this could happen on Windows with 5.5 and 5.6 servers where BUG#14642248 exists
                    assertTrue(e1.getMessage().contains("Can't find file"));
                } else {
                    throw e1;
                }

                try {
                    props.setProperty("characterSetResults", "SJIS");
                    testConn = getConnectionWithProps(props);
                    testSt = testConn.createStatement();
                    testSt.execute("SET lc_messages = 'ru_RU'");
                    testRs = testSt.executeQuery("SELECT * FROM `" + dbname + "`.`\u307b\u3052\u307b\u3052`");
                } catch (SQLException e2) {
                    if (e2.getClass().getName().endsWith("MySQLSyntaxErrorException")) {
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
                props.setProperty("characterSetResults", "EUC_JP");
                props.setProperty("cacheServerConfiguration", "true");
                testConn = getConnectionWithProps(props);
                testSt = testConn.createStatement();
                testRs = testSt.executeQuery("SELECT * FROM `" + dbname + "`.`\u307b\u3052\u307b\u3052`");
                fail("Exception should be thrown for attemping to query non-existing table");
            } catch (SQLException e1) {
                if (e1.getClass().getName().endsWith("MySQLSyntaxErrorException")) {
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

            // Error messages may also be received after the handshake but before connection initialization is complete. This tests the interpretation of
            // errors thrown during this time window using an invalid session variable
            try {
                props.setProperty("characterSetResults", "EUC_JP");
                props.setProperty("sessionVariables", "lc_messages=ru_RU,invalidVar=1");
                testConn = getConnectionWithProps(props);
                fail("Exception should be thrown for attempting to set an unknown system variable");
            } catch (SQLException e1) {
                // The Russian version of this error message is 45 characters long. A mis-interpretation, e.g. decoding as latin1, would return a length of 75
                assertEquals(45, e1.getMessage().length());
            } finally {
                testConn.close();
            }
        }
    }

    public void testIsLocal() throws Exception {
        boolean normalState = ((ConnectionImpl) this.conn).isServerLocal();

        if (normalState) {
            boolean isNotLocal = ((ConnectionImpl) getConnectionWithProps(StandardSocketFactory.IS_LOCAL_HOSTNAME_REPLACEMENT_PROPERTY_NAME
                    + "=www.oracle.com:3306")).isServerLocal();

            assertFalse(isNotLocal == normalState);
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
            props.setProperty("profileSQL", "true");
            props.setProperty("useNanosForElapsedTime", "true");
            props.setProperty("logger", "testsuite.simple.TestBug57662Logger");
            conn_is = getConnectionWithProps(props);
            this.rs = conn_is.getMetaData().getColumns(null, null, "testBug57662", "%");

            assertFalse(((testsuite.simple.TestBug57662Logger) ((ConnectionImpl) conn_is).getLog()).hasNegativeDurations);

        } finally {
            if (conn_is != null) {
                conn_is.close();
            }
        }

    }

    public void testBug14563127() throws Exception {
        Properties props = new Properties();
        props.setProperty("loadBalanceStrategy", ForcedLoadBalanceStrategy.class.getName());
        props.setProperty("loadBalanceBlacklistTimeout", "5000");
        props.setProperty("loadBalancePingTimeout", "100");
        props.setProperty("loadBalanceValidateConnectionOnSwapServer", "true");

        String portNumber = new NonRegisteringDriver().parseURL(dbUrl, null).getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

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
        props.put("useCompression", "true");
        Connection conn1 = getConnectionWithProps(props);
        Statement stmt1 = conn1.createStatement();

        int updateCount = stmt1.executeUpdate("LOAD DATA LOCAL INFILE '" + fileNameBuf.toString() + "' INTO TABLE testBug11237 CHARACTER SET "
                + CharsetMapping.getMysqlCharsetForJavaEncoding(((MySQLConnection) this.conn).getEncoding(), (com.mysql.jdbc.Connection) conn1));

        assertTrue(updateCount == loops);

    }

    public void testStackOverflowOnMissingInterceptor() throws Exception {
        try {
            Properties props = new Properties();
            props.setProperty("statementInterceptors", "fooBarBaz");

            getConnectionWithProps(props).close();
        } catch (Exception e) {
        }
    }

    public void testExpiredPassword() throws Exception {
        if (versionMeetsMinimum(5, 6, 10)) {
            Connection testConn = null;
            Statement testSt = null;
            ResultSet testRs = null;

            Properties urlProps = new NonRegisteringDriver().parseURL(dbUrl, null);
            String dbname = urlProps.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);

            try {

                this.stmt.executeUpdate("grant all on `" + dbname + "`.* to 'must_change1'@'%' IDENTIFIED BY 'aha'");
                this.stmt.executeUpdate("grant all on `" + dbname + "`.* to 'must_change2'@'%' IDENTIFIED BY 'aha'");
                this.stmt.executeUpdate(versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'must_change1'@'%', 'must_change2'@'%' PASSWORD EXPIRE"
                        : "ALTER USER 'must_change1'@'%' PASSWORD EXPIRE, 'must_change2'@'%' PASSWORD EXPIRE");

                Properties props = new Properties();

                // ALTER USER can be prepared as of 5.6.8 (BUG#14646014)
                if (versionMeetsMinimum(5, 6, 8)) {
                    props.setProperty("useServerPrepStmts", "true");
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

                props.setProperty("user", "must_change1");
                props.setProperty("password", "aha");

                try {
                    testConn = getConnectionWithProps(props);
                    fail("SQLException expected due to password expired");
                } catch (SQLException e1) {

                    if (e1.getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || e1.getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD_LOGIN) {

                        props.setProperty("disconnectOnExpiredPasswords", "false");
                        try {
                            testConn = getConnectionWithProps(props);
                            testSt = testConn.createStatement();
                            testRs = testSt.executeQuery("SHOW VARIABLES LIKE 'disconnect_on_expired_password'");
                            fail("SQLException expected due to password expired");

                        } catch (SQLException e3) {
                            if (e3.getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD_LOGIN) {
                                testConn = getConnectionWithProps(props);
                                testSt = testConn.createStatement();
                            }
                            testSt.executeUpdate(versionMeetsMinimum(5, 7, 6) ? "ALTER USER USER() IDENTIFIED BY 'newpwd'"
                                    : "SET PASSWORD = PASSWORD('newpwd')");
                            testConn.close();

                            props.setProperty("user", "must_change1");
                            props.setProperty("password", "newpwd");
                            props.setProperty("disconnectOnExpiredPasswords", "true");
                            testConn = getConnectionWithProps(props);
                            testSt = testConn.createStatement();
                            testRs = testSt.executeQuery("SHOW VARIABLES LIKE 'disconnect_on_expired_password'");
                            assertTrue(testRs.next());

                            // change user
                            try {
                                ((MySQLConnection) testConn).changeUser("must_change2", "aha");
                                fail("SQLException expected due to password expired");

                            } catch (SQLException e4) {
                                if (e4.getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD
                                        || e4.getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD_LOGIN) {
                                    props.setProperty("disconnectOnExpiredPasswords", "false");
                                    testConn = getConnectionWithProps(props);

                                    try {
                                        ((MySQLConnection) testConn).changeUser("must_change2", "aha");
                                        testSt = testConn.createStatement();
                                        testRs = testSt.executeQuery("SHOW VARIABLES LIKE 'disconnect_on_expired_password'");
                                        fail("SQLException expected due to password expired");

                                    } catch (SQLException e5) {
                                        if (e5.getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD_LOGIN) {
                                            testConn = getConnectionWithProps(props);
                                            testSt = testConn.createStatement();
                                        }
                                        testSt.executeUpdate(versionMeetsMinimum(5, 7, 6) ? "ALTER USER USER() IDENTIFIED BY 'newpwd'"
                                                : "SET PASSWORD = PASSWORD('newpwd')");
                                        testConn.close();

                                        props.setProperty("user", "must_change2");
                                        props.setProperty("password", "newpwd");
                                        props.setProperty("disconnectOnExpiredPasswords", "true");
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
                this.stmt.executeUpdate("drop user 'must_change1'@'%'");
                this.stmt.executeUpdate("drop user 'must_change2'@'%'");
            }

        }

    }

    public void testBug68011() throws Exception {

        Connection c = null;
        try {
            Properties props = new Properties();
            props.setProperty("noDatetimeStringSync", "true");
            props.setProperty("useTimezone", "true");
            c = getConnectionWithProps(props);
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains("noDatetimeStringSync"));
        } finally {
            if (c != null) {
                c.close();
            }
        }
    }

    /**
     * Tests connection attributes
     * 
     * @throws Exception
     */
    public void testConnectionAttributes() throws Exception {
        if (!versionMeetsMinimum(5, 6)) {
            return;
        }
        Properties props = new Properties();
        props.setProperty("connectionAttributes", "first:one,again:two");
        props.setProperty("user", "root");
        Connection attConn = super.getConnectionWithProps(props);
        ResultSet rslt = attConn.createStatement()
                .executeQuery("SELECT * FROM performance_schema.session_connect_attrs WHERE processlist_id = CONNECTION_ID()");
        Map<String, Integer> matchedCounts = new HashMap<String, Integer>();

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
                assertEquals(System.getProperty("java.version"), val);
            } else if (key.equals("_os")) {
                assertEquals(NonRegisteringDriver.OS, val);
            } else if (key.equals("_platform")) {
                assertEquals(NonRegisteringDriver.PLATFORM, val);
            } else if (key.equals("_runtime_vendor")) {
                assertEquals(System.getProperty("java.vendor"), val);
            } else if (key.equals("_client_version")) {
                assertEquals(NonRegisteringDriver.VERSION, val);
            } else if (key.equals("_client_license")) {
                assertEquals(NonRegisteringDriver.LICENSE, val);
            } else if (key.equals("_client_name")) {
                assertEquals(NonRegisteringDriver.NAME, val);
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

        props.setProperty("connectionAttributes", "none");
        attConn = super.getConnectionWithProps(props);
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

        Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
        String host = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY, "localhost");
        String port = props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");
        String hostSpec = host;
        if (!NonRegisteringDriver.isHostPropertiesList(host)) {
            hostSpec = host + ":" + port;
        }

        String database = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
        removeHostRelatedProps(props);
        props.remove(NonRegisteringDriver.DBNAME_PROPERTY_KEY);

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
            mysqlCls = jcl.loadClass("com.mysql.jdbc.Connection");
        } else {
            mysqlCls = Class.forName("com.mysql.jdbc.Connection", true, null);
        }

        if ((mysqlCls != null) && (mysqlCls.isAssignableFrom(jcls))) {
            Method abort = mysqlCls.getMethod("abortInternal", new Class[] {});
            boolean hasAbortMethod = abort != null;
            assertTrue("abortInternal() method should be found for connection class " + jcls, hasAbortMethod);
        } else {
            fail("com.mysql.jdbc.Connection interface IS NOT ASSIGNABE from connection class " + jcls);
        }
        //-------------

        // Concurrent test
        System.out.println("Warming up");
        for (int i = 0; i < failoverconnection.length; i++) {
            this.stmt = failoverconnection[i].createStatement();
            this.pstmt = failoverconnection[i].prepareStatement("SELECT 1 FROM DUAL");
            for (int j = 0; j < 10000; j++) {
                this.pstmt.executeQuery();
                this.stmt.executeQuery("SELECT 1 FROM DUAL");
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
        props.setProperty("loadBalanceStrategy", ForcedLoadBalanceStrategy.class.getName());
        props.setProperty("loadBalancePingTimeout", "100");
        props.setProperty("autoReconnect", "true");
        props.setProperty("retriesAllDown", "1");

        String portNumber = new NonRegisteringDriver().parseURL(dbUrl, null).getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

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
                        Proxy.getInvocationHandler(this.c).invoke(this.c, Connection.class.getMethod("close", new Class[] {}), null);
                    } else if (this.num == 8 || this.num == 11) {
                        Proxy.getInvocationHandler(this.c).invoke(this.c, MySQLConnection.class.getMethod("abortInternal", new Class[] {}), null);
                    } else if (this.num == 9 || this.num == 12) {
                        Proxy.getInvocationHandler(this.c).invoke(this.c, com.mysql.jdbc.Connection.class.getMethod("abort", new Class[] { Executor.class }),
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

        Field f = com.mysql.jdbc.NonRegisteringDriver.class.getDeclaredField("connectionPhantomRefs");
        f.setAccessible(true);
        Map<?, ?> connectionTrackingMap = (Map<?, ?>) f.get(com.mysql.jdbc.NonRegisteringDriver.class);

        Field referentField = java.lang.ref.Reference.class.getDeclaredField("referent");
        referentField.setAccessible(true);

        createTable("testBug68400", "(x VARCHAR(255) NOT NULL DEFAULT '')");
        String s1 = "a very very very very very very very very very very very very very very very very very very very very very very very very large string to ensure compression enabled";
        this.stmt.executeUpdate("insert into testBug68400 values ('" + s1 + "')");

        Properties props = new Properties();
        props.setProperty("useCompression", "true");
        props.setProperty("connectionAttributes", "testBug68400:true");

        testMemLeakBatch(props, connectionTrackingMap, referentField, 0, 0, s1, "testBug68400:true");
        testMemLeakBatch(props, connectionTrackingMap, referentField, 0, 1, s1, "testBug68400:true");
        testMemLeakBatch(props, connectionTrackingMap, referentField, 0, 2, s1, "testBug68400:true");

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
    private void testMemLeakBatch(Properties props, Map<?, ?> connectionTrackingMap, Field referentField, int connectionType, int finType, String s1,
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
                    Properties baseprops = new Driver().parseURL(BaseTestCase.dbUrl, null);
                    baseprops.setProperty("autoReconnect", "true");
                    baseprops.setProperty("socketFactory", "testsuite.UnreliableSocketFactory");

                    Properties urlProps = new NonRegisteringDriver().parseURL(BaseTestCase.dbUrl, null);
                    String host = urlProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
                    String port = urlProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

                    baseprops.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);
                    baseprops.remove(NonRegisteringDriver.NUM_HOSTS_PROPERTY_KEY);
                    baseprops.remove(NonRegisteringDriver.HOST_PROPERTY_KEY + ".1");
                    baseprops.remove(NonRegisteringDriver.PORT_PROPERTY_KEY + ".1");

                    baseprops.setProperty("queriesBeforeRetryMaster", "50");
                    baseprops.setProperty("maxReconnects", "1");

                    UnreliableSocketFactory.mapHost("master", host);
                    UnreliableSocketFactory.mapHost("slave", host);

                    baseprops.putAll(props);

                    connection = getConnectionWithProps("jdbc:mysql://master:" + port + ",slave:" + port + "/", baseprops);
                    break;
                case 3:
                    //ReplicationConnection;
                    Properties replProps = new Properties();
                    replProps.putAll(props);
                    replProps.setProperty("loadBalanceStrategy", ForcedLoadBalanceStrategy.class.getName());
                    replProps.setProperty("loadBalancePingTimeout", "100");
                    replProps.setProperty("autoReconnect", "true");

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
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                if (finType == 1) {
                    connection.close();
                } else if (finType == 2) {
                    ((com.mysql.jdbc.Connection) connection).abortInternal();
                }
                connection = null;
            }
        }

        // 2. Count connections before GC
        System.out.println("MAP: " + connectionTrackingMap.size());

        connectionNumber = countTestConnections(connectionTrackingMap, referentField, false, attributeValue);
        System.out.println("Test related connections in MAP before GC: " + connectionNumber);

        // 3. Run GC
        Runtime.getRuntime().gc();

        // 4. Sleep to ensure abandoned connection clean up occurred
        Thread.sleep(2000);

        // 5. Count connections before GC
        connectionNumber = countTestConnections(connectionTrackingMap, referentField, true, attributeValue);
        System.out.println("Test related connections in MAP after GC: " + connectionNumber);
        System.out.println("MAP: " + connectionTrackingMap.size());

        assertEquals("No connection with \"" + attributeValue
                + "\" connection attribute should exist in NonRegisteringDriver.connectionPhantomRefs map after GC", 0, connectionNumber);
    }

    private int countTestConnections(Map<?, ?> connectionTrackingMap, Field referentField, boolean show, String attributValue) throws Exception {
        int connectionNumber = 0;
        for (Object o1 : connectionTrackingMap.keySet()) {
            com.mysql.jdbc.Connection ctmp = (com.mysql.jdbc.Connection) referentField.get(o1);
            try {
                if (ctmp != null && ctmp.getConnectionAttributes() != null && ctmp.getConnectionAttributes().equals(attributValue)) {
                    connectionNumber++;
                    if (show) {
                        System.out.println(ctmp.toString());
                    }
                }
            } catch (NullPointerException e) {
                System.out.println("NullPointerException: \n" + ctmp + "\n" + ctmp.getConnectionAttributes());
            } catch (MySQLNonTransientConnectionException e) {
                System.out.println("MySQLNonTransientConnectionException (expected for explicitly closed load-balanced connection)");
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
        Properties props1 = new NonRegisteringDriver().parseURL(dbUrl, null);
        String host = props1.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY, "localhost");
        String url = "jdbc:mysql://" + host;
        if (!NonRegisteringDriver.isHostPropertiesList(host)) {
            String port = props1.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");
            url = url + ":" + port;
        }

        try {
            props.setProperty("characterEncoding", "UTF-8");
            c1 = getConnectionWithProps(props);
            st1 = c1.createStatement();
            st1.execute("create database if not exists `\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8`");
            st1.execute("grant all on `\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8`.* to '\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8'@'%' identified by 'msandbox'");

            props = new Properties();
            props.setProperty("user", "\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8");
            props.setProperty("password", "msandbox");
            c2 = DriverManager.getConnection(url + "/\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8", props);
            c2.createStatement().executeQuery("select 1");
            c2.close();

        } catch (SQLException e) {
            assertFalse("e.getCause() instanceof java.lang.ArrayIndexOutOfBoundsException", e.getCause() instanceof java.lang.ArrayIndexOutOfBoundsException);

            props.setProperty("user", "\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8");
            c2 = DriverManager.getConnection(url + "/\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8", props);
            c2.createStatement().executeQuery("select 1");
            c2.close();
        } finally {
            if (c2 != null) {
                c2.close();
            }
            if (st1 != null) {
                st1.executeUpdate("drop user '\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8'@'%'");
                st1.executeUpdate("drop database if exists `\u30C6\u30B9\u30C8\u30C6\u30B9\u30C8`");
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
        testStatement.executeQuery("CALL testBug69746_proc");
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
        testStatement.executeQuery("SELECT 2");
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
            return ex.getMessage().equalsIgnoreCase(Messages.getString("Statement.49"));
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
     * -Dcom.mysql.jdbc.testsuite.url.sha256default=jdbc:mysql://localhost:3307/test?user=root&password=pwd
     * 
     * @throws Exception
     */
    public void testLongAuthResponsePayload() throws Exception {

        String sha256defaultDbUrl = System.getProperty("com.mysql.jdbc.testsuite.url.sha256default");
        if (sha256defaultDbUrl != null && versionMeetsMinimum(5, 6, 6)) {

            Properties props = new Properties();
            props.setProperty("allowPublicKeyRetrieval", "true");

            // check that sha256_password plugin is available
            Connection c1 = DriverManager.getConnection(sha256defaultDbUrl, props);
            Statement s1 = c1.createStatement();
            if (!pluginIsActive(s1, "sha256_password")) {
                fail("sha256_password required to run this test");
            }

            try {
                // create user with long password and sha256_password auth
                s1.executeUpdate("SET @current_old_passwords = @@global.old_passwords");
                s1.executeUpdate("grant all on *.* to 'wl6134user'@'%' identified WITH sha256_password");
                s1.executeUpdate("SET GLOBAL old_passwords= 2");
                s1.executeUpdate("SET SESSION old_passwords= 2");
                s1.executeUpdate(((MySQLConnection) c1).versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'wl6134user'@'%' IDENTIFIED BY 'aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee"
                        + "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee"
                        + "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee"
                        + "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee'"
                        : "set password for 'wl6134user'@'%' = PASSWORD('aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee"
                                + "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee"
                                + "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee"
                                + "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee')");
                s1.executeUpdate("flush privileges");

                props.setProperty("user", "wl6134user");
                props.setProperty("password", "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee"
                        + "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee"
                        + "aaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeeeaaaaaaaaaabbbbbbbbbbccccccccccddddddddddeeeeeeeeee");
                props.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.Sha256PasswordPlugin");

                Connection testConn = null;
                try {
                    testConn = DriverManager.getConnection(sha256defaultDbUrl, props);
                    fail("SQLException expected due to password is too long for RSA encryption");
                } catch (Exception e) {
                    assertTrue(e.getMessage().startsWith("Data must not be longer than"));
                } finally {
                    if (testConn != null) {
                        testConn.close();
                    }
                }

                try {
                    String trustStorePath = "src/testsuite/ssl-test-certs/test-cert-store";
                    System.setProperty("javax.net.ssl.keyStore", trustStorePath);
                    System.setProperty("javax.net.ssl.keyStorePassword", "password");
                    System.setProperty("javax.net.ssl.trustStore", trustStorePath);
                    System.setProperty("javax.net.ssl.trustStorePassword", "password");

                    props.setProperty("useSSL", "true");
                    assertCurrentUser(sha256defaultDbUrl, props, "wl6134user", true);

                } catch (Exception e) {
                    throw e;
                } finally {
                    if (testConn != null) {
                        testConn.close();
                    }
                }
            } finally {
                if (c1 != null) {
                    if (s1 != null) {
                        s1.executeUpdate("drop user 'wl6134user'@'%'");
                        s1.executeUpdate("flush privileges");
                        s1.executeUpdate("SET GLOBAL old_passwords = @current_old_passwords");
                        s1.close();
                    }
                    c1.close();
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
        com.mysql.jdbc.Connection connWithMemProps;
        long[] memMultiplier = new long[] { 1024, 1024 * 1024, 1024 * 1024 * 1024 };

        // reflection is needed to access protected info from ConnectionPropertiesImpl.largeRowSizeThreshold
        Field propField = com.mysql.jdbc.ConnectionPropertiesImpl.class.getDeclaredField("largeRowSizeThreshold");
        propField.setAccessible(true);
        Class<?> propClass = null;
        for (Class<?> nestedClass : com.mysql.jdbc.ConnectionPropertiesImpl.class.getDeclaredClasses()) {
            if (nestedClass.getName().equals("com.mysql.jdbc.ConnectionPropertiesImpl$IntegerConnectionProperty")) {
                propClass = nestedClass;
                break;
            }
        }
        Method propMethod = propClass.getDeclaredMethod("getValueAsInt");
        propMethod.setAccessible(true);

        for (int i = 0; i < testMemUnits.length; i++) {
            for (int j = 0; j < testMemUnits[i].length; j++) {
                // testing with memory values under 2GB because higher values aren't supported.
                connWithMemProps = (com.mysql.jdbc.Connection) getConnectionWithProps(String.format(
                        "blobSendChunkSize=1.2%1$s,largeRowSizeThreshold=1.4%1$s,locatorFetchBufferSize=1.6%1$s", testMemUnits[i][j]));

                // test values of property 'blobSendChunkSize'
                assertEquals("Memory unit '" + testMemUnits[i][j] + "'; property 'blobSendChunkSize'", (int) (memMultiplier[i] * 1.2),
                        connWithMemProps.getBlobSendChunkSize());

                // test values of property 'largeRowSizeThreshold'
                assertEquals("Memory unit '" + testMemUnits[i][j] + "'; property 'largeRowSizeThreshold'", "1.4" + testMemUnits[i][j],
                        connWithMemProps.getLargeRowSizeThreshold());
                assertEquals("Memory unit '" + testMemUnits[i][j] + "'; property 'largeRowSizeThreshold'", (int) (memMultiplier[i] * 1.4),
                        ((Integer) propMethod.invoke(propField.get(connWithMemProps))).intValue());

                // test values of property 'locatorFetchBufferSize'
                assertEquals("Memory unit '" + testMemUnits[i][j] + "'; property 'locatorFetchBufferSize'", (int) (memMultiplier[i] * 1.6),
                        connWithMemProps.getLocatorFetchBufferSize());

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
            @SuppressWarnings("synthetic-access")
            public Void call() throws Exception {
                getConnectionWithProps("useServerPrepStmts=true,maxAllowedPacket=" + (maxPacketSizeThreshold - 1)).close();
                return null;
            }
        });

        assertThrows(SQLException.class, "Connection setting too low for 'maxAllowedPacket'.*", new Callable<Void>() {
            @SuppressWarnings("synthetic-access")
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
        long cnt0 = 0;
        long cnt1 = 0;
        for (int i = 0; i < 1000; i++) {
            cnt0 -= System.currentTimeMillis();
            Connection c = getConnectionWithProps("detectCustomCollations=false");
            cnt0 += System.currentTimeMillis();
            c.close();
            cnt1 -= System.currentTimeMillis();
            c = getConnectionWithProps("detectCustomCollations=true");
            cnt1 += System.currentTimeMillis();
            c.close();
        }
        System.out.println("detectCustomCollations=false: " + cnt0 + "\ndetectCustomCollations=true : " + cnt1);
        assertTrue(cnt0 < cnt1);
    }

    /**
     * Internal method for tests to get a replcation connection with a
     * single master host to the test URL.
     */
    private ReplicationConnection getTestReplicationConnectionNoSlaves(String masterHost) throws Exception {
        Properties props = getPropertiesFromTestsuiteUrl();
        List<String> masterHosts = new ArrayList<String>();
        masterHosts.add(masterHost);
        List<String> slaveHosts = new ArrayList<String>(); // empty
        ReplicationConnection replConn = new ReplicationConnection(props, props, masterHosts, slaveHosts);
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
        String masterHost = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY) + ":" + props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);
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
        String masterHost = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY) + ":" + props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);
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
            assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex.getSQLState());
        }
        try {
            s.execute("truncate non_existing_table");
            fail("executeUpdate should not be allowed in read-only mode");
        } catch (SQLException ex) {
            assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex.getSQLState());
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
            @SuppressWarnings("synthetic-access")
            public Void call() throws Exception {
                getConnectionWithProps("exceptionInterceptors=testsuite.regression.ConnectionRegressionTest$TestBug71850ExceptionInterceptor,"
                        + "user=unexistent_user");
                return null;
            }
        });
    }

    public static class TestBug71850ExceptionInterceptor implements ExceptionInterceptor {

        private int counter = 0;

        public void init(com.mysql.jdbc.Connection conn, Properties props) throws SQLException {
            this.counter++;
        }

        public void destroy() {
        }

        public SQLException interceptException(SQLException sqlEx, com.mysql.jdbc.Connection conn) {

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
        dataSource.setUseCursorFetch(true);
        dataSource.setDefaultFetchSize(50);
        dataSource.setUseServerPrepStmts(true);
        dataSource.setExceptionInterceptors("testsuite.regression.ConnectionRegressionTest$TestBug67803ExceptionInterceptor");

        XAConnection testXAConn1 = dataSource.getXAConnection();
        testXAConn1.getXAResource().start(new MysqlXid("2".getBytes(), "2".getBytes(), 1), 0);
    }

    public static class TestBug67803ExceptionInterceptor implements ExceptionInterceptor {

        public void init(com.mysql.jdbc.Connection conn, Properties props) throws SQLException {
        }

        public void destroy() {
        }

        public SQLException interceptException(SQLException sqlEx, com.mysql.jdbc.Connection conn) {
            if (sqlEx.getErrorCode() == 1295 || sqlEx.getMessage().contains("This command is not supported in the prepared statement protocol yet")) {
                // SQLException will not be re-thrown if emulateUnsupportedPstmts=true, thus throw RuntimeException to fail the test
                throw new RuntimeException(sqlEx);
            }
            return sqlEx;
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
        if (!((MySQLConnection) this.conn).getServerVariable("character_set_server").equals("latin1")) {
            return;
        }

        Properties p = new Properties();
        p.setProperty("characterEncoding", "cp1252");
        p.setProperty("characterSetResults", "cp1252");
        p.setProperty("statementInterceptors", Bug72712StatementInterceptor.class.getName());

        getConnectionWithProps(p);
        // exception will be thrown from the statement interceptor if any SET statements are issued
    }

    /**
     * Statement interceptor used to implement preceding test.
     */
    public static class Bug72712StatementInterceptor extends BaseStatementInterceptor {
        @Override
        public ResultSetInternalMethods preProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, com.mysql.jdbc.Connection connection)
                throws SQLException {
            if (sql.contains("SET NAMES") || sql.contains("character_set_results") && !(sql.contains("SHOW VARIABLES") || sql.contains("SELECT @@"))) {
                throw new SQLException("Wrongt statement issued: " + sql);
            }
            return null;
        }
    }

    /**
     * Test for Bug#62577 - XA connection fails with ClassCastException
     */
    public void testBug62577() throws Exception {

        Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
        String host = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY, "localhost");
        String port = props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");

        String hostSpec = host;

        if (!NonRegisteringDriver.isHostPropertiesList(host)) {
            hostSpec = host + ":" + port;
        }

        String database = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
        removeHostRelatedProps(props);
        props.remove(NonRegisteringDriver.DBNAME_PROPERTY_KEY);

        StringBuilder configs = new StringBuilder();
        for (@SuppressWarnings("rawtypes")
        Map.Entry entry : props.entrySet()) {
            configs.append(entry.getKey());
            configs.append("=");
            configs.append(entry.getValue());
            configs.append("&");
        }
        String cfg1 = configs.toString();

        configs.append("pinGlobalTxToPhysicalConnection");
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
        st.executeQuery("SELECT 1;");
        xaConn.close();
    }

    /**
     * Test fix for Bug#18869381 - CHANGEUSER() FOR SHA USER RESULTS IN NULLPOINTEREXCEPTION
     * 
     * This test requires additional server instance configured with
     * default-authentication-plugin=sha256_password and RSA encryption enabled.
     * 
     * To run this test please add this variable to ant call:
     * -Dcom.mysql.jdbc.testsuite.url.sha256default=jdbc:mysql://localhost:3307/test?user=root&password=pwd
     * 
     * @throws Exception
     */
    public void testBug18869381() throws Exception {

        String sha256defaultDbUrl = System.getProperty("com.mysql.jdbc.testsuite.url.sha256default");
        if (sha256defaultDbUrl != null && versionMeetsMinimum(5, 6, 6)) {

            Properties props = new Properties();
            props.setProperty("allowPublicKeyRetrieval", "true");

            // check that sha256_password plugin is available
            Connection con = DriverManager.getConnection(sha256defaultDbUrl, props);
            Statement st = con.createStatement();
            if (!pluginIsActive(st, "sha256_password")) {
                fail("sha256_password required to run this test");
            }

            try {
                st.executeUpdate("SET @current_old_passwords = @@global.old_passwords");
                st.executeUpdate("grant all on *.* to 'bug18869381user1'@'%' identified WITH sha256_password");
                st.executeUpdate("grant all on *.* to 'bug18869381user2'@'%' identified WITH sha256_password");
                st.executeUpdate("grant all on *.* to 'bug18869381user3'@'%' identified WITH mysql_native_password");
                st.executeUpdate(((MySQLConnection) con).versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'bug18869381user3'@'%' IDENTIFIED BY 'pwd3'"
                        : "set password for 'bug18869381user3'@'%' = PASSWORD('pwd3')");
                st.executeUpdate("SET GLOBAL old_passwords= 2");
                st.executeUpdate("SET SESSION old_passwords= 2");
                st.executeUpdate(((MySQLConnection) con).versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'bug18869381user1'@'%' IDENTIFIED BY 'LongLongLongLongLongLongLongLongLongLongLongLongPwd1'"
                        : "set password for 'bug18869381user1'@'%' = PASSWORD('LongLongLongLongLongLongLongLongLongLongLongLongPwd1')");
                st.executeUpdate(((MySQLConnection) con).versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'bug18869381user2'@'%' IDENTIFIED BY 'pwd2'"
                        : "set password for 'bug18869381user2'@'%' = PASSWORD('pwd2')");
                st.executeUpdate("flush privileges");

                props.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");
                props.setProperty("useCompression", "false");
                testBug18869381WithProperties(sha256defaultDbUrl, props);
                props.setProperty("useCompression", "true");
                testBug18869381WithProperties(sha256defaultDbUrl, props);

                props.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.Sha256PasswordPlugin");
                props.setProperty("useCompression", "false");
                testBug18869381WithProperties(sha256defaultDbUrl, props);
                props.setProperty("useCompression", "true");
                testBug18869381WithProperties(sha256defaultDbUrl, props);

                props.setProperty("serverRSAPublicKeyFile", "src/testsuite/ssl-test-certs/mykey.pub");
                props.setProperty("useCompression", "false");
                testBug18869381WithProperties(sha256defaultDbUrl, props);
                props.setProperty("useCompression", "true");
                testBug18869381WithProperties(sha256defaultDbUrl, props);

                String trustStorePath = "src/testsuite/ssl-test-certs/test-cert-store";
                System.setProperty("javax.net.ssl.keyStore", trustStorePath);
                System.setProperty("javax.net.ssl.keyStorePassword", "password");
                System.setProperty("javax.net.ssl.trustStore", trustStorePath);
                System.setProperty("javax.net.ssl.trustStorePassword", "password");
                props.setProperty("useSSL", "true");
                props.setProperty("useCompression", "false");
                testBug18869381WithProperties(sha256defaultDbUrl, props);
                props.setProperty("useCompression", "true");
                testBug18869381WithProperties(sha256defaultDbUrl, props);

            } finally {
                st.executeUpdate("drop user 'bug18869381user1'@'%'");
                st.executeUpdate("drop user 'bug18869381user2'@'%'");
                st.executeUpdate("drop user 'bug18869381user3'@'%'");
                st.executeUpdate("flush privileges");
                st.executeUpdate("SET GLOBAL old_passwords = @current_old_passwords");
                con.close();
            }
        }
    }

    private void testBug18869381WithProperties(String sha256defaultDbUrl, Properties props) throws Exception {
        Connection testConn = null;
        Statement testSt = null;
        ResultSet testRs = null;

        try {
            testConn = getConnectionWithProps(sha256defaultDbUrl, props);

            ((MySQLConnection) testConn).changeUser("bug18869381user1", "LongLongLongLongLongLongLongLongLongLongLongLongPwd1");
            testSt = testConn.createStatement();
            testRs = testSt.executeQuery("select USER(),CURRENT_USER()");
            testRs.next();
            assertEquals("bug18869381user1", testRs.getString(1).split("@")[0]);
            assertEquals("bug18869381user1", testRs.getString(2).split("@")[0]);
            testSt.close();

            ((MySQLConnection) testConn).changeUser("bug18869381user2", "pwd2");
            testSt = testConn.createStatement();
            testRs = testSt.executeQuery("select USER(),CURRENT_USER()");
            testRs.next();
            assertEquals("bug18869381user2", testRs.getString(1).split("@")[0]);
            assertEquals("bug18869381user2", testRs.getString(2).split("@")[0]);
            testSt.close();

            ((MySQLConnection) testConn).changeUser("bug18869381user3", "pwd3");
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
            Statement testStmt = this.conn.createStatement();
            testStmt.executeQuery("SELECT 1");
            testStmt.close();
            testConn.close();
        } catch (SQLException e) {
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
                        @SuppressWarnings("synthetic-access")
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
        Socket underlyingSocket;

        @Override
        public Socket connect(String hostname, int portNumber, Properties props) throws SocketException, IOException {
            return this.underlyingSocket = new ConnectionRegressionTest.TestBug73053SocketWrapper(super.connect(hostname, portNumber, props));
        }

        @Override
        public Socket beforeHandshake() throws SocketException, IOException {
            super.beforeHandshake();
            return this.underlyingSocket;
        }

        @Override
        public Socket afterHandshake() throws SocketException, IOException {
            super.afterHandshake();
            return this.underlyingSocket;
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
        if (versionMeetsMinimum(5, 5, 7)) {
            Connection con = null;
            this.stmt.executeUpdate("grant all on *.* to 'bug19354014user'@'%' identified WITH mysql_native_password");
            this.stmt.executeUpdate(versionMeetsMinimum(5, 7, 6) ? "ALTER USER 'bug19354014user'@'%' IDENTIFIED BY 'pwd'"
                    : "set password for 'bug19354014user'@'%' = PASSWORD('pwd')");
            this.stmt.executeUpdate("flush privileges");

            try {
                Properties props = new Properties();
                props.setProperty("useCompression", "true");

                con = getConnectionWithProps(props);
                ((MySQLConnection) con).changeUser("bug19354014user", "pwd");
            } finally {
                this.stmt.executeUpdate("drop user 'bug19354014user'@'%'");
                this.stmt.executeUpdate("flush privileges");

                if (con != null) {
                    con.close();
                }
            }
        }
    }

    /**
     * Tests fix for Bug#75168 - loadBalanceExceptionChecker interface cannot work using JDBC4/JDK7
     * 
     * Bug observed only with JDBC4 classes. This test is a duplication of testsuite.regression.jdbc4.ConnectionRegressionTest#testBug75168().
     * The two nested static classes, Bug75168LoadBalanceExceptionChecker and Bug75168StatementInterceptor are shared between the two tests.
     * 
     * @throws Exception
     */
    public void testBug75168() throws Exception {
        final Properties props = new Properties();
        props.setProperty("loadBalanceExceptionChecker", "testsuite.regression.ConnectionRegressionTest$Bug75168LoadBalanceExceptionChecker");
        props.setProperty("statementInterceptors", Bug75168StatementInterceptor.class.getName());

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
            props.setProperty("useServerPrepStmts", "true");
        } while (stop = !stop);
    }

    public static class Bug75168LoadBalanceExceptionChecker implements LoadBalanceExceptionChecker {
        public void init(com.mysql.jdbc.Connection conn, Properties props) throws SQLException {
        }

        public void destroy() {
        }

        public boolean shouldExceptionTriggerFailover(SQLException ex) {
            return ex.getMessage().endsWith("nonexistent_table' doesn't exist");
        }
    }

    public static class Bug75168StatementInterceptor extends BaseStatementInterceptor {
        static Connection previousConnection = null;

        @Override
        public void destroy() {
            if (previousConnection == null) {
                fail("Test testBug75168 didn't run as expected.");
            }
        }

        @Override
        public ResultSetInternalMethods preProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, com.mysql.jdbc.Connection connection)
                throws SQLException {
            if (sql == null) {
                sql = "";
            }
            if (sql.length() == 0 && interceptedStatement instanceof com.mysql.jdbc.PreparedStatement) {
                sql = ((com.mysql.jdbc.PreparedStatement) interceptedStatement).asSql();
            }
            if (sql.indexOf("nonexistent_table") >= 0) {
                assertTrue("Different connection expected.", !connection.equals(previousConnection));
                previousConnection = connection;
            }
            return null;
        }
    }

    /**
     * Tests fix for BUG#71084 - Wrong java.sql.Date stored if client and server time zones differ
     * 
     * This tests the behavior of the new connection property 'noTimezoneConversionForDateType'
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug71084() throws Exception {
        createTable("testBug71084", "(id INT, dt DATE)");

        Properties connProps = new Properties();
        connProps.setProperty("cacheDefaultTimezone", "false");

        /*
         * case 0: default settings (no conversions)
         */
        testBug71084AssertCase(connProps, "GMT+2", "GMT+6", null, "1998-05-21", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-6", "GMT+2", null, "1998-05-21", "1998-05-21", "1998-05-21 0:00:00");

        /*
         * case 1: connection property 'useLegacyDatetimeCode=false'
         */
        connProps.setProperty("useLegacyDatetimeCode", "false");

        // client 25 hours behind server
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", null, "1998-05-21 22:59:59", "1998-05-22", "1998-05-20 23:00:00");
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", null, "1998-05-21 23:00:00", "1998-05-23", "1998-05-21 23:00:00");
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", null, "1998-05-22 22:59:59", "1998-05-23", "1998-05-21 23:00:00");
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", null, "1998-05-22 23:00:00", "1998-05-24", "1998-05-22 23:00:00");
        // client 25 hours behind server, 24 hours behind target calendar
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", "GMT+11", "1998-05-20 23:59:59", "1998-05-21", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", "GMT+11", "1998-05-21 0:00:00", "1998-05-22", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", "GMT+11", "1998-05-21 23:59:59", "1998-05-22", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", "GMT+11", "1998-05-22 0:00:00", "1998-05-23", "1998-05-22 0:00:00");

        // client 24 hours behind server
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", null, "1998-05-20 23:59:59", "1998-05-21", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", null, "1998-05-21 0:00:00", "1998-05-22", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", null, "1998-05-21 23:59:59", "1998-05-22", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", null, "1998-05-22 0:00:00", "1998-05-23", "1998-05-22 0:00:00");
        // client 24 hours behind server, 25 hours behind target calendar
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", "GMT+15", "1998-05-21 22:59:59", "1998-05-22", "1998-05-20 23:00:00");
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", "GMT+15", "1998-05-21 23:00:00", "1998-05-23", "1998-05-21 23:00:00");
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", "GMT+15", "1998-05-22 22:59:59", "1998-05-23", "1998-05-21 23:00:00");
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", "GMT+15", "1998-05-22 23:00:00", "1998-05-24", "1998-05-22 23:00:00");

        // client 2 hours behind server
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", null, "1998-05-21 21:59:59", "1998-05-21", "1998-05-20 22:00:00");
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", null, "1998-05-21 22:00:00", "1998-05-22", "1998-05-21 22:00:00");
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", null, "1998-05-22 21:59:59", "1998-05-22", "1998-05-21 22:00:00");
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", null, "1998-05-22 22:00:00", "1998-05-23", "1998-05-22 22:00:00");
        // client 2 hours behind server, 2 hours ahead of target calendar
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", "GMT+6", "1998-05-21 1:59:59", "1998-05-20", "1998-05-20 2:00:00");
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", "GMT+6", "1998-05-21 2:00:00", "1998-05-21", "1998-05-21 2:00:00");
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", "GMT+6", "1998-05-22 1:59:59", "1998-05-21", "1998-05-21 2:00:00");
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", "GMT+6", "1998-05-22 2:00:00", "1998-05-22", "1998-05-22 2:00:00");

        // client and server in the same time zone
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", null, "1998-05-20 23:59:59", "1998-05-20", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", null, "1998-05-21 0:00:00", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", null, "1998-05-21 23:59:59", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", null, "1998-05-22 0:00:00", "1998-05-22", "1998-05-22 0:00:00");
        // client, server and target calendar in the same time zone
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", "GMT+7", "1998-05-20 23:59:59", "1998-05-20", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", "GMT+7", "1998-05-21 0:00:00", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", "GMT+7", "1998-05-21 23:59:59", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", "GMT+7", "1998-05-22 0:00:00", "1998-05-22", "1998-05-22 0:00:00");

        // client 2 hours ahead of server
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", null, "1998-05-21 1:59:59", "1998-05-20", "1998-05-20 2:00:00");
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", null, "1998-05-21 2:00:00", "1998-05-21", "1998-05-21 2:00:00");
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", null, "1998-05-22 1:59:59", "1998-05-21", "1998-05-21 2:00:00");
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", null, "1998-05-22 2:00:00", "1998-05-22", "1998-05-22 2:00:00");
        // client 2 hours ahead of server, 2 hours behind target calendar
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", "GMT-7", "1998-05-21 21:59:59", "1998-05-21", "1998-05-20 22:00:00");
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", "GMT-7", "1998-05-21 22:00:00", "1998-05-22", "1998-05-21 22:00:00");
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", "GMT-7", "1998-05-22 21:59:59", "1998-05-22", "1998-05-21 22:00:00");
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", "GMT-7", "1998-05-22 22:00:00", "1998-05-23", "1998-05-22 22:00:00");

        // client 24 hours ahead of server
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", null, "1998-05-20 23:59:59", "1998-05-19", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", null, "1998-05-21 0:00:00", "1998-05-20", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", null, "1998-05-21 23:59:59", "1998-05-20", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", null, "1998-05-22 0:00:00", "1998-05-21", "1998-05-22 0:00:00");
        // client 24 hours ahead of server, 25 hours ahead of target calendar
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", "GMT-13", "1998-05-21 0:59:59", "1998-05-19", "1998-05-20 1:00:00");
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", "GMT-13", "1998-05-21 1:00:00", "1998-05-20", "1998-05-21 1:00:00");
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", "GMT-13", "1998-05-22 0:59:59", "1998-05-20", "1998-05-21 1:00:00");
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", "GMT-13", "1998-05-22 1:00:00", "1998-05-21", "1998-05-22 1:00:00");

        // client 25 hours ahead of server
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", null, "1998-05-21 0:59:59", "1998-05-19", "1998-05-20 1:00:00");
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", null, "1998-05-21 1:00:00", "1998-05-20", "1998-05-21 1:00:00");
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", null, "1998-05-22 0:59:59", "1998-05-20", "1998-05-21 1:00:00");
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", null, "1998-05-22 1:00:00", "1998-05-21", "1998-05-22 1:00:00");
        // client 25 hours ahead of server, 24 hours ahead of target calendar
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", "GMT-11", "1998-05-20 23:59:59", "1998-05-19", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", "GMT-11", "1998-05-21 0:00:00", "1998-05-20", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", "GMT-11", "1998-05-21 23:59:59", "1998-05-20", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", "GMT-11", "1998-05-22 0:00:00", "1998-05-21", "1998-05-22 0:00:00");
        connProps.remove("useLegacyDatetimeCode");

        /*
         * case 2: connection property 'useTimezone=true'
         */
        connProps.setProperty("useTimezone", "true");

        // client 25 hours behind server
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", null, "1998-05-20 23:59:59", "1998-05-20", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", null, "1998-05-21 0:00:00", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", null, "1998-05-21 23:59:59", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", null, "1998-05-22 0:00:00", "1998-05-22", "1998-05-22 0:00:00");
        // client 25 hours behind server, 24 hours behind target calendar
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", "GMT+11", "1998-05-20 23:59:59", "1998-05-21", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", "GMT+11", "1998-05-21 0:00:00", "1998-05-22", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", "GMT+11", "1998-05-21 23:59:59", "1998-05-22", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-13", "GMT+12", "GMT+11", "1998-05-22 0:00:00", "1998-05-23", "1998-05-22 0:00:00");

        // client 24 hours behind server
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", null, "1998-05-20 23:59:59", "1998-05-20", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", null, "1998-05-21 0:00:00", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", null, "1998-05-21 23:59:59", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", null, "1998-05-22 0:00:00", "1998-05-22", "1998-05-22 0:00:00");
        // client 24 hours behind server, 25 hours behind target calendar
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", "GMT+15", "1998-05-21 22:59:59", "1998-05-22", "1998-05-20 23:00:00");
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", "GMT+15", "1998-05-21 23:00:00", "1998-05-23", "1998-05-21 23:00:00");
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", "GMT+15", "1998-05-22 22:59:59", "1998-05-23", "1998-05-21 23:00:00");
        testBug71084AssertCase(connProps, "GMT-10", "GMT+14", "GMT+15", "1998-05-22 23:00:00", "1998-05-24", "1998-05-22 23:00:00");

        // client 2 hours behind server
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", null, "1998-05-20 23:59:59", "1998-05-20", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", null, "1998-05-21 0:00:00", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", null, "1998-05-21 23:59:59", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", null, "1998-05-22 0:00:00", "1998-05-22", "1998-05-22 0:00:00");
        // client 2 hours behind server, 2 hours ahead of target calendar
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", "GMT+6", "1998-05-21 1:59:59", "1998-05-20", "1998-05-20 2:00:00");
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", "GMT+6", "1998-05-21 2:00:00", "1998-05-21", "1998-05-21 2:00:00");
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", "GMT+6", "1998-05-22 1:59:59", "1998-05-21", "1998-05-21 2:00:00");
        testBug71084AssertCase(connProps, "GMT+8", "GMT+10", "GMT+6", "1998-05-22 2:00:00", "1998-05-22", "1998-05-22 2:00:00");

        // client and server in the same time zone
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", null, "1998-05-20 23:59:59", "1998-05-20", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", null, "1998-05-21 0:00:00", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", null, "1998-05-21 23:59:59", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", null, "1998-05-22 0:00:00", "1998-05-22", "1998-05-22 0:00:00");
        // client, server and target calendar in the same time zone
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", "GMT+7", "1998-05-20 23:59:59", "1998-05-20", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", "GMT+7", "1998-05-21 0:00:00", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", "GMT+7", "1998-05-21 23:59:59", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+7", "GMT+7", "GMT+7", "1998-05-22 0:00:00", "1998-05-22", "1998-05-22 0:00:00");

        // client 2 hours ahead of server
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", null, "1998-05-20 23:59:59", "1998-05-20", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", null, "1998-05-21 0:00:00", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", null, "1998-05-21 23:59:59", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", null, "1998-05-22 0:00:00", "1998-05-22", "1998-05-22 0:00:00");
        // client 2 hours ahead of server, 2 hours behind target calendar
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", "GMT-7", "1998-05-21 21:59:59", "1998-05-21", "1998-05-20 22:00:00");
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", "GMT-7", "1998-05-21 22:00:00", "1998-05-22", "1998-05-21 22:00:00");
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", "GMT-7", "1998-05-22 21:59:59", "1998-05-22", "1998-05-21 22:00:00");
        testBug71084AssertCase(connProps, "GMT-9", "GMT-11", "GMT-7", "1998-05-22 22:00:00", "1998-05-23", "1998-05-22 22:00:00");

        // client 24 hours ahead of server
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", null, "1998-05-20 23:59:59", "1998-05-20", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", null, "1998-05-21 0:00:00", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", null, "1998-05-21 23:59:59", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", null, "1998-05-22 0:00:00", "1998-05-22", "1998-05-22 0:00:00");
        // client 24 hours ahead of server, 25 hours ahead of target calendar
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", "GMT-13", "1998-05-21 0:59:59", "1998-05-19", "1998-05-20 1:00:00");
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", "GMT-13", "1998-05-21 1:00:00", "1998-05-20", "1998-05-21 1:00:00");
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", "GMT-13", "1998-05-22 0:59:59", "1998-05-20", "1998-05-21 1:00:00");
        testBug71084AssertCase(connProps, "GMT+12", "GMT-12", "GMT-13", "1998-05-22 1:00:00", "1998-05-21", "1998-05-22 1:00:00");

        // client 25 hours ahead of server
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", null, "1998-05-20 23:59:59", "1998-05-20", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", null, "1998-05-21 0:00:00", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", null, "1998-05-21 23:59:59", "1998-05-21", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", null, "1998-05-22 0:00:00", "1998-05-22", "1998-05-22 0:00:00");
        // client 25 hours ahead of server, 24 hours ahead of target calendar
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", "GMT-11", "1998-05-20 23:59:59", "1998-05-19", "1998-05-20 0:00:00");
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", "GMT-11", "1998-05-21 0:00:00", "1998-05-20", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", "GMT-11", "1998-05-21 23:59:59", "1998-05-20", "1998-05-21 0:00:00");
        testBug71084AssertCase(connProps, "GMT+13", "GMT-12", "GMT-11", "1998-05-22 0:00:00", "1998-05-21", "1998-05-22 0:00:00");
        connProps.remove("useTimezone");
    }

    private void testBug71084AssertCase(Properties connProps, String clientTZ, String serverTZ, String targetTZ, String insertDate, String expectedStoredDate,
            String expectedRetrievedDate) throws Exception {
        final TimeZone defaultTZ = TimeZone.getDefault();
        final boolean useTargetCal = targetTZ != null;
        final Properties testExtraProperties = new Properties();

        testExtraProperties.setProperty("", "");
        testExtraProperties.setProperty("useFastDateParsing", "false");
        testExtraProperties.setProperty("useJDBCCompliantTimezoneShift", "true");
        testExtraProperties.setProperty("useSSPSCompatibleTimezoneShift", "true");

        this.stmt.execute("DELETE FROM testBug71084");

        try {
            TimeZone.setDefault(TimeZone.getTimeZone(clientTZ));

            SimpleDateFormat longDateFrmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            longDateFrmt.setTimeZone(TimeZone.getDefault());
            SimpleDateFormat shortDateFrmt = new SimpleDateFormat("yyyy-MM-dd");
            shortDateFrmt.setTimeZone(TimeZone.getDefault());

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
            Date expectedDateInRSNoConv = shortDateFrmt.parse(expectedDateInDBNoConv);

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
                    if (!"cacheDefaultTimezone".equalsIgnoreCase((String) k)) {
                        propsList += "," + (String) k;
                    }
                }

                connPropsLocal.setProperty("serverTimezone", serverTZ);

                /*
                 * Test using the property "noTimezoneConversionForDateType=false". Conversions should occur.
                 */
                connPropsLocal.setProperty("noTimezoneConversionForDateType", "false");
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

                /*
                 * Repeat the test using the property "noTimezoneConversionForDateType=true". No conversions should occur now.
                 */
                id++;

                propsList += ",noTimezoneConversionForDateType";

                connPropsLocal.setProperty("noTimezoneConversionForDateType", "true");
                testConn = getConnectionWithProps(connPropsLocal);

                testPstmt = testConn.prepareStatement("INSERT INTO testBug71084 VALUES (?, ?)");
                testPstmt.setInt(1, id);
                if (useTargetCal) {
                    testPstmt.setDate(2, new java.sql.Date(dateIn.getTime()), targetCal);
                } else {
                    testPstmt.setDate(2, new java.sql.Date(dateIn.getTime()));
                }
                testPstmt.execute();
                testPstmt.close();

                testStmt = testConn.createStatement();
                // Get date value from database: Column `dt` - allowing time zone conversion by returning it as is; Column `dtStr` - preventing time zone
                // conversion by returning it as String and invalidating the date format so that no automatic conversion can ever happen.
                restRs = testStmt.executeQuery("SELECT dt, CONCAT('$', dt) AS dtStr FROM testBug71084 WHERE id = " + id);
                restRs.next();
                dateOut = useTargetCal ? restRs.getDate(1, targetCal) : restRs.getDate(1);
                dateInDB = restRs.getString(2).substring(1);
                restRs.close();
                testStmt.close();

                testConn.close();

                if (useTargetCal) {
                    assertEquals(id + ". [" + propsList + "] Date stored" + targetCalMsg, expectedDateInDB, dateInDB);
                    assertEquals(id + ". [" + propsList + "] Date retrieved" + targetCalMsg, longDateFrmt.format(expectedDateInRS),
                            longDateFrmt.format(dateOut));
                } else {
                    assertEquals(id + ". [" + propsList + "] Date stored" + targetCalMsg, expectedDateInDBNoConv, dateInDB);
                    assertEquals(id + ". [" + propsList + "] Date retrieved" + targetCalMsg, longDateFrmt.format(expectedDateInRSNoConv),
                            longDateFrmt.format(dateOut));
                }
            }
        } finally {
            TimeZone.setDefault(defaultTZ);
        }
    }

    /**
     * Tests fix for BUG#20685022 - SSL CONNECTION TO MYSQL 5.7.6 COMMUNITY SERVER FAILS
     * 
     * This test is duplicated in testuite.regression.ConnectionRegressionTest.jdbc4.testBug20685022().
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20685022() throws Exception {
        final boolean sslCipherSuitesReqFor576 = Util.getJVMVersion() < 8 && versionMeetsMinimum(5, 7, 6) && isCommunityEdition();
        final Properties props = new Properties();
        final Callable<Void> callableInstance = new Callable<Void>() {
            public Void call() throws Exception {
                getConnectionWithProps(props);
                return null;
            }
        };

        /*
         * case 1: non verifying server certificate
         */
        props.clear();
        props.setProperty("useSSL", "true");
        props.setProperty("requireSSL", "true");
        props.setProperty("verifyServerCertificate", "false");

        if (sslCipherSuitesReqFor576) {
            assertThrows(SQLException.class, Messages.getString("CommunicationsException.incompatibleSSLCipherSuites"), callableInstance);

            props.setProperty("enabledSSLCipherSuites", SSL_CIPHERS_FOR_576);
        }
        getConnectionWithProps(props);

        /*
         * case 2: verifying server certificate using key store provided by connection properties
         */
        props.clear();
        props.setProperty("useSSL", "true");
        props.setProperty("requireSSL", "true");
        props.setProperty("verifyServerCertificate", "true");
        props.setProperty("trustCertificateKeyStoreUrl", "file:src/testsuite/ssl-test-certs/test-cert-store");
        props.setProperty("trustCertificateKeyStoreType", "JKS");
        props.setProperty("trustCertificateKeyStorePassword", "password");

        if (sslCipherSuitesReqFor576) {
            assertThrows(SQLException.class, Messages.getString("CommunicationsException.incompatibleSSLCipherSuites"), callableInstance);

            props.setProperty("enabledSSLCipherSuites", SSL_CIPHERS_FOR_576);
        }

        getConnectionWithProps(props);

        /*
         * case 3: verifying server certificate using key store provided by system properties
         */
        props.clear();
        props.setProperty("useSSL", "true");
        props.setProperty("requireSSL", "true");
        props.setProperty("verifyServerCertificate", "true");

        String trustStorePath = "src/testsuite/ssl-test-certs/test-cert-store";
        System.setProperty("javax.net.ssl.keyStore", trustStorePath);
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStore", trustStorePath);
        System.setProperty("javax.net.ssl.trustStorePassword", "password");

        if (sslCipherSuitesReqFor576) {
            assertThrows(SQLException.class, Messages.getString("CommunicationsException.incompatibleSSLCipherSuites"), callableInstance);

            props.setProperty("enabledSSLCipherSuites", SSL_CIPHERS_FOR_576);
        }

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

            MySQLConnection con = (MySQLConnection) getConnectionWithProps("statementInterceptors=" + Bug75592StatementInterceptor.class.getName());

            // reference values
            Map<String, String> serverVariables = new HashMap<String, String>();
            this.rs = con.createStatement().executeQuery("SHOW VARIABLES");
            while (this.rs.next()) {
                serverVariables.put(this.rs.getString(1), this.rs.getString(2));
            }

            // check values from "select @@var..."
            assertEquals(serverVariables.get("auto_increment_increment"), con.getServerVariable("auto_increment_increment"));
            assertEquals(serverVariables.get("character_set_client"), con.getServerVariable("character_set_client"));
            assertEquals(serverVariables.get("character_set_connection"), con.getServerVariable("character_set_connection"));

            // we override character_set_results sometimes when configuring client charsets, thus need to check against actual value
            if (con.getServerVariable(ConnectionImpl.JDBC_LOCAL_CHARACTER_SET_RESULTS) == null) {
                assertEquals("", serverVariables.get("character_set_results"));
            } else {
                assertEquals(serverVariables.get("character_set_results"), con.getServerVariable(ConnectionImpl.JDBC_LOCAL_CHARACTER_SET_RESULTS));
            }

            assertEquals(serverVariables.get("character_set_server"), con.getServerVariable("character_set_server"));
            assertEquals(serverVariables.get("init_connect"), con.getServerVariable("init_connect"));
            assertEquals(serverVariables.get("interactive_timeout"), con.getServerVariable("interactive_timeout"));
            assertEquals(serverVariables.get("license"), con.getServerVariable("license"));
            assertEquals(serverVariables.get("lower_case_table_names"), con.getServerVariable("lower_case_table_names"));
            assertEquals(serverVariables.get("max_allowed_packet"), con.getServerVariable("max_allowed_packet"));
            assertEquals(serverVariables.get("net_buffer_length"), con.getServerVariable("net_buffer_length"));
            assertEquals(serverVariables.get("net_write_timeout"), con.getServerVariable("net_write_timeout"));
            assertEquals(serverVariables.get("query_cache_size"), con.getServerVariable("query_cache_size"));
            assertEquals(serverVariables.get("query_cache_type"), con.getServerVariable("query_cache_type"));

            // not necessarily contains STRICT_TRANS_TABLES
            for (String sm : serverVariables.get("sql_mode").split(",")) {
                if (!sm.equals("STRICT_TRANS_TABLES")) {
                    assertTrue(con.getServerVariable("sql_mode").contains(sm));
                }
            }

            assertEquals(serverVariables.get("system_time_zone"), con.getServerVariable("system_time_zone"));
            assertEquals(serverVariables.get("time_zone"), con.getServerVariable("time_zone"));
            assertEquals(serverVariables.get("tx_isolation"), con.getServerVariable("tx_isolation"));
            assertEquals(serverVariables.get("wait_timeout"), con.getServerVariable("wait_timeout"));
            if (!versionMeetsMinimum(5, 5, 0)) {
                assertEquals(serverVariables.get("language"), con.getServerVariable("language"));
            }
        }
    }

    /**
     * Statement interceptor for preceding testBug75592().
     */
    public static class Bug75592StatementInterceptor extends BaseStatementInterceptor {
        @Override
        public ResultSetInternalMethods preProcess(String sql, com.mysql.jdbc.Statement interceptedStatement, com.mysql.jdbc.Connection connection)
                throws SQLException {
            if (sql.contains("SHOW VARIABLES WHERE")) {
                throw new SQLException("'SHOW VARIABLES WHERE' statement issued: " + sql);
            }
            return null;
        }
    }

    /**
     * Tests fix for BUG#20825727 - CONNECT FAILURE WHEN TRY TO CONNECT SHA USER WITH DIFFERENT CHARSET.
     * 
     * This test runs through all authentication plugins when one of the following server requirements is met:
     * 1. Default connection string points to a server configured with both SSL *and* RSA encryption.
     * or
     * 2. Default connection string points to a server configured with SSL enabled but no RSA encryption *and* the property
     * com.mysql.jdbc.testsuite.url.sha256default points to an additional server configured with
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
        final String sha256defaultDbUrl = System.getProperty("com.mysql.jdbc.testsuite.url.sha256default");
        Properties props = new Properties();
        props.setProperty("allowPublicKeyRetrieval", "true");

        if (sha256defaultDbUrl != null) {
            com.mysql.jdbc.Connection testConn = (com.mysql.jdbc.Connection) getConnectionWithProps(sha256defaultDbUrl, props);
            if (testConn.versionMeetsMinimum(5, 5, 7)) {
                testDbUrls = new String[] { BaseTestCase.dbUrl, sha256defaultDbUrl };
            } else {
                testDbUrls = new String[] { BaseTestCase.dbUrl };
            }
            testConn.close();
        } else {
            testDbUrls = new String[] { BaseTestCase.dbUrl };
        }

        for (String testDbUrl : testDbUrls) {
            com.mysql.jdbc.Connection testConn = (com.mysql.jdbc.Connection) getConnectionWithProps(testDbUrl, props);
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
                this.rs = testStmt.executeQuery("SELECT (PLUGIN_LIBRARY LIKE 'auth_test_plugin%') FROM INFORMATION_SCHEMA.PLUGINS"
                        + " WHERE PLUGIN_NAME='cleartext_plugin_server'");
                if (!this.rs.next() || !this.rs.getBoolean(1)) {
                    String ext = System.getProperty("os.name").toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
                    testStmt.execute("INSTALL PLUGIN cleartext_plugin_server SONAME 'auth_test_plugin" + ext + "'");
                    clearTextPluginInstalled = true;
                }

                if (testConn.versionMeetsMinimum(5, 7, 5)) {
                    // mysql_old_password plugin not supported
                    plugins = new String[] { "cleartext_plugin_server,-1", "mysql_native_password,0", "sha256_password,2" };
                } else if (testConn.versionMeetsMinimum(5, 6, 6)) {
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
                            testBug20825727TestLogin(testDbUrl, testConn.getEncoding(), sslEnabled, rsaEnabled, "testBug20825727", simplePwd, encoding,
                                    pluginName);

                            testStep = "change password";
                            testBug20825727ChangePassword(testDbUrl, "testBug20825727", complexPwd, encoding, pluginName, pwdHashingMethod);
                            testStep = "login with complex password";
                            testBug20825727TestLogin(testDbUrl, testConn.getEncoding(), sslEnabled, rsaEnabled, "testBug20825727", complexPwd, encoding,
                                    pluginName);
                        } catch (SQLException e) {
                            e.printStackTrace();
                            fail("Failed at '" + testStep + "' using encoding '" + encoding + "' and plugin '" + pluginName
                                    + "'. See also system output for more details.");
                        } finally {
                            try {
                                testStmt.execute("DROP USER 'testBug20825727'@'%'");
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
        com.mysql.jdbc.Connection testConn = null;
        try {
            Properties props = new Properties();
            props.setProperty("allowPublicKeyRetrieval", "true");
            if (encoding.length() > 0) {
                props.setProperty("characterEncoding", encoding);
            }
            testConn = (com.mysql.jdbc.Connection) getConnectionWithProps(testDbUrl, props);
            Statement testStmt = testConn.createStatement();

            if (testConn.versionMeetsMinimum(5, 7, 6)) {
                testStmt.execute("CREATE USER '" + user + "'@'%' IDENTIFIED WITH " + pluginName + " BY '" + password + "'");
            } else if (pwdHashingMethod >= 0) {
                // for mysql_native_password, mysql_old_password and sha256_password plugins
                testStmt.execute("CREATE USER '" + user + "'@'%' IDENTIFIED WITH " + pluginName);
                testStmt.execute("SET @@session.old_passwords = " + pwdHashingMethod);
                testStmt.execute("SET PASSWORD FOR '" + user + "'@'%' = PASSWORD('" + password + "')");
                testStmt.execute("SET @@session.old_passwords = @@global.old_passwords");
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
        com.mysql.jdbc.Connection testConn = null;
        try {
            Properties props = new Properties();
            props.setProperty("allowPublicKeyRetrieval", "true");
            if (encoding.length() > 0) {
                props.setProperty("characterEncoding", encoding);
            }
            testConn = (com.mysql.jdbc.Connection) getConnectionWithProps(testDbUrl, props);
            Statement testStmt = testConn.createStatement();

            if (testConn.versionMeetsMinimum(5, 7, 6)) {
                testStmt.execute("ALTER USER '" + user + "'@'%' IDENTIFIED BY '" + password + "'");
            } else if (pwdHashingMethod >= 0) {
                // for mysql_native_password, mysql_old_password and sha256_password plugins
                testStmt.execute("SET @@session.old_passwords = " + pwdHashingMethod);
                testStmt.execute("SET PASSWORD FOR '" + user + "'@'%' = PASSWORD('" + password + "')");
                testStmt.execute("SET @@session.old_passwords = @@global.old_passwords");
            } else {
                // for cleartext_plugin_server plugin
                testStmt.execute("DROP USER '" + user + "'@'%'");
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
        props.setProperty("allowPublicKeyRetrieval", "true");
        final com.mysql.jdbc.MySQLConnection testBaseConn = (com.mysql.jdbc.MySQLConnection) getConnectionWithProps(testDbUrl, props);
        final boolean pwdIsComplex = !Charset.forName("US-ASCII").newEncoder().canEncode(password);

        for (String encProp : encoding.length() == 0 ? new String[] { "*none*" } : new String[] { "characterEncoding", "passwordCharacterEncoding" }) {
            for (int testCase = 1; testCase <= 4; testCase++) {
                props.setProperty("user", user);
                props.setProperty("password", password);
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
                        props.setProperty("allowPublicKeyRetrieval", "true");
                        props.setProperty("useSSL", "false");
                        props.setProperty("requireSSL", "false");
                        testCaseMsg = "Non-SSL/Non-RSA";
                        break;

                    case 2:
                        /*
                         * Test with an SSL enabled connection.
                         */
                        if (!sslEnabled) {
                            continue;
                        }
                        props.setProperty("allowPublicKeyRetrieval", "false");
                        props.setProperty("useSSL", "true");
                        props.setProperty("requireSSL", "true");
                        props.setProperty("verifyServerCertificate", "false");
                        if (Util.getJVMVersion() < 8 && testBaseConn.versionMeetsMinimum(5, 7, 6) && Util.isCommunityEdition(testBaseConn.getServerVersion())) {
                            props.setProperty("enabledSSLCipherSuites", SSL_CIPHERS_FOR_576);
                        }
                        testCaseMsg = "SSL";
                        break;

                    case 3:
                        /*
                         * Test with an RSA encryption enabled connection, using public key retrieved from server.
                         * Requires additional server instance pointed by 'com.mysql.jdbc.testsuite.url.sha256default'.
                         * Can't be used with plugin 'cleartext_plugin_server'.
                         */
                        if (pluginName.equals("cleartext_plugin_server") || !rsaEnabled) {
                            continue;
                        }
                        props.setProperty("allowPublicKeyRetrieval", "true");
                        testCaseMsg = "RSA [pubkey-retrieval]";
                        break;

                    case 4:
                        /*
                         * Test with an RSA encryption enabled connection, using public key pointed by the property 'serverRSAPublicKeyFile'.
                         * Requires additional server instance pointed by 'com.mysql.jdbc.testsuite.url.sha256default'.
                         * Can't be used with plugin 'cleartext_plugin_server'.
                         */
                        if (pluginName.equals("cleartext_plugin_server") || !rsaEnabled) {
                            continue;
                        }
                        props.setProperty("allowPublicKeyRetrieval", "false");
                        props.setProperty("serverRSAPublicKeyFile", "src/testsuite/ssl-test-certs/mykey.pub");
                        testCaseMsg = "RSA [pubkey-file]";
                        break;
                }

                boolean testShouldPass = true;
                if (pwdIsComplex) {
                    // if no encoding is specifically defined then our default password encoding ('UTF-8') and server's encoding must coincide
                    testShouldPass = encoding.length() > 0 || defaultServerEncoding.equalsIgnoreCase("UTF-8");

                    if (!testBaseConn.versionMeetsMinimum(5, 7, 6) && pluginName.equals("cleartext_plugin_server")) {
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
}
