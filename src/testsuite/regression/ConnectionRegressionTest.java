/*
  Copyright (c) 2002, 2013, Oracle and/or its affiliates. All rights reserved.

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
import java.io.PrintStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Socket;
import java.net.SocketException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import testsuite.BaseTestCase;
import testsuite.UnreliableSocketFactory;

import com.mysql.jdbc.AuthenticationPlugin;
import com.mysql.jdbc.Buffer;
import com.mysql.jdbc.CharsetMapping;
import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.Driver;
import com.mysql.jdbc.LoadBalancingConnectionProxy;
import com.mysql.jdbc.Messages;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.MysqlDataTruncation;
import com.mysql.jdbc.MysqlErrorNumbers;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.RandomBalanceStrategy;
import com.mysql.jdbc.ReplicationConnection;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.StandardSocketFactory;
import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.TimeUtil;
import com.mysql.jdbc.exceptions.MySQLNonTransientException;
import com.mysql.jdbc.exceptions.MySQLSyntaxErrorException;
import com.mysql.jdbc.integration.jboss.MysqlValidConnectionChecker;
import com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlXADataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlXid;
import com.mysql.jdbc.jdbc2.optional.SuspendableXAConnection;
import com.mysql.jdbc.log.StandardLogger;

/**
 * Regression tests for Connections
 * 
 * @author Mark Matthews
 * @version $Id: ConnectionRegressionTest.java,v 1.1.2.1 2005/05/13 18:58:38
 *          mmatthews Exp $
 */
public class ConnectionRegressionTest extends BaseTestCase {
	/**
	 * DOCUMENT ME!
	 * 
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

	/**
	 * DOCUMENT ME!
	 * 
	 * @throws Exception
	 *             ...
	 */
	public void testBug1914() throws Exception {
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), BIGINT)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), BINARY)}"));
		System.out
				.println(this.conn.nativeSQL("{fn convert(foo(a,b,c), BIT)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), CHAR)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), DATE)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), DECIMAL)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), DOUBLE)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), FLOAT)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), INTEGER)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), LONGVARBINARY)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), LONGVARCHAR)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), TIME)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), TIMESTAMP)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), TINYINT)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), VARBINARY)}"));
		System.out.println(this.conn
				.nativeSQL("{fn convert(foo(a,b,c), VARCHAR)}"));
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
			new NonRegisteringDriver().connect(
					"jdbc:mysql://localhost:3306/?user=root&password=root",
					new Properties());
		} catch (SQLException sqlEx) {
			assertTrue(sqlEx.getMessage().indexOf("Malformed") == -1);
		}
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @throws Exception
	 *             ...
	 */
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
			createTable("testBug3790",
					"(field1 INT NOT NULL PRIMARY KEY, field2 VARCHAR(32)) ",
					"InnoDB");
			this.stmt.executeUpdate("INSERT INTO testBug3790 VALUES ("
					+ field1OldValue + ", '" + field2OldValue + "')");

			conn1 = getConnectionWithProps(props); // creates a new connection
			conn2 = getConnectionWithProps(props); // creates another new
			// connection
			conn1.setAutoCommit(false);
			conn2.setAutoCommit(false);

			stmt1 = conn1.createStatement();
			stmt1.executeUpdate("UPDATE testBug3790 SET field2 = '"
					+ field2NewValue + "' WHERE field1=" + field1OldValue);
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

					charsetStmt
							.executeUpdate("DROP DATABASE IF EXISTS testCollation41");
					charsetStmt
							.executeUpdate("DROP TABLE IF EXISTS testCollation41");

					charsetStmt
							.executeUpdate("CREATE DATABASE testCollation41 DEFAULT CHARACTER SET "
									+ charsetName);
					charsetConn.setCatalog("testCollation41");

					// We've switched catalogs, so we need to recreate the
					// statement to pick this up...
					charsetStmt = charsetConn.createStatement();

					StringBuffer createTableCommand = new StringBuffer(
							"CREATE TABLE testCollation41"
									+ "(field1 VARCHAR(255), field2 INT)");

					charsetStmt.executeUpdate(createTableCommand.toString());

					charsetStmt
							.executeUpdate("INSERT INTO testCollation41 VALUES ('abc', 0)");

					int updateCount = charsetStmt
							.executeUpdate("UPDATE testCollation41 SET field2=1 WHERE field1='abc'");
					assertTrue(updateCount == 1);
				} finally {
					if (charsetStmt != null) {
						charsetStmt
								.executeUpdate("DROP TABLE IF EXISTS testCollation41");
						charsetStmt
								.executeUpdate("DROP DATABASE IF EXISTS testCollation41");
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

		Connection reconnectableConn = DriverManager.getConnection(
				BaseTestCase.dbUrl + sepChar + "autoReconnect=true", props);

		this.rs = reconnectableConn.createStatement().executeQuery(
				"SELECT CONNECTION_ID()");
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

		System.out
				.println("Executing statement on reconnectable connection...");

		this.rs = reconnectableConn.createStatement().executeQuery(
				"SELECT CONNECTION_ID()");
		this.rs.next();
		assertTrue("Connection is not a reconnected-connection",
				!connectionId.equals(this.rs.getString(1)));

		try {
			reconnectableConn.createStatement().executeQuery("SELECT 1");
		} catch (SQLException sqlEx) {
			; // ignore
		}

		reconnectableConn.createStatement().executeQuery("SELECT 1");

		assertTrue(reconnectableConn.isReadOnly() == isReadOnly);
	}

	private Map<String, String> getCharacterSetsAndCollations() throws Exception {
		Map<String, String> charsetsToLoad = new HashMap<String, String>();

		try {
			this.rs = this.stmt.executeQuery("SHOW character set");

			while (this.rs.next()) {
				charsetsToLoad.put(this.rs.getString("Charset"),
						this.rs.getString("Default collation"));
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
				String database = oldProps
						.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
				String user = oldProps
						.getProperty(NonRegisteringDriver.USER_PROPERTY_KEY);
				String password = oldProps
						.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY);

				StringBuffer newUrlToTestPortNum = new StringBuffer(
						"jdbc:mysql://");

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
						newUrlToTestPortNum.append("password=")
								.append(password);
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
				Connection portNumConn = DriverManager.getConnection(
						newUrlToTestPortNum.toString(), autoReconnectProps);
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
					assertTrue(sqlEx.getMessage().toLowerCase()
							.indexOf("connection refused") != -1);
				}

				//
				// Now make sure failover works
				//
				StringBuffer newUrlToTestFailover = new StringBuffer(
						"jdbc:mysql://");

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
						newUrlToTestFailover.append("password=").append(
								password);
					}
				}

				Connection failoverConn = DriverManager.getConnection(
						newUrlToTestFailover.toString(), autoReconnectProps);
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

	private static void killConnection(Connection adminConn, String threadId)
			throws SQLException {
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

		Properties urlProps = new NonRegisteringDriver().parseURL(dbUrl,
				null);

		String host = urlProps.getProperty(Driver.HOST_PROPERTY_KEY);
		String port = urlProps.getProperty(Driver.PORT_PROPERTY_KEY);

		props.remove(Driver.HOST_PROPERTY_KEY);
		props.remove(Driver.NUM_HOSTS_PROPERTY_KEY);
		props.remove(Driver.HOST_PROPERTY_KEY + ".1");
		props.remove(Driver.PORT_PROPERTY_KEY + ".1");

		props.setProperty("queriesBeforeRetryMaster", "50");
		props.setProperty("maxReconnects", "1");

		UnreliableSocketFactory.mapHost("master", host);
		UnreliableSocketFactory.mapHost("slave", host);
		UnreliableSocketFactory.downHost("master");

		Connection failoverConnection = null;

		try {
			failoverConnection = getConnectionWithProps("jdbc:mysql://master:"
					+ port + ",slave:" + port + "/", props);
			failoverConnection.setAutoCommit(false);

			String originalConnectionId = getSingleIndexedValueWithQuery(
					failoverConnection, 1, "SELECT CONNECTION_ID()").toString();

			for (int i = 0; i < 50; i++) {
				failoverConnection.createStatement().executeQuery("SELECT 1");
			}

			((com.mysql.jdbc.Connection) failoverConnection)
					.clearHasTriedMaster();
			UnreliableSocketFactory.dontDownHost("master");

			failoverConnection.setAutoCommit(true);

			String newConnectionId = getSingleIndexedValueWithQuery(
					failoverConnection, 1, "SELECT CONNECTION_ID()").toString();

			assertTrue(((com.mysql.jdbc.Connection) failoverConnection)
					.hasTriedMaster());

			assertTrue(!newConnectionId.equals(originalConnectionId));

			failoverConnection.createStatement().executeQuery("SELECT 1");
		} finally {
			UnreliableSocketFactory.flushAllHostLists();

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
			String port = props.getProperty(
					NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");

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
			failoverConnection = getConnectionWithProps("jdbc:mysql://" + host
					+ "/", props);
			((com.mysql.jdbc.Connection) failoverConnection)
					.setPreferSlaveDuringFailover(true);
			failoverConnection.setAutoCommit(false);

			String failoverConnectionId = getSingleIndexedValueWithQuery(
					failoverConnection, 1, "SELECT CONNECTION_ID()").toString();

			System.out.println("Connection id: " + failoverConnectionId);

			killConnection(killerConnection, failoverConnectionId);

			Thread.sleep(3000); // This can take some time....

			try {
				failoverConnection.createStatement().executeQuery("SELECT 1");
			} catch (SQLException sqlEx) {
				assertTrue("08S01".equals(sqlEx.getSQLState()));
			}

			((com.mysql.jdbc.Connection) failoverConnection)
					.setPreferSlaveDuringFailover(false);
			((com.mysql.jdbc.Connection) failoverConnection)
					.setFailedOver(true);

			failoverConnection.setAutoCommit(true);

			String failedConnectionId = getSingleIndexedValueWithQuery(
					failoverConnection, 1, "SELECT CONNECTION_ID()").toString();
			System.out.println("Failed over connection id: "
					+ failedConnectionId);

			((com.mysql.jdbc.Connection) failoverConnection)
					.setPreferSlaveDuringFailover(false);
			((com.mysql.jdbc.Connection) failoverConnection)
					.setFailedOver(true);

			for (int i = 0; i < 30; i++) {
				failoverConnection.setAutoCommit(true);
				System.out.println(getSingleIndexedValueWithQuery(
						failoverConnection, 1, "SELECT CONNECTION_ID()"));
				// failoverConnection.createStatement().executeQuery("SELECT
				// 1");
				failoverConnection.setAutoCommit(true);
			}

			String fallbackConnectionId = getSingleIndexedValueWithQuery(
					failoverConnection, 1, "SELECT CONNECTION_ID()").toString();
			System.out.println("fallback connection id: "
					+ fallbackConnectionId);

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

				this.rs = ms932Conn.createStatement().executeQuery(
						"SHOW VARIABLES LIKE 'character_set_client'");
				assertTrue(this.rs.next());
				String encoding = this.rs.getString(2);
				if (!versionMeetsMinimum(5, 0, 3)
						&& !versionMeetsMinimum(4, 1, 11)) {
					assertEquals("sjis", encoding.toLowerCase(Locale.ENGLISH));
				} else {
					assertEquals("cp932", encoding.toLowerCase(Locale.ENGLISH));
				}

				this.rs = ms932Conn.createStatement().executeQuery(
						"SELECT 'abc'");
				assertTrue(this.rs.next());

				String charsetToCheck = "ms932";

				if (versionMeetsMinimum(5, 0, 3)
						|| versionMeetsMinimum(4, 1, 11)) {
					charsetToCheck = "windows-31j";
				}

				assertEquals(charsetToCheck,
						((com.mysql.jdbc.ResultSetMetaData) this.rs
								.getMetaData()).getColumnCharacterSet(1)
								.toLowerCase(Locale.ENGLISH));

				try {
					ms932Conn.createStatement().executeUpdate(
							"drop table if exists testBug7607");
					ms932Conn
							.createStatement()
							.executeUpdate(
									"create table testBug7607 (sortCol int, col1 varchar(100) ) character set sjis");
					ms932Conn.createStatement().executeUpdate(
							"insert into testBug7607 values(1, 0x835C)"); // standard
					// sjis
					ms932Conn.createStatement().executeUpdate(
							"insert into testBug7607 values(2, 0x878A)"); // NEC
					// kanji

					this.rs = ms932Conn
							.createStatement()
							.executeQuery(
									"SELECT col1 FROM testBug7607 ORDER BY sortCol ASC");
					assertTrue(this.rs.next());
					String asString = this.rs.getString(1);
					assertTrue("\u30bd".equals(asString));

					// Can't be fixed unless server is fixed,
					// this is fixed in 4.1.7.

					assertTrue(this.rs.next());
					asString = this.rs.getString(1);
					assertEquals("\u3231", asString);
				} finally {
					ms932Conn.createStatement().executeUpdate(
							"drop table if exists testBug7607");
				}

				props = new Properties();
				props.setProperty("characterEncoding", "SHIFT_JIS");

				shiftJisConn = getConnectionWithProps(props);

				this.rs = shiftJisConn.createStatement().executeQuery(
						"SHOW VARIABLES LIKE 'character_set_client'");
				assertTrue(this.rs.next());
				encoding = this.rs.getString(2);
				assertTrue("sjis".equalsIgnoreCase(encoding));

				this.rs = shiftJisConn.createStatement().executeQuery(
						"SELECT 'abc'");
				assertTrue(this.rs.next());

				String charSetUC = ((com.mysql.jdbc.ResultSetMetaData) this.rs
						.getMetaData()).getColumnCharacterSet(1).toUpperCase(
						Locale.US);

				if (isRunningOnJdk131()) {
					assertEquals("WINDOWS-31J", charSetUC);
				} else {
					// assertEquals("SHIFT_JIS", charSetUC);
				}

				props = new Properties();
				props.setProperty("characterEncoding", "WINDOWS-31J");

				windows31JConn = getConnectionWithProps(props);

				this.rs = windows31JConn.createStatement().executeQuery(
						"SHOW VARIABLES LIKE 'character_set_client'");
				assertTrue(this.rs.next());
				encoding = this.rs.getString(2);

				if (!versionMeetsMinimum(5, 0, 3)
						&& !versionMeetsMinimum(4, 1, 11)) {
					assertEquals("sjis", encoding.toLowerCase(Locale.ENGLISH));
				} else {
					assertEquals("cp932", encoding.toLowerCase(Locale.ENGLISH));
				}

				this.rs = windows31JConn.createStatement().executeQuery(
						"SELECT 'abc'");
				assertTrue(this.rs.next());

				if (!versionMeetsMinimum(4, 1, 11)) {
					assertEquals("sjis".toLowerCase(Locale.ENGLISH),
							((com.mysql.jdbc.ResultSetMetaData) this.rs
									.getMetaData()).getColumnCharacterSet(1)
									.toLowerCase(Locale.ENGLISH));
				} else {
					assertEquals("windows-31j".toLowerCase(Locale.ENGLISH),
							((com.mysql.jdbc.ResultSetMetaData) this.rs
									.getMetaData()).getColumnCharacterSet(1)
									.toLowerCase(Locale.ENGLISH));
				}

				props = new Properties();
				props.setProperty("characterEncoding", "CP943");

				cp943Conn = getConnectionWithProps(props);

				this.rs = cp943Conn.createStatement().executeQuery(
						"SHOW VARIABLES LIKE 'character_set_client'");
				assertTrue(this.rs.next());
				encoding = this.rs.getString(2);
				assertTrue("sjis".equalsIgnoreCase(encoding));

				this.rs = cp943Conn.createStatement().executeQuery(
						"SELECT 'abc'");
				assertTrue(this.rs.next());

				charSetUC = ((com.mysql.jdbc.ResultSetMetaData) this.rs
						.getMetaData()).getColumnCharacterSet(1).toUpperCase(
						Locale.US);

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

			int currentOpenStatements = ((com.mysql.jdbc.Connection) bareConn)
					.getActiveStatementCount();

			try {
				bareConn.prepareStatement("Boo!");
				fail("Should not've been able to prepare that one!");
			} catch (SQLException sqlEx) {
				assertEquals(currentOpenStatements,
						((com.mysql.jdbc.Connection) bareConn)
								.getActiveStatementCount());
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
		assertEquals(true,
				((com.mysql.jdbc.Connection) maxPerfConn)
						.getElideSetAutoCommits());
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
				assertTrue(!((MySQLConnection) ((ReplicationConnection) replConn)
						.getMasterConnection())
						.hasSameProperties(((ReplicationConnection) replConn)
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

		this.pstmt = explainConn
				.prepareStatement("SELECT `int_field` FROM `testBug12229` WHERE `int_field` = ?");
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

			StringBuffer queryBuf = new StringBuffer("SELECT '");

			for (int i = 0; i < 500; i++) {
				queryBuf.append("a");
			}

			queryBuf.append("'");

			this.rs = profileConn.createStatement().executeQuery(
					queryBuf.toString());
			this.rs.close();

			String logString = new String(bOut.toString("ISO8859-1"));
			assertTrue(logString.indexOf("... (truncated)") != -1);

			bOut = new ByteArrayOutputStream();
			System.setErr(new PrintStream(bOut));

			this.rs = profileConn.prepareStatement(queryBuf.toString())
					.executeQuery();
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
		StringBuffer urlBuf = new StringBuffer(dbUrl);

		if (dbUrl.indexOf('?') == -1) {
			urlBuf.append('?');
		} else {
			urlBuf.append('&');
		}

		urlBuf.append("sessionVariables=@testBug13453='%25%26+%3D'");

		Connection encodedConn = null;

		try {
			encodedConn = DriverManager.getConnection(urlBuf.toString(), null);

			this.rs = encodedConn.createStatement().executeQuery(
					"SELECT @testBug13453");
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

						if (methodName.startsWith("get")
								&& !methodsToSkipMap.containsKey(methodName)) {
							Class<?>[] parameterTypes = getMethods[i]
									.getParameterTypes();

							if (parameterTypes.length == 1
									&& parameterTypes[0] == Integer.TYPE) {
								if (j == 0) {
									this.rs = advisorStmt
											.executeQuery("SELECT COUNT(*) FROM testBug15065");
								} else {
									this.rs = advisorConn
											.prepareStatement(
													"SELECT COUNT(*) FROM testBug15065")
											.executeQuery();
								}

								this.rs.next();

								try {

									getMethods[i].invoke(this.rs,
											new Object[] { new Integer(1) });
								} catch (InvocationTargetException invokeEx) {
									// we don't care about bad values, just that
									// the
									// column gets "touched"
									if (!invokeEx
											.getCause()
											.getClass()
											.isAssignableFrom(
													java.sql.SQLException.class)
											&& !invokeEx
													.getCause()
													.getClass()
													.getName()
													.equals("com.mysql.jdbc.NotImplemented")
											&& !invokeEx
													.getCause()
													.getClass()
													.getName()
													.equals("java.sql.SQLFeatureNotSupportedException")) {
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

				assertTrue("Usage advisor complained about columns:\n\n"
						+ logOut, logOut.indexOf("columns") == -1);
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
			this.rs = utf8Conn.createStatement().executeQuery(
					"SHOW VARIABLES LIKE 'character_%'");
			while (this.rs.next()) {
				System.out.println(this.rs.getString(1) + " = "
						+ this.rs.getString(2));
			}

			this.rs = utf8Conn.createStatement().executeQuery(
					"SHOW VARIABLES LIKE 'collation_%'");
			while (this.rs.next()) {
				System.out.println(this.rs.getString(1) + " = "
						+ this.rs.getString(2));
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

			int masterConnectionId = Integer
					.parseInt(getSingleIndexedValueWithQuery(replConn, 1,
							"SELECT CONNECTION_ID()").toString());

			replConn.setReadOnly(false);

			assertEquals(
					masterConnectionId,
					Integer.parseInt(getSingleIndexedValueWithQuery(replConn,
							1, "SELECT CONNECTION_ID()").toString()));

			String currentCatalog = replConn.getCatalog();

			replConn.setCatalog(currentCatalog);
			assertEquals(currentCatalog, replConn.getCatalog());

			replConn.setReadOnly(true);

			int slaveConnectionId = Integer
					.parseInt(getSingleIndexedValueWithQuery(replConn, 1,
							"SELECT CONNECTION_ID()").toString());

			// The following test is okay for now, as the chance
			// of MySQL wrapping the connection id counter during our
			// testsuite is very small.

			assertTrue("Slave id " + slaveConnectionId
					+ " is not newer than master id " + masterConnectionId,
					slaveConnectionId > masterConnectionId);

			assertEquals(currentCatalog, replConn.getCatalog());

			String newCatalog = "mysql";

			replConn.setCatalog(newCatalog);
			assertEquals(newCatalog, replConn.getCatalog());

			replConn.setReadOnly(true);
			assertEquals(newCatalog, replConn.getCatalog());

			replConn.setReadOnly(false);
			assertEquals(
					masterConnectionId,
					Integer.parseInt(getSingleIndexedValueWithQuery(replConn,
							1, "SELECT CONNECTION_ID()").toString()));
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
			String port = props.getProperty(
					NonRegisteringDriver.PORT_PROPERTY_KEY, "3306");

			host = host + ":" + port;
		}

		props.remove("PORT");
		props.remove("HOST");

		StringBuffer newHostBuf = new StringBuffer();

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
			failoverConnection = getConnectionWithProps("jdbc:mysql://"
					+ newHostBuf.toString() + "/", props);

			String originalConnectionId = getSingleIndexedValueWithQuery(
					failoverConnection, 1, "SELECT CONNECTION_ID()").toString();

			System.out.println(originalConnectionId);

			Connection nextConnection = getConnectionWithProps("jdbc:mysql://"
					+ newHostBuf.toString() + "/", props);

			String nextId = getSingleIndexedValueWithQuery(nextConnection, 1,
					"SELECT CONNECTION_ID()").toString();

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

		StringBuffer logBuf = new StringBuffer();

		StandardLogger.bufferedLog = logBuf;

		try {
			c = getConnectionWithProps(props);
			c.setAutoCommit(true);
			c.createStatement().execute("SELECT 1");
			c.setAutoCommit(true);
			c.setAutoCommit(false);
			c.createStatement().execute("SELECT 1");
			c.setAutoCommit(false);

			// We should only see _one_ "set autocommit=" sent to the server

			String log = logBuf.toString();
			int searchFrom = 0;
			int count = 0;
			int found = 0;

			while ((found = log.indexOf("SET autocommit=", searchFrom)) != -1) {
				searchFrom = found + 1;
				count++;
			}

			// The SELECT doesn't actually start a transaction, so being
			// pedantic the
			// driver issues SET autocommit=0 again in this case.
			assertEquals(2, count);
		} finally {
			StandardLogger.bufferedLog = null;

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
			numThreadsNamedTimer = findNamedThreadCount(root,
					"MySQL Statement Cancellation Timer");
		}

		// Notice that this seems impossible to test on JDKs prior to 1.5, as
		// there is no
		// reliable way to find the TimerThread, so we have to rely on new JDKs
		// for this
		// test.
		assertTrue("More than one timer for cancel was created",
				numThreadsNamedTimer <= 1);
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

		DriverPropertyInfo[] dpi = new NonRegisteringDriver().getPropertyInfo(
				dbUrl, null);
		StringBuffer missingSettersBuf = new StringBuffer();
		StringBuffer missingGettersBuf = new StringBuffer();

		Class<?>[][] argTypes = { new Class[] { String.class },
				new Class[] { Integer.TYPE }, new Class[] { Long.TYPE },
				new Class[] { Boolean.TYPE } };

		for (int i = 0; i < dpi.length; i++) {

			String propertyName = dpi[i].name;

			if (propertyName.equals("HOST") || propertyName.equals("PORT")
					|| propertyName.equals("DBNAME")
					|| propertyName.equals("user")
					|| propertyName.equals("password")) {
				continue;
			}

			StringBuffer mutatorName = new StringBuffer("set");
			mutatorName.append(Character.toUpperCase(propertyName.charAt(0)));
			mutatorName.append(propertyName.substring(1));

			StringBuffer accessorName = new StringBuffer("get");
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

		assertEquals("Missing setters for listed configuration properties.",
				"", missingSettersBuf.toString());
		assertEquals("Missing getters for listed configuration properties.",
				"", missingSettersBuf.toString());
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
			String port = props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY,	"3306");
			String db = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY,	"test");

			String hostSpec = host;

			if (!NonRegisteringDriver.isHostPropertiesList(host)) {
				hostSpec = host + ":" + port;
			}
			
			final String url = "jdbc:mysql://"+hostSpec+"/"+db+"?"
                    + "useSSL=true"
                    + "&requireSSL=true"
                    + "&verifyServerCertificate=true"
                    + "&trustCertificateKeyStoreUrl=file:src/testsuite/ssl-test-certs/test-cert-store"
                    + "&trustCertificateKeyStoreType=JKS"
                    + "&trustCertificateKeyStorePassword=password";
			
            _conn = DriverManager.getConnection(url,
            		(String) this.getPropertiesFromTestsuiteUrl().get("user"),
            		(String) this.getPropertiesFromTestsuiteUrl().get("password"));
            
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
		StringBuffer logBuf = new StringBuffer();
		Properties props = new Properties();
		props.setProperty("profileSQL", "true");
		props.setProperty("logger", "StandardLogger");
		StandardLogger.bufferedLog = logBuf;

		Connection loggedConn = null;

		try {
			loggedConn = getConnectionWithProps(props);
			loggedConn.getTransactionIsolation();

			if (versionMeetsMinimum(4, 0, 3)) {
				assertEquals(
						-1,
						logBuf.toString().indexOf(
								"SHOW VARIABLES LIKE 'tx_isolation'"));
			}
		} finally {
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

		Connection failoverConn = null;

		Statement failoverStmt = null;

		try {
			failoverConn = getConnectionWithProps(getMasterSlaveUrl(), props);

			((com.mysql.jdbc.Connection) failoverConn)
					.setPreferSlaveDuringFailover(true);

			failoverStmt = failoverConn.createStatement();

			String masterConnectionId = getSingleIndexedValueWithQuery(
					failoverConn, 1, "SELECT connection_id()").toString();

			this.stmt.execute("KILL " + masterConnectionId);

			// die trying, so we get the next host
			for (int i = 0; i < 100; i++) {
				try {
					failoverStmt.executeQuery("SELECT 1");
				} catch (SQLException sqlEx) {
					break;
				}
			}

			String slaveConnectionId = getSingleIndexedValueWithQuery(
					failoverConn, 1, "SELECT connection_id()").toString();

			assertTrue("Didn't get a new physical connection",
					!masterConnectionId.equals(slaveConnectionId));

			failoverConn.setReadOnly(false); // this should be ignored

			assertTrue(failoverConn.isReadOnly());

			((com.mysql.jdbc.Connection) failoverConn)
					.setPreferSlaveDuringFailover(false);

			this.stmt.execute("KILL " + slaveConnectionId); // we can't issue
															// this on our own
															// connection :p

			// die trying, so we get the next host
			for (int i = 0; i < 100; i++) {
				try {
					failoverStmt.executeQuery("SELECT 1");
				} catch (SQLException sqlEx) {
					break;
				}
			}

			String newMasterId = getSingleIndexedValueWithQuery(failoverConn,
					1, "SELECT connection_id()").toString();

			assertTrue("Didn't get a new physical connection",
					!slaveConnectionId.equals(newMasterId));

			failoverConn.setReadOnly(false);

			assertTrue(!failoverConn.isReadOnly());
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
		DriverPropertyInfo[] dpi = new NonRegisteringDriver().getPropertyInfo(
				dbUrl, null);

		for (int i = 0; i < dpi.length; i++) {
			String description = dpi[i].description;
			String propertyName = dpi[i].name;

			if (description.indexOf("Missing error message for key '") != -1
					|| description.startsWith("!")) {
				fail("Missing message for configuration property "
						+ propertyName);
			}

			if (description.length() < 10) {
				fail("Suspiciously short description for configuration property "
						+ propertyName);
			}
		}
	}

	public void testBug29106() throws Exception {
		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		Class<?> checkerClass = cl
				.loadClass("com.mysql.jdbc.integration.jboss.MysqlValidConnectionChecker");
		((MysqlValidConnectionChecker) checkerClass.newInstance())
				.isValidConnection(this.conn);
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
		// Yes, I know we're sending 2, and looking for 1
		// that's part of the test, since we don't _really_
		// send the query to the server!
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

		Method isValid = java.sql.Connection.class.getMethod("isValid",
				new Class[] { Integer.TYPE });

		Connection newConn = getConnectionWithProps((Properties) null);
		isValid.invoke(newConn, new Object[] { new Integer(1) });
		Thread.sleep(2000);
		assertTrue(((Boolean) isValid.invoke(newConn,
				new Object[] { new Integer(0) })).booleanValue());
	}

	public void testBug34937() throws Exception {
		com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource ds = new com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource();
		StringBuffer urlBuf = new StringBuffer();
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
		url = "jdbc:mysql:replication:"
				+ url.substring(url.indexOf("jdbc:mysql:")
						+ "jdbc:mysql:".length());
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

			adminStmt.executeUpdate("grant usage on *.* to '" + user
					+ "'@'127.0.0.1' identified by 'foo'");
			adminStmt.executeUpdate("update mysql.user set password=PASSWORD('"
					+ unicodePassword + "') where user = '" + user + "'");
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
		Connection conn2 = this.getUnreliableLoadBalancedConnection(
				new String[] { "first", "second" }, props);
		assertNotNull("Connection should not be null", conn);

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

	public void testBug43421() throws Exception {

		Properties props = new Properties();
		props.setProperty("loadBalanceStrategy", "bestResponseTime");

		Connection conn2 = this.getUnreliableLoadBalancedConnection(
				new String[] { "first", "second" }, props);

		assertNotNull("Connection should not be null", conn2);

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

		UnreliableSocketFactory.flushAllHostLists();
		props = new Properties();
		props.setProperty("globalBlacklistTimeout", "200");
		props.setProperty("loadBalanceStrategy", "bestResponseTime");

		conn2 = this.getUnreliableLoadBalancedConnection(new String[] {
				"first", "second" }, props);

		assertNotNull("Connection should not be null", conn);

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
		Connection conn2 = this.getUnreliableLoadBalancedConnection(
				new String[] { "first", "second" }, props);

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
		statementsToTest.add(((com.mysql.jdbc.Connection) this.conn)
				.clientPrepareStatement("SELECT 1"));
		statementsToTest.add(((com.mysql.jdbc.Connection) this.conn)
				.clientPrepareStatement("SELECT 1",
						Statement.RETURN_GENERATED_KEYS));
		statementsToTest.add(((com.mysql.jdbc.Connection) this.conn)
				.clientPrepareStatement("SELECT 1", new int[0]));
		statementsToTest.add(((com.mysql.jdbc.Connection) this.conn)
				.clientPrepareStatement("SELECT 1", new String[0]));
		statementsToTest.add(((com.mysql.jdbc.Connection) this.conn)
				.serverPrepareStatement("SELECT 1"));
		statementsToTest.add(((com.mysql.jdbc.Connection) this.conn)
				.serverPrepareStatement("SELECT 1",
						Statement.RETURN_GENERATED_KEYS));
		statementsToTest.add(((com.mysql.jdbc.Connection) this.conn)
				.serverPrepareStatement("SELECT 1", new int[0]));
		statementsToTest.add(((com.mysql.jdbc.Connection) this.conn)
				.serverPrepareStatement("SELECT 1", new String[0]));

		for (Statement toTest : statementsToTest) {
			assertEquals(toTest.getResultSetType(), ResultSet.TYPE_FORWARD_ONLY);
			assertEquals(toTest.getResultSetConcurrency(),
					ResultSet.CONCUR_READ_ONLY);
		}

	}

	/**
	 * Tests fix for BUG#44587, provide last packet sent/received timing in all
	 * connection failure errors.
	 */
	public void testBug44587() throws Exception {
		Exception e = null;
		String msg = SQLError.createLinkFailureMessageBasedOnHeuristics(
				(MySQLConnection) this.conn, System.currentTimeMillis() - 1000,
				System.currentTimeMillis() - 2000, e, false);
		assertTrue(containsMessage(msg,
				"CommunicationsException.ServerPacketTimingInfo"));
	}

	/**
	 * Tests fix for BUG#45419, ensure that time is not converted to seconds
	 * before being reported as milliseconds.
	 */
	public void testBug45419() throws Exception {
		Exception e = null;
		String msg = SQLError.createLinkFailureMessageBasedOnHeuristics(
				(MySQLConnection) this.conn, System.currentTimeMillis() - 1000,
				System.currentTimeMillis() - 2000, e, false);
		Matcher m = Pattern.compile("([\\d\\,\\.]+)", Pattern.MULTILINE)
				.matcher(msg);
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
		UnreliableSocketFactory.flushAllHostLists();
		UnreliableSocketFactory.downHost(hostname);

		try {
			Connection noConn = getConnectionWithProps("socketFactory=testsuite.UnreliableSocketFactory");
			noConn.close();
		} catch (SQLException sqlEx) {
			assertTrue(sqlEx.getMessage().indexOf("has not received") != -1);
		} finally {
			UnreliableSocketFactory.flushAllHostLists();
		}
	}

	public void testBug32216() throws Exception {
		checkBug32216("www.mysql.com", "12345", "my_database");
		checkBug32216("www.mysql.com", null, "my_database");
	}

	private void checkBug32216(String host, String port, String dbname)
			throws SQLException {
		NonRegisteringDriver driver = new NonRegisteringDriver();

		StringBuffer url = new StringBuffer("jdbc:mysql://");
		url.append(host);

		if (port != null) {
			url.append(':');
			url.append(port);
		}

		url.append('/');
		url.append(dbname);

		Properties result = driver.parseURL(url.toString(), new Properties());

		assertEquals("hostname not equal", host,
				result.getProperty(Driver.HOST_PROPERTY_KEY));
		if (port != null) {
			assertEquals("port not equal", port,
					result.getProperty(Driver.PORT_PROPERTY_KEY));
		} else {
			assertEquals("port default incorrect", "3306",
					result.getProperty(Driver.PORT_PROPERTY_KEY));
		}

		assertEquals("dbname not equal", dbname,
				result.getProperty(Driver.DBNAME_PROPERTY_KEY));
	}

	public void testBug44324() throws Exception {
		createTable(
				"bug44324",
				"(Id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, SomeVChar VARCHAR(10)) ENGINE=MyISAM;");

		try {
			this.stmt
					.executeUpdate("INSERT INTO bug44324 values (null, 'Some text much longer than 10 characters')");
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
		// prepare on another one. Since we are using a "pinned" connection
		// we should have the same "currentXAConnection" for both
		// SuspendableXAConnection
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

		public Socket connect(String hostname, int portNumber, Properties props)
				throws SocketException, IOException {
			assertEquals(9999, portNumber);

			throw new IOException();
		}

	}

	public void testBug48486() throws Exception {

		Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
		String host = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY,
				"localhost");
		String port = props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY,
				"3306");

		String hostSpec = host;

		if (!NonRegisteringDriver.isHostPropertiesList(host)) {
			hostSpec = host + ":" + port;
		}

		String database = props
				.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
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

		String newUrl = String.format("jdbc:mysql:loadbalance://%s,%s/%s?%s",
				hostSpec, hostSpec, database, configs.toString());

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
		Connection conn2 = this.getUnreliableLoadBalancedConnection(
				new String[] { "first", "second" }, props);

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
			getUnreliableLoadBalancedConnection(
					new String[] { "first", "second" }, props, downedHosts)
					.close();
		}
	}

	// Tests fix for Bug#51643 - connection chosen by load balancer "sticks" to
	// statements
	// that live past commit()/rollback().

	public void testBug51643() throws Exception {
		Properties props = new Properties();
		props.setProperty("loadBalanceStrategy",
				"com.mysql.jdbc.SequentialBalanceStrategy");

		Connection lbConn = getUnreliableLoadBalancedConnection(new String[] {
				"first", "second" }, props);
		try {
			PreparedStatement cPstmt = lbConn
					.prepareStatement("SELECT connection_id()");
			PreparedStatement serverPstmt = lbConn
					.prepareStatement("SELECT connection_id()");
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
		props.setProperty("loadBalanceStrategy",
				ForcedLoadBalanceStrategy.class.getName());
		props.setProperty("loadBalanceBlacklistTimeout", "5000");
		props.setProperty("loadBalancePingTimeout", "100");
		props.setProperty("loadBalanceValidateConnectionOnSwapServer", "true");

		String portNumber = new NonRegisteringDriver().parseURL(dbUrl, null)
				.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

		if (portNumber == null) {
			portNumber = "3306";
		}

		ForcedLoadBalanceStrategy.forceFutureServer("first:" + portNumber, -1);
		Connection conn2 = this.getUnreliableLoadBalancedConnection(
				new String[] { "first", "second" }, props);
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
		props.setProperty("loadBalanceStrategy",
				ForcedLoadBalanceStrategy.class.getName());
		props.setProperty("loadBalanceBlacklistTimeout", "5000");
		props.setProperty("loadBalancePingTimeout", "100");
		props.setProperty("loadBalanceValidateConnectionOnSwapServer", "false");
		ForcedLoadBalanceStrategy.forceFutureServer("first:" + portNumber, -1);
		conn2 = this.getUnreliableLoadBalancedConnection(new String[] {
				"first", "second" }, props);
		conn2.setAutoCommit(false);
		conn2.createStatement().execute("SELECT 1");
		ForcedLoadBalanceStrategy.forceFutureServer("second:" + portNumber, 1);
		UnreliableSocketFactory.downHost("second");
		try {
			conn2.commit(); // will be on second after this
			assertFalse(
					"Connection should not be closed, should be able to connect to first",
					conn2.isClosed());
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

		public com.mysql.jdbc.ConnectionImpl pickConnection(
				LoadBalancingConnectionProxy proxy, List<String> configuredHosts,
				Map<String, ConnectionImpl> liveConnections, long[] responseTimes, int numRetries)
				throws SQLException {
			if (forcedFutureServer == null || forceFutureServerTimes == 0) {
				return super.pickConnection(proxy, configuredHosts,
						liveConnections, responseTimes, numRetries);
			}
			if (forceFutureServerTimes > 0) {
				forceFutureServerTimes--;
			}
			ConnectionImpl conn = liveConnections
					.get(forcedFutureServer);

			if (conn == null) {
				conn = proxy.createConnectionForHost(forcedFutureServer);

			}
			return conn;
		}

		public void destroy() {
			super.destroy();

		}

		public void init(com.mysql.jdbc.Connection conn, Properties props)
				throws SQLException {
			super.init(conn, props);

		}

	}

	public void testAutoCommitLB() throws Exception {
		Properties props = new Properties();
		props.setProperty("loadBalanceStrategy",
				CountingReBalanceStrategy.class.getName());
		props.setProperty("loadBalanceAutoCommitStatementThreshold", "3");

		String portNumber = new NonRegisteringDriver().parseURL(dbUrl, null)
				.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

		if (portNumber == null) {
			portNumber = "3306";
		}

		Connection conn2 = this.getUnreliableLoadBalancedConnection(
				new String[] { "first", "second" }, props);
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
		conn2 = this.getUnreliableLoadBalancedConnection(new String[] {
				"first", "second" }, props);
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
		conn2 = this.getUnreliableLoadBalancedConnection(new String[] {
				"first", "second" }, props);
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

		public com.mysql.jdbc.ConnectionImpl pickConnection(
				LoadBalancingConnectionProxy proxy, List<String> configuredHosts,
				Map<String, ConnectionImpl> liveConnections, long[] responseTimes, int numRetries)
				throws SQLException {
			rebalancedTimes++;
			return super.pickConnection(proxy, configuredHosts,
					liveConnections, responseTimes, numRetries);

		}

		public void destroy() {
			super.destroy();

		}

		public void init(com.mysql.jdbc.Connection conn, Properties props)
				throws SQLException {
			super.init(conn, props);

		}

	}

	public void testBug56429() throws Exception {
		Properties props = new Driver().parseURL(BaseTestCase.dbUrl, null);
		props.setProperty("autoReconnect", "true");
		props.setProperty("socketFactory", "testsuite.UnreliableSocketFactory");

		Properties urlProps = new NonRegisteringDriver().parseURL(
				BaseTestCase.dbUrl, null);

		String host = urlProps.getProperty(Driver.HOST_PROPERTY_KEY);
		String port = urlProps.getProperty(Driver.PORT_PROPERTY_KEY);

		props.remove(Driver.HOST_PROPERTY_KEY);
		props.remove(Driver.NUM_HOSTS_PROPERTY_KEY);
		props.remove(Driver.HOST_PROPERTY_KEY + ".1");
		props.remove(Driver.PORT_PROPERTY_KEY + ".1");

		props.setProperty("queriesBeforeRetryMaster", "50");
		props.setProperty("maxReconnects", "1");

		UnreliableSocketFactory.mapHost("master", host);
		UnreliableSocketFactory.mapHost("slave", host);

		Connection failoverConnection = null;

		try {
			failoverConnection = getConnectionWithProps("jdbc:mysql://master:"
					+ port + ",slave:" + port + "/", props);

			String userHost = getSingleIndexedValueWithQuery(1, "SELECT USER()")
					.toString();
			String[] userParts = userHost.split("@");

			this.rs = this.stmt.executeQuery("SHOW PROCESSLIST");

			int startConnCount = 0;

			while (this.rs.next()) {
				if (this.rs.getString("User").equals(userParts[0])
						&& this.rs.getString("Host").equals(userParts[1])) {
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
				if (this.rs.getString("User").equals(userParts[0])
						&& this.rs.getString("Host").equals(userParts[1])) {
					endConnCount++;
				}
			}

			assert (endConnCount > 0);

			if (endConnCount - startConnCount >= 20) { // this may be bogus if
														// run on a real system,
														// we should probably
														// look to see they're
														// coming from this
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
		assertEquals("JKS",
				((com.mysql.jdbc.Connection) this.conn)
						.getTrustCertificateKeyStoreType());
		assertEquals("JKS",
				((com.mysql.jdbc.Connection) this.conn)
						.getClientCertificateKeyStoreType());
	}

	public void testBug57262() throws Exception {
		Properties props = new Properties();
		props.setProperty("characterEncoding", "utf-8");
		props.setProperty("useUnicode", "true");
		props.setProperty("useOldUTF8Behavior", "true");

		Connection c = getConnectionWithProps(props);
		ResultSet r = c.createStatement().executeQuery(
				"SHOW SESSION VARIABLES LIKE 'character_set_connection'");
		r.next();
		assertEquals("latin1", r.getString(2));
	}
	
	public void testBug58706() throws Exception {
		Properties props = new Driver().parseURL(BaseTestCase.dbUrl, null);
		props.setProperty("autoReconnect", "true");
		props.setProperty("socketFactory", "testsuite.UnreliableSocketFactory");

		Properties urlProps = new NonRegisteringDriver().parseURL(dbUrl,
				null);

		String host = urlProps.getProperty(Driver.HOST_PROPERTY_KEY);
		String port = urlProps.getProperty(Driver.PORT_PROPERTY_KEY);

		props.remove(Driver.HOST_PROPERTY_KEY);
		props.remove(Driver.NUM_HOSTS_PROPERTY_KEY);
		props.remove(Driver.HOST_PROPERTY_KEY + ".1");
		props.remove(Driver.PORT_PROPERTY_KEY + ".1");

		props.setProperty("queriesBeforeRetryMaster", "0");
		props.setProperty("failOverReadOnly", "false");
		props.setProperty("secondsBeforeRetryMaster", "1");

		UnreliableSocketFactory.mapHost("master", host);
		UnreliableSocketFactory.mapHost("slave", host);

		Connection failoverConnection = null;

		try {
			failoverConnection = getConnectionWithProps("jdbc:mysql://master:"
					+ port + ",slave:" + port + "/", props);
			failoverConnection.setAutoCommit(false);

			assertTrue(((com.mysql.jdbc.Connection)failoverConnection).isMasterConnection());
			
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
			assertTrue(!((com.mysql.jdbc.Connection)failoverConnection).isMasterConnection());
			assertTrue(!failoverConnection.isReadOnly());
			failoverConnection.createStatement().executeQuery("SELECT 1");
			failoverConnection.createStatement().executeQuery("SELECT 1");
			UnreliableSocketFactory.dontDownHost("master");
			Thread.sleep(2000);
			failoverConnection.setAutoCommit(true);
			failoverConnection.createStatement().executeQuery("SELECT 1");
			assertTrue(((com.mysql.jdbc.Connection)failoverConnection).isMasterConnection());
			failoverConnection.createStatement().executeQuery("SELECT 1");
		} finally {
			UnreliableSocketFactory.flushAllHostLists();

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
			
			((com.mysql.jdbc.Connection)c).setStatementComment("Hi there");
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
		
		for (int i = 0; i < 500; i++) {
			((com.mysql.jdbc.Connection) this.conn).changeUser(props.getProperty(NonRegisteringDriver.USER_PROPERTY_KEY), props.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY));
			
			if (i % 10 == 0) {
				try {
					((com.mysql.jdbc.Connection) this.conn).changeUser("bubba", props.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY));
				} catch (SQLException sqlEx) {
					// expect
				}
			}
			
			this.stmt.executeQuery("SELECT 1");
		}
	}
	
	public void testChangeUserClosedConn() throws Exception {
		Properties props = getPropertiesFromTestsuiteUrl();
		Connection newConn = getConnectionWithProps((Properties)null);
		
		try {
			newConn.close();
			((com.mysql.jdbc.Connection) newConn).changeUser(props.getProperty(NonRegisteringDriver.USER_PROPERTY_KEY), props.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY));
			fail("Expected SQL Exception");
		} catch (MySQLNonTransientException ex) {
			// expected
		} finally {
			newConn.close();
		}
	}

	public void testBug63284() throws Exception {
		Properties props = new Driver().parseURL(BaseTestCase.dbUrl, null);
		props.setProperty("autoReconnect", "true");
		props.setProperty("socketFactory", "testsuite.UnreliableSocketFactory");

		Properties urlProps = new NonRegisteringDriver().parseURL(
            BaseTestCase.dbUrl, null);

		String host = urlProps.getProperty(Driver.HOST_PROPERTY_KEY);
		String port = urlProps.getProperty(Driver.PORT_PROPERTY_KEY);

		props.remove(Driver.HOST_PROPERTY_KEY);
		props.remove(Driver.NUM_HOSTS_PROPERTY_KEY);
		props.remove(Driver.HOST_PROPERTY_KEY + ".1");
		props.remove(Driver.PORT_PROPERTY_KEY + ".1");

		props.setProperty("queriesBeforeRetryMaster", "50");
		props.setProperty("maxReconnects", "1");

		UnreliableSocketFactory.mapHost("master", host);
		UnreliableSocketFactory.mapHost("slave", host);

		Connection failoverConnection1 = null;
		Connection failoverConnection2 = null;
	
		try{
			failoverConnection1 = getConnectionWithProps("jdbc:mysql://master:"
                + port + ",slave:" + port + "/", props);

			failoverConnection2 = getConnectionWithProps("jdbc:mysql://master:"
                + port + ",slave:" + port + "/", props);
       
			assert(((com.mysql.jdbc.Connection)failoverConnection1).isMasterConnection());

			// Two different Connection objects should not equal each other:
			assert(!failoverConnection1.equals(failoverConnection2));

			int hc = failoverConnection1.hashCode();
       
			UnreliableSocketFactory.downHost("master");
       
			for(int i = 0; i < 3; i++ ){
				try{
					failoverConnection1.createStatement().execute("SELECT 1");
				} catch (SQLException e){
                // do nothing, expect SQLException when failing over initially
                // goal here is to ensure valid connection against a slave
				}
			}
			// ensure we're now connected to the slave
			assert(!((com.mysql.jdbc.Connection)failoverConnection1).isMasterConnection());
       
			// ensure that hashCode() result is persistent across failover events when proxy state changes
			assert(failoverConnection1.hashCode() == hc);
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
				if (testConn != null) testConn.close();
			}

			props.setProperty("defaultAuthenticationPlugin", "mysql_native_password");
			try {
				testConn = getConnectionWithProps(props);
				assertTrue("Exception is expected due to incorrect defaultAuthenticationPlugin value (mechanism name instead of class name)", false);
			} catch (SQLException sqlEx) {
				assertTrue(true);
			} finally {
				if (testConn != null) testConn.close();
			}

			props.setProperty("defaultAuthenticationPlugin", "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
			try {
				testConn = getConnectionWithProps(props);
				assertTrue("Exception is expected due to defaultAuthenticationPlugin value is not listed", false);
			} catch (SQLException sqlEx) {
				assertTrue(true);
			} finally {
				if (testConn != null) testConn.close();
			}

			props.setProperty("authenticationPlugins", "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
			props.setProperty("defaultAuthenticationPlugin", "testsuite.regression.ConnectionRegressionTest$AuthTestPlugin");
			try {
				testConn = getConnectionWithProps(props);
				assertTrue(true);
			} catch (SQLException sqlEx) {
				assertTrue("Exception is not expected due to defaultAuthenticationPlugin value is correctly listed", false);
			} finally {
				if (testConn != null) testConn.close();
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
				if (testConn != null) testConn.close();
			}

			props.setProperty("disabledAuthenticationPlugins", "com.mysql.jdbc.authentication.MysqlNativePasswordPlugin");
			try {
				testConn = getConnectionWithProps(props);
				assertTrue("Exception is expected due to disabled defaultAuthenticationPlugin", false);
			} catch (SQLException sqlEx) {
				assertTrue(true);
			} finally {
				if (testConn != null) testConn.close();
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
				if (testConn != null) testConn.close();
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
				if (testConn != null) testConn.close();
			}
		}
	}

	public void testAuthTestPlugin() throws Exception {
		if (versionMeetsMinimum(5, 5, 7)) {

			boolean install_plugin_in_runtime = false;
			try {

				// install plugin if required
				this.rs = this.stmt.executeQuery(
						"select (PLUGIN_LIBRARY LIKE 'auth_test_plugin%') as `TRUE`" +
						" FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='test_plugin_server'");
				if (rs.next()) {
					if (!rs.getBoolean(1)) install_plugin_in_runtime = true;
				} else {
					install_plugin_in_runtime = true;
				}

				if (install_plugin_in_runtime) {
					String ext = System.getProperty("os.name").toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
					this.stmt.executeUpdate("INSTALL PLUGIN test_plugin_server SONAME 'auth_test_plugin"+ext+"'");
				}

				String dbname = null;
				this.rs = this.stmt.executeQuery("select database() as dbname");
				if(this.rs.first()) {
					dbname = this.rs.getString("dbname");
				}
				if (dbname == null) assertTrue("No database selected", false);
				
				// create proxy users
				this.stmt.executeUpdate("grant usage on *.* to 'wl5851user'@'%' identified WITH test_plugin_server AS 'plug_dest'");
				this.stmt.executeUpdate("grant usage on *.* to 'plug_dest'@'%' IDENTIFIED BY 'foo'");
				this.stmt.executeUpdate("GRANT PROXY ON 'plug_dest'@'%' TO 'wl5851user'@'%'");
				this.stmt.executeUpdate("delete from mysql.db where user='plug_dest'");
				this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '"+dbname+"', 'plug_dest', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
				this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', 'information\\_schema', 'plug_dest', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
				this.stmt.executeUpdate("flush privileges");
				
				Properties props = new Properties();
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
					if (testRs != null) testRs.close();
					if (testSt != null) testSt.close();
					if (testConn != null) testConn.close();
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
				this.rs = this.stmt.executeQuery(
						"select (PLUGIN_LIBRARY LIKE 'two_questions%') as `TRUE`" +
						" FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='two_questions'");
				if (rs.next()) {
					if (!rs.getBoolean(1)) install_plugin_in_runtime = true;
				} else {
					install_plugin_in_runtime = true;
				}

				if (install_plugin_in_runtime) {
					String ext = System.getProperty("os.name").toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
					this.stmt.executeUpdate("INSTALL PLUGIN two_questions SONAME 'auth"+ext+"'");
				}

				String dbname = null;
				this.rs = this.stmt.executeQuery("select database() as dbname");
				if(this.rs.first()) {
					dbname = this.rs.getString("dbname");
				}
				if (dbname == null) assertTrue("No database selected", false);
				
				this.stmt.executeUpdate("grant usage on *.* to 'wl5851user2'@'%' identified WITH two_questions AS 'two_questions_password'");
				this.stmt.executeUpdate("delete from mysql.db where user='wl5851user2'");
				this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '"+dbname+"', 'wl5851user2', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
				this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', 'information\\_schema', 'wl5851user2', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
				this.stmt.executeUpdate("flush privileges");
				
				Properties props = new Properties();
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
					if (testRs != null) testRs.close();
					if (testSt != null) testSt.close();
					if (testConn != null) testConn.close();
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
				this.rs = this.stmt.executeQuery(
						"select (PLUGIN_LIBRARY LIKE 'three_attempts%') as `TRUE`" +
						" FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='three_attempts'");
				if (rs.next()) {
					if (!rs.getBoolean(1)) install_plugin_in_runtime = true;
				} else {
					install_plugin_in_runtime = true;
				}

				if (install_plugin_in_runtime) {
					String ext = System.getProperty("os.name").toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
					this.stmt.executeUpdate("INSTALL PLUGIN three_attempts SONAME 'auth"+ext+"'");
				}

				String dbname = null;
				this.rs = this.stmt.executeQuery("select database() as dbname");
				if(this.rs.first()) {
					dbname = this.rs.getString("dbname");
				}
				if (dbname == null) assertTrue("No database selected", false);
				
				this.stmt.executeUpdate("grant usage on *.* to 'wl5851user3'@'%' identified WITH three_attempts AS 'three_attempts_password'");
				this.stmt.executeUpdate("delete from mysql.db where user='wl5851user3'");
				this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '"+dbname+"', 'wl5851user3', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
				this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', 'information\\_schema', 'wl5851user3', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
				this.stmt.executeUpdate("flush privileges");
				
				Properties props = new Properties();
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
					if (testRs != null) testRs.close();
					if (testSt != null) testSt.close();
					if (testConn != null) testConn.close();
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
				Buffer bresp = new Buffer(StringUtils.getBytes(counter>2 ? this.password : "wrongpassword"+counter));
				toServer.add(bresp);
			} else {
				Buffer bresp = new Buffer(fromServer.getByteBuffer());
				toServer.add(bresp);
			}
			return true;
		}

	}

	public void testOldPasswordPlugin() throws Exception {

		boolean secure_auth = false;
		this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'secure_auth'");
		while (this.rs.next()) {
			secure_auth = "ON".equalsIgnoreCase(this.rs.getString(2));
		}

		if (versionMeetsMinimum(5, 5, 7) && !secure_auth) {

			String dbname = null;
			this.rs = this.stmt.executeQuery("select database() as dbname");
			if(this.rs.first()) {
				dbname = this.rs.getString("dbname");
			}
			if (dbname == null) assertTrue("No database selected", false);
			
			Connection adminConn = null;
			Statement adminStmt = null;

			try {
				testOldPasswordPlugin_createUsers(this.stmt, dbname);
			} catch (Exception e) {
				adminConn = getAdminConnection();
				if (adminConn == null) {
					assertTrue("Lack of grant permissions. Change default user or set com.mysql.jdbc.testsuite.admin-url property.", false);
				} else {
					adminStmt = adminConn.createStatement();
					testOldPasswordPlugin_createUsers(adminStmt, dbname);
				}
				
			}
			
			Properties props = new Properties();
			props.setProperty("user", "bug64983user1");
			props.setProperty("password", "pwd");
			props.setProperty("defaultAuthenticationPlugin", "com.mysql.jdbc.authentication.MysqlOldPasswordPlugin");

			Connection testConn = null;
			Statement testSt = null;
			ResultSet testRs = null;

			try {
				testConn = getConnectionWithProps(props);
				testSt = testConn.createStatement();
				testRs = testSt.executeQuery("select USER()");
				testRs.next();
				assertEquals("bug64983user1", testRs.getString(1).split("@")[0]);
				
				((MySQLConnection)testConn).changeUser("bug64983user2", "");
				testRs = testSt.executeQuery("select USER()");
				testRs.next();
				assertEquals("bug64983user2", testRs.getString(1).split("@")[0]);
				
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
				if (adminStmt != null) {
					adminStmt.executeUpdate("drop user 'bug64983user1'@'%'");
					adminStmt.executeUpdate("drop user 'bug64983user2'@'%'");
					adminStmt.close();
				} else {
					this.stmt.executeUpdate("drop user 'bug64983user1'@'%'");
					this.stmt.executeUpdate("drop user 'bug64983user2'@'%'");
				}
				if (adminConn != null) {
					adminConn.close();
				}
			}

		}
	}

	public void testOldPasswordPlugin_createUsers(Statement adminStmt, String dbname) throws Exception {
		
		adminStmt.executeUpdate("grant usage on *.* to 'bug64983user1'@'%'");
		adminStmt.executeUpdate("set password for 'bug64983user1'@'%' = OLD_PASSWORD('pwd')");
		adminStmt.executeUpdate("delete from mysql.db where user='bug64983user1'");
		adminStmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '"+dbname+"', 'bug64983user1', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
		adminStmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', 'information\\_schema', 'bug64983user1', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");

		adminStmt.executeUpdate("grant usage on *.* to 'bug64983user2'@'%'");
		adminStmt.executeUpdate("set password for 'bug64983user2'@'%' = OLD_PASSWORD('')");
		adminStmt.executeUpdate("delete from mysql.db where user='bug64983user2'");
		adminStmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '"+dbname+"', 'bug64983user2', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
		adminStmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', 'information\\_schema', 'bug64983user2', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");

		adminStmt.executeUpdate("flush privileges");
	}

	public void testAuthCleartextPlugin() throws Exception {
		if (versionMeetsMinimum(5, 5, 7)) {

			boolean install_plugin_in_runtime = false;
			try {

				// install plugin if required
				this.rs = this.stmt.executeQuery(
						"select (PLUGIN_LIBRARY LIKE 'auth_test_plugin%') as `TRUE`" +
						" FROM INFORMATION_SCHEMA.PLUGINS WHERE PLUGIN_NAME='cleartext_plugin_server'");
				if (rs.next()) {
					if (!rs.getBoolean(1)) install_plugin_in_runtime = true;
				} else {
					install_plugin_in_runtime = true;
				}

				if (install_plugin_in_runtime) {
					String ext = System.getProperty("os.name").toUpperCase().indexOf("WINDOWS") > -1 ? ".dll" : ".so";
					this.stmt.executeUpdate("INSTALL PLUGIN cleartext_plugin_server SONAME 'auth_test_plugin"+ext+"'");
				}

				String dbname = null;
				this.rs = this.stmt.executeQuery("select database() as dbname");
				if(this.rs.first()) {
					dbname = this.rs.getString("dbname");
				}
				if (dbname == null) assertTrue("No database selected", false);
				
				// create proxy users
				this.stmt.executeUpdate("grant usage on *.* to 'wl5735user'@'%' identified WITH cleartext_plugin_server AS ''");
				this.stmt.executeUpdate("delete from mysql.db where user='wl5735user'");
				this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '"+dbname+"', 'wl5735user', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
				this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', 'information\\_schema', 'wl5735user', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
				this.stmt.executeUpdate("flush privileges");
				
				Properties props = new Properties();
				props.setProperty("user", "wl5735user");
				props.setProperty("password", "");

				Connection testConn = null;
				Statement testSt = null;
				ResultSet testRs = null;
				try {
					try {
						testConn = getConnectionWithProps(props);
						assertFalse("SQLException expected due to SSL connection is required", true);
					} catch (SQLException e) {
						String trustStorePath = "src/testsuite/ssl-test-certs/test-cert-store";
						System.setProperty("javax.net.ssl.keyStore", trustStorePath);
						System.setProperty("javax.net.ssl.keyStorePassword", "password");
						System.setProperty("javax.net.ssl.trustStore", trustStorePath);
						System.setProperty("javax.net.ssl.trustStorePassword", "password");
						props.setProperty("useSSL", "true");
						props.setProperty("requireSSL", "true");
						testConn = getConnectionWithProps(props);
					}
					
					testSt = testConn.createStatement();
					testRs = testSt.executeQuery("select USER(),CURRENT_USER()");
					testRs.next();
					
					assertEquals("wl5735user", testRs.getString(1).split("@")[0]);
					assertEquals("wl5735user", testRs.getString(2).split("@")[0]);
					
				} finally {
					if (testRs != null) testRs.close();
					if (testSt != null) testSt.close();
					if (testConn != null) testConn.close();
				}

			} finally {
				this.stmt.executeUpdate("drop user 'wl5735user'@'%'");
				if (install_plugin_in_runtime) {
					this.stmt.executeUpdate("UNINSTALL PLUGIN cleartext_plugin_server");
				}
			}
		}
	}

	public void testSha256PasswordPlugin() throws Exception {
		if (versionMeetsMinimum(5, 6, 5)) {

			// check that sha256_password plugin is available
			boolean plugin_is_active = false;
			this.rs = this.stmt.executeQuery("select (PLUGIN_STATUS='ACTIVE') as `TRUE` from INFORMATION_SCHEMA.PLUGINS where PLUGIN_NAME='sha256_password'");
			if (rs.next()) {
				plugin_is_active = rs.getBoolean(1);
			}

			if (plugin_is_active) {
				try {
					String dbname = null;
					this.rs = this.stmt.executeQuery("select database() as dbname");
					if(this.rs.first()) {
						dbname = this.rs.getString("dbname");
					}
					if (dbname == null) assertTrue("No database selected", false);
					
					// create proxy users
					this.stmt.executeUpdate("SET @current_old_passwords = @@global.old_passwords");
					
					this.stmt.executeUpdate("grant usage on *.* to 'wl5602user'@'%' identified WITH sha256_password");
					this.stmt.executeUpdate("SET GLOBAL old_passwords= 2");
					this.stmt.executeUpdate("SET SESSION old_passwords= 2");
					this.stmt.executeUpdate("set password for 'wl5602user'@'%' = PASSWORD('pwd')");
					this.stmt.executeUpdate("delete from mysql.db where user='wl5602user'");
					this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', '"+dbname+"', 'wl5602user', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
					this.stmt.executeUpdate("insert into mysql.db (Host, Db, User, Select_priv, Insert_priv, Update_priv, Delete_priv, Create_priv,Drop_priv, Grant_priv, References_priv, Index_priv, Alter_priv, Create_tmp_table_priv, Lock_tables_priv, Create_view_priv,Show_view_priv, Create_routine_priv, Alter_routine_priv, Execute_priv, Event_priv, Trigger_priv) VALUES ('%', 'information\\_schema', 'wl5602user', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'Y', 'N', 'N')");
					this.stmt.executeUpdate("flush privileges");
					
					Properties props = new Properties();
					props.setProperty("user", "wl5602user");
					props.setProperty("password", "pwd");

					Connection testConn = null;
					Statement testSt = null;
					ResultSet testRs = null;
					try {
						try {
							testConn = getConnectionWithProps(props);
							assertFalse("SQLException expected due to SSL connection is required", true);
						} catch (SQLException e) {
							String trustStorePath = "src/testsuite/ssl-test-certs/test-cert-store";
							System.setProperty("javax.net.ssl.keyStore", trustStorePath);
							System.setProperty("javax.net.ssl.keyStorePassword", "password");
							System.setProperty("javax.net.ssl.trustStore", trustStorePath);
							System.setProperty("javax.net.ssl.trustStorePassword", "password");
							props.setProperty("useSSL", "true");
							props.setProperty("requireSSL", "true");
							testConn = getConnectionWithProps(props);
						}
						
						testSt = testConn.createStatement();
						testRs = testSt.executeQuery("select USER(),CURRENT_USER()");
						testRs.next();
						
						assertEquals("wl5602user", testRs.getString(1).split("@")[0]);
						assertEquals("wl5602user", testRs.getString(2).split("@")[0]);
						
					} finally {
						if (testRs != null) testRs.close();
						if (testSt != null) testSt.close();
						if (testConn != null) testConn.close();
					}

				} finally {
					this.stmt.executeUpdate("drop user 'wl5602user'@'%'");
					this.stmt.executeUpdate("flush privileges");
					this.stmt.executeUpdate("SET GLOBAL old_passwords = @current_old_passwords");
				}
			}
		}
	}

	public void testBug36662() throws Exception {

		try {
			String tz1 = TimeUtil.getCanoncialTimezone("MEST", null);
			assertNotNull(tz1);
		} catch (Exception e1) {
			String mes1 = e1.getMessage();
			mes1 = mes1.substring(mes1.lastIndexOf("The timezones that 'MEST' maps to are:")+39);
			try {
				String tz2 = TimeUtil.getCanoncialTimezone("CEST", null);
				assertEquals(mes1, tz2);
			} catch (Exception e2) {
				String mes2 = e2.getMessage();
				mes2 = mes2.substring(mes2.lastIndexOf("The timezones that 'CEST' maps to are:")+39);
				assertEquals(mes1, mes2);
			}
		}
	}

	public void testBug37931() throws Exception {

		Connection _conn = null;
		Properties props = new Properties();
		props.setProperty("characterSetResults", "ISO8859-1");

		try {
			_conn = getConnectionWithProps(props);
			assertTrue("This point should not be reached.", false);
		} catch (Exception e) {
			assertEquals(
					"Can't map ISO8859-1 given for characterSetResults to a supported MySQL encoding.",
					e.getMessage());
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
			String dbname = null;
			this.rs = this.stmt.executeQuery("select database() as dbname");
			if(this.rs.first()) {
				dbname = this.rs.getString("dbname");
			}
			if (dbname == null) assertTrue("No database selected", false);

			Properties props = new Properties();
			props.setProperty("characterEncoding", "EUC_JP");

			Connection testConn = null;
			Statement testSt = null;
			ResultSet testRs = null;
			try {
				testConn = getConnectionWithProps(props);
				testSt = testConn.createStatement();
				testRs = testSt.executeQuery("SELECT * FROM `"+dbname+"`.`ほげほげ`");
			} catch (MySQLSyntaxErrorException e1) {
				assertEquals("Table '"+dbname+".ほげほげ' doesn't exist", e1.getMessage());

				try {
					props.setProperty("characterSetResults", "SJIS");
					testConn = getConnectionWithProps(props);
					testSt = testConn.createStatement();
					testSt.execute("SET lc_messages = 'ru_RU'");
					testRs = testSt.executeQuery("SELECT * FROM `"+dbname+"`.`ほげほげ`");
				} catch (MySQLSyntaxErrorException e2) {
					assertEquals("Таблица '"+dbname+".ほげほげ' не существует", e2.getMessage());
				}			

			} finally {
				if (testRs != null) testRs.close();
				if (testSt != null) testSt.close();
				if (testConn != null) testConn.close();
			}
		}
	}
	
	public void testIsLocal() throws Exception {
		boolean normalState = ((ConnectionImpl) conn).isServerLocal();
		
		if (normalState) {
			boolean isNotLocal = ((ConnectionImpl) getConnectionWithProps(StandardSocketFactory.IS_LOCAL_HOSTNAME_REPLACEMENT_PROPERTY_NAME + "=www.oracle.com:3306")).isServerLocal();
			
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
			this.rs = conn_is.getMetaData().getColumns(null, null,
					"testBug57662", "%");
			
			assertFalse(((testsuite.simple.TestBug57662Logger)((ConnectionImpl) conn_is).getLog()).hasNegativeDurations);

		} finally {
			if (conn_is != null) {
				conn_is.close();
			}
		}

	}
	
	public void testBug14563127() throws Exception {
		Properties props = new Properties();
		props.setProperty("loadBalanceStrategy",
				ForcedLoadBalanceStrategy.class.getName());
		props.setProperty("loadBalanceBlacklistTimeout", "5000");
		props.setProperty("loadBalancePingTimeout", "100");
		props.setProperty("loadBalanceValidateConnectionOnSwapServer", "true");

		String portNumber = new NonRegisteringDriver().parseURL(dbUrl, null)
				.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);

		if (portNumber == null) {
			portNumber = "3306";
		}

		ForcedLoadBalanceStrategy.forceFutureServer("first:" + portNumber, -1);
		Connection conn2 = this.getUnreliableLoadBalancedConnection(
				new String[] { "first", "second" }, props);
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
		if (this.rs.getInt(2) < 4+1024*1024*16-1) {
			fail("You need to increase max_allowed_packet to at least "+(4+1024*1024*16-1)+" before running this test!");
		}

		int requiredSize = 1024*1024*300;
		int fieldLength = 1023;
		int loops = requiredSize / 2 / (fieldLength + 1);
		
		File testFile = File.createTempFile("cj-testloaddata", ".dat");
		testFile.deleteOnExit();
		cleanupTempFiles(testFile, "cj-testloaddata");

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

		StringBuffer fileNameBuf = null;

		if (File.separatorChar == '\\') {
			fileNameBuf = new StringBuffer();

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
			fileNameBuf = new StringBuffer(testFile.getAbsolutePath());
		}

		Properties props = new Properties();
		props.put("useCompression", "true");
		Connection conn1 = getConnectionWithProps(props);
		Statement stmt1 = conn1.createStatement();

		int updateCount = stmt1
				.executeUpdate("LOAD DATA LOCAL INFILE '"
						+ fileNameBuf.toString()
						+ "' INTO TABLE testBug11237" +
						" CHARACTER SET " + CharsetMapping.getMysqlEncodingForJavaEncoding(((MySQLConnection)this.conn).getEncoding(), (com.mysql.jdbc.Connection) conn1));

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

			try {

				this.stmt.executeUpdate("CREATE USER 'must_change1'@'%' IDENTIFIED BY 'aha'");
				this.stmt.executeUpdate("CREATE USER 'must_change2'@'%' IDENTIFIED BY 'aha'");
				this.stmt.executeUpdate("ALTER USER 'must_change1'@'%' PASSWORD EXPIRE, 'must_change2'@'%' PASSWORD EXPIRE");

				Properties props = new Properties();

				// ALTER USER can be prepared as of 5.6.8 (BUG#14646014)
				if (versionMeetsMinimum(5, 6, 8)) {
					props.setProperty("useServerPrepStmts", "true");
					testConn = getConnectionWithProps(props);

					this.pstmt = testConn.prepareStatement("ALTER USER 'must_change1'@'%' PASSWORD EXPIRE, 'must_change2'@'%' PASSWORD EXPIRE");
					this.pstmt.executeUpdate();
					this.pstmt.close();

					this.pstmt = testConn.prepareStatement("ALTER USER ? PASSWORD EXPIRE, 'must_change2'@'%' PASSWORD EXPIRE");
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
					
					if (e1.getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD) {

						props.setProperty("disconnectOnExpiredPasswords", "false");
						try {
							testConn = getConnectionWithProps(props);
							testSt = testConn.createStatement();
							testRs = testSt.executeQuery("SHOW VARIABLES LIKE 'disconnect_on_expired_password'");
							fail("SQLException expected due to password expired");

						} catch (SQLException e3) {
							testSt.executeUpdate("SET PASSWORD = PASSWORD('newpwd')");
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
								if (e1.getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD) {
									props.setProperty("disconnectOnExpiredPasswords", "false");
									testConn = getConnectionWithProps(props);
									
									try {
										((MySQLConnection) testConn).changeUser("must_change2", "aha");
										testSt = testConn.createStatement();
										testRs = testSt.executeQuery("SHOW VARIABLES LIKE 'disconnect_on_expired_password'");
										fail("SQLException expected due to password expired");

									} catch (SQLException e5) {
										testSt.executeUpdate("SET PASSWORD = PASSWORD('newpwd')");
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
				if (testRs != null) testRs.close();
				if (testSt != null) testSt.close();
				if (testConn != null) testConn.close();
				this.stmt.executeUpdate("drop user 'must_change1'@'%'");
				this.stmt.executeUpdate("drop user 'must_change2'@'%'");
			}
			
		}

	}

}