/*
 Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA



 */
package testsuite.regression;

import java.io.ByteArrayOutputStream;
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
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import testsuite.BaseTestCase;
import testsuite.UnreliableSocketFactory;

import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.Driver;
import com.mysql.jdbc.Messages;
import com.mysql.jdbc.MysqlDataTruncation;
import com.mysql.jdbc.MysqlErrorNumbers;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.ReplicationConnection;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.StandardSocketFactory;
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
			createTable("testBug3790", "(field1 INT NOT NULL PRIMARY KEY, field2 VARCHAR(32)) ", "InnoDB");
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
			Map charsetsAndCollations = getCharacterSetsAndCollations();
			charsetsAndCollations.remove("latin7"); // Maps to multiple Java
			// charsets
			charsetsAndCollations.remove("ucs2"); // can't be used as a
			// connection charset

			Iterator charsets = charsetsAndCollations.keySet().iterator();

			while (charsets.hasNext()) {
				Connection charsetConn = null;
				Statement charsetStmt = null;

				try {
					String charsetName = charsets.next().toString();
					String collationName = charsetsAndCollations.get(
							charsetName).toString();
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

		Connection killConn = getConnectionWithProps((Properties)null);

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
		assertTrue("Connection is not a reconnected-connection", !connectionId
				.equals(this.rs.getString(1)));

		try {
			reconnectableConn.createStatement().executeQuery("SELECT 1");
		} catch (SQLException sqlEx) {
			; // ignore
		}

		reconnectableConn.createStatement().executeQuery("SELECT 1");

		assertTrue(reconnectableConn.isReadOnly() == isReadOnly);
	}

	private Map getCharacterSetsAndCollations() throws Exception {
		Map charsetsToLoad = new HashMap();

		try {
			this.rs = this.stmt.executeQuery("SHOW character set");

			while (this.rs.next()) {
				charsetsToLoad.put(this.rs.getString("Charset"), this.rs
						.getString("Default collation"));
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
					assertTrue(sqlEx.getMessage().toLowerCase().indexOf(
							"connection refused") != -1);
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
				Statement failoverStmt = portNumConn.createStatement();
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

		Properties urlProps = new NonRegisteringDriver().parseURL(this.dbUrl, null);
		
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
			failoverConnection = getConnectionWithProps("jdbc:mysql://master:" + port + ",slave:" + port + "/", props);
			failoverConnection.setAutoCommit(false);

			String originalConnectionId = getSingleIndexedValueWithQuery(
					failoverConnection, 1, "SELECT CONNECTION_ID()").toString();
			
			for (int i = 0; i < 49; i++) {
				failoverConnection.createStatement().executeQuery("SELECT 1");
			}

			((com.mysql.jdbc.Connection)failoverConnection).clearHasTriedMaster();
			UnreliableSocketFactory.dontDownHost("master");
			
			failoverConnection.setAutoCommit(true);

			String newConnectionId = getSingleIndexedValueWithQuery(
					failoverConnection, 1, "SELECT CONNECTION_ID()").toString();
			
			assertTrue(((com.mysql.jdbc.Connection)failoverConnection).hasTriedMaster());
			
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

		// Re-build the connection information
		int firstIndexOfHost = BaseTestCase.dbUrl.indexOf("//") + 2;
		int lastIndexOfHost = BaseTestCase.dbUrl.indexOf("/", firstIndexOfHost);

		String hostPortPair = BaseTestCase.dbUrl.substring(firstIndexOfHost,
				lastIndexOfHost);

		StringTokenizer st = new StringTokenizer(hostPortPair, ":");

		String host = null;
		String port = null;

		if (st.hasMoreTokens()) {
			String possibleHostOrPort = st.nextToken();

			if (possibleHostOrPort.indexOf(".") == -1
					&& Character.isDigit(possibleHostOrPort.charAt(0))) {
				port = possibleHostOrPort;
				host = "localhost";
			} else {
				host = possibleHostOrPort;
			}
		}

		if (st.hasMoreTokens()) {
			port = st.nextToken();
		}

		if (host == null) {
			host = "";
		}

		if (port == null) {
			port = "3306";
		}

		StringBuffer newHostBuf = new StringBuffer();
		newHostBuf.append(host);
		newHostBuf.append(":");
		newHostBuf.append(port);
		newHostBuf.append(",");
		newHostBuf.append(host);
		if (port != null) {
			newHostBuf.append(":");
			newHostBuf.append(port);
		}

		props.remove("PORT");

		props.setProperty("HOST", newHostBuf.toString());
		props.setProperty("queriesBeforeRetryMaster", "10");
		props.setProperty("maxReconnects", "1");

		Connection failoverConnection = null;
		Connection killerConnection = getConnectionWithProps((String)null);

		try {
			failoverConnection = getConnectionWithProps("jdbc:mysql://"
					+ newHostBuf + "/", props);
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
//					assertEquals("SHIFT_JIS", charSetUC);
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
	 * "autoRecconect=true","roundRobinLoadBalance=true","failOverReadOnly=false".
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
	
	/* FIXME: This test is no longer valid with random selection of hosts 
	public void testBug8643() throws Exception {
		if (runMultiHostTests()) {
			Properties defaultProps = getMasterSlaveProps();

			defaultProps.remove(NonRegisteringDriver.HOST_PROPERTY_KEY);
			defaultProps.remove(NonRegisteringDriver.PORT_PROPERTY_KEY);

			defaultProps.put("autoReconnect", "true");
			defaultProps.put("roundRobinLoadBalance", "true");
			defaultProps.put("failOverReadOnly", "false");

			Connection con = null;
			try {
				con = DriverManager.getConnection(getMasterSlaveUrl(),
						defaultProps);
				Statement stmt1 = con.createStatement();

				ResultSet rs1 = stmt1
						.executeQuery("show variables like 'port'");
				rs1.next();

				rs1 = stmt1.executeQuery("select connection_id()");
				rs1.next();
				String originalConnectionId = rs1.getString(1);
				this.stmt.executeUpdate("kill " + originalConnectionId);

				int numLoops = 8;

				SQLException caughtException = null;

				while (caughtException == null && numLoops > 0) {
					numLoops--;

					try {
						rs1 = stmt1.executeQuery("show variables like 'port'");
					} catch (SQLException sqlEx) {
						caughtException = sqlEx;
					}
				}

				assertNotNull(caughtException);

				// failover and retry
				rs1 = stmt1.executeQuery("show variables like 'port'");

				rs1.next();
				assertTrue(!((com.mysql.jdbc.Connection) con)
						.isMasterConnection());

				rs1 = stmt1.executeQuery("select connection_id()");
				rs1.next();
				String nextConnectionId = rs1.getString(1);
				assertTrue(!nextConnectionId.equals(originalConnectionId));

				this.stmt.executeUpdate("kill " + nextConnectionId);

				numLoops = 8;

				caughtException = null;

				while (caughtException == null && numLoops > 0) {
					numLoops--;

					try {
						rs1 = stmt1.executeQuery("show variables like 'port'");
					} catch (SQLException sqlEx) {
						caughtException = sqlEx;
					}
				}

				assertNotNull(caughtException);

				// failover and retry
				rs1 = stmt1.executeQuery("show variables like 'port'");

				rs1.next();
				assertTrue(((com.mysql.jdbc.Connection) con)
						.isMasterConnection());

			} finally {
				if (con != null) {
					try {
						con.close();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	
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
		assertEquals(true, ((com.mysql.jdbc.Connection) maxPerfConn)
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
				assertTrue(!((ConnectionImpl) ((ReplicationConnection) replConn)
						.getMasterConnection()).hasSameProperties(
								((ReplicationConnection) replConn)
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
				
				HashMap methodsToSkipMap = new HashMap();
				
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
							Class[] parameterTypes = getMethods[i]
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
													.equals(
															"com.mysql.jdbc.NotImplemented")
											&& !invokeEx
											.getCause()
											.getClass()
											.getName()
											.equals(
													"java.sql.SQLFeatureNotSupportedException")) {
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

			assertEquals(masterConnectionId, Integer
					.parseInt(getSingleIndexedValueWithQuery(replConn, 1,
							"SELECT CONNECTION_ID()").toString()));

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
			assertEquals(masterConnectionId, Integer
					.parseInt(getSingleIndexedValueWithQuery(replConn, 1,
							"SELECT CONNECTION_ID()").toString()));
		} finally {
			if (replConn != null) {
				replConn.close();
			}
		}
	}

	/**
	 * Tests bug where downed slave caused round robin load balance not to
	 * cycle back to first host in the list.
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
		
		// Re-build the connection information
		int firstIndexOfHost = BaseTestCase.dbUrl.indexOf("//") + 2;
		int lastIndexOfHost = BaseTestCase.dbUrl.indexOf("/", firstIndexOfHost);
	
		String hostPortPair = BaseTestCase.dbUrl.substring(firstIndexOfHost,
				lastIndexOfHost);
	
		StringTokenizer st = new StringTokenizer(hostPortPair, ":");
	
		String host = null;
		String port = null;
	
		if (st.hasMoreTokens()) {
			String possibleHostOrPort = st.nextToken();
	
			if (Character.isDigit(possibleHostOrPort.charAt(0)) && 
					(possibleHostOrPort.indexOf(".") == -1 /* IPV4 */)  &&
					(possibleHostOrPort.indexOf("::") == -1 /* IPV6 */)) {
				port = possibleHostOrPort;
				host = "localhost";
			} else {
				host = possibleHostOrPort;
			}
		}
	
		if (st.hasMoreTokens()) {
			port = st.nextToken();
		}
	
		if (host == null) {
			host = "";
		}
	
		if (port == null) {
			port = "3306";
		}
	
		StringBuffer newHostBuf = new StringBuffer();
		
		newHostBuf.append(host);
		if (port != null) {
			newHostBuf.append(":");
			newHostBuf.append(port);
		}
	
		newHostBuf.append(",");
		//newHostBuf.append(host);
		newHostBuf.append("192.0.2.1"); // non-exsitent machine from RFC3330 test network
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
			
			String nextId = getSingleIndexedValueWithQuery(
					nextConnection, 1, "SELECT CONNECTION_ID()").toString();
			
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
	 * @throws Exception if the test fails.
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
				searchFrom =  found + 1;
				count++;
			}
			
			// The SELECT doesn't actually start a transaction, so being pedantic the
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
	 * Tests fix for BUG#25514 - Timer instance used for Statement.setQueryTimeout()
	 * created per-connection, rather than per-VM, causing memory leak.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug25514() throws Exception {

		for (int i = 0; i < 10; i++) {
			getConnectionWithProps((Properties)null).close();
		}
		
		ThreadGroup root = Thread.currentThread().getThreadGroup().getParent();

		while (root.getParent() != null) {
	        root = root.getParent();
	    }

		int numThreadsNamedTimer = findNamedThreadCount(root, "Timer");

		if (numThreadsNamedTimer == 0) {
			numThreadsNamedTimer = findNamedThreadCount(root, "MySQL Statement Cancellation Timer");
		}
		
		// Notice that this seems impossible to test on JDKs prior to 1.5, as there is no
		// reliable way to find the TimerThread, so we have to rely on new JDKs for this 
		// test.
		assertTrue("More than one timer for cancel was created", numThreadsNamedTimer <= 1);
	}
	
	private int findNamedThreadCount(ThreadGroup group, String nameStart) {
		
		int count = 0;
		
        int numThreads = group.activeCount();
        Thread[] threads = new Thread[numThreads*2];
        numThreads = group.enumerate(threads, false);
    
        for (int i=0; i<numThreads; i++) {
            if (threads[i].getName().startsWith(nameStart)) {
            	count++;
            }
        }

        int numGroups = group.activeGroupCount();
        ThreadGroup[] groups = new ThreadGroup[numGroups*2];
        numGroups = group.enumerate(groups, false);
    
        for (int i=0; i<numGroups; i++) {
        	count += findNamedThreadCount(groups[i], nameStart);
        }

        return count;
	}
	
	/**
	 * Ensures that we don't miss getters/setters for driver properties in
	 * ConnectionProperties so that names given in documentation work with 
	 * DataSources which will use JavaBean-style names and reflection to 
	 * set the values (and often fail silently! when the method isn't available).
	 * 
	 * @throws Exception
	 */
	public void testBug23626() throws Exception {
		Class clazz = this.conn.getClass();
		
		DriverPropertyInfo[] dpi = new NonRegisteringDriver().getPropertyInfo(dbUrl, null);
		StringBuffer missingSettersBuf = new StringBuffer();
		StringBuffer missingGettersBuf = new StringBuffer();
		
		Class[][] argTypes = {new Class[] { String.class }, new Class[] {Integer.TYPE}, new Class[] {Long.TYPE}, new Class[] {Boolean.TYPE}};
		
		for (int i = 0; i < dpi.length; i++) {
			
			String propertyName = dpi[i].name;
		
			if (propertyName.equals("HOST") || propertyName.equals("PORT") 
					|| propertyName.equals("DBNAME") || propertyName.equals("user") ||
					propertyName.equals("password")) {
				continue;
			}
					
			StringBuffer mutatorName = new StringBuffer("set");
			mutatorName.append(Character.toUpperCase(propertyName.charAt(0)));
			mutatorName.append(propertyName.substring(1));
				
			StringBuffer accessorName = new StringBuffer("get");
			accessorName.append(Character.toUpperCase(propertyName.charAt(0)));
			accessorName.append(propertyName.substring(1));
			
			try {
				clazz.getMethod(accessorName.toString(), null);
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
	 * Tests fix for BUG#25545 - Client flags not sent correctly during handshake
	 * when using SSL.
	 * 
	 * Requires test certificates from testsuite/ssl-test-certs to be installed
	 * on the server being tested.
	 * 
	 * @throws Exception if the test fails.
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
		System.setProperty("javax.net.ssl.keyStorePassword","password");
		System.setProperty("javax.net.ssl.trustStore", trustStorePath);
		System.setProperty("javax.net.ssl.trustStorePassword","password");
		
		
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
				assertEquals(-1, logBuf.toString().indexOf("SHOW VARIABLES LIKE 'tx_isolation'"));
			}
		} finally {
			if (loggedConn != null) {
				loggedConn.close();
			}
		}
	}
	
	/**
	 * Tests fix for issue where a failed-over connection would let
	 * an application call setReadOnly(false), when that call 
	 * should be ignored until the connection is reconnected to a 
	 * writable master.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testFailoverReadOnly() throws Exception {
		Properties props = getMasterSlaveProps();
		props.setProperty("autoReconnect", "true");
	
		Connection failoverConn = null;

		Statement failoverStmt = 
			null;
		
		try {
			failoverConn = getConnectionWithProps(getMasterSlaveUrl(), props);
			
			((com.mysql.jdbc.Connection)failoverConn).setPreferSlaveDuringFailover(true);
			
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
			
			assertTrue("Didn't get a new physical connection",
					!masterConnectionId.equals(slaveConnectionId));
			
			failoverConn.setReadOnly(false); // this should be ignored
			
			assertTrue(failoverConn.isReadOnly());
			
			((com.mysql.jdbc.Connection)failoverConn).setPreferSlaveDuringFailover(false);
			
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
		Class checkerClass = cl.loadClass("com.mysql.jdbc.integration.jboss.MysqlValidConnectionChecker");
		((MysqlValidConnectionChecker)checkerClass.newInstance()).isValidConnection(this.conn);
	}
	
	public void testBug29852() throws Exception {
    	Connection lbConn = getLoadBalancedConnection();
    	assertTrue(!lbConn.getClass().getName().startsWith("com.mysql.jdbc"));
    	lbConn.close();
    }

	/**
	 * Test of a new feature to fix BUG 22643, specifying a
	 * "validation query" in your connection pool that starts
	 * with "slash-star ping slash-star" _exactly_ will cause the driver to " +
	 * instead send a ping to the server (much lighter weight), and when using
	 * a ReplicationConnection or a LoadBalancedConnection, will send
	 * the ping across all active connections.
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
		
		try {
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
		} finally {
			closeMemberJDBCResources();
		}
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
	 * prepared statements and enabling caching of prepared statements (only present
	 * in nightly builds of 5.1).
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
		
		Method isValid = java.sql.Connection.class.getMethod("isValid", new Class[] {Integer.TYPE});
		
		
		Connection newConn = getConnectionWithProps((Properties)null);
		isValid.invoke(newConn, new Object[] {new Integer(1)});
		Thread.sleep(2000);
		assertTrue(((Boolean)isValid.invoke(newConn, new Object[] {new Integer(0)})).booleanValue());
	}
	
	public void testBug34937() throws Exception {
		com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource ds = new
		com.mysql.jdbc.jdbc2.optional.MysqlConnectionPoolDataSource();
		StringBuffer urlBuf = new StringBuffer();
		urlBuf.append(getMasterSlaveUrl());
		urlBuf.append("?");
		Properties props = getMasterSlaveProps();
		String key = null;
		
		Enumeration keyEnum = props.keys();
		
		while (keyEnum.hasMoreElements()) {
			key = (String)keyEnum.nextElement();
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
			try {
				String unicodePassword = "\u0430\u0431\u0432"; // Cyrillic string
				String user = "bug37570";
				Statement adminStmt = adminConn.createStatement();

				adminStmt.executeUpdate("grant usage on *.* to '" + user + "'@'127.0.0.1' identified by 'foo'");
				adminStmt.executeUpdate("update mysql.user set password=PASSWORD('"+ unicodePassword +"') where user = '" + user + "'");
				adminStmt.executeUpdate("flush privileges");
				
				try {
					((ConnectionImpl)adminConn).changeUser(user, unicodePassword);
				} catch (SQLException sqle) {
					assertTrue("Connection with non-latin1 password failed", false);
				}
			} finally {
		    	closeMemberJDBCResources();
			}
		}
	}
	public void testUnreliableSocketFactory() throws Exception {
		Properties props = new Properties();
		props.setProperty("loadBalanceStrategy", "bestResponseTime");
		Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[]{"first", "second"}, props);
		assertNotNull("Connection should not be null", conn);
		try {
			conn2.createStatement().execute("SELECT 1");
			conn2.createStatement().execute("SELECT 1");
			// both connections are live now
			UnreliableSocketFactory.downHost("first");
			UnreliableSocketFactory.downHost("second");
			try{
				conn2.createStatement().execute("SELECT 1");
				fail("Should hang here.");
			} catch (SQLException sqlEx){
				assertEquals("08S01", sqlEx.getSQLState());
			}
		} finally {
	    	closeMemberJDBCResources();
		}
	}

	public void testBug43421() throws Exception {
		
		Properties props = new Properties();
		props.setProperty("loadBalanceStrategy", "bestResponseTime");
		
		Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[]{"first", "second"}, props);
			
		assertNotNull("Connection should not be null", conn2);
		
		try {
			conn2.createStatement().execute("SELECT 1");
			conn2.createStatement().execute("SELECT 1");
			// both connections are live now
			UnreliableSocketFactory.downHost("second");
			UnreliableSocketFactory.downHost("first");
			try{
				conn2.createStatement().execute("/* ping */");
				fail("Pings will not succeed when one host is down and using loadbalance w/o global blacklist.");
			} catch (SQLException sqlEx){
			}
		} finally {
	    	closeMemberJDBCResources();
		}
	
		UnreliableSocketFactory.flushAllHostLists();
		props = new Properties();
		props.setProperty("globalBlacklistTimeout", "200");
		props.setProperty("loadBalanceStrategy", "bestResponseTime");
		
		conn2 = this.getUnreliableLoadBalancedConnection(new String[]{"first", "second"}, props);
		
		assertNotNull("Connection should not be null", conn);
		
		try {
			conn2.createStatement().execute("SELECT 1");
			conn2.createStatement().execute("SELECT 1");
			// both connections are live now
			UnreliableSocketFactory.downHost("second");
			try{
				conn2.createStatement().execute("/* ping */");
			} catch (SQLException sqlEx){
				fail("Pings should succeed even though host is down.");
			}
		} finally {
	    	closeMemberJDBCResources();
		}

	}

	public void testBug48442() throws Exception {
		
		Properties props = new Properties();
		props.setProperty("loadBalanceStrategy", "random");
		Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[]{"first", "second"}, props);
			
		assertNotNull("Connection should not be null", conn2);
		conn2.setAutoCommit(false);
		UnreliableSocketFactory.downHost("second");
		int hc = 0;
		try {
			try{
				conn2.createStatement().execute("SELECT 1");
			} catch (SQLException e){
				conn2.createStatement().execute("SELECT 1");
			}
			hc = conn2.hashCode();
			conn2.commit();
			UnreliableSocketFactory.dontDownHost("second");
			UnreliableSocketFactory.downHost("first");
			try{
				conn2.commit();
			} catch (SQLException e){}
			assertTrue(hc == conn2.hashCode());
			

		} finally {
	    	closeMemberJDBCResources();
		}
	}
	
	public void testBug45171() throws Exception {
		List statementsToTest = new LinkedList();
		statementsToTest.add(this.conn.createStatement());
		statementsToTest.add(((com.mysql.jdbc.Connection)this.conn).clientPrepareStatement("SELECT 1"));
		statementsToTest.add(((com.mysql.jdbc.Connection)this.conn).clientPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS));
		statementsToTest.add(((com.mysql.jdbc.Connection)this.conn).clientPrepareStatement("SELECT 1", new int[0]));
		statementsToTest.add(((com.mysql.jdbc.Connection)this.conn).clientPrepareStatement("SELECT 1", new String[0]));
		statementsToTest.add(((com.mysql.jdbc.Connection)this.conn).serverPrepareStatement("SELECT 1"));
		statementsToTest.add(((com.mysql.jdbc.Connection)this.conn).serverPrepareStatement("SELECT 1", Statement.RETURN_GENERATED_KEYS));
		statementsToTest.add(((com.mysql.jdbc.Connection)this.conn).serverPrepareStatement("SELECT 1", new int[0]));
		statementsToTest.add(((com.mysql.jdbc.Connection)this.conn).serverPrepareStatement("SELECT 1", new String[0]));
		
		Iterator iter = statementsToTest.iterator();
		
		while (iter.hasNext()) {
			Statement toTest = (Statement) iter.next();
			assertEquals(toTest.getResultSetType(), ResultSet.TYPE_FORWARD_ONLY);
			assertEquals(toTest.getResultSetConcurrency(), ResultSet.CONCUR_READ_ONLY);
		}
		
	}
	
	/**
	 * Tests fix for BUG#44587, provide last packet sent/received
	 * timing in all connection failure errors.
	 */
	public void testBug44587() throws Exception {
		Exception e = null;
		String msg = SQLError.createLinkFailureMessageBasedOnHeuristics(
				(ConnectionImpl) this.conn, 
				System.currentTimeMillis() - 1000,
				System.currentTimeMillis() - 2000,
				e, 
				false);
		assertTrue(containsMessage(msg,"CommunicationsException.ServerPacketTimingInfo"));
	}
	
	/**
	 * Tests fix for BUG#45419, ensure that time is not converted to seconds
	 * before being reported as milliseconds.
	 */
	public void testBug45419() throws Exception {
		Exception e = null;
		String msg = SQLError.createLinkFailureMessageBasedOnHeuristics(
				(ConnectionImpl) this.conn, 
				System.currentTimeMillis() - 1000,
				System.currentTimeMillis() - 2000,
				e, 
				false);
		Matcher m = Pattern.compile("([\\d\\,]+)").matcher(msg);
		assertTrue(m.find());
		assertTrue(Long.parseLong(m.group(0).replaceAll(",", "")) >= 2000);
		assertTrue(Long.parseLong(m.group(1).replaceAll(",", "")) >= 1000);
	}
	
	public static boolean containsMessage(String msg, String key) {
		String [] expectedFragments = Messages.getString(key).split("\\{\\d\\}");
		for(int i = 0; i < expectedFragments.length; i++) {
			if(msg.indexOf(expectedFragments[i]) < 0) {
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
        throws SQLException
    {
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

        assertEquals("hostname not equal", host, result
                .getProperty(Driver.HOST_PROPERTY_KEY));
        if (port != null) {
        	assertEquals("port not equal", port, result
                .getProperty(Driver.PORT_PROPERTY_KEY));
        } else {
        	assertEquals("port default incorrect", "3306", result.getProperty(Driver.PORT_PROPERTY_KEY));	
        }
        
        assertEquals("dbname not equal", dbname, result
                .getProperty(Driver.DBNAME_PROPERTY_KEY));
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

		Xid txid = new MysqlXid(new byte[] {0x1}, new byte[] {0xf}, 3306);
		
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
		// we should have the same "currentXAConnection" for both SuspendableXAConnection 
		c2.getXAResource().prepare(txid); // this will fail without the fix.
		c2.getXAResource().commit(txid,false);
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
		int endHost = dbUrl.lastIndexOf("/");
		String databaseStuff = dbUrl.substring(endHost + 1);
		
		Properties props = new NonRegisteringDriver().parseURL(dbUrl, null);
		String host = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
		String port = props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);
		
		String newUrl =  "jdbc:mysql:loadbalance://" + host + ":" + port + "," + host + ":" + port + "/" + databaseStuff;
		
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
		Connection conn2 = this.getUnreliableLoadBalancedConnection(new String[]{"first", "second"}, props);
			
		assertNotNull("Connection should not be null", conn2);
		conn2.setAutoCommit(false);
		conn2.createStatement().execute("SELECT 1");
		conn2.createStatement().execute("SELECT 1");
		conn2.createStatement().execute("SELECT 1");
		conn2.createStatement().execute("SELECT 1");
		conn2.createStatement().execute("SELECT 1");
		conn2.commit();
		try{
			conn2.createStatement().execute("/* ping */ SELECT 1");
		// don't care about this - we want the SQLExceptions passed up early for ping failures, rather
		// than waiting until commit/rollback and pickNewConnection().
		} catch(SQLException e){ }
		assertTrue(conn2.isClosed());
		try{
			conn2.createStatement().execute("SELECT 1");
			fail("Should throw Exception, connection is closed.");
		} catch(SQLException e){ }
		
		
		closeMemberJDBCResources();
	}

	public void testBug49700() throws Exception {
		Connection c = getConnectionWithProps("sessionVariables=@foo='bar'");
		assertEquals("bar", getSingleIndexedValueWithQuery(c, 1, "SELECT @foo"));
		((com.mysql.jdbc.Connection)c).resetServerState();
		assertEquals("bar", getSingleIndexedValueWithQuery(c, 1, "SELECT @foo"));
	}
}
