/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Locale;
import java.util.Properties;

import testsuite.BaseTestCase;

import com.mysql.jdbc.CharsetMapping;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.NotImplemented;
import com.mysql.jdbc.ParameterBindings;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.exceptions.MySQLStatementCancelledException;
import com.mysql.jdbc.exceptions.MySQLTimeoutException;

/**
 * DOCUMENT ME!
 *
 * @author Mark Matthews
 * @version $Id: StatementsTest.java 4494 2005-10-31 22:30:34 -0600 (Mon, 31 Oct
 *          2005) mmatthews $
 */
public class StatementsTest extends BaseTestCase {
	private static final int MAX_COLUMN_LENGTH = 255;

	private static final int MAX_COLUMNS_TO_TEST = 40;

	//private static final int MIN_COLUMN_LENGTH = 10;

	private static final int STEP = 8;

	/**
	 * Runs all test cases in this test suite
	 *
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(StatementsTest.class);
	}

	/**
	 * Creates a new StatementsTest object.
	 *
	 * @param name
	 *            DOCUMENT ME!
	 */
	public StatementsTest(String name) {
		super(name);
	}

	/**
	 * DOCUMENT ME!
	 *
	 * @throws Exception
	 *             DOCUMENT ME!
	 */
	public void setUp() throws Exception {
		super.setUp();

		this.stmt.executeUpdate("DROP TABLE IF EXISTS statement_test");

		this.stmt.executeUpdate("DROP TABLE IF EXISTS statement_batch_test");

		this.stmt
				.executeUpdate("CREATE TABLE statement_test (id int not null primary key auto_increment, strdata1 varchar(255) not null, strdata2 varchar(255))");

		try {
			this.stmt.executeUpdate("CREATE TABLE statement_batch_test "
				+ "(id int not null primary key auto_increment, "
				+ "strdata1 varchar(255) not null, strdata2 varchar(255), "
				+ "UNIQUE INDEX (strdata1))");
		} catch (SQLException sqlEx) {
			if (sqlEx.getMessage().indexOf("max key length") != -1) {
				createTable("statement_batch_test", "(id int not null primary key auto_increment, "
						+ "strdata1 varchar(175) not null, strdata2 varchar(175), "
						+ "UNIQUE INDEX (strdata1))");
			}
		}

		for (int i = 6; i < MAX_COLUMNS_TO_TEST; i += STEP) {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS statement_col_test_"
					+ i);

			StringBuffer insertBuf = new StringBuffer(
					"INSERT INTO statement_col_test_");
			StringBuffer stmtBuf = new StringBuffer(
					"CREATE TABLE IF NOT EXISTS statement_col_test_");
			stmtBuf.append(i);
			insertBuf.append(i);
			stmtBuf.append(" (");
			insertBuf.append(" VALUES (");

			boolean firstTime = true;

			for (int j = 0; j < i; j++) {
				if (!firstTime) {
					stmtBuf.append(",");
					insertBuf.append(",");
				} else {
					firstTime = false;
				}

				stmtBuf.append("col_");
				stmtBuf.append(j);
				stmtBuf.append(" VARCHAR(");
				stmtBuf.append(MAX_COLUMN_LENGTH);
				stmtBuf.append(")");
				insertBuf.append("'");

				int numChars = 16;

				for (int k = 0; k < numChars; k++) {
					insertBuf.append("A");
				}

				insertBuf.append("'");
			}

			stmtBuf.append(")");
			insertBuf.append(")");
			this.stmt.executeUpdate(stmtBuf.toString());
			this.stmt.executeUpdate(insertBuf.toString());
		}

		// explicitly set the catalog to exercise code in execute(),
		// executeQuery() and
		// executeUpdate()
		// FIXME: Only works on Windows!
		// this.conn.setCatalog(this.conn.getCatalog().toUpperCase());
	}

	/**
	 * DOCUMENT ME!
	 *
	 * @throws Exception
	 *             DOCUMENT ME!
	 */
	public void tearDown() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE statement_test");
	
			for (int i = 6; i < MAX_COLUMNS_TO_TEST; i += STEP) {
				StringBuffer stmtBuf = new StringBuffer(
						"DROP TABLE IF EXISTS statement_col_test_");
				stmtBuf.append(i);
				this.stmt.executeUpdate(stmtBuf.toString());
			}
	
			try {
				this.stmt.executeUpdate("DROP TABLE statement_batch_test");
			} catch (SQLException sqlEx) {
				;
			}
		} finally {
			super.tearDown();
		}
	}

	/**
	 * DOCUMENT ME!
	 *
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public void testAccessorsAndMutators() throws SQLException {
		assertTrue("Connection can not be null, and must be same connection",
				this.stmt.getConnection() == this.conn);

		// Set max rows, to exercise code in execute(), executeQuery() and
		// executeUpdate()
		Statement accessorStmt = null;

		try {
			accessorStmt = this.conn.createStatement();
			accessorStmt.setMaxRows(1);
			accessorStmt.setMaxRows(0); // FIXME, test that this actually
			// affects rows returned
			accessorStmt.setMaxFieldSize(255);
			assertTrue("Max field size should match what was set", accessorStmt
					.getMaxFieldSize() == 255);

			try {
				accessorStmt.setMaxFieldSize(Integer.MAX_VALUE);
				fail("Should not be able to set max field size > max_packet_size");
			} catch (SQLException sqlEx) {
				;
			}

			accessorStmt.setCursorName("undef");
			accessorStmt.setEscapeProcessing(true);
			accessorStmt.setFetchDirection(java.sql.ResultSet.FETCH_FORWARD);

			int fetchDirection = accessorStmt.getFetchDirection();
			assertTrue("Set fetch direction != get fetch direction",
					fetchDirection == java.sql.ResultSet.FETCH_FORWARD);

			try {
				accessorStmt.setFetchDirection(Integer.MAX_VALUE);
				fail("Should not be able to set fetch direction to invalid value");
			} catch (SQLException sqlEx) {
				;
			}

			try {
				accessorStmt.setMaxRows(50000000 + 10);
				fail("Should not be able to set max rows > 50000000");
			} catch (SQLException sqlEx) {
				;
			}

			try {
				accessorStmt.setMaxRows(Integer.MIN_VALUE);
				fail("Should not be able to set max rows < 0");
			} catch (SQLException sqlEx) {
				;
			}

			int fetchSize = this.stmt.getFetchSize();

			try {
				accessorStmt.setMaxRows(4);
				accessorStmt.setFetchSize(Integer.MAX_VALUE);
				fail("Should not be able to set FetchSize > max rows");
			} catch (SQLException sqlEx) {
				;
			}

			try {
				accessorStmt.setFetchSize(-2);
				fail("Should not be able to set FetchSize < 0");
			} catch (SQLException sqlEx) {
				;
			}

			assertTrue(
					"Fetch size before invalid setFetchSize() calls should match fetch size now",
					fetchSize == this.stmt.getFetchSize());
		} finally {
			if (accessorStmt != null) {
				try {
					accessorStmt.close();
				} catch (SQLException sqlEx) {
					;
				}

				accessorStmt = null;
			}
		}
	}

	/**
	 * DOCUMENT ME!
	 *
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public void testAutoIncrement() throws SQLException {
		if (!isRunningOnJdk131()) {
			try {
				this.stmt.setFetchSize(Integer.MIN_VALUE);
				
				this.stmt
						.executeUpdate("INSERT INTO statement_test (strdata1) values ('blah')", Statement.RETURN_GENERATED_KEYS);

				int autoIncKeyFromApi = -1;
				this.rs = this.stmt.getGeneratedKeys();

				if (this.rs.next()) {
					autoIncKeyFromApi = this.rs.getInt(1);
				} else {
					fail("Failed to retrieve AUTO_INCREMENT using Statement.getGeneratedKeys()");
				}

				this.rs.close();

				int autoIncKeyFromFunc = -1;
				this.rs = this.stmt.executeQuery("SELECT LAST_INSERT_ID()");

				if (this.rs.next()) {
					autoIncKeyFromFunc = this.rs.getInt(1);
				} else {
					fail("Failed to retrieve AUTO_INCREMENT using LAST_INSERT_ID()");
				}

				if ((autoIncKeyFromApi != -1) && (autoIncKeyFromFunc != -1)) {
					assertTrue(
							"Key retrieved from API ("
									+ autoIncKeyFromApi
									+ ") does not match key retrieved from LAST_INSERT_ID() "
									+ autoIncKeyFromFunc + ") function",
							autoIncKeyFromApi == autoIncKeyFromFunc);
				} else {
					fail("AutoIncrement keys were '0'");
				}
			} finally {
				if (this.rs != null) {
					try {
						this.rs.close();
					} catch (Exception ex) { /* ignore */
						;
					}
				}

				this.rs = null;
			}
		}
	}

	/**
	 * Tests all variants of numerical types (signed/unsigned) for correct
	 * operation when used as return values from a prepared statement.
	 *
	 * @throws Exception
	 */
	public void testBinaryResultSetNumericTypes() throws Exception {
		/*
		 * TINYINT 1 -128 127 SMALLINT 2 -32768 32767 MEDIUMINT 3 -8388608
		 * 8388607 INT 4 -2147483648 2147483647 BIGINT 8 -9223372036854775808
		 * 9223372036854775807
		 */

		String unsignedMinimum = "0";

		String tiMinimum = "-128";
		String tiMaximum = "127";
		String utiMaximum = "255";

		String siMinimum = "-32768";
		String siMaximum = "32767";
		String usiMaximum = "65535";

		String miMinimum = "-8388608";
		String miMaximum = "8388607";
		String umiMaximum = "16777215";

		String iMinimum = "-2147483648";
		String iMaximum = "2147483647";
		String uiMaximum = "4294967295";

		String biMinimum = "-9223372036854775808";
		String biMaximum = "9223372036854775807";
		String ubiMaximum = "18446744073709551615";

		try {
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS testBinaryResultSetNumericTypes");
			this.stmt
					.executeUpdate("CREATE TABLE testBinaryResultSetNumericTypes(rowOrder TINYINT, ti TINYINT,"
							+ "uti TINYINT UNSIGNED, si SMALLINT,"
							+ "usi SMALLINT UNSIGNED, mi MEDIUMINT,"
							+ "umi MEDIUMINT UNSIGNED, i INT, ui INT UNSIGNED,"
							+ "bi BIGINT, ubi BIGINT UNSIGNED)");
			PreparedStatement inserter = this.conn
					.prepareStatement("INSERT INTO testBinaryResultSetNumericTypes VALUES (?,?,?,?,?,?,?,?,?,?,?)");
			inserter.setInt(1, 0);
			inserter.setString(2, tiMinimum);
			inserter.setString(3, unsignedMinimum);
			inserter.setString(4, siMinimum);
			inserter.setString(5, unsignedMinimum);
			inserter.setString(6, miMinimum);
			inserter.setString(7, unsignedMinimum);
			inserter.setString(8, iMinimum);
			inserter.setString(9, unsignedMinimum);
			inserter.setString(10, biMinimum);
			inserter.setString(11, unsignedMinimum);
			inserter.executeUpdate();

			inserter.setInt(1, 1);
			inserter.setString(2, tiMaximum);
			inserter.setString(3, utiMaximum);
			inserter.setString(4, siMaximum);
			inserter.setString(5, usiMaximum);
			inserter.setString(6, miMaximum);
			inserter.setString(7, umiMaximum);
			inserter.setString(8, iMaximum);
			inserter.setString(9, uiMaximum);
			inserter.setString(10, biMaximum);
			inserter.setString(11, ubiMaximum);
			inserter.executeUpdate();

			PreparedStatement selector = this.conn
					.prepareStatement("SELECT * FROM testBinaryResultSetNumericTypes ORDER by rowOrder ASC");
			this.rs = selector.executeQuery();

			assertTrue(this.rs.next());

			assertTrue(this.rs.getString(2).equals(tiMinimum));
			assertTrue(this.rs.getString(3).equals(unsignedMinimum));
			assertTrue(this.rs.getString(4).equals(siMinimum));
			assertTrue(this.rs.getString(5).equals(unsignedMinimum));
			assertTrue(this.rs.getString(6).equals(miMinimum));
			assertTrue(this.rs.getString(7).equals(unsignedMinimum));
			assertTrue(this.rs.getString(8).equals(iMinimum));
			assertTrue(this.rs.getString(9).equals(unsignedMinimum));
			assertTrue(this.rs.getString(10).equals(biMinimum));
			assertTrue(this.rs.getString(11).equals(unsignedMinimum));

			assertTrue(this.rs.next());

			assertTrue(this.rs.getString(2) + " != " + tiMaximum, this.rs
					.getString(2).equals(tiMaximum));
			assertTrue(this.rs.getString(3) + " != " + utiMaximum, this.rs
					.getString(3).equals(utiMaximum));
			assertTrue(this.rs.getString(4) + " != " + siMaximum, this.rs
					.getString(4).equals(siMaximum));
			assertTrue(this.rs.getString(5) + " != " + usiMaximum, this.rs
					.getString(5).equals(usiMaximum));
			assertTrue(this.rs.getString(6) + " != " + miMaximum, this.rs
					.getString(6).equals(miMaximum));
			assertTrue(this.rs.getString(7) + " != " + umiMaximum, this.rs
					.getString(7).equals(umiMaximum));
			assertTrue(this.rs.getString(8) + " != " + iMaximum, this.rs
					.getString(8).equals(iMaximum));
			assertTrue(this.rs.getString(9) + " != " + uiMaximum, this.rs
					.getString(9).equals(uiMaximum));
			assertTrue(this.rs.getString(10) + " != " + biMaximum, this.rs
					.getString(10).equals(biMaximum));
			assertTrue(this.rs.getString(11) + " != " + ubiMaximum, this.rs
					.getString(11).equals(ubiMaximum));

			assertTrue(!this.rs.next());
		} finally {
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS testBinaryResultSetNumericTypes");
		}
	}

	/**
	 * Tests stored procedure functionality
	 *
	 * @throws Exception
	 *             if an error occurs.
	 */
	public void testCallableStatement() throws Exception {
		if (versionMeetsMinimum(5, 0)) {
			CallableStatement cStmt = null;
			String stringVal = "abcdefg";
			int intVal = 42;

			try {
				try {
					this.stmt.executeUpdate("DROP PROCEDURE testCallStmt");
				} catch (SQLException sqlEx) {
					if (sqlEx.getMessage().indexOf("does not exist") == -1) {
						throw sqlEx;
					}
				}

				this.stmt.executeUpdate("DROP TABLE IF EXISTS callStmtTbl");
				this.stmt
						.executeUpdate("CREATE TABLE callStmtTbl (x CHAR(16), y INT)");

				this.stmt
						.executeUpdate("CREATE PROCEDURE testCallStmt(n INT, x CHAR(16), y INT)"
								+ " WHILE n DO"
								+ "    SET n = n - 1;"
								+ "    INSERT INTO callStmtTbl VALUES (x, y);"
								+ " END WHILE;");

				int rowsToCheck = 15;

				cStmt = this.conn.prepareCall("{call testCallStmt(?,?,?)}");
				cStmt.setInt(1, rowsToCheck);
				cStmt.setString(2, stringVal);
				cStmt.setInt(3, intVal);
				cStmt.execute();

				this.rs = this.stmt.executeQuery("SELECT x,y FROM callStmtTbl");

				int numRows = 0;

				while (this.rs.next()) {
					assertTrue(this.rs.getString(1).equals(stringVal)
							&& (this.rs.getInt(2) == intVal));

					numRows++;
				}

				this.rs.close();
				this.rs = null;

				cStmt.close();
				cStmt = null;

				System.out.println(rowsToCheck + " rows returned");

				assertTrue(numRows == rowsToCheck);
			} finally {
				try {
					this.stmt.executeUpdate("DROP PROCEDURE testCallStmt");
				} catch (SQLException sqlEx) {
					if (sqlEx.getMessage().indexOf("does not exist") == -1) {
						throw sqlEx;
					}
				}

				this.stmt.executeUpdate("DROP TABLE IF EXISTS callStmtTbl");

				if (cStmt != null) {
					cStmt.close();
				}
			}
		}
	}

	public void testCancelStatement() throws Exception {

		if (versionMeetsMinimum(5, 0)) {
			Connection cancelConn = null;

			try {
				cancelConn = getConnectionWithProps((String)null);
				final Statement cancelStmt = cancelConn.createStatement();

				cancelStmt.setQueryTimeout(1);

				long begin = System.currentTimeMillis();

				try {
					cancelStmt.execute("SELECT SLEEP(30)");
				} catch (SQLException sqlEx) {
					assertTrue("Probably wasn't actually cancelled", System
							.currentTimeMillis()
							- begin < 30000);
				}

				for (int i = 0; i < 1000; i++) {
					try {
						cancelStmt.executeQuery("SELECT 1");
					} catch (SQLException timedOutEx) {
						break;
					}
				}

				// Make sure we can still use the connection...

				cancelStmt.setQueryTimeout(0);
				this.rs = cancelStmt.executeQuery("SELECT 1");

				assertTrue(this.rs.next());
				assertEquals(1, this.rs.getInt(1));

				cancelStmt.setQueryTimeout(0);

				new Thread() {

					public void run() {
						try {
							try {
								sleep(5000);
							} catch (InterruptedException iEx) {
								// ignore
							}

							cancelStmt.cancel();
						} catch (SQLException sqlEx) {
							throw new RuntimeException(sqlEx.toString());
						}
					}

				}.start();

				begin = System.currentTimeMillis();

				try {
					cancelStmt.execute("SELECT SLEEP(30)");
				} catch (SQLException sqlEx) {
					assertTrue("Probably wasn't actually cancelled", System
							.currentTimeMillis()
							- begin < 30000);
				}

				for (int i = 0; i < 1000; i++) {
					try {
						cancelStmt.executeQuery("SELECT 1");
					} catch (SQLException timedOutEx) {
						break;
					}
				}

				// Make sure we can still use the connection...

				this.rs = cancelStmt.executeQuery("SELECT 1");

				assertTrue(this.rs.next());
				assertEquals(1, this.rs.getInt(1));

				final PreparedStatement cancelPstmt = cancelConn.prepareStatement("SELECT SLEEP(30)");

				cancelPstmt.setQueryTimeout(1);

				begin = System.currentTimeMillis();

				try {
					cancelPstmt.execute();
				} catch (SQLException sqlEx) {
					assertTrue("Probably wasn't actually cancelled", System
							.currentTimeMillis()
							- begin < 30000);
				}

				for (int i = 0; i < 1000; i++) {
					try {
						cancelPstmt.executeQuery("SELECT 1");
					} catch (SQLException timedOutEx) {
						break;
					}
				}

				// Make sure we can still use the connection...

				this.rs = cancelStmt.executeQuery("SELECT 1");

				assertTrue(this.rs.next());
				assertEquals(1, this.rs.getInt(1));

				cancelPstmt.setQueryTimeout(0);

				new Thread() {

					public void run() {
						try {
							try {
								sleep(5000);
							} catch (InterruptedException iEx) {
								// ignore
							}


							cancelPstmt.cancel();
						} catch (SQLException sqlEx) {
							throw new RuntimeException(sqlEx.toString());
						}
					}

				}.start();

				begin = System.currentTimeMillis();

				try {
					cancelPstmt.execute();
				} catch (SQLException sqlEx) {
					assertTrue("Probably wasn't actually cancelled", System
							.currentTimeMillis()
							- begin < 30000);
				}

				for (int i = 0; i < 1000; i++) {
					try {
						cancelPstmt.executeQuery("SELECT 1");
					} catch (SQLException timedOutEx) {
						break;
					}
				}

				// Make sure we can still use the connection...

				this.rs = cancelStmt.executeQuery("SELECT 1");

				assertTrue(this.rs.next());
				assertEquals(1, this.rs.getInt(1));

				final PreparedStatement cancelClientPstmt = ((com.mysql.jdbc.Connection)cancelConn).clientPrepareStatement("SELECT SLEEP(30)");

				cancelClientPstmt.setQueryTimeout(1);

				begin = System.currentTimeMillis();

				try {
					cancelClientPstmt.execute();
				} catch (SQLException sqlEx) {
					assertTrue("Probably wasn't actually cancelled", System
							.currentTimeMillis()
							- begin < 30000);
				}

				for (int i = 0; i < 1000; i++) {
					try {
						cancelStmt.executeQuery("SELECT 1");
					} catch (SQLException timedOutEx) {
						break;
					}
				}

				// Make sure we can still use the connection...

				this.rs = cancelStmt.executeQuery("SELECT 1");

				assertTrue(this.rs.next());
				assertEquals(1, this.rs.getInt(1));

				cancelClientPstmt.setQueryTimeout(0);

				new Thread() {

					public void run() {
						try {
							try {
								sleep(5000);
							} catch (InterruptedException iEx) {
								// ignore
							}


							cancelClientPstmt.cancel();
						} catch (SQLException sqlEx) {
							throw new RuntimeException(sqlEx.toString());
						}
					}

				}.start();

				begin = System.currentTimeMillis();

				try {
					cancelClientPstmt.execute();
				} catch (SQLException sqlEx) {
					assertTrue("Probably wasn't actually cancelled", System
							.currentTimeMillis()
							- begin < 30000);
				}

				for (int i = 0; i < 1000; i++) {
					try {
						cancelClientPstmt.executeQuery("SELECT 1");
					} catch (SQLException timedOutEx) {
						break;
					}
				}

				// Make sure we can still use the connection...

				this.rs = cancelStmt.executeQuery("SELECT 1");

				assertTrue(this.rs.next());
				assertEquals(1, this.rs.getInt(1));
				
				Connection forceCancel = getConnectionWithProps("queryTimeoutKillsConnection=true");
				Statement forceStmt = forceCancel.createStatement();
				forceStmt.setQueryTimeout(1);
				
				try {
					forceStmt.execute("SELECT SLEEP(30)");
					fail("Statement should have been cancelled");
				} catch (MySQLTimeoutException timeout) {
					// expected
				}
				
				int count = 1000;
				
				for (; count > 0; count--) {
					if (forceCancel.isClosed()) {
						break;
					}
					
					Thread.sleep(100);
				}
				
				if (count == 0) {
					fail("Connection was never killed");
				}
				
				try {
					forceCancel.setAutoCommit(true); // should fail too
				} catch (SQLException sqlEx) {
					assertTrue(sqlEx.getCause() instanceof MySQLStatementCancelledException);
				}
				
			} finally {
				if (this.rs != null) {
					ResultSet toClose = this.rs;
					this.rs = null;
					toClose.close();
				}

				if (cancelConn != null) {
					cancelConn.close();
				}
			}
		}
	}

	/**
	 * DOCUMENT ME!
	 *
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public void testClose() throws SQLException {
		Statement closeStmt = null;
		boolean exceptionAfterClosed = false;

		try {
			closeStmt = this.conn.createStatement();
			closeStmt.close();

			try {
				closeStmt.executeQuery("SELECT 1");
			} catch (SQLException sqlEx) {
				exceptionAfterClosed = true;
			}
		} finally {
			if (closeStmt != null) {
				try {
					closeStmt.close();
				} catch (SQLException sqlEx) {
					/* ignore */
				}
			}

			closeStmt = null;
		}

		assertTrue(
				"Operations not allowed on Statement after .close() is called!",
				exceptionAfterClosed);
	}

	public void testEnableStreamingResults() throws Exception {
		Statement streamStmt = this.conn.createStatement();
		((com.mysql.jdbc.Statement) streamStmt).enableStreamingResults();
		assertEquals(streamStmt.getFetchSize(), Integer.MIN_VALUE);
		assertEquals(streamStmt.getResultSetType(), ResultSet.TYPE_FORWARD_ONLY);
	}

	public void testHoldingResultSetsOverClose() throws Exception {
		Properties props = new Properties();
		props.setProperty("holdResultsOpenOverStatementClose", "true");

		Connection conn2 = getConnectionWithProps(props);

		Statement stmt2 = null;
		PreparedStatement pstmt2 = null;

		try {
			stmt2 = conn2.createStatement();

			this.rs = stmt2.executeQuery("SELECT 1");
			this.rs.next();
			this.rs.getInt(1);
			stmt2.close();
			this.rs.getInt(1);

			stmt2 = conn2.createStatement();
			stmt2.execute("SELECT 1");
			this.rs = stmt2.getResultSet();
			this.rs.next();
			this.rs.getInt(1);
			stmt2.execute("SELECT 2");
			this.rs.getInt(1);

			pstmt2 = conn2.prepareStatement("SELECT 1");
			this.rs = pstmt2.executeQuery();
			this.rs.next();
			this.rs.getInt(1);
			pstmt2.close();
			this.rs.getInt(1);

			pstmt2 = conn2.prepareStatement("SELECT 1");
			this.rs = pstmt2.executeQuery();
			this.rs.next();
			this.rs.getInt(1);
			pstmt2.executeQuery();
			this.rs.getInt(1);
			pstmt2.execute();
			this.rs.getInt(1);

			pstmt2 = ((com.mysql.jdbc.Connection) conn2)
					.clientPrepareStatement("SELECT 1");
			this.rs = pstmt2.executeQuery();
			this.rs.next();
			this.rs.getInt(1);
			pstmt2.close();
			this.rs.getInt(1);

			pstmt2 = ((com.mysql.jdbc.Connection) conn2)
					.clientPrepareStatement("SELECT 1");
			this.rs = pstmt2.executeQuery();
			this.rs.next();
			this.rs.getInt(1);
			pstmt2.executeQuery();
			this.rs.getInt(1);
			pstmt2.execute();
			this.rs.getInt(1);

			stmt2 = conn2.createStatement();
			this.rs = stmt2.executeQuery("SELECT 1");
			this.rs.next();
			this.rs.getInt(1);
			stmt2.executeQuery("SELECT 2");
			this.rs.getInt(1);
			this.rs = stmt2.executeQuery("SELECT 1");
			this.rs.next();
			this.rs.getInt(1);
			stmt2.executeUpdate("SET @var=1");
			this.rs.getInt(1);
			stmt2.execute("SET @var=2");
			this.rs.getInt(1);
		} finally {
			if (stmt2 != null) {
				stmt2.close();
			}
		}
	}

	/**
	 * DOCUMENT ME!
	 *
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public void testInsert() throws SQLException {
		try {
			boolean autoCommit = this.conn.getAutoCommit();

			// Test running a query for an update. It should fail.
			try {
				this.conn.setAutoCommit(false);
				this.stmt.executeUpdate("SELECT * FROM statement_test");
			} catch (SQLException sqlEx) {
				assertTrue("Exception thrown for unknown reason", sqlEx
						.getSQLState().equalsIgnoreCase("01S03"));
			} finally {
				this.conn.setAutoCommit(autoCommit);
			}

			// Test running a update for an query. It should fail.
			try {
				this.conn.setAutoCommit(false);
				this.stmt
						.executeQuery("UPDATE statement_test SET strdata1='blah' WHERE 1=0");
			} catch (SQLException sqlEx) {
				assertTrue("Exception thrown for unknown reason", sqlEx
						.getSQLState().equalsIgnoreCase(
								SQLError.SQL_STATE_ILLEGAL_ARGUMENT));
			} finally {
				this.conn.setAutoCommit(autoCommit);
			}

			for (int i = 0; i < 10; i++) {
				int updateCount = this.stmt
						.executeUpdate("INSERT INTO statement_test (strdata1,strdata2) values ('abcdefg', 'poi')");
				assertTrue("Update count must be '1', was '" + updateCount
						+ "'", (updateCount == 1));
			}

			if (!isRunningOnJdk131()) {
				int insertIdFromGeneratedKeys = Integer.MIN_VALUE;

				this.stmt
						.executeUpdate("INSERT INTO statement_test (strdata1, strdata2) values ('a', 'a'), ('b', 'b'), ('c', 'c')", Statement.RETURN_GENERATED_KEYS);
				this.rs = this.stmt.getGeneratedKeys();

				if (this.rs.next()) {
					insertIdFromGeneratedKeys = this.rs.getInt(1);
				}

				this.rs.close();
				this.rs = this.stmt.executeQuery("SELECT LAST_INSERT_ID()");

				int insertIdFromServer = Integer.MIN_VALUE;

				if (this.rs.next()) {
					insertIdFromServer = this.rs.getInt(1);
				}

				assertEquals(insertIdFromGeneratedKeys, insertIdFromServer);
			}
		} finally {
			if (this.rs != null) {
				try {
					this.rs.close();
				} catch (Exception ex) { /* ignore */
					;
				}
			}

			this.rs = null;
		}
	}

	/**
	 * Tests multiple statement support
	 *
	 * @throws Exception
	 *             DOCUMENT ME!
	 */
	public void testMultiStatements() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			Connection multiStmtConn = null;
			Statement multiStmt = null;

			try {
				Properties props = new Properties();
				props.setProperty("allowMultiQueries", "true");

				multiStmtConn = getConnectionWithProps(props);

				multiStmt = multiStmtConn.createStatement();

				multiStmt
						.executeUpdate("DROP TABLE IF EXISTS testMultiStatements");
				multiStmt
						.executeUpdate("CREATE TABLE testMultiStatements (field1 VARCHAR(255), field2 INT, field3 DOUBLE)");
				multiStmt
						.executeUpdate("INSERT INTO testMultiStatements VALUES ('abcd', 1, 2)");

				multiStmt
						.execute("SELECT field1 FROM testMultiStatements WHERE field1='abcd';"
								+ "UPDATE testMultiStatements SET field3=3;"
								+ "SELECT field3 FROM testMultiStatements WHERE field3=3");

				this.rs = multiStmt.getResultSet();

				assertTrue(this.rs.next());

				assertTrue("abcd".equals(this.rs.getString(1)));
				this.rs.close();

				// Next should be an update count...
				assertTrue(!multiStmt.getMoreResults());

				assertTrue("Update count was " + multiStmt.getUpdateCount()
						+ ", expected 1", multiStmt.getUpdateCount() == 1);

				assertTrue(multiStmt.getMoreResults());

				this.rs = multiStmt.getResultSet();

				assertTrue(this.rs.next());

				assertTrue(this.rs.getDouble(1) == 3);

				// End of multi results
				assertTrue(!multiStmt.getMoreResults());
				assertTrue(multiStmt.getUpdateCount() == -1);
			} finally {
				if (multiStmt != null) {
					multiStmt
							.executeUpdate("DROP TABLE IF EXISTS testMultiStatements");

					multiStmt.close();
				}

				if (multiStmtConn != null) {
					multiStmtConn.close();
				}
			}
		}
	}

	/**
	 * Tests that NULLs and '' work correctly.
	 *
	 * @throws SQLException
	 *             if an error occurs
	 */
	public void testNulls() throws SQLException {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS nullTest");
			this.stmt
					.executeUpdate("CREATE TABLE IF NOT EXISTS nullTest (field_1 CHAR(20), rowOrder INT)");
			this.stmt
					.executeUpdate("INSERT INTO nullTest VALUES (null, 1), ('', 2)");

			this.rs = this.stmt
					.executeQuery("SELECT field_1 FROM nullTest ORDER BY rowOrder");

			this.rs.next();

			assertTrue("NULL field not returned as NULL", (this.rs
					.getString("field_1") == null)
					&& this.rs.wasNull());

			this.rs.next();

			assertTrue("Empty field not returned as \"\"", this.rs.getString(
					"field_1").equals("")
					&& !this.rs.wasNull());

			this.rs.close();
		} finally {
			if (this.rs != null) {
				try {
					this.rs.close();
				} catch (Exception ex) {
					// ignore
				}
			}

			this.stmt.executeUpdate("DROP TABLE IF EXISTS nullTest");
		}
	}

	public void testParsedConversionWarning() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			try {
				Properties props = new Properties();
				props.setProperty("useUsageAdvisor", "true");
				Connection warnConn = getConnectionWithProps(props);

				this.stmt
						.executeUpdate("DROP TABLE IF EXISTS testParsedConversionWarning");
				this.stmt
						.executeUpdate("CREATE TABLE testParsedConversionWarning(field1 VARCHAR(255))");
				this.stmt
						.executeUpdate("INSERT INTO testParsedConversionWarning VALUES ('1.0')");

				PreparedStatement badStmt = warnConn
						.prepareStatement("SELECT field1 FROM testParsedConversionWarning");

				this.rs = badStmt.executeQuery();
				assertTrue(this.rs.next());
				this.rs.getFloat(1);
			} finally {
				this.stmt
						.executeUpdate("DROP TABLE IF EXISTS testParsedConversionWarning");
			}
		}
	}

	/**
	 * DOCUMENT ME!
	 *
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public void testPreparedStatement() throws SQLException {
		this.stmt
				.executeUpdate("INSERT INTO statement_test (id, strdata1,strdata2) values (999,'abcdefg', 'poi')");
		this.pstmt = this.conn
				.prepareStatement("UPDATE statement_test SET strdata1=?, strdata2=? where id=999");
		this.pstmt.setString(1, "iop");
		this.pstmt.setString(2, "higjklmn");

		// pstmt.setInt(3, 999);
		int updateCount = this.pstmt.executeUpdate();
		assertTrue("Update count must be '1', was '" + updateCount + "'",
				(updateCount == 1));

		this.pstmt.clearParameters();

		this.pstmt.close();

		this.rs = this.stmt
				.executeQuery("SELECT id, strdata1, strdata2 FROM statement_test");

		assertTrue(this.rs.next());
		assertTrue(this.rs.getInt(1) == 999);
		assertTrue("Expected 'iop', received '" + this.rs.getString(2) + "'",
				"iop".equals(this.rs.getString(2)));
		assertTrue("Expected 'higjklmn', received '" + this.rs.getString(3)
				+ "'", "higjklmn".equals(this.rs.getString(3)));
	}

	/**
	 * DOCUMENT ME!
	 *
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public void testPreparedStatementBatch() throws SQLException {
		this.pstmt = this.conn.prepareStatement("INSERT INTO "
				+ "statement_batch_test (strdata1, strdata2) VALUES (?,?)");

		for (int i = 0; i < 1000; i++) {
			this.pstmt.setString(1, "batch_" + i);
			this.pstmt.setString(2, "batch_" + i);
			this.pstmt.addBatch();
		}

		int[] updateCounts = this.pstmt.executeBatch();

		for (int i = 0; i < updateCounts.length; i++) {
			assertTrue("Update count must be '1', was '" + updateCounts[i]
					+ "'", (updateCounts[i] == 1));
		}
	}

	public void testRowFetch() throws Exception {
		if (versionMeetsMinimum(5, 0, 5)) {
			createTable("testRowFetch", "(field1 int)");

			this.stmt.executeUpdate("INSERT INTO testRowFetch VALUES (1)");

			Connection fetchConn = null;

			Properties props = new Properties();
			props.setProperty("useCursorFetch", "true");


			try {
				fetchConn = getConnectionWithProps(props);

				PreparedStatement fetchStmt = fetchConn
						.prepareStatement("SELECT field1 FROM testRowFetch WHERE field1=1");
				fetchStmt.setFetchSize(10);
				this.rs = fetchStmt.executeQuery();
				assertTrue(this.rs.next());

				this.stmt.executeUpdate("INSERT INTO testRowFetch VALUES (2), (3)");

				fetchStmt = fetchConn
						.prepareStatement("SELECT field1 FROM testRowFetch ORDER BY field1");
				fetchStmt.setFetchSize(1);
				this.rs = fetchStmt.executeQuery();

				assertTrue(this.rs.next());
				assertEquals(1, this.rs.getInt(1));
				assertTrue(this.rs.next());
				assertEquals(2, this.rs.getInt(1));
				assertTrue(this.rs.next());
				assertEquals(3, this.rs.getInt(1));
				assertEquals(false, this.rs.next());

				fetchStmt.executeQuery();
			} finally {
				if (fetchConn != null) {
					fetchConn.close();
				}
			}

		}
	}

	/**
	 * DOCUMENT ME!
	 *
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public void testSelectColumns() throws SQLException {
		for (int i = 6; i < MAX_COLUMNS_TO_TEST; i += STEP) {
			long start = System.currentTimeMillis();
			this.rs = this.stmt
					.executeQuery("SELECT * from statement_col_test_" + i);

			if (this.rs.next()) {
				;
			}

			long end = System.currentTimeMillis();
			System.out.println(i + " columns = " + (end - start) + " ms");
		}
	}

	/**
	 * Tests for PreparedStatement.setObject()
	 *
	 * @throws Exception
	 */
	public void testSetObject() throws Exception {
		Properties props = new Properties();
		props.put("noDatetimeStringSync", "true"); // value=true for #5
		Connection conn1 = getConnectionWithProps(props);
		Statement stmt1 = conn1.createStatement();
		createTable("t1", " (" + "c1 DECIMAL," // instance of
																// String
				+ "c2 VARCHAR(255)," // instance of String
				+ "c3 BLOB," // instance of byte[]
				+ "c4 DATE," // instance of java.util.Date
				+ "c5 TIMESTAMP," // instance of String
				+ "c6 TIME," // instance of String
				+ "c7 TIME)"); // instance of java.sql.Timestamp

		this.pstmt = conn1
				.prepareStatement("INSERT INTO t1 VALUES (?, ?, ?, ?, ?, ?, ?)");

		long currentTime = System.currentTimeMillis();

		this.pstmt.setObject(1, "1000", Types.DECIMAL);
		this.pstmt.setObject(2, "2000", Types.VARCHAR);
		this.pstmt.setObject(3, new byte[] { 0 }, Types.BLOB);
		this.pstmt.setObject(4, new java.util.Date(currentTime), Types.DATE);
		this.pstmt.setObject(5, "2000-01-01 23-59-59", Types.TIMESTAMP);
		this.pstmt.setObject(6, "11:22:33", Types.TIME);
		this.pstmt
				.setObject(7, new java.sql.Timestamp(currentTime), Types.TIME);
		this.pstmt.execute();
		this.rs = stmt1.executeQuery("SELECT * FROM t1");
		this.rs.next();

		assertEquals("1000", this.rs.getString(1));
		assertEquals("2000", this.rs.getString(2));
		assertEquals(1, ((byte[]) this.rs.getObject(3)).length);
		assertEquals(0, ((byte[]) this.rs.getObject(3))[0]);
		assertEquals(new java.sql.Date(currentTime).toString(), this.rs
				.getDate(4).toString());

		if (versionMeetsMinimum(4, 1)) {
			assertEquals("2000-01-01 23:59:59", this.rs.getString(5));
		} else {
			assertEquals("20000101235959", this.rs.getString(5));
		}

		assertEquals("11:22:33", this.rs.getString(6));
		assertEquals(new java.sql.Time(currentTime).toString(), this.rs
				.getString(7));
	}

	public void testStatementRewriteBatch() throws Exception {
		for (int j = 0; j < 2; j++) {
			Properties props = new Properties();

			if (j == 0) {
				props.setProperty("useServerPrepStmts", "true");
			}

			props.setProperty("rewriteBatchedStatements", "true");
			Connection multiConn = getConnectionWithProps(props);
			createTable("testStatementRewriteBatch", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
			Statement multiStmt = multiConn.createStatement();
			multiStmt.addBatch("INSERT INTO testStatementRewriteBatch(field1) VALUES (1)");
			multiStmt.addBatch("INSERT INTO testStatementRewriteBatch(field1) VALUES (2)");
			multiStmt.addBatch("INSERT INTO testStatementRewriteBatch(field1) VALUES (3)");
			multiStmt.addBatch("INSERT INTO testStatementRewriteBatch(field1) VALUES (4)");
			multiStmt.addBatch("UPDATE testStatementRewriteBatch SET field1=5 WHERE field1=1");
			multiStmt.addBatch("UPDATE testStatementRewriteBatch SET field1=6 WHERE field1=2 OR field1=3");

			int[] counts = multiStmt.executeBatch();

			if (!isRunningOnJdk131()) {
				ResultSet genKeys = multiStmt.getGeneratedKeys();

				for (int i = 1; i < 5; i++) {
					genKeys.next();
					assertEquals(i, genKeys.getInt(1));
				}
			}

			assertEquals(counts.length, 6);
			assertEquals(counts[0], 1);
			assertEquals(counts[1], 1);
			assertEquals(counts[2], 1);
			assertEquals(counts[3], 1);
			assertEquals(counts[4], 1);
			assertEquals(counts[5], 2);

			this.rs = multiStmt.executeQuery("SELECT field1 FROM testStatementRewriteBatch ORDER BY field1");
			assertTrue(this.rs.next());
			assertEquals(this.rs.getInt(1), 4);
			assertTrue(this.rs.next());
			assertEquals(this.rs.getInt(1), 5);
			assertTrue(this.rs.next());
			assertEquals(this.rs.getInt(1), 6);
			assertTrue(this.rs.next());
			assertEquals(this.rs.getInt(1), 6);

			createTable("testStatementRewriteBatch", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
			props.clear();
			props.setProperty("rewriteBatchedStatements", "true");
			props.setProperty("maxAllowedPacket", "1024");
			multiConn = getConnectionWithProps(props);
			multiStmt = multiConn.createStatement();

			for (int i = 0; i < 1000; i++) {
				multiStmt.addBatch("INSERT INTO testStatementRewriteBatch(field1) VALUES (" + i + ")");
			}

			multiStmt.executeBatch();

			if (!isRunningOnJdk131()) {
				ResultSet genKeys = multiStmt.getGeneratedKeys();

				for (int i = 1; i < 1000; i++) {
					genKeys.next();
					assertEquals(i, genKeys.getInt(1));
				}
			}

			createTable("testStatementRewriteBatch", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");

			props.clear();
			props.setProperty("useServerPrepStmts", j == 0 ? "true" : "false");
			props.setProperty("rewriteBatchedStatements", "true");
			multiConn = getConnectionWithProps(props);

			PreparedStatement pStmt = null;

			if (!isRunningOnJdk131()) {
				pStmt = multiConn.prepareStatement("INSERT INTO testStatementRewriteBatch(field1) VALUES (?)",
						Statement.RETURN_GENERATED_KEYS);

				for (int i = 0; i < 1000; i++) {
					pStmt.setInt(1, i);
					pStmt.addBatch();
				}

				pStmt.executeBatch();

				ResultSet genKeys = pStmt.getGeneratedKeys();

				for (int i = 1; i < 1000; i++) {
					genKeys.next();
					assertEquals(i, genKeys.getInt(1));
				}
			}

			createTable("testStatementRewriteBatch", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
			props.setProperty("useServerPrepStmts", j == 0 ? "true" : "false");
			props.setProperty("rewriteBatchedStatements", "true");
			props.setProperty("maxAllowedPacket", j == 0 ? "10240" : "1024");
			multiConn = getConnectionWithProps(props);

			if (!isRunningOnJdk131()) {

				pStmt = multiConn.prepareStatement("INSERT INTO testStatementRewriteBatch(field1) VALUES (?)",
					Statement.RETURN_GENERATED_KEYS);

				for (int i = 0; i < 1000; i++) {
					pStmt.setInt(1, i);
					pStmt.addBatch();
				}

				pStmt.executeBatch();


				ResultSet genKeys = pStmt.getGeneratedKeys();

				for (int i = 1; i < 1000; i++) {
					genKeys.next();
					assertEquals(i, genKeys.getInt(1));
				}
			}

			Object[][] differentTypes = new Object[1000][14];

			createTable("rewriteBatchTypes", "(internalOrder int, f1 tinyint null, "
					+ "f2 smallint null, f3 int null, f4 bigint null, "
					+ "f5 decimal(8, 2) null, f6 float null, f7 double null, "
					+ "f8 varchar(255) null, f9 text null, f10 blob null, f11 blob null, "
					+ (versionMeetsMinimum(5, 6, 4) ?
						"f12 datetime(3) null, f13 time(3) null, f14 date null)" :
						"f12 datetime null, f13 time null, f14 date null)"
						)
					);

			for (int i = 0; i < 1000; i++) {
				differentTypes[i][0] = Math.random() < .5 ? null : new Byte((byte)(Math.random() * 127));
				differentTypes[i][1] = Math.random() < .5 ? null : new Short((short)(Math.random() * Short.MAX_VALUE));
				differentTypes[i][2] = Math.random() < .5 ? null : new Integer((int)(Math.random() * Integer.MAX_VALUE));
				differentTypes[i][3] = Math.random() < .5 ? null : new Long((long)(Math.random() * Long.MAX_VALUE));
				differentTypes[i][4] = Math.random() < .5 ? null : new BigDecimal("19.95");
				differentTypes[i][5] = Math.random() < .5 ? null : new Float(3 + ((float)(Math.random())));
				differentTypes[i][6] = Math.random() < .5 ? null : new Double(3 + (Math.random()));
				differentTypes[i][7] = Math.random() < .5 ? null : randomString();
				differentTypes[i][8] = Math.random() < .5 ? null : randomString();
				differentTypes[i][9] = Math.random() < .5 ? null : randomString().getBytes();
				differentTypes[i][10] = Math.random() < .5 ? null : randomString().getBytes();
				differentTypes[i][11] = Math.random() < .5 ? null : new Timestamp(System.currentTimeMillis());
				differentTypes[i][12] = Math.random() < .5 ? null : new Time(System.currentTimeMillis());
				differentTypes[i][13] = Math.random() < .5 ? null : new Date(System.currentTimeMillis());
			}

			props.setProperty("useServerPrepStmts", j == 0 ? "true" : "false");
			props.setProperty("rewriteBatchedStatements", "true");
			props.setProperty("maxAllowedPacket", j == 0 ? "10240" : "1024");
			multiConn = getConnectionWithProps(props);
			pStmt = multiConn.prepareStatement("INSERT INTO rewriteBatchTypes(internalOrder,f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f11,f12,f13,f14) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

			for (int i = 0; i < 1000; i++) {
				pStmt.setInt(1, i);
				for (int k = 0; k < 14; k++) {
					if (k == 8) {
						String asString = (String)differentTypes[i][k];

						if (asString == null) {
							pStmt.setObject(k + 2, null);
						} else {
							pStmt.setCharacterStream(k + 2, new StringReader(asString), asString.length());
						}
					} else if (k == 9) {
						byte[] asBytes = (byte[])differentTypes[i][k];

						if (asBytes == null) {
							pStmt.setObject(k + 2, null);
						} else {
							pStmt.setBinaryStream(k + 2, new ByteArrayInputStream(asBytes), asBytes.length);
						}
					} else {
						pStmt.setObject(k + 2, differentTypes[i][k]);
					}
				}
				pStmt.addBatch();
			}

			pStmt.executeBatch();

			this.rs = this.stmt.executeQuery("SELECT f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14 FROM rewriteBatchTypes ORDER BY internalOrder");

			int idx = 0;

			// We need to format this ourselves, since we have to strip the nanos off of
			// TIMESTAMPs, so .equals() doesn't really work...

			SimpleDateFormat sdf = new SimpleDateFormat("''yyyy-MM-dd HH:mm:ss''", Locale.US);

			while (this.rs.next()) {
				for (int k = 0; k < 14; k++) {
					if (differentTypes[idx][k] == null) {
						assertTrue("On row " + idx + " expected NULL, found " + this.rs.getObject(k + 1)
								+ " in column " + (k + 1), this.rs.getObject(k + 1) == null);
					} else {
						String className = differentTypes[idx][k].getClass().getName();

						if (className.equals("java.io.StringReader")) {
							StringReader reader = (StringReader)differentTypes[idx][k];
							StringBuffer buf = new StringBuffer();

							int c = 0;

							while ((c = reader.read()) != -1) {
								buf.append((char)c);
							}

							String asString = this.rs.getString(k + 1);

							assertEquals("On row " + idx + ", column " + (k + 1), buf.toString(), asString);

						} else if (differentTypes[idx][k] instanceof java.io.InputStream) {
							ByteArrayOutputStream bOut = new ByteArrayOutputStream();

							int bytesRead = 0;

							byte[] buf = new byte[128];
							InputStream in = (InputStream)differentTypes[idx][k];

							while ((bytesRead = in.read(buf)) != -1) {
								bOut.write(buf, 0, bytesRead);
							}

							byte[] expected = bOut.toByteArray();
							byte[] actual = this.rs.getBytes(k + 1);

							assertEquals("On row " + idx + ", column " + (k + 1), StringUtils.dumpAsHex(expected, expected.length), StringUtils.dumpAsHex(actual, actual.length));
						} else if (differentTypes[idx][k] instanceof byte[]) {
							byte[] expected = (byte[])differentTypes[idx][k];
							byte[] actual = this.rs.getBytes(k + 1);
							assertEquals("On row " + idx + ", column " + (k + 1), StringUtils.dumpAsHex(expected, expected.length), StringUtils.dumpAsHex(actual, actual.length));
						} else if (differentTypes[idx][k] instanceof Timestamp) {
							assertEquals("On row " + idx + ", column " + (k + 1), sdf.format(differentTypes[idx][k]), sdf.format(this.rs.getObject(k + 1)));
						} else if (differentTypes[idx][k] instanceof Double) {
							assertEquals("On row " + idx + ", column " + (k + 1), ((Double)differentTypes[idx][k]).doubleValue(), this.rs.getDouble(k + 1), .1);
						} else if (differentTypes[idx][k] instanceof Float) {
							assertEquals("On row " + idx + ", column " + (k + 1), ((Float)differentTypes[idx][k]).floatValue(), this.rs.getFloat(k + 1), .1);
						} else if (className.equals("java.lang.Byte")) {
							// special mapping in JDBC for ResultSet.getObject()
							assertEquals("On row " + idx + ", column " + (k + 1), new Integer(((Byte)differentTypes[idx][k]).byteValue()), this.rs.getObject(k + 1));
						} else if (className.equals("java.lang.Short")) {
							// special mapping in JDBC for ResultSet.getObject()
							assertEquals("On row " + idx + ", column " + (k + 1), new Integer(((Short)differentTypes[idx][k]).shortValue()), this.rs.getObject(k + 1));
						} else {
							assertEquals("On row " + idx + ", column " + (k + 1) + " (" + differentTypes[idx][k].getClass() + "/" + this.rs.getObject(k + 1).getClass(), differentTypes[idx][k].toString(), this.rs.getObject(k + 1).toString());
						}
					}
				}

				idx++;
			}
		}
	}
	
	public void testBatchRewriteErrors() throws Exception {
		createTable("rewriteErrors", "(field1 int not null primary key) ENGINE=MyISAM");

		Properties props = new Properties();
		Connection multiConn = null;

		for (int j = 0; j < 2; j++) {
			props.setProperty("useServerPrepStmts", "false");
	
			if (j == 1) {
				props.setProperty("continueBatchOnError", "false");
			} else {
				props.setProperty("continueBatchOnError", "true");
			}
			
			props.setProperty("maxAllowedPacket", "4096");
			props.setProperty("rewriteBatchedStatements", "true");
			multiConn = getConnectionWithProps(props);
			this.pstmt = multiConn.prepareStatement("INSERT INTO rewriteErrors VALUES (?)");
			Statement multiStmt = multiConn.createStatement();
			
			for (int i = 0; i < 4096; i++) {
				multiStmt.addBatch("INSERT INTO rewriteErrors VALUES (" + i + ")");
				this.pstmt.setInt(1, i);
				this.pstmt.addBatch();
			}
			
			multiStmt.addBatch("INSERT INTO rewriteErrors VALUES (2048)");
			
			this.pstmt.setInt(1, 2048);
			this.pstmt.addBatch();
			
			try {
				this.pstmt.executeBatch();
			} catch (BatchUpdateException bUpE) {
				int[] counts = bUpE.getUpdateCounts();
	
				for (int i = 4059; i < counts.length; i++) {
					assertEquals(counts[i], Statement.EXECUTE_FAILED);
				}
				
				// this depends on max_allowed_packet, only a sanity check
				assertTrue(getRowCount("rewriteErrors") >= 4000);
			}
			
			this.stmt.execute("TRUNCATE TABLE rewriteErrors");
			
			try {
				multiStmt.executeBatch();
			} catch (BatchUpdateException bUpE) {
				int[] counts = bUpE.getUpdateCounts();
	
				for (int i = 4094; i < counts.length; i++) {
					assertEquals(counts[i], Statement.EXECUTE_FAILED);
				}
				
				// this depends on max_allowed_packet, only a sanity check
				assertTrue(getRowCount("rewriteErrors") >= 4000);
			}
			
			if (versionMeetsMinimum(5, 0)) {
				this.stmt.execute("TRUNCATE TABLE rewriteErrors");
				
				createProcedure("sp_rewriteErrors", "(param1 INT)\nBEGIN\nINSERT INTO rewriteErrors VALUES (param1);\nEND");
				
				CallableStatement cStmt = multiConn.prepareCall("{ CALL sp_rewriteErrors(?)}");
				
				for (int i = 0; i < 4096; i++) {
					cStmt.setInt(1, i);
					cStmt.addBatch();
				}
				
				cStmt.setInt(1, 2048);
				cStmt.addBatch();
				
				try {
					cStmt.executeBatch();
				} catch (BatchUpdateException bUpE) {
					int[] counts = bUpE.getUpdateCounts();
	
					for (int i = 4093; i < counts.length; i++) {
						assertEquals(counts[i], Statement.EXECUTE_FAILED);
					}
					
					// this depends on max_allowed_packet, only a sanity check
					assertTrue(getRowCount("rewriteErrors") >= 4000);
				}
			}
		}
	}

	public void testStreamChange() throws Exception {
		createTable("testStreamChange",
				"(field1 varchar(32), field2 int, field3 TEXT, field4 BLOB)");
		this.pstmt = this.conn
				.prepareStatement("INSERT INTO testStreamChange VALUES (?, ?, ?, ?)");

		try {
			this.pstmt.setString(1, "A");
			this.pstmt.setInt(2, 1);

			char[] cArray = { 'A', 'B', 'C' };
			Reader r = new CharArrayReader(cArray);
			this.pstmt.setCharacterStream(3, r, cArray.length);

			byte[] bArray = { 'D', 'E', 'F' };
			ByteArrayInputStream bais = new ByteArrayInputStream(bArray);
			this.pstmt.setBinaryStream(4, bais, bArray.length);

			assertEquals(1, this.pstmt.executeUpdate());

			this.rs = this.stmt
					.executeQuery("SELECT field3, field4 from testStreamChange where field1='A'");
			this.rs.next();
			assertEquals("ABC", this.rs.getString(1));
			assertEquals("DEF", this.rs.getString(2));

			char[] ucArray = { 'C', 'E', 'S', 'U' };
			this.pstmt.setString(1, "CESU");
			this.pstmt.setInt(2, 3);
			Reader ucReader = new CharArrayReader(ucArray);
			this.pstmt.setCharacterStream(3, ucReader, ucArray.length);
			this.pstmt.setBinaryStream(4, null, 0);
			assertEquals(1, this.pstmt.executeUpdate());

			this.rs = this.stmt
					.executeQuery("SELECT field3, field4 from testStreamChange where field1='CESU'");
			this.rs.next();
			assertEquals("CESU", this.rs.getString(1));
			assertEquals(null, this.rs.getString(2));
		} finally {
			if (this.rs != null) {
				this.rs.close();
				this.rs = null;
			}

			if (this.pstmt != null) {
				this.pstmt.close();
				this.pstmt = null;
			}
		}
	}

	/**
	 * DOCUMENT ME!
	 *
	 * @throws SQLException
	 *             DOCUMENT ME!
	 */
	public void testStubbed() throws SQLException {
		if (!isRunningOnJdk131()) {
			try {
				this.stmt.getResultSetHoldability();
			} catch (NotImplemented notImplEx) {
				;
			}
		}
	}

	public void testTruncationOnRead() throws Exception {
		this.rs = this.stmt.executeQuery("SELECT '" + Long.MAX_VALUE + "'");
		this.rs.next();

		try {
			this.rs.getByte(1);
			fail("Should've thrown an out-of-range exception");
		} catch (SQLException sqlEx) {
			assertTrue(SQLError.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE
					.equals(sqlEx.getSQLState()));
		}

		try {
			this.rs.getShort(1);
			fail("Should've thrown an out-of-range exception");
		} catch (SQLException sqlEx) {
			assertTrue(SQLError.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE
					.equals(sqlEx.getSQLState()));
		}

		try {
			this.rs.getInt(1);
			fail("Should've thrown an out-of-range exception");
		} catch (SQLException sqlEx) {
			assertTrue(SQLError.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE
					.equals(sqlEx.getSQLState()));
		}

		this.rs = this.stmt.executeQuery("SELECT '" + Double.MAX_VALUE + "'");

		this.rs.next();

		try {
			this.rs.getByte(1);
			fail("Should've thrown an out-of-range exception");
		} catch (SQLException sqlEx) {
			assertTrue(SQLError.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE
					.equals(sqlEx.getSQLState()));
		}

		try {
			this.rs.getShort(1);
			fail("Should've thrown an out-of-range exception");
		} catch (SQLException sqlEx) {
			assertTrue(SQLError.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE
					.equals(sqlEx.getSQLState()));
		}

		try {
			this.rs.getInt(1);
			fail("Should've thrown an out-of-range exception");
		} catch (SQLException sqlEx) {
			assertTrue(SQLError.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE
					.equals(sqlEx.getSQLState()));
		}

		try {
			this.rs.getLong(1);
			fail("Should've thrown an out-of-range exception");
		} catch (SQLException sqlEx) {
			assertTrue(SQLError.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE
					.equals(sqlEx.getSQLState()));
		}

		try {
			this.rs.getLong(1);
			fail("Should've thrown an out-of-range exception");
		} catch (SQLException sqlEx) {
			assertTrue(SQLError.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE
					.equals(sqlEx.getSQLState()));
		}

		PreparedStatement pStmt = null;

		System.out
				.println("Testing prepared statements with binary result sets now");

		try {
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS testTruncationOnRead");
			this.stmt
					.executeUpdate("CREATE TABLE testTruncationOnRead(intField INTEGER, bigintField BIGINT, doubleField DOUBLE)");
			this.stmt.executeUpdate("INSERT INTO testTruncationOnRead VALUES ("
					+ Integer.MAX_VALUE + ", " + Long.MAX_VALUE + ", "
					+ Double.MAX_VALUE + ")");
			this.stmt.executeUpdate("INSERT INTO testTruncationOnRead VALUES ("
					+ Integer.MIN_VALUE + ", " + Long.MIN_VALUE + ", "
					+ Double.MIN_VALUE + ")");

			pStmt = this.conn
					.prepareStatement("SELECT intField, bigintField, doubleField FROM testTruncationOnRead ORDER BY intField DESC");
			this.rs = pStmt.executeQuery();

			this.rs.next();

			try {
				this.rs.getByte(1);
				fail("Should've thrown an out-of-range exception");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE
						.equals(sqlEx.getSQLState()));
			}

			try {
				this.rs.getInt(2);
				fail("Should've thrown an out-of-range exception");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE
						.equals(sqlEx.getSQLState()));
			}

			try {
				this.rs.getLong(3);
				fail("Should've thrown an out-of-range exception");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_NUMERIC_VALUE_OUT_OF_RANGE
						.equals(sqlEx.getSQLState()));
			}
		} finally {
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS testTruncationOnRead");
		}

	}

	public void testStatementInterceptors() throws Exception {
		Connection interceptedConn = null;

		/*
		try {
			Properties props = new Properties();
			props.setProperty("statementInterceptors", "com.mysql.jdbc.interceptors.ResultSetScannerInterceptor");
			props.setProperty("resultSetScannerRegex", ".*");
			interceptedConn = getConnectionWithProps(props);
			this.rs = interceptedConn.createStatement().executeQuery("SELECT 'abc'");
			this.rs.next();
			this.rs.getString(1);
		} finally {
			closeMemberJDBCResources();

			if (interceptedConn != null) {
				interceptedConn.close();
			}
		}
		*/

		try {
			Properties props = new Properties();
			props.setProperty("statementInterceptors", "com.mysql.jdbc.interceptors.ServerStatusDiffInterceptor");

			interceptedConn = getConnectionWithProps(props);
			this.rs = interceptedConn.createStatement().executeQuery("SELECT 'abc'");
		} finally {
			if (interceptedConn != null) {
				interceptedConn.close();
			}
		}
	}

	public void testParameterBindings() throws Exception {
		// Need to check character set stuff, so need a new connection
		Connection utfConn = getConnectionWithProps("characterEncoding=utf-8,treatUtilDateAsTimestamp=false,autoDeserialize=true");

		java.util.Date now = new java.util.Date();

		Object[] valuesToTest = new Object[] {
				new Byte(Byte.MIN_VALUE),
				new Short(Short.MIN_VALUE),
				new Integer(Integer.MIN_VALUE),
				new Long(Long.MIN_VALUE),
				new Double(Double.MIN_VALUE),
				"\u4E2D\u6587",
				new BigDecimal(Math.PI),
				null, // to test isNull
				now // to test serialization
		};

		StringBuffer statementText = new StringBuffer("SELECT ?");

		for (int i = 1; i < valuesToTest.length; i++) {
			statementText.append(",?");
		}

		this.pstmt = utfConn.prepareStatement(statementText.toString());

		for (int i = 0; i < valuesToTest.length; i++) {
			this.pstmt.setObject(i + 1, valuesToTest[i]);
		}

		ParameterBindings bindings = ((com.mysql.jdbc.PreparedStatement)this.pstmt).getParameterBindings();

		for (int i = 0; i < valuesToTest.length; i++) {
			Object boundObject = bindings.getObject(i + 1);
			
			if (boundObject == null && valuesToTest[i] == null) {
				continue;
			}
			
			Class<?> boundObjectClass= boundObject.getClass();
			Class<?> testObjectClass = valuesToTest[i].getClass();
			
			if (boundObject instanceof Number) {
				assertEquals("For binding #" + (i + 1) + " of class " + boundObjectClass + " compared to " + testObjectClass, boundObject.toString(), valuesToTest[i].toString());
			} else if (boundObject instanceof Date) {
				
			} else {
				assertEquals("For binding #" + (i + 1) + " of class " + boundObjectClass + " compared to " + testObjectClass, boundObject, valuesToTest[i]);
			}
		}
	}

	public void testLocalInfileHooked() throws Exception {
	    createTable("localInfileHooked", "(field1 int, field2 varchar(255))");
	    String streamData = "1\tabcd\n2\tefgh\n3\tijkl";
	    InputStream stream = new ByteArrayInputStream(streamData.getBytes());
	    try {
	        ((com.mysql.jdbc.Statement) this.stmt).setLocalInfileInputStream(stream);
	        this.stmt.execute("LOAD DATA LOCAL INFILE 'bogusFileName' INTO TABLE localInfileHooked"+
	        		" CHARACTER SET " + CharsetMapping.getMysqlEncodingForJavaEncoding(((MySQLConnection)this.conn).getEncoding(), (com.mysql.jdbc.Connection) this.conn));
	        assertEquals(-1, stream.read());
	        this.rs = this.stmt.executeQuery("SELECT field2 FROM localInfileHooked ORDER BY field1 ASC");
	        this.rs.next();
	        assertEquals("abcd", this.rs.getString(1));
	        this.rs.next();
            assertEquals("efgh", this.rs.getString(1));
            this.rs.next();
            assertEquals("ijkl", this.rs.getString(1));
	    } finally {
	        ((com.mysql.jdbc.Statement) this.stmt).setLocalInfileInputStream(null);
	    }
	}
}
