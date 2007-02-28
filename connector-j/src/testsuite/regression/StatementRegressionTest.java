/*
 Copyright (C) 2002-2007 MySQL AB

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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.CharArrayReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.StringReader;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.BatchUpdateException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DataTruncation;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;

import testsuite.BaseTestCase;

import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.ServerPreparedStatement;
import com.mysql.jdbc.exceptions.MySQLTimeoutException;

/**
 * Regression tests for the Statement class
 * 
 * @author Mark Matthews
 */
public class StatementRegressionTest extends BaseTestCase {
	class PrepareThread extends Thread {
		Connection c;

		PrepareThread(Connection cn) {
			this.c = cn;
		}

		public void run() {
			for (int i = 0; i < 20; i++) // force this to end eventually
			{
				try {
					this.c.prepareStatement("SELECT 1");
					StatementRegressionTest.this.testServerPrepStmtDeadlockCounter++;
					Thread.sleep(400);
				} catch (SQLException sqlEx) {
					throw new RuntimeException(sqlEx);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}
	}

	static int count = 0;

	static int nextID = 1; // The next ID we expected to generate

	/*
	 * Each row in this table is to be converted into a single REPLACE
	 * statement. If the value is zero, a new record is to be created using then
	 * autoincrement feature. If the value is non-zero, the existing row of that
	 * value is to be replace with, obviously, the same key. I expect one
	 * Generated Key for each zero value - but I would accept one key for each
	 * value, with non-zero values coming back as themselves.
	 */
	static final int[][] tests = { { 0 }, // generate 1
			{ 1, 0, 0 }, // update 1, generate 2, 3
			{ 2, 0, 0, }, // update 2, generate 3, 4
	};

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(StatementRegressionTest.class);
	}

	private int testServerPrepStmtDeadlockCounter = 0;

	/**
	 * Constructor for StatementRegressionTest.
	 * 
	 * @param name
	 *            the name of the test to run
	 */
	public StatementRegressionTest(String name) {
		super(name);
	}

	private void addBatchItems(Statement statement, PreparedStatement pStmt,
			String tableName, int i) throws SQLException {
		pStmt.setString(1, "ps_batch_" + i);
		pStmt.setString(2, "ps_batch_" + i);
		pStmt.addBatch();

		statement.addBatch("INSERT INTO " + tableName
				+ " (strdata1, strdata2) VALUES " + "(\"s_batch_" + i
				+ "\",\"s_batch_" + i + "\")");
	}

	private void createGGKTables() throws Exception {
		// Delete and recreate table
		dropGGKTables();

		this.stmt.executeUpdate("CREATE TABLE testggk ("
				+ "id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,"
				+ "val INT NOT NULL" + ")");
	}

	private void doGGKTestPreparedStatement(int[] values, boolean useUpdate)
			throws Exception {
		// Generate the the multiple replace command
		StringBuffer cmd = new StringBuffer("REPLACE INTO testggk VALUES ");
		int newKeys = 0;

		for (int i = 0; i < values.length; i++) {
			cmd.append("(");

			if (values[i] == 0) {
				cmd.append("NULL");
				newKeys += 1;
			} else {
				cmd.append(values[i]);
			}

			cmd.append(", ");
			cmd.append(count++);
			cmd.append("), ");
		}

		cmd.setLength(cmd.length() - 2); // trim the final ", "

		// execute and print it
		System.out.println(cmd.toString());

		PreparedStatement pStmt = this.conn.prepareStatement(cmd.toString(),
				Statement.RETURN_GENERATED_KEYS);

		if (useUpdate) {
			pStmt.executeUpdate();
		} else {
			pStmt.execute();
		}

		// print out what actually happened
		System.out.println("Expect " + newKeys
				+ " generated keys, starting from " + nextID);

		this.rs = pStmt.getGeneratedKeys();
		StringBuffer res = new StringBuffer("Got keys");

		int[] generatedKeys = new int[newKeys];
		int i = 0;

		while (this.rs.next()) {
			if (i < generatedKeys.length) {
				generatedKeys[i] = this.rs.getInt(1);
			}

			i++;

			res.append(" " + this.rs.getInt(1));
		}

		int numberOfGeneratedKeys = i;

		assertTrue(
				"Didn't retrieve expected number of generated keys, expected "
						+ newKeys + ", found " + numberOfGeneratedKeys,
				numberOfGeneratedKeys == newKeys);
		assertTrue("Keys didn't start with correct sequence: ",
				generatedKeys[0] == nextID);

		System.out.println(res.toString());

		// Read and print the new state of the table
		this.rs = this.stmt.executeQuery("SELECT id, val FROM testggk");
		System.out.println("New table contents ");

		while (this.rs.next())
			System.out.println("Id " + this.rs.getString(1) + " val "
					+ this.rs.getString(2));

		// Tidy up
		System.out.println("");
		nextID += newKeys;
	}

	private void doGGKTestStatement(int[] values, boolean useUpdate)
			throws Exception {
		// Generate the the multiple replace command
		StringBuffer cmd = new StringBuffer("REPLACE INTO testggk VALUES ");
		int newKeys = 0;

		for (int i = 0; i < values.length; i++) {
			cmd.append("(");

			if (values[i] == 0) {
				cmd.append("NULL");
				newKeys += 1;
			} else {
				cmd.append(values[i]);
			}

			cmd.append(", ");
			cmd.append(count++);
			cmd.append("), ");
		}

		cmd.setLength(cmd.length() - 2); // trim the final ", "

		// execute and print it
		System.out.println(cmd.toString());

		if (useUpdate) {
			this.stmt.executeUpdate(cmd.toString(),
					Statement.RETURN_GENERATED_KEYS);
		} else {
			this.stmt.execute(cmd.toString(), Statement.RETURN_GENERATED_KEYS);
		}

		// print out what actually happened
		System.out.println("Expect " + newKeys
				+ " generated keys, starting from " + nextID);

		this.rs = this.stmt.getGeneratedKeys();
		StringBuffer res = new StringBuffer("Got keys");

		int[] generatedKeys = new int[newKeys];
		int i = 0;

		while (this.rs.next()) {
			if (i < generatedKeys.length) {
				generatedKeys[i] = this.rs.getInt(1);
			}

			i++;

			res.append(" " + this.rs.getInt(1));
		}

		int numberOfGeneratedKeys = i;

		assertTrue(
				"Didn't retrieve expected number of generated keys, expected "
						+ newKeys + ", found " + numberOfGeneratedKeys,
				numberOfGeneratedKeys == newKeys);
		assertTrue("Keys didn't start with correct sequence: ",
				generatedKeys[0] == nextID);

		System.out.println(res.toString());

		// Read and print the new state of the table
		this.rs = this.stmt.executeQuery("SELECT id, val FROM testggk");
		System.out.println("New table contents ");

		while (this.rs.next())
			System.out.println("Id " + this.rs.getString(1) + " val "
					+ this.rs.getString(2));

		// Tidy up
		System.out.println("");
		nextID += newKeys;
	}

	private void dropGGKTables() throws Exception {
		this.stmt.executeUpdate("DROP TABLE IF EXISTS testggk");
	}

	/**
	 * @param pStmt
	 * @param catId
	 * @throws SQLException
	 */
	private void execQueryBug5191(PreparedStatement pStmt, int catId)
			throws SQLException {
		pStmt.setInt(1, catId);

		this.rs = pStmt.executeQuery();

		assertTrue(this.rs.next());
		assertTrue(this.rs.next());
		// assertTrue(rs.next());

		assertFalse(this.rs.next());
	}

	private String getByteArrayString(byte[] ba) {
		StringBuffer buffer = new StringBuffer();
		if (ba != null) {
			for (int i = 0; i < ba.length; i++) {
				buffer.append("0x" + Integer.toHexString(ba[i] & 0xff) + " ");
			}
		} else {
			buffer.append("null");
		}
		return buffer.toString();
	}

	/**
	 * @param continueBatchOnError
	 * @throws SQLException
	 */
	private void innerBug6823(boolean continueBatchOnError) throws SQLException {
		Properties continueBatchOnErrorProps = new Properties();
		continueBatchOnErrorProps.setProperty("continueBatchOnError", String
				.valueOf(continueBatchOnError));
		this.conn = getConnectionWithProps(continueBatchOnErrorProps);
		Statement statement = this.conn.createStatement();

		String tableName = "testBug6823";

		createTable(tableName, "(id int not null primary key auto_increment,"
				+ " strdata1 varchar(255) not null, strdata2 varchar(255),"
				+ " UNIQUE INDEX (strdata1))");

		PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO "
				+ tableName + " (strdata1, strdata2) VALUES (?,?)");

		int c = 0;
		addBatchItems(statement, pStmt, tableName, ++c);
		addBatchItems(statement, pStmt, tableName, ++c);
		addBatchItems(statement, pStmt, tableName, ++c);
		addBatchItems(statement, pStmt, tableName, c); // duplicate entry
		addBatchItems(statement, pStmt, tableName, ++c);
		addBatchItems(statement, pStmt, tableName, ++c);

		int expectedUpdateCounts = continueBatchOnError ? 6 : 3;

		BatchUpdateException e1 = null;
		BatchUpdateException e2 = null;

		int[] updateCountsPstmt = null;
		try {
			updateCountsPstmt = pStmt.executeBatch();
		} catch (BatchUpdateException e) {
			e1 = e;
			updateCountsPstmt = e1.getUpdateCounts();
		}

		int[] updateCountsStmt = null;
		try {
			updateCountsStmt = statement.executeBatch();
		} catch (BatchUpdateException e) {
			e2 = e;
			updateCountsStmt = e1.getUpdateCounts();
		}

		assertNotNull(e1);
		assertNotNull(e2);

		assertEquals(expectedUpdateCounts, updateCountsPstmt.length);
		assertEquals(expectedUpdateCounts, updateCountsStmt.length);

		if (continueBatchOnError) {
			assertTrue(updateCountsPstmt[3] == Statement.EXECUTE_FAILED);
			assertTrue(updateCountsStmt[3] == Statement.EXECUTE_FAILED);
		}

		int psRows = 0;
		this.rs = this.stmt.executeQuery("SELECT * from " + tableName
				+ " WHERE strdata1 like \"ps_%\"");
		while (this.rs.next()) {
			psRows++;
		}
		assertTrue(psRows > 0);

		int sRows = 0;
		this.rs = this.stmt.executeQuery("SELECT * from " + tableName
				+ " WHERE strdata1 like \"s_%\"");
		while (this.rs.next()) {
			sRows++;
		}
		assertTrue(sRows > 0);

		assertTrue(psRows + "!=" + sRows, psRows == sRows);
	}

	/**
	 * Tests fix for BUG#10155, double quotes not recognized when parsing
	 * client-side prepared statements.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug10155() throws Exception {
		this.conn.prepareStatement(
				"SELECT \"Test question mark? Test single quote'\"")
				.executeQuery().close();
	}

	/**
	 * Tests fix for BUG#10630, Statement.getWarnings() fails with NPE if
	 * statement has been closed.
	 */
	public void testBug10630() throws Exception {
		Connection conn2 = null;
		Statement stmt2 = null;

		try {
			conn2 = getConnectionWithProps(null);
			stmt2 = conn2.createStatement();

			conn2.close();
			stmt2.getWarnings();
			fail("Should've caught an exception here");
		} catch (SQLException sqlEx) {
			assertEquals("08003", sqlEx.getSQLState());
		} finally {
			if (stmt2 != null) {
				stmt2.close();
			}

			if (conn2 != null) {
				conn2.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#11115, Varbinary data corrupted when using server-side
	 * prepared statements.
	 */
	public void testBug11115() throws Exception {
		String tableName = "testBug11115";

		if (versionMeetsMinimum(4, 1, 0)) {

			createTable(tableName,
					"(pwd VARBINARY(30)) TYPE=InnoDB DEFAULT CHARACTER SET utf8");

			byte[] bytesToTest = new byte[] { 17, 120, -1, -73, -5 };

			PreparedStatement insStmt = this.conn
					.prepareStatement("INSERT INTO " + tableName
							+ " (pwd) VALUES (?)");
			insStmt.setBytes(1, bytesToTest);
			insStmt.executeUpdate();

			this.rs = this.stmt.executeQuery("SELECT pwd FROM " + tableName);
			this.rs.next();

			byte[] fromDatabase = this.rs.getBytes(1);

			assertEquals(bytesToTest.length, fromDatabase.length);

			for (int i = 0; i < bytesToTest.length; i++) {
				assertEquals(bytesToTest[i], fromDatabase[i]);
			}

			this.rs = this.conn
					.prepareStatement("SELECT pwd FROM " + tableName)
					.executeQuery();
			this.rs.next();

			fromDatabase = this.rs.getBytes(1);

			assertEquals(bytesToTest.length, fromDatabase.length);

			for (int i = 0; i < bytesToTest.length; i++) {
				assertEquals(bytesToTest[i], fromDatabase[i]);
			}
		}
	}

	public void testBug11540() throws Exception {
		Locale originalLocale = Locale.getDefault();
		Connection thaiConn = null;
		Statement thaiStmt = null;
		PreparedStatement thaiPrepStmt = null;

		try {
			createTable("testBug11540", "(field1 DATE, field2 TIMESTAMP)");
			this.stmt
					.executeUpdate("INSERT INTO testBug11540 VALUES (NOW(), NOW())");
			Locale.setDefault(new Locale("th", "TH"));
			Properties props = new Properties();
			props.setProperty("jdbcCompliantTruncation", "false");

			thaiConn = getConnectionWithProps(props);
			thaiStmt = thaiConn.createStatement();

			this.rs = thaiStmt
					.executeQuery("SELECT field1, field2 FROM testBug11540");
			this.rs.next();

			Date origDate = this.rs.getDate(1);
			Timestamp origTimestamp = this.rs.getTimestamp(1);
			this.rs.close();

			thaiStmt.executeUpdate("TRUNCATE TABLE testBug11540");

			thaiPrepStmt = ((com.mysql.jdbc.Connection) thaiConn)
					.clientPrepareStatement("INSERT INTO testBug11540 VALUES (?,?)");
			thaiPrepStmt.setDate(1, origDate);
			thaiPrepStmt.setTimestamp(2, origTimestamp);
			thaiPrepStmt.executeUpdate();

			this.rs = thaiStmt
					.executeQuery("SELECT field1, field2 FROM testBug11540");
			this.rs.next();

			Date testDate = this.rs.getDate(1);
			Timestamp testTimestamp = this.rs.getTimestamp(1);
			this.rs.close();

			assertEquals(origDate, testDate);
			assertEquals(origTimestamp, testTimestamp);

		} finally {
			Locale.setDefault(originalLocale);
		}
	}

	/**
	 * Tests fix for BUG#11663, autoGenerateTestcaseScript uses bogus parameter
	 * names for server-side prepared statements.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug11663() throws Exception {
		if (versionMeetsMinimum(4, 1, 0)
				&& ((com.mysql.jdbc.Connection) this.conn)
						.getUseServerPreparedStmts()) {
			Connection testcaseGenCon = null;
			PrintStream oldErr = System.err;

			try {
				createTable("testBug11663", "(field1 int)");

				Properties props = new Properties();
				props.setProperty("autoGenerateTestcaseScript", "true");
				testcaseGenCon = getConnectionWithProps(props);
				ByteArrayOutputStream testStream = new ByteArrayOutputStream();
				PrintStream testErr = new PrintStream(testStream);
				System.setErr(testErr);
				this.pstmt = testcaseGenCon
						.prepareStatement("SELECT field1 FROM testBug11663 WHERE field1=?");
				this.pstmt.setInt(1, 1);
				this.pstmt.execute();
				System.setErr(oldErr);
				String testString = new String(testStream.toByteArray());

				int setIndex = testString.indexOf("SET @debug_stmt_param");
				int equalsIndex = testString.indexOf("=", setIndex);
				String paramName = testString.substring(setIndex + 4,
						equalsIndex);

				int usingIndex = testString.indexOf("USING " + paramName,
						equalsIndex);

				assertTrue(usingIndex != -1);
			} finally {
				System.setErr(oldErr);

				if (this.pstmt != null) {
					this.pstmt.close();
					this.pstmt = null;
				}

				if (testcaseGenCon != null) {
					testcaseGenCon.close();
				}

			}
		}
	}

	/**
	 * Tests fix for BUG#11798 - Pstmt.setObject(...., Types.BOOLEAN) throws
	 * exception.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug11798() throws Exception {
		if (isRunningOnJdk131()) {
			return; // test not valid on JDK-1.3.1
		}

		try {
			this.pstmt = this.conn.prepareStatement("SELECT ?");
			this.pstmt.setObject(1, Boolean.TRUE, Types.BOOLEAN);
			this.pstmt.setObject(1, new BigDecimal("1"), Types.BOOLEAN);
			this.pstmt.setObject(1, "true", Types.BOOLEAN);
		} finally {
			if (this.pstmt != null) {
				this.pstmt.close();
				this.pstmt = null;
			}
		}
	}

	/**
	 * Tests fix for BUG#13255 - Reconnect during middle of executeBatch()
	 * should not happen.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug13255() throws Exception {

		createTable("testBug13255", "(field_1 int)");

		Properties props = new Properties();
		props.setProperty("autoReconnect", "true");

		Connection reconnectConn = null;
		Statement reconnectStmt = null;
		PreparedStatement reconnectPStmt = null;

		try {
			reconnectConn = getConnectionWithProps(props);
			reconnectStmt = reconnectConn.createStatement();

			String connectionId = getSingleIndexedValueWithQuery(reconnectConn,
					1, "SELECT CONNECTION_ID()").toString();

			reconnectStmt.addBatch("INSERT INTO testBug13255 VALUES (1)");
			reconnectStmt.addBatch("INSERT INTO testBug13255 VALUES (2)");
			reconnectStmt.addBatch("KILL " + connectionId);

			for (int i = 0; i < 100; i++) {
				reconnectStmt.addBatch("INSERT INTO testBug13255 VALUES (" + i
						+ ")");
			}

			try {
				reconnectStmt.executeBatch();
			} catch (SQLException sqlEx) {
				// We expect this...we killed the connection
			}

			assertEquals(2, getRowCount("testBug13255"));

			this.stmt.executeUpdate("TRUNCATE TABLE testBug13255");

			reconnectConn.close();

			reconnectConn = getConnectionWithProps(props);

			connectionId = getSingleIndexedValueWithQuery(reconnectConn, 1,
					"SELECT CONNECTION_ID()").toString();

			reconnectPStmt = reconnectConn
					.prepareStatement("INSERT INTO testBug13255 VALUES (?)");
			reconnectPStmt.setInt(1, 1);
			reconnectPStmt.addBatch();
			reconnectPStmt.setInt(1, 2);
			reconnectPStmt.addBatch();
			reconnectPStmt.addBatch("KILL " + connectionId);

			for (int i = 3; i < 100; i++) {
				reconnectPStmt.setInt(1, i);
				reconnectPStmt.addBatch();
			}

			try {
				reconnectPStmt.executeBatch();
			} catch (SQLException sqlEx) {
				// We expect this...we killed the connection
			}

			assertEquals(2, getRowCount("testBug13255"));

		} finally {
			if (reconnectStmt != null) {
				reconnectStmt.close();
			}

			if (reconnectConn != null) {
				reconnectConn.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#15024 - Driver incorrectly closes streams passed as
	 * arguments to PreparedStatements.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug15024() throws Exception {
		createTable("testBug15024", "(field1 BLOB)");

		try {
			this.pstmt = this.conn
					.prepareStatement("INSERT INTO testBug15024 VALUES (?)");
			testStreamsForBug15024(false, false);

			Properties props = new Properties();
			props.setProperty("useConfigs", "3-0-Compat");

			Connection compatConn = null;

			try {
				compatConn = getConnectionWithProps(props);

				this.pstmt = compatConn
						.prepareStatement("INSERT INTO testBug15024 VALUES (?)");
				testStreamsForBug15024(true, false);
			} finally {
				if (compatConn != null) {
					compatConn.close();
				}
			}
		} finally {
			if (this.pstmt != null) {
				PreparedStatement toClose = this.pstmt;
				this.pstmt = null;

				toClose.close();
			}
		}
	}

	/**
	 * PreparedStatement should call EscapeProcessor.escapeSQL?
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug15141() throws Exception {
		try {
			createTable("testBug15141", "(field1 VARCHAR(32))");
			this.stmt.executeUpdate("INSERT INTO testBug15141 VALUES ('abc')");

			this.pstmt = this.conn
					.prepareStatement("select {d '1997-05-24'} FROM testBug15141");
			this.rs = this.pstmt.executeQuery();
			assertTrue(this.rs.next());
			assertEquals("1997-05-24", this.rs.getString(1));
			this.rs.close();
			this.rs = null;
			this.pstmt.close();
			this.pstmt = null;

			this.pstmt = ((com.mysql.jdbc.Connection) this.conn)
					.clientPrepareStatement("select {d '1997-05-24'} FROM testBug15141");
			this.rs = this.pstmt.executeQuery();
			assertTrue(this.rs.next());
			assertEquals("1997-05-24", this.rs.getString(1));
			this.rs.close();
			this.rs = null;
			this.pstmt.close();
			this.pstmt = null;
		} finally {
			if (this.rs != null) {
				ResultSet toCloseRs = this.rs;
				this.rs = null;
				toCloseRs.close();
			}

			if (this.pstmt != null) {
				PreparedStatement toClosePstmt = this.pstmt;
				this.pstmt = null;
				toClosePstmt.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#18041 - Server-side prepared statements don't cause
	 * truncation exceptions to be thrown.
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug18041() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			createTable("testBug18041", "(`a` tinyint(4) NOT NULL,"
					+ "`b` char(4) default NULL)");

			Properties props = new Properties();
			props.setProperty("jdbcCompliantTruncation", "true");
			props.setProperty("useServerPrepStmts", "true");

			Connection truncConn = null;
			PreparedStatement stm = null;

			try {
				truncConn = getConnectionWithProps(props);

				stm = truncConn
						.prepareStatement("insert into testBug18041 values (?,?)");
				stm.setInt(1, 1000);
				stm.setString(2, "nnnnnnnnnnnnnnnnnnnnnnnnnnnnnn");
				stm.executeUpdate();
				fail("Truncation exception should have been thrown");
			} catch (DataTruncation truncEx) {
				// we expect this
			} finally {
				if (this.stmt != null) {
					this.stmt.close();
				}

				if (truncConn != null) {
					truncConn.close();
				}
			}
		}
	}

	private void testStreamsForBug15024(boolean shouldBeClosedStream,
			boolean shouldBeClosedReader) throws SQLException {
		IsClosedInputStream bIn = new IsClosedInputStream(new byte[4]);
		IsClosedReader readerIn = new IsClosedReader("abcdef");

		this.pstmt.setBinaryStream(1, bIn, 4);
		this.pstmt.execute();
		assertEquals(shouldBeClosedStream, bIn.isClosed());

		this.pstmt.setCharacterStream(1, readerIn, 6);
		this.pstmt.execute();
		assertEquals(shouldBeClosedReader, readerIn.isClosed());

		this.pstmt.close();
	}

	class IsClosedReader extends StringReader {

		boolean isClosed = false;

		public IsClosedReader(String arg0) {
			super(arg0);
		}

		public void close() {
			super.close();

			this.isClosed = true;
		}

		public boolean isClosed() {
			return this.isClosed;
		}

	}

	class IsClosedInputStream extends ByteArrayInputStream {

		boolean isClosed = false;

		public IsClosedInputStream(byte[] arg0, int arg1, int arg2) {
			super(arg0, arg1, arg2);
		}

		public IsClosedInputStream(byte[] arg0) {
			super(arg0);
		}

		public void close() throws IOException {
			// TODO Auto-generated method stub
			super.close();
			this.isClosed = true;
		}

		public boolean isClosed() {
			return this.isClosed;
		}
	}

	/**
	 * Tests fix for BUG#1774 -- Truncated words after double quote
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug1774() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1774");
			this.stmt
					.executeUpdate("CREATE TABLE testBug1774 (field1 VARCHAR(255))");

			PreparedStatement pStmt = this.conn
					.prepareStatement("INSERT INTO testBug1774 VALUES (?)");

			String testString = "The word contains \" character";

			pStmt.setString(1, testString);
			pStmt.executeUpdate();

			this.rs = this.stmt.executeQuery("SELECT * FROM testBug1774");
			this.rs.next();
			assertEquals(this.rs.getString(1), testString);
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1774");
		}
	}

	/**
	 * Tests fix for BUG#1901 -- PreparedStatement.setObject(int, Object, int,
	 * int) doesn't support CLOB or BLOB types.
	 * 
	 * @throws Exception
	 *             if this test fails for any reason
	 */
	public void testBug1901() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1901");
			this.stmt
					.executeUpdate("CREATE TABLE testBug1901 (field1 VARCHAR(255))");
			this.stmt.executeUpdate("INSERT INTO testBug1901 VALUES ('aaa')");

			this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug1901");
			this.rs.next();

			Clob valueAsClob = this.rs.getClob(1);
			Blob valueAsBlob = this.rs.getBlob(1);

			PreparedStatement pStmt = this.conn
					.prepareStatement("INSERT INTO testBug1901 VALUES (?)");
			pStmt.setObject(1, valueAsClob, java.sql.Types.CLOB, 0);
			pStmt.executeUpdate();
			pStmt.setObject(1, valueAsBlob, java.sql.Types.BLOB, 0);
			pStmt.executeUpdate();
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1901");
		}
	}

	/**
	 * Test fix for BUG#1933 -- Driver property 'maxRows' has no effect.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug1933() throws Exception {
		if (versionMeetsMinimum(4, 0)) {
			Connection maxRowsConn = null;
			PreparedStatement maxRowsPrepStmt = null;
			Statement maxRowsStmt = null;

			try {
				Properties props = new Properties();

				props.setProperty("maxRows", "1");

				maxRowsConn = getConnectionWithProps(props);

				maxRowsStmt = maxRowsConn.createStatement();

				assertTrue(maxRowsStmt.getMaxRows() == 1);

				this.rs = maxRowsStmt.executeQuery("SELECT 1 UNION SELECT 2");

				this.rs.next();

				maxRowsPrepStmt = maxRowsConn
						.prepareStatement("SELECT 1 UNION SELECT 2");

				assertTrue(maxRowsPrepStmt.getMaxRows() == 1);

				this.rs = maxRowsPrepStmt.executeQuery();

				this.rs.next();

				assertTrue(!this.rs.next());

				props.setProperty("useServerPrepStmts", "false");

				maxRowsConn = getConnectionWithProps(props);

				maxRowsPrepStmt = maxRowsConn
						.prepareStatement("SELECT 1 UNION SELECT 2");

				assertTrue(maxRowsPrepStmt.getMaxRows() == 1);

				this.rs = maxRowsPrepStmt.executeQuery();

				this.rs.next();

				assertTrue(!this.rs.next());
			} finally {
				maxRowsConn.close();
			}
		}
	}

	/**
	 * Tests the fix for BUG#1934 -- prepareStatement dies silently when
	 * encountering Statement.RETURN_GENERATED_KEY
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug1934() throws Exception {
		if (isRunningOnJdk131()) {
			return; // test not valid on JDK-1.3.1
		}

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1934");
			this.stmt.executeUpdate("CREATE TABLE testBug1934 (field1 INT)");

			System.out.println("Before prepareStatement()");

			this.pstmt = this.conn.prepareStatement(
					"INSERT INTO testBug1934 VALUES (?)",
					java.sql.Statement.RETURN_GENERATED_KEYS);

			assertTrue(this.pstmt != null);

			System.out.println("After prepareStatement() - " + this.pstmt);
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1934");
		}
	}

	/**
	 * Tests fix for BUG#1958 - Improper bounds checking on
	 * PreparedStatement.setFoo().
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug1958() throws Exception {
		PreparedStatement pStmt = null;

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1958");
			this.stmt.executeUpdate("CREATE TABLE testBug1958 (field1 int)");

			pStmt = this.conn
					.prepareStatement("SELECT * FROM testBug1958 WHERE field1 IN (?, ?, ?)");

			try {
				pStmt.setInt(4, 1);
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx
						.getSQLState()));
			}
		} finally {
			if (pStmt != null) {
				pStmt.close();
			}

			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug1958");
		}
	}

	/**
	 * Tests the fix for BUG#2606, server-side prepared statements not returning
	 * datatype YEAR correctly.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug2606() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug2606");
			this.stmt
					.executeUpdate("CREATE TABLE testBug2606(year_field YEAR)");
			this.stmt.executeUpdate("INSERT INTO testBug2606 VALUES (2004)");

			PreparedStatement yrPstmt = this.conn
					.prepareStatement("SELECT year_field FROM testBug2606");

			this.rs = yrPstmt.executeQuery();

			assertTrue(this.rs.next());

			assertEquals(2004, this.rs.getInt(1));
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug2606");
		}
	}

	/**
	 * Tests the fix for BUG#2671, nulls encoded incorrectly in server-side
	 * prepared statements.
	 * 
	 * @throws Exception
	 *             if an error occurs.
	 */
	public void testBug2671() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			try {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS test3");
				this.stmt
						.executeUpdate("CREATE TABLE test3 ("
								+ " `field1` int(8) NOT NULL auto_increment,"
								+ " `field2` int(8) unsigned zerofill default NULL,"
								+ " `field3` varchar(30) binary NOT NULL default '',"
								+ " `field4` varchar(100) default NULL,"
								+ " `field5` datetime NULL default '0000-00-00 00:00:00',"
								+ " PRIMARY KEY  (`field1`),"
								+ " UNIQUE KEY `unq_id` (`field2`),"
								+ " UNIQUE KEY  (`field3`),"
								+ " UNIQUE KEY  (`field2`)"
								+ " ) TYPE=InnoDB CHARACTER SET utf8");

				this.stmt
						.executeUpdate("insert into test3 (field1, field3, field4) values (1,'blewis','Bob Lewis')");

				String query = "              " + "UPDATE                   "
						+ "  test3                  "
						+ "SET                      "
						+ "  field2=?               " + "  ,field3=?          "
						+ "  ,field4=?           " + "  ,field5=?        "
						+ "WHERE                    "
						+ "  field1 = ?                 ";

				java.sql.Date mydate = null;

				this.pstmt = this.conn.prepareStatement(query);

				this.pstmt.setInt(1, 13);
				this.pstmt.setString(2, "abc");
				this.pstmt.setString(3, "def");
				this.pstmt.setDate(4, mydate);
				this.pstmt.setInt(5, 1);

				int retval = this.pstmt.executeUpdate();
				assertTrue(retval == 1);
			} finally {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS test3");
			}
		}
	}

	/**
	 * Tests fix for BUG#3103 -- java.util.Date not accepted as parameter to
	 * PreparedStatement.setObject().
	 * 
	 * @throws Exception
	 *             if the test fails
	 * 
	 * @deprecated uses deprecated methods of Date class
	 */
	public void testBug3103() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3103");
			this.stmt
					.executeUpdate("CREATE TABLE testBug3103 (field1 DATETIME)");

			PreparedStatement pStmt = this.conn
					.prepareStatement("INSERT INTO testBug3103 VALUES (?)");

			java.util.Date utilDate = new java.util.Date();

			pStmt.setObject(1, utilDate);
			pStmt.executeUpdate();

			this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug3103");
			this.rs.next();

			java.util.Date retrUtilDate = new java.util.Date(this.rs
					.getTimestamp(1).getTime());

			// We can only compare on the day/month/year hour/minute/second
			// interval, because the timestamp has added milliseconds to the
			// internal date...
			assertTrue("Dates not equal", (utilDate.getMonth() == retrUtilDate
					.getMonth())
					&& (utilDate.getDate() == retrUtilDate.getDate())
					&& (utilDate.getYear() == retrUtilDate.getYear())
					&& (utilDate.getHours() == retrUtilDate.getHours())
					&& (utilDate.getMinutes() == retrUtilDate.getMinutes())
					&& (utilDate.getSeconds() == retrUtilDate.getSeconds()));
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3103");
		}
	}

	/**
	 * Tests fix for BUG#3520
	 * 
	 * @throws Exception
	 *             ...
	 */
	public void testBug3520() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS t");
			this.stmt.executeUpdate("CREATE TABLE t (s1 int,primary key (s1))");
			this.stmt.executeUpdate("INSERT INTO t VALUES (1)");
			this.stmt.executeUpdate("INSERT INTO t VALUES (1)");
		} catch (SQLException sqlEx) {
			System.out.println(sqlEx.getSQLState());
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS t");
		}
	}

	/**
	 * Test fix for BUG#3557 -- UpdatableResultSet not picking up default values
	 * 
	 * @throws Exception
	 *             if test fails.
	 */
	public void testBug3557() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3557");

			this.stmt.executeUpdate("CREATE TABLE testBug3557 ( "
					+ "`a` varchar(255) NOT NULL default 'XYZ', "
					+ "`b` varchar(255) default '123', "
					+ "PRIMARY KEY  (`a`))");

			Statement updStmt = this.conn
					.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
			this.rs = updStmt.executeQuery("SELECT * FROM testBug3557");

			assertTrue(this.rs.getConcurrency() == ResultSet.CONCUR_UPDATABLE);

			this.rs.moveToInsertRow();

			assertEquals("XYZ", this.rs.getObject(1));
			assertEquals("123", this.rs.getObject(2));
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3557");
		}
	}

	/**
	 * Tests fix for BUG#3620 -- Timezone not respected correctly.
	 * 
	 * @throws SQLException
	 *             if the test fails.
	 */
	public void testBug3620() throws SQLException {
		if (isRunningOnJRockit()) {
			// bug with their timezones
			return;
		}
		
		long epsillon = 3000; // 3 seconds time difference

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3620");
			this.stmt
					.executeUpdate("CREATE TABLE testBug3620 (field1 TIMESTAMP)");

			PreparedStatement tsPstmt = this.conn
					.prepareStatement("INSERT INTO testBug3620 VALUES (?)");

			Calendar pointInTime = Calendar.getInstance();
			pointInTime.set(2004, 02, 29, 10, 0, 0);

			long pointInTimeOffset = pointInTime.getTimeZone().getRawOffset();

			java.sql.Timestamp ts = new java.sql.Timestamp(pointInTime
					.getTime().getTime());

			tsPstmt.setTimestamp(1, ts);
			tsPstmt.executeUpdate();

			String tsValueAsString = getSingleValue("testBug3620", "field1",
					null).toString();

			System.out.println("Timestamp as string with no calendar: "
					+ tsValueAsString.toString());

			Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

			this.stmt.executeUpdate("DELETE FROM testBug3620");

			Properties props = new Properties();
			props.put("useTimezone", "true");
			// props.put("serverTimezone", "UTC");

			Connection tzConn = getConnectionWithProps(props);

			Statement tsStmt = tzConn.createStatement();

			tsPstmt = tzConn
					.prepareStatement("INSERT INTO testBug3620 VALUES (?)");

			tsPstmt.setTimestamp(1, ts, cal);
			tsPstmt.executeUpdate();

			tsValueAsString = getSingleValue("testBug3620", "field1", null)
					.toString();

			Timestamp tsValueAsTimestamp = (Timestamp) getSingleValue(
					"testBug3620", "field1", null);

			System.out.println("Timestamp as string with UTC calendar: "
					+ tsValueAsString.toString());
			System.out.println("Timestamp as Timestamp with UTC calendar: "
					+ tsValueAsTimestamp);

			this.rs = tsStmt.executeQuery("SELECT field1 FROM testBug3620");
			this.rs.next();

			Timestamp tsValueUTC = this.rs.getTimestamp(1, cal);

			//
			// We use this testcase with other vendors, JDBC spec
			// requires result set fields can only be read once,
			// although MySQL doesn't require this ;)
			//
			this.rs = tsStmt.executeQuery("SELECT field1 FROM testBug3620");
			this.rs.next();

			Timestamp tsValueStmtNoCal = this.rs.getTimestamp(1);

			System.out
					.println("Timestamp specifying UTC calendar from normal statement: "
							+ tsValueUTC.toString());

			PreparedStatement tsPstmtRetr = tzConn
					.prepareStatement("SELECT field1 FROM testBug3620");

			this.rs = tsPstmtRetr.executeQuery();
			this.rs.next();

			Timestamp tsValuePstmtUTC = this.rs.getTimestamp(1, cal);

			System.out
					.println("Timestamp specifying UTC calendar from prepared statement: "
							+ tsValuePstmtUTC.toString());

			//
			// We use this testcase with other vendors, JDBC spec
			// requires result set fields can only be read once,
			// although MySQL doesn't require this ;)
			//
			this.rs = tsPstmtRetr.executeQuery();
			this.rs.next();

			Timestamp tsValuePstmtNoCal = this.rs.getTimestamp(1);

			System.out
					.println("Timestamp specifying no calendar from prepared statement: "
							+ tsValuePstmtNoCal.toString());

			long stmtDeltaTWithCal = (ts.getTime() - tsValueStmtNoCal.getTime());

			long deltaOrig = Math.abs(stmtDeltaTWithCal - pointInTimeOffset);

			assertTrue(
					"Difference between original timestamp and timestamp retrieved using java.sql.Statement "
							+ "set in database using UTC calendar is not ~= "
							+ epsillon + ", it is actually " + deltaOrig,
					(deltaOrig < epsillon));

			long pStmtDeltaTWithCal = (ts.getTime() - tsValuePstmtNoCal
					.getTime());

			System.out
					.println(Math.abs(pStmtDeltaTWithCal - pointInTimeOffset)
							+ " < "
							+ epsillon
							+ (Math.abs(pStmtDeltaTWithCal - pointInTimeOffset) < epsillon));
			assertTrue(
					"Difference between original timestamp and timestamp retrieved using java.sql.PreparedStatement "
							+ "set in database using UTC calendar is not ~= "
							+ epsillon
							+ ", it is actually "
							+ pStmtDeltaTWithCal, (Math.abs(pStmtDeltaTWithCal
							- pointInTimeOffset) < epsillon));

			System.out
					.println("Difference between original ts and ts with no calendar: "
							+ (ts.getTime() - tsValuePstmtNoCal.getTime())
							+ ", offset should be " + pointInTimeOffset);
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3620");
		}
	}

	/**
	 * Tests that DataTruncation is thrown when data is truncated.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug3697() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3697");
			this.stmt
					.executeUpdate("CREATE TABLE testBug3697 (field1 VARCHAR(255))");

			StringBuffer updateBuf = new StringBuffer(
					"INSERT INTO testBug3697 VALUES ('");

			for (int i = 0; i < 512; i++) {
				updateBuf.append("A");
			}

			updateBuf.append("')");

			try {
				this.stmt.executeUpdate(updateBuf.toString());
			} catch (DataTruncation dtEx) {
				// This is an expected exception....
			}

			SQLWarning warningChain = this.stmt.getWarnings();

			System.out.println(warningChain);
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3697");
		}
	}

	/**
	 * Tests fix for BUG#3804, data truncation on server should throw
	 * DataTruncation exception.
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug3804() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			try {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3804");
				this.stmt
						.executeUpdate("CREATE TABLE testBug3804 (field1 VARCHAR(5))");

				boolean caughtTruncation = false;

				try {
					this.stmt
							.executeUpdate("INSERT INTO testBug3804 VALUES ('1234567')");
				} catch (DataTruncation truncationEx) {
					caughtTruncation = true;
					System.out.println(truncationEx);
				}

				assertTrue("Data truncation exception should've been thrown",
						caughtTruncation);
			} finally {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3804");
			}
		}
	}

	/**
	 * Tests BUG#3873 - PreparedStatement.executeBatch() not returning all
	 * generated keys (even though that's not JDBC compliant).
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug3873() throws Exception {
		if (isRunningOnJdk131()) {
			return; // test not valid on JDK-1.3.1
		}

		PreparedStatement batchStmt = null;

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3873");
			this.stmt
					.executeUpdate("CREATE TABLE testBug3873 (keyField INT NOT NULL PRIMARY KEY AUTO_INCREMENT, dataField VARCHAR(32))");
			batchStmt = this.conn.prepareStatement(
					"INSERT INTO testBug3873 (dataField) VALUES (?)",
					Statement.RETURN_GENERATED_KEYS);
			batchStmt.setString(1, "abc");
			batchStmt.addBatch();
			batchStmt.setString(1, "def");
			batchStmt.addBatch();
			batchStmt.setString(1, "ghi");
			batchStmt.addBatch();

			int[] updateCounts = batchStmt.executeBatch();

			this.rs = batchStmt.getGeneratedKeys();

			while (this.rs.next()) {
				System.out.println(this.rs.getInt(1));
			}

			this.rs = batchStmt.getGeneratedKeys();
			assertTrue(this.rs.next());
			assertTrue(1 == this.rs.getInt(1));
			assertTrue(this.rs.next());
			assertTrue(2 == this.rs.getInt(1));
			assertTrue(this.rs.next());
			assertTrue(3 == this.rs.getInt(1));
			assertTrue(!this.rs.next());
		} finally {
			if (batchStmt != null) {
				batchStmt.close();
			}

			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3873");
		}
	}

	/**
	 * Tests fix for BUG#4119 -- misbehavior in a managed environment from
	 * MVCSoft JDO
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug4119() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4119");
			this.stmt.executeUpdate("CREATE TABLE `testBug4119` ("
					+ "`field1` varchar(255) NOT NULL default '',"
					+ "`field2` bigint(20) default NULL,"
					+ "`field3` int(11) default NULL,"
					+ "`field4` datetime default NULL,"
					+ "`field5` varchar(75) default NULL,"
					+ "`field6` varchar(75) default NULL,"
					+ "`field7` varchar(75) default NULL,"
					+ "`field8` datetime default NULL,"
					+ " PRIMARY KEY  (`field1`)" + ")");

			PreparedStatement pStmt = this.conn
					.prepareStatement("insert into testBug4119 (field2, field3,"
							+ "field4, field5, field6, field7, field8, field1) values (?, ?,"
							+ "?, ?, ?, ?, ?, ?)");

			pStmt.setString(1, "0");
			pStmt.setString(2, "0");
			pStmt.setTimestamp(3, new java.sql.Timestamp(System
					.currentTimeMillis()));
			pStmt.setString(4, "ABC");
			pStmt.setString(5, "DEF");
			pStmt.setString(6, "AA");
			pStmt.setTimestamp(7, new java.sql.Timestamp(System
					.currentTimeMillis()));
			pStmt.setString(8, "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA");
			pStmt.executeUpdate();
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4119");
		}
	}

	/**
	 * Tests fix for BUG#4311 - Error in JDBC retrieval of mediumint column when
	 * using prepared statements and binary result sets.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug4311() throws Exception {
		try {
			int lowValue = -8388608;
			int highValue = 8388607;

			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4311");
			this.stmt
					.executeUpdate("CREATE TABLE testBug4311 (low MEDIUMINT, high MEDIUMINT)");
			this.stmt.executeUpdate("INSERT INTO testBug4311 VALUES ("
					+ lowValue + ", " + highValue + ")");

			PreparedStatement pStmt = this.conn
					.prepareStatement("SELECT low, high FROM testBug4311");
			this.rs = pStmt.executeQuery();
			assertTrue(this.rs.next());
			assertTrue(this.rs.getInt(1) == lowValue);
			assertTrue(this.rs.getInt(2) == highValue);
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4311");
		}
	}

	/**
	 * Tests fix for BUG#4510 -- Statement.getGeneratedKeys() fails when key >
	 * 32767
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug4510() throws Exception {
		if (isRunningOnJdk131()) {
			return; // test not valid on JDK-1.3.1
		}

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4510");
			this.stmt.executeUpdate("CREATE TABLE testBug4510 ("
					+ "field1 INT NOT NULL PRIMARY KEY AUTO_INCREMENT,"
					+ "field2 VARCHAR(100))");
			this.stmt
					.executeUpdate("INSERT INTO testBug4510 (field1, field2) VALUES (32767, 'bar')");

			PreparedStatement p = this.conn.prepareStatement(
					"insert into testBug4510 (field2) values (?)",
					Statement.RETURN_GENERATED_KEYS);

			p.setString(1, "blah");

			p.executeUpdate();

			ResultSet rs = p.getGeneratedKeys();
			rs.next();
			System.out.println("Id: " + rs.getInt(1));
			rs.close();
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4510");
		}
	}

	/**
	 * Server doesn't accept everything as a server-side prepared statement, so
	 * by default we scan for stuff it can't handle.
	 * 
	 * @throws SQLException
	 */
	public void testBug4718() throws SQLException {
		if (versionMeetsMinimum(4, 1, 0)
				&& ((com.mysql.jdbc.Connection) this.conn)
						.getUseServerPreparedStmts()) {
			this.pstmt = this.conn.prepareStatement("SELECT 1 LIMIT ?");
			assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

			this.pstmt = this.conn.prepareStatement("SELECT 1 LIMIT 1");
			assertTrue(this.pstmt instanceof com.mysql.jdbc.ServerPreparedStatement);

			this.pstmt = this.conn.prepareStatement("SELECT 1 LIMIT 1, ?");
			assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

			try {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4718");
				this.stmt
						.executeUpdate("CREATE TABLE testBug4718 (field1 char(32))");

				this.pstmt = this.conn
						.prepareStatement("ALTER TABLE testBug4718 ADD INDEX (field1)");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

				this.pstmt = this.conn.prepareStatement("SELECT 1");
				assertTrue(this.pstmt instanceof ServerPreparedStatement);

				this.pstmt = this.conn
						.prepareStatement("UPDATE testBug4718 SET field1=1");
				assertTrue(this.pstmt instanceof ServerPreparedStatement);

				this.pstmt = this.conn
						.prepareStatement("UPDATE testBug4718 SET field1=1 LIMIT 1");
				assertTrue(this.pstmt instanceof ServerPreparedStatement);

				this.pstmt = this.conn
						.prepareStatement("UPDATE testBug4718 SET field1=1 LIMIT ?");
				assertTrue(this.pstmt instanceof com.mysql.jdbc.PreparedStatement);

				this.pstmt = this.conn
						.prepareStatement("UPDATE testBug4718 SET field1='Will we ignore LIMIT ?,?'");
				assertTrue(this.pstmt instanceof ServerPreparedStatement);

			} finally {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4718");
			}
		}
	}

	/**
	 * Tests fix for BUG#5012 -- ServerPreparedStatements dealing with return of
	 * DECIMAL type don't work.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug5012() throws Exception {
		PreparedStatement pStmt = null;
		String valueAsString = "12345.12";

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5012");
			this.stmt
					.executeUpdate("CREATE TABLE testBug5012(field1 DECIMAL(10,2))");
			this.stmt.executeUpdate("INSERT INTO testBug5012 VALUES ("
					+ valueAsString + ")");

			pStmt = this.conn
					.prepareStatement("SELECT field1 FROM testBug5012");
			this.rs = pStmt.executeQuery();
			assertTrue(this.rs.next());
			assertEquals(new BigDecimal(valueAsString), this.rs
					.getBigDecimal(1));
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5012");

			if (pStmt != null) {
				pStmt.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#5133 -- PreparedStatement.toString() doesn't return
	 * correct value if no parameters are present in statement.
	 * 
	 * @throws Exception
	 */
	public void testBug5133() throws Exception {
		String query = "SELECT 1";
		String output = this.conn.prepareStatement(query).toString();
		System.out.println(output);

		assertTrue(output.indexOf(query) != -1);
	}

	/**
	 * Tests for BUG#5191 -- PreparedStatement.executeQuery() gives
	 * OutOfMemoryError
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug5191() throws Exception {
		PreparedStatement pStmt = null;

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5191Q");
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5191C");

			this.stmt.executeUpdate("CREATE TABLE testBug5191Q"
					+ "(QuestionId int NOT NULL AUTO_INCREMENT, "
					+ "Text VARCHAR(200), " + "PRIMARY KEY(QuestionId))");

			this.stmt.executeUpdate("CREATE TABLE testBug5191C"
					+ "(CategoryId int, " + "QuestionId int)");

			String[] questions = new String[] { "What is your name?",
					"What is your quest?",
					"What is the airspeed velocity of an unladen swollow?",
					"How many roads must a man walk?", "Where's the tea?", };

			for (int i = 0; i < questions.length; i++) {
				this.stmt.executeUpdate("INSERT INTO testBug5191Q(Text)"
						+ " VALUES (\"" + questions[i] + "\")");
				int catagory = (i < 3) ? 0 : i;

				this.stmt.executeUpdate("INSERT INTO testBug5191C"
						+ "(CategoryId, QuestionId) VALUES (" + catagory + ", "
						+ i + ")");
				/*
				 * this.stmt.executeUpdate("INSERT INTO testBug5191C" +
				 * "(CategoryId, QuestionId) VALUES (" + catagory + ", (SELECT
				 * testBug5191Q.QuestionId" + " FROM testBug5191Q " + "WHERE
				 * testBug5191Q.Text LIKE '" + questions[i] + "'))");
				 */
			}

			pStmt = this.conn.prepareStatement("SELECT qc.QuestionId, q.Text "
					+ "FROM testBug5191Q q, testBug5191C qc "
					+ "WHERE qc.CategoryId = ? "
					+ " AND q.QuestionId = qc.QuestionId");

			int catId = 0;
			for (int i = 0; i < 100; i++) {
				execQueryBug5191(pStmt, catId);
			}

		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5191Q");
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5191C");

			if (pStmt != null) {
				pStmt.close();
			}
		}
	}

	public void testBug5235() throws Exception {
		Properties props = new Properties();
		props.setProperty("zeroDateTimeBehavior", "convertToNull");

		Connection convertToNullConn = getConnectionWithProps(props);
		Statement convertToNullStmt = convertToNullConn.createStatement();
		try {
			convertToNullStmt.executeUpdate("DROP TABLE IF EXISTS testBug5235");
			convertToNullStmt
					.executeUpdate("CREATE TABLE testBug5235(field1 DATE)");
			convertToNullStmt
					.executeUpdate("INSERT INTO testBug5235 (field1) VALUES ('0000-00-00')");

			PreparedStatement ps = convertToNullConn
					.prepareStatement("SELECT field1 FROM testBug5235");
			this.rs = ps.executeQuery();

			if (this.rs.next()) {
				Date d = (Date) this.rs.getObject("field1");
				System.out.println("date: " + d);
			}
		} finally {
			convertToNullStmt.executeUpdate("DROP TABLE IF EXISTS testBug5235");
		}
	}

	public void testBug5450() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			String table = "testBug5450";
			String column = "policyname";

			try {
				Properties props = new Properties();
				props.setProperty("characterEncoding", "utf-8");

				Connection utf8Conn = getConnectionWithProps(props);
				Statement utfStmt = utf8Conn.createStatement();

				this.stmt.executeUpdate("DROP TABLE IF EXISTS " + table);

				this.stmt.executeUpdate("CREATE TABLE " + table
						+ "(policyid int NOT NULL AUTO_INCREMENT, " + column
						+ " VARCHAR(200), "
						+ "PRIMARY KEY(policyid)) DEFAULT CHARACTER SET utf8");

				String pname0 = "inserted \uac00 - foo - \u4e00";

				utfStmt.executeUpdate("INSERT INTO " + table + "(" + column
						+ ")" + " VALUES (\"" + pname0 + "\")");

				this.rs = utfStmt.executeQuery("SELECT " + column + " FROM "
						+ table);

				this.rs.first();
				String pname1 = this.rs.getString(column);

				assertEquals(pname0, pname1);
				byte[] bytes = this.rs.getBytes(column);

				String pname2 = new String(bytes, "utf-8");
				assertEquals(pname1, pname2);

				utfStmt.executeUpdate("delete from " + table + " where "
						+ column + " like 'insert%'");

				PreparedStatement s1 = utf8Conn.prepareStatement("insert into "
						+ table + "(" + column + ") values (?)");

				s1.setString(1, pname0);
				s1.executeUpdate();

				String byteesque = "byte " + pname0;
				byte[] newbytes = byteesque.getBytes("utf-8");

				s1.setBytes(1, newbytes);
				s1.executeUpdate();

				this.rs = utfStmt.executeQuery("select " + column + " from "
						+ table + " where " + column + " like 'insert%'");
				this.rs.first();
				String pname3 = this.rs.getString(column);
				assertEquals(pname0, pname3);

				this.rs = utfStmt.executeQuery("select " + column + " from "
						+ table + " where " + column + " like 'byte insert%'");
				this.rs.first();

				String pname4 = this.rs.getString(column);
				assertEquals(byteesque, pname4);

			} finally {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS " + table);
			}
		}
	}

	public void testBug5510() throws Exception {
		// This is a server bug that should be fixed by 4.1.6
		if (versionMeetsMinimum(4, 1, 6)) {
			try {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5510");

				this.stmt
						.executeUpdate("CREATE TABLE `testBug5510` ("
								+ "`a` bigint(20) NOT NULL auto_increment,"
								+ "`b` varchar(64) default NULL,"
								+ "`c` varchar(64) default NULL,"
								+ "`d` varchar(255) default NULL,"
								+ "`e` int(11) default NULL,"
								+ "`f` varchar(32) default NULL,"
								+ "`g` varchar(32) default NULL,"
								+ "`h` varchar(80) default NULL,"
								+ "`i` varchar(255) default NULL,"
								+ "`j` varchar(255) default NULL,"
								+ "`k` varchar(255) default NULL,"
								+ "`l` varchar(32) default NULL,"
								+ "`m` varchar(32) default NULL,"
								+ "`n` timestamp NOT NULL default CURRENT_TIMESTAMP on update"
								+ " CURRENT_TIMESTAMP,"
								+ "`o` int(11) default NULL,"
								+ "`p` int(11) default NULL,"
								+ "PRIMARY KEY  (`a`)"
								+ ") ENGINE=InnoDB DEFAULT CHARSET=latin1");
				PreparedStatement pStmt = this.conn
						.prepareStatement("INSERT INTO testBug5510 (a) VALUES (?)");
				pStmt.setNull(1, 0);
				pStmt.executeUpdate();

			} finally {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5510");
			}
		}
	}

	/**
	 * Tests fix for BUG#5874, timezone correction goes in wrong 'direction'
	 * when useTimezone=true and server timezone differs from client timezone.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug5874() throws Exception {
		/*
		try {
			String clientTimezoneName = "America/Los_Angeles";
			String serverTimezoneName = "America/Chicago";

			TimeZone.setDefault(TimeZone.getTimeZone(clientTimezoneName));

			long epsillon = 3000; // 3 seconds difference

			long clientTimezoneOffsetMillis = TimeZone.getDefault()
					.getRawOffset();
			long serverTimezoneOffsetMillis = TimeZone.getTimeZone(
					serverTimezoneName).getRawOffset();

			long offsetDifference = clientTimezoneOffsetMillis
					- serverTimezoneOffsetMillis;

			Properties props = new Properties();
			props.put("useTimezone", "true");
			props.put("serverTimezone", serverTimezoneName);

			Connection tzConn = getConnectionWithProps(props);
			Statement tzStmt = tzConn.createStatement();
			tzStmt.executeUpdate("DROP TABLE IF EXISTS timeTest");
			tzStmt
					.executeUpdate("CREATE TABLE timeTest (tstamp DATETIME, t TIME)");

			PreparedStatement pstmt = tzConn
					.prepareStatement("INSERT INTO timeTest VALUES (?, ?)");

			long now = System.currentTimeMillis(); // Time in milliseconds
			// since 1/1/1970 GMT

			Timestamp nowTstamp = new Timestamp(now);
			Time nowTime = new Time(now);

			pstmt.setTimestamp(1, nowTstamp);
			pstmt.setTime(2, nowTime);
			pstmt.executeUpdate();

			this.rs = tzStmt.executeQuery("SELECT * from timeTest");

			// Timestamps look like this: 2004-11-29 13:43:21
			SimpleDateFormat timestampFormat = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss");
			SimpleDateFormat timeFormat = new SimpleDateFormat("HH:mm:ss");

			while (this.rs.next()) {
				// Driver now converts/checks DATE/TIME/TIMESTAMP/DATETIME types
				// when calling getString()...
				String retrTimestampString = new String(this.rs.getBytes(1));
				Timestamp retrTimestamp = this.rs.getTimestamp(1);

				java.util.Date timestampOnServer = timestampFormat
						.parse(retrTimestampString);

				long retrievedOffsetForTimestamp = retrTimestamp.getTime()
						- timestampOnServer.getTime();

				assertTrue(
						"Difference between original timestamp and timestamp retrieved using client timezone is not "
								+ offsetDifference, (Math
								.abs(retrievedOffsetForTimestamp
										- offsetDifference) < epsillon));

				String retrTimeString = new String(this.rs.getBytes(2));
				Time retrTime = this.rs.getTime(2);

				java.util.Date timeOnServerAsDate = timeFormat
						.parse(retrTimeString);
				Time timeOnServer = new Time(timeOnServerAsDate.getTime());

				long retrievedOffsetForTime = retrTime.getTime()
						- timeOnServer.getTime();

				assertTrue(
						"Difference between original times and time retrieved using client timezone is not "
								+ offsetDifference,
						(Math.abs(retrievedOffsetForTime - offsetDifference) < epsillon));
			}
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS timeTest");
		} */
	}

	public void testBug6823() throws SQLException {
		innerBug6823(true);
		innerBug6823(false);
	}

	public void testBug7461() throws Exception {
		String tableName = "testBug7461";

		try {
			createTable(tableName, "(field1 varchar(4))");
			File tempFile = File.createTempFile("mysql-test", ".txt");
			tempFile.deleteOnExit();

			FileOutputStream fOut = new FileOutputStream(tempFile);
			fOut.write("abcdefghijklmnop".getBytes());
			fOut.close();

			try {
				this.stmt.executeQuery("LOAD DATA LOCAL INFILE '"
						+ tempFile.toString() + "' INTO TABLE " + tableName);
			} catch (SQLException sqlEx) {
				this.stmt.getWarnings();
			}

		} finally {
			dropTable(tableName);
		}

	}

	public void testBug8181() throws Exception {

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug8181");
			this.stmt
					.executeUpdate("CREATE TABLE testBug8181(col1 VARCHAR(20),col2 INT)");

			this.pstmt = this.conn
					.prepareStatement("INSERT INTO testBug8181(col1,col2) VALUES(?,?)");

			for (int i = 0; i < 20; i++) {
				this.pstmt.setString(1, "Test " + i);
				this.pstmt.setInt(2, i);
				this.pstmt.addBatch();
			}

			this.pstmt.executeBatch();

		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug8181");

			if (this.pstmt != null) {
				this.pstmt.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#8487 - PreparedStatements not creating streaming result
	 * sets.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug8487() throws Exception {
		try {
			this.pstmt = this.conn.prepareStatement("SELECT 1",
					ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			this.pstmt.setFetchSize(Integer.MIN_VALUE);
			this.rs = this.pstmt.executeQuery();
			try {
				this.conn.createStatement().executeQuery("SELECT 2");
				fail("Should have caught a streaming exception here");
			} catch (SQLException sqlEx) {
				assertTrue(sqlEx.getMessage() != null
						&& sqlEx.getMessage().indexOf("Streaming") != -1);
			}

		} finally {
			if (this.rs != null) {
				while (this.rs.next())
					;

				this.rs.close();
			}

			if (this.pstmt != null) {
				this.pstmt.close();
			}
		}
	}

	/**
	 * Tests multiple statement support with fix for BUG#9704.
	 * 
	 * @throws Exception
	 *             DOCUMENT ME!
	 */
	public void testBug9704() throws Exception {
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
	 * Tests that you can close a statement twice without an NPE.
	 * 
	 * @throws Exception
	 *             if an error occurs.
	 */
	public void testCloseTwice() throws Exception {
		Statement closeMe = this.conn.createStatement();
		closeMe.close();
		closeMe.close();
	}

	public void testCsc4194() throws Exception {
		if (isRunningOnJdk131()) {
			return; // test not valid on JDK-1.3.1
		}

		Connection sjisConn = null;
		Connection windows31JConn = null;

		try {
			String tableNameText = "testCsc4194Text";
			String tableNameBlob = "testCsc4194Blob";

			createTable(tableNameBlob, "(field1 BLOB)");
			String charset = "";

			if (versionMeetsMinimum(5, 0, 3) || versionMeetsMinimum(4, 1, 12)) {
				charset = " CHARACTER SET cp932";
			} else if (versionMeetsMinimum(4, 1, 0)) {
				charset = " CHARACTER SET sjis";
			}

			createTable(tableNameText, "(field1 TEXT)" + charset);

			Properties windows31JProps = new Properties();
			windows31JProps.setProperty("useUnicode", "true");
			windows31JProps.setProperty("characterEncoding", "Windows-31J");

			windows31JConn = getConnectionWithProps(windows31JProps);
			testCsc4194InsertCheckBlob(windows31JConn, tableNameBlob);

			if (versionMeetsMinimum(4, 1, 0)) {
				testCsc4194InsertCheckText(windows31JConn, tableNameText,
						"Windows-31J");
			}

			Properties sjisProps = new Properties();
			sjisProps.setProperty("useUnicode", "true");
			sjisProps.setProperty("characterEncoding", "sjis");

			sjisConn = getConnectionWithProps(sjisProps);
			testCsc4194InsertCheckBlob(sjisConn, tableNameBlob);

			if (versionMeetsMinimum(5, 0, 3)) {
				testCsc4194InsertCheckText(sjisConn, tableNameText,
						"Windows-31J");
			}

		} finally {

			if (windows31JConn != null) {
				windows31JConn.close();
			}

			if (sjisConn != null) {
				sjisConn.close();
			}
		}
	}

	private void testCsc4194InsertCheckBlob(Connection c, String tableName)
			throws Exception {
		byte[] bArray = new byte[] { (byte) 0xac, (byte) 0xed, (byte) 0x00,
				(byte) 0x05 };

		PreparedStatement testStmt = c.prepareStatement("INSERT INTO "
				+ tableName + " VALUES (?)");
		testStmt.setBytes(1, bArray);
		testStmt.executeUpdate();

		this.rs = c.createStatement().executeQuery(
				"SELECT field1 FROM " + tableName);
		assertTrue(this.rs.next());
		assertEquals(getByteArrayString(bArray), getByteArrayString(this.rs
				.getBytes(1)));
		this.rs.close();
	}

	private void testCsc4194InsertCheckText(Connection c, String tableName,
			String encoding) throws Exception {
		byte[] kabuInShiftJIS = { (byte) 0x87, // a double-byte
				// charater("kabu") in Shift JIS
				(byte) 0x8a, };

		String expected = new String(kabuInShiftJIS, encoding);
		PreparedStatement testStmt = c.prepareStatement("INSERT INTO "
				+ tableName + " VALUES (?)");
		testStmt.setString(1, expected);
		testStmt.executeUpdate();

		this.rs = c.createStatement().executeQuery(
				"SELECT field1 FROM " + tableName);
		assertTrue(this.rs.next());
		assertEquals(expected, this.rs.getString(1));
		this.rs.close();
	}

	/**
	 * Tests all forms of statements influencing getGeneratedKeys().
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testGetGeneratedKeysAllCases() throws Exception {
		if (isRunningOnJdk131()) {
			return; // test not valid on JDK-1.3.1
		}

		System.out.println("Using Statement.executeUpdate()\n");

		try {
			createGGKTables();

			// Do the tests
			for (int i = 0; i < tests.length; i++) {
				doGGKTestStatement(tests[i], true);
			}
		} finally {
			dropGGKTables();
		}

		nextID = 1;
		count = 0;

		System.out.println("Using Statement.execute()\n");

		try {
			createGGKTables();

			// Do the tests
			for (int i = 0; i < tests.length; i++) {
				doGGKTestStatement(tests[i], false);
			}
		} finally {
			dropGGKTables();
		}

		nextID = 1;
		count = 0;

		System.out.println("Using PreparedStatement.executeUpdate()\n");

		try {
			createGGKTables();

			// Do the tests
			for (int i = 0; i < tests.length; i++) {
				doGGKTestPreparedStatement(tests[i], true);
			}
		} finally {
			dropGGKTables();
		}

		nextID = 1;
		count = 0;

		System.out.println("Using PreparedStatement.execute()\n");

		try {
			createGGKTables();

			// Do the tests
			for (int i = 0; i < tests.length; i++) {
				doGGKTestPreparedStatement(tests[i], false);
			}
		} finally {
			dropGGKTables();
		}
	}

	/**
	 * Tests that max_rows and 'limit' don't cause exceptions to be thrown.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testLimitAndMaxRows() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testMaxRowsAndLimit");
			this.stmt
					.executeUpdate("CREATE TABLE testMaxRowsAndLimit(limitField INT)");

			for (int i = 0; i < 500; i++) {
				this.stmt
						.executeUpdate("INSERT INTO testMaxRowsAndLimit VALUES ("
								+ i + ")");
			}

			this.stmt.setMaxRows(250);
			this.stmt
					.executeQuery("SELECT limitField FROM testMaxRowsAndLimit");
		} finally {
			this.stmt.setMaxRows(0);

			this.stmt.executeUpdate("DROP TABLE IF EXISTS testMaxRowsAndLimit");
		}
	}

	/*
	 * public void testBug9595() throws Exception { double[] vals = new double[]
	 * {52.21, 52.22, 52.23, 52.24};
	 * 
	 * createTable("testBug9595", "(field1 DECIMAL(10,2), sortField INT)");
	 * 
	 * this.pstmt = this.conn.prepareStatement("INSERT INTO testBug9595 VALUES
	 * (?, ?)"); // Try setting as doubles for (int i = 0; i < vals.length; i++) {
	 * this.pstmt.setDouble(1, vals[i]); this.pstmt.setInt(2, i);
	 * this.pstmt.executeUpdate(); }
	 * 
	 * this.pstmt = this.conn.prepareStatement("SELECT field1 FROM testBug9595
	 * ORDER BY sortField"); this.rs = this.pstmt.executeQuery();
	 * 
	 * int i = 0;
	 * 
	 * while (this.rs.next()) { double valToTest = vals[i++];
	 * 
	 * assertEquals(this.rs.getDouble(1), valToTest, 0.001);
	 * assertEquals(this.rs.getBigDecimal(1).doubleValue(), valToTest, 0.001); }
	 * 
	 * this.pstmt = this.conn.prepareStatement("INSERT INTO testBug9595 VALUES
	 * (?, ?)");
	 * 
	 * this.stmt.executeUpdate("TRUNCATE TABLE testBug9595"); // Now, as
	 * BigDecimals for (i = 0; i < vals.length; i++) { BigDecimal foo = new
	 * BigDecimal(vals[i]);
	 * 
	 * this.pstmt.setObject(1, foo, Types.DECIMAL, 2); this.pstmt.setInt(2, i);
	 * this.pstmt.executeUpdate(); }
	 * 
	 * this.pstmt = this.conn.prepareStatement("SELECT field1 FROM testBug9595
	 * ORDER BY sortField"); this.rs = this.pstmt.executeQuery();
	 * 
	 * i = 0;
	 * 
	 * while (this.rs.next()) { double valToTest = vals[i++];
	 * System.out.println(this.rs.getString(1));
	 * assertEquals(this.rs.getDouble(1), valToTest, 0.001);
	 * assertEquals(this.rs.getBigDecimal(1).doubleValue(), valToTest, 0.001); } }
	 */

	/**
	 * Tests that 'LOAD DATA LOCAL INFILE' works
	 * 
	 * @throws Exception
	 *             if any errors occur
	 */
	public void testLoadData() throws Exception {
		try {
			int maxAllowedPacket = 1048576;

			this.stmt.executeUpdate("DROP TABLE IF EXISTS loadDataRegress");
			this.stmt
					.executeUpdate("CREATE TABLE loadDataRegress (field1 int, field2 int)");

			File tempFile = File.createTempFile("mysql", ".txt");

			// tempFile.deleteOnExit();
			System.out.println(tempFile);

			Writer out = new FileWriter(tempFile);

			int localCount = 0;
			int rowCount = 128; // maxAllowedPacket * 4;

			for (int i = 0; i < rowCount; i++) {
				out.write((localCount++) + "\t" + (localCount++) + "\n");
			}

			out.close();

			StringBuffer fileNameBuf = null;

			if (File.separatorChar == '\\') {
				fileNameBuf = new StringBuffer();

				String fileName = tempFile.getAbsolutePath();
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
				fileNameBuf = new StringBuffer(tempFile.getAbsolutePath());
			}

			int updateCount = this.stmt
					.executeUpdate("LOAD DATA LOCAL INFILE '"
							+ fileNameBuf.toString()
							+ "' INTO TABLE loadDataRegress");
			assertTrue(updateCount == rowCount);
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS loadDataRegress");
		}
	}

	public void testNullClob() throws Exception {
		createTable("testNullClob", "(field1 TEXT NULL)");

		PreparedStatement pStmt = null;

		try {
			pStmt = this.conn
					.prepareStatement("INSERT INTO testNullClob VALUES (?)");
			pStmt.setClob(1, null);
			pStmt.executeUpdate();
		} finally {
			if (pStmt != null) {
				pStmt.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#1658
	 * 
	 * @throws Exception
	 *             if the fix for parameter bounds checking doesn't work.
	 */
	public void testParameterBoundsCheck() throws Exception {
		try {
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS testParameterBoundsCheck");
			this.stmt
					.executeUpdate("CREATE TABLE testParameterBoundsCheck(f1 int, f2 int, f3 int, f4 int, f5 int)");

			PreparedStatement pstmt = this.conn
					.prepareStatement("UPDATE testParameterBoundsCheck SET f1=?, f2=?,f3=?,f4=? WHERE f5=?");

			pstmt.setString(1, "");
			pstmt.setString(2, "");

			try {
				pstmt.setString(25, "");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_ILLEGAL_ARGUMENT.equals(sqlEx
						.getSQLState()));
			}
		} finally {
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS testParameterBoundsCheck");
		}
	}

	public void testPStmtTypesBug() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testPStmtTypesBug");
			this.stmt
					.executeUpdate("CREATE TABLE testPStmtTypesBug(field1 INT)");
			this.pstmt = this.conn
					.prepareStatement("INSERT INTO testPStmtTypesBug VALUES (?)");
			this.pstmt.setObject(1, null, Types.INTEGER);
			this.pstmt.addBatch();
			this.pstmt.setInt(1, 1);
			this.pstmt.addBatch();
			this.pstmt.executeBatch();

		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testPStmtTypesBug");
		}
	}

	/**
	 * Tests fix for BUG#1511
	 * 
	 * @throws Exception
	 *             if the quoteid parsing fix in PreparedStatement doesn't work.
	 */
	public void testQuotedIdRecognition() throws Exception {
		if (!this.versionMeetsMinimum(4, 1)) {
			try {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testQuotedId");
				this.stmt
						.executeUpdate("CREATE TABLE testQuotedId (col1 VARCHAR(32))");

				PreparedStatement pStmt = this.conn
						.prepareStatement("SELECT * FROM testQuotedId WHERE col1='ABC`DEF' or col1=?");
				pStmt.setString(1, "foo");
				pStmt.execute();

				this.stmt.executeUpdate("DROP TABLE IF EXISTS testQuotedId2");
				this.stmt
						.executeUpdate("CREATE TABLE testQuotedId2 (`Works?` INT)");
				pStmt = this.conn
						.prepareStatement("INSERT INTO testQuotedId2 (`Works?`) VALUES (?)");
				pStmt.setInt(1, 1);
				pStmt.executeUpdate();
			} finally {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testQuotedId");
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testQuotedId2");
			}
		}
	}

	/**
	 * Tests for BUG#9288, parameter index out of range if LIKE, ESCAPE '\'
	 * present in query.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	/*
	 * public void testBug9288() throws Exception { String tableName =
	 * "testBug9288"; PreparedStatement pStmt = null;
	 * 
	 * try { createTable(tableName, "(field1 VARCHAR(32), field2 INT)"); pStmt =
	 * ((com.mysql.jdbc.Connection)this.conn).clientPrepareStatement( "SELECT
	 * COUNT(1) FROM " + tableName + " WHERE " + "field1 LIKE '%' ESCAPE '\\'
	 * AND " + "field2 > ?"); pStmt.setInt(1, 0);
	 * 
	 * this.rs = pStmt.executeQuery(); } finally { if (this.rs != null) {
	 * this.rs.close(); this.rs = null; }
	 * 
	 * if (pStmt != null) { pStmt.close(); } } }
	 */

	/*
	 * public void testBug10999() throws Exception { if (versionMeetsMinimum(5,
	 * 0, 5)) {
	 * 
	 * String tableName = "testBug10999"; String updateTrigName =
	 * "testBug10999Update"; String insertTrigName = "testBug10999Insert"; try {
	 * createTable(tableName, "(pkfield INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
	 * field1 VARCHAR(32))");
	 * 
	 * try { this.stmt.executeUpdate("DROP TRIGGER " + updateTrigName); } catch
	 * (SQLException sqlEx) { // ignore for now }
	 * 
	 * this.stmt.executeUpdate("CREATE TRIGGER " + updateTrigName + " AFTER
	 * UPDATE ON " + tableName + " FOR EACH ROW " + "BEGIN " + "END");
	 * 
	 * try { this.stmt.executeUpdate("DROP TRIGGER " + insertTrigName); } catch
	 * (SQLException sqlEx) { // ignore }
	 * 
	 * this.stmt.executeUpdate("CREATE TRIGGER " + insertTrigName + " AFTER
	 * INSERT ON " + tableName + " FOR EACH ROW " + " BEGIN " + "END");
	 * 
	 * this.conn.setAutoCommit(false);
	 * 
	 * String updateSQL = "INSERT INTO " + tableName + " (field1) VALUES
	 * ('abcdefg')"; int rowCount = this.stmt.executeUpdate(updateSQL,
	 * Statement.RETURN_GENERATED_KEYS);
	 * 
	 * this.rs = stmt.getGeneratedKeys(); if (rs.next()) {
	 * System.out.println(rs.getInt(1)); int id = rs.getInt(1); //if
	 * (log.isDebugEnabled()) // log.debug("Retrieved ID = " + id); } //else {
	 * //log.error("Can't retrieve ID with getGeneratedKeys."); // Retrieve ID
	 * using a SELECT statement instead. // querySQL = "SELECT id from tab1
	 * WHERE ...";
	 * 
	 * //if (log.isDebugEnabled()) // log.debug(querySQL);
	 * 
	 * //rs = stmt.executeQuery(querySQL); this.rs =
	 * this.stmt.executeQuery("SELECT pkfield FROM " + tableName); } finally {
	 * this.conn.setAutoCommit(true);
	 * 
	 * try { this.stmt.executeUpdate("DROP TRIGGER IF EXISTS " +
	 * insertTrigName); } catch (SQLException sqlEx) { // ignore }
	 * 
	 * try { this.stmt.executeUpdate("DROP TRIGGER IF EXISTS " +
	 * updateTrigName); } catch (SQLException sqlEx) { // ignore } } } }
	 */

	/**
	 * Tests that binary dates/times are encoded/decoded correctly.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 * 
	 * @deprecated because we need to use this particular constructor for the
	 *             date class, as Calendar-constructed dates don't pass the
	 *             .equals() test :(
	 */
	public void testServerPrepStmtAndDate() throws Exception {
		try {
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS testServerPrepStmtAndDate");
			this.stmt.executeUpdate("CREATE TABLE testServerPrepStmtAndDate("
					+ "`P_ID` int(10) NOT NULL default '0',"
					+ "`H_ID` int(10) NOT NULL default '0',"
					+ "`R_ID` int(10) NOT NULL default '0',"
					+ "`H_Age` int(10) default NULL,"
					+ "`R_Date` date NOT NULL default '0000-00-00',"
					+ "`Comments` varchar(255) default NULL,"
					+ "`Weight` int(10) default NULL,"
					+ "`HeadGear` char(1) NOT NULL default '',"
					+ "`FinPos` int(10) default NULL,"
					+ "`Jock_ID` int(10) default NULL,"
					+ "`BtnByPrev` double default NULL,"
					+ "`BtnByWinner` double default NULL,"
					+ "`Jock_All` int(10) default NULL,"
					+ "`Draw` int(10) default NULL,"
					+ "`SF` int(10) default NULL,"
					+ "`RHR` int(10) default NULL,"
					+ "`ORating` int(10) default NULL,"
					+ "`Odds` double default NULL,"
					+ "`RaceFormPlus` int(10) default NULL,"
					+ "`PrevPerform` int(10) default NULL,"
					+ "`TrainerID` int(10) NOT NULL default '0',"
					+ "`DaysSinceRun` int(10) default NULL,"
					+ "UNIQUE KEY `P_ID` (`P_ID`),"
					+ "UNIQUE KEY `R_H_ID` (`R_ID`,`H_ID`),"
					+ "KEY `R_Date` (`R_Date`)," + "KEY `H_Age` (`H_Age`),"
					+ "KEY `TrainerID` (`TrainerID`)," + "KEY `H_ID` (`H_ID`)"
					+ ")");

			Date dt = new java.sql.Date(102, 1, 2); // Note, this represents the
			// date 2002-02-02

			PreparedStatement pStmt2 = this.conn
					.prepareStatement("INSERT INTO testServerPrepStmtAndDate (P_ID, R_Date) VALUES (171576, ?)");
			pStmt2.setDate(1, dt);
			pStmt2.executeUpdate();
			pStmt2.close();

			this.rs = this.stmt
					.executeQuery("SELECT R_Date FROM testServerPrepStmtAndDate");
			this.rs.next();

			System.out.println("Date that was stored (as String) "
					+ this.rs.getString(1)); // comes back as 2002-02-02

			PreparedStatement pStmt = this.conn
					.prepareStatement("Select P_ID,R_Date from testServerPrepStmtAndDate Where R_Date = ?   and P_ID = 171576");
			pStmt.setDate(1, dt);

			this.rs = pStmt.executeQuery();

			assertTrue(this.rs.next());

			assertEquals("171576", this.rs.getString(1));

			assertEquals(dt, this.rs.getDate(2));
		} finally {
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS testServerPrepStmtAndDate");
		}
	}

	public void testServerPrepStmtDeadlock() throws Exception {

		Connection c = getConnectionWithProps(null);

		Thread testThread1 = new PrepareThread(c);
		Thread testThread2 = new PrepareThread(c);
		testThread1.start();
		testThread2.start();
		Thread.sleep(30000);
		assertTrue(this.testServerPrepStmtDeadlockCounter >= 10);
	}

	/**
	 * Tests PreparedStatement.setCharacterStream() to ensure it accepts > 4K
	 * streams
	 * 
	 * @throws Exception
	 *             if an error occurs.
	 */
	public void testSetCharacterStream() throws Exception {
		try {
			((com.mysql.jdbc.Connection) this.conn).setTraceProtocol(true);

			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS charStreamRegressTest");
			this.stmt
					.executeUpdate("CREATE TABLE charStreamRegressTest(field1 text)");

			this.pstmt = this.conn
					.prepareStatement("INSERT INTO charStreamRegressTest VALUES (?)");

			// char[] charBuf = new char[16384];
			char[] charBuf = new char[32];

			for (int i = 0; i < charBuf.length; i++) {
				charBuf[i] = 'A';
			}

			CharArrayReader reader = new CharArrayReader(charBuf);

			this.pstmt.setCharacterStream(1, reader, charBuf.length);
			this.pstmt.executeUpdate();

			this.rs = this.stmt
					.executeQuery("SELECT LENGTH(field1) FROM charStreamRegressTest");

			this.rs.next();

			System.out.println("Character stream length: "
					+ this.rs.getString(1));

			this.rs = this.stmt
					.executeQuery("SELECT field1 FROM charStreamRegressTest");

			this.rs.next();

			String result = this.rs.getString(1);

			assertTrue(result.length() == charBuf.length);

			this.stmt.execute("TRUNCATE TABLE charStreamRegressTest");

			// Test that EOF is not thrown
			reader = new CharArrayReader(charBuf);
			this.pstmt.clearParameters();
			this.pstmt.setCharacterStream(1, reader, charBuf.length);
			this.pstmt.executeUpdate();

			this.rs = this.stmt
					.executeQuery("SELECT LENGTH(field1) FROM charStreamRegressTest");

			this.rs.next();

			System.out.println("Character stream length: "
					+ this.rs.getString(1));

			this.rs = this.stmt
					.executeQuery("SELECT field1 FROM charStreamRegressTest");

			this.rs.next();

			result = this.rs.getString(1);

			assertTrue("Retrieved value of length " + result.length()
					+ " != length of inserted value " + charBuf.length, result
					.length() == charBuf.length);

			// Test single quotes inside identifers
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS `charStream'RegressTest`");
			this.stmt
					.executeUpdate("CREATE TABLE `charStream'RegressTest`(field1 text)");

			this.pstmt = this.conn
					.prepareStatement("INSERT INTO `charStream'RegressTest` VALUES (?)");

			reader = new CharArrayReader(charBuf);
			this.pstmt.setCharacterStream(1, reader, (charBuf.length * 2));
			this.pstmt.executeUpdate();

			this.rs = this.stmt
					.executeQuery("SELECT field1 FROM `charStream'RegressTest`");

			this.rs.next();

			result = this.rs.getString(1);

			assertTrue("Retrieved value of length " + result.length()
					+ " != length of inserted value " + charBuf.length, result
					.length() == charBuf.length);
		} finally {
			((com.mysql.jdbc.Connection) this.conn).setTraceProtocol(false);

			if (this.rs != null) {
				try {
					this.rs.close();
				} catch (Exception ex) {
					// ignore
				}

				this.rs = null;
			}

			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS `charStream'RegressTest`");
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS charStreamRegressTest");
		}
	}

	/**
	 * Tests a bug where Statement.setFetchSize() does not work for values other
	 * than 0 or Integer.MIN_VALUE
	 * 
	 * @throws Exception
	 *             if any errors occur
	 */
	public void testSetFetchSize() throws Exception {
		int oldFetchSize = this.stmt.getFetchSize();

		try {
			this.stmt.setFetchSize(10);
		} finally {
			this.stmt.setFetchSize(oldFetchSize);
		}
	}

	/**
	 * Tests fix for BUG#907
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void testSetMaxRows() throws Exception {
		Statement maxRowsStmt = null;

		try {
			maxRowsStmt = this.conn.createStatement();
			maxRowsStmt.setMaxRows(1);
			maxRowsStmt.executeQuery("SELECT 1");
		} finally {
			if (maxRowsStmt != null) {
				maxRowsStmt.close();
			}
		}
	}

	/**
	 * Tests for timestamp NPEs occuring in binary-format timestamps.
	 * 
	 * @throws Exception
	 *             DOCUMENT ME!
	 * 
	 * @deprecated yes, we know we are using deprecated methods here :)
	 */
	public void testTimestampNPE() throws Exception {
		try {
			Timestamp ts = new Timestamp(System.currentTimeMillis());

			this.stmt.executeUpdate("DROP TABLE IF EXISTS testTimestampNPE");
			this.stmt
					.executeUpdate("CREATE TABLE testTimestampNPE (field1 TIMESTAMP)");

			this.pstmt = this.conn
					.prepareStatement("INSERT INTO testTimestampNPE VALUES (?)");
			this.pstmt.setTimestamp(1, ts);
			this.pstmt.executeUpdate();

			this.pstmt = this.conn
					.prepareStatement("SELECT field1 FROM testTimestampNPE");

			this.rs = this.pstmt.executeQuery();

			this.rs.next();

			System.out.println(this.rs.getString(1));

			this.rs.getDate(1);

			Timestamp rTs = this.rs.getTimestamp(1);
			assertTrue("Retrieved year of " + rTs.getYear()
					+ " does not match " + ts.getYear(), rTs.getYear() == ts
					.getYear());
			assertTrue("Retrieved month of " + rTs.getMonth()
					+ " does not match " + ts.getMonth(), rTs.getMonth() == ts
					.getMonth());
			assertTrue("Retrieved date of " + rTs.getDate()
					+ " does not match " + ts.getDate(), rTs.getDate() == ts
					.getDate());
		} finally {
		}
	}

	public void testTruncationWithChar() throws Exception {
		try {
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS testTruncationWithChar");
			this.stmt
					.executeUpdate("CREATE TABLE testTruncationWithChar (field1 char(2))");

			this.pstmt = this.conn
					.prepareStatement("INSERT INTO testTruncationWithChar VALUES (?)");
			this.pstmt.setString(1, "00");
			this.pstmt.executeUpdate();
		} finally {
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS testTruncationWithChar");
		}
	}

	/**
	 * Tests fix for updatable streams being supported in updatable result sets.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testUpdatableStream() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS updateStreamTest");
			this.stmt
					.executeUpdate("CREATE TABLE updateStreamTest (keyField INT NOT NULL AUTO_INCREMENT PRIMARY KEY, field1 BLOB)");

			int streamLength = 16385;
			byte[] streamData = new byte[streamLength];

			/* create an updatable statement */
			Statement updStmt = this.conn.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);

			/* fill the resultset with some values */
			ResultSet updRs = updStmt
					.executeQuery("SELECT * FROM updateStreamTest");

			/* move to insertRow */
			updRs.moveToInsertRow();

			/* update the table */
			updRs.updateBinaryStream("field1", new ByteArrayInputStream(
					streamData), streamLength);

			updRs.insertRow();
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS updateStreamTest");
		}
	}

	/**
	 * Tests fix for BUG#15383 - PreparedStatement.setObject() serializes
	 * BigInteger as object, rather than sending as numeric value (and is thus
	 * not complementary to .getObject() on an UNSIGNED LONG type).
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug15383() throws Exception {
		createTable(
				"testBug15383",
				"(id INTEGER UNSIGNED NOT NULL "
						+ "AUTO_INCREMENT,value BIGINT UNSIGNED NULL DEFAULT 0,PRIMARY "
						+ "KEY(id))ENGINE=InnoDB;");

		this.stmt.executeUpdate("INSERT INTO testBug15383(value) VALUES(1)");

		Statement updatableStmt = this.conn.createStatement(
				ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

		try {
			this.rs = updatableStmt.executeQuery("SELECT * from testBug15383");

			assertTrue(this.rs.next());

			Object bigIntObj = this.rs.getObject("value");
			assertEquals("java.math.BigInteger", bigIntObj.getClass().getName());

			this.rs.updateObject("value", new BigInteger("3"));
			this.rs.updateRow();

			assertEquals("3", this.rs.getString("value"));
		} finally {
			if (this.rs != null) {
				ResultSet toClose = this.rs;
				this.rs = null;
				toClose.close();
			}

			if (updatableStmt != null) {
				updatableStmt.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#17099 - Statement.getGeneratedKeys() throws NPE when no
	 * query has been processed.
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug17099() throws Exception {
		if (isRunningOnJdk131()) {
			return; // test not valid
		}

		Statement newStmt = this.conn.createStatement();
		assertNotNull(newStmt.getGeneratedKeys());

		PreparedStatement pStmt = this.conn.prepareStatement("SELECT 1");
		assertNotNull(pStmt.getGeneratedKeys());

		if (versionMeetsMinimum(4, 1)) {
			pStmt = ((com.mysql.jdbc.Connection) this.conn)
					.clientPrepareStatement("SELECT 1");
			assertNotNull(pStmt.getGeneratedKeys());
		}
	}

	/**
	 * Tests fix for BUG#17587 - clearParameters() on a closed prepared
	 * statement causes NPE.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug17587() throws Exception {
		createTable("testBug17857", "(field1 int)");
		PreparedStatement pStmt = null;

		try {
			pStmt = this.conn
					.prepareStatement("INSERT INTO testBug17857 VALUES (?)");
			pStmt.close();
			try {
				pStmt.clearParameters();
			} catch (SQLException sqlEx) {
				assertEquals("08003", sqlEx.getSQLState());
			}

			pStmt = ((com.mysql.jdbc.Connection) this.conn)
					.clientPrepareStatement("INSERT INTO testBug17857 VALUES (?)");
			pStmt.close();
			try {
				pStmt.clearParameters();
			} catch (SQLException sqlEx) {
				assertEquals("08003", sqlEx.getSQLState());
			}

		} finally {
			if (pStmt != null) {
				pStmt.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#18740 - Data truncation and getWarnings() only returns
	 * last warning in set.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug18740() throws Exception {
		if (!versionMeetsMinimum(5, 0, 2)) {
			createTable("testWarnings", "(field1 smallint(6),"
					+ "field2 varchar(6)," + "UNIQUE KEY field1(field1))");

			try {
				this.stmt.executeUpdate("INSERT INTO testWarnings VALUES "
						+ "(10001, 'data1')," + "(10002, 'data2 foo'),"
						+ "(10003, 'data3')," + "(10004999, 'data4'),"
						+ "(10005, 'data5')");
			} catch (SQLException sqlEx) {
				String sqlStateToCompare = "01004";
				
				if (isJdbc4()) {
					sqlStateToCompare = "22001";
				}

				assertEquals(sqlStateToCompare, sqlEx.getSQLState());
				assertEquals(sqlStateToCompare, sqlEx.getNextException().getSQLState());

				SQLWarning sqlWarn = this.stmt.getWarnings();
				assertEquals("01000", sqlWarn.getSQLState());
				assertEquals("01000", sqlWarn.getNextWarning().getSQLState());
			}
		}
	}

	protected boolean isJdbc4() {
		boolean isJdbc4;
		
		try {
			Class.forName("java.sql.Wrapper");
			isJdbc4 = true;
		} catch (Throwable t) {
			isJdbc4 = false;
		}
		
		return isJdbc4;
	}

	/**
	 * Tests fix for BUG#19615, PreparedStatement.setObject(int, Object, int)
	 * doesn't respect scale of BigDecimals.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug19615() throws Exception {
		createTable("testBug19615", "(field1 DECIMAL(19, 12))");

		try {
			BigDecimal dec = new BigDecimal("1.234567");

			this.pstmt = this.conn
					.prepareStatement("INSERT INTO testBug19615 VALUES (?)");
			this.pstmt.setObject(1, dec, Types.DECIMAL);
			this.pstmt.executeUpdate();
			this.pstmt.close();

			this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug19615");
			this.rs.next();
			assertEquals(dec, this.rs.getBigDecimal(1).setScale(6));
			this.rs.close();
			this.stmt.executeUpdate("TRUNCATE TABLE testBug19615");

			this.pstmt = ((com.mysql.jdbc.Connection) this.conn)
					.clientPrepareStatement("INSERT INTO testBug19615 VALUES (?)");
			this.pstmt.setObject(1, dec, Types.DECIMAL);
			this.pstmt.executeUpdate();
			this.pstmt.close();

			this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug19615");
			this.rs.next();
			assertEquals(dec, this.rs.getBigDecimal(1).setScale(6));
			this.rs.close();
		} finally {
			closeMemberJDBCResources();
		}
	}
	
	/**
	 * Tests fix for BUG#20029 - NPE thrown from executeBatch().
	 * 
	 * @throws Exception
	 */
	public void testBug20029() throws Exception {
		createTable("testBug20029", ("(field1 int)"));
		
		long initialTimeout = 20; // may need to raise this depending on environment
		                          // we try and do this automatically in this testcase
		
		for (int i = 0; i < 10; i++) {
			final Connection toBeKilledConn = getConnectionWithProps(new Properties());
			final long timeout = initialTimeout;
			PreparedStatement toBeKilledPstmt = null;
			
			try {
				toBeKilledPstmt = ((com.mysql.jdbc.Connection)toBeKilledConn).clientPrepareStatement("INSERT INTO testBug20029 VALUES (?)");
				
				for (int j = 0; j < 1000; j++) {
					toBeKilledPstmt.setInt(1, j);
					toBeKilledPstmt.addBatch();
				}
				
				Thread t = new Thread() {
					public void run() {
						try {
							sleep(timeout);
							toBeKilledConn.close();
						} catch (Throwable t) {
							
						}
					}
				};
				
				t.start();
				
				try {
					if (toBeKilledConn.isClosed()) {
						initialTimeout *= 2;
						continue;
					}
					
					toBeKilledPstmt.executeBatch();
					fail("Should've caught a SQLException for the statement being closed here");
				} catch (BatchUpdateException batchEx) {
					assertEquals("08003", batchEx.getSQLState());
					break;
				}
				
				fail("Connection didn't close while in the middle of PreparedStatement.executeBatch()");
			} finally {
				if (toBeKilledPstmt != null) {
					toBeKilledPstmt.close();
				}
				
				if (toBeKilledConn != null) {
					toBeKilledConn.close();
				}
			}
		}
	}
	
	/**
	 * Fixes BUG#20687 - Can't pool server-side prepared statements, exception
	 * raised when re-using them.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug20687() throws Exception {
		if (!isRunningOnJdk131() && versionMeetsMinimum(5, 0)) {
			createTable("testBug20687", "(field1 int)");
			Connection poolingConn = null;
			
			Properties props = new Properties();
			props.setProperty("cachePrepStmts", "true");
			props.setProperty("useServerPrepStmts", "true");
			PreparedStatement pstmt1 = null;
			PreparedStatement pstmt2  = null;
			
			try {
				poolingConn = getConnectionWithProps(props);
				pstmt1 = poolingConn.prepareStatement("SELECT field1 FROM testBug20687");
				pstmt1.executeQuery();
				pstmt1.close();
				
				pstmt2 = poolingConn.prepareStatement("SELECT field1 FROM testBug20687");
				pstmt2.executeQuery();
				assertTrue(pstmt1 == pstmt2);
				pstmt2.close();
			} finally {
				if (pstmt1 != null) {
					pstmt1.close();
				}
				
				if (pstmt2 != null) {
					pstmt2.close();
				}
				
				if (poolingConn != null) {
					poolingConn.close();
				}
			}
		}
	}
	
	public void testLikeWithBackslashes() throws Exception {
		if (!versionMeetsMinimum(5, 0, 0)) {
			return;
		}

		Connection noBackslashEscapesConn = null;

		try {
			Properties props = new Properties();
			props.setProperty("sessionVariables",
					"sql_mode=NO_BACKSLASH_ESCAPES");

			noBackslashEscapesConn = getConnectionWithProps(props);

			createTable(
					"X_TEST",
					"(userName varchar(32) not null, ivalue integer, CNAME varchar(255), bvalue CHAR(1), svalue varchar(255), ACTIVE CHAR(1), primary key (userName))");

			String insert_sql = "insert into X_TEST (ivalue, CNAME, bvalue, svalue, ACTIVE, userName) values (?, ?, ?, ?, ?, ?)";

			this.pstmt = noBackslashEscapesConn.prepareStatement(insert_sql);
			this.pstmt.setInt(1, 0);
			this.pstmt.setString(2, "c:\\jetson");
			this.pstmt.setInt(3, 1);
			this.pstmt.setString(4, "c:\\jetson");
			this.pstmt.setInt(5, 1);
			this.pstmt.setString(6, "c:\\jetson");
			this.pstmt.execute();

			String select_sql = "select user0_.userName as userName0_0_, user0_.ivalue as ivalue0_0_, user0_.CNAME as CNAME0_0_, user0_.bvalue as bvalue0_0_, user0_.svalue as svalue0_0_, user0_.ACTIVE as ACTIVE0_0_ from X_TEST user0_ where user0_.userName like ?";
			this.pstmt = noBackslashEscapesConn.prepareStatement(select_sql);
			this.pstmt.setString(1, "c:\\j%");
			// if we comment out the previous line and uncomment the following, the like clause matches
			// stmt.setString(1,"c:\\\\j%");
			System.out.println("about to execute query " + select_sql);
			this.rs = this.pstmt.executeQuery();
			assertTrue(this.rs.next());
		} finally {
			closeMemberJDBCResources();

			if (noBackslashEscapesConn != null) {
				noBackslashEscapesConn.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#20650 - Statement.cancel() causes NullPointerException
     * if underlying connection has been closed due to server failure.
     * 
	 * @throws Exception if the test fails.
	 */
	public void testBug20650() throws Exception {
		Connection closedConn = null;
		Statement cancelStmt = null;
		
		try {
			closedConn = getConnectionWithProps(null);
			cancelStmt = closedConn.createStatement();
		
			closedConn.close();

			cancelStmt.cancel();
		} finally {
			if (cancelStmt != null) {
				cancelStmt.close();
			}
			
			if (closedConn != null && !closedConn.isClosed()) {
				closedConn.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#20888 - escape of quotes in client-side prepared 
	 * statements parsing not respected.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug20888() throws Exception {
		
		try {
			String s = "SELECT 'What do you think about D\\'Artanian''?', \"What do you think about D\\\"Artanian\"\"?\"";
			this.pstmt = ((com.mysql.jdbc.Connection)this.conn).clientPrepareStatement(s);
			
			this.rs = this.pstmt.executeQuery();
			this.rs.next();
			assertEquals(this.rs.getString(1), "What do you think about D'Artanian'?");
			assertEquals(this.rs.getString(2), "What do you think about D\"Artanian\"?");
		} finally {
			closeMemberJDBCResources();
		}
	}

	/**
	 * Tests Bug#21207 - Driver throws NPE when tracing prepared statements that
	 * have been closed (in asSQL()).
	 * 
	 * @throws Exception if the test fails
	 */
	public void testBug21207() throws Exception {
		try {
			this.pstmt = this.conn.prepareStatement("SELECT 1");
			this.pstmt.close();
			this.pstmt.toString(); // this used to cause an NPE
		} finally {
			closeMemberJDBCResources();
		}
	}

	/**
	 * Tests BUG#21438, server-side PS fails when using jdbcCompliantTruncation.
	 * If either is set to FALSE (&useServerPrepStmts=false or
	 * &jdbcCompliantTruncation=false) test succedes.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */

	public void testBug21438() throws Exception {
		createTable("testBug21438","(t_id int(10), test_date timestamp(30) NOT NULL,primary key t_pk (t_id));");		
		
		assertEquals(1, this.stmt.executeUpdate("insert into testBug21438 values (1,NOW());"));
		
		if (this.versionMeetsMinimum(4, 1)) {
			this.pstmt = ((com.mysql.jdbc.Connection)this.conn)
			.serverPrepare("UPDATE testBug21438 SET test_date=ADDDATE(?,INTERVAL 1 YEAR) WHERE t_id=1;");
	    	
			try {
	    		Timestamp ts = new Timestamp(System.currentTimeMillis());
	    		ts.setNanos(999999999);
	    		
	    		this.pstmt.setTimestamp(1, ts);	
	    	
	    		assertEquals(1, this.pstmt.executeUpdate());
	    		
	    		Timestamp future = (Timestamp)getSingleIndexedValueWithQuery(1, "SELECT test_date FROM testBug21438");
	    		assertEquals(future.getYear() - ts.getYear(), 1);
	
	    	} finally {
				closeMemberJDBCResources();
			}        
		}
	}

	/**
	 * Tests fix for BUG#22359 - Driver was using millis for
	 * Statement.setQueryTimeout() when spec says argument is
	 * seconds.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug22359() throws Exception {
		if (versionMeetsMinimum(5, 0)) {
			Statement timeoutStmt = null;
			
			try {
				timeoutStmt = this.conn.createStatement();
				timeoutStmt.setQueryTimeout(2);
				
				long begin = System.currentTimeMillis();
				
				try {
					timeoutStmt.execute("SELECT SLEEP(30)");
				} catch (MySQLTimeoutException timeoutEx) {
					long end = System.currentTimeMillis();
					
					assertTrue((end - begin) > 1000);
				}
			} finally {
				if (timeoutStmt != null) {
					timeoutStmt.close();
				}
			}
		}
	}
	
	/**
	 * Tests fix for BUG#22290 - Driver issues truncation on write exception when
	 * it shouldn't (due to sending big decimal incorrectly to server with
	 * server-side prepared statement).
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug22290() throws Exception {
		if (!versionMeetsMinimum(5, 0)) {
			return;
		}
		
		createTable(
				"testbug22290",
				"(`id` int(11) NOT NULL default '1',`cost` decimal(10,2) NOT NULL,PRIMARY KEY  (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8");
		assertEquals(
				this.stmt
						.executeUpdate("INSERT INTO testbug22290 (`id`,`cost`) VALUES (1,'1.00')"),
				1);

		Connection configuredConn = null;
		
		try {
			Properties props = new Properties();
			props.setProperty("sessionVariables", "sql_mode='STRICT_TRANS_TABLES'");
			
			
			configuredConn = getConnectionWithProps(props);
			
			this.pstmt = configuredConn
					.prepareStatement("update testbug22290 set cost = cost + ? where id = 1");
			this.pstmt.setBigDecimal(1, new BigDecimal("1.11"));
			assertEquals(this.pstmt.executeUpdate(), 1);
			
			assertEquals(this.stmt
					.executeUpdate("UPDATE testbug22290 SET cost='1.00'"), 1);
			this.pstmt = ((com.mysql.jdbc.Connection)configuredConn)
				.clientPrepareStatement("update testbug22290 set cost = cost + ? where id = 1");
			this.pstmt.setBigDecimal(1, new BigDecimal("1.11"));
			assertEquals(this.pstmt.executeUpdate(), 1);
		} finally {
			closeMemberJDBCResources();
			
			if (configuredConn != null) {
				configuredConn.close();
			}
		}
	}

	public void testClientPreparedSetBoolean() throws Exception {
		try {
			this.pstmt = ((com.mysql.jdbc.Connection)this.conn).clientPrepareStatement("SELECT ?");
			this.pstmt.setBoolean(1, false);
			assertEquals("SELECT 0", 
					this.pstmt.toString().substring(this.pstmt.toString().indexOf("SELECT")));
			this.pstmt.setBoolean(1, true);
			assertEquals("SELECT 1", 
					this.pstmt.toString().substring(this.pstmt.toString().indexOf("SELECT")));
		} finally {
			closeMemberJDBCResources();
		}
	}
	
	/**
	 * Tests fix for BUG#24360 .setFetchSize() breaks prepared 
	 * SHOW and other commands.
	 * 
	 * @throws Exception if the test fails
	 */
	public void testBug24360() throws Exception {
		if (!versionMeetsMinimum(5, 0)) {
			return;
		}
	
		Connection c = null;
		
		Properties props = new Properties();
		props.setProperty("useServerPrepStmts", "true");
		
		try {
			c = getConnectionWithProps(props);
			
			this.pstmt = c.prepareStatement("SHOW PROCESSLIST");
			this.pstmt.setFetchSize(5);
			this.pstmt.execute();
		} finally {
			closeMemberJDBCResources();
			
			if (c != null) {
				c.close();
			}
		}
	}
	
	/**
	 * Tests fix for BUG#24344 - useJDBCCompliantTimezoneShift with server-side prepared
	 * statements gives different behavior than when using client-side prepared
	 * statements. (this is now fixed if moving from server-side prepared statements
	 * to client-side prepared statements by setting "useSSPSCompatibleTimezoneShift" to
	 * "true", as the driver can't tell if this is a new deployment that never used 
	 * server-side prepared statements, or if it is an existing deployment that is
	 * switching to client-side prepared statements from server-side prepared statements.
	 * 
	 * @throws Exception if the test fails
	 */
	public void testBug24344() throws Exception {
		
		if (!versionMeetsMinimum(4, 1)) {
			return; // need SSPS
		}
		
		super.createTable("testBug24344", 
				"(i INT AUTO_INCREMENT, t1 DATETIME, PRIMARY KEY (i)) ENGINE = MyISAM");
		
		Connection conn2 = null;
		
		try {
			Properties props = new Properties();
			props.setProperty("useServerPrepStmts", "true");
			props.setProperty("useJDBCCompliantTimezoneShift", "true");
			conn2 = super.getConnectionWithProps(props);
			this.pstmt = conn2.prepareStatement("INSERT INTO testBug24344 (t1) VALUES (?)");
			Calendar c = Calendar.getInstance();
			this.pstmt.setTimestamp(1, new Timestamp(c.getTime().getTime()));
			this.pstmt.execute();
			this.pstmt.close();
			conn2.close();
			
			props.setProperty("useServerPrepStmts", "false");
			props.setProperty("useJDBCCompliantTimezoneShift", "true");
			props.setProperty("useSSPSCompatibleTimezoneShift", "true");
			
			conn2 = super.getConnectionWithProps(props);
			this.pstmt = conn2.prepareStatement("INSERT INTO testBug24344 (t1) VALUES (?)");
			this.pstmt.setTimestamp(1, new Timestamp(c.getTime().getTime()));
			this.pstmt.execute();
			this.pstmt.close();
			conn2.close();
			
			props.setProperty("useServerPrepStmts", "false");
			props.setProperty("useJDBCCompliantTimezoneShift", "false");
			props.setProperty("useSSPSCompatibleTimezoneShift", "false");
			conn2 = super.getConnectionWithProps(props);
			this.pstmt = conn2.prepareStatement("INSERT INTO testBug24344 (t1) VALUES (?)");
			this.pstmt.setTimestamp(1, new Timestamp(c.getTime().getTime()));
			this.pstmt.execute();
			this.pstmt.close();
			
			Statement s = conn2.createStatement();
			 this.rs = s.executeQuery("SELECT t1 FROM testBug24344 ORDER BY i ASC");
			
			 Timestamp[] dates = new Timestamp[3];
			
			int i = 0;
			
			while(rs.next()){
				dates[i++] = rs.getTimestamp(1);
			}
			
			assertEquals( "Number of rows should be 3.", 3, i);
			assertEquals(dates[0], dates[1]);
			assertTrue(!dates[1].equals(dates[2]));
		} finally {
			closeMemberJDBCResources();
			
			if (conn2 != null) {
				conn2.close();
			}
		}
	}
	
	/**
	 * Tests fix for BUG#25073 - rewriting batched statements leaks internal statement
	 * instances, and causes a memory leak.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug25073() throws Exception {
		if (isRunningOnJdk131()) {
			return;
		}
		
		Properties props = new Properties();
		props.setProperty("rewriteBatchedStatements", "true");
		Connection multiConn = getConnectionWithProps(props);
		createTable("testBug25073", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
		Statement multiStmt = multiConn.createStatement();
		multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (1)");
		multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (2)");
		multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (3)");
		multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (4)");
		multiStmt.addBatch("UPDATE testBug25073 SET field1=5 WHERE field1=1");
		multiStmt.addBatch("UPDATE testBug25073 SET field1=6 WHERE field1=2 OR field1=3");
		
		int beforeOpenStatementCount = ((com.mysql.jdbc.Connection)multiConn).getActiveStatementCount();
		
		multiStmt.executeBatch();
		
		int afterOpenStatementCount = ((com.mysql.jdbc.Connection)multiConn).getActiveStatementCount();
		
		assertEquals(beforeOpenStatementCount, afterOpenStatementCount);
		

		createTable("testBug25073", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
		props.clear();
		props.setProperty("rewriteBatchedStatements", "true");
		props.setProperty("sessionVariables", "max_allowed_packet=1024");
		multiConn = getConnectionWithProps(props);
		multiStmt = multiConn.createStatement();
		
		for (int i = 0; i < 1000; i++) {
			multiStmt.addBatch("INSERT INTO testBug25073(field1) VALUES (" + i + ")");
		}
		
		beforeOpenStatementCount = ((com.mysql.jdbc.Connection)multiConn).getActiveStatementCount();
		
		multiStmt.executeBatch();
		
		afterOpenStatementCount = ((com.mysql.jdbc.Connection)multiConn).getActiveStatementCount();
		
		assertEquals(beforeOpenStatementCount, afterOpenStatementCount);
		
		createTable("testBug25073", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
		
		props.clear();
		props.setProperty("useServerPrepStmts", "false");
		props.setProperty("rewriteBatchedStatements", "true");
		multiConn = getConnectionWithProps(props);
		PreparedStatement pStmt = multiConn.prepareStatement("INSERT INTO testBug25073(field1) VALUES (?)", 
				Statement.RETURN_GENERATED_KEYS);
		
		for (int i = 0; i < 1000; i++) {
			pStmt.setInt(1, i);
			pStmt.addBatch();
		}
		
		beforeOpenStatementCount = ((com.mysql.jdbc.Connection)multiConn).getActiveStatementCount();
		
		pStmt.executeBatch();
		
		afterOpenStatementCount = ((com.mysql.jdbc.Connection)multiConn).getActiveStatementCount();
		
		assertEquals(beforeOpenStatementCount, afterOpenStatementCount);
		
		createTable("testBug25073", "(pk_field INT PRIMARY KEY NOT NULL AUTO_INCREMENT, field1 INT)");
		props.setProperty("useServerPrepStmts", "false");
		props.setProperty("rewriteBatchedStatements", "true");
		props.setProperty("sessionVariables", "max_allowed_packet=1024");
		multiConn = getConnectionWithProps(props);
		pStmt = multiConn.prepareStatement("INSERT INTO testBug25073(field1) VALUES (?)", 
				Statement.RETURN_GENERATED_KEYS);
		
		for (int i = 0; i < 1000; i++) {
			pStmt.setInt(1, i);
			pStmt.addBatch();
		}
		
		beforeOpenStatementCount = ((com.mysql.jdbc.Connection)multiConn).getActiveStatementCount();
		
		pStmt.executeBatch();

		afterOpenStatementCount = ((com.mysql.jdbc.Connection)multiConn).getActiveStatementCount();
		
		assertEquals(beforeOpenStatementCount, afterOpenStatementCount);
	}
	
	/**
	 * Tests fix for BUG#25009 - Results from updates not handled correctly in multi-statement
	 * queries.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug25009() throws Exception {
		if (!versionMeetsMinimum(4, 1)) {
			return;
		}
		
		Properties props = new Properties();
		props.setProperty("allowMultiQueries", "true");
		
		Connection multiConn = getConnectionWithProps(props);
		createTable("testBug25009", "(field1 INT)");
		
		try {
			Statement multiStmt = multiConn.createStatement();
			multiStmt.execute("SELECT 1;SET @a=1; SET @b=2; SET @c=3; INSERT INTO testBug25009 VALUES (1)");
			
			assertEquals(-1, multiStmt.getUpdateCount());
			
			this.rs = multiStmt.getResultSet();
			assertTrue(this.rs.next());
			assertEquals(multiStmt.getMoreResults(), false);
			
			for (int i = 0; i < 3; i++) {
				assertEquals(0, multiStmt.getUpdateCount());
				assertEquals(multiStmt.getMoreResults(), false);
			}
			
			assertEquals(1, multiStmt.getUpdateCount());

			this.rs = multiStmt.executeQuery("SELECT field1 FROM testBug25009");
			assertTrue(this.rs.next());
			assertEquals(1, this.rs.getInt(1));
			
		} finally {
			closeMemberJDBCResources();
			
			if (multiConn != null) {
				multiConn.close();
			}
		}
	}
	
	/**
	 * Tests fix for BUG#25025 - Client-side prepared statement parser gets confused by
	 * in-line (slash-star) comments and therefore can't rewrite batched statements or
	 * reliably detect type of statements when they're used.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug25025() throws Exception {
		
		Connection multiConn = null;
		
		createTable("testBug25025", "(field1 INT)");
		
		try {
			Properties props = new Properties();
			props.setProperty("rewriteBatchedStatements", "true");
			props.setProperty("useServerPrepStmts", "false");
			
			multiConn = getConnectionWithProps(props);
			
			this.pstmt = multiConn.prepareStatement("/* insert foo.bar.baz INSERT INTO foo VALUES (?,?,?,?) to trick parser */ INSERT into testBug25025 VALUES (?)");
			this.pstmt.setInt(1, 1);
			this.pstmt.addBatch();
			this.pstmt.setInt(1, 2);
			this.pstmt.addBatch();
			this.pstmt.setInt(1, 3);
			this.pstmt.addBatch();
			
			int[] counts = this.pstmt.executeBatch();
			
			assertEquals(3, counts.length);
			assertEquals(1, counts[0]);
			assertEquals(1, counts[1]);
			assertEquals(1, counts[2]);
			assertEquals(true, 
					((com.mysql.jdbc.PreparedStatement)this.pstmt).canRewriteAsMultivalueInsertStatement());
		} finally {
			closeMemberJDBCResources();
			
			if (multiConn != null) {
				multiConn.close();
			}
		}
	}

	public void testBug25606() throws Exception {
		if (!versionMeetsMinimum(5, 0)) {
			return;
		}
		
		createTable("testtable", "(c0 int not null) engine=myisam");
		this.stmt
				.execute("alter table testtable add unique index testtable_i(c0)");

		Properties props = new Properties();
		props.setProperty("allowMultiQueries", "true");

		Connection multiConn = getConnectionWithProps(props);

		multiConn
				.createStatement()
				.execute(
						"create temporary table testtable_td(_batch int not null,c0 int not null)");
		multiConn.createStatement().execute(
				"create unique index testtable_td_b on testtable_td(_batch)");

		PreparedStatement ps;
		int num_changes = 0;

		ps = multiConn.prepareStatement("insert into testtable(c0) values(?)");
		while (num_changes < 10000) {
			ps.setInt(1, num_changes);

			ps.addBatch();

			num_changes++;

			if ((num_changes % 1000) == 0) {
				ps.executeBatch();
			}
		}

		ps.close();

		int num_deletes = 3646;

		int i = 0;
		ps = multiConn
				.prepareStatement("insert into testtable_td(_batch,c0) values(?,?)");
		while (i < num_deletes) {
			ps.setInt(1, i);
			ps.setInt(2, i);

			ps.addBatch();

			i++;

			if ((i % 1000) == 0) {
				ps.executeBatch();
			}
		}

		ps.executeBatch();

		ps.close();

		String sql = "lock tables testtable write;\n"
				+ "delete testtable from testtable_td force index(testtable_td_b) straight_join testtable on testtable.c0=testtable_td.c0 where testtable_td._batch>=? and testtable_td._batch<?;\n"
				+ "unlock tables;";

		ps = multiConn.prepareStatement(sql);

		int bsize = 100;

		for (int start_index = 0, end_index = Math.min(bsize, num_deletes); start_index < num_changes && start_index < 3646; start_index = end_index, end_index = Math
				.min(start_index + bsize, num_deletes)) {
			ps.clearParameters();
			ps.setInt(1, start_index);
			ps.setInt(2, end_index);
			ps.execute();

			//ignore the results from the "lock_tables"
			int c1 = ps.getUpdateCount();
			boolean b1 = ps.getMoreResults();

			//should always be the results from the "delete"
			int nrows_changed = ps.getUpdateCount();

			if (nrows_changed == -1)
				throw new SQLException("nrows_changed==-1"); //????

			bsize *= 4;
		}
	}
}
