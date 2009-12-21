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

import java.io.Reader;
import java.lang.reflect.Array;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;

import testsuite.BaseTestCase;

import com.mysql.jdbc.Messages;
import com.mysql.jdbc.MysqlDataTruncation;
import com.mysql.jdbc.NotUpdatable;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.StringUtils;
import com.mysql.jdbc.Util;
import com.mysql.jdbc.log.StandardLogger;

/**
 * Regression test cases for the ResultSet class.
 * 
 * @author Mark Matthews
 */
public class ResultSetRegressionTest extends BaseTestCase {
	/**
	 * Creates a new ResultSetRegressionTest
	 * 
	 * @param name
	 *            the name of the test to run
	 */
	public ResultSetRegressionTest(String name) {
		super(name);
	}

	/**
	 * Runs all test cases in this test suite
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		junit.textui.TestRunner.run(ResultSetRegressionTest.class);
	}

	/**
	 * Tests fix for BUG#???? -- Numeric types and server-side prepared
	 * statements incorrectly detect nulls.
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug2359() throws Exception {
		try {
			/*
			 * this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug2359");
			 * this.stmt.executeUpdate("CREATE TABLE testBug2359 (field1 INT)
			 * TYPE=InnoDB"); this.stmt.executeUpdate("INSERT INTO testBug2359
			 * VALUES (null), (1)");
			 * 
			 * this.pstmt = this.conn.prepareStatement("SELECT field1 FROM
			 * testBug2359 WHERE field1 IS NULL"); this.rs =
			 * this.pstmt.executeQuery();
			 * 
			 * assertTrue(this.rs.next());
			 * 
			 * assertTrue(this.rs.getByte(1) == 0);
			 * assertTrue(this.rs.wasNull());
			 * 
			 * assertTrue(this.rs.getShort(1) == 0);
			 * assertTrue(this.rs.wasNull());
			 * 
			 * assertTrue(this.rs.getInt(1) == 0);
			 * assertTrue(this.rs.wasNull());
			 * 
			 * assertTrue(this.rs.getLong(1) == 0);
			 * assertTrue(this.rs.wasNull());
			 * 
			 * assertTrue(this.rs.getFloat(1) == 0);
			 * assertTrue(this.rs.wasNull());
			 * 
			 * assertTrue(this.rs.getDouble(1) == 0);
			 * assertTrue(this.rs.wasNull());
			 * 
			 * assertTrue(this.rs.getBigDecimal(1) == null);
			 * assertTrue(this.rs.wasNull());
			 * 
			 * this.rs.close();
			 * 
			 * this.pstmt = this.conn.prepareStatement("SELECT max(field1) FROM
			 * testBug2359 WHERE field1 IS NOT NULL"); this.rs =
			 * this.pstmt.executeQuery(); assertTrue(this.rs.next());
			 * 
			 * assertTrue(this.rs.getByte(1) == 1);
			 * assertTrue(!this.rs.wasNull());
			 * 
			 * assertTrue(this.rs.getShort(1) == 1);
			 * assertTrue(!this.rs.wasNull());
			 * 
			 * assertTrue(this.rs.getInt(1) == 1);
			 * assertTrue(!this.rs.wasNull());
			 * 
			 * assertTrue(this.rs.getLong(1) == 1);
			 * assertTrue(!this.rs.wasNull());
			 * 
			 * assertTrue(this.rs.getFloat(1) == 1);
			 * assertTrue(!this.rs.wasNull());
			 * 
			 * assertTrue(this.rs.getDouble(1) == 1);
			 * assertTrue(!this.rs.wasNull());
			 * 
			 * assertTrue(this.rs.getBigDecimal(1) != null);
			 * assertTrue(!this.rs.wasNull());
			 * 
			 */
			createTable("testBug2359_1", "(id INT)", "InnoDB");
			this.stmt.executeUpdate("INSERT INTO testBug2359_1 VALUES (1)");

			this.pstmt = this.conn
					.prepareStatement("SELECT max(id) FROM testBug2359_1");
			this.rs = this.pstmt.executeQuery();

			if (this.rs.next()) {
				assertTrue(this.rs.getInt(1) != 0);
				this.rs.close();
			}

			this.rs.close();
		} finally {
			this.rs.close();
			this.pstmt.close();
		}
	}

	/**
	 * Tests fix for BUG#2643, ClassCastException when using this.rs.absolute()
	 * and server-side prepared statements.
	 * 
	 * @throws Exception
	 */
	public void testBug2623() throws Exception {
		PreparedStatement pStmt = null;

		try {
			pStmt = this.conn
					.prepareStatement("SELECT NOW()",
							ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_READ_ONLY);

			this.rs = pStmt.executeQuery();

			this.rs.absolute(1);
		} finally {
			if (this.rs != null) {
				this.rs.close();
			}

			this.rs = null;

			if (pStmt != null) {
				pStmt.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#2654, "Column 'column.table' not found" when "order by"
	 * in query"
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug2654() throws Exception {
		if (false) { // this is currently a server-level bug
			this.stmt.executeUpdate("DROP TABLE IF EXISTS foo");
			this.stmt.executeUpdate("DROP TABLE IF EXISTS bar");
	
			createTable("foo", "("
					+ "  id tinyint(3) default NULL,"
					+ "  data varchar(255) default NULL"
					+ ") DEFAULT CHARSET=latin1", "MyISAM ");
			this.stmt
					.executeUpdate("INSERT INTO foo VALUES (1,'male'),(2,'female')");
	
			createTable("bar", "("
					+ "id tinyint(3) unsigned default NULL,"
					+ "data char(3) default '0'"
					+ ") DEFAULT CHARSET=latin1", "MyISAM ");
	
			this.stmt
					.executeUpdate("INSERT INTO bar VALUES (1,'yes'),(2,'no')");
	
			String statement = "select foo.id, foo.data, "
					+ "bar.data from foo, bar" + "	where "
					+ "foo.id = bar.id order by foo.id";
	
			String column = "foo.data";
	
			this.rs = this.stmt.executeQuery(statement);
	
			ResultSetMetaData rsmd = this.rs.getMetaData();
			System.out.println(rsmd.getTableName(1));
			System.out.println(rsmd.getColumnName(1));
	
			this.rs.next();
	
			String fooData = this.rs.getString(column);
		}
	}

	/**
	 * Tests for fix to BUG#1130
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testClobTruncate() throws Exception {
		if (isRunningOnJdk131()) {
			return; // test not valid on JDK-1.3.1
		}

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testClobTruncate");
			this.stmt
					.executeUpdate("CREATE TABLE testClobTruncate (field1 TEXT)");
			this.stmt
					.executeUpdate("INSERT INTO testClobTruncate VALUES ('abcdefg')");

			this.rs = this.stmt.executeQuery("SELECT * FROM testClobTruncate");
			this.rs.next();

			Clob clob = this.rs.getClob(1);
			clob.truncate(3);

			Reader reader = clob.getCharacterStream();
			char[] buf = new char[8];
			int charsRead = reader.read(buf);

			String clobAsString = new String(buf, 0, charsRead);

			assertTrue(clobAsString.equals("abc"));
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testClobTruncate");
		}
	}

	/**
	 * Tests that streaming result sets are registered correctly.
	 * 
	 * @throws Exception
	 *             if any errors occur
	 */
	public void testClobberStreamingRS() throws Exception {
		try {
			Properties props = new Properties();
			props.setProperty("clobberStreamingResults", "true");
			
			Connection clobberConn = getConnectionWithProps(props);

			Statement clobberStmt = clobberConn.createStatement();

			clobberStmt.executeUpdate("DROP TABLE IF EXISTS StreamingClobber");
			clobberStmt
					.executeUpdate("CREATE TABLE StreamingClobber ( DUMMYID "
							+ " INTEGER NOT NULL, DUMMYNAME VARCHAR(32),PRIMARY KEY (DUMMYID) )");
			clobberStmt
					.executeUpdate("INSERT INTO StreamingClobber (DUMMYID, DUMMYNAME) VALUES (0, NULL)");
			clobberStmt
					.executeUpdate("INSERT INTO StreamingClobber (DUMMYID, DUMMYNAME) VALUES (1, 'nro 1')");
			clobberStmt
					.executeUpdate("INSERT INTO StreamingClobber (DUMMYID, DUMMYNAME) VALUES (2, 'nro 2')");
			clobberStmt
					.executeUpdate("INSERT INTO StreamingClobber (DUMMYID, DUMMYNAME) VALUES (3, 'nro 3')");

			Statement streamStmt = null;

			try {
				streamStmt = clobberConn.createStatement(
						java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY);
				streamStmt.setFetchSize(Integer.MIN_VALUE);

				this.rs = streamStmt.executeQuery("SELECT DUMMYID, DUMMYNAME "
						+ "FROM StreamingClobber ORDER BY DUMMYID");

				this.rs.next();

				// This should proceed normally, after the driver
				// clears the input stream
				clobberStmt.executeQuery("SHOW VARIABLES");
				this.rs.close();
			} finally {
				if (streamStmt != null) {
					streamStmt.close();
				}
			}
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS StreamingClobber");
		}
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @throws Exception
	 *             DOCUMENT ME!
	 */
	public void testEmptyResultSetGet() throws Exception {
		try {
			this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'foo'");
			System.out.println(this.rs.getInt(1));
		} catch (SQLException sqlEx) {
			assertTrue("Correct exception not thrown",
					SQLError.SQL_STATE_GENERAL_ERROR
							.equals(sqlEx.getSQLState()));
		}
	}

	/**
	 * Checks fix for BUG#1592 -- cross-database updatable result sets are not
	 * checked for updatability correctly.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testFixForBug1592() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			Statement updatableStmt = this.conn
					.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);

			try {
				updatableStmt.execute("SELECT * FROM mysql.user");

				this.rs = updatableStmt.getResultSet();
			} catch (SQLException sqlEx) {
				String message = sqlEx.getMessage();

				if ((message != null) && (message.indexOf("denied") != -1)) {
					System.err
							.println("WARN: Can't complete testFixForBug1592(), access to"
									+ " 'mysql' database not allowed");
				} else {
					throw sqlEx;
				}
			}
		}
	}

	/**
	 * Tests fix for BUG#2006, where 2 columns with same name in a result set
	 * are returned via findColumn() in the wrong order...The JDBC spec states,
	 * that the _first_ matching column should be returned.
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testFixForBug2006() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testFixForBug2006_1");
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testFixForBug2006_2");
			this.stmt
					.executeUpdate("CREATE TABLE testFixForBug2006_1 (key_field INT NOT NULL)");
			this.stmt
					.executeUpdate("CREATE TABLE testFixForBug2006_2 (key_field INT NULL)");
			this.stmt
					.executeUpdate("INSERT INTO testFixForBug2006_1 VALUES (1)");

			this.rs = this.stmt
					.executeQuery("SELECT testFixForBug2006_1.key_field, testFixForBug2006_2.key_field FROM testFixForBug2006_1 LEFT JOIN testFixForBug2006_2 USING(key_field)");

			ResultSetMetaData rsmd = this.rs.getMetaData();

			assertTrue(rsmd.getColumnName(1).equals(rsmd.getColumnName(2)));
			assertTrue(rsmd.isNullable(this.rs.findColumn("key_field")) == ResultSetMetaData.columnNoNulls);
			assertTrue(rsmd.isNullable(2) == ResultSetMetaData.columnNullable);
			assertTrue(this.rs.next());
			assertTrue(this.rs.getObject(1) != null);
			assertTrue(this.rs.getObject(2) == null);
		} finally {
			if (this.rs != null) {
				try {
					this.rs.close();
				} catch (SQLException sqlEx) {
					// ignore
				}

				this.rs = null;
			}

			this.stmt.executeUpdate("DROP TABLE IF EXISTS testFixForBug2006_1");
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testFixForBug2006_2");
		}
	}

	/**
	 * Tests that ResultSet.getLong() does not truncate values.
	 * 
	 * @throws Exception
	 *             if any errors occur
	 */
	public void testGetLongBug() throws Exception {
		this.stmt.executeUpdate("DROP TABLE IF EXISTS getLongBug");
		this.stmt
				.executeUpdate("CREATE TABLE IF NOT EXISTS getLongBug (int_col int, bigint_col bigint)");

		int intVal = 123456;
		long longVal1 = 123456789012345678L;
		long longVal2 = -2079305757640172711L;
		this.stmt.executeUpdate("INSERT INTO getLongBug "
				+ "(int_col, bigint_col) " + "VALUES (" + intVal + ", "
				+ longVal1 + "), " + "(" + intVal + ", " + longVal2 + ")");

		try {
			this.rs = this.stmt
					.executeQuery("SELECT int_col, bigint_col FROM getLongBug ORDER BY bigint_col DESC");
			this.rs.next();
			assertTrue(
					"Values not decoded correctly",
					((this.rs.getInt(1) == intVal) && (this.rs.getLong(2) == longVal1)));
			this.rs.next();
			assertTrue(
					"Values not decoded correctly",
					((this.rs.getInt(1) == intVal) && (this.rs.getLong(2) == longVal2)));
		} finally {
			if (this.rs != null) {
				try {
					this.rs.close();
				} catch (Exception ex) {
					// ignore
				}
			}

			this.stmt.executeUpdate("DROP TABLE IF EXISTS getLongBug");
		}
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @throws Exception
	 *             DOCUMENT ME!
	 */
	public void testGetTimestampWithDate() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testGetTimestamp");
			this.stmt.executeUpdate("CREATE TABLE testGetTimestamp (d date)");
			this.stmt
					.executeUpdate("INSERT INTO testGetTimestamp values (now())");

			this.rs = this.stmt.executeQuery("SELECT * FROM testGetTimestamp");
			this.rs.next();
			System.out.println(this.rs.getTimestamp(1));
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testGetTimestamp");
		}
	}

	/**
	 * Tests a bug where ResultSet.isBefireFirst() would return true when the
	 * result set was empty (which is incorrect)
	 * 
	 * @throws Exception
	 *             if an error occurs.
	 */
	public void testIsBeforeFirstOnEmpty() throws Exception {
		try {
			// Query with valid rows: isBeforeFirst() correctly returns True
			this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'version'");
			assertTrue("Non-empty search should return true", this.rs
					.isBeforeFirst());

			// Query with empty result: isBeforeFirst() falsely returns True
			// Sun's documentation says it should return false
			this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'garbage'");
			assertTrue("Empty search should return false ", !this.rs
					.isBeforeFirst());
		} finally {
			this.rs.close();
		}
	}

	/**
	 * Tests a bug where ResultSet.isBefireFirst() would return true when the
	 * result set was empty (which is incorrect)
	 * 
	 * @throws Exception
	 *             if an error occurs.
	 */
	public void testMetaDataIsWritable() throws Exception {
		try {
			// Query with valid rows
			this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'version'");

			ResultSetMetaData rsmd = this.rs.getMetaData();

			int numColumns = rsmd.getColumnCount();

			for (int i = 1; i <= numColumns; i++) {
				assertTrue("rsmd.isWritable() should != rsmd.isReadOnly()",
						rsmd.isWritable(i) != rsmd.isReadOnly(i));
			}
		} finally {
			this.rs.close();
		}
	}

	/**
	 * Tests fix for bug # 496
	 * 
	 * @throws Exception
	 *             if an error happens.
	 */
	public void testNextAndPrevious() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testNextAndPrevious");
			this.stmt
					.executeUpdate("CREATE TABLE testNextAndPrevious (field1 int)");
			this.stmt
					.executeUpdate("INSERT INTO testNextAndPrevious VALUES (1)");

			this.rs = this.stmt
					.executeQuery("SELECT * from testNextAndPrevious");

			System.out.println("Currently at row " + this.rs.getRow());
			this.rs.next();
			System.out.println("Value at row " + this.rs.getRow() + " is "
					+ this.rs.getString(1));

			this.rs.previous();

			try {
				System.out.println("Value at row " + this.rs.getRow() + " is "
						+ this.rs.getString(1));
				fail("Should not be able to retrieve values with invalid cursor");
			} catch (SQLException sqlEx) {
				assertTrue(sqlEx.getMessage().startsWith("Before start"));
			}

			this.rs.next();

			this.rs.next();

			try {
				System.out.println("Value at row " + this.rs.getRow() + " is "
						+ this.rs.getString(1));
				fail("Should not be able to retrieve values with invalid cursor");
			} catch (SQLException sqlEx) {
				assertTrue(sqlEx.getMessage().startsWith("After end"));
			}
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testNextAndPrevious");
		}
	}

	/**
	 * Tests fix for BUG#1630 (not updatable exception turning into NPE on
	 * second updateFoo() method call.
	 * 
	 * @throws Exception
	 *             if an unexpected exception is thrown.
	 */
	public void testNotUpdatable() throws Exception {
		this.rs = null;

		try {
			String sQuery = "SHOW VARIABLES";
			this.pstmt = this.conn
					.prepareStatement(sQuery, ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);

			this.rs = this.pstmt.executeQuery();

			if (this.rs.next()) {
				this.rs.absolute(1);

				try {
					this.rs.updateInt(1, 1);
				} catch (SQLException sqlEx) {
					assertTrue(sqlEx instanceof NotUpdatable);
				}

				try {
					this.rs.updateString(1, "1");
				} catch (SQLException sqlEx) {
					assertTrue(sqlEx instanceof NotUpdatable);
				}
			}
		} finally {
			if (this.pstmt != null) {
				try {
					this.pstmt.close();
				} catch (Exception e) {
					// ignore
				}
			}
		}
	}

	/**
	 * Tests that streaming result sets are registered correctly.
	 * 
	 * @throws Exception
	 *             if any errors occur
	 */
	public void testStreamingRegBug() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS StreamingRegBug");
			this.stmt
					.executeUpdate("CREATE TABLE StreamingRegBug ( DUMMYID "
							+ " INTEGER NOT NULL, DUMMYNAME VARCHAR(32),PRIMARY KEY (DUMMYID) )");
			this.stmt
					.executeUpdate("INSERT INTO StreamingRegBug (DUMMYID, DUMMYNAME) VALUES (0, NULL)");
			this.stmt
					.executeUpdate("INSERT INTO StreamingRegBug (DUMMYID, DUMMYNAME) VALUES (1, 'nro 1')");
			this.stmt
					.executeUpdate("INSERT INTO StreamingRegBug (DUMMYID, DUMMYNAME) VALUES (2, 'nro 2')");
			this.stmt
					.executeUpdate("INSERT INTO StreamingRegBug (DUMMYID, DUMMYNAME) VALUES (3, 'nro 3')");

			PreparedStatement streamStmt = null;

			try {
				streamStmt = this.conn.prepareStatement(
						"SELECT DUMMYID, DUMMYNAME "
								+ "FROM StreamingRegBug ORDER BY DUMMYID",
						java.sql.ResultSet.TYPE_FORWARD_ONLY,
						java.sql.ResultSet.CONCUR_READ_ONLY);
				streamStmt.setFetchSize(Integer.MIN_VALUE);

				this.rs = streamStmt.executeQuery();

				while (this.rs.next()) {
					this.rs.getString(1);
				}

				this.rs.close(); // error occurs here
			} catch (SQLException sqlEx) {

			} finally {
				if (streamStmt != null) {
					try {
						streamStmt.close();
					} catch (SQLException exWhileClose) {
						exWhileClose.printStackTrace();
					}
				}
			}
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS StreamingRegBug");
		}
	}

	/**
	 * Tests that result sets can be updated when all parameters are correctly
	 * set.
	 * 
	 * @throws Exception
	 *             if any errors occur
	 */
	public void testUpdatability() throws Exception {
		this.rs = null;

		createTable("updatabilityBug", "("
				+ " id int(10) unsigned NOT NULL auto_increment,"
				+ " field1 varchar(32) NOT NULL default '',"
				+ " field2 varchar(128) NOT NULL default '',"
				+ " field3 varchar(128) default NULL,"
				+ " field4 varchar(128) default NULL,"
				+ " field5 varchar(64) default NULL,"
				+ " field6 int(10) unsigned default NULL,"
				+ " field7 varchar(64) default NULL," + " PRIMARY KEY  (id)"
				+ ") ", "InnoDB");
		this.stmt.executeUpdate("insert into updatabilityBug (id) values (1)");

		try {
			String sQuery = " SELECT * FROM updatabilityBug WHERE id = ? ";
			this.pstmt = this.conn
					.prepareStatement(sQuery, ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);
			this.conn.setAutoCommit(false);
			this.pstmt.setInt(1, 1);
			this.rs = this.pstmt.executeQuery();

			if (this.rs.next()) {
				this.rs.absolute(1);
				this.rs.updateInt("id", 1);
				this.rs.updateString("field1", "1");
				this.rs.updateString("field2", "1");
				this.rs.updateString("field3", "1");
				this.rs.updateString("field4", "1");
				this.rs.updateString("field5", "1");
				this.rs.updateInt("field6", 1);
				this.rs.updateString("field7", "1");
				this.rs.updateRow();
			}

			this.conn.commit();
			this.conn.setAutoCommit(true);
		} finally {
			if (this.pstmt != null) {
				try {
					this.pstmt.close();
				} catch (Exception e) {
					// ignore
				}
			}

			this.stmt.execute("DROP TABLE IF EXISTS updatabilityBug");
		}
	}

	/**
	 * Test fixes for BUG#1071
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testUpdatabilityAndEscaping() throws Exception {
		Properties props = new Properties();
		props.setProperty("useUnicode", "true");
		props.setProperty("characterEncoding", "big5");

		Connection updConn = getConnectionWithProps(props);
		Statement updStmt = updConn.createStatement(
				ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);

		try {
			updStmt
					.executeUpdate("DROP TABLE IF EXISTS testUpdatesWithEscaping");
			updStmt
					.executeUpdate("CREATE TABLE testUpdatesWithEscaping (field1 INT PRIMARY KEY, field2 VARCHAR(64))");
			updStmt
					.executeUpdate("INSERT INTO testUpdatesWithEscaping VALUES (1, null)");

			String stringToUpdate = "\" \\ '";

			this.rs = updStmt
					.executeQuery("SELECT * from testUpdatesWithEscaping");

			this.rs.next();
			this.rs.updateString(2, stringToUpdate);
			this.rs.updateRow();

			assertTrue(stringToUpdate.equals(this.rs.getString(2)));
		} finally {
			updStmt
					.executeUpdate("DROP TABLE IF EXISTS testUpdatesWithEscaping");
			updStmt.close();
			updConn.close();
		}
	}

	/**
	 * Tests the fix for BUG#661 ... refreshRow() fails when primary key values
	 * have escaped data in them.
	 * 
	 * @throws Exception
	 *             if an error occurs
	 */
	public void testUpdatabilityWithQuotes() throws Exception {
		Statement updStmt = null;

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testUpdWithQuotes");
			this.stmt
					.executeUpdate("CREATE TABLE testUpdWithQuotes (keyField CHAR(32) PRIMARY KEY NOT NULL, field2 int)");

			PreparedStatement pStmt = this.conn
					.prepareStatement("INSERT INTO testUpdWithQuotes VALUES (?, ?)");
			pStmt.setString(1, "Abe's");
			pStmt.setInt(2, 1);
			pStmt.executeUpdate();

			updStmt = this.conn
					.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);

			this.rs = updStmt.executeQuery("SELECT * FROM testUpdWithQuotes");
			this.rs.next();
			this.rs.updateInt(2, 2);
			this.rs.updateRow();
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testUpdWithQuotes");

			if (this.rs != null) {
				this.rs.close();
			}

			this.rs = null;

			if (updStmt != null) {
				updStmt.close();
			}

			updStmt = null;
		}
	}

	/**
	 * Checks whether or not ResultSet.updateClob() is implemented
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testUpdateClob() throws Exception {
		if (isRunningOnJdk131()) {
			return; // test not valid on JDK-1.3.1
		}

		Statement updatableStmt = this.conn.createStatement(
				ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testUpdateClob");
			this.stmt
					.executeUpdate("CREATE TABLE testUpdateClob(intField INT NOT NULL PRIMARY KEY, clobField TEXT)");
			this.stmt
					.executeUpdate("INSERT INTO testUpdateClob VALUES (1, 'foo')");

			this.rs = updatableStmt
					.executeQuery("SELECT intField, clobField FROM testUpdateClob");
			this.rs.next();

			Clob clob = this.rs.getClob(2);

			clob.setString(1, "bar");

			this.rs.updateClob(2, clob);
			this.rs.updateRow();

			this.rs.moveToInsertRow();

			clob.setString(1, "baz");
			this.rs.updateInt(1, 2);
			this.rs.updateClob(2, clob);
			this.rs.insertRow();

			clob.setString(1, "bat");
			this.rs.updateInt(1, 3);
			this.rs.updateClob(2, clob);
			this.rs.insertRow();

			this.rs.close();

			this.rs = this.stmt
					.executeQuery("SELECT intField, clobField FROM testUpdateClob ORDER BY intField");

			this.rs.next();
			assertTrue((this.rs.getInt(1) == 1)
					&& this.rs.getString(2).equals("bar"));

			this.rs.next();
			assertTrue((this.rs.getInt(1) == 2)
					&& this.rs.getString(2).equals("baz"));

			this.rs.next();
			assertTrue((this.rs.getInt(1) == 3)
					&& this.rs.getString(2).equals("bat"));
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testUpdateClob");
		}
	}

	/**
	 * Tests fix for BUG#4482, ResultSet.getObject() returns wrong type for
	 * strings when using prepared statements.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug4482() throws Exception {
		this.rs = this.conn.prepareStatement("SELECT 'abcdef'").executeQuery();
		assertTrue(this.rs.next());
		assertTrue(this.rs.getObject(1) instanceof String);
	}

	/**
	 * Test fix for BUG#4689 - WasNull not getting set correctly for binary
	 * result sets.
	 */
	public void testBug4689() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4689");
			this.stmt
					.executeUpdate("CREATE TABLE testBug4689 (tinyintField tinyint, tinyintFieldNull tinyint, "
							+ "intField int, intFieldNull int, "
							+ "bigintField bigint, bigintFieldNull bigint, "
							+ "shortField smallint, shortFieldNull smallint, "
							+ "doubleField double, doubleFieldNull double)");

			this.stmt.executeUpdate("INSERT INTO testBug4689 VALUES (1, null, "
					+ "1, null, " + "1, null, " + "1, null, " + "1, null)");

			PreparedStatement pStmt = this.conn
					.prepareStatement("SELECT tinyintField, tinyintFieldNull,"
							+ "intField, intFieldNull, "
							+ "bigintField, bigintFieldNull, "
							+ "shortField, shortFieldNull, "
							+ "doubleField, doubleFieldNull FROM testBug4689");
			this.rs = pStmt.executeQuery();
			assertTrue(this.rs.next());

			assertTrue(this.rs.getByte(1) == 1);
			assertTrue(this.rs.wasNull() == false);
			assertTrue(this.rs.getByte(2) == 0);
			assertTrue(this.rs.wasNull() == true);

			assertTrue(this.rs.getInt(3) == 1);
			assertTrue(this.rs.wasNull() == false);
			assertTrue(this.rs.getInt(4) == 0);
			assertTrue(this.rs.wasNull() == true);

			assertTrue(this.rs.getInt(5) == 1);
			assertTrue(this.rs.wasNull() == false);
			assertTrue(this.rs.getInt(6) == 0);
			assertTrue(this.rs.wasNull() == true);

			assertTrue(this.rs.getShort(7) == 1);
			assertTrue(this.rs.wasNull() == false);
			assertTrue(this.rs.getShort(8) == 0);
			assertTrue(this.rs.wasNull() == true);

			assertTrue(this.rs.getDouble(9) == 1);
			assertTrue(this.rs.wasNull() == false);
			assertTrue(this.rs.getDouble(10) == 0);
			assertTrue(this.rs.wasNull() == true);
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug4689");
		}
	}

	/**
	 * Tests fix for BUG#5032 -- ResultSet.getObject() doesn't return type
	 * Boolean for pseudo-bit types from prepared statements on 4.1.x.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug5032() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			PreparedStatement pStmt = null;

			try {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5032");
				this.stmt.executeUpdate("CREATE TABLE testBug5032(field1 BIT)");
				this.stmt.executeUpdate("INSERT INTO testBug5032 VALUES (1)");

				pStmt = this.conn
						.prepareStatement("SELECT field1 FROM testBug5032");
				this.rs = pStmt.executeQuery();
				assertTrue(this.rs.next());
				assertTrue(this.rs.getObject(1) instanceof Boolean);
			} finally {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5032");

				if (pStmt != null) {
					pStmt.close();
				}
			}
		}
	}

	/**
	 * Tests fix for BUG#5069 -- ResultSet.getMetaData() should not return
	 * incorrectly-initialized metadata if the result set has been closed, but
	 * should instead throw a SQLException. Also tests fix for getRow() and
	 * getWarnings() and traversal methods.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug5069() throws Exception {
		try {
			this.rs = this.stmt.executeQuery("SELECT 1");
			this.rs.close();

			try {
				ResultSetMetaData md = this.rs.getMetaData();
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}

			try {
				this.rs.getRow();
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}

			try {
				this.rs.getWarnings();
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}

			try {
				this.rs.first();
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}

			try {
				this.rs.beforeFirst();
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}

			try {
				this.rs.last();
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}

			try {
				this.rs.afterLast();
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}

			try {
				this.rs.relative(0);
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}

			try {
				this.rs.next();
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}

			try {
				this.rs.previous();
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}

			try {
				this.rs.isBeforeFirst();
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}

			try {
				this.rs.isFirst();
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}

			try {
				this.rs.isAfterLast();
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}

			try {
				this.rs.isLast();
			} catch (NullPointerException npEx) {
				fail("Should not catch NullPointerException here");
			} catch (SQLException sqlEx) {
				assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx
						.getSQLState()));
			}
		} finally {
			if (this.rs != null) {
				this.rs.close();
				this.rs = null;
			}
		}
	}

	/**
	 * Tests for BUG#5235, ClassCastException on all-zero date field when
	 * zeroDatetimeBehavior is 'convertToNull'...however it appears that this
	 * bug doesn't exist. This is a placeholder until we get more data from the
	 * user on how they provoke this bug to happen.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug5235() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5235");
			this.stmt.executeUpdate("CREATE TABLE testBug5235(field1 DATE)");
			this.stmt
					.executeUpdate("INSERT INTO testBug5235 (field1) VALUES ('0000-00-00')");

			Properties props = new Properties();
			props.setProperty("zeroDateTimeBehavior", "convertToNull");

			Connection nullConn = getConnectionWithProps(props);

			this.rs = nullConn.createStatement().executeQuery(
					"SELECT field1 FROM testBug5235");
			this.rs.next();
			assertTrue(null == this.rs.getObject(1));
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5235");
		}
	}

	/**
	 * Tests for BUG#5136, GEOMETRY types getting corrupted, turns out to be a
	 * server bug.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug5136() throws Exception {
		if (false) {
			PreparedStatement toGeom = this.conn
					.prepareStatement("select GeomFromText(?)");
			PreparedStatement toText = this.conn
					.prepareStatement("select AsText(?)");

			String inText = "POINT(146.67596278 -36.54368233)";

			// First assert that the problem is not at the server end
			this.rs = this.stmt.executeQuery("select AsText(GeomFromText('"
					+ inText + "'))");
			this.rs.next();

			String outText = this.rs.getString(1);
			this.rs.close();
			assertTrue(
					"Server side only\n In: " + inText + "\nOut: " + outText,
					inText.equals(outText));

			// Now bring a binary geometry object to the client and send it back
			toGeom.setString(1, inText);
			this.rs = toGeom.executeQuery();
			this.rs.next();

			// Return a binary geometry object from the WKT
			Object geom = this.rs.getObject(1);
			this.rs.close();
			toText.setObject(1, geom);
			this.rs = toText.executeQuery();
			this.rs.next();

			// Return WKT from the binary geometry
			outText = this.rs.getString(1);
			this.rs.close();
			assertTrue("Server to client and back\n In: " + inText + "\nOut: "
					+ outText, inText.equals(outText));
		}
	}

	/**
	 * Tests fix for BUG#5664, ResultSet.updateByte() when on insert row throws
	 * ArrayOutOfBoundsException.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug5664() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5664");
			this.stmt
					.executeUpdate("CREATE TABLE testBug5664 (pkfield int PRIMARY KEY NOT NULL, field1 SMALLINT)");
			this.stmt.executeUpdate("INSERT INTO testBug5664 VALUES (1, 1)");

			Statement updatableStmt = this.conn
					.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);

			this.rs = updatableStmt
					.executeQuery("SELECT pkfield, field1 FROM testBug5664");
			this.rs.next();
			this.rs.moveToInsertRow();
			this.rs.updateInt(1, 2);
			this.rs.updateByte(2, (byte) 2);
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5664");
		}
	}

	public void testBogusTimestampAsString() throws Exception {

		this.rs = this.stmt.executeQuery("SELECT '2004-08-13 13:21:17.'");

		this.rs.next();

		// We're only checking for an exception being thrown here as the bug
		this.rs.getTimestamp(1);

	}

	/**
	 * Tests our ability to reject NaN and +/- INF in
	 * PreparedStatement.setDouble();
	 */
	public void testBug5717() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5717");
			this.stmt.executeUpdate("CREATE TABLE testBug5717 (field1 DOUBLE)");
			this.pstmt = this.conn
					.prepareStatement("INSERT INTO testBug5717 VALUES (?)");

			try {
				this.pstmt.setDouble(1, Double.NEGATIVE_INFINITY);
				fail("Exception should've been thrown");
			} catch (Exception ex) {
				// expected
			}

			try {
				this.pstmt.setDouble(1, Double.POSITIVE_INFINITY);
				fail("Exception should've been thrown");
			} catch (Exception ex) {
				// expected
			}

			try {
				this.pstmt.setDouble(1, Double.NaN);
				fail("Exception should've been thrown");
			} catch (Exception ex) {
				// expected
			}
		} finally {
			if (this.pstmt != null) {
				this.pstmt.close();
			}

			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug5717");
		}
	}

	/**
	 * Tests fix for server issue that drops precision on aggregate operations
	 * on DECIMAL types, because they come back as DOUBLEs.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug6537() throws Exception {
		if (versionMeetsMinimum(4, 1, 0)) {
			String tableName = "testBug6537";

			try {
				createTable(
						tableName,
						"(`id` int(11) NOT NULL default '0',"
								+ "`value` decimal(10,2) NOT NULL default '0.00', `stringval` varchar(10),"
								+ "PRIMARY KEY  (`id`)"
								+ ") DEFAULT CHARSET=latin1", "MyISAM");
				this.stmt
						.executeUpdate("INSERT INTO "
								+ tableName
								+ "(id, value, stringval) VALUES (1, 100.00, '100.00'), (2, 200, '200')");

				String sql = "SELECT SUM(value) as total FROM " + tableName
						+ " WHERE id = ? ";
				PreparedStatement pStmt = this.conn.prepareStatement(sql);
				pStmt.setInt(1, 1);
				this.rs = pStmt.executeQuery();
				assertTrue(this.rs.next());

				assertTrue("100.00".equals(this.rs.getBigDecimal("total")
						.toString()));

				sql = "SELECT stringval as total FROM " + tableName
						+ " WHERE id = ? ";
				pStmt = this.conn.prepareStatement(sql);
				pStmt.setInt(1, 2);
				this.rs = pStmt.executeQuery();
				assertTrue(this.rs.next());

				assertTrue("200.00".equals(this.rs.getBigDecimal("total", 2)
						.toString()));

			} finally {
				dropTable(tableName);
			}
		}
	}

	/**
	 * Tests fix for BUG#6231, ResultSet.getTimestamp() on a column with TIME in
	 * it fails.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug6231() throws Exception {

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug6231");
			this.stmt.executeUpdate("CREATE TABLE testBug6231 (field1 TIME)");
			this.stmt
					.executeUpdate("INSERT INTO testBug6231 VALUES ('09:16:00')");

			this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug6231");
			this.rs.next();
			long asMillis = this.rs.getTimestamp(1).getTime();
			Calendar cal = Calendar.getInstance();

			if (isRunningOnJdk131()) {
				cal.setTime(new Date(asMillis));
			} else {
				cal.setTimeInMillis(asMillis);
			}

			assertEquals(9, cal.get(Calendar.HOUR));
			assertEquals(16, cal.get(Calendar.MINUTE));
			assertEquals(0, cal.get(Calendar.SECOND));
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug6231");
		}
	}

	public void testBug6619() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug6619");
			this.stmt.executeUpdate("CREATE TABLE testBug6619 (field1 int)");
			this.stmt.executeUpdate("INSERT INTO testBug6619 VALUES (1), (2)");

			PreparedStatement pStmt = this.conn
					.prepareStatement("SELECT SUM(field1) FROM testBug6619");

			this.rs = pStmt.executeQuery();
			this.rs.next();
			System.out.println(this.rs.getString(1));

		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug6619");
		}
	}

	public void testBug6743() throws Exception {
		// 0x835C U+30BD # KATAKANA LETTER SO
		String katakanaStr = "\u30BD";

		Properties props = new Properties();

		props.setProperty("useUnicode", "true");
		props.setProperty("characterEncoding", "SJIS");

		Connection sjisConn = null;
		Statement sjisStmt = null;

		try {
			sjisConn = getConnectionWithProps(props);
			sjisStmt = sjisConn.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);

			sjisStmt.executeUpdate("DROP TABLE IF EXISTS testBug6743");
			StringBuffer queryBuf = new StringBuffer(
					"CREATE TABLE testBug6743 (pkField INT NOT NULL PRIMARY KEY, field1 VARCHAR(32)");

			if (versionMeetsMinimum(4, 1)) {
				queryBuf.append(" CHARACTER SET SJIS");
			}

			queryBuf.append(")");
			sjisStmt.executeUpdate(queryBuf.toString());
			sjisStmt.executeUpdate("INSERT INTO testBug6743 VALUES (1, 'abc')");

			this.rs = sjisStmt
					.executeQuery("SELECT pkField, field1 FROM testBug6743");
			this.rs.next();
			this.rs.updateString(2, katakanaStr);
			this.rs.updateRow();

			String retrString = this.rs.getString(2);
			assertTrue(katakanaStr.equals(retrString));

			this.rs = sjisStmt
					.executeQuery("SELECT pkField, field1 FROM testBug6743");
			this.rs.next();

			retrString = this.rs.getString(2);
			assertTrue(katakanaStr.equals(retrString));
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug6743");

			if (sjisStmt != null) {
				sjisStmt.close();
			}

			if (sjisConn != null) {
				sjisConn.close();
			}
		}
	}

	/**
	 * Tests for presence of BUG#6561, NPE thrown when dealing with 0 dates and
	 * non-unpacked result sets.
	 * 
	 * @throws Exception
	 *             if the test occurs.
	 */
	public void testBug6561() throws Exception {

		try {
			Properties props = new Properties();
			props.setProperty("zeroDateTimeBehavior", "convertToNull");

			Connection zeroConn = getConnectionWithProps(props);

			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug6561");
			this.stmt
					.executeUpdate("CREATE TABLE testBug6561 (ofield int, field1 DATE, field2 integer, field3 integer)");
			this.stmt
					.executeUpdate("INSERT INTO testBug6561 (ofield, field1,field2,field3)	VALUES (1, 0,NULL,0)");
			this.stmt
					.executeUpdate("INSERT INTO testBug6561 (ofield, field1,field2,field3) VALUES (2, '2004-11-20',NULL,0)");

			PreparedStatement ps = zeroConn
					.prepareStatement("SELECT field1,field2,field3 FROM testBug6561 ORDER BY ofield");
			this.rs = ps.executeQuery();

			assertTrue(this.rs.next());
			assertTrue(null == this.rs.getObject("field1"));
			assertTrue(null == this.rs.getObject("field2"));
			assertTrue(0 == this.rs.getInt("field3"));

			assertTrue(this.rs.next());
			assertEquals("2004-11-20", this.rs.getString("field1"));
			assertTrue(null == this.rs.getObject("field2"));
			assertTrue(0 == this.rs.getInt("field3"));

			ps.close();
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS test");
		}
	}

	public void testBug7686() throws SQLException {
		String tableName = "testBug7686";
		createTable(tableName, "(id1 int(10) unsigned NOT NULL,"
				+ " id2 DATETIME, "
				+ " field1 varchar(128) NOT NULL default '',"
				+ " PRIMARY KEY  (id1, id2))", "InnoDB;");

		this.stmt.executeUpdate("insert into " + tableName
				+ " (id1, id2, field1)"
				+ " values (1, '2005-01-05 13:59:20', 'foo')");

		String sQuery = " SELECT * FROM " + tableName
				+ " WHERE id1 = ? AND id2 = ?";
		this.pstmt = this.conn.prepareStatement(sQuery,
				ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

		this.conn.setAutoCommit(false);
		this.pstmt.setInt(1, 1);
		GregorianCalendar cal = new GregorianCalendar();
		cal.clear();
		cal.set(2005, 00, 05, 13, 59, 20);

		Timestamp jan5before2pm = null;

		if (isRunningOnJdk131()) {
			jan5before2pm = new java.sql.Timestamp(cal.getTime().getTime());
		} else {
			jan5before2pm = new java.sql.Timestamp(cal.getTimeInMillis());
		}

		this.pstmt.setTimestamp(2, jan5before2pm);
		this.rs = this.pstmt.executeQuery();
		assertTrue(this.rs.next());
		this.rs.absolute(1);
		this.rs.updateString("field1", "bar");
		this.rs.updateRow();
		this.conn.commit();
		this.conn.setAutoCommit(true);
	}

	/**
	 * Tests fix for BUG#7715 - Timestamps converted incorrectly to strings with
	 * SSPS and Upd. Result Sets.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug7715() throws Exception {
		PreparedStatement pStmt = null;

		try {
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS testConvertedBinaryTimestamp");
			this.stmt
					.executeUpdate("CREATE TABLE testConvertedBinaryTimestamp (field1 VARCHAR(32), field2 VARCHAR(32), field3 VARCHAR(32), field4 TIMESTAMP)");
			this.stmt
					.executeUpdate("INSERT INTO testConvertedBinaryTimestamp VALUES ('abc', 'def', 'ghi', NOW())");

			pStmt = this.conn
					.prepareStatement(
							"SELECT field1, field2, field3, field4 FROM testConvertedBinaryTimestamp",
							ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);

			this.rs = pStmt.executeQuery();
			assertTrue(this.rs.next());

			this.rs.getObject(4); // fails if bug exists
		} finally {
			this.stmt
					.executeUpdate("DROP TABLE IF EXISTS testConvertedBinaryTimestamp");
		}
	}

	/**
	 * Tests fix for BUG#8428 - getString() doesn't maintain format stored on
	 * server.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug8428() throws Exception {
		Connection noSyncConn = null;

		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug8428");
			this.stmt
					.executeUpdate("CREATE TABLE testBug8428 (field1 YEAR, field2 DATETIME)");
			this.stmt
					.executeUpdate("INSERT INTO testBug8428 VALUES ('1999', '2005-02-11 12:54:41')");

			Properties props = new Properties();
			props.setProperty("noDatetimeStringSync", "true");
			props.setProperty("useUsageAdvisor", "true");
			props.setProperty("yearIsDateType", "false"); // for 3.1.9+

			noSyncConn = getConnectionWithProps(props);

			this.rs = noSyncConn.createStatement().executeQuery(
					"SELECT field1, field2 FROM testBug8428");
			this.rs.next();
			assertEquals("1999", this.rs.getString(1));
			assertEquals("2005-02-11 12:54:41", this.rs.getString(2));

			this.rs = noSyncConn.prepareStatement(
					"SELECT field1, field2 FROM testBug8428").executeQuery();
			this.rs.next();
			assertEquals("1999", this.rs.getString(1));
			assertEquals("2005-02-11 12:54:41", this.rs.getString(2));
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug8428");
		}
	}

	/**
	 * Tests fix for Bug#8868, DATE_FORMAT() queries returned as BLOBs from
	 * getObject().
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug8868() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			createTable("testBug8868",
					"(field1 DATE, field2 VARCHAR(32) CHARACTER SET BINARY)");
			this.stmt
					.executeUpdate("INSERT INTO testBug8868 VALUES (NOW(), 'abcd')");
			try {
				this.rs = this.stmt
						.executeQuery("SELECT DATE_FORMAT(field1,'%b-%e %l:%i%p') as fmtddate, field2 FROM testBug8868");
				this.rs.next();
				assertEquals("java.lang.String", this.rs.getObject(1)
						.getClass().getName());
			} finally {
				if (this.rs != null) {
					this.rs.close();
				}
			}
		}
	}

	/**
	 * Tests fix for BUG#9098 - Server doesn't give us info to distinguish
	 * between CURRENT_TIMESTAMP and 'CURRENT_TIMESTAMP' for default values.
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug9098() throws Exception {
		if (versionMeetsMinimum(4, 1, 10)) {
			Statement updatableStmt = null;

			try {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug9098");
				this.stmt
						.executeUpdate("CREATE TABLE testBug9098(pkfield INT PRIMARY KEY NOT NULL AUTO_INCREMENT, \n"
								+ "tsfield TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, tsfield2 TIMESTAMP NOT NULL DEFAULT '2005-12-25 12:20:52', charfield VARCHAR(4) NOT NULL DEFAULT 'abcd')");
				updatableStmt = this.conn.createStatement(
						ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
				this.rs = updatableStmt
						.executeQuery("SELECT pkfield, tsfield, tsfield2, charfield FROM testBug9098");
				this.rs.moveToInsertRow();
				this.rs.insertRow();

			} finally {
				this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug9098");
			}
		}
	}

	/**
	 * Tests fix for BUG#9236, a continuation of BUG#8868, where functions used
	 * in queries that should return non-string types when resolved by temporary
	 * tables suddenly become opaque binary strings (work-around for server
	 * limitation)
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug9236() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			try {
				createTable(
						"testBug9236",
						"("
								+ "field_1 int(18) NOT NULL auto_increment,"
								+ "field_2 varchar(50) NOT NULL default '',"
								+ "field_3 varchar(12) default NULL,"
								+ "field_4 int(18) default NULL,"
								+ "field_5 int(18) default NULL,"
								+ "field_6 datetime default NULL,"
								+ "field_7 varchar(30) default NULL,"
								+ "field_8 varchar(50) default NULL,"
								+ "field_9 datetime default NULL,"
								+ "field_10 int(18) NOT NULL default '0',"
								+ "field_11 int(18) default NULL,"
								+ "field_12 datetime NOT NULL default '0000-00-00 00:00:00',"
								+ "PRIMARY KEY  (field_1)," + "KEY (field_4),"
								+ "KEY (field_2)," + "KEY (field_3),"
								+ "KEY (field_7,field_1)," + "KEY (field_5),"
								+ "KEY (field_6,field_10,field_9),"
								+ "KEY (field_11,field_10),"
								+ "KEY (field_12,field_10)"
								+ ") DEFAULT CHARSET=latin1", "InnoDB");

				this.stmt
						.executeUpdate("INSERT INTO testBug9236 VALUES "
								+ "(1,'0',NULL,-1,0,'0000-00-00 00:00:00','123456789','-1','2004-03-13 14:21:38',0,NULL,'2004-03-13 14:21:38'),"
								+ "(2,'0',NULL,1,0,'0000-00-00 00:00:00',NULL,'1',NULL,0,NULL,'2004-07-13 14:29:52'),"
								+ "(3,'0',NULL,1,0,'0000-00-00 00:00:00',NULL,'2',NULL,0,NULL,'2004-07-16 13:20:51'),"
								+ "(4,'0',NULL,1,0,'0000-00-00 00:00:00',NULL,'3','2004-07-16 13:43:39',0,NULL,'2004-07-16 13:22:01'),"
								+ "(5,'0',NULL,1,0,'0000-00-00 00:00:00',NULL,'4','2004-07-16 13:23:48',0,NULL,'2004-07-16 13:23:01'),"
								+ "(6,'0',NULL,1,0,'0000-00-00 00:00:00',NULL,'5',NULL,0,NULL,'2004-07-16 14:41:07'),"
								+ "(7,'0',NULL,1,0,'0000-00-00 00:00:00',NULL,'6',NULL,0,NULL,'2004-07-16 14:41:34'),"
								+ "(8,'0',NULL,1,0,'0000-00-00 00:00:00',NULL,'7',NULL,0,NULL,'2004-07-16 14:41:54'),"
								+ "(9,'0',NULL,1,0,'0000-00-00 00:00:00',NULL,'8',NULL,0,NULL,'2004-07-16 14:42:42'),"
								+ "(10,'0','PI',1,0,'0000-00-00 00:00:00',NULL,'9',NULL,0,NULL,'2004-07-18 10:51:30'),"
								+ "(11,'0',NULL,1,0,'0000-00-00 00:00:00',NULL,'10','2004-07-23 17:23:06',0,NULL,'2004-07-23 17:18:19'),"
								+ "(12,'0',NULL,1,0,'0000-00-00 00:00:00',NULL,'11','2004-07-23 17:24:45',0,NULL,'2004-07-23 17:23:57'),"
								+ "(13,'0',NULL,1,0,'0000-00-00 00:00:00',NULL,'12','2004-07-23 17:30:51',0,NULL,'2004-07-23 17:30:15'),"
								+ "(14,'0',NULL,1,0,'0000-00-00 00:00:00',NULL,'13','2004-07-26 17:50:19',0,NULL,'2004-07-26 17:49:38'),"
								+ "(15,'0','FRL',1,0,'0000-00-00 00:00:00',NULL,'1',NULL,0,NULL,'2004-08-19 18:29:18'),"
								+ "(16,'0','FRL',1,0,'0000-00-00 00:00:00',NULL,'15',NULL,0,NULL,'2005-03-16 12:08:28')");

				createTable("testBug9236_1",
						"(field1 CHAR(2) CHARACTER SET BINARY)");
				this.stmt
						.executeUpdate("INSERT INTO testBug9236_1 VALUES ('ab')");
				this.rs = this.stmt
						.executeQuery("SELECT field1 FROM testBug9236_1");

				ResultSetMetaData rsmd = this.rs.getMetaData();
				assertEquals("[B", rsmd.getColumnClassName(1));
				assertTrue(this.rs.next());
				Object asObject = this.rs.getObject(1);
				assertEquals("[B", asObject.getClass().getName());

				this.rs = this.stmt
						.executeQuery("select DATE_FORMAT(field_12, '%Y-%m-%d') as date, count(*) as count from testBug9236 where field_10 = 0 and field_3 = 'FRL' and field_12 >= '2005-03-02 00:00:00' and field_12 <= '2005-03-17 00:00:00' group by date");
				rsmd = this.rs.getMetaData();
				assertEquals("java.lang.String", rsmd.getColumnClassName(1));
				this.rs.next();
				asObject = this.rs.getObject(1);
				assertEquals("java.lang.String", asObject.getClass().getName());

				this.rs.close();

				createTable("testBug8868_2",
						"(field1 CHAR(4) CHARACTER SET BINARY)");
				this.stmt
						.executeUpdate("INSERT INTO testBug8868_2 VALUES ('abc')");
				this.rs = this.stmt
						.executeQuery("SELECT field1 FROM testBug8868_2");

				rsmd = this.rs.getMetaData();
				assertEquals("[B", rsmd.getColumnClassName(1));
				this.rs.next();
				asObject = this.rs.getObject(1);
				assertEquals("[B", asObject.getClass().getName());
			} finally {
				if (this.rs != null) {
					this.rs.close();
					this.rs = null;
				}
			}
		}
	}

	/**
	 * Tests fix for BUG#9437, IF() returns type of [B or java.lang.String
	 * depending on platform. Fixed earlier, but in here to catch if it ever
	 * regresses.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug9437() throws Exception {
		String tableName = "testBug9437";

		if (versionMeetsMinimum(4, 1, 0)) {
			try {
				createTable(
						tableName,
						"("
								+ "languageCode char(2) NOT NULL default '',"
								+ "countryCode char(2) NOT NULL default '',"
								+ "supported enum('no','yes') NOT NULL default 'no',"
								+ "ordering int(11) default NULL,"
								+ "createDate datetime NOT NULL default '1000-01-01 00:00:03',"
								+ "modifyDate timestamp NOT NULL default CURRENT_TIMESTAMP on update"
								+ " CURRENT_TIMESTAMP,"
								+ "PRIMARY KEY  (languageCode,countryCode),"
								+ "KEY languageCode (languageCode),"
								+ "KEY countryCode (countryCode),"
								+ "KEY ordering (ordering),"
								+ "KEY modifyDate (modifyDate)"
								+ ") DEFAULT CHARSET=utf8", "InnoDB");

				this.stmt.executeUpdate("INSERT INTO " + tableName
						+ " (languageCode) VALUES ('en')");

				String alias = "someLocale";
				String sql = "select if ( languageCode = ?, ?, ? ) as " + alias
						+ " from " + tableName;
				this.pstmt = this.conn.prepareStatement(sql);

				int count = 1;
				this.pstmt.setObject(count++, "en");
				this.pstmt.setObject(count++, "en_US");
				this.pstmt.setObject(count++, "en_GB");

				this.rs = this.pstmt.executeQuery();

				assertTrue(this.rs.next());

				Object object = this.rs.getObject(alias);

				if (object != null) {
					assertEquals("java.lang.String", object.getClass()
							.getName());
					assertEquals("en_US", object.toString());
				}

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
	}

	public void testBug9684() throws Exception {
		if (versionMeetsMinimum(4, 1, 9)) {
			String tableName = "testBug9684";

			try {
				createTable(tableName,
						"(sourceText text character set utf8 collate utf8_bin)");
				this.stmt.executeUpdate("INSERT INTO " + tableName
						+ " VALUES ('abc')");
				this.rs = this.stmt.executeQuery("SELECT sourceText FROM "
						+ tableName);
				assertTrue(this.rs.next());
				assertEquals("java.lang.String", this.rs.getString(1)
						.getClass().getName());
				assertEquals("abc", this.rs.getString(1));
			} finally {
				if (this.rs != null) {
					this.rs.close();
					this.rs = null;
				}
			}
		}
	}

	/**
	 * Tests fix for BUG#10156 - Unsigned SMALLINT treated as signed
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug10156() throws Exception {
		String tableName = "testBug10156";
		try {
			createTable(tableName, "(field1 smallint(5) unsigned, "
					+ "field2 tinyint unsigned," + "field3 int unsigned)");
			this.stmt.executeUpdate("INSERT INTO " + tableName
					+ " VALUES (32768, 255, 4294967295)");
			this.rs = this.conn.prepareStatement(
					"SELECT field1, field2, field3 FROM " + tableName)
					.executeQuery();
			assertTrue(this.rs.next());
			assertEquals(32768, this.rs.getInt(1));
			assertEquals(255, this.rs.getInt(2));
			assertEquals(4294967295L, this.rs.getLong(3));

			assertEquals(String.valueOf(this.rs.getObject(1)), String
					.valueOf(this.rs.getInt(1)));
			assertEquals(String.valueOf(this.rs.getObject(2)), String
					.valueOf(this.rs.getInt(2)));
			assertEquals(String.valueOf(this.rs.getObject(3)), String
					.valueOf(this.rs.getLong(3)));

		} finally {
			if (this.rs != null) {
				this.rs.close();
				this.rs = null;
			}
		}
	}

	public void testBug10212() throws Exception {
		String tableName = "testBug10212";

		try {
			createTable(tableName, "(field1 YEAR(4))");
			this.stmt.executeUpdate("INSERT INTO " + tableName
					+ " VALUES (1974)");
			this.rs = this.conn.prepareStatement(
					"SELECT field1 FROM " + tableName).executeQuery();

			ResultSetMetaData rsmd = this.rs.getMetaData();
			assertTrue(this.rs.next());
			assertEquals("java.sql.Date", rsmd.getColumnClassName(1));
			assertEquals("java.sql.Date", this.rs.getObject(1).getClass()
					.getName());

			this.rs = this.stmt.executeQuery("SELECT field1 FROM " + tableName);

			rsmd = this.rs.getMetaData();
			assertTrue(this.rs.next());
			assertEquals("java.sql.Date", rsmd.getColumnClassName(1));
			assertEquals("java.sql.Date", this.rs.getObject(1).getClass()
					.getName());
		} finally {
			if (this.rs != null) {
				this.rs.close();
				this.rs = null;
			}
		}
	}

	/**
	 * Tests fix for BUG#11190 - ResultSet.moveToCurrentRow() fails to work when
	 * preceeded with .moveToInsertRow().
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug11190() throws Exception {

		createTable("testBug11190", "(a CHAR(4) PRIMARY KEY, b VARCHAR(20))");
		this.stmt
				.executeUpdate("INSERT INTO testBug11190 VALUES('3000','L'),('3001','H'),('1050','B')");

		Statement updStmt = null;

		try {
			updStmt = this.conn
					.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
							ResultSet.CONCUR_UPDATABLE);

			this.rs = updStmt.executeQuery("select * from testBug11190");
			assertTrue("must return a row", this.rs.next());
			String savedValue = this.rs.getString(1);
			this.rs.moveToInsertRow();
			this.rs.updateString(1, "4000");
			this.rs.updateString(2, "C");
			this.rs.insertRow();

			this.rs.moveToCurrentRow();
			assertEquals(savedValue, this.rs.getString(1));
		} finally {
			if (this.rs != null) {
				this.rs.close();
				this.rs = null;
			}

			if (updStmt != null) {
				updStmt.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#12104 - Geometry types not handled with server-side
	 * prepared statements.
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug12104() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			createTable("testBug12104", "(field1 GEOMETRY)", "MyISAM");

			try {
				this.stmt
						.executeUpdate("INSERT INTO testBug12104 VALUES (GeomFromText('POINT(1 1)'))");
				this.pstmt = this.conn
						.prepareStatement("SELECT field1 FROM testBug12104");
				this.rs = this.pstmt.executeQuery();
				assertTrue(this.rs.next());
				System.out.println(this.rs.getObject(1));
			} finally {

			}
		}
	}

	/**
	 * Tests fix for BUG#13043 - when 'gatherPerfMetrics' is enabled for servers <
	 * 4.1.0, a NPE is thrown from the constructor of ResultSet if the query
	 * doesn't use any tables.
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug13043() throws Exception {
		if (!versionMeetsMinimum(4, 1)) {
			Connection perfConn = null;

			try {
				Properties props = new Properties();
				props.put("gatherPerfMetrics", "true"); // this property is
				// reported as the cause
				// of
				// NullPointerException
				props.put("reportMetricsIntervalMillis", "30000"); // this
				// property
				// is
				// reported
				// as the
				// cause of
				// NullPointerException
				perfConn = getConnectionWithProps(props);
				perfConn.createStatement().executeQuery("SELECT 1");
			} finally {
				if (perfConn != null) {
					perfConn.close();
				}
			}
		}
	}

	/**
	 * Tests fix for BUG#13374 - ResultSet.getStatement() on closed result set
	 * returns NULL (as per JDBC 4.0 spec, but not backwards-compatible).
	 * 
	 * @throws Exception
	 *             if the test fails
	 */

	public void testBug13374() throws Exception {
		Statement retainStmt = null;
		Connection retainConn = null;

		try {
			Properties props = new Properties();

			props.setProperty("retainStatementAfterResultSetClose", "true");

			retainConn = getConnectionWithProps(props);

			retainStmt = retainConn.createStatement();

			this.rs = retainStmt.executeQuery("SELECT 1");
			this.rs.close();
			assertNotNull(this.rs.getStatement());

			this.rs = this.stmt.executeQuery("SELECT 1");
			this.rs.close();

			try {
				this.rs.getStatement();
			} catch (SQLException sqlEx) {
				assertEquals(sqlEx.getSQLState(),
						SQLError.SQL_STATE_GENERAL_ERROR);
			}

		} finally {
			if (this.rs != null) {
				this.rs.close();
				this.rs = null;
			}

			if (retainStmt != null) {
				retainStmt.close();
			}

			if (retainConn != null) {
				retainConn.close();
			}
		}
	}

	/**
	 * Tests bugfix for BUG#14562 - metadata/type for MEDIUMINT UNSIGNED is
	 * incorrect.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug14562() throws Exception {
		createTable("testBug14562",
				"(row_order INT, signed_field MEDIUMINT, unsigned_field MEDIUMINT UNSIGNED)");

		try {
			this.stmt
					.executeUpdate("INSERT INTO testBug14562 VALUES (1, -8388608, 0), (2, 8388607, 16777215)");

			this.rs = this.stmt
					.executeQuery("SELECT signed_field, unsigned_field FROM testBug14562 ORDER BY row_order");
			traverseResultSetBug14562();

			this.rs = this.conn
					.prepareStatement(
							"SELECT signed_field, unsigned_field FROM testBug14562 ORDER BY row_order")
					.executeQuery();
			traverseResultSetBug14562();

			if (versionMeetsMinimum(5, 0)) {
				CallableStatement storedProc = null;

				try {
					this.stmt
							.executeUpdate("DROP PROCEDURE IF EXISTS sp_testBug14562");
					this.stmt
							.executeUpdate("CREATE PROCEDURE sp_testBug14562() BEGIN SELECT signed_field, unsigned_field FROM testBug14562 ORDER BY row_order; END");
					storedProc = this.conn
							.prepareCall("{call sp_testBug14562()}");
					storedProc.execute();
					this.rs = storedProc.getResultSet();
					traverseResultSetBug14562();

					this.stmt
							.executeUpdate("DROP PROCEDURE IF EXISTS sp_testBug14562_1");
					this.stmt
							.executeUpdate("CREATE PROCEDURE sp_testBug14562_1(OUT param_1 MEDIUMINT, OUT param_2 MEDIUMINT UNSIGNED) BEGIN SELECT signed_field, unsigned_field INTO param_1, param_2 FROM testBug14562 WHERE row_order=1; END");
					storedProc = this.conn
							.prepareCall("{call sp_testBug14562_1(?, ?)}");
					storedProc.registerOutParameter(1, Types.INTEGER);
					storedProc.registerOutParameter(2, Types.INTEGER);

					storedProc.execute();

					assertEquals("java.lang.Integer", storedProc.getObject(1)
							.getClass().getName());
					
					if (versionMeetsMinimum(5, 1) || versionMeetsMinimum(5, 0, 67)) {
						assertEquals("java.lang.Long", storedProc.getObject(2)
								.getClass().getName());
					} else {
						assertEquals("java.lang.Integer", storedProc.getObject(2)
							.getClass().getName());
					}

				} finally {
					if (storedProc != null) {
						storedProc.close();
					}

					this.stmt
							.executeUpdate("DROP PROCEDURE IF EXISTS sp_testBug14562");
				}
			}

			this.rs = this.conn.getMetaData().getColumns(
					this.conn.getCatalog(), null, "testBug14562", "%field");

			assertTrue(this.rs.next());

			assertEquals(Types.INTEGER, this.rs.getInt("DATA_TYPE"));
			assertEquals("MEDIUMINT", this.rs.getString("TYPE_NAME")
					.toUpperCase(Locale.US));

			assertTrue(this.rs.next());

			assertEquals(Types.INTEGER, this.rs.getInt("DATA_TYPE"));
			assertEquals("MEDIUMINT UNSIGNED", this.rs.getString("TYPE_NAME")
					.toUpperCase(Locale.US));

			//
			// The following test is harmless in the 3.1 driver, but
			// is needed for the 5.0 driver, so we'll leave it here
			//
			if (versionMeetsMinimum(5, 0, 14)) {
				Connection infoSchemConn = null;

				try {
					Properties props = new Properties();
					props.setProperty("useInformationSchema", "true");

					infoSchemConn = getConnectionWithProps(props);

					this.rs = infoSchemConn.getMetaData().getColumns(
							infoSchemConn.getCatalog(), null, "testBug14562",
							"%field");

					assertTrue(this.rs.next());

					assertEquals(Types.INTEGER, this.rs.getInt("DATA_TYPE"));
					assertEquals("MEDIUMINT", this.rs.getString("TYPE_NAME")
							.toUpperCase(Locale.US));

					assertTrue(this.rs.next());

					assertEquals(Types.INTEGER, this.rs.getInt("DATA_TYPE"));
					assertEquals("MEDIUMINT UNSIGNED", this.rs.getString(
							"TYPE_NAME").toUpperCase(Locale.US));

				} finally {
					if (infoSchemConn != null) {
						infoSchemConn.close();
					}
				}
			}
		} finally {

		}
	}

	public void testBug15604() throws Exception {
		createTable("testBug15604_date_cal", "(field1 DATE)");
		Properties props = new Properties();
		props.setProperty("useLegacyDatetimeCode", "false");
		props.setProperty("sessionVariables", "time_zone='America/Chicago'");
		
		Connection nonLegacyConn = getConnectionWithProps(props);
		
		Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		
		cal.set(Calendar.YEAR, 2005);
	    cal.set(Calendar.MONTH, 4);
	    cal.set(Calendar.DAY_OF_MONTH, 15);
	    cal.set(Calendar.HOUR_OF_DAY, 0);
	    cal.set(Calendar.MINUTE, 0);
	    cal.set(Calendar.SECOND, 0);
	    cal.set(Calendar.MILLISECOND, 0);
	    
	    java.sql.Date sqlDate = new java.sql.Date(cal.getTime().getTime());

	    Calendar cal2 = Calendar.getInstance();
	    cal2.setTime(sqlDate);
	    System.out.println(new java.sql.Date(cal2.getTime().getTime()));
	    this.pstmt = nonLegacyConn.prepareStatement("INSERT INTO testBug15604_date_cal VALUES (?)");
	    
	    this.pstmt.setDate(1, sqlDate, cal);
	    this.pstmt.executeUpdate();
	    this.rs = nonLegacyConn.createStatement().executeQuery("SELECT field1 FROM testBug15604_date_cal");
	    this.rs.next();

	    assertEquals(sqlDate.getTime(), this.rs.getDate(1, cal).getTime());
	}
	
	public void testBug14897() throws Exception {
		createTable("table1", "(id int, name_id int)");
		createTable("table2", "(id int)");
		createTable(
				"lang_table",
				"(id int, en varchar(255) CHARACTER SET utf8, cz varchar(255) CHARACTER SET utf8)");

		this.stmt.executeUpdate("insert into table1 values (0, 0)");
		this.stmt.executeUpdate("insert into table2 values (0)");
		this.stmt
				.executeUpdate("insert into lang_table values (0, 'abcdef', 'ghijkl')");
		this.rs = this.stmt
				.executeQuery("select a.id, b.id, c.en, c.cz from table1 as a, table2 as b, lang_table as c where a.id = b.id and a.name_id = c.id");
		assertTrue(this.rs.next());
		this.rs.getString("c.cz");

		this.rs = this.stmt
				.executeQuery("select table1.*, table2.* FROM table1, table2");
		this.rs.findColumn("table1.id");
		this.rs.findColumn("table2.id");
	}

	/**
	 * Tests fix for BUG#14609 - Exception thrown for new decimal type when
	 * using updatable result sets.
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug14609() throws Exception {
		if (versionMeetsMinimum(5, 0)) {
			createTable("testBug14609",
					"(field1 int primary key, field2 decimal)");
			this.stmt.executeUpdate("INSERT INTO testBug14609 VALUES (1, 1)");

			PreparedStatement updatableStmt = this.conn.prepareStatement(
					"SELECT field1, field2 FROM testBug14609",
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_UPDATABLE);

			try {
				this.rs = updatableStmt.executeQuery();
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
	}

	/**
	 * Tests fix for BUG#16169 - ResultSet.getNativeShort() causes stack
	 * overflow error via recurisve calls.
	 * 
	 * @throws Exception
	 *             if the tests fails
	 */
	public void testBug16169() throws Exception {
		createTable("testBug16169", "(field1 smallint)");

		try {

			this.stmt
					.executeUpdate("INSERT INTO testBug16169 (field1) VALUES (0)");

			this.pstmt = this.conn
					.prepareStatement("SELECT * FROM testBug16169");
			this.rs = this.pstmt.executeQuery();
			assertTrue(this.rs.next());

			assertEquals(0, ((Integer) rs.getObject("field1")).intValue());
		} finally {
			if (this.rs != null) {
				ResultSet toCloseRs = this.rs;
				this.rs = null;
				toCloseRs.close();
			}

			if (this.pstmt != null) {
				PreparedStatement toCloseStmt = this.pstmt;
				this.pstmt = null;
				toCloseStmt.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#16841 - updatable result set doesn't return
	 * AUTO_INCREMENT values for insertRow() when multiple column primary keys
	 * are used.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug16841() throws Exception {

		createTable("testBug16841", "(" + "CID int( 20 ) NOT NULL default '0',"
				+ "OID int( 20 ) NOT NULL AUTO_INCREMENT ,"
				+ "PatientID int( 20 ) default NULL ,"
				+ "PRIMARY KEY ( CID , OID ) ," + "KEY OID ( OID ) ,"
				+ "KEY Path ( CID, PatientID)" + ")", "MYISAM");

		String sSQLQuery = "SELECT * FROM testBug16841 WHERE 1 = 0";
		Statement updStmt = null;

		try {
			updStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_UPDATABLE);

			this.rs = updStmt.executeQuery(sSQLQuery);

			this.rs.moveToInsertRow();

			this.rs.updateInt("CID", 1);
			this.rs.updateInt("PatientID", 1);

			this.rs.insertRow();

			this.rs.last();
			assertEquals(1, this.rs.getInt("OID"));
		} finally {
			if (this.rs != null) {
				ResultSet toClose = this.rs;
				this.rs = null;
				toClose.close();
			}

			if (updStmt != null) {
				updStmt.close();
			}
		}
	}

	/**
	 * Tests fix for BUG#17450 - ResultSet.wasNull() not always reset correctly
	 * for booleans when done via conversion for server-side prepared
	 * statements.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug17450() throws Exception {
		if (versionMeetsMinimum(4, 1, 0)) {
			createTable("testBug17450", "(FOO VARCHAR(100), BAR CHAR NOT NULL)");

			this.stmt
					.execute("insert into testBug17450 (foo,bar) values ('foo',true)");
			this.stmt
					.execute("insert into testBug17450 (foo,bar) values (null,true)");

			this.pstmt = this.conn
					.prepareStatement("select * from testBug17450 where foo=?");
			this.pstmt.setString(1, "foo");
			this.rs = this.pstmt.executeQuery();
			checkResult17450();

			this.pstmt = this.conn
					.prepareStatement("select * from testBug17450 where foo is null");
			this.rs = this.pstmt.executeQuery();
			checkResult17450();

			this.rs = this.stmt
					.executeQuery("select * from testBug17450 where foo='foo'");
			checkResult17450();

			this.rs = this.stmt
					.executeQuery("select * from testBug17450 where foo is null");
			checkResult17450();
		}
	}

	/**
	 * Tests fix for BUG#19282 - ResultSet.wasNull() returns incorrect value
	 * when extracting native string from server-side prepared statement
	 * generated result set.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug19282() throws Exception {
		createTable("testBug19282", "(field1 VARCHAR(32))");
		try {
			this.pstmt = this.conn
					.prepareStatement("SELECT field1 FROM testBug19282");
			this.stmt
					.executeUpdate("INSERT INTO testBug19282 VALUES ('abcdefg')");

			this.rs = this.pstmt.executeQuery();
			this.rs.next();
			assertEquals(false, this.rs.wasNull());
			this.rs.getString(1);
			assertEquals(false, this.rs.wasNull());
		} finally {
			if (this.rs != null) {
				ResultSet toClose = this.rs;
				this.rs = null;
				toClose.close();
			}

			if (this.pstmt != null) {
				PreparedStatement toClose = this.pstmt;
				this.pstmt = null;
				toClose.close();
			}
		}
	}

	private void checkResult17450() throws Exception {
		this.rs.next();
		this.rs.getString(1);
		boolean bar = this.rs.getBoolean(2);

		assertEquals("field 2 should be true", true, bar);
		assertFalse("wasNull should return false", this.rs.wasNull());
	}

	/**
	 * Tests fix for BUG#
	 * 
	 * @throws Exception
	 */
	public void testBug19568() throws Exception {
		if (versionMeetsMinimum(4, 1, 0)) {
			createTable("testBug19568", "(field1 BOOLEAN,"
					+ (versionMeetsMinimum(5, 0, 0) ? "field2 BIT"
							: "field2 BOOLEAN") + ")");

			this.stmt
					.executeUpdate("INSERT INTO testBug19568 VALUES (1,0), (0, 1)");

			try {
				this.pstmt = this.conn
						.prepareStatement("SELECT field1, field2 FROM testBug19568 ORDER BY field1 DESC");
				this.rs = this.pstmt.executeQuery();

				checkResultsBug19568();

				this.rs = this.stmt
						.executeQuery("SELECT field1, field2 FROM testBug19568 ORDER BY field1 DESC");
				checkResultsBug19568();
			} finally {
				closeMemberJDBCResources();
			}
		}
	}

	private void checkResultsBug19568() throws SQLException {
		// Test all numerical getters, and make sure to alternate true/false
		// across rows so we can catch
		// false-positives if off-by-one errors exist in the column getters.

		for (int i = 0; i < 2; i++) {
			assertTrue(this.rs.next());

			for (int j = 0; j < 2; j++) {
				assertEquals((i == 1 && j == 1) || (i == 0 && j == 0), this.rs
						.getBoolean(j + 1));
				assertEquals(
						((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0),
						this.rs.getBigDecimal(j + 1).intValue());
				assertEquals(
						((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0),
						this.rs.getByte(j + 1));
				assertEquals(
						((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0),
						this.rs.getShort(j + 1));
				assertEquals(
						((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0),
						this.rs.getInt(j + 1));
				assertEquals(
						((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0),
						this.rs.getLong(j + 1));
				assertEquals(
						((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0),
						this.rs.getFloat(j + 1), .1);
				assertEquals(
						((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0),
						this.rs.getDouble(j + 1), .1);
			}
		}
	}

	public void testBug19724() throws Exception {
		if (versionMeetsMinimum(4, 1)) {
			// can't set this via session on 4.0 :(

			createTable("test19724",
					"(col1 INTEGER NOT NULL, col2 VARCHAR(255) NULL, PRIMARY KEY (col1))");

			this.stmt
					.execute("INSERT IGNORE INTO test19724 VALUES (0, 'Blah'),(1,'Boo')");

			Connection ansiConn = null;
			Statement updStmt = null;

			Properties props = new Properties();
			props.setProperty("sessionVariables", "sql_mode=ansi");

			try {
				ansiConn = getConnectionWithProps(props);
				updStmt = ansiConn.createStatement(
						ResultSet.TYPE_SCROLL_INSENSITIVE,
						ResultSet.CONCUR_UPDATABLE);
				this.rs = updStmt.executeQuery("SELECT * FROM test19724");

				this.rs.beforeFirst();

				this.rs.next();

				this.rs.updateString("col2", "blah2");
				this.rs.updateRow();
			} finally {
				closeMemberJDBCResources();

				if (ansiConn != null) {
					ansiConn.close();
				}
			}
		}
	}

	private void traverseResultSetBug14562() throws SQLException {
		assertTrue(this.rs.next());

		ResultSetMetaData rsmd = this.rs.getMetaData();
		assertEquals("MEDIUMINT", rsmd.getColumnTypeName(1));
		assertEquals("MEDIUMINT UNSIGNED", rsmd.getColumnTypeName(2));

		assertEquals(Types.INTEGER, rsmd.getColumnType(1));
		assertEquals(Types.INTEGER, rsmd.getColumnType(2));

		assertEquals("java.lang.Integer", rsmd.getColumnClassName(1));
		assertEquals("java.lang.Integer", rsmd.getColumnClassName(2));

		assertEquals(-8388608, this.rs.getInt(1));
		assertEquals(0, this.rs.getInt(2));

		assertEquals("java.lang.Integer", this.rs.getObject(1).getClass()
				.getName());
		assertEquals("java.lang.Integer", this.rs.getObject(2).getClass()
				.getName());

		assertTrue(this.rs.next());

		assertEquals(8388607, this.rs.getInt(1));
		assertEquals(16777215, this.rs.getInt(2));

		assertEquals("java.lang.Integer", this.rs.getObject(1).getClass()
				.getName());
		assertEquals("java.lang.Integer", this.rs.getObject(2).getClass()
				.getName());
	}

	/*
	 * public void testBug16458() throws Exception { createTable("a", "(id
	 * INTEGER NOT NULL, primary key (id)) Type=InnoDB"); createTable("b", "(id
	 * INTEGER NOT NULL, primary key (id)) Type=InnoDB"); createTable("c", "(id
	 * INTEGER NOT NULL, primary key (id)) Type=InnoDB");
	 * 
	 * createTable( "problem_table", "(id int(11) NOT NULL auto_increment," +
	 * "a_id int(11) NOT NULL default '0'," + "b_id int(11) NOT NULL default
	 * '0'," + "c_id int(11) default NULL," + "order_num int(2) NOT NULL default
	 * '0'," + "PRIMARY KEY (id)," + "KEY idx_problem_table__b_id (b_id)," +
	 * "KEY idx_problem_table__a_id (a_id)," + "KEY idx_problem_table__c_id
	 * (c_id)," + "CONSTRAINT fk_problem_table__c FOREIGN KEY (c_id) REFERENCES
	 * c (id)," + "CONSTRAINT fk_problem_table__a FOREIGN KEY (a_id) REFERENCES
	 * a (id)," + "CONSTRAINT fk_problem_table__b FOREIGN KEY (b_id) REFERENCES
	 * b (id)" + ")" + "Type=InnoDB");
	 * 
	 * this.stmt .executeUpdate("INSERT INTO `a` VALUES " +
	 * "(1),(4),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23" +
	 * "),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39" +
	 * "),(40),(41),(42),(43),(45),(46),(47),(48),(49),(50)");
	 * 
	 * this.stmt .executeUpdate("INSERT INTO `b` VALUES " +
	 * "(1),(2),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19" +
	 * "),(20)");
	 * 
	 * this.stmt .executeUpdate("INSERT INTO `c` VALUES " +
	 * "(1),(2),(3),(13),(15),(16),(22),(30),(31),(32),(33),(34),(35),(36),(37),(148),(1" +
	 * "59),(167),(174),(176),(177),(178),(179),(180),(187),(188),(189),(190),(191),(192" +
	 * "),(193),(194),(195),(196),(197),(198),(199),(200),(201),(202),(203),(204),(205)," +
	 * "(206),(207),(208)");
	 * 
	 * this.stmt .executeUpdate("INSERT INTO `problem_table` VALUES " +
	 * "(1,1,1,NULL,1),(2,1,4,NULL,1),(3,1,5,NULL,1),(4,1,8,NULL,1),(5,23,1,NULL,1),(6,2" +
	 * "3,4,NULL,1),(7,24,1,NULL,1),(8,24,2,NULL,1),(9,24,4,NULL,1),(10,25,1,NULL,1),(11" +
	 * ",25,2,NULL,1),(12,25,4,NULL,1),(13,27,1,NULL,1),(14,28,1,NULL,1),(15,29,1,NULL,1" +
	 * "),(16,15,2,NULL,1),(17,15,5,NULL,1),(18,15,8,NULL,1),(19,30,1,NULL,1),(20,31,1,N" +
	 * "ULL,1),(21,31,4,NULL,1),(22,32,2,NULL,1),(23,32,4,NULL,1),(24,32,6,NULL,1),(25,3" +
	 * "2,8,NULL,1),(26,32,10,NULL,1),(27,32,11,NULL,1),(28,32,13,NULL,1),(29,32,16,NULL" +
	 * ",1),(30,32,17,NULL,1),(31,32,18,NULL,1),(32,32,19,NULL,1),(33,32,20,NULL,1),(34," +
	 * "33,15,NULL,1),(35,33,15,NULL,1),(36,32,20,206,1),(96,32,9,NULL,1),(100,47,6,NULL" +
	 * ",1),(101,47,10,NULL,1),(102,47,5,NULL,1),(105,47,19,NULL,1)");
	 * PreparedStatement ps = null;
	 * 
	 * try { ps = conn.prepareStatement("SELECT DISTINCT id,order_num FROM
	 * problem_table WHERE a_id=? FOR UPDATE", ResultSet.TYPE_FORWARD_ONLY,
	 * ResultSet.CONCUR_UPDATABLE);
	 * 
	 * ps.setInt(1, 32);
	 * 
	 * this.rs = ps.executeQuery();
	 * 
	 * while(this.rs.next()) { this.rs.updateInt(3, 51);
	 * 
	 * this.rs.updateRow(); } } finally { if (this.rs != null) { ResultSet
	 * toCloseRs = this.rs; this.rs = null; toCloseRs.close(); }
	 * 
	 * if (ps != null) { PreparedStatement toClosePs = ps; ps = null;
	 * toClosePs.close(); } } }
	 */

	public void testNPEWithUsageAdvisor() throws Exception {
		Connection advisorConn = null;

		try {
			Properties props = new Properties();
			props.setProperty("useUsageAdvisor", "true");

			advisorConn = getConnectionWithProps(props);
			this.pstmt = advisorConn.prepareStatement("SELECT 1");
			this.rs = this.pstmt.executeQuery();
			this.rs.close();
			this.rs = this.pstmt.executeQuery();

		} finally {
		}
	}
	
	public void testAllTypesForNull() throws Exception {
		if (!isRunningOnJdk131()) {
			Properties props = new Properties();
			props.setProperty("jdbcCompliantTruncation", "false");
			props.setProperty("zeroDateTimeBehavior", "round");
			Connection conn2 = getConnectionWithProps(props);
			Statement stmt2 = conn2.createStatement();

			DatabaseMetaData dbmd = this.conn.getMetaData();

			this.rs = dbmd.getTypeInfo();

			boolean firstColumn = true;
			int numCols = 1;
			StringBuffer createStatement = new StringBuffer(
					"CREATE TABLE testAllTypes (");
			List wasDatetimeTypeList = new ArrayList();

			while (this.rs.next()) {
				String dataType = this.rs.getString("TYPE_NAME").toUpperCase();

				boolean wasDateTime = false;

				if (dataType.indexOf("DATE") != -1
						|| dataType.indexOf("TIME") != -1) {
					wasDateTime = true;
				}

				if (!"BOOL".equalsIgnoreCase(dataType)
						&& !"LONG VARCHAR".equalsIgnoreCase(dataType)
						&& !"LONG VARBINARY".equalsIgnoreCase(dataType)
						&& !"ENUM".equalsIgnoreCase(dataType)
						&& !"SET".equalsIgnoreCase(dataType)) {
					wasDatetimeTypeList.add(new Boolean(wasDateTime));
					createStatement.append("\n\t");
					if (!firstColumn) {
						createStatement.append(",");
					} else {
						firstColumn = false;
					}

					createStatement.append("field_");
					createStatement.append(numCols++);
					createStatement.append(" ");

					createStatement.append(dataType);

					if (dataType.indexOf("CHAR") != -1
							|| dataType.indexOf("BINARY") != -1
							&& dataType.indexOf("BLOB") == -1
							&& dataType.indexOf("TEXT") == -1) {
						createStatement.append("(");
						createStatement.append(this.rs.getString("PRECISION"));
						createStatement.append(")");
					}

					createStatement.append(" NULL DEFAULT NULL");
				}
			}

			createStatement.append("\n)");

			stmt2.executeUpdate("DROP TABLE IF EXISTS testAllTypes");

			stmt2.executeUpdate(createStatement.toString());
			StringBuffer insertStatement = new StringBuffer(
					"INSERT INTO testAllTypes VALUES (NULL");
			for (int i = 1; i < numCols - 1; i++) {
				insertStatement.append(", NULL");
			}
			insertStatement.append(")");
			stmt2.executeUpdate(insertStatement.toString());

			this.rs = stmt2.executeQuery("SELECT * FROM testAllTypes");

			testAllFieldsForNull(this.rs);
			this.rs.close();

			this.rs = this.conn.prepareStatement("SELECT * FROM testAllTypes")
					.executeQuery();
			testAllFieldsForNull(this.rs);

			stmt2.executeUpdate("DELETE FROM testAllTypes");

			insertStatement = new StringBuffer(
					"INSERT INTO testAllTypes VALUES (");

			boolean needsNow = ((Boolean) wasDatetimeTypeList.get(0))
					.booleanValue();

			if (needsNow) {
				insertStatement.append("NOW()");
			} else {
				insertStatement.append("'0'");
			}

			for (int i = 1; i < numCols - 1; i++) {
				needsNow = ((Boolean) wasDatetimeTypeList.get(i))
						.booleanValue();
				insertStatement.append(",");
				if (needsNow) {
					insertStatement.append("NOW()");
				} else {
					insertStatement.append("'0'");
				}
			}

			insertStatement.append(")");

			stmt2.executeUpdate(insertStatement.toString());

			this.rs = stmt2.executeQuery("SELECT * FROM testAllTypes");

			testAllFieldsForNotNull(this.rs, wasDatetimeTypeList);
			this.rs.close();

			this.rs = conn2.prepareStatement("SELECT * FROM testAllTypes")
					.executeQuery();
			testAllFieldsForNotNull(this.rs, wasDatetimeTypeList);
		}
	}

	private void testAllFieldsForNull(ResultSet rsToTest) throws Exception {
		ResultSetMetaData rsmd = this.rs.getMetaData();
		int numCols = rsmd.getColumnCount();

		while (rsToTest.next()) {
			for (int i = 0; i < numCols - 1; i++) {
				String typeName = rsmd.getColumnTypeName(i + 1);

				if ("VARBINARY".equalsIgnoreCase(typeName)) {
					System.out.println();
				}

				if (!"BIT".equalsIgnoreCase(typeName)) {
					assertEquals(false, rsToTest.getBoolean(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());

					assertEquals(0, rsToTest.getDouble(i + 1), 0 /* delta */);
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(0, rsToTest.getFloat(i + 1), 0 /* delta */);
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(0, rsToTest.getInt(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(0, rsToTest.getLong(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getObject(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getString(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getAsciiStream(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getBigDecimal(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getBinaryStream(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getBlob(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(0, rsToTest.getByte(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getBytes(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getCharacterStream(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getClob(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getDate(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(0, rsToTest.getShort(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getTime(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getTimestamp(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getUnicodeStream(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
					assertEquals(null, rsToTest.getURL(i + 1));
					assertTrue("for type " + typeName, rsToTest.wasNull());
				}
			}
		}
	}

	private void testAllFieldsForNotNull(ResultSet rsToTest,
			List wasDatetimeTypeList) throws Exception {
		ResultSetMetaData rsmd = this.rs.getMetaData();
		int numCols = rsmd.getColumnCount();

		while (rsToTest.next()) {
			for (int i = 0; i < numCols - 1; i++) {
				boolean wasDatetimeType = ((Boolean) wasDatetimeTypeList.get(i))
						.booleanValue();
				String typeName = rsmd.getColumnTypeName(i + 1);
				int sqlType = rsmd.getColumnType(i + 1);

				if (!"BIT".equalsIgnoreCase(typeName)
						&& sqlType != Types.BINARY
						&& sqlType != Types.VARBINARY
						&& sqlType != Types.LONGVARBINARY) {
					if (!wasDatetimeType) {

						assertEquals(false, rsToTest.getBoolean(i + 1));

						assertTrue(!rsToTest.wasNull());

						assertEquals(0, rsToTest.getDouble(i + 1), 0 /* delta */);
						assertTrue(!rsToTest.wasNull());
						assertEquals(0, rsToTest.getFloat(i + 1), 0 /* delta */);
						assertTrue(!rsToTest.wasNull());
						assertEquals(0, rsToTest.getInt(i + 1));
						assertTrue(!rsToTest.wasNull());
						assertEquals(0, rsToTest.getLong(i + 1));
						assertTrue(!rsToTest.wasNull());
						assertEquals(0, rsToTest.getByte(i + 1));
						assertTrue(!rsToTest.wasNull());
						assertEquals(0, rsToTest.getShort(i + 1));
						assertTrue(!rsToTest.wasNull());
					}

					assertNotNull(rsToTest.getObject(i + 1));
					assertTrue(!rsToTest.wasNull());
					assertNotNull(rsToTest.getString(i + 1));
					assertTrue(!rsToTest.wasNull());
					assertNotNull(rsToTest.getAsciiStream(i + 1));
					assertTrue(!rsToTest.wasNull());

					assertNotNull(rsToTest.getBinaryStream(i + 1));
					assertTrue(!rsToTest.wasNull());
					assertNotNull(rsToTest.getBlob(i + 1));
					assertTrue(!rsToTest.wasNull());
					assertNotNull(rsToTest.getBytes(i + 1));
					assertTrue(!rsToTest.wasNull());
					assertNotNull(rsToTest.getCharacterStream(i + 1));
					assertTrue(!rsToTest.wasNull());
					assertNotNull(rsToTest.getClob(i + 1));
					assertTrue(!rsToTest.wasNull());

					String columnClassName = rsmd.getColumnClassName(i + 1);

					boolean canBeUsedAsDate = !("java.lang.Boolean"
							.equals(columnClassName)
							|| "java.lang.Double".equals(columnClassName)
							|| "java.lang.Float".equals(columnClassName)
							|| "java.lang.Real".equals(columnClassName) || "java.math.BigDecimal"
							.equals(columnClassName));

					if (canBeUsedAsDate) {
						assertNotNull(rsToTest.getDate(i + 1));
						assertTrue(!rsToTest.wasNull());
						assertNotNull(rsToTest.getTime(i + 1));
						assertTrue(!rsToTest.wasNull());
						assertNotNull(rsToTest.getTimestamp(i + 1));
						assertTrue(!rsToTest.wasNull());
					}

					assertNotNull(rsToTest.getUnicodeStream(i + 1));
					assertTrue(!rsToTest.wasNull());

					try {
						if (!isRunningOnJdk131()) {
							assertNotNull(rsToTest.getURL(i + 1));
						}
					} catch (SQLException sqlEx) {
						assertTrue(sqlEx.getMessage().indexOf("URL") != -1);
					}

					assertTrue(!rsToTest.wasNull());
				}
			}
		}
	}

	public void testNPEWithStatementsAndTime() throws Exception {
		try {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testNPETime");
			this.stmt
					.executeUpdate("CREATE TABLE testNPETime (field1 TIME NULL, field2 DATETIME NULL, field3 DATE NULL)");
			this.stmt
					.executeUpdate("INSERT INTO testNPETime VALUES (null, null, null)");
			this.pstmt = this.conn
					.prepareStatement("SELECT field1, field2, field3 FROM testNPETime");
			this.rs = this.pstmt.executeQuery();
			this.rs.next();

			for (int i = 0; i < 3; i++) {
				assertEquals(null, this.rs.getTime(i + 1));
				assertEquals(true, this.rs.wasNull());
			}

			for (int i = 0; i < 3; i++) {
				assertEquals(null, this.rs.getTimestamp(i + 1));
				assertEquals(true, this.rs.wasNull());
			}

			for (int i = 0; i < 3; i++) {
				assertEquals(null, this.rs.getDate(i + 1));
				assertEquals(true, this.rs.wasNull());
			}
		} finally {
			this.stmt.executeUpdate("DROP TABLE IF EXISTS testNPETime");
		}
	}

	public void testEmptyStringsWithNumericGetters() throws Exception {
		try {
			createTable("emptyStringTable", "(field1 char(32))");
			this.stmt.executeUpdate("INSERT INTO emptyStringTable VALUES ('')");
			this.rs = this.stmt
					.executeQuery("SELECT field1 FROM emptyStringTable");
			assertTrue(this.rs.next());
			createTable("emptyStringTable", "(field1 char(32))");
			this.stmt.executeUpdate("INSERT INTO emptyStringTable VALUES ('')");

			this.rs = this.stmt
					.executeQuery("SELECT field1 FROM emptyStringTable");
			assertTrue(this.rs.next());
			checkEmptyConvertToZero();

			this.rs = this.conn.prepareStatement(
					"SELECT field1 FROM emptyStringTable").executeQuery();
			assertTrue(this.rs.next());
			checkEmptyConvertToZero();

			Properties props = new Properties();
			props.setProperty("useFastIntParsing", "false");

			Connection noFastIntParseConn = getConnectionWithProps(props);
			Statement noFastIntStmt = noFastIntParseConn.createStatement();

			this.rs = noFastIntStmt
					.executeQuery("SELECT field1 FROM emptyStringTable");
			assertTrue(this.rs.next());
			checkEmptyConvertToZero();

			this.rs = noFastIntParseConn.prepareStatement(
					"SELECT field1 FROM emptyStringTable").executeQuery();
			assertTrue(this.rs.next());
			checkEmptyConvertToZero();

			//
			// Now, be more pedantic....
			//

			props = new Properties();
			props.setProperty("emptyStringsConvertToZero", "false");

			Connection pedanticConn = getConnectionWithProps(props);
			Statement pedanticStmt = pedanticConn.createStatement();

			this.rs = pedanticStmt
					.executeQuery("SELECT field1 FROM emptyStringTable");
			assertTrue(this.rs.next());

			checkEmptyConvertToZeroException();

			this.rs = pedanticConn.prepareStatement(
					"SELECT field1 FROM emptyStringTable").executeQuery();
			assertTrue(this.rs.next());
			checkEmptyConvertToZeroException();

			props = new Properties();
			props.setProperty("emptyStringsConvertToZero", "false");
			props.setProperty("useFastIntParsing", "false");

			pedanticConn = getConnectionWithProps(props);
			pedanticStmt = pedanticConn.createStatement();

			this.rs = pedanticStmt
					.executeQuery("SELECT field1 FROM emptyStringTable");
			assertTrue(this.rs.next());

			checkEmptyConvertToZeroException();

			this.rs = pedanticConn.prepareStatement(
					"SELECT field1 FROM emptyStringTable").executeQuery();
			assertTrue(this.rs.next());
			checkEmptyConvertToZeroException();

		} finally {
			if (this.rs != null) {
				this.rs.close();

				this.rs = null;
			}
		}
	}

	public void testNegativeOneIsTrue() throws Exception {
		if (!versionMeetsMinimum(5, 0, 3)) {
			String tableName = "testNegativeOneIsTrue";
			Connection tinyInt1IsBitConn = null;

			try {
				createTable(tableName, "(field1 BIT)");
				this.stmt.executeUpdate("INSERT INTO " + tableName
						+ " VALUES (-1)");

				Properties props = new Properties();
				props.setProperty("tinyInt1isBit", "true");
				tinyInt1IsBitConn = getConnectionWithProps(props);

				this.rs = tinyInt1IsBitConn.createStatement().executeQuery(
						"SELECT field1 FROM " + tableName);
				assertTrue(this.rs.next());
				assertEquals(true, this.rs.getBoolean(1));

				this.rs = tinyInt1IsBitConn.prepareStatement(
						"SELECT field1 FROM " + tableName).executeQuery();
				assertTrue(this.rs.next());
				assertEquals(true, this.rs.getBoolean(1));

			} finally {
				if (tinyInt1IsBitConn != null) {
					tinyInt1IsBitConn.close();
				}
			}
		}
	}

	/**
	 * @throws SQLException
	 */
	private void checkEmptyConvertToZero() throws SQLException {
		assertEquals(0, this.rs.getByte(1));
		assertEquals(0, this.rs.getShort(1));
		assertEquals(0, this.rs.getInt(1));
		assertEquals(0, this.rs.getLong(1));
		assertEquals(0, this.rs.getFloat(1), 0.1);
		assertEquals(0, this.rs.getDouble(1), 0.1);
		assertEquals(0, this.rs.getBigDecimal(1).intValue());
	}

	/**
	 * 
	 */
	private void checkEmptyConvertToZeroException() {
		try {
			assertEquals(0, this.rs.getByte(1));
			fail("Should've thrown an exception!");
		} catch (SQLException sqlEx) {
			assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST,
					sqlEx.getSQLState());
		}
		try {
			assertEquals(0, this.rs.getShort(1));
			fail("Should've thrown an exception!");
		} catch (SQLException sqlEx) {
			assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST,
					sqlEx.getSQLState());
		}
		try {
			assertEquals(0, this.rs.getInt(1));
			fail("Should've thrown an exception!");
		} catch (SQLException sqlEx) {
			assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST,
					sqlEx.getSQLState());
		}
		try {
			assertEquals(0, this.rs.getLong(1));
			fail("Should've thrown an exception!");
		} catch (SQLException sqlEx) {
			assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST,
					sqlEx.getSQLState());
		}
		try {
			assertEquals(0, this.rs.getFloat(1), 0.1);
			fail("Should've thrown an exception!");
		} catch (SQLException sqlEx) {
			assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST,
					sqlEx.getSQLState());
		}
		try {
			assertEquals(0, this.rs.getDouble(1), 0.1);
			fail("Should've thrown an exception!");
		} catch (SQLException sqlEx) {
			assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST,
					sqlEx.getSQLState());
		}
		try {
			assertEquals(0, this.rs.getBigDecimal(1).intValue());
			fail("Should've thrown an exception!");
		} catch (SQLException sqlEx) {
			assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST,
					sqlEx.getSQLState());
		}
	}

	/**
	 * Tests fix for BUG#10485, SQLException thrown when retrieving YEAR(2) with
	 * ResultSet.getString().
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug10485() throws Exception {
		String tableName = "testBug10485";

		Calendar nydCal = null;

		if (((com.mysql.jdbc.Connection) this.conn)
				.getUseGmtMillisForDatetimes()) {
			nydCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
		} else {
			nydCal = Calendar.getInstance();
		}

		nydCal.set(2005, 0, 1, 0, 0, 0);

		Date newYears2005 = new Date(nydCal.getTime().getTime());

		createTable(tableName, "(field1 YEAR(2))");
		this.stmt.executeUpdate("INSERT INTO " + tableName + " VALUES ('05')");

		this.rs = this.stmt.executeQuery("SELECT field1 FROM " + tableName);
		assertTrue(this.rs.next());

		assertEquals(newYears2005.toString(), this.rs.getString(1));

		this.rs = this.conn.prepareStatement("SELECT field1 FROM " + tableName)
				.executeQuery();
		assertTrue(this.rs.next());
		assertEquals(newYears2005.toString(), this.rs.getString(1));

		Properties props = new Properties();
		props.setProperty("yearIsDateType", "false");

		Connection yearShortConn = getConnectionWithProps(props);
		this.rs = yearShortConn.createStatement().executeQuery(
				"SELECT field1 FROM " + tableName);
		assertTrue(this.rs.next());
		assertEquals("05", this.rs.getString(1));

		this.rs = yearShortConn.prepareStatement(
				"SELECT field1 FROM " + tableName).executeQuery();
		assertTrue(this.rs.next());
		assertEquals("05", this.rs.getString(1));

		if (versionMeetsMinimum(5, 0)) {
			try {
				this.stmt
						.executeUpdate("DROP PROCEDURE IF EXISTS testBug10485");
				this.stmt
						.executeUpdate("CREATE PROCEDURE testBug10485()\nBEGIN\nSELECT field1 FROM "
								+ tableName + ";\nEND");

				this.rs = this.conn.prepareCall("{CALL testBug10485()}")
						.executeQuery();
				assertTrue(this.rs.next());
				assertEquals(newYears2005.toString(), this.rs.getString(1));

				this.rs = yearShortConn.prepareCall("{CALL testBug10485()}")
						.executeQuery();
				assertTrue(this.rs.next());
				assertEquals("05", this.rs.getString(1));
			} finally {
				this.stmt
						.executeUpdate("DROP PROCEDURE IF EXISTS testBug10485");
			}
		}
	}

	/**
	 * Tests fix for BUG#11552, wrong values returned from server-side prepared
	 * statements if values are unsigned.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug11552() throws Exception {
		try {
			createTable(
					"testBug11552",
					"(field1 INT UNSIGNED, field2 TINYINT UNSIGNED, field3 SMALLINT UNSIGNED, field4 BIGINT UNSIGNED)");
			this.stmt
					.executeUpdate("INSERT INTO testBug11552 VALUES (2, 2, 2, 2), (4294967294, 255, 32768, 18446744073709551615 )");
			this.rs = this.conn
					.prepareStatement(
							"SELECT field1, field2, field3, field4 FROM testBug11552 ORDER BY field1 ASC")
					.executeQuery();
			this.rs.next();
			assertEquals("2", this.rs.getString(1));
			assertEquals("2", this.rs.getObject(1).toString());
			assertEquals("2", String.valueOf(this.rs.getLong(1)));

			assertEquals("2", this.rs.getString(2));
			assertEquals("2", this.rs.getObject(2).toString());
			assertEquals("2", String.valueOf(this.rs.getLong(2)));

			assertEquals("2", this.rs.getString(3));
			assertEquals("2", this.rs.getObject(3).toString());
			assertEquals("2", String.valueOf(this.rs.getLong(3)));

			assertEquals("2", this.rs.getString(4));
			assertEquals("2", this.rs.getObject(4).toString());
			assertEquals("2", String.valueOf(this.rs.getLong(4)));

			this.rs.next();

			assertEquals("4294967294", this.rs.getString(1));
			assertEquals("4294967294", this.rs.getObject(1).toString());
			assertEquals("4294967294", String.valueOf(this.rs.getLong(1)));

			assertEquals("255", this.rs.getString(2));
			assertEquals("255", this.rs.getObject(2).toString());
			assertEquals("255", String.valueOf(this.rs.getLong(2)));

			assertEquals("32768", this.rs.getString(3));
			assertEquals("32768", this.rs.getObject(3).toString());
			assertEquals("32768", String.valueOf(this.rs.getLong(3)));

			assertEquals("18446744073709551615", this.rs.getString(4));
			assertEquals("18446744073709551615", this.rs.getObject(4)
					.toString());
		} finally {
			if (this.rs != null) {
				this.rs.close();
				this.rs = null;
			}
		}
	}

	/**
	 * Tests correct detection of truncation of non-sig digits.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testTruncationOfNonSigDigits() throws Exception {
		if (versionMeetsMinimum(4, 1, 0)) {
			createTable("testTruncationOfNonSigDigits",
					"(field1 decimal(12,2), field2 varchar(2))", "Innodb");

			this.stmt
					.executeUpdate("INSERT INTO testTruncationOfNonSigDigits VALUES (123456.2345, 'ab')");

			try {
				this.stmt
						.executeUpdate("INSERT INTO testTruncationOfNonSigDigits VALUES (1234561234561.2345, 'ab')");
				fail("Should have thrown a truncation error");
			} catch (MysqlDataTruncation truncEx) {
				// We expect this
			}

			try {
				this.stmt
						.executeUpdate("INSERT INTO testTruncationOfNonSigDigits VALUES (1234.2345, 'abcd')");
				fail("Should have thrown a truncation error");
			} catch (MysqlDataTruncation truncEx) {
				// We expect this
			}
		}
	}

	/**
	 * Tests fix for BUG#20479 - Updatable result set throws ClassCastException
	 * when there is row data and moveToInsertRow() is called.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug20479() throws Exception {
		PreparedStatement updStmt = null;
		
		createTable("testBug20479", "(field1 INT NOT NULL PRIMARY KEY)");
		this.stmt.executeUpdate("INSERT INTO testBug20479 VALUES (2), (3), (4)");
		
		try {
			updStmt = this.conn.prepareStatement("SELECT * FROM testBug20479 Where field1 > ? ORDER BY field1",
			ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			updStmt.setInt(1,1);			
			this.rs = updStmt.executeQuery();
			this.rs.next();
			this.rs.moveToInsertRow();
			this.rs.updateInt(1, 45);
			this.rs.insertRow();
			this.rs.moveToCurrentRow();
			assertEquals(2, this.rs.getInt(1));
			this.rs.next();
			this.rs.next();
			this.rs.next();
			assertEquals(45, this.rs.getInt(1));
		} finally {
			if (this.rs != null) {
				this.rs.close();
				this.rs = null;
			}
			
			if (updStmt != null) {
				updStmt.close();
			}		
		}
	}

	/**
	 * Tests fix for BUG#20485 - Updatable result set that contains
	 * a BIT column fails when server-side prepared statements are used.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug20485() throws Exception {
		if (!versionMeetsMinimum(5, 0)) {
			return;
		}
		
		PreparedStatement updStmt = null;
		
		createTable("testBug20485", "(field1 INT NOT NULL PRIMARY KEY, field2 BIT)");
		this.stmt.executeUpdate("INSERT INTO testBug20485 VALUES (2, 1), (3, 1), (4, 1)");
		
		try {
			updStmt = this.conn.prepareStatement("SELECT * FROM testBug20485 ORDER BY field1",
			ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			this.rs = updStmt.executeQuery();
		} finally {
			if (this.rs != null) {
				this.rs.close();
				this.rs = null;
			}
			
			if (updStmt != null) {
				updStmt.close();
			}		
		}
	}

	/** 
	 * Tests fix for BUG#20306 - ResultSet.getShort() for UNSIGNED TINYINT
	 * returns incorrect values when using server-side prepared statements.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug20306() throws Exception {
		createTable("testBug20306", "(field1 TINYINT UNSIGNED, field2 TINYINT UNSIGNED)");
		this.stmt.executeUpdate("INSERT INTO testBug20306 VALUES (2, 133)");
		try {
			this.pstmt = this.conn.prepareStatement("SELECT field1, field2 FROM testBug20306");
			this.rs = this.pstmt.executeQuery();
			this.rs.next();
			checkBug20306();
			
			this.rs = this.stmt.executeQuery("SELECT field1, field2 FROM testBug20306");
			this.rs.next();
			checkBug20306();
			
		} finally {
			closeMemberJDBCResources();
		}
	}

	private void checkBug20306() throws Exception {
		assertEquals(2, this.rs.getByte(1));
		assertEquals(2, this.rs.getInt(1));
		assertEquals(2, this.rs.getShort(1));
		assertEquals(2, this.rs.getLong(1));
		assertEquals(2.0, this.rs.getFloat(1), 0);
		assertEquals(2.0, this.rs.getDouble(1), 0);
		assertEquals(2, this.rs.getBigDecimal(1).intValue());

		assertEquals(133, this.rs.getInt(2));
		assertEquals(133, this.rs.getShort(2));
		assertEquals(133, this.rs.getLong(2));
		assertEquals(133.0, this.rs.getFloat(2), 0);
		assertEquals(133.0, this.rs.getDouble(2), 0);
		assertEquals(133, this.rs.getBigDecimal(2).intValue());
	}

	/**
	 * Tests fix for BUG#21062 - ResultSet.getSomeInteger() doesn't work for BIT(>1)
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug21062() throws Exception {
		if (versionMeetsMinimum(5, 0, 5)) {
			createTable("testBug21062", "(bit_7_field BIT(7), bit_31_field BIT(31), bit_12_field BIT(12))");
			
			int max7Bits = 127;
			long max31Bits = 2147483647L;
			int max12Bits = 4095;
			
			this.stmt.executeUpdate("INSERT INTO testBug21062 VALUES (" + max7Bits + "," + max31Bits + "," + max12Bits + ")");
			
			this.rs = this.stmt.executeQuery("SELECT * FROM testBug21062");
			
			this.rs.next();
	
			assertEquals(127, this.rs.getInt(1));
			assertEquals(127, this.rs.getShort(1));
			assertEquals(127, this.rs.getLong(1));
			
			assertEquals(2147483647, this.rs.getInt(2));
			assertEquals(2147483647, this.rs.getLong(2));
			
			assertEquals(4095, this.rs.getInt(3));
			assertEquals(4095, this.rs.getShort(3));
			assertEquals(4095, this.rs.getLong(3));
		}
	}

	/**
	 * Tests fix for BUG#18880 - ResultSet.getFloatFromString() can't retrieve
	 * values near Float.MIN/MAX_VALUE.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug18880() throws Exception {
		try {
			this.rs = this.stmt.executeQuery("SELECT 3.4E38,1.4E-45");
			this.rs.next();
			this.rs.getFloat(1);
			this.rs.getFloat(2);
		} finally {
			closeMemberJDBCResources();
		}
	}

	/**
	 * Tests fix for BUG#15677, wrong values returned from getShort() if SQL
	 * values are tinyint unsigned.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug15677() throws Exception {
		try {
			createTable("testBug15677", "(id BIGINT, field1 TINYINT UNSIGNED)");
			this.stmt
					.executeUpdate("INSERT INTO testBug15677 VALUES (1, 0), (2, 127), (3, 128), (4, 255)");
			this.rs = this.conn.prepareStatement(
					"SELECT field1 FROM testBug15677 ORDER BY id ASC")
					.executeQuery();
			this.rs.next();
			assertEquals("0", this.rs.getString(1));
			assertEquals("0", this.rs.getObject(1).toString());
			assertEquals("0", String.valueOf(this.rs.getShort(1)));
	
			this.rs.next();
			assertEquals("127", this.rs.getString(1));
			assertEquals("127", this.rs.getObject(1).toString());
			assertEquals("127", String.valueOf(this.rs.getShort(1)));
	
			this.rs.next();
			assertEquals("128", this.rs.getString(1));
			assertEquals("128", this.rs.getObject(1).toString());
			assertEquals("128", String.valueOf(this.rs.getShort(1)));
	
			this.rs.next();
			assertEquals("255", this.rs.getString(1));
			assertEquals("255", this.rs.getObject(1).toString());
			assertEquals("255", String.valueOf(this.rs.getShort(1)));
		} finally {
			closeMemberJDBCResources();
		}
	}
	
	public void testBooleans() throws Exception {
		if (versionMeetsMinimum(5, 0)) {
			try {
				createTable("testBooleans",
						"(ob int, field1 BOOLEAN, field2 TINYINT, field3 SMALLINT, field4 INT, field5 MEDIUMINT, field6 BIGINT, field7 FLOAT, field8 DOUBLE, field9 DECIMAL, field10 VARCHAR(32), field11 BINARY(3), field12 VARBINARY(3),  field13 BLOB)");
				this.pstmt = this.conn
						.prepareStatement("INSERT INTO testBooleans VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
	
				this.pstmt.setInt(1, 1);
				this.pstmt.setBoolean(2, false);
				this.pstmt.setByte(3, (byte)0);
				this.pstmt.setInt(4, 0);
				this.pstmt.setInt(5, 0);
				this.pstmt.setInt(6, 0);
				this.pstmt.setLong(7, 0);
				this.pstmt.setFloat(8, 0);
				this.pstmt.setDouble(9, 0);
				this.pstmt.setBigDecimal(10, new BigDecimal("0"));
				this.pstmt.setString(11, "false");
				this.pstmt.setBytes(12, new byte[] { 0 });
				this.pstmt.setBytes(13, new byte[] { 0 });
				this.pstmt.setBytes(14, new byte[] { 0 });
				
				this.pstmt.executeUpdate();
	
				this.pstmt.setInt(1, 2);
				this.pstmt.setBoolean(2, true);
				this.pstmt.setByte(3, (byte)1);
				this.pstmt.setInt(4, 1);
				this.pstmt.setInt(5, 1);
				this.pstmt.setInt(6, 1);
				this.pstmt.setLong(7, 1);
				this.pstmt.setFloat(8, 1);
				this.pstmt.setDouble(9, 1);
				this.pstmt.setBigDecimal(10, new BigDecimal("1"));
				this.pstmt.setString(11, "true");
				this.pstmt.setBytes(12, new byte[] { 1 });
				this.pstmt.setBytes(13, new byte[] { 1 });
				this.pstmt.setBytes(14, new byte[] { 1 });
				this.pstmt.executeUpdate();
	
				this.pstmt.setInt(1, 3);
				this.pstmt.setBoolean(2, true);
				this.pstmt.setByte(3, (byte)1);
				this.pstmt.setInt(4, 1);
				this.pstmt.setInt(5, 1);
				this.pstmt.setInt(6, 1);
				this.pstmt.setLong(7, 1);
				this.pstmt.setFloat(8, 1);
				this.pstmt.setDouble(9, 1);
				this.pstmt.setBigDecimal(10, new BigDecimal("1"));
				this.pstmt.setString(11, "true");
				this.pstmt.setBytes(12, new byte[] { 2 });
				this.pstmt.setBytes(13, new byte[] { 2 });
				this.pstmt.setBytes(14, new byte[] { 2 });
				this.pstmt.executeUpdate();
	
				this.pstmt.setInt(1, 4);
				this.pstmt.setBoolean(2, true);
				this.pstmt.setByte(3, (byte)1);
				this.pstmt.setInt(4, 1);
				this.pstmt.setInt(5, 1);
				this.pstmt.setInt(6, 1);
				this.pstmt.setLong(7, 1);
				this.pstmt.setFloat(8, 1);
				this.pstmt.setDouble(9, 1);
				this.pstmt.setBigDecimal(10, new BigDecimal("1"));
				this.pstmt.setString(11, "true");
				this.pstmt.setBytes(12, new byte[] { -1 });
				this.pstmt.setBytes(13, new byte[] { -1 });
				this.pstmt.setBytes(14, new byte[] { -1 });
				this.pstmt.executeUpdate();
	
				this.pstmt.setInt(1, 5);
				this.pstmt.setBoolean(2, false);
				this.pstmt.setByte(3, (byte)0);
				this.pstmt.setInt(4, 0);
				this.pstmt.setInt(5, 0);
				this.pstmt.setInt(6, 0);
				this.pstmt.setLong(7, 0);
				this.pstmt.setFloat(8, 0);
				this.pstmt.setDouble(9, 0);
				this.pstmt.setBigDecimal(10, new BigDecimal("0"));
				this.pstmt.setString(11, "false");
				this.pstmt.setBytes(12, new byte[] { 0, 0 });
				this.pstmt.setBytes(13, new byte[] { 0, 0 });
				this.pstmt.setBytes(14, new byte[] { 0, 0 });
				this.pstmt.executeUpdate();
	
				this.pstmt.setInt(1, 6);
				this.pstmt.setBoolean(2, true);
				this.pstmt.setByte(3, (byte)1);
				this.pstmt.setInt(4, 1);
				this.pstmt.setInt(5, 1);
				this.pstmt.setInt(6, 1);
				this.pstmt.setLong(7, 1);
				this.pstmt.setFloat(8, 1);
				this.pstmt.setDouble(9, 1);
				this.pstmt.setBigDecimal(10, new BigDecimal("1"));
				this.pstmt.setString(11, "true");
				this.pstmt.setBytes(12, new byte[] { 1, 0 });
				this.pstmt.setBytes(13, new byte[] { 1, 0 });
				this.pstmt.setBytes(14, new byte[] { 1, 0 });
				this.pstmt.executeUpdate();
	
				this.pstmt.setInt(1, 7);
				this.pstmt.setBoolean(2, false);
				this.pstmt.setByte(3, (byte)0);
				this.pstmt.setInt(4, 0);
				this.pstmt.setInt(5, 0);
				this.pstmt.setInt(6, 0);
				this.pstmt.setLong(7, 0);
				this.pstmt.setFloat(8, 0);
				this.pstmt.setDouble(9, 0);
				this.pstmt.setBigDecimal(10, new BigDecimal("0"));
				this.pstmt.setString(11, "");
				this.pstmt.setBytes(12, new byte[] {});
				this.pstmt.setBytes(13, new byte[] {});
				this.pstmt.setBytes(14, new byte[] {});
				this.pstmt.executeUpdate();
	
				this.rs = this.stmt
						.executeQuery("SELECT field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, field11, field12, field13 FROM testBooleans ORDER BY ob");
	
				boolean[] testVals = new boolean[] { false, true, true, true,
						false, true, false };
	
				int i = 0;
	
				while (this.rs.next()) {
					for (int j = 0; j > 13; j++) {
						assertEquals("For field_" + (j + 1) + ", row " + (i + 1), testVals[i], this.rs
								.getBoolean(j + 1));
					}
					
					i++;
				}
	
				this.rs = this.conn
						.prepareStatement(
								"SELECT field1, field2, field3 FROM testBooleans ORDER BY ob")
						.executeQuery();
	
				i = 0;
	
				while (this.rs.next()) {
					for (int j = 0; j > 13; j++) {
						assertEquals("For field_" + (j + 1) + ", row " + (i + 1), testVals[i], this.rs
								.getBoolean(j + 1));
					}
					
					i++;
				}
			} finally {
				closeMemberJDBCResources();
			}
		}
	}
	
	/**
	 * Tests fix(es) for BUG#21379 - column names don't match metadata
	 * in cases where server doesn't return original column names (functions)
	 * thus breaking compatibility with applications that expect 1-1 mappings
	 * between findColumn() and rsmd.getColumnName().
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug21379() throws Exception {
		try {
			//
			// Test the 1-1 mapping between rs.findColumn() and rsmd.getColumnName()
			// in the case where original column names are not returned,
			// thus preserving pre-C/J 5.0 behavior for these cases
			//
			
			this.rs = this.stmt.executeQuery("SELECT LAST_INSERT_ID() AS id");
			this.rs.next();
			assertEquals("id", this.rs.getMetaData().getColumnName(1));
			assertEquals(1, this.rs.findColumn("id"));
			
			if (versionMeetsMinimum(4, 1)) {
				// 
				// test complete emulation of C/J 3.1 and earlier behavior
				// through configuration option
				//
				
				createTable("testBug21379", "(field1 int)");
				Connection legacyConn = null;
				Statement legacyStmt = null;
				
				try {
					Properties props = new Properties();
					props.setProperty("useOldAliasMetadataBehavior", "true");
					legacyConn = getConnectionWithProps(props);
					legacyStmt = legacyConn.createStatement();
					
					this.rs = legacyStmt.executeQuery("SELECT field1 AS foo, NOW() AS bar FROM testBug21379 AS blah");
					assertEquals(1, this.rs.findColumn("foo"));
					assertEquals(2, this.rs.findColumn("bar"));
					assertEquals("blah", this.rs.getMetaData().getTableName(1));
				} finally {
					if (legacyConn != null) {
						legacyConn.close();
					}
				}
			}
		} finally {
			closeMemberJDBCResources();
		}
	}
	
	/** 
	 * Tests fix for BUG#21814 - time values outside valid range silently wrap
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug21814() throws Exception {
		try {
			try {
				this.rs = this.stmt.executeQuery("SELECT '25:01'");
				this.rs.next();
				this.rs.getTime(1);
				fail("Expected exception");
			} catch (SQLException sqlEx) {
				assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());	
			}
			
			try {
				this.rs = this.stmt.executeQuery("SELECT '23:92'");
				this.rs.next();
				this.rs.getTime(1);
				fail("Expected exception");
			} catch (SQLException sqlEx) {
				assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());	
			}
		} finally {
			closeMemberJDBCResources();
		}
	}
	
	/**
	 * Tests for a server bug - needs to be revisited when the server is fixed.
	 * 
	 * @throws Exception
	 *             if the test fails.
	 */
	public void testBug24710() throws Exception {
		if (!versionMeetsMinimum(6, 0)) {
			return;
		}

		createTable("testBug24710", "(x varbinary(256))");

		try {
			this.stmt
					.executeUpdate("insert into testBug24710(x) values(0x0000000000),"
							+ "(0x1111111111),"
							+ "(0x2222222222),"
							+ "(0x3333333333),"
							+ "(0x4444444444),"
							+ "(0x5555555555),"
							+ "(0x6666666666),"
							+ "(0x7777777777),"
							+ "(0x8888888888),"
							+ "(0x9999999999),"
							+ "(0xaaaaaaaaaa),"
							+ "(0xbbbbbbbbbb),"
							+ "(0xcccccccccc),"
							+ "(0xdddddddddd),"
							+ "(0xeeeeeeeeee),"
							+ "(0xffffffffff)");

			this.rs = this.stmt
					.executeQuery("select t1.x t1x,(select x from testBug24710 t2 where t2.x=t1.x) t2x from testBug24710 t1");

			assertEquals(Types.VARBINARY, this.rs.getMetaData()
					.getColumnType(1));
			assertEquals(Types.VARBINARY, this.rs.getMetaData()
					.getColumnType(2));

			this.rs = ((com.mysql.jdbc.Connection) this.conn)
					.serverPrepareStatement(
							"select t1.x t1x,(select x from testBug24710 t2 where t2.x=t1.x) t2x from testBug24710 t1")
					.executeQuery();

			assertEquals(Types.VARBINARY, this.rs.getMetaData()
					.getColumnType(1));
			assertEquals(Types.VARBINARY, this.rs.getMetaData()
					.getColumnType(2));
		} finally {
			closeMemberJDBCResources();
		}
	}
	
	/**
	 * Tests fix for BUG#25328 - BIT(> 1) is returned as java.lang.String
	 * from ResultSet.getObject() rather than byte[].
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testbug25328() throws Exception {
		if (!versionMeetsMinimum(5, 0)) {
			return;
		}
		
		createTable("testBug25382", "(BINARY_VAL BIT(64) NULL)");

		byte[] bytearr = new byte[8];

		this.pstmt = this.conn
				.prepareStatement("INSERT INTO testBug25382 VALUES(?)");
		try {

			this.pstmt.setObject(1, bytearr, java.sql.Types.BINARY);
			assertEquals(1, this.pstmt.executeUpdate());
			this.pstmt.clearParameters();

			this.rs = this.stmt.executeQuery("Select BINARY_VAL from testBug25382");
			this.rs.next();
			assertEquals(this.rs.getObject(1).getClass(), bytearr.getClass());
		} finally {
			closeMemberJDBCResources();
		}        
	}
	
	/**
	 * Tests fix for BUG#25517 - Statement.setMaxRows() is not effective
	 * on result sets materialized from cursors.
	 * 
	 * @throws Exception if the test fails
	 */
	public void testBug25517() throws Exception {
		Connection fetchConn = null;
		Statement fetchStmt = null;
		
		createTable("testBug25517", "(field1 int)");
		
		StringBuffer insertBuf = new StringBuffer("INSERT INTO testBug25517 VALUES (1)");
		
		for (int i = 0; i < 100; i++) {
			insertBuf.append(",(" + i + ")");
		}
		
		this.stmt.executeUpdate(insertBuf.toString());
		
		try {
			Properties props = new Properties();
			props.setProperty("useServerPrepStmts", "true");
			props.setProperty("useCursorFetch", "true");
		
			fetchConn = getConnectionWithProps(props);
			fetchStmt = fetchConn.createStatement();
			
			//int[] maxRows = new int[] {1, 4, 5, 11, 12, 13, 16, 50, 51, 52, 100};
			int[] fetchSizes = new int[] {1, 4, 10, 25, 100};
			List maxRows = new ArrayList();
			maxRows.add(new Integer(1));
			
			for (int i = 0; i < fetchSizes.length; i++) {
				if (fetchSizes[i] != 1) {
					maxRows.add(new Integer(fetchSizes[i] - 1));
				}
				
				maxRows.add(new Integer(fetchSizes[i]));
				
				if (i != fetchSizes.length - 1) {
					maxRows.add(new Integer(fetchSizes[i] + 1));
				}
			}
			
			for (int fetchIndex = 0; fetchIndex < fetchSizes.length; fetchIndex++) {
				fetchStmt.setFetchSize(fetchSizes[fetchIndex]);
				
				for (int maxRowIndex = 0; maxRowIndex < maxRows.size(); maxRowIndex++) {
					
					int maxRowsToExpect = ((Integer)maxRows.get(maxRowIndex)).intValue();
					fetchStmt.setMaxRows(maxRowsToExpect);
					
					int rowCount = 0;
					
					this.rs = fetchStmt.executeQuery("SELECT * FROM testBug25517");
					
					while (this.rs.next()) {
						rowCount++;
					}
					
					assertEquals(maxRowsToExpect, rowCount);
				}
			}
			
			this.pstmt = fetchConn.prepareStatement("SELECT * FROM testBug25517");
			
			for (int fetchIndex = 0; fetchIndex < fetchSizes.length; fetchIndex++) {
				this.pstmt.setFetchSize(fetchSizes[fetchIndex]);
				
				for (int maxRowIndex = 0; maxRowIndex < maxRows.size(); maxRowIndex++) {
					
					int maxRowsToExpect = ((Integer)maxRows.get(maxRowIndex)).intValue();
					this.pstmt.setMaxRows(maxRowsToExpect);
					
					int rowCount = 0;
					
					this.rs = this.pstmt.executeQuery();
					
					while (this.rs.next()) {
						rowCount++;
					}
					
					assertEquals(maxRowsToExpect, rowCount);
				}
			}
			
		} finally {
			closeMemberJDBCResources();
			
			if (fetchStmt != null) {
				fetchStmt.close();
			}
			
			if (fetchConn != null) {
				fetchConn.close();
			}
		}
	}
	
	/**
	 * Tests fix for BUG#25787 - java.util.Date should be serialized for PreparedStatement.setObject().
	 * 
	 * We add a new configuration option "treatUtilDateAsTimestamp", which is false by default,
	 * as (1) We already had specific behavior to treat java.util.Date as a java.sql.Timestamp because
	 * it's useful to many folks, and (2) that behavior will very likely be in JDBC-post-4.0 as a 
	 * requirement.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug25787() throws Exception {
		createTable("testBug25787", "(MY_OBJECT_FIELD BLOB)");
		
		Connection deserializeConn = null;
		
		Properties props = new Properties();
		props.setProperty("autoDeserialize", "true");
		props.setProperty("treatUtilDateAsTimestamp", "false");
		
		try {
			deserializeConn = getConnectionWithProps(props);
			
			this.pstmt = deserializeConn.prepareStatement("INSERT INTO testBug25787 (MY_OBJECT_FIELD) VALUES (?)");
			java.util.Date dt = new java.util.Date();
			
			this.pstmt.setObject(1, dt);
			this.pstmt.execute();
			
			this.rs = deserializeConn.createStatement().executeQuery("SELECT MY_OBJECT_FIELD FROM testBug25787");
			this.rs.next();
			assertEquals("java.util.Date", this.rs.getObject(1).getClass().getName());
			assertEquals(dt, this.rs.getObject(1));
		} finally {
			closeMemberJDBCResources();
		}
	}
	
	public void testTruncationDisable() throws Exception {
		Properties props = new Properties();
		props.setProperty("jdbcCompliantTruncation", "false");
		Connection truncConn = null;
		
		try {
			truncConn = getConnectionWithProps(props);
			this.rs = truncConn.createStatement().executeQuery("SELECT " + Long.MAX_VALUE);
			this.rs.next();
			this.rs.getInt(1);
		} finally {
			closeMemberJDBCResources();
		}
		
	}
	
	public void testUsageAdvisorOnZeroRowResultSet() throws Exception {
		Connection advisorConn = null;
		Statement advisorStmt = null;
		
		try {
			Properties props = new Properties();
			props.setProperty("useUsageAdvisor", "true");
			
			advisorConn = getConnectionWithProps(props);
			
			advisorStmt = advisorConn.createStatement();
			
			StringBuffer advisorBuf = new StringBuffer();
			StandardLogger.bufferedLog = advisorBuf;
			
			this.rs = advisorStmt.executeQuery("SELECT 1, 2 LIMIT 0");
			this.rs.next();
			this.rs.close();
			
			advisorStmt.close();
			
			advisorStmt = advisorConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, 
					ResultSet.CONCUR_READ_ONLY);
			
			advisorStmt.setFetchSize(Integer.MIN_VALUE);
			
			this.rs = advisorStmt.executeQuery("SELECT 1, 2 LIMIT 0");
			this.rs.next();
			this.rs.close();
			
			StandardLogger.bufferedLog = null;
			
			if (versionMeetsMinimum(5, 0, 2)) {
				advisorConn.close();
				
				props.setProperty("useCursorFetch", "true");
				props.setProperty("useServerPrepStmts", "true");
				
				advisorConn = getConnectionWithProps(props);
				
				advisorStmt = advisorConn.createStatement();
				advisorStmt.setFetchSize(1);
				
				this.rs = advisorStmt.executeQuery("SELECT 1, 2 LIMIT 0");
				advisorBuf = new StringBuffer();
				StandardLogger.bufferedLog = advisorBuf;
				this.rs.next();
				this.rs.close();
			}
			
			assertEquals(
					-1, advisorBuf.toString().indexOf(
					Messages.getString("ResultSet.Possible_incomplete_traversal_of_result_set").substring(0, 10)));
		} finally {
			StandardLogger.bufferedLog = null;
			
			closeMemberJDBCResources();
			
			if (advisorStmt != null) {
				advisorStmt.close();
			}
			
			if (advisorConn != null) {
				advisorConn.close();
			}
		}
	}
	
	public void testBug25894() throws Exception {
    	createTable("bug25894", "("+
    		    "tinyInt_type TINYINT DEFAULT 1,"+
    		    "tinyIntU_type TINYINT UNSIGNED DEFAULT 1,"+
    		    "smallInt_type SMALLINT DEFAULT 1,"+
    		    "smallIntU_type SMALLINT UNSIGNED DEFAULT 1,"+
    		    "mediumInt_type MEDIUMINT DEFAULT 1,"+
    		    "mediumIntU_type MEDIUMINT UNSIGNED DEFAULT 1,"+
    		    "int_type INT DEFAULT 1,"+
    		    "intU_type INT UNSIGNED DEFAULT 1,"+
    		    "bigInt_type BIGINT DEFAULT 1,"+
    		    "bigIntU_type BIGINT UNSIGNED DEFAULT 1"+
   			");");    
	    	try {
	    		this.stmt.executeUpdate("INSERT INTO bug25894 VALUES (-1,1,-1,1,-1,1,-1,1,-1,1)"); 
	    		this.rs = this.stmt.executeQuery("SELECT * FROM bug25894");
	    		java.sql.ResultSetMetaData tblMD = this.rs.getMetaData();
	    		this.rs.first();
	    		for (int i=1; i<tblMD.getColumnCount()+1; i++)
	    		{	
	    			String typesName = "";
	    			switch (tblMD.getColumnType(i)) {
	    			case Types.INTEGER:
	    				typesName = "Types.INTEGER";
	    				break;
	    			case Types.TINYINT:
	    				typesName = "Types.TINYINT";
	    				break;
	    			case Types.BIGINT:
	    				typesName = "Types.BIGINT";
	    				break;
	    			case Types.SMALLINT:
	    				typesName = "Types.SMALLINT";
	    				break;
	    			}
	    			
	    			System.out.println(i + " .fld: " + tblMD.getColumnName(i) + "T: " + typesName + ", MDC: " +
	    					tblMD.getColumnClassName(i) + " " + tblMD.getColumnTypeName(i) + " " +
	    					 ", getObj: " + this.rs.getObject(i).getClass());
	    		}    		
			
		} finally {
			closeMemberJDBCResources();
		}
	}

	/**
	 * Tests fix for BUG#26173 - fetching rows via cursor retrieves
	 * corrupted data.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug26173() throws Exception {
		if (!versionMeetsMinimum(5, 0)) {
			return;
		}
	
		 createTable("testBug26173", 
				 "(fkey int, fdate date, fprice decimal(15, 2), fdiscount decimal(5,3))");
         this.stmt.executeUpdate("insert into testBug26173 values (1, '2007-02-23', 99.9, 0.02)");
		 
         Connection fetchConn = null;
         Statement stmtRead = null;
         
         Properties props = new Properties();
         props.setProperty("useServerPrepStmts", "true");
         props.setProperty("useCursorFetch", "true");
         
         try {
        	 
        	 fetchConn = getConnectionWithProps(props);
        	 stmtRead = fetchConn.createStatement();
             stmtRead.setFetchSize(1000);
          
             this.rs = stmtRead.executeQuery("select extract(year from fdate) as fyear, fprice * (1 - fdiscount) as fvalue from testBug26173");
            
             assertTrue(this.rs.next());
             assertEquals(2007, this.rs.getInt(1));
             assertEquals("97.90200", this.rs.getString(2));          
         } finally {
        	 if (stmtRead != null) {
        		 stmtRead.close();
        	 }
        	 
        	 if (fetchConn != null) {
        		 fetchConn.close();
        	 }
        	 
        	 closeMemberJDBCResources();
         }
	}
	
	/**
	 * Tests fix for BUG#26789 - fast date/time parsing doesn't take into
	 * account 00:00:00 as a legal value.
	 * 
	 * @throws Exception
	 *             if the test fails
	 */
	public void testBug26789() throws Exception {
		try {
			this.rs = this.stmt.executeQuery("SELECT '00:00:00'");
			this.rs.next();
			this.rs.getTime(1);
			assertEquals("00:00:00", this.rs.getTime(1).toString());
			assertEquals("1970-01-01 00:00:00.0", this.rs.getTimestamp(1)
					.toString());
			assertEquals("1970-01-01", this.rs.getDate(1).toString());

			this.rs.close();

			this.rs = this.stmt.executeQuery("SELECT '00/00/0000 00:00:00'");
			this.rs.next();

			try {
				this.rs.getTime(1);
			} catch (SQLException sqlEx) {
				assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx
						.getSQLState());
			}

			try {
				this.rs.getTimestamp(1);
			} catch (SQLException sqlEx) {
				assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx
						.getSQLState());
			}

			try {
				this.rs.getDate(1);
			} catch (SQLException sqlEx) {
				assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx
						.getSQLState());
			}
		} finally {
			closeMemberJDBCResources();
		}
	}
	
	/**
	 * Tests fix for BUG#27317 - column index < 1 returns misleading
	 * error message.
	 * 
	 * @throws Exception if the test fails.
	 */
	public void testBug27317() throws Exception {
		try {
			this.rs = this.stmt.executeQuery("SELECT NULL");
			this.rs.next();
			String messageLowBound = null;

			Method[] getterMethods = ResultSet.class.getMethods();
			Integer zeroIndex = new Integer(0);
			Integer twoIndex = new Integer(2);

			for (int i = 0; i < getterMethods.length; i++) {
				Class[] parameterTypes = getterMethods[i].getParameterTypes();

				if (getterMethods[i].getName().startsWith("get")
						&& parameterTypes.length == 1
						&& (parameterTypes[0].equals(Integer.TYPE) || parameterTypes[0]
								.equals(Integer.class))) {
					if (getterMethods[i].getName().equals("getRowId")) {
						continue; // we don't support this yet, ever?
					}
					
					try {
						getterMethods[i].invoke(this.rs,
								new Object[] { zeroIndex });
					} catch (InvocationTargetException invokeEx) {
						Throwable ex = invokeEx.getTargetException();

						if (ex != null && ex instanceof SQLException) {
							SQLException sqlEx = (SQLException) ex;

							assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
									sqlEx.getSQLState());

							messageLowBound = sqlEx.getMessage();
						} else {
							throw new RuntimeException(Util.stackTraceToString(ex), ex);
						}
					}

					String messageHighBound = null;

					try {
						getterMethods[i].invoke(this.rs,
								new Object[] { twoIndex });
					} catch (InvocationTargetException invokeEx) {
						Throwable ex = invokeEx.getTargetException();

						if (ex != null && ex instanceof SQLException) {
							SQLException sqlEx = (SQLException) ex;

							assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
									sqlEx.getSQLState());

							messageHighBound = sqlEx.getMessage();
						} else {
							throw new RuntimeException(ex);
						}
					}

					assertNotNull("Exception message null for method "
							+ getterMethods[i], messageHighBound);
					assertNotNull("Exception message null for method "
							+ getterMethods[i], messageLowBound);

					assertTrue(!messageHighBound.equals(messageLowBound));
				}
			}
		} finally {
			closeMemberJDBCResources();
		}
	}

	/**
	 * Tests fix for BUG#28085 - Need more useful error messages for diagnostics
	 * when the driver thinks a result set isn't updatable.
	 * 
	 * @throws Exception if the tests fail.
	 */
	public void testBug28085() throws Exception {

		Statement updStmt = null;
		
		try {
			createTable("testBug28085_oneKey", 
				"(pk int primary key not null, field2 varchar(3))");
			
			this.stmt.executeUpdate("INSERT INTO testBug28085_oneKey (pk, field2) VALUES (1, 'abc')");
			
			createTable("testBug28085_multiKey", 
				"(pk1 int not null, pk2 int not null, field2 varchar(3), primary key (pk1, pk2))");
			
			this.stmt.executeUpdate("INSERT INTO testBug28085_multiKey VALUES (1,2,'abc')");
			
			createTable("testBug28085_noKey", 
					"(field1 varchar(3) not null)");
	
			this.stmt.executeUpdate("INSERT INTO testBug28085_noKey VALUES ('abc')");
			
			updStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
					ResultSet.CONCUR_UPDATABLE);
			
			this.rs = updStmt.executeQuery("SELECT field2 FROM testBug28085_oneKey");
			exerciseUpdatableResultSet(1, "NotUpdatableReason.4");
			
			this.rs = updStmt.executeQuery("SELECT pk1, field2 FROM testBug28085_multiKey");
			this.rs.next();
			exerciseUpdatableResultSet(1, "NotUpdatableReason.7");
			
			this.rs = updStmt.executeQuery("SELECT t1.field2, t1.pk, t2.pk1 FROM testBug28085_oneKey t1 INNER JOIN testBug28085_multiKey t2 ON t1.pk = t2.pk1");
			exerciseUpdatableResultSet(1, "NotUpdatableReason.0");
			
			this.rs = updStmt.executeQuery("SELECT field1 FROM testBug28085_noKey");
			exerciseUpdatableResultSet(1, "NotUpdatableReason.5");
			
			this.rs = updStmt.executeQuery("SELECT 1");
			exerciseUpdatableResultSet(1, "NotUpdatableReason.3");
			
			this.rs = updStmt.executeQuery("SELECT pk1, pk2, LEFT(field2, 2) FROM testBug28085_multiKey");
			this.rs.next();
			exerciseUpdatableResultSet(1, "NotUpdatableReason.3");
		} finally {
			closeMemberJDBCResources();
			
			if (updStmt != null) {
				updStmt.close();
			}
		}
	}
	
	private void exerciseUpdatableResultSet(int columnUpdateIndex,
			String messageToCheck) throws Exception {
		this.rs.next();
		
		try {
			this.rs.updateString(columnUpdateIndex, "def");
		} catch (SQLException sqlEx) {
			checkUpdatabilityMessage(sqlEx, 
					messageToCheck);
		}
		
		try {
			this.rs.moveToInsertRow();
		} catch (SQLException sqlEx) {
			checkUpdatabilityMessage(sqlEx, 
					messageToCheck);
		}
		
		try {
			this.rs.deleteRow();
		} catch (SQLException sqlEx) {
			checkUpdatabilityMessage(sqlEx, 
					messageToCheck);
		}

		this.rs.close();
	}
	
	private void checkUpdatabilityMessage(SQLException sqlEx,
			String messageToCheck) throws Exception {

		String message = sqlEx.getMessage();

		assertNotNull(message);

		String localizedMessage = Messages.getString(messageToCheck);

		assertTrue("Didn't find required message component '"
				+ localizedMessage + "', instead found:\n\n" + message,
				message.indexOf(localizedMessage) != -1);
	}

	public void testBug24886() throws Exception {
	    Properties props = new Properties();
	    props.setProperty("blobsAreStrings", "true");

	    Connection noBlobConn = getConnectionWithProps(props);

	    createTable("testBug24886", "(sepallength double,"
	            + "sepalwidth double,"
	            + "petallength double,"
	            + "petalwidth double,"
	            + "Class mediumtext, "
	            + "fy TIMESTAMP)");

	    noBlobConn.createStatement().executeUpdate("INSERT INTO testBug24886 VALUES (1,2,3,4,'1234', now()),(5,6,7,8,'12345678', now())");
	    this.rs = noBlobConn.createStatement().executeQuery("SELECT concat(Class,petallength), COUNT(*) FROM `testBug24886` GROUP BY `concat(Class,petallength)`");
	    this.rs.next();
	    assertEquals("java.lang.String", this.rs.getObject(1).getClass().getName());

	    props.clear();
	    props.setProperty("functionsNeverReturnBlobs", "true");
	    noBlobConn = getConnectionWithProps(props);
	    this.rs = noBlobConn.createStatement().executeQuery("SELECT concat(Class,petallength), COUNT(*) FROM `testBug24886` GROUP BY `concat(Class,petallength)`");
        this.rs.next();
        
        if (versionMeetsMinimum(4, 1)) {
        	assertEquals("java.lang.String", this.rs.getObject(1).getClass().getName());
        }
	}

	
	/**
	 * Tests fix for BUG#30664. Note that this fix only works
	 * for MySQL server 5.0.25 and newer, since earlier versions
	 * didn't consistently return correct metadata for functions,
	 * and thus results from subqueries and functions were indistinguishable
	 * from each other, leading to type-related bugs.
	 * 
	 * @throws Exception
	 */
	public void testBug30664() throws Exception {
		if (!versionMeetsMinimum(5, 0, 25)) {
			return;
		}
		
		createTable("testBug30664_1", "(id int)");
		createTable("testBug30664_2", "(id int, binaryvalue varbinary(255))");

		try {
			this.stmt
					.executeUpdate("insert into testBug30664_1 values (1),(2),(3)");
			this.stmt
					.executeUpdate("insert into testBug30664_2 values (1,'���'),(2,'����'),(3,' ���')");
			this.rs = this.stmt
					.executeQuery("select testBug30664_1.id, (select testBug30664_2.binaryvalue from testBug30664_2 where testBug30664_2.id=testBug30664_1.id) as value from testBug30664_1");
			ResultSetMetaData tblMD = this.rs.getMetaData();

			for (int i = 1; i < tblMD.getColumnCount() + 1; i++) {
				switch (i) {
				case 1:
					assertEquals("INT", tblMD.getColumnTypeName(i)
							.toUpperCase());
					break;
				case 2:
					assertEquals("VARBINARY", tblMD.getColumnTypeName(i)
							.toUpperCase());
					break;
				}
			}
		} finally {
			closeMemberJDBCResources();
		}
	}
	
	/**
	 * Tests fix for BUG#30851, NPE with null column values when
	 * "padCharsWithSpace" is set to "true".
	 * 
	 * @throws Exception
	 */
	public void testbug30851() throws Exception {
		Connection padConn = getConnectionWithProps("padCharsWithSpace=true");
		
    	try {
        	createTable("bug30851", "(CharCol CHAR(10) DEFAULT NULL)");
    		this.stmt.execute("INSERT INTO bug30851 VALUES (NULL)");
    		this.rs = padConn.createStatement().executeQuery("SELECT * FROM bug30851");
    		this.rs.first();
    		String strvar = this.rs.getString(1);
    		assertNull("Should be null", strvar);

    	} finally {
			closeMemberJDBCResources();
			
			if (padConn != null) {
				padConn.close();
			}
		}        
	}
	
		/**
	 * Tests fix for Bug#33678 - Multiple result sets not supported in
	 * "streaming" mode. This fix covers both normal statements, and stored
	 * procedures, with the exception of stored procedures with registered 
	 * OUTPUT parameters, which can't be used at all with "streaming" result 
	 * sets.
	 * 
	 * @throws Exception
	 */
	public void testBug33678() throws Exception {
		if (!versionMeetsMinimum(4, 1)) {
			return;
		}
		
		createTable("testBug33678", "(field1 INT)");
		
		
		Connection multiConn = getConnectionWithProps("allowMultiQueries=true");
		Statement multiStmt = multiConn.createStatement();
		
		try {
			multiStmt.setFetchSize(Integer.MIN_VALUE);
			
			multiStmt.execute("SELECT 1 UNION SELECT 2; INSERT INTO testBug33678 VALUES (1); UPDATE testBug33678 set field1=2; INSERT INTO testBug33678 VALUES(3); UPDATE testBug33678 set field1=2 WHERE field1=3; UPDATE testBug33678 set field1=2; SELECT 1");
			this.rs = multiStmt.getResultSet();
			this.rs.next();
			assertEquals("1", this.rs.getString(1));
			
			assertFalse(multiStmt.getMoreResults());
			assertEquals(1, multiStmt.getUpdateCount());
			assertFalse(multiStmt.getMoreResults());
			assertEquals(1, multiStmt.getUpdateCount());
			assertFalse(multiStmt.getMoreResults());
			assertEquals(1, multiStmt.getUpdateCount());
			assertFalse(multiStmt.getMoreResults());
			assertEquals(1, multiStmt.getUpdateCount());
			assertFalse(multiStmt.getMoreResults());
			assertEquals(2, multiStmt.getUpdateCount());
			assertTrue(multiStmt.getMoreResults());
			this.rs = multiStmt.getResultSet();
			this.rs.next();
			assertEquals("1", this.rs.getString(1));
			
			this.rs.close();
			
			multiStmt.execute("INSERT INTO testBug33678 VALUES (1); INSERT INTO testBug33678 VALUES (1), (2); INSERT INTO testBug33678 VALUES (1), (2), (3)");
			
			assertEquals(1, multiStmt.getUpdateCount());
			assertFalse(multiStmt.getMoreResults());
			assertEquals(2, multiStmt.getUpdateCount());
			assertFalse(multiStmt.getMoreResults());
			assertEquals(3, multiStmt.getUpdateCount());
			assertFalse(multiStmt.getMoreResults() && multiStmt.getUpdateCount() == -1);
			
			this.rs.close();
			
			if (versionMeetsMinimum(5, 0)) {
				createProcedure("spBug33678", "() BEGIN SELECT 1; SELECT 2; SELECT 3; END");
				
				CallableStatement cStmt = multiConn.prepareCall("{CALL spBug33678()}");
				cStmt.setFetchSize(Integer.MIN_VALUE);
				cStmt.execute();
				
				for (int i = 0; i < 2; i++) {
					if (i != 0) {
						assertTrue(cStmt.getMoreResults());
					}
					
					this.rs = cStmt.getResultSet();
					assertTrue(this.rs.next());
					assertEquals(i + 1, this.rs.getInt(1));	
				}
			}
		} finally {
			multiStmt.close();
			multiConn.close();
			
			closeMemberJDBCResources();
		}
	}
	
	public void testBug33162() throws Exception {
		if (!versionMeetsMinimum(5, 0)) {
			return;
		}
		
		this.rs = this.stmt.executeQuery("select now() from dual where 1=0");
		this.rs.next();
		try {
			this.rs.getTimestamp(1);  // fails
		} catch (SQLException sqlEx) {
			assertEquals(SQLError.SQL_STATE_GENERAL_ERROR, sqlEx.getSQLState());
		}
	}
	
	public void testBug34762() throws Exception {
		createTable("testBug34762", "(field1 TIMESTAMP)");
		int numRows = 10;
		
		for (int i = 0; i < numRows; i++) {
			this.stmt.executeUpdate("INSERT INTO testBug34762 VALUES (NOW())");
		}
		
		this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug34762");
		
		while (this.rs.next()) {
			this.rs.getTimestamp(1);
		}
		
		this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug34762");
		
		for (int i = 1; i <= numRows; i++) {
			this.rs.absolute(i);
			this.rs.getTimestamp(1);
		}
		
		this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug34762");
		
		this.rs.last();
		this.rs.getTimestamp(1);
		
		while (this.rs.previous()) {
			this.rs.getTimestamp(1);
		}
		
		this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug34762");
		
		this.rs.last();
		
		while (this.rs.relative(-1)) {
			this.rs.getTimestamp(1);
		}
		
		this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug34762");
		
		this.rs.beforeFirst();
		
		while (this.rs.relative(1)) {
			this.rs.getTimestamp(1);
		}
	}
	
	/**
	 * @deprecated because we use deprecated methods
	 */
	public void testBug34913() throws Exception {
		try {
			Timestamp ts = new Timestamp(new Date(109, 5, 1).getTime());
			
			this.pstmt = ((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement("SELECT 'abcdefghij', ?");
			this.pstmt.setTimestamp(1, ts);
			this.rs = this.pstmt.executeQuery();
			this.rs.next();
			assertTrue(this.rs.getTimestamp(2).getMonth() == 5);
			assertTrue(this.rs.getTimestamp(2).getDate() == 1);
		} finally {
			closeMemberJDBCResources();
		}
	}
	
	public void testBug36051() throws Exception {
		try {
			this.rs = this.stmt.executeQuery("SELECT '24:00:00'");
			this.rs.next();
			this.rs.getTime(1);
		} finally {
			closeMemberJDBCResources();
		}
	}
	
	/**
	 * Tests fix for BUG#35610, BUG#35150. We follow the JDBC Spec here, in that the 4.0 behavior
	 * is correct, the JDBC-3.0 (and earlier) spec has a bug, but you can get the buggy behavior
	 * (allowing column names *and* labels to be used) by setting "useColumnNamesInFindColumn" to
	 * "true".
	 * 
	 * @throws Exception
	 */
	public void testBug35610() throws Exception {
		createTable("testBug35610", "(field1 int, field2 int, field3 int)");
		this.stmt.executeUpdate("INSERT INTO testBug35610 VALUES (1, 2, 3)");
		exercise35610(this.stmt, false);
		exercise35610(getConnectionWithProps("useColumnNamesInFindColumn=true").createStatement(), true);
	}
	
	private void exercise35610(Statement configuredStmt, boolean force30Behavior) throws Exception {
		try {
			this.rs = configuredStmt.executeQuery("SELECT field1 AS f1, field2 AS f2, field3 FROM testBug35610");
			
			ResultSetMetaData rsmd = this.rs.getMetaData();
			
			assertEquals("field1", rsmd.getColumnName(1));
			assertEquals("field2", rsmd.getColumnName(2));
			assertEquals("f1", rsmd.getColumnLabel(1));
			assertEquals("f2", rsmd.getColumnLabel(2));
			

			
			assertEquals("field3", rsmd.getColumnName(3));
			assertEquals("field3", rsmd.getColumnLabel(3));
			
			this.rs.next();
			
			// From ResultSet.html#getInt(java.lang.String) in JDBC-4.0
			//
			// Retrieves the value of the designated column in the current row of this ResultSet
			// object as an int in the Java programming language.
			//
			// Parameters:
			// columnLabel - the label for the column specified with the SQL AS clause. If the 
			//               SQL AS clause was not specified, then the label is the name of the column
			//
			
			assertEquals(1, this.rs.getInt("f1"));
			assertEquals(2, this.rs.getInt("f2"));
			assertEquals(3, this.rs.getInt("field3"));
			
			// Pre-JDBC 4.0, some versions of the spec say "column name *or* label"
			// for the column name argument...
			
			if (force30Behavior) {
				assertEquals(1, this.rs.getInt("field1"));
				assertEquals(2, this.rs.getInt("field2"));
			}
			
			if (!force30Behavior) {
				try {
					this.rs.findColumn("field1");
					fail("findColumn(\"field1\" should have failed with an exception");
				} catch (SQLException sqlEx) {
					// expected
				}
				
				try {
					this.rs.findColumn("field2");
					fail("findColumn(\"field2\" should have failed with an exception");
				} catch (SQLException sqlEx) {
					// expected
				}
			}
		} finally {
			closeMemberJDBCResources();
		}
	}
	
	/**
	 * Tests fix for BUG#39911 - We don't retrieve nanos correctly when -parsing- a string for a TIMESTAMP.
	 */
	public void testBug39911() throws Exception {
		try {
			this.rs = this.stmt
					.executeQuery("SELECT '2008-09-26 15:47:20.797283'");
			this.rs.next();

			checkTimestampNanos();

			this.rs = ((com.mysql.jdbc.Connection) this.conn)
					.serverPrepareStatement(
							"SELECT '2008-09-26 15:47:20.797283'")
					.executeQuery();
			this.rs.next();

			checkTimestampNanos();

			this.rs.close();
		} finally {
			closeMemberJDBCResources();
		}

	}

	private void checkTimestampNanos() throws SQLException {
		Timestamp ts = this.rs.getTimestamp(1);
		assertEquals(797283000, ts.getNanos());
		Calendar cal = Calendar.getInstance();
		cal.setTime(ts);
		assertEquals(797, cal.get(Calendar.MILLISECOND));
	}
	
	public void testBug38387() throws Exception {
		Connection noBlobConn = null;
	    Properties props = new Properties();
	    props.put("functionsNeverReturnBlobs","true");//toggle, no change
	    noBlobConn = getConnectionWithProps(props);
		try {
			Statement noBlobStmt = noBlobConn.createStatement();
			this.rs = noBlobStmt.executeQuery("SELECT TRIM(1) AS Rslt");
	        while (this.rs.next()) {
	        	assertEquals("1", this.rs.getString("Rslt"));
	            assertEquals("java.lang.String", this.rs.getObject(1).getClass().getName());
	        }
		} finally {
			noBlobConn.close();
		}

	}
	
	public void testRanges() throws Exception {
		createTable("testRanges", "(int_field INT, long_field BIGINT, double_field DOUBLE, string_field VARCHAR(32))");
		
		this.pstmt = this.conn.prepareStatement("INSERT INTO testRanges VALUES (?,?,?, ?)");
		this.pstmt.setInt(1, Integer.MIN_VALUE);
		this.pstmt.setLong(2, Long.MIN_VALUE);
		this.pstmt.setDouble(3, (double)Long.MAX_VALUE + 1D);
		this.pstmt.setString(4, "1E4");
		
		this.pstmt.executeUpdate();
		
		checkRangeMatrix(this.conn);
		checkRangeMatrix(getConnectionWithProps("useFastIntParsing=false"));
	}
	
	private void checkRangeMatrix(Connection c) throws Exception {
		this.rs = c.createStatement().executeQuery("SELECT int_field, long_field, double_field, string_field FROM testRanges");
		this.rs.next();
		checkRanges();
		this.rs.close();
		
		this.pstmt = ((com.mysql.jdbc.Connection)c).serverPrepareStatement("SELECT int_field, long_field, double_field, string_field FROM testRanges");
		this.rs = this.pstmt.executeQuery();
		this.rs.next();
		checkRanges();
		this.rs.close();
		
		this.pstmt.setFetchSize(Integer.MIN_VALUE);
		this.rs = this.pstmt.executeQuery();
		this.rs.next();
		checkRanges();
		this.rs.close();
		
		this.pstmt = ((com.mysql.jdbc.Connection)c).clientPrepareStatement("SELECT int_field, long_field, double_field, string_field FROM testRanges");
		this.rs = this.pstmt.executeQuery();
		this.rs.next();
		checkRanges();
		this.rs.close();
		
		this.pstmt.setFetchSize(Integer.MIN_VALUE);
		this.rs = this.pstmt.executeQuery();
		this.rs.next();
		checkRanges();
		this.rs.close();
	}

	private void checkRanges() throws SQLException {
		assertEquals(Integer.MIN_VALUE, this.rs.getInt(1));
		
		try {
			this.rs.getInt(2);
		} catch (SQLException sqlEx) {
			assertTrue(sqlEx.getMessage().indexOf(" in column '2'") != -1);
		}
		
		assertEquals(Long.MIN_VALUE, this.rs.getLong(2));
		
		try {
			this.rs.getLong(3);
		} catch (SQLException sqlEx) {
			assertTrue(sqlEx.getMessage().indexOf(" in column '3'") != -1);
		}
		
		assertEquals(10000, this.rs.getInt(4));
		assertEquals(10000, this.rs.getLong(4));
	}
	
	/**
	 * Bug #41484
	 * Accessing fields by name after the ResultSet is closed throws NullPointerException.
	 */
	public void testBug41484() throws Exception {
		try {
			rs = stmt.executeQuery("select 1 as abc");
			rs.next();
			rs.getString("abc");
			rs.close();
			rs.getString("abc");
		} catch(SQLException ex) {
			/* expected */
			assertEquals(0, ex.getErrorCode());
			assertEquals("S1000", ex.getSQLState());
		} finally {
			closeMemberJDBCResources();
		}
	}
	
	public void testBug41484_2() throws Exception {
		Connection cachedRsmdConn = getConnectionWithProps("cacheResultSetMetadata=true");
		
		try {
			createTable("bug41484", "(id int not null primary key, day date not null) DEFAULT CHARSET=utf8");
			this.pstmt = cachedRsmdConn.prepareStatement("INSERT INTO bug41484(id, day) values(1, ?)");
			this.pstmt.setInt(1, 20080509);
			assertEquals(1, this.pstmt.executeUpdate());
			this.pstmt.close();
	
			this.pstmt = cachedRsmdConn.prepareStatement("SELECT * FROM bug41484 WHERE id = ?");
			this.pstmt.setInt(1, 1);
			this.rs = this.pstmt.executeQuery();
			this.rs.first();
			this.rs.getString("day");
			this.rs.close();
			this.pstmt.close();
	
			this.pstmt = cachedRsmdConn.prepareStatement("INSERT INTO bug41484(id, day) values(2, ?)");
			this.pstmt.setInt(1, 20090212);
			assertEquals(1, this.pstmt.executeUpdate());
			this.pstmt.close();
	
			this.pstmt = cachedRsmdConn.prepareStatement("SELECT * FROM bug41484 WHERE id = ?");
			this.pstmt.setInt(1, 2);
			this.rs = this.pstmt.executeQuery();
			this.rs.first();
			assertEquals(this.rs.getString(1), "2");
			this.rs.getString("day");
			this.rs.close();
			
			this.pstmt.close();
		} finally {
			cachedRsmdConn.close();
		}
	}
	
	public void testBug27431() throws Exception {
		createTable("bug27431", "(`ID` int(20) NOT NULL auto_increment,"
		  + "`Name` varchar(255) NOT NULL default '',"
		  + "PRIMARY KEY  (`ID`))");
		
		this.stmt.executeUpdate("INSERT INTO bug27431 (`ID`, `Name`) VALUES 	(1, 'Lucho'),(2, 'Lily'),(3, 'Kiro')");

		Statement updStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,	ResultSet.CONCUR_UPDATABLE);
		this.rs = updStmt.executeQuery("SELECT ID, Name FROM bug27431");

		while(this.rs.next()) {
		   this.rs.deleteRow();
		}
		
		assertEquals(0, getRowCount("bug27431"));
	}
	
	public void testBug43759() throws Exception {
        createTable("testtable_bincolumn", "(" +
                "bincolumn binary(8) NOT NULL, " +
                "PRIMARY KEY (bincolumn)" +
                ")", "innodb");
        
        String pkValue1 = "0123456789ABCD90";
        String pkValue2 = "0123456789ABCD00";
        // put some data in it
       this.stmt.executeUpdate("INSERT INTO testtable_bincolumn (bincolumn) " +
                "VALUES (unhex('"+pkValue1+"')), (unhex('"+pkValue2+"'))");

        // cause the bug
        Statement updStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        this.rs = updStmt.executeQuery("SELECT * FROM testtable_bincolumn WHERE bincolumn = unhex('"+pkValue1+"')");
        assertTrue(this.rs.next());
        this.rs.deleteRow();
        
        // At this point the row with pkValue1 should be deleted.  We'll select it back to see.
        // If the row comes back, the testcase has failed.

        this.rs = this.stmt.executeQuery("SELECT * FROM testtable_bincolumn WHERE bincolumn = unhex('"+pkValue1+"')");
        assertFalse(rs.next());

        // Now, show a case where it happens to work, because the binary data is different
        updStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        this.rs = updStmt.executeQuery("SELECT * FROM testtable_bincolumn WHERE bincolumn = unhex('"+pkValue2+"')");
        assertTrue (this.rs.next());
        rs.deleteRow();

        
        this.rs = this.stmt.executeQuery("SELECT * FROM testtable_bincolumn WHERE bincolumn = unhex('"+pkValue2+"')");
        assertFalse(rs.next());
	}
	
	public void testBug32525() throws Exception {
		createTable("bug32525", "(field1 date, field2 timestamp)");
		this.stmt.executeUpdate("INSERT INTO bug32525 VALUES ('0000-00-00', '0000-00-00 00:00:00')");
		Connection noStringSyncConn = getConnectionWithProps("noDatetimeStringSync=true");
		
		try {
			this.rs = ((com.mysql.jdbc.Connection) noStringSyncConn).serverPrepareStatement("SELECT field1, field2 FROM bug32525").executeQuery();
			this.rs.next();
			assertEquals("0000-00-00", this.rs.getString(1));
			assertEquals("0000-00-00 00:00:00", this.rs.getString(2));
		} finally {
			noStringSyncConn.close();
		}
		
	}
	
	public void testBug49797() throws Exception {
		createTable("testBug49797", "(`Id` int(2) not null auto_increment, " +
				"`abc` char(50) , " +
				"PRIMARY KEY (`Id`)) ENGINE=MyISAM DEFAULT CHARSET=utf8");
		this.stmt.executeUpdate("INSERT into testBug49797 VALUES (1,'1'),(2,'2'),(3,'3')");
		assertEquals(3, getRowCount("testBug49797"));
		
		Statement updStmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, 
				ResultSet.CONCUR_UPDATABLE );
		try {
			this.rs = updStmt.executeQuery("SELECT * FROM testBug49797");
			while(rs.next()) {
				rs.deleteRow();
			}
			assertEquals(0, getRowCount("testBug49797"));
		} finally {
			updStmt.close();
		}
	}
}
