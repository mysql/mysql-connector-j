/*
  Copyright (c) 2002, 2019, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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

import java.io.Reader;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
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
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.sql.rowset.CachedRowSet;

import com.mysql.jdbc.CommunicationsException;
import com.mysql.jdbc.ConnectionImpl.ExceptionInterceptorChain;
import com.mysql.jdbc.ExceptionInterceptor;
import com.mysql.jdbc.Extension;
import com.mysql.jdbc.Messages;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.MysqlDataTruncation;
import com.mysql.jdbc.NotUpdatable;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.StatementImpl;
import com.mysql.jdbc.TimeUtil;
import com.mysql.jdbc.Util;

import testsuite.BaseTestCase;
import testsuite.BufferingLogger;

/**
 * Regression test cases for the ResultSet class.
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
         * assertTrue(this.rs.getByte(1) == 0); assertTrue(this.rs.wasNull());
         * 
         * assertTrue(this.rs.getShort(1) == 0); assertTrue(this.rs.wasNull());
         * 
         * assertTrue(this.rs.getInt(1) == 0); assertTrue(this.rs.wasNull());
         * 
         * assertTrue(this.rs.getLong(1) == 0); assertTrue(this.rs.wasNull());
         * 
         * assertTrue(this.rs.getFloat(1) == 0); assertTrue(this.rs.wasNull());
         * 
         * assertTrue(this.rs.getDouble(1) == 0); assertTrue(this.rs.wasNull());
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
         * assertTrue(this.rs.getByte(1) == 1); assertTrue(!this.rs.wasNull());
         * 
         * assertTrue(this.rs.getShort(1) == 1); assertTrue(!this.rs.wasNull());
         * 
         * assertTrue(this.rs.getInt(1) == 1); assertTrue(!this.rs.wasNull());
         * 
         * assertTrue(this.rs.getLong(1) == 1); assertTrue(!this.rs.wasNull());
         * 
         * assertTrue(this.rs.getFloat(1) == 1); assertTrue(!this.rs.wasNull());
         * 
         * assertTrue(this.rs.getDouble(1) == 1);
         * assertTrue(!this.rs.wasNull());
         * 
         * assertTrue(this.rs.getBigDecimal(1) != null);
         * assertTrue(!this.rs.wasNull());
         */
        createTable("testBug2359_1", "(id INT)", "InnoDB");
        this.stmt.executeUpdate("INSERT INTO testBug2359_1 VALUES (1)");

        this.pstmt = this.conn.prepareStatement("SELECT max(id) FROM testBug2359_1");
        this.rs = this.pstmt.executeQuery();

        if (this.rs.next()) {
            assertTrue(this.rs.getInt(1) != 0);
            this.rs.close();
        }

        this.rs.close();
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
            pStmt = this.conn.prepareStatement("SELECT NOW()", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

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
        if (!this.DISABLED_testBug2654) { // this is currently a server-level bug

            createTable("foo", "(id tinyint(3) default NULL, data varchar(255) default NULL) DEFAULT CHARSET=latin1", "MyISAM ");
            this.stmt.executeUpdate("INSERT INTO foo VALUES (1,'male'),(2,'female')");

            createTable("bar", "(id tinyint(3) unsigned default NULL, data char(3) default '0') DEFAULT CHARSET=latin1", "MyISAM ");

            this.stmt.executeUpdate("INSERT INTO bar VALUES (1,'yes'),(2,'no')");

            String statement = "select foo.id, foo.data, bar.data from foo, bar	where foo.id = bar.id order by foo.id";

            String column = "foo.data";

            this.rs = this.stmt.executeQuery(statement);

            ResultSetMetaData rsmd = this.rs.getMetaData();
            System.out.println(rsmd.getTableName(1));
            System.out.println(rsmd.getColumnName(1));

            this.rs.next();

            String fooData = this.rs.getString(column);
            assertNotNull(fooData);

        }
    }

    /**
     * Tests for fix to BUG#1130
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testClobTruncate() throws Exception {
        createTable("testClobTruncate", "(field1 TEXT)");
        this.stmt.executeUpdate("INSERT INTO testClobTruncate VALUES ('abcdefg')");

        this.rs = this.stmt.executeQuery("SELECT * FROM testClobTruncate");
        this.rs.next();

        Clob clob = this.rs.getClob(1);
        clob.truncate(3);

        Reader reader = clob.getCharacterStream();
        char[] buf = new char[8];
        int charsRead = reader.read(buf);

        String clobAsString = new String(buf, 0, charsRead);

        assertTrue(clobAsString.equals("abc"));
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
            clobberStmt.executeUpdate("CREATE TABLE StreamingClobber ( DUMMYID INTEGER NOT NULL, DUMMYNAME VARCHAR(32),PRIMARY KEY (DUMMYID) )");
            clobberStmt.executeUpdate("INSERT INTO StreamingClobber (DUMMYID, DUMMYNAME) VALUES (0, NULL)");
            clobberStmt.executeUpdate("INSERT INTO StreamingClobber (DUMMYID, DUMMYNAME) VALUES (1, 'nro 1')");
            clobberStmt.executeUpdate("INSERT INTO StreamingClobber (DUMMYID, DUMMYNAME) VALUES (2, 'nro 2')");
            clobberStmt.executeUpdate("INSERT INTO StreamingClobber (DUMMYID, DUMMYNAME) VALUES (3, 'nro 3')");

            Statement streamStmt = null;

            try {
                streamStmt = clobberConn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
                streamStmt.setFetchSize(Integer.MIN_VALUE);

                this.rs = streamStmt.executeQuery("SELECT DUMMYID, DUMMYNAME FROM StreamingClobber ORDER BY DUMMYID");

                this.rs.next();

                // This should proceed normally, after the driver clears the input stream
                ResultSet rs2 = clobberStmt.executeQuery("SHOW VARIABLES");
                rs2.next();
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

    public void testEmptyResultSetGet() throws Exception {
        try {
            this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'foo'");
            System.out.println(this.rs.getInt(1));
        } catch (SQLException sqlEx) {
            assertTrue("Correct exception not thrown", SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
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
            Statement updatableStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            try {
                updatableStmt.execute("SELECT * FROM mysql.user");

                this.rs = updatableStmt.getResultSet();
            } catch (SQLException sqlEx) {
                String message = sqlEx.getMessage();

                if ((message != null) && (message.indexOf("denied") != -1)) {
                    System.err.println("WARN: Can't complete testFixForBug1592(), access to 'mysql' database not allowed");
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

        createTable("testFixForBug2006_1", "(key_field INT NOT NULL)");
        createTable("testFixForBug2006_2", "(key_field INT NULL)");
        this.stmt.executeUpdate("INSERT INTO testFixForBug2006_1 VALUES (1)");

        this.rs = this.stmt.executeQuery(
                "SELECT testFixForBug2006_1.key_field, testFixForBug2006_2.key_field FROM testFixForBug2006_1 LEFT JOIN testFixForBug2006_2 USING(key_field)");

        ResultSetMetaData rsmd = this.rs.getMetaData();

        assertTrue(rsmd.getColumnName(1).equals(rsmd.getColumnName(2)));
        assertTrue(rsmd.isNullable(this.rs.findColumn("key_field")) == ResultSetMetaData.columnNoNulls);
        assertTrue(rsmd.isNullable(2) == ResultSetMetaData.columnNullable);
        assertTrue(this.rs.next());
        assertTrue(this.rs.getObject(1) != null);
        assertTrue(this.rs.getObject(2) == null);

    }

    /**
     * Tests that ResultSet.getLong() does not truncate values.
     * 
     * @throws Exception
     *             if any errors occur
     */
    public void testGetLongBug() throws Exception {
        createTable("getLongBug", "(int_col int, bigint_col bigint)");

        int intVal = 123456;
        long longVal1 = 123456789012345678L;
        long longVal2 = -2079305757640172711L;
        this.stmt.executeUpdate("INSERT INTO getLongBug (int_col, bigint_col) VALUES (" + intVal + ", " + longVal1 + "), (" + intVal + ", " + longVal2 + ")");

        this.rs = this.stmt.executeQuery("SELECT int_col, bigint_col FROM getLongBug ORDER BY bigint_col DESC");
        this.rs.next();
        assertTrue("Values not decoded correctly", ((this.rs.getInt(1) == intVal) && (this.rs.getLong(2) == longVal1)));
        this.rs.next();
        assertTrue("Values not decoded correctly", ((this.rs.getInt(1) == intVal) && (this.rs.getLong(2) == longVal2)));

    }

    public void testGetTimestampWithDate() throws Exception {
        createTable("testGetTimestamp", "(d date)");
        this.stmt.executeUpdate("INSERT INTO testGetTimestamp values (now())");

        this.rs = this.stmt.executeQuery("SELECT * FROM testGetTimestamp");
        this.rs.next();
        System.out.println(this.rs.getTimestamp(1));
    }

    /**
     * Tests a bug where ResultSet.isBefireFirst() would return true when the
     * result set was empty (which is incorrect)
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testIsBeforeFirstOnEmpty() throws Exception {
        // Query with valid rows: isBeforeFirst() correctly returns True
        this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'version'");
        assertTrue("Non-empty search should return true", this.rs.isBeforeFirst());

        // Query with empty result: isBeforeFirst() falsely returns True. Sun's documentation says it should return false
        this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'garbage'");
        assertTrue("Empty search should return false ", !this.rs.isBeforeFirst());
    }

    /**
     * Tests a bug where ResultSet.isBefireFirst() would return true when the
     * result set was empty (which is incorrect)
     * 
     * @throws Exception
     *             if an error occurs.
     */
    public void testMetaDataIsWritable() throws Exception {
        // Query with valid rows
        this.rs = this.stmt.executeQuery("SHOW VARIABLES LIKE 'version'");

        ResultSetMetaData rsmd = this.rs.getMetaData();

        int numColumns = rsmd.getColumnCount();

        for (int i = 1; i <= numColumns; i++) {
            assertTrue("rsmd.isWritable() should != rsmd.isReadOnly()", rsmd.isWritable(i) != rsmd.isReadOnly(i));
        }
    }

    /**
     * Tests fix for bug # 496
     * 
     * @throws Exception
     *             if an error happens.
     */
    public void testNextAndPrevious() throws Exception {
        createTable("testNextAndPrevious", "(field1 int)");
        this.stmt.executeUpdate("INSERT INTO testNextAndPrevious VALUES (1)");

        this.rs = this.stmt.executeQuery("SELECT * from testNextAndPrevious");

        System.out.println("Currently at row " + this.rs.getRow());
        this.rs.next();
        System.out.println("Value at row " + this.rs.getRow() + " is " + this.rs.getString(1));

        this.rs.previous();

        try {
            System.out.println("Value at row " + this.rs.getRow() + " is " + this.rs.getString(1));
            fail("Should not be able to retrieve values with invalid cursor");
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage().startsWith("Before start"));
        }

        this.rs.next();

        this.rs.next();

        try {
            System.out.println("Value at row " + this.rs.getRow() + " is " + this.rs.getString(1));
            fail("Should not be able to retrieve values with invalid cursor");
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage().startsWith("After end"));
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

        String sQuery = "SHOW VARIABLES";
        this.pstmt = this.conn.prepareStatement(sQuery, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

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
    }

    /**
     * Tests that streaming result sets are registered correctly.
     * 
     * @throws Exception
     *             if any errors occur
     */
    public void testStreamingRegBug() throws Exception {
        createTable("StreamingRegBug", "( DUMMYID INTEGER NOT NULL, DUMMYNAME VARCHAR(32),PRIMARY KEY (DUMMYID) )");
        this.stmt.executeUpdate("INSERT INTO StreamingRegBug (DUMMYID, DUMMYNAME) VALUES (0, NULL)");
        this.stmt.executeUpdate("INSERT INTO StreamingRegBug (DUMMYID, DUMMYNAME) VALUES (1, 'nro 1')");
        this.stmt.executeUpdate("INSERT INTO StreamingRegBug (DUMMYID, DUMMYNAME) VALUES (2, 'nro 2')");
        this.stmt.executeUpdate("INSERT INTO StreamingRegBug (DUMMYID, DUMMYNAME) VALUES (3, 'nro 3')");

        PreparedStatement streamStmt = null;

        try {
            streamStmt = this.conn.prepareStatement("SELECT DUMMYID, DUMMYNAME FROM StreamingRegBug ORDER BY DUMMYID", java.sql.ResultSet.TYPE_FORWARD_ONLY,
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

        createTable("updatabilityBug",
                "(id int(10) unsigned NOT NULL auto_increment, field1 varchar(32) NOT NULL default '',"
                        + " field2 varchar(128) NOT NULL default '', field3 varchar(128) default NULL, field4 varchar(128) default NULL,"
                        + " field5 varchar(64) default NULL, field6 int(10) unsigned default NULL, field7 varchar(64) default NULL, PRIMARY KEY  (id)) ",
                "InnoDB");
        this.stmt.executeUpdate("insert into updatabilityBug (id) values (1)");

        String sQuery = " SELECT * FROM updatabilityBug WHERE id = ? ";
        this.pstmt = this.conn.prepareStatement(sQuery, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
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
        Statement updStmt = updConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);

        try {
            updStmt.executeUpdate("DROP TABLE IF EXISTS testUpdatesWithEscaping");
            updStmt.executeUpdate("CREATE TABLE testUpdatesWithEscaping (field1 INT PRIMARY KEY, field2 VARCHAR(64))");
            updStmt.executeUpdate("INSERT INTO testUpdatesWithEscaping VALUES (1, null)");

            String stringToUpdate = "\" \\ '";

            this.rs = updStmt.executeQuery("SELECT * from testUpdatesWithEscaping");

            this.rs.next();
            this.rs.updateString(2, stringToUpdate);
            this.rs.updateRow();

            assertTrue(stringToUpdate.equals(this.rs.getString(2)));
        } finally {
            updStmt.executeUpdate("DROP TABLE IF EXISTS testUpdatesWithEscaping");
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
            createTable("testUpdWithQuotes", "(keyField CHAR(32) PRIMARY KEY NOT NULL, field2 int)");

            PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO testUpdWithQuotes VALUES (?, ?)");
            pStmt.setString(1, "Abe's");
            pStmt.setInt(2, 1);
            pStmt.executeUpdate();

            updStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

            this.rs = updStmt.executeQuery("SELECT * FROM testUpdWithQuotes");
            this.rs.next();
            this.rs.updateInt(2, 2);
            this.rs.updateRow();
        } finally {
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
        Statement updatableStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        createTable("testUpdateClob", "(intField INT NOT NULL PRIMARY KEY, clobField TEXT)");
        this.stmt.executeUpdate("INSERT INTO testUpdateClob VALUES (1, 'foo')");

        this.rs = updatableStmt.executeQuery("SELECT intField, clobField FROM testUpdateClob");
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

        this.rs = this.stmt.executeQuery("SELECT intField, clobField FROM testUpdateClob ORDER BY intField");

        this.rs.next();
        assertTrue((this.rs.getInt(1) == 1) && this.rs.getString(2).equals("bar"));

        this.rs.next();
        assertTrue((this.rs.getInt(1) == 2) && this.rs.getString(2).equals("baz"));

        this.rs.next();
        assertTrue((this.rs.getInt(1) == 3) && this.rs.getString(2).equals("bat"));
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
        createTable("testBug4689", "(tinyintField tinyint, tinyintFieldNull tinyint, intField int, intFieldNull int, "
                + "bigintField bigint, bigintFieldNull bigint, shortField smallint, shortFieldNull smallint, doubleField double, doubleFieldNull double)");

        this.stmt.executeUpdate("INSERT INTO testBug4689 VALUES (1, null, 1, null, 1, null, 1, null, 1, null)");

        PreparedStatement pStmt = this.conn.prepareStatement("SELECT tinyintField, tinyintFieldNull, intField, intFieldNull, "
                + "bigintField, bigintFieldNull, shortField, shortFieldNull, doubleField, doubleFieldNull FROM testBug4689");
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

            createTable("testBug5032", "(field1 BIT)");
            this.stmt.executeUpdate("INSERT INTO testBug5032 VALUES (1)");

            this.pstmt = this.conn.prepareStatement("SELECT field1 FROM testBug5032");
            this.rs = this.pstmt.executeQuery();
            assertTrue(this.rs.next());
            assertTrue(this.rs.getObject(1) instanceof Boolean);

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

        this.rs = this.stmt.executeQuery("SELECT 1");
        this.rs.close();

        try {
            @SuppressWarnings("unused")
            ResultSetMetaData md = this.rs.getMetaData();
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.getRow();
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.getWarnings();
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.first();
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.beforeFirst();
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.last();
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.afterLast();
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.relative(0);
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.next();
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.previous();
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.isBeforeFirst();
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.isFirst();
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.isAfterLast();
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
        }

        try {
            this.rs.isLast();
        } catch (NullPointerException npEx) {
            fail("Should not catch NullPointerException here");
        } catch (SQLException sqlEx) {
            assertTrue(SQLError.SQL_STATE_GENERAL_ERROR.equals(sqlEx.getSQLState()));
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
        if (!this.DISABLED_testBug5136) {
            PreparedStatement toGeom = this.conn.prepareStatement("select GeomFromText(?)");
            PreparedStatement toText = this.conn.prepareStatement("select AsText(?)");

            String inText = "POINT(146.67596278 -36.54368233)";

            // First assert that the problem is not at the server end
            this.rs = this.stmt.executeQuery("select AsText(GeomFromText('" + inText + "'))");
            this.rs.next();

            String outText = this.rs.getString(1);
            this.rs.close();
            assertTrue("Server side only\n In: " + inText + "\nOut: " + outText, inText.equals(outText));

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
            assertTrue("Server to client and back\n In: " + inText + "\nOut: " + outText, inText.equals(outText));
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
        createTable("testBug5664", "(pkfield int PRIMARY KEY NOT NULL, field1 SMALLINT)");
        this.stmt.executeUpdate("INSERT INTO testBug5664 VALUES (1, 1)");

        Statement updatableStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

        this.rs = updatableStmt.executeQuery("SELECT pkfield, field1 FROM testBug5664");
        this.rs.next();
        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 2);
        this.rs.updateByte(2, (byte) 2);

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
        createTable("testBug5717", "(field1 DOUBLE)");
        this.pstmt = this.conn.prepareStatement("INSERT INTO testBug5717 VALUES (?)");

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
    }

    /**
     * Tests fix for server issue that drops precision on aggregate operations
     * on DECIMAL types, because they come back as DOUBLEs.
     * 
     * @throws Exception
     *             if the test fails.
     */
    @SuppressWarnings("deprecation")
    public void testBug6537() throws Exception {
        if (versionMeetsMinimum(4, 1, 0)) {
            String tableName = "testBug6537";

            createTable(tableName, "(`id` int(11) NOT NULL default '0', `value` decimal(10,2) NOT NULL default '0.00', `stringval` varchar(10),"
                    + "PRIMARY KEY  (`id`)) DEFAULT CHARSET=latin1", "MyISAM");
            this.stmt.executeUpdate("INSERT INTO " + tableName + "(id, value, stringval) VALUES (1, 100.00, '100.00'), (2, 200, '200')");

            String sql = "SELECT SUM(value) as total FROM " + tableName + " WHERE id = ? ";
            PreparedStatement pStmt = this.conn.prepareStatement(sql);
            pStmt.setInt(1, 1);
            this.rs = pStmt.executeQuery();
            assertTrue(this.rs.next());

            assertTrue("100.00".equals(this.rs.getBigDecimal("total").toString()));

            sql = "SELECT stringval as total FROM " + tableName + " WHERE id = ? ";
            pStmt = this.conn.prepareStatement(sql);
            pStmt.setInt(1, 2);
            this.rs = pStmt.executeQuery();
            assertTrue(this.rs.next());

            assertTrue("200.00".equals(this.rs.getBigDecimal("total", 2).toString()));

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

        createTable("testBug6231", "(field1 TIME)");
        this.stmt.executeUpdate("INSERT INTO testBug6231 VALUES ('09:16:00')");

        this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug6231");
        this.rs.next();
        long asMillis = this.rs.getTimestamp(1).getTime();
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis(asMillis);

        assertEquals(9, cal.get(Calendar.HOUR));
        assertEquals(16, cal.get(Calendar.MINUTE));
        assertEquals(0, cal.get(Calendar.SECOND));

    }

    public void testBug6619() throws Exception {

        createTable("testBug6619", "(field1 int)");
        this.stmt.executeUpdate("INSERT INTO testBug6619 VALUES (1), (2)");

        PreparedStatement pStmt = this.conn.prepareStatement("SELECT SUM(field1) FROM testBug6619");

        this.rs = pStmt.executeQuery();
        this.rs.next();
        System.out.println(this.rs.getString(1));

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
            sjisStmt = sjisConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);

            sjisStmt.executeUpdate("DROP TABLE IF EXISTS testBug6743");
            StringBuilder queryBuf = new StringBuilder("CREATE TABLE testBug6743 (pkField INT NOT NULL PRIMARY KEY, field1 VARCHAR(32)");

            if (versionMeetsMinimum(4, 1)) {
                queryBuf.append(" CHARACTER SET SJIS");
            }

            queryBuf.append(")");
            sjisStmt.executeUpdate(queryBuf.toString());
            sjisStmt.executeUpdate("INSERT INTO testBug6743 VALUES (1, 'abc')");

            this.rs = sjisStmt.executeQuery("SELECT pkField, field1 FROM testBug6743");
            this.rs.next();
            this.rs.updateString(2, katakanaStr);
            this.rs.updateRow();

            String retrString = this.rs.getString(2);
            assertTrue(katakanaStr.equals(retrString));

            this.rs = sjisStmt.executeQuery("SELECT pkField, field1 FROM testBug6743");
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
        Connection testConn = this.conn;
        Connection zeroConn = getConnectionWithProps("zeroDateTimeBehavior=convertToNull");
        try {
            if (versionMeetsMinimum(5, 7, 4)) {
                Properties props = new Properties();
                props.put("jdbcCompliantTruncation", "false");
                if (versionMeetsMinimum(5, 7, 5)) {
                    String sqlMode = getMysqlVariable("sql_mode");
                    if (sqlMode.contains("STRICT_TRANS_TABLES")) {
                        sqlMode = removeSqlMode("STRICT_TRANS_TABLES", sqlMode);
                        props.put("sessionVariables", "sql_mode='" + sqlMode + "'");
                    }
                }
                testConn = getConnectionWithProps(props);
                this.stmt = testConn.createStatement();
            }

            createTable("testBug6561", "(ofield int, field1 DATE, field2 integer, field3 integer)");
            this.stmt.executeUpdate("INSERT INTO testBug6561 (ofield, field1,field2,field3)	VALUES (1, 0,NULL,0)");
            this.stmt.executeUpdate("INSERT INTO testBug6561 (ofield, field1,field2,field3) VALUES (2, '2004-11-20',NULL,0)");

            PreparedStatement ps = zeroConn.prepareStatement("SELECT field1,field2,field3 FROM testBug6561 ORDER BY ofield");
            this.rs = ps.executeQuery();

            assertTrue(this.rs.next());
            assertNull(this.rs.getObject("field1"));
            assertNull(this.rs.getObject("field2"));
            assertEquals(0, this.rs.getInt("field3"));

            assertTrue(this.rs.next());
            assertEquals("2004-11-20", this.rs.getString("field1"));
            assertNull(this.rs.getObject("field2"));
            assertEquals(0, this.rs.getInt("field3"));

            ps.close();
        } finally {
            zeroConn.close();
            if (testConn != this.conn) {
                testConn.close();
            }
        }
    }

    public void testBug7686() throws SQLException {
        String tableName = "testBug7686";
        createTable(tableName, "(id1 int(10) unsigned NOT NULL, id2 DATETIME, field1 varchar(128) NOT NULL default '', PRIMARY KEY  (id1, id2))", "InnoDB;");

        this.stmt.executeUpdate("insert into " + tableName + " (id1, id2, field1) values (1, '2005-01-05 13:59:20', 'foo')");

        String sQuery = " SELECT * FROM " + tableName + " WHERE id1 = ? AND id2 = ?";
        this.pstmt = this.conn.prepareStatement(sQuery, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

        this.conn.setAutoCommit(false);
        this.pstmt.setInt(1, 1);
        GregorianCalendar cal = new GregorianCalendar();
        cal.clear();
        cal.set(2005, 00, 05, 13, 59, 20);

        Timestamp jan5before2pm = null;
        jan5before2pm = new java.sql.Timestamp(cal.getTimeInMillis());

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

        createTable("testConvertedBinaryTimestamp", "(field1 VARCHAR(32), field2 VARCHAR(32), field3 VARCHAR(32), field4 TIMESTAMP)");
        this.stmt.executeUpdate("INSERT INTO testConvertedBinaryTimestamp VALUES ('abc', 'def', 'ghi', NOW())");

        pStmt = this.conn.prepareStatement("SELECT field1, field2, field3, field4 FROM testConvertedBinaryTimestamp", ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE);

        this.rs = pStmt.executeQuery();
        assertTrue(this.rs.next());

        this.rs.getObject(4); // fails if bug exists
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

        createTable("testBug8428", "(field1 YEAR, field2 DATETIME)");
        this.stmt.executeUpdate("INSERT INTO testBug8428 VALUES ('1999', '2005-02-11 12:54:41')");

        Properties props = new Properties();
        props.setProperty("noDatetimeStringSync", "true");
        props.setProperty("useUsageAdvisor", "true");
        props.setProperty("yearIsDateType", "false"); // for 3.1.9+

        noSyncConn = getConnectionWithProps(props);

        this.rs = noSyncConn.createStatement().executeQuery("SELECT field1, field2 FROM testBug8428");
        this.rs.next();
        assertEquals("1999", this.rs.getString(1));
        assertEquals("2005-02-11 12:54:41", this.rs.getString(2));

        this.rs = noSyncConn.prepareStatement("SELECT field1, field2 FROM testBug8428").executeQuery();
        this.rs.next();
        assertEquals("1999", this.rs.getString(1));
        assertEquals("2005-02-11 12:54:41", this.rs.getString(2));

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
            createTable("testBug8868", "(field1 DATE, field2 VARCHAR(32) CHARACTER SET BINARY)");
            this.stmt.executeUpdate("INSERT INTO testBug8868 VALUES (NOW(), 'abcd')");
            this.rs = this.stmt.executeQuery("SELECT DATE_FORMAT(field1,'%b-%e %l:%i%p') as fmtddate, field2 FROM testBug8868");
            this.rs.next();
            assertEquals("java.lang.String", this.rs.getObject(1).getClass().getName());
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

            createTable("testBug9098", "(pkfield INT PRIMARY KEY NOT NULL AUTO_INCREMENT, \n"
                    + "tsfield TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP, tsfield2 TIMESTAMP NOT NULL DEFAULT '2005-12-25 12:20:52', charfield VARCHAR(4) NOT NULL DEFAULT 'abcd')");
            updatableStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            this.rs = updatableStmt.executeQuery("SELECT pkfield, tsfield, tsfield2, charfield FROM testBug9098");
            this.rs.moveToInsertRow();
            this.rs.insertRow();
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
            Connection testConn = this.conn;
            try {
                if (versionMeetsMinimum(5, 7, 4)) {
                    Properties props = new Properties();
                    props.put("jdbcCompliantTruncation", "false");
                    if (versionMeetsMinimum(5, 7, 5)) {
                        String sqlMode = getMysqlVariable("sql_mode");
                        if (sqlMode.contains("STRICT_TRANS_TABLES")) {
                            sqlMode = removeSqlMode("STRICT_TRANS_TABLES", sqlMode);
                            props.put("sessionVariables", "sql_mode='" + sqlMode + "'");
                        }
                    }
                    testConn = getConnectionWithProps(props);
                    this.stmt = testConn.createStatement();
                }

                createTable("testBug9236",
                        "(field_1 int(18) NOT NULL auto_increment, field_2 varchar(50) NOT NULL default '',"
                                + "field_3 varchar(12) default NULL, field_4 int(18) default NULL, field_5 int(18) default NULL,"
                                + "field_6 datetime default NULL, field_7 varchar(30) default NULL, field_8 varchar(50) default NULL,"
                                + "field_9 datetime default NULL, field_10 int(18) NOT NULL default '0', field_11 int(18) default NULL,"
                                + "field_12 datetime NOT NULL default '0000-00-00 00:00:00', PRIMARY KEY  (field_1), KEY (field_4), KEY (field_2),"
                                + "KEY (field_3), KEY (field_7,field_1), KEY (field_5), KEY (field_6,field_10,field_9), KEY (field_11,field_10),"
                                + "KEY (field_12,field_10)) DEFAULT CHARSET=latin1",
                        "InnoDB");

                this.stmt.executeUpdate("INSERT INTO testBug9236 VALUES "
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

                createTable("testBug9236_1", "(field1 CHAR(2) CHARACTER SET BINARY)");
                this.stmt.executeUpdate("INSERT INTO testBug9236_1 VALUES ('ab')");
                this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug9236_1");

                ResultSetMetaData rsmd = this.rs.getMetaData();
                assertEquals("[B", rsmd.getColumnClassName(1));
                assertTrue(this.rs.next());
                Object asObject = this.rs.getObject(1);
                assertEquals("[B", asObject.getClass().getName());

                this.rs = this.stmt.executeQuery(
                        "select DATE_FORMAT(field_12, '%Y-%m-%d') as date, count(*) as count from testBug9236 where field_10 = 0 and field_3 = 'FRL' and field_12 >= '2005-03-02 00:00:00' and field_12 <= '2005-03-17 00:00:00' group by date");
                rsmd = this.rs.getMetaData();
                assertEquals("java.lang.String", rsmd.getColumnClassName(1));
                this.rs.next();
                asObject = this.rs.getObject(1);
                assertEquals("java.lang.String", asObject.getClass().getName());

                this.rs.close();

                createTable("testBug8868_2", "(field1 CHAR(4) CHARACTER SET BINARY)");
                this.stmt.executeUpdate("INSERT INTO testBug8868_2 VALUES ('abc')");
                this.rs = this.stmt.executeQuery("SELECT field1 FROM testBug8868_2");

                rsmd = this.rs.getMetaData();
                assertEquals("[B", rsmd.getColumnClassName(1));
                this.rs.next();
                asObject = this.rs.getObject(1);
                assertEquals("[B", asObject.getClass().getName());
            } finally {
                if (testConn != this.conn) {
                    testConn.close();
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
            createTable(tableName,
                    "(languageCode char(2) NOT NULL default '', countryCode char(2) NOT NULL default '',"
                            + "supported enum('no','yes') NOT NULL default 'no', ordering int(11) default NULL,"
                            + "createDate datetime NOT NULL default '1000-01-01 00:00:03', modifyDate timestamp NOT NULL default CURRENT_TIMESTAMP on update"
                            + " CURRENT_TIMESTAMP, PRIMARY KEY  (languageCode,countryCode), KEY languageCode (languageCode),"
                            + "KEY countryCode (countryCode), KEY ordering (ordering), KEY modifyDate (modifyDate)) DEFAULT CHARSET=utf8",
                    "InnoDB");

            this.stmt.executeUpdate("INSERT INTO " + tableName + " (languageCode) VALUES ('en')");

            String alias = "someLocale";
            String sql = "select if ( languageCode = ?, ?, ? ) as " + alias + " from " + tableName;
            this.pstmt = this.conn.prepareStatement(sql);

            int count = 1;
            this.pstmt.setObject(count++, "en");
            this.pstmt.setObject(count++, "en_US");
            this.pstmt.setObject(count++, "en_GB");

            this.rs = this.pstmt.executeQuery();

            assertTrue(this.rs.next());

            Object object = this.rs.getObject(alias);

            if (object != null) {
                assertEquals("java.lang.String", object.getClass().getName());
                assertEquals("en_US", object.toString());
            }
        }
    }

    public void testBug9684() throws Exception {
        if (versionMeetsMinimum(4, 1, 9)) {
            String tableName = "testBug9684";

            createTable(tableName, "(sourceText text character set utf8 collate utf8_bin)");
            this.stmt.executeUpdate("INSERT INTO " + tableName + " VALUES ('abc')");
            this.rs = this.stmt.executeQuery("SELECT sourceText FROM " + tableName);
            assertTrue(this.rs.next());
            assertEquals("java.lang.String", this.rs.getString(1).getClass().getName());
            assertEquals("abc", this.rs.getString(1));
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
        createTable(tableName, "(field1 smallint(5) unsigned, field2 tinyint unsigned, field3 int unsigned)");
        this.stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (32768, 255, 4294967295)");
        this.rs = this.conn.prepareStatement("SELECT field1, field2, field3 FROM " + tableName).executeQuery();
        assertTrue(this.rs.next());
        assertEquals(32768, this.rs.getInt(1));
        assertEquals(255, this.rs.getInt(2));
        assertEquals(4294967295L, this.rs.getLong(3));

        assertEquals(String.valueOf(this.rs.getObject(1)), String.valueOf(this.rs.getInt(1)));
        assertEquals(String.valueOf(this.rs.getObject(2)), String.valueOf(this.rs.getInt(2)));
        assertEquals(String.valueOf(this.rs.getObject(3)), String.valueOf(this.rs.getLong(3)));

    }

    public void testBug10212() throws Exception {
        String tableName = "testBug10212";
        createTable(tableName, "(field1 YEAR(4))");
        this.stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (1974)");
        this.rs = this.conn.prepareStatement("SELECT field1 FROM " + tableName).executeQuery();

        ResultSetMetaData rsmd = this.rs.getMetaData();
        assertTrue(this.rs.next());
        assertEquals("java.sql.Date", rsmd.getColumnClassName(1));
        assertEquals("java.sql.Date", this.rs.getObject(1).getClass().getName());

        this.rs = this.stmt.executeQuery("SELECT field1 FROM " + tableName);

        rsmd = this.rs.getMetaData();
        assertTrue(this.rs.next());
        assertEquals("java.sql.Date", rsmd.getColumnClassName(1));
        assertEquals("java.sql.Date", this.rs.getObject(1).getClass().getName());
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
        this.stmt.executeUpdate("INSERT INTO testBug11190 VALUES('3000','L'),('3001','H'),('1050','B')");

        Statement updStmt = null;

        try {
            updStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

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
        createTable("testBug12104", "(field1 GEOMETRY)", "MyISAM");

        if (!versionMeetsMinimum(5, 6)) {
            this.stmt.executeUpdate("INSERT INTO testBug12104 VALUES (GeomFromText('POINT(1 1)'))");
        } else {
            this.stmt.executeUpdate("INSERT INTO testBug12104 VALUES (ST_GeomFromText('POINT(1 1)'))");
        }
        this.pstmt = this.conn.prepareStatement("SELECT field1 FROM testBug12104");
        this.rs = this.pstmt.executeQuery();
        assertTrue(this.rs.next());
        System.out.println(this.rs.getObject(1));
    }

    /**
     * Tests fix for BUG#13043 - when 'gatherPerfMetrics' is enabled for servers
     * < 4.1.0, a NPE is thrown from the constructor of ResultSet if the query
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
                props.put("gatherPerfMetrics", "true"); // this property is reported as the cause of NullPointerException
                props.put("reportMetricsIntervalMillis", "30000"); // this property is reported as the cause of NullPointerException
                perfConn = getConnectionWithProps(props);
                this.rs = perfConn.createStatement().executeQuery("SELECT 1");
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
                assertEquals(sqlEx.getSQLState(), SQLError.SQL_STATE_GENERAL_ERROR);
            }

        } finally {

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
        createTable("testBug14562", "(row_order INT, signed_field MEDIUMINT, unsigned_field MEDIUMINT UNSIGNED)");

        this.stmt.executeUpdate("INSERT INTO testBug14562 VALUES (1, -8388608, 0), (2, 8388607, 16777215)");

        this.rs = this.stmt.executeQuery("SELECT signed_field, unsigned_field FROM testBug14562 ORDER BY row_order");
        traverseResultSetBug14562();

        this.rs = this.conn.prepareStatement("SELECT signed_field, unsigned_field FROM testBug14562 ORDER BY row_order").executeQuery();
        traverseResultSetBug14562();

        if (versionMeetsMinimum(5, 0)) {
            CallableStatement storedProc = null;

            try {
                createProcedure("sp_testBug14562", "() BEGIN SELECT signed_field, unsigned_field FROM testBug14562 ORDER BY row_order; END");
                storedProc = this.conn.prepareCall("{call sp_testBug14562()}");
                storedProc.execute();
                this.rs = storedProc.getResultSet();
                traverseResultSetBug14562();

                createProcedure("sp_testBug14562_1", "(OUT param_1 MEDIUMINT, OUT param_2 MEDIUMINT UNSIGNED)"
                        + "BEGIN SELECT signed_field, unsigned_field INTO param_1, param_2 FROM testBug14562 WHERE row_order=1; END");
                storedProc = this.conn.prepareCall("{call sp_testBug14562_1(?, ?)}");
                storedProc.registerOutParameter(1, Types.INTEGER);
                storedProc.registerOutParameter(2, Types.INTEGER);

                storedProc.execute();

                assertEquals("java.lang.Integer", storedProc.getObject(1).getClass().getName());

                if (versionMeetsMinimum(5, 1) || versionMeetsMinimum(5, 0, 67)) {
                    assertEquals("java.lang.Long", storedProc.getObject(2).getClass().getName());
                } else {
                    assertEquals("java.lang.Integer", storedProc.getObject(2).getClass().getName());
                }

            } finally {
                if (storedProc != null) {
                    storedProc.close();
                }
            }
        }

        this.rs = this.conn.getMetaData().getColumns(this.conn.getCatalog(), null, "testBug14562", "%field");

        assertTrue(this.rs.next());

        assertEquals(Types.INTEGER, this.rs.getInt("DATA_TYPE"));
        assertEquals("MEDIUMINT", this.rs.getString("TYPE_NAME").toUpperCase(Locale.US));

        assertTrue(this.rs.next());

        assertEquals(Types.INTEGER, this.rs.getInt("DATA_TYPE"));
        assertEquals("MEDIUMINT UNSIGNED", this.rs.getString("TYPE_NAME").toUpperCase(Locale.US));

        //
        // The following test is harmless in the 3.1 driver, but is needed for the 5.0 driver, so we'll leave it here
        //
        if (versionMeetsMinimum(5, 0, 14)) {
            Connection infoSchemConn = null;

            try {
                Properties props = new Properties();
                props.setProperty("useInformationSchema", "true");

                infoSchemConn = getConnectionWithProps(props);

                this.rs = infoSchemConn.getMetaData().getColumns(infoSchemConn.getCatalog(), null, "testBug14562", "%field");

                assertTrue(this.rs.next());

                assertEquals(Types.INTEGER, this.rs.getInt("DATA_TYPE"));
                assertEquals("MEDIUMINT", this.rs.getString("TYPE_NAME").toUpperCase(Locale.US));

                assertTrue(this.rs.next());

                assertEquals(Types.INTEGER, this.rs.getInt("DATA_TYPE"));
                assertEquals("MEDIUMINT UNSIGNED", this.rs.getString("TYPE_NAME").toUpperCase(Locale.US));

            } finally {
                if (infoSchemConn != null) {
                    infoSchemConn.close();
                }
            }
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
        createTable("lang_table", "(id int, en varchar(255) CHARACTER SET utf8, cz varchar(255) CHARACTER SET utf8)");

        this.stmt.executeUpdate("insert into table1 values (0, 0)");
        this.stmt.executeUpdate("insert into table2 values (0)");
        this.stmt.executeUpdate("insert into lang_table values (0, 'abcdef', 'ghijkl')");
        this.rs = this.stmt.executeQuery("select a.id, b.id, c.en, c.cz from table1 as a, table2 as b, lang_table as c where a.id = b.id and a.name_id = c.id");
        assertTrue(this.rs.next());
        this.rs.getString("c.cz");

        this.rs = this.stmt.executeQuery("select table1.*, table2.* FROM table1, table2");
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
            createTable("testBug14609", "(field1 int primary key, field2 decimal)");
            this.stmt.executeUpdate("INSERT INTO testBug14609 VALUES (1, 1)");

            PreparedStatement updatableStmt = this.conn.prepareStatement("SELECT field1, field2 FROM testBug14609", ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);

            try {
                this.rs = updatableStmt.executeQuery();
            } finally {
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

        this.stmt.executeUpdate("INSERT INTO testBug16169 (field1) VALUES (0)");

        this.pstmt = this.conn.prepareStatement("SELECT * FROM testBug16169");
        this.rs = this.pstmt.executeQuery();
        assertTrue(this.rs.next());

        assertEquals(0, ((Integer) this.rs.getObject("field1")).intValue());
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

        createTable("testBug16841", "(CID int( 20 ) NOT NULL default '0', OID int( 20 ) NOT NULL AUTO_INCREMENT ,"
                + "PatientID int( 20 ) default NULL , PRIMARY KEY ( CID , OID ) , KEY OID ( OID ) , KEY Path ( CID, PatientID))", "MYISAM");

        String sSQLQuery = "SELECT * FROM testBug16841 WHERE 1 = 0";
        Statement updStmt = null;

        try {
            updStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

            this.rs = updStmt.executeQuery(sSQLQuery);

            this.rs.moveToInsertRow();

            this.rs.updateInt("CID", 1);
            this.rs.updateInt("PatientID", 1);

            this.rs.insertRow();

            this.rs.last();
            assertEquals(1, this.rs.getInt("OID"));
        } finally {

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

            this.stmt.execute("insert into testBug17450 (foo,bar) values ('foo',true)");
            this.stmt.execute("insert into testBug17450 (foo,bar) values (null,true)");

            this.pstmt = this.conn.prepareStatement("select * from testBug17450 where foo=?");
            this.pstmt.setString(1, "foo");
            this.rs = this.pstmt.executeQuery();
            checkResult17450();

            this.pstmt = this.conn.prepareStatement("select * from testBug17450 where foo is null");
            this.rs = this.pstmt.executeQuery();
            checkResult17450();

            this.rs = this.stmt.executeQuery("select * from testBug17450 where foo='foo'");
            checkResult17450();

            this.rs = this.stmt.executeQuery("select * from testBug17450 where foo is null");
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
        this.pstmt = this.conn.prepareStatement("SELECT field1 FROM testBug19282");
        this.stmt.executeUpdate("INSERT INTO testBug19282 VALUES ('abcdefg')");

        this.rs = this.pstmt.executeQuery();
        this.rs.next();
        assertEquals(false, this.rs.wasNull());
        this.rs.getString(1);
        assertEquals(false, this.rs.wasNull());
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
            createTable("testBug19568", "(field1 BOOLEAN," + (versionMeetsMinimum(5, 0, 0) ? "field2 BIT" : "field2 BOOLEAN") + ")");

            this.stmt.executeUpdate("INSERT INTO testBug19568 VALUES (1,0), (0, 1)");

            this.pstmt = this.conn.prepareStatement("SELECT field1, field2 FROM testBug19568 ORDER BY field1 DESC");
            this.rs = this.pstmt.executeQuery();

            checkResultsBug19568();

            this.rs = this.stmt.executeQuery("SELECT field1, field2 FROM testBug19568 ORDER BY field1 DESC");
            checkResultsBug19568();
        }
    }

    private void checkResultsBug19568() throws SQLException {
        // Test all numerical getters, and make sure to alternate true/false across rows so we can catch false-positives if off-by-one errors exist in the
        // column getters.

        for (int i = 0; i < 2; i++) {
            assertTrue(this.rs.next());

            for (int j = 0; j < 2; j++) {
                assertEquals((i == 1 && j == 1) || (i == 0 && j == 0), this.rs.getBoolean(j + 1));
                assertEquals(((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0), this.rs.getBigDecimal(j + 1).intValue());
                assertEquals(((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0), this.rs.getByte(j + 1));
                assertEquals(((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0), this.rs.getShort(j + 1));
                assertEquals(((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0), this.rs.getInt(j + 1));
                assertEquals(((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0), this.rs.getLong(j + 1));
                assertEquals(((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0), this.rs.getFloat(j + 1), .1);
                assertEquals(((i == 1 && j == 1) || (i == 0 && j == 0) ? 1 : 0), this.rs.getDouble(j + 1), .1);
            }
        }
    }

    public void testBug19724() throws Exception {
        if (versionMeetsMinimum(4, 1)) {
            // can't set this via session on 4.0 :(

            createTable("test19724", "(col1 INTEGER NOT NULL, col2 VARCHAR(255) NULL, PRIMARY KEY (col1))");

            this.stmt.execute("INSERT IGNORE INTO test19724 VALUES (0, 'Blah'),(1,'Boo')");

            Connection ansiConn = null;
            Statement updStmt = null;

            Properties props = new Properties();
            props.setProperty("sessionVariables", "sql_mode=ansi");

            try {
                ansiConn = getConnectionWithProps(props);
                updStmt = ansiConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                this.rs = updStmt.executeQuery("SELECT * FROM test19724");

                this.rs.beforeFirst();

                this.rs.next();

                this.rs.updateString("col2", "blah2");
                this.rs.updateRow();
            } finally {
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

        assertEquals("java.lang.Integer", this.rs.getObject(1).getClass().getName());
        assertEquals("java.lang.Integer", this.rs.getObject(2).getClass().getName());

        assertTrue(this.rs.next());

        assertEquals(8388607, this.rs.getInt(1));
        assertEquals(16777215, this.rs.getInt(2));

        assertEquals("java.lang.Integer", this.rs.getObject(1).getClass().getName());
        assertEquals("java.lang.Integer", this.rs.getObject(2).getClass().getName());
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
     * "(1),(4),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19),(20),(21),(22),(23"
     * +
     * "),(24),(25),(26),(27),(28),(29),(30),(31),(32),(33),(34),(35),(36),(37),(38),(39"
     * + "),(40),(41),(42),(43),(45),(46),(47),(48),(49),(50)");
     * 
     * this.stmt .executeUpdate("INSERT INTO `b` VALUES " +
     * "(1),(2),(4),(5),(6),(7),(8),(9),(10),(11),(12),(13),(14),(15),(16),(17),(18),(19"
     * + "),(20)");
     * 
     * this.stmt .executeUpdate("INSERT INTO `c` VALUES " +
     * "(1),(2),(3),(13),(15),(16),(22),(30),(31),(32),(33),(34),(35),(36),(37),(148),(1"
     * +
     * "59),(167),(174),(176),(177),(178),(179),(180),(187),(188),(189),(190),(191),(192"
     * +
     * "),(193),(194),(195),(196),(197),(198),(199),(200),(201),(202),(203),(204),(205),"
     * + "(206),(207),(208)");
     * 
     * this.stmt .executeUpdate("INSERT INTO `problem_table` VALUES " +
     * "(1,1,1,NULL,1),(2,1,4,NULL,1),(3,1,5,NULL,1),(4,1,8,NULL,1),(5,23,1,NULL,1),(6,2"
     * +
     * "3,4,NULL,1),(7,24,1,NULL,1),(8,24,2,NULL,1),(9,24,4,NULL,1),(10,25,1,NULL,1),(11"
     * +
     * ",25,2,NULL,1),(12,25,4,NULL,1),(13,27,1,NULL,1),(14,28,1,NULL,1),(15,29,1,NULL,1"
     * +
     * "),(16,15,2,NULL,1),(17,15,5,NULL,1),(18,15,8,NULL,1),(19,30,1,NULL,1),(20,31,1,N"
     * +
     * "ULL,1),(21,31,4,NULL,1),(22,32,2,NULL,1),(23,32,4,NULL,1),(24,32,6,NULL,1),(25,3"
     * +
     * "2,8,NULL,1),(26,32,10,NULL,1),(27,32,11,NULL,1),(28,32,13,NULL,1),(29,32,16,NULL"
     * +
     * ",1),(30,32,17,NULL,1),(31,32,18,NULL,1),(32,32,19,NULL,1),(33,32,20,NULL,1),(34,"
     * +
     * "33,15,NULL,1),(35,33,15,NULL,1),(36,32,20,206,1),(96,32,9,NULL,1),(100,47,6,NULL"
     * + ",1),(101,47,10,NULL,1),(102,47,5,NULL,1),(105,47,19,NULL,1)");
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

        Properties props = new Properties();
        props.setProperty("useUsageAdvisor", "true");

        advisorConn = getConnectionWithProps(props);
        this.pstmt = advisorConn.prepareStatement("SELECT 1");
        this.rs = this.pstmt.executeQuery();
        this.rs.close();
        this.rs = this.pstmt.executeQuery();
    }

    public void testAllTypesForNull() throws Exception {
        Properties props = new Properties();
        props.setProperty("jdbcCompliantTruncation", "false");
        props.setProperty("zeroDateTimeBehavior", "round");
        Connection conn2 = getConnectionWithProps(props);
        Statement stmt2 = conn2.createStatement();

        DatabaseMetaData dbmd = this.conn.getMetaData();

        this.rs = dbmd.getTypeInfo();

        boolean firstColumn = true;
        int numCols = 1;
        StringBuilder createStatement = new StringBuilder("CREATE TABLE testAllTypes (");
        List<Boolean> wasDatetimeTypeList = new ArrayList<Boolean>();

        while (this.rs.next()) {
            String dataType = this.rs.getString("TYPE_NAME").toUpperCase();

            boolean wasDateTime = false;

            if (dataType.indexOf("DATE") != -1 || dataType.indexOf("TIME") != -1) {
                wasDateTime = true;
            }

            if (!"BOOL".equalsIgnoreCase(dataType) && !"LONG VARCHAR".equalsIgnoreCase(dataType) && !"LONG VARBINARY".equalsIgnoreCase(dataType)
                    && !"ENUM".equalsIgnoreCase(dataType) && !"SET".equalsIgnoreCase(dataType)) {
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

                if ("VARCHAR".equalsIgnoreCase(dataType) || "VARBINARY".equalsIgnoreCase(dataType)) {
                    // we can't use max varchar precision because it is equal to max row length
                    createStatement.append("(255)");

                } else if (dataType.indexOf("CHAR") != -1
                        || dataType.indexOf("BINARY") != -1 && dataType.indexOf("BLOB") == -1 && dataType.indexOf("TEXT") == -1) {
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
        StringBuilder insertStatement = new StringBuilder("INSERT INTO testAllTypes VALUES (NULL");
        for (int i = 1; i < numCols - 1; i++) {
            insertStatement.append(", NULL");
        }
        insertStatement.append(")");
        stmt2.executeUpdate(insertStatement.toString());

        this.rs = stmt2.executeQuery("SELECT * FROM testAllTypes");

        testAllFieldsForNull(this.rs);
        this.rs.close();

        this.rs = this.conn.prepareStatement("SELECT * FROM testAllTypes").executeQuery();
        testAllFieldsForNull(this.rs);

        stmt2.executeUpdate("DELETE FROM testAllTypes");

        insertStatement = new StringBuilder("INSERT INTO testAllTypes VALUES (");

        boolean needsNow = wasDatetimeTypeList.get(0).booleanValue();

        if (needsNow) {
            insertStatement.append("NOW()");
        } else {
            insertStatement.append("0");
        }

        for (int i = 1; i < numCols - 1; i++) {
            needsNow = wasDatetimeTypeList.get(i).booleanValue();
            insertStatement.append(",");
            if (needsNow) {
                insertStatement.append("NOW()");
            } else {
                insertStatement.append("0");
            }
        }

        insertStatement.append(")");

        stmt2.executeUpdate(insertStatement.toString());

        this.rs = stmt2.executeQuery("SELECT * FROM testAllTypes");

        testAllFieldsForNotNull(this.rs, wasDatetimeTypeList);
        this.rs.close();

        this.rs = conn2.prepareStatement("SELECT * FROM testAllTypes").executeQuery();
        testAllFieldsForNotNull(this.rs, wasDatetimeTypeList);

        stmt2.executeUpdate("DROP TABLE IF EXISTS testAllTypes");
    }

    @SuppressWarnings("deprecation")
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

    @SuppressWarnings("deprecation")
    private void testAllFieldsForNotNull(ResultSet rsToTest, List<Boolean> wasDatetimeTypeList) throws Exception {
        ResultSetMetaData rsmd = this.rs.getMetaData();
        int numCols = rsmd.getColumnCount();

        while (rsToTest.next()) {
            for (int i = 0; i < numCols - 1; i++) {
                boolean wasDatetimeType = wasDatetimeTypeList.get(i).booleanValue();
                String typeName = rsmd.getColumnTypeName(i + 1);
                int sqlType = rsmd.getColumnType(i + 1);

                if (!"BIT".equalsIgnoreCase(typeName) && sqlType != Types.BINARY && sqlType != Types.VARBINARY && sqlType != Types.LONGVARBINARY) {
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

                    boolean canBeUsedAsDate = !("java.lang.Boolean".equals(columnClassName) || "java.lang.Double".equals(columnClassName)
                            || "java.lang.Float".equals(columnClassName) || "java.lang.Real".equals(columnClassName)
                            || "java.math.BigDecimal".equals(columnClassName));

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
                        assertNotNull(rsToTest.getURL(i + 1));
                    } catch (SQLException sqlEx) {
                        assertTrue(sqlEx.getMessage().indexOf("URL") != -1);
                    }

                    assertTrue(!rsToTest.wasNull());
                }
            }
        }
    }

    public void testNPEWithStatementsAndTime() throws Exception {
        createTable("testNPETime", "(field1 TIME NULL, field2 DATETIME NULL, field3 DATE NULL)");
        this.stmt.executeUpdate("INSERT INTO testNPETime VALUES (null, null, null)");
        this.pstmt = this.conn.prepareStatement("SELECT field1, field2, field3 FROM testNPETime");
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
    }

    public void testEmptyStringsWithNumericGetters() throws Exception {
        createTable("emptyStringTable", "(field1 char(32))");
        this.stmt.executeUpdate("INSERT INTO emptyStringTable VALUES ('')");
        this.rs = this.stmt.executeQuery("SELECT field1 FROM emptyStringTable");
        assertTrue(this.rs.next());
        createTable("emptyStringTable", "(field1 char(32))");
        this.stmt.executeUpdate("INSERT INTO emptyStringTable VALUES ('')");

        this.rs = this.stmt.executeQuery("SELECT field1 FROM emptyStringTable");
        assertTrue(this.rs.next());
        checkEmptyConvertToZero();

        this.rs = this.conn.prepareStatement("SELECT field1 FROM emptyStringTable").executeQuery();
        assertTrue(this.rs.next());
        checkEmptyConvertToZero();

        Properties props = new Properties();
        props.setProperty("useFastIntParsing", "false");

        Connection noFastIntParseConn = getConnectionWithProps(props);
        Statement noFastIntStmt = noFastIntParseConn.createStatement();

        this.rs = noFastIntStmt.executeQuery("SELECT field1 FROM emptyStringTable");
        assertTrue(this.rs.next());
        checkEmptyConvertToZero();

        this.rs = noFastIntParseConn.prepareStatement("SELECT field1 FROM emptyStringTable").executeQuery();
        assertTrue(this.rs.next());
        checkEmptyConvertToZero();

        //
        // Now, be more pedantic....
        //

        props = new Properties();
        props.setProperty("emptyStringsConvertToZero", "false");

        Connection pedanticConn = getConnectionWithProps(props);
        Statement pedanticStmt = pedanticConn.createStatement();

        this.rs = pedanticStmt.executeQuery("SELECT field1 FROM emptyStringTable");
        assertTrue(this.rs.next());

        checkEmptyConvertToZeroException();

        this.rs = pedanticConn.prepareStatement("SELECT field1 FROM emptyStringTable").executeQuery();
        assertTrue(this.rs.next());
        checkEmptyConvertToZeroException();

        props = new Properties();
        props.setProperty("emptyStringsConvertToZero", "false");
        props.setProperty("useFastIntParsing", "false");

        pedanticConn = getConnectionWithProps(props);
        pedanticStmt = pedanticConn.createStatement();

        this.rs = pedanticStmt.executeQuery("SELECT field1 FROM emptyStringTable");
        assertTrue(this.rs.next());

        checkEmptyConvertToZeroException();

        this.rs = pedanticConn.prepareStatement("SELECT field1 FROM emptyStringTable").executeQuery();
        assertTrue(this.rs.next());
        checkEmptyConvertToZeroException();
    }

    public void testNegativeOneIsTrue() throws Exception {
        if (!versionMeetsMinimum(5, 0, 3)) {
            String tableName = "testNegativeOneIsTrue";
            Connection tinyInt1IsBitConn = null;

            try {
                createTable(tableName, "(field1 BIT)");
                this.stmt.executeUpdate("INSERT INTO " + tableName + " VALUES (-1)");

                Properties props = new Properties();
                props.setProperty("tinyInt1isBit", "true");
                tinyInt1IsBitConn = getConnectionWithProps(props);

                this.rs = tinyInt1IsBitConn.createStatement().executeQuery("SELECT field1 FROM " + tableName);
                assertTrue(this.rs.next());
                assertEquals(true, this.rs.getBoolean(1));

                this.rs = tinyInt1IsBitConn.prepareStatement("SELECT field1 FROM " + tableName).executeQuery();
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
     */
    private void checkEmptyConvertToZeroException() {
        try {
            assertEquals(0, this.rs.getByte(1));
            fail("Should've thrown an exception!");
        } catch (SQLException sqlEx) {
            assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST, sqlEx.getSQLState());
        }
        try {
            assertEquals(0, this.rs.getShort(1));
            fail("Should've thrown an exception!");
        } catch (SQLException sqlEx) {
            assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST, sqlEx.getSQLState());
        }
        try {
            assertEquals(0, this.rs.getInt(1));
            fail("Should've thrown an exception!");
        } catch (SQLException sqlEx) {
            assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST, sqlEx.getSQLState());
        }
        try {
            assertEquals(0, this.rs.getLong(1));
            fail("Should've thrown an exception!");
        } catch (SQLException sqlEx) {
            assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST, sqlEx.getSQLState());
        }
        try {
            assertEquals(0, this.rs.getFloat(1), 0.1);
            fail("Should've thrown an exception!");
        } catch (SQLException sqlEx) {
            assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST, sqlEx.getSQLState());
        }
        try {
            assertEquals(0, this.rs.getDouble(1), 0.1);
            fail("Should've thrown an exception!");
        } catch (SQLException sqlEx) {
            assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST, sqlEx.getSQLState());
        }
        try {
            assertEquals(0, this.rs.getBigDecimal(1).intValue());
            fail("Should've thrown an exception!");
        } catch (SQLException sqlEx) {
            assertEquals(SQLError.SQL_STATE_INVALID_CHARACTER_VALUE_FOR_CAST, sqlEx.getSQLState());
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

        if (versionMeetsMinimum(5, 7, 5)) {
            // Nothing to test, YEAR(2) is removed starting from 5.7.5
            return;
        }

        String tableName = "testBug10485";

        Calendar nydCal = null;

        if (((com.mysql.jdbc.Connection) this.conn).getUseGmtMillisForDatetimes()) {
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

        this.rs = this.conn.prepareStatement("SELECT field1 FROM " + tableName).executeQuery();
        assertTrue(this.rs.next());
        assertEquals(newYears2005.toString(), this.rs.getString(1));

        Properties props = new Properties();
        props.setProperty("yearIsDateType", "false");

        Connection yearShortConn = getConnectionWithProps(props);
        this.rs = yearShortConn.createStatement().executeQuery("SELECT field1 FROM " + tableName);
        assertTrue(this.rs.next());

        String expectedShort = versionMeetsMinimum(5, 6, 6) ? "2005" : "05";

        assertEquals(expectedShort, this.rs.getString(1));

        this.rs = yearShortConn.prepareStatement("SELECT field1 FROM " + tableName).executeQuery();
        assertTrue(this.rs.next());
        assertEquals(expectedShort, this.rs.getString(1));

        if (versionMeetsMinimum(5, 0)) {

            createProcedure("testBug10485", "()\nBEGIN\nSELECT field1 FROM " + tableName + ";\nEND");

            this.rs = this.conn.prepareCall("{CALL testBug10485()}").executeQuery();
            assertTrue(this.rs.next());
            assertEquals(newYears2005.toString(), this.rs.getString(1));

            this.rs = yearShortConn.prepareCall("{CALL testBug10485()}").executeQuery();
            assertTrue(this.rs.next());
            assertEquals(expectedShort, this.rs.getString(1));

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
        createTable("testBug11552", "(field1 INT UNSIGNED, field2 TINYINT UNSIGNED, field3 SMALLINT UNSIGNED, field4 BIGINT UNSIGNED)");
        this.stmt.executeUpdate("INSERT INTO testBug11552 VALUES (2, 2, 2, 2), (4294967294, 255, 32768, 18446744073709551615 )");
        this.rs = this.conn.prepareStatement("SELECT field1, field2, field3, field4 FROM testBug11552 ORDER BY field1 ASC").executeQuery();
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
        assertEquals("18446744073709551615", this.rs.getObject(4).toString());
    }

    /**
     * Tests correct detection of truncation of non-sig digits.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testTruncationOfNonSigDigits() throws Exception {
        if (versionMeetsMinimum(4, 1, 0)) {
            createTable("testTruncationOfNonSigDigits", "(field1 decimal(12,2), field2 varchar(2))", "Innodb");

            this.stmt.executeUpdate("INSERT INTO testTruncationOfNonSigDigits VALUES (123456.2345, 'ab')");

            try {
                this.stmt.executeUpdate("INSERT INTO testTruncationOfNonSigDigits VALUES (1234561234561.2345, 'ab')");
                fail("Should have thrown a truncation error");
            } catch (MysqlDataTruncation truncEx) {
                // We expect this
            }

            try {
                this.stmt.executeUpdate("INSERT INTO testTruncationOfNonSigDigits VALUES (1234.2345, 'abcd')");
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
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20479() throws Exception {
        PreparedStatement updStmt = null;

        createTable("testBug20479", "(field1 INT NOT NULL PRIMARY KEY)");
        this.stmt.executeUpdate("INSERT INTO testBug20479 VALUES (2), (3), (4)");

        try {
            updStmt = this.conn.prepareStatement("SELECT * FROM testBug20479 Where field1 > ? ORDER BY field1", ResultSet.TYPE_SCROLL_SENSITIVE,
                    ResultSet.CONCUR_UPDATABLE);

            updStmt.setInt(1, 1);
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
            if (updStmt != null) {
                updStmt.close();
            }
        }
    }

    /**
     * Tests fix for BUG#20485 - Updatable result set that contains a BIT column
     * fails when server-side prepared statements are used.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20485() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        PreparedStatement updStmt = null;

        createTable("testBug20485", "(field1 INT NOT NULL PRIMARY KEY, field2 BIT)");
        this.stmt.executeUpdate("INSERT INTO testBug20485 VALUES (2, 1), (3, 1), (4, 1)");

        try {
            updStmt = this.conn.prepareStatement("SELECT * FROM testBug20485 ORDER BY field1", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            this.rs = updStmt.executeQuery();
        } finally {
            if (updStmt != null) {
                updStmt.close();
            }
        }
    }

    /**
     * Tests fix for BUG#20306 - ResultSet.getShort() for UNSIGNED TINYINT
     * returns incorrect values when using server-side prepared statements.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20306() throws Exception {
        createTable("testBug20306", "(field1 TINYINT UNSIGNED, field2 TINYINT UNSIGNED)");
        this.stmt.executeUpdate("INSERT INTO testBug20306 VALUES (2, 133)");

        this.pstmt = this.conn.prepareStatement("SELECT field1, field2 FROM testBug20306");
        this.rs = this.pstmt.executeQuery();
        this.rs.next();
        checkBug20306();

        this.rs = this.stmt.executeQuery("SELECT field1, field2 FROM testBug20306");
        this.rs.next();
        checkBug20306();

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
     * Tests fix for BUG#21062 - ResultSet.getSomeInteger() doesn't work for
     * BIT(>1)
     * 
     * @throws Exception
     *             if the test fails.
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
     * @throws Exception
     *             if the test fails.
     */
    public void testBug18880() throws Exception {
        this.rs = this.stmt.executeQuery("SELECT 3.4E38,1.4E-45");
        this.rs.next();
        this.rs.getFloat(1);
        this.rs.getFloat(2);
    }

    /**
     * Tests fix for BUG#15677, wrong values returned from getShort() if SQL
     * values are tinyint unsigned.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug15677() throws Exception {
        createTable("testBug15677", "(id BIGINT, field1 TINYINT UNSIGNED)");
        this.stmt.executeUpdate("INSERT INTO testBug15677 VALUES (1, 0), (2, 127), (3, 128), (4, 255)");
        this.rs = this.conn.prepareStatement("SELECT field1 FROM testBug15677 ORDER BY id ASC").executeQuery();
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
    }

    public void testBooleans() throws Exception {
        if (versionMeetsMinimum(5, 0)) {
            createTable("testBooleans",
                    "(ob int, field1 BOOLEAN, field2 TINYINT, field3 SMALLINT, field4 INT, field5 MEDIUMINT, field6 BIGINT, field7 FLOAT, field8 DOUBLE, field9 DECIMAL, field10 VARCHAR(32), field11 BINARY(3), field12 VARBINARY(3),  field13 BLOB)");
            this.pstmt = this.conn.prepareStatement("INSERT INTO testBooleans VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

            this.pstmt.setInt(1, 1);
            this.pstmt.setBoolean(2, false);
            this.pstmt.setByte(3, (byte) 0);
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
            this.pstmt.setByte(3, (byte) 1);
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
            this.pstmt.setByte(3, (byte) 1);
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
            this.pstmt.setByte(3, (byte) 1);
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
            this.pstmt.setByte(3, (byte) 0);
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
            this.pstmt.setByte(3, (byte) 1);
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
            this.pstmt.setByte(3, (byte) 0);
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

            this.rs = this.stmt.executeQuery(
                    "SELECT field1, field2, field3, field4, field5, field6, field7, field8, field9, field10, field11, field12, field13 FROM testBooleans ORDER BY ob");

            boolean[] testVals = new boolean[] { false, true, true, true, false, true, false };

            int i = 0;

            while (this.rs.next()) {
                for (int j = 0; j > 13; j++) {
                    assertEquals("For field_" + (j + 1) + ", row " + (i + 1), testVals[i], this.rs.getBoolean(j + 1));
                }

                i++;
            }

            this.rs = this.conn.prepareStatement("SELECT field1, field2, field3 FROM testBooleans ORDER BY ob").executeQuery();

            i = 0;

            while (this.rs.next()) {
                for (int j = 0; j > 13; j++) {
                    assertEquals("For field_" + (j + 1) + ", row " + (i + 1), testVals[i], this.rs.getBoolean(j + 1));
                }

                i++;
            }
        }
    }

    /**
     * Tests fix(es) for BUG#21379 - column names don't match metadata in cases
     * where server doesn't return original column names (functions) thus
     * breaking compatibility with applications that expect 1-1 mappings between
     * findColumn() and rsmd.getColumnName().
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug21379() throws Exception {
        //
        // Test the 1-1 mapping between rs.findColumn() and rsmd.getColumnName() in the case where original column names are not returned, thus preserving 
        // pre-C/J 5.0 behavior for these cases
        //

        this.rs = this.stmt.executeQuery("SELECT LAST_INSERT_ID() AS id");
        this.rs.next();
        assertEquals("id", this.rs.getMetaData().getColumnName(1));
        assertEquals(1, this.rs.findColumn("id"));

        if (versionMeetsMinimum(4, 1)) {
            //
            // test complete emulation of C/J 3.1 and earlier behavior through configuration option
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
    }

    /**
     * Tests fix for BUG#21814 - time values outside valid range silently wrap
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug21814() throws Exception {

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

        this.stmt.executeUpdate("insert into testBug24710(x) values(0x0000000000), (0x1111111111), (0x2222222222), (0x3333333333),"
                + "(0x4444444444), (0x5555555555), (0x6666666666), (0x7777777777), (0x8888888888), (0x9999999999), (0xaaaaaaaaaa),"
                + "(0xbbbbbbbbbb), (0xcccccccccc), (0xdddddddddd), (0xeeeeeeeeee), (0xffffffffff)");

        this.rs = this.stmt.executeQuery("select t1.x t1x,(select x from testBug24710 t2 where t2.x=t1.x) t2x from testBug24710 t1");

        assertEquals(Types.VARBINARY, this.rs.getMetaData().getColumnType(1));
        assertEquals(Types.VARBINARY, this.rs.getMetaData().getColumnType(2));

        this.rs = ((com.mysql.jdbc.Connection) this.conn)
                .serverPrepareStatement("select t1.x t1x,(select x from testBug24710 t2 where t2.x=t1.x) t2x from testBug24710 t1").executeQuery();

        assertEquals(Types.VARBINARY, this.rs.getMetaData().getColumnType(1));
        assertEquals(Types.VARBINARY, this.rs.getMetaData().getColumnType(2));
    }

    /**
     * Tests fix for BUG#25328 - BIT(> 1) is returned as java.lang.String from
     * ResultSet.getObject() rather than byte[].
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testbug25328() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        createTable("testBug25382", "(BINARY_VAL BIT(64) NULL)");

        byte[] bytearr = new byte[8];

        this.pstmt = this.conn.prepareStatement("INSERT INTO testBug25382 VALUES(?)");
        this.pstmt.setObject(1, bytearr, java.sql.Types.BINARY);
        assertEquals(1, this.pstmt.executeUpdate());
        this.pstmt.clearParameters();

        this.rs = this.stmt.executeQuery("Select BINARY_VAL from testBug25382");
        this.rs.next();
        assertEquals(this.rs.getObject(1).getClass(), bytearr.getClass());
    }

    /**
     * Tests fix for BUG#25517 - Statement.setMaxRows() is not effective on
     * result sets materialized from cursors.
     * 
     * @throws Exception
     *             if the test fails
     */
    public void testBug25517() throws Exception {
        Connection fetchConn = null;
        Statement fetchStmt = null;

        createTable("testBug25517", "(field1 int)");

        StringBuilder insertBuf = new StringBuilder("INSERT INTO testBug25517 VALUES (1)");

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

            // int[] maxRows = new int[] {1, 4, 5, 11, 12, 13, 16, 50, 51, 52, 100};
            int[] fetchSizes = new int[] { 1, 4, 10, 25, 100 };
            List<Integer> maxRows = new ArrayList<Integer>();
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

                    int maxRowsToExpect = maxRows.get(maxRowIndex).intValue();
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

                    int maxRowsToExpect = maxRows.get(maxRowIndex).intValue();
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
            if (fetchStmt != null) {
                fetchStmt.close();
            }

            if (fetchConn != null) {
                fetchConn.close();
            }
        }
    }

    /**
     * Tests fix for BUG#25787 - java.util.Date should be serialized for
     * PreparedStatement.setObject().
     * 
     * We add a new configuration option "treatUtilDateAsTimestamp", which is
     * false by default, as (1) We already had specific behavior to treat
     * java.util.Date as a java.sql.Timestamp because it's useful to many folks,
     * and (2) that behavior will very likely be in JDBC-post-4.0 as a
     * requirement.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug25787() throws Exception {
        createTable("testBug25787", "(MY_OBJECT_FIELD BLOB)");

        Connection deserializeConn = null;

        Properties props = new Properties();
        props.setProperty("autoDeserialize", "true");
        props.setProperty("treatUtilDateAsTimestamp", "false");

        deserializeConn = getConnectionWithProps(props);

        this.pstmt = deserializeConn.prepareStatement("INSERT INTO testBug25787 (MY_OBJECT_FIELD) VALUES (?)");
        java.util.Date dt = new java.util.Date();

        this.pstmt.setObject(1, dt);
        this.pstmt.execute();

        this.rs = deserializeConn.createStatement().executeQuery("SELECT MY_OBJECT_FIELD FROM testBug25787");
        this.rs.next();
        assertEquals("java.util.Date", this.rs.getObject(1).getClass().getName());
        assertEquals(dt, this.rs.getObject(1));
    }

    public void testTruncationDisable() throws Exception {
        Properties props = new Properties();
        props.setProperty("jdbcCompliantTruncation", "false");
        Connection truncConn = null;

        truncConn = getConnectionWithProps(props);
        this.rs = truncConn.createStatement().executeQuery("SELECT " + Long.MAX_VALUE);
        this.rs.next();
        this.rs.getInt(1);

    }

    public void testUsageAdvisorOnZeroRowResultSet() throws Exception {
        Connection advisorConn = null;
        Statement advisorStmt = null;

        try {
            Properties props = new Properties();
            props.setProperty("useUsageAdvisor", "true");
            props.setProperty("logger", BufferingLogger.class.getName());

            advisorConn = getConnectionWithProps(props);

            advisorStmt = advisorConn.createStatement();

            BufferingLogger.startLoggingToBuffer();

            this.rs = advisorStmt.executeQuery("SELECT 1, 2 LIMIT 0");
            this.rs.next();
            this.rs.close();

            advisorStmt.close();

            advisorStmt = advisorConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

            advisorStmt.setFetchSize(Integer.MIN_VALUE);

            this.rs = advisorStmt.executeQuery("SELECT 1, 2 LIMIT 0");
            this.rs.next();
            this.rs.close();

            if (versionMeetsMinimum(5, 0, 2)) {
                advisorConn.close();

                props.setProperty("useCursorFetch", "true");
                props.setProperty("useServerPrepStmts", "true");

                advisorConn = getConnectionWithProps(props);

                advisorStmt = advisorConn.createStatement();
                advisorStmt.setFetchSize(1);

                this.rs = advisorStmt.executeQuery("SELECT 1, 2 LIMIT 0");
                BufferingLogger.startLoggingToBuffer();
                this.rs.next();
                this.rs.close();
            }

            assertEquals(-1, BufferingLogger.getBuffer().toString()
                    .indexOf(Messages.getString("ResultSet.Possible_incomplete_traversal_of_result_set").substring(0, 10)));
        } finally {
            BufferingLogger.dropBuffer();

            if (advisorStmt != null) {
                advisorStmt.close();
            }

            if (advisorConn != null) {
                advisorConn.close();
            }
        }
    }

    public void testBug25894() throws Exception {
        createTable("bug25894",
                "(tinyInt_type TINYINT DEFAULT 1, tinyIntU_type TINYINT UNSIGNED DEFAULT 1, smallInt_type SMALLINT DEFAULT 1,"
                        + "smallIntU_type SMALLINT UNSIGNED DEFAULT 1, mediumInt_type MEDIUMINT DEFAULT 1, mediumIntU_type MEDIUMINT UNSIGNED DEFAULT 1,"
                        + "int_type INT DEFAULT 1, intU_type INT UNSIGNED DEFAULT 1, bigInt_type BIGINT DEFAULT 1, bigIntU_type BIGINT UNSIGNED DEFAULT 1);");
        this.stmt.executeUpdate("INSERT INTO bug25894 VALUES (-1,1,-1,1,-1,1,-1,1,-1,1)");
        this.rs = this.stmt.executeQuery("SELECT * FROM bug25894");
        java.sql.ResultSetMetaData tblMD = this.rs.getMetaData();
        this.rs.first();
        for (int i = 1; i < tblMD.getColumnCount() + 1; i++) {
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

            System.out.println(i + " .fld: " + tblMD.getColumnName(i) + "T: " + typesName + ", MDC: " + tblMD.getColumnClassName(i) + " "
                    + tblMD.getColumnTypeName(i) + " , getObj: " + this.rs.getObject(i).getClass());
        }

    }

    /**
     * Tests fix for BUG#26173 - fetching rows via cursor retrieves corrupted
     * data.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug26173() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        createTable("testBug26173", "(fkey int, fdate date, fprice decimal(15, 2), fdiscount decimal(5,3))", "InnoDB");
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
        this.rs = this.stmt.executeQuery("SELECT '00:00:00'");
        this.rs.next();
        this.rs.getTime(1);
        assertEquals("00:00:00", this.rs.getTime(1).toString());
        assertEquals("1970-01-01 00:00:00.0", this.rs.getTimestamp(1).toString());
        assertEquals("1970-01-01", this.rs.getDate(1).toString());

        this.rs.close();

        this.rs = this.stmt.executeQuery("SELECT '00/00/0000 00:00:00'");
        this.rs.next();

        try {
            this.rs.getTime(1);
        } catch (SQLException sqlEx) {
            assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());
        }

        try {
            this.rs.getTimestamp(1);
        } catch (SQLException sqlEx) {
            assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());
        }

        try {
            this.rs.getDate(1);
        } catch (SQLException sqlEx) {
            assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());
        }
    }

    /**
     * Tests fix for BUG#27317 - column index < 1 returns misleading error
     * message.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug27317() throws Exception {
        this.rs = this.stmt.executeQuery("SELECT NULL");
        this.rs.next();
        String messageLowBound = null;

        Method[] getterMethods = ResultSet.class.getMethods();
        Integer zeroIndex = new Integer(0);
        Integer twoIndex = new Integer(2);

        for (int i = 0; i < getterMethods.length; i++) {
            Class<?>[] parameterTypes = getterMethods[i].getParameterTypes();

            if (getterMethods[i].getName().startsWith("get") && parameterTypes.length == 1
                    && (parameterTypes[0].equals(Integer.TYPE) || parameterTypes[0].equals(Integer.class))) {
                if (getterMethods[i].getName().equals("getRowId")) {
                    continue; // we don't support this yet, ever?
                }

                try {
                    getterMethods[i].invoke(this.rs, new Object[] { zeroIndex });
                } catch (InvocationTargetException invokeEx) {
                    Throwable ex = invokeEx.getTargetException();

                    if (ex != null && ex instanceof SQLException) {
                        SQLException sqlEx = (SQLException) ex;

                        assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());

                        messageLowBound = sqlEx.getMessage();
                    } else {
                        throw new RuntimeException(Util.stackTraceToString(ex), ex);
                    }
                }

                String messageHighBound = null;

                try {
                    getterMethods[i].invoke(this.rs, new Object[] { twoIndex });
                } catch (InvocationTargetException invokeEx) {
                    Throwable ex = invokeEx.getTargetException();

                    if (ex != null && ex instanceof SQLException) {
                        SQLException sqlEx = (SQLException) ex;

                        assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, sqlEx.getSQLState());

                        messageHighBound = sqlEx.getMessage();
                    } else {
                        throw new RuntimeException(ex);
                    }
                }

                assertNotNull("Exception message null for method " + getterMethods[i], messageHighBound);
                assertNotNull("Exception message null for method " + getterMethods[i], messageLowBound);

                assertTrue(!messageHighBound.equals(messageLowBound));
            }
        }
    }

    /**
     * Tests fix for BUG#28085 - Need more useful error messages for diagnostics
     * when the driver thinks a result set isn't updatable.
     * 
     * @throws Exception
     *             if the tests fail.
     */
    public void testBug28085() throws Exception {

        Statement updStmt = null;

        try {
            createTable("testBug28085_oneKey", "(pk int primary key not null, field2 varchar(3))");

            this.stmt.executeUpdate("INSERT INTO testBug28085_oneKey (pk, field2) VALUES (1, 'abc')");

            createTable("testBug28085_multiKey", "(pk1 int not null, pk2 int not null, field2 varchar(3), primary key (pk1, pk2))");

            this.stmt.executeUpdate("INSERT INTO testBug28085_multiKey VALUES (1,2,'abc')");

            createTable("testBug28085_noKey", "(field1 varchar(3) not null)");

            this.stmt.executeUpdate("INSERT INTO testBug28085_noKey VALUES ('abc')");

            updStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

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
            if (updStmt != null) {
                updStmt.close();
            }
        }
    }

    private void exerciseUpdatableResultSet(int columnUpdateIndex, String messageToCheck) throws Exception {
        this.rs.next();

        try {
            this.rs.updateString(columnUpdateIndex, "def");
        } catch (SQLException sqlEx) {
            checkUpdatabilityMessage(sqlEx, messageToCheck);
        }

        try {
            this.rs.moveToInsertRow();
        } catch (SQLException sqlEx) {
            checkUpdatabilityMessage(sqlEx, messageToCheck);
        }

        try {
            this.rs.deleteRow();
        } catch (SQLException sqlEx) {
            checkUpdatabilityMessage(sqlEx, messageToCheck);
        }

        this.rs.close();
    }

    private void checkUpdatabilityMessage(SQLException sqlEx, String messageToCheck) throws Exception {

        String message = sqlEx.getMessage();

        assertNotNull(message);

        String localizedMessage = Messages.getString(messageToCheck);

        assertTrue("Didn't find required message component '" + localizedMessage + "', instead found:\n\n" + message, message.indexOf(localizedMessage) != -1);
    }

    public void testBug24886() throws Exception {
        Properties props = new Properties();
        props.setProperty("blobsAreStrings", "true");

        Connection noBlobConn = getConnectionWithProps(props);

        createTable("testBug24886", "(sepallength double, sepalwidth double, petallength double, petalwidth double, Class mediumtext, fy TIMESTAMP)");

        noBlobConn.createStatement().executeUpdate("INSERT INTO testBug24886 VALUES (1,2,3,4,'1234', now()),(5,6,7,8,'12345678', now())");
        this.rs = noBlobConn.createStatement()
                .executeQuery("SELECT concat(Class,petallength), COUNT(*) FROM `testBug24886` GROUP BY `concat(Class,petallength)`");
        this.rs.next();
        assertEquals("java.lang.String", this.rs.getObject(1).getClass().getName());

        props.clear();
        props.setProperty("functionsNeverReturnBlobs", "true");
        noBlobConn = getConnectionWithProps(props);
        this.rs = noBlobConn.createStatement()
                .executeQuery("SELECT concat(Class,petallength), COUNT(*) FROM `testBug24886` GROUP BY `concat(Class,petallength)`");
        this.rs.next();

        if (versionMeetsMinimum(4, 1)) {
            assertEquals("java.lang.String", this.rs.getObject(1).getClass().getName());

        }
    }

    /**
     * Tests fix for BUG#30664. Note that this fix only works for MySQL server
     * 5.0.25 and newer, since earlier versions didn't consistently return
     * correct metadata for functions, and thus results from subqueries and
     * functions were indistinguishable from each other, leading to type-related
     * bugs.
     * 
     * @throws Exception
     */
    public void testBug30664() throws Exception {
        if (!versionMeetsMinimum(5, 0, 25)) {
            return;
        }

        createTable("testBug30664_1", "(id int)");
        createTable("testBug30664_2", "(id int, binaryvalue varbinary(255))");

        this.stmt.executeUpdate("insert into testBug30664_1 values (1),(2),(3)");
        this.stmt.executeUpdate("insert into testBug30664_2 values (1,'���'),(2,'����'),(3,' ���')");
        this.rs = this.stmt.executeQuery("select testBug30664_1.id, (select testBug30664_2.binaryvalue from testBug30664_2 "
                + "where testBug30664_2.id=testBug30664_1.id) as value from testBug30664_1");
        ResultSetMetaData tblMD = this.rs.getMetaData();

        for (int i = 1; i < tblMD.getColumnCount() + 1; i++) {
            switch (i) {
                case 1:
                    assertEquals("INT", tblMD.getColumnTypeName(i).toUpperCase());
                    break;
                case 2:
                    assertEquals("VARBINARY", tblMD.getColumnTypeName(i).toUpperCase());
                    break;
            }
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

            multiStmt.execute("SELECT 1 UNION SELECT 2; INSERT INTO testBug33678 VALUES (1); UPDATE testBug33678 set field1=2; "
                    + "INSERT INTO testBug33678 VALUES(3); UPDATE testBug33678 set field1=2 WHERE field1=3; UPDATE testBug33678 set field1=2; SELECT 1");
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
        }
    }

    public void testBug33162() throws Exception {
        if (!versionMeetsMinimum(5, 0)) {
            return;
        }

        this.rs = this.stmt.executeQuery("select now() from dual where 1=0");
        this.rs.next();
        try {
            this.rs.getTimestamp(1); // fails
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
    @Deprecated
    public void testBug34913() throws Exception {
        Timestamp ts = new Timestamp(new Date(109, 5, 1).getTime());

        this.pstmt = ((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement("SELECT 'abcdefghij', ?");
        this.pstmt.setTimestamp(1, ts);
        this.rs = this.pstmt.executeQuery();
        this.rs.next();
        assertTrue(this.rs.getTimestamp(2).getMonth() == 5);
        assertTrue(this.rs.getTimestamp(2).getDate() == 1);
    }

    public void testBug36051() throws Exception {
        this.rs = this.stmt.executeQuery("SELECT '24:00:00'");
        this.rs.next();
        this.rs.getTime(1);
    }

    /**
     * Tests fix for BUG#35610, BUG#35150. We follow the JDBC Spec here, in that
     * the 4.0 behavior is correct, the JDBC-3.0 (and earlier) spec has a bug,
     * but you can get the buggy behavior (allowing column names *and* labels to
     * be used) by setting "useColumnNamesInFindColumn" to "true".
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
        // Retrieves the value of the designated column in the current row of
        // this ResultSet
        // object as an int in the Java programming language.
        //
        // Parameters:
        // columnLabel - the label for the column specified with the SQL AS
        // clause. If the
        // SQL AS clause was not specified, then the label is the name of the
        // column
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
    }

    /**
     * Tests fix for BUG#39911 - We don't retrieve nanos correctly when
     * -parsing- a string for a TIMESTAMP.
     */
    public void testBug39911() throws Exception {
        this.rs = this.stmt.executeQuery("SELECT '2008-09-26 15:47:20.797283'");
        this.rs.next();

        checkTimestampNanos();

        this.rs = ((com.mysql.jdbc.Connection) this.conn).serverPrepareStatement("SELECT '2008-09-26 15:47:20.797283'").executeQuery();
        this.rs.next();

        checkTimestampNanos();

        this.rs.close();
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
        props.put("functionsNeverReturnBlobs", "true");// toggle, no change
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
        this.pstmt.setDouble(3, Long.MAX_VALUE + 1D);
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

        this.pstmt = ((com.mysql.jdbc.Connection) c).serverPrepareStatement("SELECT int_field, long_field, double_field, string_field FROM testRanges");
        this.rs = this.pstmt.executeQuery();
        this.rs.next();
        checkRanges();
        this.rs.close();

        this.pstmt.setFetchSize(Integer.MIN_VALUE);
        this.rs = this.pstmt.executeQuery();
        this.rs.next();
        checkRanges();
        this.rs.close();

        this.pstmt = ((com.mysql.jdbc.Connection) c).clientPrepareStatement("SELECT int_field, long_field, double_field, string_field FROM testRanges");
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
     * Bug #41484 Accessing fields by name after the ResultSet is closed throws
     * NullPointerException.
     */
    public void testBug41484() throws Exception {
        try {
            this.rs = this.stmt.executeQuery("select 1 as abc");
            this.rs.next();
            this.rs.getString("abc");
            this.rs.close();
            this.rs.getString("abc");
        } catch (SQLException ex) {
            /* expected */
            assertEquals(0, ex.getErrorCode());
            assertEquals("S1000", ex.getSQLState());
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
        createTable("bug27431", "(`ID` int(20) NOT NULL auto_increment, `Name` varchar(255) NOT NULL default '', PRIMARY KEY  (`ID`))");

        this.stmt.executeUpdate("INSERT INTO bug27431 (`ID`, `Name`) VALUES 	(1, 'Lucho'),(2, 'Lily'),(3, 'Kiro')");

        Statement updStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        this.rs = updStmt.executeQuery("SELECT ID, Name FROM bug27431");

        while (this.rs.next()) {
            this.rs.deleteRow();
        }

        assertEquals(0, getRowCount("bug27431"));
    }

    public void testBug43759() throws Exception {
        createTable("testtable_bincolumn", "(bincolumn binary(8) NOT NULL, PRIMARY KEY (bincolumn))", "innodb");

        String pkValue1 = "0123456789ABCD90";
        String pkValue2 = "0123456789ABCD00";
        // put some data in it
        this.stmt.executeUpdate("INSERT INTO testtable_bincolumn (bincolumn) VALUES (unhex('" + pkValue1 + "')), (unhex('" + pkValue2 + "'))");

        // cause the bug
        Statement updStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        this.rs = updStmt.executeQuery("SELECT * FROM testtable_bincolumn WHERE bincolumn = unhex('" + pkValue1 + "')");
        assertTrue(this.rs.next());
        this.rs.deleteRow();

        // At this point the row with pkValue1 should be deleted. We'll select
        // it back to see.
        // If the row comes back, the testcase has failed.

        this.rs = this.stmt.executeQuery("SELECT * FROM testtable_bincolumn WHERE bincolumn = unhex('" + pkValue1 + "')");
        assertFalse(this.rs.next());

        // Now, show a case where it happens to work, because the binary data is
        // different
        updStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        this.rs = updStmt.executeQuery("SELECT * FROM testtable_bincolumn WHERE bincolumn = unhex('" + pkValue2 + "')");
        assertTrue(this.rs.next());
        this.rs.deleteRow();

        this.rs = this.stmt.executeQuery("SELECT * FROM testtable_bincolumn WHERE bincolumn = unhex('" + pkValue2 + "')");
        assertFalse(this.rs.next());
    }

    public void testBug32525() throws Exception {
        Connection testConn = this.conn;
        Connection noStringSyncConn = getConnectionWithProps("noDatetimeStringSync=true");
        try {
            if (versionMeetsMinimum(5, 7, 4)) {
                Properties props = new Properties();
                props.put("jdbcCompliantTruncation", "false");
                if (versionMeetsMinimum(5, 7, 5)) {
                    String sqlMode = getMysqlVariable("sql_mode");
                    if (sqlMode.contains("STRICT_TRANS_TABLES")) {
                        sqlMode = removeSqlMode("STRICT_TRANS_TABLES", sqlMode);
                        props.put("sessionVariables", "sql_mode='" + sqlMode + "'");
                    }
                }
                testConn = getConnectionWithProps(props);
                this.stmt = testConn.createStatement();
            }

            createTable("bug32525", "(field1 date, field2 timestamp)");
            this.stmt.executeUpdate("INSERT INTO bug32525 VALUES ('0000-00-00', '0000-00-00 00:00:00')");

            this.rs = ((com.mysql.jdbc.Connection) noStringSyncConn).serverPrepareStatement("SELECT field1, field2 FROM bug32525").executeQuery();
            this.rs.next();
            assertEquals("0000-00-00", this.rs.getString(1));
            assertEquals("0000-00-00 00:00:00", this.rs.getString(2));
        } finally {
            noStringSyncConn.close();
            if (testConn != this.conn) {
                testConn.close();
            }
        }
    }

    public void testBug49797() throws Exception {
        createTable("testBug49797", "(`Id` int(2) not null auto_increment, `abc` char(50) , PRIMARY KEY (`Id`)) ENGINE=MyISAM DEFAULT CHARSET=utf8");
        this.stmt.executeUpdate("INSERT into testBug49797 VALUES (1,'1'),(2,'2'),(3,'3')");
        assertEquals(3, getRowCount("testBug49797"));

        Statement updStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        try {
            this.rs = updStmt.executeQuery("SELECT * FROM testBug49797");
            while (this.rs.next()) {
                this.rs.deleteRow();
            }
            assertEquals(0, getRowCount("testBug49797"));
        } finally {
            updStmt.close();
        }
    }

    public void testBug49516() throws Exception {

        CachedRowSet crs;

        createTable("bug49516", "(`testingID` INT NOT NULL AUTO_INCREMENT PRIMARY KEY, `firstName` TEXT NOT NULL) CHARACTER SET utf8;");
        this.stmt.executeUpdate("insert into bug49516 set firstName ='John'");

        this.rs = this.stmt.executeQuery("select firstName as 'first person' from bug49516");
        this.rs.first();
        assertEquals("John", this.rs.getString("first person"));
        // this.rs.close();
        // this.stmt.close();

        this.rs = this.stmt.executeQuery("select firstName as 'first person' from bug49516");

        crs = (CachedRowSet) Class.forName("com.sun.rowset.CachedRowSetImpl").newInstance();
        crs.populate(this.rs);
        crs.first();

        assertEquals("John", crs.getString(1));
    }

    public void testBug48820() throws Exception {
        if (versionMeetsMinimum(8, 0, 5)) {
            // old_passwords and PASSWORD() were removed since MySQL 8.0.5
            return;
        }

        CachedRowSet crs;

        Connection noBlobsConn = getConnectionWithProps("functionsNeverReturnBlobs=true");

        if (versionMeetsMinimum(5, 6, 6)) {
            this.rs = noBlobsConn.createStatement().executeQuery("SHOW VARIABLES LIKE 'old_passwords'");
            if (this.rs.next()) {
                if (this.rs.getInt(2) == 2) {
                    System.out.println("Skip testBug48820 due to SHA-256 password hashing.");
                    return;
                }
            }
        }

        this.rs = noBlobsConn.createStatement().executeQuery("SELECT PASSWORD ('SOMETHING')");
        this.rs.first();

        String fromPlainResultSet = this.rs.getString(1);

        this.rs = noBlobsConn.createStatement().executeQuery("SELECT PASSWORD ('SOMETHING')");

        crs = (CachedRowSet) Class.forName("com.sun.rowset.CachedRowSetImpl").newInstance();
        crs.populate(this.rs);
        crs.first();

        assertEquals(fromPlainResultSet, crs.getString(1));
    }

    /**
     * Bug #60313 bug in com.mysql.jdbc.ResultSetRow.getTimestampFast
     */
    public void testBug60313() throws Exception {
        this.stmt.execute("select repeat('Z', 3000), now() + interval 1 microsecond");
        this.rs = this.stmt.getResultSet();
        assertTrue(this.rs.next());
        assertEquals(1000, this.rs.getTimestamp(2).getNanos());
        this.rs.close();

        this.pstmt = this.conn.prepareStatement("select repeat('Z', 3000), now() + interval 1 microsecond");
        this.rs = this.pstmt.executeQuery();
        assertTrue(this.rs.next());
        assertEquals(1000, this.rs.getTimestamp(2).getNanos());
        this.rs.close();

        Properties props = new Properties();
        props.setProperty("useServerPrepStmts", "true");
        Connection sspsCon = getConnectionWithProps(props);
        PreparedStatement ssPStmt = sspsCon.prepareStatement("select repeat('Z', 3000), now() + interval 1 microsecond");
        this.rs = ssPStmt.executeQuery();
        assertTrue(this.rs.next());
        assertEquals(1000, this.rs.getTimestamp(2).getNanos());
        this.rs.close();
        ssPStmt.close();
        sspsCon.close();
    }

    /**
     * Tests fix for BUG#65503 - ResultSets created by PreparedStatement.getGeneratedKeys() are not close()d.
     * 
     * To get results quicker add option -Xmx10M, with this option I got an out of memory failure after about 6500 passes.
     * Since it's a very long test it is disabled by default.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug65503() throws Exception {
        if (!this.DISABLED_testBug65503) {
            createTable("testBug65503", "(id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY, value INTEGER)");

            PreparedStatement pStmt = this.conn.prepareStatement("INSERT INTO testBug65503 (value) VALUES (?)", Statement.RETURN_GENERATED_KEYS),
                    stmt2 = this.conn.prepareStatement("SELECT * FROM testBug65503 LIMIT 6");
            for (int i = 0; i < 100000000; ++i) {
                pStmt.setString(1, "48");
                pStmt.executeUpdate();

                ResultSet result = pStmt.getGeneratedKeys();
                result.next();
                result.getInt(1);
                result.next();

                result = stmt2.executeQuery();
                while (result.next()) {
                }

                if (i % 500 == 0) {
                    System.out.printf("free-mem: %d, id: %d\n", Runtime.getRuntime().freeMemory() / 1024 / 1024, i);
                    this.conn.createStatement().execute("TRUNCATE TABLE testBug65503");
                }
            }
        }
    }

    /**
     * Tests fix for BUG#64204 - ResultSet.close hangs if streaming query is killed
     * 
     * @throws Exception
     */
    public void testBug64204() throws Exception {
        final Properties props = new Properties();
        props.setProperty("socketTimeout", "30000");

        this.conn = getConnectionWithProps(props);
        this.conn.setCatalog("information_schema");
        this.conn.setAutoCommit(true);

        this.stmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        this.stmt.setFetchSize(Integer.MIN_VALUE); // turn on streaming mode

        this.rs = this.stmt.executeQuery("SELECT CONNECTION_ID()");
        this.rs.next();
        final String connectionId = this.rs.getString(1);
        this.rs.close();

        System.out.println("testBug64204.main: PID is " + connectionId);

        ScheduledExecutorService es = Executors.newSingleThreadScheduledExecutor();
        es.schedule(new Callable<Boolean>() {

            public Boolean call() throws Exception {
                boolean res = false;
                Connection con2 = getConnectionWithProps(props);
                con2.setCatalog("information_schema");
                con2.setAutoCommit(true);

                Statement st2 = con2.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                st2.setFetchSize(Integer.MIN_VALUE); // turn on streaming mode
                try {
                    System.out.println("testBug64204.slave: Running KILL QUERY " + connectionId);
                    st2.execute("KILL QUERY " + connectionId + ";");

                    Thread.sleep(5000);
                    System.out.println("testBug64204.slave: parent thread should be hung now!!!");
                    res = true;
                } finally {
                    st2.close();
                    con2.close();
                }

                System.out.println("testBug64204.slave: Done.");
                return res;
            }
        }, 10, TimeUnit.SECONDS);

        try {
            this.rs = this.stmt.executeQuery("SELECT sleep(5) FROM character_sets LIMIT 10");

            int rows = 0;
            int columnCount = this.rs.getMetaData().getColumnCount();
            System.out.println("testBug64204.main: fetched result set, " + columnCount + " columns");

            long totalDataCount = 0;
            while (this.rs.next()) {
                rows++;
                //get row size
                long rowSize = 0;
                for (int i = 0; i < columnCount; i++) {
                    String s = this.rs.getString(i + 1);
                    if (s != null) {
                        rowSize += s.length();
                    }
                }
                totalDataCount += rowSize;
            }

            System.out.println("testBug64204.main: character_sets total rows " + rows + ", data " + totalDataCount);

        } catch (SQLException se) {
            assertEquals("ER_QUERY_INTERRUPTED expected.", "70100", se.getSQLState());
            if (!"70100".equals(se.getSQLState())) {
                throw se;
            }
        }
    }

    /**
     * Bug #45757 - ResultSet.updateRow should throw SQLException when cursor is on insert row
     */
    public void testBug45757() throws SQLException {
        createTable("bug45757", "(id INTEGER NOT NULL PRIMARY KEY)");
        this.stmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        this.rs = this.stmt.executeQuery("select id from bug45757");
        this.rs.moveToInsertRow();
        try {
            this.rs.updateRow();
            fail("updateRow() should throw an exception, not allowed to be called on insert row");
        } catch (SQLException sqlEx) {
            assertTrue(sqlEx.getMessage().startsWith("Can not call updateRow() when on insert row."));
        }
    }

    /**
     * Tests fix for BUG#38252 - ResultSet.absolute(0) is not behaving according to JDBC specification.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug38252() throws Exception {
        createTable("testBug38252", "(id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY)");

        this.stmt = this.conn.createStatement();
        this.stmt.executeUpdate("INSERT INTO testBug38252 VALUES (NULL), (NULL)");

        this.rs = this.stmt.executeQuery("SELECT * FROM testBug38252");

        // test ResultSet.absolute(0) before iterating the ResultSet
        assertFalse("Cursor should be moved to before the first row.", this.rs.absolute(0));
        assertTrue("ResultSet's cursor should be at 'before first'.", this.rs.isBeforeFirst());
        assertTrue("First row expected from ResultSet.", this.rs.next());
        assertTrue("Second row expected from ResultSet.", this.rs.next());
        assertFalse("No more rows expected from ResultSet.", this.rs.next());
        assertTrue("ResultSet's cursor should be at 'after last'.", this.rs.isAfterLast());

        // test ResultSet.absolute(0) after iterating the ResultSet
        assertFalse("Cursor should be moved to before the first row.", this.rs.absolute(0));
        assertTrue("ResultSet's cursor should be at 'before first'.", this.rs.isBeforeFirst());
        assertTrue("First row expected from ResultSet.", this.rs.next());
        assertTrue("Second row expected from ResultSet.", this.rs.next());
        assertFalse("No more rows expected from ResultSet.", this.rs.next());
        assertTrue("ResultSet's cursor should be at 'after last'.", this.rs.isAfterLast());

        this.rs.close();
        this.stmt.close();

        // test ResultSet.absolute(0) with an empty ResultSet
        this.stmt = this.conn.createStatement();
        this.rs = this.stmt.executeQuery("SELECT * FROM testBug38252 where 0 = 1");
        assertFalse("Cursor should be moved to before the first row.", this.rs.absolute(0));
    }

    /**
     * Tests fix for Bug#67318 - SQLException thrown on already closed ResultSet
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug67318() throws Exception {
        Properties props = new Properties();
        props.setProperty("useServerPrepStmts", "true");
        props.setProperty("exceptionInterceptors", "testsuite.regression.ResultSetRegressionTest$TestBug67318ExceptionInterceptor");

        Connection c = null;
        try {
            c = getConnectionWithProps(props);
            ExceptionInterceptorChain eic = (ExceptionInterceptorChain) ((MySQLConnection) c).getExceptionInterceptor();

            TestBug67318ExceptionInterceptor ei = null;
            for (Extension ext : eic.getInterceptors()) {
                if (ext instanceof TestBug67318ExceptionInterceptor) {
                    ei = (TestBug67318ExceptionInterceptor) ext;
                    break;
                }
            }

            if (ei == null) {
                fail("TestBug67318ExceptionInterceptor is not found on connection");
            }

            Statement st1 = c.createStatement();
            ResultSet rs1 = st1.executeQuery("select 1");
            rs1.close();
            rs1.close();
            assertEquals("Operation not allowed after ResultSet closed exception shouldn't be thrown second time", 0, ei.alreadyClosedCounter);
            st1.close();
            st1.close();
            ((StatementImpl) st1).isClosed();
            assertEquals("No operations allowed after statement closed exception shouldn't be thrown second time", 0, ei.alreadyClosedCounter);

            PreparedStatement ps1 = c.prepareStatement("select 1");
            ps1.close();
            ps1.close();
            assertEquals("No operations allowed after statement closed exception shouldn't be thrown second time", 0, ei.alreadyClosedCounter);

        } finally {
            if (c != null) {
                c.close();
            }
        }

    }

    public static class TestBug67318ExceptionInterceptor implements ExceptionInterceptor {

        public int alreadyClosedCounter = 0;

        public void init(com.mysql.jdbc.Connection conn, Properties props) throws SQLException {
        }

        public void destroy() {
        }

        public SQLException interceptException(SQLException sqlEx, com.mysql.jdbc.Connection conn) {

            sqlEx.printStackTrace();

            if ("Operation not allowed after ResultSet closed".equals(sqlEx.getMessage())
                    || "No operations allowed after statement closed.".equals(sqlEx.getMessage())) {
                this.alreadyClosedCounter++;
            }
            return sqlEx;
        }

    }

    /**
     * Tests fix for BUG#72000 - java.lang.ArrayIndexOutOfBoundsException on java.sql.ResultSet.getInt(String).
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug72000() throws Exception {
        final ResultSet testRS = this.stmt.executeQuery("SELECT ' '");

        assertTrue(testRS.next());

        assertThrows(SQLException.class, "Bad format for BigDecimal ' ' in column 1.", new Callable<Void>() {
            public Void call() throws Exception {
                testRS.getBigDecimal(1);
                return null;
            }
        });
        assertFalse(testRS.getBoolean(1));
        assertThrows(SQLException.class, "Value '' is out of range \\[-127,127\\]", new Callable<Void>() {
            public Void call() throws Exception {
                testRS.getByte(1);
                return null;
            }
        });
        assertThrows(SQLException.class, "Value ' ' can not be represented as java.sql.Date", new Callable<Void>() {
            public Void call() throws Exception {
                testRS.getDate(1);
                return null;
            }
        });
        assertThrows(SQLException.class, "Bad format for number ' ' in column 1.", new Callable<Void>() {
            public Void call() throws Exception {
                testRS.getDouble(1);
                return null;
            }
        });
        assertThrows(SQLException.class, "Invalid value for getFloat\\(\\) - ' ' in column 1", new Callable<Void>() {
            public Void call() throws Exception {
                testRS.getFloat(1);
                return null;
            }
        });
        assertThrows(SQLException.class, "Invalid value for getInt\\(\\) - ' '", new Callable<Void>() {
            public Void call() throws Exception {
                testRS.getInt(1);
                return null;
            }
        });
        assertThrows(SQLException.class, "Invalid value for getLong\\(\\) - ' '", new Callable<Void>() {
            public Void call() throws Exception {
                testRS.getLong(1);
                return null;
            }
        });
        assertThrows(SQLException.class, "Invalid value for getShort\\(\\) - ' '", new Callable<Void>() {
            public Void call() throws Exception {
                testRS.getShort(1);
                return null;
            }
        });
        assertThrows(SQLException.class, "Value ' ' can not be represented as java.sql.Time", new Callable<Void>() {
            public Void call() throws Exception {
                testRS.getTime(1);
                return null;
            }
        });
        assertThrows(SQLException.class, "Value ' ' can not be represented as java.sql.Timestamp", new Callable<Void>() {
            public Void call() throws Exception {
                testRS.getTimestamp(1);
                return null;
            }
        });
    }

    /**
     * Tests fix for BUG#72023 - Avoid byte array creation in MysqlIO#unpackBinaryResultSetRow.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug72023() throws Exception {
        // null bitmask contains 2 reserved bits plus 1 bit per field
        //
        // boundary cases at 8n - 2 / 8n - 1 field count; e.g. 6/7, 14/15
        String[] selectList = new String[] { "NULL", "1", "NULL,NULL,NULL,NULL,NULL,NULL", "1,NULL,NULL,1,1,NULL", "1,1,1,1,1,1",
                "NULL,NULL,NULL,NULL,NULL,NULL,NULL", "1,1,1,NULL,1,NULL,NULL", "1,1,1,1,1,1,1",
                "NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL", "NULL,NULL,NULL,1,NULL,1,NULL,NULL,1,NULL,1,1,NULL,NULL",
                "1,1,1,1,1,1,1,1,1,1,1,1,1,1", "NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL",
                "NULL,1,NULL,1,NULL,NULL,1,NULL,1,NULL,NULL,NULL,NULL,1,1", "1,1,1,1,1,1,1,1,1,1,1,1,1,1,1" };

        Connection testConn = getConnectionWithProps("useServerPrepStmts=true");
        PreparedStatement testPstmt;
        ResultSet testRS;

        for (int i = 0, s = selectList.length; i < s; i++) {
            String sl = selectList[i];
            testPstmt = testConn.prepareStatement("SELECT " + sl);
            testRS = testPstmt.executeQuery();
            assertTrue(testRS.next());
            int j = 1;
            for (String fld : sl.split(",")) {
                if (fld.equals("NULL")) {
                    assertNull("Bad results for query " + i + ", field " + j, testRS.getObject(j));
                } else {
                    assertEquals("Bad results for query " + i + ", field " + j, 1, testRS.getInt(j));
                }
                j++;
            }
            assertFalse(testRS.next());
            testRS.close();
            testPstmt.close();
        }
        testConn.close();
    }

    /**
     * Tests fix for BUG#75309 - mysql connector/J driver in streaming mode will in the blocking state.
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug75309() throws Exception {
        if (!versionMeetsMinimum(5, 5)) {
            return;
        }

        Connection testConn = getConnectionWithProps("socketTimeout=1000");
        Statement testStmt = testConn.createStatement();

        // turn on streaming results.
        testStmt.setFetchSize(Integer.MIN_VALUE);

        final ResultSet testRs1 = testStmt.executeQuery("SELECT 1 + 18446744073709551615");

        assertThrows(SQLException.class, "Data truncation: BIGINT UNSIGNED value is out of range in '\\(1 \\+ 18446744073709551615\\)'", new Callable<Void>() {
            public Void call() throws Exception {
                testRs1.next();
                return null;
            }
        });

        try {
            testRs1.close();
        } catch (CommunicationsException ex) {
            fail("ResultSet.close() locked while trying to read remaining, nonexistent, streamed data.");
        }

        try {
            ResultSet testRs2 = testStmt.executeQuery("SELECT 1");
            assertTrue(testRs2.next());
            assertEquals(1, testRs2.getInt(1));
            testRs2.close();
        } catch (SQLException ex) {
            if (ex.getMessage().startsWith("Streaming result set")) {
                fail("There is a Streaming result set still active. No other statements can be issued on this connection.");
            } else {
                ex.printStackTrace();
                fail(ex.getMessage());
            }
        }

        testStmt.close();
        testConn.close();
    }

    /**
     * Tests fix for BUG#19536760 - GETSTRING() CALL AFTER RS.RELATIVE() RETURNS NULLPOINTEREXCEPTION
     * 
     * @throws Exception
     *             if the test fails.
     */
    public void testBug19536760() throws Exception {

        createTable("testBug19536760", "(id int)");

        this.stmt.execute("insert into testBug19536760 values(1),(2),(3)");
        this.rs = this.stmt.executeQuery("select * from testBug19536760");

        // "before first" check
        testBug19536760CheckStates(this.rs, true, false, false, false);

        assertFalse(this.rs.previous());
        assertFalse(this.rs.previous());
        assertFalse(this.rs.previous());
        testBug19536760CheckStates(this.rs, true, false, false, false);

        assertFalse(this.rs.absolute(-7));
        testBug19536760CheckStates(this.rs, true, false, false, false);

        assertTrue(this.rs.next());
        this.rs.beforeFirst();
        testBug19536760CheckStates(this.rs, true, false, false, false);

        // "first" check
        this.rs.next();
        testBug19536760CheckStates(this.rs, false, true, false, false);

        this.rs.absolute(-3);
        testBug19536760CheckStates(this.rs, false, true, false, false);

        assertTrue(this.rs.relative(1));
        assertTrue(this.rs.previous());
        testBug19536760CheckStates(this.rs, false, true, false, false);

        this.rs.absolute(2);
        testBug19536760CheckStates(this.rs, false, false, false, false);
        this.rs.first();
        testBug19536760CheckStates(this.rs, false, true, false, false);

        // "last" check
        this.rs.absolute(-1);
        testBug19536760CheckStates(this.rs, false, false, true, false);

        assertFalse(this.rs.next());
        testBug19536760CheckStates(this.rs, false, false, false, true);
        assertTrue(this.rs.previous());
        testBug19536760CheckStates(this.rs, false, false, true, false);

        assertFalse(this.rs.relative(1));
        testBug19536760CheckStates(this.rs, false, false, false, true);
        assertTrue(this.rs.relative(-1));
        testBug19536760CheckStates(this.rs, false, false, true, false);

        assertTrue(this.rs.relative(-1));
        testBug19536760CheckStates(this.rs, false, false, false, false);
        this.rs.last();
        testBug19536760CheckStates(this.rs, false, false, true, false);

        // "after last" check
        assertFalse(this.rs.next());
        assertFalse(this.rs.next());
        assertFalse(this.rs.next());
        testBug19536760CheckStates(this.rs, false, false, false, true);

        assertTrue(this.rs.relative(-1));
        testBug19536760CheckStates(this.rs, false, false, true, false);

        assertFalse(this.rs.relative(3));
        testBug19536760CheckStates(this.rs, false, false, false, true);

        assertTrue(this.rs.previous());
        testBug19536760CheckStates(this.rs, false, false, true, false);

        this.rs.afterLast();
        testBug19536760CheckStates(this.rs, false, false, false, true);

        assertFalse(this.rs.next());
        testBug19536760CheckStates(this.rs, false, false, false, true);

        // empty result set
        this.rs = this.stmt.executeQuery("select * from testBug19536760 where id=5");
        assertFalse(this.rs.first());
        assertFalse(this.rs.last());

        testBug19536760CheckStates(this.rs, false, false, false, false);

        assertFalse(this.rs.next());
        testBug19536760CheckStates(this.rs, false, false, false, false);

        assertFalse(this.rs.relative(2));
        testBug19536760CheckStates(this.rs, false, false, false, false);

    }

    private void testBug19536760CheckStates(ResultSet rset, boolean expectedIsBeforeFirst, boolean expectedIsFirst, boolean expectedIsLast,
            boolean expectedIsAfterLast) throws Exception {
        assertEquals(expectedIsBeforeFirst, rset.isBeforeFirst());
        assertEquals(expectedIsFirst, rset.isFirst());
        assertEquals(expectedIsLast, rset.isLast());
        assertEquals(expectedIsAfterLast, rset.isAfterLast());
    }

    /**
     * Tests for fix to BUG#20804635 - GETTIME() AND GETDATE() FUNCTIONS FAILS WHEN FRACTIONAL PART EXISTS
     *
     * @throws Exception
     *             if the test fails
     */
    public void testBug20804635() throws Exception {
        if (!versionMeetsMinimum(5, 6, 4)) {
            return; // fractional seconds are not supported in previous versions
        }

        createTable("testBug20804635", "(c1 timestamp(2), c2 time(3), c3 datetime(4))");
        this.stmt.executeUpdate("INSERT INTO testBug20804635 VALUES ('2031-01-15 03:14:07.339999','12:59:00.9889','2031-01-15 03:14:07.333399')");

        Calendar cal = Calendar.getInstance();

        Connection testConn;
        ResultSet rset;
        Properties props = new Properties();
        props.setProperty("useFastDateParsing", "true");

        for (int i = 0; i < 2; i++) {
            System.out.println("With useFastDateParsing=" + props.getProperty("useFastDateParsing"));
            testConn = getConnectionWithProps(props);
            rset = testConn.createStatement().executeQuery("SELECT * FROM testBug20804635");
            rset.next();

            assertEquals("2031-01-15", rset.getDate(1).toString());
            assertEquals("2031-01-15", rset.getDate(1, cal).toString());
            assertEquals("03:14:07", rset.getTime(1).toString());
            assertEquals("03:14:07", rset.getTime(1, cal).toString());
            assertEquals("2031-01-15 03:14:07.34", rset.getTimestamp(1).toString());
            assertEquals("2031-01-15 03:14:07.34", rset.getTimestamp(1, cal).toString());

            assertEquals("1970-01-01", rset.getDate(2).toString());
            assertEquals("1970-01-01", rset.getDate(2, cal).toString());
            assertEquals("12:59:00", rset.getTime(2).toString());
            assertEquals("12:59:00", rset.getTime(2, cal).toString());
            assertEquals("1970-01-01 12:59:00.989", rset.getTimestamp(2).toString());
            assertEquals("1970-01-01 12:59:00.989", rset.getTimestamp(2, cal).toString());

            assertEquals("2031-01-15", rset.getDate(3).toString());
            assertEquals("2031-01-15", rset.getDate(3, cal).toString());
            assertEquals("03:14:07", rset.getTime(3).toString());
            assertEquals("03:14:07", rset.getTime(3, cal).toString());
            assertEquals("2031-01-15 03:14:07.3334", rset.getTimestamp(3).toString());
            assertEquals("2031-01-15 03:14:07.3334", rset.getTimestamp(3, cal).toString());

            testConn.close();
            props.setProperty("useFastDateParsing", "false");
        }
    }

    /**
     * Tests fix for Bug#80522 - Using useCursorFetch leads to data corruption in Connector/J for TIME type.
     */
    public void testBug80522() throws Exception {
        createTable("testBug80522", "(t TIME, d DATE, s TEXT)");

        Properties props = new Properties();
        String sqlMode = getMysqlVariable("sql_mode");
        if (sqlMode.contains("NO_ZERO_DATE")) {
            sqlMode = removeSqlMode("NO_ZERO_DATE", sqlMode);
            props.put("sessionVariables", "sql_mode='" + sqlMode + "'");
        }
        props.setProperty("traceProtocol", "false");
        props.setProperty("defaultFetchSize", "5");
        props.setProperty("useCursorFetch", "true");
        Connection testConn = getConnectionWithProps(props);
        Statement testStmt = testConn.createStatement();

        testStmt.executeUpdate("INSERT INTO testBug80522 VALUES ('00:00:00', '0000-00-00', 'Zeros')");
        final ResultSet testRs = testStmt.executeQuery("SELECT * FROM testBug80522");
        assertTrue(testRs.next());
        assertEquals(new Timestamp(TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd HH:mm:ss", null, null).parse("1970-01-01 00:00:00").getTime()),
                testRs.getTimestamp(1));
        assertThrows(SQLException.class, "Value '0000-00-00' can not be represented as java\\.sql\\.Timestamp", new Callable<Void>() {
            public Void call() throws Exception {
                System.out.println(testRs.getTimestamp(2));
                return null;
            }
        });
        assertEquals("Zeros", testRs.getString(3));

        testRs.close();
        testStmt.close();
        testConn.close();
    }

    /**
     * Tests fix for Bug#56479 - getTimestamp throws exception.
     * 
     * This bug occurs exclusively on UpdatableResultSets when retrieving previously set timestamp values.
     */
    public void testBug56479() throws Exception {
        if (!versionMeetsMinimum(5, 6)) {
            return;
        }

        String tsStr1 = "2010-09-02 03:55:10";
        String tsStr2 = "2010-09-02 03:55:10.123456";
        Timestamp ts1 = Timestamp.valueOf(tsStr1);
        Timestamp ts2 = Timestamp.valueOf(tsStr2);

        createTable("testBug56479", "(id INT PRIMARY KEY, ts1 TIMESTAMP NULL, ts2 TIMESTAMP(6) NULL)", "InnoDB");
        this.stmt.executeUpdate("INSERT INTO testBug56479 VALUES (1, '" + tsStr1 + "', '" + tsStr2 + "'), (2, '" + tsStr1 + "', '" + tsStr2 + "')");

        Statement testStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
        ResultSet testRs = testStmt.executeQuery("SELECT * FROM testBug56479");

        // Initial verifications.
        assertTrue(testRs.next());
        assertEquals(1, testRs.getInt(1));
        assertEquals(ts1, testRs.getTimestamp(2));
        assertEquals(ts2, testRs.getTimestamp(3));
        assertTrue(testRs.next());
        assertEquals(2, testRs.getInt(1));
        assertEquals(ts1, testRs.getTimestamp(2));
        assertEquals(ts2, testRs.getTimestamp(3));
        assertFalse(testRs.next());

        // Update second row to null.
        testRs.absolute(2);
        testRs.updateNull(2);
        testRs.updateNull(3);
        testRs.updateRow();
        assertEquals(2, testRs.getInt(1));
        assertNull(testRs.getTimestamp(2));
        assertNull(testRs.getTimestamp(3));
        testRs.beforeFirst();

        // Check data changes using a plain ResultSet.
        this.rs = this.stmt.executeQuery("SELECT * FROM testBug56479");
        assertTrue(this.rs.next());
        assertEquals(1, this.rs.getInt(1));
        assertEquals(ts1, this.rs.getTimestamp(2));
        assertEquals(ts2, this.rs.getTimestamp(3));
        assertTrue(this.rs.next());
        assertEquals(2, this.rs.getInt(1));
        assertNull(this.rs.getTimestamp(2));
        assertNull(this.rs.getTimestamp(3));
        assertFalse(this.rs.next());

        // Update second row to original values.
        testRs.absolute(2);
        testRs.updateTimestamp(2, ts1);
        testRs.updateTimestamp(3, ts2);
        testRs.updateRow();
        assertEquals(2, testRs.getInt(1));
        assertEquals(ts1, testRs.getTimestamp(2));
        assertEquals(ts2, testRs.getTimestamp(3));
        testRs.beforeFirst();

        // Check data changes using a plain ResultSet.
        this.rs = this.stmt.executeQuery("SELECT * FROM testBug56479");
        assertTrue(this.rs.next());
        assertEquals(1, this.rs.getInt(1));
        assertEquals(ts1, this.rs.getTimestamp(2));
        assertEquals(ts2, this.rs.getTimestamp(3));
        assertTrue(this.rs.next());
        assertEquals(2, this.rs.getInt(1));
        assertEquals(ts1, this.rs.getTimestamp(2));
        assertEquals(ts2, this.rs.getTimestamp(3));
        assertFalse(this.rs.next());

        // Insert new row.
        testRs.moveToInsertRow();
        testRs.updateInt(1, 3);
        testRs.updateTimestamp(2, ts1);
        testRs.updateTimestamp(3, ts2);
        testRs.insertRow();
        assertEquals(3, testRs.getInt(1));
        assertEquals(ts1, testRs.getTimestamp(2));
        assertEquals(ts2, testRs.getTimestamp(3));
        testRs.beforeFirst();

        // Check final data using a plain ResultSet.
        this.rs = this.stmt.executeQuery("SELECT * FROM testBug56479");
        assertTrue(this.rs.next());
        assertEquals(1, this.rs.getInt(1));
        assertEquals(ts1, this.rs.getTimestamp(2));
        assertEquals(ts2, this.rs.getTimestamp(3));
        assertTrue(this.rs.next());
        assertEquals(2, this.rs.getInt(1));
        assertEquals(ts1, this.rs.getTimestamp(2));
        assertEquals(ts2, this.rs.getTimestamp(3));
        assertTrue(this.rs.next());
        assertEquals(3, this.rs.getInt(1));
        assertEquals(ts1, this.rs.getTimestamp(2));
        assertEquals(ts2, this.rs.getTimestamp(3));
        assertFalse(this.rs.next());
    }

    /**
     * Tests fix for Bug#78685 - Wrong results when retrieving the value of a BIT column as an integer.
     */
    public void testBug78685() throws Exception {
        createTable("testBug78685", "(b1 BIT(8), b2 BIT(16), b3 BIT(24))", "InnoDB");
        // 46 == b'00101110' == '.'
        // 11822 == b'0010111000101110' == '..'
        // --
        // 47 == '/'
        // 12079 == '//'
        // --
        // 48 == '0'
        // 12336 = '00'
        // --
        // 49 == b'00110001' == '1'
        // 12593 == b'0011000100110001'  == '11'
        // --
        // 50 == '2'
        // 12850 == '22'
        // --
        // 51 == '3'
        // 13107 == '33'
        this.stmt.executeUpdate("INSERT INTO testBug78685 VALUES (b'00101110', b'0010111000101110', b'0010111000101110'), ('/', '//', '//'), "
                + "(48, 12336, 12336), (b'00110001', b'0011000100110001', b'0011000100110001'), ('2', '22', '22'), (51, 13107, 13107)");

        boolean useFastIntParsing = false;
        boolean useServerPrepStmts = false;
        do {
            // Test result set from plain statements.
            String testCase = String.format("Case [fstIntPrs: %s, useSPS: %s, StmtType: %s]", useFastIntParsing ? "Y" : "N", useServerPrepStmts ? "Y" : "N",
                    "Plain");

            final Properties props = new Properties();
            props.setProperty("useFastIntParsing", Boolean.toString(useFastIntParsing));
            Connection testConn = getConnectionWithProps(props);
            this.rs = testConn.createStatement().executeQuery("SELECT b1, b1 + 0, BIN(b1), b2, b2 + 0, BIN(b2), b3, b3 + 0, BIN(b3) FROM testBug78685");
            testBug78685CheckData(testCase);
            testConn.close();

            // Test result set from prepared statements
            testCase = String.format("Case [fstIntPrs: %s, useSPS: %s, StmtType: %s]", useFastIntParsing ? "Y" : "N", useServerPrepStmts ? "Y" : "N",
                    "PrepStmt");

            props.setProperty("useServerPrepStmts", Boolean.toString(useServerPrepStmts));
            testConn = getConnectionWithProps(props);
            this.pstmt = testConn.prepareStatement("SELECT b1, b1 + 0, BIN(b1), b2, b2 + 0, BIN(b2), b3, b3 + 0, BIN(b3) FROM testBug78685");
            this.rs = this.pstmt.executeQuery();
            testBug78685CheckData("");
            testConn.close();
        } while ((useFastIntParsing = !useFastIntParsing) || (useServerPrepStmts = !useServerPrepStmts));
    }

    private void testBug78685CheckData(String testCase) throws Exception {
        int rowCount = 0;
        while (this.rs.next()) {
            int expectedNumBase = 46 + rowCount;

            // Column "b1 BIT(8)"
            int expectedNum = expectedNumBase;

            assertEquals(testCase, expectedNum, this.rs.getShort(1));
            assertEquals(testCase, expectedNum, this.rs.getInt(1));
            assertEquals(testCase, expectedNum, this.rs.getLong(1));
            assertEquals(testCase, expectedNum, this.rs.getBigDecimal(1).intValue());
            assertEquals(testCase, String.valueOf(expectedNum), this.rs.getString(1));
            assertTrue(this.rs.getObject(1) instanceof byte[]);
            assertByteArrayEquals(testCase, new byte[] { (byte) (expectedNumBase) }, (byte[]) this.rs.getObject(1));

            assertEquals(testCase, expectedNum, this.rs.getShort(2));
            assertEquals(testCase, expectedNum, this.rs.getInt(2));
            assertEquals(testCase, expectedNum, this.rs.getLong(2));
            assertEquals(testCase, expectedNum, this.rs.getBigDecimal(2).intValue());
            assertEquals(testCase, String.valueOf(expectedNum), this.rs.getString(2));
            assertEquals(testCase, BigInteger.valueOf(expectedNum), this.rs.getObject(2));

            final ResultSet testRs1 = this.rs;
            assertThrows(SQLException.class, "'[01\\.]+' in column '3' is outside valid range for the datatype SMALLINT\\.", new Callable<Void>() {
                public Void call() throws Exception {
                    testRs1.getShort(3);
                    return null;
                }
            });
            String expectedString = Integer.toBinaryString(expectedNum);
            assertEquals(testCase, Integer.parseInt(expectedString), this.rs.getInt(3));
            assertEquals(testCase, Long.parseLong(expectedString), this.rs.getLong(3));
            assertEquals(testCase, expectedString, this.rs.getString(3));
            assertEquals(testCase, expectedString, this.rs.getObject(3));

            // Column "b1 BIT(16)"
            expectedNum = expectedNumBase + expectedNumBase * 256;

            assertEquals(testCase, expectedNum, this.rs.getShort(4));
            assertEquals(testCase, expectedNum, this.rs.getInt(4));
            assertEquals(testCase, expectedNum, this.rs.getLong(4));
            assertEquals(testCase, expectedNum, this.rs.getBigDecimal(4).intValue());
            assertEquals(testCase, String.valueOf(expectedNum), this.rs.getString(4));
            assertTrue(this.rs.getObject(4) instanceof byte[]);
            assertByteArrayEquals(testCase, new byte[] { (byte) (expectedNumBase), (byte) (expectedNumBase) }, (byte[]) this.rs.getObject(4));

            assertEquals(testCase, expectedNum, this.rs.getShort(5));
            assertEquals(testCase, expectedNum, this.rs.getInt(5));
            assertEquals(testCase, expectedNum, this.rs.getLong(5));
            assertEquals(testCase, expectedNum, this.rs.getBigDecimal(5).intValue());
            assertEquals(testCase, String.valueOf(expectedNum), this.rs.getString(5));
            assertEquals(testCase, BigInteger.valueOf(expectedNum), this.rs.getObject(5));

            final ResultSet testRs2 = this.rs;
            assertThrows(SQLException.class, "'[E\\d\\.]+' in column '6' is outside valid range for the datatype SMALLINT\\.", new Callable<Void>() {
                public Void call() throws Exception {
                    testRs2.getShort(6);
                    return null;
                }
            });
            assertThrows(SQLException.class, "'[E\\d\\.]+' in column '6' is outside valid range for the datatype INTEGER\\.", new Callable<Void>() {
                public Void call() throws Exception {
                    testRs2.getInt(6);
                    return null;
                }
            });
            expectedString = Long.toBinaryString(expectedNum);
            assertEquals(testCase, Long.parseLong(expectedString), this.rs.getLong(6));
            assertEquals(testCase, expectedString, this.rs.getString(6));
            assertEquals(testCase, expectedString, this.rs.getObject(6));

            // Column "b1 BIT(24)"
            expectedNum = expectedNumBase + expectedNumBase * 256;

            assertEquals(testCase, expectedNum, this.rs.getShort(7));
            assertEquals(testCase, expectedNum, this.rs.getInt(7));
            assertEquals(testCase, expectedNum, this.rs.getLong(7));
            assertEquals(testCase, expectedNum, this.rs.getBigDecimal(7).intValue());
            assertEquals(testCase, String.valueOf(expectedNum), this.rs.getString(7));
            assertTrue(this.rs.getObject(7) instanceof byte[]);
            assertByteArrayEquals(testCase, new byte[] { 0, (byte) (expectedNumBase), (byte) (expectedNumBase) }, (byte[]) this.rs.getObject(7));

            assertEquals(testCase, expectedNum, this.rs.getShort(8));
            assertEquals(testCase, expectedNum, this.rs.getInt(8));
            assertEquals(testCase, expectedNum, this.rs.getLong(8));
            assertEquals(testCase, expectedNum, this.rs.getBigDecimal(8).intValue());
            assertEquals(testCase, String.valueOf(expectedNum), this.rs.getString(8));
            assertEquals(testCase, BigInteger.valueOf(expectedNum), this.rs.getObject(8));

            final ResultSet testRs3 = this.rs;
            assertThrows(SQLException.class, "'[E\\d\\.]+' in column '9' is outside valid range for the datatype SMALLINT\\.", new Callable<Void>() {
                public Void call() throws Exception {
                    testRs3.getShort(9);
                    return null;
                }
            });
            assertThrows(SQLException.class, "'[E\\d\\.]+' in column '9' is outside valid range for the datatype INTEGER\\.", new Callable<Void>() {
                public Void call() throws Exception {
                    testRs3.getInt(9);
                    return null;
                }
            });
            expectedString = Long.toBinaryString(expectedNum);
            assertEquals(testCase, Long.parseLong(expectedString), this.rs.getLong(9));
            assertEquals(testCase, expectedString, this.rs.getString(9));
            assertEquals(testCase, expectedString, this.rs.getObject(9));

            rowCount++;
        }
        assertEquals(testCase, 6, rowCount);
    }

    /**
     * Tests fix for Bug#80631 - ResultSet.getString return garbled result with json type data.
     */
    public void testBug80631() throws Exception {
        if (!versionMeetsMinimum(5, 7, 9)) {
            return;
        }

        /*
         * \u4E2D\u56FD (Simplified Chinese): "China"
         * \u65E5\u672C (Japanese): "Japan"
         * \uD83D\uDC2C (Emoji): "Dolphin"
         * \u263A (Symbols): "White Smiling Face"
         */
        String[] data = new String[] { "\u4E2D\u56FD", "\u65E5\u672C", "\uD83D\uDC2C", "\u263A" };
        String jsonTmpl = "{\"data\": \"%s\"}";

        createTable("testBug80631", "(data JSON)");
        createProcedure("testBug80631Insert", "(IN data JSON) BEGIN INSERT INTO testBug80631 VALUES (data); END;");
        createProcedure("testBug80631SELECT", "() BEGIN SELECT * FROM testBug80631; END;");

        boolean useSPS = false;
        do {
            final String testCase = String.format("Case: [SPS: %s]", useSPS ? "Y" : "N");
            final Connection testConn = getConnectionWithProps("characterEncoding=UTF-8,useUnicode=true,useServerPrepStmts=" + useSPS);

            // Insert and select using a Statement.
            Statement testStmt = testConn.createStatement();
            for (String d : data) {
                assertEquals(testCase, 1, testStmt.executeUpdate("INSERT INTO testBug80631 VALUES ('" + String.format(jsonTmpl, d) + "')"));
            }
            this.rs = testStmt.executeQuery("SELECT * FROM testBug80631");
            for (int i = 0; i < data.length; i++) {
                assertTrue(testCase, this.rs.next());
                assertEquals(testCase, String.format(jsonTmpl, data[i]), this.rs.getString(1));
            }
            testStmt.close();

            testConn.createStatement().execute("TRUNCATE TABLE testBug80631");

            // Insert and select using a PreparedStatement.
            PreparedStatement testPstmt = testConn.prepareStatement("INSERT INTO testBug80631 VALUES (?)");
            for (String d : data) {
                testPstmt.setString(1, String.format(jsonTmpl, d));
                assertEquals(testCase, 1, testPstmt.executeUpdate());
            }
            testPstmt.close();
            testPstmt = testConn.prepareStatement("SELECT * FROM testBug80631");
            this.rs = testPstmt.executeQuery();
            for (int i = 0; i < data.length; i++) {
                assertTrue(testCase, this.rs.next());
                assertEquals(testCase, String.format(jsonTmpl, data[i]), this.rs.getString(1));
            }
            testPstmt.close();

            testConn.createStatement().execute("TRUNCATE TABLE testBug80631");

            // Insert and select using a CallableStatement.
            CallableStatement testCstmt = testConn.prepareCall("{CALL testBug80631Insert(?)}");
            for (String d : data) {
                testCstmt.setString(1, String.format(jsonTmpl, d));
                assertEquals(testCase, 1, testCstmt.executeUpdate());
            }
            testCstmt.close();
            testCstmt = testConn.prepareCall("{CALL testBug80631Select()}");
            testCstmt.execute();
            this.rs = testCstmt.getResultSet();
            for (int i = 0; i < data.length; i++) {
                assertTrue(testCase, this.rs.next());
                assertEquals(testCase, String.format(jsonTmpl, data[i]), this.rs.getString(1));
            }
            testCstmt.close();

            testConn.close();
        } while (useSPS = !useSPS);
    }

    /**
     * Tests fix for Bug#23197238 - EXECUTEQUERY() FAILS FOR JSON DATA WHEN RESULTSETCONCURRENCY=CONCUR_UPDATABLE.
     */
    public void testBug23197238() throws Exception {
        if (!versionMeetsMinimum(5, 7, 9)) {
            return;
        }

        createTable("testBug23197238", "(id INT AUTO_INCREMENT PRIMARY KEY, doc JSON DEFAULT NULL)");

        String[] docs = new String[] { "{\"key1\": \"value1\"}", "{\"key2\": \"value2\"}", "{\"key3\": \"value3\"}" };
        Connection testConn = getConnectionWithProps("useCursorFetch=true");

        Statement testStmt = testConn.createStatement();
        testStmt.execute("INSERT INTO testBug23197238 (doc) VALUES ('" + docs[2] + "')");
        testStmt.close();

        testBug23197238Assert(new String[] { docs[2] });

        testStmt = testConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        this.rs = testStmt.executeQuery("SELECT * FROM testBug23197238");
        assertTrue(this.rs.next());
        this.rs.updateObject(2, docs[1]);
        this.rs.updateRow();
        this.rs.moveToInsertRow();
        this.rs.updateObject(2, docs[1]);
        this.rs.insertRow();
        testStmt.close();

        testBug23197238Assert(new String[] { docs[1], docs[1] });

        PreparedStatement testPstmt = testConn.prepareStatement("SELECT * FROM testBug23197238 WHERE id = ?", ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_UPDATABLE);
        testPstmt.setObject(1, 1, Types.INTEGER);
        this.rs = testPstmt.executeQuery();
        assertTrue(this.rs.next());
        this.rs.updateObject(2, docs[0]);
        this.rs.updateRow();
        this.rs.moveToInsertRow();
        this.rs.updateObject(2, docs[2]);
        this.rs.insertRow();
        testPstmt.close();

        testBug23197238Assert(docs);

        testConn.close();
    }

    private void testBug23197238Assert(String[] expected) throws Exception {
        this.rs = this.stmt.executeQuery("SELECT * FROM testBug23197238");
        for (String e : expected) {
            assertTrue(this.rs.next());
            assertEquals(e, this.rs.getString(2));
        }
        assertFalse(this.rs.next());
    }

    /**
     * Tests fix for Bug#83368 - 5.1.40 regression: wasNull not updated when calling getInt for a bit column.
     */
    public void testBug83368() throws Exception {
        createTable("testBug83368", "(c1 VARCHAR(1), c2 BIT)");
        this.stmt.execute("INSERT INTO testBug83368 VALUES (NULL, 1)");
        this.rs = this.stmt.executeQuery("SELECT * FROM testBug83368");

        assertTrue(this.rs.next());

        assertNull(this.rs.getString(1));
        assertTrue(this.rs.wasNull());
        assertEquals((byte) 1, this.rs.getByte(2));
        assertFalse(this.rs.wasNull());

        assertNull(this.rs.getString(1));
        assertTrue(this.rs.wasNull());
        assertEquals((short) 1, this.rs.getShort(2));
        assertFalse(this.rs.wasNull());

        assertNull(this.rs.getString(1));
        assertTrue(this.rs.wasNull());
        assertEquals(1, this.rs.getInt(2));
        assertFalse(this.rs.wasNull());

        assertNull(this.rs.getString(1));
        assertTrue(this.rs.wasNull());
        assertEquals(1L, this.rs.getLong(2));
        assertFalse(this.rs.wasNull());

        assertNull(this.rs.getString(1));
        assertTrue(this.rs.wasNull());
        assertEquals(BigDecimal.valueOf(1), this.rs.getBigDecimal(2));
        assertFalse(this.rs.wasNull());
    }

    /**
     * Tests fix for Bug#83662 - NullPointerException while reading NULL boolean value from DB.
     * 
     * This fix was actually done in the patch for Bug#83368, as both are fixed in the same way.
     */
    public void testBug83662() throws Exception {
        createTable("testBug83662", "(b BIT(1) NULL)");
        this.stmt.executeUpdate("INSERT INTO testBug83662 VALUES (null)");

        this.rs = this.stmt.executeQuery("SELECT * FROM testBug83662");
        assertTrue(this.rs.next());
        assertEquals((byte) 0, this.rs.getByte(1));
        assertEquals((short) 0, this.rs.getShort(1));
        assertEquals(0, this.rs.getInt(1));
        assertEquals(0L, this.rs.getLong(1));
        assertEquals(0, this.rs.getInt(1));
        assertNull(this.rs.getBigDecimal(1));
    }

    /**
     * Tests fix for Bug#70704 - Deadlock using UpdatableResultSet.
     * 
     * Doesn't actually test the buggy behavior since it is not verifiable since the fix for Bug#59462 (revision 385a151). However, the patch for this fix is
     * needed because the synchronization in UpdatableResultSet was dated.
     * This test makes sure there is no regression.
     * 
     * WARNING! If this test fails there is no guarantee that the JVM will remain stable and won't affect any other tests. It is imperative that this test
     * passes to ensure other tests results.
     */
    public void testBug70704() throws Exception {
        for (int i = 0; i < 100; i++) {
            final Statement testStmt = this.conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            final ResultSet testRs = testStmt.executeQuery("SELECT 1");

            ExecutorService executorService = Executors.newFixedThreadPool(2);

            executorService.submit(new Callable<Void>() {
                public Void call() throws Exception {
                    testStmt.close();
                    return null;
                }
            });

            executorService.submit(new Callable<Void>() {
                public Void call() throws Exception {
                    testRs.close();
                    return null;
                }
            });

            executorService.shutdown();
            if (!executorService.awaitTermination(2, TimeUnit.SECONDS)) {
                ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
                long[] threadIds = threadMXBean.findMonitorDeadlockedThreads();
                if (threadIds != null) {
                    System.err.println("Deadlock detected!");
                    ThreadInfo[] threadInfos = threadMXBean.getThreadInfo(threadIds, Integer.MAX_VALUE);
                    for (ThreadInfo ti : threadInfos) {
                        System.err.println();
                        System.err.println(ti);
                        System.err.println("Stack trace:");
                        for (StackTraceElement ste : ti.getStackTrace()) {
                            System.err.println("   " + ste);
                        }
                    }
                    fail("Unexpected deadlock detected. Consult system output for more details. WARNING: this failure may lead to JVM instability.");
                }
            }
        }
    }

    /**
     * Tests for fix to BUG#25650305 - GETDATE(),GETTIME() AND GETTIMESTAMP() CALL WITH NULL CALENDAR RETURNS NPE
     *
     * @throws Exception
     *             if the test fails
     */
    public void testBug25650305() throws Exception {
        if (!versionMeetsMinimum(5, 6, 4)) {
            return; // fractional seconds are not supported in previous versions
        }

        createTable("testBug25650305", "(c1 timestamp(5))");
        this.stmt.executeUpdate("INSERT INTO testBug25650305 VALUES ('2031-01-15 03:14:07.339999')");

        Calendar cal = Calendar.getInstance();

        Connection testConn;
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        props.setProperty("useFastDateParsing", "true");

        for (int i = 0; i < 2; i++) {
            System.out.println("With useFastDateParsing=" + props.getProperty("useFastDateParsing"));
            testConn = getConnectionWithProps(props);
            this.rs = testConn.createStatement().executeQuery("SELECT * FROM testBug25650305");
            this.rs.next();

            assertEquals("2031-01-15", this.rs.getDate(1).toString());
            assertEquals("2031-01-15", this.rs.getDate(1, cal).toString());
            assertEquals("2031-01-15", this.rs.getDate(1, null).toString());

            assertEquals("03:14:07", this.rs.getTime(1).toString());
            assertEquals("03:14:07", this.rs.getTime(1, cal).toString());
            assertEquals("03:14:07", this.rs.getTime(1, null).toString());

            assertEquals("2031-01-15 03:14:07.34", this.rs.getTimestamp(1).toString());
            assertEquals("2031-01-15 03:14:07.34", this.rs.getTimestamp(1, cal).toString());
            assertEquals("2031-01-15 03:14:07.34", this.rs.getTimestamp(1, null).toString());

            testConn.close();
            props.setProperty("useFastDateParsing", "false");
        }
    }

    /**
     * Tests fix for Bug#22305979, WRONG RECORD UPDATED IF SENDFRACTIONALSECONDS=FALSE AND SMT IS SCROLLABLE.
     */
    public void testBug22305979() throws Exception {
        if (!versionMeetsMinimum(5, 6, 4)) {
            return; // fractional seconds are not supported in previous versions
        }

        /* Test from bug report */
        Connection testConn2;
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");
        props.setProperty("sendFractionalSeconds", "false");

        for (String serverTimezone : new String[] { null, "GMT", "Asia/Calcutta" }) {
            if (serverTimezone != null) {
                props.setProperty("serverTimezone", serverTimezone);
            }

            testConn2 = getConnectionWithProps(props);

            Timestamp ts2 = new Timestamp(TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd HH:mm:ss.SSS", null, null).parse("2019-12-30 13:59:57.789").getTime());
            createTable("testBug22305979_orig_1",
                    "(id int, tmp int,ts1 timestamp(6),ts2 timestamp(3) NOT NULL DEFAULT '2001-01-01 00:00:01',primary key(id,ts1) )");
            this.stmt.execute("insert into testBug22305979_orig_1 values (1,100,'2014-12-31 23:59:59.123','2015-12-31 23:59:59.456')");
            this.stmt.execute("insert into testBug22305979_orig_1 values (1,200,'2014-12-31 23:59:59','2022-12-31 23:59:59.456')");

            Statement scrollableStmt = testConn2.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
            ResultSet rs1 = scrollableStmt.executeQuery("SELECT * FROM testBug22305979_orig_1 where id=1 and ts1='2014-12-31 23:59:59.123'");
            if (rs1.next()) {
                rs1.updateTimestamp(3, ts2); //Updating part of primary key
                rs1.updateRow();
            }

            this.rs = scrollableStmt.executeQuery("SELECT * FROM testBug22305979_orig_1 order by tmp");
            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(1));
            assertEquals(100, this.rs.getInt(2));
            assertEquals("2019-12-30 13:59:57.0", this.rs.getString(3));
            assertEquals("2015-12-31 23:59:59.456", this.rs.getString(4));
            assertTrue(this.rs.next());
            assertEquals(1, this.rs.getInt(1));
            assertEquals(200, this.rs.getInt(2));
            assertEquals("2014-12-31 23:59:59.0", this.rs.getString(3));
            assertEquals("2022-12-31 23:59:59.456", this.rs.getString(4));

            testConn2.close();
        }

        /* Unified test */

        // Original values we insert
        Timestamp[] ts_ins = new Timestamp[] { //
                Timestamp.valueOf("2018-07-09 13:14:15"), //
                Timestamp.valueOf("2018-07-09 13:14:15.1"), //
                Timestamp.valueOf("2018-07-09 13:14:15.12"), //
                Timestamp.valueOf("2018-07-09 13:14:15.123"), //
                Timestamp.valueOf("2018-07-09 13:14:15.1234"), //
                Timestamp.valueOf("2018-07-09 13:14:15.12345"), //
                Timestamp.valueOf("2018-07-09 13:14:15.123456"), //
                Timestamp.valueOf("2018-07-09 13:14:15.1234567"), //
                Timestamp.valueOf("2018-07-09 13:14:15.12345678"), //
                Timestamp.valueOf("2018-07-09 13:14:15.999999999") };

        // Values we expect in DB after insert operation if TIME_TRUNCATE_FRACTIONAL sql_mode is unset
        Timestamp[] ts_ins_expected_round = new Timestamp[] { //
                Timestamp.valueOf("2018-07-09 13:14:15"), //
                Timestamp.valueOf("2018-07-09 13:14:15.1"), //
                Timestamp.valueOf("2018-07-09 13:14:15.12"), //
                Timestamp.valueOf("2018-07-09 13:14:15.123"), //
                Timestamp.valueOf("2018-07-09 13:14:15.1234"), //
                Timestamp.valueOf("2018-07-09 13:14:15.12345"), //
                Timestamp.valueOf("2018-07-09 13:14:15.123456"), //
                Timestamp.valueOf("2018-07-09 13:14:15.123457"), //
                Timestamp.valueOf("2018-07-09 13:14:15.123457"), //
                Timestamp.valueOf("2018-07-09 13:14:16.0") };

        // Values we expect in DB after insert operation if TIME_TRUNCATE_FRACTIONAL sql_mode is set
        Timestamp[] ts_ins_expected_truncate = new Timestamp[] { //
                Timestamp.valueOf("2018-07-09 13:14:15"), //
                Timestamp.valueOf("2018-07-09 13:14:15.1"), //
                Timestamp.valueOf("2018-07-09 13:14:15.12"), //
                Timestamp.valueOf("2018-07-09 13:14:15.123"), //
                Timestamp.valueOf("2018-07-09 13:14:15.1234"), //
                Timestamp.valueOf("2018-07-09 13:14:15.12345"), //
                Timestamp.valueOf("2018-07-09 13:14:15.123456"), //
                Timestamp.valueOf("2018-07-09 13:14:15.123456"), //
                Timestamp.valueOf("2018-07-09 13:14:15.123456"), //
                Timestamp.valueOf("2018-07-09 13:14:15.999999") };

        // Values we expect in DB after insert operation if sendFractionalSeconds=false
        Timestamp[] ts_ins_expected_not_sendFractionalSeconds = new Timestamp[] { //
                Timestamp.valueOf("2018-07-09 13:14:15"), //
                Timestamp.valueOf("2018-07-09 13:14:15.0"), //
                Timestamp.valueOf("2018-07-09 13:14:15.0"), //
                Timestamp.valueOf("2018-07-09 13:14:15.0"), //
                Timestamp.valueOf("2018-07-09 13:14:15.0"), //
                Timestamp.valueOf("2018-07-09 13:14:15.0"), //
                Timestamp.valueOf("2018-07-09 13:14:15.0"), //
                Timestamp.valueOf("2018-07-09 13:14:15.0"), //
                Timestamp.valueOf("2018-07-09 13:14:15.0"), //
                Timestamp.valueOf("2018-07-09 13:14:15.0") };

        // Original values we pass to update operation
        Timestamp[] ts_upd = new Timestamp[] { //
                Timestamp.valueOf("2018-07-09 03:14:15"), //
                Timestamp.valueOf("2018-07-09 03:14:15.1"), //
                Timestamp.valueOf("2018-07-09 03:14:15.12"), //
                Timestamp.valueOf("2018-07-09 03:14:15.123"), //
                Timestamp.valueOf("2018-07-09 03:14:15.1234"), //
                Timestamp.valueOf("2018-07-09 03:14:15.12345"), //
                Timestamp.valueOf("2018-07-09 03:14:15.123456"), //
                Timestamp.valueOf("2018-07-09 03:14:15.1234567"), //
                Timestamp.valueOf("2018-07-09 03:14:15.12345678"), //
                Timestamp.valueOf("2018-07-09 03:14:15.999999999") };

        // Values we expect in DB after update operation if TIME_TRUNCATE_FRACTIONAL sql_mode is unset
        Timestamp[] ts_upd_expected_round = new Timestamp[] { //
                Timestamp.valueOf("2018-07-09 03:14:15"), //
                Timestamp.valueOf("2018-07-09 03:14:15.1"), //
                Timestamp.valueOf("2018-07-09 03:14:15.12"), //
                Timestamp.valueOf("2018-07-09 03:14:15.123"), //
                Timestamp.valueOf("2018-07-09 03:14:15.1234"), //
                Timestamp.valueOf("2018-07-09 03:14:15.12345"), //
                Timestamp.valueOf("2018-07-09 03:14:15.123456"), //
                Timestamp.valueOf("2018-07-09 03:14:15.123457"), //
                Timestamp.valueOf("2018-07-09 03:14:15.123457"), //
                Timestamp.valueOf("2018-07-09 03:14:16.0") };

        // Values we expect in DB after update operation if TIME_TRUNCATE_FRACTIONAL sql_mode is set
        Timestamp[] ts_upd_expected_truncate = new Timestamp[] { //
                Timestamp.valueOf("2018-07-09 03:14:15"), //
                Timestamp.valueOf("2018-07-09 03:14:15.1"), //
                Timestamp.valueOf("2018-07-09 03:14:15.12"), //
                Timestamp.valueOf("2018-07-09 03:14:15.123"), //
                Timestamp.valueOf("2018-07-09 03:14:15.1234"), //
                Timestamp.valueOf("2018-07-09 03:14:15.12345"), //
                Timestamp.valueOf("2018-07-09 03:14:15.123456"), //
                Timestamp.valueOf("2018-07-09 03:14:15.123457"), //
                Timestamp.valueOf("2018-07-09 03:14:15.123457"), //
                Timestamp.valueOf("2018-07-09 03:14:15.999999") };

        // Values we expect in DB after update operation if sendFractionalSeconds=false
        Timestamp[] ts_upd_expected_not_sendFractionalSeconds = new Timestamp[] { //
                Timestamp.valueOf("2018-07-09 03:14:15"), //
                Timestamp.valueOf("2018-07-09 03:14:15.0"), //
                Timestamp.valueOf("2018-07-09 03:14:15.0"), //
                Timestamp.valueOf("2018-07-09 03:14:15.0"), //
                Timestamp.valueOf("2018-07-09 03:14:15.0"), //
                Timestamp.valueOf("2018-07-09 03:14:15.0"), //
                Timestamp.valueOf("2018-07-09 03:14:15.0"), //
                Timestamp.valueOf("2018-07-09 03:14:15.0"), //
                Timestamp.valueOf("2018-07-09 03:14:15.0"), //
                Timestamp.valueOf("2018-07-09 03:14:15.0") };

        Connection testConn;

        boolean sqlModeTimeTruncateFractional = false;
        boolean sendFractionalSeconds = false;
        boolean useServerPrepStmts = false;
        boolean useLegacyDatetimeCode = false;
        boolean useJDBCCompliantTimezoneShift = false;
        boolean useGmtMillisForDatetimes = false;
        boolean useSSPSCompatibleTimezoneShift = false;
        boolean useFastDateParsing = false;

        do {
            // TIME_TRUNCATE_FRACTIONAL was added in MySQL 8.0
            if (sqlModeTimeTruncateFractional && !versionMeetsMinimum(8, 0)) {
                continue;
            }
            for (String serverTimezone : new String[] { null, "GMT", "Asia/Calcutta" }) {
                if (serverTimezone != null) {
                    props.setProperty("serverTimezone", serverTimezone);
                } else {
                    props.remove("serverTimezone");
                }

                final String testCase = String.format(
                        "Case: [TIME_TRUNCATE_FRACTIONAL=%s, sendFractionalSeconds=%s, useServerPrepStmts=%s," + " useLegacyDatetimeCode=%s,"
                                + " useJDBCCompliantTimezoneShift=%s, useGmtMillisForDatetimes=%s, useSSPSCompatibleTimezoneShift=%s,"
                                + " useFastDateParsing=%s, serverTimezone=%s]",
                        sqlModeTimeTruncateFractional ? "Y" : "N", sendFractionalSeconds ? "Y" : "N", useServerPrepStmts ? "Y" : "N",
                        useLegacyDatetimeCode ? "Y" : "N", useJDBCCompliantTimezoneShift ? "Y" : "N", useGmtMillisForDatetimes ? "Y" : "N",
                        useSSPSCompatibleTimezoneShift ? "Y" : "N", useFastDateParsing ? "Y" : "N", serverTimezone);
                System.out.println(testCase);

                String sqlMode = getMysqlVariable("sql_mode");
                sqlMode = removeSqlMode("TIME_TRUNCATE_FRACTIONAL", sqlMode);
                if (sqlModeTimeTruncateFractional) {
                    if (sqlMode.length() > 0) {
                        sqlMode += ",";
                    }
                    sqlMode += "TIME_TRUNCATE_FRACTIONAL";
                }

                props.setProperty("sessionVariables", "sql_mode='" + sqlMode + "'");
                props.setProperty("sendFractionalSeconds", "" + sendFractionalSeconds);
                props.setProperty("useServerPrepStmts", "" + useServerPrepStmts);
                props.setProperty("useLegacyDatetimeCode", "" + useLegacyDatetimeCode);
                props.setProperty("useJDBCCompliantTimezoneShift", "" + useJDBCCompliantTimezoneShift);
                props.setProperty("useGmtMillisForDatetimes", "" + useGmtMillisForDatetimes);
                props.setProperty("useSSPSCompatibleTimezoneShift", "" + useSSPSCompatibleTimezoneShift);
                props.setProperty("useFastDateParsing", "" + useFastDateParsing);

                testConn = getConnectionWithProps(props);

                // specifying different fractional length
                for (int len = 0; len < 10; len++) {

                    int fieldLen = len > 6 ? 6 : len;

                    String tableName = "testBug22305979_" + len;
                    createTable(tableName,
                            "(id INTEGER, dt DATETIME" + (fieldLen == 0 ? "" : "(" + fieldLen + ")") + ", ts TIMESTAMP"
                                    + (fieldLen == 0 ? "" : "(" + fieldLen + ")") + ", tm TIME" + (fieldLen == 0 ? "" : "(" + fieldLen + ")")
                                    + ", PRIMARY KEY(id,dt,ts,tm))");

                    Statement st = testConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    this.rs = st.executeQuery("SELECT id,dt,ts,tm FROM " + tableName + " FOR UPDATE");
                    this.rs.next(); // No rows
                    this.rs.moveToInsertRow();
                    this.rs.updateInt("id", 1);
                    this.rs.updateTimestamp("dt", ts_ins[len]);
                    this.rs.updateTimestamp("ts", ts_ins[len]);
                    this.rs.updateTime("tm", new Time(ts_ins[len].getTime()));
                    this.rs.insertRow();
                    assertTrue(testCase, this.rs.last());

                    // checking only seconds and nanos, other date parts are not relevant to this bug
                    Calendar c_exp = new GregorianCalendar();
                    c_exp.setTime(sendFractionalSeconds ? (sqlModeTimeTruncateFractional ? ts_ins_expected_truncate[len] : ts_ins_expected_round[len])
                            : ts_ins_expected_not_sendFractionalSeconds[len]);
                    Calendar c_res = new GregorianCalendar();
                    c_res.setTime(this.rs.getTimestamp("dt"));
                    assertEquals(testCase, c_exp.get(Calendar.SECOND), c_res.get(Calendar.SECOND));
                    assertEquals(testCase, c_exp.get(Calendar.MILLISECOND), c_res.get(Calendar.MILLISECOND));
                    c_res.setTime(this.rs.getTimestamp("ts"));
                    assertEquals(testCase, c_exp.get(Calendar.SECOND), c_res.get(Calendar.SECOND));
                    assertEquals(testCase, c_exp.get(Calendar.MILLISECOND), c_res.get(Calendar.MILLISECOND));

                    // TODO java.sql.Time does not provide any way for setting/getting milliseconds and removes them from toString() method.
                    // So the rs.updateTime(String columnName, java.sql.Time x) will always truncate milliseconds. Probably it is a bug because
                    // java.sql.Time contains milliseconds internally. We have a Bug#76775 feature request about that.
                    if (sendFractionalSeconds) {
                        c_exp.setTime(ts_ins_expected_truncate[len]);
                    }
                    c_res.setTime(this.rs.getTime("tm"));
                    assertEquals(testCase, c_exp.get(Calendar.SECOND), c_res.get(Calendar.SECOND));
                    assertEquals(testCase, 0, c_res.get(Calendar.MILLISECOND));

                    this.rs.updateTimestamp("dt", ts_upd[len]);
                    this.rs.updateTimestamp("ts", ts_upd[len]);
                    this.rs.updateTime("tm", new Time(ts_upd[len].getTime()));
                    this.rs.updateRow();
                    c_exp.setTime(sendFractionalSeconds ? (sqlModeTimeTruncateFractional ? ts_upd_expected_truncate[len] : ts_upd_expected_round[len])
                            : ts_upd_expected_not_sendFractionalSeconds[len]);
                    c_res.setTime(this.rs.getTimestamp("dt"));
                    assertEquals(testCase, c_exp.get(Calendar.SECOND), c_res.get(Calendar.SECOND));
                    assertEquals(testCase, c_exp.get(Calendar.MILLISECOND), c_res.get(Calendar.MILLISECOND));
                    c_res.setTime(this.rs.getTimestamp("ts"));
                    assertEquals(testCase, c_exp.get(Calendar.SECOND), c_res.get(Calendar.SECOND));
                    assertEquals(testCase, c_exp.get(Calendar.MILLISECOND), c_res.get(Calendar.MILLISECOND));

                    if (sendFractionalSeconds) {
                        c_exp.setTime(ts_upd_expected_truncate[len]);
                    }
                    c_res.setTime(this.rs.getTime("tm"));
                    assertEquals(testCase, c_exp.get(Calendar.SECOND), c_res.get(Calendar.SECOND));
                    assertEquals(testCase, 0, c_res.get(Calendar.MILLISECOND));

                    st.close();
                }

                testConn.close();
            }
        } while ((sqlModeTimeTruncateFractional = !sqlModeTimeTruncateFractional) || (sendFractionalSeconds = !sendFractionalSeconds)
                || (useServerPrepStmts = !useServerPrepStmts) || (useLegacyDatetimeCode = !useLegacyDatetimeCode)
                || (useJDBCCompliantTimezoneShift = !useJDBCCompliantTimezoneShift) || (useGmtMillisForDatetimes = !useGmtMillisForDatetimes)
                || (useSSPSCompatibleTimezoneShift = !useSSPSCompatibleTimezoneShift) || (useFastDateParsing = !useFastDateParsing));

    }

    /**
     * Tests fix for Bug#80532 (22847443), ENCODING OF RESULTSET.UPDATEROW IS BROKEN FOR NON ASCII CHARCTERS.
     */
    public void testBug80532() throws Exception {
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        for (String enc : new String[] { "ISO8859_1", "UTF-8" }) {
            for (String useSSPS : new String[] { "false", "true" }) {
                final String testCase = String.format("Case: [characterEncoding=%s, useServerPrepStmts=%s]", enc, useSSPS);
                System.out.println(testCase);

                createTable("testBug80532", "(id char(50) NOT NULL, data longtext, num int, PRIMARY KEY (id,num)) CHARACTER SET "
                        + (versionMeetsMinimum(5, 5) ? "utf8mb4" : "utf8"));

                props.setProperty("characterEncoding", enc);
                props.setProperty("useServerPrepStmts", useSSPS);

                Connection c1 = getConnectionWithProps(props);

                String id1 = "äöü";
                String id2 = "öäü";
                String data1 = "my data";
                String data2 = "new data";

                c1.createStatement().executeUpdate("INSERT INTO testBug80532(id,data,num) VALUES( '" + id1 + "', '" + data1 + "', 1 )");

                Statement st = this.conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                this.rs = st.executeQuery("select * From testBug80532"); // where id='" + id1 + "'"
                this.rs.next();

                System.out.println(this.rs.getString("id") + ", " + this.rs.getString("data"));
                assertEquals(id1, this.rs.getString("id"));
                assertEquals(data1, this.rs.getString("data"));
                this.rs.updateString("data", data2);
                this.rs.updateRow();
                System.out.println(this.rs.getString("id") + ", " + this.rs.getString("data"));
                assertEquals(id1, this.rs.getString("id"));
                assertEquals(data2, this.rs.getString("data"));

                this.rs.moveToInsertRow();
                this.rs.updateString("id", id2);
                this.rs.updateString("data", data1);
                this.rs.updateInt("num", 2);
                this.rs.insertRow();
                assertTrue(this.rs.last());
                System.out.println(this.rs.getString("id") + ", " + this.rs.getString("data"));
                assertEquals(id2, this.rs.getString("id"));
                assertEquals(data1, this.rs.getString("data"));

                this.rs.updateString("id", id1);
                this.rs.updateRow();
                System.out.println(this.rs.getString("id") + ", " + this.rs.getString("data"));
                assertEquals(id1, this.rs.getString("id"));
                assertEquals(data1, this.rs.getString("data"));
            }
        }
    }

    /**
     * Tests fix for Bug#72609 (18749544), SETDATE() NOT USING A PROLEPTIC GREGORIAN CALENDAR.
     */
    public void testBug72609() throws Exception {
        GregorianCalendar prolepticGc = new GregorianCalendar();
        prolepticGc.setGregorianChange(new Date(Long.MIN_VALUE));
        prolepticGc.clear();
        prolepticGc.set(Calendar.DAY_OF_MONTH, 8);
        prolepticGc.set(Calendar.MONTH, Calendar.OCTOBER);
        prolepticGc.set(Calendar.YEAR, 1582);

        GregorianCalendar gc = new GregorianCalendar();
        gc.clear();
        gc.setTimeInMillis(prolepticGc.getTimeInMillis());

        assertEquals(1582, gc.get(Calendar.YEAR));
        assertEquals(8, gc.get(Calendar.MONTH));
        assertEquals(28, gc.get(Calendar.DAY_OF_MONTH));

        // TIMESTAMP can't represent dates before 1970-01-01, so we need to test only DATE and DATETIME types
        createTable("testBug72609", "(d date, pd date, dt datetime, pdt datetime)");

        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        props.setProperty("allowPublicKeyRetrieval", "true");

        boolean sendFractionalSeconds = false;
        boolean useServerPrepStmts = false;
        boolean useLegacyDatetimeCode = false;
        boolean useJDBCCompliantTimezoneShift = false;
        boolean useGmtMillisForDatetimes = false;
        boolean useSSPSCompatibleTimezoneShift = false;
        boolean useFastDateParsing = false;

        do {

            final String testCase = String.format(
                    "Case: [sendFractionalSeconds=%s, useServerPrepStmts=%s," + " useLegacyDatetimeCode=%s," + " useJDBCCompliantTimezoneShift=%s,"
                            + " useGmtMillisForDatetimes=%s," + " useSSPSCompatibleTimezoneShift=%s," + " useFastDateParsing=%s]",
                    sendFractionalSeconds ? "Y" : "N", useServerPrepStmts ? "Y" : "N", useLegacyDatetimeCode ? "Y" : "N",
                    useJDBCCompliantTimezoneShift ? "Y" : "N", useGmtMillisForDatetimes ? "Y" : "N", useSSPSCompatibleTimezoneShift ? "Y" : "N",
                    useFastDateParsing ? "Y" : "N"

            );
            System.out.println(testCase);

            props.setProperty("sendFractionalSeconds", "" + sendFractionalSeconds);
            props.setProperty("useServerPrepStmts", "" + useServerPrepStmts);
            props.setProperty("useLegacyDatetimeCode", "" + useLegacyDatetimeCode);
            props.setProperty("useJDBCCompliantTimezoneShift", "" + useJDBCCompliantTimezoneShift);
            props.setProperty("useGmtMillisForDatetimes", "" + useGmtMillisForDatetimes);
            props.setProperty("useSSPSCompatibleTimezoneShift", "" + useSSPSCompatibleTimezoneShift);
            props.setProperty("useFastDateParsing", "" + useFastDateParsing);

            Connection c1 = getConnectionWithProps(props);
            Statement st1 = c1.createStatement();

            st1.execute("truncate table testBug72609");

            java.sql.Date d1 = new java.sql.Date(prolepticGc.getTime().getTime());
            Timestamp ts1 = new Timestamp(prolepticGc.getTime().getTime());

            this.pstmt = c1.prepareStatement("insert into testBug72609 values(?,?,?,?)");
            this.pstmt.setDate(1, d1);
            this.pstmt.setDate(2, d1, prolepticGc);
            this.pstmt.setTimestamp(3, ts1);
            this.pstmt.setTimestamp(4, ts1, prolepticGc);
            this.pstmt.execute();

            /*
             * Checking stored values by retrieving them as strings to avoid conversions on c/J side.
             */
            this.rs = st1.executeQuery("select DATE_FORMAT(d, '%Y-%m-%d') as d, DATE_FORMAT(pd, '%Y-%m-%d') as pd,"
                    + " DATE_FORMAT(dt, '%Y-%m-%d %H:%i:%s.%f') as dt, DATE_FORMAT(pdt, '%Y-%m-%d %H:%i:%s.%f') as pdt from testBug72609");
            this.rs.next();
            System.out.println(this.rs.getString(1) + ", " + this.rs.getString(2) + ", " + this.rs.getString(3) + ", " + this.rs.getString(4));

            assertEquals(testCase, "1582-09-28", this.rs.getString(1)); // according to Julian calendar
            assertEquals(testCase, "1582-10-08", this.rs.getString(2)); // according to proleptic Gregorian calendar

            // the exact day depends on adjustments between time zones, but that's not interesting for this test
            assertTrue(testCase, this.rs.getString(3).startsWith("1582-09-2"));
            assertTrue(testCase, this.rs.getString(4).startsWith("1582-10-0"));

            /*
             * Getting stored values back.
             * 
             * Default Julian to Gregorian calendar switch is: October 4, 1582 (Julian) is followed by October 15, 1582 (Gregorian).
             * So when default non-proleptic calendar is used the given 1582-10-08 date is in a "missing" period. In this case GregorianCalendar
             * uses a Julian calendar system for counting date in milliseconds, thus adding another 10 days to the date and returning 1582-10-18.
             * 
             * With explicit proleptic calendar we get the symmetric back conversion.
             */
            this.rs = this.stmt.executeQuery("select * from testBug72609");
            this.rs.next();

            assertEquals(testCase, "1582-09-28", this.rs.getDate(1).toString());

            assertEquals(testCase, "1582-10-18", this.rs.getDate(2).toString()); // according to Julian calendar
            assertEquals(testCase, "1582-09-28", this.rs.getDate(2, prolepticGc).toString()); // according to proleptic Gregorian calendar

            assertTrue(this.rs.getTimestamp(3).toString().startsWith("1582-09-2"));

            // the exact day depends on adjustments between time zones, but that's not interesting for this test
            assertTrue(testCase, this.rs.getTimestamp(4).toString().startsWith("1582-10-1")); // according to Julian calendar
            assertTrue(this.rs.getTimestamp(4, prolepticGc).toString().startsWith("1582-09-2")); // according to proleptic Gregorian calendar

            c1.close();

        } while ((sendFractionalSeconds = !sendFractionalSeconds) || (useServerPrepStmts = !useServerPrepStmts)
                || (useLegacyDatetimeCode = !useLegacyDatetimeCode) || (useJDBCCompliantTimezoneShift = !useJDBCCompliantTimezoneShift)
                || (useGmtMillisForDatetimes = !useGmtMillisForDatetimes) || (useSSPSCompatibleTimezoneShift = !useSSPSCompatibleTimezoneShift)
                || (useFastDateParsing = !useFastDateParsing));
    }

    /**
     * Tests fix for BUG#94585 (29452669), GETTABLENAME() RETURNS NULL FOR A QUERY HAVING COUNT(*) WITH JDBC DRIVER V8.0.12
     *
     * @throws Exception
     *             if the test fails.
     */
    public void testBug94585() throws Exception {
        createTable("testBug94585", "(column_1 INT NOT NULL)");
        Properties props = new Properties();
        for (boolean useSSPS : new boolean[] { false, true }) {
            props.setProperty("useServerPrepStmts", "" + useSSPS);
            Connection con = getConnectionWithProps(props);
            this.pstmt = con.prepareStatement("select count(*) from testBug94585");
            ResultSetMetaData rsMeta = this.pstmt.getMetaData();
            if (rsMeta != null) {
                for (int i = 1; i <= rsMeta.getColumnCount(); i++) {
                    assertNotNull(rsMeta.getTableName(i));
                }
            }
        }
    }

    /**
     * Tests fix for Bug#80441 (22850444), SYNTAX ERROR ON RESULTSET.UPDATEROW() WITH SQL_MODE NO_BACKSLASH_ESCAPES.
     *
     * @throws Exception
     *             if the test fails.
     */
    public void testBug80441() throws Exception {
        createTable("testBug80441", "( id varchar(50) NOT NULL, data longtext, start DATETIME, PRIMARY KEY (id) )");
        Properties props = new Properties();
        Connection con = null;
        for (String sessVars : new String[] { null, "sql_mode='NO_BACKSLASH_ESCAPES'" }) {
            for (boolean useSSPS : new boolean[] { false, true }) {
                props.setProperty("useServerPrepStmts", "" + useSSPS);
                if (sessVars != null) {
                    props.setProperty("sessionVariables", sessVars);
                }
                String errMsg = "Using sessionVariables=" + sessVars + ", useSSPS=" + useSSPS + ":";
                try {
                    con = getConnectionWithProps(dbUrl, props);
                    Statement st = con.createStatement();
                    try {
                        st.execute("INSERT INTO testBug80441(id,data,start) VALUES( 'key''s', 'my data', {ts '2005-01-05 13:59:20'})");
                        this.pstmt = con.prepareStatement("SELECT * FROM testBug80441", ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                        this.rs = this.pstmt.executeQuery();
                        assertTrue(errMsg, this.rs.next());
                        String text = "any\\other\ntext's\r\032\u0000\"";
                        this.rs.updateString("data", text);
                        this.rs.updateRow();
                        assertEquals(errMsg, text, this.rs.getString("data"));
                        this.rs.close();
                        this.rs = this.pstmt.executeQuery();
                        assertTrue(errMsg, this.rs.next());
                        assertEquals(errMsg, text, this.rs.getString("data"));
                    } finally {
                        st.execute("TRUNCATE TABLE testBug80441");
                    }
                } finally {
                    if (con != null) {
                        con.close();
                    }
                }
            }
        }
    }

    /**
     * Tests fix for Bug#20913289, PSTMT.EXECUTEUPDATE() FAILS WHEN SQL MODE IS NO_BACKSLASH_ESCAPES.
     *
     * @throws Exception
     *             if the test fails.
     */
    public void testBug20913289() throws Exception {
        createTable("testBug20913289", "(c1 int,c2 blob)");
        Properties props = new Properties();
        Connection con = null;
        for (String sessVars : new String[] { null, "sql_mode='NO_BACKSLASH_ESCAPES'" }) {
            for (boolean useSSPS : new boolean[] { false, true }) {
                props.setProperty("useServerPrepStmts", "" + useSSPS);
                if (sessVars != null) {
                    props.setProperty("sessionVariables", sessVars);
                }
                String errMsg = "Using sessionVariables=" + sessVars + ", useSSPS=" + useSSPS + ":";

                PreparedStatement ps = null;
                String text = " ' This is first record ' ";
                String escapedText = " '' This is first record '' ";

                try {
                    con = getConnectionWithProps(dbUrl, props);
                    Statement st = con.createStatement();
                    try {
                        st.execute("insert into testBug20913289 values(100,'" + escapedText + "')");

                        this.rs = st.executeQuery("select * from testBug20913289");
                        this.rs.next();
                        Blob bval1 = this.rs.getBlob(2);
                        assertEquals(errMsg, "100", this.rs.getString(1));
                        assertEquals(errMsg, text, this.rs.getString(2));
                        this.rs.close();

                        ps = con.prepareStatement("update testBug20913289 set c1=c1+?,c2=? ");
                        ps.setObject(1, "100", java.sql.Types.INTEGER);
                        ps.setBlob(2, bval1);
                        ps.executeUpdate();

                        this.rs = st.executeQuery("select * from testBug20913289");
                        this.rs.next();
                        assertEquals(errMsg, "200", this.rs.getString(1));
                        assertEquals(errMsg, text, this.rs.getString(2));
                        this.rs.close();
                        ps.close();
                    } catch (Exception ex) {
                        System.out.println(errMsg);
                        throw ex;
                    }
                } finally {
                    this.stmt.execute("TRUNCATE TABLE testBug20913289");
                }

            }
        }
    }
}
