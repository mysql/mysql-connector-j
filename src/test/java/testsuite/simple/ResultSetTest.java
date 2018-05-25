/*
 * Copyright (c) 2005, 2018, Oracle and/or its affiliates. All rights reserved.
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

package testsuite.simple;

import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.exceptions.NotUpdatable;

import testsuite.BaseTestCase;

public class ResultSetTest extends BaseTestCase {

    public ResultSetTest(String name) {
        super(name);
    }

    /**
     * Runs all test cases in this test suite
     * 
     * @param args
     */
    public static void main(String[] args) {
        junit.textui.TestRunner.run(ResultSetTest.class);
    }

    public void testPadding() throws Exception {
        Connection paddedConn = null;

        int numChars = 32;

        // build map of charsets supported by server
        Connection c = getConnectionWithProps("detectCustomCollations=true");
        Map<String, Integer> charsetsMap = new HashMap<>();
        this.rs = this.stmt.executeQuery("SHOW COLLATION");
        while (this.rs.next()) {
            int index = ((Number) this.rs.getObject(3)).intValue();
            String charsetName = null;
            if (((ConnectionImpl) c).getSession().getProtocol().getServerSession().indexToCustomMysqlCharset != null) {
                charsetName = ((ConnectionImpl) c).getSession().getProtocol().getServerSession().indexToCustomMysqlCharset.get(index);
            }
            if (charsetName == null) {
                charsetName = CharsetMapping.getMysqlCharsetNameForCollationIndex(index);
            }
            if (charsetName != null) {
                charsetsMap.put(charsetName, index);
            }
        }
        c.close();

        Iterator<String> charsetNames = charsetsMap.keySet().iterator();
        StringBuilder columns = new StringBuilder();
        StringBuilder emptyBuf = new StringBuilder();
        StringBuilder abcBuf = new StringBuilder();
        StringBuilder repeatBuf = new StringBuilder();
        StringBuilder selectBuf = new StringBuilder();

        int counter = 0;

        while (charsetNames.hasNext()) {
            String charsetName = charsetNames.next();
            System.out.println(charsetName);

            if (charsetName.equalsIgnoreCase("LATIN7") || charsetName.equalsIgnoreCase("BINARY")) {
                continue; // no mapping in Java
            }

            try {
                "".getBytes(charsetName);
            } catch (UnsupportedEncodingException uee) {
                continue; // not supported on this platform
            }

            if (counter != 0) {
                columns.append(",");
                emptyBuf.append(",");
                abcBuf.append(",");
                repeatBuf.append(",");
                selectBuf.append(",");
            }

            emptyBuf.append("''");
            abcBuf.append("'abc'");
            repeatBuf.append("REPEAT('b', " + numChars + ")");

            columns.append("field_");
            columns.append(charsetName);

            columns.append(" CHAR(");
            columns.append(numChars);
            columns.append(") CHARACTER SET ");
            columns.append(charsetName);

            selectBuf.append("field_");
            selectBuf.append(charsetName);

            counter++;
        }

        createTable("testPadding", "(" + columns.toString() + ", ord INT)");

        this.stmt.executeUpdate(
                "INSERT INTO testPadding VALUES (" + emptyBuf.toString() + ", 1), (" + abcBuf.toString() + ", 2), (" + repeatBuf.toString() + ", 3)");

        try {
            Properties props = new Properties();
            props.setProperty(PropertyDefinitions.PNAME_padCharsWithSpace, "true");

            paddedConn = getConnectionWithProps(props);

            testPaddingForConnection(paddedConn, numChars, selectBuf);
        } finally {
            if (paddedConn != null) {
                paddedConn.close();
            }
        }
    }

    private void testPaddingForConnection(Connection paddedConn, int numChars, StringBuilder selectBuf) throws SQLException {

        String query = "SELECT " + selectBuf.toString() + " FROM testPadding ORDER by ord";

        this.rs = paddedConn.createStatement().executeQuery(query);
        int numCols = this.rs.getMetaData().getColumnCount();

        while (this.rs.next()) {
            for (int i = 0; i < numCols; i++) {
                assertEquals(
                        "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                + ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(i + 1),
                        numChars, this.rs.getString(i + 1).length());
            }
        }

        this.rs = ((com.mysql.cj.jdbc.JdbcConnection) paddedConn).clientPrepareStatement(query).executeQuery();

        while (this.rs.next()) {
            for (int i = 0; i < numCols; i++) {
                assertEquals(
                        "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                + ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(i + 1),
                        numChars, this.rs.getString(i + 1).length());
            }
        }

        this.rs = ((com.mysql.cj.jdbc.JdbcConnection) paddedConn).serverPrepareStatement(query).executeQuery();

        while (this.rs.next()) {
            for (int i = 0; i < numCols; i++) {
                assertEquals(
                        "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                + ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(i + 1),
                        numChars, this.rs.getString(i + 1).length());
            }
        }

        this.rs = this.stmt.executeQuery(query);

        while (this.rs.next()) {
            for (int i = 0; i < numCols; i++) {
                if (this.rs.getRow() != 3) {
                    assertTrue(
                            "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                    + ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(i + 1),
                            numChars != this.rs.getString(i + 1).length());
                } else {
                    assertEquals(
                            "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                    + ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(i + 1),
                            numChars, this.rs.getString(i + 1).length());
                }
            }
        }

        this.rs = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement(query).executeQuery();

        while (this.rs.next()) {
            for (int i = 0; i < numCols; i++) {
                if (this.rs.getRow() != 3) {
                    assertTrue(
                            "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                    + ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(i + 1),
                            numChars != this.rs.getString(i + 1).length());
                } else {
                    assertEquals(
                            "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                    + ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(i + 1),
                            numChars, this.rs.getString(i + 1).length());
                }
            }
        }

        this.rs = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).serverPrepareStatement(query).executeQuery();

        while (this.rs.next()) {
            for (int i = 0; i < numCols; i++) {
                if (this.rs.getRow() != 3) {
                    assertTrue(
                            "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                    + ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(i + 1),
                            numChars != this.rs.getString(i + 1).length());
                } else {
                    assertEquals(
                            "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                    + ((com.mysql.cj.jdbc.result.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterEncoding(i + 1),
                            numChars, this.rs.getString(i + 1).length());
                }
            }
        }
    }

    public void testWarningOnTimestampTruncation() throws SQLException {
        this.rs = this.stmt.executeQuery("select cast('2006-01-01 12:13:14' as DATETIME) as ts_val");
        this.rs.next();
        assertNull(this.rs.getWarnings());

        // first warning on truncation of timestamp to date
        this.rs.getDate(1);
        assertTrue(this.rs.getWarnings().getMessage().startsWith("Precision lost converting DATETIME/TIMESTAMP to java.sql.Date"));
        assertNull(this.rs.getWarnings().getNextWarning());

        this.rs.clearWarnings();

        // first warning on truncation of timestamp to time
        this.rs.getTime(1);
        assertTrue(this.rs.getWarnings().getMessage().startsWith("Precision lost converting DATETIME/TIMESTAMP to java.sql.Time"));
        assertNull(this.rs.getWarnings().getNextWarning());

        this.rs.clearWarnings();

        // ensure that they chain properly
        this.rs.getDate(1);
        this.rs.getDate(1);
        assertNotNull(this.rs.getWarnings());
        assertNotNull(this.rs.getWarnings().getNextWarning());
        assertNull(this.rs.getWarnings().getNextWarning().getNextWarning());
    }

    /*
     * Date and time retrieval tests with and without ssps.
     */
    public void testDateTimeRetrieval() throws Exception {
        testDateTimeRetrieval_internal(this.conn);
        Connection sspsConn = getConnectionWithProps("useServerPrepStmts=true");
        testDateTimeRetrieval_internal(sspsConn);
        sspsConn.close();
    }

    private void testDateTimeRetrieval_internal(Connection c) throws Exception {
        createTable("testDateTypes", "(d DATE, t TIME, dt DATETIME)");
        this.stmt.executeUpdate("INSERT INTO testDateTypes VALUES ('2006-02-01', '-40:20:10', '2006-02-01 12:13:14')");
        this.rs = c.createStatement().executeQuery("select d, t, dt from testDateTypes");
        this.rs.next();

        // this shows that the decoder properly decodes them
        String d = this.rs.getString(1);
        String t = this.rs.getString(2);
        String ts = this.rs.getString(3);
        assertEquals("2006-02-01", d);
        assertEquals("-40:20:10", t);
        assertEquals("2006-02-01 12:13:14", ts);

        // this shows that the date/time value factories work
        Date date = this.rs.getDate(1);
        assertEquals("2006-02-01", date.toString()); // java.sql.Date.toString() is NOT locale-specific
        try {
            // -40:20:10 is an invalid value for a time object
            this.rs.getTime(2);
        } catch (SQLException ex) {
            assertEquals(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, ex.getSQLState());
        }
        Timestamp timestamp = this.rs.getTimestamp(3);
        assertEquals("2006-02-01 12:13:14.0", timestamp.toString());

        Time time = this.rs.getTime(3);
        assertEquals("12:13:14", time.toString());

        // make sure TS also throws exception on weird HOUR_OF_DAY value
        try {
            this.rs.getTimestamp(2);
        } catch (SQLException ex) {
            assertEquals(MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, ex.getSQLState());
        }
    }

    /**
     * Test for ResultSet.updateObject(), non-updatable ResultSet behaviour.
     */
    public void testNonUpdResultSetUpdateObject() throws Exception {
        this.rs = this.stmt.executeQuery("SELECT 'testResultSetUpdateObject' AS test");

        final ResultSet rsTmp = this.rs;
        assertThrows(NotUpdatable.class, "Result Set not updatable.*", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(1, rsTmp.toString(), JDBCType.VARCHAR);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, "Result Set not updatable.*", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(1, rsTmp.toString(), MysqlType.VARCHAR);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, "Result Set not updatable.*", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(1, rsTmp.toString(), JDBCType.VARCHAR, 10);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, "Result Set not updatable.*", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(1, rsTmp.toString(), MysqlType.VARCHAR, 10);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, "Result Set not updatable.*", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("test", rsTmp.toString(), JDBCType.VARCHAR);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, "Result Set not updatable.*", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("test", rsTmp.toString(), MysqlType.VARCHAR);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, "Result Set not updatable.*", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("test", rsTmp.toString(), JDBCType.VARCHAR, 10);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, "Result Set not updatable.*", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("test", rsTmp.toString(), MysqlType.VARCHAR, 10);
                return null;
            }
        });
    }

    /**
     * Test for (Updatable)ResultSet.[update|get]Object().
     * Note: ResultSet.getObject() is covered in methods TestJDBC42Statemet.validateTestData[Local|Offset]DTTypes.
     */
    public void testUpdResultSetUpdateObjectAndNewSupportedTypes() throws Exception {
        /*
         * Objects java.time.Local[Date][Time] are supported via conversion to/from java.sql.[Date|Time|Timestamp].
         */
        createTable("testUpdateObject1", "(id INT PRIMARY KEY, d DATE, t TIME, dt DATETIME, ts TIMESTAMP)");

        Statement testStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

        /*
         * Test insert new rows.
         */
        String testDateString = "2015-01-01";
        String testTimeString = "00:00:01";
        String testDateTimeString = testDateString + " " + testTimeString + ".0";
        String testISODateTimeString = testDateString + "T" + testTimeString + ".0";

        Date testSqlDate = Date.valueOf(testDateString);
        Time testSqlTime = Time.valueOf(testTimeString);
        Timestamp testSqlTimeStamp = Timestamp.valueOf(testDateTimeString);

        LocalDate testLocalDate = LocalDate.parse(testDateString);
        LocalTime testLocalTime = LocalTime.parse(testTimeString);
        LocalDateTime testLocalDateTime = LocalDateTime.parse(testISODateTimeString);

        this.rs = testStmt.executeQuery("SELECT * FROM testUpdateObject1");

        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 1);
        this.rs.updateObject(2, testLocalDate);
        this.rs.updateObject(3, testLocalTime);
        this.rs.updateObject(4, testLocalDateTime);
        this.rs.updateObject(5, testLocalDateTime);
        this.rs.insertRow();
        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 2);
        this.rs.updateObject(2, testLocalDate, 10);
        this.rs.updateObject(3, testLocalTime, 8);
        this.rs.updateObject(4, testLocalDateTime, 20);
        this.rs.updateObject(5, testLocalDateTime, 20);
        this.rs.insertRow();
        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 3);
        this.rs.updateObject("d", testLocalDate);
        this.rs.updateObject("t", testLocalTime);
        this.rs.updateObject("dt", testLocalDateTime);
        this.rs.updateObject("ts", testLocalDateTime);
        this.rs.insertRow();
        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 4);
        this.rs.updateObject("d", testLocalDate, 10);
        this.rs.updateObject("t", testLocalTime, 8);
        this.rs.updateObject("dt", testLocalDateTime, 20);
        this.rs.updateObject("ts", testLocalDateTime, 20);
        this.rs.insertRow();
        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 5);
        this.rs.updateObject(2, testLocalDate, JDBCType.DATE);
        this.rs.updateObject(3, testLocalTime, JDBCType.TIME);
        this.rs.updateObject(4, testLocalDateTime, JDBCType.TIMESTAMP);
        this.rs.updateObject(5, testLocalDateTime, JDBCType.TIMESTAMP);
        this.rs.insertRow();
        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 6);
        this.rs.updateObject(2, testLocalDate, JDBCType.DATE, 10);
        this.rs.updateObject(3, testLocalTime, JDBCType.TIME, 8);
        this.rs.updateObject(4, testLocalDateTime, JDBCType.TIMESTAMP, 20);
        this.rs.updateObject(5, testLocalDateTime, JDBCType.TIMESTAMP, 20);
        this.rs.insertRow();
        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 7);
        this.rs.updateObject("d", testLocalDate, JDBCType.DATE);
        this.rs.updateObject("t", testLocalTime, JDBCType.TIME);
        this.rs.updateObject("dt", testLocalDateTime, JDBCType.TIMESTAMP);
        this.rs.updateObject("ts", testLocalDateTime, JDBCType.TIMESTAMP);
        this.rs.insertRow();
        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 8);
        this.rs.updateObject("d", testLocalDate, JDBCType.DATE, 10);
        this.rs.updateObject("t", testLocalTime, JDBCType.TIME, 8);
        this.rs.updateObject("dt", testLocalDateTime, JDBCType.TIMESTAMP, 20);
        this.rs.updateObject("ts", testLocalDateTime, JDBCType.TIMESTAMP, 20);
        this.rs.insertRow();
        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 9);
        this.rs.updateObject(2, testLocalDate, MysqlType.DATE);
        this.rs.updateObject(3, testLocalTime, MysqlType.TIME);
        this.rs.updateObject(4, testLocalDateTime, MysqlType.TIMESTAMP);
        this.rs.updateObject(5, testLocalDateTime, MysqlType.TIMESTAMP);
        this.rs.insertRow();
        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 10);
        this.rs.updateObject(2, testLocalDate, MysqlType.DATE, 10);
        this.rs.updateObject(3, testLocalTime, MysqlType.TIME, 8);
        this.rs.updateObject(4, testLocalDateTime, MysqlType.TIMESTAMP, 20);
        this.rs.updateObject(5, testLocalDateTime, MysqlType.TIMESTAMP, 20);
        this.rs.insertRow();
        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 11);
        this.rs.updateObject("d", testLocalDate, MysqlType.DATE);
        this.rs.updateObject("t", testLocalTime, MysqlType.TIME);
        this.rs.updateObject("dt", testLocalDateTime, MysqlType.TIMESTAMP);
        this.rs.updateObject("ts", testLocalDateTime, MysqlType.TIMESTAMP);
        this.rs.insertRow();
        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 12);
        this.rs.updateObject("d", testLocalDate, MysqlType.DATE, 10);
        this.rs.updateObject("t", testLocalTime, MysqlType.TIME, 8);
        this.rs.updateObject("dt", testLocalDateTime, MysqlType.TIMESTAMP, 20);
        this.rs.updateObject("ts", testLocalDateTime, MysqlType.TIMESTAMP, 20);
        this.rs.insertRow();

        // check final results.
        this.rs = testStmt.executeQuery("SELECT * FROM testUpdateObject1");
        for (int i = 1; i <= 12; i++) {
            assertTrue(this.rs.next());
            assertEquals(i, this.rs.getInt(1));
            assertEquals(testSqlDate, this.rs.getDate(2));
            assertEquals(testSqlTime, this.rs.getTime(3));
            assertEquals(testSqlTimeStamp, this.rs.getTimestamp(4));
            assertEquals(testSqlTimeStamp, this.rs.getTimestamp(5));
        }
        assertFalse(this.rs.next());

        /*
         * Test update rows.
         */
        testDateString = "2015-12-31";
        testTimeString = "23:59:59";
        testDateTimeString = testDateString + " " + testTimeString + ".0";
        testISODateTimeString = testDateString + "T" + testTimeString + ".0";

        testSqlDate = Date.valueOf(testDateString);
        testSqlTime = Time.valueOf(testTimeString);
        testSqlTimeStamp = Timestamp.valueOf(testDateTimeString);

        testLocalDate = LocalDate.parse(testDateString);
        testLocalTime = LocalTime.parse(testTimeString);
        testLocalDateTime = LocalDateTime.parse(testISODateTimeString);

        this.rs = testStmt.executeQuery("SELECT * FROM testUpdateObject1");

        assertTrue(this.rs.next());
        this.rs.updateObject(2, testLocalDate);
        this.rs.updateObject(3, testLocalTime);
        this.rs.updateObject(4, testLocalDateTime);
        this.rs.updateObject(5, testLocalDateTime);
        this.rs.updateRow();
        assertTrue(this.rs.next());
        this.rs.updateObject(2, testLocalDate, 10);
        this.rs.updateObject(3, testLocalTime, 8);
        this.rs.updateObject(4, testLocalDateTime, 20);
        this.rs.updateObject(5, testLocalDateTime, 20);
        this.rs.updateRow();
        assertTrue(this.rs.next());
        this.rs.updateObject("d", testLocalDate);
        this.rs.updateObject("t", testLocalTime);
        this.rs.updateObject("dt", testLocalDateTime);
        this.rs.updateObject("ts", testLocalDateTime);
        this.rs.updateRow();
        assertTrue(this.rs.next());
        this.rs.updateObject("d", testLocalDate, 10);
        this.rs.updateObject("t", testLocalTime, 8);
        this.rs.updateObject("dt", testLocalDateTime, 20);
        this.rs.updateObject("ts", testLocalDateTime, 20);
        this.rs.updateRow();
        assertTrue(this.rs.next());
        this.rs.updateObject(2, testLocalDate, JDBCType.DATE);
        this.rs.updateObject(3, testLocalTime, JDBCType.TIME);
        this.rs.updateObject(4, testLocalDateTime, JDBCType.TIMESTAMP);
        this.rs.updateObject(5, testLocalDateTime, JDBCType.TIMESTAMP);
        this.rs.updateRow();
        assertTrue(this.rs.next());
        this.rs.updateObject(2, testLocalDate, JDBCType.DATE, 10);
        this.rs.updateObject(3, testLocalTime, JDBCType.TIME, 8);
        this.rs.updateObject(4, testLocalDateTime, JDBCType.TIMESTAMP, 20);
        this.rs.updateObject(5, testLocalDateTime, JDBCType.TIMESTAMP, 20);
        this.rs.updateRow();
        assertTrue(this.rs.next());
        this.rs.updateObject("d", testLocalDate, JDBCType.DATE);
        this.rs.updateObject("t", testLocalTime, JDBCType.TIME);
        this.rs.updateObject("dt", testLocalDateTime, JDBCType.TIMESTAMP);
        this.rs.updateObject("ts", testLocalDateTime, JDBCType.TIMESTAMP);
        this.rs.updateRow();
        assertTrue(this.rs.next());
        this.rs.updateObject("d", testLocalDate, JDBCType.DATE, 10);
        this.rs.updateObject("t", testLocalTime, JDBCType.TIME, 8);
        this.rs.updateObject("dt", testLocalDateTime, JDBCType.TIMESTAMP, 20);
        this.rs.updateObject("ts", testLocalDateTime, JDBCType.TIMESTAMP, 20);
        this.rs.updateRow();
        assertTrue(this.rs.next());
        this.rs.updateObject(2, testLocalDate, MysqlType.DATE);
        this.rs.updateObject(3, testLocalTime, MysqlType.TIME);
        this.rs.updateObject(4, testLocalDateTime, MysqlType.TIMESTAMP);
        this.rs.updateObject(5, testLocalDateTime, MysqlType.TIMESTAMP);
        this.rs.updateRow();
        assertTrue(this.rs.next());
        this.rs.updateObject(2, testLocalDate, MysqlType.DATE, 10);
        this.rs.updateObject(3, testLocalTime, MysqlType.TIME, 8);
        this.rs.updateObject(4, testLocalDateTime, MysqlType.TIMESTAMP, 20);
        this.rs.updateObject(5, testLocalDateTime, MysqlType.TIMESTAMP, 20);
        this.rs.updateRow();
        assertTrue(this.rs.next());
        this.rs.updateObject("d", testLocalDate, MysqlType.DATE);
        this.rs.updateObject("t", testLocalTime, MysqlType.TIME);
        this.rs.updateObject("dt", testLocalDateTime, MysqlType.TIMESTAMP);
        this.rs.updateObject("ts", testLocalDateTime, MysqlType.TIMESTAMP);
        this.rs.updateRow();
        assertTrue(this.rs.next());
        this.rs.updateObject("d", testLocalDate, MysqlType.DATE, 10);
        this.rs.updateObject("t", testLocalTime, MysqlType.TIME, 8);
        this.rs.updateObject("dt", testLocalDateTime, MysqlType.TIMESTAMP, 20);
        this.rs.updateObject("ts", testLocalDateTime, MysqlType.TIMESTAMP, 20);
        this.rs.updateRow();

        // check final results.
        this.rs = testStmt.executeQuery("SELECT * FROM testUpdateObject1");
        int rowCount = 0;
        while (this.rs.next()) {
            String row = "Row " + this.rs.getInt(1);
            assertEquals(row, ++rowCount, this.rs.getInt(1));

            assertEquals(row, testSqlDate, this.rs.getDate(2));
            assertEquals(row, testSqlTime, this.rs.getTime(3));
            assertEquals(row, testSqlTimeStamp, this.rs.getTimestamp(4));
            assertEquals(row, testSqlTimeStamp, this.rs.getTimestamp(5));

            assertEquals(row, testLocalDate, this.rs.getObject(2, LocalDate.class));
            assertEquals(row, testLocalTime, this.rs.getObject(3, LocalTime.class));
            assertEquals(row, testLocalDateTime, this.rs.getObject(4, LocalDateTime.class));
            assertEquals(row, testLocalDateTime, this.rs.getObject(5, LocalDateTime.class));

            assertEquals(row, rowCount, this.rs.getInt("id"));

            assertEquals(row, testSqlDate, this.rs.getDate("d"));
            assertEquals(row, testSqlTime, this.rs.getTime("t"));
            assertEquals(row, testSqlTimeStamp, this.rs.getTimestamp("dt"));
            assertEquals(row, testSqlTimeStamp, this.rs.getTimestamp("ts"));

            assertEquals(row, testLocalDate, this.rs.getObject("d", LocalDate.class));
            assertEquals(row, testLocalTime, this.rs.getObject("t", LocalTime.class));
            assertEquals(row, testLocalDateTime, this.rs.getObject("dt", LocalDateTime.class));
            assertEquals(row, testLocalDateTime, this.rs.getObject("ts", LocalDateTime.class));
        }
        assertEquals(12, rowCount);

        /*
         * Objects java.time.Offset[Date]Time are supported via conversion to *CHAR or serialization.
         */
        OffsetDateTime testOffsetDateTime = OffsetDateTime.of(2015, 8, 04, 12, 34, 56, 7890, ZoneOffset.UTC);
        OffsetTime testOffsetTime = OffsetTime.of(12, 34, 56, 7890, ZoneOffset.UTC);

        createTable("testUpdateObject2", "(id INT PRIMARY KEY, ot1 VARCHAR(100), ot2 BLOB, odt1 VARCHAR(100), odt2 BLOB)");

        this.rs = testStmt.executeQuery("SELECT * FROM testUpdateObject2");
        this.rs.moveToInsertRow();
        this.rs.updateInt(1, 1);
        this.rs.updateObject(2, testOffsetTime, JDBCType.VARCHAR);
        this.rs.updateObject(3, testOffsetTime);
        this.rs.updateObject(4, testOffsetDateTime, JDBCType.VARCHAR);
        this.rs.updateObject(5, testOffsetDateTime);
        this.rs.insertRow();

        this.rs.updateInt("id", 2);
        this.rs.updateObject("ot1", testOffsetTime, JDBCType.VARCHAR);
        this.rs.updateObject("ot2", testOffsetTime);
        this.rs.updateObject("odt1", testOffsetDateTime, JDBCType.VARCHAR);
        this.rs.updateObject("odt2", testOffsetDateTime);
        this.rs.insertRow();

        Connection testConn = getConnectionWithProps("autoDeserialize=true");
        testStmt = testConn.createStatement();

        this.rs = testStmt.executeQuery("SELECT * FROM testUpdateObject2");
        rowCount = 0;
        while (this.rs.next()) {
            String row = "Row " + this.rs.getInt(1);
            assertEquals(row, ++rowCount, this.rs.getInt(1));

            assertEquals(row, testOffsetTime, this.rs.getObject(2, OffsetTime.class));
            assertEquals(row, testOffsetTime, this.rs.getObject(3, OffsetTime.class));
            assertEquals(row, testOffsetDateTime, this.rs.getObject(4, OffsetDateTime.class));
            assertEquals(row, testOffsetDateTime, this.rs.getObject(5, OffsetDateTime.class));

            assertEquals(row, rowCount, this.rs.getInt("id"));

            assertEquals(row, testOffsetTime, this.rs.getObject("ot1", OffsetTime.class));
            assertEquals(row, testOffsetTime, this.rs.getObject("ot2", OffsetTime.class));
            assertEquals(row, testOffsetDateTime, this.rs.getObject("odt1", OffsetDateTime.class));
            assertEquals(row, testOffsetDateTime, this.rs.getObject("odt2", OffsetDateTime.class));
        }
        assertEquals(2, rowCount);

        testConn.close();
    }

    /**
     * Test for (Updatable)ResultSet.updateObject(), unsupported SQL types TIME_WITH_TIMEZONE, TIMESTAMP_WITH_TIMEZONE and REF_CURSOR.
     */
    public void testUpdResultSetUpdateObjectAndNewUnsupportedTypes() throws SQLException {
        createTable("testUnsupportedTypes", "(id INT PRIMARY KEY, col VARCHAR(20))");

        Statement testStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        assertEquals(1, testStmt.executeUpdate("INSERT INTO testUnsupportedTypes VALUES (1, 'dummy')"));
        this.rs = testStmt.executeQuery("SELECT * FROM testUnsupportedTypes");

        /*
         * Unsupported SQL types TIME_WITH_TIMEZONE and TIMESTAMP_WITH_TIMEZONE.
         */

        assertTrue(this.rs.next());

        final ResultSet rsTmp = this.rs;
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(2, LocalTime.now(), JDBCType.TIME_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(2, LocalTime.now(), JDBCType.TIME_WITH_TIMEZONE, 8);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("col", LocalTime.now(), JDBCType.TIME_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIME_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("col", LocalTime.now(), JDBCType.TIME_WITH_TIMEZONE, 8);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(2, LocalDateTime.now(), JDBCType.TIMESTAMP_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(2, LocalDateTime.now(), JDBCType.TIMESTAMP_WITH_TIMEZONE, 20);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("col", LocalDateTime.now(), JDBCType.TIMESTAMP_WITH_TIMEZONE);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: TIMESTAMP_WITH_TIMEZONE", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("col", LocalDateTime.now(), JDBCType.TIMESTAMP_WITH_TIMEZONE, 20);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(2, new Object(), JDBCType.REF_CURSOR);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(2, new Object(), JDBCType.REF_CURSOR, 32);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("col", new Object(), JDBCType.REF_CURSOR);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, "Unsupported SQL type: REF_CURSOR", new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("col", new Object(), JDBCType.REF_CURSOR, 32);
                return null;
            }
        });
    }

    /**
     * Test exceptions thrown when trying to update a read-only result set.
     */
    public void testUpdateForReadOnlyResultSet() throws SQLException {
        Statement testStmt = this.conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        this.rs = testStmt.executeQuery("SELECT 'aaa' as f1");

        assertTrue(this.rs.next());

        final ResultSet rsTmp = this.rs;
        assertThrows(SQLFeatureNotSupportedException.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateArray(1, null);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateArray("f1", null);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateAsciiStream(1, null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateAsciiStream("f1", null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateAsciiStream(1, null, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateAsciiStream("f1", null, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateAsciiStream(1, null, 0L);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateAsciiStream("f1", null, 0L);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBigDecimal(1, null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBigDecimal("f1", null);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBinaryStream(1, null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBinaryStream("f1", null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBinaryStream(1, null, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBinaryStream("f1", null, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBinaryStream(1, null, 0L);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBinaryStream("f1", null, 0L);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBlob(1, (Blob) null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBlob("f1", (Blob) null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBlob(1, (InputStream) null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBlob("f1", (InputStream) null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBlob(1, null, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBlob("f1", null, 0);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBoolean(1, false);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBoolean("f1", false);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateByte(1, (byte) 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateByte("f1", (byte) 0);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBytes(1, new byte[] {});
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateBytes("f1", new byte[] {});
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateCharacterStream(1, null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateCharacterStream("f1", null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateCharacterStream(1, null, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateCharacterStream("f1", null, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateCharacterStream(1, null, 0L);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateCharacterStream("f1", null, 0L);
                return null;
            }
        });

        assertThrows(SQLFeatureNotSupportedException.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateClob(1, (Clob) null);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateClob("f1", (Clob) null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateClob(1, (Reader) null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateClob("f1", (Reader) null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateClob(1, (Reader) null, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateClob("f1", (Reader) null, 0);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateDate(1, null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateDate("f1", null);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateDouble(1, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateDouble("f1", 0);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateFloat(1, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateFloat("f1", 0);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateInt(1, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateInt("f1", 0);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateLong(1, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateLong("f1", 0);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNCharacterStream(1, null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNCharacterStream("f1", null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNCharacterStream(1, null, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNCharacterStream("f1", null, 0);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNClob(1, (NClob) null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNClob("f1", (NClob) null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNClob(1, (Reader) null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNClob("f1", (Reader) null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNClob(1, (Reader) null, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNClob("f1", (Reader) null, 0);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNString(1, null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNString("f1", null);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNull(1);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateNull("f1");
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(1, null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("f1", null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(1, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("f1", 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(1, null, MysqlType.BLOB);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("f1", null, MysqlType.BLOB);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject(1, null, MysqlType.BLOB, 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateObject("f1", null, MysqlType.BLOB, 0);
                return null;
            }
        });

        assertThrows(SQLFeatureNotSupportedException.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateRef(1, null);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateRef("f1", null);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateRow();
                return null;
            }
        });

        assertThrows(SQLFeatureNotSupportedException.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateRowId(1, null);
                return null;
            }
        });
        assertThrows(SQLFeatureNotSupportedException.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateRowId("f1", null);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateShort(1, (short) 0);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateShort("f1", (short) 0);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateSQLXML(1, null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateSQLXML("f1", null);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateString(1, null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateString("f1", null);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateTime(1, null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateTime("f1", null);
                return null;
            }
        });

        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateTimestamp(1, null);
                return null;
            }
        });
        assertThrows(NotUpdatable.class, null, new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                rsTmp.updateTimestamp("f1", null);
                return null;
            }
        });
    }
}
