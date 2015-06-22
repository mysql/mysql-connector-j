/*
  Copyright (c) 2005, 2015, Oracle and/or its affiliates. All rights reserved.

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

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import testsuite.BaseTestCase;

import com.mysql.cj.core.CharsetMapping;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.exceptions.SQLError;

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
        Map<String, Integer> charsetsMap = new HashMap<String, Integer>();
        Iterator<Integer> collationIndexes = ((ConnectionImpl) c).indexToMysqlCharset.keySet().iterator();
        while (collationIndexes.hasNext()) {
            Integer index = collationIndexes.next();
            String charsetName = null;
            if (((ConnectionImpl) c).indexToCustomMysqlCharset != null) {
                charsetName = ((ConnectionImpl) c).indexToCustomMysqlCharset.get(index);
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

        this.stmt.executeUpdate("INSERT INTO testPadding VALUES (" + emptyBuf.toString() + ", 1), (" + abcBuf.toString() + ", 2), (" + repeatBuf.toString()
                + ", 3)");

        try {
            Properties props = new Properties();
            props.setProperty(PropertyDefinitions.PNAME_padCharsWithSpace, "true");

            paddedConn = getConnectionWithProps(props);

            testPaddingForConnection(paddedConn, numChars, selectBuf);

            props.setProperty(PropertyDefinitions.PNAME_useDynamicCharsetInfo, "true");

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
                                + ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(i + 1), numChars, this.rs.getString(i + 1)
                                .length());
            }
        }

        this.rs = ((com.mysql.cj.jdbc.JdbcConnection) paddedConn).clientPrepareStatement(query).executeQuery();

        while (this.rs.next()) {
            for (int i = 0; i < numCols; i++) {
                assertEquals(
                        "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                + ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(i + 1), numChars, this.rs.getString(i + 1)
                                .length());
            }
        }

        this.rs = ((com.mysql.cj.jdbc.JdbcConnection) paddedConn).serverPrepareStatement(query).executeQuery();

        while (this.rs.next()) {
            for (int i = 0; i < numCols; i++) {
                assertEquals(
                        "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                + ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(i + 1), numChars, this.rs.getString(i + 1)
                                .length());
            }
        }

        this.rs = this.stmt.executeQuery(query);

        while (this.rs.next()) {
            for (int i = 0; i < numCols; i++) {
                if (this.rs.getRow() != 3) {
                    assertTrue(
                            "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                    + ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(i + 1),
                            numChars != this.rs.getString(i + 1).length());
                } else {
                    assertEquals(
                            "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                    + ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(i + 1), numChars,
                            this.rs.getString(i + 1).length());
                }
            }
        }

        this.rs = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).clientPrepareStatement(query).executeQuery();

        while (this.rs.next()) {
            for (int i = 0; i < numCols; i++) {
                if (this.rs.getRow() != 3) {
                    assertTrue(
                            "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                    + ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(i + 1),
                            numChars != this.rs.getString(i + 1).length());
                } else {
                    assertEquals(
                            "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                    + ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(i + 1), numChars,
                            this.rs.getString(i + 1).length());
                }
            }
        }

        this.rs = ((com.mysql.cj.jdbc.JdbcConnection) this.conn).serverPrepareStatement(query).executeQuery();

        while (this.rs.next()) {
            for (int i = 0; i < numCols; i++) {
                if (this.rs.getRow() != 3) {
                    assertTrue(
                            "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                    + ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(i + 1),
                            numChars != this.rs.getString(i + 1).length());
                } else {
                    assertEquals(
                            "For column '" + this.rs.getMetaData().getColumnName(i + 1) + "' of collation "
                                    + ((com.mysql.jdbc.ResultSetMetaData) this.rs.getMetaData()).getColumnCharacterSet(i + 1), numChars,
                            this.rs.getString(i + 1).length());
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

    private void testDateTimeRetrieval_internal(Connection conn) throws Exception {
        createTable("testDateTypes", "(d DATE, t TIME, dt DATETIME)");
        this.stmt.executeUpdate("INSERT INTO testDateTypes VALUES ('2006-02-01', '-40:20:10', '2006-02-01 12:13:14')");
        this.rs = conn.createStatement().executeQuery("select d, t, dt from testDateTypes");
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
            assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex.getSQLState());
        }
        Timestamp timestamp = this.rs.getTimestamp(3);
        assertEquals("2006-02-01 12:13:14.0", timestamp.toString());

        Time time = this.rs.getTime(3);
        assertEquals("12:13:14", time.toString());

        // make sure TS also throws exception on weird HOUR_OF_DAY value
        try {
            this.rs.getTimestamp(2);
        } catch (SQLException ex) {
            assertEquals(SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex.getSQLState());
        }
    }
}
