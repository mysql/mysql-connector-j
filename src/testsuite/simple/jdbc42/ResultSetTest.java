/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package testsuite.simple.jdbc42;

import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
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
import java.util.concurrent.Callable;

import com.mysql.jdbc.NotUpdatable;

import testsuite.BaseTestCase;

public class ResultSetTest extends BaseTestCase {

    public ResultSetTest(String name) {
        super(name);
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
                rsTmp.updateObject(1, rsTmp.toString(), JDBCType.VARCHAR, 10);
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
                rsTmp.updateObject("test", rsTmp.toString(), JDBCType.VARCHAR, 10);
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

        // check final results.
        this.rs = testStmt.executeQuery("SELECT * FROM testUpdateObject1");
        for (int i = 1; i <= 8; i++) {
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

        // check final results.
        this.rs = testStmt.executeQuery("SELECT * FROM testUpdateObject1");
        int rowCount = 0;
        while (rs.next()) {
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
        assertEquals(8, rowCount);

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
        while (rs.next()) {
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
}