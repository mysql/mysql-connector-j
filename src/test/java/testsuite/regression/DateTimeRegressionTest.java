/*
 * Copyright (c) 2021, Oracle and/or its affiliates.
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

package testsuite.regression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

import org.junit.jupiter.api.Test;

import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertyDefinitions.SslMode;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.util.TimeUtil;

import testsuite.BaseTestCase;

public class DateTimeRegressionTest extends BaseTestCase {

    /**
     * Tests fix for BUG#3620 -- Timezone not respected correctly.
     * 
     * @throws SQLException
     */
    @Test
    public void testBug3620() throws SQLException {
        // FIXME: This test is sensitive to being in CST/CDT it seems
        assumeTrue(TimeZone.getDefault().equals(TimeZone.getTimeZone("America/Chicago")), "This test is running only in America/Chicago time zone.");

        long epsillon = 3000; // 3 seconds time difference

        try {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3620");
            this.stmt.executeUpdate("CREATE TABLE testBug3620 (field1 TIMESTAMP)");

            PreparedStatement tsPstmt = this.conn.prepareStatement("INSERT INTO testBug3620 VALUES (?)");

            Calendar pointInTime = Calendar.getInstance();
            pointInTime.set(2004, 02, 29, 10, 0, 0);

            long pointInTimeOffset = pointInTime.getTimeZone().getRawOffset();

            java.sql.Timestamp ts = new java.sql.Timestamp(pointInTime.getTime().getTime());

            tsPstmt.setTimestamp(1, ts);
            tsPstmt.executeUpdate();

            String tsValueAsString = getSingleValue("testBug3620", "field1", null).toString();

            System.out.println("Timestamp as string with no calendar: " + tsValueAsString.toString());

            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

            this.stmt.executeUpdate("DELETE FROM testBug3620");

            Statement tsStmt = this.conn.createStatement();

            tsPstmt = this.conn.prepareStatement("INSERT INTO testBug3620 VALUES (?)");

            tsPstmt.setTimestamp(1, ts, cal);
            tsPstmt.executeUpdate();

            tsValueAsString = getSingleValue("testBug3620", "field1", null).toString();

            Timestamp tsValueAsTimestamp = (Timestamp) getSingleValue("testBug3620", "field1", null);

            System.out.println("Timestamp as string with UTC calendar: " + tsValueAsString.toString());
            System.out.println("Timestamp as Timestamp with UTC calendar: " + tsValueAsTimestamp);

            this.rs = tsStmt.executeQuery("SELECT field1 FROM testBug3620");
            this.rs.next();

            Timestamp tsValueUTC = this.rs.getTimestamp(1, cal);

            //
            // We use this testcase with other vendors, JDBC spec requires result set fields can only be read once, although MySQL doesn't require this ;)
            //
            this.rs = tsStmt.executeQuery("SELECT field1 FROM testBug3620");
            this.rs.next();

            Timestamp tsValueStmtNoCal = this.rs.getTimestamp(1);

            System.out.println("Timestamp specifying UTC calendar from normal statement: " + tsValueUTC.toString());

            PreparedStatement tsPstmtRetr = this.conn.prepareStatement("SELECT field1 FROM testBug3620");

            this.rs = tsPstmtRetr.executeQuery();
            this.rs.next();

            Timestamp tsValuePstmtUTC = this.rs.getTimestamp(1, cal);

            System.out.println("Timestamp specifying UTC calendar from prepared statement: " + tsValuePstmtUTC.toString());

            //
            // We use this testcase with other vendors, JDBC spec requires result set fields can only be read once, although MySQL doesn't require this ;)
            //
            this.rs = tsPstmtRetr.executeQuery();
            this.rs.next();

            Timestamp tsValuePstmtNoCal = this.rs.getTimestamp(1);

            System.out.println("Timestamp specifying no calendar from prepared statement: " + tsValuePstmtNoCal.toString());

            long stmtDeltaTWithCal = (ts.getTime() - tsValueStmtNoCal.getTime());

            long deltaOrig = Math.abs(stmtDeltaTWithCal - pointInTimeOffset);

            assertTrue((deltaOrig < epsillon), "Difference between original timestamp and timestamp retrieved using java.sql.Statement "
                    + "set in database using UTC calendar is not ~= " + epsillon + ", it is actually " + deltaOrig);

            long pStmtDeltaTWithCal = (ts.getTime() - tsValuePstmtNoCal.getTime());

            System.out.println(
                    Math.abs(pStmtDeltaTWithCal - pointInTimeOffset) + " < " + epsillon + (Math.abs(pStmtDeltaTWithCal - pointInTimeOffset) < epsillon));
            assertTrue((Math.abs(pStmtDeltaTWithCal - pointInTimeOffset) < epsillon),
                    "Difference between original timestamp and timestamp retrieved using java.sql.PreparedStatement "
                            + "set in database using UTC calendar is not ~= " + epsillon + ", it is actually " + pStmtDeltaTWithCal);

            System.out.println("Difference between original ts and ts with no calendar: " + (ts.getTime() - tsValuePstmtNoCal.getTime()) + ", offset should be "
                    + pointInTimeOffset);
        } finally {
            this.stmt.executeUpdate("DROP TABLE IF EXISTS testBug3620");
        }
    }

    /**
     * Tests fix for Bug#5874, Timezone conversion goes in the wrong direction
     * (when useTimezone=true and server timezone differs from client timezone).
     * 
     * @throws Exception
     */
    @Test
    public void testBug5874() throws Exception {
        assumeTrue(supportsTimeZoneNames(this.stmt), "This test requies the server with populated time zone tables.");

        TimeZone defaultTimeZone = TimeZone.getDefault();

        try {
            String clientTimeZoneName = "America/Los_Angeles";
            String connectionTimeZoneName = "America/Chicago";

            TimeZone.setDefault(TimeZone.getTimeZone(clientTimeZoneName));

            long offsetDifference = TimeZone.getDefault().getRawOffset() - TimeZone.getTimeZone(connectionTimeZoneName).getRawOffset();

            SimpleDateFormat timestampFormat = TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd HH:mm:ss", null);
            SimpleDateFormat timeFormat = TimeUtil.getSimpleDateFormat(null, "HH:mm:ss", null);

            long pointInTime = timestampFormat.parse("2004-10-04 09:19:00").getTime();

            Properties props = new Properties();
            props.put("useTimezone", "true");
            props.put(PropertyKey.connectionTimeZone.getKeyName(), connectionTimeZoneName);
            props.put(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "true");
            props.put(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
            props.setProperty(PropertyKey.preserveInstants.getKeyName(), "true");

            Connection tzConn = getConnectionWithProps(props);
            Statement tzStmt = tzConn.createStatement();
            createTable("testBug5874", "(tstamp DATETIME, t TIME)");

            PreparedStatement tsPstmt = tzConn.prepareStatement("INSERT INTO testBug5874 VALUES (?, ?)");

            tsPstmt.setTimestamp(1, new Timestamp(pointInTime));
            tsPstmt.setTime(2, new Time(pointInTime));
            tsPstmt.executeUpdate();

            this.rs = tzStmt.executeQuery("SELECT * from testBug5874");

            while (this.rs.next()) { // Driver now converts/checks DATE/TIME/TIMESTAMP/DATETIME types when calling getString()...
                String retrTimestampString = new String(this.rs.getBytes(1));
                Timestamp retrTimestamp = this.rs.getTimestamp(1);

                java.util.Date timestampOnServer = timestampFormat.parse(retrTimestampString);

                long retrievedOffsetForTimestamp = retrTimestamp.getTime() - timestampOnServer.getTime();

                assertEquals(offsetDifference, retrievedOffsetForTimestamp,
                        "Original timestamp and timestamp retrieved using client timezone are not the same");

                String retrTimeString = new String(this.rs.getBytes(2));
                Time retrTime = this.rs.getTime(2);

                java.util.Date timeOnServerAsDate = timeFormat.parse(retrTimeString);
                Time timeOnServer = new Time(timeOnServerAsDate.getTime());

                long retrievedOffsetForTime = retrTime.getTime() - timeOnServer.getTime();

                assertEquals(0, retrievedOffsetForTime, "Original time and time retrieved using client timezone are not the same");
            }

            tzConn.close();
        } finally {
            TimeZone.setDefault(defaultTimeZone);
        }
    }

    /**
     * Tests fix for Bug#15604 (11745460), TIMEZONE DISCARDED STORING JAVA.UTIL.CALENDAR INTO DATETIME.
     * 
     * @throws Exception
     */
    @Test
    public void testBug15604() throws Exception {
        assumeTrue(supportsTimeZoneNames(this.stmt), "This test requies the server with populated time zone tables.");

        createTable("testBug15604_date_cal", "(field1 DATE)");
        Properties props = new Properties();
        props.setProperty(PropertyKey.sessionVariables.getKeyName(), "time_zone='America/Chicago'");

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

    /**
     * Tests fix for Bug#20391832, SETOBJECT() FOR TYPES.TIME RESULTS IN EXCEPTION WHEN VALUE HAS FRACTIONAL PART.
     * 
     * @throws Exception
     */
    @Test
    public void testBug20391832() throws Exception {
        createTable("testBug20391832", "(v varchar(40))");

        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "LOCAL");
        for (boolean useSSPS : new boolean[] { false, true }) {
            for (boolean sendFr : new boolean[] { false, true }) {

                System.out.println("useServerPrepStmts=" + useSSPS + "; sendFractSeconds=" + sendFr);

                props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFr);
                Connection testConn = getConnectionWithProps(timeZoneFreeDbUrl, props);

                this.pstmt = testConn.prepareStatement("insert into testBug20391832 values(?)");

                for (MysqlType type : new MysqlType[] { MysqlType.DATETIME, MysqlType.TIMESTAMP }) {
                    subTestBug20391832(props, type, "2038-01-19", "2038-01-19 00:00:00");
                    subTestBug20391832(props, type, "38-01-19", "2038-01-19 00:00:00");
                    subTestBug20391832(props, type, "2038#01$19", "2038-01-19 00:00:00");
                    subTestBug20391832(props, type, "38#01$19", "2038-01-19 00:00:00");
                    subTestBug20391832(props, type, "20380119", "2038-01-19 00:00:00");
                    subTestBug20391832(props, type, "380119", "2038-01-19 00:00:00");
                    subTestBug20391832(props, type, "030417", "2003-04-17 00:00:00"); // resolved as a DATE literal

                    assertThrows(SQLException.class, ".* Conversion from java.time.Duration to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "12 1", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.Duration to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "12 13:04", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.Duration to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "12 1:4:1", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.Duration to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "12 1:04:17.123456789", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.Duration to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "-838:59:59", null);
                        return null;
                    });

                    assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "13:04:17", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "13:04", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "0417", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "03:14:07.012", null);
                        return null;
                    });
                    assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to " + type + " is not supported.", () -> {
                        subTestBug20391832(props, type, "031407.123", null);
                        return null;
                    });

                    assertThrows(SQLException.class, ".* There is no known date-time pattern for.*", () -> {
                        subTestBug20391832(props, type, "031407#12", null); // wrong delimiter
                        return null;
                    });

                    subTestBug20391832(props, type, "2038-01-19 03:14:07", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038-01-19 03:14:07.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "38-01-19 03:14:07", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "38-01-19 03:14:07.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38-01-19 03:14:07.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "2038#01$19 03@14%07", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19 03@14%07.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "38#01$19 03@14%07", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "38#01$19 03@14%07.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19 03@14%07.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "2038#01$19T03@14%07", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#01$19T03@14%07.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "38#01$19T03@14%07", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "38#01$19T03@14%07.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "38#01$19T03@14%07.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "2038#1$19 3@14%7", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "2038#1$19T3@14%7.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19 3@14%7.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19T3@14%7.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19 3@14%7.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19T3@14%7.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19 3@14%7.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19T3@14%7.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19 3@14%7.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "2038#1$19T3@14%7.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "38#1$9 3@4%7", "2038-01-09 03:04:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "38#1$9T3@4%7.1", sendFr ? "2038-01-09 03:04:07.1" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.01", sendFr ? "2038-01-09 03:04:07.01" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.012", sendFr ? "2038-01-09 03:04:07.012" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.0123", sendFr ? "2038-01-09 03:04:07.0123" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.01234", sendFr ? "2038-01-09 03:04:07.01234" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.012345", sendFr ? "2038-01-09 03:04:07.012345" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.0123456", sendFr ? "2038-01-09 03:04:07.012346" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.01234567", sendFr ? "2038-01-09 03:04:07.012346" : "2038-01-09 03:04:07");
                        subTestBug20391832(props, type, "38#1$9T3@4%7.012345678", sendFr ? "2038-01-09 03:04:07.012346" : "2038-01-09 03:04:07");
                    }

                    subTestBug20391832(props, type, "20380119031407", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "20380119031407.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "20380119031407.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }

                    subTestBug20391832(props, type, "380119031407", "2038-01-19 03:14:07");
                    if (withFract) {
                        subTestBug20391832(props, type, "380119031407.1", sendFr ? "2038-01-19 03:14:07.1" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.01", sendFr ? "2038-01-19 03:14:07.01" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.012", sendFr ? "2038-01-19 03:14:07.012" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.0123", sendFr ? "2038-01-19 03:14:07.0123" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.01234", sendFr ? "2038-01-19 03:14:07.01234" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.012345", sendFr ? "2038-01-19 03:14:07.012345" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.0123456", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.01234567", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                        subTestBug20391832(props, type, "380119031407.012345678", sendFr ? "2038-01-19 03:14:07.012346" : "2038-01-19 03:14:07");
                    }
                }

                // ================================================

                subTestBug20391832(props, MysqlType.TIME, "12 1", "289:0:0");
                subTestBug20391832(props, MysqlType.TIME, "-12 1", "-289:0:0");
                subTestBug20391832(props, MysqlType.TIME, "12 12:04", "300:4:0");
                subTestBug20391832(props, MysqlType.TIME, "-12 12:04", "-300:4:0");
                subTestBug20391832(props, MysqlType.TIME, "12 1:4:1", "289:4:1");
                subTestBug20391832(props, MysqlType.TIME, "-12 1:4:7", "-289:4:7");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "5 1:04:17.123456789", sendFr ? "121:4:17.123457" : "121:4:17");
                    subTestBug20391832(props, MysqlType.TIME, "-5 1:04:17.123456789", sendFr ? "-121:4:17.123457" : "-121:4:17");
                }
                subTestBug20391832(props, MysqlType.TIME, "25:59:59", "25:59:59");
                subTestBug20391832(props, MysqlType.TIME, "-838:59:59", "-838:59:59");

                assertThrows(SQLException.class, ".* Conversion from java.time.LocalDate to TIME is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalDate to TIME is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalDate to TIME is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalDate to TIME is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalDate to TIME is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.TIME, "20380119", null);
                    return null;
                });
                subTestBug20391832(props, MysqlType.TIME, "120119", "12:01:19");
                subTestBug20391832(props, MysqlType.TIME, "13:04:17", "13:04:17");
                subTestBug20391832(props, MysqlType.TIME, "13:04", "13:04:00");
                subTestBug20391832(props, MysqlType.TIME, "130417", "13:04:17");
                subTestBug20391832(props, MysqlType.TIME, "0417", "00:04:17");
                subTestBug20391832(props, MysqlType.TIME, "17", "00:00:17");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "0417.1234567", sendFr ? "00:04:17.123457" : "00:04:17");
                    subTestBug20391832(props, MysqlType.TIME, "17.1234567", sendFr ? "00:00:17.123457" : "00:00:17");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "3:14:07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:4:07.012345", sendFr ? "03:04:07.012345" : "03:04:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "3:4:7.0123456", sendFr ? "03:04:07.012346" : "03:04:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:7.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "03:14:07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "031407.123", sendFr ? "03:14:07.123" : "03:14:07");
                }

                assertThrows(SQLException.class, ".* There is no known date-time pattern for.*", () -> {
                    subTestBug20391832(props, MysqlType.TIME, "031407#12", null); // wrong delimiter
                    return null;
                });

                subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038-01-19 03:14:07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38-01-19 03:14:07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19 03@14%07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19 03@14%07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "2038#01$19T03@14%07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#01$19T03@14%07.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "2038#1$19 3@14%7", "03:14:07");
                subTestBug20391832(props, MysqlType.TIME, "38#1$19 3@14%7", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19T3@14%7.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19 3@14%7.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19T3@14%7.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19T3@14%7.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19T3@14%7.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19 3@14%7.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19T3@14%7.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19T3@14%7.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "38#1$19 3@14%7.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "20380119031407", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "20380119031407.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                subTestBug20391832(props, MysqlType.TIME, "380119031407", "03:14:07");
                if (withFract) {
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.1", sendFr ? "03:14:07.1" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.01", sendFr ? "03:14:07.01" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.012", sendFr ? "03:14:07.012" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.0123", sendFr ? "03:14:07.0123" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.01234", sendFr ? "03:14:07.01234" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.012345", sendFr ? "03:14:07.012345" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.0123456", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.01234567", sendFr ? "03:14:07.012346" : "03:14:07");
                    subTestBug20391832(props, MysqlType.TIME, "380119031407.012345678", sendFr ? "03:14:07.012346" : "03:14:07");
                }

                // ================================================

                subTestBug20391832(props, MysqlType.DATE, "2038-01-19", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-1-9", "2038-01-09");
                subTestBug20391832(props, MysqlType.DATE, "38-1-9", "2038-01-09");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "030417", "2003-04-17");

                assertThrows(SQLException.class, ".* Conversion from java.time.Duration to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "12 1", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.Duration to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "12 13:04", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.Duration to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "12 1:4:1", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.Duration to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "12 1:04:17.123456789", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.Duration to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "-838:59:59", null);
                    return null;
                });

                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "13:04:17", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "13:04", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "0417", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "03:14:07.012", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to DATE is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "031407.123", null);
                    return null;
                });

                assertThrows(SQLException.class, ".* There is no known date-time pattern for.*", () -> {
                    subTestBug20391832(props, MysqlType.DATE, "031407#12", null); // wrong delimiter
                    return null;
                });

                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.12", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038-01-19 03:14:07.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.12", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38-01-19 03:14:07.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.12", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19 03@14%07.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.12", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19 03@14%07.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.12", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "2038#01$19T03@14%07.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.12", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#01$19T03@14%07.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "2038#1$19 3@14%7", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "38#1$9 3@4%7", "2038-01-09");

                subTestBug20391832(props, MysqlType.DATE, "20380119031407", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.01", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "20380119031407.012345678", "2038-01-19");

                subTestBug20391832(props, MysqlType.DATE, "380119031407", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.1", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.01", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.012", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.0123", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.01234", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.012345", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.0123456", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.01234567", "2038-01-19");
                subTestBug20391832(props, MysqlType.DATE, "380119031407.012345678", "2038-01-19");

                // ================================================

                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "030417", "2003");

                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to YEAR is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "13:04:17", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to YEAR is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "13:04", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to YEAR is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "0417", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to YEAR is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "03:14:07.012", null);
                    return null;
                });
                assertThrows(SQLException.class, ".* Conversion from java.time.LocalTime to YEAR is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "031407.123", null);
                    return null;
                });

                assertThrows(SQLException.class, ".* There is no known date-time pattern for.*", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "031407#12", null); // wrong delimiter
                    return null;
                });

                assertThrows(SQLException.class, ".* Conversion from java.time.Duration to YEAR is not supported.", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "-838:59:59", null);
                    return null;
                });

                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.12", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.012345678", "2038");

                assertThrows(SQLException.class, ".* There is no known date-time pattern for.*", () -> {
                    subTestBug20391832(props, MysqlType.YEAR, "2038-01-19 03:14:07.0123456789", null); // nanos part is too long
                    return null;
                });

                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.12", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38-01-19 03:14:07.012345678", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.12", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19 03@14%07.012345678", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.12", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19 03@14%07.012345678", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.12", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "2038#01$19T03@14%07.012345678", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.12", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#01$19T03@14%07.012345678", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "2038#1$19 3@14%7", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "38#1$9 3@4%7", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "20380119031407", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.01", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "20380119031407.012345678", "2038");

                subTestBug20391832(props, MysqlType.YEAR, "380119031407", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.1", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.01", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.012", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.0123", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.01234", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.012345", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.0123456", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.01234567", "2038");
                subTestBug20391832(props, MysqlType.YEAR, "380119031407.012345678", "2038");

                testConn.close();
            }
        }
    }

    private void subTestBug20391832(Properties props, MysqlType targetType, String val, String exp) throws Exception {
        boolean useSSPS = Boolean.parseBoolean(props.getProperty(PropertyKey.useServerPrepStmts.getKeyName()));

        this.pstmt.setObject(1, val, targetType);
        String query = this.pstmt.toString().replace(".0)", ")");

        assertEquals((useSSPS ? "com.mysql.cj.jdbc.ServerPreparedStatement[1]: " : "com.mysql.cj.jdbc.ClientPreparedStatement: ")
                + "insert into testBug20391832 values(" + (targetType == MysqlType.YEAR ? exp : "'" + exp + "'") + ")", query);
    }

    /**
     * Tests fix for Bug#20316640, GETTIMESTAMP() CALL WITH CALANDER VALUE NULL RESULTS IN NULLPOINTEREXCEPTION.
     * 
     * @throws Exception
     */
    @Test
    public void testBug20316640() throws Exception {
        createTable("testBug20316640", "(c1 timestamp, c2 time, c3 date)");
        this.stmt.execute("insert into testBug20316640 values('2038-01-19 03:14:07','18:59:59','9999-12-31')");
        this.rs = this.stmt.executeQuery("select * from testBug20316640");
        this.rs.next();
        System.out.println("Col 1 [" + this.rs.getTimestamp("c1", null) + "]");
        System.out.println("Col 2 [" + this.rs.getTime("c2", null) + "]");
        System.out.println("Col 3 [" + this.rs.getDate("c3", null) + "]");
    }

    /**
     * Tests fix for Bug#20818678, GETFLOAT() AND GETDOUBLE() CALL ON YEAR COLUMN RESULTS IN EXCEPTION
     * 
     * @throws Exception
     */
    @Test
    public void testBug20818678() throws Exception {
        createTable("testBug20818678", "(c1 YEAR)");
        this.stmt.execute("insert into testBug20818678 values(2155)");

        Connection con = null;
        PreparedStatement ps = null;

        try {

            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            for (boolean yearIsDateType : new boolean[] { true, false }) {
                for (boolean useSSPS : new boolean[] { false, true }) {
                    props.setProperty(PropertyKey.yearIsDateType.getKeyName(), "" + yearIsDateType);
                    props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);

                    con = getConnectionWithProps(props);
                    ps = con.prepareStatement("select * from testBug20818678 ");
                    this.rs = ps.executeQuery();
                    this.rs.next();

                    if (yearIsDateType) {
                        assertEquals("2155-01-01", this.rs.getString("c1"));
                    } else {
                        assertEquals("2155", this.rs.getString("c1"));
                    }
                    assertEquals(Float.valueOf(2155), this.rs.getFloat("c1"));
                    assertEquals(Double.valueOf(2155), this.rs.getDouble("c1"));

                    ps.close();
                    con.close();
                }
            }

        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    /**
     * Tests fix for Bug#21308907, NO WAY TO GET THE FRACTIONAL PART OF A TIME FIELD WHEN USESERVERPREPSTMTS=TRUE.
     * 
     * @throws Exception
     */
    @Test
    public void testBug21308907() throws Exception {
        assumeTrue(versionMeetsMinimum(5, 6, 4), "Fractional seconds are not supported in this server version.");

        createTable("testBug21308907", "(c1 time(6))");
        this.stmt.execute("insert into testBug21308907 values('12:59:59.123456')");

        Connection con = null;
        try {
            Properties props = new Properties();
            props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
            props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
            con = getConnectionWithProps(props);

            this.stmt = con.createStatement();
            this.rs = this.stmt.executeQuery("select * from testBug21308907");
            this.rs.next();

            assertEquals(123456000, this.rs.getTimestamp("c1").getNanos());
            String s = this.rs.getString("c1");
            assertEquals(".123456", s.substring(s.indexOf('.')));

            this.rs.close();

            this.pstmt = con.prepareStatement("select * from testBug21308907 ");
            this.rs = this.pstmt.executeQuery();
            this.rs.next();

            assertEquals(123456000, this.rs.getTimestamp("c1").getNanos());
            s = this.rs.getString("c1");
            assertEquals(".123456", s.substring(s.indexOf('.')));

        } finally {
            if (con != null) {
                con.close();
            }
        }
    }

    /**
     * Tests fix for Bug#22305930, GETTIMESTAMP() CALL ON CLOSED RESULT SET PRODUCES NPE.
     * 
     * @throws Exception
     */
    @Test
    public void testBug22305930() throws Exception {
        ResultSet rs1 = this.stmt.executeQuery("select '2015-12-09 16:28:01' as tm");
        rs1.next();
        rs1.close();

        assertThrows(SQLException.class, "Operation not allowed after ResultSet closed", () -> {
            rs1.getTimestamp("tm");
            return null;
        });
    }

    /**
     * Tests fix for Bug#101413 (32099505), JAVA.TIME.LOCALDATETIME CANNOT BE CAST TO JAVA.SQL.TIMESTAMP.
     * 
     * @throws Exception
     */
    @Test
    public void testBug101413() throws Exception {
        assumeTrue(supportsTimeZoneNames(this.stmt), "This test requies the server with populated time zone tables.");

        createTable("testBug101413", "(createtime1 TIMESTAMP, createtime2 DATETIME)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
            for (boolean preserveInstants : new boolean[] { false, true }) {
                for (boolean useSSPS : new boolean[] { false, true }) {
                    props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                    props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                    props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                    props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");

                    Connection con = getConnectionWithProps(props);
                    PreparedStatement ps = con.prepareStatement("insert into testBug101413(createtime1, createtime2) values(?, ?)");

                    con.setAutoCommit(false);
                    for (int i = 1; i <= 20000; i++) {
                        ps.setObject(1, LocalDateTime.now());
                        ps.setObject(2, LocalDateTime.now());
                        ps.addBatch();
                        if (i % 500 == 0) {
                            ps.executeBatch();
                            ps.clearBatch();
                        }
                    }
                    con.commit();
                }
            }
        }
    }

    /**
     * Tests fix for Bug#104559 (33232419), ResultSet.getObject(i, java.util.Date.class) throws NPE when the value is null.
     * 
     * @throws Exception
     */
    @Test
    public void testBug104559() throws Exception {
        createTable("testBug104559", "(`dt` DATE DEFAULT NULL)");
        this.stmt.executeUpdate("INSERT INTO testBug104559 VALUES (null)");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), SslMode.DISABLED.name());
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        Connection con = getConnectionWithProps(props);
        Statement st = con.createStatement();
        this.rs = st.executeQuery("SELECT * FROM testBug104559");
        assertTrue(this.rs.next());
        assertNull(this.rs.getObject("dt", java.util.Date.class));
        assertFalse(this.rs.next());
    }
}
