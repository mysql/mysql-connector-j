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

package testsuite.simple;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.jdbc.JdbcStatement;

import testsuite.BaseTestCase;

public class QueryAttributesTest extends BaseTestCase {
    @BeforeEach
    public void setUp() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 26));

        this.rs = this.stmt.executeQuery("SELECT * FROM mysql.component WHERE component_urn = 'file://component_query_attributes'");
        if (!this.rs.next()) {
            this.stmt.execute("INSTALL COMPONENT 'file://component_query_attributes'");
        }
    }

    @AfterEach
    public void teardown() throws Exception {
        assumeTrue(versionMeetsMinimum(8, 0, 26));

        this.stmt.execute("UNINSTALL COMPONENT 'file://component_query_attributes'");
    }

    /**
     * Tests all supported query attributes types when used in plain statements.
     * 
     * @throws Exception
     */
    @Test
    public void queryAttributesTypesInPlainStatement() throws Exception {
        long testInstInMilli = 801216026987l; // Tuesday, May 23, 1995 08:00:26.987654 GMT
        long testInstInSecs = 801216026;
        int testInstHour = 8;
        int testInstMin = 0;
        int testInstSec = 26;
        int testInstNano = 987654321;
        int testOffset = 2;
        String testZoneId = "UTC+2";
        String testTimezone = "Europe/Stockholm";
        Calendar testCal = Calendar.getInstance(TimeZone.getTimeZone(testTimezone));
        testCal.setTimeInMillis(testInstInMilli);
        List<String> testList = Arrays.asList("MySQL", "Connector/J");

        String expectedLocalTime = new SimpleDateFormat("HH:mm:ss.SSS000").format(new Date(testInstInMilli));
        String expectedLocalDatetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000").format(new Date(testInstInMilli))
                + new SimpleDateFormat("XXX").format(new Date(testInstInMilli)).replaceAll("([+-])0", "$1").replace("Z", "+0:00");

        Statement testStmt = this.conn.createStatement();

        assertTrue(JdbcStatement.class.isInstance(testStmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testStmt;
        testJdbcStmt.setAttribute("qa01", null);
        testJdbcStmt.setAttribute("qa02", "Query Attributes");
        testJdbcStmt.setAttribute("qa03", false);
        testJdbcStmt.setAttribute("qa04", (byte) 42);
        testJdbcStmt.setAttribute("qa05", (short) -42);
        testJdbcStmt.setAttribute("qa06", Integer.MAX_VALUE);
        testJdbcStmt.setAttribute("qa07", Long.MAX_VALUE);
        testJdbcStmt.setAttribute("qa08", new BigInteger("351910092110"));
        testJdbcStmt.setAttribute("qa09", 2.71828182f);
        testJdbcStmt.setAttribute("qa10", 3.141592653589793d);
        testJdbcStmt.setAttribute("qa11", new BigDecimal("1.61803398874989484820"));
        testJdbcStmt.setAttribute("qa12", new java.sql.Date(testInstInMilli));
        testJdbcStmt.setAttribute("qa13", LocalDate.of(1995, 5, 23));
        testJdbcStmt.setAttribute("qa14", new Time(testInstInMilli));
        testJdbcStmt.setAttribute("qa15", LocalTime.of(testInstHour, testInstMin, testInstSec, testInstNano));
        testJdbcStmt.setAttribute("qa16", OffsetTime.of(testInstHour, testInstMin, testInstSec, testInstNano, ZoneOffset.ofHours(testOffset)));
        testJdbcStmt.setAttribute("qa17", Duration.ofDays(-2).plusHours(2).plusMinutes(20));
        testJdbcStmt.setAttribute("qa18", LocalDateTime.ofEpochSecond(testInstInSecs, testInstNano, ZoneOffset.ofHours(testOffset)));
        testJdbcStmt.setAttribute("qa19", new Timestamp(testInstInMilli));
        testJdbcStmt.setAttribute("qa20", Instant.ofEpochMilli(testInstInMilli));
        testJdbcStmt.setAttribute("qa21", OffsetDateTime.ofInstant(Instant.ofEpochMilli(testInstInMilli), ZoneId.of(testZoneId)));
        testJdbcStmt.setAttribute("qa22", ZonedDateTime.ofInstant(Instant.ofEpochMilli(testInstInMilli), ZoneId.of(testTimezone)));
        testJdbcStmt.setAttribute("qa23", new Date(testInstInMilli));
        testJdbcStmt.setAttribute("qa24", testCal);
        testJdbcStmt.setAttribute("qa25", testList);

        this.rs = testStmt.executeQuery("SELECT 'MySQL Connector/J', " + IntStream.range(1, 26)
                .mapToObj(i -> String.format("mysql_query_attribute_string('qa%1$02d') AS qa%1$02d", i)).collect(Collectors.joining(", ")) + ", '8.0.26'");
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertNull(this.rs.getString("qa01"));
        assertTrue(this.rs.wasNull());
        assertEquals("Query Attributes", this.rs.getString("qa02"));
        assertEquals("0", this.rs.getString("qa03"));
        assertEquals("42", this.rs.getString("qa04"));
        assertEquals("-42", this.rs.getString("qa05"));
        assertEquals("2147483647", this.rs.getString("qa06"));
        assertEquals("9223372036854775807", this.rs.getString("qa07"));
        assertEquals("351910092110", this.rs.getString("qa08"));
        assertTrue(this.rs.getString("qa09").startsWith("2.71828"));
        assertTrue(this.rs.getString("qa10").startsWith("3.14159"));
        assertTrue(this.rs.getString("qa11").startsWith("1.61803"));
        assertEquals("1995-05-23", this.rs.getString("qa12"));
        assertEquals("1995-05-23", this.rs.getString("qa13"));
        assertEquals(expectedLocalTime, this.rs.getString("qa14"));
        assertEquals("08:00:26.987654", this.rs.getString("qa15"));
        assertEquals("08:00:26.987654", this.rs.getString("qa16"));
        assertEquals("-45:40:00.000000", this.rs.getString("qa17"));
        assertEquals("1995-05-23 10:00:26.987654", this.rs.getString("qa18"));
        assertEquals(expectedLocalDatetime, this.rs.getString("qa19"));
        assertEquals("1995-05-23 08:00:26.987000+0:00", this.rs.getString("qa20"));
        assertEquals("1995-05-23 10:00:26.987000+2:00", this.rs.getString("qa21"));
        assertEquals("1995-05-23 10:00:26.987000+2:00", this.rs.getString("qa22"));
        assertEquals(expectedLocalDatetime, this.rs.getString("qa23"));
        assertEquals("1995-05-23 10:00:26.987000+2:00", this.rs.getString("qa24"));
        assertEquals("[MySQL, Connector/J]", this.rs.getString("qa25"));
        assertEquals("8.0.26", this.rs.getString(27));
        assertFalse(this.rs.next());
    }

    /**
     * Tests all supported query attributes types when used in client prepared statements.
     * 
     * @throws Exception
     */
    @Test
    public void queryAttributesTypesInClientPreparedStatement() throws Exception {
        long testInstInMilli = 801216026987l; // Tuesday, May 23, 1995 08:00:26.987654 GMT
        long testInstInSecs = 801216026;
        int testInstHour = 8;
        int testInstMin = 0;
        int testInstSec = 26;
        int testInstNano = 987654321;
        int testOffset = 2;
        String testZoneId = "UTC+2";
        String testTimezone = "Europe/Stockholm";
        Calendar testCal = Calendar.getInstance(TimeZone.getTimeZone(testTimezone));
        testCal.setTimeInMillis(testInstInMilli);
        List<String> testList = Arrays.asList("MySQL", "Connector/J");

        String expectedLocalTime = new SimpleDateFormat("HH:mm:ss.SSS000").format(new Date(testInstInMilli));
        String expectedLocalDatetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000").format(new Date(testInstInMilli))
                + new SimpleDateFormat("XXX").format(new Date(testInstInMilli)).replaceAll("([+-])0", "$1").replace("Z", "+0:00");

        Properties props = new Properties();
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false");
        Connection testConn = getConnectionWithProps(props);

        PreparedStatement testPstmt = testConn.prepareStatement("SELECT ?, " + IntStream.range(1, 26)
                .mapToObj(i -> String.format("mysql_query_attribute_string('qa%1$02d') AS qa%1$02d", i)).collect(Collectors.joining(", ")) + ", ?");
        testPstmt.setString(1, "MySQL Connector/J");
        testPstmt.setString(2, "8.0.26");

        assertTrue(JdbcStatement.class.isInstance(testPstmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testPstmt;
        testJdbcStmt.setAttribute("qa01", null);
        testJdbcStmt.setAttribute("qa02", "Query Attributes");
        testJdbcStmt.setAttribute("qa03", false);
        testJdbcStmt.setAttribute("qa04", (byte) 42);
        testJdbcStmt.setAttribute("qa05", (short) -42);
        testJdbcStmt.setAttribute("qa06", Integer.MAX_VALUE);
        testJdbcStmt.setAttribute("qa07", Long.MAX_VALUE);
        testJdbcStmt.setAttribute("qa08", new BigInteger("351910092110"));
        testJdbcStmt.setAttribute("qa09", 2.71828182f);
        testJdbcStmt.setAttribute("qa10", 3.141592653589793d);
        testJdbcStmt.setAttribute("qa11", new BigDecimal("1.61803398874989484820"));
        testJdbcStmt.setAttribute("qa12", new java.sql.Date(testInstInMilli));
        testJdbcStmt.setAttribute("qa13", LocalDate.of(1995, 5, 23));
        testJdbcStmt.setAttribute("qa14", new Time(testInstInMilli));
        testJdbcStmt.setAttribute("qa15", LocalTime.of(testInstHour, testInstMin, testInstSec, testInstNano));
        testJdbcStmt.setAttribute("qa16", OffsetTime.of(testInstHour, testInstMin, testInstSec, testInstNano, ZoneOffset.ofHours(testOffset)));
        testJdbcStmt.setAttribute("qa17", Duration.ofDays(-2).plusHours(2).plusMinutes(20));
        testJdbcStmt.setAttribute("qa18", LocalDateTime.ofEpochSecond(testInstInSecs, testInstNano, ZoneOffset.ofHours(testOffset)));
        testJdbcStmt.setAttribute("qa19", new Timestamp(testInstInMilli));
        testJdbcStmt.setAttribute("qa20", Instant.ofEpochMilli(testInstInMilli));
        testJdbcStmt.setAttribute("qa21", OffsetDateTime.ofInstant(Instant.ofEpochMilli(testInstInMilli), ZoneId.of(testZoneId)));
        testJdbcStmt.setAttribute("qa22", ZonedDateTime.ofInstant(Instant.ofEpochMilli(testInstInMilli), ZoneId.of(testTimezone)));
        testJdbcStmt.setAttribute("qa23", new Date(testInstInMilli));
        testJdbcStmt.setAttribute("qa24", testCal);
        testJdbcStmt.setAttribute("qa25", testList);

        this.rs = testPstmt.executeQuery();
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertNull(this.rs.getString("qa01"));
        assertTrue(this.rs.wasNull());
        assertEquals("Query Attributes", this.rs.getString("qa02"));
        assertEquals("0", this.rs.getString("qa03"));
        assertEquals("42", this.rs.getString("qa04"));
        assertEquals("-42", this.rs.getString("qa05"));
        assertEquals("2147483647", this.rs.getString("qa06"));
        assertEquals("9223372036854775807", this.rs.getString("qa07"));
        assertEquals("351910092110", this.rs.getString("qa08"));
        assertTrue(this.rs.getString("qa09").startsWith("2.71828"));
        assertTrue(this.rs.getString("qa10").startsWith("3.14159"));
        assertTrue(this.rs.getString("qa11").startsWith("1.61803"));
        assertEquals("1995-05-23", this.rs.getString("qa12"));
        assertEquals("1995-05-23", this.rs.getString("qa13"));
        assertEquals(expectedLocalTime, this.rs.getString("qa14"));
        assertEquals("08:00:26.987654", this.rs.getString("qa15"));
        assertEquals("08:00:26.987654", this.rs.getString("qa16"));
        assertEquals("-45:40:00.000000", this.rs.getString("qa17"));
        assertEquals("1995-05-23 10:00:26.987654", this.rs.getString("qa18"));
        assertEquals(expectedLocalDatetime, this.rs.getString("qa19"));
        assertEquals("1995-05-23 08:00:26.987000+0:00", this.rs.getString("qa20"));
        assertEquals("1995-05-23 10:00:26.987000+2:00", this.rs.getString("qa21"));
        assertEquals("1995-05-23 10:00:26.987000+2:00", this.rs.getString("qa22"));
        assertEquals(expectedLocalDatetime, this.rs.getString("qa23"));
        assertEquals("1995-05-23 10:00:26.987000+2:00", this.rs.getString("qa24"));
        assertEquals("[MySQL, Connector/J]", this.rs.getString("qa25"));
        assertEquals("8.0.26", this.rs.getString(27));
        assertFalse(this.rs.next());

        testConn.close();
    }

    /**
     * Tests all supported query attributes types when used in server prepared statements.
     * 
     * @throws Exception
     */
    @Test
    public void queryAttributesTypesInServerPreparedStatement() throws Exception {
        long testInstInMilli = 801216026987l; // Tuesday, May 23, 1995 08:00:26.987654 GMT
        long testInstInSecs = 801216026;
        int testInstHour = 8;
        int testInstMin = 0;
        int testInstSec = 26;
        int testInstNano = 987654321;
        int testOffset = 2;
        String testZoneId = "UTC+2";
        String testTimezone = "Europe/Stockholm";
        Calendar testCal = Calendar.getInstance(TimeZone.getTimeZone(testTimezone));
        testCal.setTimeInMillis(testInstInMilli);
        List<String> testList = Arrays.asList("MySQL", "Connector/J");

        String expectedLocalTime = new SimpleDateFormat("HH:mm:ss.SSS000").format(new Date(testInstInMilli));
        String expectedLocalDatetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000").format(new Date(testInstInMilli))
                + new SimpleDateFormat("XXX").format(new Date(testInstInMilli)).replaceAll("([+-])0", "$1").replace("Z", "+0:00");

        Properties props = new Properties();
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);

        PreparedStatement testPstmt = testConn.prepareStatement("SELECT ?, " + IntStream.range(1, 26)
                .mapToObj(i -> String.format("mysql_query_attribute_string('qa%1$02d') AS qa%1$02d", i)).collect(Collectors.joining(", ")) + ", ?");
        testPstmt.setString(1, "MySQL Connector/J");
        testPstmt.setString(2, "8.0.26");

        assertTrue(JdbcStatement.class.isInstance(testPstmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testPstmt;
        testJdbcStmt.setAttribute("qa01", null);
        testJdbcStmt.setAttribute("qa02", "Query Attributes");
        testJdbcStmt.setAttribute("qa03", false);
        testJdbcStmt.setAttribute("qa04", (byte) 42);
        testJdbcStmt.setAttribute("qa05", (short) -42);
        testJdbcStmt.setAttribute("qa06", Integer.MAX_VALUE);
        testJdbcStmt.setAttribute("qa07", Long.MAX_VALUE);
        testJdbcStmt.setAttribute("qa08", new BigInteger("351910092110"));
        testJdbcStmt.setAttribute("qa09", 2.71828182f);
        testJdbcStmt.setAttribute("qa10", 3.141592653589793d);
        testJdbcStmt.setAttribute("qa11", new BigDecimal("1.61803398874989484820"));
        testJdbcStmt.setAttribute("qa12", new java.sql.Date(testInstInMilli));
        testJdbcStmt.setAttribute("qa13", LocalDate.of(1995, 5, 23));
        testJdbcStmt.setAttribute("qa14", new Time(testInstInMilli));
        testJdbcStmt.setAttribute("qa15", LocalTime.of(testInstHour, testInstMin, testInstSec, testInstNano));
        testJdbcStmt.setAttribute("qa16", OffsetTime.of(testInstHour, testInstMin, testInstSec, testInstNano, ZoneOffset.ofHours(testOffset)));
        testJdbcStmt.setAttribute("qa17", Duration.ofDays(-2).plusHours(2).plusMinutes(20));
        testJdbcStmt.setAttribute("qa18", LocalDateTime.ofEpochSecond(testInstInSecs, testInstNano, ZoneOffset.ofHours(testOffset)));
        testJdbcStmt.setAttribute("qa19", new Timestamp(testInstInMilli));
        testJdbcStmt.setAttribute("qa20", Instant.ofEpochMilli(testInstInMilli));
        testJdbcStmt.setAttribute("qa21", OffsetDateTime.ofInstant(Instant.ofEpochMilli(testInstInMilli), ZoneId.of(testZoneId)));
        testJdbcStmt.setAttribute("qa22", ZonedDateTime.ofInstant(Instant.ofEpochMilli(testInstInMilli), ZoneId.of(testTimezone)));
        testJdbcStmt.setAttribute("qa23", new Date(testInstInMilli));
        testJdbcStmt.setAttribute("qa24", testCal);
        testJdbcStmt.setAttribute("qa25", testList);

        this.rs = testPstmt.executeQuery();
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertNull(this.rs.getString("qa01"));
        assertTrue(this.rs.wasNull());
        assertEquals("Query Attributes", this.rs.getString("qa02"));
        assertEquals("0", this.rs.getString("qa03"));
        assertEquals("42", this.rs.getString("qa04"));
        assertEquals("-42", this.rs.getString("qa05"));
        assertEquals("2147483647", this.rs.getString("qa06"));
        assertEquals("9223372036854775807", this.rs.getString("qa07"));
        assertEquals("351910092110", this.rs.getString("qa08"));
        assertTrue(this.rs.getString("qa09").startsWith("2.71828"));
        assertTrue(this.rs.getString("qa10").startsWith("3.14159"));
        assertTrue(this.rs.getString("qa11").startsWith("1.61803"));
        assertEquals("1995-05-23", this.rs.getString("qa12"));
        assertEquals("1995-05-23", this.rs.getString("qa13"));
        assertEquals(expectedLocalTime, this.rs.getString("qa14"));
        assertEquals("08:00:26.987654", this.rs.getString("qa15"));
        assertEquals("08:00:26.987654", this.rs.getString("qa16"));
        assertEquals("-45:40:00.000000", this.rs.getString("qa17"));
        assertEquals("1995-05-23 10:00:26.987654", this.rs.getString("qa18"));
        assertEquals(expectedLocalDatetime, this.rs.getString("qa19"));
        assertEquals("1995-05-23 08:00:26.987000+0:00", this.rs.getString("qa20"));
        assertEquals("1995-05-23 10:00:26.987000+2:00", this.rs.getString("qa21"));
        assertEquals("1995-05-23 10:00:26.987000+2:00", this.rs.getString("qa22"));
        assertEquals(expectedLocalDatetime, this.rs.getString("qa23"));
        assertEquals("1995-05-23 10:00:26.987000+2:00", this.rs.getString("qa24"));
        assertEquals("[MySQL, Connector/J]", this.rs.getString("qa25"));
        assertEquals("8.0.26", this.rs.getString(27));
        assertFalse(this.rs.next());

        testConn.close();
    }

    /**
     * Tests all supported query attributes types when used in callable statements.
     * 
     * @throws Exception
     */
    @Test
    public void queryAttributesTypesInCallableStatement() throws Exception {
        long testInstInMilli = 801216026987l; // Tuesday, May 23, 1995 08:00:26.987654 GMT
        long testInstInSecs = 801216026;
        int testInstHour = 8;
        int testInstMin = 0;
        int testInstSec = 26;
        int testInstNano = 987654321;
        int testOffset = 2;
        String testZoneId = "UTC+2";
        String testTimezone = "Europe/Stockholm";
        Calendar testCal = Calendar.getInstance(TimeZone.getTimeZone(testTimezone));
        testCal.setTimeInMillis(testInstInMilli);
        List<String> testList = Arrays.asList("MySQL", "Connector/J");

        String expectedLocalTime = new SimpleDateFormat("HH:mm:ss.SSS000").format(new Date(testInstInMilli));
        String expectedLocalDatetime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS000").format(new Date(testInstInMilli))
                + new SimpleDateFormat("XXX").format(new Date(testInstInMilli)).replaceAll("([+-])0", "$1").replace("Z", "+0:00");

        createProcedure("testQueryAttrTypes",
                "(IN p1 VARCHAR(100), IN p2 VARCHAR(100)) BEGIN SELECT p1, " + IntStream.range(1, 26)
                        .mapToObj(i -> String.format("mysql_query_attribute_string('qa%1$02d') AS qa%1$02d", i)).collect(Collectors.joining(", "))
                        + ", p2; END");

        PreparedStatement testCstmt = this.conn.prepareCall("{ CALL testQueryAttrTypes(?, ?) }");
        testCstmt.setString(1, "MySQL Connector/J");
        testCstmt.setString(2, "8.0.26");

        assertTrue(JdbcStatement.class.isInstance(testCstmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testCstmt;
        testJdbcStmt.setAttribute("qa01", null);
        testJdbcStmt.setAttribute("qa02", "Query Attributes");
        testJdbcStmt.setAttribute("qa03", false);
        testJdbcStmt.setAttribute("qa04", (byte) 42);
        testJdbcStmt.setAttribute("qa05", (short) -42);
        testJdbcStmt.setAttribute("qa06", Integer.MAX_VALUE);
        testJdbcStmt.setAttribute("qa07", Long.MAX_VALUE);
        testJdbcStmt.setAttribute("qa08", new BigInteger("351910092110"));
        testJdbcStmt.setAttribute("qa09", 2.71828182f);
        testJdbcStmt.setAttribute("qa10", 3.141592653589793d);
        testJdbcStmt.setAttribute("qa11", new BigDecimal("1.61803398874989484820"));
        testJdbcStmt.setAttribute("qa12", new java.sql.Date(testInstInMilli));
        testJdbcStmt.setAttribute("qa13", LocalDate.of(1995, 5, 23));
        testJdbcStmt.setAttribute("qa14", new Time(testInstInMilli));
        testJdbcStmt.setAttribute("qa15", LocalTime.of(testInstHour, testInstMin, testInstSec, testInstNano));
        testJdbcStmt.setAttribute("qa16", OffsetTime.of(testInstHour, testInstMin, testInstSec, testInstNano, ZoneOffset.ofHours(testOffset)));
        testJdbcStmt.setAttribute("qa17", Duration.ofDays(-2).plusHours(2).plusMinutes(20));
        testJdbcStmt.setAttribute("qa18", LocalDateTime.ofEpochSecond(testInstInSecs, testInstNano, ZoneOffset.ofHours(testOffset)));
        testJdbcStmt.setAttribute("qa19", new Timestamp(testInstInMilli));
        testJdbcStmt.setAttribute("qa20", Instant.ofEpochMilli(testInstInMilli));
        testJdbcStmt.setAttribute("qa21", OffsetDateTime.ofInstant(Instant.ofEpochMilli(testInstInMilli), ZoneId.of(testZoneId)));
        testJdbcStmt.setAttribute("qa22", ZonedDateTime.ofInstant(Instant.ofEpochMilli(testInstInMilli), ZoneId.of(testTimezone)));
        testJdbcStmt.setAttribute("qa23", new Date(testInstInMilli));
        testJdbcStmt.setAttribute("qa24", testCal);
        testJdbcStmt.setAttribute("qa25", testList);

        this.rs = testCstmt.executeQuery();
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertNull(this.rs.getString("qa01"));
        assertTrue(this.rs.wasNull());
        assertEquals("Query Attributes", this.rs.getString("qa02"));
        assertEquals("0", this.rs.getString("qa03"));
        assertEquals("42", this.rs.getString("qa04"));
        assertEquals("-42", this.rs.getString("qa05"));
        assertEquals("2147483647", this.rs.getString("qa06"));
        assertEquals("9223372036854775807", this.rs.getString("qa07"));
        assertEquals("351910092110", this.rs.getString("qa08"));
        assertTrue(this.rs.getString("qa09").startsWith("2.71828"));
        assertTrue(this.rs.getString("qa10").startsWith("3.14159"));
        assertTrue(this.rs.getString("qa11").startsWith("1.61803"));
        assertEquals("1995-05-23", this.rs.getString("qa12"));
        assertEquals("1995-05-23", this.rs.getString("qa13"));
        assertEquals(expectedLocalTime, this.rs.getString("qa14"));
        assertEquals("08:00:26.987654", this.rs.getString("qa15"));
        assertEquals("08:00:26.987654", this.rs.getString("qa16"));
        assertEquals("-45:40:00.000000", this.rs.getString("qa17"));
        assertEquals("1995-05-23 10:00:26.987654", this.rs.getString("qa18"));
        assertEquals(expectedLocalDatetime, this.rs.getString("qa19"));
        assertEquals("1995-05-23 08:00:26.987000+0:00", this.rs.getString("qa20"));
        assertEquals("1995-05-23 10:00:26.987000+2:00", this.rs.getString("qa21"));
        assertEquals("1995-05-23 10:00:26.987000+2:00", this.rs.getString("qa22"));
        assertEquals(expectedLocalDatetime, this.rs.getString("qa23"));
        assertEquals("1995-05-23 10:00:26.987000+2:00", this.rs.getString("qa24"));
        assertEquals("[MySQL, Connector/J]", this.rs.getString("qa25"));
        assertEquals("8.0.26", this.rs.getString(27));
        assertFalse(this.rs.next());
    }

    /**
     * Tests if query attributes are preserved between plain statement executions and cleared after calling the 'clearAttributes' method.
     * 
     * @throws Exception
     */
    @Test
    public void preserveAndClearAttributesInPlainStatement() throws Exception {
        Statement testStmt = this.conn.createStatement();

        assertTrue(JdbcStatement.class.isInstance(testStmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testStmt;
        testJdbcStmt.setAttribute("qa", "8.0.26");

        for (int c = 0; c < 2; c++) {
            this.rs = testStmt.executeQuery("SELECT 'MySQL Connector/J', mysql_query_attribute_string('qa') AS qa");
            assertTrue(this.rs.next());
            assertEquals("MySQL Connector/J", this.rs.getString(1));
            assertEquals("8.0.26", this.rs.getString("qa"));
            assertFalse(this.rs.next());
        } // Execute twice. Query Attributes must be preserved.

        testJdbcStmt.clearAttributes();

        this.rs = testStmt.executeQuery("SELECT 'MySQL Connector/J', mysql_query_attribute_string('qa') AS qa");
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertNull(this.rs.getString("qa"));
        assertTrue(this.rs.wasNull());
        assertFalse(this.rs.next());
    }

    /**
     * Tests if query attributes are preserved between client prepared statement executions and cleared after calling the 'clearAttributes' method.
     * 
     * @throws Exception
     */
    @Test
    public void preserveAndClearAttributesInClientPreparedStatement() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false");
        Connection testConn = getConnectionWithProps(props);

        PreparedStatement testPstmt = testConn.prepareStatement("SELECT ?, mysql_query_attribute_string('qa') AS qa");
        testPstmt.setString(1, "MySQL Connector/J");

        assertTrue(JdbcStatement.class.isInstance(testPstmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testPstmt;
        testJdbcStmt.setAttribute("qa", "8.0.26");

        for (int c = 0; c < 2; c++) {
            this.rs = testPstmt.executeQuery();
            assertTrue(this.rs.next());
            assertEquals("MySQL Connector/J", this.rs.getString(1));
            assertEquals("8.0.26", this.rs.getString("qa"));
            assertFalse(this.rs.next());
        } // Execute twice. Query Attributes must be preserved.

        testJdbcStmt.clearAttributes();

        this.rs = testPstmt.executeQuery();
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertNull(this.rs.getString("qa"));
        assertTrue(this.rs.wasNull());
        assertFalse(this.rs.next());

        testConn.close();
    }

    /**
     * Tests if query attributes are preserved between server prepared statement executions and cleared after calling the 'clearAttributes' method.
     * 
     * @throws Exception
     */
    @Test
    public void preserveAndClearAttributesInServerPreparedStatement() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);

        PreparedStatement testPstmt = testConn.prepareStatement("SELECT ?, mysql_query_attribute_string('qa') AS qa");
        testPstmt.setString(1, "MySQL Connector/J");

        assertTrue(JdbcStatement.class.isInstance(testPstmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testPstmt;
        testJdbcStmt.setAttribute("qa", "8.0.26");

        for (int c = 0; c < 2; c++) {
            this.rs = testPstmt.executeQuery();
            assertTrue(this.rs.next());
            assertEquals("MySQL Connector/J", this.rs.getString(1));
            assertEquals("8.0.26", this.rs.getString("qa"));
            assertFalse(this.rs.next());
        } // Execute twice. Query Attributes must be preserved.

        testJdbcStmt.clearAttributes();

        this.rs = testPstmt.executeQuery();
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertNull(this.rs.getString("qa"));
        assertTrue(this.rs.wasNull());
        assertFalse(this.rs.next());

        testConn.close();
    }

    /**
     * Tests if query attributes are preserved between callable statement executions and cleared after calling the 'clearAttributes' method.
     * 
     * @throws Exception
     */
    @Test
    public void preserveAndClearAttributesInCallableStatement() throws Exception {
        createProcedure("testQueryAttrPreserveAndClear", "(IN p1 VARCHAR(100)) BEGIN SELECT p1, mysql_query_attribute_string('qa') AS qa; END");

        PreparedStatement testCstmt = this.conn.prepareCall("{ CALL testQueryAttrPreserveAndClear(?) }");
        testCstmt.setString(1, "MySQL Connector/J");

        assertTrue(JdbcStatement.class.isInstance(testCstmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testCstmt;
        testJdbcStmt.setAttribute("qa", "8.0.26");

        for (int c = 0; c < 2; c++) {
            this.rs = testCstmt.executeQuery();
            assertTrue(this.rs.next());
            assertEquals("MySQL Connector/J", this.rs.getString(1));
            assertEquals("8.0.26", this.rs.getString("qa"));
            assertFalse(this.rs.next());
        } // Execute twice. Query Attributes must be preserved.

        testJdbcStmt.clearAttributes();

        this.rs = testCstmt.executeQuery();
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertNull(this.rs.getString("qa"));
        assertTrue(this.rs.wasNull());
        assertFalse(this.rs.next());
    }

    /**
     * Tests if query attributes hold in plain statements with multi-queries.
     * 
     * @throws Exception
     */
    @Test
    public void multiQueriesWithAttributesInPlainStatement() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);

        Statement testStmt = testConn.createStatement();

        assertTrue(JdbcStatement.class.isInstance(testStmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testStmt;
        testJdbcStmt.setAttribute("qa01", "MySQL Connector/J");
        testJdbcStmt.setAttribute("qa02", "8.0.26");

        this.rs = testStmt.executeQuery("SELECT mysql_query_attribute_string('qa01') AS qa01; SELECT mysql_query_attribute_string('qa02') AS qa02;");
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString("qa01"));
        assertFalse(this.rs.next());

        assertTrue(testStmt.getMoreResults());
        this.rs = testStmt.getResultSet();
        assertTrue(this.rs.next());
        assertEquals("8.0.26", this.rs.getString("qa02"));
        assertFalse(this.rs.next());

        testConn.close();
    }

    /**
     * Tests if query attributes hold in prepared statements with multi-queries.
     * 
     * @throws Exception
     */
    @Test
    public void multiQueriesWithAttributesInPreparedStatement() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true"); // Will fall-back to client prepared statement, anyway.
        props.setProperty(PropertyKey.allowMultiQueries.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);

        PreparedStatement testPstmt = testConn
                .prepareStatement("SELECT mysql_query_attribute_string('qa01') AS qa01; SELECT mysql_query_attribute_string('qa02') AS qa02;");

        assertTrue(JdbcStatement.class.isInstance(testPstmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testPstmt;
        testJdbcStmt.setAttribute("qa01", "MySQL Connector/J");
        testJdbcStmt.setAttribute("qa02", "8.0.26");

        this.rs = testPstmt.executeQuery();
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString("qa01"));
        assertFalse(this.rs.next());

        assertTrue(testPstmt.getMoreResults());
        this.rs = testPstmt.getResultSet();
        assertTrue(this.rs.next());
        assertEquals("8.0.26", this.rs.getString("qa02"));
        assertFalse(this.rs.next());

        testConn.close();
    }

    /**
     * Tests whether the query attributes are propagated to the internally created statement on query rewrites in plain statements.
     * 
     * @throws Exception
     */
    @Test
    void rewriteQueriesWithAttributesInPlainStatement() throws Exception {
        createTable("testRewritePlainStmt", "(c1 VARCHAR(100), c2 VARCHAR(100))");

        Properties props = new Properties();
        props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);

        Statement testStmt = testConn.createStatement();

        assertTrue(JdbcStatement.class.isInstance(testStmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testStmt;
        testJdbcStmt.setAttribute("qa", "MySQL Connector/J");

        testStmt.addBatch("INSERT INTO testRewritePlainStmt VALUES ('Row 1', mysql_query_attribute_string('qa'))");
        testStmt.addBatch("INSERT INTO testRewritePlainStmt VALUES ('Row 2', mysql_query_attribute_string('qa'))");
        testStmt.addBatch("INSERT INTO testRewritePlainStmt VALUES ('Row 3', mysql_query_attribute_string('qa'))");
        testStmt.addBatch("INSERT INTO testRewritePlainStmt VALUES ('Row 4', mysql_query_attribute_string('qa'))");
        testStmt.addBatch("INSERT INTO testRewritePlainStmt VALUES ('Row 5', mysql_query_attribute_string('qa'))");
        testStmt.executeBatch(); // Need 5 to rewrite as multi-queries.

        this.rs = this.stmt.executeQuery("SELECT * FROM testRewritePlainStmt");
        assertTrue(this.rs.next());
        assertEquals("Row 1", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertTrue(this.rs.next());
        assertEquals("Row 2", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertTrue(this.rs.next());
        assertEquals("Row 3", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertTrue(this.rs.next());
        assertEquals("Row 4", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertTrue(this.rs.next());
        assertEquals("Row 5", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertFalse(this.rs.next());

        testConn.close();
    }

    /**
     * Tests whether the query attributes are propagated to the internally created statement on query rewrites in client prepared statements.
     * 
     * @throws Exception
     */
    @Test
    void rewriteQueriesWithAttributesInClientPreparedStatement() throws Exception {
        createTable("testRewriteClientPstmt", "(c1 VARCHAR(100), c2 VARCHAR(100))");

        Properties props = new Properties();
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "false");
        props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);

        PreparedStatement testPstmt = testConn.prepareStatement("INSERT INTO testRewriteClientPstmt VALUES (?, mysql_query_attribute_string('qa'))");

        assertTrue(JdbcStatement.class.isInstance(testPstmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testPstmt;
        testJdbcStmt.setAttribute("qa", "MySQL Connector/J");

        testPstmt.setString(1, "Row 1");
        testPstmt.addBatch();
        testPstmt.setString(1, "Row 2");
        testPstmt.addBatch();
        testPstmt.setString(1, "Row 3");
        testPstmt.addBatch();
        testPstmt.setString(1, "Row 4");
        testPstmt.addBatch();
        testPstmt.setString(1, "Row 5");
        testPstmt.addBatch();
        testPstmt.executeBatch();

        this.rs = this.stmt.executeQuery("SELECT * FROM testRewriteClientPstmt");
        assertTrue(this.rs.next());
        assertEquals("Row 1", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertTrue(this.rs.next());
        assertEquals("Row 2", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertTrue(this.rs.next());
        assertEquals("Row 3", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertTrue(this.rs.next());
        assertEquals("Row 4", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertTrue(this.rs.next());
        assertEquals("Row 5", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertFalse(this.rs.next());

        testConn.close();
    }

    /**
     * Tests whether the query attributes are propagated to the internally created statement on query rewrites in server prepared statements.
     * 
     * @throws Exception
     */
    @Test
    void rewriteQueriesWithAttributesInServerPreparedStatement() throws Exception {
        createTable("testRewriteServerPstmt", "(c1 VARCHAR(100), c2 VARCHAR(100))");

        Properties props = new Properties();
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
        props.setProperty(PropertyKey.rewriteBatchedStatements.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);

        PreparedStatement testPstmt = testConn.prepareStatement("INSERT INTO testRewriteServerPstmt VALUES (?, mysql_query_attribute_string('qa'))");

        assertTrue(JdbcStatement.class.isInstance(testPstmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testPstmt;
        testJdbcStmt.setAttribute("qa", "MySQL Connector/J");

        testPstmt.setString(1, "Row 1");
        testPstmt.addBatch();
        testPstmt.setString(1, "Row 2");
        testPstmt.addBatch();
        testPstmt.setString(1, "Row 3");
        testPstmt.addBatch();
        testPstmt.setString(1, "Row 4");
        testPstmt.addBatch();
        testPstmt.setString(1, "Row 5");
        testPstmt.addBatch();
        testPstmt.executeBatch();

        this.rs = this.stmt.executeQuery("SELECT * FROM testRewriteServerPstmt");
        assertTrue(this.rs.next());
        assertEquals("Row 1", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertTrue(this.rs.next());
        assertEquals("Row 2", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertTrue(this.rs.next());
        assertEquals("Row 3", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertTrue(this.rs.next());
        assertEquals("Row 4", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertTrue(this.rs.next());
        assertEquals("Row 5", this.rs.getString(1));
        assertEquals("MySQL Connector/J", this.rs.getString(2));
        assertFalse(this.rs.next());

        testConn.close();
    }

    /**
     * Tests if server prepared statements get their query attributes cleared automatically when cached.
     * 
     * @throws Exception
     */
    @Test
    void cachedServerPreparedStatementsWithQueryAttributes() throws Exception {
        Properties props = new Properties();
        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "true");
        props.setProperty(PropertyKey.cachePrepStmts.getKeyName(), "true");
        Connection testConn = getConnectionWithProps(props);

        PreparedStatement testPstmt1 = testConn.prepareStatement("SELECT ?, mysql_query_attribute_string('qa')");
        testPstmt1.setString(1, "Param 1");

        assertTrue(JdbcStatement.class.isInstance(testPstmt1));
        JdbcStatement testJdbcStmt1 = (JdbcStatement) testPstmt1;
        testJdbcStmt1.setAttribute("qa", "QA 1");

        this.rs = testPstmt1.executeQuery();
        assertTrue(this.rs.next());
        assertEquals("Param 1", this.rs.getString(1));
        assertEquals("QA 1", this.rs.getString(2));
        assertFalse(this.rs.next());

        testPstmt1.close();

        PreparedStatement testPstmt2 = testConn.prepareStatement("SELECT ?, mysql_query_attribute_string('qa')");
        assertSame(testPstmt1, testPstmt2);
        testPstmt2.setString(1, "Param 2");

        assertTrue(JdbcStatement.class.isInstance(testPstmt2));
        JdbcStatement testJdbcStmt2 = (JdbcStatement) testPstmt2;
        testJdbcStmt2.setAttribute("qa", "QA 2");

        this.rs = testPstmt2.executeQuery();
        assertTrue(this.rs.next());
        assertEquals("Param 2", this.rs.getString(1));
        assertEquals("QA 2", this.rs.getString(2));
        assertFalse(this.rs.next());

        testPstmt2.close();

        PreparedStatement testPstmt3 = testConn.prepareStatement("SELECT ?, mysql_query_attribute_string('qa')");
        assertSame(testPstmt1, testPstmt3);
        testPstmt3.setString(1, "Param 3");

        assertTrue(JdbcStatement.class.isInstance(testPstmt3));
        JdbcStatement testJdbcStmt3 = (JdbcStatement) testPstmt3;
        testJdbcStmt3.setAttribute("qa_new", "QA 3"); // Don't set or set different query attribute.

        this.rs = testPstmt3.executeQuery();
        assertTrue(this.rs.next());
        assertEquals("Param 3", this.rs.getString(1));
        assertNull(this.rs.getString(2));
        assertTrue(this.rs.wasNull());
        assertFalse(this.rs.next());

        testPstmt3.close();

        testConn.close();
    }

    /**
     * Tests whether proxied plain statement objects created in multi-host connections handle query attributes correctly.
     * 
     * @throws Exception
     */
    @Test
    void plainStatementWithQueryAttributesInMultiHost() throws Exception {
        // Failover connection.
        Connection testConn = getFailoverConnection();

        Statement testStmt = testConn.createStatement();

        assertTrue(JdbcStatement.class.isInstance(testStmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testStmt;
        testJdbcStmt.setAttribute("qa", "MySQL Connector/J");

        this.rs = testStmt.executeQuery("SELECT mysql_query_attribute_string('qa')");
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertFalse(this.rs.next());

        testConn.close();

        // Loadbalanced connection.
        testConn = getLoadBalancedConnection();

        testStmt = testConn.createStatement();

        assertTrue(JdbcStatement.class.isInstance(testStmt));
        testJdbcStmt = (JdbcStatement) testStmt;
        testJdbcStmt.setAttribute("qa", "MySQL Connector/J");

        this.rs = testStmt.executeQuery("SELECT mysql_query_attribute_string('qa')");
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertFalse(this.rs.next());

        testConn.close();

        // Replication connection.
        testConn = getSourceReplicaReplicationConnection();

        testStmt = testConn.createStatement();

        assertTrue(JdbcStatement.class.isInstance(testStmt));
        testJdbcStmt = (JdbcStatement) testStmt;
        testJdbcStmt.setAttribute("qa", "MySQL Connector/J");

        this.rs = testStmt.executeQuery("SELECT mysql_query_attribute_string('qa')");
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertFalse(this.rs.next());

        testConn.close();
    }

    /**
     * Tests whether proxied server prepared statement objects created in multi-host connections handle query attributes correctly.
     * 
     * @throws Exception
     */
    @Test
    void serverPreparedStatementWithQueryAttributesInMultiHost() throws Exception {
        Properties props = new Properties();
        props.setProperty("useServerPrepStmts", "true");

        // Failover connection.
        Connection testConn = getFailoverConnection(props);

        PreparedStatement testPstmt = testConn.prepareStatement("SELECT mysql_query_attribute_string('qa')");

        assertTrue(JdbcStatement.class.isInstance(testPstmt));
        JdbcStatement testJdbcStmt = (JdbcStatement) testPstmt;
        testJdbcStmt.setAttribute("qa", "MySQL Connector/J");

        this.rs = testPstmt.executeQuery();
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertFalse(this.rs.next());

        testConn.close();

        // Loadbalanced connection.
        testConn = getLoadBalancedConnection(props);

        testPstmt = testConn.prepareStatement("SELECT mysql_query_attribute_string('qa')");

        assertTrue(JdbcStatement.class.isInstance(testPstmt));
        testJdbcStmt = (JdbcStatement) testPstmt;
        testJdbcStmt.setAttribute("qa", "MySQL Connector/J");

        this.rs = testPstmt.executeQuery();
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertFalse(this.rs.next());

        testConn.close();

        // Replication connection.
        testConn = getSourceReplicaReplicationConnection(props);

        testPstmt = testConn.prepareStatement("SELECT mysql_query_attribute_string('qa')");

        assertTrue(JdbcStatement.class.isInstance(testPstmt));
        testJdbcStmt = (JdbcStatement) testPstmt;
        testJdbcStmt.setAttribute("qa", "MySQL Connector/J");

        this.rs = testPstmt.executeQuery();
        assertTrue(this.rs.next());
        assertEquals("MySQL Connector/J", this.rs.getString(1));
        assertFalse(this.rs.next());

        testConn.close();
    }
}
