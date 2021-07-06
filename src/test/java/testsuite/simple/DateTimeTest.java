/*
 * Copyright (c) 2020, 2021, Oracle and/or its affiliates. All rights reserved.
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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
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
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Hashtable;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Callable;

import org.junit.jupiter.api.Test;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlConnection;
import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.util.TimeUtil;

import testsuite.BaseTestCase;

public class DateTimeTest extends BaseTestCase {

    enum UseMethod {
        setObject, setDate, setTime, setTimestamp, getTimestamp, getObject
    };

    private static String tYear = "testSetObjectYear";
    private static String tDate = "testSetObjectDate";
    private static String tTime = "testSetObjectTime";
    private static String tDatetime = "testSetObjectDatetime";
    private static String tTimestamp = "testSetObjectTimestamp";
    private static String tVarchar = "testSetObjectVarchar";

    private static TimeZone tz_minus_10_00 = TimeZone.getTimeZone("GMT-10:00");
    //private static TimeZone tz_plus_01_00 = TimeZone.getTimeZone("Europe/Berlin");
    private static TimeZone tz_plus_02_00 = TimeZone.getTimeZone("Europe/Helsinki");
    private static TimeZone tz_plus_05_00 = TimeZone.getTimeZone("GMT+05:00");
    private static TimeZone tz_UTC = TimeZone.getTimeZone("UTC");

    TimeZone[] senderTimeZones = new TimeZone[] { tz_minus_10_00, tz_plus_05_00 }; //, tz_UTC, tz_plus_01_00
    String[] connectionTimeZones = new String[] { null, "LOCAL", "SERVER", "GMT+04:00" };

    private static LocalDate ld_19700101 = LocalDate.of(1970, 1, 1);
    private static LocalDate ld_20191231 = LocalDate.of(2019, 12, 31);
    private static LocalDate ld_20200101 = LocalDate.of(2020, 1, 1);

    private static LocalTime lt_120000 = LocalTime.of(12, 0, 0);
    private static LocalTime lt_120000_123456 = LocalTime.of(12, 0, 0, 123456000);
    private static LocalTime lt_000000 = LocalTime.of(0, 0, 0);

    private static LocalDateTime ldt_20191231_0000 = LocalDateTime.of(2019, 12, 31, 0, 0);
    private static LocalDateTime ldt_20200101_0000 = LocalDateTime.of(2020, 1, 1, 0, 0);
    private static LocalDateTime ldt_20200101_120000_123456 = LocalDateTime.of(2020, 1, 1, 12, 00, 00, 123456000);

    private static LocalDateTime ldt_19700101_0000 = LocalDateTime.of(1970, 1, 1, 0, 0);
    private static LocalDateTime ldt_19700101_120000_123456 = LocalDateTime.of(1970, 1, 1, 12, 0, 0, 123456000);
    private static LocalDateTime ldt_19700101_020000_123000 = LocalDateTime.of(1970, 1, 1, 2, 0, 0, 123000000);
    private static LocalDateTime ldt_19700101_020000_123456 = LocalDateTime.of(1970, 1, 1, 2, 0, 0, 123456000);

    private static String s_2020 = "2020";
    private static String s_20200101 = "2020-01-01";
    private static String s_20191231 = "2019-12-31";

    private static String s_1970 = "1970";
    private static String s_19700101 = "1970-01-01";

    private static String s_120000 = "12:00:00";
    private static String s_120000_123456 = "12:00:00.123456";

    private static String s_000000 = "00:00:00";
    private static String s_000000_000000 = "00:00:00.000000";

    private static String dataTruncatedErr = "Data truncated for column 'd' at row 1";
    private static String incorrectDateErr = "Data truncation: Incorrect date value: 'X' for column 'd' at row 1";
    private static String incorrectTimeErr = "Data truncation: Incorrect time value: 'X' for column 'd' at row 1";
    private static String incorrectDatetimeErr = "Data truncation: Incorrect datetime value: 'X' for column 'd' at row 1";

    private static DateTimeFormatter YEAR_FORMATTER = DateTimeFormatter.ofPattern("yyyy");
    private static DateTimeFormatter TIME_FORMATTER_WITH_MILLIS_NO_OFFCET = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static DateTimeFormatter TIME_FORMATTER_WITH_MICROS_NO_OFFCET = DateTimeFormatter.ofPattern("HH:mm:ss.SSSSSS");
    private static DateTimeFormatter DATETIME_FORMATTER_WITH_MICROS_NO_OFFCET = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private Hashtable<String, Connection> tzConnections = new Hashtable<>();
    private Hashtable<String, Connection> utcConnections = new Hashtable<>();

    private static int id = 0;

    private String getKey(Properties props) {
        StringBuilder sb = new StringBuilder();
        sb.append(props.getProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName()));
        sb.append(props.getProperty(PropertyKey.preserveInstants.getKeyName()));
        sb.append(props.getProperty(PropertyKey.useServerPrepStmts.getKeyName()));
        sb.append(props.getProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName()));
        sb.append(props.getProperty(PropertyKey.sendFractionalSeconds.getKeyName()));
        return sb.toString();
    }

    private void initConnections(TimeZone senderTz, String connectionTZ) throws Exception {

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");

        // applying 8.0 defaults to old servers
        String sqlMode = getMysqlVariable("sql_mode");
        if (!sqlMode.contains("NO_ZERO_DATE")) {
            sqlMode += ",NO_ZERO_DATE";
        }
        if (!sqlMode.contains("NO_ZERO_IN_DATE")) {
            sqlMode += ",NO_ZERO_IN_DATE";
        }
        props.setProperty(PropertyKey.sessionVariables.getKeyName(), "sql_mode='" + sqlMode + "'");

        for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
            for (boolean preserveInstants : new boolean[] { false, true }) {
                for (boolean useSSPS : new boolean[] { false, true }) {
                    for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                        for (boolean sendTimeFract : new boolean[] { false, true }) {

                            if (connectionTZ == null) {
                                props.remove(PropertyKey.connectionTimeZone.getKeyName());
                            } else {
                                props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                            }

                            props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                            props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                            props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                            props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                            String key = getKey(props);
                            final TimeZone origTz = TimeZone.getDefault();
                            try {
                                TimeZone.setDefault(senderTz);
                                this.tzConnections.put(key, getConnectionWithProps(timeZoneFreeDbUrl, props));

                                TimeZone.setDefault(tz_UTC);
                                this.utcConnections.put(key, getConnectionWithProps(timeZoneFreeDbUrl, props));

                            } finally {
                                TimeZone.setDefault(origTz);
                            }
                        }
                    }
                }
            }
        }
    }

    private void closeConnections() throws Exception {
        for (Connection c : this.tzConnections.values()) {
            c.close();
        }
        for (Connection c : this.utcConnections.values()) {
            c.close();
        }
        this.tzConnections.clear();
        this.utcConnections.clear();
    }

    @Test
    public void testSqlDateSetters() throws Exception {

        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        createTable(tYear, "(id INT, d YEAR)");
        createTable(tDate, "(id INT, d DATE)");
        createTable(tTime, "(id INT, d TIME)");
        createTable(tDatetime, "(id INT, d DATETIME)");
        createTable(tTimestamp, "(id INT, d TIMESTAMP)");
        createTable(tVarchar, "(id INT, d VARCHAR(32))");

        id = 0;

        Calendar cal_02 = GregorianCalendar.getInstance(tz_plus_02_00);

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(props)) {
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
        }

        for (TimeZone senderTz : this.senderTimeZones) {
            try {
                for (String connectionTZ : this.connectionTimeZones) {
                    initConnections(senderTz, connectionTZ);

                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? senderTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));
                                        TimeZone sessionTz = forceConnectionTimeZoneToSession ? connTz : serverTz;

                                        ZonedDateTime zdt_20200101_at_senderTz = ld_20200101.atStartOfDay().atZone(senderTz.toZoneId());
                                        ZonedDateTime zdt_20200101_at_calendarTz = zdt_20200101_at_senderTz.withZoneSameInstant(tz_plus_02_00.toZoneId());
                                        ZonedDateTime zdt_20200101_senderTz_to_connTz = preserveInstants
                                                && !(connectionTZ == null || "LOCAL".equals(connectionTZ))
                                                        ? zdt_20200101_at_senderTz.withZoneSameInstant(connTz.toZoneId())
                                                        : zdt_20200101_at_senderTz;

                                        final java.sql.Date sqlDate_at_senderTz = new java.sql.Date(zdt_20200101_at_senderTz.toInstant().toEpochMilli());

                                        /* Unsupported conversions */

                                        assertThrows(props, tVarchar, sqlDate_at_senderTz, MysqlType.TIME, senderTz,
                                                ".* Conversion from java.sql.Date to TIME is not supported.");
                                        assertThrows(props, tDate, sqlDate_at_senderTz, MysqlType.INT, senderTz,
                                                ".* Conversion from java.sql.Date to INT is not supported.");

                                        /* Into YEAR field */

                                        String expYear = zdt_20200101_at_senderTz.toLocalDate().getYear() + "";
                                        String expYearCal = zdt_20200101_at_calendarTz.toLocalDate().getYear() + "";
                                        String expYearTs = zdt_20200101_senderTz_to_connTz.toLocalDate().getYear() + "";

                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tYear, sqlDate_at_senderTz, null, senderTz, expYearCal, null, UseMethod.setDate, cal_02);
                                            setObjectFromTz(props, tYear, sqlDate_at_senderTz, null, senderTz, expYear, null, UseMethod.setDate);
                                            setObjectFromTz(props, tYear, sqlDate_at_senderTz, null, senderTz, expYear);
                                            setObjectFromTz(props, tYear, sqlDate_at_senderTz, MysqlType.DATE, senderTz, expYear);
                                            setObjectFromTz(props, tYear, sqlDate_at_senderTz, MysqlType.DATETIME, senderTz, expYear);
                                            setObjectFromTz(props, tYear, sqlDate_at_senderTz, MysqlType.TIMESTAMP, senderTz, expYearTs);
                                        } else {
                                            assertThrows(props, tYear, sqlDate_at_senderTz, null, senderTz, null, null, UseMethod.setDate, cal_02,
                                                    dataTruncatedErr);
                                            assertThrows(props, tYear, sqlDate_at_senderTz, null, senderTz, null, null, UseMethod.setDate, dataTruncatedErr);
                                            assertThrows(props, tYear, sqlDate_at_senderTz, null, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, sqlDate_at_senderTz, MysqlType.DATE, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, sqlDate_at_senderTz, MysqlType.DATETIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, sqlDate_at_senderTz, MysqlType.TIMESTAMP, senderTz, dataTruncatedErr);
                                        }
                                        assertThrows(props, tYear, sqlDate_at_senderTz, MysqlType.CHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, sqlDate_at_senderTz, MysqlType.VARCHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, sqlDate_at_senderTz, MysqlType.TINYTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, sqlDate_at_senderTz, MysqlType.TEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, sqlDate_at_senderTz, MysqlType.MEDIUMTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, sqlDate_at_senderTz, MysqlType.LONGTEXT, senderTz, dataTruncatedErr);
                                        setObjectFromTz(props, tYear, sqlDate_at_senderTz, MysqlType.YEAR, senderTz, expYear);

                                        /* Into DATE field */

                                        String expDate = zdt_20200101_at_senderTz.toLocalDate().toString();
                                        String expDateCal = zdt_20200101_at_calendarTz.toLocalDate().toString();
                                        String expDateTs = zdt_20200101_senderTz_to_connTz.toLocalDate().toString();

                                        setObjectFromTz(props, tDate, sqlDate_at_senderTz, null, senderTz, expDateCal, null, UseMethod.setDate, cal_02);
                                        setObjectFromTz(props, tDate, sqlDate_at_senderTz, null, senderTz, expDate, null, UseMethod.setDate);
                                        setObjectFromTz(props, tDate, sqlDate_at_senderTz, null, senderTz, expDate);
                                        setObjectFromTz(props, tDate, sqlDate_at_senderTz, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tDate, sqlDate_at_senderTz, MysqlType.CHAR, senderTz, expDate);
                                        setObjectFromTz(props, tDate, sqlDate_at_senderTz, MysqlType.VARCHAR, senderTz, expDate);
                                        setObjectFromTz(props, tDate, sqlDate_at_senderTz, MysqlType.TINYTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, sqlDate_at_senderTz, MysqlType.TEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, sqlDate_at_senderTz, MysqlType.MEDIUMTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, sqlDate_at_senderTz, MysqlType.LONGTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, sqlDate_at_senderTz, MysqlType.DATETIME, senderTz, expDate);
                                        setObjectFromTz(props, tDate, sqlDate_at_senderTz, MysqlType.TIMESTAMP, senderTz, expDateTs);
                                        assertThrows(props, tDate, sqlDate_at_senderTz, MysqlType.YEAR, senderTz, incorrectDateErr.replace("X", expYear));

                                        /* Into TIME field */

                                        String expTimeTs = zdt_20200101_senderTz_to_connTz.toLocalTime().toString();

                                        if (useSSPS) {
                                            setObjectFromTz(props, tTime, sqlDate_at_senderTz, null, senderTz, s_000000, null, UseMethod.setDate, cal_02);
                                            setObjectFromTz(props, tTime, sqlDate_at_senderTz, null, senderTz, s_000000, null, UseMethod.setDate);
                                            setObjectFromTz(props, tTime, sqlDate_at_senderTz, null, senderTz, s_000000);
                                            setObjectFromTz(props, tTime, sqlDate_at_senderTz, MysqlType.DATE, senderTz, s_000000);
                                        } else {
                                            assertThrows(props, tTime, sqlDate_at_senderTz, null, senderTz, null, null, UseMethod.setDate, cal_02,
                                                    incorrectTimeErr.replace("X", expDateCal));
                                            assertThrows(props, tTime, sqlDate_at_senderTz, null, senderTz, null, null, UseMethod.setDate,
                                                    incorrectTimeErr.replace("X", expDate));
                                            assertThrows(props, tTime, sqlDate_at_senderTz, null, senderTz, incorrectTimeErr.replace("X", expDate));
                                            assertThrows(props, tTime, sqlDate_at_senderTz, MysqlType.DATE, senderTz, incorrectTimeErr.replace("X", expDate));
                                        }
                                        assertThrows(props, tTime, sqlDate_at_senderTz, MysqlType.CHAR, senderTz, incorrectTimeErr.replace("X", expDate));
                                        assertThrows(props, tTime, sqlDate_at_senderTz, MysqlType.VARCHAR, senderTz, incorrectTimeErr.replace("X", expDate));
                                        assertThrows(props, tTime, sqlDate_at_senderTz, MysqlType.TINYTEXT, senderTz, incorrectTimeErr.replace("X", expDate));
                                        assertThrows(props, tTime, sqlDate_at_senderTz, MysqlType.TEXT, senderTz, incorrectTimeErr.replace("X", expDate));
                                        assertThrows(props, tTime, sqlDate_at_senderTz, MysqlType.MEDIUMTEXT, senderTz, incorrectTimeErr.replace("X", expDate));
                                        assertThrows(props, tTime, sqlDate_at_senderTz, MysqlType.LONGTEXT, senderTz, incorrectTimeErr.replace("X", expDate));
                                        setObjectFromTz(props, tTime, sqlDate_at_senderTz, MysqlType.DATETIME, senderTz, s_000000);
                                        setObjectFromTz(props, tTime, sqlDate_at_senderTz, MysqlType.TIMESTAMP, senderTz, expTimeTs);
                                        setObjectFromTz(props, tTime, sqlDate_at_senderTz, MysqlType.YEAR, senderTz, expYear); // TIME takes numbers as a short notation, thus it works here
                                        setObjectFromTz(props, tTime, sqlDate_at_senderTz, MysqlType.YEAR, senderTz,
                                                "00:" + expYear.substring(0, 2) + ":" + expYear.substring(2)); // TIME takes numbers as a short notation, thus it works here

                                        /* Into DATETIME field */

                                        String expDatetime = zdt_20200101_at_senderTz.toLocalDate().atStartOfDay()
                                                .format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expDatetimeCal = zdt_20200101_at_calendarTz.toLocalDate().atStartOfDay()
                                                .format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expDatetimeTs = zdt_20200101_senderTz_to_connTz.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET);

                                        setObjectFromTz(props, tDatetime, sqlDate_at_senderTz, null, senderTz, expDatetimeCal, null, UseMethod.setDate, cal_02);
                                        setObjectFromTz(props, tDatetime, sqlDate_at_senderTz, null, senderTz, expDatetime, null, UseMethod.setDate);
                                        setObjectFromTz(props, tDatetime, sqlDate_at_senderTz, null, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, sqlDate_at_senderTz, MysqlType.DATE, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, sqlDate_at_senderTz, MysqlType.CHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, sqlDate_at_senderTz, MysqlType.VARCHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, sqlDate_at_senderTz, MysqlType.TINYTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, sqlDate_at_senderTz, MysqlType.TEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, sqlDate_at_senderTz, MysqlType.MEDIUMTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, sqlDate_at_senderTz, MysqlType.LONGTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, sqlDate_at_senderTz, MysqlType.DATETIME, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, sqlDate_at_senderTz, MysqlType.TIMESTAMP, senderTz, expDatetimeTs);
                                        assertThrows(props, tDatetime, sqlDate_at_senderTz, MysqlType.YEAR, senderTz,
                                                incorrectDatetimeErr.replace("X", expYear));

                                        /* Into TIMESTAMP field */

                                        ZonedDateTime zdt_default_on_wire = zdt_20200101_at_senderTz.toLocalDate().atStartOfDay().atZone(sessionTz.toZoneId());
                                        ZonedDateTime zdt_calendar_on_wire = zdt_20200101_at_calendarTz.toLocalDate().atStartOfDay()
                                                .atZone(sessionTz.toZoneId());
                                        ZonedDateTime zdt_ts_on_wire = zdt_20200101_senderTz_to_connTz.withZoneSameLocal(sessionTz.toZoneId());

                                        String expTimestamp = zdt_default_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).toLocalDateTime()
                                                .format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expTimestampWithCal = zdt_calendar_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).toLocalDateTime()
                                                .format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expTimestampTs = zdt_ts_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).toLocalDateTime()
                                                .format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET);

                                        String expUnixTs = "" + zdt_default_on_wire.toInstant().getEpochSecond();
                                        String expUnixTsWithCal = "" + zdt_calendar_on_wire.toInstant().getEpochSecond();
                                        String expUnixTsTs = "" + zdt_ts_on_wire.toInstant().getEpochSecond();

                                        setObjectFromTz(props, tTimestamp, sqlDate_at_senderTz, null, senderTz, expTimestampWithCal, expUnixTsWithCal,
                                                UseMethod.setDate, cal_02);
                                        setObjectFromTz(props, tTimestamp, sqlDate_at_senderTz, null, senderTz, expTimestamp, expUnixTs, UseMethod.setDate);
                                        setObjectFromTz(props, tTimestamp, sqlDate_at_senderTz, null, senderTz, expTimestamp, expUnixTs);
                                        setObjectFromTz(props, tTimestamp, sqlDate_at_senderTz, MysqlType.DATE, senderTz, expTimestamp, expUnixTs);
                                        setObjectFromTz(props, tTimestamp, sqlDate_at_senderTz, MysqlType.CHAR, senderTz, expTimestamp, expUnixTs);
                                        setObjectFromTz(props, tTimestamp, sqlDate_at_senderTz, MysqlType.VARCHAR, senderTz, expTimestamp, expUnixTs);
                                        setObjectFromTz(props, tTimestamp, sqlDate_at_senderTz, MysqlType.TINYTEXT, senderTz, expTimestamp, expUnixTs);
                                        setObjectFromTz(props, tTimestamp, sqlDate_at_senderTz, MysqlType.TEXT, senderTz, expTimestamp, expUnixTs);
                                        setObjectFromTz(props, tTimestamp, sqlDate_at_senderTz, MysqlType.MEDIUMTEXT, senderTz, expTimestamp, expUnixTs);
                                        setObjectFromTz(props, tTimestamp, sqlDate_at_senderTz, MysqlType.LONGTEXT, senderTz, expTimestamp, expUnixTs);
                                        setObjectFromTz(props, tTimestamp, sqlDate_at_senderTz, MysqlType.DATETIME, senderTz, expTimestamp, expUnixTs);
                                        setObjectFromTz(props, tTimestamp, sqlDate_at_senderTz, MysqlType.TIMESTAMP, senderTz, expTimestampTs, expUnixTsTs);
                                        assertThrows(props, tTimestamp, sqlDate_at_senderTz, MysqlType.YEAR, senderTz,
                                                incorrectDatetimeErr.replace("X", expYear));

                                        /* Into VARCHAR field */

                                        setObjectFromTz(props, tVarchar, sqlDate_at_senderTz, null, senderTz, expDateCal, null, UseMethod.setDate, cal_02);
                                        setObjectFromTz(props, tVarchar, sqlDate_at_senderTz, null, senderTz, expDate, null, UseMethod.setDate);
                                        setObjectFromTz(props, tVarchar, sqlDate_at_senderTz, null, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, sqlDate_at_senderTz, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, sqlDate_at_senderTz, MysqlType.CHAR, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, sqlDate_at_senderTz, MysqlType.VARCHAR, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, sqlDate_at_senderTz, MysqlType.TINYTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, sqlDate_at_senderTz, MysqlType.TEXT, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, sqlDate_at_senderTz, MysqlType.MEDIUMTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, sqlDate_at_senderTz, MysqlType.LONGTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, sqlDate_at_senderTz, MysqlType.DATETIME, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, sqlDate_at_senderTz, MysqlType.TIMESTAMP, senderTz, expDatetimeTs);
                                        setObjectFromTz(props, tVarchar, sqlDate_at_senderTz, MysqlType.YEAR, senderTz, expYear);
                                    }
                                }
                            }
                        }
                    }
                    closeConnections();
                }
            } finally {
                closeConnections();
            }
        }
    }

    @Test
    public void testSqlTimeSetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        createTable(tYear, "(id INT, d YEAR)");
        createTable(tDate, "(id INT, d DATE)");
        createTable(tTime, withFract ? "(id INT, d TIME(6))" : "(id INT, d TIME)");
        createTable(tDatetime, withFract ? "(id INT, d DATETIME(6))" : "(id INT, d DATETIME)");
        createTable(tTimestamp, withFract ? "(id INT, d TIMESTAMP(6))" : "(id INT, d TIMESTAMP)");
        createTable(tVarchar, "(id INT, d VARCHAR(30))");

        id = 0;

        Calendar cal_02 = GregorianCalendar.getInstance(tz_plus_02_00);

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(props)) {
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
        }

        for (TimeZone senderTz : this.senderTimeZones) {
            try {
                for (String connectionTZ : this.connectionTimeZones) {
                    initConnections(senderTz, connectionTZ);

                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? senderTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));
                                        TimeZone sessionTz = forceConnectionTimeZoneToSession ? connTz : serverTz;

                                        ZonedDateTime zdt_19700101_120000_123_at_senderTz = (withFract ? ldt_19700101_120000_123456
                                                : ldt_19700101_120000_123456.withNano(0)).atZone(senderTz.toZoneId());
                                        ZonedDateTime zdt_19700101_120000_123_at_calendarTz = zdt_19700101_120000_123_at_senderTz
                                                .withZoneSameInstant(tz_plus_02_00.toZoneId());

                                        ZonedDateTime zdt_19700101_on_wire = (sendFractionalSeconds && sendTimeFract ? zdt_19700101_120000_123_at_senderTz
                                                : zdt_19700101_120000_123_at_senderTz.withNano(0)).withZoneSameLocal(sessionTz.toZoneId());
                                        ZonedDateTime zdt_default_on_wire = LocalDate.now(sessionTz.toZoneId())
                                                .atTime(zdt_19700101_120000_123_at_senderTz.toLocalTime()).atZone(sessionTz.toZoneId());
                                        ZonedDateTime zdt_calendar_on_wire = LocalDate.now(sessionTz.toZoneId())
                                                .atTime(zdt_19700101_120000_123_at_calendarTz.toLocalTime()).atZone(sessionTz.toZoneId());

                                        java.sql.Time sqlTime_120000 = new java.sql.Time(
                                                zdt_19700101_120000_123_at_senderTz.toInstant().getEpochSecond() * 1000);
                                        java.sql.Time sqlTime_120000_123 = new java.sql.Time(zdt_19700101_120000_123_at_senderTz.toInstant().toEpochMilli());

                                        String expYear = "" + LocalDateTime.now(sessionTz.toZoneId()).toLocalDate().getYear(); // TODO server applies the time_zone, bug?
                                        String expDate = "" + LocalDateTime.now(sessionTz.toZoneId()).toLocalDate(); // TODO server applies the time_zone, bug?

                                        String expTimeNoMs = zdt_19700101_120000_123_at_senderTz.toLocalTime()
                                                .format(TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expTimeSendTimeFract = sendFractionalSeconds && sendTimeFract
                                                ? zdt_19700101_120000_123_at_senderTz.toLocalTime()
                                                        .format(withFract ? TIME_FORMATTER_WITH_MILLIS_NO_OFFCET : TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : expTimeNoMs;

                                        String expTimeNoMsCal = zdt_19700101_120000_123_at_calendarTz.toLocalTime()
                                                .format(TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expTimeCal = sendFractionalSeconds && sendTimeFract
                                                ? zdt_19700101_120000_123_at_calendarTz.toLocalTime()
                                                        .format(withFract ? TIME_FORMATTER_WITH_MILLIS_NO_OFFCET : TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : expTimeNoMsCal;

                                        /* Into YEAR field */

                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tYear, sqlTime_120000_123, null, senderTz, expYear, null, UseMethod.setTime, cal_02);
                                            setObjectFromTz(props, tYear, sqlTime_120000_123, null, senderTz, expYear, null, UseMethod.setTime);
                                            setObjectFromTz(props, tYear, sqlTime_120000_123, null, senderTz, expYear);
                                            setObjectFromTz(props, tYear, sqlTime_120000_123, MysqlType.DATE, senderTz, s_19700101);
                                            setObjectFromTz(props, tYear, sqlTime_120000_123, MysqlType.TIME, senderTz, expYear);
                                            setObjectFromTz(props, tYear, sqlTime_120000_123, MysqlType.DATETIME, senderTz, s_19700101);
                                            setObjectFromTz(props, tYear, sqlTime_120000_123, MysqlType.TIMESTAMP, senderTz, s_19700101);
                                        } else {
                                            assertThrows(props, tYear, sqlTime_120000_123, null, senderTz, null, null, UseMethod.setTime, cal_02,
                                                    dataTruncatedErr);
                                            assertThrows(props, tYear, sqlTime_120000_123, null, senderTz, null, null, UseMethod.setTime, dataTruncatedErr);
                                            assertThrows(props, tYear, sqlTime_120000_123, null, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, sqlTime_120000_123, MysqlType.DATE, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, sqlTime_120000_123, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, sqlTime_120000_123, MysqlType.DATETIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, sqlTime_120000_123, MysqlType.TIMESTAMP, senderTz, dataTruncatedErr);
                                        }
                                        assertThrows(props, tYear, sqlTime_120000_123, MysqlType.CHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, sqlTime_120000_123, MysqlType.VARCHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, sqlTime_120000_123, MysqlType.TINYTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, sqlTime_120000_123, MysqlType.TEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, sqlTime_120000_123, MysqlType.MEDIUMTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, sqlTime_120000_123, MysqlType.LONGTEXT, senderTz, dataTruncatedErr);
                                        setObjectFromTz(props, tYear, sqlTime_120000_123, MysqlType.YEAR, senderTz, s_1970);

                                        /* Into DATE field */

                                        String expDateErr = incorrectDateErr.replace("X", expTimeSendTimeFract);
                                        String expDateErrWithCal = incorrectDateErr.replace("X", expTimeCal);

                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDate, sqlTime_120000_123, null, senderTz, expDate, null, UseMethod.setTime, cal_02);
                                                setObjectFromTz(props, tDate, sqlTime_120000_123, null, senderTz, expDate, null, UseMethod.setTime);
                                                setObjectFromTz(props, tDate, sqlTime_120000_123, null, senderTz, expDate);
                                                setObjectFromTz(props, tDate, sqlTime_120000_123, MysqlType.TIME, senderTz, expDate);
                                            } else {
                                                assertThrows(props, tDate, sqlTime_120000_123, null, senderTz, null, null, UseMethod.setTime, cal_02,
                                                        dataTruncatedErr);
                                                assertThrows(props, tDate, sqlTime_120000_123, null, senderTz, null, null, UseMethod.setTime, dataTruncatedErr);
                                                assertThrows(props, tDate, sqlTime_120000_123, null, senderTz, dataTruncatedErr);
                                                assertThrows(props, tDate, sqlTime_120000_123, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDate, sqlTime_120000_123, null, senderTz, null, null, UseMethod.setTime, cal_02,
                                                    expDateErrWithCal);
                                            assertThrows(props, tDate, sqlTime_120000_123, null, senderTz, null, null, UseMethod.setTime, expDateErr);
                                            assertThrows(props, tDate, sqlTime_120000_123, null, senderTz, expDateErr);
                                            assertThrows(props, tDate, sqlTime_120000_123, MysqlType.TIME, senderTz, expDateErr);
                                        }
                                        assertThrows(props, tDate, sqlTime_120000_123, MysqlType.CHAR, senderTz, expDateErr);
                                        assertThrows(props, tDate, sqlTime_120000_123, MysqlType.VARCHAR, senderTz, expDateErr);
                                        assertThrows(props, tDate, sqlTime_120000_123, MysqlType.TINYTEXT, senderTz, expDateErr);
                                        assertThrows(props, tDate, sqlTime_120000_123, MysqlType.TEXT, senderTz, expDateErr);
                                        assertThrows(props, tDate, sqlTime_120000_123, MysqlType.MEDIUMTEXT, senderTz, expDateErr);
                                        assertThrows(props, tDate, sqlTime_120000_123, MysqlType.LONGTEXT, senderTz, expDateErr);

                                        setObjectFromTz(props, tDate, sqlTime_120000_123, MysqlType.DATE, senderTz, s_19700101);
                                        setObjectFromTz(props, tDate, sqlTime_120000_123, MysqlType.DATETIME, senderTz, s_19700101);
                                        setObjectFromTz(props, tDate, sqlTime_120000_123, MysqlType.TIMESTAMP, senderTz, s_19700101);
                                        assertThrows(props, tDate, sqlTime_120000_123, MysqlType.YEAR, senderTz, incorrectDateErr.replace("X", s_1970));

                                        /* Into TIME field */

                                        setObjectFromTz(props, tTime, sqlTime_120000_123, null, senderTz, expTimeCal, null, UseMethod.setTime, cal_02);
                                        setObjectFromTz(props, tTime, sqlTime_120000_123, null, senderTz, expTimeSendTimeFract, null, UseMethod.setTime);
                                        setObjectFromTz(props, tTime, sqlTime_120000_123, null, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tTime, sqlTime_120000, MysqlType.CHAR, senderTz, s_120000);
                                        setObjectFromTz(props, tTime, sqlTime_120000_123, MysqlType.CHAR, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tTime, sqlTime_120000_123, MysqlType.VARCHAR, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tTime, sqlTime_120000_123, MysqlType.TINYTEXT, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tTime, sqlTime_120000_123, MysqlType.TEXT, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tTime, sqlTime_120000_123, MysqlType.MEDIUMTEXT, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tTime, sqlTime_120000_123, MysqlType.LONGTEXT, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tTime, sqlTime_120000, MysqlType.TIME, senderTz, s_120000);
                                        setObjectFromTz(props, tTime, sqlTime_120000_123, MysqlType.TIME, senderTz, expTimeSendTimeFract);
                                        if (useSSPS) {
                                            setObjectFromTz(props, tTime, sqlTime_120000_123, MysqlType.DATE, senderTz, s_000000);
                                        } else {
                                            assertThrows(props, tTime, sqlTime_120000_123, MysqlType.DATE, senderTz, incorrectTimeErr.replace("X", s_19700101));
                                        }
                                        setObjectFromTz(props, tTime, sqlTime_120000_123, MysqlType.DATETIME, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tTime, sqlTime_120000_123, MysqlType.TIMESTAMP, senderTz, expTimeSendTimeFract);
                                        assertThrows(props, tTime, sqlTime_120000_123, MysqlType.YEAR, senderTz, incorrectTimeErr.replace("X", s_1970));

                                        /* Into DATETIME field */

                                        String expDatetime = expDate + " " + expTimeSendTimeFract;
                                        String expDatetimeWithCal = expDate + " " + expTimeCal;
                                        String expDatetimeErr = incorrectDatetimeErr.replace("X", expTimeSendTimeFract);
                                        String expDatetimeErrWithCal = incorrectDatetimeErr.replace("X", expTimeCal);

                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDatetime, sqlTime_120000_123, null, senderTz, expDatetimeWithCal, null,
                                                        UseMethod.setTime, cal_02);
                                                setObjectFromTz(props, tDatetime, sqlTime_120000_123, null, senderTz, expDatetime, null, UseMethod.setTime);
                                                setObjectFromTz(props, tDatetime, sqlTime_120000_123, null, senderTz, expDatetime);
                                                setObjectFromTz(props, tDatetime, sqlTime_120000_123, MysqlType.TIME, senderTz, expDatetime);
                                            } else {
                                                assertThrows(props, tDatetime, sqlTime_120000_123, null, senderTz, null, null, UseMethod.setTime, cal_02,
                                                        dataTruncatedErr);
                                                assertThrows(props, tDatetime, sqlTime_120000_123, null, senderTz, null, null, UseMethod.setTime,
                                                        dataTruncatedErr);
                                                assertThrows(props, tDatetime, sqlTime_120000_123, null, senderTz, dataTruncatedErr);
                                                assertThrows(props, tDatetime, sqlTime_120000_123, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDatetime, sqlTime_120000_123, null, senderTz, null, null, UseMethod.setTime, cal_02,
                                                    expDatetimeErrWithCal);
                                            assertThrows(props, tDatetime, sqlTime_120000_123, null, senderTz, null, null, UseMethod.setTime, expDatetimeErr);
                                            assertThrows(props, tDatetime, sqlTime_120000_123, null, senderTz, expDatetimeErr);
                                            assertThrows(props, tDatetime, sqlTime_120000_123, MysqlType.TIME, senderTz, expDatetimeErr);
                                        }
                                        assertThrows(props, tDatetime, sqlTime_120000_123, MysqlType.CHAR, senderTz, expDatetimeErr);
                                        assertThrows(props, tDatetime, sqlTime_120000_123, MysqlType.VARCHAR, senderTz, expDatetimeErr);
                                        assertThrows(props, tDatetime, sqlTime_120000_123, MysqlType.TINYTEXT, senderTz, expDatetimeErr);
                                        assertThrows(props, tDatetime, sqlTime_120000_123, MysqlType.TEXT, senderTz, expDatetimeErr);
                                        assertThrows(props, tDatetime, sqlTime_120000_123, MysqlType.MEDIUMTEXT, senderTz, expDatetimeErr);
                                        assertThrows(props, tDatetime, sqlTime_120000_123, MysqlType.LONGTEXT, senderTz, expDatetimeErr);

                                        setObjectFromTz(props, tDatetime, sqlTime_120000_123, MysqlType.DATE, senderTz, s_19700101);
                                        setObjectFromTz(props, tDatetime, sqlTime_120000_123, MysqlType.DATETIME, senderTz,
                                                s_19700101 + " " + expTimeSendTimeFract);
                                        setObjectFromTz(props, tDatetime, sqlTime_120000_123, MysqlType.TIMESTAMP, senderTz,
                                                s_19700101 + " " + expTimeSendTimeFract);
                                        assertThrows(props, tDatetime, sqlTime_120000_123, MysqlType.YEAR, senderTz, incorrectDatetimeErr.replace("X", s_1970));

                                        /* Into TIMESTAMP field */

                                        LocalDateTime ldt_exp1970Timestamp = zdt_19700101_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).toLocalDateTime();
                                        LocalDateTime ldt_expDefTimestamp = zdt_default_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).toLocalDateTime();
                                        LocalDateTime ldt_expCalTimestamp = zdt_calendar_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).toLocalDateTime();
                                        ZonedDateTime zdt_expDATETimestamp = ldt_19700101_0000.atZone(sessionTz.toZoneId())
                                                .withZoneSameInstant(tz_UTC.toZoneId());

                                        DateTimeFormatter df = sendFractionalSeconds && sendTimeFract
                                                ? (withFract ? TimeUtil.DATETIME_FORMATTER_WITH_MILLIS_NO_OFFSET
                                                        : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET;

                                        String exp1970Timestamp = ldt_exp1970Timestamp.format(df);
                                        String expDefTimestamp = ldt_expDefTimestamp.format(df);
                                        String expDefTimestampNoMs = ldt_expDefTimestamp.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expCalTimestamp = ldt_expCalTimestamp.format(df);
                                        String expDATETimestamp = zdt_expDATETimestamp.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET);

                                        String expDefUnixTsNoMs = "" + zdt_default_on_wire.toEpochSecond();
                                        String expDefUnixTs = expDefUnixTsNoMs + (sendFractionalSeconds && sendTimeFract && zdt_default_on_wire.getNano() > 0
                                                ? "." + TimeUtil.formatNanos(zdt_default_on_wire.getNano(), 3)
                                                : "");
                                        String expCalUnixTs = zdt_calendar_on_wire.toEpochSecond()
                                                + (sendFractionalSeconds && sendTimeFract && zdt_calendar_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_calendar_on_wire.getNano(), 3)
                                                        : "");
                                        String exp1970UnixTs = zdt_19700101_on_wire.toEpochSecond()
                                                + (sendFractionalSeconds && sendTimeFract && zdt_19700101_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_19700101_on_wire.getNano(), 3)
                                                        : "");

                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tTimestamp, sqlTime_120000_123, null, senderTz, expCalTimestamp, expCalUnixTs,
                                                    UseMethod.setTime, cal_02);
                                            setObjectFromTz(props, tTimestamp, sqlTime_120000_123, null, senderTz, expDefTimestamp, expDefUnixTs,
                                                    UseMethod.setTime);
                                            setObjectFromTz(props, tTimestamp, sqlTime_120000_123, null, senderTz, expDefTimestamp, expDefUnixTs);
                                            setObjectFromTz(props, tTimestamp, sqlTime_120000, null, senderTz, expDefTimestampNoMs, expDefUnixTsNoMs);
                                            setObjectFromTz(props, tTimestamp, sqlTime_120000_123, MysqlType.TIME, senderTz, expDefTimestamp, expDefUnixTs);
                                        } else {
                                            assertThrows(props, tTimestamp, sqlTime_120000_123, null, senderTz, expDatetimeErr);
                                            assertThrows(props, tTimestamp, sqlTime_120000, null, senderTz, incorrectDatetimeErr.replace("X", s_120000));
                                            assertThrows(props, tTimestamp, sqlTime_120000_123, MysqlType.TIME, senderTz, expDatetimeErr);
                                            assertThrows(props, tTimestamp, sqlTime_120000_123, null, senderTz, null, null, UseMethod.setTime, expDatetimeErr);
                                            assertThrows(props, tTimestamp, sqlTime_120000_123, null, senderTz, null, null, UseMethod.setTime, cal_02,
                                                    expDatetimeErrWithCal);
                                        }
                                        assertThrows(props, tTimestamp, sqlTime_120000_123, MysqlType.CHAR, senderTz, expDatetimeErr);
                                        assertThrows(props, tTimestamp, sqlTime_120000_123, MysqlType.VARCHAR, senderTz, expDatetimeErr);
                                        assertThrows(props, tTimestamp, sqlTime_120000_123, MysqlType.TINYTEXT, senderTz, expDatetimeErr);
                                        assertThrows(props, tTimestamp, sqlTime_120000_123, MysqlType.TEXT, senderTz, expDatetimeErr);
                                        assertThrows(props, tTimestamp, sqlTime_120000_123, MysqlType.MEDIUMTEXT, senderTz, expDatetimeErr);
                                        assertThrows(props, tTimestamp, sqlTime_120000_123, MysqlType.LONGTEXT, senderTz, expDatetimeErr);
                                        if (zdt_expDATETimestamp.toEpochSecond() <= 0) {
                                            assertThrows(props, tTimestamp, sqlTime_120000_123, MysqlType.DATE, senderTz,
                                                    incorrectDatetimeErr.replace("X", s_19700101));
                                        } else {
                                            setObjectFromTz(props, tTimestamp, sqlTime_120000_123, MysqlType.DATE, senderTz, expDATETimestamp,
                                                    "" + zdt_expDATETimestamp.toEpochSecond());
                                        }
                                        setObjectFromTz(props, tTimestamp, sqlTime_120000_123, MysqlType.DATETIME, senderTz, exp1970Timestamp, exp1970UnixTs);
                                        setObjectFromTz(props, tTimestamp, sqlTime_120000_123, MysqlType.TIMESTAMP, senderTz, exp1970Timestamp, exp1970UnixTs);
                                        assertThrows(props, tTimestamp, sqlTime_120000_123, MysqlType.YEAR, senderTz,
                                                incorrectDatetimeErr.replace("X", s_1970));

                                        /* Into VARCHAR field */

                                        String expTime2 = useSSPS ? expTimeNoMs : expTimeSendTimeFract; // TODO milliseconds are ignored by server. Bug ?
                                        String expTimeCal2 = useSSPS ? expTimeNoMsCal : expTimeCal; // TODO milliseconds are ignored by server. Bug ?

                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, null, senderTz, expTimeCal2, null, UseMethod.setTime, cal_02);
                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, null, senderTz, expTime2, null, UseMethod.setTime);
                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, null, senderTz, expTime2);
                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, MysqlType.TIME, senderTz, expTime2);
                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, MysqlType.CHAR, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, MysqlType.VARCHAR, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, MysqlType.TINYTEXT, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, MysqlType.TEXT, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, MysqlType.MEDIUMTEXT, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, MysqlType.LONGTEXT, senderTz, expTimeSendTimeFract);
                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, MysqlType.DATE, senderTz, s_19700101);
                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, MysqlType.DATETIME, senderTz, s_19700101 + " " + expTime2);
                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, MysqlType.TIMESTAMP, senderTz, s_19700101 + " " + expTime2);
                                        setObjectFromTz(props, tVarchar, sqlTime_120000_123, MysqlType.YEAR, senderTz, s_1970);
                                    }
                                }
                            }
                        }
                    }
                    closeConnections();
                }
            } finally {
                closeConnections();
            }
        }
    }

    @Test
    public void testSqlTimestampSetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        createTable(tYear, "(id INT, d YEAR)");
        createTable(tDate, "(id INT, d DATE)");
        createTable(tTime, withFract ? "(id INT, d TIME(6))" : "(id INT, d TIME)");
        createTable(tDatetime, withFract ? "(id INT, d DATETIME(6))" : "(id INT, d DATETIME)");
        createTable(tTimestamp, withFract ? "(id INT, d TIMESTAMP(6))" : "(id INT, d TIMESTAMP)");
        createTable(tVarchar, "(id INT, d VARCHAR(30))");

        id = 0;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(props)) {
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
        }

        Calendar cal_02 = GregorianCalendar.getInstance(tz_plus_02_00);

        for (TimeZone senderTz : this.senderTimeZones) {
            try {
                for (String connectionTZ : this.connectionTimeZones) {
                    initConnections(senderTz, connectionTZ);

                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? senderTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));

                                        TimeZone sessionTz = forceConnectionTimeZoneToSession ? connTz : serverTz;

                                        DateTimeFormatter dateTimeFmt = sendFractionalSeconds
                                                ? (withFract ? DATETIME_FORMATTER_WITH_MICROS_NO_OFFCET : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET;
                                        DateTimeFormatter timeFmtsendFractionalSeconds = sendFractionalSeconds
                                                ? (withFract ? TIME_FORMATTER_WITH_MICROS_NO_OFFCET : TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET;

                                        ZonedDateTime zdt_20200101_120000_123456_at_senderTz = (withFract ? ldt_20200101_120000_123456
                                                : ldt_20200101_120000_123456.withNano(0)).atZone(senderTz.toZoneId());
                                        ZonedDateTime zdt_20200101_120000_123456_at_calendarTz = zdt_20200101_120000_123456_at_senderTz
                                                .withZoneSameInstant(tz_plus_02_00.toZoneId());

                                        ZonedDateTime zdt_20200101_120000_123456_senderTz_to_connTz = preserveInstants
                                                && !(connectionTZ == null || "LOCAL".equals(connectionTZ))
                                                        ? zdt_20200101_120000_123456_at_senderTz.withZoneSameInstant(connTz.toZoneId())
                                                        : zdt_20200101_120000_123456_at_senderTz;

                                        java.sql.Timestamp ts = java.sql.Timestamp.from(zdt_20200101_120000_123456_at_senderTz.toInstant());

                                        ZonedDateTime zdt_20200101_120000_123456_on_wire = zdt_20200101_120000_123456_at_senderTz
                                                .withZoneSameLocal(sessionTz.toZoneId());
                                        ZonedDateTime zdt_no_date_120000_123456_on_wire = LocalDate.now(sessionTz.toZoneId())
                                                .atTime(zdt_20200101_120000_123456_at_senderTz.toLocalTime()).atZone(sessionTz.toZoneId());
                                        ZonedDateTime zdt_calendar_on_wire = zdt_20200101_120000_123456_at_calendarTz.withZoneSameLocal(sessionTz.toZoneId());
                                        ZonedDateTime zdt_20200101_no_time_on_wire = zdt_20200101_120000_123456_on_wire.withHour(0).withMinute(0).withSecond(0)
                                                .withNano(0);

                                        ZonedDateTime zdt_TS_on_wire = zdt_20200101_120000_123456_senderTz_to_connTz.withZoneSameLocal(sessionTz.toZoneId());

                                        String expYear = zdt_20200101_120000_123456_on_wire.format(YEAR_FORMATTER);
                                        String expYearDef = zdt_no_date_120000_123456_on_wire.format(YEAR_FORMATTER);
                                        String expYearCal = zdt_calendar_on_wire.format(YEAR_FORMATTER);
                                        String expYearTS = zdt_TS_on_wire.format(YEAR_FORMATTER);

                                        String expDate = zdt_20200101_120000_123456_on_wire.format(TimeUtil.DATE_FORMATTER);
                                        String expDateDef = zdt_no_date_120000_123456_on_wire
                                                .format(useSSPS ? TimeUtil.DATE_FORMATTER : DateTimeFormatter.ofPattern("20HH-mm-ss"));
                                        String expDateCal = zdt_calendar_on_wire.format(TimeUtil.DATE_FORMATTER);
                                        String expDateTS = zdt_TS_on_wire.format(TimeUtil.DATE_FORMATTER);

                                        String expTimeNoMs = zdt_20200101_120000_123456_on_wire.format(TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expTime = zdt_20200101_120000_123456_on_wire.format(timeFmtsendFractionalSeconds);
                                        String expTimeCal = zdt_calendar_on_wire.format(timeFmtsendFractionalSeconds);
                                        String expTimeTS = zdt_TS_on_wire.format(timeFmtsendFractionalSeconds);

                                        String expDatetime = zdt_20200101_120000_123456_on_wire.format(dateTimeFmt);
                                        String expDatetimeTS = zdt_TS_on_wire.format(dateTimeFmt);
                                        String expDatetimeDef = LocalDate.now(sessionTz.toZoneId()).atTime(zdt_20200101_120000_123456_on_wire.toLocalTime())
                                                .atZone(sessionTz.toZoneId())
                                                .format(useSSPS ? dateTimeFmt : DateTimeFormatter.ofPattern("20HH-mm-ss 00:00:00"));
                                        String expDatetimeCal = zdt_calendar_on_wire.format(dateTimeFmt);
                                        String expDatetimeNoTime = zdt_20200101_120000_123456_on_wire
                                                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00"));

                                        String expTimestamp = zdt_20200101_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expTimestampTS = zdt_TS_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expDefTimestamp = zdt_no_date_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expCalTimestamp = zdt_calendar_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expTimestampNoTime = zdt_20200101_no_time_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);

                                        String expDefUnixTs = zdt_no_date_120000_123456_on_wire.toEpochSecond()
                                                + (sendFractionalSeconds && zdt_no_date_120000_123456_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_no_date_120000_123456_on_wire.getNano(), 6)
                                                        : "");
                                        String expCalUnixTs = zdt_calendar_on_wire.toEpochSecond()
                                                + (sendFractionalSeconds && zdt_calendar_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_calendar_on_wire.getNano(), 6)
                                                        : "");
                                        String expFullUnixTs = zdt_20200101_120000_123456_on_wire.toEpochSecond()
                                                + (sendFractionalSeconds && zdt_20200101_120000_123456_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_20200101_120000_123456_on_wire.getNano(), 6)
                                                        : "");
                                        String expFullUnixTsTS = zdt_TS_on_wire.toEpochSecond() + (sendFractionalSeconds && zdt_TS_on_wire.getNano() > 0
                                                ? "." + TimeUtil.formatNanos(zdt_TS_on_wire.getNano(), 6)
                                                : "");
                                        String expUnixTsFromDate = zdt_20200101_no_time_on_wire.toEpochSecond() + "";

                                        /* Into YEAR field */

                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tYear, ts, null, senderTz, expYearCal, null, UseMethod.setTimestamp, cal_02);
                                            setObjectFromTz(props, tYear, ts, null, senderTz, expYear, null, UseMethod.setTimestamp);
                                            setObjectFromTz(props, tYear, ts, null, senderTz, expYear);
                                            setObjectFromTz(props, tYear, ts, MysqlType.DATE, senderTz, expYear);
                                            setObjectFromTz(props, tYear, ts, MysqlType.TIME, senderTz, expYearDef);
                                            setObjectFromTz(props, tYear, ts, MysqlType.DATETIME, senderTz, expYear);
                                            setObjectFromTz(props, tYear, ts, MysqlType.TIMESTAMP, senderTz, expYearTS);
                                        } else {
                                            assertThrows(props, tYear, ts, null, senderTz, null, null, UseMethod.setTimestamp, cal_02, dataTruncatedErr);
                                            assertThrows(props, tYear, ts, null, senderTz, null, null, UseMethod.setTimestamp, dataTruncatedErr);
                                            assertThrows(props, tYear, ts, null, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, ts, MysqlType.DATE, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, ts, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, ts, MysqlType.DATETIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, ts, MysqlType.TIMESTAMP, senderTz, dataTruncatedErr);
                                        }
                                        assertThrows(props, tYear, ts, MysqlType.CHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ts, MysqlType.VARCHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ts, MysqlType.TINYTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ts, MysqlType.TEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ts, MysqlType.MEDIUMTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ts, MysqlType.LONGTEXT, senderTz, dataTruncatedErr);
                                        setObjectFromTz(props, tYear, ts, MysqlType.YEAR, senderTz, expYear);

                                        /* Into DATE field */

                                        setObjectFromTz(props, tDate, ts, null, senderTz, expDateCal, null, UseMethod.setTimestamp, cal_02);
                                        setObjectFromTz(props, tDate, ts, null, senderTz, expDateTS, null, UseMethod.setTimestamp);
                                        setObjectFromTz(props, tDate, ts, null, senderTz, expDateTS);
                                        setObjectFromTz(props, tDate, ts, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ts, MysqlType.CHAR, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ts, MysqlType.VARCHAR, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ts, MysqlType.TINYTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ts, MysqlType.TEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ts, MysqlType.MEDIUMTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ts, MysqlType.LONGTEXT, senderTz, expDate);
                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDate, ts, MysqlType.TIME, senderTz, expDateDef);
                                            } else {
                                                assertThrows(props, tDate, ts, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDate, ts, MysqlType.TIME, senderTz, incorrectDateErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tDate, ts, MysqlType.DATETIME, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ts, MysqlType.TIMESTAMP, senderTz, expDateTS);
                                        assertThrows(props, tDate, ts, MysqlType.YEAR, senderTz, incorrectDateErr.replace("X", s_2020));

                                        /* Into TIME field */

                                        setObjectFromTz(props, tTime, ts, null, senderTz, expTimeCal, null, UseMethod.setTimestamp, cal_02);
                                        setObjectFromTz(props, tTime, ts, null, senderTz, expTimeTS, null, UseMethod.setTimestamp);
                                        setObjectFromTz(props, tTime, ts, null, senderTz, expTimeTS);
                                        if (useSSPS) {
                                            setObjectFromTz(props, tTime, ts, MysqlType.DATE, senderTz, s_000000);
                                        } else {
                                            assertThrows(props, tTime, ts, MysqlType.DATE, senderTz, incorrectTimeErr.replace("X", expDate));
                                        }
                                        setObjectFromTz(props, tTime, ts, MysqlType.CHAR, senderTz, expTime);
                                        setObjectFromTz(props, tTime, ts, MysqlType.VARCHAR, senderTz, expTime);
                                        setObjectFromTz(props, tTime, ts, MysqlType.TINYTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, ts, MysqlType.TEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, ts, MysqlType.MEDIUMTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, ts, MysqlType.LONGTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, ts, MysqlType.TIME, senderTz, expTime);
                                        setObjectFromTz(props, tTime, ts, MysqlType.DATETIME, senderTz, expTime);
                                        setObjectFromTz(props, tTime, ts, MysqlType.TIMESTAMP, senderTz, expTimeTS);
                                        setObjectFromTz(props, tTime, ts, MysqlType.YEAR, senderTz, expYear); // TIME takes numbers as a short notation, thus it works here
                                        setObjectFromTz(props, tTime, ts, MysqlType.YEAR, senderTz,
                                                "00:" + expYear.substring(0, 2) + ":" + expYear.substring(2)); // TIME takes numbers as a short notation, thus it works here

                                        /* Into DATETIME field */

                                        setObjectFromTz(props, tDatetime, ts, null, senderTz, expDatetimeCal, null, UseMethod.setTimestamp, cal_02);
                                        setObjectFromTz(props, tDatetime, ts, null, senderTz, expDatetimeTS, null, UseMethod.setTimestamp);
                                        setObjectFromTz(props, tDatetime, ts, null, senderTz, expDatetimeTS);
                                        setObjectFromTz(props, tDatetime, ts, MysqlType.DATE, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDatetime, ts, MysqlType.CHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, ts, MysqlType.VARCHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, ts, MysqlType.TINYTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, ts, MysqlType.TEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, ts, MysqlType.MEDIUMTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, ts, MysqlType.LONGTEXT, senderTz, expDatetime);
                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDatetime, ts, MysqlType.TIME, senderTz, expDatetimeDef);
                                            } else {
                                                assertThrows(props, tDatetime, ts, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDatetime, ts, MysqlType.TIME, senderTz, incorrectDatetimeErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tDatetime, ts, MysqlType.DATETIME, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, ts, MysqlType.TIMESTAMP, senderTz, expDatetimeTS);
                                        assertThrows(props, tDatetime, ts, MysqlType.YEAR, senderTz, incorrectDatetimeErr.replace("X", expYear));

                                        /* Into TIMESTAMP field */

                                        setObjectFromTz(props, tTimestamp, ts, null, senderTz, expCalTimestamp, expCalUnixTs, UseMethod.setTimestamp, cal_02);
                                        setObjectFromTz(props, tTimestamp, ts, null, senderTz, expTimestampTS, expFullUnixTsTS, UseMethod.setTimestamp);
                                        setObjectFromTz(props, tTimestamp, ts, null, senderTz, expTimestampTS, expFullUnixTsTS);
                                        setObjectFromTz(props, tTimestamp, ts, MysqlType.DATE, senderTz, expTimestampNoTime, expUnixTsFromDate);
                                        setObjectFromTz(props, tTimestamp, ts, MysqlType.CHAR, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, ts, MysqlType.VARCHAR, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, ts, MysqlType.TINYTEXT, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, ts, MysqlType.TEXT, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, ts, MysqlType.MEDIUMTEXT, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, ts, MysqlType.LONGTEXT, senderTz, expTimestamp, expFullUnixTs);
                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tTimestamp, ts, MysqlType.TIME, senderTz, expDefTimestamp, expDefUnixTs);
                                        } else {
                                            assertThrows(props, tTimestamp, ts, MysqlType.TIME, senderTz, incorrectDatetimeErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tTimestamp, ts, MysqlType.DATETIME, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, ts, MysqlType.TIMESTAMP, senderTz, expTimestampTS, expFullUnixTsTS);
                                        assertThrows(props, tTimestamp, ts, MysqlType.YEAR, senderTz, incorrectDatetimeErr.replace("X", expYear));

                                        /* Into VARCHAR field */

                                        String expDatetimeCal2 = useSSPS ? zdt_calendar_on_wire.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : expDatetimeCal; // TODO milliseconds are ignored by server. Bug ?
                                        String expDatetime2 = useSSPS
                                                ? zdt_20200101_120000_123456_on_wire.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : expDatetime; // TODO milliseconds are ignored by server. Bug ?
                                        String expTime2 = useSSPS ? expTimeNoMs : expTime; // TODO milliseconds are ignored by server. Bug ?
                                        String expDatetimeTS2 = useSSPS ? zdt_TS_on_wire.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET) : expDatetimeTS; // TODO milliseconds are ignored by server. Bug ?

                                        setObjectFromTz(props, tVarchar, ts, null, senderTz, expDatetimeCal2, null, UseMethod.setTimestamp, cal_02);
                                        setObjectFromTz(props, tVarchar, ts, null, senderTz, expDatetimeTS2, null, UseMethod.setTimestamp);
                                        setObjectFromTz(props, tVarchar, ts, null, senderTz, expDatetimeTS2);
                                        setObjectFromTz(props, tVarchar, ts, MysqlType.DATETIME, senderTz, expDatetime2);
                                        setObjectFromTz(props, tVarchar, ts, MysqlType.TIMESTAMP, senderTz, expDatetimeTS2);
                                        setObjectFromTz(props, tVarchar, ts, MysqlType.TIME, senderTz, expTime2);
                                        setObjectFromTz(props, tVarchar, ts, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, ts, MysqlType.CHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, ts, MysqlType.VARCHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, ts, MysqlType.TINYTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, ts, MysqlType.TEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, ts, MysqlType.MEDIUMTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, ts, MysqlType.LONGTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, ts, MysqlType.YEAR, senderTz, expYear);
                                    }
                                }
                            }
                        }
                    }
                    closeConnections();
                }
            } finally {
                closeConnections();
            }
        }
    }

    @Test
    public void testUtilCalendarSetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        createTable(tYear, "(id INT, d YEAR)");
        createTable(tDate, "(id INT, d DATE)");
        createTable(tTime, withFract ? "(id INT, d TIME(6))" : "(id INT, d TIME)");
        createTable(tDatetime, withFract ? "(id INT, d DATETIME(6))" : "(id INT, d DATETIME)");
        createTable(tTimestamp, withFract ? "(id INT, d TIMESTAMP(6))" : "(id INT, d TIMESTAMP)");
        createTable(tVarchar, "(id INT, d VARCHAR(30))");

        id = 0;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(props)) {
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
        }

        ZonedDateTime zdt_20200101_120000_123456_02_00 = ZonedDateTime.of(2020, 1, 1, 12, 00, 00, withFract ? 123456000 : 0, tz_plus_02_00.toZoneId());
        Calendar cal_02 = GregorianCalendar.from(zdt_20200101_120000_123456_02_00);

        for (TimeZone senderTz : this.senderTimeZones) {
            try {
                for (String connectionTZ : this.connectionTimeZones) {
                    initConnections(senderTz, connectionTZ);

                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? senderTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));
                                        TimeZone sessionTz = forceConnectionTimeZoneToSession ? connTz : serverTz;

                                        DateTimeFormatter dateTimeFmt = sendFractionalSeconds
                                                ? (withFract ? TimeUtil.DATETIME_FORMATTER_WITH_MILLIS_NO_OFFSET
                                                        : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET;
                                        DateTimeFormatter timeFmt = sendFractionalSeconds
                                                ? (withFract ? TIME_FORMATTER_WITH_MILLIS_NO_OFFCET : TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET;

                                        ZonedDateTime zdt_20200101_120000_123456_at_senderTz = zdt_20200101_120000_123456_02_00
                                                .withZoneSameInstant(senderTz.toZoneId());

                                        ZonedDateTime zdt_20200101_120000_123456_senderTz_to_connTz = preserveInstants
                                                && !(connectionTZ == null || "LOCAL".equals(connectionTZ))
                                                        ? zdt_20200101_120000_123456_at_senderTz.withZoneSameInstant(connTz.toZoneId())
                                                        : zdt_20200101_120000_123456_at_senderTz;

                                        ZonedDateTime zdt_20200101_120000_123456_on_wire = zdt_20200101_120000_123456_at_senderTz
                                                .withZoneSameLocal(sessionTz.toZoneId());
                                        ZonedDateTime zdt_TS_on_wire = zdt_20200101_120000_123456_senderTz_to_connTz.withZoneSameLocal(sessionTz.toZoneId());
                                        ZonedDateTime zdt_no_date_120000_123456_on_wire = LocalDate.now(sessionTz.toZoneId())
                                                .atTime(zdt_20200101_120000_123456_at_senderTz.toLocalTime()).atZone(sessionTz.toZoneId());
                                        ZonedDateTime zdt_20200101_no_time_on_wire = zdt_20200101_120000_123456_on_wire.withHour(0).withMinute(0).withSecond(0)
                                                .withNano(0);

                                        String expYear = zdt_20200101_120000_123456_on_wire.format(YEAR_FORMATTER);
                                        String expYearDef = zdt_no_date_120000_123456_on_wire.format(YEAR_FORMATTER);
                                        String expYearTS = zdt_TS_on_wire.format(YEAR_FORMATTER);

                                        String expDate = zdt_20200101_120000_123456_on_wire.format(TimeUtil.DATE_FORMATTER);
                                        String expDateDef = zdt_no_date_120000_123456_on_wire.format(useSSPS ? TimeUtil.DATE_FORMATTER
                                                : DateTimeFormatter.ofPattern(zdt_no_date_120000_123456_on_wire.getHour() == 0 ? "00HH-mm-ss" : "20HH-mm-ss"));
                                        String expDateTS = zdt_TS_on_wire.format(TimeUtil.DATE_FORMATTER);

                                        String expTimeNoMs = zdt_20200101_120000_123456_on_wire.format(TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expTime = zdt_20200101_120000_123456_on_wire.format(timeFmt);
                                        String expTimeTS = zdt_TS_on_wire.format(timeFmt);

                                        String expDatetime = zdt_20200101_120000_123456_on_wire.format(dateTimeFmt);
                                        String expDatetimeTS = zdt_TS_on_wire.format(dateTimeFmt);
                                        ZonedDateTime zdtDatetimeDef = LocalDate.now(sessionTz.toZoneId())
                                                .atTime(zdt_20200101_120000_123456_on_wire.toLocalTime()).atZone(sessionTz.toZoneId());
                                        String expDatetimeDef = zdtDatetimeDef.format(useSSPS ? dateTimeFmt
                                                : DateTimeFormatter.ofPattern(zdtDatetimeDef.getHour() == 0 ? "00HH-mm-ss 00:00:00" : "20HH-mm-ss 00:00:00"));
                                        String expDatetimeNoTime = zdt_20200101_120000_123456_on_wire
                                                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00"));

                                        String expTimestamp = zdt_20200101_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expTimestampTS = zdt_TS_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        ZonedDateTime zdtDefTimestamp = zdt_no_date_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId());
                                        String expDefTimestamp = zdtDefTimestamp.format(useSSPS ? dateTimeFmt
                                                : DateTimeFormatter.ofPattern(zdtDatetimeDef.getHour() == 0 ? "00HH-mm-ss 00:00:00" : "20HH-mm-ss 00:00:00"));
                                        String expTimestampNoTime = zdt_20200101_no_time_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);

                                        String expDefUnixTs = zdt_no_date_120000_123456_on_wire.toEpochSecond()
                                                + (sendFractionalSeconds && zdt_no_date_120000_123456_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_no_date_120000_123456_on_wire.getNano(), 3)
                                                        : "");
                                        String expFullUnixTs = zdt_20200101_120000_123456_on_wire.toEpochSecond()
                                                + (sendFractionalSeconds && zdt_20200101_120000_123456_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_20200101_120000_123456_on_wire.getNano(), 3)
                                                        : "");
                                        String expFullUnixTsTS = zdt_TS_on_wire.toEpochSecond() + (sendFractionalSeconds && zdt_TS_on_wire.getNano() > 0
                                                ? "." + TimeUtil.formatNanos(zdt_TS_on_wire.getNano(), 3)
                                                : "");
                                        String expUnixTsFromDate = zdt_20200101_no_time_on_wire.toEpochSecond() + "";

                                        /* Into YEAR field */

                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tYear, cal_02, null, senderTz, expYearTS);
                                            setObjectFromTz(props, tYear, cal_02, MysqlType.DATE, senderTz, expYear);
                                            setObjectFromTz(props, tYear, cal_02, MysqlType.TIME, senderTz, expYearDef);
                                            setObjectFromTz(props, tYear, cal_02, MysqlType.DATETIME, senderTz, expYear);
                                            setObjectFromTz(props, tYear, cal_02, MysqlType.TIMESTAMP, senderTz, expYearTS);
                                        } else {
                                            assertThrows(props, tYear, cal_02, null, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, cal_02, MysqlType.DATE, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, cal_02, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, cal_02, MysqlType.DATETIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, cal_02, MysqlType.TIMESTAMP, senderTz, dataTruncatedErr);
                                        }
                                        assertThrows(props, tYear, cal_02, MysqlType.CHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, cal_02, MysqlType.VARCHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, cal_02, MysqlType.TINYTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, cal_02, MysqlType.TEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, cal_02, MysqlType.MEDIUMTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, cal_02, MysqlType.LONGTEXT, senderTz, dataTruncatedErr);
                                        setObjectFromTz(props, tYear, cal_02, MysqlType.YEAR, senderTz, expYear);

                                        /* Into DATE field */

                                        setObjectFromTz(props, tDate, cal_02, null, senderTz, expDateTS);
                                        setObjectFromTz(props, tDate, cal_02, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tDate, cal_02, MysqlType.CHAR, senderTz, expDate);
                                        setObjectFromTz(props, tDate, cal_02, MysqlType.VARCHAR, senderTz, expDate);
                                        setObjectFromTz(props, tDate, cal_02, MysqlType.TINYTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, cal_02, MysqlType.TEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, cal_02, MysqlType.MEDIUMTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, cal_02, MysqlType.LONGTEXT, senderTz, expDate);
                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDate, cal_02, MysqlType.TIME, senderTz, expDateDef);
                                            } else {
                                                assertThrows(props, tDate, cal_02, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDate, cal_02, MysqlType.TIME, senderTz, incorrectDateErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tDate, cal_02, MysqlType.DATETIME, senderTz, expDate);
                                        setObjectFromTz(props, tDate, cal_02, MysqlType.TIMESTAMP, senderTz, expDateTS);
                                        assertThrows(props, tDate, cal_02, MysqlType.YEAR, senderTz, incorrectDateErr.replace("X", s_2020));

                                        /* Into TIME field */

                                        setObjectFromTz(props, tTime, cal_02, null, senderTz, expTimeTS);
                                        if (useSSPS) {
                                            setObjectFromTz(props, tTime, cal_02, MysqlType.DATE, senderTz, s_000000);
                                        } else {
                                            assertThrows(props, tTime, cal_02, MysqlType.DATE, senderTz, incorrectTimeErr.replace("X", expDate));
                                        }
                                        setObjectFromTz(props, tTime, cal_02, MysqlType.CHAR, senderTz, expTime);
                                        setObjectFromTz(props, tTime, cal_02, MysqlType.VARCHAR, senderTz, expTime);
                                        setObjectFromTz(props, tTime, cal_02, MysqlType.TINYTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, cal_02, MysqlType.TEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, cal_02, MysqlType.MEDIUMTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, cal_02, MysqlType.LONGTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, cal_02, MysqlType.TIME, senderTz, expTime);
                                        setObjectFromTz(props, tTime, cal_02, MysqlType.DATETIME, senderTz, expTime);
                                        setObjectFromTz(props, tTime, cal_02, MysqlType.TIMESTAMP, senderTz, expTimeTS);
                                        setObjectFromTz(props, tTime, cal_02, MysqlType.YEAR, senderTz, expYear); // TIME takes numbers as a short notation, thus it works here
                                        setObjectFromTz(props, tTime, cal_02, MysqlType.YEAR, senderTz,
                                                "00:" + expYear.substring(0, 2) + ":" + expYear.substring(2)); // TIME takes numbers as a short notation, thus it works here

                                        /* Into DATETIME field */

                                        setObjectFromTz(props, tDatetime, cal_02, null, senderTz, expDatetimeTS);
                                        setObjectFromTz(props, tDatetime, cal_02, MysqlType.DATE, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDatetime, cal_02, MysqlType.CHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, cal_02, MysqlType.VARCHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, cal_02, MysqlType.TINYTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, cal_02, MysqlType.TEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, cal_02, MysqlType.MEDIUMTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, cal_02, MysqlType.LONGTEXT, senderTz, expDatetime);
                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDatetime, cal_02, MysqlType.TIME, senderTz, expDatetimeDef);
                                            } else {
                                                assertThrows(props, tDatetime, cal_02, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDatetime, cal_02, MysqlType.TIME, senderTz, incorrectDatetimeErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tDatetime, cal_02, MysqlType.DATETIME, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, cal_02, MysqlType.TIMESTAMP, senderTz, expDatetimeTS);
                                        assertThrows(props, tDatetime, cal_02, MysqlType.YEAR, senderTz, incorrectDatetimeErr.replace("X", expYear));

                                        /* Into TIMESTAMP field */

                                        setObjectFromTz(props, tTimestamp, cal_02, null, senderTz, expTimestampTS, expFullUnixTsTS);
                                        setObjectFromTz(props, tTimestamp, cal_02, MysqlType.DATE, senderTz, expTimestampNoTime, expUnixTsFromDate);
                                        setObjectFromTz(props, tTimestamp, cal_02, MysqlType.CHAR, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, cal_02, MysqlType.VARCHAR, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, cal_02, MysqlType.TINYTEXT, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, cal_02, MysqlType.TEXT, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, cal_02, MysqlType.MEDIUMTEXT, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, cal_02, MysqlType.LONGTEXT, senderTz, expTimestamp, expFullUnixTs);

                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tTimestamp, cal_02, MysqlType.TIME, senderTz, expDefTimestamp, expDefUnixTs);
                                            } else {
                                                assertThrows(props, tTimestamp, cal_02, MysqlType.TIME, senderTz, incorrectDatetimeErr.replace("X", expTime));
                                            }
                                        } else {
                                            assertThrows(props, tTimestamp, cal_02, MysqlType.TIME, senderTz, incorrectDatetimeErr.replace("X", expTime));
                                        }

                                        setObjectFromTz(props, tTimestamp, cal_02, MysqlType.DATETIME, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, cal_02, MysqlType.TIMESTAMP, senderTz, expTimestampTS, expFullUnixTsTS);
                                        assertThrows(props, tTimestamp, cal_02, MysqlType.YEAR, senderTz, incorrectDatetimeErr.replace("X", expYear));

                                        /* Into VARCHAR field */

                                        String expDatetime2 = useSSPS
                                                ? zdt_20200101_120000_123456_on_wire.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : expDatetime; // TODO milliseconds are ignored by server. Bug ?
                                        String expDatetimeTS2 = useSSPS ? zdt_TS_on_wire.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET) : expDatetimeTS; // TODO milliseconds are ignored by server. Bug ?
                                        String expTime2 = useSSPS ? expTimeNoMs : expTime; // TODO milliseconds are ignored by server. Bug ?

                                        setObjectFromTz(props, tVarchar, cal_02, null, senderTz, expDatetimeTS2);
                                        setObjectFromTz(props, tVarchar, cal_02, MysqlType.DATETIME, senderTz, expDatetime2);
                                        setObjectFromTz(props, tVarchar, cal_02, MysqlType.TIMESTAMP, senderTz, expDatetimeTS2);
                                        setObjectFromTz(props, tVarchar, cal_02, MysqlType.TIME, senderTz, expTime2);
                                        setObjectFromTz(props, tVarchar, cal_02, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, cal_02, MysqlType.CHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, cal_02, MysqlType.VARCHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, cal_02, MysqlType.TINYTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, cal_02, MysqlType.TEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, cal_02, MysqlType.MEDIUMTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, cal_02, MysqlType.LONGTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, cal_02, MysqlType.YEAR, senderTz, expYear);
                                    }
                                }
                            }
                        }
                    }
                    closeConnections();
                }
            } finally {
                closeConnections();
            }
        }
    }

    @Test
    public void testUtilDateSetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        createTable(tYear, "(id INT, d YEAR)");
        createTable(tDate, "(id INT, d DATE)");
        createTable(tTime, withFract ? "(id INT, d TIME(6))" : "(id INT, d TIME)");
        createTable(tDatetime, withFract ? "(id INT, d DATETIME(6))" : "(id INT, d DATETIME)");
        createTable(tTimestamp, withFract ? "(id INT, d TIMESTAMP(6))" : "(id INT, d TIMESTAMP)");
        createTable(tVarchar, "(id INT, d VARCHAR(30))");

        id = 0;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(props)) {
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
        }

        for (TimeZone senderTz : this.senderTimeZones) {
            try {
                for (String connectionTZ : this.connectionTimeZones) {
                    initConnections(senderTz, connectionTZ);

                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? senderTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));
                                        TimeZone sessionTz = forceConnectionTimeZoneToSession ? connTz : serverTz;

                                        DateTimeFormatter dateTimeFmt = sendFractionalSeconds && withFract ? TimeUtil.DATETIME_FORMATTER_WITH_MILLIS_NO_OFFSET
                                                : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET;
                                        DateTimeFormatter timeFmt = sendFractionalSeconds && withFract ? TIME_FORMATTER_WITH_MILLIS_NO_OFFCET
                                                : TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET;

                                        ZonedDateTime zdt_20200101_120000_123456_at_senderTz = (withFract ? ldt_20200101_120000_123456
                                                : ldt_20200101_120000_123456.withNano(0)).atZone(senderTz.toZoneId());

                                        java.util.Date utilDate = java.util.Date.from(zdt_20200101_120000_123456_at_senderTz.toInstant());

                                        ZonedDateTime zdt_20200101_120000_123456_senderTz_to_connTz = preserveInstants
                                                && !(connectionTZ == null || "LOCAL".equals(connectionTZ))
                                                        ? zdt_20200101_120000_123456_at_senderTz.withZoneSameInstant(connTz.toZoneId())
                                                        : zdt_20200101_120000_123456_at_senderTz;

                                        ZonedDateTime zdt_20200101_120000_123456_on_wire = zdt_20200101_120000_123456_at_senderTz
                                                .withZoneSameLocal(sessionTz.toZoneId());
                                        ZonedDateTime zdt_TS_on_wire = zdt_20200101_120000_123456_senderTz_to_connTz.withZoneSameLocal(sessionTz.toZoneId());
                                        ZonedDateTime zdt_no_date_120000_123456_on_wire = LocalDate.now(sessionTz.toZoneId())
                                                .atTime(zdt_20200101_120000_123456_at_senderTz.toLocalTime()).atZone(sessionTz.toZoneId());
                                        ZonedDateTime zdt_20200101_no_time_on_wire = zdt_20200101_120000_123456_on_wire.withHour(0).withMinute(0).withSecond(0)
                                                .withNano(0);

                                        String expYear = zdt_20200101_120000_123456_on_wire.format(YEAR_FORMATTER);
                                        String expYearDef = zdt_no_date_120000_123456_on_wire.format(YEAR_FORMATTER);
                                        String expYearTS = zdt_TS_on_wire.format(YEAR_FORMATTER);

                                        String expDate = zdt_20200101_120000_123456_on_wire.format(TimeUtil.DATE_FORMATTER);
                                        String expDateDef = zdt_no_date_120000_123456_on_wire
                                                .format(useSSPS ? TimeUtil.DATE_FORMATTER : DateTimeFormatter.ofPattern("20HH-mm-ss"));
                                        String expDateTS = zdt_TS_on_wire.format(TimeUtil.DATE_FORMATTER);

                                        String expTimeNoMs = zdt_20200101_120000_123456_on_wire.format(TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expTime = zdt_20200101_120000_123456_on_wire.format(timeFmt);
                                        String expTimeTS = zdt_TS_on_wire.format(timeFmt);

                                        String expDatetime = zdt_20200101_120000_123456_on_wire.format(dateTimeFmt);
                                        String expDatetimeTS = zdt_TS_on_wire.format(dateTimeFmt);
                                        String expDatetimeDef = LocalDate.now(sessionTz.toZoneId()).atTime(zdt_20200101_120000_123456_on_wire.toLocalTime())
                                                .atZone(sessionTz.toZoneId())
                                                .format(useSSPS ? dateTimeFmt : DateTimeFormatter.ofPattern("20HH-mm-ss 00:00:00"));
                                        String expDatetimeNoTime = zdt_20200101_120000_123456_on_wire
                                                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00"));

                                        String expTimestamp = zdt_20200101_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expTimestampTS = zdt_TS_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expDefTimestamp = zdt_no_date_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expTimestampNoTime = zdt_20200101_no_time_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);

                                        String expDefUnixTs = zdt_no_date_120000_123456_on_wire.toEpochSecond()
                                                + (sendFractionalSeconds && zdt_no_date_120000_123456_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_no_date_120000_123456_on_wire.getNano(), 3)
                                                        : "");
                                        String expFullUnixTs = zdt_20200101_120000_123456_on_wire.toEpochSecond()
                                                + (sendFractionalSeconds && zdt_20200101_120000_123456_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_20200101_120000_123456_on_wire.getNano(), 3)
                                                        : "");
                                        String expFullUnixTsTS = zdt_TS_on_wire.toEpochSecond() + (sendFractionalSeconds && zdt_TS_on_wire.getNano() > 0
                                                ? "." + TimeUtil.formatNanos(zdt_TS_on_wire.getNano(), 3)
                                                : "");
                                        String expUnixTsFromDate = zdt_20200101_no_time_on_wire.toEpochSecond() + "";

                                        /* Into YEAR field */

                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tYear, utilDate, null, senderTz, expYearTS);
                                            setObjectFromTz(props, tYear, utilDate, MysqlType.DATE, senderTz, expYear);
                                            setObjectFromTz(props, tYear, utilDate, MysqlType.TIME, senderTz, expYearDef);
                                            setObjectFromTz(props, tYear, utilDate, MysqlType.DATETIME, senderTz, expYear);
                                            setObjectFromTz(props, tYear, utilDate, MysqlType.TIMESTAMP, senderTz, expYearTS);
                                        } else {
                                            assertThrows(props, tYear, utilDate, null, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, utilDate, MysqlType.DATE, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, utilDate, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, utilDate, MysqlType.DATETIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, utilDate, MysqlType.TIMESTAMP, senderTz, dataTruncatedErr);
                                        }
                                        assertThrows(props, tYear, utilDate, MysqlType.CHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, utilDate, MysqlType.VARCHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, utilDate, MysqlType.TINYTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, utilDate, MysqlType.TEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, utilDate, MysqlType.MEDIUMTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, utilDate, MysqlType.LONGTEXT, senderTz, dataTruncatedErr);
                                        setObjectFromTz(props, tYear, utilDate, MysqlType.YEAR, senderTz, expYear);

                                        /* Into DATE field */

                                        setObjectFromTz(props, tDate, utilDate, null, senderTz, expDateTS);
                                        setObjectFromTz(props, tDate, utilDate, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tDate, utilDate, MysqlType.CHAR, senderTz, expDate);
                                        setObjectFromTz(props, tDate, utilDate, MysqlType.VARCHAR, senderTz, expDate);
                                        setObjectFromTz(props, tDate, utilDate, MysqlType.TINYTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, utilDate, MysqlType.TEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, utilDate, MysqlType.MEDIUMTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, utilDate, MysqlType.LONGTEXT, senderTz, expDate);
                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDate, utilDate, MysqlType.TIME, senderTz, expDateDef);
                                            } else {
                                                assertThrows(props, tDate, utilDate, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDate, utilDate, MysqlType.TIME, senderTz, incorrectDateErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tDate, utilDate, MysqlType.DATETIME, senderTz, expDate);
                                        setObjectFromTz(props, tDate, utilDate, MysqlType.TIMESTAMP, senderTz, expDateTS);
                                        assertThrows(props, tDate, utilDate, MysqlType.YEAR, senderTz, incorrectDateErr.replace("X", expYear));

                                        /* Into TIME field */

                                        setObjectFromTz(props, tTime, utilDate, null, senderTz, expTimeTS);
                                        if (useSSPS) {
                                            setObjectFromTz(props, tTime, utilDate, MysqlType.DATE, senderTz, s_000000_000000);
                                        } else {
                                            assertThrows(props, tTime, utilDate, MysqlType.DATE, senderTz, incorrectTimeErr.replace("X", s_20200101));
                                        }
                                        setObjectFromTz(props, tTime, utilDate, MysqlType.CHAR, senderTz, expTime);
                                        setObjectFromTz(props, tTime, utilDate, MysqlType.VARCHAR, senderTz, expTime);
                                        setObjectFromTz(props, tTime, utilDate, MysqlType.TINYTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, utilDate, MysqlType.TEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, utilDate, MysqlType.MEDIUMTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, utilDate, MysqlType.LONGTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, utilDate, MysqlType.TIME, senderTz, expTime);
                                        setObjectFromTz(props, tTime, utilDate, MysqlType.DATETIME, senderTz, expTime);
                                        setObjectFromTz(props, tTime, utilDate, MysqlType.TIMESTAMP, senderTz, expTimeTS);
                                        setObjectFromTz(props, tTime, utilDate, MysqlType.YEAR, senderTz, expYear); // TIME takes numbers as a short notation, thus it works here
                                        setObjectFromTz(props, tTime, utilDate, MysqlType.YEAR, senderTz,
                                                "00:" + expYear.substring(0, 2) + ":" + expYear.substring(2)); // TIME takes numbers as a short notation, thus it works here

                                        /* Into DATETIME field */

                                        setObjectFromTz(props, tDatetime, utilDate, null, senderTz, expDatetimeTS);
                                        setObjectFromTz(props, tDatetime, utilDate, MysqlType.DATE, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDatetime, utilDate, MysqlType.CHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, utilDate, MysqlType.VARCHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, utilDate, MysqlType.TINYTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, utilDate, MysqlType.TEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, utilDate, MysqlType.MEDIUMTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, utilDate, MysqlType.LONGTEXT, senderTz, expDatetime);
                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDatetime, utilDate, MysqlType.TIME, senderTz, expDatetimeDef);
                                            } else {
                                                assertThrows(props, tDatetime, utilDate, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDatetime, utilDate, MysqlType.TIME, senderTz, incorrectDatetimeErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tDatetime, utilDate, MysqlType.DATETIME, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, utilDate, MysqlType.TIMESTAMP, senderTz, expDatetimeTS);
                                        assertThrows(props, tDatetime, utilDate, MysqlType.YEAR, senderTz, incorrectDatetimeErr.replace("X", expYear));

                                        /* Into TIMESTAMP field */

                                        setObjectFromTz(props, tTimestamp, utilDate, null, senderTz, expTimestampTS, expFullUnixTsTS);
                                        setObjectFromTz(props, tTimestamp, utilDate, MysqlType.DATE, senderTz, expTimestampNoTime, expUnixTsFromDate);
                                        setObjectFromTz(props, tTimestamp, utilDate, MysqlType.CHAR, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, utilDate, MysqlType.VARCHAR, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, utilDate, MysqlType.TINYTEXT, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, utilDate, MysqlType.TEXT, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, utilDate, MysqlType.MEDIUMTEXT, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, utilDate, MysqlType.LONGTEXT, senderTz, expTimestamp, expFullUnixTs);
                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tTimestamp, utilDate, MysqlType.TIME, senderTz, expDefTimestamp, expDefUnixTs);
                                        } else {
                                            assertThrows(props, tTimestamp, utilDate, MysqlType.TIME, senderTz, incorrectDatetimeErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tTimestamp, utilDate, MysqlType.DATETIME, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, utilDate, MysqlType.TIMESTAMP, senderTz, expTimestampTS, expFullUnixTsTS);
                                        assertThrows(props, tTimestamp, utilDate, MysqlType.YEAR, senderTz, incorrectDatetimeErr.replace("X", expYear));

                                        /* Into VARCHAR field */

                                        String expDatetime2 = useSSPS
                                                ? zdt_20200101_120000_123456_on_wire.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : expDatetime; // TODO milliseconds are ignored by server. Bug ?
                                        String expDatetimeTS2 = useSSPS ? zdt_TS_on_wire.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET) : expDatetimeTS; // TODO milliseconds are ignored by server. Bug ?
                                        String expTime2 = useSSPS ? expTimeNoMs : expTime; // TODO milliseconds are ignored by server. Bug ?

                                        setObjectFromTz(props, tVarchar, utilDate, null, senderTz, expDatetimeTS2);
                                        setObjectFromTz(props, tVarchar, utilDate, MysqlType.DATETIME, senderTz, expDatetime2);
                                        setObjectFromTz(props, tVarchar, utilDate, MysqlType.TIMESTAMP, senderTz, expDatetimeTS2);
                                        setObjectFromTz(props, tVarchar, utilDate, MysqlType.TIME, senderTz, expTime2);
                                        setObjectFromTz(props, tVarchar, utilDate, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, utilDate, MysqlType.CHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, utilDate, MysqlType.VARCHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, utilDate, MysqlType.TINYTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, utilDate, MysqlType.TEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, utilDate, MysqlType.MEDIUMTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, utilDate, MysqlType.LONGTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tVarchar, utilDate, MysqlType.YEAR, senderTz, expYear);

                                    }
                                }
                            }
                        }
                    }
                    closeConnections();
                }
            } finally {
                closeConnections();
            }
        }
    }

    @Test
    public void testLocalDateSetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        createTable(tYear, "(id INT, d YEAR)");
        createTable(tDate, "(id INT, d DATE)");
        createTable(tTime, "(id INT, d TIME)");
        createTable(tDatetime, "(id INT, d DATETIME)");
        createTable(tTimestamp, "(id INT, d TIMESTAMP)");
        createTable(tVarchar, "(id INT, d VARCHAR(30))");

        id = 0;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(props)) {
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
        }

        for (TimeZone senderTz : this.senderTimeZones) {
            try {
                for (String connectionTZ : this.connectionTimeZones) {
                    initConnections(senderTz, connectionTZ);

                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? senderTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));
                                        TimeZone sessionTz = forceConnectionTimeZoneToSession ? connTz : serverTz;

                                        ZonedDateTime zdt_20200101_no_time_on_wire = ld_20200101.atStartOfDay(sessionTz.toZoneId());

                                        String expYear = zdt_20200101_no_time_on_wire.format(YEAR_FORMATTER);
                                        String expDate = zdt_20200101_no_time_on_wire.format(TimeUtil.DATE_FORMATTER);
                                        String expTime = zdt_20200101_no_time_on_wire.format(TIME_FORMATTER_WITH_MICROS_NO_OFFCET);
                                        String expDatetimeNoTime = zdt_20200101_no_time_on_wire.format(DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00"));
                                        String expTimestamp = zdt_20200101_no_time_on_wire.withZoneSameInstant(tz_UTC.toZoneId())
                                                .format(DATETIME_FORMATTER_WITH_MICROS_NO_OFFCET);
                                        String expectedUnixTs = zdt_20200101_no_time_on_wire.toEpochSecond() + (zdt_20200101_no_time_on_wire.getNano() > 0
                                                ? "." + TimeUtil.formatNanos(zdt_20200101_no_time_on_wire.getNano(), 3)
                                                : "");

                                        String expDateErr = incorrectDateErr.replace("X", expYear);
                                        String expTimeErr = incorrectTimeErr.replace("X", expDate);
                                        String expDatetimeErr = incorrectDatetimeErr.replace("X", expYear);

                                        /* Unsupported conversions */

                                        assertThrows(props, tVarchar, ld_20200101, MysqlType.TIME, senderTz,
                                                ".* Conversion from java.time.LocalDate to TIME is not supported.");
                                        assertThrows(props, tDate, ld_20200101, MysqlType.INT, senderTz,
                                                ".* Conversion from java.time.LocalDate to INT is not supported.");

                                        /* Into YEAR field */

                                        if (useSSPS && withFract) {

                                            setObjectFromTz(props, tYear, ld_20200101, null, senderTz, expYear);
                                            setObjectFromTz(props, tYear, ld_20200101, MysqlType.DATE, senderTz, expYear);
                                            setObjectFromTz(props, tYear, ld_20200101, MysqlType.DATETIME, senderTz, expYear);
                                            setObjectFromTz(props, tYear, ld_20200101, MysqlType.TIMESTAMP, senderTz, expYear);
                                        } else {
                                            assertThrows(props, tYear, ld_20200101, null, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, ld_20200101, MysqlType.DATE, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, ld_20200101, MysqlType.DATETIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, ld_20200101, MysqlType.TIMESTAMP, senderTz, dataTruncatedErr);
                                        }
                                        assertThrows(props, tYear, ld_20200101, MysqlType.CHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ld_20200101, MysqlType.VARCHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ld_20200101, MysqlType.TINYTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ld_20200101, MysqlType.TEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ld_20200101, MysqlType.MEDIUMTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ld_20200101, MysqlType.LONGTEXT, senderTz, dataTruncatedErr);
                                        setObjectFromTz(props, tYear, ld_20200101, MysqlType.YEAR, senderTz, expYear);

                                        /* Into DATE field */

                                        setObjectFromTz(props, tDate, ld_20200101, null, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ld_20200101, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ld_20200101, MysqlType.CHAR, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ld_20200101, MysqlType.VARCHAR, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ld_20200101, MysqlType.TINYTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ld_20200101, MysqlType.TEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ld_20200101, MysqlType.MEDIUMTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ld_20200101, MysqlType.LONGTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, ld_20200101, MysqlType.DATETIME, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDate, ld_20200101, MysqlType.TIMESTAMP, senderTz, expDatetimeNoTime);
                                        assertThrows(props, tDate, ld_20200101, MysqlType.YEAR, senderTz, expDateErr);

                                        /* Into TIME field */

                                        if (useSSPS) {
                                            setObjectFromTz(props, tTime, ld_20200101, null, senderTz, expTime);
                                            setObjectFromTz(props, tTime, ld_20200101, MysqlType.DATE, senderTz, expTime);
                                        } else {
                                            assertThrows(props, tTime, ld_20200101, null, senderTz, expTimeErr);
                                            assertThrows(props, tTime, ld_20200101, MysqlType.DATE, senderTz, expTimeErr);
                                        }
                                        assertThrows(props, tTime, ld_20200101, MysqlType.CHAR, senderTz, expTimeErr);
                                        assertThrows(props, tTime, ld_20200101, MysqlType.VARCHAR, senderTz, expTimeErr);
                                        assertThrows(props, tTime, ld_20200101, MysqlType.TINYTEXT, senderTz, expTimeErr);
                                        assertThrows(props, tTime, ld_20200101, MysqlType.TEXT, senderTz, expTimeErr);
                                        assertThrows(props, tTime, ld_20200101, MysqlType.MEDIUMTEXT, senderTz, expTimeErr);
                                        assertThrows(props, tTime, ld_20200101, MysqlType.LONGTEXT, senderTz, expTimeErr);
                                        setObjectFromTz(props, tTime, ld_20200101, MysqlType.DATETIME, senderTz, expTime);
                                        setObjectFromTz(props, tTime, ld_20200101, MysqlType.TIMESTAMP, senderTz, expTime);
                                        setObjectFromTz(props, tTime, ld_20200101, MysqlType.YEAR, senderTz, expYear); // TIME takes numbers as a short notation, thus it works here
                                        setObjectFromTz(props, tTime, ld_20200101, MysqlType.YEAR, senderTz,
                                                "00:" + expYear.substring(0, 2) + ":" + expYear.substring(2)); // TIME takes numbers as a short notation, thus it works here

                                        /* Into DATETIME field */

                                        setObjectFromTz(props, tDatetime, ld_20200101, null, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDatetime, ld_20200101, MysqlType.DATE, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDatetime, ld_20200101, MysqlType.CHAR, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDatetime, ld_20200101, MysqlType.VARCHAR, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDatetime, ld_20200101, MysqlType.TINYTEXT, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDatetime, ld_20200101, MysqlType.TEXT, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDatetime, ld_20200101, MysqlType.MEDIUMTEXT, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDatetime, ld_20200101, MysqlType.LONGTEXT, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDatetime, ld_20200101, MysqlType.DATETIME, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDatetime, ld_20200101, MysqlType.TIMESTAMP, senderTz, expDatetimeNoTime);
                                        assertThrows(props, tDatetime, ld_20200101, MysqlType.YEAR, senderTz, expDatetimeErr);

                                        /* Into TIMESTAMP field */

                                        setObjectFromTz(props, tTimestamp, ld_20200101, null, senderTz, expTimestamp, expectedUnixTs);
                                        setObjectFromTz(props, tTimestamp, ld_20200101, MysqlType.DATE, senderTz, expTimestamp, expectedUnixTs);
                                        setObjectFromTz(props, tTimestamp, ld_20200101, MysqlType.CHAR, senderTz, expTimestamp, expectedUnixTs);
                                        setObjectFromTz(props, tTimestamp, ld_20200101, MysqlType.VARCHAR, senderTz, expTimestamp, expectedUnixTs);
                                        setObjectFromTz(props, tTimestamp, ld_20200101, MysqlType.TINYTEXT, senderTz, expTimestamp, expectedUnixTs);
                                        setObjectFromTz(props, tTimestamp, ld_20200101, MysqlType.TEXT, senderTz, expTimestamp, expectedUnixTs);
                                        setObjectFromTz(props, tTimestamp, ld_20200101, MysqlType.MEDIUMTEXT, senderTz, expTimestamp, expectedUnixTs);
                                        setObjectFromTz(props, tTimestamp, ld_20200101, MysqlType.LONGTEXT, senderTz, expTimestamp, expectedUnixTs);
                                        setObjectFromTz(props, tTimestamp, ld_20200101, MysqlType.DATETIME, senderTz, expTimestamp, expectedUnixTs);
                                        setObjectFromTz(props, tTimestamp, ld_20200101, MysqlType.TIMESTAMP, senderTz, expTimestamp, expectedUnixTs);
                                        assertThrows(props, tTimestamp, ld_20200101, MysqlType.YEAR, senderTz, expDatetimeErr);

                                        /* Into VARCHAR field */

                                        setObjectFromTz(props, tVarchar, ld_20200101, null, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, ld_20200101, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, ld_20200101, MysqlType.CHAR, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, ld_20200101, MysqlType.VARCHAR, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, ld_20200101, MysqlType.TINYTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, ld_20200101, MysqlType.TEXT, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, ld_20200101, MysqlType.MEDIUMTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, ld_20200101, MysqlType.LONGTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, ld_20200101, MysqlType.DATETIME, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tVarchar, ld_20200101, MysqlType.TIMESTAMP, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tVarchar, ld_20200101, MysqlType.YEAR, senderTz, expYear);
                                    }
                                }
                            }
                        }
                    }
                    closeConnections();
                }
            } finally {
                closeConnections();
            }
        }
    }

    @Test
    public void testLocalTimeSetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        createTable(tYear, "(id INT, d YEAR)");
        createTable(tDate, "(id INT, d DATE)");
        createTable(tTime, withFract ? "(id INT, d TIME(6))" : "(id INT, d TIME)");
        createTable(tDatetime, withFract ? "(id INT, d DATETIME(6))" : "(id INT, d DATETIME)");
        createTable(tTimestamp, withFract ? "(id INT, d TIMESTAMP(6))" : "(id INT, d TIMESTAMP)");
        createTable(tVarchar, "(id INT, d VARCHAR(30))");

        id = 0;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(props)) {
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
        }

        for (TimeZone senderTz : this.senderTimeZones) {
            try {
                for (String connectionTZ : this.connectionTimeZones) {
                    initConnections(senderTz, connectionTZ);

                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? senderTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));
                                        TimeZone sessionTz = forceConnectionTimeZoneToSession ? connTz : serverTz;

                                        DateTimeFormatter dateTimeFmt = withFract ? DATETIME_FORMATTER_WITH_MICROS_NO_OFFCET
                                                : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET;
                                        DateTimeFormatter timeFmt = withFract && sendFractionalSeconds ? TIME_FORMATTER_WITH_MICROS_NO_OFFCET
                                                : TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET;
                                        DateTimeFormatter timeFmtForChars = withFract && sendFractionalSeconds ? TimeUtil.TIME_FORMATTER_WITH_NANOS_NO_OFFSET
                                                : TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET;

                                        LocalTime orig_lt = withFract ? lt_120000_123456 : lt_120000;
                                        ZonedDateTime zdt_no_date_120000_123456_on_wire = LocalDate.now(sessionTz.toZoneId())
                                                .atTime(sendFractionalSeconds ? orig_lt : orig_lt.withNano(0)).atZone(sessionTz.toZoneId());

                                        String expYearDef = zdt_no_date_120000_123456_on_wire.format(YEAR_FORMATTER);
                                        String expDateDef = zdt_no_date_120000_123456_on_wire
                                                .format(useSSPS ? TimeUtil.DATE_FORMATTER : DateTimeFormatter.ofPattern("20HH-mm-ss"));
                                        String expTimeNoMs = zdt_no_date_120000_123456_on_wire.format(TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expTime6 = zdt_no_date_120000_123456_on_wire.format(timeFmt);
                                        String expTime9 = zdt_no_date_120000_123456_on_wire.format(timeFmtForChars);
                                        String expDatetimeDef = zdt_no_date_120000_123456_on_wire
                                                .format(useSSPS ? dateTimeFmt : DateTimeFormatter.ofPattern("20HH-mm-ss 00:00:00"));
                                        String expDefTimestamp = zdt_no_date_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expDefTimestampNoMs = zdt_no_date_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId())
                                                .format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET);

                                        String expDefUnixTsNoMs = zdt_no_date_120000_123456_on_wire.toEpochSecond() + "";
                                        String expDefUnixTs = expDefUnixTsNoMs + (zdt_no_date_120000_123456_on_wire.getNano() > 0
                                                ? "." + TimeUtil.formatNanos(zdt_no_date_120000_123456_on_wire.getNano(), 6)
                                                : "");

                                        String expDateErr6 = incorrectDateErr.replace("X", expTime6);
                                        String expDateErr9 = incorrectDateErr.replace("X", expTime9);
                                        String expDatetimeErr6 = incorrectDatetimeErr.replace("X", expTime6);
                                        String expDatetimeErr9 = incorrectDatetimeErr.replace("X", expTime9);

                                        /* Unsupported conversions */

                                        assertThrows(props, tVarchar, orig_lt, MysqlType.DATE, senderTz,
                                                ".* Conversion from java.time.LocalTime to DATE is not supported.");
                                        assertThrows(props, tVarchar, orig_lt, MysqlType.DATETIME, senderTz,
                                                ".* Conversion from java.time.LocalTime to DATETIME is not supported.");
                                        assertThrows(props, tVarchar, orig_lt, MysqlType.TIMESTAMP, senderTz,
                                                ".* Conversion from java.time.LocalTime to TIMESTAMP is not supported.");
                                        assertThrows(props, tVarchar, orig_lt, MysqlType.YEAR, senderTz,
                                                ".* Conversion from java.time.LocalTime to YEAR is not supported.");

                                        /* Into YEAR field */

                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tYear, orig_lt, null, senderTz, expYearDef);
                                            setObjectFromTz(props, tYear, orig_lt, MysqlType.TIME, senderTz, expYearDef);
                                        } else {
                                            assertThrows(props, tYear, orig_lt, null, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, orig_lt, MysqlType.TIME, senderTz, dataTruncatedErr);
                                        }
                                        assertThrows(props, tYear, orig_lt, MysqlType.CHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, orig_lt, MysqlType.VARCHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, orig_lt, MysqlType.TINYTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, orig_lt, MysqlType.TEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, orig_lt, MysqlType.MEDIUMTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, orig_lt, MysqlType.LONGTEXT, senderTz, dataTruncatedErr);

                                        /* Into DATE field */

                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDate, orig_lt, null, senderTz, expDateDef);
                                                setObjectFromTz(props, tDate, orig_lt, MysqlType.TIME, senderTz, expDateDef);
                                            } else {
                                                assertThrows(props, tDate, orig_lt, null, senderTz, dataTruncatedErr);
                                                assertThrows(props, tDate, orig_lt, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDate, orig_lt, null, senderTz, expDateErr6);
                                            assertThrows(props, tDate, orig_lt, MysqlType.TIME, senderTz, expDateErr6);
                                        }
                                        assertThrows(props, tDate, orig_lt, MysqlType.CHAR, senderTz, expDateErr9);
                                        assertThrows(props, tDate, orig_lt, MysqlType.VARCHAR, senderTz, expDateErr9);
                                        assertThrows(props, tDate, orig_lt, MysqlType.TINYTEXT, senderTz, expDateErr9);
                                        assertThrows(props, tDate, orig_lt, MysqlType.TEXT, senderTz, expDateErr9);
                                        assertThrows(props, tDate, orig_lt, MysqlType.MEDIUMTEXT, senderTz, expDateErr9);
                                        assertThrows(props, tDate, orig_lt, MysqlType.LONGTEXT, senderTz, expDateErr9);

                                        /* Into TIME field */

                                        setObjectFromTz(props, tTime, orig_lt, null, senderTz, expTime6);
                                        setObjectFromTz(props, tTime, lt_120000, MysqlType.CHAR, senderTz, expTimeNoMs);
                                        setObjectFromTz(props, tTime, orig_lt, MysqlType.CHAR, senderTz, expTime6);
                                        setObjectFromTz(props, tTime, orig_lt, MysqlType.VARCHAR, senderTz, expTime6);
                                        setObjectFromTz(props, tTime, orig_lt, MysqlType.TINYTEXT, senderTz, expTime6);
                                        setObjectFromTz(props, tTime, orig_lt, MysqlType.TEXT, senderTz, expTime6);
                                        setObjectFromTz(props, tTime, orig_lt, MysqlType.MEDIUMTEXT, senderTz, expTime6);
                                        setObjectFromTz(props, tTime, orig_lt, MysqlType.LONGTEXT, senderTz, expTime6);
                                        setObjectFromTz(props, tTime, lt_120000, MysqlType.TIME, senderTz, expTimeNoMs);
                                        setObjectFromTz(props, tTime, orig_lt, MysqlType.TIME, senderTz, expTime6);

                                        /* Into DATETIME field */

                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDatetime, orig_lt, null, senderTz, expDatetimeDef);
                                                setObjectFromTz(props, tDatetime, orig_lt, MysqlType.TIME, senderTz, expDatetimeDef);
                                            } else {
                                                assertThrows(props, tDatetime, orig_lt, null, senderTz, dataTruncatedErr);
                                                assertThrows(props, tDatetime, orig_lt, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDatetime, orig_lt, null, senderTz, expDatetimeErr6);
                                            assertThrows(props, tDatetime, orig_lt, MysqlType.TIME, senderTz, expDatetimeErr6);
                                        }
                                        assertThrows(props, tDatetime, orig_lt, MysqlType.CHAR, senderTz, expDatetimeErr9);
                                        assertThrows(props, tDatetime, orig_lt, MysqlType.VARCHAR, senderTz, expDatetimeErr9);
                                        assertThrows(props, tDatetime, orig_lt, MysqlType.TINYTEXT, senderTz, expDatetimeErr9);
                                        assertThrows(props, tDatetime, orig_lt, MysqlType.TEXT, senderTz, expDatetimeErr9);
                                        assertThrows(props, tDatetime, orig_lt, MysqlType.MEDIUMTEXT, senderTz, expDatetimeErr9);
                                        assertThrows(props, tDatetime, orig_lt, MysqlType.LONGTEXT, senderTz, expDatetimeErr9);

                                        /* Into TIMESTAMP field */

                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tTimestamp, orig_lt, null, senderTz, expDefTimestamp, expDefUnixTs);
                                            setObjectFromTz(props, tTimestamp, lt_120000, null, senderTz, expDefTimestampNoMs, expDefUnixTsNoMs);
                                            setObjectFromTz(props, tTimestamp, orig_lt, MysqlType.TIME, senderTz, expDefTimestamp, expDefUnixTs);
                                        } else {
                                            assertThrows(props, tTimestamp, orig_lt, null, senderTz, expDatetimeErr6);
                                            assertThrows(props, tTimestamp, lt_120000, null, senderTz, incorrectDatetimeErr.replace("X", expTimeNoMs));
                                            assertThrows(props, tTimestamp, orig_lt, MysqlType.TIME, senderTz, expDatetimeErr6);
                                        }
                                        assertThrows(props, tTimestamp, orig_lt, MysqlType.CHAR, senderTz, expDatetimeErr9);
                                        assertThrows(props, tTimestamp, orig_lt, MysqlType.VARCHAR, senderTz, expDatetimeErr9);
                                        assertThrows(props, tTimestamp, orig_lt, MysqlType.TINYTEXT, senderTz, expDatetimeErr9);
                                        assertThrows(props, tTimestamp, orig_lt, MysqlType.TEXT, senderTz, expDatetimeErr9);
                                        assertThrows(props, tTimestamp, orig_lt, MysqlType.MEDIUMTEXT, senderTz, expDatetimeErr9);
                                        assertThrows(props, tTimestamp, orig_lt, MysqlType.LONGTEXT, senderTz, expDatetimeErr9);

                                        /* Into VARCHAR field */

                                        String expTime2 = useSSPS ? expTimeNoMs : expTime6; // TODO milliseconds are ignored by server. Bug ?

                                        setObjectFromTz(props, tVarchar, orig_lt, null, senderTz, expTime2);
                                        setObjectFromTz(props, tVarchar, orig_lt, MysqlType.TIME, senderTz, expTime2);
                                        setObjectFromTz(props, tVarchar, orig_lt, MysqlType.CHAR, senderTz, expTime9);
                                        setObjectFromTz(props, tVarchar, orig_lt, MysqlType.VARCHAR, senderTz, expTime9);
                                        setObjectFromTz(props, tVarchar, orig_lt, MysqlType.TINYTEXT, senderTz, expTime9);
                                        setObjectFromTz(props, tVarchar, orig_lt, MysqlType.TEXT, senderTz, expTime9);
                                        setObjectFromTz(props, tVarchar, orig_lt, MysqlType.MEDIUMTEXT, senderTz, expTime9);
                                        setObjectFromTz(props, tVarchar, orig_lt, MysqlType.LONGTEXT, senderTz, expTime9);
                                    }
                                }
                            }
                        }
                    }
                    closeConnections();
                }
            } finally {
                closeConnections();
            }
        }
    }

    @Test
    public void testLocalDateTimeSetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        createTable(tYear, "(id INT, d YEAR)");
        createTable(tDate, "(id INT, d DATE)");
        createTable(tTime, withFract ? "(id INT, d TIME(6))" : "(id INT, d TIME)");
        createTable(tDatetime, withFract ? "(id INT, d DATETIME(6))" : "(id INT, d DATETIME)");
        createTable(tTimestamp, withFract ? "(id INT, d TIMESTAMP(6))" : "(id INT, d TIMESTAMP)");
        createTable(tVarchar, "(id INT, d VARCHAR(30))");

        id = 0;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(props)) {
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
        }

        for (TimeZone senderTz : this.senderTimeZones) {
            try {
                for (String connectionTZ : this.connectionTimeZones) {
                    initConnections(senderTz, connectionTZ);

                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? senderTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));
                                        TimeZone sessionTz = forceConnectionTimeZoneToSession ? connTz : serverTz;

                                        DateTimeFormatter dateTimeFmt = withFract && sendFractionalSeconds ? DATETIME_FORMATTER_WITH_MICROS_NO_OFFCET
                                                : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET;
                                        DateTimeFormatter dateTimeFmtForChars = withFract && sendFractionalSeconds
                                                ? TimeUtil.DATETIME_FORMATTER_WITH_NANOS_NO_OFFSET
                                                : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET;
                                        DateTimeFormatter timeFmt = withFract && sendFractionalSeconds ? TIME_FORMATTER_WITH_MICROS_NO_OFFCET
                                                : TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET;

                                        LocalDateTime orig_ldt = withFract ? ldt_20200101_120000_123456 : ldt_20200101_120000_123456.withNano(0);

                                        ZonedDateTime zdt_20200101_120000_123456_at_senderTz = orig_ldt.atZone(senderTz.toZoneId());

                                        ZonedDateTime zdt_20200101_120000_123456_on_wire = (sendFractionalSeconds ? zdt_20200101_120000_123456_at_senderTz
                                                : zdt_20200101_120000_123456_at_senderTz.withNano(0)).withZoneSameLocal(sessionTz.toZoneId());
                                        ZonedDateTime zdt_no_date_120000_123456_on_wire = LocalDate.now(sessionTz.toZoneId())
                                                .atTime((sendFractionalSeconds ? zdt_20200101_120000_123456_at_senderTz
                                                        : zdt_20200101_120000_123456_at_senderTz.withNano(0)).toLocalTime())
                                                .atZone(sessionTz.toZoneId());
                                        ZonedDateTime zdt_20200101_no_time_on_wire = zdt_20200101_120000_123456_on_wire.withHour(0).withMinute(0).withSecond(0)
                                                .withNano(0);

                                        String expYear = zdt_20200101_120000_123456_on_wire.format(YEAR_FORMATTER);
                                        String expYearDef = zdt_no_date_120000_123456_on_wire.format(YEAR_FORMATTER);

                                        String expDate = zdt_20200101_120000_123456_on_wire.format(TimeUtil.DATE_FORMATTER);
                                        String expDateDef = zdt_no_date_120000_123456_on_wire
                                                .format(useSSPS ? TimeUtil.DATE_FORMATTER : DateTimeFormatter.ofPattern("20HH-mm-ss"));

                                        String expTimeNoMs = zdt_20200101_120000_123456_on_wire.format(TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expTime = zdt_20200101_120000_123456_on_wire.format(timeFmt);

                                        String expDatetime = zdt_20200101_120000_123456_on_wire.format(dateTimeFmt);
                                        String expDatetimeChars = zdt_20200101_120000_123456_on_wire.format(dateTimeFmtForChars);
                                        String expDatetimeDef = LocalDate.now(sessionTz.toZoneId()).atTime(zdt_20200101_120000_123456_on_wire.toLocalTime())
                                                .atZone(sessionTz.toZoneId())
                                                .format(useSSPS ? dateTimeFmt : DateTimeFormatter.ofPattern("20HH-mm-ss 00:00:00"));
                                        String expDatetimeNoTime = zdt_20200101_120000_123456_on_wire
                                                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00"));

                                        String expTimestamp = zdt_20200101_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expDefTimestamp = zdt_no_date_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expTimestampNoTime = zdt_20200101_no_time_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);

                                        String expDefUnixTs = zdt_no_date_120000_123456_on_wire.toEpochSecond()
                                                + (zdt_no_date_120000_123456_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_no_date_120000_123456_on_wire.getNano(), 6)
                                                        : "");
                                        String expFullUnixTs = zdt_20200101_120000_123456_on_wire.toEpochSecond()
                                                + (zdt_20200101_120000_123456_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_20200101_120000_123456_on_wire.getNano(), 6)
                                                        : "");
                                        String expUnixTsFromDate = zdt_20200101_no_time_on_wire.toEpochSecond() + "";

                                        /* Into YEAR field */

                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tYear, orig_ldt, null, senderTz, expYear);
                                            setObjectFromTz(props, tYear, orig_ldt, MysqlType.DATE, senderTz, expYear);
                                            setObjectFromTz(props, tYear, orig_ldt, MysqlType.TIME, senderTz, expYearDef);
                                            setObjectFromTz(props, tYear, orig_ldt, MysqlType.DATETIME, senderTz, expYear);
                                            setObjectFromTz(props, tYear, orig_ldt, MysqlType.TIMESTAMP, senderTz, expYear);
                                        } else {
                                            assertThrows(props, tYear, orig_ldt, null, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, orig_ldt, MysqlType.DATE, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, orig_ldt, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, orig_ldt, MysqlType.DATETIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, orig_ldt, MysqlType.TIMESTAMP, senderTz, dataTruncatedErr);
                                        }
                                        assertThrows(props, tYear, orig_ldt, MysqlType.CHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, orig_ldt, MysqlType.VARCHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, orig_ldt, MysqlType.TINYTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, orig_ldt, MysqlType.TEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, orig_ldt, MysqlType.MEDIUMTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, orig_ldt, MysqlType.LONGTEXT, senderTz, dataTruncatedErr);

                                        setObjectFromTz(props, tYear, orig_ldt, MysqlType.YEAR, senderTz, expYear);

                                        /* Into DATE field */

                                        setObjectFromTz(props, tDate, orig_ldt, null, senderTz, expDate);
                                        setObjectFromTz(props, tDate, orig_ldt, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tDate, orig_ldt, MysqlType.CHAR, senderTz, expDate);
                                        setObjectFromTz(props, tDate, orig_ldt, MysqlType.VARCHAR, senderTz, expDate);
                                        setObjectFromTz(props, tDate, orig_ldt, MysqlType.TINYTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, orig_ldt, MysqlType.TEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, orig_ldt, MysqlType.MEDIUMTEXT, senderTz, expDate);
                                        setObjectFromTz(props, tDate, orig_ldt, MysqlType.LONGTEXT, senderTz, expDate);
                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDate, orig_ldt, MysqlType.TIME, senderTz, expDateDef);
                                            } else {
                                                assertThrows(props, tDate, orig_ldt, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDate, orig_ldt, MysqlType.TIME, senderTz, incorrectDateErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tDate, orig_ldt, MysqlType.DATETIME, senderTz, expDate);
                                        setObjectFromTz(props, tDate, orig_ldt, MysqlType.TIMESTAMP, senderTz, expDate);

                                        assertThrows(props, tDate, orig_ldt, MysqlType.YEAR, senderTz, incorrectDateErr.replace("X", expYear));

                                        /* Into TIME field */

                                        setObjectFromTz(props, tTime, orig_ldt, null, senderTz, expTime);
                                        if (useSSPS) {
                                            setObjectFromTz(props, tTime, orig_ldt, MysqlType.DATE, senderTz, s_000000_000000);
                                        } else {
                                            assertThrows(props, tTime, orig_ldt, MysqlType.DATE, senderTz, incorrectTimeErr.replace("X", expDate));
                                        }
                                        setObjectFromTz(props, tTime, orig_ldt, MysqlType.CHAR, senderTz, expTime);
                                        setObjectFromTz(props, tTime, orig_ldt, MysqlType.VARCHAR, senderTz, expTime);
                                        setObjectFromTz(props, tTime, orig_ldt, MysqlType.TINYTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, orig_ldt, MysqlType.TEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, orig_ldt, MysqlType.MEDIUMTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, orig_ldt, MysqlType.LONGTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, orig_ldt, MysqlType.TIME, senderTz, expTime);
                                        setObjectFromTz(props, tTime, orig_ldt, MysqlType.DATETIME, senderTz, expTime);
                                        setObjectFromTz(props, tTime, orig_ldt, MysqlType.TIMESTAMP, senderTz, expTime);
                                        setObjectFromTz(props, tTime, orig_ldt, MysqlType.YEAR, senderTz, expYear); // TIME takes numbers as a short notation, thus it works here
                                        setObjectFromTz(props, tTime, orig_ldt, MysqlType.YEAR, senderTz,
                                                "00:" + expYear.substring(0, 2) + ":" + expYear.substring(2)); // TIME takes numbers as a short notation, thus it works here

                                        /* Into DATETIME field */

                                        setObjectFromTz(props, tDatetime, orig_ldt, null, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, orig_ldt, MysqlType.DATE, senderTz, expDatetimeNoTime);
                                        setObjectFromTz(props, tDatetime, orig_ldt, MysqlType.CHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, orig_ldt, MysqlType.VARCHAR, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, orig_ldt, MysqlType.TINYTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, orig_ldt, MysqlType.TEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, orig_ldt, MysqlType.MEDIUMTEXT, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, orig_ldt, MysqlType.LONGTEXT, senderTz, expDatetime);
                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDatetime, orig_ldt, MysqlType.TIME, senderTz, expDatetimeDef);
                                            } else {
                                                assertThrows(props, tDatetime, orig_ldt, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDatetime, orig_ldt, MysqlType.TIME, senderTz, incorrectDatetimeErr.replace("X", expTime));
                                        }

                                        setObjectFromTz(props, tDatetime, orig_ldt, MysqlType.DATETIME, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, orig_ldt, MysqlType.TIMESTAMP, senderTz, expDatetime);

                                        assertThrows(props, tDatetime, orig_ldt, MysqlType.YEAR, senderTz, incorrectDatetimeErr.replace("X", expYear));

                                        /* Into TIMESTAMP field */

                                        setObjectFromTz(props, tTimestamp, orig_ldt, null, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, orig_ldt, MysqlType.DATE, senderTz, expTimestampNoTime, expUnixTsFromDate);
                                        setObjectFromTz(props, tTimestamp, orig_ldt, MysqlType.CHAR, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, orig_ldt, MysqlType.VARCHAR, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, orig_ldt, MysqlType.TINYTEXT, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, orig_ldt, MysqlType.TEXT, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, orig_ldt, MysqlType.MEDIUMTEXT, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, orig_ldt, MysqlType.LONGTEXT, senderTz, expTimestamp, expFullUnixTs);
                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tTimestamp, orig_ldt, MysqlType.TIME, senderTz, expDefTimestamp, expDefUnixTs);
                                        } else {
                                            assertThrows(props, tTimestamp, orig_ldt, MysqlType.TIME, senderTz, incorrectDatetimeErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tTimestamp, orig_ldt, MysqlType.DATETIME, senderTz, expTimestamp, expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, orig_ldt, MysqlType.TIMESTAMP, senderTz, expTimestamp, expFullUnixTs);
                                        assertThrows(props, tTimestamp, orig_ldt, MysqlType.YEAR, senderTz, incorrectDatetimeErr.replace("X", expYear));

                                        /* Into VARCHAR field */

                                        String expDatetime2 = useSSPS
                                                ? zdt_20200101_120000_123456_on_wire.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : zdt_20200101_120000_123456_on_wire.format(dateTimeFmt); // TODO milliseconds are ignored by server. Bug ?
                                        String expTime2 = useSSPS ? expTimeNoMs : expTime; // TODO milliseconds are ignored by server. Bug ?

                                        setObjectFromTz(props, tVarchar, orig_ldt, null, senderTz, expDatetime2);
                                        setObjectFromTz(props, tVarchar, orig_ldt, MysqlType.DATETIME, senderTz, expDatetime2);
                                        setObjectFromTz(props, tVarchar, orig_ldt, MysqlType.TIMESTAMP, senderTz, expDatetime2);
                                        setObjectFromTz(props, tVarchar, orig_ldt, MysqlType.TIME, senderTz, expTime2);
                                        setObjectFromTz(props, tVarchar, orig_ldt, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, orig_ldt, MysqlType.CHAR, senderTz, expDatetimeChars);
                                        setObjectFromTz(props, tVarchar, orig_ldt, MysqlType.VARCHAR, senderTz, expDatetimeChars);
                                        setObjectFromTz(props, tVarchar, orig_ldt, MysqlType.TINYTEXT, senderTz, expDatetimeChars);
                                        setObjectFromTz(props, tVarchar, orig_ldt, MysqlType.TEXT, senderTz, expDatetimeChars);
                                        setObjectFromTz(props, tVarchar, orig_ldt, MysqlType.MEDIUMTEXT, senderTz, expDatetimeChars);
                                        setObjectFromTz(props, tVarchar, orig_ldt, MysqlType.LONGTEXT, senderTz, expDatetimeChars);
                                        setObjectFromTz(props, tVarchar, orig_ldt, MysqlType.YEAR, senderTz, expYear);
                                    }
                                }
                            }
                        }
                    }
                    closeConnections();
                }
            } finally {
                closeConnections();
            }
        }
    }

    @Test
    public void testOffsetTimeSetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        createTable(tYear, "(id INT, d YEAR)");
        createTable(tDate, "(id INT, d DATE)");
        createTable(tTime, withFract ? "(id INT, d TIME(6))" : "(id INT, d TIME)");
        createTable(tDatetime, withFract ? "(id INT, d DATETIME(6))" : "(id INT, d DATETIME)");
        createTable(tTimestamp, withFract ? "(id INT, d TIMESTAMP(6))" : "(id INT, d TIMESTAMP)");
        createTable(tVarchar, "(id INT, d VARCHAR(30))");

        id = 0;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(props)) {
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
        }

        OffsetTime ot_120000_123456_05_00 = OffsetTime.of(12, 00, 00, withFract ? 123456000 : 0, ZoneOffset.ofHours(5));
        OffsetTime ot_120000_05_00 = OffsetTime.of(12, 00, 00, 0, ZoneOffset.ofHours(5));

        for (TimeZone senderTz : this.senderTimeZones) {
            try {
                for (String connectionTZ : this.connectionTimeZones) {
                    initConnections(senderTz, connectionTZ);

                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? senderTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));
                                        TimeZone sessionTz = forceConnectionTimeZoneToSession ? connTz : serverTz;

                                        DateTimeFormatter dateTimeFmt = withFract && sendFractionalSeconds ? DATETIME_FORMATTER_WITH_MICROS_NO_OFFCET
                                                : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET;
                                        DateTimeFormatter timeFmt = withFract && sendFractionalSeconds ? TIME_FORMATTER_WITH_MICROS_NO_OFFCET
                                                : TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET;
                                        DateTimeFormatter timeFmtTz = withFract && sendFractionalSeconds ? TimeUtil.TIME_FORMATTER_WITH_NANOS_WITH_OFFSET
                                                : TimeUtil.TIME_FORMATTER_NO_FRACT_WITH_OFFSET;

                                        ZonedDateTime zdt_20200101_120000_123456_at_senderTz = LocalDate.now(sessionTz.toZoneId())
                                                .atTime(ot_120000_123456_05_00.atDate(LocalDate.now()).atZoneSameInstant(senderTz.toZoneId()).toLocalTime())
                                                .atZone(senderTz.toZoneId());
                                        ZonedDateTime zdt_no_date_120000_123456_on_wire = (sendFractionalSeconds ? zdt_20200101_120000_123456_at_senderTz
                                                : zdt_20200101_120000_123456_at_senderTz.withNano(0)).withZoneSameLocal(sessionTz.toZoneId());

                                        String expYearDef = zdt_no_date_120000_123456_on_wire.format(YEAR_FORMATTER);
                                        String expDateDef = zdt_no_date_120000_123456_on_wire
                                                .format(useSSPS ? TimeUtil.DATE_FORMATTER : DateTimeFormatter.ofPattern("20HH-mm-ss"));
                                        String expTimeNoMs = zdt_no_date_120000_123456_on_wire.format(TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expTime = zdt_no_date_120000_123456_on_wire.format(timeFmt);
                                        String expTimeTz = ot_120000_123456_05_00.format(timeFmtTz).replace("+", "\\+");

                                        String expDatetimeDef = zdt_no_date_120000_123456_on_wire
                                                .format(useSSPS ? dateTimeFmt : DateTimeFormatter.ofPattern("20HH-mm-ss 00:00:00"));
                                        String expDefTimestamp = zdt_no_date_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expDefTimestampNoMs = zdt_no_date_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId())
                                                .format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET);

                                        String expDefUnixTsNoMs = zdt_no_date_120000_123456_on_wire.toEpochSecond() + "";
                                        String expDefUnixTs = expDefUnixTsNoMs + (zdt_no_date_120000_123456_on_wire.getNano() > 0
                                                ? "." + TimeUtil.formatNanos(zdt_no_date_120000_123456_on_wire.getNano(), 6)
                                                : "");

                                        String expDateErr = incorrectDateErr.replace("X", expTime);
                                        String expDateErrTz = incorrectDateErr.replace("X", expTimeTz);
                                        String expTimeErrTz = incorrectTimeErr.replace("X", expTimeTz);
                                        String expDatetimeErr = incorrectDatetimeErr.replace("X", expTime);
                                        String expDatetimeErrTz = incorrectDatetimeErr.replace("X", expTimeTz);

                                        /* Unsupported conversions */

                                        assertThrows(props, tVarchar, ot_120000_123456_05_00, MysqlType.DATE, senderTz,
                                                ".* Conversion from java.time.OffsetTime to DATE is not supported.");
                                        assertThrows(props, tVarchar, ot_120000_123456_05_00, MysqlType.DATETIME, senderTz,
                                                ".* Conversion from java.time.OffsetTime to DATETIME is not supported.");
                                        assertThrows(props, tVarchar, ot_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz,
                                                ".* Conversion from java.time.OffsetTime to TIMESTAMP is not supported.");
                                        assertThrows(props, tVarchar, ot_120000_123456_05_00, MysqlType.YEAR, senderTz,
                                                ".* Conversion from java.time.OffsetTime to YEAR is not supported.");

                                        /* Into YEAR field */

                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tYear, ot_120000_123456_05_00, null, senderTz, expYearDef);
                                            setObjectFromTz(props, tYear, ot_120000_123456_05_00, MysqlType.TIME, senderTz, expYearDef);
                                        } else {
                                            assertThrows(props, tYear, ot_120000_123456_05_00, null, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, ot_120000_123456_05_00, MysqlType.TIME, senderTz, dataTruncatedErr);
                                        }
                                        assertThrows(props, tYear, ot_120000_123456_05_00, MysqlType.CHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ot_120000_123456_05_00, MysqlType.VARCHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ot_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ot_120000_123456_05_00, MysqlType.TEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ot_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, ot_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, dataTruncatedErr);

                                        /* Into DATE field */

                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDate, ot_120000_123456_05_00, null, senderTz, expDateDef);
                                                setObjectFromTz(props, tDate, ot_120000_123456_05_00, MysqlType.TIME, senderTz, expDateDef);
                                            } else {
                                                assertThrows(props, tDate, ot_120000_123456_05_00, null, senderTz, dataTruncatedErr);
                                                assertThrows(props, tDate, ot_120000_123456_05_00, MysqlType.TIME, senderTz,
                                                        versionMeetsMinimum(5, 6) ? expDateErr : dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDate, ot_120000_123456_05_00, null, senderTz, expDateErr);
                                            assertThrows(props, tDate, ot_120000_123456_05_00, MysqlType.TIME, senderTz, expDateErr);
                                        }
                                        assertThrows(props, tDate, ot_120000_123456_05_00, MysqlType.CHAR, senderTz, expDateErrTz);
                                        assertThrows(props, tDate, ot_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expDateErrTz);
                                        assertThrows(props, tDate, ot_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expDateErrTz);
                                        assertThrows(props, tDate, ot_120000_123456_05_00, MysqlType.TEXT, senderTz, expDateErrTz);
                                        assertThrows(props, tDate, ot_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, expDateErrTz);
                                        assertThrows(props, tDate, ot_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expDateErrTz);

                                        /* Into TIME field */

                                        setObjectFromTz(props, tTime, ot_120000_123456_05_00, null, senderTz, expTime);
                                        assertThrows(props, tTime, ot_120000_123456_05_00, MysqlType.CHAR, senderTz, expTimeErrTz);
                                        assertThrows(props, tTime, ot_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expTimeErrTz);
                                        assertThrows(props, tTime, ot_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expTimeErrTz);
                                        assertThrows(props, tTime, ot_120000_123456_05_00, MysqlType.TEXT, senderTz, expTimeErrTz);
                                        assertThrows(props, tTime, ot_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, expTimeErrTz);
                                        assertThrows(props, tTime, ot_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expTimeErrTz);
                                        setObjectFromTz(props, tTime, ot_120000_05_00, MysqlType.TIME, senderTz, expTimeNoMs);
                                        setObjectFromTz(props, tTime, ot_120000_123456_05_00, MysqlType.TIME, senderTz, expTime);

                                        /* Into DATETIME field */

                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDatetime, ot_120000_123456_05_00, null, senderTz, expDatetimeDef);
                                                setObjectFromTz(props, tDatetime, ot_120000_123456_05_00, MysqlType.TIME, senderTz, expDatetimeDef);
                                            } else {
                                                assertThrows(props, tDatetime, ot_120000_123456_05_00, null, senderTz, dataTruncatedErr);
                                                assertThrows(props, tDatetime, ot_120000_123456_05_00, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDatetime, ot_120000_123456_05_00, null, senderTz, expDatetimeErr);
                                            assertThrows(props, tDatetime, ot_120000_123456_05_00, MysqlType.TIME, senderTz, expDatetimeErr);
                                        }
                                        assertThrows(props, tDatetime, ot_120000_123456_05_00, MysqlType.CHAR, senderTz, expDatetimeErrTz);
                                        assertThrows(props, tDatetime, ot_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expDatetimeErrTz);
                                        assertThrows(props, tDatetime, ot_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expDatetimeErrTz);
                                        assertThrows(props, tDatetime, ot_120000_123456_05_00, MysqlType.TEXT, senderTz, expDatetimeErrTz);
                                        assertThrows(props, tDatetime, ot_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, expDatetimeErrTz);
                                        assertThrows(props, tDatetime, ot_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expDatetimeErrTz);

                                        /* Into TIMESTAMP field */

                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tTimestamp, ot_120000_123456_05_00, null, senderTz, expDefTimestamp, expDefUnixTs);
                                            setObjectFromTz(props, tTimestamp, ot_120000_05_00, null, senderTz, expDefTimestampNoMs, expDefUnixTsNoMs);
                                            setObjectFromTz(props, tTimestamp, ot_120000_123456_05_00, MysqlType.TIME, senderTz, expDefTimestamp, expDefUnixTs);
                                        } else {
                                            assertThrows(props, tTimestamp, ot_120000_123456_05_00, null, senderTz, expDatetimeErr);
                                            assertThrows(props, tTimestamp, ot_120000_05_00, null, senderTz, incorrectDatetimeErr.replace("X", expTimeNoMs));
                                            assertThrows(props, tTimestamp, ot_120000_123456_05_00, MysqlType.TIME, senderTz, expDatetimeErr);
                                        }
                                        assertThrows(props, tTimestamp, ot_120000_123456_05_00, MysqlType.CHAR, senderTz, expDatetimeErrTz);
                                        assertThrows(props, tTimestamp, ot_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expDatetimeErrTz);
                                        assertThrows(props, tTimestamp, ot_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expDatetimeErrTz);
                                        assertThrows(props, tTimestamp, ot_120000_123456_05_00, MysqlType.TEXT, senderTz, expDatetimeErrTz);
                                        assertThrows(props, tTimestamp, ot_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, expDatetimeErrTz);
                                        assertThrows(props, tTimestamp, ot_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expDatetimeErrTz);

                                        /* Into VARCHAR field */

                                        String expTime2 = useSSPS ? expTimeNoMs : expTime; // TODO milliseconds are ignored by server. Bug ?

                                        setObjectFromTz(props, tVarchar, ot_120000_123456_05_00, null, senderTz, expTime2);
                                        setObjectFromTz(props, tVarchar, ot_120000_123456_05_00, MysqlType.TIME, senderTz, expTime2);
                                        setObjectFromTz(props, tVarchar, ot_120000_123456_05_00, MysqlType.CHAR, senderTz, expTimeTz);
                                        setObjectFromTz(props, tVarchar, ot_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expTimeTz);
                                        setObjectFromTz(props, tVarchar, ot_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expTimeTz);
                                        setObjectFromTz(props, tVarchar, ot_120000_123456_05_00, MysqlType.TEXT, senderTz, expTimeTz);
                                        setObjectFromTz(props, tVarchar, ot_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, expTimeTz);
                                        setObjectFromTz(props, tVarchar, ot_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expTimeTz);
                                    }
                                }
                            }
                        }
                    }
                    closeConnections();
                }
            } finally {
                closeConnections();
            }
        }
    }

    @Test
    public void testOffsetDatetimeSetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions
        boolean allowsOffset = versionMeetsMinimum(8, 0, 19);
        // Starting from MySQL 8.0.22 server also converts TIMESTAMP_WITH_TIMEZONE value to the server time zone for column types other than TIMESTAMP and DATETIME.
        // In MySQL 8.0.26 it was reverted.
        boolean serverConvertsTzForAllTypes = versionMeetsMinimum(8, 0, 22) && !versionMeetsMinimum(8, 0, 26);

        createTable(tYear, "(id INT, d YEAR)");
        createTable(tDate, "(id INT, d DATE)");
        createTable(tTime, withFract ? "(id INT, d TIME(6))" : "(id INT, d TIME)");
        createTable(tDatetime, withFract ? "(id INT, d DATETIME(6))" : "(id INT, d DATETIME)");
        createTable(tTimestamp, withFract ? "(id INT, d TIMESTAMP(6))" : "(id INT, d TIMESTAMP)");
        createTable(tVarchar, "(id INT, d VARCHAR(35))");

        id = 0;

        Properties props = new Properties();
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(props)) {
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
            System.out.println("Local tz: " + TimeZone.getDefault());
            System.out.println("Server tz: " + serverTz);
        }

        OffsetDateTime odt_20200101_120000_123456_05_00 = OffsetDateTime.of(2020, 1, 1, 12, 00, 00, withFract ? 123456000 : 0, ZoneOffset.ofHours(5));

        for (TimeZone senderTz : this.senderTimeZones) {
            try {
                for (String connectionTZ : this.connectionTimeZones) {
                    initConnections(senderTz, connectionTZ);

                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? senderTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));
                                        TimeZone sessionTz = forceConnectionTimeZoneToSession ? connTz : serverTz;

                                        DateTimeFormatter dateTimeFmt = withFract && sendFractionalSeconds ? DATETIME_FORMATTER_WITH_MICROS_NO_OFFCET
                                                : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET;
                                        DateTimeFormatter timeFmt = withFract && sendFractionalSeconds ? TIME_FORMATTER_WITH_MICROS_NO_OFFCET
                                                : TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET;

                                        ZonedDateTime zdt_20200101_120000_123456_at_senderTz = odt_20200101_120000_123456_05_00
                                                .atZoneSameInstant(senderTz.toZoneId());

                                        ZonedDateTime zdt_20200101_120000_123456_senderTz_to_connTz = preserveInstants
                                                && !(connectionTZ == null || "LOCAL".equals(connectionTZ))
                                                        ? zdt_20200101_120000_123456_at_senderTz.withZoneSameInstant(connTz.toZoneId())
                                                        : zdt_20200101_120000_123456_at_senderTz;

                                        ZonedDateTime zdt_20200101_120000_123456_on_wire = (sendFractionalSeconds ? zdt_20200101_120000_123456_at_senderTz
                                                : zdt_20200101_120000_123456_at_senderTz.withNano(0)).withZoneSameLocal(sessionTz.toZoneId());
                                        ZonedDateTime zdt_no_date_120000_123456_on_wire = LocalDate.now(sessionTz.toZoneId())
                                                .atTime((sendFractionalSeconds ? zdt_20200101_120000_123456_at_senderTz
                                                        : zdt_20200101_120000_123456_at_senderTz.withNano(0)).toLocalTime())
                                                .atZone(sessionTz.toZoneId());
                                        ZonedDateTime zdt_20200101_no_time_on_wire = zdt_20200101_120000_123456_on_wire.withHour(0).withMinute(0).withSecond(0)
                                                .withNano(0);

                                        ZonedDateTime zdt_TS_on_wire = (sendFractionalSeconds ? zdt_20200101_120000_123456_senderTz_to_connTz
                                                : zdt_20200101_120000_123456_senderTz_to_connTz.withNano(0)).withZoneSameLocal(sessionTz.toZoneId());

                                        String expYear = zdt_20200101_120000_123456_on_wire.format(YEAR_FORMATTER);
                                        String expYearDef = zdt_no_date_120000_123456_on_wire.format(YEAR_FORMATTER);
                                        String expYearTS = zdt_TS_on_wire.format(YEAR_FORMATTER);

                                        String expDate = zdt_20200101_120000_123456_on_wire.format(TimeUtil.DATE_FORMATTER);
                                        String expDateDef = zdt_no_date_120000_123456_on_wire
                                                .format(useSSPS ? TimeUtil.DATE_FORMATTER : DateTimeFormatter.ofPattern("20HH-mm-ss"));
                                        // Starting from MySQL 8.0.22 TIMESTAMP_WITH_TIMEZONE value is also converted to the server time zone by server
                                        // for column types other than TIMESTAMP or DATETIME
                                        String expDateChar = serverConvertsTzForAllTypes
                                                ? odt_20200101_120000_123456_05_00.atZoneSameInstant(sessionTz.toZoneId()).format(TimeUtil.DATE_FORMATTER)
                                                : odt_20200101_120000_123456_05_00.format(TimeUtil.DATE_FORMATTER);
                                        String expDateTS = zdt_TS_on_wire.format(TimeUtil.DATE_FORMATTER);

                                        String expTimeNoMs = zdt_20200101_120000_123456_on_wire.format(TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expTime = zdt_20200101_120000_123456_on_wire.format(timeFmt);

                                        String expTime2 = serverConvertsTzForAllTypes
                                                ? odt_20200101_120000_123456_05_00.atZoneSameInstant(sessionTz.toZoneId()).format(timeFmt)
                                                : odt_20200101_120000_123456_05_00.format(timeFmt);
                                        String expTimeTS = zdt_TS_on_wire.format(timeFmt);

                                        String expDatetime = zdt_20200101_120000_123456_on_wire.format(dateTimeFmt);
                                        String expDatetimeTS = zdt_TS_on_wire.format(dateTimeFmt);
                                        String expDatetimeDef = LocalDate.now(sessionTz.toZoneId()).atTime(zdt_20200101_120000_123456_on_wire.toLocalTime())
                                                .atZone(sessionTz.toZoneId())
                                                .format(useSSPS ? dateTimeFmt : DateTimeFormatter.ofPattern("20HH-mm-ss 00:00:00"));
                                        String expDatetimeNoTime = zdt_20200101_120000_123456_on_wire
                                                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00"));
                                        String expDatetimeExt = (sendFractionalSeconds ? odt_20200101_120000_123456_05_00
                                                : odt_20200101_120000_123456_05_00.withNano(0)).atZoneSameInstant(sessionTz.toZoneId()).format(dateTimeFmt);
                                        String expDatetimeExtOff = (sendFractionalSeconds ? odt_20200101_120000_123456_05_00
                                                : odt_20200101_120000_123456_05_00.withNano(0))
                                                        .format(withFract && sendFractionalSeconds ? TimeUtil.DATETIME_FORMATTER_WITH_NANOS_WITH_OFFSET
                                                                : TimeUtil.DATETIME_FORMATTER_NO_FRACT_WITH_OFFSET);

                                        String expTimestamp = zdt_20200101_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expTimestampTS = zdt_TS_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expDefTimestamp = zdt_no_date_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expTimestampNoTime = zdt_20200101_no_time_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);

                                        String expDefUnixTs = zdt_no_date_120000_123456_on_wire.toEpochSecond()
                                                + (zdt_no_date_120000_123456_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_no_date_120000_123456_on_wire.getNano(), 6)
                                                        : "");
                                        String expFullUnixTs = zdt_20200101_120000_123456_on_wire.toEpochSecond()
                                                + (zdt_20200101_120000_123456_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_20200101_120000_123456_on_wire.getNano(), 6)
                                                        : "");
                                        String expFullUnixTsTS = zdt_TS_on_wire.toEpochSecond()
                                                + (zdt_TS_on_wire.getNano() > 0 ? "." + TimeUtil.formatNanos(zdt_TS_on_wire.getNano(), 6) : "");
                                        String expUnixTsFromDate = zdt_20200101_no_time_on_wire.toEpochSecond() + "";

                                        /* Into YEAR field */

                                        if (useSSPS && withFract) {

                                            setObjectFromTz(props, tYear, odt_20200101_120000_123456_05_00, null, senderTz, expYearTS);
                                            setObjectFromTz(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, expYear);
                                            setObjectFromTz(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, expYearDef);
                                            setObjectFromTz(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, expYear);
                                            setObjectFromTz(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, expYearTS);
                                        } else {
                                            assertThrows(props, tYear, odt_20200101_120000_123456_05_00, null, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, dataTruncatedErr);
                                        }
                                        assertThrows(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, dataTruncatedErr);

                                        setObjectFromTz(props, tYear, odt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz, expYear);

                                        /* Into DATE field */

                                        setObjectFromTz(props, tDate, odt_20200101_120000_123456_05_00, null, senderTz, expDateTS);
                                        setObjectFromTz(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, expDate);
                                        if (allowsOffset) {
                                            setObjectFromTz(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz, expDateChar);
                                            setObjectFromTz(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expDateChar);
                                            setObjectFromTz(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expDateChar);
                                            setObjectFromTz(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz, expDateChar);
                                            setObjectFromTz(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, expDateChar);
                                            setObjectFromTz(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expDateChar);
                                        } else {
                                            assertThrows(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz,
                                                    incorrectDateErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz,
                                                    incorrectDateErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz,
                                                    incorrectDateErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz,
                                                    incorrectDateErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz,
                                                    incorrectDateErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz,
                                                    incorrectDateErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));

                                        }
                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, expDateDef);
                                            } else {
                                                assertThrows(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz,
                                                    incorrectDateErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, expDate);
                                        setObjectFromTz(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, expDateTS);

                                        assertThrows(props, tDate, odt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz,
                                                incorrectDateErr.replace("X", expYear));

                                        /* Into TIME field */

                                        setObjectFromTz(props, tTime, odt_20200101_120000_123456_05_00, null, senderTz, expTimeTS);
                                        if (useSSPS) {
                                            setObjectFromTz(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, s_000000_000000);
                                        } else {
                                            assertThrows(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz,
                                                    incorrectTimeErr.replace("X", expDate));
                                        }
                                        if (allowsOffset) {
                                            setObjectFromTz(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz, expTime2);
                                            setObjectFromTz(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expTime2);
                                            setObjectFromTz(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expTime2);
                                            setObjectFromTz(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz, expTime2);
                                            setObjectFromTz(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, expTime2);
                                            setObjectFromTz(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expTime2);
                                        } else {
                                            assertThrows(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz,
                                                    incorrectTimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz,
                                                    incorrectTimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz,
                                                    incorrectTimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz,
                                                    incorrectTimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz,
                                                    incorrectTimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz,
                                                    incorrectTimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                        }

                                        setObjectFromTz(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, expTime);
                                        setObjectFromTz(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, expTime);
                                        setObjectFromTz(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, expTimeTS);
                                        setObjectFromTz(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz, expYear); // TIME takes numbers as a short notation, thus it works here
                                        setObjectFromTz(props, tTime, odt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz,
                                                "00:" + expYear.substring(0, 2) + ":" + expYear.substring(2)); // TIME takes numbers as a short notation, thus it works here

                                        /* Into DATETIME field */

                                        setObjectFromTz(props, tDatetime, odt_20200101_120000_123456_05_00, null, senderTz, expDatetimeTS);
                                        setObjectFromTz(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, expDatetimeNoTime);
                                        if (allowsOffset) {
                                            setObjectFromTz(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz, expDatetimeExt);
                                            setObjectFromTz(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expDatetimeExt);
                                            setObjectFromTz(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expDatetimeExt);
                                            setObjectFromTz(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz, expDatetimeExt);
                                            setObjectFromTz(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, expDatetimeExt);
                                            setObjectFromTz(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expDatetimeExt);
                                        } else {
                                            assertThrows(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                        }
                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, expDatetimeDef);
                                            } else {
                                                assertThrows(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz,
                                                    incorrectDatetimeErr.replace("X", expTime));
                                        }

                                        setObjectFromTz(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, expDatetimeTS);

                                        assertThrows(props, tDatetime, odt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz,
                                                incorrectDatetimeErr.replace("X", expYear));

                                        /* Into TIMESTAMP field */

                                        String expTimestampExt = (sendFractionalSeconds ? odt_20200101_120000_123456_05_00
                                                : odt_20200101_120000_123456_05_00.withNano(0)).atZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expFullUnixTsExt = odt_20200101_120000_123456_05_00.toEpochSecond()
                                                + (sendFractionalSeconds && odt_20200101_120000_123456_05_00.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(odt_20200101_120000_123456_05_00.getNano(), 6)
                                                        : "");

                                        setObjectFromTz(props, tTimestamp, odt_20200101_120000_123456_05_00, null, senderTz, expTimestampTS, expFullUnixTsTS);
                                        setObjectFromTz(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, expTimestampNoTime,
                                                expUnixTsFromDate);

                                        if (allowsOffset) {
                                            setObjectFromTz(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz, expTimestampExt,
                                                    expFullUnixTsExt);
                                            setObjectFromTz(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expTimestampExt,
                                                    expFullUnixTsExt);
                                            setObjectFromTz(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expTimestampExt,
                                                    expFullUnixTsExt);
                                            setObjectFromTz(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz, expTimestampExt,
                                                    expFullUnixTsExt);
                                            setObjectFromTz(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz,
                                                    expTimestampExt, expFullUnixTsExt);
                                            setObjectFromTz(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expTimestampExt,
                                                    expFullUnixTsExt);
                                        } else {
                                            assertThrows(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                            assertThrows(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeExtOff.replace("+", "\\+")));
                                        }

                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, expDefTimestamp,
                                                    expDefUnixTs);
                                        } else {
                                            assertThrows(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz,
                                                    incorrectDatetimeErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, expTimestamp,
                                                expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, expTimestampTS,
                                                expFullUnixTsTS);
                                        assertThrows(props, tTimestamp, odt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz,
                                                incorrectDatetimeErr.replace("X", expYear));

                                        /* Into VARCHAR field */

                                        String expDatetime2 = useSSPS
                                                ? zdt_20200101_120000_123456_on_wire.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : expDatetime; // TODO milliseconds are ignored by server. Bug ?
                                        String expDatetimeTS2 = useSSPS ? zdt_TS_on_wire.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET) : expDatetimeTS; // TODO milliseconds are ignored by server. Bug ?

                                        expTime2 = useSSPS ? expTimeNoMs : expTime; // TODO milliseconds are ignored by server. Bug ?

                                        setObjectFromTz(props, tVarchar, odt_20200101_120000_123456_05_00, null, senderTz, expDatetimeTS2);
                                        setObjectFromTz(props, tVarchar, odt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, expDatetime2);
                                        setObjectFromTz(props, tVarchar, odt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, expDatetimeTS2);
                                        setObjectFromTz(props, tVarchar, odt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, expTime2);
                                        setObjectFromTz(props, tVarchar, odt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, odt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz, expDatetimeExtOff);
                                        setObjectFromTz(props, tVarchar, odt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expDatetimeExtOff);
                                        setObjectFromTz(props, tVarchar, odt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expDatetimeExtOff);
                                        setObjectFromTz(props, tVarchar, odt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz, expDatetimeExtOff);
                                        setObjectFromTz(props, tVarchar, odt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, expDatetimeExtOff);
                                        setObjectFromTz(props, tVarchar, odt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expDatetimeExtOff);
                                        setObjectFromTz(props, tVarchar, odt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz, expYear);
                                    }
                                }
                            }
                        }
                    }
                    closeConnections();
                }
            } finally {
                closeConnections();
            }
        }
    }

    @Test
    public void testZonedDatetimeSetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions
        boolean allowsOffset = versionMeetsMinimum(8, 0, 19);
        // Starting from MySQL 8.0.22 server also converts TIMESTAMP_WITH_TIMEZONE value to the server time zone for column types other than TIMESTAMP and DATETIME.
        // In MySQL 8.0.26 it was reverted.
        boolean serverConvertsTzForAllTypes = versionMeetsMinimum(8, 0, 22) && !versionMeetsMinimum(8, 0, 26);

        createTable(tYear, "(id INT, d YEAR)");
        createTable(tDate, "(id INT, d DATE)");
        createTable(tTime, withFract ? "(id INT, d TIME(6))" : "(id INT, d TIME)");
        createTable(tDatetime, withFract ? "(id INT, d DATETIME(6))" : "(id INT, d DATETIME)");
        createTable(tTimestamp, withFract ? "(id INT, d TIMESTAMP(6))" : "(id INT, d TIMESTAMP)");
        createTable(tVarchar, "(id INT, d VARCHAR(35))");

        id = 0;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(props)) {
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
            System.out.println("Local tz: " + TimeZone.getDefault());
            System.out.println("Server tz: " + serverTz);
        }

        ZonedDateTime zdt_20200101_120000_123456_05_00 = ZonedDateTime.of(withFract ? ldt_20200101_120000_123456 : ldt_20200101_120000_123456.withNano(0),
                ZoneId.of("Asia/Yekaterinburg"));

        for (TimeZone senderTz : this.senderTimeZones) {
            try {
                for (String connectionTZ : this.connectionTimeZones) {
                    initConnections(senderTz, connectionTZ);

                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? senderTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));
                                        TimeZone sessionTz = forceConnectionTimeZoneToSession ? connTz : serverTz;

                                        DateTimeFormatter dateTimeFmt = withFract && sendFractionalSeconds ? DATETIME_FORMATTER_WITH_MICROS_NO_OFFCET
                                                : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET;
                                        DateTimeFormatter timeFmt = withFract && sendFractionalSeconds ? TIME_FORMATTER_WITH_MICROS_NO_OFFCET
                                                : TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET;

                                        ZonedDateTime zdt_20200101_120000_123456_at_senderTz = zdt_20200101_120000_123456_05_00
                                                .withZoneSameInstant(senderTz.toZoneId());

                                        ZonedDateTime zdt_20200101_120000_123456_senderTz_to_connTz = preserveInstants
                                                && !(connectionTZ == null || "LOCAL".equals(connectionTZ))
                                                        ? zdt_20200101_120000_123456_at_senderTz.withZoneSameInstant(connTz.toZoneId())
                                                        : zdt_20200101_120000_123456_at_senderTz;

                                        ZonedDateTime zdt_20200101_120000_123456_on_wire = (sendFractionalSeconds ? zdt_20200101_120000_123456_at_senderTz
                                                : zdt_20200101_120000_123456_at_senderTz.withNano(0)).withZoneSameLocal(sessionTz.toZoneId());
                                        ZonedDateTime zdt_no_date_120000_123456_on_wire = LocalDate.now(sessionTz.toZoneId())
                                                .atTime((sendFractionalSeconds ? zdt_20200101_120000_123456_at_senderTz
                                                        : zdt_20200101_120000_123456_at_senderTz.withNano(0)).toLocalTime())
                                                .atZone(sessionTz.toZoneId());
                                        ZonedDateTime zdt_20200101_no_time_on_wire = zdt_20200101_120000_123456_on_wire.withHour(0).withMinute(0).withSecond(0)
                                                .withNano(0);
                                        ZonedDateTime zdt_TS_on_wire = (sendFractionalSeconds ? zdt_20200101_120000_123456_senderTz_to_connTz
                                                : zdt_20200101_120000_123456_senderTz_to_connTz.withNano(0)).withZoneSameLocal(sessionTz.toZoneId());

                                        String expYear = zdt_20200101_120000_123456_on_wire.format(YEAR_FORMATTER);
                                        String expYearDef = zdt_no_date_120000_123456_on_wire.format(YEAR_FORMATTER);
                                        String expYearTS = zdt_TS_on_wire.format(YEAR_FORMATTER);

                                        String expDate = zdt_20200101_120000_123456_on_wire.format(TimeUtil.DATE_FORMATTER);
                                        String expDateDef = zdt_no_date_120000_123456_on_wire
                                                .format(useSSPS ? TimeUtil.DATE_FORMATTER : DateTimeFormatter.ofPattern("20HH-mm-ss"));
                                        // Starting from MySQL 8.0.22 TIMESTAMP_WITH_TIMEZONE value is also converted to the server time zone by server
                                        // for column types other than TIMESTAMP or DATETIME
                                        String expDateChar = serverConvertsTzForAllTypes
                                                ? zdt_20200101_120000_123456_05_00.withZoneSameInstant(sessionTz.toZoneId()).format(TimeUtil.DATE_FORMATTER)
                                                : zdt_20200101_120000_123456_05_00.format(TimeUtil.DATE_FORMATTER);
                                        String expDateTS = zdt_TS_on_wire.format(TimeUtil.DATE_FORMATTER);

                                        String expTimeNoMs = zdt_20200101_120000_123456_on_wire.format(TimeUtil.TIME_FORMATTER_NO_FRACT_NO_OFFSET);
                                        String expTime = zdt_20200101_120000_123456_on_wire.format(timeFmt);
                                        String expTime2 = serverConvertsTzForAllTypes
                                                ? zdt_20200101_120000_123456_05_00.withZoneSameInstant(sessionTz.toZoneId()).format(timeFmt)
                                                : zdt_20200101_120000_123456_05_00.format(timeFmt);
                                        String expTimeTS = zdt_TS_on_wire.format(timeFmt);

                                        String expDatetime = zdt_20200101_120000_123456_on_wire.format(dateTimeFmt);
                                        String expDatetimeTS = zdt_TS_on_wire.format(dateTimeFmt);
                                        String expDatetimeDef = LocalDate.now(sessionTz.toZoneId()).atTime(zdt_20200101_120000_123456_on_wire.toLocalTime())
                                                .atZone(sessionTz.toZoneId())
                                                .format(useSSPS ? dateTimeFmt : DateTimeFormatter.ofPattern("20HH-mm-ss 00:00:00"));
                                        String expDatetimeNoTime = zdt_20200101_120000_123456_on_wire
                                                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd 00:00:00"));
                                        String expDatetimeOff = zdt_20200101_120000_123456_05_00
                                                .format(withFract && sendFractionalSeconds ? TimeUtil.DATETIME_FORMATTER_WITH_NANOS_WITH_OFFSET
                                                        : TimeUtil.DATETIME_FORMATTER_NO_FRACT_WITH_OFFSET);

                                        String expTimestamp = zdt_20200101_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expTimestampTS = zdt_TS_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expDefTimestamp = zdt_no_date_120000_123456_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expTimestampNoTime = zdt_20200101_no_time_on_wire.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);

                                        String expDefUnixTs = zdt_no_date_120000_123456_on_wire.toEpochSecond()
                                                + (zdt_no_date_120000_123456_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_no_date_120000_123456_on_wire.getNano(), 6)
                                                        : "");
                                        String expFullUnixTs = zdt_20200101_120000_123456_on_wire.toEpochSecond()
                                                + (zdt_20200101_120000_123456_on_wire.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_20200101_120000_123456_on_wire.getNano(), 6)
                                                        : "");
                                        String expFullUnixTsTS = zdt_TS_on_wire.toEpochSecond()
                                                + (zdt_TS_on_wire.getNano() > 0 ? "." + TimeUtil.formatNanos(zdt_TS_on_wire.getNano(), 6) : "");
                                        String expUnixTsFromDate = zdt_20200101_no_time_on_wire.toEpochSecond() + "";

                                        /* Into YEAR field */

                                        if (useSSPS && withFract) {

                                            setObjectFromTz(props, tYear, zdt_20200101_120000_123456_05_00, null, senderTz, expYearTS);
                                            setObjectFromTz(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, expYear);
                                            setObjectFromTz(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, expYearDef);
                                            setObjectFromTz(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, expYear);
                                            setObjectFromTz(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, expYearTS);
                                        } else {
                                            assertThrows(props, tYear, zdt_20200101_120000_123456_05_00, null, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, dataTruncatedErr);
                                            assertThrows(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, dataTruncatedErr);
                                        }
                                        assertThrows(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, dataTruncatedErr);
                                        assertThrows(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, dataTruncatedErr);

                                        setObjectFromTz(props, tYear, zdt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz, expYear);

                                        /* Into DATE field */

                                        setObjectFromTz(props, tDate, zdt_20200101_120000_123456_05_00, null, senderTz, expDateTS);
                                        setObjectFromTz(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, expDate);
                                        if (allowsOffset) {
                                            setObjectFromTz(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz, expDateChar);
                                            setObjectFromTz(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expDateChar);
                                            setObjectFromTz(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expDateChar);
                                            setObjectFromTz(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz, expDateChar);
                                            setObjectFromTz(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, expDateChar);
                                            setObjectFromTz(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expDateChar);
                                        } else {
                                            assertThrows(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz,
                                                    incorrectDateErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz,
                                                    incorrectDateErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz,
                                                    incorrectDateErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz,
                                                    incorrectDateErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz,
                                                    incorrectDateErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz,
                                                    incorrectDateErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                        }
                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, expDateDef);
                                            } else {
                                                assertThrows(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz,
                                                    incorrectDateErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, expDate);
                                        setObjectFromTz(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, expDateTS);

                                        assertThrows(props, tDate, zdt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz,
                                                incorrectDateErr.replace("X", expYear));

                                        /* Into TIME field */

                                        setObjectFromTz(props, tTime, zdt_20200101_120000_123456_05_00, null, senderTz, expTimeTS);
                                        if (useSSPS) {
                                            setObjectFromTz(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, s_000000_000000);
                                        } else {
                                            assertThrows(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz,
                                                    incorrectTimeErr.replace("X", expDate));
                                        }
                                        if (allowsOffset) {
                                            setObjectFromTz(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz, expTime2);
                                            setObjectFromTz(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expTime2);
                                            setObjectFromTz(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expTime2);
                                            setObjectFromTz(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz, expTime2);
                                            setObjectFromTz(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, expTime2);
                                            setObjectFromTz(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expTime2);
                                        } else {
                                            assertThrows(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz,
                                                    incorrectTimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz,
                                                    incorrectTimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz,
                                                    incorrectTimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz,
                                                    incorrectTimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz,
                                                    incorrectTimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz,
                                                    incorrectTimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                        }
                                        setObjectFromTz(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, expTime);
                                        setObjectFromTz(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, expTime);
                                        setObjectFromTz(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, expTimeTS);
                                        setObjectFromTz(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz, expYear); // TIME takes numbers as a short notation, thus it works here
                                        setObjectFromTz(props, tTime, zdt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz,
                                                "00:" + expYear.substring(0, 2) + ":" + expYear.substring(2)); // TIME takes numbers as a short notation, thus it works here

                                        /* Into DATETIME field */

                                        String expDatetimeExt = zdt_20200101_120000_123456_05_00.withZoneSameInstant(sessionTz.toZoneId()).format(dateTimeFmt);

                                        setObjectFromTz(props, tDatetime, zdt_20200101_120000_123456_05_00, null, senderTz, expDatetimeTS);
                                        setObjectFromTz(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, expDatetimeNoTime);
                                        if (allowsOffset) {
                                            setObjectFromTz(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz, expDatetimeExt);
                                            setObjectFromTz(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expDatetimeExt);
                                            setObjectFromTz(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expDatetimeExt);
                                            setObjectFromTz(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz, expDatetimeExt);
                                            setObjectFromTz(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, expDatetimeExt);
                                            setObjectFromTz(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expDatetimeExt);
                                        } else {
                                            assertThrows(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                        }
                                        if (useSSPS) {
                                            if (withFract) {
                                                setObjectFromTz(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, expDatetimeDef);
                                            } else {
                                                assertThrows(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, dataTruncatedErr);
                                            }
                                        } else {
                                            assertThrows(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz,
                                                    incorrectDatetimeErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, expDatetime);
                                        setObjectFromTz(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, expDatetimeTS);
                                        assertThrows(props, tDatetime, zdt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz,
                                                incorrectDatetimeErr.replace("X", expYear));

                                        /* Into TIMESTAMP field */

                                        String expTimestampExt = zdt_20200101_120000_123456_05_00.withZoneSameInstant(tz_UTC.toZoneId()).format(dateTimeFmt);
                                        String expFullUnixTsExt = zdt_20200101_120000_123456_05_00.toEpochSecond()
                                                + (sendFractionalSeconds && zdt_20200101_120000_123456_05_00.getNano() > 0
                                                        ? "." + TimeUtil.formatNanos(zdt_20200101_120000_123456_05_00.getNano(), 6)
                                                        : "");

                                        setObjectFromTz(props, tTimestamp, zdt_20200101_120000_123456_05_00, null, senderTz, expTimestampTS, expFullUnixTsTS);
                                        setObjectFromTz(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, expTimestampNoTime,
                                                expUnixTsFromDate);
                                        if (allowsOffset) {
                                            setObjectFromTz(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz, expTimestampExt,
                                                    expFullUnixTsExt);
                                            setObjectFromTz(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expTimestampExt,
                                                    expFullUnixTsExt);
                                            setObjectFromTz(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expTimestampExt,
                                                    expFullUnixTsExt);
                                            setObjectFromTz(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz, expTimestampExt,
                                                    expFullUnixTsExt);
                                            setObjectFromTz(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz,
                                                    expTimestampExt, expFullUnixTsExt);
                                            setObjectFromTz(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expTimestampExt,
                                                    expFullUnixTsExt);
                                        } else {
                                            assertThrows(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                            assertThrows(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz,
                                                    incorrectDatetimeErr.replace("X", expDatetimeOff.replace("+", "\\+")));
                                        }
                                        if (useSSPS && withFract) {
                                            setObjectFromTz(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, expDefTimestamp,
                                                    expDefUnixTs);
                                        } else {
                                            assertThrows(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz,
                                                    incorrectDatetimeErr.replace("X", expTime));
                                        }
                                        setObjectFromTz(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, expTimestamp,
                                                expFullUnixTs);
                                        setObjectFromTz(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, expTimestampTS,
                                                expFullUnixTsTS);
                                        assertThrows(props, tTimestamp, zdt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz,
                                                incorrectDatetimeErr.replace("X", expYear));

                                        /* Into VARCHAR field */

                                        String expDatetime2 = useSSPS
                                                ? zdt_20200101_120000_123456_on_wire.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET)
                                                : expDatetime; // TODO milliseconds are ignored by server. Bug ?
                                        String expDatetimeTS2 = useSSPS ? zdt_TS_on_wire.format(TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET) : expDatetimeTS; // TODO milliseconds are ignored by server. Bug ?

                                        expTime2 = useSSPS ? expTimeNoMs : expTime; // TODO milliseconds are ignored by server. Bug ?

                                        setObjectFromTz(props, tVarchar, zdt_20200101_120000_123456_05_00, null, senderTz, expDatetimeTS2);
                                        setObjectFromTz(props, tVarchar, zdt_20200101_120000_123456_05_00, MysqlType.DATETIME, senderTz, expDatetime2);
                                        setObjectFromTz(props, tVarchar, zdt_20200101_120000_123456_05_00, MysqlType.TIMESTAMP, senderTz, expDatetimeTS2);
                                        setObjectFromTz(props, tVarchar, zdt_20200101_120000_123456_05_00, MysqlType.TIME, senderTz, expTime2);
                                        setObjectFromTz(props, tVarchar, zdt_20200101_120000_123456_05_00, MysqlType.DATE, senderTz, expDate);
                                        setObjectFromTz(props, tVarchar, zdt_20200101_120000_123456_05_00, MysqlType.CHAR, senderTz, expDatetimeOff);
                                        setObjectFromTz(props, tVarchar, zdt_20200101_120000_123456_05_00, MysqlType.VARCHAR, senderTz, expDatetimeOff);
                                        setObjectFromTz(props, tVarchar, zdt_20200101_120000_123456_05_00, MysqlType.TINYTEXT, senderTz, expDatetimeOff);
                                        setObjectFromTz(props, tVarchar, zdt_20200101_120000_123456_05_00, MysqlType.TEXT, senderTz, expDatetimeOff);
                                        setObjectFromTz(props, tVarchar, zdt_20200101_120000_123456_05_00, MysqlType.MEDIUMTEXT, senderTz, expDatetimeOff);
                                        setObjectFromTz(props, tVarchar, zdt_20200101_120000_123456_05_00, MysqlType.LONGTEXT, senderTz, expDatetimeOff);
                                        setObjectFromTz(props, tVarchar, zdt_20200101_120000_123456_05_00, MysqlType.YEAR, senderTz, expYear);
                                    }
                                }
                            }
                        }
                    }
                    closeConnections();
                }
            } finally {
                closeConnections();
            }
        }
    }

    @Test
    public void testDurationSetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        createTable(tYear, "(id INT, d YEAR)");
        createTable(tDate, "(id INT, d DATE)");
        createTable(tTime, withFract ? "(id INT, d TIME(6))" : "(id INT, d TIME)");
        createTable(tDatetime, withFract ? "(id INT, d DATETIME(6))" : "(id INT, d DATETIME)");
        createTable(tTimestamp, withFract ? "(id INT, d TIMESTAMP(6))" : "(id INT, d TIMESTAMP)");
        createTable(tVarchar, "(id INT, d VARCHAR(30))");

        id = 0;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");

        for (TimeZone senderTz : this.senderTimeZones) {
            try {
                for (String connectionTZ : this.connectionTimeZones) {
                    initConnections(senderTz, connectionTZ);

                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        Duration dur_no_fract = Duration.parse("PT300H10M20S");
                                        Duration dur_with_fract = Duration.parse("PT300H10M20.123S");
                                        Duration dur = withFract ? dur_with_fract : dur_no_fract;
                                        String expDur = TimeUtil.getDurationString(sendFractionalSeconds ? dur : dur_no_fract);
                                        String expTime = TimeUtil.getDurationString(dur);

                                        Duration neg_dur_no_fract = Duration.parse("-PT300H10M20S");
                                        Duration neg_dur = withFract ? Duration.parse("-PT300H10M20.123S") : neg_dur_no_fract;
                                        String expNegDur = TimeUtil.getDurationString(sendFractionalSeconds ? neg_dur : neg_dur_no_fract);
                                        String expNegTime = TimeUtil.getDurationString(neg_dur);

                                        /* Unsupported conversions */

                                        assertThrows(props, tVarchar, dur, MysqlType.DATE, senderTz,
                                                ".* Conversion from java.time.Duration to DATE is not supported.");
                                        assertThrows(props, tVarchar, dur, MysqlType.DATETIME, senderTz,
                                                ".* Conversion from java.time.Duration to DATETIME is not supported.");
                                        assertThrows(props, tVarchar, dur, MysqlType.TIMESTAMP, senderTz,
                                                ".* Conversion from java.time.Duration to TIMESTAMP is not supported.");
                                        assertThrows(props, tVarchar, dur, MysqlType.YEAR, senderTz,
                                                ".* Conversion from java.time.Duration to YEAR is not supported.");

                                        /* Into TIME field */

                                        setObjectFromTz(props, tTime, dur, null, senderTz, expDur);
                                        setObjectFromTz(props, tTime, neg_dur, null, senderTz, expNegDur);
                                        setObjectFromTz(props, tTime, dur, MysqlType.TIME, senderTz, expDur);
                                        setObjectFromTz(props, tTime, neg_dur, MysqlType.TIME, senderTz, expNegDur);
                                        setObjectFromTz(props, tTime, dur, MysqlType.CHAR, senderTz, expTime);
                                        setObjectFromTz(props, tTime, neg_dur, MysqlType.CHAR, senderTz, expNegTime);
                                        setObjectFromTz(props, tTime, dur, MysqlType.VARCHAR, senderTz, expTime);
                                        setObjectFromTz(props, tTime, dur, MysqlType.TINYTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, dur, MysqlType.TEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, dur, MysqlType.MEDIUMTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tTime, dur, MysqlType.LONGTEXT, senderTz, expTime);

                                        /* Into VARCHAR field */

                                        String expChar = expDur + (withFract && sendFractionalSeconds && useSSPS ? "000" : "");
                                        String expChar2 = useSSPS ? TimeUtil.getDurationString(dur_no_fract) : expChar; // TODO milliseconds are ignored by server. Bug ?
                                        String expNegChar = expNegDur + (withFract && sendFractionalSeconds && useSSPS ? "000" : "");
                                        String expNegChar2 = useSSPS ? TimeUtil.getDurationString(neg_dur_no_fract) : expNegChar; // TODO milliseconds are ignored by server. Bug ?

                                        setObjectFromTz(props, tVarchar, dur, null, senderTz, expChar2);
                                        setObjectFromTz(props, tVarchar, neg_dur, null, senderTz, expNegChar2);
                                        setObjectFromTz(props, tVarchar, dur, MysqlType.TIME, senderTz, expChar2);
                                        setObjectFromTz(props, tVarchar, neg_dur, MysqlType.TIME, senderTz, expNegChar2);
                                        setObjectFromTz(props, tVarchar, dur, MysqlType.CHAR, senderTz, expTime);
                                        setObjectFromTz(props, tVarchar, neg_dur, MysqlType.CHAR, senderTz, expNegTime);
                                        setObjectFromTz(props, tVarchar, dur, MysqlType.VARCHAR, senderTz, expTime);
                                        setObjectFromTz(props, tVarchar, neg_dur, MysqlType.VARCHAR, senderTz, expNegTime);
                                        setObjectFromTz(props, tVarchar, dur, MysqlType.TINYTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tVarchar, neg_dur, MysqlType.TINYTEXT, senderTz, expNegTime);
                                        setObjectFromTz(props, tVarchar, dur, MysqlType.TEXT, senderTz, expTime);
                                        setObjectFromTz(props, tVarchar, neg_dur, MysqlType.TEXT, senderTz, expNegTime);
                                        setObjectFromTz(props, tVarchar, dur, MysqlType.MEDIUMTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tVarchar, neg_dur, MysqlType.MEDIUMTEXT, senderTz, expNegTime);
                                        setObjectFromTz(props, tVarchar, dur, MysqlType.LONGTEXT, senderTz, expTime);
                                        setObjectFromTz(props, tVarchar, neg_dur, MysqlType.LONGTEXT, senderTz, expNegTime);

                                    }
                                }
                            }
                        }
                    }
                    closeConnections();
                }
            } finally {
                closeConnections();
            }
        }
    }

    void assertThrows(Properties props, String tableName, Object parameter, SQLType targetSqlType, TimeZone senderTz, String err) throws Exception {
        assertThrows(SQLException.class, err, new Callable<Void>() {
            public Void call() throws Exception {
                setObjectFromTz(props, tableName, parameter, targetSqlType, senderTz);
                return null;
            }
        });
    }

    void assertThrows(Properties props, String tableName, Object parameter, SQLType targetSqlType, TimeZone senderTz, String expectedValue,
            String expectedUnixTimestamp, UseMethod useMethod, String err) throws Exception {
        assertThrows(SQLException.class, err, new Callable<Void>() {
            public Void call() throws Exception {
                setObjectFromTz(props, tableName, parameter, targetSqlType, senderTz, expectedValue, expectedUnixTimestamp, useMethod, null);
                return null;
            }
        });
    }

    void assertThrows(Properties props, String tableName, Object parameter, SQLType targetSqlType, TimeZone senderTz, String expectedValue,
            String expectedUnixTimestamp, UseMethod useMethod, Calendar calendar, String err) throws Exception {
        assertThrows(SQLException.class, err, new Callable<Void>() {
            public Void call() throws Exception {
                setObjectFromTz(props, tableName, parameter, targetSqlType, senderTz, expectedValue, expectedUnixTimestamp, useMethod, calendar);
                return null;
            }
        });
    }

    void setObjectFromTz(Properties props, String tableName, Object parameter, SQLType targetSqlType, TimeZone senderTz) throws Exception {
        setObjectFromTz(props, tableName, parameter, targetSqlType, senderTz, null, null, UseMethod.setObject, null);
    }

    void setObjectFromTz(Properties props, String tableName, Object parameter, SQLType targetSqlType, TimeZone senderTz, String expectedUTCValue)
            throws Exception {
        setObjectFromTz(props, tableName, parameter, targetSqlType, senderTz, expectedUTCValue, null, UseMethod.setObject, null);
    }

    void setObjectFromTz(Properties props, String tableName, Object parameter, SQLType targetSqlType, TimeZone senderTz, String expectedUTCValue,
            String expectedUnixTimestamp) throws Exception {
        setObjectFromTz(props, tableName, parameter, targetSqlType, senderTz, expectedUTCValue, expectedUnixTimestamp, UseMethod.setObject, null);
    }

    void setObjectFromTz(Properties props, String tableName, Object parameter, SQLType targetSqlType, TimeZone senderTz, String expectedUTCValue,
            String expectedUnixTimestamp, UseMethod useMethod) throws Exception {
        setObjectFromTz(props, tableName, parameter, targetSqlType, senderTz, expectedUTCValue, expectedUnixTimestamp, useMethod, null);
    }

    void setObjectFromTz(Properties props, String tableName, Object parameter, SQLType targetSqlType, TimeZone senderTz, String expectedUTCValue,
            String expectedUnixTimestamp, UseMethod useMethod, Calendar calendar) throws Exception {
        if (props == null) {
            props = new Properties();
        }
        final TimeZone origTz = TimeZone.getDefault();
        try {
            TimeZone.setDefault(senderTz);
            Connection testConn = this.tzConnections.get(getKey(props));

            id++;

            PreparedStatement localPstmt = testConn.prepareStatement("INSERT INTO " + tableName + " VALUES (?, ?)");
            localPstmt.setInt(1, id);
            switch (useMethod) {
                case setDate:
                    if (calendar == null) {
                        localPstmt.setDate(2, (Date) parameter);
                    } else {
                        localPstmt.setDate(2, (Date) parameter, calendar);
                    }
                    break;

                case setTime:
                    if (calendar == null) {
                        localPstmt.setTime(2, (Time) parameter);
                    } else {
                        localPstmt.setTime(2, (Time) parameter, calendar);
                    }
                    break;

                case setTimestamp:
                    if (calendar == null) {
                        localPstmt.setTimestamp(2, (Timestamp) parameter);
                    } else {
                        localPstmt.setTimestamp(2, (Timestamp) parameter, calendar);
                    }
                    break;

                default:
                    if (targetSqlType == null) {
                        localPstmt.setObject(2, parameter);
                    } else {
                        localPstmt.setObject(2, parameter, targetSqlType);
                    }
                    break;
            }
            assertEquals(1, localPstmt.executeUpdate());
            localPstmt.close();

            if (expectedUTCValue != null) {
                TimeZone.setDefault(tz_UTC);
                testConn = this.utcConnections.get(getKey(props));
                Statement localStmt = testConn.createStatement();
                localStmt.execute("set @@time_zone='+00:00'");
                String sql = "SELECT * FROM " + tableName + " WHERE id = " + id + " AND d = '" + expectedUTCValue + "'"
                        + (expectedUnixTimestamp != null ? " AND unix_timestamp(d) = " + expectedUnixTimestamp : "");
                ResultSet localRs = localStmt.executeQuery(sql);
                assertTrue(localRs.next(), parameter + "\n" + targetSqlType + "\n" + senderTz + "\n" + useMethod + "\n" + sql);
                localStmt.close();
            }
        } finally {
            TimeZone.setDefault(origTz);
        }
    }

    @Test
    public void testDateGetters() throws Exception {
        createTable(tDate, "(d DATE)");
        this.stmt.executeUpdate("INSERT INTO " + tDate + " VALUES ('2020-01-01'), ('2019-12-31')");

        Calendar cal_05 = GregorianCalendar.getInstance(tz_plus_05_00);

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");

        final TimeZone origTz = TimeZone.getDefault();
        try {
            for (TimeZone tz : this.senderTimeZones) {
                for (String connectionTZ : this.connectionTimeZones) {
                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone.setDefault(tz);
                                        ZoneId currZoneId = ZoneId.of(tz.getID());

                                        Connection testConn = getConnectionWithProps(timeZoneFreeDbUrl, props);
                                        this.rs = testConn.createStatement().executeQuery("select * from " + tDate);

                                        assertTrue(this.rs.next());
                                        Instant exp_instant_tz = ldt_20200101_0000.atZone(currZoneId).toInstant();
                                        Instant exp_instant_05 = ldt_20200101_0000.atOffset(ZoneOffset.ofHours(5)).toInstant();

                                        Calendar exp_cal = GregorianCalendar.getInstance(tz);
                                        exp_cal.set(2020, 0, 1, 0, 0, 0);
                                        exp_cal.set(Calendar.MILLISECOND, 0);
                                        exp_cal.setLenient(false);

                                        assertEquals(java.sql.Date.from(exp_instant_tz), this.rs.getDate(1));
                                        assertEquals(java.sql.Date.from(exp_instant_05), this.rs.getDate(1, cal_05));
                                        assertEquals(java.sql.Time.valueOf(lt_000000), this.rs.getTime(1));
                                        assertEquals(java.sql.Time.from(lt_000000.atOffset(ZoneOffset.ofHours(5)).atDate(ld_19700101).toInstant()),
                                                this.rs.getTime(1, cal_05));
                                        assertEquals(java.sql.Timestamp.from(exp_instant_tz), this.rs.getTimestamp(1));
                                        assertEquals(java.sql.Timestamp.from(exp_instant_05), this.rs.getTimestamp(1, cal_05));
                                        assertEquals(java.sql.Date.from(exp_instant_tz), this.rs.getObject(1));
                                        assertEquals(java.sql.Date.from(exp_instant_tz), this.rs.getObject(1, java.sql.Date.class));
                                        assertEquals(java.sql.Time.valueOf(lt_000000), this.rs.getObject(1, java.sql.Time.class));
                                        assertEquals(java.sql.Timestamp.from(exp_instant_tz), this.rs.getObject(1, java.sql.Timestamp.class));
                                        assertEquals(java.util.Date.from(exp_instant_tz), this.rs.getObject(1, java.util.Date.class));
                                        assertEquals(exp_cal, this.rs.getObject(1, java.util.Calendar.class));
                                        assertEquals(ld_20200101, this.rs.getObject(1, LocalDate.class));
                                        assertEquals(lt_000000, this.rs.getObject(1, LocalTime.class));
                                        assertEquals(ldt_20200101_0000, this.rs.getObject(1, LocalDateTime.class));
                                        assertEquals(ldt_20200101_0000.atZone(currZoneId).toOffsetDateTime().toOffsetTime(),
                                                this.rs.getObject(1, OffsetTime.class));
                                        assertEquals(ldt_20200101_0000.atZone(currZoneId).toOffsetDateTime(), this.rs.getObject(1, OffsetDateTime.class));
                                        assertEquals(ldt_20200101_0000.atZone(currZoneId), this.rs.getObject(1, ZonedDateTime.class));
                                        assertEquals(s_20200101, this.rs.getString(1));
                                        assertThrows(SQLException.class,
                                                Messages.getString("ResultSet.UnsupportedConversion", new Object[] { "DATE", Duration.class.getName() }),
                                                () -> {
                                                    this.rs.getObject(1, Duration.class);
                                                    return null;
                                                });

                                        assertTrue(this.rs.next());
                                        exp_instant_tz = ldt_20191231_0000.atZone(currZoneId).toInstant();
                                        exp_instant_05 = ldt_20191231_0000.atOffset(ZoneOffset.ofHours(5)).toInstant();

                                        exp_cal = GregorianCalendar.getInstance(tz);
                                        exp_cal.set(2019, 11, 31, 0, 0, 0);
                                        exp_cal.set(Calendar.MILLISECOND, 0);
                                        exp_cal.setLenient(false);

                                        assertEquals(java.sql.Date.from(exp_instant_tz), this.rs.getDate(1));
                                        assertEquals(java.sql.Date.from(exp_instant_05), this.rs.getDate(1, cal_05));
                                        assertEquals(java.sql.Time.valueOf(lt_000000), this.rs.getTime(1));
                                        assertEquals(java.sql.Time.from(lt_000000.atOffset(ZoneOffset.ofHours(5)).atDate(ld_19700101).toInstant()),
                                                this.rs.getTime(1, cal_05));
                                        assertEquals(java.sql.Timestamp.from(exp_instant_tz), this.rs.getTimestamp(1));
                                        assertEquals(java.sql.Timestamp.from(exp_instant_05), this.rs.getTimestamp(1, cal_05));
                                        assertEquals(java.sql.Date.from(exp_instant_tz), this.rs.getObject(1));
                                        assertEquals(java.sql.Date.from(exp_instant_tz), this.rs.getObject(1, java.sql.Date.class));
                                        assertEquals(java.sql.Time.valueOf(lt_000000), this.rs.getObject(1, java.sql.Time.class));
                                        assertEquals(java.sql.Timestamp.from(exp_instant_tz), this.rs.getObject(1, java.sql.Timestamp.class));
                                        assertEquals(java.util.Date.from(exp_instant_tz), this.rs.getObject(1, java.util.Date.class));
                                        assertEquals(exp_cal, this.rs.getObject(1, java.util.Calendar.class));
                                        assertEquals(ld_20191231, this.rs.getObject(1, LocalDate.class));
                                        assertEquals(lt_000000, this.rs.getObject(1, LocalTime.class));
                                        assertEquals(ldt_20191231_0000, this.rs.getObject(1, LocalDateTime.class));
                                        assertEquals(ldt_20191231_0000.atZone(currZoneId).toOffsetDateTime().toOffsetTime(),
                                                this.rs.getObject(1, OffsetTime.class));
                                        assertEquals(ldt_20191231_0000.atZone(currZoneId).toOffsetDateTime(), this.rs.getObject(1, OffsetDateTime.class));
                                        assertEquals(ldt_20191231_0000.atZone(currZoneId), this.rs.getObject(1, ZonedDateTime.class));
                                        assertEquals(s_20191231, this.rs.getString(1));

                                        testConn.close();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(origTz);
        }
    }

    @Test
    public void testTimeGetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        String dur1 = withFract ? "300:10:20.012300" : "300:10:20";
        String dur2 = withFract ? "-300:10:20.012300" : "-300:10:20";

        createTable(tTime, withFract ? "(d TIME(6))" : "(d TIME)");
        this.stmt.executeUpdate("INSERT INTO " + tTime + " VALUES ('" + lt_120000_123456.toString() + "')");
        this.stmt.executeUpdate("INSERT INTO " + tTime + " VALUES ('" + dur1 + "')");
        this.stmt.executeUpdate("INSERT INTO " + tTime + " VALUES ('" + dur2 + "')");

        Calendar cal_05 = GregorianCalendar.getInstance(tz_plus_05_00);

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");

        final TimeZone origTz = TimeZone.getDefault();
        try {
            for (TimeZone tz : this.senderTimeZones) {
                for (String connectionTZ : this.connectionTimeZones) {
                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone.setDefault(tz);
                                        ZoneId currZoneId = ZoneId.of(tz.getID());

                                        Connection testConn = getConnectionWithProps(timeZoneFreeDbUrl, props);
                                        this.rs = testConn.createStatement().executeQuery("select * from " + tTime);
                                        assertTrue(this.rs.next());

                                        LocalDateTime exp_ldt = withFract ? ldt_19700101_120000_123456 : ldt_19700101_120000_123456.withNano(0);
                                        LocalTime exp_lt = withFract ? lt_120000_123456 : lt_120000;

                                        Instant exp_instant_tz = exp_ldt.atZone(currZoneId).toInstant();
                                        Instant exp_instant_05 = exp_ldt.atOffset(ZoneOffset.ofHours(5)).toInstant();

                                        Calendar exp_cal = GregorianCalendar.getInstance(tz);
                                        exp_cal.clear();
                                        exp_cal.set(Calendar.HOUR_OF_DAY, 12);
                                        exp_cal.set(Calendar.MILLISECOND, withFract ? 123 : 0);
                                        exp_cal.setLenient(false);

                                        assertEquals(java.sql.Date.valueOf(ld_19700101), this.rs.getDate(1));
                                        assertEquals(java.sql.Date.valueOf(ld_19700101), this.rs.getDate(1, cal_05));
                                        assertEquals(java.sql.Time.from(exp_instant_tz), this.rs.getTime(1));
                                        assertEquals(java.sql.Time.from(exp_instant_05), this.rs.getTime(1, cal_05));
                                        assertEquals(java.sql.Timestamp.from(exp_instant_tz), this.rs.getTimestamp(1));
                                        assertEquals(java.sql.Timestamp.from(exp_instant_05), this.rs.getTimestamp(1, cal_05));
                                        assertEquals(java.sql.Time.from(exp_instant_tz), this.rs.getObject(1));
                                        assertEquals(java.sql.Date.valueOf(ld_19700101), this.rs.getObject(1, java.sql.Date.class));
                                        assertEquals(java.sql.Time.from(exp_instant_tz), this.rs.getObject(1, java.sql.Time.class));
                                        assertEquals(java.sql.Timestamp.from(exp_instant_tz), this.rs.getObject(1, java.sql.Timestamp.class));
                                        assertEquals(java.util.Date.from(exp_instant_tz), this.rs.getObject(1, java.util.Date.class));
                                        assertEquals(exp_cal, this.rs.getObject(1, java.util.Calendar.class));
                                        assertEquals(ld_19700101, this.rs.getObject(1, LocalDate.class));
                                        assertEquals(exp_lt, this.rs.getObject(1, LocalTime.class));
                                        assertEquals(exp_ldt, this.rs.getObject(1, LocalDateTime.class));
                                        assertEquals(exp_ldt.atZone(currZoneId).toOffsetDateTime().toOffsetTime(), this.rs.getObject(1, OffsetTime.class));
                                        assertEquals(exp_ldt.atZone(currZoneId).toOffsetDateTime(), this.rs.getObject(1, OffsetDateTime.class));
                                        assertEquals(exp_ldt.atZone(currZoneId), this.rs.getObject(1, ZonedDateTime.class));
                                        assertEquals(withFract ? s_120000_123456 : s_120000, this.rs.getString(1));

                                        assertTrue(this.rs.next());
                                        assertEquals(java.sql.Date.valueOf(ld_19700101), this.rs.getDate(1));
                                        assertEquals(java.sql.Date.valueOf(ld_19700101), this.rs.getDate(1, cal_05));
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getTime(1);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getTime(1, cal_05);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getTimestamp(1);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getTimestamp(1, cal_05);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getObject(1);
                                            return null;
                                        });
                                        assertEquals(java.sql.Date.valueOf(ld_19700101), this.rs.getObject(1, java.sql.Date.class));
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getObject(1, java.sql.Time.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getObject(1, java.sql.Timestamp.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getObject(1, java.util.Date.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getObject(1, java.util.Calendar.class);
                                            return null;
                                        });
                                        assertEquals(ld_19700101, this.rs.getObject(1, LocalDate.class));
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getObject(1, LocalTime.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getObject(1, LocalDateTime.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getObject(1, OffsetTime.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getObject(1, OffsetDateTime.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur1 }), () -> {
                                            this.rs.getObject(1, ZonedDateTime.class);
                                            return null;
                                        });
                                        assertEquals(Duration.parse(withFract ? "PT300H10M20.123S" : "PT300H10M20S"), this.rs.getObject(1, Duration.class));
                                        assertEquals(dur1, this.rs.getString(1));

                                        assertTrue(this.rs.next());
                                        assertEquals(java.sql.Date.valueOf(ld_19700101), this.rs.getDate(1));
                                        assertEquals(java.sql.Date.valueOf(ld_19700101), this.rs.getDate(1, cal_05));
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            DateTimeTest.this.rs.getTime(1);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            this.rs.getTime(1, cal_05);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            this.rs.getTimestamp(1);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            this.rs.getTimestamp(1, cal_05);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            this.rs.getObject(1);
                                            return null;
                                        });
                                        assertEquals(java.sql.Date.valueOf(ld_19700101), this.rs.getObject(1, java.sql.Date.class));
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            this.rs.getObject(1, java.sql.Time.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            this.rs.getObject(1, java.sql.Timestamp.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            this.rs.getObject(1, java.util.Date.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            this.rs.getObject(1, java.util.Calendar.class);
                                            return null;
                                        });
                                        assertEquals(ld_19700101, this.rs.getObject(1, LocalDate.class));
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            this.rs.getObject(1, LocalTime.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            this.rs.getObject(1, LocalDateTime.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            this.rs.getObject(1, OffsetTime.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            this.rs.getObject(1, OffsetDateTime.class);
                                            return null;
                                        });
                                        assertThrows(SQLException.class, Messages.getString("ResultSet.InvalidTimeValue", new Object[] { dur2 }), () -> {
                                            this.rs.getObject(1, ZonedDateTime.class);
                                            return null;
                                        });
                                        assertEquals(Duration.parse(withFract ? "-PT300H10M20.123S" : "-PT300H10M20S"), this.rs.getObject(1, Duration.class));
                                        assertEquals(dur2, this.rs.getString(1));

                                        testConn.close();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(origTz);
        }
    }

    @Test
    public void testTimestampGetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        OffsetDateTime orig_UTC = OffsetDateTime.of(2019, 12, 31, 22, 0, 0, withFract ? 123456000 : 0, ZoneOffset.UTC);

        createTable(tTimestamp, withFract ? "(d TIMESTAMP(6))" : "(d TIMESTAMP)");

        DateTimeFormatter dateTimeFmt = withFract ? DATETIME_FORMATTER_WITH_MICROS_NO_OFFCET : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET;

        Calendar cal_05 = GregorianCalendar.getInstance(tz_plus_05_00);

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(timeZoneFreeDbUrl, props)) {
            this.pstmt = testConn.prepareStatement("INSERT INTO " + tTimestamp + " VALUES (?)");
            this.pstmt.setObject(1, orig_UTC);
            this.pstmt.execute();
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
        }

        final TimeZone origTz = TimeZone.getDefault();
        try {
            for (TimeZone clientTz : this.senderTimeZones) {
                for (String connectionTZ : this.connectionTimeZones) {
                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone.setDefault(clientTz);
                                        ZoneId currZoneId = ZoneId.of(clientTz.getID());

                                        Connection testConn = getConnectionWithProps(timeZoneFreeDbUrl, props);
                                        this.rs = testConn.createStatement().executeQuery("select * from " + tTimestamp);
                                        assertTrue(this.rs.next());

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? clientTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));

                                        TimeZone sessionTz = forceConnectionTimeZoneToSession ? connTz : serverTz;

                                        ZonedDateTime exp_on_wire = orig_UTC.atZoneSameInstant(ZoneId.of(sessionTz.getID()));

                                        ZonedDateTime exp_date = exp_on_wire.withHour(0).withNano(0).withZoneSameLocal(currZoneId);
                                        Instant exp_date_with_cal = exp_on_wire.withHour(0).withNano(0).toOffsetDateTime()
                                                .withOffsetSameLocal(ZoneOffset.ofHours(5)).toInstant();

                                        ZonedDateTime exp_time = exp_on_wire.withYear(1970).withMonth(1).withDayOfMonth(1).withZoneSameLocal(currZoneId);
                                        Instant exp_time_with_cal = exp_on_wire.withYear(1970).withMonth(1).withDayOfMonth(1).toOffsetDateTime()
                                                .withOffsetSameLocal(ZoneOffset.ofHours(5)).toInstant();

                                        ZonedDateTime exp_datetime = exp_on_wire.withZoneSameLocal(currZoneId);
                                        ZonedDateTime exp_timestamp = preserveInstants
                                                ? exp_on_wire.withZoneSameLocal(connTz.toZoneId()).withZoneSameInstant(currZoneId)
                                                : exp_on_wire.withZoneSameLocal(currZoneId);

                                        Instant exp_timestamp_with_cal = exp_on_wire.toOffsetDateTime().withOffsetSameLocal(ZoneOffset.ofHours(5)).toInstant();

                                        ZonedDateTime exp_odt = exp_on_wire.withZoneSameLocal(preserveInstants ? connTz.toZoneId() : currZoneId);

                                        Calendar exp_cal = GregorianCalendar.getInstance(preserveInstants ? connTz : clientTz);
                                        exp_cal.set(exp_on_wire.getYear(), exp_on_wire.getMonthValue() - 1, exp_on_wire.getDayOfMonth(), exp_on_wire.getHour(),
                                                exp_on_wire.getMinute(), exp_on_wire.getSecond());
                                        exp_cal.set(Calendar.MILLISECOND, exp_on_wire.getNano() / 1000000);
                                        exp_cal.setLenient(false);

                                        assertEquals(java.sql.Date.from(exp_date.toInstant()), this.rs.getDate(1));
                                        assertEquals(java.sql.Date.from(exp_date_with_cal), this.rs.getDate(1, cal_05));
                                        assertEquals(java.sql.Time.from(exp_time.toInstant()), this.rs.getTime(1));
                                        assertEquals(java.sql.Time.from(exp_time_with_cal), this.rs.getTime(1, cal_05));
                                        assertEquals(java.sql.Timestamp.from(exp_timestamp.toInstant()), this.rs.getTimestamp(1));
                                        assertEquals(java.sql.Timestamp.from(exp_timestamp_with_cal), this.rs.getTimestamp(1, cal_05));
                                        assertEquals(java.sql.Timestamp.from(exp_timestamp.toInstant()), this.rs.getObject(1));
                                        assertEquals(java.sql.Date.from(exp_date.toInstant()), this.rs.getObject(1, java.sql.Date.class));
                                        assertEquals(java.sql.Time.from(exp_time.toInstant()), this.rs.getObject(1, java.sql.Time.class));
                                        assertEquals(java.sql.Timestamp.from(exp_timestamp.toInstant()), this.rs.getObject(1, java.sql.Timestamp.class));
                                        assertEquals(java.util.Date.from(exp_timestamp.toInstant()), this.rs.getObject(1, java.util.Date.class));
                                        assertEquals(exp_cal, this.rs.getObject(1, java.util.Calendar.class));
                                        assertEquals(exp_datetime.toLocalDate(), this.rs.getObject(1, LocalDate.class));
                                        assertEquals(exp_time.toLocalTime(), this.rs.getObject(1, LocalTime.class));
                                        assertEquals(exp_datetime.toLocalDateTime(), this.rs.getObject(1, LocalDateTime.class));
                                        assertEquals(exp_datetime.toOffsetDateTime().toOffsetTime(), this.rs.getObject(1, OffsetTime.class));
                                        assertEquals(exp_odt.toOffsetDateTime(), this.rs.getObject(1, OffsetDateTime.class));
                                        assertEquals(exp_odt, this.rs.getObject(1, ZonedDateTime.class));
                                        assertEquals(exp_on_wire.toLocalDateTime().format(dateTimeFmt), this.rs.getString(1));
                                        assertThrows(SQLException.class,
                                                Messages.getString("ResultSet.UnsupportedConversion", new Object[] { "TIMESTAMP", Duration.class.getName() }),
                                                () -> {
                                                    this.rs.getObject(1, Duration.class);
                                                    return null;
                                                });

                                        testConn.close();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(origTz);
        }
    }

    @Test
    public void testDatetimeGetters() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions
        LocalDateTime ldt_20200101_020000_123456 = LocalDateTime.of(2020, 1, 1, 2, 0, 0, withFract ? 123456000 : 0);
        DateTimeFormatter dateTimeFmt = withFract ? DATETIME_FORMATTER_WITH_MICROS_NO_OFFCET : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET;
        OffsetDateTime orig_UTC = OffsetDateTime.of(2020, 1, 1, 2, 0, 0, withFract ? 123456000 : 0, ZoneOffset.UTC);

        Calendar cal_05 = GregorianCalendar.getInstance(tz_plus_05_00);

        createTable(tDatetime, withFract ? "(d DATETIME(6))" : "(d DATETIME)");
        this.stmt.executeUpdate("INSERT INTO " + tDatetime + " VALUES ('" + ldt_20200101_020000_123456.toString() + "')");

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(timeZoneFreeDbUrl, props)) {
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
        }

        final TimeZone origTz = TimeZone.getDefault();
        try {
            for (TimeZone clientTz : this.senderTimeZones) {
                for (String connectionTZ : this.connectionTimeZones) {
                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        TimeZone.setDefault(clientTz);
                                        ZoneId currZoneId = ZoneId.of(clientTz.getID());

                                        Connection testConn = getConnectionWithProps(timeZoneFreeDbUrl, props);
                                        this.rs = testConn.createStatement().executeQuery("select * from " + tDatetime);
                                        assertTrue(this.rs.next());

                                        Instant exp_date_instant_tz = ldt_20200101_0000.atZone(currZoneId).toInstant();
                                        Instant exp_date_instant_05 = ldt_20200101_0000.atOffset(ZoneOffset.ofHours(5)).toInstant();

                                        Instant exp_time_instant_tz = (withFract ? ldt_19700101_020000_123000 : ldt_19700101_020000_123000.withNano(0))
                                                .atZone(currZoneId).toInstant();
                                        Instant exp_time_instant_05 = (withFract ? ldt_19700101_020000_123000 : ldt_19700101_020000_123000.withNano(0))
                                                .atOffset(ZoneOffset.ofHours(5)).toInstant();

                                        Instant exp_timestamp_instant_05 = ldt_20200101_020000_123456.atOffset(ZoneOffset.ofHours(5)).toInstant();

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? clientTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));
                                        TimeZone sessionTz = forceConnectionTimeZoneToSession ? connTz : serverTz;
                                        ZonedDateTime exp_on_wire = orig_UTC.atZoneSimilarLocal(ZoneId.of(sessionTz.getID()));
                                        Instant exp_timestamp_instant = (preserveInstants
                                                ? exp_on_wire.withZoneSameLocal(connTz.toZoneId()).withZoneSameInstant(currZoneId)
                                                : exp_on_wire.withZoneSameLocal(currZoneId)).toInstant();

                                        Calendar exp_cal = GregorianCalendar.getInstance(preserveInstants ? connTz : clientTz);
                                        exp_cal.set(exp_on_wire.getYear(), exp_on_wire.getMonthValue() - 1, exp_on_wire.getDayOfMonth(), exp_on_wire.getHour(),
                                                exp_on_wire.getMinute(), exp_on_wire.getSecond());
                                        exp_cal.set(Calendar.MILLISECOND, exp_on_wire.getNano() / 1000000);
                                        exp_cal.setLenient(false);

                                        assertEquals(java.sql.Date.from(exp_date_instant_tz), this.rs.getDate(1));
                                        assertEquals(java.sql.Date.from(exp_date_instant_05), this.rs.getDate(1, cal_05));
                                        assertEquals(java.sql.Time.from(exp_time_instant_tz), this.rs.getTime(1));
                                        assertEquals(java.sql.Time.from(exp_time_instant_05), this.rs.getTime(1, cal_05));
                                        assertEquals(java.sql.Timestamp.from(exp_timestamp_instant), this.rs.getTimestamp(1));
                                        assertEquals(java.sql.Timestamp.from(exp_timestamp_instant_05), this.rs.getTimestamp(1, cal_05));
                                        assertEquals(ldt_20200101_020000_123456, this.rs.getObject(1));
                                        assertEquals(java.sql.Date.from(exp_date_instant_tz), this.rs.getObject(1, java.sql.Date.class));
                                        assertEquals(java.sql.Time.from(exp_time_instant_tz), this.rs.getObject(1, java.sql.Time.class));
                                        assertEquals(java.sql.Timestamp.from(exp_timestamp_instant), this.rs.getObject(1, java.sql.Timestamp.class));
                                        assertEquals(java.util.Date.from(exp_timestamp_instant), this.rs.getObject(1, java.util.Date.class));
                                        assertEquals(exp_cal, this.rs.getObject(1, java.util.Calendar.class));
                                        assertEquals(ldt_20200101_020000_123456.toLocalDate(), this.rs.getObject(1, LocalDate.class));
                                        assertEquals(ldt_20200101_020000_123456.toLocalTime(), this.rs.getObject(1, LocalTime.class));
                                        assertEquals(ldt_20200101_020000_123456, this.rs.getObject(1, LocalDateTime.class));
                                        assertEquals((withFract ? ldt_19700101_020000_123456 : ldt_19700101_020000_123456.withNano(0)).atZone(currZoneId)
                                                .toOffsetDateTime().toOffsetTime(), this.rs.getObject(1, OffsetTime.class));
                                        assertEquals(ldt_20200101_020000_123456.atZone(preserveInstants ? connTz.toZoneId() : currZoneId).toOffsetDateTime(),
                                                this.rs.getObject(1, OffsetDateTime.class));
                                        assertEquals(ldt_20200101_020000_123456.atZone(preserveInstants ? connTz.toZoneId() : currZoneId),
                                                this.rs.getObject(1, ZonedDateTime.class));
                                        assertEquals(ldt_20200101_020000_123456.format(dateTimeFmt), this.rs.getString(1));
                                        assertThrows(SQLException.class,
                                                Messages.getString("ResultSet.UnsupportedConversion", new Object[] { "DATETIME", Duration.class.getName() }),
                                                () -> {
                                                    this.rs.getObject(1, Duration.class);
                                                    return null;
                                                });

                                        testConn.close();
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(origTz);
        }
    }

    @Test
    public void testYearGetters() throws Exception {
        createTable(tYear, "(d YEAR, s VARCHAR(30))");
        this.stmt.executeUpdate("INSERT INTO " + tYear + " VALUES ('" + s_2020 + "', '" + s_2020 + "')");

        Calendar cal_05 = GregorianCalendar.getInstance(tz_plus_05_00);

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");

        final TimeZone origTz = TimeZone.getDefault();
        try {
            for (boolean yearIsDateType : new boolean[] { true, false }) {
                for (TimeZone tz : this.senderTimeZones) {
                    for (String connectionTZ : this.connectionTimeZones) {
                        for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                            for (boolean preserveInstants : new boolean[] { false, true }) {
                                for (boolean useSSPS : new boolean[] { false, true }) {
                                    for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                        for (boolean sendTimeFract : new boolean[] { false, true }) {

                                            System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession="
                                                    + forceConnectionTimeZoneToSession + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts="
                                                    + useSSPS + "; sendFractSeconds=" + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                            props.setProperty(PropertyKey.yearIsDateType.getKeyName(), "" + yearIsDateType);
                                            if (connectionTZ == null) {
                                                props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                            } else {
                                                props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                            }
                                            props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                            props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                            props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                            props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                            props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                            TimeZone.setDefault(tz);
                                            ZoneId currZoneId = ZoneId.of(tz.getID());

                                            Connection testConn = getConnectionWithProps(timeZoneFreeDbUrl, props);
                                            ResultSet rs1 = testConn.createStatement().executeQuery("select * from " + tYear);

                                            assertTrue(rs1.next());
                                            Instant exp_instant_tz = ldt_20200101_0000.atZone(currZoneId).toInstant();
                                            Instant exp_instant_05 = ldt_20200101_0000.atOffset(ZoneOffset.ofHours(5)).toInstant();

                                            Calendar exp_cal = GregorianCalendar.getInstance(tz);
                                            exp_cal.set(2020, 0, 1, 0, 0, 0);
                                            exp_cal.set(Calendar.MILLISECOND, 0);
                                            exp_cal.setLenient(false);

                                            if (yearIsDateType) {
                                                assertEquals(java.sql.Date.from(exp_instant_tz), rs1.getDate(1));
                                                assertEquals(java.sql.Date.from(exp_instant_05), rs1.getDate(1, cal_05));
                                                assertEquals(java.sql.Time.valueOf(lt_000000), rs1.getTime(1));
                                                assertEquals(java.sql.Time.from(lt_000000.atOffset(ZoneOffset.ofHours(5)).atDate(ld_19700101).toInstant()),
                                                        rs1.getTime(1, cal_05));
                                                assertEquals(java.sql.Timestamp.from(exp_instant_tz), rs1.getTimestamp(1));
                                                assertEquals(java.sql.Timestamp.from(exp_instant_05), rs1.getTimestamp(1, cal_05));
                                                assertEquals(java.sql.Date.from(exp_instant_tz), rs1.getObject(1));
                                                assertEquals(java.sql.Date.from(exp_instant_tz), rs1.getObject(1, java.sql.Date.class));
                                                assertEquals(java.sql.Time.valueOf(lt_000000), rs1.getObject(1, java.sql.Time.class));
                                                assertEquals(java.sql.Timestamp.from(exp_instant_tz), rs1.getObject(1, java.sql.Timestamp.class));
                                                assertEquals(java.util.Date.from(exp_instant_tz), rs1.getObject(1, java.util.Date.class));
                                                assertEquals(exp_cal, rs1.getObject(1, java.util.Calendar.class));
                                                assertEquals(ld_20200101, rs1.getObject(1, LocalDate.class));
                                                assertEquals(lt_000000, rs1.getObject(1, LocalTime.class));
                                                assertEquals(ldt_20200101_0000, rs1.getObject(1, LocalDateTime.class));
                                                assertEquals(ldt_20200101_0000.atZone(currZoneId).toOffsetDateTime().toOffsetTime(),
                                                        rs1.getObject(1, OffsetTime.class));
                                                assertEquals(ldt_20200101_0000.atZone(currZoneId).toOffsetDateTime(), rs1.getObject(1, OffsetDateTime.class));
                                                assertEquals(ldt_20200101_0000.atZone(currZoneId), rs1.getObject(1, ZonedDateTime.class));
                                                assertEquals(s_20200101, rs1.getString(1));
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "DATE", Duration.class.getName() }), () -> {
                                                            rs1.getObject(1, Duration.class);
                                                            return null;
                                                        });
                                            } else {
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", java.sql.Date.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getDate(1);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", java.sql.Date.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getDate(1, cal_05);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", java.sql.Time.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getTime(1);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", java.sql.Time.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getTime(1, cal_05);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", java.sql.Timestamp.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getTimestamp(1);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", java.sql.Timestamp.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getTimestamp(1, cal_05);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", java.sql.Date.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getObject(1, java.sql.Date.class);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", java.sql.Time.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getObject(1, java.sql.Time.class);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", java.sql.Timestamp.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getObject(1, java.sql.Timestamp.class);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", java.sql.Timestamp.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getObject(1, java.util.Date.class);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", Calendar.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getObject(1, java.util.Calendar.class);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", LocalDate.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getObject(1, LocalDate.class);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", LocalTime.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getObject(1, LocalTime.class);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", LocalDateTime.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getObject(1, LocalDateTime.class);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", OffsetTime.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getObject(1, OffsetTime.class);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", OffsetDateTime.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getObject(1, OffsetDateTime.class);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", ZonedDateTime.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getObject(1, ZonedDateTime.class);
                                                                return null;
                                                            }
                                                        });
                                                assertThrows(SQLException.class, Messages.getString("ResultSet.UnsupportedConversion",
                                                        new Object[] { "LONG", Duration.class.getName() }), new Callable<Void>() {
                                                            public Void call() throws Exception {
                                                                rs1.getObject(1, Duration.class);
                                                                return null;
                                                            }
                                                        });
                                                assertEquals(s_2020, rs1.getString(1));
                                                assertEquals(Short.valueOf((short) 2020), rs1.getObject(1));
                                            }

                                            assertEquals(s_2020, rs1.getObject(2));
                                            assertEquals(s_2020, rs1.getString(2));

                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, java.sql.Date.class.getName() }), new Callable<Void>() {

                                                        public Void call() throws Exception {
                                                            rs1.getDate(2);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, java.sql.Date.class.getName() }), new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getDate(2, cal_05);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, java.sql.Time.class.getName() }), new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getTime(2);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, java.sql.Time.class.getName() }), new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getTime(2, cal_05);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, java.sql.Timestamp.class.getName() }), new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getTimestamp(2, cal_05);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, java.sql.Timestamp.class.getName() }), new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getTimestamp(2, cal_05);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, java.sql.Date.class.getName() }), new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getObject(2, java.sql.Date.class);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, java.sql.Time.class.getName() }), new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getObject(2, java.sql.Time.class);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, java.sql.Timestamp.class.getName() }), new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getObject(2, java.sql.Timestamp.class);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, java.sql.Timestamp.class.getName() }), new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getObject(2, java.util.Date.class);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, java.util.Calendar.class.getName() }), new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getObject(2, java.util.Calendar.class);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class,
                                                    Messages.getString("ResultSet.UnableToConvertString", new Object[] { s_2020, LocalDate.class.getName() }),
                                                    new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getObject(2, LocalDate.class);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class,
                                                    Messages.getString("ResultSet.UnableToConvertString", new Object[] { s_2020, LocalTime.class.getName() }),
                                                    new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getObject(2, LocalTime.class);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, LocalDateTime.class.getName() }), new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getObject(2, LocalDateTime.class);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class,
                                                    Messages.getString("ResultSet.UnableToConvertString", new Object[] { s_2020, OffsetTime.class.getName() }),
                                                    new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getObject(2, OffsetTime.class);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, OffsetDateTime.class.getName() }), new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getObject(2, OffsetDateTime.class);
                                                            return null;
                                                        }
                                                    });
                                            assertThrows(SQLException.class, Messages.getString("ResultSet.UnableToConvertString",
                                                    new Object[] { s_2020, ZonedDateTime.class.getName() }), new Callable<Void>() {
                                                        public Void call() throws Exception {
                                                            rs1.getObject(2, ZonedDateTime.class);
                                                            return null;
                                                        }
                                                    });

                                            testConn.close();
                                        }

                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            TimeZone.setDefault(origTz);
        }
    }

    @Test
    public void testSymmetricInstantRetrieval() throws Exception {
        boolean withFract = versionMeetsMinimum(5, 6, 4); // fractional seconds are not supported in previous versions

        createTable(tDatetime, withFract ? "(id INT, d DATETIME(6))" : "(id INT, d DATETIME)");
        createTable(tTimestamp, withFract ? "(id INT, d TIMESTAMP(6))" : "(id INT, d TIMESTAMP)");
        createTable(tVarchar, "(id INT, d VARCHAR(30))");

        id = 0;

        Properties props = new Properties();
        props.setProperty(PropertyKey.sslMode.getKeyName(), "DISABLED");
        props.setProperty(PropertyKey.allowPublicKeyRetrieval.getKeyName(), "true");
        props.setProperty(PropertyKey.cacheDefaultTimeZone.getKeyName(), "false");
        props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), "SERVER");

        TimeZone serverTz;
        try (Connection testConn = getConnectionWithProps(props)) {
            serverTz = ((MysqlConnection) testConn).getSession().getServerSession().getSessionTimeZone();
        }

        Calendar cal_02 = GregorianCalendar.getInstance(tz_plus_02_00);

        for (TimeZone senderTz : this.senderTimeZones) {
            try {
                for (String connectionTZ : this.connectionTimeZones) {
                    initConnections(senderTz, connectionTZ);

                    for (boolean forceConnectionTimeZoneToSession : new boolean[] { false, true }) {
                        for (boolean preserveInstants : new boolean[] { false, true }) {
                            for (boolean useSSPS : new boolean[] { false, true }) {
                                for (boolean sendFractionalSeconds : new boolean[] { false, true }) {
                                    for (boolean sendTimeFract : new boolean[] { false, true }) {

                                        System.out.println("connTimeZone=" + connectionTZ + "; forceConnTimeZoneToSession=" + forceConnectionTimeZoneToSession
                                                + "; preserveInstants=" + preserveInstants + "; useServerPrepStmts=" + useSSPS + "; sendFractSeconds="
                                                + sendFractionalSeconds + "; sendFractSecondsForTime=" + sendTimeFract);

                                        if (connectionTZ == null) {
                                            props.remove(PropertyKey.connectionTimeZone.getKeyName());
                                        } else {
                                            props.setProperty(PropertyKey.connectionTimeZone.getKeyName(), connectionTZ);
                                        }
                                        props.setProperty(PropertyKey.forceConnectionTimeZoneToSession.getKeyName(), "" + forceConnectionTimeZoneToSession);
                                        props.setProperty(PropertyKey.preserveInstants.getKeyName(), "" + preserveInstants);
                                        props.setProperty(PropertyKey.useServerPrepStmts.getKeyName(), "" + useSSPS);
                                        props.setProperty(PropertyKey.sendFractionalSecondsForTime.getKeyName(), "" + sendTimeFract);
                                        props.setProperty(PropertyKey.sendFractionalSeconds.getKeyName(), "" + sendFractionalSeconds);

                                        ZonedDateTime zdt_20200101_120000_123456_at_senderTz = (withFract ? ldt_20200101_120000_123456
                                                : ldt_20200101_120000_123456.withNano(0)).atZone(senderTz.toZoneId());

                                        java.sql.Timestamp ts = java.sql.Timestamp.from(zdt_20200101_120000_123456_at_senderTz.toInstant());
                                        java.util.Date ud = java.util.Date.from(zdt_20200101_120000_123456_at_senderTz.toInstant());

                                        java.sql.Timestamp expTsNoFract = java.sql.Timestamp
                                                .from(zdt_20200101_120000_123456_at_senderTz.withNano(0).toInstant());
                                        java.sql.Timestamp expTsNanos = sendFractionalSeconds
                                                ? java.sql.Timestamp.from(zdt_20200101_120000_123456_at_senderTz.toInstant())
                                                : expTsNoFract;
                                        java.sql.Timestamp expTsMillis = sendFractionalSeconds
                                                ? java.sql.Timestamp.from(zdt_20200101_120000_123456_at_senderTz.truncatedTo(ChronoUnit.MILLIS).toInstant())
                                                : expTsNoFract;

                                        java.util.Date expUdNoFract = java.util.Date.from(zdt_20200101_120000_123456_at_senderTz.withNano(0).toInstant());
                                        java.util.Date expUd = sendFractionalSeconds ? java.util.Date.from(zdt_20200101_120000_123456_at_senderTz.toInstant())
                                                : expUdNoFract;

                                        TimeZone connTz = connectionTZ == null || "LOCAL".equals(connectionTZ) ? senderTz
                                                : ("SERVER".equals(connectionTZ) ? serverTz : TimeZone.getTimeZone(connectionTZ));
                                        ZonedDateTime expZdtNoFract = zdt_20200101_120000_123456_at_senderTz.withNano(0)
                                                .withZoneSameInstant(preserveInstants ? connTz.toZoneId() : senderTz.toZoneId());
                                        ZonedDateTime expZdt = sendFractionalSeconds
                                                ? zdt_20200101_120000_123456_at_senderTz
                                                        .withZoneSameInstant(preserveInstants ? connTz.toZoneId() : senderTz.toZoneId())
                                                : expZdtNoFract;

                                        /* java.sql.Timestamp into DATETIME field */

                                        checkSymmetricSetAndGet(props, tDatetime, ts, null, senderTz, UseMethod.setTimestamp, cal_02, UseMethod.getTimestamp,
                                                null, expTsNanos);
                                        checkSymmetricSetAndGet(props, tDatetime, ts, null, senderTz, UseMethod.setTimestamp, null, UseMethod.getTimestamp,
                                                null, expTsNanos);
                                        checkSymmetricSetAndGet(props, tDatetime, ts, null, senderTz, UseMethod.setTimestamp, null, UseMethod.getObject,
                                                Timestamp.class, expTsNanos);
                                        checkSymmetricSetAndGet(props, tDatetime, ts, null, senderTz, UseMethod.setObject, null, UseMethod.getTimestamp, null,
                                                expTsNanos);
                                        checkSymmetricSetAndGet(props, tDatetime, ts, null, senderTz, UseMethod.setObject, null, UseMethod.getObject,
                                                Timestamp.class, expTsNanos);
                                        checkSymmetricSetAndGet(props, tDatetime, ts, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getTimestamp, null, expTsNanos);
                                        checkSymmetricSetAndGet(props, tDatetime, ts, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getObject, Timestamp.class, expTsNanos);

                                        /* java.util.Date into DATETIME field */

                                        checkSymmetricSetAndGet(props, tDatetime, ud, null, senderTz, UseMethod.setObject, null, UseMethod.getTimestamp, null,
                                                expTsMillis);
                                        checkSymmetricSetAndGet(props, tDatetime, ud, null, senderTz, UseMethod.setObject, null, UseMethod.getObject,
                                                Timestamp.class, expTsMillis);
                                        checkSymmetricSetAndGet(props, tDatetime, ud, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getTimestamp, null, expTsMillis);
                                        checkSymmetricSetAndGet(props, tDatetime, ud, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getObject, Timestamp.class, expTsMillis);
                                        checkSymmetricSetAndGet(props, tDatetime, ud, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getObject, java.util.Date.class, expUd);

                                        /* java.time.ZonedDateTime into DATETIME field */

                                        checkSymmetricSetAndGet(props, tDatetime, zdt_20200101_120000_123456_at_senderTz, null, senderTz, UseMethod.setObject,
                                                null, UseMethod.getTimestamp, null, expTsNanos);
                                        checkSymmetricSetAndGet(props, tDatetime, zdt_20200101_120000_123456_at_senderTz, null, senderTz, UseMethod.setObject,
                                                null, UseMethod.getObject, Timestamp.class, expTsNanos);
                                        checkSymmetricSetAndGet(props, tDatetime, zdt_20200101_120000_123456_at_senderTz, MysqlType.TIMESTAMP, senderTz,
                                                UseMethod.setObject, null, UseMethod.getTimestamp, null, expTsNanos);
                                        checkSymmetricSetAndGet(props, tDatetime, zdt_20200101_120000_123456_at_senderTz, MysqlType.TIMESTAMP, senderTz,
                                                UseMethod.setObject, null, UseMethod.getObject, Timestamp.class, expTsNanos);
                                        checkSymmetricSetAndGet(props, tDatetime, zdt_20200101_120000_123456_at_senderTz, MysqlType.TIMESTAMP, senderTz,
                                                UseMethod.setObject, null, UseMethod.getObject, java.util.Date.class, expUd);
                                        checkSymmetricSetAndGet(props, tDatetime, zdt_20200101_120000_123456_at_senderTz, MysqlType.TIMESTAMP, senderTz,
                                                UseMethod.setObject, null, UseMethod.getObject, ZonedDateTime.class, expZdt);

                                        /* java.sql.Timestamp into TIMESTAMP field */

                                        checkSymmetricSetAndGet(props, tTimestamp, ts, null, senderTz, UseMethod.setTimestamp, cal_02, UseMethod.getTimestamp,
                                                null, expTsNanos);
                                        checkSymmetricSetAndGet(props, tTimestamp, ts, null, senderTz, UseMethod.setTimestamp, null, UseMethod.getTimestamp,
                                                null, expTsNanos);
                                        checkSymmetricSetAndGet(props, tTimestamp, ts, null, senderTz, UseMethod.setTimestamp, null, UseMethod.getObject, null,
                                                expTsNanos);
                                        checkSymmetricSetAndGet(props, tTimestamp, ts, null, senderTz, UseMethod.setTimestamp, null, UseMethod.getObject,
                                                Timestamp.class, expTsNanos);
                                        checkSymmetricSetAndGet(props, tTimestamp, ts, null, senderTz, UseMethod.setObject, null, UseMethod.getTimestamp, null,
                                                expTsNanos);
                                        checkSymmetricSetAndGet(props, tTimestamp, ts, null, senderTz, UseMethod.setObject, null, UseMethod.getObject, null,
                                                expTsNanos);
                                        checkSymmetricSetAndGet(props, tTimestamp, ts, null, senderTz, UseMethod.setObject, null, UseMethod.getObject,
                                                Timestamp.class, expTsNanos);
                                        checkSymmetricSetAndGet(props, tTimestamp, ts, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getTimestamp, null, expTsNanos);
                                        checkSymmetricSetAndGet(props, tTimestamp, ts, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getObject, null, expTsNanos);
                                        checkSymmetricSetAndGet(props, tTimestamp, ts, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getObject, Timestamp.class, expTsNanos);

                                        /* java.util.Date into TIMESTAMP field */

                                        checkSymmetricSetAndGet(props, tTimestamp, ud, null, senderTz, UseMethod.setObject, null, UseMethod.getTimestamp, null,
                                                expTsMillis);
                                        checkSymmetricSetAndGet(props, tTimestamp, ud, null, senderTz, UseMethod.setObject, null, UseMethod.getObject, null,
                                                expTsMillis);
                                        checkSymmetricSetAndGet(props, tTimestamp, ud, null, senderTz, UseMethod.setObject, null, UseMethod.getObject,
                                                Timestamp.class, expTsMillis);
                                        checkSymmetricSetAndGet(props, tTimestamp, ud, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getTimestamp, null, expTsMillis);
                                        checkSymmetricSetAndGet(props, tTimestamp, ud, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getObject, null, expTsMillis);
                                        checkSymmetricSetAndGet(props, tTimestamp, ud, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getObject, Timestamp.class, expTsMillis);
                                        checkSymmetricSetAndGet(props, tTimestamp, ud, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getObject, java.util.Date.class, expUd);

                                        /* java.time.ZonedDateTime into TIMESTAMP field */

                                        checkSymmetricSetAndGet(props, tTimestamp, zdt_20200101_120000_123456_at_senderTz, null, senderTz, UseMethod.setObject,
                                                null, UseMethod.getTimestamp, null, expTsNanos);
                                        checkSymmetricSetAndGet(props, tTimestamp, zdt_20200101_120000_123456_at_senderTz, null, senderTz, UseMethod.setObject,
                                                null, UseMethod.getObject, Timestamp.class, expTsNanos);
                                        checkSymmetricSetAndGet(props, tTimestamp, zdt_20200101_120000_123456_at_senderTz, MysqlType.TIMESTAMP, senderTz,
                                                UseMethod.setObject, null, UseMethod.getTimestamp, null, expTsNanos);
                                        checkSymmetricSetAndGet(props, tTimestamp, zdt_20200101_120000_123456_at_senderTz, MysqlType.TIMESTAMP, senderTz,
                                                UseMethod.setObject, null, UseMethod.getObject, Timestamp.class, expTsNanos);
                                        checkSymmetricSetAndGet(props, tTimestamp, zdt_20200101_120000_123456_at_senderTz, MysqlType.TIMESTAMP, senderTz,
                                                UseMethod.setObject, null, UseMethod.getObject, java.util.Date.class, expUd);
                                        checkSymmetricSetAndGet(props, tTimestamp, zdt_20200101_120000_123456_at_senderTz, MysqlType.TIMESTAMP, senderTz,
                                                UseMethod.setObject, null, UseMethod.getObject, ZonedDateTime.class, expZdt);

                                        /* java.sql.Timestamp into VARCHAR field */

                                        Timestamp expTsNanosFromChars = useSSPS ? expTsNoFract : expTsNanos; // TODO milliseconds are ignored by server. Bug ?
                                        Timestamp expTsMillisFromChars = useSSPS ? expTsNoFract : expTsMillis; // TODO milliseconds are ignored by server. Bug ?
                                        java.util.Date expUdFromChars = useSSPS ? expUdNoFract : expUd; // TODO milliseconds are ignored by server. Bug ?
                                        ZonedDateTime expZdtFromChars = useSSPS ? expZdtNoFract : expZdt; // TODO milliseconds are ignored by server. Bug ?

                                        checkSymmetricSetAndGet(props, tVarchar, ts, null, senderTz, UseMethod.setTimestamp, cal_02, UseMethod.getTimestamp,
                                                null, expTsNanosFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, ts, null, senderTz, UseMethod.setTimestamp, null, UseMethod.getTimestamp, null,
                                                expTsNanosFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, ts, null, senderTz, UseMethod.setTimestamp, null, UseMethod.getObject,
                                                Timestamp.class, expTsNanosFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, ts, null, senderTz, UseMethod.setObject, null, UseMethod.getTimestamp, null,
                                                expTsNanosFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, ts, null, senderTz, UseMethod.setObject, null, UseMethod.getObject,
                                                Timestamp.class, expTsNanosFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, ts, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getTimestamp, null, expTsNanosFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, ts, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getObject, Timestamp.class, expTsNanosFromChars);

                                        /* java.util.Date into VARCHAR field */

                                        checkSymmetricSetAndGet(props, tVarchar, ud, null, senderTz, UseMethod.setObject, null, UseMethod.getTimestamp, null,
                                                expTsMillisFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, ud, null, senderTz, UseMethod.setObject, null, UseMethod.getObject,
                                                Timestamp.class, expTsMillisFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, ud, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getTimestamp, null, expTsMillisFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, ud, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getObject, Timestamp.class, expTsMillisFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, ud, MysqlType.TIMESTAMP, senderTz, UseMethod.setObject, null,
                                                UseMethod.getObject, java.util.Date.class, expUdFromChars);

                                        /* java.time.ZonedDateTime into VARCHAR field */

                                        checkSymmetricSetAndGet(props, tVarchar, zdt_20200101_120000_123456_at_senderTz, null, senderTz, UseMethod.setObject,
                                                null, UseMethod.getTimestamp, null, expTsNanosFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, zdt_20200101_120000_123456_at_senderTz, null, senderTz, UseMethod.setObject,
                                                null, UseMethod.getObject, Timestamp.class, expTsNanosFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, zdt_20200101_120000_123456_at_senderTz, MysqlType.TIMESTAMP, senderTz,
                                                UseMethod.setObject, null, UseMethod.getTimestamp, null, expTsNanosFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, zdt_20200101_120000_123456_at_senderTz, MysqlType.TIMESTAMP, senderTz,
                                                UseMethod.setObject, null, UseMethod.getObject, Timestamp.class, expTsNanosFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, zdt_20200101_120000_123456_at_senderTz, MysqlType.TIMESTAMP, senderTz,
                                                UseMethod.setObject, null, UseMethod.getObject, java.util.Date.class, expUdFromChars);
                                        checkSymmetricSetAndGet(props, tVarchar, zdt_20200101_120000_123456_at_senderTz, MysqlType.TIMESTAMP, senderTz,
                                                UseMethod.setObject, null, UseMethod.getObject, ZonedDateTime.class, expZdtFromChars);

                                    }
                                }
                            }
                        }
                    }
                    closeConnections();
                }
            } finally {
                closeConnections();
            }
        }
    }

    private void checkSymmetricSetAndGet(Properties props, String tableName, Object parameter, SQLType targetSqlType, TimeZone senderTz, UseMethod useSetter,
            Calendar calendar, UseMethod useGetter, Class<?> targetClass, Object expResult) throws Exception {
        if (props == null) {
            props = new Properties();
        }
        final TimeZone origTz = TimeZone.getDefault();
        try {
            TimeZone.setDefault(senderTz);
            Connection testConn = this.tzConnections.get(getKey(props));

            id++;

            PreparedStatement insertPstmt = testConn.prepareStatement("INSERT INTO " + tableName + " VALUES (?, ?)");
            insertPstmt.setInt(1, id);
            switch (useSetter) {
                case setTimestamp:
                    if (calendar == null) {
                        insertPstmt.setTimestamp(2, (Timestamp) parameter);
                    } else {
                        insertPstmt.setTimestamp(2, (Timestamp) parameter, calendar);
                    }
                    break;

                default:
                    if (targetSqlType == null) {
                        insertPstmt.setObject(2, parameter);
                    } else {
                        insertPstmt.setObject(2, parameter, targetSqlType);
                    }
                    break;
            }
            assertEquals(1, insertPstmt.executeUpdate());

            ResultSet res = testConn.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE id = " + id);
            assertTrue(res.next());
            switch (useGetter) {
                case getTimestamp:
                    if (calendar == null) {
                        assertEquals(expResult, res.getTimestamp(2));
                    } else {
                        assertEquals(expResult, res.getTimestamp(2, calendar));
                    }
                    break;

                default:
                    if (targetClass == null) {
                        assertEquals(expResult, res.getObject(2));
                    } else {
                        assertEquals(expResult, res.getObject(2, targetClass));
                    }
                    break;
            }

        } finally {
            TimeZone.setDefault(origTz);
        }
    }
}
