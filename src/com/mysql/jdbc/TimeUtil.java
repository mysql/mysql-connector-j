/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Properties;
import java.util.TimeZone;

/**
 * Timezone conversion routines and other time related methods
 */
public class TimeUtil {
    static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT");

    // cache this ourselves, as the method call is statically-synchronized in all but JDK6!
    private static final TimeZone DEFAULT_TIMEZONE = TimeZone.getDefault();

    // Mappings from TimeZone identifications (prefixed by type: Windows, TZ name, MetaZone, TZ alias, ...), to standard TimeZone Ids
    private static final String TIME_ZONE_MAPPINGS_RESOURCE = "/com/mysql/jdbc/TimeZoneMapping.properties";

    private static Properties timeZoneMappings = null;

    protected final static Method systemNanoTimeMethod;

    static {
        Method aMethod;

        try {
            aMethod = System.class.getMethod("nanoTime", (Class[]) null);
        } catch (SecurityException e) {
            aMethod = null;
        } catch (NoSuchMethodException e) {
            aMethod = null;
        }

        systemNanoTimeMethod = aMethod;
    }

    public static boolean nanoTimeAvailable() {
        return systemNanoTimeMethod != null;
    }

    public static final TimeZone getDefaultTimeZone(boolean useCache) {
        return (TimeZone) (useCache ? DEFAULT_TIMEZONE.clone() : TimeZone.getDefault().clone());
    }

    public static long getCurrentTimeNanosOrMillis() {
        if (systemNanoTimeMethod != null) {
            try {
                return ((Long) systemNanoTimeMethod.invoke(null, (Object[]) null)).longValue();
            } catch (IllegalArgumentException e) {
                // ignore - fall through to currentTimeMillis()
            } catch (IllegalAccessException e) {
                // ignore - fall through to currentTimeMillis()
            } catch (InvocationTargetException e) {
                // ignore - fall through to currentTimeMillis()
            }
        }

        return System.currentTimeMillis();
    }

    /**
     * Change the given times from one timezone to another
     * 
     * @param conn
     *            the current connection to the MySQL server
     * @param t
     *            the times to change
     * @param fromTz
     *            the timezone to change from
     * @param toTz
     *            the timezone to change to
     * 
     * @return the times changed to the timezone 'toTz'
     */
    public static Time changeTimezone(MySQLConnection conn, Calendar sessionCalendar, Calendar targetCalendar, Time t, TimeZone fromTz, TimeZone toTz,
            boolean rollForward) {
        if ((conn != null)) {
            if (conn.getUseTimezone() && !conn.getNoTimezoneConversionForTimeType()) {
                // Convert the timestamp from GMT to the server's timezone
                Calendar fromCal = Calendar.getInstance(fromTz);
                fromCal.setTime(t);

                int fromOffset = fromCal.get(Calendar.ZONE_OFFSET) + fromCal.get(Calendar.DST_OFFSET);
                Calendar toCal = Calendar.getInstance(toTz);
                toCal.setTime(t);

                int toOffset = toCal.get(Calendar.ZONE_OFFSET) + toCal.get(Calendar.DST_OFFSET);
                int offsetDiff = fromOffset - toOffset;
                long toTime = toCal.getTime().getTime();

                if (rollForward) {
                    toTime += offsetDiff;
                } else {
                    toTime -= offsetDiff;
                }

                Time changedTime = new Time(toTime);

                return changedTime;
            } else if (conn.getUseJDBCCompliantTimezoneShift()) {
                if (targetCalendar != null) {

                    Time adjustedTime = new Time(jdbcCompliantZoneShift(sessionCalendar, targetCalendar, t));

                    return adjustedTime;
                }
            }
        }

        return t;
    }

    /**
     * Change the given timestamp from one timezone to another
     * 
     * @param conn
     *            the current connection to the MySQL server
     * @param tstamp
     *            the timestamp to change
     * @param fromTz
     *            the timezone to change from
     * @param toTz
     *            the timezone to change to
     * 
     * @return the timestamp changed to the timezone 'toTz'
     */
    public static Timestamp changeTimezone(MySQLConnection conn, Calendar sessionCalendar, Calendar targetCalendar, Timestamp tstamp, TimeZone fromTz,
            TimeZone toTz, boolean rollForward) {
        if ((conn != null)) {
            if (conn.getUseTimezone()) {
                // Convert the timestamp from GMT to the server's timezone
                Calendar fromCal = Calendar.getInstance(fromTz);
                fromCal.setTime(tstamp);

                int fromOffset = fromCal.get(Calendar.ZONE_OFFSET) + fromCal.get(Calendar.DST_OFFSET);
                Calendar toCal = Calendar.getInstance(toTz);
                toCal.setTime(tstamp);

                int toOffset = toCal.get(Calendar.ZONE_OFFSET) + toCal.get(Calendar.DST_OFFSET);
                int offsetDiff = fromOffset - toOffset;
                long toTime = toCal.getTime().getTime();

                if (rollForward) {
                    toTime += offsetDiff;
                } else {
                    toTime -= offsetDiff;
                }

                Timestamp changedTimestamp = new Timestamp(toTime);

                return changedTimestamp;
            } else if (conn.getUseJDBCCompliantTimezoneShift()) {
                if (targetCalendar != null) {

                    Timestamp adjustedTimestamp = new Timestamp(jdbcCompliantZoneShift(sessionCalendar, targetCalendar, tstamp));

                    adjustedTimestamp.setNanos(tstamp.getNanos());

                    return adjustedTimestamp;
                }
            }
        }

        return tstamp;
    }

    private static long jdbcCompliantZoneShift(Calendar sessionCalendar, Calendar targetCalendar, java.util.Date dt) {
        if (sessionCalendar == null) {
            sessionCalendar = new GregorianCalendar();
        }

        synchronized (sessionCalendar) {
            // JDBC spec is not clear whether or not this calendar should be immutable, so let's treat it like it is, for safety

            java.util.Date origCalDate = targetCalendar.getTime();
            java.util.Date origSessionDate = sessionCalendar.getTime();

            try {
                sessionCalendar.setTime(dt);

                targetCalendar.set(Calendar.YEAR, sessionCalendar.get(Calendar.YEAR));
                targetCalendar.set(Calendar.MONTH, sessionCalendar.get(Calendar.MONTH));
                targetCalendar.set(Calendar.DAY_OF_MONTH, sessionCalendar.get(Calendar.DAY_OF_MONTH));

                targetCalendar.set(Calendar.HOUR_OF_DAY, sessionCalendar.get(Calendar.HOUR_OF_DAY));
                targetCalendar.set(Calendar.MINUTE, sessionCalendar.get(Calendar.MINUTE));
                targetCalendar.set(Calendar.SECOND, sessionCalendar.get(Calendar.SECOND));
                targetCalendar.set(Calendar.MILLISECOND, sessionCalendar.get(Calendar.MILLISECOND));

                return targetCalendar.getTime().getTime();

            } finally {
                sessionCalendar.setTime(origSessionDate);
                targetCalendar.setTime(origCalDate);
            }
        }
    }

    final static Date fastDateCreate(boolean useGmtConversion, Calendar gmtCalIfNeeded, Calendar cal, int year, int month, int day) {

        Calendar dateCal = cal;

        if (useGmtConversion) {

            if (gmtCalIfNeeded == null) {
                gmtCalIfNeeded = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
            }

            dateCal = gmtCalIfNeeded;
        }

        synchronized (dateCal) {
            java.util.Date origCalDate = dateCal.getTime();
            try {
                dateCal.clear();
                dateCal.set(Calendar.MILLISECOND, 0);

                // why-oh-why is this different than java.util.date, in the year part, but it still keeps the silly '0' for the start month????
                dateCal.set(year, month - 1, day, 0, 0, 0);

                long dateAsMillis = dateCal.getTimeInMillis();

                return new Date(dateAsMillis);
            } finally {
                dateCal.setTime(origCalDate);
            }
        }

    }

    final static Date fastDateCreate(int year, int month, int day, Calendar targetCalendar) {

        Calendar dateCal = (targetCalendar == null) ? new GregorianCalendar() : targetCalendar;

        synchronized (dateCal) {
            java.util.Date origCalDate = dateCal.getTime();
            try {
                dateCal.clear();

                // why-oh-why is this different than java.util.date, in the year part, but it still keeps the silly '0' for the start month????
                dateCal.set(year, month - 1, day, 0, 0, 0);
                dateCal.set(Calendar.MILLISECOND, 0);

                long dateAsMillis = dateCal.getTimeInMillis();

                return new Date(dateAsMillis);
            } finally {
                dateCal.setTime(origCalDate);
            }
        }
    }

    final static Time fastTimeCreate(Calendar cal, int hour, int minute, int second, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        if (hour < 0 || hour > 24) {
            throw SQLError.createSQLException("Illegal hour value '" + hour + "' for java.sql.Time type in value '" + timeFormattedString(hour, minute, second)
                    + ".", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }

        if (minute < 0 || minute > 59) {
            throw SQLError.createSQLException(
                    "Illegal minute value '" + minute + "' for java.sql.Time type in value '" + timeFormattedString(hour, minute, second) + ".",
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }

        if (second < 0 || second > 59) {
            throw SQLError.createSQLException(
                    "Illegal minute value '" + second + "' for java.sql.Time type in value '" + timeFormattedString(hour, minute, second) + ".",
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }

        synchronized (cal) {
            java.util.Date origCalDate = cal.getTime();
            try {
                cal.clear();

                // Set 'date' to epoch of Jan 1, 1970
                cal.set(1970, 0, 1, hour, minute, second);

                long timeAsMillis = cal.getTimeInMillis();

                return new Time(timeAsMillis);
            } finally {
                cal.setTime(origCalDate);
            }
        }
    }

    final static Time fastTimeCreate(int hour, int minute, int second, Calendar targetCalendar, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        if (hour < 0 || hour > 23) {
            throw SQLError.createSQLException("Illegal hour value '" + hour + "' for java.sql.Time type in value '" + timeFormattedString(hour, minute, second)
                    + ".", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }

        if (minute < 0 || minute > 59) {
            throw SQLError.createSQLException(
                    "Illegal minute value '" + minute + "' for java.sql.Time type in value '" + timeFormattedString(hour, minute, second) + ".",
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }

        if (second < 0 || second > 59) {
            throw SQLError.createSQLException(
                    "Illegal minute value '" + second + "' for java.sql.Time type in value '" + timeFormattedString(hour, minute, second) + ".",
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, exceptionInterceptor);
        }

        Calendar cal = (targetCalendar == null) ? new GregorianCalendar() : targetCalendar;

        synchronized (cal) {
            java.util.Date origCalDate = cal.getTime();
            try {
                cal.clear();

                // Set 'date' to epoch of Jan 1, 1970
                cal.set(1970, 0, 1, hour, minute, second);

                long timeAsMillis = cal.getTimeInMillis();

                return new Time(timeAsMillis);
            } finally {
                cal.setTime(origCalDate);
            }
        }
    }

    final static Timestamp fastTimestampCreate(boolean useGmtConversion, Calendar gmtCalIfNeeded, Calendar cal, int year, int month, int day, int hour,
            int minute, int seconds, int secondsPart) {

        synchronized (cal) {
            java.util.Date origCalDate = cal.getTime();
            try {
                cal.clear();

                // why-oh-why is this different than java.util.date, in the year part, but it still keeps the silly '0' for the start month????
                cal.set(year, month - 1, day, hour, minute, seconds);

                int offsetDiff = 0;

                if (useGmtConversion) {
                    int fromOffset = cal.get(Calendar.ZONE_OFFSET) + cal.get(Calendar.DST_OFFSET);

                    if (gmtCalIfNeeded == null) {
                        gmtCalIfNeeded = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                    }
                    gmtCalIfNeeded.clear();

                    gmtCalIfNeeded.setTimeInMillis(cal.getTimeInMillis());

                    int toOffset = gmtCalIfNeeded.get(Calendar.ZONE_OFFSET) + gmtCalIfNeeded.get(Calendar.DST_OFFSET);
                    offsetDiff = fromOffset - toOffset;
                }

                if (secondsPart != 0) {
                    cal.set(Calendar.MILLISECOND, secondsPart / 1000000);
                }

                long tsAsMillis = cal.getTimeInMillis();

                Timestamp ts = new Timestamp(tsAsMillis + offsetDiff);

                ts.setNanos(secondsPart);

                return ts;
            } finally {
                cal.setTime(origCalDate);
            }
        }
    }

    final static Timestamp fastTimestampCreate(TimeZone tz, int year, int month, int day, int hour, int minute, int seconds, int secondsPart) {
        Calendar cal = (tz == null) ? new GregorianCalendar() : new GregorianCalendar(tz);
        cal.clear();

        // why-oh-why is this different than java.util.date, in the year part, but it still keeps the silly '0' for the start month????
        cal.set(year, month - 1, day, hour, minute, seconds);

        long tsAsMillis = cal.getTimeInMillis();

        Timestamp ts = new Timestamp(tsAsMillis);
        ts.setNanos(secondsPart);

        return ts;
    }

    /**
     * Returns the 'official' Java timezone name for the given timezone
     * 
     * @param timezoneStr
     *            the 'common' timezone name
     * 
     * @return the Java timezone name for the given timezone
     * @throws SQLException
     * 
     * @throws IllegalArgumentException
     */
    public static String getCanonicalTimezone(String timezoneStr, ExceptionInterceptor exceptionInterceptor) throws SQLException {
        if (timezoneStr == null) {
            return null;
        }

        timezoneStr = timezoneStr.trim();

        // handle '+/-hh:mm' form ...
        if (timezoneStr.length() > 2) {
            if ((timezoneStr.charAt(0) == '+' || timezoneStr.charAt(0) == '-') && Character.isDigit(timezoneStr.charAt(1))) {
                return "GMT" + timezoneStr;
            }
        }

        synchronized (TimeUtil.class) {
            if (timeZoneMappings == null) {
                loadTimeZoneMappings(exceptionInterceptor);
            }
        }

        String canonicalTz;
        if ((canonicalTz = timeZoneMappings.getProperty(timezoneStr)) != null) {
            return canonicalTz;
        }

        throw SQLError.createSQLException(Messages.getString("TimeUtil.UnrecognizedTimezoneId", new Object[] { timezoneStr }),
                SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, exceptionInterceptor);
    }

    // we could use SimpleDateFormat, but it won't work when the time values are out-of-bounds, and we're using this for error messages for exactly  that case

    private static String timeFormattedString(int hours, int minutes, int seconds) {
        StringBuilder buf = new StringBuilder(8);
        if (hours < 10) {
            buf.append("0");
        }

        buf.append(hours);
        buf.append(":");

        if (minutes < 10) {
            buf.append("0");
        }

        buf.append(minutes);
        buf.append(":");

        if (seconds < 10) {
            buf.append("0");
        }

        buf.append(seconds);

        return buf.toString();
    }

    public static String formatNanos(int nanos, boolean serverSupportsFracSecs, boolean usingMicros) {

        // get only last 9 digits
        if (nanos > 999999999) {
            nanos %= 100000000;
        }

        if (usingMicros) {
            nanos /= 1000;
        }

        if (!serverSupportsFracSecs || nanos == 0) {
            return "0";
        }

        final int digitCount = usingMicros ? 6 : 9;

        String nanosString = Integer.toString(nanos);
        final String zeroPadding = usingMicros ? "000000" : "000000000";

        nanosString = zeroPadding.substring(0, (digitCount - nanosString.length())) + nanosString;

        int pos = digitCount - 1; // the end, we're padded to the end by the code above

        while (nanosString.charAt(pos) == '0') {
            pos--;
        }

        nanosString = nanosString.substring(0, pos + 1);

        return nanosString;
    }

    /**
     * Loads a properties file that contains all kinds of time zone mappings.
     * 
     * @param exceptionInterceptor
     * @throws SQLException
     */
    private static void loadTimeZoneMappings(ExceptionInterceptor exceptionInterceptor) throws SQLException {
        timeZoneMappings = new Properties();
        try {
            timeZoneMappings.load(TimeZone.class.getResourceAsStream(TIME_ZONE_MAPPINGS_RESOURCE));
        } catch (IOException e) {
            throw SQLError.createSQLException(Messages.getString("TimeUtil.LoadTimeZoneMappingError"), SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE,
                    exceptionInterceptor);
        }
        // bridge all Time Zone ids known by Java
        for (String tz : TimeZone.getAvailableIDs()) {
            if (!timeZoneMappings.containsKey(tz)) {
                timeZoneMappings.put(tz, tz);
            }
        }
    }

    public static Timestamp truncateFractionalSeconds(Timestamp timestamp) {
        Timestamp truncatedTimestamp = new Timestamp(timestamp.getTime());
        truncatedTimestamp.setNanos(0);
        return truncatedTimestamp;
    }
}
