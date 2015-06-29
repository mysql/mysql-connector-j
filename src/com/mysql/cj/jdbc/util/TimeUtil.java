/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc.util;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Properties;
import java.util.TimeZone;

import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.core.Messages;
import com.mysql.cj.jdbc.exceptions.SQLError;

/**
 * Timezone conversion routines and other time related methods
 */
public class TimeUtil {
    static final TimeZone GMT_TIMEZONE = TimeZone.getTimeZone("GMT");

    // Mappings from TimeZone identifications (prefixed by type: Windows, TZ name, MetaZone, TZ alias, ...), to standard TimeZone Ids
    private static final String TIME_ZONE_MAPPINGS_RESOURCE = "/com/mysql/cj/jdbc/util/TimeZoneMapping.properties";

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

    public static String formatNanos(int nanos, boolean usingMicros) {

        // get only last 9 digits
        if (nanos > 999999999) {
            nanos %= 100000000;
        }

        if (usingMicros) {
            nanos /= 1000;
        }

        if (nanos == 0) {
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
}
