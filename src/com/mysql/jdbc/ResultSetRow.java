/*
  Copyright (c) 2007, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Calendar;
import java.util.StringTokenizer;
import java.util.TimeZone;

/**
 * Classes that implement this interface represent one row of data from the MySQL server that might be stored in different ways depending on whether the result
 * set was streaming (so they wrap a reusable packet), or whether the result set was cached or via a server-side cursor (so they represent a byte[][]).
 * 
 * Notice that <strong>no</strong> bounds checking is expected for implementors of this interface, it happens in ResultSetImpl.
 */
public abstract class ResultSetRow {
    protected ExceptionInterceptor exceptionInterceptor;

    protected ResultSetRow(ExceptionInterceptor exceptionInterceptor) {
        this.exceptionInterceptor = exceptionInterceptor;
    }

    /**
     * The metadata of the fields of this result set.
     */
    protected Field[] metadata;

    /**
     * Called during navigation to next row to close all open
     * streams.
     */
    public abstract void closeOpenStreams();

    /**
     * Returns data at the given index as an InputStream with no
     * character conversion.
     * 
     * @param columnIndex
     *            of the column value (starting at 0) to return.
     * @return the value at the given index as an InputStream or null
     *         if null.
     * 
     * @throws SQLException
     *             if an error occurs while retrieving the value.
     */
    public abstract InputStream getBinaryInputStream(int columnIndex) throws SQLException;

    /**
     * Returns the value at the given column (index starts at 0) "raw" (i.e.
     * as-returned by the server).
     * 
     * @param index
     *            of the column value (starting at 0) to return.
     * @return the value for the given column (including NULL if it is)
     * @throws SQLException
     *             if an error occurs while retrieving the value.
     */
    public abstract byte[] getColumnValue(int index) throws SQLException;

    protected final java.sql.Date getDateFast(int columnIndex, byte[] dateAsBytes, int offset, int length, MySQLConnection conn, ResultSetImpl rs,
            Calendar targetCalendar) throws SQLException {

        int year = 0;
        int month = 0;
        int day = 0;

        try {
            if (dateAsBytes == null) {
                return null;
            }

            boolean allZeroDate = true;

            boolean onlyTimePresent = false;

            for (int i = 0; i < length; i++) {
                if (dateAsBytes[offset + i] == ':') {
                    onlyTimePresent = true;
                    break;
                }
            }

            for (int i = 0; i < length; i++) {
                byte b = dateAsBytes[offset + i];

                if (b == ' ' || b == '-' || b == '/') {
                    onlyTimePresent = false;
                }

                if (b != '0' && b != ' ' && b != ':' && b != '-' && b != '/' && b != '.') {
                    allZeroDate = false;

                    break;
                }
            }

            // check for the fractional part
            int decimalIndex = -1;
            for (int i = 0; i < length; i++) {
                if (dateAsBytes[offset + i] == '.') {
                    decimalIndex = i;
                    break;
                }
            }

            // ignore milliseconds
            if (decimalIndex > -1) {
                length = decimalIndex;
            }

            if (!onlyTimePresent && allZeroDate) {

                if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL.equals(conn.getZeroDateTimeBehavior())) {

                    return null;
                } else if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_EXCEPTION.equals(conn.getZeroDateTimeBehavior())) {
                    throw SQLError.createSQLException("Value '" + StringUtils.toString(dateAsBytes) + "' can not be represented as java.sql.Date",
                            SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
                }

                // We're left with the case of 'round' to a date Java _can_ represent, which is '0001-01-01'.
                return rs.fastDateCreate(targetCalendar, 1, 1, 1);

            } else if (this.metadata[columnIndex].getMysqlType() == MysqlDefs.FIELD_TYPE_TIMESTAMP) {
                // Convert from TIMESTAMP
                switch (length) {
                    case 29:
                    case 21:
                    case 19: { // java.sql.Timestamp format
                        year = StringUtils.getInt(dateAsBytes, offset + 0, offset + 4);
                        month = StringUtils.getInt(dateAsBytes, offset + 5, offset + 7);
                        day = StringUtils.getInt(dateAsBytes, offset + 8, offset + 10);

                        return rs.fastDateCreate(targetCalendar, year, month, day);
                    }

                    case 14:
                    case 8: {
                        year = StringUtils.getInt(dateAsBytes, offset + 0, offset + 4);
                        month = StringUtils.getInt(dateAsBytes, offset + 4, offset + 6);
                        day = StringUtils.getInt(dateAsBytes, offset + 6, offset + 8);

                        return rs.fastDateCreate(targetCalendar, year, month, day);
                    }

                    case 12:
                    case 10:
                    case 6: {
                        year = StringUtils.getInt(dateAsBytes, offset + 0, offset + 2);

                        if (year <= 69) {
                            year = year + 100;
                        }

                        month = StringUtils.getInt(dateAsBytes, offset + 2, offset + 4);
                        day = StringUtils.getInt(dateAsBytes, offset + 4, offset + 6);

                        return rs.fastDateCreate(targetCalendar, year + 1900, month, day);
                    }

                    case 4: {
                        year = StringUtils.getInt(dateAsBytes, offset + 0, offset + 4);

                        if (year <= 69) {
                            year = year + 100;
                        }

                        month = StringUtils.getInt(dateAsBytes, offset + 2, offset + 4);

                        return rs.fastDateCreate(targetCalendar, year + 1900, month, 1);
                    }

                    case 2: {
                        year = StringUtils.getInt(dateAsBytes, offset + 0, offset + 2);

                        if (year <= 69) {
                            year = year + 100;
                        }

                        return rs.fastDateCreate(targetCalendar, year + 1900, 1, 1);
                    }

                    default:
                        throw SQLError.createSQLException(
                                Messages.getString("ResultSet.Bad_format_for_Date",
                                        new Object[] { StringUtils.toString(dateAsBytes), Integer.valueOf(columnIndex + 1) }),
                                SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
                }
            } else if (this.metadata[columnIndex].getMysqlType() == MysqlDefs.FIELD_TYPE_YEAR) {

                if (length == 2 || length == 1) {
                    year = StringUtils.getInt(dateAsBytes, offset, offset + length);

                    if (year <= 69) {
                        year = year + 100;
                    }

                    year += 1900;
                } else {
                    year = StringUtils.getInt(dateAsBytes, offset + 0, offset + 4);
                }

                return rs.fastDateCreate(targetCalendar, year, 1, 1);
            } else if (this.metadata[columnIndex].getMysqlType() == MysqlDefs.FIELD_TYPE_TIME) {
                return rs.fastDateCreate(targetCalendar, 1970, 1, 1); // Return EPOCH
            } else {
                if (length < 10) {
                    if (length == 8) {
                        return rs.fastDateCreate(targetCalendar, 1970, 1, 1); // Return
                        // EPOCH for TIME
                    }

                    throw SQLError.createSQLException(
                            Messages.getString("ResultSet.Bad_format_for_Date",
                                    new Object[] { StringUtils.toString(dateAsBytes), Integer.valueOf(columnIndex + 1) }), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                            this.exceptionInterceptor);
                }

                if (length != 18) {
                    year = StringUtils.getInt(dateAsBytes, offset + 0, offset + 4);
                    month = StringUtils.getInt(dateAsBytes, offset + 5, offset + 7);
                    day = StringUtils.getInt(dateAsBytes, offset + 8, offset + 10);
                } else {
                    // JDK-1.3 timestamp format, not real easy to parse positionally :p
                    StringTokenizer st = new StringTokenizer(StringUtils.toString(dateAsBytes, offset, length, "ISO8859_1"), "- ");

                    year = Integer.parseInt(st.nextToken());
                    month = Integer.parseInt(st.nextToken());
                    day = Integer.parseInt(st.nextToken());
                }
            }

            return rs.fastDateCreate(targetCalendar, year, month, day);
        } catch (SQLException sqlEx) {
            throw sqlEx; // don't re-wrap
        } catch (Exception e) {
            SQLException sqlEx = SQLError.createSQLException(
                    Messages.getString("ResultSet.Bad_format_for_Date", new Object[] { StringUtils.toString(dateAsBytes), Integer.valueOf(columnIndex + 1) }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            sqlEx.initCause(e);

            throw sqlEx;
        }
    }

    public abstract java.sql.Date getDateFast(int columnIndex, MySQLConnection conn, ResultSetImpl rs, Calendar targetCalendar) throws SQLException;

    /**
     * Returns the value at the given column (index starts at 0) as an int. *
     * 
     * @param index
     *            of the column value (starting at 0) to return.
     * @return the value for the given column (returns 0 if NULL, use isNull()
     *         to determine if the value was actually NULL)
     * @throws SQLException
     *             if an error occurs while retrieving the value.
     */
    public abstract int getInt(int columnIndex) throws SQLException;

    /**
     * Returns the value at the given column (index starts at 0) as a long. *
     * 
     * @param index
     *            of the column value (starting at 0) to return.
     * @return the value for the given column (returns 0 if NULL, use isNull()
     *         to determine if the value was actually NULL)
     * @throws SQLException
     *             if an error occurs while retrieving the value.
     */
    public abstract long getLong(int columnIndex) throws SQLException;

    /**
     * @param columnIndex
     * @param bits
     * @param offset
     * @param length
     * @param conn
     * @param rs
     * @param cal
     * @throws SQLException
     */
    protected java.sql.Date getNativeDate(int columnIndex, byte[] bits, int offset, int length, MySQLConnection conn, ResultSetImpl rs, Calendar cal)
            throws SQLException {

        int year = 0;
        int month = 0;
        int day = 0;

        if (length != 0) {
            year = (bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8);

            month = bits[offset + 2];
            day = bits[offset + 3];
        }

        if (length == 0 || ((year == 0) && (month == 0) && (day == 0))) {
            if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL.equals(conn.getZeroDateTimeBehavior())) {
                return null;
            } else if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_EXCEPTION.equals(conn.getZeroDateTimeBehavior())) {
                throw SQLError.createSQLException("Value '0000-00-00' can not be represented as java.sql.Date", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }

            year = 1;
            month = 1;
            day = 1;
        }

        if (!rs.useLegacyDatetimeCode) {
            return TimeUtil.fastDateCreate(year, month, day, cal);
        }

        return rs.fastDateCreate(cal == null ? rs.getCalendarInstanceForSessionOrNew() : cal, year, month, day);
    }

    public abstract Date getNativeDate(int columnIndex, MySQLConnection conn, ResultSetImpl rs, Calendar cal) throws SQLException;

    protected Object getNativeDateTimeValue(int columnIndex, byte[] bits, int offset, int length, Calendar targetCalendar, int jdbcType, int mysqlType,
            TimeZone tz, boolean rollForward, MySQLConnection conn, ResultSetImpl rs) throws SQLException {

        int year = 0;
        int month = 0;
        int day = 0;

        int hour = 0;
        int minute = 0;
        int seconds = 0;

        int nanos = 0;

        if (bits == null) {

            return null;
        }

        Calendar sessionCalendar = conn.getUseJDBCCompliantTimezoneShift() ? conn.getUtcCalendar() : rs.getCalendarInstanceForSessionOrNew();

        boolean populatedFromDateTimeValue = false;

        switch (mysqlType) {
            case MysqlDefs.FIELD_TYPE_DATETIME:
            case MysqlDefs.FIELD_TYPE_TIMESTAMP:
                populatedFromDateTimeValue = true;

                if (length != 0) {
                    year = (bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8);
                    month = bits[offset + 2];
                    day = bits[offset + 3];

                    if (length > 4) {
                        hour = bits[offset + 4];
                        minute = bits[offset + 5];
                        seconds = bits[offset + 6];
                    }

                    if (length > 7) {
                        // MySQL uses microseconds
                        nanos = ((bits[offset + 7] & 0xff) | ((bits[offset + 8] & 0xff) << 8) | ((bits[offset + 9] & 0xff) << 16) | ((bits[offset + 10] & 0xff) << 24)) * 1000;
                    }
                }

                break;
            case MysqlDefs.FIELD_TYPE_DATE:
                populatedFromDateTimeValue = true;

                if (bits.length != 0) {
                    year = (bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8);
                    month = bits[offset + 2];
                    day = bits[offset + 3];
                }

                break;
            case MysqlDefs.FIELD_TYPE_TIME:
                populatedFromDateTimeValue = true;

                if (bits.length != 0) {
                    // bits[0] // skip tm->neg
                    // binaryData.readLong(); // skip daysPart
                    hour = bits[offset + 5];
                    minute = bits[offset + 6];
                    seconds = bits[offset + 7];
                }

                year = 1970;
                month = 1;
                day = 1;

                break;
            default:
                populatedFromDateTimeValue = false;
        }

        switch (jdbcType) {
            case Types.TIME:
                if (populatedFromDateTimeValue) {
                    if (!rs.useLegacyDatetimeCode) {
                        return TimeUtil.fastTimeCreate(hour, minute, seconds, targetCalendar, this.exceptionInterceptor);
                    }

                    Time time = TimeUtil.fastTimeCreate(rs.getCalendarInstanceForSessionOrNew(), hour, minute, seconds, this.exceptionInterceptor);

                    Time adjustedTime = TimeUtil.changeTimezone(conn, sessionCalendar, targetCalendar, time, conn.getServerTimezoneTZ(), tz, rollForward);

                    return adjustedTime;
                }

                return rs.getNativeTimeViaParseConversion(columnIndex + 1, targetCalendar, tz, rollForward);

            case Types.DATE:
                if (populatedFromDateTimeValue) {
                    if ((year == 0) && (month == 0) && (day == 0)) {
                        if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL.equals(conn.getZeroDateTimeBehavior())) {

                            return null;
                        } else if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_EXCEPTION.equals(conn.getZeroDateTimeBehavior())) {
                            throw new SQLException("Value '0000-00-00' can not be represented as java.sql.Date", SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
                        }

                        year = 1;
                        month = 1;
                        day = 1;
                    }

                    if (!rs.useLegacyDatetimeCode) {
                        return TimeUtil.fastDateCreate(year, month, day, targetCalendar);
                    }

                    return rs.fastDateCreate(rs.getCalendarInstanceForSessionOrNew(), year, month, day);
                }

                return rs.getNativeDateViaParseConversion(columnIndex + 1);
            case Types.TIMESTAMP:
                if (populatedFromDateTimeValue) {
                    if ((year == 0) && (month == 0) && (day == 0)) {
                        if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL.equals(conn.getZeroDateTimeBehavior())) {

                            return null;
                        } else if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_EXCEPTION.equals(conn.getZeroDateTimeBehavior())) {
                            throw new SQLException("Value '0000-00-00' can not be represented as java.sql.Timestamp", SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
                        }

                        year = 1;
                        month = 1;
                        day = 1;
                    }

                    if (!rs.useLegacyDatetimeCode) {
                        return TimeUtil.fastTimestampCreate(tz, year, month, day, hour, minute, seconds, nanos);
                    }

                    Timestamp ts = rs.fastTimestampCreate(rs.getCalendarInstanceForSessionOrNew(), year, month, day, hour, minute, seconds, nanos);

                    Timestamp adjustedTs = TimeUtil.changeTimezone(conn, sessionCalendar, targetCalendar, ts, conn.getServerTimezoneTZ(), tz, rollForward);

                    return adjustedTs;
                }

                return rs.getNativeTimestampViaParseConversion(columnIndex + 1, targetCalendar, tz, rollForward);

            default:
                throw new SQLException("Internal error - conversion method doesn't support this type", SQLError.SQL_STATE_GENERAL_ERROR);
        }
    }

    public abstract Object getNativeDateTimeValue(int columnIndex, Calendar targetCalendar, int jdbcType, int mysqlType, TimeZone tz, boolean rollForward,
            MySQLConnection conn, ResultSetImpl rs) throws SQLException;

    protected double getNativeDouble(byte[] bits, int offset) {
        long valueAsLong = (bits[offset + 0] & 0xff) | ((long) (bits[offset + 1] & 0xff) << 8) | ((long) (bits[offset + 2] & 0xff) << 16)
                | ((long) (bits[offset + 3] & 0xff) << 24) | ((long) (bits[offset + 4] & 0xff) << 32) | ((long) (bits[offset + 5] & 0xff) << 40)
                | ((long) (bits[offset + 6] & 0xff) << 48) | ((long) (bits[offset + 7] & 0xff) << 56);

        return Double.longBitsToDouble(valueAsLong);
    }

    public abstract double getNativeDouble(int columnIndex) throws SQLException;

    protected float getNativeFloat(byte[] bits, int offset) {
        int asInt = (bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8) | ((bits[offset + 2] & 0xff) << 16) | ((bits[offset + 3] & 0xff) << 24);

        return Float.intBitsToFloat(asInt);
    }

    public abstract float getNativeFloat(int columnIndex) throws SQLException;

    protected int getNativeInt(byte[] bits, int offset) {

        int valueAsInt = (bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8) | ((bits[offset + 2] & 0xff) << 16) | ((bits[offset + 3] & 0xff) << 24);

        return valueAsInt;
    }

    public abstract int getNativeInt(int columnIndex) throws SQLException;

    protected long getNativeLong(byte[] bits, int offset) {
        long valueAsLong = (bits[offset + 0] & 0xff) | ((long) (bits[offset + 1] & 0xff) << 8) | ((long) (bits[offset + 2] & 0xff) << 16)
                | ((long) (bits[offset + 3] & 0xff) << 24) | ((long) (bits[offset + 4] & 0xff) << 32) | ((long) (bits[offset + 5] & 0xff) << 40)
                | ((long) (bits[offset + 6] & 0xff) << 48) | ((long) (bits[offset + 7] & 0xff) << 56);

        return valueAsLong;
    }

    public abstract long getNativeLong(int columnIndex) throws SQLException;

    protected short getNativeShort(byte[] bits, int offset) {
        short asShort = (short) ((bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8));

        return asShort;
    }

    public abstract short getNativeShort(int columnIndex) throws SQLException;

    /**
     * @param columnIndex
     * @param bits
     * @param offset
     * @param length
     * @param targetCalendar
     * @param tz
     * @param rollForward
     * @param conn
     * @param rs
     * @throws SQLException
     */
    protected Time getNativeTime(int columnIndex, byte[] bits, int offset, int length, Calendar targetCalendar, TimeZone tz, boolean rollForward,
            MySQLConnection conn, ResultSetImpl rs) throws SQLException {

        int hour = 0;
        int minute = 0;
        int seconds = 0;

        if (length != 0) {
            // bits[0] // skip tm->neg
            // binaryData.readLong(); // skip daysPart
            hour = bits[offset + 5];
            minute = bits[offset + 6];
            seconds = bits[offset + 7];
        }

        if (!rs.useLegacyDatetimeCode) {
            return TimeUtil.fastTimeCreate(hour, minute, seconds, targetCalendar, this.exceptionInterceptor);
        }

        Calendar sessionCalendar = rs.getCalendarInstanceForSessionOrNew();

        Time time = TimeUtil.fastTimeCreate(sessionCalendar, hour, minute, seconds, this.exceptionInterceptor);

        Time adjustedTime = TimeUtil.changeTimezone(conn, sessionCalendar, targetCalendar, time, conn.getServerTimezoneTZ(), tz, rollForward);

        return adjustedTime;
    }

    public abstract Time getNativeTime(int columnIndex, Calendar targetCalendar, TimeZone tz, boolean rollForward, MySQLConnection conn, ResultSetImpl rs)
            throws SQLException;

    protected Timestamp getNativeTimestamp(byte[] bits, int offset, int length, Calendar targetCalendar, TimeZone tz, boolean rollForward,
            MySQLConnection conn, ResultSetImpl rs) throws SQLException {
        int year = 0;
        int month = 0;
        int day = 0;

        int hour = 0;
        int minute = 0;
        int seconds = 0;

        int nanos = 0;

        if (length != 0) {
            year = (bits[offset + 0] & 0xff) | ((bits[offset + 1] & 0xff) << 8);
            month = bits[offset + 2];
            day = bits[offset + 3];

            if (length > 4) {
                hour = bits[offset + 4];
                minute = bits[offset + 5];
                seconds = bits[offset + 6];
            }

            if (length > 7) {
                // MySQL uses microseconds
                nanos = ((bits[offset + 7] & 0xff) | ((bits[offset + 8] & 0xff) << 8) | ((bits[offset + 9] & 0xff) << 16) | ((bits[offset + 10] & 0xff) << 24)) * 1000;
            }
        }

        if (length == 0 || ((year == 0) && (month == 0) && (day == 0))) {
            if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL.equals(conn.getZeroDateTimeBehavior())) {

                return null;
            } else if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_EXCEPTION.equals(conn.getZeroDateTimeBehavior())) {
                throw SQLError.createSQLException("Value '0000-00-00' can not be represented as java.sql.Timestamp", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }

            year = 1;
            month = 1;
            day = 1;
        }

        if (!rs.useLegacyDatetimeCode) {
            return TimeUtil.fastTimestampCreate(tz, year, month, day, hour, minute, seconds, nanos);
        }

        Calendar sessionCalendar = conn.getUseJDBCCompliantTimezoneShift() ? conn.getUtcCalendar() : rs.getCalendarInstanceForSessionOrNew();

        Timestamp ts = rs.fastTimestampCreate(sessionCalendar, year, month, day, hour, minute, seconds, nanos);

        Timestamp adjustedTs = TimeUtil.changeTimezone(conn, sessionCalendar, targetCalendar, ts, conn.getServerTimezoneTZ(), tz, rollForward);

        return adjustedTs;
    }

    public abstract Timestamp getNativeTimestamp(int columnIndex, Calendar targetCalendar, TimeZone tz, boolean rollForward, MySQLConnection conn,
            ResultSetImpl rs) throws SQLException;

    public abstract Reader getReader(int columnIndex) throws SQLException;

    /**
     * Returns the value at the given column (index starts at 0) as a
     * java.lang.String with the requested encoding, using the given
     * MySQLConnection to find character converters.
     * 
     * @param index
     *            of the column value (starting at 0) to return.
     * @param encoding
     *            the Java name for the character encoding
     * @param conn
     *            the connection that created this result set row
     * 
     * @return the value for the given column (including NULL if it is) as a
     *         String
     * 
     * @throws SQLException
     *             if an error occurs while retrieving the value.
     */
    public abstract String getString(int index, String encoding, MySQLConnection conn) throws SQLException;

    /**
     * Convenience method for turning a byte[] into a string with the given
     * encoding.
     * 
     * @param encoding
     *            the Java encoding name for the byte[] -> char conversion
     * @param conn
     *            the MySQLConnection that created the result set
     * @param value
     *            the String value as a series of bytes, encoded using
     *            "encoding"
     * @param offset
     *            where to start the decoding
     * @param length
     *            how many bytes to decode
     * 
     * @return the String as decoded from bytes with the given encoding
     * 
     * @throws SQLException
     *             if an error occurs
     */
    protected String getString(String encoding, MySQLConnection conn, byte[] value, int offset, int length) throws SQLException {
        String stringVal = null;

        if ((conn != null) && conn.getUseUnicode()) {
            try {
                if (encoding == null) {
                    stringVal = StringUtils.toString(value);
                } else {
                    SingleByteCharsetConverter converter = conn.getCharsetConverter(encoding);

                    if (converter != null) {
                        stringVal = converter.toString(value, offset, length);
                    } else {
                        stringVal = StringUtils.toString(value, offset, length, encoding);
                    }
                }
            } catch (java.io.UnsupportedEncodingException E) {
                throw SQLError.createSQLException(Messages.getString("ResultSet.Unsupported_character_encoding____101") + encoding + "'.", "0S100",
                        this.exceptionInterceptor);
            }
        } else {
            stringVal = StringUtils.toAsciiString(value, offset, length);
        }

        return stringVal;
    }

    protected Time getTimeFast(int columnIndex, byte[] timeAsBytes, int offset, int fullLength, Calendar targetCalendar, TimeZone tz, boolean rollForward,
            MySQLConnection conn, ResultSetImpl rs) throws SQLException {

        int hr = 0;
        int min = 0;
        int sec = 0;
        int nanos = 0;

        int decimalIndex = -1;

        try {

            if (timeAsBytes == null) {
                return null;
            }

            boolean allZeroTime = true;
            boolean onlyTimePresent = false;

            for (int i = 0; i < fullLength; i++) {
                if (timeAsBytes[offset + i] == ':') {
                    onlyTimePresent = true;
                    break;
                }
            }

            for (int i = 0; i < fullLength; i++) {
                if (timeAsBytes[offset + i] == '.') {
                    decimalIndex = i;
                    break;
                }
            }

            for (int i = 0; i < fullLength; i++) {
                byte b = timeAsBytes[offset + i];

                if (b == ' ' || b == '-' || b == '/') {
                    onlyTimePresent = false;
                }

                if (b != '0' && b != ' ' && b != ':' && b != '-' && b != '/' && b != '.') {
                    allZeroTime = false;

                    break;
                }
            }

            if (!onlyTimePresent && allZeroTime) {
                if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL.equals(conn.getZeroDateTimeBehavior())) {
                    return null;
                } else if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_EXCEPTION.equals(conn.getZeroDateTimeBehavior())) {
                    throw SQLError.createSQLException("Value '" + StringUtils.toString(timeAsBytes) + "' can not be represented as java.sql.Time",
                            SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
                }

                // We're left with the case of 'round' to a time Java _can_ represent, which is '00:00:00'
                return rs.fastTimeCreate(targetCalendar, 0, 0, 0);
            }

            Field timeColField = this.metadata[columnIndex];

            int length = fullLength;

            if (decimalIndex != -1) {

                length = decimalIndex;

                if ((decimalIndex + 2) <= fullLength) {
                    nanos = StringUtils.getInt(timeAsBytes, offset + decimalIndex + 1, offset + fullLength);

                    int numDigits = (fullLength) - (decimalIndex + 1);

                    if (numDigits < 9) {
                        int factor = (int) (Math.pow(10, 9 - numDigits));
                        nanos = nanos * factor;
                    }
                } else {
                    throw new IllegalArgumentException(); // re-thrown
                    // further
                    // down
                    // with
                    // a
                    // much better error message
                }
            }

            if (timeColField.getMysqlType() == MysqlDefs.FIELD_TYPE_TIMESTAMP) {

                switch (length) {
                    case 19: { // YYYY-MM-DD hh:mm:ss

                        hr = StringUtils.getInt(timeAsBytes, offset + length - 8, offset + length - 6);
                        min = StringUtils.getInt(timeAsBytes, offset + length - 5, offset + length - 3);
                        sec = StringUtils.getInt(timeAsBytes, offset + length - 2, offset + length);
                    }

                        break;
                    case 14:
                    case 12: {
                        hr = StringUtils.getInt(timeAsBytes, offset + length - 6, offset + length - 4);
                        min = StringUtils.getInt(timeAsBytes, offset + length - 4, offset + length - 2);
                        sec = StringUtils.getInt(timeAsBytes, offset + length - 2, offset + length);
                    }

                        break;

                    case 10: {
                        hr = StringUtils.getInt(timeAsBytes, offset + 6, offset + 8);
                        min = StringUtils.getInt(timeAsBytes, offset + 8, offset + 10);
                        sec = 0;
                    }

                        break;

                    default:
                        throw SQLError.createSQLException(Messages.getString("ResultSet.Timestamp_too_small_to_convert_to_Time_value_in_column__257")
                                + (columnIndex + 1) + "(" + timeColField + ").", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
                } /* endswitch */

                @SuppressWarnings("unused")
                SQLWarning precisionLost = new SQLWarning(
                        Messages.getString("ResultSet.Precision_lost_converting_TIMESTAMP_to_Time_with_getTime()_on_column__261") + columnIndex + "("
                                + timeColField + ").");
                /*
                 * if (this.warningChain == null) { this.warningChain =
                 * precisionLost; } else {
                 * this.warningChain.setNextWarning(precisionLost); }
                 */
            } else if (timeColField.getMysqlType() == MysqlDefs.FIELD_TYPE_DATETIME) {
                hr = StringUtils.getInt(timeAsBytes, offset + 11, offset + 13);
                min = StringUtils.getInt(timeAsBytes, offset + 14, offset + 16);
                sec = StringUtils.getInt(timeAsBytes, offset + 17, offset + 19);

                @SuppressWarnings("unused")
                SQLWarning precisionLost = new SQLWarning(
                        Messages.getString("ResultSet.Precision_lost_converting_DATETIME_to_Time_with_getTime()_on_column__264") + (columnIndex + 1) + "("
                                + timeColField + ").");

                /*
                 * if (this.warningChain == null) { this.warningChain =
                 * precisionLost; } else {
                 * this.warningChain.setNextWarning(precisionLost); }
                 */
            } else if (timeColField.getMysqlType() == MysqlDefs.FIELD_TYPE_DATE) {
                return rs.fastTimeCreate(null, 0, 0, 0); // midnight on the
                // given date
            } else {
                // convert a String to a Time
                if ((length != 5) && (length != 8)) {
                    throw SQLError.createSQLException(
                            Messages.getString("ResultSet.Bad_format_for_Time____267") + StringUtils.toString(timeAsBytes)
                                    + Messages.getString("ResultSet.___in_column__268") + (columnIndex + 1), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                            this.exceptionInterceptor);
                }

                hr = StringUtils.getInt(timeAsBytes, offset + 0, offset + 2);
                min = StringUtils.getInt(timeAsBytes, offset + 3, offset + 5);
                sec = (length == 5) ? 0 : StringUtils.getInt(timeAsBytes, offset + 6, offset + 8);
            }

            Calendar sessionCalendar = rs.getCalendarInstanceForSessionOrNew();

            if (!rs.useLegacyDatetimeCode) {
                // TODO: return rs.fastTimeCreate(targetCalendar, hr, min, sec, nanos);
                // java.sql.Time doesn't contain fractional part, so PreparedStatement.setTime/getTime can't deal with TIME(n) fractional part.
                // There may be better mappings to high-precision time coming in JDBC-5 with the adoption of JSR-310.
                return rs.fastTimeCreate(targetCalendar, hr, min, sec);
            }

            return TimeUtil.changeTimezone(conn, sessionCalendar, targetCalendar, rs.fastTimeCreate(sessionCalendar, hr, min, sec), conn.getServerTimezoneTZ(),
                    tz, rollForward);
            // TODO: min, sec, nanos), conn.getServerTimezoneTZ(), tz,
            // java.sql.Time doesn't contain fractional part, so PreparedStatement.setTime/getTime can't deal with TIME(n) fractional part.
            // There may be better mappings to high-precision time coming in JDBC-5 with the adoption of JSR-310.
        } catch (RuntimeException ex) {
            SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            sqlEx.initCause(ex);

            throw sqlEx;
        }
    }

    public abstract Time getTimeFast(int columnIndex, Calendar targetCalendar, TimeZone tz, boolean rollForward, MySQLConnection conn, ResultSetImpl rs)
            throws SQLException;

    protected Timestamp getTimestampFast(int columnIndex, byte[] timestampAsBytes, int offset, int length, Calendar targetCalendar, TimeZone tz,
            boolean rollForward, MySQLConnection conn, ResultSetImpl rs) throws SQLException {

        try {
            Calendar sessionCalendar = conn.getUseJDBCCompliantTimezoneShift() ? conn.getUtcCalendar() : rs.getCalendarInstanceForSessionOrNew();

            boolean allZeroTimestamp = true;

            boolean onlyTimePresent = false;

            for (int i = 0; i < length; i++) {
                if (timestampAsBytes[offset + i] == ':') {
                    onlyTimePresent = true;
                    break;
                }
            }

            for (int i = 0; i < length; i++) {
                byte b = timestampAsBytes[offset + i];

                if (b == ' ' || b == '-' || b == '/') {
                    onlyTimePresent = false;
                }

                if (b != '0' && b != ' ' && b != ':' && b != '-' && b != '/' && b != '.') {
                    allZeroTimestamp = false;

                    break;
                }
            }

            if (!onlyTimePresent && allZeroTimestamp) {

                if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL.equals(conn.getZeroDateTimeBehavior())) {

                    return null;
                } else if (ConnectionPropertiesImpl.ZERO_DATETIME_BEHAVIOR_EXCEPTION.equals(conn.getZeroDateTimeBehavior())) {
                    throw SQLError.createSQLException("Value '" + StringUtils.toString(timestampAsBytes) + "' can not be represented as java.sql.Timestamp",
                            SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
                }

                if (!rs.useLegacyDatetimeCode) {
                    return TimeUtil.fastTimestampCreate(tz, 1, 1, 1, 0, 0, 0, 0);
                }
                // We're left with the case of 'round' to a date Java _can_ represent, which is '0001-01-01'.
                return rs.fastTimestampCreate(null, 1, 1, 1, 0, 0, 0, 0);

            } else if (this.metadata[columnIndex].getMysqlType() == MysqlDefs.FIELD_TYPE_YEAR) {

                if (!rs.useLegacyDatetimeCode) {
                    return TimeUtil.fastTimestampCreate(tz, StringUtils.getInt(timestampAsBytes, offset, 4), 1, 1, 0, 0, 0, 0);
                }

                return TimeUtil.changeTimezone(conn, sessionCalendar, targetCalendar,
                        rs.fastTimestampCreate(sessionCalendar, StringUtils.getInt(timestampAsBytes, offset, 4), 1, 1, 0, 0, 0, 0), conn.getServerTimezoneTZ(),
                        tz, rollForward);
            } else {
                // Convert from TIMESTAMP, TIME or DATE

                int year = 0;
                int month = 0;
                int day = 0;
                int hour = 0;
                int minutes = 0;
                int seconds = 0;
                int nanos = 0;

                // check for the fractional part
                int decimalIndex = -1;
                for (int i = 0; i < length; i++) {
                    if (timestampAsBytes[offset + i] == '.') {
                        decimalIndex = i;
                        break;
                    }
                }

                if (decimalIndex == offset + length - 1) {
                    // if the dot is in last position
                    length--;

                } else if (decimalIndex != -1) {
                    if ((decimalIndex + 2) <= length) {
                        nanos = StringUtils.getInt(timestampAsBytes, offset + decimalIndex + 1, offset + length);

                        int numDigits = (length) - (decimalIndex + 1);

                        if (numDigits < 9) {
                            int factor = (int) (Math.pow(10, 9 - numDigits));
                            nanos = nanos * factor;
                        }
                    } else {
                        throw new IllegalArgumentException(); // re-thrown
                        // further down with a much better error message
                    }

                    length = decimalIndex;
                }

                switch (length) {
                    case 29:
                    case 26:
                    case 25:
                    case 24:
                    case 23:
                    case 22:
                    case 21:
                    case 20:
                    case 19: {
                        year = StringUtils.getInt(timestampAsBytes, offset + 0, offset + 4);
                        month = StringUtils.getInt(timestampAsBytes, offset + 5, offset + 7);
                        day = StringUtils.getInt(timestampAsBytes, offset + 8, offset + 10);
                        hour = StringUtils.getInt(timestampAsBytes, offset + 11, offset + 13);
                        minutes = StringUtils.getInt(timestampAsBytes, offset + 14, offset + 16);
                        seconds = StringUtils.getInt(timestampAsBytes, offset + 17, offset + 19);

                        break;
                    }

                    case 14: {
                        year = StringUtils.getInt(timestampAsBytes, offset + 0, offset + 4);
                        month = StringUtils.getInt(timestampAsBytes, offset + 4, offset + 6);
                        day = StringUtils.getInt(timestampAsBytes, offset + 6, offset + 8);
                        hour = StringUtils.getInt(timestampAsBytes, offset + 8, offset + 10);
                        minutes = StringUtils.getInt(timestampAsBytes, offset + 10, offset + 12);
                        seconds = StringUtils.getInt(timestampAsBytes, offset + 12, offset + 14);

                        break;
                    }

                    case 12: {
                        year = StringUtils.getInt(timestampAsBytes, offset + 0, offset + 2);

                        if (year <= 69) {
                            year = (year + 100);
                        }

                        year += 1900;

                        month = StringUtils.getInt(timestampAsBytes, offset + 2, offset + 4);
                        day = StringUtils.getInt(timestampAsBytes, offset + 4, offset + 6);
                        hour = StringUtils.getInt(timestampAsBytes, offset + 6, offset + 8);
                        minutes = StringUtils.getInt(timestampAsBytes, offset + 8, offset + 10);
                        seconds = StringUtils.getInt(timestampAsBytes, offset + 10, offset + 12);

                        break;
                    }

                    case 10: {
                        boolean hasDash = false;

                        for (int i = 0; i < length; i++) {
                            if (timestampAsBytes[offset + i] == '-') {
                                hasDash = true;
                                break;
                            }
                        }

                        if ((this.metadata[columnIndex].getMysqlType() == MysqlDefs.FIELD_TYPE_DATE) || hasDash) {
                            year = StringUtils.getInt(timestampAsBytes, offset + 0, offset + 4);
                            month = StringUtils.getInt(timestampAsBytes, offset + 5, offset + 7);
                            day = StringUtils.getInt(timestampAsBytes, offset + 8, offset + 10);
                            hour = 0;
                            minutes = 0;
                        } else {
                            year = StringUtils.getInt(timestampAsBytes, offset + 0, offset + 2);

                            if (year <= 69) {
                                year = (year + 100);
                            }

                            month = StringUtils.getInt(timestampAsBytes, offset + 2, offset + 4);
                            day = StringUtils.getInt(timestampAsBytes, offset + 4, offset + 6);
                            hour = StringUtils.getInt(timestampAsBytes, offset + 6, offset + 8);
                            minutes = StringUtils.getInt(timestampAsBytes, offset + 8, offset + 10);

                            year += 1900; // two-digit year
                        }

                        break;
                    }

                    case 8: {
                        boolean hasColon = false;

                        for (int i = 0; i < length; i++) {
                            if (timestampAsBytes[offset + i] == ':') {
                                hasColon = true;
                                break;
                            }
                        }

                        if (hasColon) {
                            hour = StringUtils.getInt(timestampAsBytes, offset + 0, offset + 2);
                            minutes = StringUtils.getInt(timestampAsBytes, offset + 3, offset + 5);
                            seconds = StringUtils.getInt(timestampAsBytes, offset + 6, offset + 8);

                            year = 1970;
                            month = 1;
                            day = 1;

                            break;
                        }

                        year = StringUtils.getInt(timestampAsBytes, offset + 0, offset + 4);
                        month = StringUtils.getInt(timestampAsBytes, offset + 4, offset + 6);
                        day = StringUtils.getInt(timestampAsBytes, offset + 6, offset + 8);

                        year -= 1900;
                        month--;

                        break;
                    }

                    case 6: {
                        year = StringUtils.getInt(timestampAsBytes, offset + 0, offset + 2);

                        if (year <= 69) {
                            year = (year + 100);
                        }

                        year += 1900;

                        month = StringUtils.getInt(timestampAsBytes, offset + 2, offset + 4);
                        day = StringUtils.getInt(timestampAsBytes, offset + 4, offset + 6);

                        break;
                    }

                    case 4: {
                        year = StringUtils.getInt(timestampAsBytes, offset + 0, offset + 2);

                        if (year <= 69) {
                            year = (year + 100);
                        }

                        month = StringUtils.getInt(timestampAsBytes, offset + 2, offset + 4);

                        day = 1;

                        break;
                    }

                    case 2: {
                        year = StringUtils.getInt(timestampAsBytes, offset + 0, offset + 2);

                        if (year <= 69) {
                            year = (year + 100);
                        }

                        year += 1900;
                        month = 1;
                        day = 1;

                        break;
                    }

                    default:
                        throw new java.sql.SQLException("Bad format for Timestamp '" + StringUtils.toString(timestampAsBytes) + "' in column "
                                + (columnIndex + 1) + ".", SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
                }

                if (!rs.useLegacyDatetimeCode) {
                    return TimeUtil.fastTimestampCreate(tz, year, month, day, hour, minutes, seconds, nanos);
                }

                return TimeUtil.changeTimezone(conn, sessionCalendar, targetCalendar,
                        rs.fastTimestampCreate(sessionCalendar, year, month, day, hour, minutes, seconds, nanos), conn.getServerTimezoneTZ(), tz, rollForward);
            }
        } catch (RuntimeException e) {
            SQLException sqlEx = SQLError.createSQLException("Cannot convert value '" + getString(columnIndex, "ISO8859_1", conn) + "' from column "
                    + (columnIndex + 1) + " to TIMESTAMP.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            sqlEx.initCause(e);

            throw sqlEx;
        }
    }

    public abstract Timestamp getTimestampFast(int columnIndex, Calendar targetCalendar, TimeZone tz, boolean rollForward, MySQLConnection conn,
            ResultSetImpl rs) throws SQLException;

    /**
     * Could the column value at the given index (which starts at 0) be
     * interpreted as a floating-point number (has +/-/E/e in it)?
     * 
     * @param index
     *            of the column value (starting at 0) to check.
     * 
     * @return true if the column value at the given index looks like it might
     *         be a floating-point number, false if not.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public abstract boolean isFloatingPointNumber(int index) throws SQLException;

    /**
     * Is the column value at the given index (which starts at 0) NULL?
     * 
     * @param index
     *            of the column value (starting at 0) to check.
     * 
     * @return true if the column value is NULL, false if not.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public abstract boolean isNull(int index) throws SQLException;

    /**
     * Returns the length of the column at the given index (which starts at 0).
     * 
     * @param index
     *            of the column value (starting at 0) for which to return the
     *            length.
     * @return the length of the requested column, 0 if null (clients of this
     *         interface should use isNull() beforehand to determine status of
     *         NULL values in the column).
     * 
     * @throws SQLException
     */
    public abstract long length(int index) throws SQLException;

    /**
     * Sets the given column value (only works currently with
     * ByteArrayRowHolder).
     * 
     * @param index
     *            index of the column value (starting at 0) to set.
     * @param value
     *            the (raw) value to set
     * 
     * @throws SQLException
     *             if an error occurs, or the concrete RowHolder doesn't support
     *             this operation.
     */
    public abstract void setColumnValue(int index, byte[] value) throws SQLException;

    public ResultSetRow setMetadata(Field[] f) throws SQLException {
        this.metadata = f;

        return this;
    }

    public abstract int getBytesSize();
}
