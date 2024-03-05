/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under the terms of the GNU General Public License, version 2.0, as published by
 * the Free Software Foundation.
 *
 * This program is designed to work with certain software that is licensed under separate terms, as designated in a particular file or component or in
 * included license documentation. The authors of MySQL hereby grant you an additional permission to link the program and your derivative works with the
 * separately licensed software that they have either included with the program or referenced in the documentation.
 *
 * Without limiting anything contained in the foregoing, this file, which is part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 */

package com.mysql.cj.protocol.a;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.DataReadException;
import com.mysql.cj.exceptions.NumberOutOfRange;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;
import com.mysql.cj.protocol.ValueDecoder;
import com.mysql.cj.result.Field;
import com.mysql.cj.result.ValueFactory;
import com.mysql.cj.util.StringUtils;

/**
 * Implementation of {@link com.mysql.cj.protocol.ValueDecoder} for the MySQL text protocol. All values will be received as <i>LengthEncodedString</i> values.
 * <p>
 * Refer to MySQL documentation for format of values as strings.
 * <p>
 * Numeric values are returned as ASCII (encoding=63/binary).
 */
public class MysqlTextValueDecoder implements ValueDecoder {

    /** Buffer length of MySQL date string: 'YYYY-MM-DD'. */
    public static final int DATE_BUF_LEN = 10;
    /** Min string length of MySQL time string: 'HH:MM:SS'. */
    public static final int TIME_STR_LEN_MIN = 8;
    /** Max string length of MySQL time string (with microseconds): '-HHH:MM:SS'. */
    public static final int TIME_STR_LEN_MAX_NO_FRAC = 10;
    /** Max string length of MySQL time string (with microseconds): '-HHH:MM:SS.mmmmmm'. */
    public static final int TIME_STR_LEN_MAX_WITH_MICROS = TIME_STR_LEN_MAX_NO_FRAC + 7;
    /** String length of MySQL timestamp string (no microseconds): 'YYYY-MM-DD HH:MM:SS'. */
    public static final int TIMESTAMP_STR_LEN_NO_FRAC = 19;
    /** Max string length of MySQL timestamp (with microsecs): 'YYYY-MM-DD HH:MM:SS.mmmmmm'. */
    public static final int TIMESTAMP_STR_LEN_WITH_MICROS = TIMESTAMP_STR_LEN_NO_FRAC + 7;
    /** String length of String timestamp with nanos. This does not come from MySQL server but we support it via string conversion. */
    public static final int TIMESTAMP_STR_LEN_WITH_NANOS = TIMESTAMP_STR_LEN_NO_FRAC + 10;

    public static final Pattern TIME_PTRN = Pattern.compile("[-]{0,1}\\d{2,3}:\\d{2}:\\d{2}(\\.\\d{1,9})?");

    /** Max string length of a signed long = 9223372036854775807 (19+1 for minus sign) */
    public static final int MAX_SIGNED_LONG_LEN = 20;

    @Override
    public <T> T decodeDate(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromDate(getDate(bytes, offset, length));
    }

    @Override
    public <T> T decodeTime(byte[] bytes, int offset, int length, int scale, ValueFactory<T> vf) {
        return vf.createFromTime(getTime(bytes, offset, length, scale));
    }

    @Override
    public <T> T decodeTimestamp(byte[] bytes, int offset, int length, int scale, ValueFactory<T> vf) {
        return vf.createFromTimestamp(getTimestamp(bytes, offset, length, scale));
    }

    @Override
    public <T> T decodeDatetime(byte[] bytes, int offset, int length, int scale, ValueFactory<T> vf) {
        return vf.createFromDatetime(getTimestamp(bytes, offset, length, scale));
    }

    @Override
    public <T> T decodeUInt1(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(getInt(bytes, offset, offset + length));
    }

    @Override
    public <T> T decodeInt1(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(getInt(bytes, offset, offset + length));
    }

    @Override
    public <T> T decodeUInt2(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(getInt(bytes, offset, offset + length));
    }

    @Override
    public <T> T decodeInt2(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(getInt(bytes, offset, offset + length));
    }

    @Override
    public <T> T decodeUInt4(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(getLong(bytes, offset, offset + length));
    }

    @Override
    public <T> T decodeInt4(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(getInt(bytes, offset, offset + length));
    }

    @Override
    public <T> T decodeUInt8(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        // treat as a signed long if possible to avoid BigInteger overhead
        if (length <= MAX_SIGNED_LONG_LEN - 1 && bytes[offset] >= '0' && bytes[offset] <= '8') {
            return decodeInt8(bytes, offset, length, vf);
        }
        return vf.createFromBigInteger(getBigInteger(bytes, offset, length));
    }

    @Override
    public <T> T decodeInt8(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(getLong(bytes, offset, offset + length));
    }

    @Override
    public <T> T decodeFloat(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return decodeDouble(bytes, offset, length, vf);
    }

    @Override
    public <T> T decodeDouble(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromDouble(getDouble(bytes, offset, length));
    }

    @Override
    public <T> T decodeDecimal(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        BigDecimal d = new BigDecimal(StringUtils.toAsciiCharArray(bytes, offset, length));
        return vf.createFromBigDecimal(d);
    }

    @Override
    public <T> T decodeByteArray(byte[] bytes, int offset, int length, Field f, ValueFactory<T> vf) {
        return vf.createFromBytes(bytes, offset, length, f);
    }

    @Override
    public <T> T decodeBit(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromBit(bytes, offset, length);
    }

    @Override
    public <T> T decodeSet(byte[] bytes, int offset, int length, Field f, ValueFactory<T> vf) {
        return decodeByteArray(bytes, offset, length, f, vf);
    }

    @Override
    public <T> T decodeYear(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromYear(getLong(bytes, offset, offset + length));
    }

    public static int getInt(byte[] buf, int offset, int endpos) throws NumberFormatException {
        long l = getLong(buf, offset, endpos);
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
            throw new NumberOutOfRange(Messages.getString("StringUtils.badIntFormat", new Object[] { StringUtils.toString(buf, offset, endpos - offset) }));
        }
        return (int) l;
    }

    public static long getLong(byte[] buf, int offset, int endpos) throws NumberFormatException {
        int base = 10;

        int s = offset;

        /* Skip white space. */
        while (s < endpos && Character.isWhitespace((char) buf[s])) {
            ++s;
        }

        if (s == endpos) {
            throw new NumberFormatException(StringUtils.toString(buf));
        }

        /* Check for a sign. */
        boolean negative = false;

        if ((char) buf[s] == '-') {
            negative = true;
            ++s;
        } else if ((char) buf[s] == '+') {
            ++s;
        }

        /* Save the pointer so we can check later if anything happened. */
        int save = s;

        long cutoff = Long.MAX_VALUE / base;
        long cutlim = (int) (Long.MAX_VALUE % base);

        if (negative) {
            cutlim++;
        }

        boolean overflow = false;
        long i = 0;

        for (; s < endpos; s++) {
            char c = (char) buf[s];

            if (c >= '0' && c <= '9') {
                c -= '0';
            } else if (Character.isLetter(c)) {
                c = (char) (Character.toUpperCase(c) - 'A' + 10);
            } else {
                break;
            }

            if (c >= base) {
                break;
            }

            /* Check for overflow. */
            if (i > cutoff || i == cutoff && c > cutlim) {
                overflow = true;
            } else {
                i *= base;
                i += c;
            }
        }

        // no digits were parsed after a possible +/-
        if (s == save) {
            throw new NumberFormatException(
                    Messages.getString("StringUtils.badIntFormat", new Object[] { StringUtils.toString(buf, offset, endpos - offset) }));
        }

        if (overflow) {
            throw new NumberOutOfRange(Messages.getString("StringUtils.badIntFormat", new Object[] { StringUtils.toString(buf, offset, endpos - offset) }));
        }

        /* Return the result of the appropriate sign. */
        return negative ? -i : i;
    }

    public static BigInteger getBigInteger(byte[] buf, int offset, int length) throws NumberFormatException {
        BigInteger i = new BigInteger(StringUtils.toAsciiString(buf, offset, length));
        return i;
    }

    public static Double getDouble(byte[] bytes, int offset, int length) {
        return Double.parseDouble(StringUtils.toAsciiString(bytes, offset, length));
    }

    public static boolean isDate(String s) {
        return s.length() == DATE_BUF_LEN && s.charAt(4) == '-' && s.charAt(7) == '-'; // TODO also check proper date parts ranges
    }

    public static boolean isTime(String s) {
        Matcher matcher = TIME_PTRN.matcher(s);
        return matcher.matches();
    }

    public static boolean isTimestamp(String s) {
        Pattern DATETIME_PTRN = Pattern.compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d{1,9}){0,1}");
        Matcher matcher = DATETIME_PTRN.matcher(s);
        return matcher.matches();
    }

    public static InternalDate getDate(byte[] bytes, int offset, int length) {
        if (length != DATE_BUF_LEN) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "DATE" }));
        }
        int year = getInt(bytes, offset, offset + 4);
        int month = getInt(bytes, offset + 5, offset + 7);
        int day = getInt(bytes, offset + 8, offset + 10);
        return new InternalDate(year, month, day);
    }

    public static InternalTime getTime(byte[] bytes, int offset, int length, int scale) {
        int pos = 0;
        // used to track the length of the current time segment during parsing
        int segmentLen;

        if (length < TIME_STR_LEN_MIN || length > TIME_STR_LEN_MAX_WITH_MICROS) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "TIME" }));
        }

        boolean negative = false;

        if (bytes[offset] == '-') {
            pos++;
            negative = true;
        }

        // parse hours field
        for (segmentLen = 0; Character.isDigit((char) bytes[offset + pos + segmentLen]); segmentLen++) {

        }
        if (segmentLen == 0 || bytes[offset + pos + segmentLen] != ':') {
            throw new DataReadException(
                    Messages.getString("ResultSet.InvalidFormatForType", new Object[] { "TIME", StringUtils.toString(bytes, offset, length) }));
        }
        int hours = getInt(bytes, offset + pos, offset + pos + segmentLen);
        if (negative) {
            hours *= -1;
        }
        pos += segmentLen + 1; // +1 for ':' character

        // parse minutes field
        for (segmentLen = 0; Character.isDigit((char) bytes[offset + pos + segmentLen]); segmentLen++) {

        }
        if (segmentLen != 2 || bytes[offset + pos + segmentLen] != ':') {
            throw new DataReadException(
                    Messages.getString("ResultSet.InvalidFormatForType", new Object[] { "TIME", StringUtils.toString(bytes, offset, length) }));
        }
        int minutes = getInt(bytes, offset + pos, offset + pos + segmentLen);
        pos += segmentLen + 1;

        // parse seconds field
        for (segmentLen = 0; offset + pos + segmentLen < offset + length && Character.isDigit((char) bytes[offset + pos + segmentLen]); segmentLen++) {

        }
        if (segmentLen != 2) {
            throw new DataReadException(
                    Messages.getString("ResultSet.InvalidFormatForType", new Object[] { StringUtils.toString(bytes, offset, length), "TIME" }));
        }
        int seconds = getInt(bytes, offset + pos, offset + pos + segmentLen);
        pos += segmentLen;

        // parse optional microsecond fractional value
        int nanos = 0;
        if (length > pos) {
            pos++; // skip '.' character

            for (segmentLen = 0; offset + pos + segmentLen < offset + length && Character.isDigit((char) bytes[offset + pos + segmentLen]); segmentLen++) {

            }
            if (segmentLen + pos != length) {
                throw new DataReadException(
                        Messages.getString("ResultSet.InvalidFormatForType", new Object[] { StringUtils.toString(bytes, offset, length), "TIME" }));
            }
            nanos = getInt(bytes, offset + pos, offset + pos + segmentLen);
            // scale out nanos appropriately. mysql supports up to 6 digits of fractional seconds, each additional digit increasing the range by a factor of
            // 10. one digit is tenths, two is hundreths, etc
            nanos = nanos * (int) Math.pow(10, 9 - segmentLen);
        }

        return new InternalTime(hours, minutes, seconds, nanos, scale);
    }

    public static InternalTimestamp getTimestamp(byte[] bytes, int offset, int length, int scale) {
        if (length < TIMESTAMP_STR_LEN_NO_FRAC || length > TIMESTAMP_STR_LEN_WITH_MICROS && length != TIMESTAMP_STR_LEN_WITH_NANOS) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "TIMESTAMP" }));
        } else if (length != TIMESTAMP_STR_LEN_NO_FRAC) {
            // need at least two extra bytes for fractional, '.' and a digit
            if (bytes[offset + TIMESTAMP_STR_LEN_NO_FRAC] != (byte) '.' || length < TIMESTAMP_STR_LEN_NO_FRAC + 2) {
                throw new DataReadException(
                        Messages.getString("ResultSet.InvalidFormatForType", new Object[] { StringUtils.toString(bytes, offset, length), "TIMESTAMP" }));
            }
        }

        // delimiter verification
        if (bytes[offset + 4] != (byte) '-' || bytes[offset + 7] != (byte) '-' || bytes[offset + 10] != (byte) ' ' || bytes[offset + 13] != (byte) ':'
                || bytes[offset + 16] != (byte) ':') {
            throw new DataReadException(
                    Messages.getString("ResultSet.InvalidFormatForType", new Object[] { StringUtils.toString(bytes, offset, length), "TIMESTAMP" }));
        }

        int year = getInt(bytes, offset, offset + 4);
        int month = getInt(bytes, offset + 5, offset + 7);
        int day = getInt(bytes, offset + 8, offset + 10);
        int hours = getInt(bytes, offset + 11, offset + 13);
        int minutes = getInt(bytes, offset + 14, offset + 16);
        int seconds = getInt(bytes, offset + 17, offset + 19);
        // nanos from MySQL fractional
        int nanos;
        if (length == TIMESTAMP_STR_LEN_WITH_NANOS) {
            nanos = getInt(bytes, offset + 20, offset + length);
        } else {
            nanos = length == TIMESTAMP_STR_LEN_NO_FRAC ? 0 : getInt(bytes, offset + 20, offset + length);
            // scale out nanos appropriately. mysql supports up to 6 digits of fractional seconds, each additional digit increasing the range by a factor of
            // 10. one digit is tenths, two is hundreths, etc
            nanos = nanos * (int) Math.pow(10, 9 - (length - TIMESTAMP_STR_LEN_NO_FRAC - 1));
        }

        return new InternalTimestamp(year, month, day, hours, minutes, seconds, nanos, scale);
    }

}
