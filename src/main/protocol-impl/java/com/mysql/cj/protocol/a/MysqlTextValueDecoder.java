/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.protocol.a;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.DataReadException;
import com.mysql.cj.protocol.ValueDecoder;
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
    /** Max string length of MySQL time string (with microsecs): '-HHH:MM:SS.mmmmmm'. */
    public static final int TIME_STR_LEN_MAX = 17;
    /** String length of MySQL timestamp string (no microsecs): 'YYYY-MM-DD HH:MM:SS'. */
    public static final int TIMESTAMP_NOFRAC_STR_LEN = 19;
    /** Max string length of MySQL timestamp (with microsecs): 'YYYY-MM-DD HH:MM:SS.mmmmmm'. */
    public static final int TIMESTAMP_STR_LEN_MAX = TIMESTAMP_NOFRAC_STR_LEN + 7;
    /** String length of String timestamp with nanos. This does not come from MySQL server but we support it via string conversion. */
    public static final int TIMESTAMP_STR_LEN_WITH_NANOS = TIMESTAMP_NOFRAC_STR_LEN + 10;

    /** Max string length of a signed long = 9223372036854775807 (19+1 for minus sign) */
    private static final int MAX_SIGNED_LONG_LEN = 20;

    public <T> T decodeDate(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length != DATE_BUF_LEN) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "DATE" }));
        }
        int year = StringUtils.getInt(bytes, offset, offset + 4);
        int month = StringUtils.getInt(bytes, offset + 5, offset + 7);
        int day = StringUtils.getInt(bytes, offset + 8, offset + 10);
        return vf.createFromDate(year, month, day);
    }

    public <T> T decodeTime(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        int pos = 0;
        // used to track the length of the current time segment during parsing
        int segmentLen;

        if (length < TIME_STR_LEN_MIN || length > TIME_STR_LEN_MAX) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "TIME" }));
        }

        boolean negative = false;

        if (bytes[offset] == '-') {
            pos++;
            negative = true;
        }

        // parse hours field
        for (segmentLen = 0; Character.isDigit((char) bytes[offset + pos + segmentLen]); segmentLen++) {
            ;
        }
        if (segmentLen == 0 || bytes[offset + pos + segmentLen] != ':') {
            throw new DataReadException(
                    Messages.getString("ResultSet.InvalidFormatForType", new Object[] { "TIME", StringUtils.toString(bytes, offset, length) }));
        }
        int hours = StringUtils.getInt(bytes, offset + pos, offset + pos + segmentLen);
        if (negative) {
            hours *= -1;
        }
        pos += segmentLen + 1; // +1 for ':' character

        // parse minutes field
        for (segmentLen = 0; Character.isDigit((char) bytes[offset + pos + segmentLen]); segmentLen++) {
            ;
        }
        if (segmentLen != 2 || bytes[offset + pos + segmentLen] != ':') {
            throw new DataReadException(
                    Messages.getString("ResultSet.InvalidFormatForType", new Object[] { "TIME", StringUtils.toString(bytes, offset, length) }));
        }
        int minutes = StringUtils.getInt(bytes, offset + pos, offset + pos + segmentLen);
        pos += segmentLen + 1;

        // parse seconds field
        for (segmentLen = 0; offset + pos + segmentLen < offset + length && Character.isDigit((char) bytes[offset + pos + segmentLen]); segmentLen++) {
            ;
        }
        if (segmentLen != 2) {
            throw new DataReadException(
                    Messages.getString("ResultSet.InvalidFormatForType", new Object[] { StringUtils.toString(bytes, offset, length), "TIME" }));
        }
        int seconds = StringUtils.getInt(bytes, offset + pos, offset + pos + segmentLen);
        pos += segmentLen;

        // parse optional microsecond fractional value
        int nanos = 0;
        if (length > pos) {
            pos++; // skip '.' character

            for (segmentLen = 0; offset + pos + segmentLen < offset + length && Character.isDigit((char) bytes[offset + pos + segmentLen]); segmentLen++) {
                ;
            }
            if (segmentLen + pos != length) {
                throw new DataReadException(
                        Messages.getString("ResultSet.InvalidFormatForType", new Object[] { StringUtils.toString(bytes, offset, length), "TIME" }));
            }
            nanos = StringUtils.getInt(bytes, offset + pos, offset + pos + segmentLen);
            // scale out nanos appropriately. mysql supports up to 6 digits of fractional seconds, each additional digit increasing the range by a factor of
            // 10. one digit is tenths, two is hundreths, etc
            nanos = nanos * (int) Math.pow(10, 9 - segmentLen);
        }

        return vf.createFromTime(hours, minutes, seconds, nanos);
    }

    public <T> T decodeTimestamp(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length < TIMESTAMP_NOFRAC_STR_LEN || (length > TIMESTAMP_STR_LEN_MAX && length != TIMESTAMP_STR_LEN_WITH_NANOS)) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "TIMESTAMP" }));
        } else if (length != TIMESTAMP_NOFRAC_STR_LEN) {
            // need at least two extra bytes for fractional, '.' and a digit
            if (bytes[offset + TIMESTAMP_NOFRAC_STR_LEN] != (byte) '.' || length < TIMESTAMP_NOFRAC_STR_LEN + 2) {
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

        int year = StringUtils.getInt(bytes, offset, offset + 4);
        int month = StringUtils.getInt(bytes, offset + 5, offset + 7);
        int day = StringUtils.getInt(bytes, offset + 8, offset + 10);
        int hours = StringUtils.getInt(bytes, offset + 11, offset + 13);
        int minutes = StringUtils.getInt(bytes, offset + 14, offset + 16);
        int seconds = StringUtils.getInt(bytes, offset + 17, offset + 19);
        // nanos from MySQL fractional
        int nanos;
        if (length == TIMESTAMP_STR_LEN_WITH_NANOS) {
            nanos = StringUtils.getInt(bytes, offset + 20, offset + length);
        } else {
            nanos = (length == TIMESTAMP_NOFRAC_STR_LEN) ? 0 : StringUtils.getInt(bytes, offset + 20, offset + length);
            // scale out nanos appropriately. mysql supports up to 6 digits of fractional seconds, each additional digit increasing the range by a factor of
            // 10. one digit is tenths, two is hundreths, etc
            nanos = nanos * (int) Math.pow(10, 9 - (length - TIMESTAMP_NOFRAC_STR_LEN - 1));
        }

        return vf.createFromTimestamp(year, month, day, hours, minutes, seconds, nanos);
    }

    public <T> T decodeUInt1(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(StringUtils.getInt(bytes, offset, offset + length));
    }

    public <T> T decodeInt1(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(StringUtils.getInt(bytes, offset, offset + length));
    }

    public <T> T decodeUInt2(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(StringUtils.getInt(bytes, offset, offset + length));
    }

    public <T> T decodeInt2(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(StringUtils.getInt(bytes, offset, offset + length));
    }

    public <T> T decodeUInt4(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(StringUtils.getLong(bytes, offset, offset + length));
    }

    public <T> T decodeInt4(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(StringUtils.getInt(bytes, offset, offset + length));
    }

    public <T> T decodeUInt8(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        // treat as a signed long if possible to avoid BigInteger overhead
        if (length <= (MAX_SIGNED_LONG_LEN - 1) && bytes[0] >= '0' && bytes[0] <= '8') {
            return decodeInt8(bytes, offset, length, vf);
        }
        BigInteger i = new BigInteger(StringUtils.toAsciiString(bytes, offset, length));
        return vf.createFromBigInteger(i);
    }

    public <T> T decodeInt8(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromLong(StringUtils.getLong(bytes, offset, offset + length));
    }

    public <T> T decodeFloat(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return decodeDouble(bytes, offset, length, vf);
    }

    public <T> T decodeDouble(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        double d = Double.parseDouble(StringUtils.toAsciiString(bytes, offset, length));
        return vf.createFromDouble(d);
    }

    public <T> T decodeDecimal(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        BigDecimal d = new BigDecimal(StringUtils.toAsciiString(bytes, offset, length));
        return vf.createFromBigDecimal(d);
    }

    public <T> T decodeByteArray(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromBytes(bytes, offset, length);
    }

    public <T> T decodeBit(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return vf.createFromBit(bytes, offset, length);
    }

    @Override
    public <T> T decodeSet(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        return decodeByteArray(bytes, offset, length, vf);
    }
}
