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
 * A {@link com.mysql.cj.protocol.ValueDecoder} for the MySQL binary (prepared statement) protocol.
 */
public class MysqlBinaryValueDecoder implements ValueDecoder {

    public <T> T decodeTimestamp(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length == 0) {
            return vf.createFromTimestamp(0, 0, 0, 0, 0, 0, 0);
        } else if (length != NativeConstants.BIN_LEN_DATE && length != NativeConstants.BIN_LEN_TIMESTAMP && length != NativeConstants.BIN_LEN_TIMESTAMP_NO_US) {
            // the value can be any of these lengths (check protocol docs)
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "TIMESTAMP" }));
        }

        int year = 0;
        int month = 0;
        int day = 0;

        int hours = 0;
        int minutes = 0;
        int seconds = 0;

        int nanos = 0;

        year = (bytes[offset + 0] & 0xff) | ((bytes[offset + 1] & 0xff) << 8);
        month = bytes[offset + 2];
        day = bytes[offset + 3];

        if (length > NativeConstants.BIN_LEN_DATE) {
            hours = bytes[offset + 4];
            minutes = bytes[offset + 5];
            seconds = bytes[offset + 6];
        }

        if (length > NativeConstants.BIN_LEN_TIMESTAMP_NO_US) {
            // MySQL PS protocol uses microseconds
            nanos = 1000 * ((bytes[offset + 7] & 0xff) | ((bytes[offset + 8] & 0xff) << 8) | ((bytes[offset + 9] & 0xff) << 16)
                    | ((bytes[offset + 10] & 0xff) << 24));
        }

        return vf.createFromTimestamp(year, month, day, hours, minutes, seconds, nanos);
    }

    public <T> T decodeTime(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length == 0) {
            return vf.createFromTime(0, 0, 0, 0);
        } else if (length != NativeConstants.BIN_LEN_TIME && length != NativeConstants.BIN_LEN_TIME_NO_US) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "TIME" }));
        }

        int days = 0;
        int hours = 0;
        int minutes = 0;
        int seconds = 0;
        int nanos = 0;

        boolean negative = bytes[offset] == 1;

        days = (bytes[offset + 1] & 0xff) | ((bytes[offset + 2] & 0xff) << 8) | ((bytes[offset + 3] & 0xff) << 16) | ((bytes[offset + 4] & 0xff) << 24);
        hours = bytes[offset + 5];
        minutes = bytes[offset + 6];
        seconds = bytes[offset + 7];

        if (negative) {
            days *= -1;
        }

        if (length > NativeConstants.BIN_LEN_TIMESTAMP_NO_US) {
            // MySQL PS protocol uses microseconds
            nanos = 1000 * (bytes[offset + 1] & 0xff) | ((bytes[offset + 2] & 0xff) << 8) | ((bytes[offset + 3] & 0xff) << 16)
                    | ((bytes[offset + 4] & 0xff) << 24);
        }

        return vf.createFromTime(days * 24 + hours, minutes, seconds, nanos);
    }

    public <T> T decodeDate(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length == 0) {
            return vf.createFromDate(0, 0, 0);
        } else if (length != NativeConstants.BIN_LEN_DATE) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "DATE" }));
        }
        int year = (bytes[offset] & 0xff) | ((bytes[offset + 1] & 0xff) << 8);
        int month = bytes[offset + 2];
        int day = bytes[offset + 3];
        return vf.createFromDate(year, month, day);
    }

    public <T> T decodeUInt1(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length != NativeConstants.BIN_LEN_INT1) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "BYTE" }));
        }
        return vf.createFromLong(bytes[offset] & 0xff);
    }

    public <T> T decodeInt1(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length != NativeConstants.BIN_LEN_INT1) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "BYTE" }));
        }
        return vf.createFromLong(bytes[offset]);
    }

    public <T> T decodeUInt2(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length != NativeConstants.BIN_LEN_INT2) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "SHORT" }));
        }
        int asInt = ((bytes[offset] & 0xff) | ((bytes[offset + 1] & 0xff) << 8));
        return vf.createFromLong(asInt);
    }

    public <T> T decodeInt2(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length != NativeConstants.BIN_LEN_INT2) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "SHORT" }));
        }
        short asShort = (short) ((bytes[offset] & 0xff) | ((bytes[offset + 1] & 0xff) << 8));
        return vf.createFromLong(asShort);
    }

    public <T> T decodeUInt4(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length != NativeConstants.BIN_LEN_INT4) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "INT" }));
        }
        long asLong = (bytes[offset] & 0xff) | ((bytes[offset + 1] & 0xff) << 8) | ((bytes[offset + 2] & 0xff) << 16)
                | ((long) (bytes[offset + 3] & 0xff) << 24);
        return vf.createFromLong(asLong);
    }

    public <T> T decodeInt4(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length != NativeConstants.BIN_LEN_INT4) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "SHORT" }));
        }
        int asInt = (bytes[offset] & 0xff) | ((bytes[offset + 1] & 0xff) << 8) | ((bytes[offset + 2] & 0xff) << 16) | ((bytes[offset + 3] & 0xff) << 24);
        return vf.createFromLong(asInt);
    }

    public <T> T decodeInt8(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length != NativeConstants.BIN_LEN_INT8) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "LONG" }));
        }
        long asLong = (bytes[offset] & 0xff) | ((long) (bytes[offset + 1] & 0xff) << 8) | ((long) (bytes[offset + 2] & 0xff) << 16)
                | ((long) (bytes[offset + 3] & 0xff) << 24) | ((long) (bytes[offset + 4] & 0xff) << 32) | ((long) (bytes[offset + 5] & 0xff) << 40)
                | ((long) (bytes[offset + 6] & 0xff) << 48) | ((long) (bytes[offset + 7] & 0xff) << 56);
        return vf.createFromLong(asLong);
    }

    public <T> T decodeUInt8(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length != NativeConstants.BIN_LEN_INT8) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "LONG" }));
        }

        // don't use BigInteger unless sign bit is used
        if ((bytes[offset + 7] & 0x80) == 0) {
            return this.decodeInt8(bytes, offset, length, vf);
        }

        // first byte is 0 to indicate sign
        byte[] bigEndian = new byte[] { 0, bytes[offset + 7], bytes[offset + 6], bytes[offset + 5], bytes[offset + 4], bytes[offset + 3], bytes[offset + 2],
                bytes[offset + 1], bytes[offset] };
        BigInteger bigInt = new BigInteger(bigEndian);
        return vf.createFromBigInteger(bigInt);
    }

    public <T> T decodeFloat(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length != NativeConstants.BIN_LEN_FLOAT) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "FLOAT" }));
        }
        int asInt = (bytes[offset] & 0xff) | ((bytes[offset + 1] & 0xff) << 8) | ((bytes[offset + 2] & 0xff) << 16) | ((bytes[offset + 3] & 0xff) << 24);
        return vf.createFromDouble(Float.intBitsToFloat(asInt));
    }

    public <T> T decodeDouble(byte[] bytes, int offset, int length, ValueFactory<T> vf) {
        if (length != NativeConstants.BIN_LEN_DOUBLE) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidLengthForType", new Object[] { length, "DOUBLE" }));
        }
        long valueAsLong = (bytes[offset + 0] & 0xff) | ((long) (bytes[offset + 1] & 0xff) << 8) | ((long) (bytes[offset + 2] & 0xff) << 16)
                | ((long) (bytes[offset + 3] & 0xff) << 24) | ((long) (bytes[offset + 4] & 0xff) << 32) | ((long) (bytes[offset + 5] & 0xff) << 40)
                | ((long) (bytes[offset + 6] & 0xff) << 48) | ((long) (bytes[offset + 7] & 0xff) << 56);
        return vf.createFromDouble(Double.longBitsToDouble(valueAsLong));
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
