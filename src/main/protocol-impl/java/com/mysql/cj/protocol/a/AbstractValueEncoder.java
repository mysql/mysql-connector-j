/*
 * Copyright (c) 2022, 2023, Oracle and/or its affiliates.
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

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.concurrent.TimeUnit;

import com.mysql.cj.BindValue;
import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.ServerSession;
import com.mysql.cj.protocol.ValueEncoder;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringLengthDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.result.Field;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;

public abstract class AbstractValueEncoder implements ValueEncoder {

    protected PropertySet propertySet;
    protected ServerSession serverSession;
    protected ExceptionInterceptor exceptionInterceptor;
    protected RuntimeProperty<String> charEncoding = null;
    protected RuntimeProperty<Boolean> sendFractionalSeconds;
    protected RuntimeProperty<Boolean> sendFractionalSecondsForTime;
    protected RuntimeProperty<Boolean> preserveInstants;

    @Override
    public void init(PropertySet pset, ServerSession serverSess, ExceptionInterceptor excInterceptor) {
        this.propertySet = pset;
        this.serverSession = serverSess;
        this.exceptionInterceptor = excInterceptor;
        this.charEncoding = pset.getStringProperty(PropertyKey.characterEncoding);
        this.sendFractionalSeconds = pset.getBooleanProperty(PropertyKey.sendFractionalSeconds);
        this.sendFractionalSecondsForTime = pset.getBooleanProperty(PropertyKey.sendFractionalSecondsForTime);
        this.preserveInstants = pset.getBooleanProperty(PropertyKey.preserveInstants);
    }

    @Override
    public byte[] getBytes(BindValue binding) {
        return StringUtils.getBytes(getString(binding), this.charEncoding.getValue());
    }

    @Override
    public void encodeAsText(Message msg, BindValue binding) {
        NativePacketPayload intoPacket = (NativePacketPayload) msg;
        intoPacket.writeBytes(StringLengthDataType.STRING_FIXED, getBytes(binding));
    }

    @Override
    public void encodeAsQueryAttribute(Message msg, BindValue binding) {
        NativePacketPayload intoPacket = (NativePacketPayload) msg;
        String x = binding.getValue().toString();
        intoPacket.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes(x, this.charEncoding.getValue()));
    }

    protected BigDecimal getScaled(BigDecimal x, long scaleOrLength) {
        BigDecimal scaledBigDecimal;
        if (scaleOrLength < 0) {
            return x.setScale(x.scale());
        }
        try {
            scaledBigDecimal = x.setScale((int) scaleOrLength);
        } catch (ArithmeticException ex) {
            try {
                scaledBigDecimal = x.setScale((int) scaleOrLength, BigDecimal.ROUND_HALF_UP);
            } catch (ArithmeticException arEx) {
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("PreparedStatement.65", new Object[] { scaleOrLength, x.toPlainString() }), this.exceptionInterceptor);
            }
        }
        return scaledBigDecimal;
    }

    protected LocalTime adjustLocalTime(LocalTime x, Field f) {
        if (!this.serverSession.getCapabilities().serverSupportsFracSecs() || !this.sendFractionalSeconds.getValue()) {
            if (x.getNano() > 0) {
                x = x.withNano(0); // truncate nanoseconds
            }
            return x;
        }
        return TimeUtil.adjustNanosPrecision(x, f == null ? 6 : f.getDecimals(), !this.serverSession.isServerTruncatesFracSecs());
    }

    protected LocalDateTime adjustLocalDateTime(LocalDateTime x, Field f) {
        if (!this.serverSession.getCapabilities().serverSupportsFracSecs() || !this.sendFractionalSeconds.getValue()) {
            if (x.getNano() > 0) {
                x = x.withNano(0); // truncate nanoseconds
            }
            return x;
        }
        return TimeUtil.adjustNanosPrecision(x, f == null ? 6 : f.getDecimals(), !this.serverSession.isServerTruncatesFracSecs());
    }

    protected Duration adjustDuration(Duration x, Field f) {
        if (!this.serverSession.getCapabilities().serverSupportsFracSecs() || !this.sendFractionalSeconds.getValue()) {
            if (x.getNano() > 0) {
                x = x.isNegative() ? x.plusSeconds(1).withNanos(0) : x.withNanos(0); // truncate nanoseconds
            }
            return x;
        }
        return TimeUtil.adjustNanosPrecision(x, f == null ? 6 : f.getDecimals(), !this.serverSession.isServerTruncatesFracSecs());
    }

    protected Timestamp adjustTimestamp(Timestamp x, Field f, boolean keepOrigNanos) {
        if (keepOrigNanos) {
            return x; // if the value is set as a key for UpdatableResultSet updater, it should keep original milliseconds
        }
        if (!this.serverSession.getCapabilities().serverSupportsFracSecs() || !this.sendFractionalSeconds.getValue()) {
            return TimeUtil.truncateFractionalSeconds(x);
        }
        return TimeUtil.adjustNanosPrecision(x, f == null ? 6 : f.getDecimals(), !this.serverSession.isServerTruncatesFracSecs());
    }

    protected Time adjustTime(Time x) {
        if (!this.serverSession.getCapabilities().serverSupportsFracSecs() || !this.sendFractionalSeconds.getValue()
                || !this.sendFractionalSecondsForTime.getValue()) {
            return TimeUtil.truncateFractionalSeconds(x);
        }
        return x;
    }

    protected void writeDate(Message msg, InternalDate d) {
        NativePacketPayload intoPacket = (NativePacketPayload) msg;
        intoPacket.ensureCapacity(NativeConstants.BIN_LEN_DATE + 1);
        intoPacket.writeInteger(IntegerDataType.INT1, NativeConstants.BIN_LEN_DATE);
        intoPacket.writeInteger(IntegerDataType.INT2, d.getYear());
        intoPacket.writeInteger(IntegerDataType.INT1, d.getMonth());
        intoPacket.writeInteger(IntegerDataType.INT1, d.getDay());
    }

    protected void writeTime(Message msg, InternalTime time) {
        NativePacketPayload intoPacket = (NativePacketPayload) msg;
        boolean hasFractionalSeconds = time.getNanos() > 0;
        intoPacket.ensureCapacity((hasFractionalSeconds ? NativeConstants.BIN_LEN_TIME_WITH_MICROS : NativeConstants.BIN_LEN_TIME_NO_FRAC) + 1);
        intoPacket.writeInteger(IntegerDataType.INT1, hasFractionalSeconds ? NativeConstants.BIN_LEN_TIME_WITH_MICROS : NativeConstants.BIN_LEN_TIME_NO_FRAC);
        intoPacket.writeInteger(IntegerDataType.INT1, time.isNegative() ? 1 : 0);
        intoPacket.writeInteger(IntegerDataType.INT4, time.getHours() / 24);
        intoPacket.writeInteger(IntegerDataType.INT1, time.getHours() % 24);
        intoPacket.writeInteger(IntegerDataType.INT1, time.getMinutes());
        intoPacket.writeInteger(IntegerDataType.INT1, time.getSeconds());
        if (hasFractionalSeconds) {
            intoPacket.writeInteger(IntegerDataType.INT4, TimeUnit.NANOSECONDS.toMicros(time.getNanos()));
        }
    }

    protected void writeDateTime(Message msg, InternalTimestamp ts) {
        NativePacketPayload intoPacket = (NativePacketPayload) msg;
        long microseconds = TimeUnit.NANOSECONDS.toMicros(ts.getNanos());
        intoPacket.ensureCapacity((microseconds > 0 ? NativeConstants.BIN_LEN_TIMESTAMP_WITH_MICROS : NativeConstants.BIN_LEN_TIMESTAMP_NO_FRAC) + 1);
        intoPacket.writeInteger(IntegerDataType.INT1,
                microseconds > 0 ? NativeConstants.BIN_LEN_TIMESTAMP_WITH_MICROS : NativeConstants.BIN_LEN_TIMESTAMP_NO_FRAC);
        intoPacket.writeInteger(IntegerDataType.INT2, ts.getYear());
        intoPacket.writeInteger(IntegerDataType.INT1, ts.getMonth());
        intoPacket.writeInteger(IntegerDataType.INT1, ts.getDay());
        intoPacket.writeInteger(IntegerDataType.INT1, ts.getHours());
        intoPacket.writeInteger(IntegerDataType.INT1, ts.getMinutes());
        intoPacket.writeInteger(IntegerDataType.INT1, ts.getSeconds());
        if (microseconds > 0) {
            intoPacket.writeInteger(IntegerDataType.INT4, microseconds);
        }
    }

    public void writeDateTimeWithOffset(Message msg, InternalTimestamp timestamp) {
        NativePacketPayload intoPacket = (NativePacketPayload) msg;
        intoPacket.ensureCapacity(NativeConstants.BIN_LEN_TIMESTAMP_WITH_TZ + 1);
        intoPacket.writeInteger(IntegerDataType.INT1, NativeConstants.BIN_LEN_TIMESTAMP_WITH_TZ);
        intoPacket.writeInteger(IntegerDataType.INT2, timestamp.getYear());
        intoPacket.writeInteger(IntegerDataType.INT1, timestamp.getMonth());
        intoPacket.writeInteger(IntegerDataType.INT1, timestamp.getDay());
        intoPacket.writeInteger(IntegerDataType.INT1, timestamp.getHours());
        intoPacket.writeInteger(IntegerDataType.INT1, timestamp.getMinutes());
        intoPacket.writeInteger(IntegerDataType.INT1, timestamp.getSeconds());
        intoPacket.writeInteger(IntegerDataType.INT4, TimeUnit.NANOSECONDS.toMicros(timestamp.getNanos()));
        intoPacket.writeInteger(IntegerDataType.INT2, timestamp.getOffset());
    }

    protected byte[] escapeBytesIfNeeded(byte[] x) {
        if (this.serverSession.isNoBackslashEscapesSet() || this.serverSession.getCharsetSettings().isMultibyteCharset(this.charEncoding.getValue())) {

            // Send as hex
            ByteArrayOutputStream bOut = new ByteArrayOutputStream(x.length * 2 + 3);
            bOut.write('x');
            bOut.write('\'');

            StringUtils.hexEscapeBlock(x, x.length, (lowBits, highBits) -> {
                bOut.write(lowBits);
                bOut.write(highBits);
            });
            bOut.write('\'');
            return bOut.toByteArray();
        }

        // escape them
        ByteArrayOutputStream bOut = new ByteArrayOutputStream(x.length + 9);
        bOut.write('_');
        bOut.write('b');
        bOut.write('i');
        bOut.write('n');
        bOut.write('a');
        bOut.write('r');
        bOut.write('y');
        bOut.write('\'');
        StringUtils.escapeBytes(bOut, x);
        bOut.write('\'');
        return bOut.toByteArray();
    }

    @Override
    public long getTextLength(BindValue binding) {
        if (binding.isNull()) {
            return 4 /* for NULL literal in SQL */;
        }
        return binding.isStream() && binding.getScaleOrLength() != -1 ? binding.getScaleOrLength() * 2 /* for safety in escaping */
                : binding.getByteValue().length;
    }

    @Override
    public long getBinaryLength(BindValue binding) {
        if (binding.isNull()) {
            return 0;
        }

        if (binding.isStream() && binding.getScaleOrLength() != -1) {
            return binding.getScaleOrLength();
        }

        int bufferType = binding.getFieldType();
        switch (bufferType) {
            case MysqlType.FIELD_TYPE_TINY:
                return NativeConstants.BIN_LEN_INT1;
            case MysqlType.FIELD_TYPE_SHORT:
                return NativeConstants.BIN_LEN_INT2;
            case MysqlType.FIELD_TYPE_LONG:
                return NativeConstants.BIN_LEN_INT4;
            case MysqlType.FIELD_TYPE_LONGLONG:
                return NativeConstants.BIN_LEN_INT8;
            case MysqlType.FIELD_TYPE_FLOAT:
                return NativeConstants.BIN_LEN_FLOAT;
            case MysqlType.FIELD_TYPE_DOUBLE:
                return NativeConstants.BIN_LEN_DOUBLE;
            case MysqlType.FIELD_TYPE_DATE:
                return NativeConstants.BIN_LEN_DATE + 1;
            case MysqlType.FIELD_TYPE_TIME:
                return NativeConstants.BIN_LEN_TIME_WITH_MICROS + 1;
            case MysqlType.FIELD_TYPE_DATETIME:
                return NativeConstants.BIN_LEN_TIMESTAMP_WITH_MICROS + 1;
            case MysqlType.FIELD_TYPE_TIMESTAMP:
                return NativeConstants.BIN_LEN_TIMESTAMP_WITH_TZ + 1;
            case MysqlType.FIELD_TYPE_VAR_STRING:
                return binding.getValue().toString().length() + 9;
            case MysqlType.FIELD_TYPE_STRING:
            case MysqlType.FIELD_TYPE_VARCHAR:
            case MysqlType.FIELD_TYPE_DECIMAL:
            case MysqlType.FIELD_TYPE_NEWDECIMAL:
                if (binding.getValue() instanceof byte[]) {
                    return ((byte[]) binding.getValue()).length;
                } else if (binding.getValue() instanceof BigDecimal) {
                    return ((BigDecimal) binding.getValue()).toPlainString().length();
                }
                return ((String) binding.getValue()).length();
            default:
                return 0;
        }
    }

}
