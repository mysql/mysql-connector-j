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

package com.mysql.cj.protocol.a;

import static com.mysql.cj.util.StringUtils.getBytes;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;

public class ValueEncoder {
    private NativePacketPayload packet;
    private String characterEncoding;
    private TimeZone timezone;

    public ValueEncoder(NativePacketPayload packet, String characterEncoding, TimeZone timezone) {
        this.packet = packet;
        this.characterEncoding = characterEncoding;
        this.timezone = timezone;
    }

    public void encodeValue(Object value, int fieldType) {
        if (value == null) {
            return;
        }
        switch (fieldType) {
            case MysqlType.FIELD_TYPE_TINY:
                encodeInt1(asByte(value));
                return;
            case MysqlType.FIELD_TYPE_SHORT:
                encodeInt2(asShort(value));
                return;
            case MysqlType.FIELD_TYPE_LONG:
            case MysqlType.FIELD_TYPE_FLOAT:
                encodeInt4(asInteger(value));
                return;
            case MysqlType.FIELD_TYPE_LONGLONG:
            case MysqlType.FIELD_TYPE_DOUBLE:
                encodeInt8(asLong(value));
                return;
            case MysqlType.FIELD_TYPE_DATE:
                encodeDate(asInternalDate(value));
                return;
            case MysqlType.FIELD_TYPE_TIME:
                encodeTime(asInternalTime(value));
                return;
            case MysqlType.FIELD_TYPE_DATETIME:
                encodeDateTime(asInternalTimestampNoTz(value));
                return;
            case MysqlType.FIELD_TYPE_TIMESTAMP:
                encodeTimeStamp(asInternalTimestampTz(value));
                return;
            case MysqlType.FIELD_TYPE_STRING:
                encodeString(asString(value));
                return;
        }
    }

    public void encodeInt1(Byte value) {
        this.packet.writeInteger(IntegerDataType.INT1, value.longValue());
    }

    public void encodeInt2(Short value) {
        this.packet.writeInteger(IntegerDataType.INT2, value.longValue());
    }

    public void encodeInt4(Integer value) {
        this.packet.writeInteger(IntegerDataType.INT4, value.longValue());
    }

    public void encodeInt8(Long value) {
        this.packet.writeInteger(IntegerDataType.INT8, value.longValue());
    }

    public void encodeDate(InternalDate date) {
        this.packet.ensureCapacity(NativeConstants.BIN_LEN_DATE + 1);
        this.packet.writeInteger(IntegerDataType.INT1, NativeConstants.BIN_LEN_DATE);
        this.packet.writeInteger(IntegerDataType.INT2, date.getYear());
        this.packet.writeInteger(IntegerDataType.INT1, date.getMonth());
        this.packet.writeInteger(IntegerDataType.INT1, date.getDay());
    }

    public void encodeTime(InternalTime time) {
        boolean hasFractionalSeconds = time.getNanos() > 0;
        this.packet.ensureCapacity((hasFractionalSeconds ? NativeConstants.BIN_LEN_TIME_WITH_MICROS : NativeConstants.BIN_LEN_TIME_NO_FRAC) + 1);
        this.packet.writeInteger(IntegerDataType.INT1, hasFractionalSeconds ? NativeConstants.BIN_LEN_TIME_WITH_MICROS : NativeConstants.BIN_LEN_TIME_NO_FRAC);
        this.packet.writeInteger(IntegerDataType.INT1, time.isNegative() ? 1 : 0);
        this.packet.writeInteger(IntegerDataType.INT4, time.getHours() / 24);
        this.packet.writeInteger(IntegerDataType.INT1, time.getHours() % 24);
        this.packet.writeInteger(IntegerDataType.INT1, time.getMinutes());
        this.packet.writeInteger(IntegerDataType.INT1, time.getSeconds());
        if (hasFractionalSeconds) {
            this.packet.writeInteger(IntegerDataType.INT4, TimeUnit.NANOSECONDS.toMicros(time.getNanos()));
        }
    }

    public void encodeDateTime(InternalTimestamp timestamp) {
        boolean hasFractionalSeconds = timestamp.getNanos() > 0;
        this.packet.ensureCapacity((hasFractionalSeconds ? NativeConstants.BIN_LEN_TIMESTAMP_WITH_MICROS : NativeConstants.BIN_LEN_TIMESTAMP_NO_FRAC) + 1);
        this.packet.writeInteger(IntegerDataType.INT1,
                hasFractionalSeconds ? NativeConstants.BIN_LEN_TIMESTAMP_WITH_MICROS : NativeConstants.BIN_LEN_TIMESTAMP_NO_FRAC); // length
        this.packet.writeInteger(IntegerDataType.INT2, timestamp.getYear());
        this.packet.writeInteger(IntegerDataType.INT1, timestamp.getMonth());
        this.packet.writeInteger(IntegerDataType.INT1, timestamp.getDay());
        this.packet.writeInteger(IntegerDataType.INT1, timestamp.getHours());
        this.packet.writeInteger(IntegerDataType.INT1, timestamp.getMinutes());
        this.packet.writeInteger(IntegerDataType.INT1, timestamp.getSeconds());
        if (hasFractionalSeconds) {
            this.packet.writeInteger(IntegerDataType.INT4, TimeUnit.NANOSECONDS.toMicros(timestamp.getNanos()));
        }
    }

    public void encodeTimeStamp(InternalTimestamp timestamp) {
        this.packet.ensureCapacity(NativeConstants.BIN_LEN_TIMESTAMP_WITH_TZ + 1);
        this.packet.writeInteger(IntegerDataType.INT1, NativeConstants.BIN_LEN_TIMESTAMP_WITH_TZ);
        this.packet.writeInteger(IntegerDataType.INT2, timestamp.getYear());
        this.packet.writeInteger(IntegerDataType.INT1, timestamp.getMonth());
        this.packet.writeInteger(IntegerDataType.INT1, timestamp.getDay());
        this.packet.writeInteger(IntegerDataType.INT1, timestamp.getHours());
        this.packet.writeInteger(IntegerDataType.INT1, timestamp.getMinutes());
        this.packet.writeInteger(IntegerDataType.INT1, timestamp.getSeconds());
        this.packet.writeInteger(IntegerDataType.INT4, TimeUnit.NANOSECONDS.toMicros(timestamp.getNanos()));
        this.packet.writeInteger(IntegerDataType.INT2, timestamp.getOffset());
    }

    public void encodeString(String value) {
        this.packet.writeBytes(StringSelfDataType.STRING_LENENC, getBytes(value, this.characterEncoding));
    }

    private Byte asByte(Object value) {
        if (Boolean.class.isInstance(value)) {
            return (Boolean) value ? new Byte((byte) 1) : new Byte((byte) 0);
        }

        if (Byte.class.isInstance(value)) {
            return (Byte) value;
        }

        throw ExceptionFactory.createException(WrongArgumentException.class,
                Messages.getString("ValueEncoder.WrongTinyIntValueType", new Object[] { value.getClass() }));
    }

    private Short asShort(Object value) {
        if (Short.class.isInstance(value)) {
            return (Short) value;
        }

        throw ExceptionFactory.createException(WrongArgumentException.class,
                Messages.getString("ValueEncoder.WrongSmallIntValueType", new Object[] { value.getClass() }));
    }

    private Integer asInteger(Object value) {
        if (Integer.class.isInstance(value)) {
            return (Integer) value;
        }

        if (Float.class.isInstance(value)) {
            return Float.floatToIntBits((Float) value);
        }

        throw ExceptionFactory.createException(WrongArgumentException.class,
                Messages.getString("ValueEncoder.WrongIntValueType", new Object[] { value.getClass() }));
    }

    private Long asLong(Object value) {
        if (Long.class.isInstance(value)) {
            return (Long) value;
        }

        if (Double.class.isInstance(value)) {
            return Double.doubleToLongBits((Double) value);
        }

        if (BigInteger.class.isInstance(value)) {
            return ((BigInteger) value).longValue();
        }

        if (BigDecimal.class.isInstance(value)) {
            return Double.doubleToLongBits(((BigDecimal) value).doubleValue());
        }

        throw ExceptionFactory.createException(WrongArgumentException.class,
                Messages.getString("ValueEncoder.WrongBigIntValueType", new Object[] { value.getClass() }));
    }

    private InternalDate asInternalDate(Object value) {
        if (LocalDate.class.isInstance(value)) {
            LocalDate localDate = (LocalDate) value;

            InternalDate internalDate = new InternalDate();
            internalDate.setYear(localDate.getYear());
            internalDate.setMonth(localDate.getMonthValue());
            internalDate.setDay(localDate.getDayOfMonth());
            return internalDate;
        }

        if (Date.class.isInstance(value)) {
            Calendar calendar = Calendar.getInstance(this.timezone);
            calendar.setTime((Date) value);
            calendar.set(Calendar.HOUR_OF_DAY, 0);
            calendar.set(Calendar.MINUTE, 0);
            calendar.set(Calendar.SECOND, 0);

            InternalDate internalDate = new InternalDate();
            internalDate.setYear(calendar.get(Calendar.YEAR));
            internalDate.setMonth(calendar.get(Calendar.MONTH) + 1);
            internalDate.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            return internalDate;
        }

        throw ExceptionFactory.createException(WrongArgumentException.class,
                Messages.getString("ValueEncoder.WrongDateValueType", new Object[] { value.getClass() }));
    }

    private InternalTime asInternalTime(Object value) {
        if (LocalTime.class.isInstance(value)) {
            LocalTime localTime = (LocalTime) value;

            InternalTime internalTime = new InternalTime();
            internalTime.setHours(localTime.getHour());
            internalTime.setMinutes(localTime.getMinute());
            internalTime.setSeconds(localTime.getSecond());
            internalTime.setNanos(localTime.getNano());
            return internalTime;
        }

        if (OffsetTime.class.isInstance(value)) {
            OffsetTime offsetTime = (OffsetTime) value;

            InternalTime internalTime = new InternalTime();
            internalTime.setHours(offsetTime.getHour());
            internalTime.setMinutes(offsetTime.getMinute());
            internalTime.setSeconds(offsetTime.getSecond());
            internalTime.setNanos(offsetTime.getNano());

            return internalTime;
        }

        if (Duration.class.isInstance(value)) {
            Duration duration = (Duration) value;
            Duration durationAbs = duration.abs();

            long fullSeconds = durationAbs.getSeconds();
            int seconds = (int) (fullSeconds % 60);
            long fullMinutes = fullSeconds / 60;
            int minutes = (int) (fullMinutes % 60);
            long fullHours = fullMinutes / 60;

            InternalTime internalTime = new InternalTime();
            internalTime.setNegative(duration.isNegative());
            internalTime.setHours((int) fullHours);
            internalTime.setMinutes(minutes);
            internalTime.setSeconds(seconds);
            internalTime.setNanos(durationAbs.getNano());
            return internalTime;
        }

        if (Time.class.isInstance(value)) {
            Time time = (Time) value;

            Calendar calendar = Calendar.getInstance(this.timezone);
            calendar.setTime(time);

            InternalTime internalTime = new InternalTime();
            internalTime.setHours(calendar.get(Calendar.HOUR_OF_DAY));
            internalTime.setMinutes(calendar.get(Calendar.MINUTE));
            internalTime.setSeconds(calendar.get(Calendar.SECOND));
            internalTime.setNanos((int) TimeUnit.MILLISECONDS.toNanos(calendar.get(Calendar.MILLISECOND)));
            return internalTime;
        }

        throw ExceptionFactory.createException(WrongArgumentException.class,
                Messages.getString("ValueEncoder.WrongTimeValueType", new Object[] { value.getClass() }));
    }

    private InternalTimestamp asInternalTimestampNoTz(Object value) {
        if (LocalDateTime.class.isInstance(value)) {
            LocalDateTime localDateTime = (LocalDateTime) value;

            InternalTimestamp internalTimestamp = new InternalTimestamp();
            internalTimestamp.setYear(localDateTime.getYear());
            internalTimestamp.setMonth(localDateTime.getMonthValue());
            internalTimestamp.setDay(localDateTime.getDayOfMonth());
            internalTimestamp.setHours(localDateTime.getHour());
            internalTimestamp.setMinutes(localDateTime.getMinute());
            internalTimestamp.setSeconds(localDateTime.getSecond());
            internalTimestamp.setNanos(localDateTime.getNano());
            return internalTimestamp;
        }

        throw ExceptionFactory.createException(WrongArgumentException.class,
                Messages.getString("ValueEncoder.WrongDatetimeValueType", new Object[] { value.getClass() }));
    }

    private InternalTimestamp asInternalTimestampTz(Object value) {
        if (Instant.class.isInstance(value)) {
            Instant instant = (Instant) value;
            OffsetDateTime offsetDateTime = instant.atOffset(ZoneOffset.UTC);

            InternalTimestamp internalTimestamp = new InternalTimestamp();
            internalTimestamp.setYear(offsetDateTime.getYear());
            internalTimestamp.setMonth(offsetDateTime.getMonthValue());
            internalTimestamp.setDay(offsetDateTime.getDayOfMonth());
            internalTimestamp.setHours(offsetDateTime.getHour());
            internalTimestamp.setMinutes(offsetDateTime.getMinute());
            internalTimestamp.setSeconds(offsetDateTime.getSecond());
            internalTimestamp.setNanos(offsetDateTime.getNano());
            internalTimestamp.setOffset(0); // UTC
            return internalTimestamp;
        }

        if (OffsetDateTime.class.isInstance(value)) {
            OffsetDateTime offsetDateTime = (OffsetDateTime) value;

            InternalTimestamp internalTimestamp = new InternalTimestamp();
            internalTimestamp.setYear(offsetDateTime.getYear());
            internalTimestamp.setMonth(offsetDateTime.getMonthValue());
            internalTimestamp.setDay(offsetDateTime.getDayOfMonth());
            internalTimestamp.setHours(offsetDateTime.getHour());
            internalTimestamp.setMinutes(offsetDateTime.getMinute());
            internalTimestamp.setSeconds(offsetDateTime.getSecond());
            internalTimestamp.setNanos(offsetDateTime.getNano());
            internalTimestamp.setOffset((int) TimeUnit.SECONDS.toMinutes(offsetDateTime.getOffset().getTotalSeconds()));
            return internalTimestamp;
        }

        if (ZonedDateTime.class.isInstance(value)) {
            ZonedDateTime zonedDateTime = (ZonedDateTime) value;

            InternalTimestamp internalTimestamp = new InternalTimestamp();
            internalTimestamp.setYear(zonedDateTime.getYear());
            internalTimestamp.setMonth(zonedDateTime.getMonthValue());
            internalTimestamp.setDay(zonedDateTime.getDayOfMonth());
            internalTimestamp.setHours(zonedDateTime.getHour());
            internalTimestamp.setMinutes(zonedDateTime.getMinute());
            internalTimestamp.setSeconds(zonedDateTime.getSecond());
            internalTimestamp.setNanos(zonedDateTime.getNano());
            internalTimestamp.setOffset((int) TimeUnit.SECONDS.toMinutes(zonedDateTime.getOffset().getTotalSeconds()));
            return internalTimestamp;

        }

        if (Calendar.class.isInstance(value)) {
            Calendar calendar = (Calendar) value;

            InternalTimestamp internalTimestamp = new InternalTimestamp();
            internalTimestamp.setYear(calendar.get(Calendar.YEAR));
            internalTimestamp.setMonth(calendar.get(Calendar.MONTH) + 1);
            internalTimestamp.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            internalTimestamp.setHours(calendar.get(Calendar.HOUR_OF_DAY));
            internalTimestamp.setMinutes(calendar.get(Calendar.MINUTE));
            internalTimestamp.setSeconds(calendar.get(Calendar.SECOND));
            internalTimestamp.setNanos((int) TimeUnit.MILLISECONDS.toNanos(calendar.get(Calendar.MILLISECOND)));
            internalTimestamp.setOffset((int) TimeUnit.MILLISECONDS.toMinutes(calendar.getTimeZone().getOffset(calendar.getTimeInMillis())));
            return internalTimestamp;

        }

        if (Timestamp.class.isInstance(value)) { // must be checked before java.util.Date.
            Timestamp timestamp = (Timestamp) value;

            Calendar calendar = Calendar.getInstance(this.timezone);
            calendar.setTime(timestamp);

            InternalTimestamp internalTimestamp = new InternalTimestamp();
            internalTimestamp.setYear(calendar.get(Calendar.YEAR));
            internalTimestamp.setMonth(calendar.get(Calendar.MONTH) + 1);
            internalTimestamp.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            internalTimestamp.setHours(calendar.get(Calendar.HOUR_OF_DAY));
            internalTimestamp.setMinutes(calendar.get(Calendar.MINUTE));
            internalTimestamp.setSeconds(calendar.get(Calendar.SECOND));
            internalTimestamp.setNanos(timestamp.getNanos());
            internalTimestamp.setOffset((int) TimeUnit.MILLISECONDS.toMinutes(calendar.getTimeZone().getOffset(calendar.getTimeInMillis())));
            return internalTimestamp;
        }

        if (java.util.Date.class.isInstance(value)) {
            java.util.Date date = (java.util.Date) value;

            Calendar calendar = Calendar.getInstance(this.timezone);
            calendar.setTime(date);

            InternalTimestamp internalTimestamp = new InternalTimestamp();
            internalTimestamp.setYear(calendar.get(Calendar.YEAR));
            internalTimestamp.setMonth(calendar.get(Calendar.MONTH) + 1);
            internalTimestamp.setDay(calendar.get(Calendar.DAY_OF_MONTH));
            internalTimestamp.setHours(calendar.get(Calendar.HOUR_OF_DAY));
            internalTimestamp.setMinutes(calendar.get(Calendar.MINUTE));
            internalTimestamp.setSeconds(calendar.get(Calendar.SECOND));
            internalTimestamp.setNanos((int) TimeUnit.MILLISECONDS.toNanos(calendar.get(Calendar.MILLISECOND)));
            internalTimestamp.setOffset((int) TimeUnit.MILLISECONDS.toMinutes(calendar.getTimeZone().getOffset(date.getTime())));
            return internalTimestamp;
        }

        throw ExceptionFactory.createException(WrongArgumentException.class,
                Messages.getString("ValueEncoder.WrongTimestampValueType", new Object[] { value.getClass() }));
    }

    private String asString(Object value) {
        if (String.class.isInstance(value)) {
            return (String) value;
        }

        return value.toString();
    }
}
