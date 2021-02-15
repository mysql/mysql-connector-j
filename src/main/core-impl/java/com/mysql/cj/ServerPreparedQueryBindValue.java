/*
 * Copyright (c) 2017, 2021, Oracle and/or its affiliates.
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

package com.mysql.cj;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.protocol.a.NativePacketPayload;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;

//TODO should not be protocol-specific

public class ServerPreparedQueryBindValue extends ClientPreparedQueryBindValue implements BindValue {

    public long boundBeforeExecutionNum = 0;

    public int bufferType;

    /* Calendar to be used for DATE and DATETIME values storing */
    public Calendar calendar;

    PropertySet pset;
    private TimeZone defaultTimeZone;
    private TimeZone connectionTimeZone;
    private RuntimeProperty<Boolean> cacheDefaultTimeZone = null;

    protected String charEncoding = null;

    public ServerPreparedQueryBindValue(TimeZone defaultTimeZone, TimeZone connectionTimeZone, PropertySet pset) {
        this.pset = pset;
        this.defaultTimeZone = defaultTimeZone;
        this.connectionTimeZone = connectionTimeZone;
        this.cacheDefaultTimeZone = pset.getBooleanProperty(PropertyKey.cacheDefaultTimeZone);
    }

    @Override
    public ServerPreparedQueryBindValue clone() {
        return new ServerPreparedQueryBindValue(this);
    }

    private ServerPreparedQueryBindValue(ServerPreparedQueryBindValue copyMe) {
        super(copyMe);

        this.pset = copyMe.pset;
        this.defaultTimeZone = copyMe.defaultTimeZone;
        this.connectionTimeZone = copyMe.connectionTimeZone;
        this.cacheDefaultTimeZone = copyMe.cacheDefaultTimeZone;
        this.bufferType = copyMe.bufferType;
        this.calendar = copyMe.calendar;
        this.charEncoding = copyMe.charEncoding;
    }

    @Override
    public void reset() {
        super.reset();
        this.calendar = null;
        this.charEncoding = null;
    }

    /**
     * Reset a bind value to be used for a new value of the given type.
     * 
     * @param bufType
     *            MysqlType.FIELD_TYPE_*
     * @param numberOfExecutions
     *            current number of PreparedQuery executions
     * @return true if we need to send/resend types to the server
     */
    public boolean resetToType(int bufType, long numberOfExecutions) {
        boolean sendTypesToServer = false;

        // clear any possible old value
        reset();

        if (bufType == MysqlType.FIELD_TYPE_NULL && this.bufferType != 0) {
            // preserve the previous type to (possibly) avoid sending types at execution time
        } else if (this.bufferType != bufType) {
            sendTypesToServer = true;
            this.bufferType = bufType;
        }

        // setup bind value for use
        this.isSet = true;
        this.boundBeforeExecutionNum = numberOfExecutions;
        return sendTypesToServer;
    }

    @Override
    public String toString() {
        return toString(false);
    }

    public String toString(boolean quoteIfNeeded) {
        if (this.isStream) {
            return "' STREAM DATA '";
        }

        if (this.isNull) {
            return "NULL";
        }

        DateTimeFormatter timeFmtWithOptMicros = new DateTimeFormatterBuilder().appendPattern("HH:mm:ss").appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
                .toFormatter();
        DateTimeFormatter datetimeFmtWithOptMicros = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss")
                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true).toFormatter();

        switch (this.bufferType) {
            case MysqlType.FIELD_TYPE_TINY:
            case MysqlType.FIELD_TYPE_SHORT:
            case MysqlType.FIELD_TYPE_LONG:
            case MysqlType.FIELD_TYPE_LONGLONG:
                return String.valueOf(((Long) this.value).longValue());
            case MysqlType.FIELD_TYPE_FLOAT:
                return String.valueOf(((Float) this.value).floatValue());
            case MysqlType.FIELD_TYPE_DOUBLE:
                return String.valueOf(((Double) this.value).doubleValue());
            case MysqlType.FIELD_TYPE_TIME:
                String s;
                if (this.value instanceof LocalDateTime) {
                    s = ((LocalDateTime) this.value).format(timeFmtWithOptMicros);
                } else if (this.value instanceof LocalTime) {
                    s = ((LocalTime) this.value).format(timeFmtWithOptMicros);
                } else if (this.value instanceof Duration) {
                    s = TimeUtil.getDurationString(((Duration) this.value));
                } else {
                    s = String.valueOf(this.value);
                }
                return "'" + s + "'";
            case MysqlType.FIELD_TYPE_DATE:
                if (this.value instanceof LocalDate) {
                    s = ((LocalDate) this.value).format(TimeUtil.DATE_FORMATTER);
                } else if (this.value instanceof LocalTime) {
                    s = ((LocalTime) this.value).atDate(LocalDate.of(1970, 1, 1)).format(TimeUtil.DATE_FORMATTER);
                } else if (this.value instanceof LocalDateTime) {
                    s = ((LocalDateTime) this.value).format(TimeUtil.DATE_FORMATTER);
                } else {
                    s = String.valueOf(this.value);
                }
                return "'" + s + "'";
            case MysqlType.FIELD_TYPE_DATETIME:
            case MysqlType.FIELD_TYPE_TIMESTAMP:
                if (this.value instanceof LocalDate) {
                    s = ((LocalDate) this.value).format(datetimeFmtWithOptMicros);
                } else if (this.value instanceof LocalTime) {
                    s = ((LocalTime) this.value).atDate(LocalDate.of(1970, 1, 1)).format(timeFmtWithOptMicros);
                } else if (this.value instanceof LocalDateTime) {
                    s = ((LocalDateTime) this.value).format(datetimeFmtWithOptMicros);
                } else {
                    s = String.valueOf(this.value);
                }
                return "'" + s + "'";
            case MysqlType.FIELD_TYPE_VAR_STRING:
            case MysqlType.FIELD_TYPE_STRING:
            case MysqlType.FIELD_TYPE_VARCHAR:
                if (quoteIfNeeded) {
                    return "'" + String.valueOf(this.value) + "'";
                }
                return String.valueOf(this.value);

            default:
                if (this.value instanceof byte[]) {
                    return "byte data";
                }
                if (quoteIfNeeded) {
                    return "'" + String.valueOf(this.value) + "'";
                }
                return String.valueOf(this.value);
        }
    }

    public long getBoundLength() {
        if (this.isNull) {
            return 0;
        }

        if (this.isStream) {
            return this.streamLength;
        }

        switch (this.bufferType) {

            case MysqlType.FIELD_TYPE_TINY:
                return 1;
            case MysqlType.FIELD_TYPE_SHORT:
                return 2;
            case MysqlType.FIELD_TYPE_LONG:
                return 4;
            case MysqlType.FIELD_TYPE_LONGLONG:
                return 8;
            case MysqlType.FIELD_TYPE_FLOAT:
                return 4;
            case MysqlType.FIELD_TYPE_DOUBLE:
                return 8;
            case MysqlType.FIELD_TYPE_TIME:
                return 9;
            case MysqlType.FIELD_TYPE_DATE:
                return 7;
            case MysqlType.FIELD_TYPE_DATETIME:
            case MysqlType.FIELD_TYPE_TIMESTAMP:
                return 11;
            case MysqlType.FIELD_TYPE_VAR_STRING:
            case MysqlType.FIELD_TYPE_STRING:
            case MysqlType.FIELD_TYPE_VARCHAR:
            case MysqlType.FIELD_TYPE_DECIMAL:
            case MysqlType.FIELD_TYPE_NEWDECIMAL:
                if (this.value instanceof byte[]) {
                    return ((byte[]) this.value).length;
                }
                return ((String) this.value).length();

            default:
                return 0;
        }
    }

    public void storeBinding(NativePacketPayload intoPacket, boolean isLoadDataQuery, String characterEncoding, ExceptionInterceptor interceptor) {
        synchronized (this) {
            try {
                // Handle primitives first
                switch (this.bufferType) {

                    case MysqlType.FIELD_TYPE_TINY:
                        intoPacket.writeInteger(IntegerDataType.INT1, ((Long) this.value).longValue());
                        return;
                    case MysqlType.FIELD_TYPE_SHORT:
                        intoPacket.writeInteger(IntegerDataType.INT2, ((Long) this.value).longValue());
                        return;
                    case MysqlType.FIELD_TYPE_LONG:
                        intoPacket.writeInteger(IntegerDataType.INT4, ((Long) this.value).longValue());
                        return;
                    case MysqlType.FIELD_TYPE_LONGLONG:
                        intoPacket.writeInteger(IntegerDataType.INT8, ((Long) this.value).longValue());
                        return;
                    case MysqlType.FIELD_TYPE_FLOAT:
                        intoPacket.writeInteger(IntegerDataType.INT4, Float.floatToIntBits(((Float) this.value).floatValue()));
                        return;
                    case MysqlType.FIELD_TYPE_DOUBLE:
                        intoPacket.writeInteger(IntegerDataType.INT8, Double.doubleToLongBits(((Double) this.value).doubleValue()));
                        return;
                    case MysqlType.FIELD_TYPE_TIME:
                        storeTime(intoPacket);
                        return;
                    case MysqlType.FIELD_TYPE_DATE:
                        storeDate(intoPacket);
                        return;
                    case MysqlType.FIELD_TYPE_DATETIME:
                    case MysqlType.FIELD_TYPE_TIMESTAMP:
                        storeDateTime(intoPacket, this.bufferType);
                        return;
                    case MysqlType.FIELD_TYPE_VAR_STRING:
                    case MysqlType.FIELD_TYPE_STRING:
                    case MysqlType.FIELD_TYPE_VARCHAR:
                    case MysqlType.FIELD_TYPE_DECIMAL:
                    case MysqlType.FIELD_TYPE_NEWDECIMAL:
                        if (this.value instanceof byte[]) {
                            intoPacket.writeBytes(StringSelfDataType.STRING_LENENC, (byte[]) this.value);
                        } else if (!isLoadDataQuery) {
                            intoPacket.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes((String) this.value, characterEncoding));
                        } else {
                            intoPacket.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes((String) this.value));
                        }

                        return;
                }

            } catch (CJException uEE) {
                throw ExceptionFactory.createException(Messages.getString("ServerPreparedStatement.22") + characterEncoding + "'", uEE, interceptor);
            }
        }
    }

    private void storeDate(NativePacketPayload intoPacket) {
        synchronized (this) {
            int year, month, day;

            if (this.value instanceof LocalDate) {
                year = ((LocalDate) this.value).getYear();
                month = ((LocalDate) this.value).getMonthValue();
                day = ((LocalDate) this.value).getDayOfMonth();

            } else if (this.value instanceof LocalTime) {
                year = AbstractQueryBindings.DEFAULT_DATE.getYear();
                month = AbstractQueryBindings.DEFAULT_DATE.getMonthValue();
                day = AbstractQueryBindings.DEFAULT_DATE.getDayOfMonth();

            } else if (this.value instanceof LocalDateTime) {
                year = ((LocalDateTime) this.value).getYear();
                month = ((LocalDateTime) this.value).getMonthValue();
                day = ((LocalDateTime) this.value).getDayOfMonth();

            } else {
                if (this.calendar == null) {
                    this.calendar = Calendar.getInstance(this.cacheDefaultTimeZone.getValue() ? this.defaultTimeZone : TimeZone.getDefault(), Locale.US);
                }

                this.calendar.setTime((java.util.Date) this.value);

                this.calendar.set(Calendar.HOUR_OF_DAY, 0);
                this.calendar.set(Calendar.MINUTE, 0);
                this.calendar.set(Calendar.SECOND, 0);

                year = this.calendar.get(Calendar.YEAR);
                month = this.calendar.get(Calendar.MONTH) + 1;
                day = this.calendar.get(Calendar.DAY_OF_MONTH);
            }

            intoPacket.ensureCapacity(5);
            intoPacket.writeInteger(IntegerDataType.INT1, 4); // length
            intoPacket.writeInteger(IntegerDataType.INT2, year);
            intoPacket.writeInteger(IntegerDataType.INT1, month);
            intoPacket.writeInteger(IntegerDataType.INT1, day);
        }
    }

    private void storeTime(NativePacketPayload intoPacket) {
        int neg = 0, days = 0, hours, minutes, seconds, microseconds;

        if (this.value instanceof LocalDateTime) {
            hours = ((LocalDateTime) this.value).getHour();
            minutes = ((LocalDateTime) this.value).getMinute();
            seconds = ((LocalDateTime) this.value).getSecond();
            microseconds = ((LocalDateTime) this.value).getNano() / 1000;
        } else if (this.value instanceof LocalTime) {
            hours = ((LocalTime) this.value).getHour();
            minutes = ((LocalTime) this.value).getMinute();
            seconds = ((LocalTime) this.value).getSecond();
            microseconds = ((LocalTime) this.value).getNano() / 1000;
        } else if (this.value instanceof Duration) {
            neg = ((Duration) this.value).isNegative() ? 1 : 0;
            long fullSeconds = ((Duration) this.value).abs().getSeconds();
            seconds = (int) (fullSeconds % 60);
            long fullMinutes = fullSeconds / 60;
            minutes = (int) (fullMinutes % 60);
            long fullHours = fullMinutes / 60;
            hours = (int) (fullHours % 24);
            days = (int) (fullHours / 24);
            microseconds = ((Duration) this.value).abs().getNano() / 1000;
        } else {
            if (this.calendar == null) {
                this.calendar = Calendar.getInstance(this.defaultTimeZone, Locale.US);
            }

            this.calendar.setTime((java.util.Date) this.value);

            hours = this.calendar.get(Calendar.HOUR_OF_DAY);
            minutes = this.calendar.get(Calendar.MINUTE);
            seconds = this.calendar.get(Calendar.SECOND);
            microseconds = this.calendar.get(Calendar.MILLISECOND) * 1000;
        }

        intoPacket.ensureCapacity(microseconds > 0 ? 13 : 9);
        intoPacket.writeInteger(IntegerDataType.INT1, microseconds > 0 ? 12 : 8); // length
        intoPacket.writeInteger(IntegerDataType.INT1, neg);
        intoPacket.writeInteger(IntegerDataType.INT4, days);
        intoPacket.writeInteger(IntegerDataType.INT1, hours);
        intoPacket.writeInteger(IntegerDataType.INT1, minutes);
        intoPacket.writeInteger(IntegerDataType.INT1, seconds);
        if (microseconds > 0) {
            intoPacket.writeInteger(IntegerDataType.INT4, microseconds);
        }
    }

    /**
     * @param intoPacket
     *            packet to write into
     * @param mysqlType
     *            MysqlType.FIELD_TYPE_*
     */
    private void storeDateTime(NativePacketPayload intoPacket, int mysqlType) {
        synchronized (this) {

            int year = 0, month = 0, day = 0, hours = 0, minutes = 0, seconds = 0, microseconds = 0;

            if (this.value instanceof LocalDate) {
                year = ((LocalDate) this.value).getYear();
                month = ((LocalDate) this.value).getMonthValue();
                day = ((LocalDate) this.value).getDayOfMonth();

            } else if (this.value instanceof LocalTime) {
                year = AbstractQueryBindings.DEFAULT_DATE.getYear();
                month = AbstractQueryBindings.DEFAULT_DATE.getMonthValue();
                day = AbstractQueryBindings.DEFAULT_DATE.getDayOfMonth();
                hours = ((LocalTime) this.value).getHour();
                minutes = ((LocalTime) this.value).getMinute();
                seconds = ((LocalTime) this.value).getSecond();
                microseconds = ((LocalTime) this.value).getNano() / 1000;

            } else if (this.value instanceof LocalDateTime) {
                year = ((LocalDateTime) this.value).getYear();
                month = ((LocalDateTime) this.value).getMonthValue();
                day = ((LocalDateTime) this.value).getDayOfMonth();
                hours = ((LocalDateTime) this.value).getHour();
                minutes = ((LocalDateTime) this.value).getMinute();
                seconds = ((LocalDateTime) this.value).getSecond();
                microseconds = ((LocalDateTime) this.value).getNano() / 1000;

            } else {
                if (this.calendar == null) {
                    this.calendar = Calendar
                            .getInstance(mysqlType == MysqlType.FIELD_TYPE_TIMESTAMP && this.pset.getBooleanProperty(PropertyKey.preserveInstants).getValue()
                                    ? this.connectionTimeZone
                                    : this.defaultTimeZone, Locale.US);
                }

                this.calendar.setTime((java.util.Date) this.value);

                if (this.value instanceof java.sql.Date) {
                    this.calendar.set(Calendar.HOUR_OF_DAY, 0);
                    this.calendar.set(Calendar.MINUTE, 0);
                    this.calendar.set(Calendar.SECOND, 0);
                }

                year = this.calendar.get(Calendar.YEAR);
                month = this.calendar.get(Calendar.MONTH) + 1;
                day = this.calendar.get(Calendar.DAY_OF_MONTH);
                hours = this.calendar.get(Calendar.HOUR_OF_DAY);
                minutes = this.calendar.get(Calendar.MINUTE);
                seconds = this.calendar.get(Calendar.SECOND);

                if (this.value instanceof java.sql.Timestamp) {
                    microseconds = ((java.sql.Timestamp) this.value).getNanos() / 1000;
                } else {
                    microseconds = this.calendar.get(Calendar.MILLISECOND) * 1000;
                }
            }

            intoPacket.ensureCapacity(microseconds > 0 ? 12 : 8);
            intoPacket.writeInteger(IntegerDataType.INT1, microseconds > 0 ? 11 : 7); // length
            intoPacket.writeInteger(IntegerDataType.INT2, year);
            intoPacket.writeInteger(IntegerDataType.INT1, month);
            intoPacket.writeInteger(IntegerDataType.INT1, day);
            intoPacket.writeInteger(IntegerDataType.INT1, hours);
            intoPacket.writeInteger(IntegerDataType.INT1, minutes);
            intoPacket.writeInteger(IntegerDataType.INT1, seconds);
            if (microseconds > 0) {
                intoPacket.writeInteger(IntegerDataType.INT4, microseconds);
            }
        }
    }

    @Override
    public byte[] getByteValue() {
        if (!this.isStream) {
            return this.charEncoding != null ? StringUtils.getBytes(toString(), this.charEncoding) : toString().getBytes();
        }
        return null;
    }
}
