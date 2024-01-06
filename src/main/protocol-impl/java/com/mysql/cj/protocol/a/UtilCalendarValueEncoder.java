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

import java.sql.Timestamp;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import com.mysql.cj.BindValue;
import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;

public class UtilCalendarValueEncoder extends AbstractValueEncoder {

    @Override
    public String getString(BindValue binding) {
        Calendar x = (Calendar) binding.getValue();
        switch (binding.getMysqlType()) {
            case NULL:
                return "null";
            case DATE:
                return binding.getCalendar() != null
                        ? TimeUtil.getSimpleDateFormat("''yyyy-MM-dd''", binding.getCalendar()).format(new java.sql.Date(x.getTimeInMillis()))
                        : TimeUtil.getSimpleDateFormat(null, "''yyyy-MM-dd''", this.serverSession.getDefaultTimeZone())
                                .format(new java.sql.Date(x.getTimeInMillis()));
            case TIMESTAMP:
                Timestamp ts = adjustTimestamp(new java.sql.Timestamp(((Calendar) binding.getValue()).getTimeInMillis()), binding.getField(),
                        binding.keepOrigNanos());
                StringBuilder buf = new StringBuilder();
                if (binding.getCalendar() != null) {
                    buf.append(TimeUtil.getSimpleDateFormat("''yyyy-MM-dd HH:mm:ss", binding.getCalendar()).format(x));
                } else {
                    buf.append(TimeUtil.getSimpleDateFormat(null, "''yyyy-MM-dd HH:mm:ss",
                            binding.getMysqlType() == MysqlType.TIMESTAMP && this.preserveInstants.getValue() ? this.serverSession.getSessionTimeZone()
                                    : this.serverSession.getDefaultTimeZone())
                            .format(ts));
                }
                if (this.serverSession.getCapabilities().serverSupportsFracSecs() && ts.getNanos() > 0) {
                    buf.append('.');
                    buf.append(TimeUtil.formatNanos(ts.getNanos(), 6));
                }
                buf.append('\'');
                return buf.toString();
            case DATETIME:
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                ZonedDateTime zdt = ZonedDateTime.ofInstant(x.toInstant(), x.getTimeZone().toZoneId())
                        .withZoneSameInstant(this.serverSession.getDefaultTimeZone().toZoneId());

                StringBuilder sb = new StringBuilder("'");
                sb.append(zdt.format(zdt.getNano() > 0 && this.serverSession.getCapabilities().serverSupportsFracSecs() && this.sendFractionalSeconds.getValue()
                        ? TimeUtil.DATETIME_FORMATTER_WITH_MILLIS_NO_OFFSET
                        : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET));
                sb.append("'");
                return sb.toString();
            case YEAR:
                return String.valueOf(x.get(Calendar.YEAR));
            case TIME:
                sb = new StringBuilder("'");
                sb.append(adjustLocalTime(((Calendar) binding.getValue()).toInstant().atZone(this.serverSession.getDefaultTimeZone().toZoneId()).toLocalTime(),
                        binding.getField()).format(TimeUtil.TIME_FORMATTER_WITH_OPTIONAL_MICROS));
                sb.append("'");
                return sb.toString();
            default:
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("PreparedStatement.67", new Object[] { binding.getValue().getClass().getName(), binding.getMysqlType().toString() }),
                        this.exceptionInterceptor);
        }
    }

    @Override
    public void encodeAsBinary(Message msg, BindValue binding) {
        NativePacketPayload intoPacket = (NativePacketPayload) msg;
        Calendar x = (Calendar) binding.getValue();
        Calendar calendar = binding.getCalendar();

        switch (binding.getMysqlType()) {
            case DATE:
                if (calendar == null) {
                    calendar = Calendar.getInstance(this.serverSession.getDefaultTimeZone(), Locale.US);
                }
                calendar.setTime(new java.sql.Date(x.getTimeInMillis()));
                calendar.set(Calendar.HOUR_OF_DAY, 0);
                calendar.set(Calendar.MINUTE, 0);
                calendar.set(Calendar.SECOND, 0);
                writeDate(msg, InternalDate.from(calendar));
                return;
            case DATETIME:
            case TIMESTAMP:
                if (calendar == null) {
                    calendar = Calendar.getInstance(
                            binding.getMysqlType() == MysqlType.TIMESTAMP && this.preserveInstants.getValue() ? this.serverSession.getSessionTimeZone()
                                    : this.serverSession.getDefaultTimeZone(),
                            Locale.US);
                }
                Timestamp ts = adjustTimestamp(new java.sql.Timestamp(((Calendar) binding.getValue()).getTimeInMillis()), binding.getField(),
                        binding.keepOrigNanos());
                calendar.setTime(ts);
                writeDateTime(msg, InternalTimestamp.from(calendar, ts.getNanos()));
                return;
            case YEAR:
                intoPacket.writeInteger(IntegerDataType.INT4, x.get(Calendar.YEAR));
                return;
            case TIME:
                writeTime(msg,
                        InternalTime.from(adjustLocalTime(
                                ((Calendar) binding.getValue()).toInstant().atZone(this.serverSession.getDefaultTimeZone().toZoneId()).toLocalTime(),
                                binding.getField())));
                return;
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                ZonedDateTime zdt = ZonedDateTime.ofInstant(x.toInstant(), x.getTimeZone().toZoneId())
                        .withZoneSameInstant(this.serverSession.getDefaultTimeZone().toZoneId());
                intoPacket
                        .writeBytes(StringSelfDataType.STRING_LENENC,
                                StringUtils.getBytes(zdt.format(zdt.getNano() > 0 && this.serverSession.getCapabilities().serverSupportsFracSecs()
                                        && this.sendFractionalSeconds.getValue() ? TimeUtil.DATETIME_FORMATTER_WITH_MILLIS_NO_OFFSET
                                                : TimeUtil.DATETIME_FORMATTER_NO_FRACT_NO_OFFSET),
                                        this.charEncoding.getValue()));
                return;
            default:
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("PreparedStatement.67", new Object[] { binding.getValue().getClass().getName(), binding.getMysqlType().toString() }),
                        this.exceptionInterceptor);
        }
    }

    @Override
    public void encodeAsQueryAttribute(Message msg, BindValue binding) {
        Calendar calendar = (Calendar) binding.getValue();
        InternalTimestamp internalTimestamp = InternalTimestamp.from(calendar, (int) TimeUnit.MILLISECONDS.toNanos(calendar.get(Calendar.MILLISECOND)));
        internalTimestamp.setOffset((int) TimeUnit.MILLISECONDS.toMinutes(calendar.getTimeZone().getOffset(calendar.getTimeInMillis())));
        writeDateTimeWithOffset(msg, internalTimestamp);
    }

}
