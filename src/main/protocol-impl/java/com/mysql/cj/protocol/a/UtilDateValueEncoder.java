/*
 * Copyright (c) 2022, Oracle and/or its affiliates.
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

import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
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

public class UtilDateValueEncoder extends AbstractValueEncoder {

    private SimpleDateFormat tsdf = null;

    @Override
    public String getString(BindValue binding) {
        if (binding.isNull()) {
            return "null";
        }

        Timestamp x = adjustTimestamp(new Timestamp(((java.util.Date) binding.getValue()).getTime()), binding.getField(), binding.keepOrigNanos());

        switch (binding.getMysqlType()) {
            case DATE:
                return binding.getCalendar() != null
                        ? TimeUtil.getSimpleDateFormat("''yyyy-MM-dd''", binding.getCalendar())
                                .format(new java.sql.Date(((java.util.Date) binding.getValue()).getTime()))
                        : TimeUtil.getSimpleDateFormat(null, "''yyyy-MM-dd''", this.serverSession.getDefaultTimeZone())
                                .format(new java.sql.Date(((java.util.Date) binding.getValue()).getTime()));
            case DATETIME:
            case TIMESTAMP:
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                StringBuffer buf = new StringBuffer();

                if (binding.getCalendar() != null) {
                    buf.append(TimeUtil.getSimpleDateFormat("''yyyy-MM-dd HH:mm:ss", binding.getCalendar()).format(x));
                } else {
                    this.tsdf = TimeUtil.getSimpleDateFormat(this.tsdf, "''yyyy-MM-dd HH:mm:ss",
                            binding.getMysqlType() == MysqlType.TIMESTAMP && this.preserveInstants.getValue() ? this.serverSession.getSessionTimeZone()
                                    : this.serverSession.getDefaultTimeZone());
                    buf.append(this.tsdf.format(x));
                }

                if (this.serverSession.getCapabilities().serverSupportsFracSecs() && x.getNanos() > 0) {
                    buf.append('.');
                    buf.append(TimeUtil.formatNanos(x.getNanos(), 6));
                }
                buf.append('\'');

                return buf.toString();
            case YEAR:
                Calendar cal = Calendar.getInstance();
                cal.setTime((java.util.Date) binding.getValue());
                return String.valueOf(cal.get(Calendar.YEAR));
            case TIME:
                StringBuilder sb = new StringBuilder("'");
                sb.append(adjustLocalTime(new Timestamp(((java.util.Date) binding.getValue()).getTime()).toLocalDateTime().toLocalTime(), binding.getField())
                        .format(TimeUtil.TIME_FORMATTER_WITH_OPTIONAL_MICROS));
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
        Timestamp x = adjustTimestamp(new Timestamp(((java.util.Date) binding.getValue()).getTime()), binding.getField(), binding.keepOrigNanos());
        Calendar calendar = binding.getCalendar();
        switch (binding.getMysqlType()) {
            case DATE:
                if (calendar == null) {
                    calendar = Calendar.getInstance(this.serverSession.getDefaultTimeZone(), Locale.US);
                }
                calendar.setTime((java.util.Date) binding.getValue());
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
                calendar.setTime(x);
                writeDateTime(msg, InternalTimestamp.from(calendar, x.getNanos()));
                return;
            case YEAR:
                Calendar cal = Calendar.getInstance();
                cal.setTime((java.util.Date) binding.getValue());
                intoPacket.writeInteger(IntegerDataType.INT4, cal.get(Calendar.YEAR));
                return;
            case TIME:
                Time t = adjustTime(new Time(x.getTime()));
                if (calendar == null) {
                    calendar = Calendar.getInstance(this.serverSession.getDefaultTimeZone(), Locale.US);
                }
                calendar.setTime(t);
                writeTime(msg,
                        InternalTime.from(calendar,
                                adjustTimestamp(new Timestamp(((java.util.Date) binding.getValue()).getTime()), binding.getField(), binding.keepOrigNanos())
                                        .getNanos()));
                return;
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                StringBuffer buf = new StringBuffer();

                if (binding.getCalendar() != null) {
                    buf.append(TimeUtil.getSimpleDateFormat("yyyy-MM-dd HH:mm:ss", binding.getCalendar()).format(x));
                } else {
                    this.tsdf = TimeUtil.getSimpleDateFormat(this.tsdf, "yyyy-MM-dd HH:mm:ss",
                            binding.getMysqlType() == MysqlType.TIMESTAMP && this.preserveInstants.getValue() ? this.serverSession.getSessionTimeZone()
                                    : this.serverSession.getDefaultTimeZone());
                    buf.append(this.tsdf.format(x));
                }

                if (this.serverSession.getCapabilities().serverSupportsFracSecs() && x.getNanos() > 0) {
                    buf.append('.');
                    buf.append(TimeUtil.formatNanos(x.getNanos(), 6));
                }

                intoPacket.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes(buf.toString(), this.charEncoding.getValue()));
                break;

            default:
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("PreparedStatement.67", new Object[] { binding.getValue().getClass().getName(), binding.getMysqlType().toString() }),
                        this.exceptionInterceptor);
        }
    }

    @Override
    public void encodeAsQueryAttribute(Message msg, BindValue binding) {
        java.util.Date date = (java.util.Date) binding.getValue();
        Calendar calendar = Calendar.getInstance(this.serverSession.getDefaultTimeZone(), Locale.US);
        calendar.setTime(date);
        InternalTimestamp internalTimestamp = InternalTimestamp.from(calendar, (int) TimeUnit.MILLISECONDS.toNanos(calendar.get(Calendar.MILLISECOND)));
        internalTimestamp.setOffset((int) TimeUnit.MILLISECONDS.toMinutes(calendar.getTimeZone().getOffset(date.getTime())));
        writeDateTimeWithOffset(msg, internalTimestamp);
    }
}
