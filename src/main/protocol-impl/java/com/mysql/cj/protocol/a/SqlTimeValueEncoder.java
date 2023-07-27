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

import java.sql.Date;
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import com.mysql.cj.BindValue;
import com.mysql.cj.Messages;
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

public class SqlTimeValueEncoder extends AbstractValueEncoder {

    private SimpleDateFormat tdf;

    @Override
    public String getString(BindValue binding) {
        if (binding.isNull()) {
            return "null";
        }

        Time x = adjustTime((Time) binding.getValue());

        switch (binding.getMysqlType()) {
            case DATE:
                Date d = new java.sql.Date(x.getTime());
                return binding.getCalendar() != null ? TimeUtil.getSimpleDateFormat("''yyyy-MM-dd''", binding.getCalendar()).format(d)
                        : TimeUtil.getSimpleDateFormat(null, "''yyyy-MM-dd''", this.serverSession.getDefaultTimeZone()).format(d);
            case TIME:
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                String formatStr = this.serverSession.getCapabilities().serverSupportsFracSecs() && this.sendFractionalSeconds.getValue()
                        && this.sendFractionalSecondsForTime.getValue() && TimeUtil.hasFractionalSeconds(x) ? "''HH:mm:ss.SSS''" : "''HH:mm:ss''";

                return binding.getCalendar() != null ? TimeUtil.getSimpleDateFormat(formatStr, binding.getCalendar()).format(x)
                        : TimeUtil.getSimpleDateFormat(this.tdf, formatStr, this.serverSession.getDefaultTimeZone()).format(x);

            case DATETIME:
            case TIMESTAMP:
                java.sql.Timestamp ts = new java.sql.Timestamp(x.getTime());

                if (!this.sendFractionalSecondsForTime.getValue()) {
                    ts = TimeUtil.truncateFractionalSeconds(ts);
                }

                StringBuffer buf = new StringBuffer();
                buf.append(binding.getCalendar() != null ? TimeUtil.getSimpleDateFormat("''yyyy-MM-dd HH:mm:ss", binding.getCalendar()).format(x)
                        : TimeUtil.getSimpleDateFormat(null, "''yyyy-MM-dd HH:mm:ss", this.serverSession.getDefaultTimeZone()).format(x));
                if (this.serverSession.getCapabilities().serverSupportsFracSecs() && ts.getNanos() > 0) {
                    buf.append('.');
                    buf.append(TimeUtil.formatNanos(ts.getNanos(), 6));
                }
                buf.append('\'');
                return buf.toString();
            case YEAR:
                Calendar cal = Calendar.getInstance();
                cal.setTime((java.util.Date) binding.getValue());
                return String.valueOf(cal.get(Calendar.YEAR));
            default:
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("PreparedStatement.67", new Object[] { binding.getValue().getClass().getName(), binding.getMysqlType().toString() }),
                        this.exceptionInterceptor);
        }
    }

    @Override
    public void encodeAsBinary(Message msg, BindValue binding) {
        NativePacketPayload intoPacket = (NativePacketPayload) msg;
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
                java.sql.Timestamp ts = new java.sql.Timestamp(((java.sql.Time) binding.getValue()).getTime());
                if (!this.serverSession.getCapabilities().serverSupportsFracSecs() || !this.sendFractionalSeconds.getValue()
                        || !this.sendFractionalSecondsForTime.getValue()) {
                    ts = TimeUtil.truncateFractionalSeconds(ts);
                }
                if (calendar == null) {
                    calendar = Calendar.getInstance(this.serverSession.getDefaultTimeZone(), Locale.US);
                }
                calendar.setTime(ts);
                writeDateTime(msg, InternalTimestamp.from(calendar, ts.getNanos()));
                return;
            case YEAR:
                Calendar cal = Calendar.getInstance();
                cal.setTime((java.util.Date) binding.getValue());
                intoPacket.writeInteger(IntegerDataType.INT4, cal.get(Calendar.YEAR));
                return;
            case TIME:
                Time x = adjustTime((Time) binding.getValue());
                if (calendar == null) {
                    calendar = Calendar.getInstance(this.serverSession.getDefaultTimeZone(), Locale.US);
                }
                calendar.setTime(x);
                writeTime(msg, InternalTime.from(calendar, (int) TimeUnit.MILLISECONDS.toNanos(calendar.get(Calendar.MILLISECOND))));
                return;
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                x = adjustTime((Time) binding.getValue());

                String formatStr = this.serverSession.getCapabilities().serverSupportsFracSecs() && this.sendFractionalSeconds.getValue()
                        && this.sendFractionalSecondsForTime.getValue() && TimeUtil.hasFractionalSeconds(x) ? "HH:mm:ss.SSS" : "HH:mm:ss";

                intoPacket.writeBytes(StringSelfDataType.STRING_LENENC,
                        StringUtils.getBytes(
                                binding.getCalendar() != null ? TimeUtil.getSimpleDateFormat(formatStr, binding.getCalendar()).format(x)
                                        : TimeUtil.getSimpleDateFormat(this.tdf, formatStr, this.serverSession.getDefaultTimeZone()).format(x),
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
        Time x = (Time) binding.getValue();
        Calendar calendar = Calendar.getInstance(this.serverSession.getDefaultTimeZone(), Locale.US);
        calendar.setTime(x);
        writeTime(msg, InternalTime.from(calendar, (int) TimeUnit.MILLISECONDS.toNanos(calendar.get(Calendar.MILLISECOND))));
    }

}
