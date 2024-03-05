/*
 * Copyright (c) 2022, 2024, Oracle and/or its affiliates.
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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Locale;

import com.mysql.cj.BindValue;
import com.mysql.cj.Messages;
import com.mysql.cj.MysqlType;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTimestamp;
import com.mysql.cj.protocol.Message;
import com.mysql.cj.protocol.a.NativeConstants.IntegerDataType;
import com.mysql.cj.protocol.a.NativeConstants.StringSelfDataType;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.TimeUtil;

public class SqlDateValueEncoder extends AbstractValueEncoder {

    @Override
    public String getString(BindValue binding) {
        switch (binding.getMysqlType()) {
            case NULL:
                return "null";
            case DATE:
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                return binding.getCalendar() != null ? TimeUtil.getSimpleDateFormat("''yyyy-MM-dd''", binding.getCalendar()).format((Date) binding.getValue())
                        : TimeUtil.getSimpleDateFormat(null, "''yyyy-MM-dd''", this.serverSession.getDefaultTimeZone()).format((Date) binding.getValue());
            case DATETIME:
            case TIMESTAMP:
                Timestamp ts = new java.sql.Timestamp(((java.util.Date) binding.getValue()).getTime());
                return binding.getCalendar() != null ? TimeUtil.getSimpleDateFormat("''yyyy-MM-dd HH:mm:ss''", binding.getCalendar()).format(ts)
                        : TimeUtil.getSimpleDateFormat(null, "''yyyy-MM-dd HH:mm:ss''",
                                binding.getMysqlType() == MysqlType.TIMESTAMP && this.preserveInstants.getValue() ? this.serverSession.getSessionTimeZone()
                                        : this.serverSession.getDefaultTimeZone())
                                .format(ts);
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
                if (calendar == null) {
                    calendar = Calendar.getInstance(
                            binding.getMysqlType() == MysqlType.TIMESTAMP && this.propertySet.getBooleanProperty(PropertyKey.preserveInstants).getValue()
                                    ? this.serverSession.getSessionTimeZone()
                                    : this.serverSession.getDefaultTimeZone(),
                            Locale.US);
                }
                Timestamp ts = new java.sql.Timestamp(((java.util.Date) binding.getValue()).getTime());
                calendar.setTime(ts);
                writeDateTime(msg, InternalTimestamp.from(calendar, ts.getNanos()));
                return;
            case YEAR:
                Calendar cal = Calendar.getInstance();
                cal.setTime((java.util.Date) binding.getValue());
                intoPacket.writeInteger(IntegerDataType.INT4, cal.get(Calendar.YEAR));
                return;
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                String val = binding.getCalendar() != null ? TimeUtil.getSimpleDateFormat("yyyy-MM-dd", binding.getCalendar()).format((Date) binding.getValue())
                        : TimeUtil.getSimpleDateFormat(null, "yyyy-MM-dd", this.serverSession.getDefaultTimeZone()).format((Date) binding.getValue());
                intoPacket.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes(val, this.charEncoding.getValue()));
                return;
            default:
                throw ExceptionFactory.createException(WrongArgumentException.class,
                        Messages.getString("PreparedStatement.67", new Object[] { binding.getValue().getClass().getName(), binding.getMysqlType().toString() }),
                        this.exceptionInterceptor);
        }
    }

    @Override
    public void encodeAsQueryAttribute(Message msg, BindValue binding) {
        Calendar calendar = Calendar.getInstance(this.serverSession.getDefaultTimeZone(), Locale.US);
        calendar.setTime((java.util.Date) binding.getValue());
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        writeDate(msg, InternalDate.from(calendar));
    }

}
