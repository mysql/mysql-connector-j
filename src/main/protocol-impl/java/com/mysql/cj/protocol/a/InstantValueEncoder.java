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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Locale;

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

public class InstantValueEncoder extends AbstractValueEncoder {

    @Override
    public String getString(BindValue binding) {
        switch (binding.getMysqlType()) {
            case NULL:
                return "null";
            case DATE:
                StringBuilder sb = new StringBuilder("'");
                sb.append(((Instant) binding.getValue()).atOffset(ZoneOffset.UTC).atZoneSameInstant(this.serverSession.getDefaultTimeZone().toZoneId())
                        .toLocalDate().format(TimeUtil.DATE_FORMATTER));
                sb.append("'");
                return sb.toString();
            case TIME:
                sb = new StringBuilder("'");
                sb.append(adjustLocalTime(((Instant) binding.getValue()).atOffset(ZoneOffset.UTC)
                        .atZoneSameInstant(this.serverSession.getDefaultTimeZone().toZoneId()).toLocalTime(), binding.getField())
                                .format(TimeUtil.TIME_FORMATTER_WITH_OPTIONAL_MICROS));
                sb.append("'");
                return sb.toString();
            case DATETIME:
            case TIMESTAMP:
                Timestamp x = adjustTimestamp(
                        Timestamp.valueOf(((Instant) binding.getValue()).atOffset(ZoneOffset.UTC)
                                .atZoneSameInstant(this.serverSession.getDefaultTimeZone().toZoneId()).toLocalDateTime()),
                        binding.getField(), binding.keepOrigNanos());

                sb = new StringBuilder();

                sb.append(TimeUtil.getSimpleDateFormat(null, "''yyyy-MM-dd HH:mm:ss",
                        binding.getMysqlType() == MysqlType.TIMESTAMP && this.preserveInstants.getValue() ? this.serverSession.getSessionTimeZone()
                                : this.serverSession.getDefaultTimeZone())
                        .format(x));

                if (this.serverSession.getCapabilities().serverSupportsFracSecs() && x.getNanos() > 0) {
                    sb.append('.');
                    sb.append(TimeUtil.formatNanos(x.getNanos(), 6));
                }
                sb.append('\'');

                return sb.toString();

            case YEAR:
                return String.valueOf(((Instant) binding.getValue()).atOffset(ZoneOffset.UTC)
                        .atZoneSameInstant(this.serverSession.getDefaultTimeZone().toZoneId()).getYear());
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                sb = new StringBuilder("'");
                sb.append(((Instant) binding.getValue()).atOffset(ZoneOffset.UTC)
                        .format(this.sendFractionalSeconds.getValue() && ((Instant) binding.getValue()).getNano() > 0
                                ? TimeUtil.DATETIME_FORMATTER_WITH_NANOS_WITH_OFFSET
                                : TimeUtil.DATETIME_FORMATTER_NO_FRACT_WITH_OFFSET)

                );
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
        switch (binding.getMysqlType()) {
            case DATE:
                writeDate(msg, InternalDate.from(((Instant) binding.getValue()).atOffset(ZoneOffset.UTC)
                        .atZoneSameInstant(this.serverSession.getDefaultTimeZone().toZoneId()).toLocalDate()));
                return;
            case TIME:
                writeTime(msg, InternalTime.from(adjustLocalTime(((Instant) binding.getValue()).atOffset(ZoneOffset.UTC)
                        .atZoneSameInstant(this.serverSession.getDefaultTimeZone().toZoneId()).toLocalTime(), binding.getField())));
                return;
            case DATETIME:
            case TIMESTAMP:
                Timestamp ts = adjustTimestamp(
                        Timestamp.valueOf(((Instant) binding.getValue()).atOffset(ZoneOffset.UTC)
                                .atZoneSameInstant(this.serverSession.getDefaultTimeZone().toZoneId()).toLocalDateTime()),
                        binding.getField(), binding.keepOrigNanos());
                Calendar calendar = Calendar
                        .getInstance(binding.getMysqlType() == MysqlType.TIMESTAMP && this.preserveInstants.getValue() ? this.serverSession.getSessionTimeZone()
                                : this.serverSession.getDefaultTimeZone(), Locale.US);
                calendar.setTime(ts);
                writeDateTime(msg, InternalTimestamp.from(calendar, ts.getNanos()));
                return;
            case YEAR:
                intoPacket.writeInteger(IntegerDataType.INT4, ((Instant) binding.getValue()).atOffset(ZoneOffset.UTC)
                        .atZoneSameInstant(this.serverSession.getDefaultTimeZone().toZoneId()).getYear());
                return;
            case CHAR:
            case VARCHAR:
            case TINYTEXT:
            case TEXT:
            case MEDIUMTEXT:
            case LONGTEXT:
                intoPacket.writeBytes(StringSelfDataType.STRING_LENENC,
                        StringUtils.getBytes(((Instant) binding.getValue()).atOffset(ZoneOffset.UTC)
                                .format(this.sendFractionalSeconds.getValue() && ((Instant) binding.getValue()).getNano() > 0
                                        ? TimeUtil.DATETIME_FORMATTER_WITH_NANOS_WITH_OFFSET
                                        : TimeUtil.DATETIME_FORMATTER_NO_FRACT_WITH_OFFSET),
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
        writeDateTimeWithOffset(msg, InternalTimestamp.from(((Instant) binding.getValue()).atOffset(ZoneOffset.UTC)));
    }

}
