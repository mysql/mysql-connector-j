/*
 * Copyright (c) 2020, 2021, Oracle and/or its affiliates.
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

package com.mysql.cj.result;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.DataConversionException;
import com.mysql.cj.exceptions.DataReadException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;
import com.mysql.cj.protocol.a.MysqlTextValueDecoder;
import com.mysql.cj.util.StringUtils;

/**
 * Value factory to create {@link ZonedDateTime} instances.
 */
public class ZonedDateTimeValueFactory extends AbstractDateTimeValueFactory<ZonedDateTime> {

    private TimeZone defaultTimeZone;
    private TimeZone connectionTimeZone;

    public ZonedDateTimeValueFactory(PropertySet pset, TimeZone defaultTimeZone, TimeZone connectionTimeZone) {
        super(pset);
        this.defaultTimeZone = defaultTimeZone;
        this.connectionTimeZone = connectionTimeZone;
    }

    /**
     * Create an ZonedDateTime from a DATE value.
     *
     * @return an ZonedDateTime at midnight on the day given by the DATE value
     */
    @Override
    public ZonedDateTime localCreateFromDate(InternalDate idate) {
        if (idate.getYear() == 0 && idate.getMonth() == 0 && idate.getDay() == 0) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidZeroDate"));
        }
        return LocalDateTime.of(idate.getYear(), idate.getMonth(), idate.getDay(), 0, 0, 0, 0).atZone(this.defaultTimeZone.toZoneId());
    }

    /**
     * Create an ZonedDateTime from a TIME value.
     *
     * @return an ZonedDateTime at the given time on 1970 Jan 1.
     */
    @Override
    public ZonedDateTime localCreateFromTime(InternalTime it) {
        if (it.getHours() < 0 || it.getHours() >= 24) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidTimeValue", new Object[] { it.toString() }));
        }
        return LocalDateTime.of(1970, 1, 1, it.getHours(), it.getMinutes(), it.getSeconds(), it.getNanos()).atZone(this.defaultTimeZone.toZoneId());
    }

    @Override
    public ZonedDateTime localCreateFromTimestamp(InternalTimestamp its) {
        if (its.getYear() == 0 && its.getMonth() == 0 && its.getDay() == 0) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidZeroDate"));
        }
        return LocalDateTime.of(its.getYear(), its.getMonth(), its.getDay(), its.getHours(), its.getMinutes(), its.getSeconds(), its.getNanos())
                .atZone((this.pset.getBooleanProperty(PropertyKey.preserveInstants).getValue() ? this.connectionTimeZone : this.defaultTimeZone).toZoneId());
    }

    @Override
    public ZonedDateTime localCreateFromDatetime(InternalTimestamp its) {
        if (its.getYear() == 0 && its.getMonth() == 0 && its.getDay() == 0) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidZeroDate"));
        }
        return LocalDateTime.of(its.getYear(), its.getMonth(), its.getDay(), its.getHours(), its.getMinutes(), its.getSeconds(), its.getNanos())
                .atZone((this.pset.getBooleanProperty(PropertyKey.preserveInstants).getValue() ? this.connectionTimeZone : this.defaultTimeZone).toZoneId());
    }

    @Override
    public ZonedDateTime createFromBytes(byte[] bytes, int offset, int length, Field f) {
        if (length == 0 && this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).getValue()) {
            return createFromLong(0);
        }

        String s = StringUtils.toString(bytes, offset, length, f.getEncoding());
        byte[] newBytes = s.getBytes();

        if (MysqlTextValueDecoder.isDate(s)) {
            return createFromDate(MysqlTextValueDecoder.getDate(newBytes, 0, newBytes.length));

        } else if (MysqlTextValueDecoder.isTime(s)) {
            return createFromTime(MysqlTextValueDecoder.getTime(newBytes, 0, newBytes.length, f.getDecimals()));

        } else if (MysqlTextValueDecoder.isTimestamp(s)) {
            return createFromTimestamp(MysqlTextValueDecoder.getTimestamp(newBytes, 0, newBytes.length, f.getDecimals()));
        }

        // by default try to parse
        try {
            return ZonedDateTime.parse(s.replace(" ", "T"));
        } catch (DateTimeParseException e) {
            throw new DataConversionException(Messages.getString("ResultSet.UnableToConvertString", new Object[] { s, getTargetTypeName() }));
        }

    }

    public String getTargetTypeName() {
        return ZonedDateTime.class.getName();
    }
}
