/*
 * Copyright (c) 2020, Oracle and/or its affiliates.
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

import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;

import com.mysql.cj.Messages;
import com.mysql.cj.WarningListener;
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
 * A value factory to create {@link OffsetTime} instances.
 */
public class OffsetTimeValueFactory extends AbstractDateTimeValueFactory<OffsetTime> {
    private WarningListener warningListener;
    private TimeZone tz;

    public OffsetTimeValueFactory(PropertySet pset, TimeZone tz) {
        super(pset);
        this.tz = tz;
    }

    public OffsetTimeValueFactory(PropertySet pset, TimeZone tz, WarningListener warningListener) {
        this(pset, tz);
        this.warningListener = warningListener;
    }

    @Override
    OffsetTime localCreateFromDate(InternalDate idate) {
        return LocalTime.of(0, 0).atOffset(ZoneOffset.ofTotalSeconds(this.tz.getRawOffset() / 1000));
    }

    @Override
    public OffsetTime localCreateFromTime(InternalTime it) {
        if (it.getHours() < 0 || it.getHours() >= 24) {
            throw new DataReadException(
                    Messages.getString("ResultSet.InvalidTimeValue", new Object[] { "" + it.getHours() + ":" + it.getMinutes() + ":" + it.getSeconds() }));
        }
        return LocalTime.of(it.getHours(), it.getMinutes(), it.getSeconds(), it.getNanos()).atOffset(ZoneOffset.ofTotalSeconds(this.tz.getRawOffset() / 1000));
    }

    @Override
    public OffsetTime localCreateFromTimestamp(InternalTimestamp its) {
        if (this.warningListener != null) {
            this.warningListener.warningEncountered(Messages.getString("ResultSet.PrecisionLostWarning", new Object[] { getTargetTypeName() }));
        }
        // truncate date information
        return createFromTime(new InternalTime(its.getHours(), its.getMinutes(), its.getSeconds(), its.getNanos(), its.getScale()));
    }

    @Override
    public OffsetTime localCreateFromDatetime(InternalTimestamp its) {
        if (this.warningListener != null) {
            this.warningListener.warningEncountered(Messages.getString("ResultSet.PrecisionLostWarning", new Object[] { getTargetTypeName() }));
        }
        // truncate date information
        return createFromTime(new InternalTime(its.getHours(), its.getMinutes(), its.getSeconds(), its.getNanos(), its.getScale()));
    }

    @Override
    public OffsetTime createFromBytes(byte[] bytes, int offset, int length, Field f) {
        if (length == 0 && this.pset.getBooleanProperty(PropertyKey.emptyStringsConvertToZero).getValue()) {
            return createFromLong(0);
        }

        // TODO: Too expensive to convert from other charset to ASCII here? UTF-8 (e.g.) doesn't need any conversion before being sent to the decoder
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
            return OffsetTime.parse(s);
        } catch (DateTimeParseException e) {
            throw new DataConversionException(Messages.getString("ResultSet.UnableToConvertString", new Object[] { s, getTargetTypeName() }));
        }
    }

    public String getTargetTypeName() {
        return OffsetTime.class.getName();
    }
}
