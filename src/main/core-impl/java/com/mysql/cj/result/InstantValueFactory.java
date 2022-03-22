/*
 * Copyright (c) 2016, 2020, Oracle and/or its affiliates.
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

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.TimeZone;

import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;

/**
 * A value factory for creating {@link java.time.Instant} values. It usually can be mapped from a
 * {@link java.sql.Timestamp} or a {@link Long}, and are created from fields returned from the db without a timezone.
 * In order to create a <i>point-in-time</i>, a {@link ZoneId} (preferred) or {@link TimeZone} must be provided to
 * interpret the fields.
 */
public class InstantValueFactory extends AbstractDateTimeValueFactory<Instant> {

    private final ZoneId defaultZoneId;

    private final ZoneId connectionZoneId;

    public InstantValueFactory(PropertySet pset, ZoneId defaultZoneId, ZoneId connectionZoneId) {
        super(pset);
        this.defaultZoneId = defaultZoneId;
        this.connectionZoneId = connectionZoneId;
    }

    public InstantValueFactory(PropertySet pset, TimeZone defaultTimeZone, TimeZone connectionTimeZone) {
        this(pset, defaultTimeZone.toZoneId(), connectionTimeZone.toZoneId());
    }

    private boolean preserveInstant() {
        return pset.getBooleanProperty(PropertyKey.preserveInstants).getValue();
    }

    private ZoneId getZoneId() {
        return preserveInstant() ? connectionZoneId : defaultZoneId;
    }

    @Override
    public String getTargetTypeName() {
        return Instant.class.getName();
    }

    @Override
    Instant localCreateFromDate(InternalDate it) {
        return LocalDate.of(it.getYear(), it.getMonth(), it.getDay())
            .atStartOfDay()
            .atZone(getZoneId())
            .toInstant();
    }

    @Override
    Instant localCreateFromTime(InternalTime it) {
        LocalTime localTime = LocalTime.of(it.getHours(), it.getMinutes(), it.getSeconds(), it.getNanos());
        return LocalDate.ofEpochDay(0).atTime(localTime)
            .atZone(getZoneId())
            .toInstant();
    }

    @Override
    Instant localCreateFromTimestamp(InternalTimestamp its) {
        return LocalDateTime.of(
                its.getYear(),
                its.getMonth(),
                its.getDay(),
                its.getHours(),
                its.getMinutes(),
                its.getSeconds(),
                its.getNanos()
            ).atZone(getZoneId())
            .toInstant();
    }

    @Override
    Instant localCreateFromDatetime(InternalTimestamp its) {
        return LocalDateTime.of(
                its.getYear(),
                its.getMonth(),
                its.getDay(),
                its.getHours(),
                its.getMinutes(),
                its.getSeconds(),
                its.getNanos()
            ).atZone(getZoneId())
            .toInstant();
    }

    @Override
    public Instant createFromLong(long l) {
        return Instant.ofEpochMilli(l);
    }
}
