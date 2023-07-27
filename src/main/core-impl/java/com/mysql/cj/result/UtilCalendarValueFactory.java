/*
 * Copyright (c) 2020, 2023, Oracle and/or its affiliates.
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

import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import com.mysql.cj.Messages;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.DataReadException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;

/**
 * Value factory to create {@link Calendar} instances.
 */
public class UtilCalendarValueFactory extends AbstractDateTimeValueFactory<Calendar> {

    private TimeZone defaultTimeZone;
    private TimeZone connectionTimeZone;

    /**
     * @param pset
     *            {@link PropertySet}
     * @param defaultTimeZone
     *            The local JVM time zone.
     * @param connectionTimeZone
     *            The server session time zone as defined by connectionTimeZone property.
     */
    public UtilCalendarValueFactory(PropertySet pset, TimeZone defaultTimeZone, TimeZone connectionTimeZone) {
        super(pset);
        this.defaultTimeZone = defaultTimeZone;
        this.connectionTimeZone = connectionTimeZone;
    }

    /**
     * Create a Calendar from a DATE value.
     *
     * @return a Calendar at midnight on the day given by the DATE value
     */
    @Override
    public Calendar localCreateFromDate(InternalDate idate) {
        if (idate.getYear() == 0 && idate.getMonth() == 0 && idate.getDay() == 0) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidZeroDate"));
        }

        try {
            Calendar c = Calendar.getInstance(this.defaultTimeZone, Locale.US);
            c.set(idate.getYear(), idate.getMonth() - 1, idate.getDay(), 0, 0, 0);
            c.set(Calendar.MILLISECOND, 0);
            c.setLenient(false);
            return c;
        } catch (IllegalArgumentException e) {
            throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e);
        }
    }

    /**
     * Create a Calendar from a TIME value.
     *
     * @return a Calendar at the given time on 1970 Jan 1.
     */
    @Override
    public Calendar localCreateFromTime(InternalTime it) {
        if (it.getHours() < 0 || it.getHours() >= 24) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidTimeValue", new Object[] { it.toString() }));
        }

        try {
            Calendar c = Calendar.getInstance(this.defaultTimeZone, Locale.US);
            c.set(1970, 0, 1, it.getHours(), it.getMinutes(), it.getSeconds());
            c.set(Calendar.MILLISECOND, it.getNanos() / 1000000);
            c.setLenient(false);
            return c;
        } catch (IllegalArgumentException e) {
            throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e);
        }
    }

    @Override
    public Calendar localCreateFromTimestamp(InternalTimestamp its) {
        if (its.getYear() == 0 && its.getMonth() == 0 && its.getDay() == 0) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidZeroDate"));
        }

        try {
            Calendar c = Calendar.getInstance(
                    this.pset.getBooleanProperty(PropertyKey.preserveInstants).getValue() ? this.connectionTimeZone : this.defaultTimeZone, Locale.US);
            c.set(its.getYear(), its.getMonth() - 1, its.getDay(), its.getHours(), its.getMinutes(), its.getSeconds());
            c.set(Calendar.MILLISECOND, its.getNanos() / 1000000);
            c.setLenient(false);
            return c;
        } catch (IllegalArgumentException e) {
            throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e);
        }
    }

    @Override
    public Calendar localCreateFromDatetime(InternalTimestamp its) {
        if (its.getYear() == 0 && its.getMonth() == 0 && its.getDay() == 0) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidZeroDate"));
        }

        try {
            Calendar c = Calendar.getInstance(
                    this.pset.getBooleanProperty(PropertyKey.preserveInstants).getValue() ? this.connectionTimeZone : this.defaultTimeZone, Locale.US);
            c.set(its.getYear(), its.getMonth() - 1, its.getDay(), its.getHours(), its.getMinutes(), its.getSeconds());
            c.set(Calendar.MILLISECOND, its.getNanos() / 1000000);
            c.setLenient(false);
            return c;
        } catch (IllegalArgumentException e) {
            throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e);
        }
    }

    @Override
    public String getTargetTypeName() {
        return Calendar.class.getName();
    }

}
