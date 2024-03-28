/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates.
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

package com.mysql.cj.result;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
 * Value factory to create {@link java.sql.Timestamp} instances. Timestamp instances are created from fields returned from the db without a timezone. In order
 * to create a <i>point-in-time</i>, a time zone must be provided to interpret the fields.
 */
public class SqlTimestampValueFactory extends AbstractDateTimeValueFactory<Timestamp> {

    // cached per instance to avoid re-creation on every create*() call
    private final Calendar cal;

    private final TimeZone defaultTimeZone;
    private final Lock defaultTimeZoneLock = new ReentrantLock();
    private final TimeZone connectionTimeZone;

    /**
     * @param pset
     *            {@link PropertySet}
     * @param calendar
     *            Calendar used to interpret the fields.
     * @param defaultTimeZone
     *            The local JVM time zone.
     * @param connectionTimeZone
     *            The server session time zone as defined by connectionTimeZone property.
     */
    public SqlTimestampValueFactory(PropertySet pset, Calendar calendar, TimeZone defaultTimeZone, TimeZone connectionTimeZone) {
        super(pset);
        this.defaultTimeZone = defaultTimeZone;
        this.connectionTimeZone = connectionTimeZone;
        this.cal = calendar != null ? (Calendar) calendar.clone() : null;
    }

    /**
     * Create a Timestamp from a DATE value.
     *
     * @return a timestamp at midnight on the day given by the DATE value
     */
    @Override
    public Timestamp localCreateFromDate(InternalDate idate) {
        if (idate.getYear() == 0 && idate.getMonth() == 0 && idate.getDay() == 0) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidZeroDate"));
        }

        this.defaultTimeZoneLock.lock();
        try {
            Calendar c;

            if (this.cal != null) {
                c = this.cal;
            } else {
                // c.f. Bug#11540 for details on locale
                c = Calendar.getInstance(this.defaultTimeZone, Locale.US);
                c.setLenient(false);
            }

            try {
                c.clear();
                c.set(idate.getYear(), idate.getMonth() - 1, idate.getDay(), 0, 0, 0);
                return new Timestamp(c.getTimeInMillis());
            } catch (IllegalArgumentException e) {
                throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e);
            }
        } finally {
            this.defaultTimeZoneLock.unlock();
        }
    }

    /**
     * Create a Timestamp from a TIME value.
     *
     * @return a timestamp at the given time on 1970 Jan 1.
     */
    @Override
    public Timestamp localCreateFromTime(InternalTime it) {
        if (it.getHours() < 0 || it.getHours() >= 24) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidTimeValue", new Object[] { it.toString() }));
        }

        this.defaultTimeZoneLock.lock();
        try {
            Calendar c;

            if (this.cal != null) {
                c = this.cal;
            } else {
                // c.f. Bug#11540 for details on locale
                c = Calendar.getInstance(this.defaultTimeZone, Locale.US);
                c.setLenient(false);
            }

            try {
                c.set(1970, 0, 1, it.getHours(), it.getMinutes(), it.getSeconds());
                Timestamp ts = new Timestamp(c.getTimeInMillis());
                ts.setNanos(it.getNanos());
                return ts;
            } catch (IllegalArgumentException e) {
                throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e);
            }
        } finally {
            this.defaultTimeZoneLock.unlock();
        }
    }

    @Override
    public Timestamp localCreateFromTimestamp(InternalTimestamp its) {
        if (its.getYear() == 0 && its.getMonth() == 0 && its.getDay() == 0) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidZeroDate"));
        }

        this.defaultTimeZoneLock.lock();
        try {
            Calendar c;

            if (this.cal != null) {
                c = this.cal;
            } else {
                // c.f. Bug#11540 for details on locale
                c = Calendar.getInstance(this.pset.getBooleanProperty(PropertyKey.preserveInstants).getValue() ? this.connectionTimeZone : this.defaultTimeZone,
                        Locale.US);
                c.setLenient(false);
            }

            try {
                // this method is HUGEly faster than Java 8's Calendar.Builder()
                c.set(its.getYear(), its.getMonth() - 1, its.getDay(), its.getHours(), its.getMinutes(), its.getSeconds());
                Timestamp ts = new Timestamp(c.getTimeInMillis());
                ts.setNanos(its.getNanos());
                return ts;
            } catch (IllegalArgumentException e) {
                throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e);
            }
        } finally {
            this.defaultTimeZoneLock.unlock();
        }
    }

    @Override
    public Timestamp localCreateFromDatetime(InternalTimestamp its) {
        if (its.getYear() == 0 && its.getMonth() == 0 && its.getDay() == 0) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidZeroDate"));
        }

        this.defaultTimeZoneLock.lock();
        try {
            Calendar c;

            if (this.cal != null) {
                c = this.cal;
            } else {
                // c.f. Bug#11540 for details on locale
                c = Calendar.getInstance(this.pset.getBooleanProperty(PropertyKey.preserveInstants).getValue() ? this.connectionTimeZone : this.defaultTimeZone,
                        Locale.US);
                c.setLenient(false);
            }

            try {
                // this method is HUGEly faster than Java 8's Calendar.Builder()
                c.set(its.getYear(), its.getMonth() - 1, its.getDay(), its.getHours(), its.getMinutes(), its.getSeconds());
                Timestamp ts = new Timestamp(c.getTimeInMillis());
                ts.setNanos(its.getNanos());
                return ts;
            } catch (IllegalArgumentException e) {
                throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e);
            }
        } finally {
            this.defaultTimeZoneLock.unlock();
        }
    }

    @Override
    public String getTargetTypeName() {
        return Timestamp.class.getName();
    }

}
