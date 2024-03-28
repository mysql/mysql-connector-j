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

import java.sql.Time;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.mysql.cj.Messages;
import com.mysql.cj.WarningListener;
import com.mysql.cj.conf.PropertySet;
import com.mysql.cj.exceptions.DataReadException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.protocol.InternalDate;
import com.mysql.cj.protocol.InternalTime;
import com.mysql.cj.protocol.InternalTimestamp;

/**
 * A value factory to create {@link java.sql.Time} instances. As with other date/time types, a time zone is necessary to interpret the
 * time values returned from the server.
 */
public class SqlTimeValueFactory extends AbstractDateTimeValueFactory<Time> {

    private WarningListener warningListener;
    // cached per instance to avoid re-creation on every create*() call
    private final Calendar cal;
    private final Lock calLock = new ReentrantLock();

    public SqlTimeValueFactory(PropertySet pset, Calendar calendar, TimeZone tz) {
        super(pset);
        if (calendar != null) {
            this.cal = (Calendar) calendar.clone();
        } else {
            // c.f. Bug#11540 for details on locale
            this.cal = Calendar.getInstance(tz, Locale.US);
            this.cal.setLenient(false);
        }
    }

    public SqlTimeValueFactory(PropertySet pset, Calendar calendar, TimeZone tz, WarningListener warningListener) {
        this(pset, calendar, tz);
        this.warningListener = warningListener;
    }

    @Override
    Time localCreateFromDate(InternalDate idate) {
        this.calLock.lock();
        try {
            try {
                this.cal.clear();
                return new Time(this.cal.getTimeInMillis());
            } catch (IllegalArgumentException e) {
                throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e);
            }
        } finally {
            this.calLock.unlock();
        }
    }

    @Override
    public Time localCreateFromTime(InternalTime it) {
        if (it.getHours() < 0 || it.getHours() >= 24) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidTimeValue", new Object[] { it.toString() }));
        }

        this.calLock.lock();
        try {
            try {
                // c.f. java.sql.Time "The date components should be set to the "zero epoch" value of January 1, 1970 and should not be accessed."
                this.cal.set(1970, 0, 1, it.getHours(), it.getMinutes(), it.getSeconds());
                this.cal.set(Calendar.MILLISECOND, 0);
                long ms = it.getNanos() / 1000000 + this.cal.getTimeInMillis();
                return new Time(ms);
            } catch (IllegalArgumentException e) {
                throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e);
            }
        } finally {
            this.calLock.unlock();
        }
    }

    @Override
    public Time localCreateFromDatetime(InternalTimestamp its) {
        if (this.warningListener != null) {
            // TODO: need column context
            this.warningListener.warningEncountered(Messages.getString("ResultSet.PrecisionLostWarning", new Object[] { "java.sql.Time" }));
        }

        // truncate date information
        return createFromTime(new InternalTime(its.getHours(), its.getMinutes(), its.getSeconds(), its.getNanos(), its.getScale()));
    }

    @Override
    public Time localCreateFromTimestamp(InternalTimestamp its) {
        if (this.warningListener != null) {
            // TODO: need column context
            this.warningListener.warningEncountered(Messages.getString("ResultSet.PrecisionLostWarning", new Object[] { "java.sql.Time" }));
        }

        // truncate date information
        return createFromTime(new InternalTime(its.getHours(), its.getMinutes(), its.getSeconds(), its.getNanos(), its.getScale()));
    }

    @Override
    public String getTargetTypeName() {
        return Time.class.getName();
    }

}
