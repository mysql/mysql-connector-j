/*
 * Copyright (c) 2015, 2022, Oracle and/or its affiliates.
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

import java.sql.Date;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
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
import com.mysql.cj.util.TimeUtil;

/**
 * A value factory for creating {@link java.sql.Date} values.
 */
public class SqlDateValueFactory extends AbstractDateTimeValueFactory<Date> {
    private WarningListener warningListener;
    // cached per instance to avoid re-creation on every create*() call
    private final Calendar cal;
    private final ReentrantLock calLock = new ReentrantLock();

    public SqlDateValueFactory(PropertySet pset, Calendar calendar, TimeZone tz) {
        super(pset);
        if (calendar != null) {
            this.cal = (Calendar) calendar.clone();
        } else {
            // c.f. Bug#11540 for details on locale
            this.cal = Calendar.getInstance(tz, Locale.US);
            this.cal.set(Calendar.MILLISECOND, 0);
            this.cal.setLenient(false);
        }
    }

    public SqlDateValueFactory(PropertySet pset, Calendar calendar, TimeZone tz, WarningListener warningListener) {
        this(pset, calendar, tz);
        this.warningListener = warningListener;
    }

    @Override
    public Date localCreateFromDate(InternalDate idate) {
        this.calLock.lock();
        try {
            try {
                if (idate.isZero()) {
                    throw new DataReadException(Messages.getString("ResultSet.InvalidZeroDate"));
                }

                this.cal.clear();
                this.cal.set(idate.getYear(), idate.getMonth() - 1, idate.getDay());
                long ms = this.cal.getTimeInMillis();
                return new Date(ms);
            } catch (IllegalArgumentException e) {
                throw ExceptionFactory.createException(WrongArgumentException.class, e.getMessage(), e);
            }
        } finally {
            calLock.unlock();
        }
    }

    @Override
    public Date localCreateFromTime(InternalTime it) {
        if (this.warningListener != null) {
            // TODO: need column context
            this.warningListener.warningEncountered(Messages.getString("ResultSet.ImplicitDatePartWarning", new Object[] { "java.sql.Date" }));
        }
        return Date.valueOf(TimeUtil.DEFAULT_DATE);
    }

    @Override
    public Date localCreateFromTimestamp(InternalTimestamp its) {
        if (this.warningListener != null) {
            // TODO: need column context
            this.warningListener.warningEncountered(Messages.getString("ResultSet.PrecisionLostWarning", new Object[] { "java.sql.Date" }));
        }

        // truncate any time information
        return createFromDate(its);
    }

    @Override
    public Date localCreateFromDatetime(InternalTimestamp its) {
        if (this.warningListener != null) {
            // TODO: need column context
            this.warningListener.warningEncountered(Messages.getString("ResultSet.PrecisionLostWarning", new Object[] { "java.sql.Date" }));
        }

        // truncate any time information
        return createFromDate(its);
    }

    public String getTargetTypeName() {
        return Date.class.getName();
    }
}
