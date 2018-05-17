/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.DataReadException;

/**
 * Value factory to create {@link java.sql.Timestamp} instances. Timestamp instances are created from fields returned from the db without a timezone. In order
 * to create a <i>point-in-time</i>, a time zone must be provided to interpret the fields.
 */
public class SqlTimestampValueFactory extends DefaultValueFactory<Timestamp> {
    private TimeZone tz;
    // cached per instance to avoid re-creation on every create*() call
    private Calendar cal;

    /**
     * @param tz
     *            The time zone used to interpret the fields.
     */
    public SqlTimestampValueFactory(TimeZone tz) {
        this.tz = tz;
        this.cal = Calendar.getInstance(this.tz, Locale.US);
        this.cal.setLenient(false);
    }

    public TimeZone getTimeZone() {
        return this.tz;
    }

    /**
     * Create a Timestamp from a DATE value.
     *
     * @return a timestamp at midnight on the day given by the DATE value
     */
    @Override
    public Timestamp createFromDate(int year, int month, int day) {
        return createFromTimestamp(year, month, day, 0, 0, 0, 0);
    }

    /**
     * Create a Timestamp from a TIME value.
     *
     * @return a timestamp at the given time on 1970 Jan 1.
     */
    @Override
    public Timestamp createFromTime(int hours, int minutes, int seconds, int nanos) {
        if (hours < 0 || hours >= 24) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidTimeValue", new Object[] { "" + hours + ":" + minutes + ":" + seconds }));
        }

        return createFromTimestamp(1970, 1, 1, hours, minutes, seconds, nanos);
    }

    @Override
    public Timestamp createFromTimestamp(int year, int month, int day, int hours, int minutes, int seconds, int nanos) {
        if (year == 0 && month == 0 && day == 0) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidZeroDate"));
        }

        synchronized (this.cal) {
            // this method is HUGEly faster than Java 8's Calendar.Builder()
            this.cal.set(year, month - 1, day, hours, minutes, seconds);
            Timestamp ts = new Timestamp(this.cal.getTimeInMillis());
            ts.setNanos(nanos);
            return ts;
        }
    }

    public String getTargetTypeName() {
        return Timestamp.class.getName();
    }
}
