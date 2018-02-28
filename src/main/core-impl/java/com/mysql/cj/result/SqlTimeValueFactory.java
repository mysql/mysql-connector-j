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

import java.sql.Time;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import com.mysql.cj.Messages;
import com.mysql.cj.WarningListener;
import com.mysql.cj.exceptions.DataReadException;

/**
 * A value factory to create {@link java.sql.Time} instances. As with other date/time types, a time zone is necessary to interpret the
 * time values returned from the server.
 */
public class SqlTimeValueFactory extends DefaultValueFactory<Time> {
    private TimeZone tz;
    private WarningListener warningListener;
    // cached per instance to avoid re-creation on every create*() call
    private Calendar cal;

    public SqlTimeValueFactory(TimeZone tz) {
        this.tz = tz;
        this.cal = Calendar.getInstance(this.tz, Locale.US);
        this.cal.setLenient(false);
    }

    public SqlTimeValueFactory(TimeZone tz, WarningListener warningListener) {
        this(tz);
        this.warningListener = warningListener;
    }

    @Override
    public Time createFromTime(int hours, int minutes, int seconds, int nanos) {
        if (hours < 0 || hours >= 24) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidTimeValue", new Object[] { "" + hours + ":" + minutes + ":" + seconds }));
        }

        synchronized (this.cal) {
            // c.f. java.sql.Time "The date components should be set to the "zero epoch" value of January 1, 1970 and should not be accessed."
            this.cal.set(1970, 0, 1, hours, minutes, seconds);
            this.cal.set(Calendar.MILLISECOND, 0);
            long ms = (nanos / 1000000) + this.cal.getTimeInMillis();
            return new Time(ms);
        }
    }

    @Override
    public Time createFromTimestamp(int year, int month, int day, int hours, int minutes, int seconds, int nanos) {
        if (this.warningListener != null) {
            // TODO: need column context
            this.warningListener.warningEncountered(Messages.getString("ResultSet.PrecisionLostWarning", new Object[] { "java.sql.Time" }));
        }

        // truncate date information
        return createFromTime(hours, minutes, seconds, nanos);
    }

    public String getTargetTypeName() {
        return Time.class.getName();
    }
}
