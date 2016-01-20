/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.cj.jdbc.io;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.DataReadException;
import com.mysql.cj.core.io.DefaultValueFactory;

/**
 * Value factory to create {@link java.sql.Timestamp} instances. Timestamp instances are created from fields returned from the db without a timezone. In order
 * to create a <i>point-in-time</i>, a time zone must be provided to interpret the fields.
 */
public class JdbcTimestampValueFactory extends DefaultValueFactory<Timestamp> {
    private TimeZone tz;
    // cached per instance to avoid re-creation on every create*() call
    private Calendar cal;

    /**
     * @param tz
     *            The time zone used to interpret the fields.
     */
    public JdbcTimestampValueFactory(TimeZone tz) {
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
     * @see java.sql.ResultSet.getTimestamp(int)
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
