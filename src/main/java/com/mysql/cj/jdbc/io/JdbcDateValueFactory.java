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

import java.sql.Date;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;

import com.mysql.cj.api.WarningListener;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.DataReadException;
import com.mysql.cj.core.io.DefaultValueFactory;

/**
 * A value factory for creating {@link java.sql.Date} values.
 */
public class JdbcDateValueFactory extends DefaultValueFactory<Date> {
    private TimeZone tz;
    private WarningListener warningListener;
    // cached per instance to avoid re-creation on every create*() call
    private Calendar cal;

    public JdbcDateValueFactory(TimeZone tz) {
        this.tz = tz;
        // c.f. Bug#11540 for details on locale
        this.cal = Calendar.getInstance(this.tz, Locale.US);
        this.cal.set(Calendar.MILLISECOND, 0);
        this.cal.setLenient(false);
    }

    public JdbcDateValueFactory(TimeZone tz, WarningListener warningListener) {
        this(tz);
        this.warningListener = warningListener;
    }

    @Override
    public Date createFromDate(int year, int month, int day) {
        synchronized (this.cal) {
            if (year == 0 && month == 0 && day == 0) {
                throw new DataReadException(Messages.getString("ResultSet.InvalidZeroDate"));
            }

            this.cal.set(year, month - 1, day, 0, 0, 0);
            long ms = this.cal.getTimeInMillis();
            return new Date(ms);
        }
    }

    @Override
    public Date createFromTimestamp(int year, int month, int day, int hours, int minutes, int seconds, int nanos) {
        if (this.warningListener != null) {
            // TODO: need column context
            this.warningListener.warningEncountered(Messages.getString("ResultSet.PrecisionLostWarning", new Object[] { "java.sql.Date" }));
        }

        // truncate any time information
        return createFromDate(year, month, day);
    }

    public String getTargetTypeName() {
        return Date.class.getName();
    }
}
