/*
  Copyright (c) 2016, 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.io;

import java.time.LocalTime;

import com.mysql.cj.api.WarningListener;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.DataReadException;

/**
 * JdbcTimeValueFactory is a value factory to create {@link LocalTime} instances.
 */
public class LocalTimeValueFactory extends DefaultValueFactory<LocalTime> {
    private WarningListener warningListener;

    public LocalTimeValueFactory() {
    }

    public LocalTimeValueFactory(WarningListener warningListener) {
        this();
        this.warningListener = warningListener;
    }

    @Override
    public LocalTime createFromTime(int hours, int minutes, int seconds, int nanos) {
        if (hours < 0 || hours >= 24) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidTimeValue", new Object[] { "" + hours + ":" + minutes + ":" + seconds }));
        }
        return LocalTime.of(hours, minutes, seconds, nanos);
    }

    @Override
    public LocalTime createFromTimestamp(int year, int month, int day, int hours, int minutes, int seconds, int nanos) {
        if (this.warningListener != null) {
            this.warningListener.warningEncountered(Messages.getString("ResultSet.PrecisionLostWarning", new Object[] { getTargetTypeName() }));
        }
        // truncate date information
        return createFromTime(hours, minutes, seconds, nanos);
    }

    public String getTargetTypeName() {
        return LocalTime.class.getName();
    }
}
