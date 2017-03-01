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

import java.time.LocalDate;

import com.mysql.cj.api.WarningListener;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.DataReadException;

/**
 * A value factory for creating {@link LocalDate} values.
 */
public class LocalDateValueFactory extends DefaultValueFactory<LocalDate> {
    private WarningListener warningListener;

    public LocalDateValueFactory() {
    }

    public LocalDateValueFactory(WarningListener warningListener) {
        this();
        this.warningListener = warningListener;
    }

    @Override
    public LocalDate createFromDate(int year, int month, int day) {
        if (year == 0 && month == 0 && day == 0) {
            throw new DataReadException(Messages.getString("ResultSet.InvalidZeroDate"));
        }
        return LocalDate.of(year, month, day);
    }

    @Override
    public LocalDate createFromTimestamp(int year, int month, int day, int hours, int minutes, int seconds, int nanos) {
        if (this.warningListener != null) {
            this.warningListener.warningEncountered(Messages.getString("ResultSet.PrecisionLostWarning", new Object[] { getTargetTypeName() }));
        }
        // truncate any time information
        return createFromDate(year, month, day);
    }

    public String getTargetTypeName() {
        return LocalDate.class.getName();
    }
}
