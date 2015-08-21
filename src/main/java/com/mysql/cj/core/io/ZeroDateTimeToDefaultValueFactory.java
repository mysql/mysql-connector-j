/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

import com.mysql.cj.api.io.ValueFactory;

/**
 * A decorating value factory which "rounds" zero date/time values into defaults. All fields are set to their lowest permissible value. e.g. 0000-00-00 is
 * returned as 0001-01-01.
 */
public class ZeroDateTimeToDefaultValueFactory<T> extends BaseDecoratingValueFactory<T> {
    public ZeroDateTimeToDefaultValueFactory(ValueFactory<T> targetVf) {
        super(targetVf);
    }

    @Override
    public T createFromDate(int year, int month, int day) {
        if (year + month + day == 0) {
            return this.targetVf.createFromDate(1, 1, 1);
        }
        return this.targetVf.createFromDate(year, month, day);
    }

    @Override
    public T createFromTime(int hours, int minutes, int seconds, int nanos) {
        if (hours + minutes + seconds + nanos == 0) {
            return this.targetVf.createFromTime(0, 0, 0, 0);
        }
        return this.targetVf.createFromTime(hours, minutes, seconds, nanos);
    }

    @Override
    public T createFromTimestamp(int year, int month, int day, int hours, int minutes, int seconds, int nanos) {
        if (year + month + day + hours + minutes + seconds + nanos == 0) {
            return this.targetVf.createFromTimestamp(1, 1, 1, 0, 0, 0, 0);
        }
        return this.targetVf.createFromTimestamp(year, month, day, hours, minutes, seconds, nanos);
    }
}
