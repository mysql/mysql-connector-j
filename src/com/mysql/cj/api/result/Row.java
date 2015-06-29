/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.cj.api.result;

import com.mysql.cj.api.io.ValueFactory;

/**
 * @todo
 */
public interface Row {
    /**
     * Retrieve a value for the given column. This is the main facility to access values from the Row.
     *
     * @param columnIndex
     *            index of column to retrieve value from (0-indexed, not JDBC 1-indexed)
     * @param vf
     *            value factory used to create the return value after decoding
     * @return The return value from the value factory
     */
    <T> T getValue(int columnIndex, ValueFactory<T> vf);

    /**
     * Check whether a column is NULL and update the 'wasNull' status.
     */
    boolean getNull(int columnIndex);

    /**
     * Was the last value retrieved a NULL value?
     */
    boolean wasNull();
}
