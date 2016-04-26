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

package com.mysql.cj.api.x;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import com.mysql.cj.x.json.DbDoc;

/**
 * A row element returned from a SELECT query.
 */
public interface Row {
    /**
     * Retrieve the value for column `fieldName' as a decimal value.
     */
    BigDecimal getBigDecimal(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a decimal value.
     */
    BigDecimal getBigDecimal(int pos);

    /**
     * Retrieve the value for column `fieldName' as a boolean value.
     */
    boolean getBoolean(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a boolean value.
     */
    boolean getBoolean(int pos);

    /**
     * Retrieve the value for column `fieldName' as a byte value.
     */
    byte getByte(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a byte value.
     */
    byte getByte(int pos);

    /**
     * Retrieve the value for column `fieldName' as a {@link Date} value.
     */
    Date getDate(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a byte value.
     */
    Date getDate(int pos);

    /**
     * Retrieve the value for column `fieldName' as a DbDoc value.
     */
    DbDoc getDbDoc(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a DbDoc value.
     */
    DbDoc getDbDoc(int pos);

    /**
     * Retrieve the value for column `fieldName' as a double value.
     */
    double getDouble(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a double value.
     */
    double getDouble(int pos);

    /**
     * Retrieve the value for column `fieldName' as an integer value.
     */
    int getInt(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as an integer value.
     */
    int getInt(int pos);

    /**
     * Retrieve the value for column `fieldName' as a long value.
     */
    long getLong(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a long value.
     */
    long getLong(int pos);

    /**
     * Retrieve the value for column `fieldName' as a string value.
     */
    String getString(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a string value.
     */
    String getString(int pos);

    /**
     * Retrieve the value for column `fieldName' as a {@link Time} value.
     */
    Time getTime(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a byte value.
     */
    Time getTime(int pos);

    /**
     * Retrieve the value for column `fieldName' as a {@link Timestamp} value.
     */
    Timestamp getTimestamp(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a byte value.
     */
    Timestamp getTimestamp(int pos);
}
