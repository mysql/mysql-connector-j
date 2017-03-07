/*
  Copyright (c) 2015, 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.xdevapi;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import com.mysql.cj.xdevapi.DbDoc;

/**
 * A row element returned from a SELECT query.
 */
public interface Row {
    /**
     * Retrieve the value for column `fieldName' as a decimal value.
     * 
     * @param fieldName
     *            field name
     * @return value
     */
    BigDecimal getBigDecimal(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a decimal value.
     * 
     * @param pos
     *            field position
     * @return value
     */
    BigDecimal getBigDecimal(int pos);

    /**
     * Retrieve the value for column `fieldName' as a boolean value.
     * 
     * @param fieldName
     *            field name
     * @return value
     */
    boolean getBoolean(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a boolean value.
     * 
     * @param pos
     *            field position
     *            field position
     * @return value
     */
    boolean getBoolean(int pos);

    /**
     * Retrieve the value for column `fieldName' as a byte value.
     * 
     * @param fieldName
     *            field name
     * @return value
     */
    byte getByte(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a byte value.
     * 
     * @param pos
     *            field position
     *            field position
     * @return value
     */
    byte getByte(int pos);

    /**
     * Retrieve the value for column `fieldName' as a {@link Date} value.
     * 
     * @param fieldName
     *            field name
     * @return value
     */
    Date getDate(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a byte value.
     * 
     * @param pos
     *            field position
     *            field position
     * @return value
     */
    Date getDate(int pos);

    /**
     * Retrieve the value for column `fieldName' as a DbDoc value.
     * 
     * @param fieldName
     *            field name
     * @return value
     */
    DbDoc getDbDoc(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a DbDoc value.
     * 
     * @param pos
     *            field position
     *            field position
     * @return value
     */
    DbDoc getDbDoc(int pos);

    /**
     * Retrieve the value for column `fieldName' as a double value.
     * 
     * @param fieldName
     *            field name
     * @return value
     */
    double getDouble(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a double value.
     * 
     * @param pos
     *            field position
     *            field position
     * @return value
     */
    double getDouble(int pos);

    /**
     * Retrieve the value for column `fieldName' as an integer value.
     * 
     * @param fieldName
     *            field name
     * @return value
     */
    int getInt(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as an integer value.
     * 
     * @param pos
     *            field position
     *            field position
     * @return value
     */
    int getInt(int pos);

    /**
     * Retrieve the value for column `fieldName' as a long value.
     * 
     * @param fieldName
     *            field name
     * @return value
     */
    long getLong(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a long value.
     * 
     * @param pos
     *            field position
     * @return value
     */
    long getLong(int pos);

    /**
     * Retrieve the value for column `fieldName' as a string value.
     * 
     * @param fieldName
     *            field name
     * @return value
     */
    String getString(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a string value.
     * 
     * @param pos
     *            field position
     * @return value
     */
    String getString(int pos);

    /**
     * Retrieve the value for column `fieldName' as a {@link Time} value.
     * 
     * @param fieldName
     *            field name
     * @return value
     */
    Time getTime(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a byte value.
     * 
     * @param pos
     *            field position
     * @return value
     */
    Time getTime(int pos);

    /**
     * Retrieve the value for column `fieldName' as a {@link Timestamp} value.
     * 
     * @param fieldName
     *            field name
     * @return value
     */
    Timestamp getTimestamp(String fieldName);

    /**
     * Retrieve the value for column at position `pos' (starting at 0) as a byte value.
     * 
     * @param pos
     *            field position
     * @return value
     */
    Timestamp getTimestamp(int pos);
}
