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

package com.mysql.cj.xdevapi;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

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
