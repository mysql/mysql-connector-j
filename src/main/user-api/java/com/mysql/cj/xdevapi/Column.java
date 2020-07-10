/*
 * Copyright (c) 2015, 2020, Oracle and/or its affiliates.
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

/**
 * Represents individual column of {@link RowResult}
 */
public interface Column {

    /**
     * Get name of {@link Schema}.
     * 
     * @return schema name
     */
    String getSchemaName();

    /**
     * Get name of {@link Table}.
     * 
     * @return table name
     */
    String getTableName();

    /**
     * Get alias of {@link Table}.
     * 
     * @return table alias
     */
    String getTableLabel();

    /**
     * Get name of this column.
     * 
     * @return column name
     */
    String getColumnName();

    /**
     * Get alias of this column.
     * 
     * @return column alias
     */
    String getColumnLabel();

    /**
     * Get this column's {@link Type}.
     * 
     * @return column type
     */
    Type getType();

    /**
     * Get this column's length.
     * 
     * @return column length
     */
    long getLength();

    /**
     * Get number of fractional digits in this column's value.
     * 
     * @return number of fractional digits
     */
    int getFractionalDigits();

    /**
     * Checks if value represents a signed number.
     * 
     * @return true if value represents a signed number
     */
    boolean isNumberSigned();

    /**
     * Get collation name for the column value.
     * 
     * @return collation name
     */
    String getCollationName();

    /**
     * Get character set name for the column value.
     * 
     * @return character set name
     */
    String getCharacterSetName();

    /**
     * Checks if the value has a padding.
     * 
     * @return true if column flags contain FIELD_FLAG_ZEROFILL or field type is CHAR
     */
    boolean isPadded();

    /**
     * Checks if column can contain null values.
     * 
     * @return false if column flags contain FIELD_FLAG_NOT_NULL
     */
    boolean isNullable();

    /**
     * Checks if this is an auto increment column.
     * 
     * @return true if column flags contain FIELD_FLAG_AUTO_INCREMENT
     */
    boolean isAutoIncrement();

    /**
     * Checks if this is a primary key column.
     * 
     * @return true if column flags contain FIELD_FLAG_PRIMARY_KEY
     */
    boolean isPrimaryKey();

    /**
     * Checks if this is a unique key column.
     * 
     * @return true if column flags contain FIELD_FLAG_UNIQUE_KEY
     */
    boolean isUniqueKey();

    /**
     * Checks if this column is a part of key.
     * 
     * @return true if column flags contain FIELD_FLAG_MULTIPLE_KEY
     */
    boolean isPartKey();
}
