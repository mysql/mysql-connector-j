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
import java.util.TimeZone;

import com.mysql.cj.exceptions.DataReadException;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.result.BigDecimalValueFactory;
import com.mysql.cj.result.BooleanValueFactory;
import com.mysql.cj.result.ByteValueFactory;
import com.mysql.cj.result.DoubleValueFactory;
import com.mysql.cj.result.IntegerValueFactory;
import com.mysql.cj.result.LongValueFactory;
import com.mysql.cj.result.Row;
import com.mysql.cj.result.SqlDateValueFactory;
import com.mysql.cj.result.SqlTimeValueFactory;
import com.mysql.cj.result.SqlTimestampValueFactory;
import com.mysql.cj.result.StringValueFactory;

/**
 * {@link com.mysql.cj.xdevapi.Row} implementation.
 */
public class RowImpl implements com.mysql.cj.xdevapi.Row {
    private Row row;
    private ColumnDefinition metadata;
    /**
     * Default time zone used to create date/time result values.
     */
    private TimeZone defaultTimeZone;

    /**
     * Constructor.
     * 
     * @param row
     *            {@link Row} instance provided by c/J core.
     * @param metadata
     *            {@link ColumnDefinition} object to use for new rows.
     * @param defaultTimeZone
     *            {@link TimeZone} object representing the default time zone
     */
    public RowImpl(Row row, ColumnDefinition metadata, TimeZone defaultTimeZone) {
        this.row = row;
        this.metadata = metadata;
        this.defaultTimeZone = defaultTimeZone;
    }

    /**
     * Map a field name to it's index in the row.
     *
     * @param fieldName
     *            field name
     * @return field index
     * @throws DataReadException
     *             if the field name is not in the row
     */
    private int fieldNameToIndex(String fieldName) {
        int idx = this.metadata.findColumn(fieldName, true, 0);
        if (idx == -1) {
            throw new DataReadException("Invalid column");
        }
        return idx;
    }

    public BigDecimal getBigDecimal(String fieldName) {
        return getBigDecimal(fieldNameToIndex(fieldName));
    }

    public BigDecimal getBigDecimal(int pos) {
        return this.row.getValue(pos, new BigDecimalValueFactory());
    }

    public boolean getBoolean(String fieldName) {
        return getBoolean(fieldNameToIndex(fieldName));
    }

    public boolean getBoolean(int pos) {
        return this.row.getValue(pos, new BooleanValueFactory());
    }

    public byte getByte(String fieldName) {
        return getByte(fieldNameToIndex(fieldName));
    }

    public byte getByte(int pos) {
        return this.row.getValue(pos, new ByteValueFactory());
    }

    public Date getDate(String fieldName) {
        return getDate(fieldNameToIndex(fieldName));
    }

    public Date getDate(int pos) {
        return this.row.getValue(pos, new SqlDateValueFactory(null, this.defaultTimeZone));
    }

    public DbDoc getDbDoc(String fieldName) {
        return getDbDoc(fieldNameToIndex(fieldName));
    }

    public DbDoc getDbDoc(int pos) {
        return this.row.getValue(pos, new DbDocValueFactory());
    }

    public double getDouble(String fieldName) {
        return getDouble(fieldNameToIndex(fieldName));
    }

    public double getDouble(int pos) {
        return this.row.getValue(pos, new DoubleValueFactory());
    }

    public int getInt(String fieldName) {
        return getInt(fieldNameToIndex(fieldName));
    }

    public int getInt(int pos) {
        return this.row.getValue(pos, new IntegerValueFactory());
    }

    public long getLong(String fieldName) {
        return getLong(fieldNameToIndex(fieldName));
    }

    public long getLong(int pos) {
        return this.row.getValue(pos, new LongValueFactory());
    }

    public String getString(String fieldName) {
        return getString(fieldNameToIndex(fieldName));
    }

    public String getString(int pos) {
        // TODO: charset
        return this.row.getValue(pos, new StringValueFactory());
    }

    public Time getTime(String fieldName) {
        return getTime(fieldNameToIndex(fieldName));
    }

    public Time getTime(int pos) {
        return this.row.getValue(pos, new SqlTimeValueFactory(null, this.defaultTimeZone));
    }

    public Timestamp getTimestamp(String fieldName) {
        return getTimestamp(fieldNameToIndex(fieldName));
    }

    public Timestamp getTimestamp(int pos) {
        return this.row.getValue(pos, new SqlTimestampValueFactory(null, this.defaultTimeZone));
    }
}
