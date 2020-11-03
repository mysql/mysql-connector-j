/*
 * Copyright (c) 2017, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;

import com.mysql.cj.protocol.ColumnDefinition;

public interface QueryBindings<T extends BindValue> {

    QueryBindings<T> clone();

    void setColumnDefinition(ColumnDefinition colDef);

    boolean isLoadDataQuery();

    void setLoadDataQuery(boolean isLoadDataQuery);

    T[] getBindValues();

    void setBindValues(T[] bindValues);

    /**
     * 
     * @return true if bind values had long data
     */
    boolean clearBindValues();

    void checkParameterSet(int columnIndex);

    void checkAllParametersSet();

    int getNumberOfExecutions();

    void setNumberOfExecutions(int numberOfExecutions);

    void setValue(int paramIndex, byte[] val, MysqlType type);

    void setValue(int paramIndex, String val, MysqlType type);

    // Array getArray(int parameterIndex);

    void setAsciiStream(int parameterIndex, InputStream x);

    void setAsciiStream(int parameterIndex, InputStream x, int length);

    void setAsciiStream(int parameterIndex, InputStream x, long length);

    // InputStream getAsciiStream(int parameterIndex);

    void setBigDecimal(int parameterIndex, BigDecimal x);

    // BigDecimal getBigDecimal(int parameterIndex);

    void setBigInteger(int parameterIndex, BigInteger x);

    // BigInteger getBigInteger(int parameterIndex);

    void setBinaryStream(int parameterIndex, InputStream x);

    void setBinaryStream(int parameterIndex, InputStream x, int length);

    void setBinaryStream(int parameterIndex, InputStream x, long length);

    // InputStream getBinaryStream(int parameterIndex);

    void setBlob(int parameterIndex, java.sql.Blob x);

    void setBlob(int parameterIndex, InputStream inputStream);

    void setBlob(int parameterIndex, InputStream inputStream, long length);

    // java.sql.Blob getBlob(int parameterIndex);

    void setBoolean(int parameterIndex, boolean x);

    // boolean getBoolean(int parameterIndex);

    void setByte(int parameterIndex, byte x);

    // byte getByte(int parameterIndex);

    void setBytes(int parameterIndex, byte[] x);

    void setBytes(int parameterIndex, byte[] x, boolean checkForIntroducer, boolean escapeForMBChars);

    void setBytesNoEscape(int parameterIndex, byte[] parameterAsBytes);

    void setBytesNoEscapeNoQuotes(int parameterIndex, byte[] parameterAsBytes);

    // byte[] getBytes(int parameterIndex);

    void setCharacterStream(int parameterIndex, Reader reader);

    void setCharacterStream(int parameterIndex, Reader reader, int length);

    void setCharacterStream(int parameterIndex, Reader reader, long length);

    // Reader getCharacterStream(int parameterIndex);

    void setClob(int i, Clob x);

    void setClob(int parameterIndex, Reader reader);

    void setClob(int parameterIndex, Reader reader, long length);

    // Clob getClob(int parameterIndex);

    void setDate(int parameterIndex, Date x);

    void setDate(int parameterIndex, Date x, Calendar cal);

    // Date getDate(int parameterIndex);

    void setDouble(int parameterIndex, double x);

    // double getDouble(int parameterIndex)

    void setFloat(int parameterIndex, float x);

    // float getFloat(int parameterIndex);

    void setInt(int parameterIndex, int x);

    // int getInt(int parameterIndex);

    void setLong(int parameterIndex, long x);

    // long getLong(int parameterIndex);

    void setNCharacterStream(int parameterIndex, Reader value);

    void setNCharacterStream(int parameterIndex, Reader reader, long length);

    // Reader getNCharacterStream(int parameterIndex);

    void setNClob(int parameterIndex, Reader reader);

    void setNClob(int parameterIndex, Reader reader, long length);

    void setNClob(int parameterIndex, NClob value);

    // Reader getNClob(int parameterIndex);

    void setNString(int parameterIndex, String x);

    void setNull(int parameterIndex);

    boolean isNull(int parameterIndex);

    void setObject(int parameterIndex, Object parameterObj);

    void setObject(int parameterIndex, Object parameterObj, MysqlType targetMysqlType);

    void setObject(int parameterIndex, Object parameterObj, MysqlType targetMysqlType, int scaleOrLength);

    // Object getObject(int parameterIndex);

    // Ref getRef(int parameterIndex);

    void setShort(int parameterIndex, short x);

    // short getShort(int parameterIndex);

    void setString(int parameterIndex, String x);

    // String getString(int parameterIndex);

    void setTime(int parameterIndex, Time x);

    void setTime(int parameterIndex, Time x, Calendar cal);

    // Time getTime(int parameterIndex);

    void setTimestamp(int parameterIndex, Timestamp x, Calendar cal, MysqlType targetMysqlType);

    void setTimestamp(int parameterIndex, Timestamp x, MysqlType targetMysqlType);

    void setTimestamp(int parameterIndex, Timestamp x, Calendar targetCalendar, int fractionalLength, MysqlType targetMysqlType);

    void bindTimestamp(int parameterIndex, Timestamp x, Calendar targetCalendar, int fractionalLength, MysqlType targetMysqlType);

    // Timestamp getTimestamp(int parameterIndex);

    // URL getURL(int parameterIndex);

    byte[] getBytesRepresentation(int parameterIndex);

    byte[] getOrigBytes(int parameterIndex);

    void setLocalDate(int parameterIndex, LocalDate x, MysqlType targetMysqlType);

    void setLocalTime(int parameterIndex, LocalTime x, MysqlType targetMysqlType);

    void setLocalDateTime(int parameterIndex, LocalDateTime x, MysqlType targetMysqlType);

}
