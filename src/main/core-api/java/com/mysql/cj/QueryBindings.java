/*
 * Copyright (c) 2017, 2023, Oracle and/or its affiliates.
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
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.result.Field;

public interface QueryBindings {

    QueryBindings clone();

    void setColumnDefinition(ColumnDefinition colDef);

    BindValue[] getBindValues();

    void setBindValues(BindValue[] bindValues);

    /**
     *
     * @return true if bind values had long data
     */
    boolean clearBindValues();

    void checkParameterSet(int columnIndex);

    void checkAllParametersSet();

    int getNumberOfExecutions();

    void setNumberOfExecutions(int numberOfExecutions);

    boolean isLongParameterSwitchDetected();

    void setLongParameterSwitchDetected(boolean longParameterSwitchDetected);

    AtomicBoolean getSendTypesToServer();

    BindValue getBinding(int parameterIndex, boolean forLongData);

    void setFromBindValue(int parameterIndex, BindValue bv);

    void setAsciiStream(int parameterIndex, InputStream x, int length);

    void setBigDecimal(int parameterIndex, BigDecimal x);

    void setBigInteger(int parameterIndex, BigInteger x);

    void setBinaryStream(int parameterIndex, InputStream x, int length);

    void setBlob(int parameterIndex, java.sql.Blob x);

    void setBoolean(int parameterIndex, boolean x);

    void setByte(int parameterIndex, byte x);

    void setBytes(int parameterIndex, byte[] x, boolean escapeIfNeeded);

    void setCharacterStream(int parameterIndex, Reader reader, int length);

    void setClob(int i, Clob x);

    void setDate(int parameterIndex, Date x, Calendar cal);

    void setDouble(int parameterIndex, double x);

    void setFloat(int parameterIndex, float x);

    void setInt(int parameterIndex, int x);

    void setLong(int parameterIndex, long x);

    void setNCharacterStream(int parameterIndex, Reader reader, long length);

    void setNClob(int parameterIndex, NClob value);

    void setNString(int parameterIndex, String x);

    void setNull(int parameterIndex);

    boolean isNull(int parameterIndex);

    void setObject(int parameterIndex, Object parameterObj);

    void setObject(int parameterIndex, Object parameterObj, MysqlType targetMysqlType, int scaleOrLength);

    void setShort(int parameterIndex, short x);

    void setString(int parameterIndex, String x);

    void setTime(int parameterIndex, Time x, Calendar cal);

    void setTimestamp(int parameterIndex, Timestamp x, Calendar targetCalendar, Field field, MysqlType targetMysqlType);

    byte[] getBytesRepresentation(int parameterIndex);

}
