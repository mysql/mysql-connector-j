/*
 * Copyright (c) 2007, 2018, Oracle and/or its affiliates. All rights reserved.
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

package com.mysql.cj.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.Array;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Interface to allow PreparedStatement implementations to expose their parameter bindings to QueryInterceptors.
 */
public interface ParameterBindings {

    Array getArray(int parameterIndex) throws SQLException;

    InputStream getAsciiStream(int parameterIndex) throws SQLException;

    BigDecimal getBigDecimal(int parameterIndex) throws SQLException;

    InputStream getBinaryStream(int parameterIndex) throws SQLException;

    java.sql.Blob getBlob(int parameterIndex) throws SQLException;

    boolean getBoolean(int parameterIndex) throws SQLException;

    byte getByte(int parameterIndex) throws SQLException;

    byte[] getBytes(int parameterIndex) throws SQLException;

    Reader getCharacterStream(int parameterIndex) throws SQLException;

    Clob getClob(int parameterIndex) throws SQLException;

    Date getDate(int parameterIndex) throws SQLException;

    double getDouble(int parameterIndex) throws SQLException;

    float getFloat(int parameterIndex) throws SQLException;

    int getInt(int parameterIndex) throws SQLException;

    BigInteger getBigInteger(int parameterIndex) throws SQLException;

    long getLong(int parameterIndex) throws SQLException;

    Reader getNCharacterStream(int parameterIndex) throws SQLException;

    Reader getNClob(int parameterIndex) throws SQLException;

    Object getObject(int parameterIndex) throws SQLException;

    Ref getRef(int parameterIndex) throws SQLException;

    short getShort(int parameterIndex) throws SQLException;

    String getString(int parameterIndex) throws SQLException;

    Time getTime(int parameterIndex) throws SQLException;

    Timestamp getTimestamp(int parameterIndex) throws SQLException;

    URL getURL(int parameterIndex) throws SQLException;

    boolean isNull(int parameterIndex) throws SQLException;
}
