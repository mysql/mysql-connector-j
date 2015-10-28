/*
  Copyright (c) 2007, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.jdbc;

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
 * Interface to allow PreparedStatement implementations to expose their parameter bindings to StatementInterceptors.
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
