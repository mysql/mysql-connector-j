/*
 Copyright (c) 2007, 2010, Oracle and/or its affiliates. All rights reserved.
 

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
package com.mysql.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Interface to allow PreparedStatement implementations to expose
 * their parameter bindings to StatementInterceptors.
 * 
 * @version $Id: $
 */
public interface ParameterBindings {

	public abstract Array getArray(int parameterIndex) throws SQLException;

	public abstract InputStream getAsciiStream(int parameterIndex) throws SQLException;

	public abstract BigDecimal getBigDecimal(int parameterIndex) throws SQLException;

	public abstract InputStream getBinaryStream(int parameterIndex) throws SQLException;

	public abstract java.sql.Blob getBlob(int parameterIndex) throws SQLException;

	public abstract boolean getBoolean(int parameterIndex) throws SQLException;
	
	public abstract byte getByte(int parameterIndex) throws SQLException;

	public abstract byte[] getBytes(int parameterIndex) throws SQLException;

	public abstract Reader getCharacterStream(int parameterIndex) throws SQLException;

	public abstract Clob getClob(int parameterIndex) throws SQLException;
	
	public abstract Date getDate(int parameterIndex) throws SQLException;
	
	public abstract double getDouble(int parameterIndex) throws SQLException;

	public abstract float getFloat(int parameterIndex) throws SQLException;

	public abstract int getInt(int parameterIndex) throws SQLException;
	
	public abstract long getLong(int parameterIndex) throws SQLException;

	public abstract Reader getNCharacterStream(int parameterIndex) throws SQLException;
	
	public abstract Reader getNClob(int parameterIndex) throws SQLException;
	
	public abstract Object getObject(int parameterIndex) throws SQLException;

	public abstract Ref getRef(int parameterIndex) throws SQLException;

	public abstract short getShort(int parameterIndex) throws SQLException;

	public abstract String getString(int parameterIndex) throws SQLException;

	public abstract Time getTime(int parameterIndex) throws SQLException;

	public abstract Timestamp getTimestamp(int parameterIndex) throws SQLException;

	public abstract URL getURL(int parameterIndex) throws SQLException;

	public abstract boolean isNull(int parameterIndex) throws SQLException;
}
