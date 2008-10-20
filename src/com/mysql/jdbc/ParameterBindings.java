/*
 Copyright  2007 MySQL AB, 2008 Sun Microsystems

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

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
