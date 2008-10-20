/*
 Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems

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
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.RowId;
import java.sql.SQLXML;
import java.sql.NClob;



public class JDBC4CallableStatement extends CallableStatement {

	public JDBC4CallableStatement(ConnectionImpl conn,
			CallableStatementParamInfo paramInfo) throws SQLException {
		super(conn, paramInfo);
	}

	public JDBC4CallableStatement(ConnectionImpl conn, String sql,
			String catalog, boolean isFunctionCall) throws SQLException {
		super(conn, sql, catalog, isFunctionCall);
	}

	
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		JDBC4PreparedStatementHelper.setRowId(this, parameterIndex, x);
	}

	public void setRowId(String parameterName, RowId x) throws SQLException {
		JDBC4PreparedStatementHelper.setRowId(this, getNamedParamIndex(
				parameterName, false), x);
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		JDBC4PreparedStatementHelper.setSQLXML(this, parameterIndex, xmlObject);
	}

	public void setSQLXML(String parameterName, SQLXML xmlObject)
			throws SQLException {
		JDBC4PreparedStatementHelper.setSQLXML(this, getNamedParamIndex(
				parameterName, false), xmlObject);

	}

	public SQLXML getSQLXML(int parameterIndex) throws SQLException {
		ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

		SQLXML retValue = ((com.mysql.jdbc.JDBC4ResultSet) rs)
				.getSQLXML(mapOutputParameterIndexToRsIndex(parameterIndex));

		this.outputParamWasNull = rs.wasNull();

		return retValue;

	}

	public SQLXML getSQLXML(String parameterName) throws SQLException {
		ResultSetInternalMethods rs = getOutputParameters(0); // definitely
																// not going to
																// be
		// from ?=

		SQLXML retValue = ((com.mysql.jdbc.JDBC4ResultSet) rs)
				.getSQLXML(fixParameterName(parameterName));

		this.outputParamWasNull = rs.wasNull();

		return retValue;
	}

	public RowId getRowId(int parameterIndex) throws SQLException {
		ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

		RowId retValue = ((com.mysql.jdbc.JDBC4ResultSet) rs)
				.getRowId(mapOutputParameterIndexToRsIndex(parameterIndex));

		this.outputParamWasNull = rs.wasNull();

		return retValue;
	}

	public RowId getRowId(String parameterName) throws SQLException {
		ResultSetInternalMethods rs = getOutputParameters(0); // definitely
																// not going to
																// be
		// from ?=

		RowId retValue = ((com.mysql.jdbc.JDBC4ResultSet) rs)
				.getRowId(fixParameterName(parameterName));

		this.outputParamWasNull = rs.wasNull();

		return retValue;
	}

	/**
	 * JDBC 4.0 Set a NCLOB parameter.
	 * 
	 * @param i
	 *            the first parameter is 1, the second is 2, ...
	 * @param x
	 *            an object representing a NCLOB
	 * 
	 * @throws SQLException
	 *             if a database error occurs
	 */
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		JDBC4PreparedStatementHelper.setNClob(this, parameterIndex, value);
	}

	public void setNClob(String parameterName, NClob value) throws SQLException {
		JDBC4PreparedStatementHelper.setNClob(this, getNamedParamIndex(
				parameterName, false), value);

	}

	public void setNClob(String parameterName, Reader reader)
			throws SQLException {
		setNClob(getNamedParamIndex(parameterName, false), reader);

	}

	public void setNClob(String parameterName, Reader reader, long length)
			throws SQLException {
		setNClob(getNamedParamIndex(parameterName, false), reader, length);

	}

	public void setNString(String parameterName, String value)
			throws SQLException {
		setNString(getNamedParamIndex(parameterName, false), value);
	}

	/**
	 * @see java.sql.CallableStatement#getCharacterStream(int)
	 */
	public Reader getCharacterStream(int parameterIndex) throws SQLException {
		ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

		Reader retValue = rs
				.getCharacterStream(mapOutputParameterIndexToRsIndex(parameterIndex));

		this.outputParamWasNull = rs.wasNull();

		return retValue;
	}

	/**
	 * @see java.sql.CallableStatement#getCharacterStream(java.lang.String)
	 */
	public Reader getCharacterStream(String parameterName) throws SQLException {
		ResultSetInternalMethods rs = getOutputParameters(0); // definitely
																// not going to
																// be
		// from ?=

		Reader retValue = rs
				.getCharacterStream(fixParameterName(parameterName));

		this.outputParamWasNull = rs.wasNull();

		return retValue;
	}

	/**
	 * @see java.sql.CallableStatement#getNCharacterStream(int)
	 */
	public Reader getNCharacterStream(int parameterIndex) throws SQLException {
		ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

		Reader retValue = ((com.mysql.jdbc.JDBC4ResultSet) rs)
				.getNCharacterStream(mapOutputParameterIndexToRsIndex(parameterIndex));

		this.outputParamWasNull = rs.wasNull();

		return retValue;
	}

	/**
	 * @see java.sql.CallableStatement#getNCharacterStream(java.lang.String)
	 */
	public Reader getNCharacterStream(String parameterName) throws SQLException {
		ResultSetInternalMethods rs = getOutputParameters(0); // definitely
																// not going to
																// be
		// from ?=

		Reader retValue = ((com.mysql.jdbc.JDBC4ResultSet) rs)
				.getNCharacterStream(fixParameterName(parameterName));

		this.outputParamWasNull = rs.wasNull();

		return retValue;
	}

	/**
	 * @see java.sql.CallableStatement#getNClob(int)
	 */
	public NClob getNClob(int parameterIndex) throws SQLException {
		ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

		NClob retValue = ((com.mysql.jdbc.JDBC4ResultSet) rs)
				.getNClob(mapOutputParameterIndexToRsIndex(parameterIndex));

		this.outputParamWasNull = rs.wasNull();

		return retValue;
	}

	/**
	 * @see java.sql.CallableStatement#getNClob(java.lang.String)
	 */
	public NClob getNClob(String parameterName) throws SQLException {
		ResultSetInternalMethods rs = getOutputParameters(0); // definitely
																// not going to
																// be
		// from ?=

		NClob retValue = ((com.mysql.jdbc.JDBC4ResultSet) rs)
				.getNClob(fixParameterName(parameterName));

		this.outputParamWasNull = rs.wasNull();

		return retValue;
	}

	/**
	 * @see java.sql.CallableStatement#getNString(int)
	 */
	public String getNString(int parameterIndex) throws SQLException {
		ResultSetInternalMethods rs = getOutputParameters(parameterIndex);

		String retValue = ((com.mysql.jdbc.JDBC4ResultSet) rs)
				.getNString(mapOutputParameterIndexToRsIndex(parameterIndex));

		this.outputParamWasNull = rs.wasNull();

		return retValue;
	}

	/**
	 * @see java.sql.CallableStatement#getNString(java.lang.String)
	 */
	public String getNString(String parameterName) throws SQLException {
		ResultSetInternalMethods rs = getOutputParameters(0); // definitely
																// not going to
																// be
		// from ?=

		String retValue = ((com.mysql.jdbc.JDBC4ResultSet) rs)
				.getNString(fixParameterName(parameterName));

		this.outputParamWasNull = rs.wasNull();

		return retValue;
	}
}
