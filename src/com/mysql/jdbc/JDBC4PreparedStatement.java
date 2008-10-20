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
import java.sql.NClob;
import java.sql.RowId;
import java.sql.SQLXML;
import java.sql.SQLException;
import java.sql.Types;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import com.mysql.jdbc.PreparedStatement.ParseInfo;


public class JDBC4PreparedStatement extends PreparedStatement {

	public JDBC4PreparedStatement(ConnectionImpl conn, String catalog) throws SQLException {
		super(conn, catalog);
	}
	
	public JDBC4PreparedStatement(ConnectionImpl conn, String sql, String catalog)
		throws SQLException {
		super(conn, sql, catalog);
	}
	
	public JDBC4PreparedStatement(ConnectionImpl conn, String sql, String catalog,
			ParseInfo cachedParseInfo) throws SQLException {
		super(conn, sql, catalog, cachedParseInfo);
	}

	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		JDBC4PreparedStatementHelper.setRowId(this, parameterIndex, x);
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

	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		JDBC4PreparedStatementHelper.setSQLXML(this, parameterIndex, xmlObject);
	}
}
