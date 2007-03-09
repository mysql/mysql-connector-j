/*
 Copyright (C) 2002-2007 MySQL AB

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

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.NClob;
import java.sql.Struct;
import java.util.Properties;


import com.mysql.jdbc.Connection;
import com.mysql.jdbc.exceptions.NotYetImplementedException;

public class JDBC4Connection extends Connection {

	JDBC4Connection(String hostToConnectTo, int portToConnectTo, Properties info, String databaseToConnectTo, String url) throws SQLException {
		super(hostToConnectTo, portToConnectTo, info, databaseToConnectTo, url);
		// TODO Auto-generated constructor stub
	}

	public SQLXML createSQLXML() throws SQLException {
		return new JDBC4MysqlSQLXML();
	}
	
	public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new NotYetImplementedException();
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new NotYetImplementedException();
	}

	public Properties getClientInfo() throws SQLException {
		throw new NotYetImplementedException();
	}

	public String getClientInfo(String name) throws SQLException {
		throw new NotYetImplementedException();
	}

	public boolean isValid(int timeout) throws SQLException {
		throw new NotYetImplementedException();
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		throw new NotYetImplementedException();
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		throw new NotYetImplementedException();
	}

	public boolean isWrapperFor(Class arg0) throws SQLException {
		throw new NotYetImplementedException();
	}

	public Object unwrap(Class arg0) throws SQLException {
		throw new NotYetImplementedException();
	}

	/**
	 * @see java.sql.Connection#createBlob()
	 */
	public Blob createBlob() {
	    return new com.mysql.jdbc.Blob();
	}

	/**
	 * @see java.sql.Connection#createClob()
	 */
	public Clob createClob() {
	    return new com.mysql.jdbc.Clob();
	}

	/**
	 * @see java.sql.Connection#createNClob()
	 */
	public NClob createNClob() {
	    return new com.mysql.jdbc.JDBC4NClob();
	}
}
