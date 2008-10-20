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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLClientInfoException;
import java.util.Enumeration;
import java.util.Properties;

public class JDBC4ClientInfoProviderSP implements JDBC4ClientInfoProvider {
	PreparedStatement setClientInfoSp;

	PreparedStatement getClientInfoSp;

	PreparedStatement getClientInfoBulkSp;

	public synchronized void initialize(java.sql.Connection conn,
			Properties configurationProps) throws SQLException {
		String identifierQuote = conn.getMetaData().getIdentifierQuoteString();
		String setClientInfoSpName = configurationProps.getProperty(
				"clientInfoSetSPName", "setClientInfo");
		String getClientInfoSpName = configurationProps.getProperty(
				"clientInfoGetSPName", "getClientInfo");
		String getClientInfoBulkSpName = configurationProps.getProperty(
				"clientInfoGetBulkSPName", "getClientInfoBulk");
		String clientInfoCatalog = configurationProps.getProperty(
				"clientInfoCatalog", ""); // "" means use current from
											// connection

		String catalog = "".equals(clientInfoCatalog) ? conn.getCatalog()
				: clientInfoCatalog;

		this.setClientInfoSp = ((com.mysql.jdbc.Connection) conn)
				.clientPrepareStatement("CALL " + identifierQuote + catalog
						+ identifierQuote + "." + identifierQuote
						+ setClientInfoSpName + identifierQuote + "(?, ?)");
		
		this.getClientInfoSp = ((com.mysql.jdbc.Connection) conn)
				.clientPrepareStatement("CALL" + identifierQuote + catalog
						+ identifierQuote + "." + identifierQuote
						+ getClientInfoSpName + identifierQuote + "(?)");
		
		this.getClientInfoBulkSp = ((com.mysql.jdbc.Connection) conn)
				.clientPrepareStatement("CALL " + identifierQuote + catalog
						+ identifierQuote + "." + identifierQuote
						+ getClientInfoBulkSpName + identifierQuote + "()");
	}

	public synchronized void destroy() throws SQLException {
		if (this.setClientInfoSp != null) {
			this.setClientInfoSp.close();
			this.setClientInfoSp = null;
		}

		if (this.getClientInfoSp != null) {
			this.getClientInfoSp.close();
			this.getClientInfoSp = null;
		}

		if (this.getClientInfoBulkSp != null) {
			this.getClientInfoBulkSp.close();
			this.getClientInfoBulkSp = null;
		}
	}

	public synchronized Properties getClientInfo(java.sql.Connection conn)
			throws SQLException {
		ResultSet rs = null;

		Properties props = new Properties();
		
		try {
			this.getClientInfoBulkSp.execute();

			rs = this.getClientInfoBulkSp.getResultSet();

			while (rs.next()) {
				props.setProperty(rs.getString(1), rs.getString(2));
			}
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
		
		return props;
	}

	public synchronized String getClientInfo(java.sql.Connection conn,
			String name) throws SQLException {
		ResultSet rs = null;

		String clientInfo = null;
		
		try {
			this.getClientInfoSp.setString(1, name);
			this.getClientInfoSp.execute();

			rs = this.getClientInfoSp.getResultSet();

			if (rs.next()) {
				clientInfo = rs.getString(1);
			}
		} finally {
			if (rs != null) {
				rs.close();
			}
		}
		
		return clientInfo;
	}

	public synchronized void setClientInfo(java.sql.Connection conn,
			Properties properties) throws SQLClientInfoException {
		try {
			Enumeration propNames = properties.propertyNames();

			while (propNames.hasMoreElements()) {
				String name = (String) propNames.nextElement();
				String value = properties.getProperty(name);

				setClientInfo(conn, name, value);
			}
		} catch (SQLException sqlEx) {
			SQLClientInfoException clientInfoEx = new SQLClientInfoException();
			clientInfoEx.initCause(sqlEx);

			throw clientInfoEx;
		}
	}

	public synchronized void setClientInfo(java.sql.Connection conn,
			String name, String value) throws SQLClientInfoException {
		try {
			this.setClientInfoSp.setString(1, name);
			this.setClientInfoSp.setString(2, value);
			this.setClientInfoSp.execute();
		} catch (SQLException sqlEx) {
			SQLClientInfoException clientInfoEx = new SQLClientInfoException();
			clientInfoEx.initCause(sqlEx);

			throw clientInfoEx;
		}
	}
}