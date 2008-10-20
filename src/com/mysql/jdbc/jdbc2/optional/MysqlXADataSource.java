/*
 Copyright  2005 MySQL AB, 2008 Sun Microsystems

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
package com.mysql.jdbc.jdbc2.optional;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.XAConnection;

/**
 * @author mmatthew
 * 
 * To change this generated comment edit the template variable "typecomment":
 * Window>Preferences>Java>Templates. To enable and disable the creation of type
 * comments go to Window>Preferences>Java>Code Generation.
 */
public class MysqlXADataSource extends MysqlDataSource implements
		javax.sql.XADataSource {

	/**
	 * @see javax.sql.XADataSource#getXAConnection()
	 */
	public XAConnection getXAConnection() throws SQLException {

		Connection conn = getConnection();

		return wrapConnection(conn);
	}

	/**
	 * @see javax.sql.XADataSource#getXAConnection(String, String)
	 */
	public XAConnection getXAConnection(String u, String p)
			throws SQLException {

		Connection conn = getConnection(u, p);

		return wrapConnection(conn);
	}

	/**
	 * Wraps a connection as a 'fake' XAConnection
	 */

	private XAConnection wrapConnection(Connection conn) throws SQLException {
		if (getPinGlobalTxToPhysicalConnection() || 
				((com.mysql.jdbc.Connection)conn).getPinGlobalTxToPhysicalConnection()) {
			return SuspendableXAConnection.getInstance((com.mysql.jdbc.ConnectionImpl) conn);
		}
		
		return MysqlXAConnection.getInstance((com.mysql.jdbc.ConnectionImpl) conn, getLogXaCommands());
	}
}