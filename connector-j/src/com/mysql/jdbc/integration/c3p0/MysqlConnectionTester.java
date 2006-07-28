/*
 Copyright (C) 2002-2005 MySQL AB

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
package com.mysql.jdbc.integration.c3p0;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import com.mchange.v2.c3p0.C3P0ProxyConnection;
import com.mchange.v2.c3p0.QueryConnectionTester;
import com.mysql.jdbc.CommunicationsException;

/**
 * ConnectionTester for C3P0 connection pool that uses the more efficient
 * COM_PING method of testing connection 'liveness' for MySQL, and 'sorts'
 * exceptions based on SQLState or class of 'CommunicationsException' for
 * handling exceptions.
 * 
 * @version $Id: MysqlConnectionTester.java,v 1.1.2.1 2005/05/13 18:58:39
 *          mmatthews Exp $
 */
public final class MysqlConnectionTester implements QueryConnectionTester {

	private static final long serialVersionUID = 3256444690067896368L;

	private static final Object[] NO_ARGS_ARRAY = new Object[0];

	private Method pingMethod;

	public MysqlConnectionTester() {
		try {
			pingMethod = com.mysql.jdbc.Connection.class
					.getMethod("ping", null);
		} catch (Exception ex) {
			// punt, we have no way to recover, other than we now use 'SELECT 1'
			// for
			// handling the connection testing.
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mchange.v2.c3p0.ConnectionTester#activeCheckConnection(java.sql.Connection)
	 */
	public int activeCheckConnection(Connection con) {
		C3P0ProxyConnection castCon = (C3P0ProxyConnection) con;

		try {
			if (pingMethod != null) {
				castCon.rawConnectionOperation(pingMethod,
						C3P0ProxyConnection.RAW_CONNECTION, NO_ARGS_ARRAY);
			} else {
				Statement pingStatement = null;

				try {
					pingStatement = con.createStatement();
					pingStatement.executeQuery("SELECT 1").close();
				} finally {
					if (pingStatement != null) {
						pingStatement.close();
					}
				}
			}

			return CONNECTION_IS_OKAY;
		} catch (Exception ex) {
			return CONNECTION_IS_INVALID;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mchange.v2.c3p0.ConnectionTester#statusOnException(java.sql.Connection,
	 *      java.lang.Throwable)
	 */
	public int statusOnException(Connection arg0, Throwable throwable) {
		if (throwable instanceof CommunicationsException) {
			return CONNECTION_IS_INVALID;
		}

		if (throwable instanceof SQLException) {
			String sqlState = ((SQLException) throwable).getSQLState();

			if (sqlState != null && sqlState.startsWith("08")) {
				return CONNECTION_IS_INVALID;
			}

			return CONNECTION_IS_OKAY;
		}

		// Runtime/Unchecked?

		return CONNECTION_IS_INVALID;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.mchange.v2.c3p0.QueryConnectionTester#activeCheckConnection(java.sql.Connection,
	 *      java.lang.String)
	 */
	public int activeCheckConnection(Connection arg0, String arg1) {
		return CONNECTION_IS_OKAY;
	}
}
