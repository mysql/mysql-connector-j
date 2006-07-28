/*
 Copyright (C) 2002-2004 MySQL AB

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

import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

/**
 * This class is used to obtain a physical connection and instantiate and return
 * a MysqlPooledConnection. J2EE application servers map client calls to
 * dataSource.getConnection to this class based upon mapping set within
 * deployment descriptor. This class extends MysqlDataSource.
 * 
 * @see javax.sql.PooledConnection
 * @see javax.sql.ConnectionPoolDataSource
 * @see org.gjt.mm.mysql.MysqlDataSource
 * @author Todd Wolff <todd.wolff_at_prodigy.net>
 */
public class MysqlConnectionPoolDataSource extends MysqlDataSource implements
		ConnectionPoolDataSource {
	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * Returns a pooled connection.
	 * 
	 * @exception SQLException
	 *                if an error occurs
	 * @return a PooledConnection
	 */
	public synchronized PooledConnection getPooledConnection()
			throws SQLException {
		Connection connection = getConnection();
		MysqlPooledConnection mysqlPooledConnection = new MysqlPooledConnection(
				(com.mysql.jdbc.Connection)connection);

		return mysqlPooledConnection;
	}

	/**
	 * This method is invoked by the container. Obtains physical connection
	 * using mySql.Driver class and returns a mysqlPooledConnection object.
	 * 
	 * @param s
	 *            user name
	 * @param s1
	 *            password
	 * @exception SQLException
	 *                if an error occurs
	 * @return a PooledConnection
	 */
	public synchronized PooledConnection getPooledConnection(String s, String s1)
			throws SQLException {
		Connection connection = getConnection(s, s1);
		MysqlPooledConnection mysqlPooledConnection = new MysqlPooledConnection(
				(com.mysql.jdbc.Connection)connection);

		return mysqlPooledConnection;
	}
}
