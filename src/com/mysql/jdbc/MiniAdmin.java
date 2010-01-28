/*
 Copyright  2002-2004 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA



 */
package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Utility functions for admin functionality from Java.
 * 
 * @author Mark Matthews
 */
public class MiniAdmin {
	// ~ Instance fields
	// --------------------------------------------------------

	private Connection conn;

	// ~ Constructors
	// -----------------------------------------------------------

	/**
	 * Create a new MiniAdmin using the given connection
	 * 
	 * @param conn
	 *            the existing connection to use.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	public MiniAdmin(java.sql.Connection conn) throws SQLException {
		if (conn == null) {
			throw SQLError.createSQLException(
					Messages.getString("MiniAdmin.0"), SQLError.SQL_STATE_GENERAL_ERROR, ((com.mysql.jdbc.ConnectionImpl)conn).getExceptionInterceptor()); //$NON-NLS-1$
		}

		if (!(conn instanceof Connection)) {
			throw SQLError.createSQLException(Messages.getString("MiniAdmin.1"), //$NON-NLS-1$
					SQLError.SQL_STATE_GENERAL_ERROR, ((com.mysql.jdbc.ConnectionImpl)conn).getExceptionInterceptor());
		}

		this.conn = (Connection) conn;
	}

	/**
	 * Create a new MiniAdmin, connecting using the given JDBC URL.
	 * 
	 * @param jdbcUrl
	 *            the JDBC URL to use
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	public MiniAdmin(String jdbcUrl) throws SQLException {
		this(jdbcUrl, new Properties());
	}

	/**
	 * Create a new MiniAdmin, connecting using the given JDBC URL and
	 * properties
	 * 
	 * @param jdbcUrl
	 *            the JDBC URL to use
	 * @param props
	 *            the properties to use when connecting
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	public MiniAdmin(String jdbcUrl, Properties props) throws SQLException {
		this.conn = (Connection) (new Driver().connect(jdbcUrl, props));
	}

	// ~ Methods
	// ----------------------------------------------------------------

	/**
	 * Shuts down the MySQL server at the other end of the connection that this
	 * MiniAdmin was created from/for.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	public void shutdown() throws SQLException {
		this.conn.shutdownServer();
	}
}
