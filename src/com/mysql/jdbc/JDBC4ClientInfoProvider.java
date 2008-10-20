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

import java.sql.SQLException;
import java.sql.SQLClientInfoException;
import java.util.Properties;



/**
 * Classes that implement this interface and provide a no-args constructor
 * can be used by the driver to store and retrieve client information and/or
 * labels.
 * 
 * The driver will create an instance for each Connection instance, and call
 * initialize() once and only once. When the connection is closed, destroy()
 * will be called, and the provider is expected to clean up any resources at
 * this time.
 *
 * @version $Id: $
 */
public interface JDBC4ClientInfoProvider {
	/**
	 * Called once by the driver when it needs to configure the provider.
	 * 
	 * @param conn the connection that the provider belongs too.
	 * @param configurationProps a java.util.Properties instance that contains
	 * configuration information for the connection. 
	 * @throws SQLException if initialization fails.
	 */
	public void initialize(java.sql.Connection conn, Properties configurationProps) throws SQLException;
	
	/**
	 * Called once by the driver when the connection this provider instance
	 * belongs to is being closed.
	 * 
	 * Implementations are expected to clean up and resources at this point
	 * in time.
	 * 
	 * @throws SQLException if an error occurs.
	 */
	public void destroy() throws SQLException;
	
	/**
	 * Returns the client info for the connection that this provider
	 * instance belongs to. The connection instance is passed as an argument
	 * for convenience's sake.
	 * 
	 * Providers can use the connection to communicate with the database,
	 * but it will be within the scope of any ongoing transactions, so therefore
	 * implementations should not attempt to change isolation level, autocommit settings
	 * or call rollback() or commit() on the connection.
	 * 
	 * @param conn
	 * @return 
	 * @throws SQLException
	 * 
	 * @see java.sql.Connection#getClientInfo()
	 */
	public Properties getClientInfo(java.sql.Connection conn) throws SQLException;

	/**
	 * Returns the client info for the connection that this provider
	 * instance belongs to. The connection instance is passed as an argument
	 * for convenience's sake.
	 * 
	 * Providers can use the connection to communicate with the database,
	 * but it will be within the scope of any ongoing transactions, so therefore
	 * implementations should not attempt to change isolation level, autocommit settings
	 * or call rollback() or commit() on the connection.
	 * 
	 * @param conn
	 * @return 
	 * @throws SQLException
	 * 
	 * @see java.sql.Connection#getClientInfo(java.lang.String)
	 */
	public String getClientInfo(java.sql.Connection conn, String name) throws SQLException;
	
	/**
	 * Sets the client info for the connection that this provider
	 * instance belongs to. The connection instance is passed as an argument
	 * for convenience's sake.
	 * 
	 * Providers can use the connection to communicate with the database,
	 * but it will be within the scope of any ongoing transactions, so therefore
	 * implementations should not attempt to change isolation level, autocommit settings
	 * or call rollback() or commit() on the connection.
	 * 
	 * @param conn
	 * @return 
	 * @throws SQLException
	 * 
	 * @see java.sql.Connection#setClientInfo(java.util.Properties)
	 */
	public void setClientInfo(java.sql.Connection conn, Properties properties) throws SQLClientInfoException;

	/**
	 * Sets the client info for the connection that this provider
	 * instance belongs to. The connection instance is passed as an argument
	 * for convenience's sake.
	 * 
	 * Providers can use the connection to communicate with the database,
	 * but it will be within the scope of any ongoing transactions, so therefore
	 * implementations should not attempt to change isolation level, autocommit settings
	 * or call rollback() or commit() on the connection.
	 * 
	 * @param conn
	 * @return 
	 * @throws SQLException
	 * 
	 * @see java.sql.Connection#setClientInfo(java.lang.String,java.lang.String)
	 */
	public void setClientInfo(java.sql.Connection conn, String name, String value) throws SQLClientInfoException;
}
