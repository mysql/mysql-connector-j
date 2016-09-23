/*
  Copyright (c) 2007, 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
  <http://www.mysql.com/about/legal/licensing/foss-exception.html>.

  This program is free software; you can redistribute it and/or modify it under the terms
  of the GNU General Public License as published by the Free Software Foundation; version 2
  of the License.

  This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
  without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
  See the GNU General Public License for more details.

  You should have received a copy of the GNU General Public License along with this
  program; if not, write to the Free Software Foundation, Inc., 51 Franklin St, Fifth
  Floor, Boston, MA 02110-1301  USA

 */

package com.mysql.cj.api.jdbc;

import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Classes that implement this interface and provide a no-args constructor can be used by the driver to store and retrieve client information and/or labels.
 * 
 * The driver will create an instance for each Connection instance, and call initialize() once and only once. When the connection is closed, destroy() will be
 * called, and the provider is expected to clean up any resources at this time.
 */
public interface ClientInfoProvider {
    /**
     * Called once by the driver when it needs to configure the provider.
     * 
     * @param conn
     *            the connection that the provider belongs too.
     * @param configurationProps
     *            a java.util.Properties instance that contains
     *            configuration information for the connection.
     * @throws SQLException
     *             if initialization fails.
     */
    void initialize(java.sql.Connection conn, Properties configurationProps) throws SQLException;

    /**
     * Called once by the driver when the connection this provider instance
     * belongs to is being closed.
     * 
     * Implementations are expected to clean up and resources at this point
     * in time.
     * 
     * @throws SQLException
     *             if an error occurs.
     */
    void destroy() throws SQLException;

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
     * @throws SQLException
     * 
     * @see java.sql.Connection#getClientInfo()
     */
    Properties getClientInfo(java.sql.Connection conn) throws SQLException;

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
     * @throws SQLException
     * 
     * @see java.sql.Connection#getClientInfo(java.lang.String)
     */
    String getClientInfo(java.sql.Connection conn, String name) throws SQLException;

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
     * @throws SQLException
     * 
     * @see java.sql.Connection#setClientInfo(java.util.Properties)
     */
    void setClientInfo(java.sql.Connection conn, Properties properties) throws SQLClientInfoException;

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
     * @throws SQLException
     * 
     * @see java.sql.Connection#setClientInfo(java.lang.String,java.lang.String)
     */
    void setClientInfo(java.sql.Connection conn, String name, String value) throws SQLClientInfoException;
}
