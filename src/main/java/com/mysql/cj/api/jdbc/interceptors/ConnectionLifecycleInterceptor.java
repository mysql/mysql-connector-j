/*
  Copyright (c) 2007, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.jdbc.interceptors;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Properties;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.log.Log;

/**
 * Implementors of this interface can be installed via the "connectionLifecycleInterceptors" configuration property and receive events and alter behavior of
 * "lifecycle" methods on our connection implementation.
 * 
 * The driver will create one instance of a given interceptor per-connection.
 */
public interface ConnectionLifecycleInterceptor {
    /**
     * Called once per connection that wants to use the extension
     * 
     * The properties are the same ones passed in in the URL or arguments to
     * Driver.connect() or DriverManager.getConnection().
     * 
     * @param conn
     *            the connection for which this extension is being created
     * @param props
     *            configuration values as passed to the connection. Note that
     *            in order to support javax.sql.DataSources, configuration properties specific
     *            to an interceptor <strong>must</strong> be passed via setURL() on the
     *            DataSource. Extension properties are not exposed via
     *            accessor/mutator methods on DataSources.
     * @param log
     *            logger instance
     */

    ConnectionLifecycleInterceptor init(MysqlConnection conn, Properties props, Log log);

    /**
     * Called by the driver when this extension should release any resources
     * it is holding and cleanup internally before the connection is
     * closed.
     */
    void destroy();

    /**
     * Called when an application calls Connection.close(), before the driver
     * processes its own internal logic for close.
     * 
     * @throws SQLException
     */
    void close() throws SQLException;

    /**
     * Called when an application calls Connection.commit(), before the
     * driver processes its own internal logic for commit().
     * 
     * Interceptors should return "true" if the driver should perform
     * its own internal logic for commit(), or "false" if not.
     * 
     * @return "true" if the driver should perform
     *         its own internal logic for commit(), or "false" if not.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    boolean commit() throws SQLException;

    /**
     * Called when an application calls Connection.rollback(), before the
     * driver processes its own internal logic for rollback().
     * 
     * Interceptors should return "true" if the driver should perform
     * its own internal logic for rollback(), or "false" if not.
     * 
     * @return "true" if the driver should perform
     *         its own internal logic for rollback(), or "false" if not.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    boolean rollback() throws SQLException;

    /**
     * Called when an application calls Connection.rollback(), before the
     * driver processes its own internal logic for rollback().
     * 
     * Interceptors should return "true" if the driver should perform
     * its own internal logic for rollback(), or "false" if not.
     * 
     * @return "true" if the driver should perform
     *         its own internal logic for rollback(), or "false" if not.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    boolean rollback(Savepoint s) throws SQLException;

    /**
     * Called when an application calls Connection.setAutoCommit(), before the
     * driver processes its own internal logic for setAutoCommit().
     * 
     * Interceptors should return "true" if the driver should perform
     * its own internal logic for setAutoCommit(), or "false" if not.
     * 
     * @return "true" if the driver should perform
     *         its own internal logic for setAutoCommit(), or "false" if not.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    boolean setAutoCommit(boolean flag) throws SQLException;

    /**
     * Called when an application calls Connection.setCatalog(), before the
     * driver processes its own internal logic for setCatalog().
     * 
     * Interceptors should return "true" if the driver should perform
     * its own internal logic for setCatalog(), or "false" if not.
     * 
     * @return "true" if the driver should perform
     *         its own internal logic for setCatalog(), or "false" if not.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    boolean setCatalog(String catalog) throws SQLException;

    /**
     * Called when the driver has been told by the server that a transaction
     * is now in progress (when one has not been currently in progress).
     */
    boolean transactionBegun() throws SQLException;

    /**
     * Called when the driver has been told by the server that a transaction
     * has completed, and no transaction is currently in progress.
     */
    boolean transactionCompleted() throws SQLException;
}
