/*
 * Copyright (c) 2007, 2018, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc.interceptors;

import java.sql.SQLException;
import java.sql.Savepoint;
import java.util.Properties;

import com.mysql.cj.MysqlConnection;
import com.mysql.cj.log.Log;

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
     * @return interceptor
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
     *             if an error occurs
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
     * @param s
     *            savepoint
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
     * @param flag
     *            autocommit flag
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
     * @param catalog
     *            catalog name
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
     * 
     * @return true if transaction is in progress
     */
    boolean transactionBegun();

    /**
     * Called when the driver has been told by the server that a transaction
     * has completed, and no transaction is currently in progress.
     * 
     * @return true if transaction is completed
     */
    boolean transactionCompleted();
}
