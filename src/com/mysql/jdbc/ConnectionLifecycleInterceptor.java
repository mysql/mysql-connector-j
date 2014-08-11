/*
  Copyright (c) 2007, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.jdbc;

import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * Implementors of this interface can be installed via the "connectionLifecycleInterceptors" configuration property and receive events and alter behavior of
 * "lifecycle" methods on our connection implementation.
 * 
 * The driver will create one instance of a given interceptor per-connection.
 */
public interface ConnectionLifecycleInterceptor extends Extension {
    /**
     * Called when an application calls Connection.close(), before the driver
     * processes its own internal logic for close.
     * 
     * @throws SQLException
     */
    public abstract void close() throws SQLException;

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
    public abstract boolean commit() throws SQLException;

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
    public abstract boolean rollback() throws SQLException;

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
    public abstract boolean rollback(Savepoint s) throws SQLException;

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
    public abstract boolean setAutoCommit(boolean flag) throws SQLException;

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
    public abstract boolean setCatalog(String catalog) throws SQLException;

    /**
     * Called when the driver has been told by the server that a transaction
     * is now in progress (when one has not been currently in progress).
     */
    public abstract boolean transactionBegun() throws SQLException;

    /**
     * Called when the driver has been told by the server that a transaction
     * has completed, and no transaction is currently in progress.
     */
    public abstract boolean transactionCompleted() throws SQLException;
}
