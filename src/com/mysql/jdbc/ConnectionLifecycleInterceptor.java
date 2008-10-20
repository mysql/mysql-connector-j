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
import java.sql.Savepoint;

/**
 * Implementors of this interface can be installed via the 
 * "connectionLifecycleInterceptors" configuration property and receive
 * events and alter behavior of "lifecycle" methods on our connection
 * implementation.
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
	 * its own internal logic for commit(), or "false" if not.
	 * 
	 * @throws SQLException if an error occurs
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
	 * its own internal logic for rollback(), or "false" if not.
	 * 
	 * @throws SQLException if an error occurs
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
	 * its own internal logic for rollback(), or "false" if not.
	 * 
	 * @throws SQLException if an error occurs
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
	 * its own internal logic for setAutoCommit(), or "false" if not.
	 * 
	 * @throws SQLException if an error occurs
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
	 * its own internal logic for setCatalog(), or "false" if not.
	 * 
	 * @throws SQLException if an error occurs
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
