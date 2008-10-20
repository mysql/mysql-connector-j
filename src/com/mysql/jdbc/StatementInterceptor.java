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
import java.util.Properties;

/**
 * Implement this interface to be placed "in between" query execution, so that
 * you can influence it. (currently experimental).
 * 
 * StatementInterceptors are "chainable" when configured by the user, the
 * results returned by the "current" interceptor will be passed on to the next
 * on in the chain, from left-to-right order, as specified by the user in the 
 * JDBC configuration property "statementInterceptors".
 * 
 * @version $Id: $
 */

public interface StatementInterceptor extends Extension {

	/**
	 * Called once per connection that wants to use the interceptor
	 * 
	 * The properties are the same ones passed in in the URL or arguments to
	 * Driver.connect() or DriverManager.getConnection().
	 * 
	 * @param conn the connection for which this interceptor is being created
	 * @param props configuration values as passed to the connection. Note that
	 * in order to support javax.sql.DataSources, configuration properties specific
	 * to an interceptor <strong>must</strong> be passed via setURL() on the
	 * DataSource. StatementInterceptor properties are not exposed via 
	 * accessor/mutator methods on DataSources.
	 * 
	 * @throws SQLException should be thrown if the the StatementInterceptor
	 * can not initialize itself.
	 */
	
	public abstract void init(Connection conn, Properties props) throws SQLException;

	/**
	 * Called before the given statement is going to be sent to the
	 * server for processing.
	 * 
	 * Interceptors are free to return a result set (which must implement the
	 * interface com.mysql.jdbc.ResultSetInternalMethods), and if so,
	 * the server will not execute the query, and the given result set will be
	 * returned to the application instead.
	 * 
	 * This method will be called while the connection-level mutex is held, so
	 * it will only be called from one thread at a time.
	 * 
	 * @param sql the SQL representation of the statement
	 * @param interceptedStatement the actual statement instance being intercepted
	 * @param connection the connection the statement is using (passed in to make
	 * thread-safe implementations straightforward)
	 * 
	 * @return a result set that should be returned to the application instead
	 * of results that are created from actual execution of the intercepted 
	 * statement.
	 * 
	 * @throws SQLException if an error occurs during execution
	 * 
	 * @see com.mysql.jdbc.ResultSetInternalMethods
	 */

	public abstract ResultSetInternalMethods preProcess(String sql,
			Statement interceptedStatement, Connection connection)
			throws SQLException;

	/**
	 * Called after the given statement has been sent to the server
	 * for processing.
	 * 
	 * Interceptors are free to inspect the "original" result set, and if a
	 * different result set is returned by the interceptor, it is used in place
	 * of the "original" result set. (the result set returned by the interceptor
	 * must implement the interface
	 * com.mysql.jdbc.ResultSetInternalMethods).
	 * 
	 * This method will be called while the connection-level mutex is held, so
	 * it will only be called from one thread at a time.
	 * 
	 * @param sql the SQL representation of the statement
	 * @param interceptedStatement the actual statement instance being intercepted
	 * @param connection the connection the statement is using (passed in to make
	 * thread-safe implementations straightforward)
	 * 
	 * @return a result set that should be returned to the application instead
	 * of results that are created from actual execution of the intercepted 
	 * statement.
	 * 
	 * @throws SQLException if an error occurs during execution
	 * 
	 * @see com.mysql.jdbc.ResultSetInternalMethods
	 */
	public abstract ResultSetInternalMethods postProcess(String sql,
			Statement interceptedStatement,
			ResultSetInternalMethods originalResultSet,
			Connection connection) throws SQLException;
	
	/**
	 * Should the driver execute this interceptor only for the
	 * "original" top-level query, and not put it in the execution
	 * path for queries that may be executed from other interceptors?
	 * 
	 * If an interceptor issues queries using the connection it was created for,
	 * and does not return <code>true</code> for this method, it must ensure 
	 * that it does not cause infinite recursion.
	 * 
	 * @return true if the driver should ensure that this interceptor is only
	 * executed for the top-level "original" query.
	 */
	public abstract boolean executeTopLevelOnly();

	/**
	 * Called by the driver when this extension should release any resources
	 * it is holding and cleanup internally before the connection is
	 * closed.
	 */
	public abstract void destroy();
}