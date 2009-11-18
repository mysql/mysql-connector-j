/*
 Copyright 2009 Sun Microsystems, Inc. All rights reserved.
 U.S. Government Rights - Commercial software. Government users are subject
 to the Sun Microsystems, Inc. standard license agreement and applicable
 provisions of the FAR and its supplements. Use is subject to license terms.
 This distribution may include materials developed by third parties.Sun,
 Sun Microsystems, the Sun logo and MySQL Enterprise Monitor are
 trademarks or registered trademarks of Sun Microsystems, Inc. in the U.S.
 and other countries.

 Copyright 2009 Sun Microsystems, Inc. Tous droits réservés.
 L'utilisation est soumise aux termes du contrat de licence.Cette
 distribution peut comprendre des composants développés par des tierces
 parties.Sun, Sun Microsystems,  le logo Sun et  MySQL Enterprise Monitor sont
 des marques de fabrique ou des marques déposées de Sun Microsystems, Inc.
 aux Etats-Unis et dans du'autres pays.

 */

package com.mysql.jdbc;

import java.sql.SQLException;
import java.util.Properties;

public interface StatementInterceptorV2 extends Extension {

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
	
	/**
	 * Called after the given statement has been sent to the server
	 * for processing, instead of the StatementAware postProcess() of the earlier 
	 * api.
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
			Connection connection, int warningCount, boolean noIndexUsed, boolean noGoodIndexUsed, 
			SQLException statementException) throws SQLException;
}
