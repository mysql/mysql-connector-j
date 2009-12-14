/*
 Copyright  2002-2007 MySQL AB, 2008-2009 Sun Microsystems

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

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ExceptionInterceptor;
import com.mysql.jdbc.Extension;
import com.mysql.jdbc.MysqlErrorNumbers;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.Util;
import com.mysql.jdbc.log.Log;

/**
 * This class serves as a wrapper for the org.gjt.mm.mysql.jdbc2.Connection
 * class. It is returned to the application server which may wrap it again and
 * then return it to the application client in response to
 * dataSource.getConnection().
 * 
 * <p>
 * All method invocations are forwarded to org.gjt.mm.mysql.jdbc2.Connection
 * unless the close method was previously called, in which case a sqlException
 * is thrown. The close method performs a 'logical close' on the connection.
 * </p>
 * 
 * <p>
 * All sqlExceptions thrown by the physical connection are intercepted and sent
 * to connectionEvent listeners before being thrown to client.
 * </p>
 * 
 * @author Todd Wolff todd.wolff_at_prodigy.net
 * 
 * @see org.gjt.mm.mysql.jdbc2.Connection
 * @see org.gjt.mm.mysql.jdbc2.optional.MysqlPooledConnection
 */
public class ConnectionWrapper extends WrapperBase implements Connection {
	protected Connection mc = null;

	private String invalidHandleStr = "Logical handle no longer valid";

	private boolean closed;

	private boolean isForXa;

	private static final Constructor JDBC_4_CONNECTION_WRAPPER_CTOR;

	static {
		if (Util.isJdbc4()) {
			try {
				JDBC_4_CONNECTION_WRAPPER_CTOR = Class.forName(
						"com.mysql.jdbc.jdbc2.optional.JDBC4ConnectionWrapper")
						.getConstructor(
								new Class[] { MysqlPooledConnection.class,
										Connection.class, Boolean.TYPE });
			} catch (SecurityException e) {
				throw new RuntimeException(e);
			} catch (NoSuchMethodException e) {
				throw new RuntimeException(e);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else {
			JDBC_4_CONNECTION_WRAPPER_CTOR = null;
		}
	}

	protected static ConnectionWrapper getInstance(
			MysqlPooledConnection mysqlPooledConnection,
			Connection mysqlConnection, boolean forXa) throws SQLException {
		if (!Util.isJdbc4()) {
			return new ConnectionWrapper(mysqlPooledConnection,
					mysqlConnection, forXa);
		}

		return (ConnectionWrapper) Util.handleNewInstance(
				JDBC_4_CONNECTION_WRAPPER_CTOR, new Object[] {
						mysqlPooledConnection, mysqlConnection,
						Boolean.valueOf(forXa) }, mysqlPooledConnection.getExceptionInterceptor());
	}

	/**
	 * Construct a new LogicalHandle and set instance variables
	 * 
	 * @param mysqlPooledConnection
	 *            reference to object that instantiated this object
	 * @param mysqlConnection
	 *            physical connection to db
	 * 
	 * @throws SQLException
	 *             if an error occurs.
	 */
	public ConnectionWrapper(MysqlPooledConnection mysqlPooledConnection,
			Connection mysqlConnection, boolean forXa) throws SQLException {
		super(mysqlPooledConnection);
		
		this.mc = mysqlConnection;
		this.closed = false;
		this.isForXa = forXa;

		if (this.isForXa) {
			setInGlobalTx(false);
		}
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#setAutoCommit
	 */
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		checkClosed();

		if (autoCommit && isInGlobalTx()) {
			throw SQLError.createSQLException(
					"Can't set autocommit to 'true' on an XAConnection",
					SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION,
					MysqlErrorNumbers.ER_XA_RMERR, this.exceptionInterceptor);
		}

		try {
			this.mc.setAutoCommit(autoCommit);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#getAutoCommit()
	 */
	public boolean getAutoCommit() throws SQLException {
		checkClosed();

		try {
			return this.mc.getAutoCommit();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return false; // we don't reach this code, compiler can't tell
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#setCatalog()
	 */
	public void setCatalog(String catalog) throws SQLException {
		checkClosed();

		try {
			this.mc.setCatalog(catalog);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @return the current catalog
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	public String getCatalog() throws SQLException {
		checkClosed();

		try {
			return this.mc.getCatalog();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#isClosed()
	 */
	public boolean isClosed() throws SQLException {
		return (this.closed || this.mc.isClosed());
	}

	public boolean isMasterConnection() {
		return this.mc.isMasterConnection();
	}

	/**
	 * @see Connection#setHoldability(int)
	 */
	public void setHoldability(int arg0) throws SQLException {
		checkClosed();

		try {
			this.mc.setHoldability(arg0);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	/**
	 * @see Connection#getHoldability()
	 */
	public int getHoldability() throws SQLException {
		checkClosed();

		try {
			return this.mc.getHoldability();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return Statement.CLOSE_CURRENT_RESULT; // we don't reach this code,
		// compiler can't tell
	}

	/**
	 * Allows clients to determine how long this connection has been idle.
	 * 
	 * @return how long the connection has been idle.
	 */
	public long getIdleFor() {
		return this.mc.getIdleFor();
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @return a metadata instance
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	public java.sql.DatabaseMetaData getMetaData() throws SQLException {
		checkClosed();

		try {
			return this.mc.getMetaData();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#setReadOnly()
	 */
	public void setReadOnly(boolean readOnly) throws SQLException {
		checkClosed();

		try {
			this.mc.setReadOnly(readOnly);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#isReadOnly()
	 */
	public boolean isReadOnly() throws SQLException {
		checkClosed();

		try {
			return this.mc.isReadOnly();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return false; // we don't reach this code, compiler can't tell
	}

	/**
	 * @see Connection#setSavepoint()
	 */
	public java.sql.Savepoint setSavepoint() throws SQLException {
		checkClosed();

		if (isInGlobalTx()) {
			throw SQLError.createSQLException(
					"Can't set autocommit to 'true' on an XAConnection",
					SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION,
					MysqlErrorNumbers.ER_XA_RMERR, this.exceptionInterceptor);
		}

		try {
			return this.mc.setSavepoint();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * @see Connection#setSavepoint(String)
	 */
	public java.sql.Savepoint setSavepoint(String arg0) throws SQLException {
		checkClosed();

		if (isInGlobalTx()) {
			throw SQLError.createSQLException(
					"Can't set autocommit to 'true' on an XAConnection",
					SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION,
					MysqlErrorNumbers.ER_XA_RMERR, this.exceptionInterceptor);
		}

		try {
			return this.mc.setSavepoint(arg0);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#setTransactionIsolation()
	 */
	public void setTransactionIsolation(int level) throws SQLException {
		checkClosed();

		try {
			this.mc.setTransactionIsolation(level);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#getTransactionIsolation()
	 */
	public int getTransactionIsolation() throws SQLException {
		checkClosed();

		try {
			return this.mc.getTransactionIsolation();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return TRANSACTION_REPEATABLE_READ; // we don't reach this code,
		// compiler can't tell
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#setTypeMap()
	 */
	public void setTypeMap(java.util.Map map) throws SQLException {
		checkClosed();

		try {
			this.mc.setTypeMap(map);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#getTypeMap()
	 */
	public java.util.Map getTypeMap() throws SQLException {
		checkClosed();

		try {
			return this.mc.getTypeMap();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#getWarnings
	 */
	public java.sql.SQLWarning getWarnings() throws SQLException {
		checkClosed();

		try {
			return this.mc.getWarnings();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	public void clearWarnings() throws SQLException {
		checkClosed();

		try {
			this.mc.clearWarnings();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	/**
	 * The physical connection is not actually closed. the physical connection
	 * is closed when the application server calls
	 * mysqlPooledConnection.close(). this object is de-referenced by the pooled
	 * connection each time mysqlPooledConnection.getConnection() is called by
	 * app server.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	public void close() throws SQLException {
		close(true);
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @throws SQLException
	 *             if an error occurs
	 */
	public void commit() throws SQLException {
		checkClosed();

		if (isInGlobalTx()) {
			throw SQLError
					.createSQLException(
							"Can't call commit() on an XAConnection associated with a global transaction",
							SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION,
							MysqlErrorNumbers.ER_XA_RMERR, this.exceptionInterceptor);
		}

		try {
			this.mc.commit();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#createStatement()
	 */
	public java.sql.Statement createStatement() throws SQLException {
		checkClosed();

		try {
			return StatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.createStatement());
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#createStatement()
	 */
	public java.sql.Statement createStatement(int resultSetType,
			int resultSetConcurrency) throws SQLException {
		checkClosed();

		try {
			return StatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.createStatement(resultSetType, resultSetConcurrency));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * @see Connection#createStatement(int, int, int)
	 */
	public java.sql.Statement createStatement(int arg0, int arg1, int arg2)
			throws SQLException {
		checkClosed();

		try {
			return StatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.createStatement(arg0, arg1, arg2));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#nativeSQL()
	 */
	public String nativeSQL(String sql) throws SQLException {
		checkClosed();

		try {
			return this.mc.nativeSQL(sql);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#prepareCall()
	 */
	public java.sql.CallableStatement prepareCall(String sql)
			throws SQLException {
		checkClosed();

		try {
			return CallableStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.prepareCall(sql));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#prepareCall()
	 */
	public java.sql.CallableStatement prepareCall(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		checkClosed();

		try {
			return CallableStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.prepareCall(sql, resultSetType, resultSetConcurrency));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * @see Connection#prepareCall(String, int, int, int)
	 */
	public java.sql.CallableStatement prepareCall(String arg0, int arg1,
			int arg2, int arg3) throws SQLException {
		checkClosed();

		try {
			return CallableStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.prepareCall(arg0, arg1, arg2, arg3));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	public java.sql.PreparedStatement clientPrepare(String sql)
			throws SQLException {
		checkClosed();

		try {
			return new PreparedStatementWrapper(this, this.pooledConnection, this.mc
					.clientPrepareStatement(sql));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public java.sql.PreparedStatement clientPrepare(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		checkClosed();

		try {
			return new PreparedStatementWrapper(this, this.pooledConnection, this.mc
					.clientPrepareStatement(sql, resultSetType,
							resultSetConcurrency));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#prepareStatement()
	 */
	public java.sql.PreparedStatement prepareStatement(String sql)
			throws SQLException {
		checkClosed();

		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.prepareStatement(sql));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#prepareStatement()
	 */
	public java.sql.PreparedStatement prepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		checkClosed();

		try {
			return PreparedStatementWrapper
					.getInstance(this, this.pooledConnection, this.mc.prepareStatement(sql,
							resultSetType, resultSetConcurrency));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * @see Connection#prepareStatement(String, int, int, int)
	 */
	public java.sql.PreparedStatement prepareStatement(String arg0, int arg1,
			int arg2, int arg3) throws SQLException {
		checkClosed();

		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.prepareStatement(arg0, arg1, arg2, arg3));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * @see Connection#prepareStatement(String, int)
	 */
	public java.sql.PreparedStatement prepareStatement(String arg0, int arg1)
			throws SQLException {
		checkClosed();

		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.prepareStatement(arg0, arg1));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * @see Connection#prepareStatement(String, int[])
	 */
	public java.sql.PreparedStatement prepareStatement(String arg0, int[] arg1)
			throws SQLException {
		checkClosed();

		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.prepareStatement(arg0, arg1));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * @see Connection#prepareStatement(String, String[])
	 */
	public java.sql.PreparedStatement prepareStatement(String arg0,
			String[] arg1) throws SQLException {
		checkClosed();

		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.prepareStatement(arg0, arg1));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	/**
	 * @see Connection#releaseSavepoint(Savepoint)
	 */
	public void releaseSavepoint(Savepoint arg0) throws SQLException {
		checkClosed();

		try {
			this.mc.releaseSavepoint(arg0);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	/**
	 * Passes call to method on physical connection instance. Notifies listeners
	 * of any caught exceptions before re-throwing to client.
	 * 
	 * @see java.sql.Connection#rollback()
	 */
	public void rollback() throws SQLException {
		checkClosed();

		if (isInGlobalTx()) {
			throw SQLError
					.createSQLException(
							"Can't call rollback() on an XAConnection associated with a global transaction",
							SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION,
							MysqlErrorNumbers.ER_XA_RMERR, this.exceptionInterceptor);
		}

		try {
			this.mc.rollback();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	/**
	 * @see Connection#rollback(Savepoint)
	 */
	public void rollback(Savepoint arg0) throws SQLException {
		checkClosed();

		if (isInGlobalTx()) {
			throw SQLError
					.createSQLException(
							"Can't call rollback() on an XAConnection associated with a global transaction",
							SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION,
							MysqlErrorNumbers.ER_XA_RMERR, this.exceptionInterceptor);
		}

		try {
			this.mc.rollback(arg0);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	public boolean isSameResource(Connection c) {
		if (c instanceof ConnectionWrapper) {
			return this.mc.isSameResource(((ConnectionWrapper) c).mc);
		} else if (c instanceof com.mysql.jdbc.Connection) {
			return this.mc.isSameResource((com.mysql.jdbc.Connection) c);
		}

		return false;
	}

	protected void close(boolean fireClosedEvent) throws SQLException {
		synchronized (this.pooledConnection) {
			if (this.closed) {
				return;
			}

			if (!isInGlobalTx() && this.mc.getRollbackOnPooledClose()
					&& !this.getAutoCommit()) {
				rollback();
			}

			if (fireClosedEvent) {
				this.pooledConnection.callConnectionEventListeners(
						MysqlPooledConnection.CONNECTION_CLOSED_EVENT, null);
			}

			// set closed status to true so that if application client tries to
			// make additional
			// calls a sqlException will be thrown. The physical connection is
			// re-used by the pooled connection each time getConnection is
			// called.
			this.closed = true;
		}
	}

	protected void checkClosed() throws SQLException {
		if (this.closed) {
			throw SQLError.createSQLException(this.invalidHandleStr, this.exceptionInterceptor);
		}
	}

	public boolean isInGlobalTx() {
		return this.mc.isInGlobalTx();
	}

	public void setInGlobalTx(boolean flag) {
		this.mc.setInGlobalTx(flag);
	}

	public void ping() throws SQLException {
		if (this.mc != null) {
			this.mc.ping();
		}
	}

	public void changeUser(String userName, String newPassword)
			throws SQLException {
		checkClosed();

		try {
			this.mc.changeUser(userName, newPassword);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	public void clearHasTriedMaster() {
		this.mc.clearHasTriedMaster();
	}

	public java.sql.PreparedStatement clientPrepareStatement(String sql)
			throws SQLException {
		checkClosed();

		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.clientPrepareStatement(sql));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public java.sql.PreparedStatement clientPrepareStatement(String sql,
			int autoGenKeyIndex) throws SQLException {
		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.clientPrepareStatement(sql, autoGenKeyIndex));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public java.sql.PreparedStatement clientPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.clientPrepareStatement(sql, resultSetType,
							resultSetConcurrency));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public java.sql.PreparedStatement clientPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.clientPrepareStatement(sql, resultSetType,
							resultSetConcurrency, resultSetHoldability));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public java.sql.PreparedStatement clientPrepareStatement(String sql,
			int[] autoGenKeyIndexes) throws SQLException {
		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.clientPrepareStatement(sql, autoGenKeyIndexes));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public java.sql.PreparedStatement clientPrepareStatement(String sql,
			String[] autoGenKeyColNames) throws SQLException {
		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.clientPrepareStatement(sql, autoGenKeyColNames));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public int getActiveStatementCount() {
		return this.mc.getActiveStatementCount();
	}

	public Log getLog() throws SQLException {
		return this.mc.getLog();
	}

	public String getServerCharacterEncoding() {
		return this.mc.getServerCharacterEncoding();
	}

	public TimeZone getServerTimezoneTZ() {
		return this.mc.getServerTimezoneTZ();
	}

	public String getStatementComment() {
		return this.mc.getStatementComment();
	}

	public boolean hasTriedMaster() {
		return this.mc.hasTriedMaster();
	}

	public boolean isAbonormallyLongQuery(long millisOrNanos) {
		return this.mc.isAbonormallyLongQuery(millisOrNanos);
	}

	public boolean isNoBackslashEscapesSet() {
		return this.mc.isNoBackslashEscapesSet();
	}

	public boolean lowerCaseTableNames() {
		return this.mc.lowerCaseTableNames();
	}

	public boolean parserKnowsUnicode() {
		return this.mc.parserKnowsUnicode();
	}

	public void reportQueryTime(long millisOrNanos) {
		this.mc.reportQueryTime(millisOrNanos);
	}

	public void resetServerState() throws SQLException {
		checkClosed();

		try {
			this.mc.resetServerState();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	public java.sql.PreparedStatement serverPrepareStatement(String sql)
			throws SQLException {
		checkClosed();

		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.serverPrepareStatement(sql));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public java.sql.PreparedStatement serverPrepareStatement(String sql,
			int autoGenKeyIndex) throws SQLException {
		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.serverPrepareStatement(sql, autoGenKeyIndex));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public java.sql.PreparedStatement serverPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.serverPrepareStatement(sql, resultSetType,
							resultSetConcurrency));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public java.sql.PreparedStatement serverPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.serverPrepareStatement(sql, resultSetType,
							resultSetConcurrency, resultSetHoldability));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public java.sql.PreparedStatement serverPrepareStatement(String sql,
			int[] autoGenKeyIndexes) throws SQLException {
		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.serverPrepareStatement(sql, autoGenKeyIndexes));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public java.sql.PreparedStatement serverPrepareStatement(String sql,
			String[] autoGenKeyColNames) throws SQLException {
		try {
			return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc
					.serverPrepareStatement(sql, autoGenKeyColNames));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public void setFailedOver(boolean flag) {
		this.mc.setFailedOver(flag);

	}

	public void setPreferSlaveDuringFailover(boolean flag) {
		this.mc.setPreferSlaveDuringFailover(flag);
	}

	public void setStatementComment(String comment) {
		this.mc.setStatementComment(comment);

	}

	public void shutdownServer() throws SQLException {
		checkClosed();

		try {
			this.mc.shutdownServer();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

	}

	public boolean supportsIsolationLevel() {
		return this.mc.supportsIsolationLevel();
	}

	public boolean supportsQuotedIdentifiers() {
		return this.mc.supportsQuotedIdentifiers();
	}

	public boolean supportsTransactions() {
		return this.mc.supportsTransactions();
	}

	public boolean versionMeetsMinimum(int major, int minor, int subminor)
			throws SQLException {
		checkClosed();

		try {
			return this.mc.versionMeetsMinimum(major, minor, subminor);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return false;
	}

	public String exposeAsXml() throws SQLException {
		checkClosed();

		try {
			return this.mc.exposeAsXml();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null;
	}

	public boolean getAllowLoadLocalInfile() {
		return this.mc.getAllowLoadLocalInfile();
	}

	public boolean getAllowMultiQueries() {
		return this.mc.getAllowMultiQueries();
	}

	public boolean getAllowNanAndInf() {
		return this.mc.getAllowNanAndInf();
	}

	public boolean getAllowUrlInLocalInfile() {
		return this.mc.getAllowUrlInLocalInfile();
	}

	public boolean getAlwaysSendSetIsolation() {
		return this.mc.getAlwaysSendSetIsolation();
	}

	public boolean getAutoClosePStmtStreams() {
		return this.mc.getAutoClosePStmtStreams();
	}

	public boolean getAutoDeserialize() {
		return this.mc.getAutoDeserialize();
	}

	public boolean getAutoGenerateTestcaseScript() {
		return this.mc.getAutoGenerateTestcaseScript();
	}

	public boolean getAutoReconnectForPools() {
		return this.mc.getAutoReconnectForPools();
	}

	public boolean getAutoSlowLog() {
		return this.mc.getAutoSlowLog();
	}

	public int getBlobSendChunkSize() {
		return this.mc.getBlobSendChunkSize();
	}

	public boolean getBlobsAreStrings() {
		return this.mc.getBlobsAreStrings();
	}

	public boolean getCacheCallableStatements() {
		return this.mc.getCacheCallableStatements();
	}

	public boolean getCacheCallableStmts() {
		return this.mc.getCacheCallableStmts();
	}

	public boolean getCachePrepStmts() {
		return this.mc.getCachePrepStmts();
	}

	public boolean getCachePreparedStatements() {
		return this.mc.getCachePreparedStatements();
	}

	public boolean getCacheResultSetMetadata() {
		return this.mc.getCacheResultSetMetadata();
	}

	public boolean getCacheServerConfiguration() {
		return this.mc.getCacheServerConfiguration();
	}

	public int getCallableStatementCacheSize() {
		return this.mc.getCallableStatementCacheSize();
	}

	public int getCallableStmtCacheSize() {
		return this.mc.getCallableStmtCacheSize();
	}

	public boolean getCapitalizeTypeNames() {
		return this.mc.getCapitalizeTypeNames();
	}

	public String getCharacterSetResults() {
		return this.mc.getCharacterSetResults();
	}

	public String getClientCertificateKeyStorePassword() {
		return this.mc.getClientCertificateKeyStorePassword();
	}

	public String getClientCertificateKeyStoreType() {
		return this.mc.getClientCertificateKeyStoreType();
	}

	public String getClientCertificateKeyStoreUrl() {
		return this.mc.getClientCertificateKeyStoreUrl();
	}

	public String getClientInfoProvider() {
		return this.mc.getClientInfoProvider();
	}

	public String getClobCharacterEncoding() {
		return this.mc.getClobCharacterEncoding();
	}

	public boolean getClobberStreamingResults() {
		return this.mc.getClobberStreamingResults();
	}

	public int getConnectTimeout() {
		return this.mc.getConnectTimeout();
	}

	public String getConnectionCollation() {
		return this.mc.getConnectionCollation();
	}

	public String getConnectionLifecycleInterceptors() {
		return this.mc.getConnectionLifecycleInterceptors();
	}

	public boolean getContinueBatchOnError() {
		return this.mc.getContinueBatchOnError();
	}

	public boolean getCreateDatabaseIfNotExist() {
		return this.mc.getCreateDatabaseIfNotExist();
	}

	public int getDefaultFetchSize() {
		return this.mc.getDefaultFetchSize();
	}

	public boolean getDontTrackOpenResources() {
		return this.mc.getDontTrackOpenResources();
	}

	public boolean getDumpMetadataOnColumnNotFound() {
		return this.mc.getDumpMetadataOnColumnNotFound();
	}

	public boolean getDumpQueriesOnException() {
		return this.mc.getDumpQueriesOnException();
	}

	public boolean getDynamicCalendars() {
		return this.mc.getDynamicCalendars();
	}

	public boolean getElideSetAutoCommits() {
		return this.mc.getElideSetAutoCommits();
	}

	public boolean getEmptyStringsConvertToZero() {
		return this.mc.getEmptyStringsConvertToZero();
	}

	public boolean getEmulateLocators() {
		return this.mc.getEmulateLocators();
	}

	public boolean getEmulateUnsupportedPstmts() {
		return this.mc.getEmulateUnsupportedPstmts();
	}

	public boolean getEnablePacketDebug() {
		return this.mc.getEnablePacketDebug();
	}

	public boolean getEnableQueryTimeouts() {
		return this.mc.getEnableQueryTimeouts();
	}

	public String getEncoding() {
		return this.mc.getEncoding();
	}

	public boolean getExplainSlowQueries() {
		return this.mc.getExplainSlowQueries();
	}

	public boolean getFailOverReadOnly() {
		return this.mc.getFailOverReadOnly();
	}

	public boolean getFunctionsNeverReturnBlobs() {
		return this.mc.getFunctionsNeverReturnBlobs();
	}

	public boolean getGatherPerfMetrics() {
		return this.mc.getGatherPerfMetrics();
	}

	public boolean getGatherPerformanceMetrics() {
		return this.mc.getGatherPerformanceMetrics();
	}

	public boolean getGenerateSimpleParameterMetadata() {
		return this.mc.getGenerateSimpleParameterMetadata();
	}

	public boolean getHoldResultsOpenOverStatementClose() {
		return this.mc.getHoldResultsOpenOverStatementClose();
	}

	public boolean getIgnoreNonTxTables() {
		return this.mc.getIgnoreNonTxTables();
	}

	public boolean getIncludeInnodbStatusInDeadlockExceptions() {
		return this.mc.getIncludeInnodbStatusInDeadlockExceptions();
	}

	public int getInitialTimeout() {
		return this.mc.getInitialTimeout();
	}

	public boolean getInteractiveClient() {
		return this.mc.getInteractiveClient();
	}

	public boolean getIsInteractiveClient() {
		return this.mc.getIsInteractiveClient();
	}

	public boolean getJdbcCompliantTruncation() {
		return this.mc.getJdbcCompliantTruncation();
	}

	public boolean getJdbcCompliantTruncationForReads() {
		return this.mc.getJdbcCompliantTruncationForReads();
	}

	public String getLargeRowSizeThreshold() {
		return this.mc.getLargeRowSizeThreshold();
	}

	public String getLoadBalanceStrategy() {
		return this.mc.getLoadBalanceStrategy();
	}

	public String getLocalSocketAddress() {
		return this.mc.getLocalSocketAddress();
	}

	public int getLocatorFetchBufferSize() {
		return this.mc.getLocatorFetchBufferSize();
	}

	public boolean getLogSlowQueries() {
		return this.mc.getLogSlowQueries();
	}

	public boolean getLogXaCommands() {
		return this.mc.getLogXaCommands();
	}

	public String getLogger() {
		return this.mc.getLogger();
	}

	public String getLoggerClassName() {
		return this.mc.getLoggerClassName();
	}

	public boolean getMaintainTimeStats() {
		return this.mc.getMaintainTimeStats();
	}

	public int getMaxQuerySizeToLog() {
		return this.mc.getMaxQuerySizeToLog();
	}

	public int getMaxReconnects() {
		return this.mc.getMaxReconnects();
	}

	public int getMaxRows() {
		return this.mc.getMaxRows();
	}

	public int getMetadataCacheSize() {
		return this.mc.getMetadataCacheSize();
	}

	public int getNetTimeoutForStreamingResults() {
		return this.mc.getNetTimeoutForStreamingResults();
	}

	public boolean getNoAccessToProcedureBodies() {
		return this.mc.getNoAccessToProcedureBodies();
	}

	public boolean getNoDatetimeStringSync() {
		return this.mc.getNoDatetimeStringSync();
	}

	public boolean getNoTimezoneConversionForTimeType() {
		return this.mc.getNoTimezoneConversionForTimeType();
	}

	public boolean getNullCatalogMeansCurrent() {
		return this.mc.getNullCatalogMeansCurrent();
	}

	public boolean getNullNamePatternMatchesAll() {
		return this.mc.getNullNamePatternMatchesAll();
	}

	public boolean getOverrideSupportsIntegrityEnhancementFacility() {
		return this.mc.getOverrideSupportsIntegrityEnhancementFacility();
	}

	public int getPacketDebugBufferSize() {
		return this.mc.getPacketDebugBufferSize();
	}

	public boolean getPadCharsWithSpace() {
		return this.mc.getPadCharsWithSpace();
	}

	public boolean getParanoid() {
		return this.mc.getParanoid();
	}

	public boolean getPedantic() {
		return this.mc.getPedantic();
	}

	public boolean getPinGlobalTxToPhysicalConnection() {
		return this.mc.getPinGlobalTxToPhysicalConnection();
	}

	public boolean getPopulateInsertRowWithDefaultValues() {
		return this.mc.getPopulateInsertRowWithDefaultValues();
	}

	public int getPrepStmtCacheSize() {
		return this.mc.getPrepStmtCacheSize();
	}

	public int getPrepStmtCacheSqlLimit() {
		return this.mc.getPrepStmtCacheSqlLimit();
	}

	public int getPreparedStatementCacheSize() {
		return this.mc.getPreparedStatementCacheSize();
	}

	public int getPreparedStatementCacheSqlLimit() {
		return this.mc.getPreparedStatementCacheSqlLimit();
	}

	public boolean getProcessEscapeCodesForPrepStmts() {
		return this.mc.getProcessEscapeCodesForPrepStmts();
	}

	public boolean getProfileSQL() {
		return this.mc.getProfileSQL();
	}

	public boolean getProfileSql() {
		return this.mc.getProfileSql();
	}

	public String getPropertiesTransform() {
		return this.mc.getPropertiesTransform();
	}

	public int getQueriesBeforeRetryMaster() {
		return this.mc.getQueriesBeforeRetryMaster();
	}

	public boolean getReconnectAtTxEnd() {
		return this.mc.getReconnectAtTxEnd();
	}

	public boolean getRelaxAutoCommit() {
		return this.mc.getRelaxAutoCommit();
	}

	public int getReportMetricsIntervalMillis() {
		return this.mc.getReportMetricsIntervalMillis();
	}

	public boolean getRequireSSL() {
		return this.mc.getRequireSSL();
	}

	public String getResourceId() {
		return this.mc.getResourceId();
	}

	public int getResultSetSizeThreshold() {
		return this.mc.getResultSetSizeThreshold();
	}

	public boolean getRewriteBatchedStatements() {
		return this.mc.getRewriteBatchedStatements();
	}

	public boolean getRollbackOnPooledClose() {
		return this.mc.getRollbackOnPooledClose();
	}

	public boolean getRoundRobinLoadBalance() {
		return this.mc.getRoundRobinLoadBalance();
	}

	public boolean getRunningCTS13() {
		return this.mc.getRunningCTS13();
	}

	public int getSecondsBeforeRetryMaster() {
		return this.mc.getSecondsBeforeRetryMaster();
	}

	public String getServerTimezone() {
		return this.mc.getServerTimezone();
	}

	public String getSessionVariables() {
		return this.mc.getSessionVariables();
	}

	public int getSlowQueryThresholdMillis() {
		return this.mc.getSlowQueryThresholdMillis();
	}

	public long getSlowQueryThresholdNanos() {
		return this.mc.getSlowQueryThresholdNanos();
	}

	public String getSocketFactory() {
		return this.mc.getSocketFactory();
	}

	public String getSocketFactoryClassName() {
		return this.mc.getSocketFactoryClassName();
	}

	public int getSocketTimeout() {
		return this.mc.getSocketTimeout();
	}

	public String getStatementInterceptors() {
		return this.mc.getStatementInterceptors();
	}

	public boolean getStrictFloatingPoint() {
		return this.mc.getStrictFloatingPoint();
	}

	public boolean getStrictUpdates() {
		return this.mc.getStrictUpdates();
	}

	public boolean getTcpKeepAlive() {
		return this.mc.getTcpKeepAlive();
	}

	public boolean getTcpNoDelay() {
		return this.mc.getTcpNoDelay();
	}

	public int getTcpRcvBuf() {
		return this.mc.getTcpRcvBuf();
	}

	public int getTcpSndBuf() {
		return this.mc.getTcpSndBuf();
	}

	public int getTcpTrafficClass() {
		return this.mc.getTcpTrafficClass();
	}

	public boolean getTinyInt1isBit() {
		return this.mc.getTinyInt1isBit();
	}

	public boolean getTraceProtocol() {
		return this.mc.getTraceProtocol();
	}

	public boolean getTransformedBitIsBoolean() {
		return this.mc.getTransformedBitIsBoolean();
	}

	public boolean getTreatUtilDateAsTimestamp() {
		return this.mc.getTreatUtilDateAsTimestamp();
	}

	public String getTrustCertificateKeyStorePassword() {
		return this.mc.getTrustCertificateKeyStorePassword();
	}

	public String getTrustCertificateKeyStoreType() {
		return this.mc.getTrustCertificateKeyStoreType();
	}

	public String getTrustCertificateKeyStoreUrl() {
		return this.mc.getTrustCertificateKeyStoreUrl();
	}

	public boolean getUltraDevHack() {
		return this.mc.getUltraDevHack();
	}

	public boolean getUseBlobToStoreUTF8OutsideBMP() {
		return this.mc.getUseBlobToStoreUTF8OutsideBMP();
	}

	public boolean getUseCompression() {
		return this.mc.getUseCompression();
	}

	public String getUseConfigs() {
		return this.mc.getUseConfigs();
	}

	public boolean getUseCursorFetch() {
		return this.mc.getUseCursorFetch();
	}

	public boolean getUseDirectRowUnpack() {
		return this.mc.getUseDirectRowUnpack();
	}

	public boolean getUseDynamicCharsetInfo() {
		return this.mc.getUseDynamicCharsetInfo();
	}

	public boolean getUseFastDateParsing() {
		return this.mc.getUseFastDateParsing();
	}

	public boolean getUseFastIntParsing() {
		return this.mc.getUseFastIntParsing();
	}

	public boolean getUseGmtMillisForDatetimes() {
		return this.mc.getUseGmtMillisForDatetimes();
	}

	public boolean getUseHostsInPrivileges() {
		return this.mc.getUseHostsInPrivileges();
	}

	public boolean getUseInformationSchema() {
		return this.mc.getUseInformationSchema();
	}

	public boolean getUseJDBCCompliantTimezoneShift() {
		return this.mc.getUseJDBCCompliantTimezoneShift();
	}

	public boolean getUseJvmCharsetConverters() {
		return this.mc.getUseJvmCharsetConverters();
	}

	public boolean getUseLocalSessionState() {
		return this.mc.getUseLocalSessionState();
	}

	public boolean getUseNanosForElapsedTime() {
		return this.mc.getUseNanosForElapsedTime();
	}

	public boolean getUseOldAliasMetadataBehavior() {
		return this.mc.getUseOldAliasMetadataBehavior();
	}

	public boolean getUseOldUTF8Behavior() {
		return this.mc.getUseOldUTF8Behavior();
	}

	public boolean getUseOnlyServerErrorMessages() {
		return this.mc.getUseOnlyServerErrorMessages();
	}

	public boolean getUseReadAheadInput() {
		return this.mc.getUseReadAheadInput();
	}

	public boolean getUseSSL() {
		return this.mc.getUseSSL();
	}

	public boolean getUseSSPSCompatibleTimezoneShift() {
		return this.mc.getUseSSPSCompatibleTimezoneShift();
	}

	public boolean getUseServerPrepStmts() {
		return this.mc.getUseServerPrepStmts();
	}

	public boolean getUseServerPreparedStmts() {
		return this.mc.getUseServerPreparedStmts();
	}

	public boolean getUseSqlStateCodes() {
		return this.mc.getUseSqlStateCodes();
	}

	public boolean getUseStreamLengthsInPrepStmts() {
		return this.mc.getUseStreamLengthsInPrepStmts();
	}

	public boolean getUseTimezone() {
		return this.mc.getUseTimezone();
	}

	public boolean getUseUltraDevWorkAround() {
		return this.mc.getUseUltraDevWorkAround();
	}

	public boolean getUseUnbufferedInput() {
		return this.mc.getUseUnbufferedInput();
	}

	public boolean getUseUnicode() {
		return this.mc.getUseUnicode();
	}

	public boolean getUseUsageAdvisor() {
		return this.mc.getUseUsageAdvisor();
	}

	public String getUtf8OutsideBmpExcludedColumnNamePattern() {
		return this.mc.getUtf8OutsideBmpExcludedColumnNamePattern();
	}

	public String getUtf8OutsideBmpIncludedColumnNamePattern() {
		return this.mc.getUtf8OutsideBmpIncludedColumnNamePattern();
	}

	public boolean getYearIsDateType() {
		return this.mc.getYearIsDateType();
	}

	public String getZeroDateTimeBehavior() {
		return this.mc.getZeroDateTimeBehavior();
	}

	public void setAllowLoadLocalInfile(boolean property) {
		this.mc.setAllowLoadLocalInfile(property);
	}

	public void setAllowMultiQueries(boolean property) {
		this.mc.setAllowMultiQueries(property);
	}

	public void setAllowNanAndInf(boolean flag) {
		this.mc.setAllowNanAndInf(flag);
	}

	public void setAllowUrlInLocalInfile(boolean flag) {
		this.mc.setAllowUrlInLocalInfile(flag);
	}

	public void setAlwaysSendSetIsolation(boolean flag) {
		this.mc.setAlwaysSendSetIsolation(flag);
	}

	public void setAutoClosePStmtStreams(boolean flag) {
		this.mc.setAutoClosePStmtStreams(flag);
	}

	public void setAutoDeserialize(boolean flag) {
		this.mc.setAutoDeserialize(flag);
	}

	public void setAutoGenerateTestcaseScript(boolean flag) {
		this.mc.setAutoGenerateTestcaseScript(flag);
	}

	public void setAutoReconnect(boolean flag) {
		this.mc.setAutoReconnect(flag);
	}

	public void setAutoReconnectForConnectionPools(boolean property) {
		this.mc.setAutoReconnectForConnectionPools(property);
	}

	public void setAutoReconnectForPools(boolean flag) {
		this.mc.setAutoReconnectForPools(flag);
	}

	public void setAutoSlowLog(boolean flag) {
		this.mc.setAutoSlowLog(flag);
	}

	public void setBlobSendChunkSize(String value) throws SQLException {
		this.mc.setBlobSendChunkSize(value);
	}

	public void setBlobsAreStrings(boolean flag) {
		this.mc.setBlobsAreStrings(flag);
	}

	public void setCacheCallableStatements(boolean flag) {
		this.mc.setCacheCallableStatements(flag);
	}

	public void setCacheCallableStmts(boolean flag) {
		this.mc.setCacheCallableStmts(flag);
	}

	public void setCachePrepStmts(boolean flag) {
		this.mc.setCachePrepStmts(flag);
	}

	public void setCachePreparedStatements(boolean flag) {
		this.mc.setCachePreparedStatements(flag);
	}

	public void setCacheResultSetMetadata(boolean property) {
		this.mc.setCacheResultSetMetadata(property);
	}

	public void setCacheServerConfiguration(boolean flag) {
		this.mc.setCacheServerConfiguration(flag);
	}

	public void setCallableStatementCacheSize(int size) {
		this.mc.setCallableStatementCacheSize(size);
	}

	public void setCallableStmtCacheSize(int cacheSize) {
		this.mc.setCallableStmtCacheSize(cacheSize);
	}

	public void setCapitalizeDBMDTypes(boolean property) {
		this.mc.setCapitalizeDBMDTypes(property);
	}

	public void setCapitalizeTypeNames(boolean flag) {
		this.mc.setCapitalizeTypeNames(flag);
	}

	public void setCharacterEncoding(String encoding) {
		this.mc.setCharacterEncoding(encoding);
	}

	public void setCharacterSetResults(String characterSet) {
		this.mc.setCharacterSetResults(characterSet);
	}

	public void setClientCertificateKeyStorePassword(String value) {
		this.mc.setClientCertificateKeyStorePassword(value);
	}

	public void setClientCertificateKeyStoreType(String value) {
		this.mc.setClientCertificateKeyStoreType(value);
	}

	public void setClientCertificateKeyStoreUrl(String value) {
		this.mc.setClientCertificateKeyStoreUrl(value);
	}

	public void setClientInfoProvider(String classname) {
		this.mc.setClientInfoProvider(classname);
	}

	public void setClobCharacterEncoding(String encoding) {
		this.mc.setClobCharacterEncoding(encoding);
	}

	public void setClobberStreamingResults(boolean flag) {
		this.mc.setClobberStreamingResults(flag);
	}

	public void setConnectTimeout(int timeoutMs) {
		this.mc.setConnectTimeout(timeoutMs);
	}

	public void setConnectionCollation(String collation) {
		this.mc.setConnectionCollation(collation);
	}

	public void setConnectionLifecycleInterceptors(String interceptors) {
		this.mc.setConnectionLifecycleInterceptors(interceptors);
	}

	public void setContinueBatchOnError(boolean property) {
		this.mc.setContinueBatchOnError(property);
	}

	public void setCreateDatabaseIfNotExist(boolean flag) {
		this.mc.setCreateDatabaseIfNotExist(flag);
	}

	public void setDefaultFetchSize(int n) {
		this.mc.setDefaultFetchSize(n);
	}

	public void setDetectServerPreparedStmts(boolean property) {
		this.mc.setDetectServerPreparedStmts(property);
	}

	public void setDontTrackOpenResources(boolean flag) {
		this.mc.setDontTrackOpenResources(flag);
	}

	public void setDumpMetadataOnColumnNotFound(boolean flag) {
		this.mc.setDumpMetadataOnColumnNotFound(flag);
	}

	public void setDumpQueriesOnException(boolean flag) {
		this.mc.setDumpQueriesOnException(flag);
	}

	public void setDynamicCalendars(boolean flag) {
		this.mc.setDynamicCalendars(flag);
	}

	public void setElideSetAutoCommits(boolean flag) {
		this.mc.setElideSetAutoCommits(flag);
	}

	public void setEmptyStringsConvertToZero(boolean flag) {
		this.mc.setEmptyStringsConvertToZero(flag);
	}

	public void setEmulateLocators(boolean property) {
		this.mc.setEmulateLocators(property);
	}

	public void setEmulateUnsupportedPstmts(boolean flag) {
		this.mc.setEmulateUnsupportedPstmts(flag);
	}

	public void setEnablePacketDebug(boolean flag) {
		this.mc.setEnablePacketDebug(flag);
	}

	public void setEnableQueryTimeouts(boolean flag) {
		this.mc.setEnableQueryTimeouts(flag);
	}

	public void setEncoding(String property) {
		this.mc.setEncoding(property);
	}

	public void setExplainSlowQueries(boolean flag) {
		this.mc.setExplainSlowQueries(flag);
	}

	public void setFailOverReadOnly(boolean flag) {
		this.mc.setFailOverReadOnly(flag);
	}

	public void setFunctionsNeverReturnBlobs(boolean flag) {
		this.mc.setFunctionsNeverReturnBlobs(flag);
	}

	public void setGatherPerfMetrics(boolean flag) {
		this.mc.setGatherPerfMetrics(flag);
	}

	public void setGatherPerformanceMetrics(boolean flag) {
		this.mc.setGatherPerformanceMetrics(flag);
	}

	public void setGenerateSimpleParameterMetadata(boolean flag) {
		this.mc.setGenerateSimpleParameterMetadata(flag);
	}

	public void setHoldResultsOpenOverStatementClose(boolean flag) {
		this.mc.setHoldResultsOpenOverStatementClose(flag);
	}

	public void setIgnoreNonTxTables(boolean property) {
		this.mc.setIgnoreNonTxTables(property);
	}

	public void setIncludeInnodbStatusInDeadlockExceptions(boolean flag) {
		this.mc.setIncludeInnodbStatusInDeadlockExceptions(flag);
	}

	public void setInitialTimeout(int property) {
		this.mc.setInitialTimeout(property);
	}

	public void setInteractiveClient(boolean property) {
		this.mc.setInteractiveClient(property);
	}

	public void setIsInteractiveClient(boolean property) {
		this.mc.setIsInteractiveClient(property);
	}

	public void setJdbcCompliantTruncation(boolean flag) {
		this.mc.setJdbcCompliantTruncation(flag);
	}

	public void setJdbcCompliantTruncationForReads(
			boolean jdbcCompliantTruncationForReads) {
		this.mc
				.setJdbcCompliantTruncationForReads(jdbcCompliantTruncationForReads);
	}

	public void setLargeRowSizeThreshold(String value) {
		this.mc.setLargeRowSizeThreshold(value);
	}

	public void setLoadBalanceStrategy(String strategy) {
		this.mc.setLoadBalanceStrategy(strategy);
	}

	public void setLocalSocketAddress(String address) {
		this.mc.setLocalSocketAddress(address);
	}

	public void setLocatorFetchBufferSize(String value) throws SQLException {
		this.mc.setLocatorFetchBufferSize(value);
	}

	public void setLogSlowQueries(boolean flag) {
		this.mc.setLogSlowQueries(flag);
	}

	public void setLogXaCommands(boolean flag) {
		this.mc.setLogXaCommands(flag);
	}

	public void setLogger(String property) {
		this.mc.setLogger(property);
	}

	public void setLoggerClassName(String className) {
		this.mc.setLoggerClassName(className);
	}

	public void setMaintainTimeStats(boolean flag) {
		this.mc.setMaintainTimeStats(flag);
	}

	public void setMaxQuerySizeToLog(int sizeInBytes) {
		this.mc.setMaxQuerySizeToLog(sizeInBytes);
	}

	public void setMaxReconnects(int property) {
		this.mc.setMaxReconnects(property);
	}

	public void setMaxRows(int property) {
		this.mc.setMaxRows(property);
	}

	public void setMetadataCacheSize(int value) {
		this.mc.setMetadataCacheSize(value);
	}

	public void setNetTimeoutForStreamingResults(int value) {
		this.mc.setNetTimeoutForStreamingResults(value);
	}

	public void setNoAccessToProcedureBodies(boolean flag) {
		this.mc.setNoAccessToProcedureBodies(flag);
	}

	public void setNoDatetimeStringSync(boolean flag) {
		this.mc.setNoDatetimeStringSync(flag);
	}

	public void setNoTimezoneConversionForTimeType(boolean flag) {
		this.mc.setNoTimezoneConversionForTimeType(flag);
	}

	public void setNullCatalogMeansCurrent(boolean value) {
		this.mc.setNullCatalogMeansCurrent(value);
	}

	public void setNullNamePatternMatchesAll(boolean value) {
		this.mc.setNullNamePatternMatchesAll(value);
	}

	public void setOverrideSupportsIntegrityEnhancementFacility(boolean flag) {
		this.mc.setOverrideSupportsIntegrityEnhancementFacility(flag);
	}

	public void setPacketDebugBufferSize(int size) {
		this.mc.setPacketDebugBufferSize(size);
	}

	public void setPadCharsWithSpace(boolean flag) {
		this.mc.setPadCharsWithSpace(flag);
	}

	public void setParanoid(boolean property) {
		this.mc.setParanoid(property);
	}

	public void setPedantic(boolean property) {
		this.mc.setPedantic(property);
	}

	public void setPinGlobalTxToPhysicalConnection(boolean flag) {
		this.mc.setPinGlobalTxToPhysicalConnection(flag);
	}

	public void setPopulateInsertRowWithDefaultValues(boolean flag) {
		this.mc.setPopulateInsertRowWithDefaultValues(flag);
	}

	public void setPrepStmtCacheSize(int cacheSize) {
		this.mc.setPrepStmtCacheSize(cacheSize);
	}

	public void setPrepStmtCacheSqlLimit(int sqlLimit) {
		this.mc.setPrepStmtCacheSqlLimit(sqlLimit);
	}

	public void setPreparedStatementCacheSize(int cacheSize) {
		this.mc.setPreparedStatementCacheSize(cacheSize);
	}

	public void setPreparedStatementCacheSqlLimit(int cacheSqlLimit) {
		this.mc.setPreparedStatementCacheSqlLimit(cacheSqlLimit);
	}

	public void setProcessEscapeCodesForPrepStmts(boolean flag) {
		this.mc.setProcessEscapeCodesForPrepStmts(flag);
	}

	public void setProfileSQL(boolean flag) {
		this.mc.setProfileSQL(flag);
	}

	public void setProfileSql(boolean property) {
		this.mc.setProfileSql(property);
	}

	public void setPropertiesTransform(String value) {
		this.mc.setPropertiesTransform(value);
	}

	public void setQueriesBeforeRetryMaster(int property) {
		this.mc.setQueriesBeforeRetryMaster(property);
	}

	public void setReconnectAtTxEnd(boolean property) {
		this.mc.setReconnectAtTxEnd(property);
	}

	public void setRelaxAutoCommit(boolean property) {
		this.mc.setRelaxAutoCommit(property);
	}

	public void setReportMetricsIntervalMillis(int millis) {
		this.mc.setReportMetricsIntervalMillis(millis);
	}

	public void setRequireSSL(boolean property) {
		this.mc.setRequireSSL(property);
	}

	public void setResourceId(String resourceId) {
		this.mc.setResourceId(resourceId);
	}

	public void setResultSetSizeThreshold(int threshold) {
		this.mc.setResultSetSizeThreshold(threshold);
	}

	public void setRetainStatementAfterResultSetClose(boolean flag) {
		this.mc.setRetainStatementAfterResultSetClose(flag);
	}

	public void setRewriteBatchedStatements(boolean flag) {
		this.mc.setRewriteBatchedStatements(flag);
	}

	public void setRollbackOnPooledClose(boolean flag) {
		this.mc.setRollbackOnPooledClose(flag);
	}

	public void setRoundRobinLoadBalance(boolean flag) {
		this.mc.setRoundRobinLoadBalance(flag);
	}

	public void setRunningCTS13(boolean flag) {
		this.mc.setRunningCTS13(flag);
	}

	public void setSecondsBeforeRetryMaster(int property) {
		this.mc.setSecondsBeforeRetryMaster(property);
	}

	public void setServerTimezone(String property) {
		this.mc.setServerTimezone(property);
	}

	public void setSessionVariables(String variables) {
		this.mc.setSessionVariables(variables);
	}

	public void setSlowQueryThresholdMillis(int millis) {
		this.mc.setSlowQueryThresholdMillis(millis);
	}

	public void setSlowQueryThresholdNanos(long nanos) {
		this.mc.setSlowQueryThresholdNanos(nanos);
	}

	public void setSocketFactory(String name) {
		this.mc.setSocketFactory(name);
	}

	public void setSocketFactoryClassName(String property) {
		this.mc.setSocketFactoryClassName(property);
	}

	public void setSocketTimeout(int property) {
		this.mc.setSocketTimeout(property);
	}

	public void setStatementInterceptors(String value) {
		this.mc.setStatementInterceptors(value);
	}

	public void setStrictFloatingPoint(boolean property) {
		this.mc.setStrictFloatingPoint(property);
	}

	public void setStrictUpdates(boolean property) {
		this.mc.setStrictUpdates(property);
	}

	public void setTcpKeepAlive(boolean flag) {
		this.mc.setTcpKeepAlive(flag);
	}

	public void setTcpNoDelay(boolean flag) {
		this.mc.setTcpNoDelay(flag);
	}

	public void setTcpRcvBuf(int bufSize) {
		this.mc.setTcpRcvBuf(bufSize);
	}

	public void setTcpSndBuf(int bufSize) {
		this.mc.setTcpSndBuf(bufSize);
	}

	public void setTcpTrafficClass(int classFlags) {
		this.mc.setTcpTrafficClass(classFlags);
	}

	public void setTinyInt1isBit(boolean flag) {
		this.mc.setTinyInt1isBit(flag);
	}

	public void setTraceProtocol(boolean flag) {
		this.mc.setTraceProtocol(flag);
	}

	public void setTransformedBitIsBoolean(boolean flag) {
		this.mc.setTransformedBitIsBoolean(flag);
	}

	public void setTreatUtilDateAsTimestamp(boolean flag) {
		this.mc.setTreatUtilDateAsTimestamp(flag);
	}

	public void setTrustCertificateKeyStorePassword(String value) {
		this.mc.setTrustCertificateKeyStorePassword(value);
	}

	public void setTrustCertificateKeyStoreType(String value) {
		this.mc.setTrustCertificateKeyStoreType(value);
	}

	public void setTrustCertificateKeyStoreUrl(String value) {
		this.mc.setTrustCertificateKeyStoreUrl(value);
	}

	public void setUltraDevHack(boolean flag) {
		this.mc.setUltraDevHack(flag);
	}

	public void setUseBlobToStoreUTF8OutsideBMP(boolean flag) {
		this.mc.setUseBlobToStoreUTF8OutsideBMP(flag);
	}

	public void setUseCompression(boolean property) {
		this.mc.setUseCompression(property);
	}

	public void setUseConfigs(String configs) {
		this.mc.setUseConfigs(configs);
	}

	public void setUseCursorFetch(boolean flag) {
		this.mc.setUseCursorFetch(flag);
	}

	public void setUseDirectRowUnpack(boolean flag) {
		this.mc.setUseDirectRowUnpack(flag);
	}

	public void setUseDynamicCharsetInfo(boolean flag) {
		this.mc.setUseDynamicCharsetInfo(flag);
	}

	public void setUseFastDateParsing(boolean flag) {
		this.mc.setUseFastDateParsing(flag);
	}

	public void setUseFastIntParsing(boolean flag) {
		this.mc.setUseFastIntParsing(flag);
	}

	public void setUseGmtMillisForDatetimes(boolean flag) {
		this.mc.setUseGmtMillisForDatetimes(flag);
	}

	public void setUseHostsInPrivileges(boolean property) {
		this.mc.setUseHostsInPrivileges(property);
	}

	public void setUseInformationSchema(boolean flag) {
		this.mc.setUseInformationSchema(flag);
	}

	public void setUseJDBCCompliantTimezoneShift(boolean flag) {
		this.mc.setUseJDBCCompliantTimezoneShift(flag);
	}

	public void setUseJvmCharsetConverters(boolean flag) {
		this.mc.setUseJvmCharsetConverters(flag);
	}

	public void setUseLocalSessionState(boolean flag) {
		this.mc.setUseLocalSessionState(flag);
	}

	public void setUseNanosForElapsedTime(boolean flag) {
		this.mc.setUseNanosForElapsedTime(flag);
	}

	public void setUseOldAliasMetadataBehavior(boolean flag) {
		this.mc.setUseOldAliasMetadataBehavior(flag);
	}

	public void setUseOldUTF8Behavior(boolean flag) {
		this.mc.setUseOldUTF8Behavior(flag);
	}

	public void setUseOnlyServerErrorMessages(boolean flag) {
		this.mc.setUseOnlyServerErrorMessages(flag);
	}

	public void setUseReadAheadInput(boolean flag) {
		this.mc.setUseReadAheadInput(flag);
	}

	public void setUseSSL(boolean property) {
		this.mc.setUseSSL(property);
	}

	public void setUseSSPSCompatibleTimezoneShift(boolean flag) {
		this.mc.setUseSSPSCompatibleTimezoneShift(flag);
	}

	public void setUseServerPrepStmts(boolean flag) {
		this.mc.setUseServerPrepStmts(flag);
	}

	public void setUseServerPreparedStmts(boolean flag) {
		this.mc.setUseServerPreparedStmts(flag);
	}

	public void setUseSqlStateCodes(boolean flag) {
		this.mc.setUseSqlStateCodes(flag);
	}

	public void setUseStreamLengthsInPrepStmts(boolean property) {
		this.mc.setUseStreamLengthsInPrepStmts(property);
	}

	public void setUseTimezone(boolean property) {
		this.mc.setUseTimezone(property);
	}

	public void setUseUltraDevWorkAround(boolean property) {
		this.mc.setUseUltraDevWorkAround(property);
	}

	public void setUseUnbufferedInput(boolean flag) {
		this.mc.setUseUnbufferedInput(flag);
	}

	public void setUseUnicode(boolean flag) {
		this.mc.setUseUnicode(flag);
	}

	public void setUseUsageAdvisor(boolean useUsageAdvisorFlag) {
		this.mc.setUseUsageAdvisor(useUsageAdvisorFlag);
	}

	public void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern) {
		this.mc.setUtf8OutsideBmpExcludedColumnNamePattern(regexPattern);
	}

	public void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern) {
		this.mc.setUtf8OutsideBmpIncludedColumnNamePattern(regexPattern);
	}

	public void setYearIsDateType(boolean flag) {
		this.mc.setYearIsDateType(flag);
	}

	public void setZeroDateTimeBehavior(String behavior) {
		this.mc.setZeroDateTimeBehavior(behavior);
	}

	public boolean useUnbufferedInput() {
		return this.mc.useUnbufferedInput();
	}

	public void initializeExtension(Extension ex) throws SQLException {
		this.mc.initializeExtension(ex);
	}

	public String getProfilerEventHandler() {
		return this.mc.getProfilerEventHandler();
	}

	public void setProfilerEventHandler(String handler) {
		this.mc.setProfilerEventHandler(handler);
	}

	public boolean getVerifyServerCertificate() {
		return this.mc.getVerifyServerCertificate();
	}

	public void setVerifyServerCertificate(boolean flag) {
		this.mc.setVerifyServerCertificate(flag);
	}

	public boolean getUseLegacyDatetimeCode() {
		return this.mc.getUseLegacyDatetimeCode();
	}

	public void setUseLegacyDatetimeCode(boolean flag) {
		this.mc.setUseLegacyDatetimeCode(flag);
	}

	public int getSelfDestructOnPingMaxOperations() {
		return this.mc.getSelfDestructOnPingMaxOperations();
	}

	public int getSelfDestructOnPingSecondsLifetime() {
		return this.mc.getSelfDestructOnPingSecondsLifetime();
	}

	public void setSelfDestructOnPingMaxOperations(int maxOperations) {
		this.mc.setSelfDestructOnPingMaxOperations(maxOperations);
	}

	public void setSelfDestructOnPingSecondsLifetime(int seconds) {
		this.mc.setSelfDestructOnPingSecondsLifetime(seconds);
	}

	public boolean getUseColumnNamesInFindColumn() {
		return this.mc.getUseColumnNamesInFindColumn();
	}

	public void setUseColumnNamesInFindColumn(boolean flag) {
		this.mc.setUseColumnNamesInFindColumn(flag);
	}

	public boolean getUseLocalTransactionState() {
		return this.mc.getUseLocalTransactionState();
	}

	public void setUseLocalTransactionState(boolean flag) {
		this.mc.setUseLocalTransactionState(flag);
	}
	
	public boolean getCompensateOnDuplicateKeyUpdateCounts() {
		return this.mc.getCompensateOnDuplicateKeyUpdateCounts();
	}

	public void setCompensateOnDuplicateKeyUpdateCounts(boolean flag) {
		this.mc.setCompensateOnDuplicateKeyUpdateCounts(flag);
	}

	public boolean getUseAffectedRows() {
		return this.mc.getUseAffectedRows();
	}

	public void setUseAffectedRows(boolean flag) {
		this.mc.setUseAffectedRows(flag);
	}

	public String getPasswordCharacterEncoding() {
		return this.mc.getPasswordCharacterEncoding();
	}

	public void setPasswordCharacterEncoding(String characterSet) {
		this.mc.setPasswordCharacterEncoding(characterSet);
	}

	public int getAutoIncrementIncrement() {
		return this.mc.getAutoIncrementIncrement();
	}

	public int getLoadBalanceBlacklistTimeout() {
		return this.mc.getLoadBalanceBlacklistTimeout();
	}

	public void setLoadBalanceBlacklistTimeout(int loadBalanceBlacklistTimeout) {
		this.mc.setLoadBalanceBlacklistTimeout(loadBalanceBlacklistTimeout);
	}
	
	public void setRetriesAllDown(int retriesAllDown) {
		this.mc.setRetriesAllDown(retriesAllDown);
	}
	
	public int getRetriesAllDown() {
		return this.mc.getRetriesAllDown();
	}

	public ExceptionInterceptor getExceptionInterceptor() {
		return this.pooledConnection.getExceptionInterceptor();
	}

	public String getExceptionInterceptors() {
		return this.mc.getExceptionInterceptors();
	}

	public void setExceptionInterceptors(String exceptionInterceptors) {
		this.mc.setExceptionInterceptors(exceptionInterceptors);
	}

	public boolean getQueryTimeoutKillsConnection() {
		return this.mc.getQueryTimeoutKillsConnection();
	}

	public void setQueryTimeoutKillsConnection(
			boolean queryTimeoutKillsConnection) {
		this.mc.setQueryTimeoutKillsConnection(queryTimeoutKillsConnection);
	}

	public boolean hasSameProperties(Connection c) {
		return this.mc.hasSameProperties(c);
	}
	
	public Properties getProperties() {
		return this.mc.getProperties();
	}
}