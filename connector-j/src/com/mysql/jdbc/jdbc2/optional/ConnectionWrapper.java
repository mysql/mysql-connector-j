/*
 Copyright (C) 2002-2006 MySQL AB

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

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

import com.mysql.jdbc.MysqlErrorNumbers;
import com.mysql.jdbc.SQLError;

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
	private com.mysql.jdbc.Connection mc = null;

	private MysqlPooledConnection mpc = null;

	private String invalidHandleStr = "Logical handle no longer valid";

	private boolean closed;
	private boolean isForXa;
	
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
			com.mysql.jdbc.Connection mysqlConnection,
			boolean forXa) throws SQLException {
		this.mpc = mysqlPooledConnection;
		this.mc = mysqlConnection;
		this.closed = false;
		this.pooledConnection = this.mpc;
		this.isForXa = forXa;
		
		if (this.isForXa) {
			setInGlobalTx(false);
			setAutoCommit(false);
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
			throw SQLError.createSQLException("Can't set autocommit to 'true' on an XAConnection", 
					SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION, 
					MysqlErrorNumbers.ER_XA_RMERR);
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

	public boolean isMasterConnection() throws SQLException {
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
			throw SQLError.createSQLException("Can't set autocommit to 'true' on an XAConnection", 
					SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION, 
					MysqlErrorNumbers.ER_XA_RMERR);
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
			throw SQLError.createSQLException("Can't set autocommit to 'true' on an XAConnection", 
					SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION, 
					MysqlErrorNumbers.ER_XA_RMERR);
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
			throw SQLError.createSQLException(
					"Can't call commit() on an XAConnection associated with a global transaction",
					SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION, 
					MysqlErrorNumbers.ER_XA_RMERR);
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
			return new StatementWrapper(this, this.mpc, this.mc
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
			return new StatementWrapper(this, this.mpc, this.mc
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
			return new StatementWrapper(this, this.mpc, this.mc
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
			return new CallableStatementWrapper(this, this.mpc, this.mc
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
			return new CallableStatementWrapper(this, this.mpc, this.mc
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
			return new CallableStatementWrapper(this, this.mpc, this.mc
					.prepareCall(arg0, arg1, arg2, arg3));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // we don't reach this code, compiler can't tell
	}

	public java.sql.PreparedStatement clientPrepare(String sql) throws SQLException
	{
		checkClosed();

		try {
			return new PreparedStatementWrapper(this, this.mpc, 
					this.mc.clientPrepareStatement(sql));
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
		
		return null;
	}
	
	public java.sql.PreparedStatement clientPrepare(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException
	{
		checkClosed();

		try {
			return new PreparedStatementWrapper(this, this.mpc, 
					this.mc.clientPrepareStatement(sql,
							resultSetType, resultSetConcurrency));
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
			return new PreparedStatementWrapper(this, this.mpc, this.mc
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
			return new PreparedStatementWrapper(this, this.mpc, this.mc
					.prepareStatement(sql, resultSetType, resultSetConcurrency));
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
			return new PreparedStatementWrapper(this, this.mpc, this.mc
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
			return new PreparedStatementWrapper(this, this.mpc, this.mc
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
			return new PreparedStatementWrapper(this, this.mpc, this.mc
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
			return new PreparedStatementWrapper(this, this.mpc, this.mc
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
			throw SQLError.createSQLException("Can't call rollback() on an XAConnection associated with a global transaction",
					SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION, 
					MysqlErrorNumbers.ER_XA_RMERR);
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
			throw SQLError.createSQLException("Can't call rollback() on an XAConnection associated with a global transaction",
					SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION, 
					MysqlErrorNumbers.ER_XA_RMERR);
		}
		
		try {
			this.mc.rollback(arg0);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}
	}

	public boolean isSameResource(Connection c) {
		if (c instanceof ConnectionWrapper) {
			return this.mc.isSameResource(((ConnectionWrapper)c).mc);
		} else if (c instanceof com.mysql.jdbc.Connection) {
			return this.mc.isSameResource((com.mysql.jdbc.Connection)c);
		}
		
		return false;
	}
	
	protected void close(boolean fireClosedEvent) throws SQLException {
		synchronized (this.mpc) {
			if (this.closed) {
				return;
			}

			if (!isInGlobalTx() 
					&& this.mc.getRollbackOnPooledClose()
					&& !this.getAutoCommit()) {
				rollback();
			}

			if (fireClosedEvent) {
				this.mpc.callListener(
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

	private void checkClosed() throws SQLException {
		if (this.closed) {
			throw SQLError.createSQLException(this.invalidHandleStr);
		}
	}

	protected boolean isInGlobalTx() {
		return this.mc.isInGlobalTx();
	}

	protected void setInGlobalTx(boolean flag) {
		this.mc.setInGlobalTx(flag);
	}
	
	public void ping() throws SQLException {
		if (this.mc != null) {
			this.mc.ping();
		}
	}
}
