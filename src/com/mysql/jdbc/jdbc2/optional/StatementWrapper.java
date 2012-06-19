/*
 Copyright (c) 2002, 2012, Oracle and/or its affiliates. All rights reserved.
 

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
package com.mysql.jdbc.jdbc2.optional;

import java.lang.reflect.Constructor;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.Util;

/**
 * Wraps statements so that errors can be reported correctly to
 * ConnectionEventListeners.
 * 
 * @author Mark Matthews
 * 
 * @version $Id: StatementWrapper.java,v 1.1.2.1 2005/05/13 18:58:38 mmatthews
 *          Exp $
 */
public class StatementWrapper extends WrapperBase implements Statement {
	private static final Constructor<?> JDBC_4_STATEMENT_WRAPPER_CTOR;
	
	static {
		if (Util.isJdbc4()) {
			try {
				JDBC_4_STATEMENT_WRAPPER_CTOR = Class.forName(
						"com.mysql.jdbc.jdbc2.optional.JDBC4StatementWrapper").getConstructor(
						new Class[] { ConnectionWrapper.class, 
								MysqlPooledConnection.class, 
								Statement.class });
			} catch (SecurityException e) {
				throw new RuntimeException(e);
			} catch (NoSuchMethodException e) {
				throw new RuntimeException(e);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else {
			JDBC_4_STATEMENT_WRAPPER_CTOR = null;
		}
	}
	
	protected static StatementWrapper getInstance(ConnectionWrapper c, 
			MysqlPooledConnection conn,
			Statement toWrap) throws SQLException {
		if (!Util.isJdbc4()) {
			return new StatementWrapper(c, 
					conn, toWrap);
		}

		return (StatementWrapper) Util.handleNewInstance(
				JDBC_4_STATEMENT_WRAPPER_CTOR,
				new Object[] {c, 
						conn, toWrap }, conn.getExceptionInterceptor());
	}
	
	protected Statement wrappedStmt;

	protected ConnectionWrapper wrappedConn;

	public StatementWrapper(ConnectionWrapper c, MysqlPooledConnection conn,
			Statement toWrap) {
		super(conn);
		this.wrappedStmt = toWrap;
		this.wrappedConn = c;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getConnection()
	 */
	public Connection getConnection() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedConn;
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null; // we actually never get here, but the compiler can't
						// figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setCursorName(java.lang.String)
	 */
	public void setCursorName(String name) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				this.wrappedStmt.setCursorName(name);
			} else {
				throw SQLError.createSQLException("Statement already closed",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setEscapeProcessing(boolean)
	 */
	public void setEscapeProcessing(boolean enable) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				this.wrappedStmt.setEscapeProcessing(enable);
			} else {
				throw SQLError.createSQLException("Statement already closed",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setFetchDirection(int)
	 */
	public void setFetchDirection(int direction) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				this.wrappedStmt.setFetchDirection(direction);
			} else {
				throw SQLError.createSQLException("Statement already closed",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getFetchDirection()
	 */
	public int getFetchDirection() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.getFetchDirection();
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return ResultSet.FETCH_FORWARD; // we actually never get here, but the
										// compiler can't figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setFetchSize(int)
	 */
	public void setFetchSize(int rows) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				this.wrappedStmt.setFetchSize(rows);
			} else {
				throw SQLError.createSQLException("Statement already closed",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getFetchSize()
	 */
	public int getFetchSize() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.getFetchSize();
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0; // we actually never get here, but the compiler can't figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getGeneratedKeys()
	 */
	public ResultSet getGeneratedKeys() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.getGeneratedKeys();
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null; // we actually never get here, but the compiler can't
						// figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setMaxFieldSize(int)
	 */
	public void setMaxFieldSize(int max) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				this.wrappedStmt.setMaxFieldSize(max);
			} else {
				throw SQLError.createSQLException("Statement already closed",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getMaxFieldSize()
	 */
	public int getMaxFieldSize() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.getMaxFieldSize();
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0; // we actually never get here, but the compiler can't figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setMaxRows(int)
	 */
	public void setMaxRows(int max) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				this.wrappedStmt.setMaxRows(max);
			} else {
				throw SQLError.createSQLException("Statement already closed",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getMaxRows()
	 */
	public int getMaxRows() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.getMaxRows();
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0; // we actually never get here, but the compiler can't figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getMoreResults()
	 */
	public boolean getMoreResults() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.getMoreResults();
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getMoreResults(int)
	 */
	public boolean getMoreResults(int current) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.getMoreResults(current);
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#setQueryTimeout(int)
	 */
	public void setQueryTimeout(int seconds) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				this.wrappedStmt.setQueryTimeout(seconds);
			} else {
				throw SQLError.createSQLException("Statement already closed",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getQueryTimeout()
	 */
	public int getQueryTimeout() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.getQueryTimeout();
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getResultSet()
	 */
	public ResultSet getResultSet() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				ResultSet rs = this.wrappedStmt.getResultSet();
				
				if (rs != null) {
					((com.mysql.jdbc.ResultSetInternalMethods) rs).setWrapperStatement(this);
				}
				return rs;
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getResultSetConcurrency()
	 */
	public int getResultSetConcurrency() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.getResultSetConcurrency();
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getResultSetHoldability()
	 */
	public int getResultSetHoldability() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.getResultSetHoldability();
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return Statement.CLOSE_CURRENT_RESULT;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getResultSetType()
	 */
	public int getResultSetType() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.getResultSetType();
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return ResultSet.TYPE_FORWARD_ONLY;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getUpdateCount()
	 */
	public int getUpdateCount() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.getUpdateCount();
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return -1;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.getWarnings();
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#addBatch(java.lang.String)
	 */
	public void addBatch(String sql) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				this.wrappedStmt.addBatch(sql);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#cancel()
	 */
	public void cancel() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				this.wrappedStmt.cancel();
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#clearBatch()
	 */
	public void clearBatch() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				this.wrappedStmt.clearBatch();
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#clearWarnings()
	 */
	public void clearWarnings() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				this.wrappedStmt.clearWarnings();
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#close()
	 */
	public void close() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				this.wrappedStmt.close();
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		} finally {
			this.wrappedStmt = null;
			this.pooledConnection = null;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#execute(java.lang.String, int)
	 */
	public boolean execute(String sql, int autoGeneratedKeys)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.execute(sql, autoGeneratedKeys);
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return false; // we actually never get here, but the compiler can't
						// figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#execute(java.lang.String, int[])
	 */
	public boolean execute(String sql, int[] columnIndexes) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.execute(sql, columnIndexes);
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return false; // we actually never get here, but the compiler can't
						// figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#execute(java.lang.String, java.lang.String[])
	 */
	public boolean execute(String sql, String[] columnNames)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.execute(sql, columnNames);
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return false; // we actually never get here, but the compiler can't
						// figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#execute(java.lang.String)
	 */
	public boolean execute(String sql) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.execute(sql);
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return false; // we actually never get here, but the compiler can't
						// figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeBatch()
	 */
	public int[] executeBatch() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.executeBatch();
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null; // we actually never get here, but the compiler can't
						// figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeQuery(java.lang.String)
	 */
	public ResultSet executeQuery(String sql) throws SQLException {
		try {
			if (this.wrappedStmt != null) {

				ResultSet rs = this.wrappedStmt.executeQuery(sql);
				((com.mysql.jdbc.ResultSetInternalMethods) rs).setWrapperStatement(this);

				return rs;
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null; // we actually never get here, but the compiler can't
						// figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int)
	 */
	public int executeUpdate(String sql, int autoGeneratedKeys)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.executeUpdate(sql, autoGeneratedKeys);
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return -1; // we actually never get here, but the compiler can't figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeUpdate(java.lang.String, int[])
	 */
	public int executeUpdate(String sql, int[] columnIndexes)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.executeUpdate(sql, columnIndexes);
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return -1; // we actually never get here, but the compiler can't figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeUpdate(java.lang.String,
	 *      java.lang.String[])
	 */
	public int executeUpdate(String sql, String[] columnNames)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.executeUpdate(sql, columnNames);
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return -1; // we actually never get here, but the compiler can't figure

		// that out
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Statement#executeUpdate(java.lang.String)
	 */
	public int executeUpdate(String sql) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.executeUpdate(sql);
			}

			throw SQLError.createSQLException("Statement already closed",
					SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return -1; // we actually never get here, but the compiler can't figure

		// that out
	}

	public void enableStreamingResults() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((com.mysql.jdbc.Statement) this.wrappedStmt)
						.enableStreamingResults();
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
}
