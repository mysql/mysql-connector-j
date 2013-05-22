/*
 Copyright (c) 2002, 2013, Oracle and/or its affiliates. All rights reserved.
 

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

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.Util;

/**
 * Wraps prepared statements so that errors can be reported correctly to
 * ConnectionEventListeners.
 * 
 * @author Mark Matthews
 * 
 * @version $Id: PreparedStatementWrapper.java,v 1.1.2.1 2005/05/13 18:58:38
 *          mmatthews Exp $
 */
public class PreparedStatementWrapper extends StatementWrapper implements
		PreparedStatement {
	private static final Constructor<?> JDBC_4_PREPARED_STATEMENT_WRAPPER_CTOR;
	
	static {
		if (Util.isJdbc4()) {
			try {
				JDBC_4_PREPARED_STATEMENT_WRAPPER_CTOR = Class.forName(
						"com.mysql.jdbc.jdbc2.optional.JDBC4PreparedStatementWrapper").getConstructor(
						new Class[] { ConnectionWrapper.class, 
								MysqlPooledConnection.class, 
								PreparedStatement.class });
			} catch (SecurityException e) {
				throw new RuntimeException(e);
			} catch (NoSuchMethodException e) {
				throw new RuntimeException(e);
			} catch (ClassNotFoundException e) {
				throw new RuntimeException(e);
			}
		} else {
			JDBC_4_PREPARED_STATEMENT_WRAPPER_CTOR = null;
		}
	}
	
	protected static PreparedStatementWrapper getInstance(ConnectionWrapper c, 
			MysqlPooledConnection conn,
			PreparedStatement toWrap) throws SQLException {
		if (!Util.isJdbc4()) {
			return new PreparedStatementWrapper(c, 
					conn, toWrap);
		}

		return (PreparedStatementWrapper) Util.handleNewInstance(
				JDBC_4_PREPARED_STATEMENT_WRAPPER_CTOR,
				new Object[] {c, 
						conn, toWrap }, conn.getExceptionInterceptor());
	}
	
	PreparedStatementWrapper(ConnectionWrapper c, MysqlPooledConnection conn,
			PreparedStatement toWrap) {
		super(c, conn, toWrap);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setArray(int, java.sql.Array)
	 */
	public void setArray(int parameterIndex, Array x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setArray(parameterIndex,
						x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setAsciiStream(int, java.io.InputStream,
	 *      int)
	 */
	public void setAsciiStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setAsciiStream(
						parameterIndex, x, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBigDecimal(int, java.math.BigDecimal)
	 */
	public void setBigDecimal(int parameterIndex, BigDecimal x)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setBigDecimal(
						parameterIndex, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBinaryStream(int, java.io.InputStream,
	 *      int)
	 */
	public void setBinaryStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setBinaryStream(
						parameterIndex, x, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBlob(int, java.sql.Blob)
	 */
	public void setBlob(int parameterIndex, Blob x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setBlob(parameterIndex,
						x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBoolean(int, boolean)
	 */
	public void setBoolean(int parameterIndex, boolean x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setBoolean(
						parameterIndex, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setByte(int, byte)
	 */
	public void setByte(int parameterIndex, byte x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setByte(parameterIndex,
						x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setBytes(int, byte[])
	 */
	public void setBytes(int parameterIndex, byte[] x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setBytes(parameterIndex,
						x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setCharacterStream(int, java.io.Reader,
	 *      int)
	 */
	public void setCharacterStream(int parameterIndex, Reader reader, int length)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setCharacterStream(
						parameterIndex, reader, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setClob(int, java.sql.Clob)
	 */
	public void setClob(int parameterIndex, Clob x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setClob(parameterIndex,
						x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date)
	 */
	public void setDate(int parameterIndex, Date x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setDate(parameterIndex,
						x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setDate(int, java.sql.Date,
	 *      java.util.Calendar)
	 */
	public void setDate(int parameterIndex, Date x, Calendar cal)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setDate(parameterIndex,
						x, cal);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setDouble(int, double)
	 */
	public void setDouble(int parameterIndex, double x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setDouble(
						parameterIndex, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setFloat(int, float)
	 */
	public void setFloat(int parameterIndex, float x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setFloat(parameterIndex,
						x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setInt(int, int)
	 */
	public void setInt(int parameterIndex, int x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt)
						.setInt(parameterIndex, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setLong(int, long)
	 */
	public void setLong(int parameterIndex, long x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setLong(parameterIndex,
						x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#getMetaData()
	 */
	public ResultSetMetaData getMetaData() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((PreparedStatement) this.wrappedStmt).getMetaData();
			}

			throw SQLError.createSQLException(
					"No operations allowed after statement closed",
					SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setNull(int, int)
	 */
	public void setNull(int parameterIndex, int sqlType) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setNull(parameterIndex,
						sqlType);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setNull(int, int, java.lang.String)
	 */
	public void setNull(int parameterIndex, int sqlType, String typeName)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setNull(parameterIndex,
						sqlType, typeName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object)
	 */
	public void setObject(int parameterIndex, Object x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setObject(
						parameterIndex, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int)
	 */
	public void setObject(int parameterIndex, Object x, int targetSqlType)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setObject(
						parameterIndex, x, targetSqlType);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setObject(int, java.lang.Object, int,
	 *      int)
	 */
	public void setObject(int parameterIndex, Object x, int targetSqlType,
			int scale) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setObject(
						parameterIndex, x, targetSqlType, scale);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#getParameterMetaData()
	 */
	public ParameterMetaData getParameterMetaData() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((PreparedStatement) this.wrappedStmt)
						.getParameterMetaData();
			}

			throw SQLError.createSQLException(
					"No operations allowed after statement closed",
					SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setRef(int, java.sql.Ref)
	 */
	public void setRef(int parameterIndex, Ref x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt)
						.setRef(parameterIndex, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setShort(int, short)
	 */
	public void setShort(int parameterIndex, short x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setShort(parameterIndex,
						x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setString(int, java.lang.String)
	 */
	public void setString(int parameterIndex, String x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setString(
						parameterIndex, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time)
	 */
	public void setTime(int parameterIndex, Time x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setTime(parameterIndex,
						x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setTime(int, java.sql.Time,
	 *      java.util.Calendar)
	 */
	public void setTime(int parameterIndex, Time x, Calendar cal)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setTime(parameterIndex,
						x, cal);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp)
	 */
	public void setTimestamp(int parameterIndex, Timestamp x)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setTimestamp(
						parameterIndex, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setTimestamp(int, java.sql.Timestamp,
	 *      java.util.Calendar)
	 */
	public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setTimestamp(
						parameterIndex, x, cal);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#setURL(int, java.net.URL)
	 */
	public void setURL(int parameterIndex, URL x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt)
						.setURL(parameterIndex, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/**
	 * DOCUMENT ME!
	 * 
	 * @param parameterIndex
	 *            DOCUMENT ME!
	 * @param x
	 *            DOCUMENT ME!
	 * @param length
	 *            DOCUMENT ME!
	 * 
	 * @throws SQLException
	 *             DOCUMENT ME!
	 * 
	 * @see java.sql.PreparedStatement#setUnicodeStream(int,
	 *      java.io.InputStream, int)
	 * @deprecated
	 */
	public void setUnicodeStream(int parameterIndex, InputStream x, int length)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setUnicodeStream(
						parameterIndex, x, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#addBatch()
	 */
	public void addBatch() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).addBatch();
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#clearParameters()
	 */
	public void clearParameters() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).clearParameters();
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.PreparedStatement#execute()
	 */
	public boolean execute() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((PreparedStatement) this.wrappedStmt).execute();
			}

			throw SQLError.createSQLException(
					"No operations allowed after statement closed",
					SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
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
	 * @see java.sql.PreparedStatement#executeQuery()
	 */
	public ResultSet executeQuery() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				ResultSet rs = ((PreparedStatement) this.wrappedStmt)
						.executeQuery();

				((com.mysql.jdbc.ResultSetInternalMethods) rs).setWrapperStatement(this);

				return rs;
			}

			throw SQLError.createSQLException(
					"No operations allowed after statement closed",
					SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
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
	 * @see java.sql.PreparedStatement#executeUpdate()
	 */
	public int executeUpdate() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((PreparedStatement) this.wrappedStmt).executeUpdate();
			}

			throw SQLError.createSQLException(
					"No operations allowed after statement closed",
					SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return -1; // we actually never get here, but the compiler can't figure

		// that out
	}

	public String toString() {
		StringBuffer buf = new StringBuffer();
		buf.append(super.toString());

		if (this.wrappedStmt != null) {
			buf.append(": "); //$NON-NLS-1$
			try {
				buf.append(((com.mysql.jdbc.PreparedStatement) this.wrappedStmt).asSql());
			} catch(SQLException sqlEx) {
				buf.append("EXCEPTION: " + sqlEx.toString());
			}
		}

		return buf.toString();
	}
//
//	public void setAsciiStream(int parameterIndex, InputStream x)
//			throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setAsciiStream(
//						parameterIndex, x);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setAsciiStream(int parameterIndex, InputStream x, long length)
//			throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setAsciiStream(
//						parameterIndex, x, length);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setBinaryStream(int parameterIndex, InputStream x)
//			throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setBinaryStream(
//						parameterIndex, x);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setBinaryStream(int parameterIndex, InputStream x, long length)
//			throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setBinaryStream(
//						parameterIndex, x, length);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setBlob(int parameterIndex, InputStream inputStream)
//			throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setBlob(parameterIndex,
//						inputStream);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setBlob(int parameterIndex, InputStream inputStream, long length)
//			throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setBlob(parameterIndex,
//						inputStream, length);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setCharacterStream(int parameterIndex, Reader reader)
//			throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setCharacterStream(
//						parameterIndex, reader);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setCharacterStream(int parameterIndex, Reader reader,
//			long length) throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setCharacterStream(
//						parameterIndex, reader, length);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setClob(int parameterIndex, Reader reader) throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setClob(parameterIndex,
//						reader);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setClob(int parameterIndex, Reader reader, long length)
//			throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setClob(parameterIndex,
//						reader, length);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setNCharacterStream(int parameterIndex, Reader value)
//			throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setNCharacterStream(
//						parameterIndex, value);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setNCharacterStream(int parameterIndex, Reader value,
//			long length) throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setNCharacterStream(
//						parameterIndex, value, length);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setNClob(int parameterIndex, NClob value) throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setNClob(parameterIndex,
//						value);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setNClob(int parameterIndex, Reader reader) throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setNClob(parameterIndex,
//						reader);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setNClob(int parameterIndex, Reader reader, long length)
//			throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setNClob(parameterIndex,
//						reader, length);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setNString(int parameterIndex, String value)
//			throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setNString(
//						parameterIndex, value);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setRowId(int parameterIndex, RowId x) throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setRowId(parameterIndex,
//						x);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
//			throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setSQLXML(
//						parameterIndex, xmlObject);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public boolean isClosed() throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				return ((PreparedStatement) this.wrappedStmt).isClosed();
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//
//		return true;
//	}
//
//	public boolean isPoolable() throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				return ((PreparedStatement) this.wrappedStmt).isPoolable();
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//
//		return false;
//	}
//
//	public void setPoolable(boolean poolable) throws SQLException {
//		try {
//			if (this.wrappedStmt != null) {
//				((PreparedStatement) this.wrappedStmt).setPoolable(poolable);
//			} else {
//				throw SQLError.createSQLException(
//						"No operations allowed after statement closed",
//						SQLError.SQL_STATE_GENERAL_ERROR);
//			}
//		} catch (SQLException sqlEx) {
//			checkAndFireConnectionError(sqlEx);
//		}
//	}
//
//	public boolean isWrapperFor(Class arg0) throws SQLException {
//		throw SQLError.notImplemented();
//	}
//
//	public Object unwrap(Class arg0) throws SQLException {
//		throw SQLError.notImplemented();
//	}
}
