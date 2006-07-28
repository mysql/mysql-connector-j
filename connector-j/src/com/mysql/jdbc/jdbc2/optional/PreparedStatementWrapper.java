/*
 Copyright (C) 2002-2004 MySQL AB

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

import com.mysql.jdbc.SQLError;

import java.io.InputStream;
import java.io.Reader;

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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
					SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						parameterIndex, x, scale);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
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
					SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
						SQLError.SQL_STATE_GENERAL_ERROR);
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
					SQLError.SQL_STATE_GENERAL_ERROR);
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

				((com.mysql.jdbc.ResultSet) rs).setWrapperStatement(this);

				return rs;
			}

			throw SQLError.createSQLException(
					"No operations allowed after statement closed",
					SQLError.SQL_STATE_GENERAL_ERROR);
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
					SQLError.SQL_STATE_GENERAL_ERROR);
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return -1; // we actually never get here, but the compiler can't figure

		// that out
	}
}
