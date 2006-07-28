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

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import com.mysql.jdbc.SQLError;

/**
 * Wraps callable statements created by pooled connections.
 * 
 * @version $Id: CallableStatementWrapper.java,v 1.1.2.1 2005/05/13 18:58:38
 *          mmatthews Exp $
 */
public class CallableStatementWrapper extends PreparedStatementWrapper
		implements CallableStatement {

	/**
	 * @param c
	 * @param conn
	 * @param toWrap
	 */
	public CallableStatementWrapper(ConnectionWrapper c,
			MysqlPooledConnection conn, CallableStatement toWrap) {
		super(c, conn, toWrap);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#registerOutParameter(int, int)
	 */
	public void registerOutParameter(int parameterIndex, int sqlType)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).registerOutParameter(
						parameterIndex, sqlType);
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
	 * @see java.sql.CallableStatement#registerOutParameter(int, int, int)
	 */
	public void registerOutParameter(int parameterIndex, int sqlType, int scale)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).registerOutParameter(
						parameterIndex, sqlType, scale);
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
	 * @see java.sql.CallableStatement#wasNull()
	 */
	public boolean wasNull() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).wasNull();
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getString(int)
	 */
	public String getString(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getString(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBoolean(int)
	 */
	public boolean getBoolean(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getBoolean(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getByte(int)
	 */
	public byte getByte(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getByte(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getShort(int)
	 */
	public short getShort(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getShort(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getInt(int)
	 */
	public int getInt(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getInt(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getLong(int)
	 */
	public long getLong(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getLong(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getFloat(int)
	 */
	public float getFloat(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getFloat(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getDouble(int)
	 */
	public double getDouble(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getDouble(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBigDecimal(int, int)
	 */
	public BigDecimal getBigDecimal(int parameterIndex, int scale)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getBigDecimal(
						parameterIndex, scale);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBytes(int)
	 */
	public byte[] getBytes(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getBytes(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getDate(int)
	 */
	public Date getDate(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getDate(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTime(int)
	 */
	public Time getTime(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getTime(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTimestamp(int)
	 */
	public Timestamp getTimestamp(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getTimestamp(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getObject(int)
	 */
	public Object getObject(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getObject(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBigDecimal(int)
	 */
	public BigDecimal getBigDecimal(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getBigDecimal(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getObject(int, java.util.Map)
	 */
	public Object getObject(int parameterIndex, Map typeMap)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getObject(
						parameterIndex, typeMap);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getRef(int)
	 */
	public Ref getRef(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getRef(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBlob(int)
	 */
	public Blob getBlob(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getBlob(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getClob(int)
	 */
	public Clob getClob(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getClob(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getArray(int)
	 */
	public Array getArray(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getArray(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getDate(int, java.util.Calendar)
	 */
	public Date getDate(int parameterIndex, Calendar cal) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getDate(
						parameterIndex, cal);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTime(int, java.util.Calendar)
	 */
	public Time getTime(int parameterIndex, Calendar cal) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getTime(
						parameterIndex, cal);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTimestamp(int, java.util.Calendar)
	 */
	public Timestamp getTimestamp(int parameterIndex, Calendar cal)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getTimestamp(
						parameterIndex, cal);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#registerOutParameter(int, int,
	 *      java.lang.String)
	 */
	public void registerOutParameter(int paramIndex, int sqlType,
			String typeName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).registerOutParameter(
						paramIndex, sqlType, typeName);
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
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String,
	 *      int)
	 */
	public void registerOutParameter(String parameterName, int sqlType)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).registerOutParameter(
						parameterName, sqlType);
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
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String,
	 *      int, int)
	 */
	public void registerOutParameter(String parameterName, int sqlType,
			int scale) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).registerOutParameter(
						parameterName, sqlType, scale);
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
	 * @see java.sql.CallableStatement#registerOutParameter(java.lang.String,
	 *      int, java.lang.String)
	 */
	public void registerOutParameter(String parameterName, int sqlType,
			String typeName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).registerOutParameter(
						parameterName, sqlType, typeName);
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
	 * @see java.sql.CallableStatement#getURL(int)
	 */
	public URL getURL(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getURL(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#setURL(java.lang.String, java.net.URL)
	 */
	public void setURL(String parameterName, URL val) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setURL(parameterName,
						val);
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
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int)
	 */
	public void setNull(String parameterName, int sqlType) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setNull(parameterName,
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
	 * @see java.sql.CallableStatement#setBoolean(java.lang.String, boolean)
	 */
	public void setBoolean(String parameterName, boolean x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setBoolean(
						parameterName, x);
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
	 * @see java.sql.CallableStatement#setByte(java.lang.String, byte)
	 */
	public void setByte(String parameterName, byte x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt)
						.setByte(parameterName, x);
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
	 * @see java.sql.CallableStatement#setShort(java.lang.String, short)
	 */
	public void setShort(String parameterName, short x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setShort(parameterName,
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
	 * @see java.sql.CallableStatement#setInt(java.lang.String, int)
	 */
	public void setInt(String parameterName, int x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setInt(parameterName, x);
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
	 * @see java.sql.CallableStatement#setLong(java.lang.String, long)
	 */
	public void setLong(String parameterName, long x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt)
						.setLong(parameterName, x);
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
	 * @see java.sql.CallableStatement#setFloat(java.lang.String, float)
	 */
	public void setFloat(String parameterName, float x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setFloat(parameterName,
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
	 * @see java.sql.CallableStatement#setDouble(java.lang.String, double)
	 */
	public void setDouble(String parameterName, double x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setDouble(parameterName,
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
	 * @see java.sql.CallableStatement#setBigDecimal(java.lang.String,
	 *      java.math.BigDecimal)
	 */
	public void setBigDecimal(String parameterName, BigDecimal x)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setBigDecimal(
						parameterName, x);
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
	 * @see java.sql.CallableStatement#setString(java.lang.String,
	 *      java.lang.String)
	 */
	public void setString(String parameterName, String x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setString(parameterName,
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
	 * @see java.sql.CallableStatement#setBytes(java.lang.String, byte[])
	 */
	public void setBytes(String parameterName, byte[] x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setBytes(parameterName,
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
	 * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date)
	 */
	public void setDate(String parameterName, Date x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt)
						.setDate(parameterName, x);
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
	 * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time)
	 */
	public void setTime(String parameterName, Time x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt)
						.setTime(parameterName, x);
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
	 * @see java.sql.CallableStatement#setTimestamp(java.lang.String,
	 *      java.sql.Timestamp)
	 */
	public void setTimestamp(String parameterName, Timestamp x)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setTimestamp(
						parameterName, x);
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
	 * @see java.sql.CallableStatement#setAsciiStream(java.lang.String,
	 *      java.io.InputStream, int)
	 */
	public void setAsciiStream(String parameterName, InputStream x, int length)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setAsciiStream(
						parameterName, x, length);
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
	 * @see java.sql.CallableStatement#setBinaryStream(java.lang.String,
	 *      java.io.InputStream, int)
	 */
	public void setBinaryStream(String parameterName, InputStream x, int length)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setBinaryStream(
						parameterName, x, length);
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
	 * @see java.sql.CallableStatement#setObject(java.lang.String,
	 *      java.lang.Object, int, int)
	 */
	public void setObject(String parameterName, Object x, int targetSqlType,
			int scale) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setObject(parameterName,
						x, targetSqlType, scale);
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
	 * @see java.sql.CallableStatement#setObject(java.lang.String,
	 *      java.lang.Object, int)
	 */
	public void setObject(String parameterName, Object x, int targetSqlType)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setObject(parameterName,
						x, targetSqlType);
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
	 * @see java.sql.CallableStatement#setObject(java.lang.String,
	 *      java.lang.Object)
	 */
	public void setObject(String parameterName, Object x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setObject(parameterName,
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
	 * @see java.sql.CallableStatement#setCharacterStream(java.lang.String,
	 *      java.io.Reader, int)
	 */
	public void setCharacterStream(String parameterName, Reader reader,
			int length) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setCharacterStream(
						parameterName, reader, length);
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
	 * @see java.sql.CallableStatement#setDate(java.lang.String, java.sql.Date,
	 *      java.util.Calendar)
	 */
	public void setDate(String parameterName, Date x, Calendar cal)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setDate(parameterName,
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
	 * @see java.sql.CallableStatement#setTime(java.lang.String, java.sql.Time,
	 *      java.util.Calendar)
	 */
	public void setTime(String parameterName, Time x, Calendar cal)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setTime(parameterName,
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
	 * @see java.sql.CallableStatement#setTimestamp(java.lang.String,
	 *      java.sql.Timestamp, java.util.Calendar)
	 */
	public void setTimestamp(String parameterName, Timestamp x, Calendar cal)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setTimestamp(
						parameterName, x, cal);
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
	 * @see java.sql.CallableStatement#setNull(java.lang.String, int,
	 *      java.lang.String)
	 */
	public void setNull(String parameterName, int sqlType, String typeName)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setNull(parameterName,
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
	 * @see java.sql.CallableStatement#getString(int)
	 */
	public String getString(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getString(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBoolean(int)
	 */
	public boolean getBoolean(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getBoolean(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return false;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getByte(int)
	 */
	public byte getByte(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getByte(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getShort(int)
	 */
	public short getShort(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getShort(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getInt(int)
	 */
	public int getInt(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getInt(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getLong(int)
	 */
	public long getLong(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getLong(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getFloat(int)
	 */
	public float getFloat(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getFloat(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getDouble(int)
	 */
	public double getDouble(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getDouble(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return 0;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBytes(int)
	 */
	public byte[] getBytes(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getBytes(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getDate(int)
	 */
	public Date getDate(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getDate(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTime(int)
	 */
	public Time getTime(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getTime(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTimestamp(int)
	 */
	public Timestamp getTimestamp(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getTimestamp(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getObject(int)
	 */
	public Object getObject(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getObject(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBigDecimal(int)
	 */
	public BigDecimal getBigDecimal(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getBigDecimal(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getObject(int, java.util.Map)
	 */
	public Object getObject(String parameterName, Map typeMap)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getObject(
						parameterName, typeMap);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getRef(int)
	 */
	public Ref getRef(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getRef(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getBlob(int)
	 */
	public Blob getBlob(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getBlob(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getClob(int)
	 */
	public Clob getClob(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getClob(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getArray(int)
	 */
	public Array getArray(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getArray(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getDate(int, java.util.Calendar)
	 */
	public Date getDate(String parameterName, Calendar cal) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getDate(
						parameterName, cal);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTime(int, java.util.Calendar)
	 */
	public Time getTime(String parameterName, Calendar cal) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getTime(
						parameterName, cal);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getTimestamp(int, java.util.Calendar)
	 */
	public Timestamp getTimestamp(String parameterName, Calendar cal)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getTimestamp(
						parameterName, cal);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		return null;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.CallableStatement#getURL(java.lang.String)
	 */
	public URL getURL(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt)
						.getURL(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}

		return null;
	}

}
