/*
 Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems

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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.RowId;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.SQLError;

import com.mysql.jdbc.jdbc2.optional.ConnectionWrapper;
import com.mysql.jdbc.jdbc2.optional.MysqlPooledConnection;

/**
 */
public class JDBC4CallableStatementWrapper extends CallableStatementWrapper {

	public JDBC4CallableStatementWrapper(ConnectionWrapper c, MysqlPooledConnection conn,
			CallableStatement toWrap) {
		super(c, conn, toWrap);
	}
	
	public void close() throws SQLException {
		try {
			super.close();
		} finally {
			this.unwrappedInterfaces = null;
		}
	}
	
	public boolean isClosed() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.isClosed();
			} else {
				throw SQLError.createSQLException("Statement already closed",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return false; // never get here - compiler can't tell
	}
	
	public void setPoolable(boolean poolable) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				this.wrappedStmt.setPoolable(poolable);
			} else {
				throw SQLError.createSQLException("Statement already closed",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
	
	public boolean isPoolable() throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return this.wrappedStmt.isPoolable();
			} else {
				throw SQLError.createSQLException("Statement already closed",
						SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return false; // never get here - compiler can't tell
	}
    
	public void setRowId(int parameterIndex, RowId x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setRowId(parameterIndex,
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
	
	public void setNClob(int parameterIndex, NClob value) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setNClob(parameterIndex,
						value);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	public void setSQLXML(int parameterIndex, SQLXML xmlObject)
			throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setSQLXML(parameterIndex,
						xmlObject);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
	
	
	public void setNString(int parameterIndex,
            String value)
            throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setNString(parameterIndex,
						value);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
            
    public void setNCharacterStream(int parameterIndex,
                    Reader value,
                    long length)
                    throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setNCharacterStream(parameterIndex,
						value, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
    }

    public void setClob(int parameterIndex,
            Reader reader,
            long length)
            throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setClob(parameterIndex,
						reader, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}	
    }
    
    public void setBlob(int parameterIndex,
            InputStream inputStream,
            long length)
            throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setBlob(parameterIndex,
						inputStream, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
    }
    
    public void setNClob(int parameterIndex,
            Reader reader,
            long length)
            throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setNClob(parameterIndex,
						reader, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
    }
    
    public void setAsciiStream(int parameterIndex,
            InputStream x,
            long length)
            throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setAsciiStream(parameterIndex,
						x, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
    }
    
    public void setBinaryStream(int parameterIndex,
            InputStream x,
            long length)
            throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setBinaryStream(parameterIndex,
						x, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
    }
    
    public void setCharacterStream(int parameterIndex,
            Reader reader,
            long length)
            throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setCharacterStream(parameterIndex,
						reader, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
    }
    
    public void setAsciiStream(int parameterIndex,
            InputStream x)
            throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setAsciiStream(parameterIndex,
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
    
    public void setBinaryStream(int parameterIndex,
            InputStream x)
            throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setBinaryStream(parameterIndex,
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
    
    public void setCharacterStream(int parameterIndex,
            Reader reader)
            throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setCharacterStream(parameterIndex,
						reader);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
    	
    }
    
    public void setNCharacterStream(int parameterIndex,
            Reader value)
            throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setNCharacterStream(parameterIndex,
						value);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
    	
    }
    
    public void setClob(int parameterIndex,
            Reader reader)
            throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setClob(parameterIndex,
						reader);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
    	
    }
    
    public void setBlob(int parameterIndex,
            InputStream inputStream)
            throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setBlob(parameterIndex,
						inputStream);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
    }
    
    public void setNClob(int parameterIndex,
            Reader reader)
            throws SQLException {
    	try {
			if (this.wrappedStmt != null) {
				((PreparedStatement) this.wrappedStmt).setNClob(parameterIndex,
						reader);
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
	 * Returns true if this either implements the interface argument or is
	 * directly or indirectly a wrapper for an object that does. Returns false
	 * otherwise. If this implements the interface then return true, else if
	 * this is a wrapper then return the result of recursively calling
	 * <code>isWrapperFor</code> on the wrapped object. If this does not
	 * implement the interface and is not a wrapper, return false. This method
	 * should be implemented as a low-cost operation compared to
	 * <code>unwrap</code> so that callers can use this method to avoid
	 * expensive <code>unwrap</code> calls that may fail. If this method
	 * returns true then calling <code>unwrap</code> with the same argument
	 * should succeed.
	 * 
	 * @param interfaces
	 *            a Class defining an interface.
	 * @return true if this implements the interface or directly or indirectly
	 *         wraps an object that does.
	 * @throws java.sql.SQLException
	 *             if an error occurs while determining whether this is a
	 *             wrapper for an object with the given interface.
	 * @since 1.6
	 */
	public boolean isWrapperFor(Class<?> iface) throws SQLException {

		boolean isInstance = iface.isInstance(this);

		if (isInstance) {
			return true;
		}

		String interfaceClassName = iface.getName();
		
		return (interfaceClassName.equals("com.mysql.jdbc.Statement")
				|| interfaceClassName.equals("java.sql.Statement")
				|| interfaceClassName.equals("java.sql.PreparedStatement")
				|| interfaceClassName.equals("java.sql.Wrapper"));
	}

	/**
	 * Returns an object that implements the given interface to allow access to
	 * non-standard methods, or standard methods not exposed by the proxy. The
	 * result may be either the object found to implement the interface or a
	 * proxy for that object. If the receiver implements the interface then that
	 * is the object. If the receiver is a wrapper and the wrapped object
	 * implements the interface then that is the object. Otherwise the object is
	 * the result of calling <code>unwrap</code> recursively on the wrapped
	 * object. If the receiver is not a wrapper and does not implement the
	 * interface, then an <code>SQLException</code> is thrown.
	 * 
	 * @param iface
	 *            A Class defining an interface that the result must implement.
	 * @return an object that implements the interface. May be a proxy for the
	 *         actual implementing object.
	 * @throws java.sql.SQLException
	 *             If no object found that implements the interface
	 * @since 1.6
	 */
	public synchronized <T> T unwrap(java.lang.Class<T> iface)
			throws java.sql.SQLException {
		try {
			if ("java.sql.Statement".equals(iface.getName()) 
					|| "java.sql.PreparedStatement".equals(iface.getName()) 
					|| "java.sql.Wrapper.class".equals(iface.getName())) {
				return iface.cast(this);
			}
			
			if (unwrappedInterfaces == null) {
				unwrappedInterfaces = new HashMap();
			}
			
			Object cachedUnwrapped = unwrappedInterfaces.get(iface);
			
			if (cachedUnwrapped == null) {
				if (cachedUnwrapped == null) {
					cachedUnwrapped = Proxy.newProxyInstance(
							this.wrappedStmt.getClass().getClassLoader(), 
							new Class[] { iface },
							new ConnectionErrorFiringInvocationHandler(this.wrappedStmt));
					unwrappedInterfaces.put(iface, cachedUnwrapped);
				}
				unwrappedInterfaces.put(iface, cachedUnwrapped);
			}
			
			return iface.cast(cachedUnwrapped);
		} catch (ClassCastException cce) {
			throw SQLError.createSQLException("Unable to unwrap to "
					+ iface.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		}
	}
	
	public void setRowId(String parameterName, RowId x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setRowId(parameterName, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	public void setSQLXML(String parameterName, SQLXML xmlObject) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setSQLXML(parameterName, xmlObject);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
	
	public SQLXML getSQLXML(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getSQLXML(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return null;

	}

	public SQLXML getSQLXML(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getSQLXML(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return null;
	}

	public RowId getRowId(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getRowId(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return null;
	}
	
	public void setNClob(String parameterName, NClob value) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setNClob(parameterName, value);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	public void setNClob(String parameterName, Reader reader) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setNClob(parameterName, reader);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	public void setNClob(String parameterName, Reader reader, long length) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setNClob(parameterName, reader, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	public void setNString(String parameterName, String value) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setNString(parameterName, value);
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
	 * @see java.sql.CallableStatement#getCharacterStream(int)
	 */
	public Reader getCharacterStream(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getCharacterStream(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return null;
	}

	/**
	 * @see java.sql.CallableStatement#getCharacterStream(java.lang.String)
	 */
	public Reader getCharacterStream(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getCharacterStream(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return null;
	}

	/**
	 * @see java.sql.CallableStatement#getNCharacterStream(int)
	 */
	public Reader getNCharacterStream(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getNCharacterStream(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return null;
	}

	/**
	 * @see java.sql.CallableStatement#getNCharacterStream(java.lang.String)
	 */
	public Reader getNCharacterStream(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getNCharacterStream(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return null;
	}

	/**
	 * @see java.sql.CallableStatement#getNClob(java.lang.String)
	 */
	public NClob getNClob(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getNClob(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return null;
	}

	/**
	 * @see java.sql.CallableStatement#getNString(java.lang.String)
	 */
	public String getNString(String parameterName) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getNString(parameterName);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return null;
	}
	
	public void setAsciiStream(String parameterName, InputStream x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setAsciiStream(parameterName, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
	
	public void setAsciiStream(String parameterName, InputStream x, long length) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setAsciiStream(parameterName, x, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
	
	public void setBinaryStream(String parameterName, InputStream x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setBinaryStream(parameterName, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
	
	public void setBinaryStream(String parameterName, InputStream x, long length) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setBinaryStream(parameterName, x, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
	
	public void setBlob(String parameterName, InputStream x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setBlob(parameterName, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
	
	public void setBlob(String parameterName, InputStream x, long length) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setBlob(parameterName, x, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
	
	public void setBlob(String parameterName, Blob x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setBlob(parameterName, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
	
	public void setCharacterStream(String parameterName, Reader reader) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setCharacterStream(parameterName, reader);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	public void setCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setCharacterStream(parameterName, reader, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	public void setClob(String parameterName, Clob x) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setClob(parameterName, x);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	public void setClob(String parameterName, Reader reader) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setClob(parameterName, reader);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	public void setClob(String parameterName, Reader reader, long length) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setClob(parameterName, reader, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
	
	public void setNCharacterStream(String parameterName, Reader reader) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setNCharacterStream(parameterName, reader);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}

	public void setNCharacterStream(String parameterName, Reader reader, long length) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				((CallableStatement) this.wrappedStmt).setNCharacterStream(parameterName, reader, length);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
	}
	
	public NClob getNClob(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getNClob(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return null;
	}
	
	public String getNString(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getNString(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return null;
	}
	
	public RowId getRowId(int parameterIndex) throws SQLException {
		try {
			if (this.wrappedStmt != null) {
				return ((CallableStatement) this.wrappedStmt).getRowId(parameterIndex);
			} else {
				throw SQLError.createSQLException(
						"No operations allowed after statement closed",
						SQLError.SQL_STATE_GENERAL_ERROR, this.exceptionInterceptor);
			}
		} catch (SQLException sqlEx) {
			checkAndFireConnectionError(sqlEx);
		}
		
		return null;
	}
}
