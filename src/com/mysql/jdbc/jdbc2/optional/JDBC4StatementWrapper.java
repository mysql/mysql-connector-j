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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
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
public class JDBC4StatementWrapper extends StatementWrapper {

	public JDBC4StatementWrapper(ConnectionWrapper c, 
			MysqlPooledConnection conn,
			Statement toWrap) {
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
		
		return false; // We never get here, compiler can't tell
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
		
		return false; // We never get here, compiler can't tell
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
					|| "java.sql.Wrapper.class".equals(iface.getName())) {
				return iface.cast(this);
			}
			
			if (unwrappedInterfaces == null) {
				unwrappedInterfaces = new HashMap();
			}
			
			Object cachedUnwrapped = unwrappedInterfaces.get(iface);
			
			if (cachedUnwrapped == null) {
				cachedUnwrapped = Proxy.newProxyInstance(
						this.wrappedStmt.getClass().getClassLoader(), 
						new Class[] { iface },
						new ConnectionErrorFiringInvocationHandler(this.wrappedStmt));
				unwrappedInterfaces.put(iface, cachedUnwrapped);
			}
			
			return iface.cast(cachedUnwrapped);
		} catch (ClassCastException cce) {
			throw SQLError.createSQLException("Unable to unwrap to "
					+ iface.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		}
	}
}
