/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

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

/**
 */
public class JDBC4ConnectionWrapper extends ConnectionWrapper {

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
	public JDBC4ConnectionWrapper(MysqlPooledConnection mysqlPooledConnection,
			com.mysql.jdbc.Connection mysqlConnection, boolean forXa)
			throws SQLException {
		super(mysqlPooledConnection, mysqlConnection, forXa);
	}

	public void close() throws SQLException {
		try {
			super.close();
		} finally {
			this.unwrappedInterfaces = null;
		}
	}
	
	public SQLXML createSQLXML() throws SQLException {
		checkClosed();

		try {
			return ((java.sql.Connection) this.mc).createSQLXML();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // never reached, but compiler can't tell
	}

	public java.sql.Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		checkClosed();

		try {
			return ((java.sql.Connection) this.mc).createArrayOf(typeName,
					elements);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // never reached, but compiler can't tell
	}

	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		checkClosed();

		try {
			return ((java.sql.Connection) this.mc).createStruct(typeName,
					attributes);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // never reached, but compiler can't tell
	}

	public Properties getClientInfo() throws SQLException {
		checkClosed();

		try {
			return ((java.sql.Connection) this.mc).getClientInfo();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // never reached, but compiler can't tell
	}

	public String getClientInfo(String name) throws SQLException {
		checkClosed();

		try {
			return ((java.sql.Connection) this.mc).getClientInfo(name);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // never reached, but compiler can't tell
	}

	/**
	 * Returns true if the connection has not been closed and is still valid.
	 * The driver shall submit a query on the connection or use some other
	 * mechanism that positively verifies the connection is still valid when
	 * this method is called.
	 * <p>
	 * The query submitted by the driver to validate the connection shall be
	 * executed in the context of the current transaction.
	 * 
	 * @param timeout -
	 *            The time in seconds to wait for the database operation used to
	 *            validate the connection to complete. If the timeout period
	 *            expires before the operation completes, this method returns
	 *            false. A value of 0 indicates a timeout is not applied to the
	 *            database operation.
	 *            <p>
	 * @return true if the connection is valid, false otherwise
	 * @exception SQLException
	 *                if the value supplied for <code>timeout</code> is less
	 *                then 0
	 * @since 1.6
	 */
	public synchronized boolean isValid(int timeout) throws SQLException {
		try {
			return ((java.sql.Connection) this.mc).isValid(timeout);
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return false; // never reached, but compiler can't tell
	}

	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		try {
			checkClosed();

			((java.sql.Connection) this.mc).setClientInfo(properties);
		} catch (SQLException sqlException) {
			try {
				checkAndFireConnectionError(sqlException);
			} catch (SQLException sqlEx2) {
				SQLClientInfoException clientEx = new SQLClientInfoException();
				clientEx.initCause(sqlEx2);

				throw clientEx;
			}
		}
	}

	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		try {
			checkClosed();

			((java.sql.Connection) this.mc).setClientInfo(name, value);
		} catch (SQLException sqlException) {
			try {
				checkAndFireConnectionError(sqlException);
			} catch (SQLException sqlEx2) {
				SQLClientInfoException clientEx = new SQLClientInfoException();
				clientEx.initCause(sqlEx2);

				throw clientEx;
			}
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
		checkClosed();

		boolean isInstance = iface.isInstance(this);

		if (isInstance) {
			return true;
		}

		return (iface.getName().equals("com.mysql.jdbc.Connection") || 
				iface.getName().equals("com.mysql.jdbc.ConnectionProperties"));
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
			if ("java.sql.Connection".equals(iface.getName())
					|| "java.sql.Wrapper.class".equals(iface.getName())) {
				return iface.cast(this);
			}
			
			if (unwrappedInterfaces == null) {
				unwrappedInterfaces = new HashMap();
			}
			
			Object cachedUnwrapped = unwrappedInterfaces.get(iface);
			
			if (cachedUnwrapped == null) {
				cachedUnwrapped = Proxy.newProxyInstance(this.mc.getClass()
						.getClassLoader(), new Class[] { iface },
						new ConnectionErrorFiringInvocationHandler(this.mc));
				unwrappedInterfaces.put(iface, cachedUnwrapped);
			}
			
			return iface.cast(cachedUnwrapped);
		} catch (ClassCastException cce) {
			throw SQLError.createSQLException("Unable to unwrap to "
					+ iface.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
		}
	}
	
	/**
	 * @see java.sql.Connection#createBlob()
	 */
	public Blob createBlob() throws SQLException {
		checkClosed();

		try {
			return ((java.sql.Connection) this.mc).createBlob();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // never reached, but compiler can't tell
	}

	/**
	 * @see java.sql.Connection#createClob()
	 */
	public Clob createClob() throws SQLException {
		checkClosed();

		try {
			return ((java.sql.Connection) this.mc).createClob();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // never reached, but compiler can't tell
	}

	/**
	 * @see java.sql.Connection#createNClob()
	 */
	public NClob createNClob() throws SQLException {
		checkClosed();

		try {
			return ((java.sql.Connection) this.mc).createNClob();
		} catch (SQLException sqlException) {
			checkAndFireConnectionError(sqlException);
		}

		return null; // never reached, but compiler can't tell
	}
}
