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

package com.mysql.jdbc;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.NClob;
import java.sql.Struct;
import java.util.Properties;
import java.util.TimerTask;


import com.mysql.jdbc.ConnectionImpl;
import com.mysql.jdbc.Messages;
import com.mysql.jdbc.SQLError;


public class JDBC4Connection extends ConnectionImpl {
	private JDBC4ClientInfoProvider infoProvider;
	
	public JDBC4Connection(String hostToConnectTo, int portToConnectTo, Properties info, String databaseToConnectTo, String url) throws SQLException {
		super(hostToConnectTo, portToConnectTo, info, databaseToConnectTo, url);
		// TODO Auto-generated constructor stub
	}

	public SQLXML createSQLXML() throws SQLException {
		return new JDBC4MysqlSQLXML(getExceptionInterceptor());
	}
	
	public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw SQLError.notImplemented();
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw SQLError.notImplemented();
	}

	public Properties getClientInfo() throws SQLException {
		return getClientInfoProviderImpl().getClientInfo(this);
	}

	public String getClientInfo(String name) throws SQLException {
		return getClientInfoProviderImpl().getClientInfo(this, name);
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
	 * @param timeout -		The time in seconds to wait for the database operation 
	 * 						used to validate the connection to complete.  If 
	 * 						the timeout period expires before the operation 
	 * 						completes, this method returns false.  A value of 
	 * 						0 indicates a timeout is not applied to the 
	 * 						database operation.
	 * <p>
	 * @return true if the connection is valid, false otherwise
         * @exception SQLException if the value supplied for <code>timeout</code> 
         * is less then 0
         * @since 1.6
	 */
	public boolean isValid(int timeout) throws SQLException {
		synchronized (getConnectionMutex()) {
			if (isClosed()) {
				return false;
			}
			
			try {
				try {
					pingInternal(false, timeout * 1000);
				} catch (Throwable t) {
					try {
						abortInternal();
					} catch (Throwable ignoreThrown) {
						// we're dead now anyway
					}
					
					return false;
				}
	
			} catch (Throwable t) {
				return false;
			}
			
			return true;
		}
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		try {
			getClientInfoProviderImpl().setClientInfo(this, properties);
		} catch (SQLClientInfoException ciEx) {
			throw ciEx;
		} catch (SQLException sqlEx) {
			SQLClientInfoException clientInfoEx = new SQLClientInfoException();
			clientInfoEx.initCause(sqlEx);

			throw clientInfoEx;
		}
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		try {
			getClientInfoProviderImpl().setClientInfo(this, name, value);
		} catch (SQLClientInfoException ciEx) {
			throw ciEx;
		} catch (SQLException sqlEx) {
			SQLClientInfoException clientInfoEx = new SQLClientInfoException();
			clientInfoEx.initCause(sqlEx);

			throw clientInfoEx;
		}
	}

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper
     * for an object that does. Returns false otherwise. If this implements the interface then return true,
     * else if this is a wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped
     * object. If this does not implement the interface and is not a wrapper, return false.
     * This method should be implemented as a low-cost operation compared to <code>unwrap</code> so that
     * callers can use this method to avoid expensive <code>unwrap</code> calls that may fail. If this method
     * returns true then calling <code>unwrap</code> with the same argument should succeed.
     *
     * @param interfaces a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException  if an error occurs while determining whether this is a wrapper
     * for an object with the given interface.
     * @since 1.6
     */
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		checkClosed();
		
		// This works for classes that aren't actually wrapping
		// anything
		return iface.isInstance(this);
	}

    /**
     * Returns an object that implements the given interface to allow access to non-standard methods,
     * or standard methods not exposed by the proxy.
     * The result may be either the object found to implement the interface or a proxy for that object.
     * If the receiver implements the interface then that is the object. If the receiver is a wrapper
     * and the wrapped object implements the interface then that is the object. Otherwise the object is
     *  the result of calling <code>unwrap</code> recursively on the wrapped object. If the receiver is not a
     * wrapper and does not implement the interface, then an <code>SQLException</code> is thrown.
     *
     * @param iface A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException If no object found that implements the interface 
     * @since 1.6
     */
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
    	try {
    		// This works for classes that aren't actually wrapping
    		// anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException("Unable to unwrap to " + iface.toString(), 
            		SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }
    
	/**
	 * @see java.sql.Connection#createBlob()
	 */
	public Blob createBlob() {
	    return new com.mysql.jdbc.Blob(getExceptionInterceptor());
	}

	/**
	 * @see java.sql.Connection#createClob()
	 */
	public Clob createClob() {
	    return new com.mysql.jdbc.Clob(getExceptionInterceptor());
	}

	/**
	 * @see java.sql.Connection#createNClob()
	 */
	public NClob createNClob() {
	    return new com.mysql.jdbc.JDBC4NClob(getExceptionInterceptor());
	}
	
	protected JDBC4ClientInfoProvider getClientInfoProviderImpl() throws SQLException {
		synchronized (getConnectionMutex()) {
			if (this.infoProvider == null) {
				try {
					try {
						this.infoProvider = (JDBC4ClientInfoProvider)Util.getInstance(getClientInfoProvider(), 
								new Class[0], new Object[0], getExceptionInterceptor());
					} catch (SQLException sqlEx) {
						if (sqlEx.getCause() instanceof ClassCastException) {
							// try with package name prepended
							this.infoProvider = (JDBC4ClientInfoProvider)Util.getInstance(
									"com.mysql.jdbc." + getClientInfoProvider(), 
									new Class[0], new Object[0], getExceptionInterceptor());
						}
					}
				} catch (ClassCastException cce) {
					throw SQLError.createSQLException(Messages
							.getString("JDBC4Connection.ClientInfoNotImplemented", new Object[] {getClientInfoProvider()}), 
							SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
				}
				
				this.infoProvider.initialize(this, this.props);
			}
			
			return this.infoProvider;
		}
	}
}
