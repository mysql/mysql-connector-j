/*
 Copyright (C) 2002-2007 MySQL AB

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

package com.mysql.jdbc;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.NClob;
import java.sql.Struct;
import java.util.Properties;


import com.mysql.jdbc.Connection;
import com.mysql.jdbc.exceptions.NotYetImplementedException;

public class JDBC4Connection extends Connection {

	public JDBC4Connection(String hostToConnectTo, int portToConnectTo, Properties info, String databaseToConnectTo, String url) throws SQLException {
		super(hostToConnectTo, portToConnectTo, info, databaseToConnectTo, url);
		// TODO Auto-generated constructor stub
	}

	public SQLXML createSQLXML() throws SQLException {
		return new JDBC4MysqlSQLXML();
	}
	
	public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
		throw new NotYetImplementedException();
	}

	public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
		throw new NotYetImplementedException();
	}

	public Properties getClientInfo() throws SQLException {
		throw new NotYetImplementedException();
	}

	public String getClientInfo(String name) throws SQLException {
		throw new NotYetImplementedException();
	}

	public boolean isValid(int timeout) throws SQLException {
		throw new NotYetImplementedException();
	}

	public void setClientInfo(Properties properties) throws SQLClientInfoException {
		throw new NotYetImplementedException();
	}

	public void setClientInfo(String name, String value) throws SQLClientInfoException {
		throw new NotYetImplementedException();
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
            		SQLError.SQL_STATE_ILLEGAL_ARGUMENT);
        }
    }
    
	/**
	 * @see java.sql.Connection#createBlob()
	 */
	public Blob createBlob() {
	    return new com.mysql.jdbc.Blob();
	}

	/**
	 * @see java.sql.Connection#createClob()
	 */
	public Clob createClob() {
	    return new com.mysql.jdbc.Clob();
	}

	/**
	 * @see java.sql.Connection#createNClob()
	 */
	public NClob createNClob() {
	    return new com.mysql.jdbc.JDBC4NClob();
	}
}
