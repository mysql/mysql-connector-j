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

import java.sql.Connection;
import java.sql.SQLException;

import java.util.Enumeration;
import java.util.Hashtable;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;

import com.mysql.jdbc.SQLError;

/**
 * This class is used to wrap and return a physical connection within a logical
 * handle. It also registers and notifies ConnectionEventListeners of any
 * ConnectionEvents
 * 
 * @see javax.sql.PooledConnection
 * @see org.gjt.mm.mysql.jdbc2.optional.LogicalHandle
 * @author Todd Wolff <todd.wolff_at_prodigy.net>
 */
public class MysqlPooledConnection implements PooledConnection {

	/**
	 * The flag for an exception being thrown.
	 */
	public static final int CONNECTION_ERROR_EVENT = 1;

	/**
	 * The flag for a connection being closed.
	 */
	public static final int CONNECTION_CLOSED_EVENT = 2;

	// ~ Instance/static variables .............................................

	private Hashtable eventListeners;

	private Connection logicalHandle;

	private com.mysql.jdbc.Connection physicalConn;

	// ~ Constructors ..........................................................

	/**
	 * Construct a new MysqlPooledConnection and set instance variables
	 * 
	 * @param connection
	 *            physical connection to db
	 */
	public MysqlPooledConnection(com.mysql.jdbc.Connection connection) {
		this.logicalHandle = null;
		this.physicalConn = connection;
		this.eventListeners = new Hashtable(10);
	}

	// ~ Methods ...............................................................

	/**
	 * Adds ConnectionEventListeners to a hash table to be used for notification
	 * of ConnectionEvents
	 * 
	 * @param connectioneventlistener
	 *            listener to be notified with ConnectionEvents
	 */
	public synchronized void addConnectionEventListener(
			ConnectionEventListener connectioneventlistener) {

		if (this.eventListeners != null) {
			this.eventListeners.put(connectioneventlistener,
					connectioneventlistener);
		}
	}

	/**
	 * Removes ConnectionEventListeners from hash table used for notification of
	 * ConnectionEvents
	 * 
	 * @param connectioneventlistener
	 *            listener to be removed
	 */
	public synchronized void removeConnectionEventListener(
			ConnectionEventListener connectioneventlistener) {

		if (this.eventListeners != null) {
			this.eventListeners.remove(connectioneventlistener);
		}
	}

	/**
	 * Invoked by the container. Return a logicalHandle object that wraps a
	 * physical connection.
	 * 
	 * @see java.sql.DataSource#getConnection()
	 */
	public synchronized Connection getConnection() throws SQLException {
		return getConnection(true, false);
		
	}
	
	protected synchronized Connection getConnection(boolean resetServerState, 
			boolean forXa)
		throws SQLException {
		if (this.physicalConn == null) {

			SQLException sqlException = SQLError.createSQLException(
					"Physical Connection doesn't exist");
			callListener(CONNECTION_ERROR_EVENT, sqlException);

			throw sqlException;
		}

		try {

			if (this.logicalHandle != null) {
				((ConnectionWrapper) this.logicalHandle).close(false);
			}

			if (resetServerState) {
				((com.mysql.jdbc.Connection) this.physicalConn).resetServerState();
			}

			this.logicalHandle = new ConnectionWrapper(this, this.physicalConn, forXa);
		} catch (SQLException sqlException) {
			callListener(CONNECTION_ERROR_EVENT, sqlException);

			throw sqlException;
		}

		return this.logicalHandle;
	}

	/**
	 * Invoked by the container (not the client), and should close the physical
	 * connection. This will be called if the pool is destroyed or the
	 * connectionEventListener receives a connectionErrorOccurred event.
	 * 
	 * @see java.sql.DataSource#close()
	 */
	public synchronized void close() throws SQLException {
		if (this.physicalConn != null) {
			this.physicalConn.close();
		}

		this.physicalConn = null;
	}

	/**
	 * Notifies all registered ConnectionEventListeners of ConnectionEvents.
	 * Instantiates a new ConnectionEvent which wraps sqlException and invokes
	 * either connectionClose or connectionErrorOccurred on listener as
	 * appropriate.
	 * 
	 * @param eventType
	 *            value indicating whether connectionClosed or
	 *            connectionErrorOccurred called
	 * @param sqlException
	 *            the exception being thrown
	 */
	protected synchronized void callListener(int eventType,
			SQLException sqlException) {

		if (this.eventListeners == null) {

			return;
		}

		Enumeration enumeration = this.eventListeners.keys();
		ConnectionEvent connectionevent = new ConnectionEvent(this,
				sqlException);

		while (enumeration.hasMoreElements()) {

			ConnectionEventListener connectioneventlistener = (ConnectionEventListener) enumeration
					.nextElement();
			ConnectionEventListener connectioneventlistener1 = (ConnectionEventListener) this.eventListeners
					.get(connectioneventlistener);

			if (eventType == CONNECTION_CLOSED_EVENT) {
				connectioneventlistener1.connectionClosed(connectionevent);
			} else if (eventType == CONNECTION_ERROR_EVENT) {
				connectioneventlistener1
						.connectionErrorOccurred(connectionevent);
			}
		}
	}
}