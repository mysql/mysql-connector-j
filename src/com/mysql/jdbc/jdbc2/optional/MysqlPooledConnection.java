/*
  Copyright (c) 2002, 2014, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FOSS License Exception
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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;

import com.mysql.jdbc.ExceptionInterceptor;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.Util;

/**
 * This class is used to wrap and return a physical connection within a logical handle. It also registers and notifies ConnectionEventListeners of any
 * ConnectionEvents
 */
public class MysqlPooledConnection implements PooledConnection {

    private static final Constructor<?> JDBC_4_POOLED_CONNECTION_WRAPPER_CTOR;

    static {
        if (Util.isJdbc4()) {
            try {
                JDBC_4_POOLED_CONNECTION_WRAPPER_CTOR = Class.forName("com.mysql.jdbc.jdbc2.optional.JDBC4MysqlPooledConnection").getConstructor(
                        new Class[] { com.mysql.jdbc.Connection.class });
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else {
            JDBC_4_POOLED_CONNECTION_WRAPPER_CTOR = null;
        }
    }

    protected static MysqlPooledConnection getInstance(com.mysql.jdbc.Connection connection) throws SQLException {
        if (!Util.isJdbc4()) {
            return new MysqlPooledConnection(connection);
        }

        return (MysqlPooledConnection) Util.handleNewInstance(JDBC_4_POOLED_CONNECTION_WRAPPER_CTOR, new Object[] { connection },
                connection.getExceptionInterceptor());
    }

    /**
     * The flag for an exception being thrown.
     */
    public static final int CONNECTION_ERROR_EVENT = 1;

    /**
     * The flag for a connection being closed.
     */
    public static final int CONNECTION_CLOSED_EVENT = 2;

    private Map<ConnectionEventListener, ConnectionEventListener> connectionEventListeners;

    private Connection logicalHandle;

    private com.mysql.jdbc.Connection physicalConn;

    private ExceptionInterceptor exceptionInterceptor;

    /**
     * Construct a new MysqlPooledConnection and set instance variables
     * 
     * @param connection
     *            physical connection to db
     */
    public MysqlPooledConnection(com.mysql.jdbc.Connection connection) {
        this.logicalHandle = null;
        this.physicalConn = connection;
        this.connectionEventListeners = new HashMap<ConnectionEventListener, ConnectionEventListener>();
        this.exceptionInterceptor = this.physicalConn.getExceptionInterceptor();
    }

    /**
     * Adds ConnectionEventListeners to a hash table to be used for notification
     * of ConnectionEvents
     * 
     * @param connectioneventlistener
     *            listener to be notified with ConnectionEvents
     */
    public synchronized void addConnectionEventListener(ConnectionEventListener connectioneventlistener) {

        if (this.connectionEventListeners != null) {
            this.connectionEventListeners.put(connectioneventlistener, connectioneventlistener);
        }
    }

    /**
     * Removes ConnectionEventListeners from hash table used for notification of
     * ConnectionEvents
     * 
     * @param connectioneventlistener
     *            listener to be removed
     */
    public synchronized void removeConnectionEventListener(ConnectionEventListener connectioneventlistener) {

        if (this.connectionEventListeners != null) {
            this.connectionEventListeners.remove(connectioneventlistener);
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

    protected synchronized Connection getConnection(boolean resetServerState, boolean forXa) throws SQLException {
        if (this.physicalConn == null) {

            SQLException sqlException = SQLError.createSQLException("Physical Connection doesn't exist", this.exceptionInterceptor);
            callConnectionEventListeners(CONNECTION_ERROR_EVENT, sqlException);

            throw sqlException;
        }

        try {

            if (this.logicalHandle != null) {
                ((ConnectionWrapper) this.logicalHandle).close(false);
            }

            if (resetServerState) {
                this.physicalConn.resetServerState();
            }

            this.logicalHandle = ConnectionWrapper.getInstance(this, this.physicalConn, forXa);
        } catch (SQLException sqlException) {
            callConnectionEventListeners(CONNECTION_ERROR_EVENT, sqlException);

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

            this.physicalConn = null;
        }

        if (this.connectionEventListeners != null) {
            this.connectionEventListeners.clear();

            this.connectionEventListeners = null;
        }
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
    protected synchronized void callConnectionEventListeners(int eventType, SQLException sqlException) {

        if (this.connectionEventListeners == null) {

            return;
        }

        Iterator<Map.Entry<ConnectionEventListener, ConnectionEventListener>> iterator = this.connectionEventListeners.entrySet().iterator();

        ConnectionEvent connectionevent = new ConnectionEvent(this, sqlException);

        while (iterator.hasNext()) {

            ConnectionEventListener connectioneventlistener = iterator.next().getValue();

            if (eventType == CONNECTION_CLOSED_EVENT) {
                connectioneventlistener.connectionClosed(connectionevent);
            } else if (eventType == CONNECTION_ERROR_EVENT) {
                connectioneventlistener.connectionErrorOccurred(connectionevent);
            }
        }
    }

    protected ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }
}