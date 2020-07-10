/*
 * Copyright (c) 2002, 2020, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;

import com.mysql.cj.Messages;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.jdbc.exceptions.SQLError;

/**
 * This class is used to wrap and return a physical connection within a logical handle. It also registers and notifies ConnectionEventListeners of any
 * ConnectionEvents
 */
public class MysqlPooledConnection implements PooledConnection {

    protected static MysqlPooledConnection getInstance(com.mysql.cj.jdbc.JdbcConnection connection) throws SQLException {
        return new MysqlPooledConnection(connection);
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

    private com.mysql.cj.jdbc.JdbcConnection physicalConn;

    private ExceptionInterceptor exceptionInterceptor;

    private final Map<StatementEventListener, StatementEventListener> statementEventListeners = new HashMap<>();

    /**
     * Construct a new MysqlPooledConnection and set instance variables
     * 
     * @param connection
     *            physical connection to db
     */
    public MysqlPooledConnection(com.mysql.cj.jdbc.JdbcConnection connection) {
        this.logicalHandle = null;
        this.physicalConn = connection;
        this.connectionEventListeners = new HashMap<>();
        this.exceptionInterceptor = this.physicalConn.getExceptionInterceptor();
    }

    @Override
    public synchronized void addConnectionEventListener(ConnectionEventListener connectioneventlistener) {

        if (this.connectionEventListeners != null) {
            this.connectionEventListeners.put(connectioneventlistener, connectioneventlistener);
        }
    }

    @Override
    public synchronized void removeConnectionEventListener(ConnectionEventListener connectioneventlistener) {

        if (this.connectionEventListeners != null) {
            this.connectionEventListeners.remove(connectioneventlistener);
        }
    }

    @Override
    public synchronized Connection getConnection() throws SQLException {
        return getConnection(true, false);

    }

    protected synchronized Connection getConnection(boolean resetServerState, boolean forXa) throws SQLException {
        if (this.physicalConn == null) {

            SQLException sqlException = SQLError.createSQLException(Messages.getString("MysqlPooledConnection.0"), this.exceptionInterceptor);
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
     */
    @Override
    public synchronized void close() throws SQLException {
        if (this.physicalConn != null) {
            this.physicalConn.close();

            this.physicalConn = null;
        }

        if (this.connectionEventListeners != null) {
            this.connectionEventListeners.clear();

            this.connectionEventListeners = null;
        }

        this.statementEventListeners.clear();
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

    @Override
    public void addStatementEventListener(StatementEventListener listener) {
        synchronized (this.statementEventListeners) {
            this.statementEventListeners.put(listener, listener);
        }
    }

    @Override
    public void removeStatementEventListener(StatementEventListener listener) {
        synchronized (this.statementEventListeners) {
            this.statementEventListeners.remove(listener);
        }
    }

    void fireStatementEvent(StatementEvent event) throws SQLException {
        synchronized (this.statementEventListeners) {
            for (StatementEventListener listener : this.statementEventListeners.keySet()) {
                listener.statementClosed(event);
            }
        }
    }

}
