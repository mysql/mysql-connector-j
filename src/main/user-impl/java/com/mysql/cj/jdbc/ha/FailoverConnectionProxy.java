/*
 * Copyright (c) 2010, 2021, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc.ha;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.concurrent.Executor;

import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPropertySetImpl;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.util.Util;

/**
 * A proxy for a dynamic com.mysql.cj.jdbc.JdbcConnection implementation that provides failover features for list of hosts. Connection switching occurs on
 * communications related exceptions and/or user defined settings, namely when one of the conditions set in 'secondsBeforeRetrySource' or
 * 'queriesBeforeRetrySource' is met.
 */
public class FailoverConnectionProxy extends MultiHostConnectionProxy {
    private static final String METHOD_SET_READ_ONLY = "setReadOnly";
    private static final String METHOD_SET_AUTO_COMMIT = "setAutoCommit";
    private static final String METHOD_COMMIT = "commit";
    private static final String METHOD_ROLLBACK = "rollback";

    private static final int NO_CONNECTION_INDEX = -1;
    private static final int DEFAULT_PRIMARY_HOST_INDEX = 0;

    private int secondsBeforeRetryPrimaryHost;
    private long queriesBeforeRetryPrimaryHost;
    private boolean failoverReadOnly;
    private int retriesAllDown;

    private int currentHostIndex = NO_CONNECTION_INDEX;
    private int primaryHostIndex = DEFAULT_PRIMARY_HOST_INDEX;
    private Boolean explicitlyReadOnly = null;
    private boolean explicitlyAutoCommit = true;

    private boolean enableFallBackToPrimaryHost = true;
    private long primaryHostFailTimeMillis = 0;
    private long queriesIssuedSinceFailover = 0;

    /**
     * Proxy class to intercept and deal with errors that may occur in any object bound to the current connection.
     * Additionally intercepts query executions and triggers an execution count on the outer class.
     */
    class FailoverJdbcInterfaceProxy extends JdbcInterfaceProxy {
        FailoverJdbcInterfaceProxy(Object toInvokeOn) {
            super(toInvokeOn);
        }

        @SuppressWarnings("synthetic-access")
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String methodName = method.getName();

            boolean isExecute = methodName.startsWith("execute");

            if (FailoverConnectionProxy.this.connectedToSecondaryHost() && isExecute) {
                FailoverConnectionProxy.this.incrementQueriesIssuedSinceFailover();
            }

            Object result = super.invoke(proxy, method, args);

            if (FailoverConnectionProxy.this.explicitlyAutoCommit && isExecute && readyToFallBackToPrimaryHost()) {
                // Fall back to primary host at transaction boundary
                fallBackToPrimaryIfAvailable();
            }

            return result;
        }
    }

    public static JdbcConnection createProxyInstance(ConnectionUrl connectionUrl) throws SQLException {
        FailoverConnectionProxy connProxy = new FailoverConnectionProxy(connectionUrl);

        return (JdbcConnection) java.lang.reflect.Proxy.newProxyInstance(JdbcConnection.class.getClassLoader(), new Class<?>[] { JdbcConnection.class },
                connProxy);
    }

    /**
     * Instantiates a new FailoverConnectionProxy for the given list of hosts and connection properties.
     * 
     * @param connectionUrl
     *            {@link ConnectionUrl} instance containing the lists of hosts available to switch on.
     * @throws SQLException
     *             if an error occurs
     */
    private FailoverConnectionProxy(ConnectionUrl connectionUrl) throws SQLException {
        super(connectionUrl);

        JdbcPropertySetImpl connProps = new JdbcPropertySetImpl();
        connProps.initializeProperties(connectionUrl.getConnectionArgumentsAsProperties());

        this.secondsBeforeRetryPrimaryHost = connProps.getIntegerProperty(PropertyKey.secondsBeforeRetrySource).getValue();
        this.queriesBeforeRetryPrimaryHost = connProps.getIntegerProperty(PropertyKey.queriesBeforeRetrySource).getValue();
        this.failoverReadOnly = connProps.getBooleanProperty(PropertyKey.failOverReadOnly).getValue();
        this.retriesAllDown = connProps.getIntegerProperty(PropertyKey.retriesAllDown).getValue();

        this.enableFallBackToPrimaryHost = this.secondsBeforeRetryPrimaryHost > 0 || this.queriesBeforeRetryPrimaryHost > 0;

        pickNewConnection();

        this.explicitlyAutoCommit = this.currentConnection.getAutoCommit();
    }

    /**
     * Gets locally bound instances of FailoverJdbcInterfaceProxy.
     * 
     */
    @Override
    JdbcInterfaceProxy getNewJdbcInterfaceProxy(Object toProxy) {
        return new FailoverJdbcInterfaceProxy(toProxy);
    }

    /*
     * Local implementation for the connection switch exception checker.
     */
    @Override
    boolean shouldExceptionTriggerConnectionSwitch(Throwable t) {

        String sqlState = null;
        if (t instanceof CommunicationsException || t instanceof CJCommunicationsException) {
            return true;
        } else if (t instanceof SQLException) {
            sqlState = ((SQLException) t).getSQLState();
        } else if (t instanceof CJException) {
            sqlState = ((CJException) t).getSQLState();
        }

        if (sqlState != null) {
            if (sqlState.startsWith("08")) {
                // connection error
                return true;
            }
        }

        return false;
    }

    /**
     * Checks if current connection is to a source host.
     */
    @Override
    boolean isSourceConnection() {
        return connectedToPrimaryHost();
    }

    /*
     * Local implementation for the new connection picker.
     */
    @Override
    void pickNewConnection() throws SQLException {
        this.getObjectLock().lock();
        try {
            if (this.isClosed && this.closedExplicitly) {
                return;
            }

            if (!isConnected() || readyToFallBackToPrimaryHost()) {
                try {
                    connectTo(this.primaryHostIndex);
                } catch (SQLException e) {
                    resetAutoFallBackCounters();
                    failOver(this.primaryHostIndex);
                }
            } else {
                failOver();
            }
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Creates a new connection instance for host pointed out by the given host index.
     * 
     * @param hostIndex
     *            The host index in the global hosts list.
     * @return
     *         The new connection instance.
     * @throws SQLException
     *             if an error occurs
     */
    ConnectionImpl createConnectionForHostIndex(int hostIndex) throws SQLException {
        this.getObjectLock().lock();
        try {
            return createConnectionForHost(this.hostsList.get(hostIndex));
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Connects this dynamic failover connection proxy to the host pointed out by the given host index.
     * 
     * @param hostIndex
     *            The host index in the global hosts list.
     * @throws SQLException
     *             if an error occurs
     */
    private void connectTo(int hostIndex) throws SQLException {
        this.getObjectLock().lock();
        try {
            switchCurrentConnectionTo(hostIndex, createConnectionForHostIndex(hostIndex));
        } catch (SQLException e) {
            if (this.currentConnection != null) {
                StringBuilder msg = new StringBuilder("Connection to ").append(isPrimaryHostIndex(hostIndex) ? "primary" : "secondary").append(" host '")
                        .append(this.hostsList.get(hostIndex)).append("' failed");
                try {
                    this.currentConnection.getSession().getLog().logWarn(msg.toString(), e);
                } catch (CJException ex) {
                    throw SQLExceptionsMapping.translateException(e, this.currentConnection.getExceptionInterceptor());
                }
            }
            throw e;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Replaces the previous underlying connection by the connection given. State from previous connection, if any, is synchronized with the new one.
     * 
     * @param hostIndex
     *            The host index in the global hosts list that matches the given connection.
     * @param connection
     *            The connection instance to switch to.
     * @throws SQLException
     *             if an error occurs
     */
    private void switchCurrentConnectionTo(int hostIndex, JdbcConnection connection) throws SQLException {
        this.getObjectLock().lock();
        try {
            invalidateCurrentConnection();

            boolean readOnly;
            if (isPrimaryHostIndex(hostIndex)) {
                readOnly = this.explicitlyReadOnly == null ? false : this.explicitlyReadOnly;
            } else if (this.failoverReadOnly) {
                readOnly = true;
            } else if (this.explicitlyReadOnly != null) {
                readOnly = this.explicitlyReadOnly;
            } else if (this.currentConnection != null) {
                readOnly = this.currentConnection.isReadOnly();
            } else {
                readOnly = false;
            }
            syncSessionState(this.currentConnection, connection, readOnly);
            this.currentConnection = connection;
            this.currentHostIndex = hostIndex;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Initiates a default failover procedure starting at the current connection host index.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    private void failOver() throws SQLException {
        this.getObjectLock().lock();
        try {
            failOver(this.currentHostIndex);
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Initiates a default failover procedure starting at the given host index.
     * This process tries to connect, sequentially, to the next host in the list. The primary host may or may not be excluded from the connection attempts.
     * 
     * @param failedHostIdx
     *            The host index where to start from. First connection attempt will be the next one.
     * @throws SQLException
     *             if an error occurs
     */
    private void failOver(int failedHostIdx) throws SQLException {
        this.getObjectLock().lock();
        try {
            int prevHostIndex = this.currentHostIndex;
            int nextHostIndex = nextHost(failedHostIdx, false);
            int firstHostIndexTried = nextHostIndex;

            SQLException lastExceptionCaught = null;
            int attempts = 0;
            boolean gotConnection = false;
            boolean firstConnOrPassedByPrimaryHost = prevHostIndex == NO_CONNECTION_INDEX || isPrimaryHostIndex(prevHostIndex);
            do {
                try {
                    firstConnOrPassedByPrimaryHost = firstConnOrPassedByPrimaryHost || isPrimaryHostIndex(nextHostIndex);

                    connectTo(nextHostIndex);

                    if (firstConnOrPassedByPrimaryHost && connectedToSecondaryHost()) {
                        resetAutoFallBackCounters();
                    }
                    gotConnection = true;

                } catch (SQLException e) {
                    lastExceptionCaught = e;

                    if (shouldExceptionTriggerConnectionSwitch(e)) {
                        int newNextHostIndex = nextHost(nextHostIndex, attempts > 0);

                        if (newNextHostIndex == firstHostIndexTried && newNextHostIndex == (newNextHostIndex = nextHost(nextHostIndex, true))) { // Full turn
                            attempts++;

                            try {
                                Thread.sleep(250);
                            } catch (InterruptedException ie) {
                            }
                        }

                        nextHostIndex = newNextHostIndex;

                    } else {
                        throw e;
                    }
                }
            } while (attempts < this.retriesAllDown && !gotConnection);

            if (!gotConnection) {
                throw lastExceptionCaught;
            }
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Falls back to primary host or keep current connection if primary not available.
     */
    void fallBackToPrimaryIfAvailable() {
        this.getObjectLock().lock();
        JdbcConnection connection = null;
        try {
            connection = createConnectionForHostIndex(this.primaryHostIndex);
            switchCurrentConnectionTo(this.primaryHostIndex, connection);
        } catch (SQLException e1) {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e2) {
                }
            }
            // Keep current connection and reset counters
            resetAutoFallBackCounters();
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Gets the next host on the hosts list. Uses a round-robin algorithm to find the next element, but it may skip the index for the primary host.
     * General rules to include the primary host are:
     * - not currently connected to any host.
     * - primary host is vouched (usually because connection to all secondary hosts has failed).
     * - conditions to fall back to primary host are met (or they are disabled).
     * 
     * @param currHostIdx
     *            Current host index.
     * @param vouchForPrimaryHost
     *            Allows to return the primary host index even if the usual required conditions aren't met.
     * @return next host index
     */
    private int nextHost(int currHostIdx, boolean vouchForPrimaryHost) {
        int nextHostIdx = (currHostIdx + 1) % this.hostsList.size();
        if (isPrimaryHostIndex(nextHostIdx) && isConnected() && !vouchForPrimaryHost && this.enableFallBackToPrimaryHost && !readyToFallBackToPrimaryHost()) {
            // Skip primary host, assume this.hostList.size() >= 2
            nextHostIdx = nextHost(nextHostIdx, vouchForPrimaryHost);
        }
        return nextHostIdx;
    }

    /**
     * Increments counter for query executions.
     */
    void incrementQueriesIssuedSinceFailover() {
        this.getObjectLock().lock();
        try {
            this.queriesIssuedSinceFailover++;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Checks if at least one of the required conditions to fall back to primary host is met, which is determined by the properties 'queriesBeforeRetrySource'
     * and 'secondsBeforeRetrySource'.
     * 
     * @return true if ready
     */
    boolean readyToFallBackToPrimaryHost() {
        this.getObjectLock().lock();
        try {
            return this.enableFallBackToPrimaryHost && connectedToSecondaryHost() && (secondsBeforeRetryPrimaryHostIsMet() || queriesBeforeRetryPrimaryHostIsMet());
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Checks if there is a underlying connection for this proxy.
     * 
     * @return true if there is a connection
     */
    boolean isConnected() {
        this.getObjectLock().lock();
        try {
            return this.currentHostIndex != NO_CONNECTION_INDEX;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Checks if the given host index points to the primary host.
     * 
     * @param hostIndex
     *            The host index in the global hosts list.
     * @return true if so
     */
    boolean isPrimaryHostIndex(int hostIndex) {
        this.getObjectLock().lock();
        try {
            return hostIndex == this.primaryHostIndex;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Checks if this proxy is using the primary host in the underlying connection.
     * 
     * @return true if so
     */
    boolean connectedToPrimaryHost() {
        this.getObjectLock().lock();
        try {
            return isPrimaryHostIndex(this.currentHostIndex);
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Checks if this proxy is using a secondary host in the underlying connection.
     * 
     * @return true if so
     */
    boolean connectedToSecondaryHost() {
        this.getObjectLock().lock();
        try {
            return this.currentHostIndex >= 0 && !isPrimaryHostIndex(this.currentHostIndex);
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Checks the condition set by the property 'secondsBeforeRetrySource'.
     * 
     * @return value
     */
    private boolean secondsBeforeRetryPrimaryHostIsMet() {
        this.getObjectLock().lock();
        try {
            return this.secondsBeforeRetryPrimaryHost > 0 && Util.secondsSinceMillis(this.primaryHostFailTimeMillis) >= this.secondsBeforeRetryPrimaryHost;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Checks the condition set by the property 'queriesBeforeRetrySource'.
     * 
     * @return value
     */
    private boolean queriesBeforeRetryPrimaryHostIsMet() {
        this.getObjectLock().lock();
        try {
            return this.queriesBeforeRetryPrimaryHost > 0 && this.queriesIssuedSinceFailover >= this.queriesBeforeRetryPrimaryHost;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Resets auto-fall back counters.
     */
    private void resetAutoFallBackCounters() {
        this.getObjectLock().lock();
        try {
            this.primaryHostFailTimeMillis = System.currentTimeMillis();
            this.queriesIssuedSinceFailover = 0;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Closes current connection.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    @Override
    void doClose() throws SQLException {
        this.getObjectLock().lock();
        try {
            this.currentConnection.close();
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Aborts current connection.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    @Override
    void doAbortInternal() throws SQLException {
        this.getObjectLock().lock();
        try {
            this.currentConnection.abortInternal();
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Aborts current connection using the given executor.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    @Override
    void doAbort(Executor executor) throws SQLException {
        this.getObjectLock().lock();
        try {
            this.currentConnection.abort(executor);
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /*
     * Local method invocation handling for this proxy.
     * This is the continuation of MultiHostConnectionProxy#invoke(Object, Method, Object[]).
     */
    @Override
    public Object invokeMore(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (METHOD_SET_READ_ONLY.equals(methodName)) {
            this.explicitlyReadOnly = (Boolean) args[0];
            if (this.failoverReadOnly && connectedToSecondaryHost()) {
                return null;
            }
        }

        if (this.isClosed && !allowedOnClosedConnection(method)) {
            if (this.autoReconnect && !this.closedExplicitly) {
                this.currentHostIndex = NO_CONNECTION_INDEX; // Act as if this is the first connection but let it sync with the previous one.
                pickNewConnection();
                this.isClosed = false;
                this.closedReason = null;
            } else {
                String reason = "No operations allowed after connection closed.";
                if (this.closedReason != null) {
                    reason += ("  " + this.closedReason);
                }
                throw SQLError.createSQLException(reason, MysqlErrorNumbers.SQL_STATE_CONNECTION_NOT_OPEN, null /* no access to a interceptor here... */);
            }
        }

        Object result = null;

        try {
            result = method.invoke(this.thisAsConnection, args);
            result = proxyIfReturnTypeIsJdbcInterface(method.getReturnType(), result);
        } catch (InvocationTargetException e) {
            dealWithInvocationException(e);
        }

        if (METHOD_SET_AUTO_COMMIT.equals(methodName)) {
            this.explicitlyAutoCommit = (Boolean) args[0];
        }

        if ((this.explicitlyAutoCommit || METHOD_COMMIT.equals(methodName) || METHOD_ROLLBACK.equals(methodName)) && readyToFallBackToPrimaryHost()) {
            // Fall back to primary host at transaction boundary
            fallBackToPrimaryIfAvailable();
        }

        return result;
    }
}
