/*
  Copyright (c) 2010, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * A proxy for a dynamic com.mysql.jdbc.Connection implementation that provides failover features for list of hosts. Connection switching occurs on
 * communications related exceptions and/or user defined settings, namely when one of the conditions set in 'secondsBeforeRetryMaster' or
 * 'queriesBeforeRetryMaster' is met.
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

    private static Class<?>[] INTERFACES_TO_PROXY;

    static {
        if (Util.isJdbc4()) {
            try {
                INTERFACES_TO_PROXY = new Class<?>[] { Class.forName("com.mysql.jdbc.JDBC4MySQLConnection") };
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else {
            INTERFACES_TO_PROXY = new Class<?>[] { MySQLConnection.class };
        }
    }

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

    public static Connection createProxyInstance(List<String> hosts, Properties props) throws SQLException {
        FailoverConnectionProxy connProxy = new FailoverConnectionProxy(hosts, props);

        return (Connection) java.lang.reflect.Proxy.newProxyInstance(Connection.class.getClassLoader(), INTERFACES_TO_PROXY, connProxy);
    }

    /**
     * Instantiates a new FailoverConnectionProxy for the given list of hosts and connection properties.
     * 
     * @param hosts
     *            The lists of hosts available to switch on.
     * @param props
     *            The properties to be used in new internal connections.
     */
    private FailoverConnectionProxy(List<String> hosts, Properties props) throws SQLException {
        super(hosts, props);

        ConnectionPropertiesImpl connProps = new ConnectionPropertiesImpl();
        connProps.initializeProperties(props);

        this.secondsBeforeRetryPrimaryHost = connProps.getSecondsBeforeRetryMaster();
        this.queriesBeforeRetryPrimaryHost = connProps.getQueriesBeforeRetryMaster();
        this.failoverReadOnly = connProps.getFailOverReadOnly();
        this.retriesAllDown = connProps.getRetriesAllDown();

        this.enableFallBackToPrimaryHost = this.secondsBeforeRetryPrimaryHost > 0 || this.queriesBeforeRetryPrimaryHost > 0;

        pickNewConnection();

        this.explicitlyAutoCommit = this.currentConnection.getAutoCommit();
    }

    /*
     * Gets locally bound instances of FailoverJdbcInterfaceProxy.
     * 
     * @see com.mysql.jdbc.MultiHostConnectionProxy#getNewJdbcInterfaceProxy(java.lang.Object)
     */
    @Override
    JdbcInterfaceProxy getNewJdbcInterfaceProxy(Object toProxy) {
        return new FailoverJdbcInterfaceProxy(toProxy);
    }

    /*
     * Local implementation for the connection switch exception checker.
     * 
     * @see com.mysql.jdbc.MultiHostConnectionProxy#shouldExceptionTriggerConnectionSwitch(java.lang.Throwable)
     */
    @Override
    boolean shouldExceptionTriggerConnectionSwitch(Throwable t) {
        if (!(t instanceof SQLException)) {
            return false;
        }

        String sqlState = ((SQLException) t).getSQLState();
        if (sqlState != null) {
            if (sqlState.startsWith("08")) {
                // connection error
                return true;
            }
        }

        // Always handle CommunicationsException
        if (t instanceof CommunicationsException) {
            return true;
        }

        return false;
    }

    /**
     * Checks if current connection is to a master host.
     */
    @Override
    boolean isMasterConnection() {
        return connectedToPrimaryHost();
    }

    /*
     * Local implementation for the new connection picker.
     * 
     * @see com.mysql.jdbc.MultiHostConnectionProxy#pickNewConnection()
     */
    @Override
    synchronized void pickNewConnection() throws SQLException {
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
    }

    /**
     * Creates a new connection instance for host pointed out by the given host index.
     * 
     * @param hostIndex
     *            The host index in the global hosts list.
     * @return
     *         The new connection instance.
     */
    synchronized ConnectionImpl createConnectionForHostIndex(int hostIndex) throws SQLException {
        return createConnectionForHost(this.hostList.get(hostIndex));
    }

    /**
     * Connects this dynamic failover connection proxy to the host pointed out by the given host index.
     * 
     * @param hostIndex
     *            The host index in the global hosts list.
     */
    private synchronized void connectTo(int hostIndex) throws SQLException {
        try {
            switchCurrentConnectionTo(hostIndex, createConnectionForHostIndex(hostIndex));
        } catch (SQLException e) {
            if (this.currentConnection != null) {
                StringBuilder msg = new StringBuilder("Connection to ").append(isPrimaryHostIndex(hostIndex) ? "primary" : "secondary").append(" host '")
                        .append(this.hostList.get(hostIndex)).append("' failed");
                this.currentConnection.getLog().logWarn(msg.toString(), e);
            }
            throw e;
        }
    }

    /**
     * Replaces the previous underlying connection by the connection given. State from previous connection, if any, is synchronized with the new one.
     * 
     * @param hostIndex
     *            The host index in the global hosts list that matches the given connection.
     * @param connection
     *            The connection instance to switch to.
     */
    private synchronized void switchCurrentConnectionTo(int hostIndex, MySQLConnection connection) throws SQLException {
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
    }

    /**
     * Initiates a default failover procedure starting at the current connection host index.
     */
    private synchronized void failOver() throws SQLException {
        failOver(this.currentHostIndex);
    }

    /**
     * Initiates a default failover procedure starting at the given host index.
     * This process tries to connect, sequentially, to the next host in the list. The primary host may or may not be excluded from the connection attempts.
     * 
     * @param failedHostIdx
     *            The host index where to start from. First connection attempt will be the next one.
     */
    private synchronized void failOver(int failedHostIdx) throws SQLException {
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
    }

    /**
     * Falls back to primary host or keep current connection if primary not available.
     */
    synchronized void fallBackToPrimaryIfAvailable() {
        MySQLConnection connection = null;
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
     */
    private int nextHost(int currHostIdx, boolean vouchForPrimaryHost) {
        int nextHostIdx = (currHostIdx + 1) % this.hostList.size();
        if (isPrimaryHostIndex(nextHostIdx) && isConnected() && !vouchForPrimaryHost && this.enableFallBackToPrimaryHost && !readyToFallBackToPrimaryHost()) {
            // Skip primary host, assume this.hostList.size() >= 2
            nextHostIdx = nextHost(nextHostIdx, vouchForPrimaryHost);
        }
        return nextHostIdx;
    }

    /**
     * Increments counter for query executions.
     */
    synchronized void incrementQueriesIssuedSinceFailover() {
        this.queriesIssuedSinceFailover++;
    }

    /**
     * Checks if at least one of the required conditions to fall back to primary host is met, which is determined by the properties 'queriesBeforeRetryMaster'
     * and 'secondsBeforeRetryMaster'.
     */
    synchronized boolean readyToFallBackToPrimaryHost() {
        return this.enableFallBackToPrimaryHost && connectedToSecondaryHost() && (secondsBeforeRetryPrimaryHostIsMet() || queriesBeforeRetryPrimaryHostIsMet());
    }

    /**
     * Checks if there is a underlying connection for this proxy.
     */
    synchronized boolean isConnected() {
        return this.currentHostIndex != NO_CONNECTION_INDEX;
    }

    /**
     * Checks if the given host index points to the primary host.
     * 
     * @param hostIndex
     *            The host index in the global hosts list.
     */
    synchronized boolean isPrimaryHostIndex(int hostIndex) {
        return hostIndex == this.primaryHostIndex;
    }

    /**
     * Checks if this proxy is using the primary host in the underlying connection.
     */
    synchronized boolean connectedToPrimaryHost() {
        return isPrimaryHostIndex(this.currentHostIndex);
    }

    /**
     * Checks if this proxy is using a secondary host in the underlying connection.
     */
    synchronized boolean connectedToSecondaryHost() {
        return this.currentHostIndex >= 0 && !isPrimaryHostIndex(this.currentHostIndex);
    }

    /**
     * Checks the condition set by the property 'secondsBeforeRetryMaster'.
     */
    private synchronized boolean secondsBeforeRetryPrimaryHostIsMet() {
        return this.secondsBeforeRetryPrimaryHost > 0 && Util.secondsSinceMillis(this.primaryHostFailTimeMillis) >= this.secondsBeforeRetryPrimaryHost;
    }

    /**
     * Checks the condition set by the property 'queriesBeforeRetryMaster'.
     */
    private synchronized boolean queriesBeforeRetryPrimaryHostIsMet() {
        return this.queriesBeforeRetryPrimaryHost > 0 && this.queriesIssuedSinceFailover >= this.queriesBeforeRetryPrimaryHost;
    }

    /**
     * Resets auto-fall back counters.
     */
    private synchronized void resetAutoFallBackCounters() {
        this.primaryHostFailTimeMillis = System.currentTimeMillis();
        this.queriesIssuedSinceFailover = 0;
    }

    /**
     * Closes current connection.
     */
    @Override
    synchronized void doClose() throws SQLException {
        this.currentConnection.close();
    }

    /**
     * Aborts current connection.
     */
    @Override
    synchronized void doAbortInternal() throws SQLException {
        this.currentConnection.abortInternal();
    }

    /**
     * Aborts current connection using the given executor.
     */
    @Override
    synchronized void doAbort(Executor executor) throws SQLException {
        this.currentConnection.abort(executor);
    }

    /*
     * Local method invocation handling for this proxy.
     * This is the continuation of MultiHostConnectionProxy#invoke(Object, Method, Object[]).
     */
    @Override
    public synchronized Object invokeMore(Object proxy, Method method, Object[] args) throws Throwable {
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
                throw SQLError.createSQLException(reason, SQLError.SQL_STATE_CONNECTION_NOT_OPEN, null /* no access to a interceptor here... */);
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
