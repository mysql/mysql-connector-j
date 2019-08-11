/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
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

import static com.mysql.cj.util.StringUtils.isNullOrEmpty;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;

import com.mysql.cj.Messages;
import com.mysql.cj.PingTarget;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.HostsListView;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.url.LoadBalanceConnectionUrl;
import com.mysql.cj.conf.url.ReplicationConnectionUrl;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcStatement;
import com.mysql.cj.jdbc.exceptions.SQLError;

/**
 * Connection that opens two connections, one two a replication master, and another to one or more slaves, and decides to use master when the connection is not
 * read-only, and use slave(s) when the connection is read-only.
 */
public class ReplicationConnectionProxy extends MultiHostConnectionProxy implements PingTarget {
    private ReplicationConnection thisAsReplicationConnection;

    protected boolean enableJMX = false;
    protected boolean allowMasterDownConnections = false;
    protected boolean allowSlaveDownConnections = false;
    protected boolean readFromMasterWhenNoSlaves = false;
    protected boolean readFromMasterWhenNoSlavesOriginal = false;
    protected boolean readOnly = false;

    ReplicationConnectionGroup connectionGroup;
    private long connectionGroupID = -1;

    private List<HostInfo> masterHosts;
    protected LoadBalancedConnection masterConnection;

    private List<HostInfo> slaveHosts;
    protected LoadBalancedConnection slavesConnection;

    /**
     * Static factory to create {@link ReplicationConnection} instances.
     * 
     * @param connectionUrl
     *            The connection URL containing the hosts in a replication setup.
     * @return A {@link ReplicationConnection} proxy.
     * @throws SQLException
     *             if an error occurs
     */
    public static ReplicationConnection createProxyInstance(ConnectionUrl connectionUrl) throws SQLException {
        ReplicationConnectionProxy connProxy = new ReplicationConnectionProxy(connectionUrl);
        return (ReplicationConnection) java.lang.reflect.Proxy.newProxyInstance(ReplicationConnection.class.getClassLoader(),
                new Class<?>[] { ReplicationConnection.class, JdbcConnection.class }, connProxy);
    }

    /**
     * Creates a proxy for java.sql.Connection that routes requests to a load-balanced connection of master servers or a load-balanced connection of slave
     * servers. Each sub-connection is created with its own set of independent properties.
     * 
     * @param connectionUrl
     *            The connection URL containing the hosts in a replication setup.
     * @throws SQLException
     *             if an error occurs
     */
    private ReplicationConnectionProxy(ConnectionUrl connectionUrl) throws SQLException {
        super();

        Properties props = connectionUrl.getConnectionArgumentsAsProperties();

        this.thisAsReplicationConnection = (ReplicationConnection) this.thisAsConnection;

        this.connectionUrl = connectionUrl;

        String enableJMXAsString = props.getProperty(PropertyKey.ha_enableJMX.getKeyName(), "false");
        try {
            this.enableJMX = Boolean.parseBoolean(enableJMXAsString);
        } catch (Exception e) {
            throw SQLError.createSQLException(Messages.getString("MultihostConnection.badValueForHaEnableJMX", new Object[] { enableJMXAsString }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String allowMasterDownConnectionsAsString = props.getProperty(PropertyKey.allowMasterDownConnections.getKeyName(), "false");
        try {
            this.allowMasterDownConnections = Boolean.parseBoolean(allowMasterDownConnectionsAsString);
        } catch (Exception e) {
            throw SQLError.createSQLException(
                    Messages.getString("ReplicationConnectionProxy.badValueForAllowMasterDownConnections", new Object[] { enableJMXAsString }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String allowSlaveDownConnectionsAsString = props.getProperty(PropertyKey.allowSlaveDownConnections.getKeyName(), "false");
        try {
            this.allowSlaveDownConnections = Boolean.parseBoolean(allowSlaveDownConnectionsAsString);
        } catch (Exception e) {
            throw SQLError.createSQLException(
                    Messages.getString("ReplicationConnectionProxy.badValueForAllowSlaveDownConnections", new Object[] { allowSlaveDownConnectionsAsString }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String readFromMasterWhenNoSlavesAsString = props.getProperty(PropertyKey.readFromMasterWhenNoSlaves.getKeyName());
        try {
            this.readFromMasterWhenNoSlavesOriginal = Boolean.parseBoolean(readFromMasterWhenNoSlavesAsString);

        } catch (Exception e) {
            throw SQLError.createSQLException(
                    Messages.getString("ReplicationConnectionProxy.badValueForReadFromMasterWhenNoSlaves", new Object[] { readFromMasterWhenNoSlavesAsString }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String group = props.getProperty(PropertyKey.replicationConnectionGroup.getKeyName(), null);
        if (!isNullOrEmpty(group) && ReplicationConnectionUrl.class.isAssignableFrom(connectionUrl.getClass())) {
            this.connectionGroup = ReplicationConnectionGroupManager.getConnectionGroupInstance(group);
            if (this.enableJMX) {
                ReplicationConnectionGroupManager.registerJmx();
            }
            this.connectionGroupID = this.connectionGroup.registerReplicationConnection(this.thisAsReplicationConnection,
                    ((ReplicationConnectionUrl) connectionUrl).getMastersListAsHostPortPairs(),
                    ((ReplicationConnectionUrl) connectionUrl).getSlavesListAsHostPortPairs());

            this.masterHosts = ((ReplicationConnectionUrl) connectionUrl).getMasterHostsListFromHostPortPairs(this.connectionGroup.getMasterHosts());
            this.slaveHosts = ((ReplicationConnectionUrl) connectionUrl).getSlaveHostsListFromHostPortPairs(this.connectionGroup.getSlaveHosts());
        } else {
            this.masterHosts = new ArrayList<>(connectionUrl.getHostsList(HostsListView.MASTERS));
            this.slaveHosts = new ArrayList<>(connectionUrl.getHostsList(HostsListView.SLAVES));
        }

        resetReadFromMasterWhenNoSlaves();

        // Initialize slaves connection first so that it is ready to be used in case the masters connection fails and 'allowMasterDownConnections=true'.
        try {
            initializeSlavesConnection();
        } catch (SQLException e) {
            if (!this.allowSlaveDownConnections) {
                if (this.connectionGroup != null) {
                    this.connectionGroup.handleCloseConnection(this.thisAsReplicationConnection);
                }
                throw e;
            } // Else swallow this exception.
        }

        SQLException exCaught = null;
        try {
            this.currentConnection = initializeMasterConnection();
        } catch (SQLException e) {
            exCaught = e;
        }

        if (this.currentConnection == null) {
            if (this.allowMasterDownConnections && this.slavesConnection != null) {
                // Set read-only and fail over to the slaves connection.
                this.readOnly = true;
                this.currentConnection = this.slavesConnection;
            } else {
                if (this.connectionGroup != null) {
                    this.connectionGroup.handleCloseConnection(this.thisAsReplicationConnection);
                }
                if (exCaught != null) {
                    throw exCaught;
                }
                throw SQLError.createSQLException(Messages.getString("ReplicationConnectionProxy.initializationWithEmptyHostsLists"),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
            }
        }
    }

    /**
     * Wraps this object with a new replication Connection instance.
     * 
     * @return
     *         The connection object instance that wraps 'this'.
     */
    @Override
    JdbcConnection getNewWrapperForThisAsConnection() throws SQLException {
        return new ReplicationMySQLConnection(this);
    }

    /**
     * Propagates the connection proxy down through all live connections.
     * 
     * @param proxyConn
     *            The top level connection in the multi-host connections chain.
     */
    @Override
    protected void propagateProxyDown(JdbcConnection proxyConn) {
        if (this.masterConnection != null) {
            this.masterConnection.setProxy(proxyConn);
        }
        if (this.slavesConnection != null) {
            this.slavesConnection.setProxy(proxyConn);
        }
    }

    /**
     * Has no use in replication connections. Always return <code>false</code>.
     * 
     * @param t
     *            The Exception instance to check.
     */
    @Override
    boolean shouldExceptionTriggerConnectionSwitch(Throwable t) {
        return false;
    }

    /**
     * Checks if current connection is the masters l/b connection.
     */
    @Override
    public boolean isMasterConnection() {
        return this.currentConnection != null && this.currentConnection == this.masterConnection;
    }

    /**
     * Checks if current connection is the slaves l/b connection.
     * 
     * @return true if current connection is the slaves l/b connection
     */
    public boolean isSlavesConnection() {
        return this.currentConnection != null && this.currentConnection == this.slavesConnection;
    }

    @Override
    void pickNewConnection() throws SQLException {
        // no-op
    }

    @Override
    void syncSessionState(JdbcConnection source, JdbcConnection target, boolean readonly) throws SQLException {
        try {
            super.syncSessionState(source, target, readonly);
        } catch (SQLException e1) {
            try {
                // Try again. It may happen that the connection had recovered in the meantime but the right syncing wasn't done yet.
                super.syncSessionState(source, target, readonly);
            } catch (SQLException e2) {
            }
            // Swallow both exceptions. Replication connections must continue to "work" after swapping between masters and slaves.
        }
    }

    @Override
    void doClose() throws SQLException {
        if (this.masterConnection != null) {
            this.masterConnection.close();
        }
        if (this.slavesConnection != null) {
            this.slavesConnection.close();
        }

        if (this.connectionGroup != null) {
            this.connectionGroup.handleCloseConnection(this.thisAsReplicationConnection);
        }
    }

    @Override
    void doAbortInternal() throws SQLException {
        this.masterConnection.abortInternal();
        this.slavesConnection.abortInternal();
        if (this.connectionGroup != null) {
            this.connectionGroup.handleCloseConnection(this.thisAsReplicationConnection);
        }
    }

    @Override
    void doAbort(Executor executor) throws SQLException {
        this.masterConnection.abort(executor);
        this.slavesConnection.abort(executor);
        if (this.connectionGroup != null) {
            this.connectionGroup.handleCloseConnection(this.thisAsReplicationConnection);
        }
    }

    /**
     * Proxies method invocation on the java.sql.Connection interface.
     * This is the continuation of MultiHostConnectionProxy#invoke(Object, Method, Object[]).
     */
    @Override
    Object invokeMore(Object proxy, Method method, Object[] args) throws Throwable {
        checkConnectionCapabilityForMethod(method);

        boolean invokeAgain = false;
        while (true) {
            try {
                Object result = method.invoke(this.thisAsConnection, args);
                if (result != null && result instanceof JdbcStatement) {
                    ((JdbcStatement) result).setPingTarget(this);
                }
                return result;
            } catch (InvocationTargetException e) {
                if (invokeAgain) {
                    invokeAgain = false;
                } else if (e.getCause() != null && e.getCause() instanceof SQLException
                        && ((SQLException) e.getCause()).getSQLState() == MysqlErrorNumbers.SQL_STATE_INVALID_TRANSACTION_STATE
                        && ((SQLException) e.getCause()).getErrorCode() == MysqlErrorNumbers.ERROR_CODE_NULL_LOAD_BALANCED_CONNECTION) {
                    try {
                        // Try to re-establish the connection with the last known read-only state.
                        setReadOnly(this.readOnly);
                        invokeAgain = true;
                    } catch (SQLException sqlEx) {
                        // Still not good. Swallow this exception.
                    }
                }
                if (!invokeAgain) {
                    throw e;
                }
            }
        }
    }

    /**
     * Checks if this connection is in a state capable to invoke the provided method. If the connection is in an inconsistent state, i.e. it has no hosts for
     * both sub-connections, then throw an invalid transaction state exception. Nevertheless, the methods defined in the ReplicationConnection interface will be
     * allowed as they are the only way to leave from an empty hosts lists situation.
     * 
     * @param method
     *            method
     * @throws Throwable
     *             if an error occurs
     */
    private void checkConnectionCapabilityForMethod(Method method) throws Throwable {
        if (this.masterHosts.isEmpty() && this.slaveHosts.isEmpty() && !ReplicationConnection.class.isAssignableFrom(method.getDeclaringClass())) {
            throw SQLError.createSQLException(Messages.getString("ReplicationConnectionProxy.noHostsInconsistentState"),
                    MysqlErrorNumbers.SQL_STATE_INVALID_TRANSACTION_STATE, MysqlErrorNumbers.ERROR_CODE_REPLICATION_CONNECTION_WITH_NO_HOSTS, true, null);
        }
    }

    /**
     * Pings both l/b connections. Switch to another connection in case of failure.
     */
    @Override
    public void doPing() throws SQLException {
        boolean isMasterConn = isMasterConnection();

        SQLException mastersPingException = null;
        SQLException slavesPingException = null;

        if (this.masterConnection != null) {
            try {
                this.masterConnection.ping();
            } catch (SQLException e) {
                mastersPingException = e;
            }
        } else {
            initializeMasterConnection();
        }

        if (this.slavesConnection != null) {
            try {
                this.slavesConnection.ping();
            } catch (SQLException e) {
                slavesPingException = e;
            }
        } else {
            try {
                initializeSlavesConnection();
                if (switchToSlavesConnectionIfNecessary()) {
                    isMasterConn = false;
                }
            } catch (SQLException e) {
                if (this.masterConnection == null || !this.readFromMasterWhenNoSlaves) {
                    throw e;
                } // Else swallow this exception.
            }
        }

        if (isMasterConn && mastersPingException != null) {
            // Switch to slaves connection.
            if (this.slavesConnection != null && slavesPingException == null) {
                this.masterConnection = null;
                this.currentConnection = this.slavesConnection;
                this.readOnly = true;
            }
            throw mastersPingException;

        } else if (!isMasterConn && (slavesPingException != null || this.slavesConnection == null)) {
            // Switch to masters connection, setting read-only state, if 'readFromMasterWhenNoSlaves=true'.
            if (this.masterConnection != null && this.readFromMasterWhenNoSlaves && mastersPingException == null) {
                this.slavesConnection = null;
                this.currentConnection = this.masterConnection;
                this.readOnly = true;
                this.currentConnection.setReadOnly(true);
            }
            if (slavesPingException != null) {
                throw slavesPingException;
            }
        }
    }

    private JdbcConnection initializeMasterConnection() throws SQLException {
        this.masterConnection = null;

        if (this.masterHosts.size() == 0) {
            return null;
        }

        LoadBalancedConnection newMasterConn = LoadBalancedConnectionProxy
                .createProxyInstance(new LoadBalanceConnectionUrl(this.masterHosts, this.connectionUrl.getOriginalProperties()));
        newMasterConn.setProxy(getProxy());

        this.masterConnection = newMasterConn;
        return this.masterConnection;
    }

    private JdbcConnection initializeSlavesConnection() throws SQLException {
        this.slavesConnection = null;

        if (this.slaveHosts.size() == 0) {
            return null;
        }

        LoadBalancedConnection newSlavesConn = LoadBalancedConnectionProxy
                .createProxyInstance(new LoadBalanceConnectionUrl(this.slaveHosts, this.connectionUrl.getOriginalProperties()));
        newSlavesConn.setProxy(getProxy());
        newSlavesConn.setReadOnly(true);

        this.slavesConnection = newSlavesConn;
        return this.slavesConnection;
    }

    private synchronized boolean switchToMasterConnection() throws SQLException {
        if (this.masterConnection == null || this.masterConnection.isClosed()) {
            try {
                if (initializeMasterConnection() == null) {
                    return false;
                }
            } catch (SQLException e) {
                this.currentConnection = null;
                throw e;
            }
        }
        if (!isMasterConnection() && this.masterConnection != null) {
            syncSessionState(this.currentConnection, this.masterConnection, false);
            this.currentConnection = this.masterConnection;
        }
        return true;
    }

    private synchronized boolean switchToSlavesConnection() throws SQLException {
        if (this.slavesConnection == null || this.slavesConnection.isClosed()) {
            try {
                if (initializeSlavesConnection() == null) {
                    return false;
                }
            } catch (SQLException e) {
                this.currentConnection = null;
                throw e;
            }
        }
        if (!isSlavesConnection() && this.slavesConnection != null) {
            syncSessionState(this.currentConnection, this.slavesConnection, true);
            this.currentConnection = this.slavesConnection;
        }
        return true;
    }

    private boolean switchToSlavesConnectionIfNecessary() throws SQLException {
        // Switch to slaves connection:
        // - If the current connection is null. Or,
        // - If we're currently on the master and in read-only mode - we didn't have any slaves to use until now. Or,
        // - If we're currently on a closed master connection and there are no masters to connect to. Or,
        // - If we're currently not on a master connection that is closed - means that we were on a closed slaves connection before it was re-initialized.
        if (this.currentConnection == null || isMasterConnection() && (this.readOnly || this.masterHosts.isEmpty() && this.currentConnection.isClosed())
                || !isMasterConnection() && this.currentConnection.isClosed()) {
            return switchToSlavesConnection();
        }
        return false;
    }

    public synchronized JdbcConnection getCurrentConnection() {
        return this.currentConnection == null ? LoadBalancedConnectionProxy.getNullLoadBalancedConnectionInstance() : this.currentConnection;
    }

    public long getConnectionGroupId() {
        return this.connectionGroupID;
    }

    public synchronized JdbcConnection getMasterConnection() {
        return this.masterConnection;
    }

    public synchronized void promoteSlaveToMaster(String hostPortPair) throws SQLException {
        HostInfo host = getSlaveHost(hostPortPair);
        if (host == null) {
            return;
        }
        this.masterHosts.add(host);
        removeSlave(hostPortPair);
        if (this.masterConnection != null) {
            this.masterConnection.addHost(hostPortPair);
        }

        // Switch back to the masters connection if this connection was running in fail-safe mode.
        if (!this.readOnly && !isMasterConnection()) {
            switchToMasterConnection();
        }
    }

    public synchronized void removeMasterHost(String hostPortPair) throws SQLException {
        this.removeMasterHost(hostPortPair, true);
    }

    public synchronized void removeMasterHost(String hostPortPair, boolean waitUntilNotInUse) throws SQLException {
        this.removeMasterHost(hostPortPair, waitUntilNotInUse, false);
    }

    public synchronized void removeMasterHost(String hostPortPair, boolean waitUntilNotInUse, boolean isNowSlave) throws SQLException {
        HostInfo host = getMasterHost(hostPortPair);
        if (host == null) {
            return;
        }
        if (isNowSlave) {
            this.slaveHosts.add(host);
            resetReadFromMasterWhenNoSlaves();
        }
        this.masterHosts.remove(host);

        // The master connection may have been implicitly closed by a previous op., don't let it stop us.
        if (this.masterConnection == null || this.masterConnection.isClosed()) {
            this.masterConnection = null;
            return;
        }

        if (waitUntilNotInUse) {
            this.masterConnection.removeHostWhenNotInUse(hostPortPair);
        } else {
            this.masterConnection.removeHost(hostPortPair);
        }

        // Close the connection if that was the last master.
        if (this.masterHosts.isEmpty()) {
            this.masterConnection.close();
            this.masterConnection = null;

            // Default behavior, no need to check this.readFromMasterWhenNoSlaves.
            switchToSlavesConnectionIfNecessary();
        }
    }

    public boolean isHostMaster(String hostPortPair) {
        if (hostPortPair == null) {
            return false;
        }
        return this.masterHosts.stream().anyMatch(hi -> hostPortPair.equalsIgnoreCase(hi.getHostPortPair()));
    }

    public synchronized JdbcConnection getSlavesConnection() {
        return this.slavesConnection;
    }

    public synchronized void addSlaveHost(String hostPortPair) throws SQLException {
        if (this.isHostSlave(hostPortPair)) {
            return;
        }
        this.slaveHosts.add(getConnectionUrl().getSlaveHostOrSpawnIsolated(hostPortPair));
        resetReadFromMasterWhenNoSlaves();
        if (this.slavesConnection == null) {
            initializeSlavesConnection();
            switchToSlavesConnectionIfNecessary();
        } else {
            this.slavesConnection.addHost(hostPortPair);
        }
    }

    public synchronized void removeSlave(String hostPortPair) throws SQLException {
        removeSlave(hostPortPair, true);
    }

    public synchronized void removeSlave(String hostPortPair, boolean closeGently) throws SQLException {
        HostInfo host = getSlaveHost(hostPortPair);
        if (host == null) {
            return;
        }
        this.slaveHosts.remove(host);
        resetReadFromMasterWhenNoSlaves();

        if (this.slavesConnection == null || this.slavesConnection.isClosed()) {
            this.slavesConnection = null;
            return;
        }

        if (closeGently) {
            this.slavesConnection.removeHostWhenNotInUse(hostPortPair);
        } else {
            this.slavesConnection.removeHost(hostPortPair);
        }

        // Close the connection if that was the last slave.
        if (this.slaveHosts.isEmpty()) {
            this.slavesConnection.close();
            this.slavesConnection = null;

            // Default behavior, no need to check this.readFromMasterWhenNoSlaves.
            switchToMasterConnection();
            if (isMasterConnection()) {
                this.currentConnection.setReadOnly(this.readOnly); // Maintain.
            }
        }
    }

    public boolean isHostSlave(String hostPortPair) {
        if (hostPortPair == null) {
            return false;
        }
        return this.slaveHosts.stream().anyMatch(hi -> hostPortPair.equalsIgnoreCase(hi.getHostPortPair()));
    }

    public synchronized void setReadOnly(boolean readOnly) throws SQLException {
        if (readOnly) {
            if (!isSlavesConnection() || this.currentConnection.isClosed()) {
                boolean switched = true;
                SQLException exceptionCaught = null;
                try {
                    switched = switchToSlavesConnection();
                } catch (SQLException e) {
                    switched = false;
                    exceptionCaught = e;
                }
                if (!switched && this.readFromMasterWhenNoSlaves && switchToMasterConnection()) {
                    exceptionCaught = null; // The connection is OK. Cancel the exception, if any.
                }
                if (exceptionCaught != null) {
                    throw exceptionCaught;
                }
            }
        } else {
            if (!isMasterConnection() || this.currentConnection.isClosed()) {
                boolean switched = true;
                SQLException exceptionCaught = null;
                try {
                    switched = switchToMasterConnection();
                } catch (SQLException e) {
                    switched = false;
                    exceptionCaught = e;
                }
                if (!switched && switchToSlavesConnectionIfNecessary()) {
                    exceptionCaught = null; // The connection is OK. Cancel the exception, if any.
                }
                if (exceptionCaught != null) {
                    throw exceptionCaught;
                }
            }
        }
        this.readOnly = readOnly;

        /*
         * Reset masters connection read-only state if 'readFromMasterWhenNoSlaves=true'. If there are no slaves then the masters connection will be used with
         * read-only state in its place. Even if not, it must be reset from a possible previous read-only state.
         */
        if (this.readFromMasterWhenNoSlaves && isMasterConnection()) {
            this.currentConnection.setReadOnly(this.readOnly);
        }
    }

    public boolean isReadOnly() throws SQLException {
        return !isMasterConnection() || this.readOnly;
    }

    private void resetReadFromMasterWhenNoSlaves() {
        this.readFromMasterWhenNoSlaves = this.slaveHosts.isEmpty() || this.readFromMasterWhenNoSlavesOriginal;
    }

    private HostInfo getMasterHost(String hostPortPair) {
        return this.masterHosts.stream().filter(hi -> hostPortPair.equalsIgnoreCase(hi.getHostPortPair())).findFirst().orElse(null);
    }

    private HostInfo getSlaveHost(String hostPortPair) {
        return this.slaveHosts.stream().filter(hi -> hostPortPair.equalsIgnoreCase(hi.getHostPortPair())).findFirst().orElse(null);
    }

    private ReplicationConnectionUrl getConnectionUrl() {
        return (ReplicationConnectionUrl) this.connectionUrl;
    }
}
