/*
 * Copyright (c) 2015, 2023, Oracle and/or its affiliates.
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
 * Connection that opens two connections, one two a replication source, and another to one or more replicas, and decides to use source when the connection is
 * not
 * read-only, and use replica(s) when the connection is read-only.
 */
public class ReplicationConnectionProxy extends MultiHostConnectionProxy implements PingTarget {

    private ReplicationConnection thisAsReplicationConnection;

    protected boolean enableJMX = false;
    protected boolean allowSourceDownConnections = false;
    protected boolean allowReplicaDownConnections = false;
    protected boolean readFromSourceWhenNoReplicas = false;
    protected boolean readFromSourceWhenNoReplicasOriginal = false;
    protected boolean readOnly = false;

    ReplicationConnectionGroup connectionGroup;
    private long connectionGroupID = -1;

    private List<HostInfo> sourceHosts;
    protected LoadBalancedConnection sourceConnection;

    private List<HostInfo> replicaHosts;
    protected LoadBalancedConnection replicasConnection;

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
     * Creates a proxy for java.sql.Connection that routes requests to a load-balanced connection of source servers or a load-balanced connection of replica
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

        String allowSourceDownConnectionsAsString = props.getProperty(PropertyKey.allowSourceDownConnections.getKeyName(), "false");
        try {
            this.allowSourceDownConnections = Boolean.parseBoolean(allowSourceDownConnectionsAsString);
        } catch (Exception e) {
            throw SQLError.createSQLException(
                    Messages.getString("ReplicationConnectionProxy.badValueForAllowSourceDownConnections", new Object[] { enableJMXAsString }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String allowReplicaDownConnectionsAsString = props.getProperty(PropertyKey.allowReplicaDownConnections.getKeyName(), "false");
        try {
            this.allowReplicaDownConnections = Boolean.parseBoolean(allowReplicaDownConnectionsAsString);
        } catch (Exception e) {
            throw SQLError.createSQLException(Messages.getString("ReplicationConnectionProxy.badValueForAllowReplicaDownConnections",
                    new Object[] { allowReplicaDownConnectionsAsString }), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String readFromSourceWhenNoReplicasAsString = props.getProperty(PropertyKey.readFromSourceWhenNoReplicas.getKeyName());
        try {
            this.readFromSourceWhenNoReplicasOriginal = Boolean.parseBoolean(readFromSourceWhenNoReplicasAsString);

        } catch (Exception e) {
            throw SQLError.createSQLException(Messages.getString("ReplicationConnectionProxy.badValueForReadFromSourceWhenNoReplicas",
                    new Object[] { readFromSourceWhenNoReplicasAsString }), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String group = props.getProperty(PropertyKey.replicationConnectionGroup.getKeyName(), null);
        if (!isNullOrEmpty(group) && ReplicationConnectionUrl.class.isAssignableFrom(connectionUrl.getClass())) {
            this.connectionGroup = ReplicationConnectionGroupManager.getConnectionGroupInstance(group);
            if (this.enableJMX) {
                ReplicationConnectionGroupManager.registerJmx();
            }
            this.connectionGroupID = this.connectionGroup.registerReplicationConnection(this.thisAsReplicationConnection,
                    ((ReplicationConnectionUrl) connectionUrl).getSourcesListAsHostPortPairs(),
                    ((ReplicationConnectionUrl) connectionUrl).getReplicasListAsHostPortPairs());

            this.sourceHosts = ((ReplicationConnectionUrl) connectionUrl).getSourceHostsListFromHostPortPairs(this.connectionGroup.getSourceHosts());
            this.replicaHosts = ((ReplicationConnectionUrl) connectionUrl).getReplicaHostsListFromHostPortPairs(this.connectionGroup.getReplicaHosts());
        } else {
            this.sourceHosts = new ArrayList<>(connectionUrl.getHostsList(HostsListView.SOURCES));
            this.replicaHosts = new ArrayList<>(connectionUrl.getHostsList(HostsListView.REPLICAS));
        }

        resetReadFromSourceWhenNoReplicas();

        // Initialize replicas connection first so that it is ready to be used in case the sources connection fails and 'allowSourceDownConnections=true'.
        try {
            initializeReplicasConnection();
        } catch (SQLException e) {
            if (!this.allowReplicaDownConnections) {
                if (this.connectionGroup != null) {
                    this.connectionGroup.handleCloseConnection(this.thisAsReplicationConnection);
                }
                throw e;
            } // Else swallow this exception.
        }

        SQLException exCaught = null;
        try {
            this.currentConnection = initializeSourceConnection();
        } catch (SQLException e) {
            exCaught = e;
        }

        if (this.currentConnection == null) {
            if (this.allowSourceDownConnections && this.replicasConnection != null) {
                // Set read-only and fail over to the replicas connection.
                this.readOnly = true;
                this.currentConnection = this.replicasConnection;
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
        if (this.sourceConnection != null) {
            this.sourceConnection.setProxy(proxyConn);
        }
        if (this.replicasConnection != null) {
            this.replicasConnection.setProxy(proxyConn);
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
     * Checks if current connection is the sources l/b connection.
     */
    @Override
    public boolean isSourceConnection() {
        return this.currentConnection != null && this.currentConnection == this.sourceConnection;
    }

    /**
     * Checks if current connection is the replicas l/b connection.
     *
     * @return true if current connection is the replicas l/b connection
     */
    public boolean isReplicasConnection() {
        return this.currentConnection != null && this.currentConnection == this.replicasConnection;
    }

    /**
     * Use {@link #isReplicasConnection()} instead.
     *
     * @return true if it's a replicas connection
     * @deprecated
     */
    @Deprecated
    public boolean isSlavesConnection() {
        return isReplicasConnection();
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
            // Swallow both exceptions. Replication connections must continue to "work" after swapping between sources and replicas.
        }
    }

    @Override
    void doClose() throws SQLException {
        if (this.sourceConnection != null) {
            this.sourceConnection.close();
        }
        if (this.replicasConnection != null) {
            this.replicasConnection.close();
        }

        if (this.connectionGroup != null) {
            this.connectionGroup.handleCloseConnection(this.thisAsReplicationConnection);
        }
    }

    @Override
    void doAbortInternal() throws SQLException {
        this.sourceConnection.abortInternal();
        this.replicasConnection.abortInternal();
        if (this.connectionGroup != null) {
            this.connectionGroup.handleCloseConnection(this.thisAsReplicationConnection);
        }
    }

    @Override
    void doAbort(Executor executor) throws SQLException {
        this.sourceConnection.abort(executor);
        this.replicasConnection.abort(executor);
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
        if (this.sourceHosts.isEmpty() && this.replicaHosts.isEmpty() && !ReplicationConnection.class.isAssignableFrom(method.getDeclaringClass())) {
            throw SQLError.createSQLException(Messages.getString("ReplicationConnectionProxy.noHostsInconsistentState"),
                    MysqlErrorNumbers.SQL_STATE_INVALID_TRANSACTION_STATE, MysqlErrorNumbers.ERROR_CODE_REPLICATION_CONNECTION_WITH_NO_HOSTS, true, null);
        }
    }

    /**
     * Pings both l/b connections. Switch to another connection in case of failure.
     */
    @Override
    public void doPing() throws SQLException {
        boolean isSourceConn = isSourceConnection();

        SQLException sourcesPingException = null;
        SQLException replicasPingException = null;

        if (this.sourceConnection != null) {
            try {
                this.sourceConnection.ping();
            } catch (SQLException e) {
                sourcesPingException = e;
            }
        } else {
            initializeSourceConnection();
        }

        if (this.replicasConnection != null) {
            try {
                this.replicasConnection.ping();
            } catch (SQLException e) {
                replicasPingException = e;
            }
        } else {
            try {
                initializeReplicasConnection();
                if (switchToReplicasConnectionIfNecessary()) {
                    isSourceConn = false;
                }
            } catch (SQLException e) {
                if (this.sourceConnection == null || !this.readFromSourceWhenNoReplicas) {
                    throw e;
                } // Else swallow this exception.
            }
        }

        if (isSourceConn && sourcesPingException != null) {
            // Switch to replicas connection.
            if (this.replicasConnection != null && replicasPingException == null) {
                this.sourceConnection = null;
                this.currentConnection = this.replicasConnection;
                this.readOnly = true;
            }
            throw sourcesPingException;

        } else if (!isSourceConn && (replicasPingException != null || this.replicasConnection == null)) {
            // Switch to sources connection, setting read-only state, if 'readFromSourceWhenNoReplicas=true'.
            if (this.sourceConnection != null && this.readFromSourceWhenNoReplicas && sourcesPingException == null) {
                this.replicasConnection = null;
                this.currentConnection = this.sourceConnection;
                this.readOnly = true;
                this.currentConnection.setReadOnly(true);
            }
            if (replicasPingException != null) {
                throw replicasPingException;
            }
        }
    }

    private JdbcConnection initializeSourceConnection() throws SQLException {
        this.sourceConnection = null;

        if (this.sourceHosts.size() == 0) {
            return null;
        }

        LoadBalancedConnection newSourceConn = LoadBalancedConnectionProxy
                .createProxyInstance(new LoadBalanceConnectionUrl(this.sourceHosts, this.connectionUrl.getOriginalProperties()));
        newSourceConn.setProxy(getProxy());

        this.sourceConnection = newSourceConn;
        return this.sourceConnection;
    }

    private JdbcConnection initializeReplicasConnection() throws SQLException {
        this.replicasConnection = null;

        if (this.replicaHosts.size() == 0) {
            return null;
        }

        LoadBalancedConnection newReplicasConn = LoadBalancedConnectionProxy
                .createProxyInstance(new LoadBalanceConnectionUrl(this.replicaHosts, this.connectionUrl.getOriginalProperties()));
        newReplicasConn.setProxy(getProxy());
        newReplicasConn.setReadOnly(true);

        this.replicasConnection = newReplicasConn;
        return this.replicasConnection;
    }

    private synchronized boolean switchToSourceConnection() throws SQLException {
        if (this.sourceConnection == null || this.sourceConnection.isClosed()) {
            try {
                if (initializeSourceConnection() == null) {
                    return false;
                }
            } catch (SQLException e) {
                this.currentConnection = null;
                throw e;
            }
        }
        if (!isSourceConnection() && this.sourceConnection != null) {
            syncSessionState(this.currentConnection, this.sourceConnection, false);
            this.currentConnection = this.sourceConnection;
        }
        return true;
    }

    private synchronized boolean switchToReplicasConnection() throws SQLException {
        if (this.replicasConnection == null || this.replicasConnection.isClosed()) {
            try {
                if (initializeReplicasConnection() == null) {
                    return false;
                }
            } catch (SQLException e) {
                this.currentConnection = null;
                throw e;
            }
        }
        if (!isReplicasConnection() && this.replicasConnection != null) {
            syncSessionState(this.currentConnection, this.replicasConnection, true);
            this.currentConnection = this.replicasConnection;
        }
        return true;
    }

    private boolean switchToReplicasConnectionIfNecessary() throws SQLException {
        // Switch to replicas connection:
        // - If the current connection is null. Or,
        // - If we're currently on the source and in read-only mode - we didn't have any replicas to use until now. Or,
        // - If we're currently on a closed source connection and there are no sources to connect to. Or,
        // - If we're currently not on a source connection that is closed - means that we were on a closed replicas connection before it was re-initialized.
        if (this.currentConnection == null || isSourceConnection() && (this.readOnly || this.sourceHosts.isEmpty() && this.currentConnection.isClosed())
                || !isSourceConnection() && this.currentConnection.isClosed()) {
            return switchToReplicasConnection();
        }
        return false;
    }

    public synchronized JdbcConnection getCurrentConnection() {
        return this.currentConnection == null ? LoadBalancedConnectionProxy.getNullLoadBalancedConnectionInstance() : this.currentConnection;
    }

    public long getConnectionGroupId() {
        return this.connectionGroupID;
    }

    public synchronized JdbcConnection getSourceConnection() {
        return this.sourceConnection;
    }

    /**
     * Use {@link #getSourceConnection()} instead.
     *
     * @return {@link JdbcConnection}
     * @deprecated
     */
    @Deprecated
    public synchronized JdbcConnection getMasterConnection() {
        return getSourceConnection();
    }

    public synchronized void promoteReplicaToSource(String hostPortPair) throws SQLException {
        HostInfo host = getReplicaHost(hostPortPair);
        if (host == null) {
            return;
        }
        this.sourceHosts.add(host);
        removeReplica(hostPortPair);
        if (this.sourceConnection != null) {
            this.sourceConnection.addHost(hostPortPair);
        }

        // Switch back to the sources connection if this connection was running in fail-safe mode.
        if (!this.readOnly && !isSourceConnection()) {
            switchToSourceConnection();
        }
    }

    /**
     * Use {@link #promoteReplicaToSource(String)} instead.
     *
     * @param hostPortPair
     *            host:port
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public synchronized void promoteSlaveToMaster(String hostPortPair) throws SQLException {
        promoteReplicaToSource(hostPortPair);
    }

    public synchronized void removeSourceHost(String hostPortPair) throws SQLException {
        this.removeSourceHost(hostPortPair, true);
    }

    /**
     * Use {@link #removeSourceHost(String)} instead.
     *
     * @param hostPortPair
     *            host:port
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public synchronized void removeMasterHost(String hostPortPair) throws SQLException {
        removeSourceHost(hostPortPair);
    }

    public synchronized void removeSourceHost(String hostPortPair, boolean waitUntilNotInUse) throws SQLException {
        this.removeSourceHost(hostPortPair, waitUntilNotInUse, false);
    }

    /**
     * Use {@link #removeSourceHost(String, boolean)} instead.
     *
     * @param hostPortPair
     *            host:port
     * @param waitUntilNotInUse
     *            remove only when not in use
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public synchronized void removeMasterHost(String hostPortPair, boolean waitUntilNotInUse) throws SQLException {
        removeSourceHost(hostPortPair, waitUntilNotInUse);
    }

    public synchronized void removeSourceHost(String hostPortPair, boolean waitUntilNotInUse, boolean isNowReplica) throws SQLException {
        HostInfo host = getSourceHost(hostPortPair);
        if (host == null) {
            return;
        }
        if (isNowReplica) {
            this.replicaHosts.add(host);
            resetReadFromSourceWhenNoReplicas();
        }
        this.sourceHosts.remove(host);

        // The source connection may have been implicitly closed by a previous op., don't let it stop us.
        if (this.sourceConnection == null || this.sourceConnection.isClosed()) {
            this.sourceConnection = null;
            return;
        }

        if (waitUntilNotInUse) {
            this.sourceConnection.removeHostWhenNotInUse(hostPortPair);
        } else {
            this.sourceConnection.removeHost(hostPortPair);
        }

        // Close the connection if that was the last source.
        if (this.sourceHosts.isEmpty()) {
            this.sourceConnection.close();
            this.sourceConnection = null;

            // Default behavior, no need to check this.readFromSourceWhenNoReplicas.
            switchToReplicasConnectionIfNecessary();
        }
    }

    /**
     * Use {@link #removeSourceHost(String, boolean, boolean)} instead.
     *
     * @param hostPortPair
     *            host:port
     * @param waitUntilNotInUse
     *            remove only when not in use
     * @param isNowReplica
     *            place to replicas
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public synchronized void removeMasterHost(String hostPortPair, boolean waitUntilNotInUse, boolean isNowReplica) throws SQLException {
        removeSourceHost(hostPortPair, waitUntilNotInUse, isNowReplica);
    }

    public boolean isHostSource(String hostPortPair) {
        if (hostPortPair == null) {
            return false;
        }
        return this.sourceHosts.stream().anyMatch(hi -> hostPortPair.equalsIgnoreCase(hi.getHostPortPair()));
    }

    /**
     * Use {@link #isHostSource(String)} instead.
     *
     * @param hostPortPair
     *            host:port
     * @return true if it's a source host
     * @deprecated
     */
    @Deprecated
    public boolean isHostMaster(String hostPortPair) {
        return isHostSource(hostPortPair);
    }

    public synchronized JdbcConnection getReplicasConnection() {
        return this.replicasConnection;
    }

    /**
     * Use {@link #getReplicasConnection()} instead.
     *
     * @return {@link JdbcConnection}
     * @deprecated
     */
    @Deprecated
    public synchronized JdbcConnection getSlavesConnection() {
        return getReplicasConnection();
    }

    public synchronized void addReplicaHost(String hostPortPair) throws SQLException {
        if (this.isHostReplica(hostPortPair)) {
            return;
        }
        this.replicaHosts.add(getConnectionUrl().getReplicaHostOrSpawnIsolated(hostPortPair));
        resetReadFromSourceWhenNoReplicas();
        if (this.replicasConnection == null) {
            initializeReplicasConnection();
            switchToReplicasConnectionIfNecessary();
        } else {
            this.replicasConnection.addHost(hostPortPair);
        }
    }

    /**
     * Use {@link #addReplicaHost(String)} instead.
     *
     * @param hostPortPair
     *            host:port
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public synchronized void addSlaveHost(String hostPortPair) throws SQLException {
        addReplicaHost(hostPortPair);
    }

    public synchronized void removeReplica(String hostPortPair) throws SQLException {
        removeReplica(hostPortPair, true);
    }

    /**
     * Use {@link #removeReplica(String)} instead.
     *
     * @param hostPortPair
     *            host:port
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public synchronized void removeSlave(String hostPortPair) throws SQLException {
        removeReplica(hostPortPair);
    }

    public synchronized void removeReplica(String hostPortPair, boolean closeGently) throws SQLException {
        HostInfo host = getReplicaHost(hostPortPair);
        if (host == null) {
            return;
        }
        this.replicaHosts.remove(host);
        resetReadFromSourceWhenNoReplicas();

        if (this.replicasConnection == null || this.replicasConnection.isClosed()) {
            this.replicasConnection = null;
            return;
        }

        if (closeGently) {
            this.replicasConnection.removeHostWhenNotInUse(hostPortPair);
        } else {
            this.replicasConnection.removeHost(hostPortPair);
        }

        // Close the connection if that was the last replica.
        if (this.replicaHosts.isEmpty()) {
            this.replicasConnection.close();
            this.replicasConnection = null;

            // Default behavior, no need to check this.readFromSourceWhenNoReplicas.
            switchToSourceConnection();
            if (isSourceConnection()) {
                this.currentConnection.setReadOnly(this.readOnly); // Maintain.
            }
        }
    }

    /**
     * Use {@link #removeReplica(String, boolean)} instead.
     *
     * @param hostPortPair
     *            host:port
     * @param closeGently
     *            option
     * @throws SQLException
     * @deprecated
     */
    @Deprecated
    public synchronized void removeSlave(String hostPortPair, boolean closeGently) throws SQLException {
        removeReplica(hostPortPair, closeGently);
    }

    public boolean isHostReplica(String hostPortPair) {
        if (hostPortPair == null) {
            return false;
        }
        return this.replicaHosts.stream().anyMatch(hi -> hostPortPair.equalsIgnoreCase(hi.getHostPortPair()));
    }

    /**
     * Use {@link #isHostReplica(String)} instead.
     *
     * @param hostPortPair
     *            host:port
     * @return true if it's a replica
     * @deprecated
     */
    @Deprecated
    public boolean isHostSlave(String hostPortPair) {
        return isHostReplica(hostPortPair);
    }

    public synchronized void setReadOnly(boolean readOnly) throws SQLException {
        if (readOnly) {
            if (!isReplicasConnection() || this.currentConnection.isClosed()) {
                boolean switched = true;
                SQLException exceptionCaught = null;
                try {
                    switched = switchToReplicasConnection();
                } catch (SQLException e) {
                    switched = false;
                    exceptionCaught = e;
                }
                if (!switched && this.readFromSourceWhenNoReplicas && switchToSourceConnection()) {
                    exceptionCaught = null; // The connection is OK. Cancel the exception, if any.
                }
                if (exceptionCaught != null) {
                    throw exceptionCaught;
                }
            }
        } else {
            if (!isSourceConnection() || this.currentConnection.isClosed()) {
                boolean switched = true;
                SQLException exceptionCaught = null;
                try {
                    switched = switchToSourceConnection();
                } catch (SQLException e) {
                    switched = false;
                    exceptionCaught = e;
                }
                if (!switched && switchToReplicasConnectionIfNecessary()) {
                    exceptionCaught = null; // The connection is OK. Cancel the exception, if any.
                }
                if (exceptionCaught != null) {
                    throw exceptionCaught;
                }
            }
        }
        this.readOnly = readOnly;

        /*
         * Reset sources connection read-only state if 'readFromSourceWhenNoReplicas=true'. If there are no replicas then the sources connection will be used
         * with read-only state in its place. Even if not, it must be reset from a possible previous read-only state.
         */
        if (this.readFromSourceWhenNoReplicas && isSourceConnection()) {
            this.currentConnection.setReadOnly(this.readOnly);
        }
    }

    public boolean isReadOnly() throws SQLException {
        return !isSourceConnection() || this.readOnly;
    }

    private void resetReadFromSourceWhenNoReplicas() {
        this.readFromSourceWhenNoReplicas = this.replicaHosts.isEmpty() || this.readFromSourceWhenNoReplicasOriginal;
    }

    private HostInfo getSourceHost(String hostPortPair) {
        return this.sourceHosts.stream().filter(hi -> hostPortPair.equalsIgnoreCase(hi.getHostPortPair())).findFirst().orElse(null);
    }

    private HostInfo getReplicaHost(String hostPortPair) {
        return this.replicaHosts.stream().filter(hi -> hostPortPair.equalsIgnoreCase(hi.getHostPortPair())).findFirst().orElse(null);
    }

    private ReplicationConnectionUrl getConnectionUrl() {
        return (ReplicationConnectionUrl) this.connectionUrl;
    }

}
