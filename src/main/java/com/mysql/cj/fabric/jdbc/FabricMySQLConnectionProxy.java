/*
  Copyright (c) 2013, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.fabric.jdbc;

import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TimeZone;
import java.util.Timer;
import java.util.concurrent.Executor;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.fabric.FabricMysqlConnection;
import com.mysql.cj.api.jdbc.ClientInfoProvider;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.jdbc.ha.ReplicationConnection;
import com.mysql.cj.api.jdbc.interceptors.StatementInterceptorV2;
import com.mysql.cj.api.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.conf.url.ConnectionUrl;
import com.mysql.cj.core.conf.url.HostInfo;
import com.mysql.cj.core.conf.url.ReplicationConnectionUrl;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.UnableToConnectException;
import com.mysql.cj.core.log.LogFactory;
import com.mysql.cj.fabric.FabricConnection;
import com.mysql.cj.fabric.Server;
import com.mysql.cj.fabric.ServerGroup;
import com.mysql.cj.fabric.ShardMapping;
import com.mysql.cj.fabric.exceptions.FabricCommunicationException;
import com.mysql.cj.jdbc.AbstractJdbcConnection;
import com.mysql.cj.jdbc.ServerPreparedStatement;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.ha.LoadBalancedConnectionProxy;
import com.mysql.cj.jdbc.ha.ReplicationConnectionGroup;
import com.mysql.cj.jdbc.ha.ReplicationConnectionGroupManager;
import com.mysql.cj.jdbc.ha.ReplicationConnectionProxy;
import com.mysql.cj.jdbc.result.CachedResultSetMetaData;
import com.mysql.cj.mysqla.MysqlaSession;

/**
 * A proxy to a set of MySQL servers managed by MySQL Fabric.
 * 
 * Limitations:
 * <ul>
 * <li>One shard key can be specified</li>
 * </ul>
 */
public class FabricMySQLConnectionProxy extends AbstractJdbcConnection implements FabricMysqlConnection {

    private static final long serialVersionUID = -6276256222880055706L;

    private Log log;

    protected FabricConnection fabricConnection;

    protected boolean closed = false;

    protected boolean transactionInProgress = false;

    // Set of connections created for this proxy (initialized lazily)
    protected Map<ServerGroup, ReplicationConnection> serverConnections = new HashMap<ServerGroup, ReplicationConnection>();

    // Connection currently in use for this proxy
    protected ReplicationConnection currentConnection;

    // Server selection criteria
    //      one for group selection (i.e. sharding),
    //      one for server selection (i.e. RO, global, load balancing, etc)
    protected String shardKey;
    protected String shardTable;
    protected String serverGroupName;

    protected Set<String> queryTables = new HashSet<String>();

    protected ServerGroup serverGroup;

    protected String host;
    protected String port;
    protected String username;
    protected String password;
    protected String database;

    protected ShardMapping shardMapping;

    protected boolean readOnly = false;
    protected boolean autoCommit = true;
    protected int transactionIsolation = JdbcConnection.TRANSACTION_REPEATABLE_READ;

    private String fabricShardKey;
    private String fabricShardTable;
    private String fabricServerGroup;
    private String fabricProtocol;
    private String fabricUsername;
    private String fabricPassword;
    private boolean reportErrors = false;

    protected ConnectionUrl connectionUrl;

    // Synchronized Set that holds temporary "locks" on ReplicationConnectionGroups being synced.
    // These locks are used to prevent simultaneous syncing of the state of the current group's servers.
    private static final Set<String> replConnGroupLocks = Collections.synchronizedSet(new HashSet<String>());

    public FabricMySQLConnectionProxy(ConnectionUrl connectionString) throws SQLException {
        this.connectionUrl = connectionString;

        Properties props = connectionString.getConnectionArgumentsAsProperties();

        // first, handle and remove Fabric-specific properties.  once fabricShardKey et al are ConnectionProperty instances this will be unnecessary
        this.fabricShardKey = props.getProperty(PropertyDefinitions.PNAME_fabricShardKey);
        this.fabricShardTable = props.getProperty(PropertyDefinitions.PNAME_fabricShardTable);
        this.fabricServerGroup = props.getProperty(PropertyDefinitions.PNAME_fabricServerGroup);
        this.fabricProtocol = props.getProperty(PropertyDefinitions.PNAME_fabricProtocol);
        this.fabricUsername = props.getProperty(PropertyDefinitions.PNAME_fabricUsername);
        this.fabricPassword = props.getProperty(PropertyDefinitions.PNAME_fabricPassword);
        this.reportErrors = Boolean.valueOf(props.getProperty(PropertyDefinitions.PNAME_fabricReportErrors));
        props.remove(PropertyDefinitions.PNAME_fabricShardKey);
        props.remove(PropertyDefinitions.PNAME_fabricShardTable);
        props.remove(PropertyDefinitions.PNAME_fabricServerGroup);
        props.remove(PropertyDefinitions.PNAME_fabricProtocol);
        props.remove(PropertyDefinitions.PNAME_fabricUsername);
        props.remove(PropertyDefinitions.PNAME_fabricPassword);
        props.remove(PropertyDefinitions.PNAME_fabricReportErrors);

        this.host = props.getProperty(PropertyDefinitions.HOST_PROPERTY_KEY);
        this.port = props.getProperty(PropertyDefinitions.PORT_PROPERTY_KEY);
        this.username = props.getProperty(PropertyDefinitions.PNAME_user);
        this.password = props.getProperty(PropertyDefinitions.PNAME_password);
        this.database = props.getProperty(PropertyDefinitions.DBNAME_PROPERTY_KEY);
        if (this.username == null) {
            this.username = "";
        }
        if (this.password == null) {
            this.password = "";
        }

        // add our interceptor to pass exceptions back to the `interceptException' method
        String exceptionInterceptors = props.getProperty(PropertyDefinitions.PNAME_exceptionInterceptors);
        if (exceptionInterceptors == null || "null".equals(PropertyDefinitions.PNAME_exceptionInterceptors)) {
            exceptionInterceptors = "";
        } else {
            exceptionInterceptors += ",";
        }
        exceptionInterceptors += ErrorReportingExceptionInterceptor.class.getName();
        props.setProperty(PropertyDefinitions.PNAME_exceptionInterceptors, exceptionInterceptors);

        getPropertySet().initializeProperties(props);

        // validation check of properties
        if (this.fabricServerGroup != null && this.fabricShardTable != null) {
            throw SQLError.createSQLException("Server group and shard table are mutually exclusive. Only one may be provided.",
                    SQLError.SQL_STATE_CONNECTION_REJECTED, null, getExceptionInterceptor(), this);
        }

        try {
            String url = this.fabricProtocol + "://" + this.host + ":" + this.port;
            this.fabricConnection = new FabricConnection(url, this.fabricUsername, this.fabricPassword);
        } catch (FabricCommunicationException ex) {
            throw SQLError.createSQLException("Unable to establish connection to the Fabric server", SQLError.SQL_STATE_CONNECTION_REJECTED, ex,
                    getExceptionInterceptor(), this);
        }

        // initialize log before any further calls that might actually use it
        this.log = LogFactory.getLogger(getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_logger).getStringValue(),
                "FabricMySQLConnectionProxy", null);

        setShardTable(this.fabricShardTable);
        setShardKey(this.fabricShardKey);

        setServerGroupName(this.fabricServerGroup);
    }

    /**
     * Deal with an exception thrown on an underlying connection. We only consider connection exceptions (SQL State 08xxx). We internally handle a possible
     * failover situation.
     *
     * @param sqlEx
     * @param conn
     * @param group
     * @param hostname
     * @param portNumber
     * @throws FabricCommunicationException
     */
    synchronized SQLException interceptException(Exception sqlEx, MysqlConnection conn, String groupName, String hostname, String portNumber)
            throws FabricCommunicationException {
        // we are only concerned with connection failures, skip anything else
        if (!(sqlEx instanceof SQLException && ((SQLException) sqlEx).getSQLState() != null
                && (((SQLException) sqlEx).getSQLState().startsWith("08") || SQLNonTransientConnectionException.class.isAssignableFrom(sqlEx.getClass()))

        )) {
            return null;
        }

        // find the Server corresponding to this connection
        Server currentServer = this.serverGroup.getServer(hostname + ":" + portNumber);

        // we have already failed over or dealt with this connection, let the exception propagate
        if (currentServer == null) {
            return null;
        }

        // report error (if necessary)
        if (this.reportErrors) {
            this.fabricConnection.getClient().reportServerError(currentServer, sqlEx.toString(), true);
        }

        // no need for concurrent threads to duplicate this work
        if (replConnGroupLocks.add(this.serverGroup.getName())) {
            try {
                // refresh group status. (after reporting the error. error reporting may trigger a failover)
                try {
                    this.fabricConnection.refreshState();
                    setCurrentServerGroup(this.serverGroup.getName());
                } catch (SQLException ex) {
                    return SQLError.createSQLException("Unable to refresh Fabric state. Failover impossible", SQLError.SQL_STATE_CONNECTION_FAILURE, ex, null);
                }

                // propagate to repl conn group
                try {
                    syncGroupServersToReplicationConnectionGroup(ReplicationConnectionGroupManager.getConnectionGroup(groupName));
                } catch (SQLException ex) {
                    return ex;
                }
            } finally {
                replConnGroupLocks.remove(this.serverGroup.getName());
            }
        } else {
            return SQLError.createSQLException("Fabric state syncing already in progress in another thread.", SQLError.SQL_STATE_CONNECTION_FAILURE, sqlEx,
                    null);
        }
        return null;
    }

    /**
     * Refresh the client Fabric state cache if the TTL has expired.
     */
    private void refreshStateIfNecessary() throws SQLException {
        if (this.fabricConnection.isStateExpired()) {
            try {
                this.fabricConnection.refreshState();
            } catch (FabricCommunicationException ex) {
                throw SQLError.createSQLException("Unable to establish connection to the Fabric server", SQLError.SQL_STATE_CONNECTION_REJECTED, ex,
                        getExceptionInterceptor(), this);
            }
            if (this.serverGroup != null) {
                setCurrentServerGroup(this.serverGroup.getName());
            }
        }
    }

    /////////////////////////////////////////
    // Server selection criteria and logic //
    /////////////////////////////////////////
    public void setShardKey(String shardKey) throws SQLException {
        ensureNoTransactionInProgress();

        this.currentConnection = null;

        if (shardKey != null) {
            if (this.serverGroupName != null) {
                throw SQLError.createSQLException("Shard key cannot be provided when server group is chosen directly.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        null, getExceptionInterceptor(), this);
            } else if (this.shardTable == null) {
                throw SQLError.createSQLException("Shard key cannot be provided without a shard table.", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null,
                        getExceptionInterceptor(), this);
            }

            // sharded group selection
            setCurrentServerGroup(this.shardMapping.getGroupNameForKey(shardKey));
        } else if (this.shardTable != null) {
            setCurrentServerGroup(this.shardMapping.getGlobalGroupName());
        }
        this.shardKey = shardKey;
    }

    public String getShardKey() {
        return this.shardKey;
    }

    public void setShardTable(String shardTable) throws SQLException {
        ensureNoTransactionInProgress();

        this.currentConnection = null;

        if (this.serverGroupName != null) {
            throw SQLError.createSQLException("Server group and shard table are mutually exclusive. Only one may be provided.",
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null, getExceptionInterceptor(), this);
        }

        this.shardKey = null;
        this.serverGroup = null;
        this.shardTable = shardTable;
        if (shardTable == null) {
            this.shardMapping = null;
        } else {
            // lookup shard mapping
            String table = shardTable;
            String db = this.database;
            if (shardTable.contains(".")) {
                String pair[] = shardTable.split("\\.");
                table = pair[0];
                db = pair[1];
            }
            try {
                this.shardMapping = this.fabricConnection.getShardMapping(db, table);
                if (this.shardMapping == null) {
                    throw SQLError.createSQLException("Shard mapping not found for table `" + shardTable + "'", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null,
                            getExceptionInterceptor(), this);
                }
                // default to global group
                setCurrentServerGroup(this.shardMapping.getGlobalGroupName());

            } catch (FabricCommunicationException ex) {
                throw SQLError.createSQLException("Fabric communication failure.", SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE, ex, getExceptionInterceptor(),
                        this);
            }
        }
    }

    public String getShardTable() {
        return this.shardTable;
    }

    public void setServerGroupName(String serverGroupName) throws SQLException {
        ensureNoTransactionInProgress();

        this.currentConnection = null;

        // direct group selection
        if (serverGroupName != null) {
            setCurrentServerGroup(serverGroupName);
        }

        this.serverGroupName = serverGroupName;
    }

    public String getServerGroupName() {
        return this.serverGroupName;
    }

    public void clearServerSelectionCriteria() throws SQLException {
        ensureNoTransactionInProgress();
        this.shardTable = null;
        this.shardKey = null;
        this.serverGroupName = null;
        this.serverGroup = null;
        this.queryTables.clear();
        this.currentConnection = null;
    }

    public ServerGroup getCurrentServerGroup() {
        return this.serverGroup;
    }

    public void clearQueryTables() throws SQLException {
        ensureNoTransactionInProgress();

        this.currentConnection = null;

        this.queryTables.clear();
        setShardTable(null);
    }

    /**
     * Add a table to the set of tables used for the next query on this connection.
     * This is used for:
     * <ul>
     * <li>Choosing a shard given the tables used</li>
     * <li>Preventing cross-shard queries</li>
     * </ul>
     */
    public void addQueryTable(String tableName) throws SQLException {
        ensureNoTransactionInProgress();

        this.currentConnection = null;

        try {
            // choose shard mapping if necessary
            if (this.shardMapping == null) {
                if (this.fabricConnection.getShardMapping(this.database, tableName) != null) {
                    setShardTable(tableName);
                }
            } else { // make sure we aren't in conflict with the chosen shard mapping
                ShardMapping mappingForTableName = this.fabricConnection.getShardMapping(this.database, tableName);
                if (mappingForTableName != null && !mappingForTableName.equals(this.shardMapping)) {
                    throw SQLError.createSQLException("Cross-shard query not allowed", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null, getExceptionInterceptor(),
                            this);
                }
            }
            this.queryTables.add(tableName);
        } catch (FabricCommunicationException ex) {
            throw SQLError.createSQLException("Fabric communication failure.", SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE, ex, getExceptionInterceptor(),
                    this);
        }
    }

    /**
     * The set of tables to be used in the next query on this connection.
     */
    public Set<String> getQueryTables() {
        return this.queryTables;
    }

    /**
     * Change the server group to the given named group.
     */
    protected void setCurrentServerGroup(String serverGroupName) throws SQLException {
        this.serverGroup = null;

        try {
            this.serverGroup = this.fabricConnection.getServerGroup(serverGroupName);
        } catch (FabricCommunicationException ex) {
            throw SQLError.createSQLException("Fabric communication failure.", SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE, ex, getExceptionInterceptor(),
                    this);
        }

        if (this.serverGroup == null) {
            throw SQLError.createSQLException("Cannot find server group: `" + serverGroupName + "'", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null,
                    getExceptionInterceptor(), this);
        }

        // check for any changes that need to be propagated to the entire group
        ReplicationConnectionGroup replConnGroup = ReplicationConnectionGroupManager.getConnectionGroup(serverGroupName);
        if (replConnGroup != null) {
            if (replConnGroupLocks.add(this.serverGroup.getName())) {
                try {
                    syncGroupServersToReplicationConnectionGroup(replConnGroup);
                } finally {
                    replConnGroupLocks.remove(this.serverGroup.getName());
                }
            }
        }
    }

    //////////////////////////////////////////////////////
    // Methods dealing with state internal to the proxy //
    //////////////////////////////////////////////////////
    /**
     * Get the active connection as an object implementing the
     * internal JdbcConnection interface. This should not be used
     * unless a JdbcConnection is required.
     * 
     * {@link getActiveConnection()} is provided for the general case.
     * The returned object is not a {@link ReplicationConnection}, but
     * instead the {@link LoadBalancedConnectionProxy} for either the
     * master or slaves.
     */
    protected JdbcConnection getActiveMySQLConnection() throws SQLException {
        ReplicationConnection c = (ReplicationConnection) getActiveConnection();
        JdbcConnection mc = c.getCurrentConnection();
        return mc;
    }

    protected JdbcConnection getActiveMySQLConnectionPassive() {
        try {
            return getActiveMySQLConnection();
        } catch (SQLException ex) {
            throw new IllegalStateException("Unable to determine active connection", ex);
        }
    }

    protected JdbcConnection getActiveConnectionPassive() {
        try {
            return getActiveConnection();
        } catch (SQLException ex) {
            throw new IllegalStateException("Unable to determine active connection", ex);
        }
    }

    /**
     * Sync the state of the current group's servers to that of the given replication connection group. This is necessary as:
     * <ul>
     * <li>New connections have updated state from the Fabric server</li>
     * <li>Failover scenarios may update state and it should be propagated across the active connections</li>
     * </ul>
     */
    private void syncGroupServersToReplicationConnectionGroup(ReplicationConnectionGroup replConnGroup) throws SQLException {
        String currentMasterString = null;
        if (replConnGroup.getMasterHosts().size() == 1) {
            currentMasterString = replConnGroup.getMasterHosts().iterator().next();
        }
        // check if master has changed
        if (currentMasterString != null
                && (this.serverGroup.getMaster() == null || !currentMasterString.equals(this.serverGroup.getMaster().getHostPortString()))) {
            // old master is gone (there may be a new one) (closeGently=false)
            try {
                replConnGroup.removeMasterHost(currentMasterString, false);
            } catch (SQLException ex) {
                // effectively ignored
                getLog().logWarn("Unable to remove master: " + currentMasterString, ex);
            }
        }

        // add new master (if exists and the old master was absent has been removed)
        Server newMaster = this.serverGroup.getMaster();
        if (newMaster != null && replConnGroup.getMasterHosts().size() == 0) {
            getLog().logInfo("Changing master for group '" + replConnGroup.getGroupName() + "' to: " + newMaster);
            try {
                if (!replConnGroup.getSlaveHosts().contains(newMaster.getHostPortString())) {
                    replConnGroup.addSlaveHost(newMaster.getHostPortString());
                }
                replConnGroup.promoteSlaveToMaster(newMaster.getHostPortString());
            } catch (SQLException ex) {
                throw SQLError.createSQLException("Unable to promote new master '" + newMaster.toString() + "'", ex.getSQLState(), ex, null);
            }
        }

        // synchronize HA group state with replication connection group in two steps:
        // 1. add any new slaves to the connection group
        for (Server s : this.serverGroup.getServers()) {
            if (s.isSlave()) {
                // this is a no-op if the slave is already present
                try {
                    replConnGroup.addSlaveHost(s.getHostPortString());
                } catch (SQLException ex) {
                    // effectively ignored
                    getLog().logWarn("Unable to add slave: " + s.toString(), ex);
                }
            }
        }
        // 2. remove any old slaves from the connection group
        for (String hostPortString : replConnGroup.getSlaveHosts()) {
            Server fabServer = this.serverGroup.getServer(hostPortString);
            if (fabServer == null || !(fabServer.isSlave())) {
                try {
                    replConnGroup.removeSlaveHost(hostPortString, true);
                } catch (SQLException ex) {
                    // effectively ignored
                    getLog().logWarn("Unable to remove slave: " + hostPortString, ex);
                }
            }
        }
    }

    protected JdbcConnection getActiveConnection() throws SQLException {
        if (!this.transactionInProgress) {
            refreshStateIfNecessary();
        }

        if (this.currentConnection != null) {
            return this.currentConnection;
        }

        if (getCurrentServerGroup() == null) {
            throw SQLError.createSQLException("No server group selected.", SQLError.SQL_STATE_CONNECTION_REJECTED, null, getExceptionInterceptor(), this);
        }

        // try to find an existing replication connection to the current group
        this.currentConnection = this.serverConnections.get(this.serverGroup);
        if (this.currentConnection != null) {
            return this.currentConnection;
        }

        // otherwise, build a replication connection to the current group
        List<HostInfo> masterHost = new ArrayList<>();
        List<HostInfo> slaveHosts = new ArrayList<>();
        for (Server s : this.serverGroup.getServers()) {
            if (s.isMaster()) {
                masterHost.add(this.connectionUrl.getHostOrSpawnIsolated(s.getHostPortString()));
            } else if (s.isSlave()) {
                slaveHosts.add(this.connectionUrl.getHostOrSpawnIsolated(s.getHostPortString()));
            }
        }
        ReplicationConnectionGroup replConnGroup = ReplicationConnectionGroupManager.getConnectionGroup(this.serverGroup.getName());
        if (replConnGroup != null) {
            if (replConnGroupLocks.add(this.serverGroup.getName())) {
                try {
                    syncGroupServersToReplicationConnectionGroup(replConnGroup);
                } finally {
                    replConnGroupLocks.remove(this.serverGroup.getName());
                }
            }
        }

        Map<String, String> connProperties = this.connectionUrl.getOriginalProperties();
        connProperties.put(PropertyDefinitions.PNAME_replicationConnectionGroup, this.serverGroup.getName());
        connProperties.put(PropertyDefinitions.PNAME_user, this.username);
        connProperties.put(PropertyDefinitions.PNAME_password, this.password);
        connProperties.put(PropertyDefinitions.DBNAME_PROPERTY_KEY, getCatalog());
        connProperties.put(PropertyDefinitions.PNAME_connectionAttributes, "fabricHaGroup:" + this.serverGroup.getName());
        connProperties.put(PropertyDefinitions.PNAME_retriesAllDown, "1");
        connProperties.put(PropertyDefinitions.PNAME_allowMasterDownConnections, "true");
        connProperties.put(PropertyDefinitions.PNAME_allowSlaveDownConnections, "true");
        connProperties.put(PropertyDefinitions.PNAME_readFromMasterWhenNoSlaves, "true");
        this.currentConnection = ReplicationConnectionProxy.createProxyInstance(new ReplicationConnectionUrl(masterHost, slaveHosts, connProperties));

        this.serverConnections.put(this.serverGroup, this.currentConnection);

        this.currentConnection.setProxy(this);
        this.currentConnection.setAutoCommit(this.autoCommit);
        this.currentConnection.setReadOnly(this.readOnly);
        this.currentConnection.setTransactionIsolation(this.transactionIsolation);
        return this.currentConnection;
    }

    private void ensureOpen() throws SQLException {
        if (this.closed) {
            throw SQLError.createSQLException("No operations allowed after connection closed.", SQLError.SQL_STATE_CONNECTION_NOT_OPEN,
                    getExceptionInterceptor());
        }
    }

    private void ensureNoTransactionInProgress() throws SQLException {
        ensureOpen();
        if (this.transactionInProgress && !this.autoCommit) {
            throw SQLError.createSQLException("Not allow while a transaction is active.", "25000", getExceptionInterceptor());
        }
    }

    /**
     * Close this connection proxy which entails closing all
     * open connections to MySQL servers.
     */
    public void close() throws SQLException {
        this.closed = true;
        for (JdbcConnection c : this.serverConnections.values()) {
            try {
                c.close();
            } catch (SQLException ex) {
            }
        }
    }

    public boolean isClosed() {
        return this.closed;
    }

    /**
     * @param timeout
     * @throws SQLException
     */
    public boolean isValid(int timeout) throws SQLException {
        return !this.closed;
    }

    public void setReadOnly(boolean readOnly) throws SQLException {
        this.readOnly = readOnly;
        for (ReplicationConnection conn : this.serverConnections.values()) {
            conn.setReadOnly(readOnly);
        }
    }

    public boolean isReadOnly() throws SQLException {
        return this.readOnly;
    }

    public boolean isReadOnly(boolean useSessionStatus) throws SQLException {
        return this.readOnly;
    }

    public void setCatalog(String catalog) throws SQLException {
        this.database = catalog;
        for (JdbcConnection c : this.serverConnections.values()) {
            c.setCatalog(catalog);
        }
    }

    public String getCatalog() {
        return this.database;
    }

    public void rollback() throws SQLException {
        getActiveConnection().rollback();
        transactionCompleted();
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        getActiveConnection().rollback();
        transactionCompleted();
    }

    public void commit() throws SQLException {
        getActiveConnection().commit();
        transactionCompleted();
    }

    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
        for (JdbcConnection c : this.serverConnections.values()) {
            c.setAutoCommit(this.autoCommit);
        }
    }

    public void transactionBegun() throws SQLException {
        if (!this.autoCommit) {
            this.transactionInProgress = true;
        }
    }

    public void transactionCompleted() throws SQLException {
        this.transactionInProgress = false;
        refreshStateIfNecessary();
    }

    public boolean getAutoCommit() {
        return this.autoCommit;
    }

    public JdbcConnection getMultiHostSafeProxy() {
        return getActiveMySQLConnectionPassive();
    }

    ////////////////////////////////////////////////////////
    // Methods applying changes to all active connections //
    ////////////////////////////////////////////////////////
    public void setTransactionIsolation(int level) throws SQLException {
        this.transactionIsolation = level;
        for (JdbcConnection c : this.serverConnections.values()) {
            c.setTransactionIsolation(level);
        }
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        for (JdbcConnection c : this.serverConnections.values()) {
            c.setTypeMap(map);
        }
    }

    public void setHoldability(int holdability) throws SQLException {
        for (JdbcConnection c : this.serverConnections.values()) {
            c.setHoldability(holdability);
        }
    }

    public void setProxy(JdbcConnection proxy) {
    }

    //////////////////////////////////////////////////////////
    // Methods delegating directly to the active connection //
    //////////////////////////////////////////////////////////
    public Savepoint setSavepoint() throws SQLException {
        return getActiveConnection().setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        this.transactionInProgress = true;
        return getActiveConnection().setSavepoint(name);
    }

    public void releaseSavepoint(Savepoint savepoint) {
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        transactionBegun();
        return getActiveConnection().prepareCall(sql);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        transactionBegun();
        return getActiveConnection().prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        transactionBegun();
        return getActiveConnection().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        transactionBegun();
        return getActiveConnection().prepareStatement(sql);
    }

    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        transactionBegun();
        return getActiveConnection().prepareStatement(sql, autoGeneratedKeys);
    }

    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        transactionBegun();
        return getActiveConnection().prepareStatement(sql, columnIndexes);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        transactionBegun();
        return getActiveConnection().prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        transactionBegun();
        return getActiveConnection().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        transactionBegun();
        return getActiveConnection().prepareStatement(sql, columnNames);
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql) throws SQLException {
        transactionBegun();
        return getActiveConnection().clientPrepareStatement(sql);
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        transactionBegun();
        return getActiveConnection().clientPrepareStatement(sql, autoGenKeyIndex);
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        transactionBegun();
        return getActiveConnection().clientPrepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        transactionBegun();
        return getActiveConnection().clientPrepareStatement(sql, autoGenKeyIndexes);
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        transactionBegun();
        return getActiveConnection().clientPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        transactionBegun();
        return getActiveConnection().clientPrepareStatement(sql, autoGenKeyColNames);
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql) throws SQLException {
        transactionBegun();
        return getActiveConnection().serverPrepareStatement(sql);
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        transactionBegun();
        return getActiveConnection().serverPrepareStatement(sql, autoGenKeyIndex);
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        transactionBegun();
        return getActiveConnection().serverPrepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        transactionBegun();
        return getActiveConnection().serverPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        transactionBegun();
        return getActiveConnection().serverPrepareStatement(sql, autoGenKeyIndexes);
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        transactionBegun();
        return getActiveConnection().serverPrepareStatement(sql, autoGenKeyColNames);
    }

    public Statement createStatement() throws SQLException {
        transactionBegun();
        return getActiveConnection().createStatement();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        transactionBegun();
        return getActiveConnection().createStatement(resultSetType, resultSetConcurrency);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        transactionBegun();
        return getActiveConnection().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, PacketPayload packet, boolean streamResults,
            String catalog, ColumnDefinition cachedMetadata) throws SQLException {
        return getActiveMySQLConnection().execSQL(callingStatement, sql, maxRows, packet, streamResults, catalog, cachedMetadata);
    }

    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, PacketPayload packet, boolean streamResults,
            String catalog, ColumnDefinition cachedMetadata, boolean isBatch) throws SQLException {
        return getActiveMySQLConnection().execSQL(callingStatement, sql, maxRows, packet, streamResults, catalog, cachedMetadata, isBatch);
    }

    public StringBuilder generateConnectionCommentBlock(StringBuilder buf) {
        return getActiveMySQLConnectionPassive().generateConnectionCommentBlock(buf);
    }

    /**
     * Only valid until the end of the transaction.
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        return getActiveConnection().getMetaData();
    }

    public String getCharacterSetMetadata() {
        return getActiveMySQLConnectionPassive().getCharacterSetMetadata();
    }

    public java.sql.Statement getMetadataSafeStatement() throws SQLException {
        return getActiveMySQLConnection().getMetadataSafeStatement();
    }

    /**
     * Methods doing essentially nothing
     * 
     * @param iface
     */
    public boolean isWrapperFor(Class<?> iface) {
        return false;
    }

    /**
     * @param iface
     */
    public <T> T unwrap(Class<T> iface) {
        return null;
    }

    public void unSafeStatementInterceptors() throws SQLException {
    }

    public void createNewIO(boolean isForReconnect) {
        SQLException ex = SQLError.createSQLFeatureNotSupportedException();
        throw ExceptionFactory.createException(UnableToConnectException.class, ex.getMessage(), ex);
    }

    /**
     * 
     * @param query
     */
    public void dumpTestcaseQuery(String query) {
        // no-op
    }

    public void abortInternal() throws SQLException {
        // no-op
    }

    public boolean isServerLocal() throws SQLException {
        // Fabric doesn't support pipes
        return false;
    }

    public void shutdownServer() throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Deprecated
    public void clearHasTriedMaster() {
        // no-op
    }

    @Deprecated
    public boolean hasTriedMaster() {
        return false;
    }

    // This proxy is not XA-aware
    public boolean isInGlobalTx() {
        return false;
    }

    // This proxy is not XA-aware
    public void setInGlobalTx(boolean flag) {
        throw new RuntimeException("Global transactions not supported.");
    }

    public void changeUser(String userName, String newPassword) throws SQLException {
        throw SQLError.createSQLException("User change not allowed.", getExceptionInterceptor());
    }

    /////////////////////////////////////
    // FabricMySQLConnectionProperties //
    /////////////////////////////////////
    public void setFabricShardKey(String value) {
        this.fabricShardKey = value;
    }

    public String getFabricShardKey() {
        return this.fabricShardKey;
    }

    public void setFabricShardTable(String value) {
        this.fabricShardTable = value;
    }

    public String getFabricShardTable() {
        return this.fabricShardTable;
    }

    public void setFabricServerGroup(String value) {
        this.fabricServerGroup = value;
    }

    public String getFabricServerGroup() {
        return this.fabricServerGroup;
    }

    public void setFabricProtocol(String value) {
        this.fabricProtocol = value;
    }

    public String getFabricProtocol() {
        return this.fabricProtocol;
    }

    public void setFabricUsername(String value) {
        this.fabricUsername = value;
    }

    public String getFabricUsername() {
        return this.fabricUsername;
    }

    public void setFabricPassword(String value) {
        this.fabricPassword = value;
    }

    public String getFabricPassword() {
        return this.fabricPassword;
    }

    public void setFabricReportErrors(boolean value) {
        this.reportErrors = value;
    }

    public boolean getFabricReportErrors() {
        return this.reportErrors;
    }

    ///////////////////////////////////////////////////////
    // ConnectionProperties - applied to all connections //
    ///////////////////////////////////////////////////////

    /*
     * @Override
     * public void setAuthenticationPlugins(String authenticationPlugins) {
     * super.setAuthenticationPlugins(authenticationPlugins);
     * for (JdbcConnectionProperties cp : this.serverConnections.values()) {
     * cp.setAuthenticationPlugins(authenticationPlugins);
     * }
     * }
     */

    public int getActiveStatementCount() {
        return -1;
    }

    public long getIdleFor() {
        return -1;
    }

    public Log getLog() {
        return this.log;
    }

    public boolean isMasterConnection() {
        return false;
    }

    public boolean isNoBackslashEscapesSet() {
        return false;
    }

    public boolean isSameResource(JdbcConnection c) {
        return false;
    }

    public void ping() throws SQLException {
    }

    public void resetServerState() throws SQLException {
    }

    public void setFailedOver(boolean flag) {
    }

    public void setStatementComment(String comment) {
    }

    public void reportQueryTime(long millisOrNanos) {
    }

    public boolean isAbonormallyLongQuery(long millisOrNanos) {
        return false;
    }

    public int getAutoIncrementIncrement() {
        return -1;
    }

    public boolean hasSameProperties(JdbcConnection c) {
        return false;
    }

    public Properties getProperties() {
        return null;
    }

    public void setSchema(String schema) throws SQLException {
    }

    public String getSchema() throws SQLException {
        return null;
    }

    public void abort(Executor executor) throws SQLException {
    }

    public void setNetworkTimeout(Executor executor, final int milliseconds) throws SQLException {
    }

    public int getNetworkTimeout() throws SQLException {
        return -1;
    }

    public void checkClosed() {
    }

    public Object getConnectionMutex() {
        return this;
    }

    public void setSessionMaxRows(int max) throws SQLException {
        for (JdbcConnection c : this.serverConnections.values()) {
            c.setSessionMaxRows(max);
        }
    }

    public int getSessionMaxRows() {
        return getActiveConnectionPassive().getSessionMaxRows();
    }

    public boolean isProxySet() {
        return false;
    }

    public JdbcConnection duplicate() throws SQLException {
        return null;
    }

    public CachedResultSetMetaData getCachedMetaData(String sql) {
        return null;
    }

    public Timer getCancelTimer() {
        return null;
    }

    /**
     * @deprecated replaced by <code>getEncodingForIndex(int charsetIndex)</code>
     */
    @Deprecated
    public String getCharsetNameForIndex(int charsetIndex) throws SQLException {
        return getEncodingForIndex(charsetIndex);
    }

    /**
     * 
     * @param charsetIndex
     * @return
     */
    public String getEncodingForIndex(int charsetIndex) {
        return null;
    }

    public TimeZone getDefaultTimeZone() {
        return null;
    }

    public String getErrorMessageEncoding() {
        return null;
    }

    @Override
    public ExceptionInterceptor getExceptionInterceptor() {
        if (this.currentConnection == null) {
            return null;
        }

        return getActiveConnectionPassive().getExceptionInterceptor();
    }

    public String getHost() {
        return null;
    }

    public long getId() {
        return -1;
    }

    /**
     * 
     * @param javaCharsetName
     * @return
     */
    public int getMaxBytesPerChar(String javaCharsetName) {
        return -1;
    }

    /**
     * 
     * @param charsetIndex
     * @param javaCharsetName
     * @return
     */
    public int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName) {
        return -1;
    }

    public int getNetBufferLength() {
        return -1;
    }

    public boolean getRequiresEscapingEncoder() {
        return false;
    }

    public MysqlaSession getSession() {
        try {
            return getActiveConnection().getSession();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public ServerVersion getServerVersion() {
        return getActiveMySQLConnectionPassive().getServerVersion();
    }

    public String getStatementComment() {
        return null;
    }

    public List<StatementInterceptorV2> getStatementInterceptorsInstances() {
        return null;
    }

    public String getURL() {
        return null;
    }

    public String getUser() {
        return null;
    }

    public void incrementNumberOfPreparedExecutes() {
    }

    public void incrementNumberOfPrepares() {
    }

    public void incrementNumberOfResultSetsCreated() {
    }

    public void initializeResultsMetadataFromCache(String sql, CachedResultSetMetaData cachedMetaData, ResultSetInternalMethods resultSet) throws SQLException {
    }

    public void initializeSafeStatementInterceptors() throws SQLException {
    }

    public boolean isCursorFetchEnabled() throws SQLException {
        return false;
    }

    public boolean isReadInfoMsgEnabled() {
        return false;
    }

    public boolean lowerCaseTableNames() {
        return false;
    }

    /**
     * @param stmt
     */
    public void maxRowsChanged(com.mysql.cj.api.jdbc.Statement stmt) {
    }

    public void pingInternal(boolean checkForClosedConnection, int timeoutMillis) throws SQLException {
    }

    public void realClose(boolean calledExplicitly, boolean issueRollback, boolean skipLocalTeardown, Throwable reason) throws SQLException {
    }

    public void recachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException {
    }

    public void registerQueryExecutionTime(long queryTimeMs) {
    }

    public void registerStatement(com.mysql.cj.api.jdbc.Statement stmt) {
    }

    public void reportNumberOfTablesAccessed(int numTablesAccessed) {
    }

    public boolean serverSupportsConvertFn() throws SQLException {
        return false;
    }

    public void setReadInfoMsgEnabled(boolean flag) {
    }

    public void setReadOnlyInternal(boolean readOnlyFlag) throws SQLException {
    }

    public boolean storesLowerCaseTableName() {
        return false;
    }

    public void throwConnectionClosedException() throws SQLException {
    }

    public void unregisterStatement(com.mysql.cj.api.jdbc.Statement stmt) {
    }

    /**
     * @param stmt
     * @throws SQLException
     */
    public void unsetMaxRows(com.mysql.cj.api.jdbc.Statement stmt) throws SQLException {
    }

    public boolean useAnsiQuotedIdentifiers() {
        return false;
    }

    public boolean useMaxRows() {
        return false;
    }

    // java.sql.Connection
    public void clearWarnings() {
    }

    public Properties getClientInfo() {
        return null;
    }

    public String getClientInfo(String name) {
        return null;
    }

    public int getHoldability() {
        return -1;
    }

    public int getTransactionIsolation() {
        return -1;
    }

    public Map<String, Class<?>> getTypeMap() {
        return null;
    }

    public SQLWarning getWarnings() {
        return null;
    }

    public String nativeSQL(String sql) {
        return null;
    }

    public ProfilerEventHandler getProfilerEventHandlerInstance() {
        return null;
    }

    /**
     * 
     * @param h
     */
    public void setProfilerEventHandlerInstance(ProfilerEventHandler h) {
    }

    public void decachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException {
    }

    public Blob createBlob() {
        try {
            transactionBegun();
            return getActiveConnection().createBlob();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public Clob createClob() {
        try {
            transactionBegun();
            return getActiveConnection().createClob();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public NClob createNClob() {
        try {
            transactionBegun();
            return getActiveConnection().createNClob();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }

    public SQLXML createSQLXML() throws SQLException {
        transactionBegun();
        return getActiveConnection().createSQLXML();
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        for (JdbcConnection c : this.serverConnections.values()) {
            c.setClientInfo(properties);
        }
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        for (JdbcConnection c : this.serverConnections.values()) {
            c.setClientInfo(name, value);
        }
    }

    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return getActiveConnection().createArrayOf(typeName, elements);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        transactionBegun();
        return getActiveConnection().createStruct(typeName, attributes);
    }

    @Override
    public String getProcessHost() {
        try {
            return getActiveConnection().getProcessHost();
        } catch (SQLException ex) {
            throw ExceptionFactory.createException(ex.getMessage(), ex);
        }
    }

    @Override
    public ClientInfoProvider getClientInfoProviderImpl() throws SQLException {
        return getActiveConnection().getClientInfoProviderImpl();
    }

    @Override
    public String getHostPortPair() {
        return null;
    }

}
