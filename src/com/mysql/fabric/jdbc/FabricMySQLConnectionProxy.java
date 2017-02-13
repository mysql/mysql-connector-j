/*
  Copyright (c) 2013, 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.fabric.jdbc;

import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Calendar;
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

import com.mysql.fabric.FabricCommunicationException;
import com.mysql.fabric.FabricConnection;
import com.mysql.fabric.Server;
import com.mysql.fabric.ServerGroup;
import com.mysql.fabric.ShardMapping;
import com.mysql.jdbc.Buffer;
import com.mysql.jdbc.CachedResultSetMetaData;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.ConnectionProperties;
import com.mysql.jdbc.ConnectionPropertiesImpl;
import com.mysql.jdbc.ExceptionInterceptor;
import com.mysql.jdbc.Extension;
import com.mysql.jdbc.Field;
import com.mysql.jdbc.LoadBalancedConnectionProxy;
import com.mysql.jdbc.MySQLConnection;
import com.mysql.jdbc.MysqlIO;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.ReplicationConnection;
import com.mysql.jdbc.ReplicationConnectionGroup;
import com.mysql.jdbc.ReplicationConnectionGroupManager;
import com.mysql.jdbc.ReplicationConnectionProxy;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.SQLError;
import com.mysql.jdbc.ServerPreparedStatement;
import com.mysql.jdbc.SingleByteCharsetConverter;
import com.mysql.jdbc.StatementImpl;
import com.mysql.jdbc.StatementInterceptorV2;
import com.mysql.jdbc.Util;
import com.mysql.jdbc.exceptions.MySQLNonTransientConnectionException;
import com.mysql.jdbc.log.Log;
import com.mysql.jdbc.log.LogFactory;
import com.mysql.jdbc.profiler.ProfilerEventHandler;

/**
 * A proxy to a set of MySQL servers managed by MySQL Fabric.
 * 
 * Limitations:
 * <ul>
 * <li>One shard key can be specified</li>
 * </ul>
 */
public class FabricMySQLConnectionProxy extends ConnectionPropertiesImpl implements FabricMySQLConnection, FabricMySQLConnectionProperties {

    private static final long serialVersionUID = 5845485979107347258L;

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
    protected int transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ;

    private String fabricShardKey;
    private String fabricShardTable;
    private String fabricServerGroup;
    private String fabricProtocol;
    private String fabricUsername;
    private String fabricPassword;
    private boolean reportErrors = false;

    // Synchronized Set that holds temporary "locks" on ReplicationConnectionGroups being synced.
    // These locks are used to prevent simultaneous syncing of the state of the current group's servers.
    private static final Set<String> replConnGroupLocks = Collections.synchronizedSet(new HashSet<String>());

    private static final Class<?> JDBC4_NON_TRANSIENT_CONN_EXCEPTION;

    static {
        Class<?> clazz = null;
        try {
            if (Util.isJdbc4()) {
                clazz = Class.forName("com.mysql.jdbc.exceptions.jdbc4.MySQLNonTransientConnectionException");
            }
        } catch (ClassNotFoundException e) {
            // no-op
        }
        JDBC4_NON_TRANSIENT_CONN_EXCEPTION = clazz;
    }

    public FabricMySQLConnectionProxy(Properties props) throws SQLException {
        // first, handle and remove Fabric-specific properties.  once fabricShardKey et al are ConnectionProperty instances this will be unnecessary
        this.fabricShardKey = props.getProperty(FabricMySQLDriver.FABRIC_SHARD_KEY_PROPERTY_KEY);
        this.fabricShardTable = props.getProperty(FabricMySQLDriver.FABRIC_SHARD_TABLE_PROPERTY_KEY);
        this.fabricServerGroup = props.getProperty(FabricMySQLDriver.FABRIC_SERVER_GROUP_PROPERTY_KEY);
        this.fabricProtocol = props.getProperty(FabricMySQLDriver.FABRIC_PROTOCOL_PROPERTY_KEY);
        this.fabricUsername = props.getProperty(FabricMySQLDriver.FABRIC_USERNAME_PROPERTY_KEY);
        this.fabricPassword = props.getProperty(FabricMySQLDriver.FABRIC_PASSWORD_PROPERTY_KEY);
        this.reportErrors = Boolean.valueOf(props.getProperty(FabricMySQLDriver.FABRIC_REPORT_ERRORS_PROPERTY_KEY));
        props.remove(FabricMySQLDriver.FABRIC_SHARD_KEY_PROPERTY_KEY);
        props.remove(FabricMySQLDriver.FABRIC_SHARD_TABLE_PROPERTY_KEY);
        props.remove(FabricMySQLDriver.FABRIC_SERVER_GROUP_PROPERTY_KEY);
        props.remove(FabricMySQLDriver.FABRIC_PROTOCOL_PROPERTY_KEY);
        props.remove(FabricMySQLDriver.FABRIC_USERNAME_PROPERTY_KEY);
        props.remove(FabricMySQLDriver.FABRIC_PASSWORD_PROPERTY_KEY);
        props.remove(FabricMySQLDriver.FABRIC_REPORT_ERRORS_PROPERTY_KEY);

        this.host = props.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
        this.port = props.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY);
        this.username = props.getProperty(NonRegisteringDriver.USER_PROPERTY_KEY);
        this.password = props.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY);
        this.database = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
        if (this.username == null) {
            this.username = "";
        }
        if (this.password == null) {
            this.password = "";
        }

        // add our interceptor to pass exceptions back to the `interceptException' method
        String exceptionInterceptors = props.getProperty("exceptionInterceptors");
        if (exceptionInterceptors == null || "null".equals("exceptionInterceptors")) {
            exceptionInterceptors = "";
        } else {
            exceptionInterceptors += ",";
        }
        exceptionInterceptors += "com.mysql.fabric.jdbc.ErrorReportingExceptionInterceptor";
        props.setProperty("exceptionInterceptors", exceptionInterceptors);

        initializeProperties(props);

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
        this.log = LogFactory.getLogger(getLogger(), "FabricMySQLConnectionProxy", null);

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
    synchronized SQLException interceptException(SQLException sqlEx, Connection conn, String groupName, String hostname, String portNumber)
            throws FabricCommunicationException {
        // we are only concerned with connection failures to MySQL servers, skip anything else including connection failures to Fabric
        if ((sqlEx.getSQLState() == null || !sqlEx.getSQLState().startsWith("08"))
                && !MySQLNonTransientConnectionException.class.isAssignableFrom(sqlEx.getClass())
                && (JDBC4_NON_TRANSIENT_CONN_EXCEPTION == null || !JDBC4_NON_TRANSIENT_CONN_EXCEPTION.isAssignableFrom(sqlEx.getClass()))
                || sqlEx.getCause() != null && FabricCommunicationException.class.isAssignableFrom(sqlEx.getCause().getClass())) {
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
                    this.fabricConnection.refreshStatePassive();
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
            this.fabricConnection.refreshStatePassive();
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
                db = pair[0];
                table = pair[1];
            }
            this.shardMapping = this.fabricConnection.getShardMapping(db, table);
            if (this.shardMapping == null) {
                throw SQLError.createSQLException("Shard mapping not found for table `" + shardTable + "'", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null,
                        getExceptionInterceptor(), this);
            }
            // default to global group
            setCurrentServerGroup(this.shardMapping.getGlobalGroupName());
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

        // choose shard mapping if necessary
        if (this.shardMapping == null) {
            if (this.fabricConnection.getShardMapping(this.database, tableName) != null) {
                setShardTable(tableName);
            }
        } else { // make sure we aren't in conflict with the chosen shard mapping
            ShardMapping mappingForTableName = this.fabricConnection.getShardMapping(this.database, tableName);
            if (mappingForTableName != null && !mappingForTableName.equals(this.shardMapping)) {
                throw SQLError.createSQLException("Cross-shard query not allowed", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null, getExceptionInterceptor(), this);
            }
        }
        this.queryTables.add(tableName);
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
        this.serverGroup = this.fabricConnection.getServerGroup(serverGroupName);

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
     * internal MySQLConnection interface. This should not be used
     * unless a MySQLConnection is required.
     * 
     * {@link getActiveConnection()} is provided for the general case.
     * The returned object is not a {@link ReplicationConnection}, but
     * instead the {@link LoadBalancedConnectionProxy} for either the
     * master or slaves.
     */
    protected MySQLConnection getActiveMySQLConnection() throws SQLException {
        ReplicationConnection c = (ReplicationConnection) getActiveConnection();
        MySQLConnection mc = (MySQLConnection) c.getCurrentConnection();
        return mc;
    }

    protected MySQLConnection getActiveMySQLConnectionPassive() {
        try {
            return getActiveMySQLConnection();
        } catch (SQLException ex) {
            throw new IllegalStateException("Unable to determine active connection", ex);
        }
    }

    protected Connection getActiveConnectionPassive() {
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

    protected Connection getActiveConnection() throws SQLException {
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
        List<String> masterHost = new ArrayList<String>();
        List<String> slaveHosts = new ArrayList<String>();
        for (Server s : this.serverGroup.getServers()) {
            if (s.isMaster()) {
                masterHost.add(s.getHostPortString());
            } else if (s.isSlave()) {
                slaveHosts.add(s.getHostPortString());
            }
        }
        Properties info = exposeAsProperties(null);
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
        info.put("replicationConnectionGroup", this.serverGroup.getName());
        info.setProperty(NonRegisteringDriver.USER_PROPERTY_KEY, this.username);
        info.setProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY, this.password);
        info.setProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY, getCatalog());
        info.setProperty("connectionAttributes", "fabricHaGroup:" + this.serverGroup.getName());
        info.setProperty("retriesAllDown", "1");
        info.setProperty("allowMasterDownConnections", "true");
        info.setProperty("allowSlaveDownConnections", "true");
        info.setProperty("readFromMasterWhenNoSlaves", "true");
        this.currentConnection = ReplicationConnectionProxy.createProxyInstance(masterHost, info, slaveHosts, info);
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
        for (Connection c : this.serverConnections.values()) {
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
        for (Connection c : this.serverConnections.values()) {
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
        for (Connection c : this.serverConnections.values()) {
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

    /**
     * @deprecated replaced by <code>getMultiHostSafeProxy()</code>
     */
    @Deprecated
    public MySQLConnection getLoadBalanceSafeProxy() {
        return getMultiHostSafeProxy();
    }

    public MySQLConnection getMultiHostSafeProxy() {
        return getActiveMySQLConnectionPassive();
    }

    ////////////////////////////////////////////////////////
    // Methods applying changes to all active connections //
    ////////////////////////////////////////////////////////
    public void setTransactionIsolation(int level) throws SQLException {
        this.transactionIsolation = level;
        for (Connection c : this.serverConnections.values()) {
            c.setTransactionIsolation(level);
        }
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        for (Connection c : this.serverConnections.values()) {
            c.setTypeMap(map);
        }
    }

    public void setHoldability(int holdability) throws SQLException {
        for (Connection c : this.serverConnections.values()) {
            c.setHoldability(holdability);
        }
    }

    public void setProxy(MySQLConnection proxy) {
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

    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, Buffer packet, int resultSetType, int resultSetConcurrency,
            boolean streamResults, String catalog, Field[] cachedMetadata) throws SQLException {
        return getActiveMySQLConnection().execSQL(callingStatement, sql, maxRows, packet, resultSetType, resultSetConcurrency, streamResults, catalog,
                cachedMetadata);
    }

    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, Buffer packet, int resultSetType, int resultSetConcurrency,
            boolean streamResults, String catalog, Field[] cachedMetadata, boolean isBatch) throws SQLException {
        return getActiveMySQLConnection().execSQL(callingStatement, sql, maxRows, packet, resultSetType, resultSetConcurrency, streamResults, catalog,
                cachedMetadata, isBatch);
    }

    public String extractSqlFromPacket(String possibleSqlQuery, Buffer queryPacket, int endOfQueryPacketPosition) throws SQLException {
        return getActiveMySQLConnection().extractSqlFromPacket(possibleSqlQuery, queryPacket, endOfQueryPacketPosition);
    }

    public StringBuilder generateConnectionCommentBlock(StringBuilder buf) {
        return getActiveMySQLConnectionPassive().generateConnectionCommentBlock(buf);
    }

    public MysqlIO getIO() throws SQLException {
        return getActiveMySQLConnection().getIO();
    }

    public Calendar getCalendarInstanceForSessionOrNew() {
        return getActiveMySQLConnectionPassive().getCalendarInstanceForSessionOrNew();
    }

    /**
     * @deprecated replaced by <code>getServerCharset()</code>
     */
    @Deprecated
    public String getServerCharacterEncoding() {
        return getServerCharset();
    }

    public String getServerCharset() {
        return getActiveMySQLConnectionPassive().getServerCharset();
    }

    public TimeZone getServerTimezoneTZ() {
        return getActiveMySQLConnectionPassive().getServerTimezoneTZ();
    }

    /**
     * Only valid until the end of the transaction. These could optionally be implemented
     * to only return true if all current connections return true.
     */
    public boolean versionMeetsMinimum(int major, int minor, int subminor) throws SQLException {
        return getActiveConnection().versionMeetsMinimum(major, minor, subminor);
    }

    /**
     * Only valid until the end of the transaction.
     */
    public boolean supportsIsolationLevel() {
        return getActiveConnectionPassive().supportsIsolationLevel();
    }

    /**
     * Only valid until the end of the transaction.
     */
    public boolean supportsQuotedIdentifiers() {
        return getActiveConnectionPassive().supportsQuotedIdentifiers();
    }

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

    public boolean supportsTransactions() {
        // Fabric requires MySQL 5.6 w/GTID
        return true;
    }

    public boolean isRunningOnJDK13() {
        return false;
    }

    public void createNewIO(boolean isForReconnect) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

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
    @Override
    public void setAllowLoadLocalInfile(boolean property) {
        super.setAllowLoadLocalInfile(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setAllowLoadLocalInfile(property);
        }
    }

    @Override
    public void setAllowMultiQueries(boolean property) {
        super.setAllowMultiQueries(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setAllowMultiQueries(property);
        }
    }

    @Override
    public void setAllowNanAndInf(boolean flag) {
        super.setAllowNanAndInf(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setAllowNanAndInf(flag);
        }
    }

    @Override
    public void setAllowUrlInLocalInfile(boolean flag) {
        super.setAllowUrlInLocalInfile(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setAllowUrlInLocalInfile(flag);
        }
    }

    @Override
    public void setAlwaysSendSetIsolation(boolean flag) {
        super.setAlwaysSendSetIsolation(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setAlwaysSendSetIsolation(flag);
        }
    }

    @Override
    public void setAutoDeserialize(boolean flag) {
        super.setAutoDeserialize(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setAutoDeserialize(flag);
        }
    }

    @Override
    public void setAutoGenerateTestcaseScript(boolean flag) {
        super.setAutoGenerateTestcaseScript(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setAutoGenerateTestcaseScript(flag);
        }
    }

    @Override
    public void setAutoReconnect(boolean flag) {
        super.setAutoReconnect(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setAutoReconnect(flag);
        }
    }

    @Override
    public void setAutoReconnectForConnectionPools(boolean property) {
        super.setAutoReconnectForConnectionPools(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setAutoReconnectForConnectionPools(property);
        }
    }

    @Override
    public void setAutoReconnectForPools(boolean flag) {
        super.setAutoReconnectForPools(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setAutoReconnectForPools(flag);
        }
    }

    @Override
    public void setBlobSendChunkSize(String value) throws SQLException {
        super.setBlobSendChunkSize(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setBlobSendChunkSize(value);
        }
    }

    @Override
    public void setCacheCallableStatements(boolean flag) {
        super.setCacheCallableStatements(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCacheCallableStatements(flag);
        }
    }

    @Override
    public void setCachePreparedStatements(boolean flag) {
        super.setCachePreparedStatements(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCachePreparedStatements(flag);
        }
    }

    @Override
    public void setCacheResultSetMetadata(boolean property) {
        super.setCacheResultSetMetadata(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCacheResultSetMetadata(property);
        }
    }

    @Override
    public void setCacheServerConfiguration(boolean flag) {
        super.setCacheServerConfiguration(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCacheServerConfiguration(flag);
        }
    }

    @Override
    public void setCallableStatementCacheSize(int size) throws SQLException {
        super.setCallableStatementCacheSize(size);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCallableStatementCacheSize(size);
        }
    }

    @Override
    public void setCapitalizeDBMDTypes(boolean property) {
        super.setCapitalizeDBMDTypes(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCapitalizeDBMDTypes(property);
        }
    }

    @Override
    public void setCapitalizeTypeNames(boolean flag) {
        super.setCapitalizeTypeNames(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCapitalizeTypeNames(flag);
        }
    }

    @Override
    public void setCharacterEncoding(String encoding) {
        super.setCharacterEncoding(encoding);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCharacterEncoding(encoding);
        }
    }

    @Override
    public void setCharacterSetResults(String characterSet) {
        super.setCharacterSetResults(characterSet);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCharacterSetResults(characterSet);
        }
    }

    @Override
    public void setClobberStreamingResults(boolean flag) {
        super.setClobberStreamingResults(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setClobberStreamingResults(flag);
        }
    }

    @Override
    public void setClobCharacterEncoding(String encoding) {
        super.setClobCharacterEncoding(encoding);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setClobCharacterEncoding(encoding);
        }
    }

    @Override
    public void setConnectionCollation(String collation) {
        super.setConnectionCollation(collation);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setConnectionCollation(collation);
        }
    }

    @Override
    public void setConnectTimeout(int timeoutMs) throws SQLException {
        super.setConnectTimeout(timeoutMs);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setConnectTimeout(timeoutMs);
        }
    }

    @Override
    public void setContinueBatchOnError(boolean property) {
        super.setContinueBatchOnError(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setContinueBatchOnError(property);
        }
    }

    @Override
    public void setCreateDatabaseIfNotExist(boolean flag) {
        super.setCreateDatabaseIfNotExist(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCreateDatabaseIfNotExist(flag);
        }
    }

    @Override
    public void setDefaultFetchSize(int n) throws SQLException {
        super.setDefaultFetchSize(n);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setDefaultFetchSize(n);
        }
    }

    @Override
    public void setDetectServerPreparedStmts(boolean property) {
        super.setDetectServerPreparedStmts(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setDetectServerPreparedStmts(property);
        }
    }

    @Override
    public void setDontTrackOpenResources(boolean flag) {
        super.setDontTrackOpenResources(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setDontTrackOpenResources(flag);
        }
    }

    @Override
    public void setDumpQueriesOnException(boolean flag) {
        super.setDumpQueriesOnException(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setDumpQueriesOnException(flag);
        }
    }

    @Override
    public void setDynamicCalendars(boolean flag) {
        super.setDynamicCalendars(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setDynamicCalendars(flag);
        }
    }

    @Override
    public void setElideSetAutoCommits(boolean flag) {
        super.setElideSetAutoCommits(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setElideSetAutoCommits(flag);
        }
    }

    @Override
    public void setEmptyStringsConvertToZero(boolean flag) {
        super.setEmptyStringsConvertToZero(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setEmptyStringsConvertToZero(flag);
        }
    }

    @Override
    public void setEmulateLocators(boolean property) {
        super.setEmulateLocators(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setEmulateLocators(property);
        }
    }

    @Override
    public void setEmulateUnsupportedPstmts(boolean flag) {
        super.setEmulateUnsupportedPstmts(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setEmulateUnsupportedPstmts(flag);
        }
    }

    @Override
    public void setEnablePacketDebug(boolean flag) {
        super.setEnablePacketDebug(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setEnablePacketDebug(flag);
        }
    }

    @Override
    public void setEncoding(String property) {
        super.setEncoding(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setEncoding(property);
        }
    }

    @Override
    public void setExplainSlowQueries(boolean flag) {
        super.setExplainSlowQueries(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setExplainSlowQueries(flag);
        }
    }

    @Override
    public void setFailOverReadOnly(boolean flag) {
        super.setFailOverReadOnly(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setFailOverReadOnly(flag);
        }
    }

    @Override
    public void setGatherPerformanceMetrics(boolean flag) {
        super.setGatherPerformanceMetrics(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setGatherPerformanceMetrics(flag);
        }
    }

    @Override
    public void setHoldResultsOpenOverStatementClose(boolean flag) {
        super.setHoldResultsOpenOverStatementClose(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setHoldResultsOpenOverStatementClose(flag);
        }
    }

    @Override
    public void setIgnoreNonTxTables(boolean property) {
        super.setIgnoreNonTxTables(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setIgnoreNonTxTables(property);
        }
    }

    @Override
    public void setInitialTimeout(int property) throws SQLException {
        super.setInitialTimeout(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setInitialTimeout(property);
        }
    }

    @Override
    public void setIsInteractiveClient(boolean property) {
        super.setIsInteractiveClient(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setIsInteractiveClient(property);
        }
    }

    @Override
    public void setJdbcCompliantTruncation(boolean flag) {
        super.setJdbcCompliantTruncation(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setJdbcCompliantTruncation(flag);
        }
    }

    @Override
    public void setLocatorFetchBufferSize(String value) throws SQLException {
        super.setLocatorFetchBufferSize(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLocatorFetchBufferSize(value);
        }
    }

    @Override
    public void setLogger(String property) {
        super.setLogger(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLogger(property);
        }
    }

    @Override
    public void setLoggerClassName(String className) {
        super.setLoggerClassName(className);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLoggerClassName(className);
        }
    }

    @Override
    public void setLogSlowQueries(boolean flag) {
        super.setLogSlowQueries(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLogSlowQueries(flag);
        }
    }

    @Override
    public void setMaintainTimeStats(boolean flag) {
        super.setMaintainTimeStats(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setMaintainTimeStats(flag);
        }
    }

    @Override
    public void setMaxQuerySizeToLog(int sizeInBytes) throws SQLException {
        super.setMaxQuerySizeToLog(sizeInBytes);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setMaxQuerySizeToLog(sizeInBytes);
        }
    }

    @Override
    public void setMaxReconnects(int property) throws SQLException {
        super.setMaxReconnects(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setMaxReconnects(property);
        }
    }

    @Override
    public void setMaxRows(int property) throws SQLException {
        super.setMaxRows(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setMaxRows(property);
        }
    }

    @Override
    public void setMetadataCacheSize(int value) throws SQLException {
        super.setMetadataCacheSize(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setMetadataCacheSize(value);
        }
    }

    @Override
    public void setNoDatetimeStringSync(boolean flag) {
        super.setNoDatetimeStringSync(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setNoDatetimeStringSync(flag);
        }
    }

    @Override
    public void setNullCatalogMeansCurrent(boolean value) {
        super.setNullCatalogMeansCurrent(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setNullCatalogMeansCurrent(value);
        }
    }

    @Override
    public void setNullNamePatternMatchesAll(boolean value) {
        super.setNullNamePatternMatchesAll(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setNullNamePatternMatchesAll(value);
        }
    }

    @Override
    public void setPacketDebugBufferSize(int size) throws SQLException {
        super.setPacketDebugBufferSize(size);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setPacketDebugBufferSize(size);
        }
    }

    @Override
    public void setParanoid(boolean property) {
        super.setParanoid(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setParanoid(property);
        }
    }

    @Override
    public void setPedantic(boolean property) {
        super.setPedantic(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setPedantic(property);
        }
    }

    @Override
    public void setPreparedStatementCacheSize(int cacheSize) throws SQLException {
        super.setPreparedStatementCacheSize(cacheSize);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setPreparedStatementCacheSize(cacheSize);
        }
    }

    @Override
    public void setPreparedStatementCacheSqlLimit(int cacheSqlLimit) throws SQLException {
        super.setPreparedStatementCacheSqlLimit(cacheSqlLimit);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setPreparedStatementCacheSqlLimit(cacheSqlLimit);
        }
    }

    @Override
    public void setProfileSql(boolean property) {
        super.setProfileSql(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setProfileSql(property);
        }
    }

    @Override
    public void setProfileSQL(boolean flag) {
        super.setProfileSQL(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setProfileSQL(flag);
        }
    }

    @Override
    public void setPropertiesTransform(String value) {
        super.setPropertiesTransform(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setPropertiesTransform(value);
        }
    }

    @Override
    public void setQueriesBeforeRetryMaster(int property) throws SQLException {
        super.setQueriesBeforeRetryMaster(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setQueriesBeforeRetryMaster(property);
        }
    }

    @Override
    public void setReconnectAtTxEnd(boolean property) {
        super.setReconnectAtTxEnd(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setReconnectAtTxEnd(property);
        }
    }

    @Override
    public void setRelaxAutoCommit(boolean property) {
        super.setRelaxAutoCommit(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setRelaxAutoCommit(property);
        }
    }

    @Override
    public void setReportMetricsIntervalMillis(int millis) throws SQLException {
        super.setReportMetricsIntervalMillis(millis);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setReportMetricsIntervalMillis(millis);
        }
    }

    @Override
    public void setRequireSSL(boolean property) {
        super.setRequireSSL(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setRequireSSL(property);
        }
    }

    @Override
    public void setRetainStatementAfterResultSetClose(boolean flag) {
        super.setRetainStatementAfterResultSetClose(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setRetainStatementAfterResultSetClose(flag);
        }
    }

    @Override
    public void setRollbackOnPooledClose(boolean flag) {
        super.setRollbackOnPooledClose(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setRollbackOnPooledClose(flag);
        }
    }

    @Override
    public void setRoundRobinLoadBalance(boolean flag) {
        super.setRoundRobinLoadBalance(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setRoundRobinLoadBalance(flag);
        }
    }

    @Override
    public void setRunningCTS13(boolean flag) {
        super.setRunningCTS13(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setRunningCTS13(flag);
        }
    }

    @Override
    public void setSecondsBeforeRetryMaster(int property) throws SQLException {
        super.setSecondsBeforeRetryMaster(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setSecondsBeforeRetryMaster(property);
        }
    }

    @Override
    public void setServerTimezone(String property) {
        super.setServerTimezone(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setServerTimezone(property);
        }
    }

    @Override
    public void setSessionVariables(String variables) {
        super.setSessionVariables(variables);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setSessionVariables(variables);
        }
    }

    @Override
    public void setSlowQueryThresholdMillis(int millis) throws SQLException {
        super.setSlowQueryThresholdMillis(millis);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setSlowQueryThresholdMillis(millis);
        }
    }

    @Override
    public void setSocketFactoryClassName(String property) {
        super.setSocketFactoryClassName(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setSocketFactoryClassName(property);
        }
    }

    @Override
    public void setSocketTimeout(int property) throws SQLException {
        super.setSocketTimeout(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setSocketTimeout(property);
        }
    }

    @Override
    public void setStrictFloatingPoint(boolean property) {
        super.setStrictFloatingPoint(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setStrictFloatingPoint(property);
        }
    }

    @Override
    public void setStrictUpdates(boolean property) {
        super.setStrictUpdates(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setStrictUpdates(property);
        }
    }

    @Override
    public void setTinyInt1isBit(boolean flag) {
        super.setTinyInt1isBit(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setTinyInt1isBit(flag);
        }
    }

    @Override
    public void setTraceProtocol(boolean flag) {
        super.setTraceProtocol(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setTraceProtocol(flag);
        }
    }

    @Override
    public void setTransformedBitIsBoolean(boolean flag) {
        super.setTransformedBitIsBoolean(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setTransformedBitIsBoolean(flag);
        }
    }

    @Override
    public void setUseCompression(boolean property) {
        super.setUseCompression(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseCompression(property);
        }
    }

    @Override
    public void setUseFastIntParsing(boolean flag) {
        super.setUseFastIntParsing(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseFastIntParsing(flag);
        }
    }

    @Override
    public void setUseHostsInPrivileges(boolean property) {
        super.setUseHostsInPrivileges(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseHostsInPrivileges(property);
        }
    }

    @Override
    public void setUseInformationSchema(boolean flag) {
        super.setUseInformationSchema(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseInformationSchema(flag);
        }
    }

    @Override
    public void setUseLocalSessionState(boolean flag) {
        super.setUseLocalSessionState(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseLocalSessionState(flag);
        }
    }

    @Override
    public void setUseOldUTF8Behavior(boolean flag) {
        super.setUseOldUTF8Behavior(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseOldUTF8Behavior(flag);
        }
    }

    @Override
    public void setUseOnlyServerErrorMessages(boolean flag) {
        super.setUseOnlyServerErrorMessages(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseOnlyServerErrorMessages(flag);
        }
    }

    @Override
    public void setUseReadAheadInput(boolean flag) {
        super.setUseReadAheadInput(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseReadAheadInput(flag);
        }
    }

    @Override
    public void setUseServerPreparedStmts(boolean flag) {
        super.setUseServerPreparedStmts(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseServerPreparedStmts(flag);
        }
    }

    @Override
    public void setUseSqlStateCodes(boolean flag) {
        super.setUseSqlStateCodes(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseSqlStateCodes(flag);
        }
    }

    @Override
    public void setUseSSL(boolean property) {
        super.setUseSSL(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseSSL(property);
        }
    }

    @Override
    public void setUseStreamLengthsInPrepStmts(boolean property) {
        super.setUseStreamLengthsInPrepStmts(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseStreamLengthsInPrepStmts(property);
        }
    }

    @Override
    public void setUseTimezone(boolean property) {
        super.setUseTimezone(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseTimezone(property);
        }
    }

    @Override
    public void setUseUltraDevWorkAround(boolean property) {
        super.setUseUltraDevWorkAround(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseUltraDevWorkAround(property);
        }
    }

    @Override
    public void setUseUnbufferedInput(boolean flag) {
        super.setUseUnbufferedInput(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseUnbufferedInput(flag);
        }
    }

    @Override
    public void setUseUnicode(boolean flag) {
        super.setUseUnicode(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseUnicode(flag);
        }
    }

    @Override
    public void setUseUsageAdvisor(boolean useUsageAdvisorFlag) {
        super.setUseUsageAdvisor(useUsageAdvisorFlag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseUsageAdvisor(useUsageAdvisorFlag);
        }
    }

    @Override
    public void setYearIsDateType(boolean flag) {
        super.setYearIsDateType(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setYearIsDateType(flag);
        }
    }

    @Override
    public void setZeroDateTimeBehavior(String behavior) {
        super.setZeroDateTimeBehavior(behavior);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setZeroDateTimeBehavior(behavior);
        }
    }

    @Override
    public void setUseCursorFetch(boolean flag) {
        super.setUseCursorFetch(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseCursorFetch(flag);
        }
    }

    @Override
    public void setOverrideSupportsIntegrityEnhancementFacility(boolean flag) {
        super.setOverrideSupportsIntegrityEnhancementFacility(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setOverrideSupportsIntegrityEnhancementFacility(flag);
        }
    }

    @Override
    public void setNoTimezoneConversionForTimeType(boolean flag) {
        super.setNoTimezoneConversionForTimeType(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setNoTimezoneConversionForTimeType(flag);
        }
    }

    @Override
    public void setUseJDBCCompliantTimezoneShift(boolean flag) {
        super.setUseJDBCCompliantTimezoneShift(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseJDBCCompliantTimezoneShift(flag);
        }
    }

    @Override
    public void setAutoClosePStmtStreams(boolean flag) {
        super.setAutoClosePStmtStreams(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setAutoClosePStmtStreams(flag);
        }
    }

    @Override
    public void setProcessEscapeCodesForPrepStmts(boolean flag) {
        super.setProcessEscapeCodesForPrepStmts(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setProcessEscapeCodesForPrepStmts(flag);
        }
    }

    @Override
    public void setUseGmtMillisForDatetimes(boolean flag) {
        super.setUseGmtMillisForDatetimes(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseGmtMillisForDatetimes(flag);
        }
    }

    @Override
    public void setDumpMetadataOnColumnNotFound(boolean flag) {
        super.setDumpMetadataOnColumnNotFound(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setDumpMetadataOnColumnNotFound(flag);
        }
    }

    @Override
    public void setResourceId(String resourceId) {
        super.setResourceId(resourceId);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setResourceId(resourceId);
        }
    }

    @Override
    public void setRewriteBatchedStatements(boolean flag) {
        super.setRewriteBatchedStatements(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setRewriteBatchedStatements(flag);
        }
    }

    @Override
    public void setJdbcCompliantTruncationForReads(boolean jdbcCompliantTruncationForReads) {
        super.setJdbcCompliantTruncationForReads(jdbcCompliantTruncationForReads);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setJdbcCompliantTruncationForReads(jdbcCompliantTruncationForReads);
        }
    }

    @Override
    public void setUseJvmCharsetConverters(boolean flag) {
        super.setUseJvmCharsetConverters(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseJvmCharsetConverters(flag);
        }
    }

    @Override
    public void setPinGlobalTxToPhysicalConnection(boolean flag) {
        super.setPinGlobalTxToPhysicalConnection(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setPinGlobalTxToPhysicalConnection(flag);
        }
    }

    @Override
    public void setGatherPerfMetrics(boolean flag) {
        super.setGatherPerfMetrics(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setGatherPerfMetrics(flag);
        }
    }

    @Override
    public void setUltraDevHack(boolean flag) {
        super.setUltraDevHack(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUltraDevHack(flag);
        }
    }

    @Override
    public void setInteractiveClient(boolean property) {
        super.setInteractiveClient(property);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setInteractiveClient(property);
        }
    }

    @Override
    public void setSocketFactory(String name) {
        super.setSocketFactory(name);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setSocketFactory(name);
        }
    }

    @Override
    public void setUseServerPrepStmts(boolean flag) {
        super.setUseServerPrepStmts(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseServerPrepStmts(flag);
        }
    }

    @Override
    public void setCacheCallableStmts(boolean flag) {
        super.setCacheCallableStmts(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCacheCallableStmts(flag);
        }
    }

    @Override
    public void setCachePrepStmts(boolean flag) {
        super.setCachePrepStmts(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCachePrepStmts(flag);
        }
    }

    @Override
    public void setCallableStmtCacheSize(int cacheSize) throws SQLException {
        super.setCallableStmtCacheSize(cacheSize);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCallableStmtCacheSize(cacheSize);
        }
    }

    @Override
    public void setPrepStmtCacheSize(int cacheSize) throws SQLException {
        super.setPrepStmtCacheSize(cacheSize);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setPrepStmtCacheSize(cacheSize);
        }
    }

    @Override
    public void setPrepStmtCacheSqlLimit(int sqlLimit) throws SQLException {
        super.setPrepStmtCacheSqlLimit(sqlLimit);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setPrepStmtCacheSqlLimit(sqlLimit);
        }
    }

    @Override
    public void setNoAccessToProcedureBodies(boolean flag) {
        super.setNoAccessToProcedureBodies(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setNoAccessToProcedureBodies(flag);
        }
    }

    @Override
    public void setUseOldAliasMetadataBehavior(boolean flag) {
        super.setUseOldAliasMetadataBehavior(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseOldAliasMetadataBehavior(flag);
        }
    }

    @Override
    public void setClientCertificateKeyStorePassword(String value) {
        super.setClientCertificateKeyStorePassword(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setClientCertificateKeyStorePassword(value);
        }
    }

    @Override
    public void setClientCertificateKeyStoreType(String value) {
        super.setClientCertificateKeyStoreType(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setClientCertificateKeyStoreType(value);
        }
    }

    @Override
    public void setClientCertificateKeyStoreUrl(String value) {
        super.setClientCertificateKeyStoreUrl(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setClientCertificateKeyStoreUrl(value);
        }
    }

    @Override
    public void setTrustCertificateKeyStorePassword(String value) {
        super.setTrustCertificateKeyStorePassword(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setTrustCertificateKeyStorePassword(value);
        }
    }

    @Override
    public void setTrustCertificateKeyStoreType(String value) {
        super.setTrustCertificateKeyStoreType(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setTrustCertificateKeyStoreType(value);
        }
    }

    @Override
    public void setTrustCertificateKeyStoreUrl(String value) {
        super.setTrustCertificateKeyStoreUrl(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setTrustCertificateKeyStoreUrl(value);
        }
    }

    @Override
    public void setUseSSPSCompatibleTimezoneShift(boolean flag) {
        super.setUseSSPSCompatibleTimezoneShift(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseSSPSCompatibleTimezoneShift(flag);
        }
    }

    @Override
    public void setTreatUtilDateAsTimestamp(boolean flag) {
        super.setTreatUtilDateAsTimestamp(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setTreatUtilDateAsTimestamp(flag);
        }
    }

    @Override
    public void setUseFastDateParsing(boolean flag) {
        super.setUseFastDateParsing(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseFastDateParsing(flag);
        }
    }

    @Override
    public void setLocalSocketAddress(String address) {
        super.setLocalSocketAddress(address);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLocalSocketAddress(address);
        }
    }

    @Override
    public void setUseConfigs(String configs) {
        super.setUseConfigs(configs);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseConfigs(configs);
        }
    }

    @Override
    public void setGenerateSimpleParameterMetadata(boolean flag) {
        super.setGenerateSimpleParameterMetadata(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setGenerateSimpleParameterMetadata(flag);
        }
    }

    @Override
    public void setLogXaCommands(boolean flag) {
        super.setLogXaCommands(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLogXaCommands(flag);
        }
    }

    @Override
    public void setResultSetSizeThreshold(int threshold) throws SQLException {
        super.setResultSetSizeThreshold(threshold);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setResultSetSizeThreshold(threshold);
        }
    }

    @Override
    public void setNetTimeoutForStreamingResults(int value) throws SQLException {
        super.setNetTimeoutForStreamingResults(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setNetTimeoutForStreamingResults(value);
        }
    }

    @Override
    public void setEnableQueryTimeouts(boolean flag) {
        super.setEnableQueryTimeouts(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setEnableQueryTimeouts(flag);
        }
    }

    @Override
    public void setPadCharsWithSpace(boolean flag) {
        super.setPadCharsWithSpace(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setPadCharsWithSpace(flag);
        }
    }

    @Override
    public void setUseDynamicCharsetInfo(boolean flag) {
        super.setUseDynamicCharsetInfo(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseDynamicCharsetInfo(flag);
        }
    }

    @Override
    public void setClientInfoProvider(String classname) {
        super.setClientInfoProvider(classname);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setClientInfoProvider(classname);
        }
    }

    @Override
    public void setPopulateInsertRowWithDefaultValues(boolean flag) {
        super.setPopulateInsertRowWithDefaultValues(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setPopulateInsertRowWithDefaultValues(flag);
        }
    }

    @Override
    public void setLoadBalanceStrategy(String strategy) {
        super.setLoadBalanceStrategy(strategy);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLoadBalanceStrategy(strategy);
        }
    }

    @Override
    public void setTcpNoDelay(boolean flag) {
        super.setTcpNoDelay(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setTcpNoDelay(flag);
        }
    }

    @Override
    public void setTcpKeepAlive(boolean flag) {
        super.setTcpKeepAlive(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setTcpKeepAlive(flag);
        }
    }

    @Override
    public void setTcpRcvBuf(int bufSize) throws SQLException {
        super.setTcpRcvBuf(bufSize);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setTcpRcvBuf(bufSize);
        }
    }

    @Override
    public void setTcpSndBuf(int bufSize) throws SQLException {
        super.setTcpSndBuf(bufSize);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setTcpSndBuf(bufSize);
        }
    }

    @Override
    public void setTcpTrafficClass(int classFlags) throws SQLException {
        super.setTcpTrafficClass(classFlags);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setTcpTrafficClass(classFlags);
        }
    }

    @Override
    public void setUseNanosForElapsedTime(boolean flag) {
        super.setUseNanosForElapsedTime(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseNanosForElapsedTime(flag);
        }
    }

    @Override
    public void setSlowQueryThresholdNanos(long nanos) throws SQLException {
        super.setSlowQueryThresholdNanos(nanos);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setSlowQueryThresholdNanos(nanos);
        }
    }

    @Override
    public void setStatementInterceptors(String value) {
        super.setStatementInterceptors(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setStatementInterceptors(value);
        }
    }

    @Override
    public void setUseDirectRowUnpack(boolean flag) {
        super.setUseDirectRowUnpack(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseDirectRowUnpack(flag);
        }
    }

    @Override
    public void setLargeRowSizeThreshold(String value) throws SQLException {
        super.setLargeRowSizeThreshold(value);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLargeRowSizeThreshold(value);
        }
    }

    @Override
    public void setUseBlobToStoreUTF8OutsideBMP(boolean flag) {
        super.setUseBlobToStoreUTF8OutsideBMP(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseBlobToStoreUTF8OutsideBMP(flag);
        }
    }

    @Override
    public void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern) {
        super.setUtf8OutsideBmpExcludedColumnNamePattern(regexPattern);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUtf8OutsideBmpExcludedColumnNamePattern(regexPattern);
        }
    }

    @Override
    public void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern) {
        super.setUtf8OutsideBmpIncludedColumnNamePattern(regexPattern);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUtf8OutsideBmpIncludedColumnNamePattern(regexPattern);
        }
    }

    @Override
    public void setIncludeInnodbStatusInDeadlockExceptions(boolean flag) {
        super.setIncludeInnodbStatusInDeadlockExceptions(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setIncludeInnodbStatusInDeadlockExceptions(flag);
        }
    }

    @Override
    public void setIncludeThreadDumpInDeadlockExceptions(boolean flag) {
        super.setIncludeThreadDumpInDeadlockExceptions(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setIncludeThreadDumpInDeadlockExceptions(flag);
        }
    }

    @Override
    public void setIncludeThreadNamesAsStatementComment(boolean flag) {
        super.setIncludeThreadNamesAsStatementComment(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setIncludeThreadNamesAsStatementComment(flag);
        }
    }

    @Override
    public void setBlobsAreStrings(boolean flag) {
        super.setBlobsAreStrings(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setBlobsAreStrings(flag);
        }
    }

    @Override
    public void setFunctionsNeverReturnBlobs(boolean flag) {
        super.setFunctionsNeverReturnBlobs(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setFunctionsNeverReturnBlobs(flag);
        }
    }

    @Override
    public void setAutoSlowLog(boolean flag) {
        super.setAutoSlowLog(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setAutoSlowLog(flag);
        }
    }

    @Override
    public void setConnectionLifecycleInterceptors(String interceptors) {
        super.setConnectionLifecycleInterceptors(interceptors);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setConnectionLifecycleInterceptors(interceptors);
        }
    }

    @Override
    public void setProfilerEventHandler(String handler) {
        super.setProfilerEventHandler(handler);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setProfilerEventHandler(handler);
        }
    }

    @Override
    public void setVerifyServerCertificate(boolean flag) {
        super.setVerifyServerCertificate(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setVerifyServerCertificate(flag);
        }
    }

    @Override
    public void setUseLegacyDatetimeCode(boolean flag) {
        super.setUseLegacyDatetimeCode(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseLegacyDatetimeCode(flag);
        }
    }

    @Override
    public void setSelfDestructOnPingSecondsLifetime(int seconds) throws SQLException {
        super.setSelfDestructOnPingSecondsLifetime(seconds);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setSelfDestructOnPingSecondsLifetime(seconds);
        }
    }

    @Override
    public void setSelfDestructOnPingMaxOperations(int maxOperations) throws SQLException {
        super.setSelfDestructOnPingMaxOperations(maxOperations);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setSelfDestructOnPingMaxOperations(maxOperations);
        }
    }

    @Override
    public void setUseColumnNamesInFindColumn(boolean flag) {
        super.setUseColumnNamesInFindColumn(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseColumnNamesInFindColumn(flag);
        }
    }

    @Override
    public void setUseLocalTransactionState(boolean flag) {
        super.setUseLocalTransactionState(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseLocalTransactionState(flag);
        }
    }

    @Override
    public void setCompensateOnDuplicateKeyUpdateCounts(boolean flag) {
        super.setCompensateOnDuplicateKeyUpdateCounts(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setCompensateOnDuplicateKeyUpdateCounts(flag);
        }
    }

    @Override
    public void setUseAffectedRows(boolean flag) {
        super.setUseAffectedRows(flag);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setUseAffectedRows(flag);
        }
    }

    @Override
    public void setPasswordCharacterEncoding(String characterSet) {
        super.setPasswordCharacterEncoding(characterSet);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setPasswordCharacterEncoding(characterSet);
        }
    }

    @Override
    public void setLoadBalanceBlacklistTimeout(int loadBalanceBlacklistTimeout) throws SQLException {
        super.setLoadBalanceBlacklistTimeout(loadBalanceBlacklistTimeout);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLoadBalanceBlacklistTimeout(loadBalanceBlacklistTimeout);
        }
    }

    @Override
    public void setRetriesAllDown(int retriesAllDown) throws SQLException {
        super.setRetriesAllDown(retriesAllDown);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setRetriesAllDown(retriesAllDown);
        }
    }

    @Override
    public void setExceptionInterceptors(String exceptionInterceptors) {
        super.setExceptionInterceptors(exceptionInterceptors);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setExceptionInterceptors(exceptionInterceptors);
        }
    }

    @Override
    public void setQueryTimeoutKillsConnection(boolean queryTimeoutKillsConnection) {
        super.setQueryTimeoutKillsConnection(queryTimeoutKillsConnection);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setQueryTimeoutKillsConnection(queryTimeoutKillsConnection);
        }
    }

    @Override
    public void setLoadBalancePingTimeout(int loadBalancePingTimeout) throws SQLException {
        super.setLoadBalancePingTimeout(loadBalancePingTimeout);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLoadBalancePingTimeout(loadBalancePingTimeout);
        }
    }

    @Override
    public void setLoadBalanceValidateConnectionOnSwapServer(boolean loadBalanceValidateConnectionOnSwapServer) {
        super.setLoadBalanceValidateConnectionOnSwapServer(loadBalanceValidateConnectionOnSwapServer);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLoadBalanceValidateConnectionOnSwapServer(loadBalanceValidateConnectionOnSwapServer);
        }
    }

    @Override
    public void setLoadBalanceConnectionGroup(String loadBalanceConnectionGroup) {
        super.setLoadBalanceConnectionGroup(loadBalanceConnectionGroup);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLoadBalanceConnectionGroup(loadBalanceConnectionGroup);
        }
    }

    @Override
    public void setLoadBalanceExceptionChecker(String loadBalanceExceptionChecker) {
        super.setLoadBalanceExceptionChecker(loadBalanceExceptionChecker);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLoadBalanceExceptionChecker(loadBalanceExceptionChecker);
        }
    }

    @Override
    public void setLoadBalanceSQLStateFailover(String loadBalanceSQLStateFailover) {
        super.setLoadBalanceSQLStateFailover(loadBalanceSQLStateFailover);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLoadBalanceSQLStateFailover(loadBalanceSQLStateFailover);
        }
    }

    @Override
    public void setLoadBalanceSQLExceptionSubclassFailover(String loadBalanceSQLExceptionSubclassFailover) {
        super.setLoadBalanceSQLExceptionSubclassFailover(loadBalanceSQLExceptionSubclassFailover);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLoadBalanceSQLExceptionSubclassFailover(loadBalanceSQLExceptionSubclassFailover);
        }
    }

    @Override
    public void setLoadBalanceEnableJMX(boolean loadBalanceEnableJMX) {
        super.setLoadBalanceEnableJMX(loadBalanceEnableJMX);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLoadBalanceEnableJMX(loadBalanceEnableJMX);
        }
    }

    @Override
    public void setLoadBalanceAutoCommitStatementThreshold(int loadBalanceAutoCommitStatementThreshold) throws SQLException {
        super.setLoadBalanceAutoCommitStatementThreshold(loadBalanceAutoCommitStatementThreshold);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLoadBalanceAutoCommitStatementThreshold(loadBalanceAutoCommitStatementThreshold);
        }
    }

    @Override
    public void setLoadBalanceAutoCommitStatementRegex(String loadBalanceAutoCommitStatementRegex) {
        super.setLoadBalanceAutoCommitStatementRegex(loadBalanceAutoCommitStatementRegex);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setLoadBalanceAutoCommitStatementRegex(loadBalanceAutoCommitStatementRegex);
        }
    }

    @Override
    public void setAuthenticationPlugins(String authenticationPlugins) {
        super.setAuthenticationPlugins(authenticationPlugins);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setAuthenticationPlugins(authenticationPlugins);
        }
    }

    @Override
    public void setDisabledAuthenticationPlugins(String disabledAuthenticationPlugins) {
        super.setDisabledAuthenticationPlugins(disabledAuthenticationPlugins);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setDisabledAuthenticationPlugins(disabledAuthenticationPlugins);
        }
    }

    @Override
    public void setDefaultAuthenticationPlugin(String defaultAuthenticationPlugin) {
        super.setDefaultAuthenticationPlugin(defaultAuthenticationPlugin);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setDefaultAuthenticationPlugin(defaultAuthenticationPlugin);
        }
    }

    @Override
    public void setParseInfoCacheFactory(String factoryClassname) {
        super.setParseInfoCacheFactory(factoryClassname);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setParseInfoCacheFactory(factoryClassname);
        }
    }

    @Override
    public void setServerConfigCacheFactory(String factoryClassname) {
        super.setServerConfigCacheFactory(factoryClassname);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setServerConfigCacheFactory(factoryClassname);
        }
    }

    @Override
    public void setDisconnectOnExpiredPasswords(boolean disconnectOnExpiredPasswords) {
        super.setDisconnectOnExpiredPasswords(disconnectOnExpiredPasswords);
        for (ConnectionProperties cp : this.serverConnections.values()) {
            cp.setDisconnectOnExpiredPasswords(disconnectOnExpiredPasswords);
        }
    }

    @Override
    public void setGetProceduresReturnsFunctions(boolean getProcedureReturnsFunctions) {
        super.setGetProceduresReturnsFunctions(getProcedureReturnsFunctions);
    }

    // com.mysql.jdbc.Connection

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

    public boolean isSameResource(Connection c) {
        return false;
    }

    public boolean parserKnowsUnicode() {
        return false;
    }

    public void ping() throws SQLException {
    }

    public void resetServerState() throws SQLException {
    }

    public void setFailedOver(boolean flag) {
    }

    @Deprecated
    public void setPreferSlaveDuringFailover(boolean flag) {
    }

    public void setStatementComment(String comment) {
    }

    public void reportQueryTime(long millisOrNanos) {
    }

    public boolean isAbonormallyLongQuery(long millisOrNanos) {
        return false;
    }

    public void initializeExtension(Extension ex) throws SQLException {
    }

    public int getAutoIncrementIncrement() {
        return -1;
    }

    public boolean hasSameProperties(Connection c) {
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

    public void checkClosed() throws SQLException {
    }

    public Object getConnectionMutex() {
        return this;
    }

    public void setSessionMaxRows(int max) throws SQLException {
        for (Connection c : this.serverConnections.values()) {
            c.setSessionMaxRows(max);
        }
    }

    public int getSessionMaxRows() {
        return getActiveConnectionPassive().getSessionMaxRows();
    }

    // MySQLConnection	
    public boolean isProxySet() {
        return false;
    }

    public Connection duplicate() throws SQLException {
        return null;
    }

    public CachedResultSetMetaData getCachedMetaData(String sql) {
        return null;
    }

    public Timer getCancelTimer() {
        return null;
    }

    public SingleByteCharsetConverter getCharsetConverter(String javaEncodingName) throws SQLException {
        return null;
    }

    /**
     * @deprecated replaced by <code>getEncodingForIndex(int charsetIndex)</code>
     */
    @Deprecated
    public String getCharsetNameForIndex(int charsetIndex) throws SQLException {
        return getEncodingForIndex(charsetIndex);
    }

    public String getEncodingForIndex(int charsetIndex) throws SQLException {
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

        return this.currentConnection.getExceptionInterceptor();
    }

    public String getHost() {
        return null;
    }

    public String getHostPortPair() {
        return getActiveMySQLConnectionPassive().getHostPortPair();
    }

    public long getId() {
        return -1;
    }

    public int getMaxBytesPerChar(String javaCharsetName) throws SQLException {
        return -1;
    }

    public int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName) throws SQLException {
        return -1;
    }

    public int getNetBufferLength() {
        return -1;
    }

    public boolean getRequiresEscapingEncoder() {
        return false;
    }

    public int getServerMajorVersion() {
        return -1;
    }

    public int getServerMinorVersion() {
        return -1;
    }

    public int getServerSubMinorVersion() {
        return -1;
    }

    public String getServerVariable(String variableName) {
        return null;
    }

    public String getServerVersion() {
        return null;
    }

    public Calendar getSessionLockedCalendar() {
        return null;
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

    public Calendar getUtcCalendar() {
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

    public boolean isClientTzUTC() {
        return false;
    }

    public boolean isCursorFetchEnabled() throws SQLException {
        return false;
    }

    public boolean isReadInfoMsgEnabled() {
        return false;
    }

    public boolean isServerTzUTC() {
        return false;
    }

    public boolean lowerCaseTableNames() {
        return getActiveMySQLConnectionPassive().lowerCaseTableNames();
    }

    /**
     * 
     * @param stmt
     */
    public void maxRowsChanged(com.mysql.jdbc.Statement stmt) {
    }

    public void pingInternal(boolean checkForClosedConnection, int timeoutMillis) throws SQLException {
    }

    public void realClose(boolean calledExplicitly, boolean issueRollback, boolean skipLocalTeardown, Throwable reason) throws SQLException {
    }

    public void recachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException {
    }

    public void registerQueryExecutionTime(long queryTimeMs) {
    }

    public void registerStatement(com.mysql.jdbc.Statement stmt) {
    }

    public void reportNumberOfTablesAccessed(int numTablesAccessed) {
    }

    public boolean serverSupportsConvertFn() throws SQLException {
        return getActiveMySQLConnection().serverSupportsConvertFn();
    }

    public void setReadInfoMsgEnabled(boolean flag) {
    }

    public void setReadOnlyInternal(boolean readOnlyFlag) throws SQLException {
    }

    public boolean storesLowerCaseTableName() {
        return getActiveMySQLConnectionPassive().storesLowerCaseTableName();
    }

    public void throwConnectionClosedException() throws SQLException {
    }

    public void unregisterStatement(com.mysql.jdbc.Statement stmt) {
    }

    /**
     * 
     * @param stmt
     * @throws SQLException
     */
    public void unsetMaxRows(com.mysql.jdbc.Statement stmt) throws SQLException {
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

    /**
     * 
     * @param name
     * @return
     */
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

    public SQLWarning getWarnings() throws SQLException {
        return getActiveMySQLConnection().getWarnings();
    }

    public String nativeSQL(String sql) throws SQLException {
        return getActiveMySQLConnection().nativeSQL(sql);
    }

    public ProfilerEventHandler getProfilerEventHandlerInstance() {
        return null;
    }

    public void setProfilerEventHandlerInstance(ProfilerEventHandler h) {
    }

    public void decachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException {
    }
}
