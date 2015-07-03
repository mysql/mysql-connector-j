/*
  Copyright (c) 2004, 2015, Oracle and/or its affiliates. All rights reserved.

  The MySQL Connector/J is licensed under the terms of the GPLv2
  <http://www.gnu.org/licenses/old-licenses/gpl-2.0.html>, like most MySQL Connectors.
  There are special exceptions to the terms and conditions of the GPLv2 as it is applied to
  this software, see the FLOSS License Exception
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

package com.mysql.cj.jdbc.ha;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.Executor;

import com.mysql.cj.api.Extension;
import com.mysql.cj.api.PingTarget;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.jdbc.JdbcPropertySet;
import com.mysql.cj.api.jdbc.ResultSetInternalMethods;
import com.mysql.cj.api.jdbc.ha.LoadBalancedConnection;
import com.mysql.cj.api.jdbc.interceptors.StatementInterceptorV2;
import com.mysql.cj.core.ConnectionString.ConnectionStringType;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.jdbc.CachedResultSetMetaData;
import com.mysql.cj.jdbc.Field;
import com.mysql.cj.jdbc.NonRegisteringDriver;
import com.mysql.cj.jdbc.ServerPreparedStatement;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.mysqla.MysqlaSession;
import com.mysql.cj.mysqla.io.Buffer;

/**
 * Connection that opens two connections, one two a replication master, and another to one or more slaves, and decides to use master when the connection is not
 * read-only, and use slave(s) when the connection is read-only.
 */
public class ReplicationConnection implements JdbcConnection, PingTarget {
    protected JdbcConnection currentConnection;

    protected LoadBalancedConnection masterConnection;

    protected LoadBalancedConnection slavesConnection;

    private Properties slaveProperties;

    private Properties masterProperties;

    private NonRegisteringDriver driver;

    private long connectionGroupID = -1;

    private ReplicationConnectionGroup connectionGroup;

    private List<String> slaveHosts;

    private List<String> masterHosts;

    private boolean allowMasterDownConnections = false;

    private boolean enableJMX = false;

    private boolean readOnly = false;

    protected ReplicationConnection() {
    }

    public ReplicationConnection(Properties masterProperties, Properties slaveProperties, List<String> masterHostList, List<String> slaveHostList)
            throws SQLException {
        String enableJMXAsString = masterProperties.getProperty(PropertyDefinitions.PNAME_ha_enableJMX, "false");
        try {
            this.enableJMX = Boolean.parseBoolean(enableJMXAsString);
        } catch (Exception e) {
            throw SQLError.createSQLException(Messages.getString("MultihostConnection.badValueForHaEnableJMX", new Object[] { enableJMXAsString }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String allowMasterDownConnectionsAsString = masterProperties.getProperty(PropertyDefinitions.PNAME_allowMasterDownConnections, "false");
        try {
            this.allowMasterDownConnections = Boolean.parseBoolean(allowMasterDownConnectionsAsString);
        } catch (Exception e) {
            throw SQLError.createSQLException(
                    Messages.getString("ReplicationConnection.badValueForAllowMasterDownConnections", new Object[] { enableJMXAsString }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String group = masterProperties.getProperty(PropertyDefinitions.PNAME_replicationConnectionGroup, null);

        if (group != null) {
            this.connectionGroup = ReplicationConnectionGroupManager.getConnectionGroupInstance(group);
            if (this.enableJMX) {
                ReplicationConnectionGroupManager.registerJmx();
            }
            this.connectionGroupID = this.connectionGroup.registerReplicationConnection(this, masterHostList, slaveHostList);

            this.slaveHosts = new ArrayList<String>(this.connectionGroup.getSlaveHosts());
            this.masterHosts = new ArrayList<String>(this.connectionGroup.getMasterHosts());
        } else {
            this.slaveHosts = new ArrayList<String>(slaveHostList);
            this.masterHosts = new ArrayList<String>(masterHostList);
        }

        this.driver = new NonRegisteringDriver();
        this.slaveProperties = slaveProperties;
        this.masterProperties = masterProperties;

        boolean createdMaster = this.initializeMasterConnection();
        this.initializeSlaveConnection();
        if (!createdMaster) {
            this.readOnly = true;
            this.currentConnection = this.slavesConnection;
            return;
        }

        this.currentConnection = this.masterConnection;
    }

    private boolean initializeMasterConnection() throws SQLException {
        return this.initializeMasterConnection(this.allowMasterDownConnections);
    }

    public long getConnectionGroupId() {
        return this.connectionGroupID;
    }

    private boolean initializeMasterConnection(boolean allowMasterDown) throws SQLException {
        // get this value before we change the masterConnection reference:
        boolean isMaster = this.isMasterConnection();

        StringBuilder masterUrl = new StringBuilder(ConnectionStringType.LOADBALANCING_CONNECTION.urlPrefix);

        boolean firstHost = true;
        for (String host : this.masterHosts) {
            if (!firstHost) {
                masterUrl.append(',');
            }
            masterUrl.append(host);
            firstHost = false;
        }

        String masterDb = this.masterProperties.getProperty(PropertyDefinitions.DBNAME_PROPERTY_KEY);

        masterUrl.append("/");

        if (masterDb != null) {
            masterUrl.append(masterDb);
        }

        LoadBalancedConnection newMasterConn = null;
        try {
            newMasterConn = (com.mysql.cj.api.jdbc.ha.LoadBalancedConnection) this.driver.connect(masterUrl.toString(), this.masterProperties);
        } catch (SQLException ex) {
            if (allowMasterDown) {
                this.currentConnection = this.slavesConnection;
                this.masterConnection = null;
                this.readOnly = true;
                return false;
            }
            throw ex;
        }

        if (isMaster && this.currentConnection != null) {
            this.swapConnections(newMasterConn, this.currentConnection);
        }

        if (this.masterConnection != null) {
            try {
                this.masterConnection.close();
                this.masterConnection = null;
            } catch (SQLException e) {
            }
        }

        this.masterConnection = newMasterConn;
        return true;

    }

    private void initializeSlaveConnection() throws SQLException {
        if (this.slaveHosts.size() == 0) {
            return;
        }

        StringBuilder slaveUrl = new StringBuilder(ConnectionStringType.LOADBALANCING_CONNECTION.urlPrefix);

        boolean firstHost = true;
        for (String host : this.slaveHosts) {
            if (!firstHost) {
                slaveUrl.append(',');
            }
            slaveUrl.append(host);
            firstHost = false;
        }

        String slaveDb = this.slaveProperties.getProperty(PropertyDefinitions.DBNAME_PROPERTY_KEY);

        slaveUrl.append("/");

        if (slaveDb != null) {
            slaveUrl.append(slaveDb);
        }

        this.slavesConnection = (com.mysql.cj.api.jdbc.ha.LoadBalancedConnection) this.driver.connect(slaveUrl.toString(), this.slaveProperties);
        this.slavesConnection.setReadOnly(true);

        // switch to slaves connection if we're in read-only mode and
        // currently on the master. this means we didn't have any
        // slaves to use until now
        if (this.currentConnection != null && this.currentConnection == this.masterConnection && this.readOnly) {
            switchToSlavesConnection();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#clearWarnings()
     */
    public void clearWarnings() throws SQLException {
        getCurrentConnection().clearWarnings();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#close()
     */
    public synchronized void close() throws SQLException {
        if (this.masterConnection != null) {
            this.masterConnection.close();
        }
        if (this.slavesConnection != null) {
            this.slavesConnection.close();
        }

        if (this.connectionGroup != null) {
            this.connectionGroup.handleCloseConnection(this);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#commit()
     */
    public void commit() throws SQLException {
        getCurrentConnection().commit();
    }

    public boolean isHostMaster(String host) {
        if (host == null) {
            return false;
        }
        for (String test : this.masterHosts) {
            if (test.equalsIgnoreCase(host)) {
                return true;
            }
        }
        return false;
    }

    public boolean isHostSlave(String host) {
        if (host == null) {
            return false;
        }
        for (String test : this.slaveHosts) {
            if (test.equalsIgnoreCase(host)) {
                return true;
            }
        }
        return false;

    }

    public synchronized void removeSlave(String host) throws SQLException {
        removeSlave(host, true);
    }

    public synchronized void removeSlave(String host, boolean closeGently) throws SQLException {

        this.slaveHosts.remove(host);
        if (this.slavesConnection == null) {
            return;
        }

        if (closeGently) {
            this.slavesConnection.removeHostWhenNotInUse(host);
        } else {
            this.slavesConnection.removeHost(host);
        }

        // close the connection if it's the last slave
        if (this.slaveHosts.size() == 0) {
            switchToMasterConnection();
            this.slavesConnection.close();
            this.slavesConnection = null;
            setReadOnly(this.readOnly); // maintain
        }
    }

    public synchronized void addSlaveHost(String host) throws SQLException {
        if (this.isHostSlave(host)) {
            // throw new SQLException("Cannot add existing host!");
            return;
        }
        this.slaveHosts.add(host);
        if (this.slavesConnection == null) {
            initializeSlaveConnection();
        } else {
            this.slavesConnection.addHost(host);
        }
    }

    public synchronized void promoteSlaveToMaster(String host) throws SQLException {
        if (!this.isHostSlave(host)) {
            //			turned this off as one might walk up the replication tree and set master
            //			to the current's master's master.
            //			throw SQLError.createSQLException("Cannot promote host " + host + " to master, as it must first be configured as a slave.", null);

        }

        this.masterHosts.add(host);
        this.removeSlave(host);
        if (this.masterConnection != null) {
            this.masterConnection.addHost(host);
        }

    }

    public synchronized void removeMasterHost(String host) throws SQLException {
        this.removeMasterHost(host, true);
    }

    public synchronized void removeMasterHost(String host, boolean waitUntilNotInUse) throws SQLException {
        this.removeMasterHost(host, waitUntilNotInUse, false);
    }

    public synchronized void removeMasterHost(String host, boolean waitUntilNotInUse, boolean isNowSlave) throws SQLException {
        if (isNowSlave) {
            this.slaveHosts.add(host);
        }
        this.masterHosts.remove(host);

        if (this.masterConnection == null) {
            return;
        }

        if (waitUntilNotInUse) {
            this.masterConnection.removeHostWhenNotInUse(host);
        } else {
            this.masterConnection.removeHost(host);
        }

    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#createStatement()
     */
    public Statement createStatement() throws SQLException {
        Statement stmt = getCurrentConnection().createStatement();
        ((com.mysql.cj.api.jdbc.Statement) stmt).setPingTarget(this);

        return stmt;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#createStatement(int, int)
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        Statement stmt = getCurrentConnection().createStatement(resultSetType, resultSetConcurrency);

        ((com.mysql.cj.api.jdbc.Statement) stmt).setPingTarget(this);

        return stmt;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#createStatement(int, int, int)
     */
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        Statement stmt = getCurrentConnection().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);

        ((com.mysql.cj.api.jdbc.Statement) stmt).setPingTarget(this);

        return stmt;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#getAutoCommit()
     */
    public boolean getAutoCommit() throws SQLException {
        return getCurrentConnection().getAutoCommit();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#getCatalog()
     */
    public String getCatalog() throws SQLException {
        return getCurrentConnection().getCatalog();
    }

    public synchronized JdbcConnection getCurrentConnection() {
        return this.currentConnection;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#getHoldability()
     */
    public int getHoldability() throws SQLException {
        return getCurrentConnection().getHoldability();
    }

    public synchronized JdbcConnection getMasterConnection() {
        return this.masterConnection;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#getMetaData()
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        return getCurrentConnection().getMetaData();
    }

    public synchronized JdbcConnection getSlavesConnection() {
        return this.slavesConnection;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#getTransactionIsolation()
     */
    public int getTransactionIsolation() throws SQLException {
        return getCurrentConnection().getTransactionIsolation();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#getTypeMap()
     */
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return getCurrentConnection().getTypeMap();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#getWarnings()
     */
    public SQLWarning getWarnings() throws SQLException {
        return getCurrentConnection().getWarnings();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#isClosed()
     */
    public boolean isClosed() throws SQLException {
        return getCurrentConnection().isClosed();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#isReadOnly()
     */
    public synchronized boolean isReadOnly() throws SQLException {
        return this.readOnly;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#nativeSQL(java.lang.String)
     */
    public String nativeSQL(String sql) throws SQLException {
        return getCurrentConnection().nativeSQL(sql);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#prepareCall(java.lang.String)
     */
    public CallableStatement prepareCall(String sql) throws SQLException {
        return getCurrentConnection().prepareCall(sql);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
     */
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return getCurrentConnection().prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
     */
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return getCurrentConnection().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#prepareStatement(java.lang.String)
     */
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().prepareStatement(sql);

        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#prepareStatement(java.lang.String, int)
     */
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().prepareStatement(sql, autoGeneratedKeys);

        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().prepareStatement(sql, resultSetType, resultSetConcurrency);

        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#prepareStatement(java.lang.String, int, int,
     * int)
     */
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);

        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
     */
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().prepareStatement(sql, columnIndexes);

        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#prepareStatement(java.lang.String,
     * java.lang.String[])
     */
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().prepareStatement(sql, columnNames);

        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
     */
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        getCurrentConnection().releaseSavepoint(savepoint);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#rollback()
     */
    public void rollback() throws SQLException {
        getCurrentConnection().rollback();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#rollback(java.sql.Savepoint)
     */
    public void rollback(Savepoint savepoint) throws SQLException {
        getCurrentConnection().rollback(savepoint);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setAutoCommit(boolean)
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        getCurrentConnection().setAutoCommit(autoCommit);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setCatalog(java.lang.String)
     */
    public void setCatalog(String catalog) throws SQLException {
        getCurrentConnection().setCatalog(catalog);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setHoldability(int)
     */
    public void setHoldability(int holdability) throws SQLException {
        getCurrentConnection().setHoldability(holdability);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setReadOnly(boolean)
     */
    public synchronized void setReadOnly(boolean readOnly) throws SQLException {
        if (readOnly) {
            if (this.currentConnection != this.slavesConnection) {
                switchToSlavesConnection();
            }
        } else {
            if (this.currentConnection != this.masterConnection) {
                switchToMasterConnection();
            }
        }
        this.readOnly = readOnly;
        // allow master connection to be set to/from read-only if
        // there are no slaves
        if (this.currentConnection == this.masterConnection) {
            this.currentConnection.setReadOnly(this.readOnly);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setSavepoint()
     */
    public Savepoint setSavepoint() throws SQLException {
        return getCurrentConnection().setSavepoint();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setSavepoint(java.lang.String)
     */
    public Savepoint setSavepoint(String name) throws SQLException {
        return getCurrentConnection().setSavepoint(name);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Connection#setTransactionIsolation(int)
     */
    public void setTransactionIsolation(int level) throws SQLException {
        getCurrentConnection().setTransactionIsolation(level);
    }

    // For testing

    private synchronized void switchToMasterConnection() throws SQLException {
        if (this.masterConnection == null || this.masterConnection.isClosed()) {
            this.initializeMasterConnection();
        }
        swapConnections(this.masterConnection, this.slavesConnection);
        this.masterConnection.setReadOnly(false);
    }

    private synchronized void switchToSlavesConnection() throws SQLException {
        if (this.slavesConnection == null || this.slavesConnection.isClosed()) {
            this.initializeSlaveConnection();
        }
        if (this.slavesConnection != null) {
            swapConnections(this.slavesConnection, this.masterConnection);
            this.slavesConnection.setReadOnly(true);
        }
    }

    /**
     * Swaps current context (catalog, autocommit and txn_isolation) from
     * sourceConnection to targetConnection, and makes targetConnection
     * the "current" connection that will be used for queries.
     * 
     * @param switchToConnection
     *            the connection to swap from
     * @param switchFromConnection
     *            the connection to swap to
     * 
     * @throws SQLException
     *             if an error occurs
     */
    private synchronized void swapConnections(JdbcConnection switchToConnection, JdbcConnection switchFromConnection) throws SQLException {

        String switchFromCatalog = switchFromConnection.getCatalog();
        String switchToCatalog = switchToConnection.getCatalog();

        if (switchToCatalog != null && !switchToCatalog.equals(switchFromCatalog)) {
            switchToConnection.setCatalog(switchFromCatalog);
        } else if (switchFromCatalog != null) {
            switchToConnection.setCatalog(switchFromCatalog);
        }

        boolean switchToAutoCommit = switchToConnection.getAutoCommit();
        boolean switchFromConnectionAutoCommit = switchFromConnection.getAutoCommit();

        if (switchFromConnectionAutoCommit != switchToAutoCommit) {
            switchToConnection.setAutoCommit(switchFromConnectionAutoCommit);
        }

        int switchToIsolation = switchToConnection.getTransactionIsolation();

        int switchFromIsolation = switchFromConnection.getTransactionIsolation();

        if (switchFromIsolation != switchToIsolation) {
            switchToConnection.setTransactionIsolation(switchFromIsolation);
        }

        switchToConnection.setSessionMaxRows(switchFromConnection.getSessionMaxRows());

        this.currentConnection = switchToConnection;
    }

    public synchronized void doPing() throws SQLException {
        boolean isMasterConn = this.isMasterConnection();
        if (this.masterConnection != null) {
            try {
                this.masterConnection.ping();
            } catch (SQLException e) {
                if (isMasterConn) {
                    // flip to slave connections:
                    this.currentConnection = this.slavesConnection;
                    this.masterConnection = null;

                    throw e;
                }
            }
        } else {
            this.initializeMasterConnection();
        }

        if (this.slavesConnection != null) {
            try {
                this.slavesConnection.ping();
            } catch (SQLException e) {
                if (!isMasterConn) {
                    // flip to master connection:
                    this.currentConnection = this.masterConnection;
                    this.slavesConnection = null;

                    throw e;
                }
            }
        } else {
            this.initializeSlaveConnection();
        }
    }

    public synchronized void changeUser(String userName, String newPassword) throws SQLException {
        this.masterConnection.changeUser(userName, newPassword);
        this.slavesConnection.changeUser(userName, newPassword);
    }

    public synchronized void clearHasTriedMaster() {
        this.masterConnection.clearHasTriedMaster();
        this.slavesConnection.clearHasTriedMaster();

    }

    public PreparedStatement clientPrepareStatement(String sql) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().clientPrepareStatement(sql);
        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    public PreparedStatement clientPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().clientPrepareStatement(sql, autoGenKeyIndex);
        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    public PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().clientPrepareStatement(sql, resultSetType, resultSetConcurrency);
        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    public PreparedStatement clientPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().clientPrepareStatement(sql, autoGenKeyIndexes);
        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    public PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().clientPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    public PreparedStatement clientPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().clientPrepareStatement(sql, autoGenKeyColNames);
        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    public int getActiveStatementCount() {
        return getCurrentConnection().getActiveStatementCount();
    }

    public long getIdleFor() {
        return getCurrentConnection().getIdleFor();
    }

    public String getStatementComment() {
        return getCurrentConnection().getStatementComment();
    }

    public boolean hasTriedMaster() {
        return getCurrentConnection().hasTriedMaster();
    }

    public void initializeExtension(Extension ex) {
        getCurrentConnection().initializeExtension(ex);
    }

    public boolean isAbonormallyLongQuery(long millisOrNanos) {
        return getCurrentConnection().isAbonormallyLongQuery(millisOrNanos);
    }

    public boolean isInGlobalTx() {
        return getCurrentConnection().isInGlobalTx();
    }

    public boolean isMasterConnection() {
        if (this.currentConnection == null) {
            return true;
        }
        return this.currentConnection == this.masterConnection;
    }

    public boolean isNoBackslashEscapesSet() {
        return getCurrentConnection().isNoBackslashEscapesSet();
    }

    public boolean lowerCaseTableNames() {
        return getCurrentConnection().lowerCaseTableNames();
    }

    public synchronized void ping() throws SQLException {
        try {
            this.masterConnection.ping();
        } catch (SQLException e) {
            if (this.isMasterConnection()) {
                throw e;
            }
        }
        try {
            this.slavesConnection.ping();
        } catch (SQLException e) {
            if (!this.isMasterConnection()) {
                throw e;
            }
        }
    }

    public void reportQueryTime(long millisOrNanos) {
        getCurrentConnection().reportQueryTime(millisOrNanos);
    }

    public void resetServerState() throws SQLException {
        getCurrentConnection().resetServerState();
    }

    public PreparedStatement serverPrepareStatement(String sql) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().serverPrepareStatement(sql);
        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    public PreparedStatement serverPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().serverPrepareStatement(sql, autoGenKeyIndex);
        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    public PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().serverPrepareStatement(sql, resultSetType, resultSetConcurrency);
        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    public PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().serverPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    public PreparedStatement serverPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().serverPrepareStatement(sql, autoGenKeyIndexes);
        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    public PreparedStatement serverPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        PreparedStatement pstmt = getCurrentConnection().serverPrepareStatement(sql, autoGenKeyColNames);
        ((com.mysql.cj.api.jdbc.Statement) pstmt).setPingTarget(this);

        return pstmt;
    }

    public void setFailedOver(boolean flag) {
        getCurrentConnection().setFailedOver(flag);
    }

    public synchronized void setStatementComment(String comment) {
        this.masterConnection.setStatementComment(comment);
        this.slavesConnection.setStatementComment(comment);
    }

    public void shutdownServer() throws SQLException {
        getCurrentConnection().shutdownServer();
    }

    public boolean isSameResource(JdbcConnection c) {
        return getCurrentConnection().isSameResource(c);
    }

    public void setInGlobalTx(boolean flag) {
        getCurrentConnection().setInGlobalTx(flag);
    }

    public int getAutoIncrementIncrement() {
        return getCurrentConnection().getAutoIncrementIncrement();
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return getCurrentConnection().getExceptionInterceptor();
    }

    public boolean hasSameProperties(JdbcConnection c) {
        return this.masterConnection.hasSameProperties(c) && this.slavesConnection.hasSameProperties(c);
    }

    public Properties getProperties() {
        Properties props = new Properties();
        props.putAll(this.masterConnection.getProperties());
        props.putAll(this.slavesConnection.getProperties());

        return props;
    }

    public String getHost() {
        return getCurrentConnection().getHost();
    }

    public void setProxy(JdbcConnection proxy) {
        getCurrentConnection().setProxy(proxy);
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        getCurrentConnection().setTypeMap(map);

    }

    public boolean isServerLocal() throws SQLException {
        return getCurrentConnection().isServerLocal();
    }

    public void setSchema(String schema) throws SQLException {
        getCurrentConnection().setSchema(schema);
    }

    public String getSchema() throws SQLException {
        return getCurrentConnection().getSchema();
    }

    public void abort(Executor executor) throws SQLException {
        getCurrentConnection().abort(executor);
        if (this.connectionGroup != null) {
            this.connectionGroup.handleCloseConnection(this);
        }
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        getCurrentConnection().setNetworkTimeout(executor, milliseconds);
    }

    public int getNetworkTimeout() throws SQLException {
        return getCurrentConnection().getNetworkTimeout();
    }

    public void abortInternal() throws SQLException {
        getCurrentConnection().abortInternal();
        if (this.connectionGroup != null) {
            this.connectionGroup.handleCloseConnection(this);
        }
    }

    public void checkClosed() {
        getCurrentConnection().checkClosed();
    }

    public Object getConnectionMutex() {
        return getCurrentConnection().getConnectionMutex();
    }

    public boolean getAllowMasterDownConnections() {
        return this.allowMasterDownConnections;
    }

    public void setAllowMasterDownConnections(boolean connectIfMasterDown) {
        this.allowMasterDownConnections = connectIfMasterDown;
    }

    // TODO
    public boolean getHaEnableJMX() {
        return this.enableJMX;
    }

    public void setHaEnableJMX(boolean replicationEnableJMX) {
        this.enableJMX = replicationEnableJMX;

    }

    public int getSessionMaxRows() {
        return getCurrentConnection().getSessionMaxRows();
    }

    public void setSessionMaxRows(int max) throws SQLException {
        getCurrentConnection().setSessionMaxRows(max);
    }

    public Clob createClob() throws SQLException {
        return getCurrentConnection().createClob();
    }

    public Blob createBlob() throws SQLException {
        return getCurrentConnection().createBlob();
    }

    public NClob createNClob() throws SQLException {
        return getCurrentConnection().createNClob();
    }

    public SQLXML createSQLXML() throws SQLException {
        return getCurrentConnection().createSQLXML();
    }

    public boolean isValid(int timeout) throws SQLException {
        return getCurrentConnection().isValid(timeout);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        getCurrentConnection().setClientInfo(name, value);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        getCurrentConnection().setClientInfo(properties);
    }

    public String getClientInfo(String name) throws SQLException {
        return getCurrentConnection().getClientInfo(name);
    }

    public Properties getClientInfo() throws SQLException {
        return getCurrentConnection().getClientInfo();
    }

    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return getCurrentConnection().createArrayOf(typeName, elements);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return getCurrentConnection().createStruct(typeName, attributes);
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        try {
            // This works for classes that aren't actually wrapping
            // anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.getExceptionInterceptor());
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // This works for classes that aren't actually wrapping
        // anything
        return iface.isInstance(this);
    }

    @Override
    public String getProcessHost() {
        return getCurrentConnection().getProcessHost();
    }

    @Override
    public MysqlaSession getSession() {
        return getCurrentConnection().getSession();
    }

    @Override
    public long getId() {
        return getCurrentConnection().getId();
    }

    @Override
    public String getURL() {
        return getCurrentConnection().getURL();
    }

    @Override
    public String getUser() {
        return getCurrentConnection().getUser();
    }

    @Override
    public void createNewIO(boolean isForReconnect) {
        getCurrentConnection().createNewIO(isForReconnect);
    }

    @Override
    public boolean isProxySet() {
        return getCurrentConnection().isProxySet();
    }

    @Override
    public JdbcConnection duplicate() throws SQLException {
        return getCurrentConnection().duplicate();
    }

    @Override
    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, Buffer packet, int resultSetType,
            int resultSetConcurrency, boolean streamResults, String catalog, Field[] cachedMetadata) throws SQLException {
        return getCurrentConnection().execSQL(callingStatement, sql, maxRows, packet, resultSetType, resultSetConcurrency, streamResults, catalog,
                cachedMetadata);
    }

    @Override
    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, Buffer packet, int resultSetType,
            int resultSetConcurrency, boolean streamResults, String catalog, Field[] cachedMetadata, boolean isBatch) throws SQLException {
        return getCurrentConnection().execSQL(callingStatement, sql, maxRows, packet, resultSetType, resultSetConcurrency, streamResults, catalog,
                cachedMetadata, isBatch);
    }

    @Override
    public JdbcPropertySet getPropertySet() {
        return getCurrentConnection().getPropertySet();
    }

    @Override
    public StringBuilder generateConnectionCommentBlock(StringBuilder buf) {
        return getCurrentConnection().generateConnectionCommentBlock(buf);
    }

    @Override
    public CachedResultSetMetaData getCachedMetaData(String sql) {
        return getCurrentConnection().getCachedMetaData(sql);
    }

    @Override
    public Timer getCancelTimer() {
        return getCurrentConnection().getCancelTimer();
    }

    @Override
    public String getCharacterSetMetadata() {
        return getCurrentConnection().getCharacterSetMetadata();
    }

    @Override
    public Statement getMetadataSafeStatement() throws SQLException {
        return getCurrentConnection().getMetadataSafeStatement();
    }

    @Override
    public boolean getRequiresEscapingEncoder() {
        return getCurrentConnection().getRequiresEscapingEncoder();
    }

    @Override
    public ServerVersion getServerVersion() {
        return getCurrentConnection().getServerVersion();
    }

    @Override
    public List<StatementInterceptorV2> getStatementInterceptorsInstances() {
        return getCurrentConnection().getStatementInterceptorsInstances();
    }

    @Override
    public void incrementNumberOfPreparedExecutes() {
        getCurrentConnection().incrementNumberOfPreparedExecutes();
    }

    @Override
    public void incrementNumberOfPrepares() {
        getCurrentConnection().incrementNumberOfPrepares();
    }

    @Override
    public void incrementNumberOfResultSetsCreated() {
        getCurrentConnection().incrementNumberOfResultSetsCreated();
    }

    @Override
    public void initializeResultsMetadataFromCache(String sql, CachedResultSetMetaData cachedMetaData, ResultSetInternalMethods resultSet) throws SQLException {
        getCurrentConnection().initializeResultsMetadataFromCache(sql, cachedMetaData, resultSet);
    }

    @Override
    public void initializeSafeStatementInterceptors() throws SQLException {
        getCurrentConnection().initializeSafeStatementInterceptors();
    }

    @Override
    public boolean isReadInfoMsgEnabled() {
        return getCurrentConnection().isReadInfoMsgEnabled();
    }

    @Override
    public boolean isReadOnly(boolean useSessionStatus) throws SQLException {
        return getCurrentConnection().isReadOnly(useSessionStatus);
    }

    @Override
    public void pingInternal(boolean checkForClosedConnection, int timeoutMillis) throws SQLException {
        getCurrentConnection().pingInternal(checkForClosedConnection, timeoutMillis);
    }

    @Override
    public void realClose(boolean calledExplicitly, boolean issueRollback, boolean skipLocalTeardown, Throwable reason) throws SQLException {
        getCurrentConnection().realClose(calledExplicitly, issueRollback, skipLocalTeardown, reason);
    }

    @Override
    public void recachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException {
        getCurrentConnection().recachePreparedStatement(pstmt);
    }

    @Override
    public void decachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException {
        getCurrentConnection().decachePreparedStatement(pstmt);
    }

    @Override
    public void registerQueryExecutionTime(long queryTimeMs) {
        getCurrentConnection().registerQueryExecutionTime(queryTimeMs);
    }

    @Override
    public void registerStatement(com.mysql.cj.api.jdbc.Statement stmt) {
        getCurrentConnection().registerStatement(stmt);
    }

    @Override
    public void reportNumberOfTablesAccessed(int numTablesAccessed) {
        getCurrentConnection().reportNumberOfTablesAccessed(numTablesAccessed);
    }

    @Override
    public void setReadInfoMsgEnabled(boolean flag) {
        getCurrentConnection().setReadInfoMsgEnabled(flag);
    }

    @Override
    public void setReadOnlyInternal(boolean readOnlyFlag) throws SQLException {
        getCurrentConnection().setReadOnlyInternal(readOnlyFlag);
    }

    @Override
    public boolean storesLowerCaseTableName() {
        return getCurrentConnection().storesLowerCaseTableName();
    }

    @Override
    public void throwConnectionClosedException() throws SQLException {
        getCurrentConnection().throwConnectionClosedException();
    }

    @Override
    public void transactionBegun() throws SQLException {
        getCurrentConnection().transactionBegun();
    }

    @Override
    public void transactionCompleted() throws SQLException {
        getCurrentConnection().transactionCompleted();
    }

    @Override
    public void unregisterStatement(com.mysql.cj.api.jdbc.Statement stmt) {
        getCurrentConnection().unregisterStatement(stmt);
    }

    @Override
    public void unSafeStatementInterceptors() throws SQLException {
        getCurrentConnection().unSafeStatementInterceptors();
    }

    @Override
    public boolean useAnsiQuotedIdentifiers() {
        return getCurrentConnection().useAnsiQuotedIdentifiers();
    }

    @Override
    public JdbcConnection getMultiHostSafeProxy() {
        return getCurrentConnection().getMultiHostSafeProxy();
    }

}
