/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * Connection that opens two connections, one two a replication master, and another to one or more slaves, and decides to use master when the connection is not
 * read-only, and use slave(s) when the connection is read-only.
 */
public class ReplicationConnectionProxy extends MultiHostConnectionProxy implements PingTarget {
    private ReplicationConnection thisAsReplicationConnection;

    private NonRegisteringDriver driver;

    protected boolean enableJMX = false;
    protected boolean allowMasterDownConnections = false;
    protected boolean allowSlaveDownConnections = false;
    protected boolean readFromMasterWhenNoSlaves = false;
    protected boolean readFromMasterWhenNoSlavesOriginal = false;
    protected boolean readOnly = false;

    ReplicationConnectionGroup connectionGroup;
    private long connectionGroupID = -1;

    private List<String> masterHosts;
    private Properties masterProperties;
    protected LoadBalancedConnection masterConnection;

    private List<String> slaveHosts;
    private Properties slaveProperties;
    protected LoadBalancedConnection slavesConnection;

    private static Constructor<?> JDBC_4_REPL_CONNECTION_CTOR;
    private static Class<?>[] INTERFACES_TO_PROXY;

    static {
        if (Util.isJdbc4()) {
            try {
                JDBC_4_REPL_CONNECTION_CTOR = Class.forName("com.mysql.jdbc.JDBC4ReplicationMySQLConnection")
                        .getConstructor(new Class[] { ReplicationConnectionProxy.class });
                INTERFACES_TO_PROXY = new Class<?>[] { ReplicationConnection.class, Class.forName("com.mysql.jdbc.JDBC4MySQLConnection") };
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else {
            INTERFACES_TO_PROXY = new Class<?>[] { ReplicationConnection.class };
        }
    }

    public static ReplicationConnection createProxyInstance(List<String> masterHostList, Properties masterProperties, List<String> slaveHostList,
            Properties slaveProperties) throws SQLException {
        ReplicationConnectionProxy connProxy = new ReplicationConnectionProxy(masterHostList, masterProperties, slaveHostList, slaveProperties);

        return (ReplicationConnection) java.lang.reflect.Proxy.newProxyInstance(ReplicationConnection.class.getClassLoader(), INTERFACES_TO_PROXY, connProxy);
    }

    /**
     * Creates a proxy for java.sql.Connection that routes requests to a load-balanced connection of master servers or a load-balanced connection of slave
     * servers. Each sub-connection is created with its own set of independent properties.
     * 
     * @param masterHostList
     *            The list of hosts to use in the masters connection.
     * @param masterProperties
     *            The properties for the masters connection.
     * @param slaveHostList
     *            The list of hosts to use in the slaves connection.
     * @param slaveProperties
     *            The properties for the slaves connection.
     * @throws SQLException
     */
    private ReplicationConnectionProxy(List<String> masterHostList, Properties masterProperties, List<String> slaveHostList, Properties slaveProperties)
            throws SQLException {
        super();

        this.thisAsReplicationConnection = (ReplicationConnection) this.thisAsConnection;

        String enableJMXAsString = masterProperties.getProperty("replicationEnableJMX", "false");
        try {
            this.enableJMX = Boolean.parseBoolean(enableJMXAsString);
        } catch (Exception e) {
            throw SQLError.createSQLException(
                    Messages.getString("ReplicationConnectionProxy.badValueForReplicationEnableJMX", new Object[] { enableJMXAsString }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String allowMasterDownConnectionsAsString = masterProperties.getProperty("allowMasterDownConnections", "false");
        try {
            this.allowMasterDownConnections = Boolean.parseBoolean(allowMasterDownConnectionsAsString);
        } catch (Exception e) {
            throw SQLError.createSQLException(
                    Messages.getString("ReplicationConnectionProxy.badValueForAllowMasterDownConnections", new Object[] { allowMasterDownConnectionsAsString }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String allowSlaveDownConnectionsAsString = masterProperties.getProperty("allowSlaveDownConnections", "false");
        try {
            this.allowSlaveDownConnections = Boolean.parseBoolean(allowSlaveDownConnectionsAsString);
        } catch (Exception e) {
            throw SQLError.createSQLException(
                    Messages.getString("ReplicationConnectionProxy.badValueForAllowSlaveDownConnections", new Object[] { allowSlaveDownConnectionsAsString }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String readFromMasterWhenNoSlavesAsString = masterProperties.getProperty("readFromMasterWhenNoSlaves");
        try {
            this.readFromMasterWhenNoSlavesOriginal = Boolean.parseBoolean(readFromMasterWhenNoSlavesAsString);

        } catch (Exception e) {
            throw SQLError.createSQLException(
                    Messages.getString("ReplicationConnectionProxy.badValueForReadFromMasterWhenNoSlaves", new Object[] { readFromMasterWhenNoSlavesAsString }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String group = masterProperties.getProperty("replicationConnectionGroup", null);

        if (group != null) {
            this.connectionGroup = ReplicationConnectionGroupManager.getConnectionGroupInstance(group);
            if (this.enableJMX) {
                ReplicationConnectionGroupManager.registerJmx();
            }
            this.connectionGroupID = this.connectionGroup.registerReplicationConnection(this.thisAsReplicationConnection, masterHostList, slaveHostList);

            this.slaveHosts = new ArrayList<String>(this.connectionGroup.getSlaveHosts());
            this.masterHosts = new ArrayList<String>(this.connectionGroup.getMasterHosts());
        } else {
            this.slaveHosts = new ArrayList<String>(slaveHostList);
            this.masterHosts = new ArrayList<String>(masterHostList);
        }

        this.driver = new NonRegisteringDriver();
        this.slaveProperties = slaveProperties;
        this.masterProperties = masterProperties;

        resetReadFromMasterWhenNoSlaves();

        // Initialize slaves connection first so that it is ready to be used in case the masters connection fails and 'allowMasterDownConnections=true'.
        try {
            initializeSlavesConnection();
        } catch (SQLException e) {
            if (!this.allowSlaveDownConnections) {
                throw e;
            } // Else swallow this exception.
        }

        try {
            this.currentConnection = initializeMasterConnection();
        } catch (SQLException e) {
            if (this.allowMasterDownConnections && this.slavesConnection != null) {
                // Set read-only and fail over to the slaves connection.
                this.readOnly = true;
                this.currentConnection = this.slavesConnection;
            } else {
                throw e;
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
    MySQLConnection getNewWrapperForThisAsConnection() throws SQLException {
        if (Util.isJdbc4() || JDBC_4_REPL_CONNECTION_CTOR != null) {
            return (MySQLConnection) Util.handleNewInstance(JDBC_4_REPL_CONNECTION_CTOR, new Object[] { this }, null);
        }
        return new ReplicationMySQLConnection(this);
    }

    /**
     * Propagates the connection proxy down through all live connections.
     * 
     * @param proxyConn
     *            The top level connection in the multi-host connections chain.
     */
    @Override
    protected void propagateProxyDown(MySQLConnection proxyConn) {
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
     * @param ex
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
     */
    public boolean isSlavesConnection() {
        return this.currentConnection != null && this.currentConnection == this.slavesConnection;
    }

    @Override
    void pickNewConnection() throws SQLException {
        // no-op
    }

    @Override
    void doClose() throws SQLException {
        MySQLConnection prevConnection = this.currentConnection;
        if (this.masterConnection != null) {
            this.currentConnection = this.masterConnection;
            this.masterConnection.close();
        }
        if (this.slavesConnection != null) {
            this.currentConnection = this.slavesConnection;
            this.slavesConnection.close();
        }
        this.currentConnection = prevConnection;

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
        Object result = method.invoke(this.thisAsConnection, args);
        if (result != null && result instanceof Statement) {
            ((Statement) result).setPingTarget(this);
        }
        return result;
    }

    /**
     * Pings both l/b connections. Switch to another connection in case of failure.
     */
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
                if (!this.readFromMasterWhenNoSlaves) {
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
            if (this.readFromMasterWhenNoSlaves && mastersPingException == null) {
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

    private MySQLConnection initializeMasterConnection() throws SQLException {
        this.masterConnection = null;

        if (this.masterHosts.size() == 0) {
            return null;
        }

        LoadBalancedConnection newMasterConn = (LoadBalancedConnection) this.driver.connect(buildURL(this.masterHosts, this.masterProperties),
                this.masterProperties);
        newMasterConn.setProxy(getProxy());

        this.masterConnection = newMasterConn;
        return this.masterConnection;
    }

    private MySQLConnection initializeSlavesConnection() throws SQLException {
        this.slavesConnection = null;

        if (this.slaveHosts.size() == 0) {
            return null;
        }

        LoadBalancedConnection newSlavesConn = (LoadBalancedConnection) this.driver.connect(buildURL(this.slaveHosts, this.slaveProperties),
                this.slaveProperties);
        newSlavesConn.setProxy(getProxy());
        newSlavesConn.setReadOnly(true);

        this.slavesConnection = newSlavesConn;
        return this.slavesConnection;
    }

    private String buildURL(List<String> hosts, Properties props) {
        StringBuilder url = new StringBuilder(NonRegisteringDriver.LOADBALANCE_URL_PREFIX);

        boolean firstHost = true;
        for (String host : hosts) {
            if (!firstHost) {
                url.append(',');
            }
            url.append(host);
            firstHost = false;
        }
        url.append("/");
        String masterDb = props.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
        if (masterDb != null) {
            url.append(masterDb);
        }

        return url.toString();
    }

    private synchronized void switchToMasterConnection() throws SQLException {
        if (this.masterConnection == null || this.masterConnection.isClosed()) {
            try {
                initializeMasterConnection();
            } catch (SQLException e) {
                this.currentConnection = null;
                throw e;
            }
        }
        if (!isMasterConnection() && this.masterConnection != null) {
            syncSessionState(this.currentConnection, this.masterConnection, false);
            this.currentConnection = this.masterConnection;
        }
    }

    private synchronized void switchToSlavesConnection() throws SQLException {
        if (this.slavesConnection == null || this.slavesConnection.isClosed()) {
            try {
                initializeSlavesConnection();
            } catch (SQLException e) {
                this.currentConnection = null;
                throw e;
            }
        }
        if (!isSlavesConnection() && this.slavesConnection != null) {
            syncSessionState(this.currentConnection, this.slavesConnection, true);
            this.currentConnection = this.slavesConnection;
        }
    }

    private boolean switchToSlavesConnectionIfNecessary() throws SQLException {
        // Switch to slaves connection if we're in read-only mode and currently on the master. This means we didn't have any slaves to use until now.
        if (isMasterConnection() && this.readOnly) {
            switchToSlavesConnection();
            return true;
        }
        return false;
    }

    public synchronized Connection getCurrentConnection() {
        return this.currentConnection;
    }

    public long getConnectionGroupId() {
        return this.connectionGroupID;
    }

    public synchronized Connection getMasterConnection() {
        return this.masterConnection;
    }

    public synchronized void promoteSlaveToMaster(String host) throws SQLException {
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
            resetReadFromMasterWhenNoSlaves();

        }
        this.masterHosts.remove(host);

        // the master connection may have been implicitly closed by a previous op. don't let it stop us
        if (this.masterConnection == null || this.masterConnection.isClosed()) {
            this.masterConnection = null;
            return;
        }

        if (waitUntilNotInUse) {
            this.masterConnection.removeHostWhenNotInUse(host);
        } else {
            this.masterConnection.removeHost(host);
        }
    }

    public boolean isHostMaster(String host) {
        if (host == null) {
            return false;
        }
        for (String masterHost : this.masterHosts) {
            if (masterHost.equalsIgnoreCase(host)) {
                return true;
            }
        }
        return false;
    }

    public synchronized Connection getSlavesConnection() {
        return this.slavesConnection;
    }

    public synchronized void addSlaveHost(String host) throws SQLException {
        if (this.isHostSlave(host)) {
            return;
        }
        this.slaveHosts.add(host);
        resetReadFromMasterWhenNoSlaves();
        if (this.slavesConnection == null) {
            initializeSlavesConnection();
            switchToSlavesConnectionIfNecessary();
        } else {
            this.slavesConnection.addHost(host);
        }
    }

    public synchronized void removeSlave(String host) throws SQLException {
        removeSlave(host, true);
    }

    public synchronized void removeSlave(String host, boolean closeGently) throws SQLException {
        this.slaveHosts.remove(host);
        resetReadFromMasterWhenNoSlaves();

        if (this.slavesConnection == null || this.slavesConnection.isClosed()) {
            this.slavesConnection = null;
            return;
        }

        if (closeGently) {
            this.slavesConnection.removeHostWhenNotInUse(host);
        } else {
            this.slavesConnection.removeHost(host);
        }

        // close the connection if it's the last slave
        if (this.slaveHosts.isEmpty()) {
            this.slavesConnection.close();
            this.slavesConnection = null;

            // Default behavior, no need to check this.readFromMasterWhenNoSlaves
            switchToMasterConnection();
            this.currentConnection.setReadOnly(this.readOnly); // maintain
        }
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

    public synchronized void setReadOnly(boolean readOnly) throws SQLException {
        if (readOnly) {
            if (!isSlavesConnection() || this.currentConnection.isClosed()) {
                try {
                    switchToSlavesConnection();
                } catch (SQLException e) {
                    if (this.readFromMasterWhenNoSlaves) {
                        switchToMasterConnection();
                    } else {
                        throw e;
                    }
                }
            }
        } else {
            if (!isMasterConnection() || this.currentConnection.isClosed()) {
                switchToMasterConnection();
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
}
