/*
  Copyright (c) 2007, 2015, Oracle and/or its affiliates. All rights reserved.

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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * A proxy for a dynamic com.mysql.jdbc.Connection implementation that load balances requests across a series of MySQL JDBC connections, where the balancing
 * takes place at transaction commit.
 * 
 * Therefore, for this to work (at all), you must use transactions, even if only reading data.
 * 
 * This implementation will invalidate connections that it detects have had communication errors when processing a request. Problematic hosts will be added to a
 * global blacklist for loadBalanceBlacklistTimeout ms, after which they will be removed from the blacklist and made eligible once again to be selected for new
 * connections.
 * 
 * This implementation is thread-safe, but it's questionable whether sharing a connection instance amongst threads is a good idea, given that transactions are
 * scoped to connections in JDBC.
 */
public class LoadBalancingConnectionProxy extends MultiHostConnectionProxy implements PingTarget {
    private ConnectionGroup connectionGroup = null;
    private long connectionGroupProxyID = 0;

    protected Map<String, ConnectionImpl> liveConnections;
    private Map<String, Integer> hostsToListIndexMap;
    private Map<ConnectionImpl, String> connectionsToHostsMap;
    private long activePhysicalConnections = 0;
    private long totalPhysicalConnections = 0;
    private long[] responseTimes;

    private int retriesAllDown;
    private BalanceStrategy balancer;
    private int autoCommitSwapThreshold = 0;

    public static final String BLACKLIST_TIMEOUT_PROPERTY_KEY = "loadBalanceBlacklistTimeout";
    private int globalBlacklistTimeout = 0;
    private static Map<String, Long> globalBlacklist = new HashMap<String, Long>();
    private String hostToRemove = null;

    private boolean inTransaction = false;
    private long transactionStartTime = 0;
    private long transactionCount = 0;

    private LoadBalanceExceptionChecker exceptionChecker;

    private static Constructor<?> JDBC_4_LB_CONNECTION_CTOR;
    static {
        if (Util.isJdbc4()) {
            try {
                JDBC_4_LB_CONNECTION_CTOR = Class.forName("com.mysql.jdbc.JDBC4LoadBalancedMySQLConnection").getConstructor(
                        new Class[] { LoadBalancingConnectionProxy.class });
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Creates a proxy for java.sql.Connection that routes requests between the given list of host:port and uses the given properties when creating connections.
     * 
     * @param hosts
     *            The list of the hosts to load balance.
     * @param props
     *            Connection properties from where to get initial settings and to be used in new connections.
     * @throws SQLException
     */
    LoadBalancingConnectionProxy(List<String> hosts, Properties props) throws SQLException {
        super();

        String group = props.getProperty("loadBalanceConnectionGroup", null);
        boolean enableJMX = false;
        String enableJMXAsString = props.getProperty("loadBalanceEnableJMX", "false");
        try {
            enableJMX = Boolean.parseBoolean(enableJMXAsString);
        } catch (Exception e) {
            throw SQLError.createSQLException(
                    Messages.getString("LoadBalancingConnectionProxy.badValueForLoadBalanceEnableJMX", new Object[] { enableJMXAsString }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        if (group != null) {
            this.connectionGroup = ConnectionGroupManager.getConnectionGroupInstance(group);
            if (enableJMX) {
                ConnectionGroupManager.registerJmx();
            }
            this.connectionGroupProxyID = this.connectionGroup.registerConnectionProxy(this, hosts);
            hosts = new ArrayList<String>(this.connectionGroup.getInitialHosts());
        }

        // hosts specifications may have been reset with settings from a previous connection group
        int numHosts = initializeHostsSpecs(hosts, props);

        this.liveConnections = new HashMap<String, ConnectionImpl>(numHosts);
        this.hostsToListIndexMap = new HashMap<String, Integer>(numHosts);
        for (int i = 0; i < numHosts; i++) {
            this.hostsToListIndexMap.put(this.hostList.get(i), i);
        }
        this.connectionsToHostsMap = new HashMap<ConnectionImpl, String>(numHosts);
        this.responseTimes = new long[numHosts];

        String retriesAllDownAsString = this.localProps.getProperty("retriesAllDown", "120");
        try {
            this.retriesAllDown = Integer.parseInt(retriesAllDownAsString);
        } catch (NumberFormatException nfe) {
            throw SQLError.createSQLException(
                    Messages.getString("LoadBalancingConnectionProxy.badValueForRetriesAllDown", new Object[] { retriesAllDownAsString }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String blacklistTimeoutAsString = this.localProps.getProperty(BLACKLIST_TIMEOUT_PROPERTY_KEY, "0");
        try {
            this.globalBlacklistTimeout = Integer.parseInt(blacklistTimeoutAsString);
        } catch (NumberFormatException nfe) {
            throw SQLError.createSQLException(
                    Messages.getString("LoadBalancingConnectionProxy.badValueForLoadBalanceBlacklistTimeout", new Object[] { retriesAllDownAsString }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String strategy = this.localProps.getProperty("loadBalanceStrategy", "random");
        if ("random".equals(strategy)) {
            this.balancer = (BalanceStrategy) Util.loadExtensions(null, props, "com.mysql.jdbc.RandomBalanceStrategy", "InvalidLoadBalanceStrategy", null).get(
                    0);
        } else if ("bestResponseTime".equals(strategy)) {
            this.balancer = (BalanceStrategy) Util.loadExtensions(null, props, "com.mysql.jdbc.BestResponseTimeBalanceStrategy", "InvalidLoadBalanceStrategy",
                    null).get(0);
        } else {
            this.balancer = (BalanceStrategy) Util.loadExtensions(null, props, strategy, "InvalidLoadBalanceStrategy", null).get(0);
        }

        String autoCommitSwapThresholdAsString = props.getProperty("loadBalanceAutoCommitStatementThreshold", "0");
        try {
            this.autoCommitSwapThreshold = Integer.parseInt(autoCommitSwapThresholdAsString);
        } catch (NumberFormatException nfe) {
            throw SQLError.createSQLException(Messages.getString("LoadBalancingConnectionProxy.badValueForLoadBalanceAutoCommitStatementThreshold",
                    new Object[] { autoCommitSwapThresholdAsString }), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String autoCommitSwapRegex = props.getProperty("loadBalanceAutoCommitStatementRegex", "");
        if (!("".equals(autoCommitSwapRegex))) {
            try {
                "".matches(autoCommitSwapRegex);
            } catch (Exception e) {
                throw SQLError
                        .createSQLException(Messages.getString("LoadBalancingConnectionProxy.badValueForLoadBalanceAutoCommitStatementRegex",
                                new Object[] { autoCommitSwapRegex }), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
            }
        }

        if (this.autoCommitSwapThreshold > 0) {
            String statementInterceptors = this.localProps.getProperty("statementInterceptors");
            if (statementInterceptors == null) {
                this.localProps.setProperty("statementInterceptors", "com.mysql.jdbc.LoadBalancedAutoCommitInterceptor");
            } else if (statementInterceptors.length() > 0) {
                this.localProps.setProperty("statementInterceptors", statementInterceptors + ",com.mysql.jdbc.LoadBalancedAutoCommitInterceptor");
            }
            props.setProperty("statementInterceptors", this.localProps.getProperty("statementInterceptors"));

        }

        this.balancer.init(null, props);

        String lbExceptionChecker = this.localProps.getProperty("loadBalanceExceptionChecker", "com.mysql.jdbc.StandardLoadBalanceExceptionChecker");
        this.exceptionChecker = (LoadBalanceExceptionChecker) Util.loadExtensions(null, props, lbExceptionChecker, "InvalidLoadBalanceExceptionChecker", null)
                .get(0);

        pickNewConnection();
    }

    /**
     * Wraps this object with a new load balanced Connection instance.
     * 
     * @return
     *         The connection object instance that wraps 'this'.
     */
    @Override
    MySQLConnection getNewWrapperForThisAsConnection() throws SQLException {
        if (Util.isJdbc4() || JDBC_4_LB_CONNECTION_CTOR != null) {
            return (MySQLConnection) Util.handleNewInstance(JDBC_4_LB_CONNECTION_CTOR, new Object[] { this }, null);
        }
        return new LoadBalancedMySQLConnection(this);
    }

    @Deprecated
    boolean shouldExceptionTriggerFailover(Throwable t) {
        return shouldExceptionTriggerConnectionSwitch(t);
    }

    /**
     * Consults the registered LoadBalanceExceptionChecker if the given exception should trigger a connection fail-over.
     * 
     * @param ex
     *            The Exception instance to check.
     * @return
     */
    @Override
    boolean shouldExceptionTriggerConnectionSwitch(Throwable t) {
        return t instanceof SQLException && this.exceptionChecker.shouldExceptionTriggerFailover((SQLException) t);
    }

    /**
     * Closes specified connection and removes it from required mappings.
     * 
     * @param conn
     * @throws SQLException
     */
    @Override
    synchronized void invalidateConnection(MySQLConnection conn) throws SQLException {
        super.invalidateConnection(conn);

        // add host to the global blacklist, if enabled
        if (this.isGlobalBlacklistEnabled()) {
            addToGlobalBlacklist(this.connectionsToHostsMap.get(conn));
        }

        // remove from liveConnections
        this.liveConnections.remove(this.connectionsToHostsMap.get(conn));
        Object mappedHost = this.connectionsToHostsMap.remove(conn);
        if (mappedHost != null && this.hostsToListIndexMap.containsKey(mappedHost)) {
            int hostIndex = this.hostsToListIndexMap.get(mappedHost);
            // reset the statistics for the host
            synchronized (this.responseTimes) {
                this.responseTimes[hostIndex] = 0;
            }
        }
    }

    /**
     * Picks the "best" connection to use for the next transaction based on the BalanceStrategy in use.
     * 
     * @throws SQLException
     */
    @Override
    synchronized void pickNewConnection() throws SQLException {
        if (this.isClosed && this.closedExplicitly) {
            return;
        }

        if (this.currentConnection == null) { // startup
            this.currentConnection = this.balancer.pickConnection(this, Collections.unmodifiableList(this.hostList),
                    Collections.unmodifiableMap(this.liveConnections), this.responseTimes.clone(), this.retriesAllDown);
            return;
        }

        if (this.currentConnection.isClosed()) {
            invalidateCurrentConnection();
        }

        int pingTimeout = this.currentConnection.getLoadBalancePingTimeout();
        boolean pingBeforeReturn = this.currentConnection.getLoadBalanceValidateConnectionOnSwapServer();

        for (int hostsTried = 0, hostsToTry = this.hostList.size(); hostsTried <= hostsToTry; hostsTried++) {
            ConnectionImpl newConn = null;
            try {
                newConn = this.balancer.pickConnection(this, Collections.unmodifiableList(this.hostList), Collections.unmodifiableMap(this.liveConnections),
                        this.responseTimes.clone(), this.retriesAllDown);

                if (this.currentConnection != null) {
                    if (pingBeforeReturn) {
                        if (pingTimeout == 0) {
                            newConn.ping();
                        } else {
                            newConn.pingInternal(true, pingTimeout);
                        }
                    }

                    syncSessionState(this.currentConnection, newConn);
                }

                this.currentConnection = newConn;
                return;

            } catch (SQLException e) {
                if (shouldExceptionTriggerConnectionSwitch(e) && newConn != null) {
                    // connection error, close up shop on current connection
                    invalidateConnection(newConn);
                }
            }
        }

        // no hosts available to swap connection to, close up.
        this.isClosed = true;
        this.closedReason = "Connection closed after inability to pick valid new connection during load-balance.";
    }

    /**
     * Creates a new physical connection for the given host:port and updates required internal mappings and statistics for that connection.
     * 
     * @param hostPortSpec
     *            The host:port specification.
     * @throws SQLException
     */
    @Override
    public synchronized ConnectionImpl createConnectionForHost(String hostPortSpec) throws SQLException {
        ConnectionImpl conn = super.createConnectionForHost(hostPortSpec);

        this.liveConnections.put(hostPortSpec, conn);
        this.connectionsToHostsMap.put(conn, hostPortSpec);

        this.activePhysicalConnections++;
        this.totalPhysicalConnections++;

        return conn;
    }

    /**
     * Closes all live connections.
     */
    private synchronized void closeAllConnections() {
        // close all underlying connections
        for (Connection c : this.liveConnections.values()) {
            try {
                this.activePhysicalConnections--;
                c.close();
            } catch (SQLException e) {
            }
        }

        if (!this.isClosed) {
            this.balancer.destroy();
            if (this.connectionGroup != null) {
                this.connectionGroup.closeConnectionProxy(this);
            }
        }

        this.liveConnections.clear();
        this.connectionsToHostsMap.clear();
    }

    /**
     * Closes all live connections.
     */
    @Override
    synchronized void doClose() {
        closeAllConnections();
    }

    /**
     * Aborts all live connections
     */
    @Override
    synchronized void doAbortInternal() {
        // abort all underlying connections
        for (Connection c : this.liveConnections.values()) {
            try {
                this.activePhysicalConnections--;
                c.abortInternal();
            } catch (SQLException e) {
            }
        }

        if (!this.isClosed) {
            this.balancer.destroy();
            if (this.connectionGroup != null) {
                this.connectionGroup.closeConnectionProxy(this);
            }
        }

        this.liveConnections.clear();
        this.connectionsToHostsMap.clear();
    }

    /**
     * Aborts all live connections, using the provided Executor.
     */
    @Override
    synchronized void doAbort(Executor executor) {
        // close all underlying connections
        for (Connection c : this.liveConnections.values()) {
            try {
                this.activePhysicalConnections--;
                c.abort(executor);
            } catch (SQLException e) {
            }
        }

        if (!this.isClosed) {
            this.balancer.destroy();
            if (this.connectionGroup != null) {
                this.connectionGroup.closeConnectionProxy(this);
            }
        }

        this.liveConnections.clear();
        this.connectionsToHostsMap.clear();
    }

    /**
     * Proxies method invocation on the java.sql.Connection interface, trapping "close", "isClosed" and "commit/rollback" to switch connections for load
     * balancing.
     * This is the continuation of MultiHostConnectionProxy#invoke(Object, Method, Object[]).
     */
    @Override
    public synchronized Object invokeMore(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (this.isClosed) {
            if (this.autoReconnect && !this.closedExplicitly) {
                // try to reconnect first!
                this.currentConnection = null;
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

        if (!this.inTransaction) {
            this.inTransaction = true;
            this.transactionStartTime = System.nanoTime();
            this.transactionCount++;
        }

        Object result = null;

        try {
            result = method.invoke(this.thisAsConnection, args);

            if (result != null) {
                if (result instanceof com.mysql.jdbc.Statement) {
                    ((com.mysql.jdbc.Statement) result).setPingTarget(this);
                }
                result = proxyIfReturnTypeIsJdbcInterface(method.getReturnType(), result);
            }

        } catch (InvocationTargetException e) {
            dealWithInvocationException(e);

        } finally {
            if ("commit".equals(methodName) || "rollback".equals(methodName)) {
                this.inTransaction = false;

                // Update stats
                String host = this.connectionsToHostsMap.get(this.currentConnection);
                // avoid NPE if the connection has already been removed from connectionsToHostsMap in invalidateCurrenctConnection()
                if (host != null) {
                    synchronized (this.responseTimes) {
                        Integer hostIndex = (this.hostsToListIndexMap.get(host));

                        if (hostIndex != null && hostIndex < this.responseTimes.length) {
                            this.responseTimes[hostIndex] = System.nanoTime() - this.transactionStartTime;
                        }
                    }
                }
                pickNewConnection();
            }
        }

        return result;
    }

    /**
     * Pings live connections.
     */
    public synchronized void doPing() throws SQLException {
        SQLException se = null;
        boolean foundHost = false;
        int pingTimeout = this.currentConnection.getLoadBalancePingTimeout();

        synchronized (this) {
            for (Iterator<String> i = this.hostList.iterator(); i.hasNext();) {
                String host = i.next();
                ConnectionImpl conn = this.liveConnections.get(host);
                if (conn == null) {
                    continue;
                }
                try {
                    if (pingTimeout == 0) {
                        conn.ping();
                    } else {
                        conn.pingInternal(true, pingTimeout);
                    }
                    foundHost = true;
                } catch (SQLException e) {
                    this.activePhysicalConnections--;
                    // give up if it is the current connection, otherwise NPE faking resultset later.
                    if (host.equals(this.connectionsToHostsMap.get(this.currentConnection))) {
                        // clean up underlying connections, since connection pool won't do it
                        closeAllConnections();
                        this.isClosed = true;
                        this.closedReason = "Connection closed because ping of current connection failed.";
                        throw e;
                    }

                    // if the Exception is caused by ping connection lifetime checks, don't add to blacklist
                    if (e.getMessage().equals(Messages.getString("Connection.exceededConnectionLifetime"))) {
                        // only set the return Exception if it's null
                        if (se == null) {
                            se = e;
                        }
                    } else {
                        // overwrite the return Exception no matter what
                        se = e;
                        if (isGlobalBlacklistEnabled()) {
                            addToGlobalBlacklist(host);
                        }
                    }
                    // take the connection out of the liveConnections Map
                    this.liveConnections.remove(this.connectionsToHostsMap.get(conn));
                }
            }
        }
        // if there were no successful pings
        if (!foundHost) {
            closeAllConnections();
            this.isClosed = true;
            this.closedReason = "Connection closed due to inability to ping any active connections.";
            // throw the stored Exception, if exists
            if (se != null) {
                throw se;
            }
            // or create a new SQLException and throw it, must be no liveConnections
            ((ConnectionImpl) this.currentConnection).throwConnectionClosedException();
        }
    }

    /**
     * Adds a host to the blacklist with the given timeout.
     * 
     * @param host
     *            The host to be blacklisted.
     * @param timeout
     *            The blacklist timeout for this entry.
     */
    public void addToGlobalBlacklist(String host, long timeout) {
        if (isGlobalBlacklistEnabled()) {
            synchronized (globalBlacklist) {
                globalBlacklist.put(host, timeout);
            }
        }
    }

    /**
     * Adds a host to the blacklist.
     * 
     * @param host
     *            The host to be blacklisted.
     */
    public void addToGlobalBlacklist(String host) {
        addToGlobalBlacklist(host, System.currentTimeMillis() + this.globalBlacklistTimeout);

    }

    /**
     * Checks if host blacklist management was enabled.
     * 
     * @return
     */
    public boolean isGlobalBlacklistEnabled() {
        return (this.globalBlacklistTimeout > 0);
    }

    /**
     * Returns a local hosts blacklist, or a blacklist with a single host to be removed, while cleaning up expired records from the global blacklist.
     * 
     * @return
     *         A local hosts blacklist.
     */
    public synchronized Map<String, Long> getGlobalBlacklist() {
        if (!isGlobalBlacklistEnabled()) {
            String localHostToRemove = this.hostToRemove;

            if (this.hostToRemove != null) {
                HashMap<String, Long> fakedBlacklist = new HashMap<String, Long>();
                fakedBlacklist.put(localHostToRemove, System.currentTimeMillis() + 5000);
                return fakedBlacklist;
            }

            return new HashMap<String, Long>(1);
        }

        // Make a local copy of the blacklist
        Map<String, Long> blacklistClone = new HashMap<String, Long>(globalBlacklist.size());
        // Copy everything from synchronized global blacklist to local copy for manipulation
        synchronized (globalBlacklist) {
            blacklistClone.putAll(globalBlacklist);
        }
        Set<String> keys = blacklistClone.keySet();

        // We're only interested in blacklisted hosts that are in the hostList
        keys.retainAll(this.hostList);

        // Don't need to synchronize here as we using a local copy
        for (Iterator<String> i = keys.iterator(); i.hasNext();) {
            String host = i.next();
            // OK if null is returned because another thread already purged Map entry.
            Long timeout = globalBlacklist.get(host);
            if (timeout != null && timeout < System.currentTimeMillis()) {
                // Timeout has expired, remove from blacklist
                synchronized (globalBlacklist) {
                    globalBlacklist.remove(host);
                }
                i.remove();
            }

        }
        if (keys.size() == this.hostList.size()) {
            // return an empty blacklist, let the BalanceStrategy implementations try to connect to everything since it appears that all hosts are
            // unavailable - we don't want to wait for loadBalanceBlacklistTimeout to expire.
            return new HashMap<String, Long>(1);
        }

        return blacklistClone;
    }

    /**
     * Removes a host from the host list, allowing it some time to be released gracefully if needed.
     * 
     * @param host
     *            The host to be removed.
     * @throws SQLException
     */
    public void removeHostWhenNotInUse(String host) throws SQLException {
        int timeBetweenChecks = 1000;
        long timeBeforeHardFail = 15000;

        synchronized (this) {
            addToGlobalBlacklist(host, System.currentTimeMillis() + timeBeforeHardFail + 1000);

            long cur = System.currentTimeMillis();

            while (System.currentTimeMillis() < cur + timeBeforeHardFail) {
                this.hostToRemove = host;

                if (!host.equals(this.currentConnection.getHost())) {
                    removeHost(host);
                    return;
                }

                try {
                    Thread.sleep(timeBetweenChecks);
                } catch (InterruptedException e) {
                    // better to swallow this and retry.
                }
            }
        }

        removeHost(host);
    }

    /**
     * Removes a host from the host list.
     * 
     * @param host
     *            The host to be removed.
     * @throws SQLException
     */
    public synchronized void removeHost(String host) throws SQLException {
        if (this.connectionGroup != null) {
            if (this.connectionGroup.getInitialHosts().size() == 1 && this.connectionGroup.getInitialHosts().contains(host)) {
                throw SQLError.createSQLException("Cannot remove only configured host.", null);
            }

            this.hostToRemove = host;

            if (host.equals(this.currentConnection.getHost())) {
                closeAllConnections();
            } else {
                this.connectionsToHostsMap.remove(this.liveConnections.remove(host));
                Integer idx = this.hostsToListIndexMap.remove(host);
                long[] newResponseTimes = new long[this.responseTimes.length - 1];
                int newIdx = 0;
                for (Iterator<String> i = this.hostList.iterator(); i.hasNext(); newIdx++) {
                    String copyHost = i.next();
                    if (idx != null && idx < this.responseTimes.length) {
                        newResponseTimes[newIdx] = this.responseTimes[idx];
                        this.hostsToListIndexMap.put(copyHost, newIdx);
                    }
                }
                this.responseTimes = newResponseTimes;
            }
        }
    }

    /**
     * Adds a host to the hosts list.
     * 
     * @param host
     *            The host to be added.
     * @return
     */
    public synchronized boolean addHost(String host) {
        if (this.hostsToListIndexMap.containsKey(host)) {
            return false;
        }

        long[] newResponseTimes = new long[this.responseTimes.length + 1];
        System.arraycopy(this.responseTimes, 0, newResponseTimes, 0, this.responseTimes.length);

        this.responseTimes = newResponseTimes;
        this.hostList.add(host);
        this.hostsToListIndexMap.put(host, this.responseTimes.length - 1);

        return true;
    }

    public synchronized boolean inTransaction() {
        return this.inTransaction;
    }

    public synchronized long getTransactionCount() {
        return this.transactionCount;
    }

    public synchronized long getActivePhysicalConnectionCount() {
        return this.activePhysicalConnections;
    }

    public synchronized long getTotalPhysicalConnectionCount() {
        return this.totalPhysicalConnections;
    }

    public synchronized long getConnectionGroupProxyID() {
        return this.connectionGroupProxyID;
    }

    public synchronized String getCurrentActiveHost() {
        MySQLConnection c = this.currentConnection;
        if (c != null) {
            Object o = this.connectionsToHostsMap.get(c);
            if (o != null) {
                return o.toString();
            }
        }
        return null;
    }

    public synchronized long getCurrentTransactionDuration() {
        if (this.inTransaction && this.transactionStartTime > 0) {
            return System.nanoTime() - this.transactionStartTime;
        }
        return 0;
    }
}
