/*
 * Copyright (c) 2007, 2021, Oracle and/or its affiliates.
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

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.mysql.cj.Messages;
import com.mysql.cj.PingTarget;
import com.mysql.cj.conf.ConnectionUrl;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.url.LoadBalanceConnectionUrl;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.ConnectionGroup;
import com.mysql.cj.jdbc.ConnectionGroupManager;
import com.mysql.cj.jdbc.ConnectionImpl;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.util.Util;

/**
 * A proxy for a dynamic com.mysql.cj.jdbc.JdbcConnection implementation that load balances requests across a series of MySQL JDBC connections, where the
 * balancing
 * takes place at transaction commit.
 * 
 * Therefore, for this to work (at all), you must use transactions, even if only reading data.
 * 
 * This implementation will invalidate connections that it detects have had communication errors when processing a request. Problematic hosts will be added to a
 * global blocklist for loadBalanceBlocklistTimeout ms, after which they will be removed from the blocklist and made eligible once again to be selected for new
 * connections.
 * 
 * This implementation is thread-safe, but it's questionable whether sharing a connection instance amongst threads is a good idea, given that transactions are
 * scoped to connections in JDBC.
 */
public class LoadBalancedConnectionProxy extends MultiHostConnectionProxy implements PingTarget {

    private static final ReentrantLock OBJECT_LOCK = new ReentrantLock();

    private ConnectionGroup connectionGroup = null;
    private long connectionGroupProxyID = 0;

    protected Map<String, ConnectionImpl> liveConnections;
    private Map<String, Integer> hostsToListIndexMap;
    private Map<ConnectionImpl, String> connectionsToHostsMap;
    private long totalPhysicalConnections = 0;
    private long[] responseTimes;

    private int retriesAllDown;
    private BalanceStrategy balancer;

    private int globalBlocklistTimeout = 0;
    private static Map<String, Long> globalBlocklist = new HashMap<>();
    private int hostRemovalGracePeriod = 0;
    // host:port pairs to be considered as removed (definitely blocklisted) from the original hosts list.
    private Set<String> hostsToRemove = new HashSet<>();

    private boolean inTransaction = false;
    private long transactionStartTime = 0;
    private long transactionCount = 0;

    private LoadBalanceExceptionChecker exceptionChecker;

    private static Class<?>[] INTERFACES_TO_PROXY = new Class<?>[] { LoadBalancedConnection.class, JdbcConnection.class };

    /**
     * Static factory to create {@link LoadBalancedConnection} instances.
     * 
     * @param connectionUrl
     *            The connection URL containing the hosts in a load-balance setup.
     * @return A {@link LoadBalancedConnection} proxy.
     * @throws SQLException
     *             if an error occurs
     */
    public static LoadBalancedConnection createProxyInstance(ConnectionUrl connectionUrl) throws SQLException {
        LoadBalancedConnectionProxy connProxy = new LoadBalancedConnectionProxy(connectionUrl);
        return (LoadBalancedConnection) java.lang.reflect.Proxy.newProxyInstance(LoadBalancedConnection.class.getClassLoader(), INTERFACES_TO_PROXY, connProxy);
    }

    /**
     * Creates a proxy for java.sql.Connection that routes requests between the hosts in the connection URL.
     * 
     * @param connectionUrl
     *            The connection URL containing the hosts to load balance.
     * @throws SQLException
     *             if an error occurs
     */
    public LoadBalancedConnectionProxy(ConnectionUrl connectionUrl) throws SQLException {
        super();

        List<HostInfo> hosts;
        Properties props = connectionUrl.getConnectionArgumentsAsProperties();

        String group = props.getProperty(PropertyKey.loadBalanceConnectionGroup.getKeyName(), null);
        boolean enableJMX = false;
        String enableJMXAsString = props.getProperty(PropertyKey.ha_enableJMX.getKeyName(), "false");
        try {
            enableJMX = Boolean.parseBoolean(enableJMXAsString);
        } catch (Exception e) {
            throw SQLError.createSQLException(Messages.getString("MultihostConnection.badValueForHaEnableJMX", new Object[] { enableJMXAsString }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        if (!isNullOrEmpty(group) && LoadBalanceConnectionUrl.class.isAssignableFrom(connectionUrl.getClass())) {
            this.connectionGroup = ConnectionGroupManager.getConnectionGroupInstance(group);
            if (enableJMX) {
                ConnectionGroupManager.registerJmx();
            }
            this.connectionGroupProxyID = this.connectionGroup.registerConnectionProxy(this,
                    ((LoadBalanceConnectionUrl) connectionUrl).getHostInfoListAsHostPortPairs());
            hosts = ((LoadBalanceConnectionUrl) connectionUrl).getHostInfoListFromHostPortPairs(this.connectionGroup.getInitialHosts());
        } else {
            hosts = connectionUrl.getHostsList();
        }

        // hosts specifications may have been reset with settings from a previous connection group
        int numHosts = initializeHostsSpecs(connectionUrl, hosts);

        this.liveConnections = new HashMap<>(numHosts);
        this.hostsToListIndexMap = new HashMap<>(numHosts);
        for (int i = 0; i < numHosts; i++) {
            this.hostsToListIndexMap.put(this.hostsList.get(i).getHostPortPair(), i);
        }
        this.connectionsToHostsMap = new HashMap<>(numHosts);
        this.responseTimes = new long[numHosts];

        String retriesAllDownAsString = props.getProperty(PropertyKey.retriesAllDown.getKeyName(), "120");
        try {
            this.retriesAllDown = Integer.parseInt(retriesAllDownAsString);
        } catch (NumberFormatException nfe) {
            throw SQLError.createSQLException(
                    Messages.getString("LoadBalancedConnectionProxy.badValueForRetriesAllDown", new Object[] { retriesAllDownAsString }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String blocklistTimeoutAsString = props.getProperty(PropertyKey.loadBalanceBlocklistTimeout.getKeyName(), "0");
        try {
            this.globalBlocklistTimeout = Integer.parseInt(blocklistTimeoutAsString);
        } catch (NumberFormatException nfe) {
            throw SQLError.createSQLException(
                    Messages.getString("LoadBalancedConnectionProxy.badValueForLoadBalanceBlocklistTimeout", new Object[] { blocklistTimeoutAsString }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String hostRemovalGracePeriodAsString = props.getProperty(PropertyKey.loadBalanceHostRemovalGracePeriod.getKeyName(), "15000");
        try {
            this.hostRemovalGracePeriod = Integer.parseInt(hostRemovalGracePeriodAsString);
        } catch (NumberFormatException nfe) {
            throw SQLError.createSQLException(Messages.getString("LoadBalancedConnectionProxy.badValueForLoadBalanceHostRemovalGracePeriod",
                    new Object[] { hostRemovalGracePeriodAsString }), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String strategy = props.getProperty(PropertyKey.ha_loadBalanceStrategy.getKeyName(), "random");
        try {
            switch (strategy) {
                case "random":
                    this.balancer = new RandomBalanceStrategy();
                    break;
                case "bestResponseTime":
                    this.balancer = new BestResponseTimeBalanceStrategy();
                    break;
                case "serverAffinity":
                    this.balancer = new ServerAffinityStrategy(props.getProperty(PropertyKey.serverAffinityOrder.getKeyName(), null));
                    break;
                default:
                    this.balancer = (BalanceStrategy) Class.forName(strategy).newInstance();
            }
        } catch (Throwable t) {
            throw SQLError.createSQLException(Messages.getString("InvalidLoadBalanceStrategy", new Object[] { strategy }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, t, null);
        }

        String autoCommitSwapThresholdAsString = props.getProperty(PropertyKey.loadBalanceAutoCommitStatementThreshold.getKeyName(), "0");
        try {
            Integer.parseInt(autoCommitSwapThresholdAsString);
        } catch (NumberFormatException nfe) {
            throw SQLError.createSQLException(Messages.getString("LoadBalancedConnectionProxy.badValueForLoadBalanceAutoCommitStatementThreshold",
                    new Object[] { autoCommitSwapThresholdAsString }), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
        }

        String autoCommitSwapRegex = props.getProperty(PropertyKey.loadBalanceAutoCommitStatementRegex.getKeyName(), "");
        if (!("".equals(autoCommitSwapRegex))) {
            try {
                "".matches(autoCommitSwapRegex);
            } catch (Exception e) {
                throw SQLError.createSQLException(
                        Messages.getString("LoadBalancedConnectionProxy.badValueForLoadBalanceAutoCommitStatementRegex", new Object[] { autoCommitSwapRegex }),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, null);
            }
        }

        try {
            String lbExceptionChecker = props.getProperty(PropertyKey.loadBalanceExceptionChecker.getKeyName(),
                    StandardLoadBalanceExceptionChecker.class.getName());
            this.exceptionChecker = (LoadBalanceExceptionChecker) Util.getInstance(lbExceptionChecker, new Class<?>[0], new Object[0], null,
                    Messages.getString("InvalidLoadBalanceExceptionChecker"));
            this.exceptionChecker.init(props);

        } catch (CJException e) {
            throw SQLExceptionsMapping.translateException(e, null);
        }

        pickNewConnection();
    }

    /**
     * Wraps this object with a new load balanced Connection instance.
     * 
     * @return
     *         The connection object instance that wraps 'this'.
     * @throws SQLException
     *             if an error occurs
     */
    @Override
    JdbcConnection getNewWrapperForThisAsConnection() throws SQLException {
        return new LoadBalancedMySQLConnection(this);
    }

    /**
     * Propagates the connection proxy down through all live connections.
     * 
     * @param proxyConn
     *            The top level connection in the multi-host connections chain.
     */
    @Override
    protected void propagateProxyDown(JdbcConnection proxyConn) {
        for (JdbcConnection c : this.liveConnections.values()) {
            c.setProxy(proxyConn);
        }
    }

    @Deprecated
    public boolean shouldExceptionTriggerFailover(Throwable t) {
        return shouldExceptionTriggerConnectionSwitch(t);
    }

    /**
     * Consults the registered LoadBalanceExceptionChecker if the given exception should trigger a connection fail-over.
     * 
     * @param t
     *            The Exception instance to check.
     * @return true if the given exception should trigger a connection fail-over
     */
    @Override
    boolean shouldExceptionTriggerConnectionSwitch(Throwable t) {
        return t instanceof SQLException && this.exceptionChecker.shouldExceptionTriggerFailover(t);
    }

    /**
     * Always returns 'true' as there are no "sources" and "replicas" in this type of connection.
     */
    @Override
    boolean isSourceConnection() {
        return true;
    }

    /**
     * Closes specified connection and removes it from required mappings.
     * 
     * @param conn
     *            connection
     * @throws SQLException
     *             if an error occurs
     */
    @Override
    void invalidateConnection(JdbcConnection conn) throws SQLException {
        this.getObjectLock().lock();
        try {
            super.invalidateConnection(conn);

            // add host to the global blocklist, if enabled
            if (this.isGlobalBlocklistEnabled()) {
                String host = this.connectionsToHostsMap.get(conn);
                if (host != null) {
                    addToGlobalBlocklist(host);
                }
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
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Picks the "best" connection to use for the next transaction based on the BalanceStrategy in use.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    @Override
    public void pickNewConnection() throws SQLException {
        this.getObjectLock().lock();
        try {
            if (this.isClosed && this.closedExplicitly) {
                return;
            }

            List<String> hostPortList = Collections.unmodifiableList(this.hostsList.stream().map(hi -> hi.getHostPortPair()).collect(Collectors.toList()));

            if (this.currentConnection == null) { // startup
                this.currentConnection = this.balancer.pickConnection(this, hostPortList, Collections.unmodifiableMap(this.liveConnections),
                        this.responseTimes.clone(), this.retriesAllDown);
                return;
            }

            if (this.currentConnection.isClosed()) {
                invalidateCurrentConnection();
            }

            int pingTimeout = this.currentConnection.getPropertySet().getIntegerProperty(PropertyKey.loadBalancePingTimeout).getValue();
            boolean pingBeforeReturn = this.currentConnection.getPropertySet().getBooleanProperty(PropertyKey.loadBalanceValidateConnectionOnSwapServer).getValue();

            for (int hostsTried = 0, hostsToTry = this.hostsList.size(); hostsTried < hostsToTry; hostsTried++) {
                ConnectionImpl newConn = null;
                try {
                    newConn = (ConnectionImpl) this.balancer.pickConnection(this, hostPortList, Collections.unmodifiableMap(this.liveConnections),
                            this.responseTimes.clone(), this.retriesAllDown);

                    if (this.currentConnection != null) {
                        if (pingBeforeReturn) {
                            newConn.pingInternal(true, pingTimeout);
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
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Creates a new physical connection for the given {@link HostInfo} and updates required internal mappings and statistics for that connection.
     * 
     * @param hostInfo
     *            The host info instance.
     * @return
     *         The new Connection instance.
     * @throws SQLException
     *             if an error occurs
     */
    @Override
    public ConnectionImpl createConnectionForHost(HostInfo hostInfo) throws SQLException {
        this.getObjectLock().lock();
        try {
            ConnectionImpl conn = super.createConnectionForHost(hostInfo);

            this.liveConnections.put(hostInfo.getHostPortPair(), conn);
            this.connectionsToHostsMap.put(conn, hostInfo.getHostPortPair());

            removeFromGlobalBlocklist(hostInfo.getHostPortPair());

            this.totalPhysicalConnections++;

            for (QueryInterceptor stmtInterceptor : conn.getQueryInterceptorsInstances()) {
                if (stmtInterceptor instanceof LoadBalancedAutoCommitInterceptor) {
                    ((LoadBalancedAutoCommitInterceptor) stmtInterceptor).resumeCounters();
                    break;
                }
            }

            return conn;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    @Override
    void syncSessionState(JdbcConnection source, JdbcConnection target, boolean readOnly) throws SQLException {
        LoadBalancedAutoCommitInterceptor lbAutoCommitStmtInterceptor = null;
        for (QueryInterceptor stmtInterceptor : target.getQueryInterceptorsInstances()) {
            if (stmtInterceptor instanceof LoadBalancedAutoCommitInterceptor) {
                lbAutoCommitStmtInterceptor = (LoadBalancedAutoCommitInterceptor) stmtInterceptor;
                lbAutoCommitStmtInterceptor.pauseCounters();
                break;
            }
        }
        super.syncSessionState(source, target, readOnly);
        if (lbAutoCommitStmtInterceptor != null) {
            lbAutoCommitStmtInterceptor.resumeCounters();
        }
    }

    /**
     * Creates a new physical connection for the given host:port info. If the this connection's connection URL knows about this host:port then its host info is
     * used, otherwise a new host info based on current connection URL defaults is spawned.
     * 
     * @param hostPortPair
     *            The host:port pair identifying the host to connect to.
     * @return
     *         The new Connection instance.
     * @throws SQLException
     *             if an error occurs
     */
    public ConnectionImpl createConnectionForHost(String hostPortPair) throws SQLException {
        this.getObjectLock().lock();
        try {
            for (HostInfo hi : this.hostsList) {
                if (hi.getHostPortPair().equals(hostPortPair)) {
                    return createConnectionForHost(hi);
                }
            }
            return null;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Closes all live connections.
     */
    private void closeAllConnections() {
        this.getObjectLock().lock();
        try {
            // close all underlying connections
            for (Connection c : this.liveConnections.values()) {
                try {
                    c.close();
                } catch (SQLException e) {
                }
            }

            if (!this.isClosed) {
                if (this.connectionGroup != null) {
                    this.connectionGroup.closeConnectionProxy(this);
                }
            }

            this.liveConnections.clear();
            this.connectionsToHostsMap.clear();
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Closes all live connections.
     */
    @Override
    void doClose() {
        this.getObjectLock().lock();
        try {
            closeAllConnections();
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Aborts all live connections
     */
    @Override
    void doAbortInternal() {
        this.getObjectLock().lock();
        try {
            // abort all underlying connections
            for (JdbcConnection c : this.liveConnections.values()) {
                try {
                    c.abortInternal();
                } catch (SQLException e) {
                }
            }

            if (!this.isClosed) {
                if (this.connectionGroup != null) {
                    this.connectionGroup.closeConnectionProxy(this);
                }
            }

            this.liveConnections.clear();
            this.connectionsToHostsMap.clear();
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Aborts all live connections, using the provided Executor.
     */
    @Override
    void doAbort(Executor executor) {
        this.getObjectLock().lock();
        try {
            // close all underlying connections
            for (Connection c : this.liveConnections.values()) {
                try {
                    c.abort(executor);
                } catch (SQLException e) {
                }
            }

            if (!this.isClosed) {
                if (this.connectionGroup != null) {
                    this.connectionGroup.closeConnectionProxy(this);
                }
            }

            this.liveConnections.clear();
            this.connectionsToHostsMap.clear();
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Proxies method invocation on the java.sql.Connection interface, trapping "close", "isClosed" and "commit/rollback" to switch connections for load
     * balancing.
     * This is the continuation of MultiHostConnectionProxy#invoke(Object, Method, Object[]).
     */
    @Override
    Object invokeMore(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();

        if (this.isClosed && !allowedOnClosedConnection(method) && method.getExceptionTypes().length > 0) { // TODO remove method.getExceptionTypes().length ?
            if (this.autoReconnect && !this.closedExplicitly) {
                // try to reconnect first!
                this.currentConnection = null;
                pickNewConnection();
                this.isClosed = false;
                this.closedReason = null;
            } else {
                String reason = "No operations allowed after connection closed.";
                if (this.closedReason != null) {
                    reason += " " + this.closedReason;
                }

                for (Class<?> excls : method.getExceptionTypes()) {
                    if (SQLException.class.isAssignableFrom(excls)) {
                        throw SQLError.createSQLException(reason, MysqlErrorNumbers.SQL_STATE_CONNECTION_NOT_OPEN,
                                null /* no access to an interceptor here... */);
                    }
                }
                throw ExceptionFactory.createException(CJCommunicationsException.class, reason);
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
                if (result instanceof com.mysql.cj.jdbc.JdbcStatement) {
                    ((com.mysql.cj.jdbc.JdbcStatement) result).setPingTarget(this);
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
     * 
     * @throws SQLException
     *             if an error occurs
     */
    @Override
    public void doPing() throws SQLException {
        this.getObjectLock().lock();
        try {
            SQLException se = null;
            boolean foundHost = false;
            int pingTimeout = this.currentConnection.getPropertySet().getIntegerProperty(PropertyKey.loadBalancePingTimeout).getValue();

            this.getObjectLock().lock();
            try {
                for (HostInfo hi : this.hostsList) {
                    String host = hi.getHostPortPair();
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
                        // give up if it is the current connection, otherwise NPE faking resultset later.
                        if (host.equals(this.connectionsToHostsMap.get(this.currentConnection))) {
                            // clean up underlying connections, since connection pool won't do it
                            closeAllConnections();
                            this.isClosed = true;
                            this.closedReason = "Connection closed because ping of current connection failed.";
                            throw e;
                        }

                        // if the Exception is caused by ping connection lifetime checks, don't add to blocklist
                        if (e.getMessage().equals(Messages.getString("Connection.exceededConnectionLifetime"))) {
                            // only set the return Exception if it's null
                            if (se == null) {
                                se = e;
                            }
                        } else {
                            // overwrite the return Exception no matter what
                            se = e;
                            if (isGlobalBlocklistEnabled()) {
                                addToGlobalBlocklist(host);
                            }
                        }
                        // take the connection out of the liveConnections Map
                        this.liveConnections.remove(this.connectionsToHostsMap.get(conn));
                    }
                }
            } finally {
                this.getObjectLock().unlock();
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
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Adds a host to the blocklist with the given timeout.
     * 
     * @param host
     *            The host to be blocklisted.
     * @param timeout
     *            The blocklist timeout for this entry.
     */
    public void addToGlobalBlocklist(String host, long timeout) {
        if (isGlobalBlocklistEnabled()) {
            synchronized (globalBlocklist) {
                globalBlocklist.put(host, timeout);
            }
        }
    }

    /**
     * Removes a host from the blocklist.
     *
     * @param host
     *            The host to be removed from the blocklist.
     */
    public void removeFromGlobalBlocklist(String host) {
        if (isGlobalBlocklistEnabled() && globalBlocklist.containsKey(host)) {
            synchronized (globalBlocklist) {
                globalBlocklist.remove(host);
            }
        }
    }

    /**
     * Use {@link #removeFromGlobalBlocklist(String)} instead.
     * 
     * @param host
     *            host
     * @deprecated
     */
    @Deprecated
    public void removeFromGlobalBlacklist(String host) {
        this.removeFromGlobalBlocklist(host);
    }

    /**
     * Use {@link #addToGlobalBlocklist(String, long)} instead.
     * 
     * @param host
     *            The host to be blocklisted.
     * @param timeout
     *            The blocklist timeout for this entry.
     * @deprecated
     */
    @Deprecated
    public void addToGlobalBlacklist(String host, long timeout) {
        this.addToGlobalBlocklist(host, timeout);
    }

    /**
     * Adds a host to the blocklist.
     * 
     * @param host
     *            The host to be blocklisted.
     */
    public void addToGlobalBlocklist(String host) {
        addToGlobalBlocklist(host, System.currentTimeMillis() + this.globalBlocklistTimeout);
    }

    /**
     * Use {@link #addToGlobalBlocklist(String)} instead.
     * 
     * @param host
     *            The host to be blocklisted.
     * @deprecated
     */
    @Deprecated
    public void addToGlobalBlacklist(String host) {
        this.addToGlobalBlocklist(host);
    }

    /**
     * Checks if host blocklist management was enabled.
     * 
     * @return true if host blocklist management was enabled
     */
    public boolean isGlobalBlocklistEnabled() {
        return (this.globalBlocklistTimeout > 0);
    }

    /**
     * Use {@link #isGlobalBlocklistEnabled()} instead.
     * 
     * @return true if host blocklist management was enabled
     * @deprecated
     */
    @Deprecated
    public boolean isGlobalBlacklistEnabled() {
        return this.isGlobalBlocklistEnabled();
    }

    /**
     * Returns a local hosts blocklist, while cleaning up expired records from the global blocklist, or a blocklist with the hosts to be removed.
     * 
     * @return
     *         A local hosts blocklist.
     */
    public Map<String, Long> getGlobalBlocklist() {
        this.getObjectLock().lock();
        try {
            if (!isGlobalBlocklistEnabled()) {
                if (this.hostsToRemove.isEmpty()) {
                    return new HashMap<>(1);
                }
                HashMap<String, Long> fakedBlocklist = new HashMap<>();
                for (String h : this.hostsToRemove) {
                    fakedBlocklist.put(h, System.currentTimeMillis() + 5000);
                }
                return fakedBlocklist;
            }

            // Make a local copy of the blocklist
            Map<String, Long> blocklistClone = new HashMap<>(globalBlocklist.size());
            // Copy everything from synchronized global blocklist to local copy for manipulation
            synchronized (globalBlocklist) {
                blocklistClone.putAll(globalBlocklist);
            }
            Set<String> keys = blocklistClone.keySet();

            // We're only interested in blocklisted hosts that are in the hostList
            keys.retainAll(this.hostsList.stream().map(hi -> hi.getHostPortPair()).collect(Collectors.toList()));

            // Don't need to synchronize here as we using a local copy
            for (Iterator<String> i = keys.iterator(); i.hasNext();) {
                String host = i.next();
                // OK if null is returned because another thread already purged Map entry.
                Long timeout = globalBlocklist.get(host);
                if (timeout != null && timeout < System.currentTimeMillis()) {
                    // Timeout has expired, remove from blocklist
                    synchronized (globalBlocklist) {
                        globalBlocklist.remove(host);
                    }
                    i.remove();
                }

            }
            if (keys.size() == this.hostsList.size()) {
                // return an empty blocklist, let the BalanceStrategy implementations try to connect to everything since it appears that all hosts are
                // unavailable - we don't want to wait for loadBalanceBlocklistTimeout to expire.
                return new HashMap<>(1);
            }

            return blocklistClone;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Use {@link #getGlobalBlocklist()} instead.
     * 
     * @return
     *         A local hosts blocklist.
     * @deprecated
     */
    @Deprecated
    public Map<String, Long> getGlobalBlacklist() {
        this.getObjectLock().lock();
        try {
            return getGlobalBlocklist();
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Removes a host from the host list, allowing it some time to be released gracefully if needed.
     * 
     * @param hostPortPair
     *            The host to be removed.
     * @throws SQLException
     *             if an error occurs
     */
    public void removeHostWhenNotInUse(String hostPortPair) throws SQLException {
        if (this.hostRemovalGracePeriod <= 0) {
            removeHost(hostPortPair);
            return;
        }

        int timeBetweenChecks = this.hostRemovalGracePeriod > 1000 ? 1000 : this.hostRemovalGracePeriod;

        this.getObjectLock().lock();
        try {
            addToGlobalBlocklist(hostPortPair, System.currentTimeMillis() + this.hostRemovalGracePeriod + timeBetweenChecks);

            long cur = System.currentTimeMillis();

            while (System.currentTimeMillis() < cur + this.hostRemovalGracePeriod) {
                this.hostsToRemove.add(hostPortPair);

                if (!hostPortPair.equals(this.currentConnection.getHostPortPair())) {
                    removeHost(hostPortPair);
                    return;
                }

                try {
                    Thread.sleep(timeBetweenChecks);
                } catch (InterruptedException e) {
                    // better to swallow this and retry.
                }
            }
        } finally {
            this.getObjectLock().unlock();
        }

        removeHost(hostPortPair);
    }

    /**
     * Removes a host from the host list.
     * 
     * @param hostPortPair
     *            The host to be removed.
     * @throws SQLException
     *             if an error occurs
     */
    public void removeHost(String hostPortPair) throws SQLException {
        this.getObjectLock().lock();
        try {
            if (this.connectionGroup != null) {
                if (this.connectionGroup.getInitialHosts().size() == 1 && this.connectionGroup.getInitialHosts().contains(hostPortPair)) {
                    throw SQLError.createSQLException(Messages.getString("LoadBalancedConnectionProxy.0"), null);
                }
            }

            this.hostsToRemove.add(hostPortPair);

            this.connectionsToHostsMap.remove(this.liveConnections.remove(hostPortPair));
            if (this.hostsToListIndexMap.remove(hostPortPair) != null) {
                long[] newResponseTimes = new long[this.responseTimes.length - 1];
                int newIdx = 0;
                for (HostInfo hostInfo : this.hostsList) {
                    String host = hostInfo.getHostPortPair();
                    if (!this.hostsToRemove.contains(host)) {
                        Integer idx = this.hostsToListIndexMap.get(host);
                        if (idx != null && idx < this.responseTimes.length) {
                            newResponseTimes[newIdx] = this.responseTimes[idx];
                        }
                        this.hostsToListIndexMap.put(host, newIdx++);
                    }
                }
                this.responseTimes = newResponseTimes;
            }

            if (hostPortPair.equals(this.currentConnection.getHostPortPair())) {
                invalidateConnection(this.currentConnection);
                pickNewConnection();
            }
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * Adds a host to the hosts list.
     * 
     * @param hostPortPair
     *            The host to be added.
     * @return true if host was added and false if the host list already contains it
     */
    public boolean addHost(String hostPortPair) {
        this.getObjectLock().lock();
        try {
            if (this.hostsToListIndexMap.containsKey(hostPortPair)) {
                return false;
            }

            long[] newResponseTimes = new long[this.responseTimes.length + 1];
            System.arraycopy(this.responseTimes, 0, newResponseTimes, 0, this.responseTimes.length);

            this.responseTimes = newResponseTimes;
            if (this.hostsList.stream().noneMatch(hi -> hostPortPair.equals(hi.getHostPortPair()))) {
                this.hostsList.add(this.connectionUrl.getHostOrSpawnIsolated(hostPortPair));
            }
            this.hostsToListIndexMap.put(hostPortPair, this.responseTimes.length - 1);
            this.hostsToRemove.remove(hostPortPair);

            return true;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    public boolean inTransaction() {
        this.getObjectLock().lock();
        try {
            return this.inTransaction;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    public long getTransactionCount() {
        this.getObjectLock().lock();
        try {
            return this.transactionCount;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    public long getActivePhysicalConnectionCount() {
        this.getObjectLock().lock();
        try {
            return this.liveConnections.size();
        } finally {
            this.getObjectLock().unlock();
        }
    }

    public long getTotalPhysicalConnectionCount() {
        this.getObjectLock().lock();
        try {
            return this.totalPhysicalConnections;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    public long getConnectionGroupProxyID() {
        this.getObjectLock().lock();
        try {
            return this.connectionGroupProxyID;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    public String getCurrentActiveHost() {
        this.getObjectLock().lock();
        try {
            JdbcConnection c = this.currentConnection;
            if (c != null) {
                Object o = this.connectionsToHostsMap.get(c);
                if (o != null) {
                    return o.toString();
                }
            }
            return null;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    public long getCurrentTransactionDuration() {
        this.getObjectLock().lock();
        try {
            if (this.inTransaction && this.transactionStartTime > 0) {
                return System.nanoTime() - this.transactionStartTime;
            }
            return 0;
        } finally {
            this.getObjectLock().unlock();
        }
    }

    /**
     * A LoadBalancedConnection proxy that provides null-functionality. It can be used as a replacement of the <b>null</b> keyword in the places where a
     * LoadBalancedConnection object cannot be effectively <b>null</b> because that would be a potential source of NPEs.
     */
    private static class NullLoadBalancedConnectionProxy implements InvocationHandler {
        public NullLoadBalancedConnectionProxy() {
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            SQLException exceptionToThrow = SQLError.createSQLException(Messages.getString("LoadBalancedConnectionProxy.unusableConnection"),
                    MysqlErrorNumbers.SQL_STATE_INVALID_TRANSACTION_STATE, MysqlErrorNumbers.ERROR_CODE_NULL_LOAD_BALANCED_CONNECTION, true, null);
            Class<?>[] declaredException = method.getExceptionTypes();
            for (Class<?> declEx : declaredException) {
                if (declEx.isAssignableFrom(exceptionToThrow.getClass())) {
                    throw exceptionToThrow;
                }
            }
            throw new IllegalStateException(exceptionToThrow.getMessage(), exceptionToThrow);
        }
    }

    private static LoadBalancedConnection nullLBConnectionInstance = null;

    static LoadBalancedConnection getNullLoadBalancedConnectionInstance() {
        OBJECT_LOCK.lock();
        try {
            if (nullLBConnectionInstance == null) {
                nullLBConnectionInstance = (LoadBalancedConnection) java.lang.reflect.Proxy.newProxyInstance(LoadBalancedConnection.class.getClassLoader(),
                        INTERFACES_TO_PROXY, new NullLoadBalancedConnectionProxy());
            }
            return nullLBConnectionInstance;
        } finally {
            OBJECT_LOCK.unlock();
        }
    }
}
