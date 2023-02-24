/*
 * Copyright (c) 2002, 2023, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc;

import java.io.Serializable;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLPermission;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Stack;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import com.mysql.cj.CacheAdapter;
import com.mysql.cj.CacheAdapterFactory;
import com.mysql.cj.LicenseConfiguration;
import com.mysql.cj.Messages;
import com.mysql.cj.NativeSession;
import com.mysql.cj.NoSubInterceptorWrapper;
import com.mysql.cj.PreparedQuery;
import com.mysql.cj.QueryInfo;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.Session.SessionEventListener;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.PropertyDefinitions.DatabaseTerm;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.conf.RuntimeProperty;
import com.mysql.cj.exceptions.CJCommunicationsException;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.ExceptionInterceptorChain;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.PasswordExpiredException;
import com.mysql.cj.exceptions.UnableToConnectException;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.ha.MultiHostMySQLConnection;
import com.mysql.cj.jdbc.interceptors.ConnectionLifecycleInterceptor;
import com.mysql.cj.jdbc.result.CachedResultSetMetaData;
import com.mysql.cj.jdbc.result.CachedResultSetMetaDataImpl;
import com.mysql.cj.jdbc.result.ResultSetFactory;
import com.mysql.cj.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.jdbc.result.UpdatableResultSet;
import com.mysql.cj.log.ProfilerEvent;
import com.mysql.cj.log.StandardLogger;
import com.mysql.cj.protocol.ServerSessionStateController;
import com.mysql.cj.protocol.SocksProxySocketFactory;
import com.mysql.cj.protocol.a.NativeProtocol;
import com.mysql.cj.util.LRUCache;
import com.mysql.cj.util.StringUtils;
import com.mysql.cj.util.Util;

/**
 * A Connection represents a session with a specific database. Within the context of a Connection, SQL statements are executed and results are returned.
 * 
 * <P>
 * A Connection's database is able to provide information describing its tables, its supported SQL grammar, its stored procedures, the capabilities of this
 * connection, etc. This information is obtained with the getMetaData method.
 * </p>
 */
public class ConnectionImpl implements JdbcConnection, SessionEventListener, Serializable {

    private static final long serialVersionUID = 4009476458425101761L;

    private static final SQLPermission SET_NETWORK_TIMEOUT_PERM = new SQLPermission("setNetworkTimeout");

    private static final SQLPermission ABORT_PERM = new SQLPermission("abort");

    @Override
    public String getHost() {
        return this.session.getHostInfo().getHost();
    }

    private JdbcConnection parentProxy = null;
    private JdbcConnection topProxy = null;
    private InvocationHandler realProxy = null;

    @Override
    public boolean isProxySet() {
        return this.topProxy != null;
    }

    @Override
    public void setProxy(JdbcConnection proxy) {
        if (this.parentProxy == null) { // Only set this once.
            this.parentProxy = proxy;
        }
        this.topProxy = proxy;
        this.realProxy = this.topProxy instanceof MultiHostMySQLConnection ? ((MultiHostMySQLConnection) proxy).getThisAsProxy() : null;
    }

    // this connection has to be proxied when using multi-host settings so that statements get routed to the right physical connection
    // (works as "logical" connection)
    private JdbcConnection getProxy() {
        return (this.topProxy != null) ? this.topProxy : (JdbcConnection) this;
    }

    @Override
    public JdbcConnection getMultiHostSafeProxy() {
        return this.getProxy();
    }

    @Override
    public JdbcConnection getMultiHostParentProxy() {
        return this.parentProxy;
    }

    @Override
    public JdbcConnection getActiveMySQLConnection() {
        return this;
    }

    @Override
    public Object getConnectionMutex() {
        return (this.realProxy != null) ? this.realProxy : getProxy();
    }

    /**
     * Used as a key for caching callable/prepared statements which (may) depend on current database.
     */
    static class CompoundCacheKey {
        final String componentOne;
        final String componentTwo;
        final int hashCode;

        CompoundCacheKey(String partOne, String partTwo) {
            this.componentOne = partOne;
            this.componentTwo = partTwo;

            int hc = 17;
            hc = 31 * hc + (this.componentOne != null ? this.componentOne.hashCode() : 0);
            hc = 31 * hc + (this.componentTwo != null ? this.componentTwo.hashCode() : 0);
            this.hashCode = hc;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj != null && CompoundCacheKey.class.isAssignableFrom(obj.getClass())) {
                CompoundCacheKey another = (CompoundCacheKey) obj;
                if (this.componentOne == null ? another.componentOne == null : this.componentOne.equals(another.componentOne)) {
                    return this.componentTwo == null ? another.componentTwo == null : this.componentTwo.equals(another.componentTwo);
                }
            }
            return false;
        }

        @Override
        public int hashCode() {
            return this.hashCode;
        }
    }

    /** Default logger class name */
    protected static final String DEFAULT_LOGGER_CLASS = StandardLogger.class.getName();

    /**
     * Map mysql transaction isolation level name to
     * java.sql.Connection.TRANSACTION_XXX
     */
    private static Map<String, Integer> mapTransIsolationNameToValue = null;
    static {
        mapTransIsolationNameToValue = new HashMap<>(8);
        mapTransIsolationNameToValue.put("READ-UNCOMMITED", TRANSACTION_READ_UNCOMMITTED);
        mapTransIsolationNameToValue.put("READ-UNCOMMITTED", TRANSACTION_READ_UNCOMMITTED);
        mapTransIsolationNameToValue.put("READ-COMMITTED", TRANSACTION_READ_COMMITTED);
        mapTransIsolationNameToValue.put("REPEATABLE-READ", TRANSACTION_REPEATABLE_READ);
        mapTransIsolationNameToValue.put("SERIALIZABLE", TRANSACTION_SERIALIZABLE);
    }

    protected static Map<?, ?> roundRobinStatsMap;

    private List<ConnectionLifecycleInterceptor> connectionLifecycleInterceptors;

    private static final int DEFAULT_RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;

    private static final int DEFAULT_RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;

    /**
     * Creates a connection instance.
     * 
     * @param hostInfo
     *            {@link HostInfo} instance
     * @return new {@link ConnectionImpl} instance
     * @throws SQLException
     *             if a database access error occurs
     */
    public static JdbcConnection getInstance(HostInfo hostInfo) throws SQLException {
        return new ConnectionImpl(hostInfo);
    }

    private static final Random random = new Random();

    /**
     * @param url
     *            connection URL
     * @param hostList
     *            hosts list
     * @return index in a host list
     */
    protected static synchronized int getNextRoundRobinHostIndex(String url, List<?> hostList) {
        // we really do "random" here, because you don't get even distribution when this is coupled with connection pools

        int indexRange = hostList.size();

        int index = random.nextInt(indexRange);

        return index;
    }

    private static boolean nullSafeCompare(String s1, String s2) {
        if (s1 == null && s2 == null) {
            return true;
        }

        if (s1 == null && s2 != null) {
            return false;
        }

        return s1 != null && s1.equals(s2);
    }

    /** A cache of SQL to parsed prepared statement parameters. */
    private CacheAdapter<String, QueryInfo> queryInfoCache;

    /** The database we're currently using. */
    private String database = null;

    /** Internal DBMD to use for various database-version specific features */
    private DatabaseMetaData dbmd = null;

    private NativeSession session = null;

    /** Is this connection associated with a global tx? */
    private boolean isInGlobalTx = false;

    /** isolation level */
    private int isolationLevel = java.sql.Connection.TRANSACTION_READ_COMMITTED;

    /**
     * An array of currently open statements.
     * Copy-on-write used here to avoid ConcurrentModificationException when statements unregister themselves while we iterate over the list.
     */
    private final CopyOnWriteArrayList<JdbcStatement> openStatements = new CopyOnWriteArrayList<>();

    private LRUCache<CompoundCacheKey, CallableStatement.CallableStatementParamInfo> parsedCallableStatementCache;

    /** The password we used */
    private String password = null;

    /** Properties for this connection specified by user */
    protected Properties props = null;

    /** Are we in read-only mode? */
    private boolean readOnly = false;

    /** Cache of ResultSet metadata */
    protected LRUCache<String, CachedResultSetMetaData> resultSetMetadataCache;

    /**
     * The type map for UDTs (not implemented, but used by some third-party
     * vendors, most notably IBM WebSphere)
     */
    private Map<String, Class<?>> typeMap;

    /** The user we're connected as */
    private String user = null;

    private LRUCache<String, Boolean> serverSideStatementCheckCache;
    private LRUCache<CompoundCacheKey, ServerPreparedStatement> serverSideStatementCache;

    private HostInfo origHostInfo;

    private String origHostToConnectTo;

    // we don't want to be able to publicly clone this...

    private int origPortToConnectTo;

    /*
     * For testing failover scenarios
     */
    private boolean hasTriedSourceFlag = false;

    private List<QueryInterceptor> queryInterceptors;

    protected JdbcPropertySet propertySet;

    private RuntimeProperty<Boolean> autoReconnectForPools;
    private RuntimeProperty<Boolean> cachePrepStmts;
    private RuntimeProperty<Boolean> autoReconnect;
    private RuntimeProperty<Boolean> useUsageAdvisor;
    private RuntimeProperty<Boolean> reconnectAtTxEnd;
    private RuntimeProperty<Boolean> emulateUnsupportedPstmts;
    private RuntimeProperty<Boolean> ignoreNonTxTables;
    private RuntimeProperty<Boolean> pedantic;
    private RuntimeProperty<Integer> prepStmtCacheSqlLimit;
    private RuntimeProperty<Boolean> useLocalSessionState;
    private RuntimeProperty<Boolean> useServerPrepStmts;
    private RuntimeProperty<Boolean> processEscapeCodesForPrepStmts;
    private RuntimeProperty<Boolean> useLocalTransactionState;
    private RuntimeProperty<Boolean> disconnectOnExpiredPasswords;
    private RuntimeProperty<Boolean> readOnlyPropagatesToServer;

    protected ResultSetFactory nullStatementResultSetFactory;

    /**
     * '
     * For the delegate only
     */
    protected ConnectionImpl() {
    }

    /**
     * Creates a connection to a MySQL Server.
     * 
     * @param hostInfo
     *            the {@link HostInfo} instance that contains the host, user and connections attributes for this connection
     * @exception SQLException
     *                if a database access error occurs
     */
    public ConnectionImpl(HostInfo hostInfo) throws SQLException {

        try {
            // Stash away for later, used to clone this connection for Statement.cancel and Statement.setQueryTimeout().
            this.origHostInfo = hostInfo;
            this.origHostToConnectTo = hostInfo.getHost();
            this.origPortToConnectTo = hostInfo.getPort();

            this.database = hostInfo.getDatabase();
            this.user = hostInfo.getUser();
            this.password = hostInfo.getPassword();

            this.props = hostInfo.exposeAsProperties();

            this.propertySet = new JdbcPropertySetImpl();

            this.propertySet.initializeProperties(this.props);

            // We need Session ASAP to get access to central driver functionality
            this.nullStatementResultSetFactory = new ResultSetFactory(this, null);
            this.session = new NativeSession(hostInfo, this.propertySet);
            this.session.addListener(this); // listen for session status changes

            // we can't cache fixed values here because properties are still not initialized with user provided values
            this.autoReconnectForPools = this.propertySet.getBooleanProperty(PropertyKey.autoReconnectForPools);
            this.cachePrepStmts = this.propertySet.getBooleanProperty(PropertyKey.cachePrepStmts);
            this.autoReconnect = this.propertySet.getBooleanProperty(PropertyKey.autoReconnect);
            this.useUsageAdvisor = this.propertySet.getBooleanProperty(PropertyKey.useUsageAdvisor);
            this.reconnectAtTxEnd = this.propertySet.getBooleanProperty(PropertyKey.reconnectAtTxEnd);
            this.emulateUnsupportedPstmts = this.propertySet.getBooleanProperty(PropertyKey.emulateUnsupportedPstmts);
            this.ignoreNonTxTables = this.propertySet.getBooleanProperty(PropertyKey.ignoreNonTxTables);
            this.pedantic = this.propertySet.getBooleanProperty(PropertyKey.pedantic);
            this.prepStmtCacheSqlLimit = this.propertySet.getIntegerProperty(PropertyKey.prepStmtCacheSqlLimit);
            this.useLocalSessionState = this.propertySet.getBooleanProperty(PropertyKey.useLocalSessionState);
            this.useServerPrepStmts = this.propertySet.getBooleanProperty(PropertyKey.useServerPrepStmts);
            this.processEscapeCodesForPrepStmts = this.propertySet.getBooleanProperty(PropertyKey.processEscapeCodesForPrepStmts);
            this.useLocalTransactionState = this.propertySet.getBooleanProperty(PropertyKey.useLocalTransactionState);
            this.disconnectOnExpiredPasswords = this.propertySet.getBooleanProperty(PropertyKey.disconnectOnExpiredPasswords);
            this.readOnlyPropagatesToServer = this.propertySet.getBooleanProperty(PropertyKey.readOnlyPropagatesToServer);

            String exceptionInterceptorClasses = this.propertySet.getStringProperty(PropertyKey.exceptionInterceptors).getStringValue();
            if (exceptionInterceptorClasses != null && !"".equals(exceptionInterceptorClasses)) {
                this.exceptionInterceptor = new ExceptionInterceptorChain(exceptionInterceptorClasses, this.props, this.session.getLog());
            }

            if (this.cachePrepStmts.getValue()) {
                createPreparedStatementCaches();
            }

            if (this.propertySet.getBooleanProperty(PropertyKey.cacheCallableStmts).getValue()) {
                this.parsedCallableStatementCache = new LRUCache<>(this.propertySet.getIntegerProperty(PropertyKey.callableStmtCacheSize).getValue());
            }

            if (this.propertySet.getBooleanProperty(PropertyKey.allowMultiQueries).getValue()) {
                this.propertySet.getProperty(PropertyKey.cacheResultSetMetadata).setValue(false); // we don't handle this yet
            }

            if (this.propertySet.getBooleanProperty(PropertyKey.cacheResultSetMetadata).getValue()) {
                this.resultSetMetadataCache = new LRUCache<>(this.propertySet.getIntegerProperty(PropertyKey.metadataCacheSize).getValue());
            }

            if (this.propertySet.getStringProperty(PropertyKey.socksProxyHost).getStringValue() != null) {
                this.propertySet.getProperty(PropertyKey.socketFactory).setValue(SocksProxySocketFactory.class.getName());
            }

            this.dbmd = getMetaData(false, false);

            initializeSafeQueryInterceptors();

        } catch (CJException e) {
            throw SQLExceptionsMapping.translateException(e, getExceptionInterceptor());
        }

        try {
            createNewIO(false);

            unSafeQueryInterceptors();

            AbandonedConnectionCleanupThread.trackConnection(this, this.getSession().getNetworkResources());
        } catch (SQLException ex) {
            cleanup(ex);

            // don't clobber SQL exceptions
            throw ex;
        } catch (Exception ex) {
            cleanup(ex);

            throw SQLError
                    .createSQLException(
                            this.propertySet.getBooleanProperty(PropertyKey.paranoid).getValue() ? Messages.getString("Connection.0")
                                    : Messages.getString("Connection.1",
                                            new Object[] { this.session.getHostInfo().getHost(), this.session.getHostInfo().getPort() }),
                            MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE, ex, getExceptionInterceptor());
        }
    }

    @Override
    public JdbcPropertySet getPropertySet() {
        return this.propertySet;
    }

    @Override
    public void unSafeQueryInterceptors() throws SQLException {
        this.queryInterceptors = this.queryInterceptors.stream().map(NoSubInterceptorWrapper.class::cast).map(NoSubInterceptorWrapper::getUnderlyingInterceptor)
                .collect(Collectors.toCollection(LinkedList::new));
        if (this.session != null) {
            this.session.setQueryInterceptors(this.queryInterceptors);
        }
    }

    @Override
    public void initializeSafeQueryInterceptors() throws SQLException {
        this.queryInterceptors = Util
                .loadClasses(QueryInterceptor.class, this.propertySet.getStringProperty(PropertyKey.queryInterceptors).getStringValue(),
                        "MysqlIo.BadQueryInterceptor", getExceptionInterceptor())
                .stream().map(o -> new NoSubInterceptorWrapper(o.init(this, this.props, this.session.getLog())))
                .collect(Collectors.toCollection(LinkedList::new));
    }

    @Override
    public List<QueryInterceptor> getQueryInterceptorsInstances() {
        return this.queryInterceptors;
    }

    private boolean canHandleAsServerPreparedStatement(String sql) throws SQLException {
        if (sql == null || sql.length() == 0) {
            return true;
        }

        if (!this.useServerPrepStmts.getValue()) {
            return false;
        }

        boolean allowMultiQueries = this.propertySet.getBooleanProperty(PropertyKey.allowMultiQueries).getValue();

        if (this.cachePrepStmts.getValue()) {
            synchronized (this.serverSideStatementCheckCache) {
                Boolean flag = this.serverSideStatementCheckCache.get(sql);

                if (flag != null) {
                    return flag.booleanValue();
                }

                boolean canHandle = StringUtils.canHandleAsServerPreparedStatementNoCache(sql, getServerVersion(), allowMultiQueries,
                        this.session.getServerSession().isNoBackslashEscapesSet(), this.session.getServerSession().useAnsiQuotedIdentifiers());

                if (sql.length() < this.prepStmtCacheSqlLimit.getValue()) {
                    this.serverSideStatementCheckCache.put(sql, canHandle ? Boolean.TRUE : Boolean.FALSE);
                }

                return canHandle;
            }
        }

        return StringUtils.canHandleAsServerPreparedStatementNoCache(sql, getServerVersion(), allowMultiQueries,
                this.session.getServerSession().isNoBackslashEscapesSet(), this.session.getServerSession().useAnsiQuotedIdentifiers());
    }

    @Override
    public void changeUser(String userName, String newPassword) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            if ((userName == null) || userName.equals("")) {
                userName = "";
            }

            if (newPassword == null) {
                newPassword = "";
            }

            try {
                this.session.changeUser(userName, newPassword, this.database);
            } catch (CJException ex) {
                // After Bug#16241992 fix the server doesn't return to previous credentials if COM_CHANGE_USER attempt failed. 
                if ("28000".equals(ex.getSQLState())) {
                    cleanup(ex);
                }
                throw ex;
            }
            this.user = userName;
            this.password = newPassword;

            this.session.getServerSession().getCharsetSettings().configurePostHandshake(true);

            this.session.setSessionVariables();

            setupServerForTruncationChecks();
        }
    }

    @Override
    public void checkClosed() {
        this.session.checkClosed();
    }

    @Override
    public void throwConnectionClosedException() throws SQLException {
        SQLException ex = SQLError.createSQLException(Messages.getString("Connection.2"), MysqlErrorNumbers.SQL_STATE_CONNECTION_NOT_OPEN,
                getExceptionInterceptor());

        if (this.session.getForceClosedReason() != null) {
            ex.initCause(this.session.getForceClosedReason());
        }

        throw ex;
    }

    /**
     * Set transaction isolation level to the value received from server if any.
     * Is called by connectionInit(...)
     */
    private void checkTransactionIsolationLevel() {
        String s = this.session.getServerSession().getServerVariable("transaction_isolation");
        if (s == null) {
            s = this.session.getServerSession().getServerVariable("tx_isolation");
        }

        if (s != null) {
            Integer intTI = mapTransIsolationNameToValue.get(s);

            if (intTI != null) {
                this.isolationLevel = intTI.intValue();
            }
        }
    }

    @Override
    public void abortInternal() throws SQLException {
        this.session.forceClose();
    }

    @Override
    public void cleanup(Throwable whyCleanedUp) {
        try {
            if (this.session != null) {
                if (isClosed()) {
                    this.session.forceClose();
                } else {
                    realClose(false, false, false, whyCleanedUp);
                }
            }
        } catch (SQLException | CJException sqlEx) {
            // ignore, we're going away.
        }
    }

    @Deprecated
    @Override
    public void clearHasTriedMaster() {
        this.hasTriedSourceFlag = false;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // firstWarning = null;
    }

    @Override
    public java.sql.PreparedStatement clientPrepareStatement(String sql) throws SQLException {
        return clientPrepareStatement(sql, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    @Override
    public java.sql.PreparedStatement clientPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        java.sql.PreparedStatement pStmt = clientPrepareStatement(sql);

        ((ClientPreparedStatement) pStmt).setRetrieveGeneratedKeys(autoGenKeyIndex == java.sql.Statement.RETURN_GENERATED_KEYS);

        return pStmt;
    }

    @Override
    public java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return clientPrepareStatement(sql, resultSetType, resultSetConcurrency, true);
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, boolean processEscapeCodesIfNeeded)
            throws SQLException {

        String nativeSql = processEscapeCodesIfNeeded && this.processEscapeCodesForPrepStmts.getValue() ? nativeSQL(sql) : sql;

        ClientPreparedStatement pStmt = null;

        if (this.cachePrepStmts.getValue()) {
            QueryInfo pStmtInfo = this.queryInfoCache.get(nativeSql);

            if (pStmtInfo == null) {
                pStmt = ClientPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.database);

                this.queryInfoCache.put(nativeSql, pStmt.getQueryInfo());
            } else {
                pStmt = ClientPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.database, pStmtInfo);
            }
        } else {
            pStmt = ClientPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.database);
        }

        pStmt.setResultSetType(resultSetType);
        pStmt.setResultSetConcurrency(resultSetConcurrency);

        return pStmt;
    }

    @Override
    public java.sql.PreparedStatement clientPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {

        ClientPreparedStatement pStmt = (ClientPreparedStatement) clientPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyIndexes != null) && (autoGenKeyIndexes.length > 0));

        return pStmt;
    }

    @Override
    public java.sql.PreparedStatement clientPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        ClientPreparedStatement pStmt = (ClientPreparedStatement) clientPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyColNames != null) && (autoGenKeyColNames.length > 0));

        return pStmt;
    }

    @Override
    public java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return clientPrepareStatement(sql, resultSetType, resultSetConcurrency, true);
    }

    @Override
    public void close() throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.connectionLifecycleInterceptors != null) {
                for (ConnectionLifecycleInterceptor cli : this.connectionLifecycleInterceptors) {
                    cli.close();
                }
            }

            realClose(true, true, false, null);
        }
    }

    @Override
    public void normalClose() {
        try {
            close();
        } catch (SQLException e) {
            ExceptionFactory.createException(e.getMessage(), e);
        }
    }

    /**
     * Closes all currently open statements.
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    private void closeAllOpenStatements() throws SQLException {
        SQLException postponedException = null;

        for (JdbcStatement stmt : this.openStatements) {
            try {
                ((StatementImpl) stmt).realClose(false, true);
            } catch (SQLException sqlEx) {
                postponedException = sqlEx; // throw it later, cleanup all statements first
            }
        }

        if (postponedException != null) {
            throw postponedException;
        }
    }

    private void closeStatement(java.sql.Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException sqlEx) {
                // ignore
            }

            stmt = null;
        }
    }

    @Override
    public void commit() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            try {
                if (this.connectionLifecycleInterceptors != null) {
                    IterateBlock<ConnectionLifecycleInterceptor> iter = new IterateBlock<ConnectionLifecycleInterceptor>(
                            this.connectionLifecycleInterceptors.iterator()) {

                        @Override
                        void forEach(ConnectionLifecycleInterceptor each) throws SQLException {
                            if (!each.commit()) {
                                this.stopIterating = true;
                            }
                        }
                    };

                    iter.doForAll();

                    if (!iter.fullIteration()) {
                        return;
                    }
                }

                if (this.session.getServerSession().isAutoCommit()) {
                    throw SQLError.createSQLException(Messages.getString("Connection.3"), getExceptionInterceptor());
                }
                if (this.useLocalTransactionState.getValue()) {
                    if (!this.session.getServerSession().inTransactionOnServer()) {
                        return; // effectively a no-op
                    }
                }

                this.session.execSQL(null, "commit", -1, null, false, this.nullStatementResultSetFactory, null, false);
            } catch (SQLException sqlException) {
                if (MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlException.getSQLState())) {
                    throw SQLError.createSQLException(Messages.getString("Connection.4"), MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN,
                            getExceptionInterceptor());
                }

                throw sqlException;
            } finally {
                this.session.setNeedsPing(this.reconnectAtTxEnd.getValue());
            }
        }
        return;
    }

    @Override
    public void createNewIO(boolean isForReconnect) {
        synchronized (getConnectionMutex()) {
            // Synchronization Not needed for *new* connections, but definitely for connections going through fail-over, since we might get the new connection
            // up and running *enough* to start sending cached or still-open server-side prepared statements over to the backend before we get a chance to
            // re-prepare them...

            try {
                if (!this.autoReconnect.getValue()) {
                    connectOneTryOnly(isForReconnect);

                    return;
                }

                connectWithRetries(isForReconnect);
            } catch (SQLException ex) {
                throw ExceptionFactory.createException(UnableToConnectException.class, ex.getMessage(), ex);
            }
        }
    }

    private void connectWithRetries(boolean isForReconnect) throws SQLException {
        double timeout = this.propertySet.getIntegerProperty(PropertyKey.initialTimeout).getValue();
        boolean connectionGood = false;

        Exception connectionException = null;

        for (int attemptCount = 0; (attemptCount < this.propertySet.getIntegerProperty(PropertyKey.maxReconnects).getValue())
                && !connectionGood; attemptCount++) {
            try {
                this.session.forceClose();

                JdbcConnection c = getProxy();
                this.session.connect(this.origHostInfo, this.user, this.password, this.database, getLoginTimeout(), c);
                pingInternal(false, 0);

                boolean oldAutoCommit;
                int oldIsolationLevel;
                boolean oldReadOnly;
                String oldDb;

                synchronized (getConnectionMutex()) {
                    // save state from old connection
                    oldAutoCommit = getAutoCommit();
                    oldIsolationLevel = this.isolationLevel;
                    oldReadOnly = isReadOnly(false);
                    oldDb = getDatabase();

                    this.session.setQueryInterceptors(this.queryInterceptors);
                }

                // Server properties might be different from previous connection, so initialize again...
                initializePropsFromServer();

                if (isForReconnect) {
                    // Restore state from old connection
                    setAutoCommit(oldAutoCommit);
                    setTransactionIsolation(oldIsolationLevel);
                    setDatabase(oldDb);
                    setReadOnly(oldReadOnly);
                }

                connectionGood = true;

                break;
            } catch (UnableToConnectException rejEx) {
                close();
                this.session.getProtocol().getSocketConnection().forceClose();

            } catch (Exception e) {
                connectionException = e;
                connectionGood = false;
            }

            if (connectionGood) {
                break;
            }

            if (attemptCount > 0) {
                try {
                    Thread.sleep((long) timeout * 1000);
                } catch (InterruptedException IE) {
                    // ignore
                }
            }
        } // end attempts for a single host

        if (!connectionGood) {
            // We've really failed!
            SQLException chainedEx = SQLError.createSQLException(
                    Messages.getString("Connection.UnableToConnectWithRetries",
                            new Object[] { this.propertySet.getIntegerProperty(PropertyKey.maxReconnects).getValue() }),
                    MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, connectionException, getExceptionInterceptor());
            throw chainedEx;
        }

        if (this.propertySet.getBooleanProperty(PropertyKey.paranoid).getValue() && !this.autoReconnect.getValue()) {
            this.password = null;
            this.user = null;
        }

        if (isForReconnect) {
            //
            // Retrieve any 'lost' prepared statements if re-connecting
            //
            Iterator<JdbcStatement> statementIter = this.openStatements.iterator();

            //
            // We build a list of these outside the map of open statements, because in the process of re-preparing, we might end up having to close a prepared
            // statement, thus removing it from the map, and generating a ConcurrentModificationException
            //
            Stack<JdbcStatement> serverPreparedStatements = null;

            while (statementIter.hasNext()) {
                JdbcStatement statementObj = statementIter.next();

                if (statementObj instanceof ServerPreparedStatement) {
                    if (serverPreparedStatements == null) {
                        serverPreparedStatements = new Stack<>();
                    }

                    serverPreparedStatements.add(statementObj);
                }
            }

            if (serverPreparedStatements != null) {
                while (!serverPreparedStatements.isEmpty()) {
                    ((ServerPreparedStatement) serverPreparedStatements.pop()).rePrepare();
                }
            }
        }
    }

    private void connectOneTryOnly(boolean isForReconnect) throws SQLException {
        Exception connectionNotEstablishedBecause = null;

        try {

            JdbcConnection c = getProxy();
            this.session.connect(this.origHostInfo, this.user, this.password, this.database, getLoginTimeout(), c);

            // save state from old connection
            boolean oldAutoCommit = getAutoCommit();
            int oldIsolationLevel = this.isolationLevel;
            boolean oldReadOnly = isReadOnly(false);
            String oldDb = getDatabase();

            this.session.setQueryInterceptors(this.queryInterceptors);

            // Server properties might be different from previous connection, so initialize again...
            initializePropsFromServer();

            if (isForReconnect) {
                // Restore state from old connection
                setAutoCommit(oldAutoCommit);
                setTransactionIsolation(oldIsolationLevel);
                setDatabase(oldDb);
                setReadOnly(oldReadOnly);
            }
            return;

        } catch (UnableToConnectException rejEx) {
            close();
            NativeProtocol protocol = this.session.getProtocol();
            if (protocol != null) {
                protocol.getSocketConnection().forceClose();
            }
            throw rejEx;

        } catch (Exception e) {

            if ((e instanceof PasswordExpiredException
                    || e instanceof SQLException && ((SQLException) e).getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD)
                    && !this.disconnectOnExpiredPasswords.getValue()) {
                return;
            }

            if (this.session != null) {
                this.session.forceClose();
            }

            connectionNotEstablishedBecause = e;

            if (e instanceof SQLException) {
                throw (SQLException) e;
            }

            if (e.getCause() != null && e.getCause() instanceof SQLException) {
                throw (SQLException) e.getCause();
            }

            if (e instanceof CJException) {
                throw (CJException) e;
            }

            SQLException chainedEx = SQLError.createSQLException(Messages.getString("Connection.UnableToConnect"),
                    MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, getExceptionInterceptor());
            chainedEx.initCause(connectionNotEstablishedBecause);

            throw chainedEx;
        }
    }

    private int getLoginTimeout() {
        int loginTimeoutSecs = DriverManager.getLoginTimeout();
        if (loginTimeoutSecs <= 0) {
            return 0;
        }
        return loginTimeoutSecs * 1000;
    }

    private void createPreparedStatementCaches() throws SQLException {
        synchronized (getConnectionMutex()) {
            int cacheSize = this.propertySet.getIntegerProperty(PropertyKey.prepStmtCacheSize).getValue();
            String queryInfoCacheFactory = this.propertySet.getStringProperty(PropertyKey.queryInfoCacheFactory).getValue();

            @SuppressWarnings("unchecked")
            CacheAdapterFactory<String, QueryInfo> cacheFactory = Util.getInstance(CacheAdapterFactory.class, queryInfoCacheFactory, null, null,
                    getExceptionInterceptor());
            this.queryInfoCache = cacheFactory.getInstance(this, this.origHostInfo.getDatabaseUrl(), cacheSize, this.prepStmtCacheSqlLimit.getValue());

            if (this.useServerPrepStmts.getValue()) {
                this.serverSideStatementCheckCache = new LRUCache<>(cacheSize);
                this.serverSideStatementCache = new LRUCache<CompoundCacheKey, ServerPreparedStatement>(cacheSize) {
                    private static final long serialVersionUID = 7692318650375988114L;

                    @Override
                    protected boolean removeEldestEntry(java.util.Map.Entry<CompoundCacheKey, ServerPreparedStatement> eldest) {
                        if (this.maxElements <= 1) {
                            return false;
                        }
                        boolean removeIt = super.removeEldestEntry(eldest);
                        if (removeIt) {
                            ServerPreparedStatement ps = eldest.getValue();
                            ps.isCached = false;
                            ps.setClosed(false);
                            try {
                                ps.realClose(true, true);
                            } catch (SQLException sqlEx) {
                                // punt
                            }
                        }
                        return removeIt;
                    }
                };
            }
        }
    }

    @Override
    public java.sql.Statement createStatement() throws SQLException {
        return createStatement(DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {

        StatementImpl stmt = new StatementImpl(getMultiHostSafeProxy(), this.database);
        stmt.setResultSetType(resultSetType);
        stmt.setResultSetConcurrency(resultSetConcurrency);

        return stmt;
    }

    @Override
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (this.pedantic.getValue()) {
            if (resultSetHoldability != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                throw SQLError.createSQLException("HOLD_CUSRORS_OVER_COMMIT is only supported holdability level", MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        return createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public int getActiveStatementCount() {
        return this.openStatements.size();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        synchronized (getConnectionMutex()) {
            return this.session.getServerSession().isAutoCommit();
        }
    }

    @Override
    public String getCatalog() throws SQLException {
        synchronized (getConnectionMutex()) {
            return this.propertySet.<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA ? null : this.database;
        }
    }

    @Override
    public String getCharacterSetMetadata() {
        synchronized (getConnectionMutex()) {
            return this.session.getServerSession().getCharsetSettings().getMetadataEncoding();
        }
    }

    @Override
    public int getHoldability() throws SQLException {
        return java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public long getId() {
        return this.session.getThreadId();
    }

    /**
     * NOT JDBC-Compliant, but clients can use this method to determine how long
     * this connection has been idle. This time (reported in milliseconds) is
     * updated once a query has completed.
     * 
     * @return number of ms that this connection has been idle, 0 if the driver
     *         is busy retrieving results.
     */
    @Override
    public long getIdleFor() {
        synchronized (getConnectionMutex()) {
            return this.session.getIdleFor();
        }
    }

    @Override
    public java.sql.DatabaseMetaData getMetaData() throws SQLException {
        return getMetaData(true, true);
    }

    private java.sql.DatabaseMetaData getMetaData(boolean checkClosed, boolean checkForInfoSchema) throws SQLException {
        if (checkClosed) {
            checkClosed();
        }

        com.mysql.cj.jdbc.DatabaseMetaData dbmeta = com.mysql.cj.jdbc.DatabaseMetaData.getInstance(getMultiHostSafeProxy(), this.database, checkForInfoSchema,
                this.nullStatementResultSetFactory);

        if (getSession() != null && getSession().getProtocol() != null) {
            dbmeta.setMetadataEncoding(getSession().getServerSession().getCharsetSettings().getMetadataEncoding());
            dbmeta.setMetadataCollationIndex(getSession().getServerSession().getCharsetSettings().getMetadataCollationIndex());
        }

        return dbmeta;
    }

    @Override
    public java.sql.Statement getMetadataSafeStatement() throws SQLException {
        return getMetadataSafeStatement(0);
    }

    public java.sql.Statement getMetadataSafeStatement(int maxRows) throws SQLException {
        java.sql.Statement stmt = createStatement();

        stmt.setMaxRows(maxRows == -1 ? 0 : maxRows);

        stmt.setEscapeProcessing(false);

        if (stmt.getFetchSize() != 0) {
            stmt.setFetchSize(0);
        }

        return stmt;
    }

    @Override
    public ServerVersion getServerVersion() {
        return this.session.getServerSession().getServerVersion();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {

        synchronized (getConnectionMutex()) {
            if (!this.useLocalSessionState.getValue()) {
                String s = this.session.queryServerVariable(
                        versionMeetsMinimum(8, 0, 3) || (versionMeetsMinimum(5, 7, 20) && !versionMeetsMinimum(8, 0, 0)) ? "@@session.transaction_isolation"
                                : "@@session.tx_isolation");

                if (s != null) {
                    Integer intTI = mapTransIsolationNameToValue.get(s);
                    if (intTI != null) {
                        this.isolationLevel = intTI.intValue();
                        return this.isolationLevel;
                    }
                    throw SQLError.createSQLException(Messages.getString("Connection.12", new Object[] { s }), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                            getExceptionInterceptor());
                }
                throw SQLError.createSQLException(Messages.getString("Connection.13"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
            }

            return this.isolationLevel;
        }
    }

    @Override
    public java.util.Map<String, Class<?>> getTypeMap() throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.typeMap == null) {
                this.typeMap = new HashMap<>();
            }

            return this.typeMap;
        }
    }

    @Override
    public String getURL() {
        return this.origHostInfo.getDatabaseUrl();
    }

    @Override
    public String getUser() {
        return this.user;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public boolean hasSameProperties(JdbcConnection c) {
        return this.props.equals(c.getProperties());
    }

    @Override
    public Properties getProperties() {
        return this.props;
    }

    @Deprecated
    @Override
    public boolean hasTriedMaster() {
        return this.hasTriedSourceFlag;
    }

    /**
     * Sets varying properties that depend on server information. Called once we
     * have connected to the server.
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    private void initializePropsFromServer() throws SQLException {
        String connectionInterceptorClasses = this.propertySet.getStringProperty(PropertyKey.connectionLifecycleInterceptors).getStringValue();
        this.connectionLifecycleInterceptors = null;

        if (connectionInterceptorClasses != null) {
            try {
                this.connectionLifecycleInterceptors = Util
                        .loadClasses(ConnectionLifecycleInterceptor.class, connectionInterceptorClasses, "Connection.badLifecycleInterceptor",
                                getExceptionInterceptor())
                        .stream().map(i -> i.init(this, this.props, this.session.getLog())).collect(Collectors.toCollection(LinkedList::new));
            } catch (CJException e) {
                throw SQLExceptionsMapping.translateException(e, getExceptionInterceptor());
            }
        }

        this.session.setSessionVariables();

        this.session.loadServerVariables(this.getConnectionMutex(), this.dbmd.getDriverVersion());

        this.autoIncrementIncrement = this.session.getServerSession().getServerVariable("auto_increment_increment", 1);

        try {
            LicenseConfiguration.checkLicenseType(this.session.getServerSession().getServerVariables());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, getExceptionInterceptor());
        }

        this.session.getProtocol().initServerSession();

        checkTransactionIsolationLevel();

        handleAutoCommitDefaults();

        ((com.mysql.cj.jdbc.DatabaseMetaData) this.dbmd).setMetadataEncoding(this.session.getServerSession().getCharsetSettings().getMetadataEncoding());
        ((com.mysql.cj.jdbc.DatabaseMetaData) this.dbmd)
                .setMetadataCollationIndex(this.session.getServerSession().getCharsetSettings().getMetadataCollationIndex());

        //
        // Server can do this more efficiently for us
        //

        setupServerForTruncationChecks();
    }

    /**
     * Resets a default auto-commit value of 0 to 1, as required by JDBC specification.
     * Takes into account that the default auto-commit value of 0 may have been changed on the server via init_connect.
     * 
     * @throws SQLException
     *             if a database access error occurs
     */
    private void handleAutoCommitDefaults() throws SQLException {
        boolean resetAutoCommitDefault = false;

        // Server Bug#66884 (SERVER_STATUS is always initiated with SERVER_STATUS_AUTOCOMMIT=1) invalidates "elideSetAutoCommits" feature.
        // TODO Turn this feature back on as soon as the server bug is fixed. Consider making it version specific.
        // if (!getPropertySet().getBooleanReadableProperty(PropertyKey.elideSetAutoCommits).getValue()) {
        String initConnectValue = this.session.getServerSession().getServerVariable("init_connect");
        if (initConnectValue != null && initConnectValue.length() > 0) {
            // auto-commit might have changed

            String s = this.session.queryServerVariable("@@session.autocommit");
            if (s != null) {
                this.session.getServerSession().setAutoCommit(Boolean.parseBoolean(s));
                if (!this.session.getServerSession().isAutoCommit()) {
                    resetAutoCommitDefault = true;
                }
            }

        } else {
            // reset it anyway, the server may have been initialized with --autocommit=0
            resetAutoCommitDefault = true;
        }
        //} else if (getSession().isSetNeededForAutoCommitMode(true)) {
        //    // we're not in standard autocommit=true mode
        //    this.session.setAutoCommit(false);
        //    resetAutoCommitDefault = true;
        //}

        if (resetAutoCommitDefault) {
            try {
                setAutoCommit(true); // required by JDBC spec
            } catch (SQLException ex) {
                if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || this.disconnectOnExpiredPasswords.getValue()) {
                    throw ex;
                }
            }
        }
    }

    @Override
    public boolean isClosed() {
        return this.session.isClosed();
    }

    @Override
    public boolean isInGlobalTx() {
        return this.isInGlobalTx;
    }

    @Override
    public boolean isSourceConnection() {
        return false; // handled higher up
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return isReadOnly(true);
    }

    @Override
    public boolean isReadOnly(boolean useSessionStatus) throws SQLException {
        if (useSessionStatus && !this.session.isClosed() && versionMeetsMinimum(5, 6, 5) && !this.useLocalSessionState.getValue()
                && this.readOnlyPropagatesToServer.getValue()) {
            String s = this.session.queryServerVariable(
                    versionMeetsMinimum(8, 0, 3) || (versionMeetsMinimum(5, 7, 20) && !versionMeetsMinimum(8, 0, 0)) ? "@@session.transaction_read_only"
                            : "@@session.tx_read_only");
            if (s != null) {
                return Integer.parseInt(s) != 0; // mysql has a habit of tri+ state booleans
            }
        }

        return this.readOnly;
    }

    @Override
    public boolean isSameResource(JdbcConnection otherConnection) {
        synchronized (getConnectionMutex()) {
            if (otherConnection == null) {
                return false;
            }

            boolean directCompare = true;

            String otherHost = ((ConnectionImpl) otherConnection).origHostToConnectTo;
            String otherOrigDatabase = ((ConnectionImpl) otherConnection).origHostInfo.getDatabase();
            String otherCurrentDb = ((ConnectionImpl) otherConnection).database;

            if (!nullSafeCompare(otherHost, this.origHostToConnectTo)) {
                directCompare = false;
            } else if (otherHost != null && otherHost.indexOf(',') == -1 && otherHost.indexOf(':') == -1) {
                // need to check port numbers
                directCompare = (((ConnectionImpl) otherConnection).origPortToConnectTo == this.origPortToConnectTo);
            }

            if (directCompare) {
                if (!nullSafeCompare(otherOrigDatabase, this.origHostInfo.getDatabase()) || !nullSafeCompare(otherCurrentDb, this.database)) {
                    directCompare = false;
                }
            }

            if (directCompare) {
                return true;
            }

            // Has the user explicitly set a resourceId?
            String otherResourceId = ((ConnectionImpl) otherConnection).getPropertySet().getStringProperty(PropertyKey.resourceId).getValue();
            String myResourceId = this.propertySet.getStringProperty(PropertyKey.resourceId).getValue();

            if (otherResourceId != null || myResourceId != null) {
                directCompare = nullSafeCompare(otherResourceId, myResourceId);

                if (directCompare) {
                    return true;
                }
            }

            return false;
        }
    }

    private int autoIncrementIncrement = 0;

    @Override
    public int getAutoIncrementIncrement() {
        return this.autoIncrementIncrement;
    }

    @Override
    public boolean lowerCaseTableNames() {
        return this.session.getServerSession().isLowerCaseTableNames();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        if (sql == null) {
            return null;
        }

        Object escapedSqlResult = EscapeProcessor.escapeSQL(sql, getMultiHostSafeProxy().getSession().getServerSession().getSessionTimeZone(),
                getMultiHostSafeProxy().getSession().getServerSession().getCapabilities().serverSupportsFracSecs(),
                getMultiHostSafeProxy().getSession().getServerSession().isServerTruncatesFracSecs(), getExceptionInterceptor());

        if (escapedSqlResult instanceof String) {
            return (String) escapedSqlResult;
        }

        return ((EscapeProcessorResult) escapedSqlResult).escapedSql;
    }

    private CallableStatement parseCallableStatement(String sql) throws SQLException {
        Object escapedSqlResult = EscapeProcessor.escapeSQL(sql, getMultiHostSafeProxy().getSession().getServerSession().getSessionTimeZone(),
                getMultiHostSafeProxy().getSession().getServerSession().getCapabilities().serverSupportsFracSecs(),
                getMultiHostSafeProxy().getSession().getServerSession().isServerTruncatesFracSecs(), getExceptionInterceptor());

        boolean isFunctionCall = false;
        String parsedSql = null;

        if (escapedSqlResult instanceof EscapeProcessorResult) {
            parsedSql = ((EscapeProcessorResult) escapedSqlResult).escapedSql;
            isFunctionCall = ((EscapeProcessorResult) escapedSqlResult).callingStoredFunction;
        } else {
            parsedSql = (String) escapedSqlResult;
            isFunctionCall = false;
        }

        return CallableStatement.getInstance(getMultiHostSafeProxy(), parsedSql, this.database, isFunctionCall);
    }

    @Override
    public void ping() throws SQLException {
        pingInternal(true, 0);
    }

    @Override
    public void pingInternal(boolean checkForClosedConnection, int timeoutMillis) throws SQLException {
        this.session.ping(checkForClosedConnection, timeoutMillis);
    }

    @Override
    public java.sql.CallableStatement prepareCall(String sql) throws SQLException {

        return prepareCall(sql, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    @Override
    public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        CallableStatement cStmt = null;

        if (!this.propertySet.getBooleanProperty(PropertyKey.cacheCallableStmts).getValue()) {

            cStmt = parseCallableStatement(sql);
        } else {
            synchronized (this.parsedCallableStatementCache) {
                CompoundCacheKey key = new CompoundCacheKey(getDatabase(), sql);

                CallableStatement.CallableStatementParamInfo cachedParamInfo = this.parsedCallableStatementCache.get(key);

                if (cachedParamInfo != null) {
                    cStmt = CallableStatement.getInstance(getMultiHostSafeProxy(), cachedParamInfo);
                } else {
                    cStmt = parseCallableStatement(sql);

                    synchronized (cStmt) {
                        cachedParamInfo = cStmt.paramInfo;
                    }

                    this.parsedCallableStatementCache.put(key, cachedParamInfo);
                }
            }
        }

        cStmt.setResultSetType(resultSetType);
        cStmt.setResultSetConcurrency(resultSetConcurrency);

        return cStmt;
    }

    @Override
    public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (this.pedantic.getValue()) {
            if (resultSetHoldability != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                throw SQLError.createSQLException(Messages.getString("Connection.17"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        CallableStatement cStmt = (com.mysql.cj.jdbc.CallableStatement) prepareCall(sql, resultSetType, resultSetConcurrency);

        return cStmt;
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);

        ((ClientPreparedStatement) pStmt).setRetrieveGeneratedKeys(autoGenKeyIndex == java.sql.Statement.RETURN_GENERATED_KEYS);

        return pStmt;
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            //
            // FIXME: Create warnings if can't create results of the given type or concurrency
            //
            ClientPreparedStatement pStmt = null;

            boolean canServerPrepare = true;

            String nativeSql = this.processEscapeCodesForPrepStmts.getValue() ? nativeSQL(sql) : sql;

            if (this.useServerPrepStmts.getValue() && this.emulateUnsupportedPstmts.getValue()) {
                canServerPrepare = canHandleAsServerPreparedStatement(nativeSql);
            }

            if (this.useServerPrepStmts.getValue() && canServerPrepare) {
                if (this.cachePrepStmts.getValue()) {
                    synchronized (this.serverSideStatementCache) {
                        pStmt = this.serverSideStatementCache.remove(new CompoundCacheKey(this.database, sql));

                        if (pStmt != null) {
                            ((com.mysql.cj.jdbc.ServerPreparedStatement) pStmt).setClosed(false);
                            pStmt.clearParameters();
                            pStmt.setResultSetType(resultSetType);
                            pStmt.setResultSetConcurrency(resultSetConcurrency);
                        }

                        if (pStmt == null) {
                            try {
                                pStmt = ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.database, resultSetType,
                                        resultSetConcurrency);
                                if (sql.length() < this.prepStmtCacheSqlLimit.getValue()) {
                                    ((com.mysql.cj.jdbc.ServerPreparedStatement) pStmt).isCacheable = true;
                                }

                                pStmt.setResultSetType(resultSetType);
                                pStmt.setResultSetConcurrency(resultSetConcurrency);
                            } catch (SQLException sqlEx) {
                                // Punt, if necessary
                                if (this.emulateUnsupportedPstmts.getValue()) {
                                    pStmt = (ClientPreparedStatement) clientPrepareStatement(nativeSql, resultSetType, resultSetConcurrency, false);

                                    if (sql.length() < this.prepStmtCacheSqlLimit.getValue()) {
                                        this.serverSideStatementCheckCache.put(sql, Boolean.FALSE);
                                    }
                                } else {
                                    throw sqlEx;
                                }
                            }
                        }
                    }
                } else {
                    try {
                        pStmt = ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.database, resultSetType, resultSetConcurrency);

                        pStmt.setResultSetType(resultSetType);
                        pStmt.setResultSetConcurrency(resultSetConcurrency);
                    } catch (SQLException sqlEx) {
                        // Punt, if necessary
                        if (this.emulateUnsupportedPstmts.getValue()) {
                            pStmt = (ClientPreparedStatement) clientPrepareStatement(nativeSql, resultSetType, resultSetConcurrency, false);
                        } else {
                            throw sqlEx;
                        }
                    }
                }
            } else {
                pStmt = (ClientPreparedStatement) clientPrepareStatement(nativeSql, resultSetType, resultSetConcurrency, false);
            }

            return pStmt;
        }
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (this.pedantic.getValue()) {
            if (resultSetHoldability != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                throw SQLError.createSQLException(Messages.getString("Connection.17"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);

        ((ClientPreparedStatement) pStmt).setRetrieveGeneratedKeys((autoGenKeyIndexes != null) && (autoGenKeyIndexes.length > 0));

        return pStmt;
    }

    @Override
    public java.sql.PreparedStatement prepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);

        ((ClientPreparedStatement) pStmt).setRetrieveGeneratedKeys((autoGenKeyColNames != null) && (autoGenKeyColNames.length > 0));

        return pStmt;
    }

    @Override
    public void realClose(boolean calledExplicitly, boolean issueRollback, boolean skipLocalTeardown, Throwable reason) throws SQLException {
        SQLException sqlEx = null;

        if (this.isClosed()) {
            return;
        }

        this.session.setForceClosedReason(reason);

        try {
            if (!skipLocalTeardown) {
                if (!getAutoCommit() && issueRollback) {
                    try {
                        rollback();
                    } catch (SQLException ex) {
                        sqlEx = ex;
                    }
                }

                if (this.propertySet.getBooleanProperty(PropertyKey.gatherPerfMetrics).getValue()) {
                    this.session.getProtocol().getMetricsHolder().reportMetrics(this.session.getLog());
                }

                if (this.useUsageAdvisor.getValue()) {
                    if (!calledExplicitly) {
                        this.session.getProfilerEventHandler().processEvent(ProfilerEvent.TYPE_USAGE, this.session, null, null, 0, new Throwable(),
                                Messages.getString("Connection.18"));
                    }

                    if (System.currentTimeMillis() - this.session.getConnectionCreationTimeMillis() < 500) {
                        this.session.getProfilerEventHandler().processEvent(ProfilerEvent.TYPE_USAGE, this.session, null, null, 0, new Throwable(),
                                Messages.getString("Connection.19"));
                    }
                }

                try {
                    closeAllOpenStatements();
                } catch (SQLException ex) {
                    sqlEx = ex;
                }

                this.session.quit();
            } else {
                this.session.forceClose();
            }

            if (this.queryInterceptors != null) {
                this.queryInterceptors.forEach(QueryInterceptor::destroy);
            }

            if (this.exceptionInterceptor != null) {
                this.exceptionInterceptor.destroy();
            }

        } finally {
            this.openStatements.clear();
            this.queryInterceptors = null;
            this.exceptionInterceptor = null;
            this.nullStatementResultSetFactory = null;
        }

        if (sqlEx != null) {
            throw sqlEx;
        }

    }

    @Override
    public void recachePreparedStatement(JdbcPreparedStatement pstmt) throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.cachePrepStmts.getValue() && pstmt.isPoolable()) {
                synchronized (this.serverSideStatementCache) {
                    Object oldServerPrepStmt = this.serverSideStatementCache.put(
                            new CompoundCacheKey(pstmt.getCurrentDatabase(), ((PreparedQuery) pstmt.getQuery()).getOriginalSql()),
                            (ServerPreparedStatement) pstmt);
                    if (oldServerPrepStmt != null && oldServerPrepStmt != pstmt) {
                        ((ServerPreparedStatement) oldServerPrepStmt).isCached = false;
                        ((ServerPreparedStatement) oldServerPrepStmt).setClosed(false);
                        ((ServerPreparedStatement) oldServerPrepStmt).realClose(true, true);
                    }
                }
            }
        }
    }

    @Override
    public void decachePreparedStatement(JdbcPreparedStatement pstmt) throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.cachePrepStmts.getValue()) {
                synchronized (this.serverSideStatementCache) {
                    this.serverSideStatementCache.remove(new CompoundCacheKey(pstmt.getCurrentDatabase(), ((PreparedQuery) pstmt.getQuery()).getOriginalSql()));
                }
            }
        }
    }

    @Override
    public void registerStatement(JdbcStatement stmt) {
        this.openStatements.addIfAbsent(stmt);
    }

    @Override
    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        // this is a no-op
    }

    @Override
    public void resetServerState() throws SQLException {
        if (!this.propertySet.getBooleanProperty(PropertyKey.paranoid).getValue() && (this.session != null)) {
            changeUser(this.user, this.password);
        }
    }

    @Override
    public void rollback() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            try {
                if (this.connectionLifecycleInterceptors != null) {
                    IterateBlock<ConnectionLifecycleInterceptor> iter = new IterateBlock<ConnectionLifecycleInterceptor>(
                            this.connectionLifecycleInterceptors.iterator()) {

                        @Override
                        void forEach(ConnectionLifecycleInterceptor each) throws SQLException {
                            if (!each.rollback()) {
                                this.stopIterating = true;
                            }
                        }
                    };

                    iter.doForAll();

                    if (!iter.fullIteration()) {
                        return;
                    }
                }
                if (this.session.getServerSession().isAutoCommit()) {
                    throw SQLError.createSQLException(Messages.getString("Connection.20"), MysqlErrorNumbers.SQL_STATE_CONNECTION_NOT_OPEN,
                            getExceptionInterceptor());
                }
                try {
                    rollbackNoChecks();
                } catch (SQLException sqlEx) {
                    // We ignore non-transactional tables if told to do so
                    if (this.ignoreNonTxTables.getInitialValue() && (sqlEx.getErrorCode() == MysqlErrorNumbers.ER_WARNING_NOT_COMPLETE_ROLLBACK)) {
                        return;
                    }
                    throw sqlEx;

                }
            } catch (SQLException sqlException) {
                if (MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlException.getSQLState())) {
                    throw SQLError.createSQLException(Messages.getString("Connection.21"), MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN,
                            getExceptionInterceptor());
                }

                throw sqlException;
            } finally {
                this.session.setNeedsPing(this.reconnectAtTxEnd.getValue());
            }
        }
    }

    @Override
    public void rollback(final Savepoint savepoint) throws SQLException {

        synchronized (getConnectionMutex()) {
            checkClosed();

            try {
                if (this.connectionLifecycleInterceptors != null) {
                    IterateBlock<ConnectionLifecycleInterceptor> iter = new IterateBlock<ConnectionLifecycleInterceptor>(
                            this.connectionLifecycleInterceptors.iterator()) {

                        @Override
                        void forEach(ConnectionLifecycleInterceptor each) throws SQLException {
                            if (!each.rollback(savepoint)) {
                                this.stopIterating = true;
                            }
                        }
                    };

                    iter.doForAll();

                    if (!iter.fullIteration()) {
                        return;
                    }
                }

                StringBuilder rollbackQuery = new StringBuilder("ROLLBACK TO SAVEPOINT ");
                rollbackQuery.append('`');
                rollbackQuery.append(savepoint.getSavepointName());
                rollbackQuery.append('`');

                java.sql.Statement stmt = null;

                try {
                    stmt = getMetadataSafeStatement();

                    stmt.executeUpdate(rollbackQuery.toString());
                } catch (SQLException sqlEx) {
                    int errno = sqlEx.getErrorCode();

                    if (errno == 1181) {
                        String msg = sqlEx.getMessage();

                        if (msg != null) {
                            int indexOfError153 = msg.indexOf("153");

                            if (indexOfError153 != -1) {
                                throw SQLError.createSQLException(Messages.getString("Connection.22", new Object[] { savepoint.getSavepointName() }),
                                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, errno, getExceptionInterceptor());
                            }
                        }
                    }

                    // We ignore non-transactional tables if told to do so
                    if (this.ignoreNonTxTables.getValue() && (sqlEx.getErrorCode() != MysqlErrorNumbers.ER_WARNING_NOT_COMPLETE_ROLLBACK)) {
                        throw sqlEx;
                    }

                    if (MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlEx.getSQLState())) {
                        throw SQLError.createSQLException(Messages.getString("Connection.23"), MysqlErrorNumbers.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN,
                                getExceptionInterceptor());
                    }

                    throw sqlEx;
                } finally {
                    closeStatement(stmt);
                }
            } finally {
                this.session.setNeedsPing(this.reconnectAtTxEnd.getValue());
            }
        }
    }

    private void rollbackNoChecks() throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.useLocalTransactionState.getValue()) {
                if (!this.session.getServerSession().inTransactionOnServer()) {
                    return; // effectively a no-op
                }
            }

            this.session.execSQL(null, "rollback", -1, null, false, this.nullStatementResultSetFactory, null, false);

        }
    }

    @Override
    public java.sql.PreparedStatement serverPrepareStatement(String sql) throws SQLException {
        String nativeSql = this.processEscapeCodesForPrepStmts.getValue() ? nativeSQL(sql) : sql;

        return ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.getDatabase(), DEFAULT_RESULT_SET_TYPE,
                DEFAULT_RESULT_SET_CONCURRENCY);
    }

    @Override
    public java.sql.PreparedStatement serverPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        String nativeSql = this.processEscapeCodesForPrepStmts.getValue() ? nativeSQL(sql) : sql;

        ClientPreparedStatement pStmt = ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.getDatabase(), DEFAULT_RESULT_SET_TYPE,
                DEFAULT_RESULT_SET_CONCURRENCY);

        pStmt.setRetrieveGeneratedKeys(autoGenKeyIndex == java.sql.Statement.RETURN_GENERATED_KEYS);

        return pStmt;
    }

    @Override
    public java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        String nativeSql = this.processEscapeCodesForPrepStmts.getValue() ? nativeSQL(sql) : sql;

        return ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.getDatabase(), resultSetType, resultSetConcurrency);
    }

    @Override
    public java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        if (this.pedantic.getValue()) {
            if (resultSetHoldability != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                throw SQLError.createSQLException(Messages.getString("Connection.17"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        return serverPrepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public java.sql.PreparedStatement serverPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {

        ClientPreparedStatement pStmt = (ClientPreparedStatement) serverPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyIndexes != null) && (autoGenKeyIndexes.length > 0));

        return pStmt;
    }

    @Override
    public java.sql.PreparedStatement serverPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        ClientPreparedStatement pStmt = (ClientPreparedStatement) serverPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyColNames != null) && (autoGenKeyColNames.length > 0));

        return pStmt;
    }

    @Override
    public void setAutoCommit(final boolean autoCommitFlag) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            if (this.connectionLifecycleInterceptors != null) {
                IterateBlock<ConnectionLifecycleInterceptor> iter = new IterateBlock<ConnectionLifecycleInterceptor>(
                        this.connectionLifecycleInterceptors.iterator()) {

                    @Override
                    void forEach(ConnectionLifecycleInterceptor each) throws SQLException {
                        if (!each.setAutoCommit(autoCommitFlag)) {
                            this.stopIterating = true;
                        }
                    }
                };

                iter.doForAll();

                if (!iter.fullIteration()) {
                    return;
                }
            }

            if (this.autoReconnectForPools.getValue()) {
                this.autoReconnect.setValue(true);
            }

            boolean isAutoCommit = this.session.getServerSession().isAutoCommit();
            try {
                boolean needsSetOnServer = true;
                if (this.useLocalSessionState.getValue() && isAutoCommit == autoCommitFlag) {
                    needsSetOnServer = false;
                } else if (!this.autoReconnect.getValue()) {
                    needsSetOnServer = getSession().isSetNeededForAutoCommitMode(autoCommitFlag);
                }

                // this internal value must be set first as failover depends on it being set to true to fail over (which is done by most app servers and
                // connection pools at the end of a transaction), and the driver issues an implicit set based on this value when it (re)-connects to a
                // server so the value holds across connections
                this.session.getServerSession().setAutoCommit(autoCommitFlag);

                if (needsSetOnServer) {
                    this.session.execSQL(null, autoCommitFlag ? "SET autocommit=1" : "SET autocommit=0", -1, null, false, this.nullStatementResultSetFactory,
                            null, false);
                }
            } catch (CJCommunicationsException e) {
                throw e;
            } catch (CJException e) {
                // Reset to current autocommit value in case of an exception different than a communication exception occurs.
                this.session.getServerSession().setAutoCommit(isAutoCommit);
                // Update the stacktrace.
                throw SQLError.createSQLException(e.getMessage(), e.getSQLState(), e.getVendorCode(), e.isTransient(), e, getExceptionInterceptor());
            } finally {
                if (this.autoReconnectForPools.getValue()) {
                    this.autoReconnect.setValue(false);
                }
            }

            return;
        }
    }

    @Override
    public void setCatalog(final String catalog) throws SQLException {
        if (this.propertySet.<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.CATALOG) {
            setDatabase(catalog);
        }
    }

    public void setDatabase(final String db) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            if (db == null) {
                throw SQLError.createSQLException("Database can not be null", MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            if (this.connectionLifecycleInterceptors != null) {
                IterateBlock<ConnectionLifecycleInterceptor> iter = new IterateBlock<ConnectionLifecycleInterceptor>(
                        this.connectionLifecycleInterceptors.iterator()) {

                    @Override
                    void forEach(ConnectionLifecycleInterceptor each) throws SQLException {
                        if (!each.setDatabase(db)) {
                            this.stopIterating = true;
                        }
                    }
                };

                iter.doForAll();

                if (!iter.fullIteration()) {
                    return;
                }
            }

            if (this.useLocalSessionState.getValue()) {
                if (this.session.getServerSession().isLowerCaseTableNames()) {
                    if (this.database.equalsIgnoreCase(db)) {
                        return;
                    }
                } else {
                    if (this.database.equals(db)) {
                        return;
                    }
                }
            }

            String quotedId = this.session.getIdentifierQuoteString();

            if ((quotedId == null) || quotedId.equals(" ")) {
                quotedId = "";
            }

            StringBuilder query = new StringBuilder("USE ");
            query.append(StringUtils.quoteIdentifier(db, quotedId, this.pedantic.getValue()));

            this.session.execSQL(null, query.toString(), -1, null, false, this.nullStatementResultSetFactory, null, false);

            this.database = db;
        }
    }

    @Override
    public String getDatabase() throws SQLException {
        synchronized (getConnectionMutex()) {
            return this.database;
        }
    }

    @Override
    public void setFailedOver(boolean flag) {
        // handled higher up
    }

    @Override
    public void setHoldability(int arg0) throws SQLException {
        // do nothing
    }

    @Override
    public void setInGlobalTx(boolean flag) {
        this.isInGlobalTx = flag;
    }

    @Override
    public void setReadOnly(boolean readOnlyFlag) throws SQLException {

        setReadOnlyInternal(readOnlyFlag);
    }

    @Override
    public void setReadOnlyInternal(boolean readOnlyFlag) throws SQLException {
        synchronized (getConnectionMutex()) {
            // note this this is safe even inside a transaction
            if (this.readOnlyPropagatesToServer.getValue() && versionMeetsMinimum(5, 6, 5)) {
                if (!this.useLocalSessionState.getValue() || (readOnlyFlag != this.readOnly)) {
                    this.session.execSQL(null, "set session transaction " + (readOnlyFlag ? "read only" : "read write"), -1, null, false,
                            this.nullStatementResultSetFactory, null, false);
                }
            }

            this.readOnly = readOnlyFlag;
        }
    }

    @Override
    public java.sql.Savepoint setSavepoint() throws SQLException {
        MysqlSavepoint savepoint = new MysqlSavepoint(getExceptionInterceptor());

        setSavepoint(savepoint);

        return savepoint;
    }

    private void setSavepoint(MysqlSavepoint savepoint) throws SQLException {

        synchronized (getConnectionMutex()) {
            checkClosed();

            StringBuilder savePointQuery = new StringBuilder("SAVEPOINT ");
            savePointQuery.append('`');
            savePointQuery.append(savepoint.getSavepointName());
            savePointQuery.append('`');

            java.sql.Statement stmt = null;

            try {
                stmt = getMetadataSafeStatement();

                stmt.executeUpdate(savePointQuery.toString());
            } finally {
                closeStatement(stmt);
            }
        }
    }

    @Override
    public java.sql.Savepoint setSavepoint(String name) throws SQLException {
        synchronized (getConnectionMutex()) {
            MysqlSavepoint savepoint = new MysqlSavepoint(name, getExceptionInterceptor());

            setSavepoint(savepoint);

            return savepoint;
        }
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            String sql = null;

            boolean shouldSendSet = false;

            if (this.propertySet.getBooleanProperty(PropertyKey.alwaysSendSetIsolation).getValue()) {
                shouldSendSet = true;
            } else {
                if (level != this.isolationLevel) {
                    shouldSendSet = true;
                }
            }

            if (this.useLocalSessionState.getValue()) {
                shouldSendSet = this.isolationLevel != level;
            }

            if (shouldSendSet) {
                switch (level) {
                    case java.sql.Connection.TRANSACTION_NONE:
                        throw SQLError.createSQLException(Messages.getString("Connection.24"), getExceptionInterceptor());

                    case java.sql.Connection.TRANSACTION_READ_COMMITTED:
                        sql = "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED";

                        break;

                    case java.sql.Connection.TRANSACTION_READ_UNCOMMITTED:
                        sql = "SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED";

                        break;

                    case java.sql.Connection.TRANSACTION_REPEATABLE_READ:
                        sql = "SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ";

                        break;

                    case java.sql.Connection.TRANSACTION_SERIALIZABLE:
                        sql = "SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE";

                        break;

                    default:
                        throw SQLError.createSQLException(Messages.getString("Connection.25", new Object[] { level }),
                                MysqlErrorNumbers.SQL_STATE_DRIVER_NOT_CAPABLE, getExceptionInterceptor());
                }

                this.session.execSQL(null, sql, -1, null, false, this.nullStatementResultSetFactory, null, false);

                this.isolationLevel = level;
            }
        }
    }

    @Override
    public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException {
        synchronized (getConnectionMutex()) {
            this.typeMap = map;
        }
    }

    private void setupServerForTruncationChecks() throws SQLException {
        synchronized (getConnectionMutex()) {
            RuntimeProperty<Boolean> jdbcCompliantTruncation = this.propertySet.getProperty(PropertyKey.jdbcCompliantTruncation);
            if (jdbcCompliantTruncation.getValue()) {
                String currentSqlMode = this.session.getServerSession().getServerVariable("sql_mode");

                boolean strictTransTablesIsSet = StringUtils.indexOfIgnoreCase(currentSqlMode, "STRICT_TRANS_TABLES") != -1;

                if (currentSqlMode == null || currentSqlMode.length() == 0 || !strictTransTablesIsSet) {
                    StringBuilder commandBuf = new StringBuilder("SET sql_mode='");

                    if (currentSqlMode != null && currentSqlMode.length() > 0) {
                        commandBuf.append(currentSqlMode);
                        commandBuf.append(",");
                    }

                    commandBuf.append("STRICT_TRANS_TABLES'");

                    this.session.execSQL(null, commandBuf.toString(), -1, null, false, this.nullStatementResultSetFactory, null, false);

                    jdbcCompliantTruncation.setValue(false); // server's handling this for us now
                } else if (strictTransTablesIsSet) {
                    // We didn't set it, but someone did, so we piggy back on it
                    jdbcCompliantTruncation.setValue(false); // server's handling this for us now
                }
            }
        }
    }

    @Override
    public void shutdownServer() throws SQLException {
        try {
            this.session.shutdownServer();
        } catch (CJException ex) {
            SQLException sqlEx = SQLError.createSQLException(Messages.getString("Connection.UnhandledExceptionDuringShutdown"),
                    MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());

            sqlEx.initCause(ex);

            throw sqlEx;
        }
    }

    @Override
    public void unregisterStatement(JdbcStatement stmt) {
        this.openStatements.remove(stmt);
    }

    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        return this.session.versionMeetsMinimum(major, minor, subminor);
    }

    @Override
    public CachedResultSetMetaData getCachedMetaData(String sql) {
        if (this.resultSetMetadataCache != null) {
            synchronized (this.resultSetMetadataCache) {
                return this.resultSetMetadataCache.get(sql);
            }
        }

        return null; // no cache exists
    }

    @Override
    public void initializeResultsMetadataFromCache(String sql, CachedResultSetMetaData cachedMetaData, ResultSetInternalMethods resultSet) throws SQLException {

        if (cachedMetaData == null) {

            // read from results
            cachedMetaData = new CachedResultSetMetaDataImpl();

            // assume that users will use named-based lookups
            resultSet.getColumnDefinition().buildIndexMapping();
            resultSet.initializeWithMetadata();

            if (resultSet instanceof UpdatableResultSet) {
                ((UpdatableResultSet) resultSet).checkUpdatability();
            }

            resultSet.populateCachedMetaData(cachedMetaData);

            this.resultSetMetadataCache.put(sql, cachedMetaData);
        } else {
            resultSet.getColumnDefinition().initializeFrom(cachedMetaData);
            resultSet.initializeWithMetadata();

            if (resultSet instanceof UpdatableResultSet) {
                ((UpdatableResultSet) resultSet).checkUpdatability();
            }
        }
    }

    @Override
    public String getStatementComment() {
        return this.session.getProtocol().getQueryComment();
    }

    @Override
    public void setStatementComment(String comment) {
        this.session.getProtocol().setQueryComment(comment);
    }

    @Override
    public void transactionBegun() {
        synchronized (getConnectionMutex()) {
            if (this.connectionLifecycleInterceptors != null) {
                this.connectionLifecycleInterceptors.stream().forEach(ConnectionLifecycleInterceptor::transactionBegun);
            }
        }
    }

    @Override
    public void transactionCompleted() {
        synchronized (getConnectionMutex()) {
            if (this.connectionLifecycleInterceptors != null) {
                this.connectionLifecycleInterceptors.stream().forEach(ConnectionLifecycleInterceptor::transactionCompleted);
            }
        }
    }

    @Override
    public boolean storesLowerCaseTableName() {
        return this.session.getServerSession().storesLowerCaseTableNames();
    }

    private ExceptionInterceptor exceptionInterceptor;

    @Override
    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    @Override
    public boolean isServerLocal() throws SQLException {
        synchronized (getConnectionMutex()) {
            try {
                return this.session.isServerLocal(this.getSession());
            } catch (CJException ex) {
                SQLException sqlEx = SQLExceptionsMapping.translateException(ex, getExceptionInterceptor());
                throw sqlEx;
            }
        }
    }

    @Override
    public int getSessionMaxRows() {
        synchronized (getConnectionMutex()) {
            return this.session.getSessionMaxRows();
        }
    }

    @Override
    public void setSessionMaxRows(int max) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
            if (this.session.getSessionMaxRows() != max) {
                this.session.setSessionMaxRows(max);
                this.session.execSQL(null, "SET SQL_SELECT_LIMIT=" + (this.session.getSessionMaxRows() == -1 ? "DEFAULT" : this.session.getSessionMaxRows()),
                        -1, null, false, this.nullStatementResultSetFactory, null, false);
            }
        }
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        checkClosed();
        if (this.propertySet.<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA) {
            setDatabase(schema);
        }
    }

    @Override
    public String getSchema() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
            return this.propertySet.<DatabaseTerm>getEnumProperty(PropertyKey.databaseTerm).getValue() == DatabaseTerm.SCHEMA ? this.database : null;
        }
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        SecurityManager sec = System.getSecurityManager();

        if (sec != null) {
            sec.checkPermission(ABORT_PERM);
        }

        if (executor == null) {
            throw SQLError.createSQLException(Messages.getString("Connection.26"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        executor.execute(new Runnable() {

            @Override
            public void run() {
                try {
                    abortInternal();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    @Override
    public void setNetworkTimeout(Executor executor, final int milliseconds) throws SQLException {
        synchronized (getConnectionMutex()) {
            SecurityManager sec = System.getSecurityManager();

            if (sec != null) {
                sec.checkPermission(SET_NETWORK_TIMEOUT_PERM);
            }

            if (executor == null) {
                throw SQLError.createSQLException(Messages.getString("Connection.26"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            checkClosed();

            executor.execute(new NetworkTimeoutSetter(this, milliseconds));
        }
    }

    private static class NetworkTimeoutSetter implements Runnable {
        private final WeakReference<JdbcConnection> connRef;
        private final int milliseconds;

        public NetworkTimeoutSetter(JdbcConnection conn, int milliseconds) {
            this.connRef = new WeakReference<>(conn);
            this.milliseconds = milliseconds;
        }

        @Override
        public void run() {
            JdbcConnection conn = this.connRef.get();
            if (conn != null) {
                synchronized (conn.getConnectionMutex()) {
                    ((NativeSession) conn.getSession()).setSocketTimeout(this.milliseconds);
                }
            }
        }
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
            return this.session.getSocketTimeout();
        }
    }

    @Override
    public Clob createClob() {
        return new com.mysql.cj.jdbc.Clob(getExceptionInterceptor());
    }

    @Override
    public Blob createBlob() {
        return new com.mysql.cj.jdbc.Blob(getExceptionInterceptor());
    }

    @Override
    public NClob createNClob() {
        return new com.mysql.cj.jdbc.NClob(getExceptionInterceptor());
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return new MysqlSQLXML(getExceptionInterceptor());
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        synchronized (getConnectionMutex()) {
            if (isClosed()) {
                return false;
            }

            try {
                try {
                    pingInternal(false, timeout * 1000);
                } catch (Throwable t) {
                    try {
                        abortInternal();
                    } catch (Throwable ignoreThrown) {
                        // we're dead now anyway
                    }

                    return false;
                }

            } catch (Throwable t) {
                return false;
            }

            return true;
        }
    }

    private ClientInfoProvider infoProvider;

    @Override
    public ClientInfoProvider getClientInfoProviderImpl() throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.infoProvider == null) {
                String clientInfoProvider = this.propertySet.getStringProperty(PropertyKey.clientInfoProvider).getStringValue();
                try {
                    this.infoProvider = Util.getInstance(ClientInfoProvider.class, clientInfoProvider, null, null, getExceptionInterceptor());
                } catch (CJException e1) {
                    if (ClassNotFoundException.class.isInstance(e1.getCause())) {
                        // Retry with package name prepended.
                        try {
                            this.infoProvider = Util.getInstance(ClientInfoProvider.class, "com.mysql.cj.jdbc." + clientInfoProvider, null, null,
                                    getExceptionInterceptor());
                        } catch (CJException e2) {
                            throw SQLExceptionsMapping.translateException(e1, getExceptionInterceptor());
                        }
                    } else {
                        throw SQLExceptionsMapping.translateException(e1, getExceptionInterceptor());
                    }
                }
                this.infoProvider.initialize(this, this.props);
            }
            return this.infoProvider;
        }
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            getClientInfoProviderImpl().setClientInfo(this, name, value);
        } catch (SQLClientInfoException ciEx) {
            throw ciEx;
        } catch (SQLException | CJException sqlEx) {
            SQLClientInfoException clientInfoEx = new SQLClientInfoException();
            clientInfoEx.initCause(sqlEx);

            throw clientInfoEx;
        }
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            getClientInfoProviderImpl().setClientInfo(this, properties);
        } catch (SQLClientInfoException ciEx) {
            throw ciEx;
        } catch (SQLException | CJException sqlEx) {
            SQLClientInfoException clientInfoEx = new SQLClientInfoException();
            clientInfoEx.initCause(sqlEx);

            throw clientInfoEx;
        }
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return getClientInfoProviderImpl().getClientInfo(this, name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return getClientInfoProviderImpl().getClientInfo(this);
    }

    @Override
    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            // This works for classes that aren't actually wrapping
            // anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException("Unable to unwrap to " + iface.toString(), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                    getExceptionInterceptor());
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {

        // This works for classes that aren't actually wrapping
        // anything
        return iface.isInstance(this);
    }

    @Override
    public NativeSession getSession() {
        return this.session;
    }

    @Override
    public String getHostPortPair() {
        return this.origHostInfo.getHostPortPair();
    }

    @Override
    public void handleNormalClose() {
        try {
            close();
        } catch (SQLException e) {
            ExceptionFactory.createException(e.getMessage(), e);
        }
    }

    @Override
    public void handleReconnect() {
        createNewIO(true);
    }

    @Override
    public void handleCleanup(Throwable whyCleanedUp) {
        cleanup(whyCleanedUp);
    }

    @Override
    public ServerSessionStateController getServerSessionStateController() {
        return this.session.getServerSession().getServerSessionStateController();
    }

}
