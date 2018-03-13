/*
 * Copyright (c) 2002, 2018, Oracle and/or its affiliates. All rights reserved.
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
import com.mysql.cj.Constants;
import com.mysql.cj.LicenseConfiguration;
import com.mysql.cj.Messages;
import com.mysql.cj.NativeSession;
import com.mysql.cj.NoSubInterceptorWrapper;
import com.mysql.cj.ParseInfo;
import com.mysql.cj.PreparedQuery;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.Session.SessionEventListener;
import com.mysql.cj.conf.HostInfo;
import com.mysql.cj.conf.ModifiableProperty;
import com.mysql.cj.conf.PropertyDefinitions;
import com.mysql.cj.conf.ReadableProperty;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.ExceptionInterceptorChain;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.PasswordExpiredException;
import com.mysql.cj.exceptions.UnableToConnectException;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
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
import com.mysql.cj.log.ProfilerEventHandlerFactory;
import com.mysql.cj.log.ProfilerEventImpl;
import com.mysql.cj.log.StandardLogger;
import com.mysql.cj.protocol.SocksProxySocketFactory;
import com.mysql.cj.util.LRUCache;
import com.mysql.cj.util.LogUtils;
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

    public String getHost() {
        return this.session.getHostInfo().getHost();
    }

    private JdbcConnection proxy = null;
    private InvocationHandler realProxy = null;

    public boolean isProxySet() {
        return this.proxy != null;
    }

    public void setProxy(JdbcConnection proxy) {
        this.proxy = proxy;
        this.realProxy = this.proxy instanceof MultiHostMySQLConnection ? ((MultiHostMySQLConnection) proxy).getThisAsProxy() : null;
    }

    // this connection has to be proxied when using multi-host settings so that statements get routed to the right physical connection
    // (works as "logical" connection)
    private JdbcConnection getProxy() {
        return (this.proxy != null) ? this.proxy : (JdbcConnection) this;
    }

    public JdbcConnection getMultiHostSafeProxy() {
        return this.getProxy();
    }

    public JdbcConnection getActiveMySQLConnection() {
        return this;
    }

    public Object getConnectionMutex() {
        return (this.realProxy != null) ? this.realProxy : getProxy();
    }

    /**
     * Used as a key for caching callable statements which (may) depend on
     * current catalog...In 5.0.x, they don't (currently), but stored procedure
     * names soon will, so current catalog is a (hidden) component of the name.
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

    /**
     * The mapping between MySQL charset names and Java charset names.
     * Initialized by loadCharacterSetMapping()
     */
    public static Map<?, ?> charsetMap;

    /** Default logger class name */
    protected static final String DEFAULT_LOGGER_CLASS = StandardLogger.class.getName();

    /**
     * Map mysql transaction isolation level name to
     * java.sql.Connection.TRANSACTION_XXX
     */
    private static Map<String, Integer> mapTransIsolationNameToValue = null;

    protected static Map<?, ?> roundRobinStatsMap;

    private List<ConnectionLifecycleInterceptor> connectionLifecycleInterceptors;

    private static final int DEFAULT_RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;

    private static final int DEFAULT_RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;

    static {
        mapTransIsolationNameToValue = new HashMap<>(8);
        mapTransIsolationNameToValue.put("READ-UNCOMMITED", TRANSACTION_READ_UNCOMMITTED);
        mapTransIsolationNameToValue.put("READ-UNCOMMITTED", TRANSACTION_READ_UNCOMMITTED);
        mapTransIsolationNameToValue.put("READ-COMMITTED", TRANSACTION_READ_COMMITTED);
        mapTransIsolationNameToValue.put("REPEATABLE-READ", TRANSACTION_REPEATABLE_READ);
        mapTransIsolationNameToValue.put("SERIALIZABLE", TRANSACTION_SERIALIZABLE);
    }

    /**
     * Creates a connection instance
     */
    public static JdbcConnection getInstance(HostInfo hostInfo) throws SQLException {
        return new ConnectionImpl(hostInfo);
    }

    private static final Random random = new Random();

    /**
     * @param url
     * @param hostList
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
    private CacheAdapter<String, ParseInfo> cachedPreparedStatementParams;

    /** The database we're currently using (called Catalog in JDBC terms). */
    private String database = null;

    /** Internal DBMD to use for various database-version specific features */
    private DatabaseMetaData dbmd = null;

    private NativeSession session = null;

    /** Is this connection associated with a global tx? */
    private boolean isInGlobalTx = false;

    /** isolation level */
    private int isolationLevel = java.sql.Connection.TRANSACTION_READ_COMMITTED;

    /** When did the master fail? */
    //	private long masterFailTimeMillis = 0L;

    /**
     * An array of currently open statements.
     * Copy-on-write used here to avoid ConcurrentModificationException when statements unregister themselves while we iterate over the list.
     */
    private final CopyOnWriteArrayList<JdbcStatement> openStatements = new CopyOnWriteArrayList<>();

    private LRUCache<CompoundCacheKey, CallableStatement.CallableStatementParamInfo> parsedCallableStatementCache;

    /** The password we used */
    private String password = null;

    /** Point of origin where this Connection was created */
    private String pointOfOrigin;

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
    private boolean hasTriedMasterFlag = false;

    private List<QueryInterceptor> queryInterceptors;

    protected JdbcPropertySet propertySet;

    private ReadableProperty<Boolean> autoReconnectForPools;
    private ReadableProperty<Boolean> cachePrepStmts;
    private ModifiableProperty<Boolean> autoReconnect;
    private ReadableProperty<Boolean> useUsageAdvisor;
    private ReadableProperty<Boolean> reconnectAtTxEnd;
    private ReadableProperty<Boolean> emulateUnsupportedPstmts;
    private ReadableProperty<Boolean> ignoreNonTxTables;
    private ReadableProperty<Boolean> pedantic;
    private ReadableProperty<Integer> prepStmtCacheSqlLimit;
    private ReadableProperty<Boolean> useLocalSessionState;
    private ReadableProperty<Boolean> useServerPrepStmts;
    private ReadableProperty<Boolean> processEscapeCodesForPrepStmts;
    private ReadableProperty<Boolean> useLocalTransactionState;
    private ReadableProperty<Boolean> disconnectOnExpiredPasswords;
    private ReadableProperty<Boolean> readOnlyPropagatesToServer;

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
            this.user = StringUtils.isNullOrEmpty(hostInfo.getUser()) ? "" : hostInfo.getUser();
            this.password = StringUtils.isNullOrEmpty(hostInfo.getPassword()) ? "" : hostInfo.getPassword();

            this.props = hostInfo.exposeAsProperties();

            this.propertySet = new JdbcPropertySetImpl();

            this.propertySet.initializeProperties(this.props);

            // We need Session ASAP to get access to central driver functionality
            this.nullStatementResultSetFactory = new ResultSetFactory(this, null);
            this.session = new NativeSession(hostInfo, this.propertySet);
            this.session.addListener(this); // listen for session status changes

            // we can't cache fixed values here because properties are still not initialized with user provided values
            this.autoReconnectForPools = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_autoReconnectForPools);
            this.cachePrepStmts = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_cachePrepStmts);
            this.autoReconnect = this.propertySet.<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_autoReconnect);
            this.useUsageAdvisor = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useUsageAdvisor);
            this.reconnectAtTxEnd = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_reconnectAtTxEnd);
            this.emulateUnsupportedPstmts = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_emulateUnsupportedPstmts);
            this.ignoreNonTxTables = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_ignoreNonTxTables);
            this.pedantic = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_pedantic);
            this.prepStmtCacheSqlLimit = this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_prepStmtCacheSqlLimit);
            this.useLocalSessionState = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useLocalSessionState);
            this.useServerPrepStmts = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useServerPrepStmts);
            this.processEscapeCodesForPrepStmts = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_processEscapeCodesForPrepStmts);
            this.useLocalTransactionState = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_useLocalTransactionState);
            this.disconnectOnExpiredPasswords = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords);
            this.readOnlyPropagatesToServer = this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_readOnlyPropagatesToServer);

            String exceptionInterceptorClasses = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_exceptionInterceptors).getStringValue();
            if (exceptionInterceptorClasses != null && !"".equals(exceptionInterceptorClasses)) {
                this.exceptionInterceptor = new ExceptionInterceptorChain(exceptionInterceptorClasses, this.props, this.session.getLog());
            }

            if (this.cachePrepStmts.getValue()) {
                createPreparedStatementCaches();
            }

            if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheCallableStmts).getValue()) {
                this.parsedCallableStatementCache = new LRUCache<>(
                        this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_callableStmtCacheSize).getValue());
            }

            if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_allowMultiQueries).getValue()) {
                this.propertySet.<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_cacheResultSetMetadata).setValue(false); // we don't handle this yet
            }

            if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheResultSetMetadata).getValue()) {
                this.resultSetMetadataCache = new LRUCache<>(
                        this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_metadataCacheSize).getValue());
            }

            if (this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_socksProxyHost).getStringValue() != null) {
                this.propertySet.getJdbcModifiableProperty(PropertyDefinitions.PNAME_socketFactory).setValue(SocksProxySocketFactory.class.getName());
            }

            this.pointOfOrigin = this.useUsageAdvisor.getValue() ? LogUtils.findCallingClassAndMethod(new Throwable()) : "";

            this.dbmd = getMetaData(false, false);

            initializeSafeQueryInterceptors();

        } catch (CJException e1) {
            throw SQLExceptionsMapping.translateException(e1, getExceptionInterceptor());
        }

        try {
            createNewIO(false);

            unSafeQueryInterceptors();

            NonRegisteringDriver.trackConnection(this);
        } catch (SQLException ex) {
            cleanup(ex);

            // don't clobber SQL exceptions
            throw ex;
        } catch (Exception ex) {
            cleanup(ex);

            throw SQLError
                    .createSQLException(
                            this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_paranoid).getValue() ? Messages.getString("Connection.0")
                                    : Messages.getString("Connection.1",
                                            new Object[] { this.session.getHostInfo().getHost(), this.session.getHostInfo().getPort() }),
                            MysqlErrorNumbers.SQL_STATE_COMMUNICATION_LINK_FAILURE, ex, getExceptionInterceptor());
        }

    }

    @Override
    public JdbcPropertySet getPropertySet() {
        return this.propertySet;
    }

    public void unSafeQueryInterceptors() throws SQLException {
        this.queryInterceptors = this.queryInterceptors.stream().map(u -> ((NoSubInterceptorWrapper) u).getUnderlyingInterceptor())
                .collect(Collectors.toList());

        if (this.session != null) {
            this.session.setQueryInterceptors(this.queryInterceptors);
        }
    }

    public void initializeSafeQueryInterceptors() throws SQLException {
        this.queryInterceptors = Util
                .<QueryInterceptor> loadClasses(this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_queryInterceptors).getStringValue(),
                        "MysqlIo.BadQueryInterceptor", getExceptionInterceptor())
                .stream().map(o -> new NoSubInterceptorWrapper(o.init(this, this.props, this.session.getLog()))).collect(Collectors.toList());
    }

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

        if (this.cachePrepStmts.getValue()) {
            synchronized (this.serverSideStatementCheckCache) {
                Boolean flag = this.serverSideStatementCheckCache.get(sql);

                if (flag != null) {
                    return flag.booleanValue();
                }

                boolean canHandle = StringUtils.canHandleAsServerPreparedStatementNoCache(sql, getServerVersion());

                if (sql.length() < this.prepStmtCacheSqlLimit.getValue()) {
                    this.serverSideStatementCheckCache.put(sql, canHandle ? Boolean.TRUE : Boolean.FALSE);
                }

                return canHandle;
            }
        }

        return StringUtils.canHandleAsServerPreparedStatementNoCache(sql, getServerVersion());
    }

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

            this.session.configureClientCharacterSet(true);

            this.session.setSessionVariables();

            setupServerForTruncationChecks();
        }
    }

    public void checkClosed() {
        this.session.checkClosed();
    }

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

    /**
     * Clobbers the physical network connection and marks
     * this connection as closed.
     * 
     * @throws SQLException
     */
    public void abortInternal() throws SQLException {
        this.session.forceClose();
    }

    /**
     * Destroys this connection and any underlying resources
     * 
     * @param fromWhere
     * @param whyCleanedUp
     */
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
    public void clearHasTriedMaster() {
        this.hasTriedMasterFlag = false;
    }

    /**
     * After this call, getWarnings returns null until a new warning is reported
     * for this connection.
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
    public void clearWarnings() throws SQLException {
        // firstWarning = null;
    }

    /**
     * @param sql
     * @throws SQLException
     */
    public java.sql.PreparedStatement clientPrepareStatement(String sql) throws SQLException {
        return clientPrepareStatement(sql, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        java.sql.PreparedStatement pStmt = clientPrepareStatement(sql);

        ((ClientPreparedStatement) pStmt).setRetrieveGeneratedKeys(autoGenKeyIndex == java.sql.Statement.RETURN_GENERATED_KEYS);

        return pStmt;
    }

    /**
     * @param sql
     * @param resultSetType
     * @param resultSetConcurrency
     * @throws SQLException
     */
    public java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return clientPrepareStatement(sql, resultSetType, resultSetConcurrency, true);
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, boolean processEscapeCodesIfNeeded)
            throws SQLException {

        String nativeSql = processEscapeCodesIfNeeded && this.processEscapeCodesForPrepStmts.getValue() ? nativeSQL(sql) : sql;

        ClientPreparedStatement pStmt = null;

        if (this.cachePrepStmts.getValue()) {
            ParseInfo pStmtInfo = this.cachedPreparedStatementParams.get(nativeSql);

            if (pStmtInfo == null) {
                pStmt = ClientPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.database);

                this.cachedPreparedStatementParams.put(nativeSql, pStmt.getParseInfo());
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

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {

        ClientPreparedStatement pStmt = (ClientPreparedStatement) clientPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyIndexes != null) && (autoGenKeyIndexes.length > 0));

        return pStmt;
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        ClientPreparedStatement pStmt = (ClientPreparedStatement) clientPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyColNames != null) && (autoGenKeyColNames.length > 0));

        return pStmt;
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        return clientPrepareStatement(sql, resultSetType, resultSetConcurrency, true);
    }

    // --------------------------JDBC 2.0-----------------------------

    /**
     * In some cases, it is desirable to immediately release a Connection's
     * database and JDBC resources instead of waiting for them to be
     * automatically released (cant think why off the top of my head) <B>Note:</B>
     * A Connection is automatically closed when it is garbage collected.
     * Certain fatal errors also result in a closed connection.
     * 
     * @exception SQLException
     *                if a database access error occurs
     */
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

    /**
     * The method commit() makes all changes made since the previous
     * commit/rollback permanent and releases any database locks currently held
     * by the Connection. This method should only be used when auto-commit has
     * been disabled.
     * <p>
     * <b>Note:</b> MySQL does not support transactions, so this method is a no-op.
     * </p>
     * 
     * @exception SQLException
     *                if a database access error occurs
     * @see setAutoCommit
     */
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

                this.session.execSQL(null, "commit", -1, null, false, this.nullStatementResultSetFactory, this.database, null, false);
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

    /**
     * Creates an IO channel to the server
     * 
     * @param isForReconnect
     *            is this request for a re-connect
     * @throws CommunicationsException
     */
    public void createNewIO(boolean isForReconnect) {
        synchronized (getConnectionMutex()) {
            // Synchronization Not needed for *new* connections, but defintely for connections going through fail-over, since we might get the new connection up
            // and running *enough* to start sending cached or still-open server-side prepared statements over to the backend before we get a chance to
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
        double timeout = this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_initialTimeout).getValue();
        boolean connectionGood = false;

        Exception connectionException = null;

        for (int attemptCount = 0; (attemptCount < this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_maxReconnects).getValue())
                && !connectionGood; attemptCount++) {
            try {
                this.session.forceClose();

                JdbcConnection c = getProxy();
                this.session.connect(this.origHostInfo, this.user, this.password, this.database, DriverManager.getLoginTimeout() * 1000, c);
                pingInternal(false, 0);

                boolean oldAutoCommit;
                int oldIsolationLevel;
                boolean oldReadOnly;
                String oldCatalog;

                synchronized (getConnectionMutex()) {
                    // save state from old connection
                    oldAutoCommit = getAutoCommit();
                    oldIsolationLevel = this.isolationLevel;
                    oldReadOnly = isReadOnly(false);
                    oldCatalog = getCatalog();

                    this.session.setQueryInterceptors(this.queryInterceptors);
                }

                // Server properties might be different from previous connection, so initialize again...
                initializePropsFromServer();

                if (isForReconnect) {
                    // Restore state from old connection
                    setAutoCommit(oldAutoCommit);
                    setTransactionIsolation(oldIsolationLevel);
                    setCatalog(oldCatalog);
                    setReadOnly(oldReadOnly);
                }

                connectionGood = true;

                break;
            } catch (UnableToConnectException rejEx) {
                close();
                this.session.getProtocol().getSocketConnection().forceClose();

            } catch (Exception EEE) {
                connectionException = EEE;
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
                            new Object[] { this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_maxReconnects).getValue() }),
                    MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, connectionException, getExceptionInterceptor());
            throw chainedEx;
        }

        if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_paranoid).getValue() && !this.autoReconnect.getValue()) {
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
            this.session.connect(this.origHostInfo, this.user, this.password, this.database, DriverManager.getLoginTimeout() * 1000, c);

            // save state from old connection
            boolean oldAutoCommit = getAutoCommit();
            int oldIsolationLevel = this.isolationLevel;
            boolean oldReadOnly = isReadOnly(false);
            String oldCatalog = getCatalog();

            this.session.setQueryInterceptors(this.queryInterceptors);

            // Server properties might be different from previous connection, so initialize again...
            initializePropsFromServer();

            if (isForReconnect) {
                // Restore state from old connection
                setAutoCommit(oldAutoCommit);
                setTransactionIsolation(oldIsolationLevel);
                setCatalog(oldCatalog);
                setReadOnly(oldReadOnly);
            }
            return;

        } catch (UnableToConnectException rejEx) {
            close();
            this.session.getProtocol().getSocketConnection().forceClose();
            throw rejEx;

        } catch (Exception EEE) {

            if ((EEE instanceof PasswordExpiredException
                    || EEE instanceof SQLException && ((SQLException) EEE).getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD)
                    && !this.disconnectOnExpiredPasswords.getValue()) {
                return;
            }

            if (this.session != null) {
                this.session.forceClose();
            }

            connectionNotEstablishedBecause = EEE;

            if (EEE instanceof SQLException) {
                throw (SQLException) EEE;
            }

            if (EEE.getCause() != null && EEE.getCause() instanceof SQLException) {
                throw (SQLException) EEE.getCause();
            }

            if (EEE instanceof CJException) {
                throw (CJException) EEE;
            }

            SQLException chainedEx = SQLError.createSQLException(Messages.getString("Connection.UnableToConnect"),
                    MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, getExceptionInterceptor());
            chainedEx.initCause(connectionNotEstablishedBecause);

            throw chainedEx;
        }
    }

    private void createPreparedStatementCaches() throws SQLException {
        synchronized (getConnectionMutex()) {
            int cacheSize = this.propertySet.getIntegerReadableProperty(PropertyDefinitions.PNAME_prepStmtCacheSize).getValue();
            String parseInfoCacheFactory = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory).getValue();

            try {
                Class<?> factoryClass;

                factoryClass = Class.forName(parseInfoCacheFactory);

                @SuppressWarnings("unchecked")
                CacheAdapterFactory<String, ParseInfo> cacheFactory = ((CacheAdapterFactory<String, ParseInfo>) factoryClass.newInstance());

                this.cachedPreparedStatementParams = cacheFactory.getInstance(this, this.origHostInfo.getDatabaseUrl(), cacheSize,
                        this.prepStmtCacheSqlLimit.getValue());

            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                SQLException sqlEx = SQLError.createSQLException(Messages.getString("Connection.CantFindCacheFactory",
                        new Object[] { parseInfoCacheFactory, PropertyDefinitions.PNAME_parseInfoCacheFactory }), getExceptionInterceptor());
                sqlEx.initCause(e);

                throw sqlEx;
            } catch (Exception e) {
                SQLException sqlEx = SQLError.createSQLException(Messages.getString("Connection.CantLoadCacheFactory",
                        new Object[] { parseInfoCacheFactory, PropertyDefinitions.PNAME_parseInfoCacheFactory }), getExceptionInterceptor());
                sqlEx.initCause(e);

                throw sqlEx;
            }

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
                                ps.close();
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

    /**
     * SQL statements without parameters are normally executed using Statement
     * objects. If the same SQL statement is executed many times, it is more
     * efficient to use a PreparedStatement
     * 
     * @return a new Statement object
     * @throws SQLException
     *             passed through from the constructor
     */
    public java.sql.Statement createStatement() throws SQLException {
        return createStatement(DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    /**
     * JDBC 2.0 Same as createStatement() above, but allows the default result
     * set type and result set concurrency type to be overridden.
     * 
     * @param resultSetType
     *            a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency
     *            a concurrency type, see ResultSet.CONCUR_XXX
     * @return a new Statement object
     * @exception SQLException
     *                if a database-access error occurs.
     */
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {

        StatementImpl stmt = new StatementImpl(getMultiHostSafeProxy(), this.database);
        stmt.setResultSetType(resultSetType);
        stmt.setResultSetConcurrency(resultSetConcurrency);

        return stmt;
    }

    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (this.pedantic.getValue()) {
            if (resultSetHoldability != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                throw SQLError.createSQLException("HOLD_CUSRORS_OVER_COMMIT is only supported holdability level", MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        return createStatement(resultSetType, resultSetConcurrency);
    }

    public int getActiveStatementCount() {
        return this.openStatements.size();
    }

    /**
     * Gets the current auto-commit state
     * 
     * @return Current state of auto-commit
     * @exception SQLException
     *                if an error occurs
     * @see setAutoCommit
     */
    public boolean getAutoCommit() throws SQLException {
        synchronized (getConnectionMutex()) {
            return this.session.getServerSession().isAutoCommit();
        }
    }

    /**
     * Return the connections current catalog name, or null if no catalog name
     * is set, or we dont support catalogs.
     * <p>
     * <b>Note:</b> MySQL's notion of catalogs are individual databases.
     * </p>
     * 
     * @return the current catalog name or null
     * @exception SQLException
     *                if a database access error occurs
     */
    public String getCatalog() throws SQLException {
        synchronized (getConnectionMutex()) {
            return this.database;
        }
    }

    /**
     * @return Returns the characterSetMetadata.
     */
    public String getCharacterSetMetadata() {
        synchronized (getConnectionMutex()) {
            return this.session.getServerSession().getCharacterSetMetadata();
        }
    }

    public int getHoldability() throws SQLException {
        return java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }

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
    public long getIdleFor() {
        synchronized (getConnectionMutex()) {
            return this.session.getIdleFor();
        }
    }

    /**
     * A connection's database is able to provide information describing its
     * tables, its supported SQL grammar, its stored procedures, the
     * capabilities of this connection, etc. This information is made available
     * through a DatabaseMetaData object.
     * 
     * @return a DatabaseMetaData object for this connection
     * @exception SQLException
     *                if a database access error occurs
     */
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
            dbmeta.setMetadataEncoding(getSession().getServerSession().getCharacterSetMetadata());
            dbmeta.setMetadataCollationIndex(getSession().getServerSession().getMetadataCollationIndex());
        }

        return dbmeta;
    }

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

    public ServerVersion getServerVersion() {
        return this.session.getServerSession().getServerVersion();
    }

    /**
     * Get this Connection's current transaction isolation mode.
     * 
     * @return the current TRANSACTION_ mode value
     * @exception SQLException
     *                if a database access error occurs
     */
    public int getTransactionIsolation() throws SQLException {

        synchronized (getConnectionMutex()) {
            if (!this.useLocalSessionState.getValue()) {
                String s = this.session.queryServerVariable(versionMeetsMinimum(8, 0, 3) || (versionMeetsMinimum(5, 7, 20) && !versionMeetsMinimum(8, 0, 0))
                        ? "@@session.transaction_isolation" : "@@session.tx_isolation");

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

    /**
     * JDBC 2.0 Get the type-map object associated with this connection. By
     * default, the map returned is empty.
     * 
     * @return the type map
     * @throws SQLException
     *             if a database error occurs
     */
    public java.util.Map<String, Class<?>> getTypeMap() throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.typeMap == null) {
                this.typeMap = new HashMap<>();
            }

            return this.typeMap;
        }
    }

    public String getURL() {
        return this.origHostInfo.getDatabaseUrl();
    }

    public String getUser() {
        return this.user;
    }

    /**
     * The first warning reported by calls on this Connection is returned.
     * <B>Note:</B> Subsequent warnings will be changed to this
     * java.sql.SQLWarning
     * 
     * @return the first java.sql.SQLWarning or null
     * @exception SQLException
     *                if a database access error occurs
     */
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public boolean hasSameProperties(JdbcConnection c) {
        return this.props.equals(c.getProperties());
    }

    public Properties getProperties() {
        return this.props;
    }

    @Deprecated
    public boolean hasTriedMaster() {
        return this.hasTriedMasterFlag;
    }

    /**
     * Sets varying properties that depend on server information. Called once we
     * have connected to the server.
     * 
     * @param info
     * @throws SQLException
     */
    private void initializePropsFromServer() throws SQLException {
        String connectionInterceptorClasses = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_connectionLifecycleInterceptors)
                .getStringValue();

        this.connectionLifecycleInterceptors = null;

        if (connectionInterceptorClasses != null) {
            try {
                this.connectionLifecycleInterceptors = Util
                        .<ConnectionLifecycleInterceptor> loadClasses(
                                this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_connectionLifecycleInterceptors).getStringValue(),
                                "Connection.badLifecycleInterceptor", getExceptionInterceptor())
                        .stream().map(o -> o.init(this, this.props, this.session.getLog())).collect(Collectors.toList());
            } catch (CJException e) {
                throw SQLExceptionsMapping.translateException(e, getExceptionInterceptor());
            }
        }

        this.session.setSessionVariables();

        this.session.loadServerVariables(this.getConnectionMutex(), this.dbmd.getDriverVersion());

        this.autoIncrementIncrement = this.session.getServerSession().getServerVariable("auto_increment_increment", 1);

        this.session.buildCollationMapping();

        try {
            LicenseConfiguration.checkLicenseType(this.session.getServerSession().getServerVariables());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), MysqlErrorNumbers.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, getExceptionInterceptor());
        }

        this.session.getProtocol().initServerSession();

        checkTransactionIsolationLevel();

        this.session.checkForCharsetMismatch();

        this.session.configureClientCharacterSet(false);

        handleAutoCommitDefaults();

        //
        // We need to figure out what character set metadata and error messages will be returned in, and then map them to Java encoding names
        //
        // We've already set it, and it might be different than what was originally on the server, which is why we use the "special" key to retrieve it
        this.session.getServerSession().configureCharacterSets();

        ((com.mysql.cj.jdbc.DatabaseMetaData) this.dbmd).setMetadataEncoding(getSession().getServerSession().getCharacterSetMetadata());
        ((com.mysql.cj.jdbc.DatabaseMetaData) this.dbmd).setMetadataCollationIndex(getSession().getServerSession().getMetadataCollationIndex());

        //
        // Server can do this more efficiently for us
        //

        setupServerForTruncationChecks();
    }

    /**
     * Resets a default auto-commit value of 0 to 1, as required by JDBC specification.
     * Takes into account that the default auto-commit value of 0 may have been changed on the server via init_connect.
     */
    private void handleAutoCommitDefaults() throws SQLException {
        boolean resetAutoCommitDefault = false;

        // Server Bug#66884 (SERVER_STATUS is always initiated with SERVER_STATUS_AUTOCOMMIT=1) invalidates "elideSetAutoCommits" feature.
        // TODO Turn this feature back on as soon as the server bug is fixed. Consider making it version specific.
        // if (!getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_elideSetAutoCommits).getValue()) {
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

    public boolean isClosed() {
        return this.session.isClosed();
    }

    public boolean isInGlobalTx() {
        return this.isInGlobalTx;
    }

    /**
     * Is this connection connected to the first host in the list if
     * there is a list of servers in the URL?
     * 
     * @return true if this connection is connected to the first in
     *         the list.
     */
    public boolean isMasterConnection() {
        return false; // handled higher up
    }

    /**
     * Tests to see if the connection is in Read Only Mode.
     * 
     * @return true if the connection is read only
     * @exception SQLException
     *                if a database access error occurs
     */
    public boolean isReadOnly() throws SQLException {
        return isReadOnly(true);
    }

    /**
     * Tests to see if the connection is in Read Only Mode.
     * 
     * @param useSessionStatus
     *            in some cases, for example when restoring connection with autoReconnect=true,
     *            we can rely only on saved readOnly state, so use useSessionStatus=false in that case
     * 
     * @return true if the connection is read only
     * @exception SQLException
     *                if a database access error occurs
     */
    public boolean isReadOnly(boolean useSessionStatus) throws SQLException {
        if (useSessionStatus && !this.session.isClosed() && versionMeetsMinimum(5, 6, 5) && !this.useLocalSessionState.getValue()
                && this.readOnlyPropagatesToServer.getValue()) {
            try {
                String s = this.session.queryServerVariable(versionMeetsMinimum(8, 0, 3) || (versionMeetsMinimum(5, 7, 20) && !versionMeetsMinimum(8, 0, 0))
                        ? "@@session.transaction_read_only" : "@@session.tx_read_only");
                if (s != null) {
                    return Integer.parseInt(s) != 0; // mysql has a habit of tri+ state booleans
                }
            } catch (PasswordExpiredException ex) {
                if (this.disconnectOnExpiredPasswords.getValue()) {
                    throw SQLError.createSQLException(Messages.getString("Connection.16"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, ex,
                            getExceptionInterceptor());
                }
            }
        }

        return this.readOnly;
    }

    public boolean isSameResource(JdbcConnection otherConnection) {
        synchronized (getConnectionMutex()) {
            if (otherConnection == null) {
                return false;
            }

            boolean directCompare = true;

            String otherHost = ((ConnectionImpl) otherConnection).origHostToConnectTo;
            String otherOrigDatabase = ((ConnectionImpl) otherConnection).origHostInfo.getDatabase();
            String otherCurrentCatalog = ((ConnectionImpl) otherConnection).database;

            if (!nullSafeCompare(otherHost, this.origHostToConnectTo)) {
                directCompare = false;
            } else if (otherHost != null && otherHost.indexOf(',') == -1 && otherHost.indexOf(':') == -1) {
                // need to check port numbers
                directCompare = (((ConnectionImpl) otherConnection).origPortToConnectTo == this.origPortToConnectTo);
            }

            if (directCompare) {
                if (!nullSafeCompare(otherOrigDatabase, this.origHostInfo.getDatabase()) || !nullSafeCompare(otherCurrentCatalog, this.database)) {
                    directCompare = false;
                }
            }

            if (directCompare) {
                return true;
            }

            // Has the user explicitly set a resourceId?
            String otherResourceId = ((ConnectionImpl) otherConnection).getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_resourceId)
                    .getValue();
            String myResourceId = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_resourceId).getValue();

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

    public int getAutoIncrementIncrement() {
        return this.autoIncrementIncrement;
    }

    /**
     * Is the server configured to use lower-case table names only?
     * 
     * @return true if lower_case_table_names is 'on'
     */
    public boolean lowerCaseTableNames() {
        return this.session.getServerSession().isLowerCaseTableNames();
    }

    /**
     * A driver may convert the JDBC sql grammar into its system's native SQL
     * grammar prior to sending it; nativeSQL returns the native form of the
     * statement that the driver would have sent.
     * 
     * @param sql
     *            a SQL statement that may contain one or more '?' parameter
     *            placeholders
     * @return the native form of this statement
     * @exception SQLException
     *                if a database access error occurs
     */
    public String nativeSQL(String sql) throws SQLException {
        if (sql == null) {
            return null;
        }

        Object escapedSqlResult = EscapeProcessor.escapeSQL(sql, getMultiHostSafeProxy().getSession().getServerSession().getDefaultTimeZone(),
                getMultiHostSafeProxy().getSession().getServerSession().getCapabilities().serverSupportsFracSecs(), getExceptionInterceptor());

        if (escapedSqlResult instanceof String) {
            return (String) escapedSqlResult;
        }

        return ((EscapeProcessorResult) escapedSqlResult).escapedSql;
    }

    private CallableStatement parseCallableStatement(String sql) throws SQLException {
        Object escapedSqlResult = EscapeProcessor.escapeSQL(sql, getMultiHostSafeProxy().getSession().getServerSession().getDefaultTimeZone(),
                getMultiHostSafeProxy().getSession().getServerSession().getCapabilities().serverSupportsFracSecs(), getExceptionInterceptor());

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

    /**
     * Detect if the connection is still good
     * 
     * @throws SQLException
     *             if the ping fails
     */
    public void ping() throws SQLException {
        pingInternal(true, 0);
    }

    @Override
    public void pingInternal(boolean checkForClosedConnection, int timeoutMillis) throws SQLException {
        this.session.ping(checkForClosedConnection, timeoutMillis);
    }

    /**
     * @param sql
     * @throws SQLException
     */
    public java.sql.CallableStatement prepareCall(String sql) throws SQLException {

        return prepareCall(sql, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    /**
     * JDBC 2.0 Same as prepareCall() above, but allows the default result set
     * type and result set concurrency type to be overridden.
     * 
     * @param sql
     *            the SQL representing the callable statement
     * @param resultSetType
     *            a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency
     *            a concurrency type, see ResultSet.CONCUR_XXX
     * @return a new CallableStatement object containing the pre-compiled SQL
     *         statement
     * @exception SQLException
     *                if a database-access error occurs.
     */
    public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        CallableStatement cStmt = null;

        if (!this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheCallableStmts).getValue()) {

            cStmt = parseCallableStatement(sql);
        } else {
            synchronized (this.parsedCallableStatementCache) {
                CompoundCacheKey key = new CompoundCacheKey(getCatalog(), sql);

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

    public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (this.pedantic.getValue()) {
            if (resultSetHoldability != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                throw SQLError.createSQLException(Messages.getString("Connection.17"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        CallableStatement cStmt = (com.mysql.cj.jdbc.CallableStatement) prepareCall(sql, resultSetType, resultSetConcurrency);

        return cStmt;
    }

    /**
     * A SQL statement with or without IN parameters can be pre-compiled and
     * stored in a PreparedStatement object. This object can then be used to
     * efficiently execute this statement multiple times.
     * <p>
     * <B>Note:</B> This method is optimized for handling parametric SQL statements that benefit from precompilation if the driver supports precompilation. In
     * this case, the statement is not sent to the database until the PreparedStatement is executed. This has no direct effect on users; however it does affect
     * which method throws certain java.sql.SQLExceptions
     * </p>
     * <p>
     * MySQL does not support precompilation of statements, so they are handled by the driver.
     * </p>
     * 
     * @param sql
     *            a SQL statement that may contain one or more '?' IN parameter
     *            placeholders
     * @return a new PreparedStatement object containing the pre-compiled
     *         statement.
     * @exception SQLException
     *                if a database access error occurs.
     */
    public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException {
        return prepareStatement(sql, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY);
    }

    public java.sql.PreparedStatement prepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);

        ((ClientPreparedStatement) pStmt).setRetrieveGeneratedKeys(autoGenKeyIndex == java.sql.Statement.RETURN_GENERATED_KEYS);

        return pStmt;
    }

    /**
     * JDBC 2.0 Same as prepareStatement() above, but allows the default result
     * set type and result set concurrency type to be overridden.
     * 
     * @param sql
     *            the SQL query containing place holders
     * @param resultSetType
     *            a result set type, see ResultSet.TYPE_XXX
     * @param resultSetConcurrency
     *            a concurrency type, see ResultSet.CONCUR_XXX
     * @return a new PreparedStatement object containing the pre-compiled SQL
     *         statement
     * @exception SQLException
     *                if a database-access error occurs.
     */
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
                        }

                        if (pStmt == null) {
                            try {
                                pStmt = ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.database, resultSetType,
                                        resultSetConcurrency);
                                if (sql.length() < this.prepStmtCacheSqlLimit.getValue()) {
                                    ((com.mysql.cj.jdbc.ServerPreparedStatement) pStmt).isCached = true;
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

    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (this.pedantic.getValue()) {
            if (resultSetHoldability != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                throw SQLError.createSQLException(Messages.getString("Connection.17"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public java.sql.PreparedStatement prepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);

        ((ClientPreparedStatement) pStmt).setRetrieveGeneratedKeys((autoGenKeyIndexes != null) && (autoGenKeyIndexes.length > 0));

        return pStmt;
    }

    public java.sql.PreparedStatement prepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);

        ((ClientPreparedStatement) pStmt).setRetrieveGeneratedKeys((autoGenKeyColNames != null) && (autoGenKeyColNames.length > 0));

        return pStmt;
    }

    /**
     * Closes connection and frees resources.
     * 
     * @param calledExplicitly
     *            is this being called from close()
     * @param issueRollback
     *            should a rollback() be issued?
     * @throws SQLException
     *             if an error occurs
     */
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

                this.session.reportMetrics();

                if (this.useUsageAdvisor.getValue()) {
                    if (!calledExplicitly) {
                        this.session.getProfilerEventHandler()
                                .consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_WARN, "", this.getCatalog(), this.session.getThreadId(), -1, -1,
                                        System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, this.pointOfOrigin, Messages.getString("Connection.18")));
                    }

                    long connectionLifeTime = System.currentTimeMillis() - this.session.getConnectionCreationTimeMillis();

                    if (connectionLifeTime < 500) {
                        this.session.getProfilerEventHandler()
                                .consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_WARN, "", this.getCatalog(), this.session.getThreadId(), -1, -1,
                                        System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, this.pointOfOrigin, Messages.getString("Connection.19")));
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
                for (int i = 0; i < this.queryInterceptors.size(); i++) {
                    this.queryInterceptors.get(i).destroy();
                }
            }

            if (this.exceptionInterceptor != null) {
                this.exceptionInterceptor.destroy();
            }
        } finally {
            ProfilerEventHandlerFactory.removeInstance(this.session);

            this.openStatements.clear();
            this.queryInterceptors = null;
            this.exceptionInterceptor = null;
            this.nullStatementResultSetFactory = null;
        }

        if (sqlEx != null) {
            throw sqlEx;
        }

    }

    public void recachePreparedStatement(JdbcPreparedStatement pstmt) throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.cachePrepStmts.getValue() && pstmt.isPoolable()) {
                synchronized (this.serverSideStatementCache) {
                    Object oldServerPrepStmt = this.serverSideStatementCache.put(
                            new CompoundCacheKey(pstmt.getCurrentCatalog(), ((PreparedQuery<?>) pstmt.getQuery()).getOriginalSql()),
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

    public void decachePreparedStatement(JdbcPreparedStatement pstmt) throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.cachePrepStmts.getValue() && pstmt.isPoolable()) {
                synchronized (this.serverSideStatementCache) {
                    this.serverSideStatementCache
                            .remove(new CompoundCacheKey(pstmt.getCurrentCatalog(), ((PreparedQuery<?>) pstmt.getQuery()).getOriginalSql()));
                }
            }
        }
    }

    /**
     * Register a Statement instance as open.
     * 
     * @param stmt
     *            the Statement instance to remove
     */
    public void registerStatement(JdbcStatement stmt) {
        this.openStatements.addIfAbsent(stmt);
    }

    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        // this is a no-op
    }

    /**
     * Resets the server-side state of this connection. Doesn't work if isParanoid() is set (it will become a
     * no-op in this case). Usually only used from connection pooling code.
     * 
     * @throws SQLException
     *             if the operation fails while resetting server state.
     */
    public void resetServerState() throws SQLException {
        if (!this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_paranoid).getValue() && (this.session != null)) {
            changeUser(this.user, this.password);
        }
    }

    /**
     * The method rollback() drops all changes made since the previous
     * commit/rollback and releases any database locks currently held by the
     * Connection.
     * 
     * @exception SQLException
     *                if a database access error occurs
     * @see commit
     */
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

            this.session.execSQL(null, "rollback", -1, null, false, this.nullStatementResultSetFactory, this.database, null, false);

        }
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql) throws SQLException {
        String nativeSql = this.processEscapeCodesForPrepStmts.getValue() ? nativeSQL(sql) : sql;

        return ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.getCatalog(), DEFAULT_RESULT_SET_TYPE,
                DEFAULT_RESULT_SET_CONCURRENCY);
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        String nativeSql = this.processEscapeCodesForPrepStmts.getValue() ? nativeSQL(sql) : sql;

        ClientPreparedStatement pStmt = ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.getCatalog(), DEFAULT_RESULT_SET_TYPE,
                DEFAULT_RESULT_SET_CONCURRENCY);

        pStmt.setRetrieveGeneratedKeys(autoGenKeyIndex == java.sql.Statement.RETURN_GENERATED_KEYS);

        return pStmt;
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        String nativeSql = this.processEscapeCodesForPrepStmts.getValue() ? nativeSQL(sql) : sql;

        return ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.getCatalog(), resultSetType, resultSetConcurrency);
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        if (this.pedantic.getValue()) {
            if (resultSetHoldability != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                throw SQLError.createSQLException(Messages.getString("Connection.17"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        return serverPrepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {

        ClientPreparedStatement pStmt = (ClientPreparedStatement) serverPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyIndexes != null) && (autoGenKeyIndexes.length > 0));

        return pStmt;
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        ClientPreparedStatement pStmt = (ClientPreparedStatement) serverPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyColNames != null) && (autoGenKeyColNames.length > 0));

        return pStmt;
    }

    /**
     * If a connection is in auto-commit mode, than all its SQL statements will
     * be executed and committed as individual transactions. Otherwise, its SQL
     * statements are grouped into transactions that are terminated by either
     * commit() or rollback(). By default, new connections are in auto-commit
     * mode. The commit occurs when the statement completes or the next execute
     * occurs, whichever comes first. In the case of statements returning a
     * ResultSet, the statement completes when the last row of the ResultSet has
     * been retrieved or the ResultSet has been closed. In advanced cases, a
     * single statement may return multiple results as well as output parameter
     * values. Here the commit occurs when all results and output param values
     * have been retrieved.
     * 
     * @param autoCommitFlag
     *            true enables auto-commit; false disables it
     * @exception SQLException
     *                if a database access error occurs
     */
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

            try {
                boolean needsSetOnServer = true;

                if (this.useLocalSessionState.getValue() && this.session.getServerSession().isAutoCommit() == autoCommitFlag) {
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
                            this.database, null, false);
                }
            } finally {
                if (this.autoReconnectForPools.getValue()) {
                    this.autoReconnect.setValue(false);
                }
            }

            return;
        }
    }

    /**
     * A sub-space of this Connection's database may be selected by setting a
     * catalog name. If the driver does not support catalogs, it will silently
     * ignore this request
     * <p>
     * <b>Note:</b> MySQL's notion of catalogs are individual databases.
     * </p>
     * 
     * @param catalog
     *            the database for this connection to use
     * @throws SQLException
     *             if a database access error occurs
     */
    public void setCatalog(final String catalog) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            if (catalog == null) {
                throw SQLError.createSQLException("Catalog can not be null", MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            if (this.connectionLifecycleInterceptors != null) {
                IterateBlock<ConnectionLifecycleInterceptor> iter = new IterateBlock<ConnectionLifecycleInterceptor>(
                        this.connectionLifecycleInterceptors.iterator()) {

                    @Override
                    void forEach(ConnectionLifecycleInterceptor each) throws SQLException {
                        if (!each.setCatalog(catalog)) {
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
                    if (this.database.equalsIgnoreCase(catalog)) {
                        return;
                    }
                } else {
                    if (this.database.equals(catalog)) {
                        return;
                    }
                }
            }

            String quotedId = this.session.getIdentifierQuoteString();

            if ((quotedId == null) || quotedId.equals(" ")) {
                quotedId = "";
            }

            StringBuilder query = new StringBuilder("USE ");
            query.append(StringUtils.quoteIdentifier(catalog, quotedId, this.pedantic.getValue()));

            this.session.execSQL(null, query.toString(), -1, null, false, this.nullStatementResultSetFactory, this.database, null, false);

            this.database = catalog;
        }
    }

    /**
     * @param failedOver
     *            The failedOver to set.
     */
    public void setFailedOver(boolean flag) {
        // handled higher up
    }

    public void setHoldability(int arg0) throws SQLException {
        // do nothing
    }

    public void setInGlobalTx(boolean flag) {
        this.isInGlobalTx = flag;
    }

    /**
     * You can put a connection in read-only mode as a hint to enable database
     * optimizations <B>Note:</B> setReadOnly cannot be called while in the
     * middle of a transaction
     * 
     * @param readOnlyFlag
     *            -
     *            true enables read-only mode; false disables it
     * @exception SQLException
     *                if a database access error occurs
     */
    public void setReadOnly(boolean readOnlyFlag) throws SQLException {

        setReadOnlyInternal(readOnlyFlag);
    }

    public void setReadOnlyInternal(boolean readOnlyFlag) throws SQLException {
        synchronized (getConnectionMutex()) {
            // note this this is safe even inside a transaction
            if (this.readOnlyPropagatesToServer.getValue() && versionMeetsMinimum(5, 6, 5)) {
                if (!this.useLocalSessionState.getValue() || (readOnlyFlag != this.readOnly)) {
                    this.session.execSQL(null, "set session transaction " + (readOnlyFlag ? "read only" : "read write"), -1, null, false,
                            this.nullStatementResultSetFactory, this.database, null, false);
                }
            }

            this.readOnly = readOnlyFlag;
        }
    }

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

    public java.sql.Savepoint setSavepoint(String name) throws SQLException {
        synchronized (getConnectionMutex()) {
            MysqlSavepoint savepoint = new MysqlSavepoint(name, getExceptionInterceptor());

            setSavepoint(savepoint);

            return savepoint;
        }
    }

    /**
     * @param level
     * @throws SQLException
     */
    public void setTransactionIsolation(int level) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            String sql = null;

            boolean shouldSendSet = false;

            if (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_alwaysSendSetIsolation).getValue()) {
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

                this.session.execSQL(null, sql, -1, null, false, this.nullStatementResultSetFactory, this.database, null, false);

                this.isolationLevel = level;
            }
        }
    }

    /**
     * JDBC 2.0 Install a type-map object as the default type-map for this
     * connection
     * 
     * @param map
     *            the type mapping
     * @throws SQLException
     *             if a database error occurs.
     */
    public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLException {
        synchronized (getConnectionMutex()) {
            this.typeMap = map;
        }
    }

    private void setupServerForTruncationChecks() throws SQLException {
        synchronized (getConnectionMutex()) {
            ModifiableProperty<Boolean> jdbcCompliantTruncation = this.propertySet
                    .<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation);
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

                    this.session.execSQL(null, commandBuf.toString(), -1, null, false, this.nullStatementResultSetFactory, this.database, null, false);

                    jdbcCompliantTruncation.setValue(false); // server's handling this for us now
                } else if (strictTransTablesIsSet) {
                    // We didn't set it, but someone did, so we piggy back on it
                    jdbcCompliantTruncation.setValue(false); // server's handling this for us now
                }
            }
        }
    }

    /**
     * Used by MiniAdmin to shutdown a MySQL server
     * 
     * @throws SQLException
     *             if the command can not be issued.
     */
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

    /**
     * Remove the given statement from the list of open statements
     * 
     * @param stmt
     *            the Statement instance to remove
     */
    public void unregisterStatement(JdbcStatement stmt) {
        this.openStatements.remove(stmt);
    }

    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        return this.session.versionMeetsMinimum(major, minor, subminor);
    }

    /**
     * Returns cached metadata (or null if not cached) for the given query,
     * which must match _exactly_.
     * 
     * This method is synchronized by the caller on getMutex(), so if
     * calling this method from internal code in the driver, make sure it's
     * synchronized on the mutex that guards communication with the server.
     * 
     * @param sql
     *            the query that is the key to the cache
     * 
     * @return metadata cached for the given SQL, or none if it doesn't
     *         exist.
     */
    public CachedResultSetMetaData getCachedMetaData(String sql) {
        if (this.resultSetMetadataCache != null) {
            synchronized (this.resultSetMetadataCache) {
                return this.resultSetMetadataCache.get(sql);
            }
        }

        return null; // no cache exists
    }

    /**
     * Caches CachedResultSetMetaData that has been placed in the cache using
     * the given SQL as a key.
     * 
     * This method is synchronized by the caller on getMutex(), so if
     * calling this method from internal code in the driver, make sure it's
     * synchronized on the mutex that guards communication with the server.
     * 
     * @param sql
     *            the query that the metadata pertains too.
     * @param cachedMetaData
     *            metadata (if it exists) to populate the cache.
     * @param resultSet
     *            the result set to retreive metadata from, or apply to.
     * 
     * @throws SQLException
     */
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

    /**
     * Returns the comment that will be prepended to all statements
     * sent to the server.
     * 
     * @return the comment that will be prepended to all statements
     *         sent to the server.
     */
    public String getStatementComment() {
        return this.session.getProtocol().getQueryComment();
    }

    /**
     * Sets the comment that will be prepended to all statements
     * sent to the server. Do not use slash-star or star-slash tokens
     * in the comment as these will be added by the driver itself.
     * 
     * @param comment
     *            the comment that will be prepended to all statements
     *            sent to the server.
     */
    public void setStatementComment(String comment) {
        this.session.getProtocol().setQueryComment(comment);
    }

    public void transactionBegun() {
        synchronized (getConnectionMutex()) {
            if (this.connectionLifecycleInterceptors != null) {
                this.connectionLifecycleInterceptors.stream().forEach(ConnectionLifecycleInterceptor::transactionBegun);
            }
        }
    }

    public void transactionCompleted() {
        synchronized (getConnectionMutex()) {
            if (this.connectionLifecycleInterceptors != null) {
                this.connectionLifecycleInterceptors.stream().forEach(ConnectionLifecycleInterceptor::transactionCompleted);
            }
        }
    }

    public boolean storesLowerCaseTableName() {
        return this.session.getServerSession().storesLowerCaseTableNames();
    }

    private ExceptionInterceptor exceptionInterceptor;

    @Override
    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

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

    /**
     * Returns the sql select limit max-rows for this session.
     */
    public int getSessionMaxRows() {
        synchronized (getConnectionMutex()) {
            return this.session.getSessionMaxRows();
        }
    }

    /**
     * Sets the sql select limit max-rows for this session if different from current.
     * 
     * @param max
     *            the new max-rows value to set.
     * @throws SQLException
     *             if a database error occurs issuing the statement that sets the limit.
     */
    public void setSessionMaxRows(int max) throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.session.getSessionMaxRows() != max) {
                this.session.setSessionMaxRows(max);
                this.session.execSQL(null, "SET SQL_SELECT_LIMIT=" + (this.session.getSessionMaxRows() == -1 ? "DEFAULT" : this.session.getSessionMaxRows()),
                        -1, null, false, this.nullStatementResultSetFactory, this.database, null, false);
            }
        }
    }

    // until we flip catalog/schema, this is a no-op
    public void setSchema(String schema) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
        }
    }

    public String getSchema() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            return null;
        }
    }

    /**
     * Terminates an open connection. Calling <code>abort</code> results in:
     * <ul>
     * <li>The connection marked as closed
     * <li>Closes any physical connection to the database
     * <li>Releases resources used by the connection
     * <li>Insures that any thread that is currently accessing the connection will either progress to completion or throw an <code>SQLException</code>.
     * </ul>
     * <p>
     * Calling <code>abort</code> marks the connection closed and releases any resources. Calling <code>abort</code> on a closed connection is a no-op.
     * <p>
     * It is possible that the aborting and releasing of the resources that are held by the connection can take an extended period of time. When the
     * <code>abort</code> method returns, the connection will have been marked as closed and the <code>Executor</code> that was passed as a parameter to abort
     * may still be executing tasks to release resources.
     * <p>
     * This method checks to see that there is an <code>SQLPermission</code> object before allowing the method to proceed. If a <code>SecurityManager</code>
     * exists and its <code>checkPermission</code> method denies calling <code>abort</code>, this method throws a <code>java.lang.SecurityException</code>.
     * 
     * @param executor
     *            The <code>Executor</code> implementation which will
     *            be used by <code>abort</code>.
     * @throws java.sql.SQLException
     *             if a database access error occurs or
     *             the {@code executor} is {@code null},
     * @throws java.lang.SecurityException
     *             if a security manager exists and its <code>checkPermission</code> method denies calling <code>abort</code>
     * @see SecurityManager#checkPermission
     * @see Executor
     * @since 1.7
     */
    public void abort(Executor executor) throws SQLException {
        SecurityManager sec = System.getSecurityManager();

        if (sec != null) {
            sec.checkPermission(ABORT_PERM);
        }

        if (executor == null) {
            throw SQLError.createSQLException(Messages.getString("Connection.26"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }

        executor.execute(new Runnable() {

            public void run() {
                try {
                    abortInternal();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

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

        public void run() {
            JdbcConnection conn = this.connRef.get();
            if (conn != null) {
                synchronized (conn.getConnectionMutex()) {
                    ((NativeSession) conn.getSession()).setSocketTimeout(this.milliseconds);
                }
            }
        }
    }

    public int getNetworkTimeout() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
            return this.session.getSocketTimeout();
        }
    }

    public Clob createClob() {
        return new com.mysql.cj.jdbc.Clob(getExceptionInterceptor());
    }

    public Blob createBlob() {
        return new com.mysql.cj.jdbc.Blob(getExceptionInterceptor());
    }

    public NClob createNClob() {
        return new com.mysql.cj.jdbc.NClob(getExceptionInterceptor());
    }

    public SQLXML createSQLXML() throws SQLException {
        return new MysqlSQLXML(getExceptionInterceptor());
    }

    /**
     * Returns true if the connection has not been closed and is still valid.
     * The driver shall submit a query on the connection or use some other
     * mechanism that positively verifies the connection is still valid when
     * this method is called.
     * <p>
     * The query submitted by the driver to validate the connection shall be executed in the context of the current transaction.
     * 
     * @param timeout
     *            - The time in seconds to wait for the database operation
     *            used to validate the connection to complete. If
     *            the timeout period expires before the operation
     *            completes, this method returns false. A value of
     *            0 indicates a timeout is not applied to the
     *            database operation.
     *            <p>
     * @return true if the connection is valid, false otherwise
     * @exception SQLException
     *                if the value supplied for <code>timeout</code> is less then 0
     * @since 1.6
     */
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

    public ClientInfoProvider getClientInfoProviderImpl() throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.infoProvider == null) {
                String clientInfoProvider = this.propertySet.getStringReadableProperty(PropertyDefinitions.PNAME_clientInfoProvider).getStringValue();
                try {
                    try {
                        this.infoProvider = (ClientInfoProvider) Util.getInstance(clientInfoProvider, new Class<?>[0], new Object[0],
                                getExceptionInterceptor());
                    } catch (CJException ex) {
                        if (ex.getCause() instanceof ClassCastException) {
                            // try with package name prepended
                            try {
                                this.infoProvider = (ClientInfoProvider) Util.getInstance("com.mysql.cj.jdbc." + clientInfoProvider, new Class<?>[0],
                                        new Object[0], getExceptionInterceptor());
                            } catch (CJException e) {
                                throw SQLExceptionsMapping.translateException(e, getExceptionInterceptor());
                            }
                        }
                    }
                } catch (ClassCastException cce) {
                    throw SQLError.createSQLException(Messages.getString("Connection.ClientInfoNotImplemented", new Object[] { clientInfoProvider }),
                            MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
                }

                this.infoProvider.initialize(this, this.props);
            }

            return this.infoProvider;
        }
    }

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

    public String getClientInfo(String name) throws SQLException {
        return getClientInfoProviderImpl().getClientInfo(this, name);
    }

    public Properties getClientInfo() throws SQLException {
        return getClientInfoProviderImpl().getClientInfo(this);
    }

    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    /**
     * Returns an object that implements the given interface to allow access to non-standard methods,
     * or standard methods not exposed by the proxy.
     * The result may be either the object found to implement the interface or a proxy for that object.
     * If the receiver implements the interface then that is the object. If the receiver is a wrapper
     * and the wrapped object implements the interface then that is the object. Otherwise the object is
     * the result of calling <code>unwrap</code> recursively on the wrapped object. If the receiver is not a
     * wrapper and does not implement the interface, then an <code>SQLException</code> is thrown.
     * 
     * @param iface
     *            A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the actual implementing object.
     * @throws java.sql.SQLException
     *             If no object found that implements the interface
     * @since 1.6
     */
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

    /**
     * Returns true if this either implements the interface argument or is directly or indirectly a wrapper
     * for an object that does. Returns false otherwise. If this implements the interface then return true,
     * else if this is a wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped
     * object. If this does not implement the interface and is not a wrapper, return false.
     * This method should be implemented as a low-cost operation compared to <code>unwrap</code> so that
     * callers can use this method to avoid expensive <code>unwrap</code> calls that may fail. If this method
     * returns true then calling <code>unwrap</code> with the same argument should succeed.
     * 
     * @param interfaces
     *            a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly wraps an object that does.
     * @throws java.sql.SQLException
     *             if an error occurs while determining whether this is a wrapper
     *             for an object with the given interface.
     * @since 1.6
     */
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

}
