/*
  Copyright (c) 2002, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc;

import java.lang.reflect.InvocationHandler;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
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
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Stack;
import java.util.Timer;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

import com.mysql.cj.api.CacheAdapter;
import com.mysql.cj.api.CacheAdapterFactory;
import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.api.conf.ModifiableProperty;
import com.mysql.cj.api.conf.ReadableProperty;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.io.ServerSession;
import com.mysql.cj.api.jdbc.ClientInfoProvider;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.jdbc.Statement;
import com.mysql.cj.api.jdbc.interceptors.ConnectionLifecycleInterceptor;
import com.mysql.cj.api.jdbc.interceptors.StatementInterceptor;
import com.mysql.cj.api.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.core.CharsetMapping;
import com.mysql.cj.core.Constants;
import com.mysql.cj.core.LicenseConfiguration;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.conf.url.HostInfo;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.ConnectionIsClosedException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.core.exceptions.PasswordExpiredException;
import com.mysql.cj.core.exceptions.UnableToConnectException;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.SocksProxySocketFactory;
import com.mysql.cj.core.log.LogFactory;
import com.mysql.cj.core.log.StandardLogger;
import com.mysql.cj.core.profiler.ProfilerEventHandlerFactory;
import com.mysql.cj.core.profiler.ProfilerEventImpl;
import com.mysql.cj.core.util.LRUCache;
import com.mysql.cj.core.util.LogUtils;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.Util;
import com.mysql.cj.jdbc.PreparedStatement.ParseInfo;
import com.mysql.cj.jdbc.exceptions.CommunicationsException;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.ha.MultiHostMySQLConnection;
import com.mysql.cj.jdbc.interceptors.NoSubInterceptorWrapper;
import com.mysql.cj.jdbc.io.ResultSetFactory;
import com.mysql.cj.jdbc.result.CachedResultSetMetaData;
import com.mysql.cj.jdbc.result.UpdatableResultSet;
import com.mysql.cj.jdbc.util.ResultSetUtil;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.MysqlaSession;
import com.mysql.cj.mysqla.MysqlaUtils;

/**
 * A Connection represents a session with a specific database. Within the context of a Connection, SQL statements are executed and results are returned.
 * 
 * <P>
 * A Connection's database is able to provide information describing its tables, its supported SQL grammar, its stored procedures, the capabilities of this
 * connection, etc. This information is obtained with the getMetaData method.
 * </p>
 */
public class ConnectionImpl extends AbstractJdbcConnection implements JdbcConnection {

    private static final long serialVersionUID = 2877471301981509474L;

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

    public Object getConnectionMutex() {
        return (this.realProxy != null) ? this.realProxy : getProxy();
    }

    public class ExceptionInterceptorChain implements ExceptionInterceptor {
        List<ExceptionInterceptor> interceptors;

        ExceptionInterceptorChain(String interceptorClasses) {
            this.interceptors = Util.<ExceptionInterceptor> loadClasses(interceptorClasses, "Connection.BadExceptionInterceptor", this).stream()
                    .map(o -> o.init(ConnectionImpl.this.props, ConnectionImpl.this.getSession().getLog())).collect(Collectors.toList());
        }

        void addRingZero(ExceptionInterceptor interceptor) throws SQLException {
            this.interceptors.add(0, interceptor);
        }

        public Exception interceptException(Exception sqlEx) {
            if (this.interceptors != null) {
                Iterator<ExceptionInterceptor> iter = this.interceptors.iterator();

                while (iter.hasNext()) {
                    sqlEx = iter.next().interceptException(sqlEx);
                }
            }

            return sqlEx;
        }

        public void destroy() {
            if (this.interceptors != null) {
                Iterator<ExceptionInterceptor> iter = this.interceptors.iterator();

                while (iter.hasNext()) {
                    iter.next().destroy();
                }
            }

        }

        public ExceptionInterceptor init(Properties properties, Log log) {
            if (this.interceptors != null) {
                Iterator<ExceptionInterceptor> iter = this.interceptors.iterator();

                while (iter.hasNext()) {
                    iter.next().init(properties, log);
                }
            }
            return this;
        }

        public List<ExceptionInterceptor> getInterceptors() {
            return this.interceptors;
        }

    }

    /**
     * Used as a key for caching callable statements which (may) depend on
     * current catalog...In 5.0.x, they don't (currently), but stored procedure
     * names soon will, so current catalog is a (hidden) component of the name.
     */
    static class CompoundCacheKey {
        String componentOne;

        String componentTwo;

        int hashCode;

        CompoundCacheKey(String partOne, String partTwo) {
            this.componentOne = partOne;
            this.componentTwo = partTwo;

            // Handle first component (in most cases, currentCatalog being NULL....
            this.hashCode = (((this.componentOne != null) ? this.componentOne : "") + this.componentTwo).hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof CompoundCacheKey) {
                CompoundCacheKey another = (CompoundCacheKey) obj;

                boolean firstPartEqual = false;

                if (this.componentOne == null) {
                    firstPartEqual = (another.componentOne == null);
                } else {
                    firstPartEqual = this.componentOne.equals(another.componentOne);
                }

                return (firstPartEqual && this.componentTwo.equals(another.componentTwo));
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

    private final static int HISTOGRAM_BUCKETS = 20;

    /**
     * Map mysql transaction isolation level name to
     * java.sql.Connection.TRANSACTION_XXX
     */
    private static Map<String, Integer> mapTransIsolationNameToValue = null;

    protected static Map<?, ?> roundRobinStatsMap;

    /**
     * Actual collation index to collation name map for given server URLs.
     */
    private static final Map<String, Map<Number, String>> dynamicIndexToCollationMapByUrl = new HashMap<String, Map<Number, String>>();

    /**
     * Actual collation index to mysql charset name map for given server URLs.
     */
    private static final Map<String, Map<Integer, String>> dynamicIndexToCharsetMapByUrl = new HashMap<String, Map<Integer, String>>();

    /**
     * Actual collation index to mysql charset name map of user defined charsets for given server URLs.
     */
    private static final Map<String, Map<Integer, String>> customIndexToCharsetMapByUrl = new HashMap<String, Map<Integer, String>>();

    /**
     * Actual mysql charset name to mblen map of user defined charsets for given server URLs.
     */
    private static final Map<String, Map<String, Integer>> customCharsetToMblenMapByUrl = new HashMap<String, Map<String, Integer>>();

    private CacheAdapter<String, Map<String, String>> serverConfigCache;

    private long queryTimeCount;
    private double queryTimeSum;
    private double queryTimeSumSquares;
    private double queryTimeMean;

    private transient Timer cancelTimer;

    private List<ConnectionLifecycleInterceptor> connectionLifecycleInterceptors;

    private static final int DEFAULT_RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;

    private static final int DEFAULT_RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;

    static {
        mapTransIsolationNameToValue = new HashMap<String, Integer>(8);
        mapTransIsolationNameToValue.put("READ-UNCOMMITED", TRANSACTION_READ_UNCOMMITTED);
        mapTransIsolationNameToValue.put("READ-UNCOMMITTED", TRANSACTION_READ_UNCOMMITTED);
        mapTransIsolationNameToValue.put("READ-COMMITTED", TRANSACTION_READ_COMMITTED);
        mapTransIsolationNameToValue.put("REPEATABLE-READ", TRANSACTION_REPEATABLE_READ);
        mapTransIsolationNameToValue.put("SERIALIZABLE", TRANSACTION_SERIALIZABLE);
    }

    protected static SQLException appendMessageToException(SQLException sqlEx, String messageToAppend, ExceptionInterceptor interceptor) {
        String origMessage = sqlEx.getMessage();
        String sqlState = sqlEx.getSQLState();
        int vendorErrorCode = sqlEx.getErrorCode();

        StringBuilder messageBuf = new StringBuilder(origMessage.length() + messageToAppend.length());
        messageBuf.append(origMessage);
        messageBuf.append(messageToAppend);

        SQLException sqlExceptionWithNewMessage = SQLError.createSQLException(messageBuf.toString(), sqlState, vendorErrorCode, interceptor);
        sqlExceptionWithNewMessage.setStackTrace(sqlEx.getStackTrace());

        return sqlExceptionWithNewMessage;
    }

    public Timer getCancelTimer() {
        synchronized (getConnectionMutex()) {
            if (this.cancelTimer == null) {
                this.cancelTimer = new Timer("MySQL Statement Cancellation Timer", Boolean.TRUE);
            }

            return this.cancelTimer;
        }
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

    /** Are we in autoCommit mode? */
    private boolean autoCommit = true;

    /** A cache of SQL to parsed prepared statement parameters. */
    private CacheAdapter<String, ParseInfo> cachedPreparedStatementParams;

    /** The point in time when this connection was created */
    private long connectionCreationTimeMillis = 0;

    /** ID used when profiling */
    private long connectionId;

    /** The database we're currently using (called Catalog in JDBC terms). */
    private String database = null;

    /** Internal DBMD to use for various database-version specific features */
    private DatabaseMetaData dbmd = null;

    /** Why was this connection implicitly closed, if known? (for diagnostics) */
    private Throwable forceClosedReason;

    private MysqlaSession session = null;

    /** Has this connection been closed? */
    private boolean isClosed = true;

    /** Is this connection associated with a global tx? */
    private boolean isInGlobalTx = false;

    /** isolation level */
    private int isolationLevel = java.sql.Connection.TRANSACTION_READ_COMMITTED;

    /** When did the last query finish? */
    private long lastQueryFinishedTime = 0;

    /**
     * If gathering metrics, what was the execution time of the longest query so
     * far ?
     */
    private long longestQueryTimeMs = 0;

    /** Is the server configured to use lower-case table names only? */
    private boolean lowerCaseTableNames = false;

    /** When did the master fail? */
    //	private long masterFailTimeMillis = 0L;

    private long maximumNumberTablesAccessed = 0;

    /** When was the last time we reported metrics? */
    private long metricsLastReportedMs;

    private long minimumNumberTablesAccessed = Long.MAX_VALUE;

    /** Does this connection need to be tested? */
    private boolean needsPing = false;

    private boolean noBackslashEscapes = false;

    private long numberOfPreparedExecutes = 0;

    private long numberOfPrepares = 0;

    private long numberOfQueriesIssued = 0;

    private long numberOfResultSetsCreated = 0;

    private long[] numTablesMetricsHistBreakpoints;

    private int[] numTablesMetricsHistCounts;

    private long[] oldHistBreakpoints = null;

    private int[] oldHistCounts = null;

    /**
     * An array of currently open statements.
     * Copy-on-write used here to avoid ConcurrentModificationException when statements unregister themselves while we iterate over the list.
     */
    private final CopyOnWriteArrayList<Statement> openStatements = new CopyOnWriteArrayList<Statement>();

    private LRUCache parsedCallableStatementCache;

    /** The password we used */
    private String password = null;

    private long[] perfMetricsHistBreakpoints;

    private int[] perfMetricsHistCounts;

    /** Point of origin where this Connection was created */
    private String pointOfOrigin;

    /** Properties for this connection specified by user */
    protected Properties props = null;

    /** Should we retrieve 'info' messages from the server? */
    private boolean readInfoMsg = false;

    /** Are we in read-only mode? */
    private boolean readOnly = false;

    /** Cache of ResultSet metadata */
    protected LRUCache resultSetMetadataCache;

    private long shortestQueryTimeMs = Long.MAX_VALUE;

    private double totalQueryTimeMs = 0;

    /**
     * The type map for UDTs (not implemented, but used by some third-party
     * vendors, most notably IBM WebSphere)
     */
    private Map<String, Class<?>> typeMap;

    /** Has ANSI_QUOTES been enabled on the server? */
    private boolean useAnsiQuotes = false;

    /** The user we're connected as */
    private String user = null;

    /**
     * Should we use server-side prepared statements? (auto-detected, but can be
     * disabled by user)
     */
    private boolean useServerPreparedStmts = false;

    private LRUCache serverSideStatementCheckCache;
    private LRUCache serverSideStatementCache;

    private HostInfo origHostInfo;

    private String origHostToConnectTo;

    // we don't want to be able to publicly clone this...

    private int origPortToConnectTo;

    /*
     * For testing failover scenarios
     */
    private boolean hasTriedMasterFlag = false;

    /**
     * The comment (if any) that we'll prepend to all statements
     * sent to the server (to show up in "SHOW PROCESSLIST")
     */
    private String statementComment = null;

    private boolean storesLowerCaseTableName;

    private List<StatementInterceptor> statementInterceptors;

    /**
     * If a CharsetEncoder is required for escaping. Needed for SJIS and related
     * problems with \u00A5.
     */
    private boolean requiresEscapingEncoder;

    private ModifiableProperty<String> characterEncoding;
    private ReadableProperty<Boolean> autoReconnectForPools;
    private ReadableProperty<Boolean> cachePrepStmts;
    private ReadableProperty<Boolean> cacheServerConfiguration;
    private ModifiableProperty<Boolean> autoReconnect;
    private ModifiableProperty<Boolean> profileSQL;
    private ReadableProperty<Boolean> useUsageAdvisor;
    private ReadableProperty<Boolean> reconnectAtTxEnd;
    private ReadableProperty<Boolean> useOldUTF8Behavior;
    private ReadableProperty<Boolean> maintainTimeStats;
    private ReadableProperty<Boolean> emulateUnsupportedPstmts;
    private ReadableProperty<Boolean> gatherPerfMetrics;
    private ReadableProperty<Boolean> ignoreNonTxTables;
    private ReadableProperty<Boolean> pedantic;
    private ReadableProperty<Integer> prepStmtCacheSqlLimit;
    private ReadableProperty<Boolean> useLocalSessionState;
    private ReadableProperty<Boolean> useServerPrepStmts;
    private ReadableProperty<Boolean> processEscapeCodesForPrepStmts;
    private ReadableProperty<Boolean> useLocalTransactionState;
    protected ModifiableProperty<Integer> maxAllowedPacket;
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
            this.connectionCreationTimeMillis = System.currentTimeMillis();

            // Stash away for later, used to clone this connection for Statement.cancel and Statement.setQueryTimeout().
            this.origHostInfo = hostInfo;
            this.origHostToConnectTo = hostInfo.getHost();
            this.origPortToConnectTo = hostInfo.getPort();

            // We need Session ASAP to get access to central driver functionality
            this.nullStatementResultSetFactory = new ResultSetFactory(this, null);
            this.session = new MysqlaSession(hostInfo, getPropertySet());

            // we can't cache fixed values here because properties are still not initialized with user provided values
            this.characterEncoding = getPropertySet().getJdbcModifiableProperty(PropertyDefinitions.PNAME_characterEncoding);
            this.autoReconnectForPools = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoReconnectForPools);
            this.cachePrepStmts = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_cachePrepStmts);
            this.cacheServerConfiguration = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheServerConfiguration);
            this.autoReconnect = getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_autoReconnect);
            this.profileSQL = getPropertySet().getModifiableProperty(PropertyDefinitions.PNAME_profileSQL);
            this.useUsageAdvisor = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useUsageAdvisor);
            this.reconnectAtTxEnd = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_reconnectAtTxEnd);
            this.useOldUTF8Behavior = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useOldUTF8Behavior);
            this.maintainTimeStats = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_maintainTimeStats);
            this.emulateUnsupportedPstmts = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_emulateUnsupportedPstmts);
            this.gatherPerfMetrics = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_gatherPerfMetrics);
            this.ignoreNonTxTables = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_ignoreNonTxTables);
            this.pedantic = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_pedantic);
            this.prepStmtCacheSqlLimit = getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_prepStmtCacheSqlLimit);
            this.useLocalSessionState = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useLocalSessionState);
            this.useServerPrepStmts = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useServerPrepStmts);
            this.processEscapeCodesForPrepStmts = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_processEscapeCodesForPrepStmts);
            this.useLocalTransactionState = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useLocalTransactionState);
            this.maxAllowedPacket = getPropertySet().getModifiableProperty(PropertyDefinitions.PNAME_maxAllowedPacket);
            this.disconnectOnExpiredPasswords = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords);
            this.readOnlyPropagatesToServer = getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_readOnlyPropagatesToServer);

            this.database = hostInfo.getDatabase();
            this.user = StringUtils.isNullOrEmpty(hostInfo.getUser()) ? "" : hostInfo.getUser();
            this.password = StringUtils.isNullOrEmpty(hostInfo.getPassword()) ? "" : hostInfo.getPassword();

            this.props = hostInfo.exposeAsProperties();
            initializeDriverProperties(this.props);

            this.pointOfOrigin = this.useUsageAdvisor.getValue() ? LogUtils.findCallingClassAndMethod(new Throwable()) : "";

            this.dbmd = getMetaData(false, false);

            initializeSafeStatementInterceptors();

        } catch (CJException e1) {
            throw SQLExceptionsMapping.translateException(e1, getExceptionInterceptor());
        }

        try {
            createNewIO(false);

            unSafeStatementInterceptors();

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
                    SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE, ex, getExceptionInterceptor());
        }

    }

    public void unSafeStatementInterceptors() throws SQLException {
        this.statementInterceptors = this.statementInterceptors.stream().map(u -> ((NoSubInterceptorWrapper) u).getUnderlyingInterceptor())
                .collect(Collectors.toList());

        if (this.session != null) {
            this.session.setStatementInterceptors(this.statementInterceptors);
        }
    }

    public void initializeSafeStatementInterceptors() throws SQLException {
        this.isClosed = false;
        this.statementInterceptors = Util
                .<StatementInterceptor> loadClasses(
                        getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_statementInterceptors).getStringValue(),
                        "MysqlIo.BadStatementInterceptor", getExceptionInterceptor())
                .stream().map(o -> new NoSubInterceptorWrapper(o.init(this, this.props, this.session.getLog()))).collect(Collectors.toList());
    }

    public List<StatementInterceptor> getStatementInterceptorsInstances() {
        return this.statementInterceptors;
    }

    private void addToHistogram(int[] histogramCounts, long[] histogramBreakpoints, long value, int numberOfTimes, long currentLowerBound,
            long currentUpperBound) {
        if (histogramCounts == null) {
            createInitialHistogram(histogramBreakpoints, currentLowerBound, currentUpperBound);
        } else {
            for (int i = 0; i < HISTOGRAM_BUCKETS; i++) {
                if (histogramBreakpoints[i] >= value) {
                    histogramCounts[i] += numberOfTimes;

                    break;
                }
            }
        }
    }

    private void addToPerformanceHistogram(long value, int numberOfTimes) {
        checkAndCreatePerformanceHistogram();

        addToHistogram(this.perfMetricsHistCounts, this.perfMetricsHistBreakpoints, value, numberOfTimes,
                this.shortestQueryTimeMs == Long.MAX_VALUE ? 0 : this.shortestQueryTimeMs, this.longestQueryTimeMs);
    }

    private void addToTablesAccessedHistogram(long value, int numberOfTimes) {
        checkAndCreateTablesAccessedHistogram();

        addToHistogram(this.numTablesMetricsHistCounts, this.numTablesMetricsHistBreakpoints, value, numberOfTimes,
                this.minimumNumberTablesAccessed == Long.MAX_VALUE ? 0 : this.minimumNumberTablesAccessed, this.maximumNumberTablesAccessed);
    }

    /**
     * Builds the map needed for 4.1.0 and newer servers that maps field-level
     * charset/collation info to a java character encoding name.
     * 
     * @throws SQLException
     */
    private void buildCollationMapping() throws SQLException {

        Map<Integer, String> indexToCharset = null;
        Map<Number, String> sortedCollationMap = null;
        Map<Integer, String> customCharset = null;
        Map<String, Integer> customMblen = null;

        if (this.cacheServerConfiguration.getValue()) {
            synchronized (dynamicIndexToCharsetMapByUrl) {
                indexToCharset = dynamicIndexToCharsetMapByUrl.get(getURL());
                sortedCollationMap = dynamicIndexToCollationMapByUrl.get(getURL());
                customCharset = customIndexToCharsetMapByUrl.get(getURL());
                customMblen = customCharsetToMblenMapByUrl.get(getURL());
            }
        }

        if (indexToCharset == null) {
            indexToCharset = new HashMap<Integer, String>();

            if (getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_detectCustomCollations).getValue()) {

                java.sql.Statement stmt = null;
                java.sql.ResultSet results = null;

                try {
                    sortedCollationMap = new TreeMap<Number, String>();
                    customCharset = new HashMap<Integer, String>();
                    customMblen = new HashMap<String, Integer>();

                    stmt = getMetadataSafeStatement();

                    try {
                        results = stmt.executeQuery("SHOW COLLATION");
                        ResultSetUtil.resultSetToMap(sortedCollationMap, results, 3, 2);
                    } catch (PasswordExpiredException ex) {
                        if (this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    } catch (SQLException ex) {
                        if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    }

                    for (Iterator<Map.Entry<Number, String>> indexIter = sortedCollationMap.entrySet().iterator(); indexIter.hasNext();) {
                        Map.Entry<Number, String> indexEntry = indexIter.next();

                        int collationIndex = indexEntry.getKey().intValue();
                        String charsetName = indexEntry.getValue();

                        indexToCharset.put(collationIndex, charsetName);

                        // if no static map for charsetIndex or server has a different mapping then our static map, adding it to custom map 
                        if (collationIndex >= CharsetMapping.MAP_SIZE
                                || !charsetName.equals(CharsetMapping.getMysqlCharsetNameForCollationIndex(collationIndex))) {
                            customCharset.put(collationIndex, charsetName);
                        }

                        // if no static map for charsetName adding to custom map
                        if (!CharsetMapping.CHARSET_NAME_TO_CHARSET.containsKey(charsetName)) {
                            customMblen.put(charsetName, null);
                        }
                    }

                    // if there is a number of custom charsets we should execute SHOW CHARACTER SET to know theirs mblen
                    if (customMblen.size() > 0) {
                        try {
                            results = stmt.executeQuery("SHOW CHARACTER SET");
                            while (results.next()) {
                                String charsetName = results.getString("Charset");
                                if (customMblen.containsKey(charsetName)) {
                                    customMblen.put(charsetName, results.getInt("Maxlen"));
                                }
                            }
                        } catch (PasswordExpiredException ex) {
                            if (this.disconnectOnExpiredPasswords.getValue()) {
                                throw ex;
                            }
                        } catch (SQLException ex) {
                            if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || this.disconnectOnExpiredPasswords.getValue()) {
                                throw ex;
                            }
                        }
                    }

                    if (this.cacheServerConfiguration.getValue()) {
                        synchronized (dynamicIndexToCharsetMapByUrl) {
                            dynamicIndexToCharsetMapByUrl.put(getURL(), indexToCharset);
                            dynamicIndexToCollationMapByUrl.put(getURL(), sortedCollationMap);
                            customIndexToCharsetMapByUrl.put(getURL(), customCharset);
                            customCharsetToMblenMapByUrl.put(getURL(), customMblen);
                        }
                    }

                } catch (SQLException ex) {
                    throw ex;
                } catch (RuntimeException ex) {
                    SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
                    sqlEx.initCause(ex);
                    throw sqlEx;
                } finally {
                    if (results != null) {
                        try {
                            results.close();
                        } catch (java.sql.SQLException sqlE) {
                            // ignore
                        }
                    }

                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (java.sql.SQLException sqlE) {
                            // ignore
                        }
                    }
                }
            } else {
                for (int i = 1; i < CharsetMapping.MAP_SIZE; i++) {
                    indexToCharset.put(i, CharsetMapping.getMysqlCharsetNameForCollationIndex(i));
                }
                if (this.cacheServerConfiguration.getValue()) {
                    synchronized (dynamicIndexToCharsetMapByUrl) {
                        dynamicIndexToCharsetMapByUrl.put(getURL(), indexToCharset);
                    }
                }
            }

        }

        this.session.setCharsetMaps(indexToCharset, customCharset, customMblen);

    }

    private boolean canHandleAsServerPreparedStatement(String sql) throws SQLException {
        if (sql == null || sql.length() == 0) {
            return true;
        }

        if (!this.useServerPreparedStmts) {
            return false;
        }

        if (this.cachePrepStmts.getValue()) {
            synchronized (this.serverSideStatementCheckCache) {
                Boolean flag = (Boolean) this.serverSideStatementCheckCache.get(sql);

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

            configureClientCharacterSet(true);

            setSessionVariables();

            setupServerForTruncationChecks();
        }
    }

    private void checkAndCreatePerformanceHistogram() {
        if (this.perfMetricsHistCounts == null) {
            this.perfMetricsHistCounts = new int[HISTOGRAM_BUCKETS];
        }

        if (this.perfMetricsHistBreakpoints == null) {
            this.perfMetricsHistBreakpoints = new long[HISTOGRAM_BUCKETS];
        }
    }

    private void checkAndCreateTablesAccessedHistogram() {
        if (this.numTablesMetricsHistCounts == null) {
            this.numTablesMetricsHistCounts = new int[HISTOGRAM_BUCKETS];
        }

        if (this.numTablesMetricsHistBreakpoints == null) {
            this.numTablesMetricsHistBreakpoints = new long[HISTOGRAM_BUCKETS];
        }
    }

    public void checkClosed() {
        if (this.isClosed) {
            throw ExceptionFactory.createException(ConnectionIsClosedException.class, Messages.getString("Connection.2"), this.forceClosedReason,
                    getExceptionInterceptor());
        }
    }

    public void throwConnectionClosedException() throws SQLException {
        SQLException ex = SQLError.createSQLException(Messages.getString("Connection.2"), SQLError.SQL_STATE_CONNECTION_NOT_OPEN, getExceptionInterceptor());

        if (this.forceClosedReason != null) {
            ex.initCause(this.forceClosedReason);
        }

        throw ex;
    }

    /**
     * Set transaction isolation level to the value received from server if any.
     * Is called by connectionInit(...)
     * 
     * @throws SQLException
     */
    private void checkTransactionIsolationLevel() throws SQLException {

        String s = this.session.getServerVariable("tx_isolation");

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
        this.session.abortInternal();

        this.isClosed = true;
    }

    /**
     * Destroys this connection and any underlying resources
     * 
     * @param fromWhere
     * @param whyCleanedUp
     */
    private void cleanup(Throwable whyCleanedUp) {
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

        this.isClosed = true;
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

        ((com.mysql.cj.jdbc.PreparedStatement) pStmt).setRetrieveGeneratedKeys(autoGenKeyIndex == java.sql.Statement.RETURN_GENERATED_KEYS);

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

        PreparedStatement pStmt = null;

        if (this.cachePrepStmts.getValue()) {
            PreparedStatement.ParseInfo pStmtInfo = this.cachedPreparedStatementParams.get(nativeSql);

            if (pStmtInfo == null) {
                pStmt = com.mysql.cj.jdbc.PreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.database);

                this.cachedPreparedStatementParams.put(nativeSql, pStmt.getParseInfo());
            } else {
                pStmt = com.mysql.cj.jdbc.PreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.database, pStmtInfo);
            }
        } else {
            pStmt = com.mysql.cj.jdbc.PreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.database);
        }

        pStmt.setResultSetType(resultSetType);
        pStmt.setResultSetConcurrency(resultSetConcurrency);

        return pStmt;
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {

        PreparedStatement pStmt = (PreparedStatement) clientPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyIndexes != null) && (autoGenKeyIndexes.length > 0));

        return pStmt;
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        PreparedStatement pStmt = (PreparedStatement) clientPrepareStatement(sql);

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
                new IterateBlock<ConnectionLifecycleInterceptor>(this.connectionLifecycleInterceptors.iterator()) {
                    @Override
                    void forEach(ConnectionLifecycleInterceptor each) throws SQLException {
                        each.close();
                    }
                }.doForAll();
            }

            realClose(true, true, false, null);
        }
    }

    /**
     * Closes all currently open statements.
     * 
     * @throws SQLException
     */
    private void closeAllOpenStatements() throws SQLException {
        SQLException postponedException = null;

        for (Statement stmt : this.openStatements) {
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

                if (this.autoCommit) {
                    throw SQLError.createSQLException(Messages.getString("Connection.3"), getExceptionInterceptor());
                }
                if (this.useLocalTransactionState.getValue()) {
                    if (!this.session.inTransactionOnServer()) {
                        return; // effectively a no-op
                    }
                }

                execSQL(null, "commit", -1, null, false, this.database, null, false);
            } catch (SQLException sqlException) {
                if (SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlException.getSQLState())) {
                    throw SQLError.createSQLException(Messages.getString("Connection.4"), SQLError.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN,
                            getExceptionInterceptor());
                }

                throw sqlException;
            } finally {
                this.needsPing = this.reconnectAtTxEnd.getValue();
            }
        }
        return;
    }

    /**
     * Configures client-side properties for character set information.
     * 
     * @throws SQLException
     *             if unable to configure the specified character set.
     */
    private void configureCharsetProperties() throws SQLException {
        if (this.characterEncoding.getValue() != null) {
            // Attempt to use the encoding, and bail out if it can't be used
            try {
                String testString = "abc";
                StringUtils.getBytes(testString, this.characterEncoding.getValue());
            } catch (WrongArgumentException waEx) {
                // Try the MySQL character encoding, then....
                String oldEncoding = this.characterEncoding.getValue();

                try {
                    this.characterEncoding.setValue(CharsetMapping.getJavaEncodingForMysqlCharset(oldEncoding));
                } catch (RuntimeException ex) {
                    throw SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, ex, null);
                }

                if (this.characterEncoding.getValue() == null) {
                    throw SQLError.createSQLException(Messages.getString("Connection.5", new Object[] { oldEncoding }),
                            SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, getExceptionInterceptor());
                }

                try {
                    String testString = "abc";
                    StringUtils.getBytes(testString, this.characterEncoding.getValue());
                } catch (WrongArgumentException encodingEx) {
                    throw SQLError.createSQLException(encodingEx.getMessage(), SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, getExceptionInterceptor());
                }
            }
        }
    }

    /**
     * Sets up client character set for MySQL-4.1 and newer if the user This
     * must be done before any further communication with the server!
     * 
     * @return true if this routine actually configured the client character
     *         set, or false if the driver needs to use 'older' methods to
     *         detect the character set, as it is connected to a MySQL server
     *         older than 4.1.0
     * @throws SQLException
     *             if an exception happens while sending 'SET NAMES' to the
     *             server, or the server sends character set information that
     *             the client doesn't know about.
     */
    private boolean configureClientCharacterSet(boolean dontCheckServerMatch) throws SQLException {
        String realJavaEncoding = this.characterEncoding.getValue();
        ModifiableProperty<String> characterSetResults = getPropertySet().getModifiableProperty(PropertyDefinitions.PNAME_characterSetResults);
        boolean characterSetAlreadyConfigured = false;

        try {
            characterSetAlreadyConfigured = true;

            configureCharsetProperties();
            realJavaEncoding = this.characterEncoding.getValue(); // we need to do this again to grab this for versions > 4.1.0

            try {

                // Fault injection for testing server character set indices
                if (this.props != null && this.props.getProperty(PropertyDefinitions.PNAME_testsuite_faultInjection_serverCharsetIndex) != null) {
                    this.session.setServerDefaultCollationIndex(
                            Integer.parseInt(this.props.getProperty(PropertyDefinitions.PNAME_testsuite_faultInjection_serverCharsetIndex)));
                }

                String serverEncodingToSet = CharsetMapping.getJavaEncodingForCollationIndex(this.session.getServerDefaultCollationIndex());

                if (serverEncodingToSet == null || serverEncodingToSet.length() == 0) {
                    if (realJavaEncoding != null) {
                        // user knows best, try it
                        this.characterEncoding.setValue(realJavaEncoding);
                    } else {
                        throw SQLError.createSQLException(Messages.getString("Connection.6", new Object[] { this.session.getServerDefaultCollationIndex() }),
                                SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                    }
                }

                // "latin1" on MySQL-4.1.0+ is actually CP1252, not ISO8859_1
                if ("ISO8859_1".equalsIgnoreCase(serverEncodingToSet)) {
                    serverEncodingToSet = "Cp1252";
                }
                if ("UnicodeBig".equalsIgnoreCase(serverEncodingToSet) || "UTF-16".equalsIgnoreCase(serverEncodingToSet)
                        || "UTF-16LE".equalsIgnoreCase(serverEncodingToSet) || "UTF-32".equalsIgnoreCase(serverEncodingToSet)) {
                    serverEncodingToSet = "UTF-8";
                }

                this.characterEncoding.setValue(serverEncodingToSet);

            } catch (ArrayIndexOutOfBoundsException outOfBoundsEx) {
                if (realJavaEncoding != null) {
                    // user knows best, try it
                    this.characterEncoding.setValue(realJavaEncoding);
                } else {
                    throw SQLError.createSQLException(Messages.getString("Connection.6", new Object[] { this.session.getServerDefaultCollationIndex() }),
                            SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                }
            } catch (SQLException ex) {
                throw ex;
            } catch (RuntimeException ex) {
                SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
                sqlEx.initCause(ex);
                throw sqlEx;
            }

            if (this.characterEncoding.getValue() == null) {
                // punt?
                this.characterEncoding.setValue("ISO8859_1");
            }

            if (realJavaEncoding != null) {

                //
                // Now, inform the server what character set we will be using from now-on...
                //
                if (realJavaEncoding.equalsIgnoreCase("UTF-8") || realJavaEncoding.equalsIgnoreCase("UTF8")) {
                    // charset names are case-sensitive

                    boolean useutf8mb4 = CharsetMapping.UTF8MB4_INDEXES.contains(this.session.getServerDefaultCollationIndex());

                    if (!this.useOldUTF8Behavior.getValue()) {
                        if (dontCheckServerMatch || !this.session.characterSetNamesMatches("utf8") || (!this.session.characterSetNamesMatches("utf8mb4"))) {
                            execSQL(null, "SET NAMES " + (useutf8mb4 ? "utf8mb4" : "utf8"), -1, null, false, this.database, null, false);
                            this.session.getServerVariables().put("character_set_client", useutf8mb4 ? "utf8mb4" : "utf8");
                            this.session.getServerVariables().put("character_set_connection", useutf8mb4 ? "utf8mb4" : "utf8");
                        }
                    } else {
                        execSQL(null, "SET NAMES latin1", -1, null, false, this.database, null, false);
                        this.session.getServerVariables().put("character_set_client", "latin1");
                        this.session.getServerVariables().put("character_set_connection", "latin1");
                    }

                    this.characterEncoding.setValue(realJavaEncoding);
                } /* not utf-8 */else {
                    String mysqlCharsetName = CharsetMapping.getMysqlCharsetForJavaEncoding(realJavaEncoding.toUpperCase(Locale.ENGLISH), getServerVersion());

                    if (mysqlCharsetName != null) {

                        if (dontCheckServerMatch || !this.session.characterSetNamesMatches(mysqlCharsetName)) {
                            execSQL(null, "SET NAMES " + mysqlCharsetName, -1, null, false, this.database, null, false);
                            this.session.getServerVariables().put("character_set_client", mysqlCharsetName);
                            this.session.getServerVariables().put("character_set_connection", mysqlCharsetName);
                        }
                    }

                    // Switch driver's encoding now, since the server knows what we're sending...
                    //
                    this.characterEncoding.setValue(realJavaEncoding);
                }
            } else if (this.characterEncoding.getValue() != null) {
                // Tell the server we'll use the server default charset to send our queries from now on....
                String mysqlCharsetName = getSession().getServerCharset();

                if (this.useOldUTF8Behavior.getValue()) {
                    mysqlCharsetName = "latin1";
                }

                boolean ucs2 = false;
                if ("ucs2".equalsIgnoreCase(mysqlCharsetName) || "utf16".equalsIgnoreCase(mysqlCharsetName) || "utf16le".equalsIgnoreCase(mysqlCharsetName)
                        || "utf32".equalsIgnoreCase(mysqlCharsetName)) {
                    mysqlCharsetName = "utf8";
                    ucs2 = true;
                    if (characterSetResults.getValue() == null) {
                        characterSetResults.setValue("UTF-8");
                    }
                }

                if (dontCheckServerMatch || !this.session.characterSetNamesMatches(mysqlCharsetName) || ucs2) {
                    try {
                        execSQL(null, "SET NAMES " + mysqlCharsetName, -1, null, false, this.database, null, false);
                        this.session.getServerVariables().put("character_set_client", mysqlCharsetName);
                        this.session.getServerVariables().put("character_set_connection", mysqlCharsetName);
                    } catch (PasswordExpiredException ex) {
                        if (this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    } catch (SQLException ex) {
                        if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    }
                }

                realJavaEncoding = this.characterEncoding.getValue();
            }

            //
            // We know how to deal with any charset coming back from the database, so tell the server not to do conversion if the user hasn't 'forced' a
            // result-set character set
            //

            String onServer = null;
            boolean isNullOnServer = false;

            if (this.session.getServerVariables() != null) {
                onServer = this.session.getServerVariable("character_set_results");

                isNullOnServer = onServer == null || "NULL".equalsIgnoreCase(onServer) || onServer.length() == 0;
            }

            if (characterSetResults.getValue() == null) {

                //
                // Only send if needed, if we're caching server variables we -have- to send, because we don't know what it was before we cached them.
                //
                if (!isNullOnServer) {
                    try {
                        execSQL(null, "SET character_set_results = NULL", -1, null, false, this.database, null, false);
                    } catch (PasswordExpiredException ex) {
                        if (this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    } catch (SQLException ex) {
                        if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    }
                    this.session.getServerVariables().put(ServerSession.JDBC_LOCAL_CHARACTER_SET_RESULTS, null);
                } else {
                    this.session.getServerVariables().put(ServerSession.JDBC_LOCAL_CHARACTER_SET_RESULTS, onServer);
                }
            } else {

                if (this.useOldUTF8Behavior.getValue()) {
                    try {
                        execSQL(null, "SET NAMES latin1", -1, null, false, this.database, null, false);
                        this.session.getServerVariables().put("character_set_client", "latin1");
                        this.session.getServerVariables().put("character_set_connection", "latin1");
                    } catch (PasswordExpiredException ex) {
                        if (this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    } catch (SQLException ex) {
                        if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    }
                }
                String charsetResults = characterSetResults.getValue();
                String mysqlEncodingName = null;

                if ("UTF-8".equalsIgnoreCase(charsetResults) || "UTF8".equalsIgnoreCase(charsetResults)) {
                    mysqlEncodingName = "utf8";
                } else if ("null".equalsIgnoreCase(charsetResults)) {
                    mysqlEncodingName = "NULL";
                } else {
                    mysqlEncodingName = CharsetMapping.getMysqlCharsetForJavaEncoding(charsetResults.toUpperCase(Locale.ENGLISH), getServerVersion());
                }

                //
                // Only change the value if needed
                //

                if (mysqlEncodingName == null) {
                    throw SQLError.createSQLException(Messages.getString("Connection.7", new Object[] { charsetResults }), SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                            getExceptionInterceptor());
                }

                if (!mysqlEncodingName.equalsIgnoreCase(this.session.getServerVariable("character_set_results"))) {
                    StringBuilder setBuf = new StringBuilder("SET character_set_results = ".length() + mysqlEncodingName.length());
                    setBuf.append("SET character_set_results = ").append(mysqlEncodingName);

                    try {
                        execSQL(null, setBuf.toString(), -1, null, false, this.database, null, false);
                    } catch (PasswordExpiredException ex) {
                        if (this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    } catch (SQLException ex) {
                        if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || this.disconnectOnExpiredPasswords.getValue()) {
                            throw ex;
                        }
                    }

                    this.session.getServerVariables().put(ServerSession.JDBC_LOCAL_CHARACTER_SET_RESULTS, mysqlEncodingName);

                    // We have to set errorMessageEncoding according to new value of charsetResults for server version 5.5 and higher
                    this.session.setErrorMessageEncoding(charsetResults);

                } else {
                    this.session.getServerVariables().put(ServerSession.JDBC_LOCAL_CHARACTER_SET_RESULTS, onServer);
                }
            }

            String connectionCollation = getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_connectionCollation).getStringValue();
            if (connectionCollation != null) {
                StringBuilder setBuf = new StringBuilder("SET collation_connection = ".length() + connectionCollation.length());
                setBuf.append("SET collation_connection = ").append(connectionCollation);

                try {
                    execSQL(null, setBuf.toString(), -1, null, false, this.database, null, false);
                } catch (PasswordExpiredException ex) {
                    if (this.disconnectOnExpiredPasswords.getValue()) {
                        throw ex;
                    }
                } catch (SQLException ex) {
                    if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || this.disconnectOnExpiredPasswords.getValue()) {
                        throw ex;
                    }
                }
            }
        } finally {
            // Failsafe, make sure that the driver's notion of character encoding matches what the user has specified.
            //
            this.characterEncoding.setValue(realJavaEncoding);
        }

        /**
         * Check if we need a CharsetEncoder for escaping codepoints that are
         * transformed to backslash (0x5c) in the connection encoding.
         */
        try {
            CharsetEncoder enc = Charset.forName(this.characterEncoding.getValue()).newEncoder();
            CharBuffer cbuf = CharBuffer.allocate(1);
            ByteBuffer bbuf = ByteBuffer.allocate(1);

            cbuf.put("\u00a5");
            cbuf.position(0);
            enc.encode(cbuf, bbuf, true);
            if (bbuf.get(0) == '\\') {
                this.requiresEscapingEncoder = true;
            } else {
                cbuf.clear();
                bbuf.clear();

                cbuf.put("\u20a9");
                cbuf.position(0);
                enc.encode(cbuf, bbuf, true);
                if (bbuf.get(0) == '\\') {
                    this.requiresEscapingEncoder = true;
                }
            }
        } catch (java.nio.charset.UnsupportedCharsetException ucex) {
            // fallback to String API
            byte bbuf[] = StringUtils.getBytes("\u00a5", this.characterEncoding.getValue());
            if (bbuf[0] == '\\') {
                this.requiresEscapingEncoder = true;
            } else {
                bbuf = StringUtils.getBytes("\u20a9", this.characterEncoding.getValue());
                if (bbuf[0] == '\\') {
                    this.requiresEscapingEncoder = true;
                }
            }
        }

        return characterSetAlreadyConfigured;
    }

    private void createInitialHistogram(long[] breakpoints, long lowerBound, long upperBound) {

        double bucketSize = (((double) upperBound - (double) lowerBound) / HISTOGRAM_BUCKETS) * 1.25;

        if (bucketSize < 1) {
            bucketSize = 1;
        }

        for (int i = 0; i < HISTOGRAM_BUCKETS; i++) {
            breakpoints[i] = lowerBound;
            lowerBound += bucketSize;
        }
    }

    /**
     * Creates an IO channel to the server
     * 
     * @param isForReconnect
     *            is this request for a re-connect
     * @return a new MysqlIO instance connected to a server
     * @throws SQLException
     *             if a database access error occurs
     * @throws CommunicationsException
     */
    public void createNewIO(boolean isForReconnect) {
        synchronized (getConnectionMutex()) {
            // Synchronization Not needed for *new* connections, but defintely for connections going through fail-over, since we might get the new connection up
            // and running *enough* to start sending cached or still-open server-side prepared statements over to the backend before we get a chance to
            // re-prepare them...

            try {
                Properties mergedProps = getPropertySet().exposeAsProperties(this.props);

                if (!this.autoReconnect.getValue()) {
                    connectOneTryOnly(isForReconnect, mergedProps);

                    return;
                }

                connectWithRetries(isForReconnect, mergedProps);
            } catch (SQLException ex) {
                throw ExceptionFactory.createException(UnableToConnectException.class, ex.getMessage(), ex);
            }
        }
    }

    private void connectWithRetries(boolean isForReconnect, Properties mergedProps) throws SQLException {
        double timeout = getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_initialTimeout).getValue();
        boolean connectionGood = false;

        Exception connectionException = null;

        for (int attemptCount = 0; (attemptCount < getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_maxReconnects).getValue())
                && !connectionGood; attemptCount++) {
            try {
                this.session.forceClose();

                this.session.connect(getProxy(), this.origHostInfo, mergedProps, this.user, this.password, this.database,
                        DriverManager.getLoginTimeout() * 1000);
                pingInternal(false, 0);

                boolean oldAutoCommit;
                int oldIsolationLevel;
                boolean oldReadOnly;
                String oldCatalog;

                synchronized (getConnectionMutex()) {
                    this.connectionId = this.session.getThreadId();
                    this.isClosed = false;

                    // save state from old connection
                    oldAutoCommit = getAutoCommit();
                    oldIsolationLevel = this.isolationLevel;
                    oldReadOnly = isReadOnly(false);
                    oldCatalog = getCatalog();

                    this.session.setStatementInterceptors(this.statementInterceptors);
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
                            new Object[] { getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_maxReconnects).getValue() }),
                    SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, connectionException, getExceptionInterceptor());
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
            Iterator<Statement> statementIter = this.openStatements.iterator();

            //
            // We build a list of these outside the map of open statements, because in the process of re-preparing, we might end up having to close a prepared
            // statement, thus removing it from the map, and generating a ConcurrentModificationException
            //
            Stack<Statement> serverPreparedStatements = null;

            while (statementIter.hasNext()) {
                Statement statementObj = statementIter.next();

                if (statementObj instanceof ServerPreparedStatement) {
                    if (serverPreparedStatements == null) {
                        serverPreparedStatements = new Stack<Statement>();
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

    private void connectOneTryOnly(boolean isForReconnect, Properties mergedProps) throws SQLException {
        Exception connectionNotEstablishedBecause = null;

        try {

            this.session.connect(getProxy(), this.origHostInfo, mergedProps, this.user, this.password, this.database, DriverManager.getLoginTimeout() * 1000);
            this.connectionId = this.session.getThreadId();
            this.isClosed = false;

            // save state from old connection
            boolean oldAutoCommit = getAutoCommit();
            int oldIsolationLevel = this.isolationLevel;
            boolean oldReadOnly = isReadOnly(false);
            String oldCatalog = getCatalog();

            this.session.setStatementInterceptors(this.statementInterceptors);

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
                    SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, getExceptionInterceptor());
            chainedEx.initCause(connectionNotEstablishedBecause);

            throw chainedEx;
        }
    }

    private void createPreparedStatementCaches() throws SQLException {
        synchronized (getConnectionMutex()) {
            int cacheSize = getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_prepStmtCacheSize).getValue();
            String parseInfoCacheFactory = getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory).getValue();

            try {
                Class<?> factoryClass;

                factoryClass = Class.forName(parseInfoCacheFactory);

                @SuppressWarnings("unchecked")
                CacheAdapterFactory<String, ParseInfo> cacheFactory = ((CacheAdapterFactory<String, ParseInfo>) factoryClass.newInstance());

                this.cachedPreparedStatementParams = cacheFactory.getInstance(this, this.origHostInfo.getDatabaseUrl(), cacheSize,
                        this.prepStmtCacheSqlLimit.getValue(), this.props);

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
                this.serverSideStatementCheckCache = new LRUCache(cacheSize);

                this.serverSideStatementCache = new LRUCache(cacheSize) {

                    private static final long serialVersionUID = 7692318650375988114L;

                    @Override
                    protected boolean removeEldestEntry(java.util.Map.Entry<Object, Object> eldest) {
                        if (this.maxElements <= 1) {
                            return false;
                        }

                        boolean removeIt = super.removeEldestEntry(eldest);

                        if (removeIt) {
                            ServerPreparedStatement ps = (ServerPreparedStatement) eldest.getValue();
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
                throw SQLError.createSQLException("HOLD_CUSRORS_OVER_COMMIT is only supported holdability level", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        return createStatement(resultSetType, resultSetConcurrency);
    }

    public JdbcConnection duplicate() throws SQLException {
        return new ConnectionImpl(this.origHostInfo);
    }

    /**
     * Send a query to the server. Returns one of the ResultSet objects. This is
     * synchronized, so Statement's queries will be serialized.
     * 
     * @param callingStatement
     * @param sql
     *            the SQL statement to be executed
     * @param maxRows
     * @param packet
     * @param resultSetType
     * @param resultSetConcurrency
     * @param streamResults
     * @param queryIsSelectOnly
     * @param catalog
     * @param unpackFields
     * @return a ResultSet holding the results
     * @exception SQLException
     *                if a database error occurs
     */

    // ResultSet execSQL(Statement callingStatement, String sql,
    // int maxRowsToRetreive, String catalog) throws SQLException {
    // return execSQL(callingStatement, sql, maxRowsToRetreive, null,
    // java.sql.ResultSet.TYPE_FORWARD_ONLY,
    // DEFAULT_RESULT_SET_CONCURRENCY, catalog);
    // }
    // ResultSet execSQL(Statement callingStatement, String sql, int maxRows,
    // int resultSetType, int resultSetConcurrency, boolean streamResults,
    // boolean queryIsSelectOnly, String catalog, boolean unpackFields) throws
    // SQLException {
    // return execSQL(callingStatement, sql, maxRows, null, resultSetType,
    // resultSetConcurrency, streamResults, queryIsSelectOnly, catalog,
    // unpackFields);
    // }
    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, PacketPayload packet, boolean streamResults,
            String catalog, ColumnDefinition cachedMetadata) throws SQLException {
        return execSQL(callingStatement, sql, maxRows, packet, streamResults, catalog, cachedMetadata, false);
    }

    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, PacketPayload packet, boolean streamResults,
            String catalog, ColumnDefinition cachedMetadata, boolean isBatch) throws SQLException {
        synchronized (getConnectionMutex()) {
            //
            // Fall-back if the master is back online if we've issued queriesBeforeRetryMaster queries since we failed over
            //

            long queryStartTime = 0;

            int endOfQueryPacketPosition = 0;

            if (packet != null) {
                endOfQueryPacketPosition = packet.getPosition();
            }

            if (this.gatherPerfMetrics.getValue()) {
                queryStartTime = System.currentTimeMillis();
            }

            this.lastQueryFinishedTime = 0; // we're busy!

            if ((this.autoReconnect.getValue()) && (this.autoCommit || this.autoReconnectForPools.getValue()) && this.needsPing && !isBatch) {
                try {
                    pingInternal(false, 0);

                    this.needsPing = false;
                } catch (Exception Ex) {
                    createNewIO(true);
                }
            }

            try {
                if (packet == null) {
                    String encoding = this.characterEncoding.getValue();

                    return this.session.sqlQueryDirect(callingStatement, sql, encoding, null, maxRows, streamResults, catalog, cachedMetadata,
                            callingStatement != null ? callingStatement.getResultSetFactory() : this.nullStatementResultSetFactory);
                }

                return this.session.sqlQueryDirect(callingStatement, null, null, packet, maxRows, streamResults, catalog, cachedMetadata,
                        callingStatement != null ? callingStatement.getResultSetFactory() : this.nullStatementResultSetFactory);
            } catch (CJException sqlE) {
                // don't clobber SQL exceptions

                SQLException cause = SQLExceptionsMapping.translateException(sqlE, getExceptionInterceptor());

                if (getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_dumpQueriesOnException).getValue()) {
                    String extractedSql = MysqlaUtils.extractSqlFromPacket(sql, packet, endOfQueryPacketPosition,
                            getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_maxQuerySizeToLog).getValue());
                    StringBuilder messageBuf = new StringBuilder(extractedSql.length() + 32);
                    messageBuf.append("\n\nQuery being executed when exception was thrown:\n");
                    messageBuf.append(extractedSql);
                    messageBuf.append("\n\n");

                    cause = appendMessageToException(cause, messageBuf.toString(), getExceptionInterceptor());
                }

                if ((this.autoReconnect.getValue())) {
                    this.needsPing = true;
                } else {
                    String sqlState = cause.getSQLState();

                    if ((sqlState != null) && sqlState.equals(SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE)) {
                        cleanup(cause);
                    }
                }

                throw cause;
            } catch (Exception ex) {
                if (this.autoReconnect.getValue()) {
                    this.needsPing = true;
                    //} else if (ex instanceof IOException) {
                    //    cleanup(ex);
                }

                SQLException sqlEx = SQLError.createSQLException(Messages.getString("Connection.UnexpectedException"), SQLError.SQL_STATE_GENERAL_ERROR,
                        getExceptionInterceptor());
                sqlEx.initCause(ex);

                throw sqlEx;
            } finally {
                if (this.maintainTimeStats.getValue()) {
                    this.lastQueryFinishedTime = System.currentTimeMillis();
                }

                if (this.gatherPerfMetrics.getValue()) {
                    long queryTime = System.currentTimeMillis() - queryStartTime;

                    registerQueryExecutionTime(queryTime);
                }
            }
        }
    }

    public StringBuilder generateConnectionCommentBlock(StringBuilder buf) {
        buf.append("/* conn id ");
        buf.append(getId());
        buf.append(" clock: ");
        buf.append(System.currentTimeMillis());
        buf.append(" */ ");

        return buf;
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
            return this.autoCommit;
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
        return this.connectionId;
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
            if (this.lastQueryFinishedTime == 0) {
                return 0;
            }

            long now = System.currentTimeMillis();
            long idleTime = now - this.lastQueryFinishedTime;

            return idleTime;
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
        java.sql.Statement stmt = createStatement();

        if (stmt.getMaxRows() != 0) {
            stmt.setMaxRows(0);
        }

        stmt.setEscapeProcessing(false);

        if (stmt.getFetchSize() != 0) {
            stmt.setFetchSize(0);
        }

        return stmt;
    }

    public ServerVersion getServerVersion() {
        return this.session.getServerVersion();
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
                java.sql.Statement stmt = null;
                java.sql.ResultSet rs = null;

                try {
                    stmt = getMetadataSafeStatement();

                    rs = stmt.executeQuery("SELECT @@session.tx_isolation");

                    if (rs.next()) {
                        String s = rs.getString(1);

                        if (s != null) {
                            Integer intTI = mapTransIsolationNameToValue.get(s);

                            if (intTI != null) {
                                return intTI.intValue();
                            }
                        }

                        throw SQLError.createSQLException(Messages.getString("Connection.12", new Object[] { s }), SQLError.SQL_STATE_GENERAL_ERROR,
                                getExceptionInterceptor());
                    }

                    throw SQLError.createSQLException(Messages.getString("Connection.13"), SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());

                } finally {
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (Exception ex) {
                            // ignore
                        }

                        rs = null;
                    }

                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (Exception ex) {
                            // ignore
                        }

                        stmt = null;
                    }
                }
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
                this.typeMap = new HashMap<String, Class<?>>();
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

    public void incrementNumberOfPreparedExecutes() {
        if (this.gatherPerfMetrics.getValue()) {
            this.numberOfPreparedExecutes++;

            // We need to increment this, because server-side prepared statements bypass any execution by the connection itself...
            this.numberOfQueriesIssued++;
        }
    }

    public void incrementNumberOfPrepares() {
        if (this.gatherPerfMetrics.getValue()) {
            this.numberOfPrepares++;
        }
    }

    public void incrementNumberOfResultSetsCreated() {
        if (this.gatherPerfMetrics.getValue()) {
            this.numberOfResultSetsCreated++;
        }
    }

    /**
     * Initializes driver properties that come from URL or properties passed to
     * the driver manager.
     * 
     * @param info
     * @throws SQLException
     */
    private void initializeDriverProperties(Properties info) throws SQLException {
        getPropertySet().initializeProperties(info);

        String exceptionInterceptorClasses = getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_exceptionInterceptors).getStringValue();

        if (exceptionInterceptorClasses != null && !"".equals(exceptionInterceptorClasses)) {
            this.exceptionInterceptor = new ExceptionInterceptorChain(exceptionInterceptorClasses);
        }

        this.session.setLog(LogFactory.getLogger(getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_logger).getStringValue(),
                Log.LOGGER_INSTANCE_NAME, getExceptionInterceptor()));

        if (this.profileSQL.getValue() || this.useUsageAdvisor.getValue()) {
            ProfilerEventHandlerFactory.getInstance(this.session);
        }

        if (this.cachePrepStmts.getValue()) {
            createPreparedStatementCaches();
        }

        if (getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheCallableStmts).getValue()) {
            this.parsedCallableStatementCache = new LRUCache(
                    getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_callableStmtCacheSize).getValue());
        }

        if (getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowMultiQueries).getValue()) {
            getPropertySet().<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_cacheResultSetMetadata).setValue(false); // we don't handle this yet
        }

        if (getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheResultSetMetadata).getValue()) {
            this.resultSetMetadataCache = new LRUCache(getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_metadataCacheSize).getValue());
        }

        if (getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_socksProxyHost).getStringValue() != null) {
            getPropertySet().getJdbcModifiableProperty(PropertyDefinitions.PNAME_socketFactory).setValue(SocksProxySocketFactory.class.getName());
        }
    }

    /**
     * Sets varying properties that depend on server information. Called once we
     * have connected to the server.
     * 
     * @param info
     * @throws SQLException
     */
    private void initializePropsFromServer() throws SQLException {
        String connectionInterceptorClasses = getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_connectionLifecycleInterceptors)
                .getStringValue();

        this.connectionLifecycleInterceptors = null;

        if (connectionInterceptorClasses != null) {
            try {
                this.connectionLifecycleInterceptors = Util
                        .<ConnectionLifecycleInterceptor> loadClasses(
                                getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_connectionLifecycleInterceptors).getStringValue(),
                                "Connection.badLifecycleInterceptor", getExceptionInterceptor())
                        .stream().map(o -> o.init(this, this.props, this.session.getLog())).collect(Collectors.toList());
            } catch (CJException e) {
                throw SQLExceptionsMapping.translateException(e, getExceptionInterceptor());
            }
        }

        setSessionVariables();

        //
        // Users can turn off detection of server-side prepared statements
        //
        if (this.useServerPrepStmts.getValue()) {
            this.useServerPreparedStmts = true;
        }

        loadServerVariables();

        this.autoIncrementIncrement = this.session.getServerVariable("auto_increment_increment", 1);

        buildCollationMapping();

        try {
            LicenseConfiguration.checkLicenseType(this.session.getServerVariables());
        } catch (CJException e) {
            throw SQLError.createSQLException(e.getMessage(), SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, getExceptionInterceptor());
        }

        String lowerCaseTables = this.session.getServerVariable("lower_case_table_names");

        this.lowerCaseTableNames = "on".equalsIgnoreCase(lowerCaseTables) || "1".equalsIgnoreCase(lowerCaseTables) || "2".equalsIgnoreCase(lowerCaseTables);

        this.storesLowerCaseTableName = "1".equalsIgnoreCase(lowerCaseTables) || "on".equalsIgnoreCase(lowerCaseTables);

        this.session.configureTimezone();

        if (this.session.getServerVariables().containsKey("max_allowed_packet")) {
            int serverMaxAllowedPacket = this.session.getServerVariable("max_allowed_packet", -1);

            // use server value if maxAllowedPacket hasn't been given, or max_allowed_packet is smaller
            if (serverMaxAllowedPacket != -1 && (!this.maxAllowedPacket.isExplicitlySet() || serverMaxAllowedPacket < this.maxAllowedPacket.getValue())) {
                this.maxAllowedPacket.setValue(serverMaxAllowedPacket);
            }

            if (this.useServerPrepStmts.getValue()) {
                ModifiableProperty<Integer> blobSendChunkSize = getPropertySet().<Integer> getModifiableProperty(PropertyDefinitions.PNAME_blobSendChunkSize);
                int preferredBlobSendChunkSize = blobSendChunkSize.getValue();

                // LONG_DATA and MySQLIO packet header size
                int packetHeaderSize = ServerPreparedStatement.BLOB_STREAM_READ_BUF_SIZE + 11;
                int allowedBlobSendChunkSize = Math.min(preferredBlobSendChunkSize, this.maxAllowedPacket.getValue()) - packetHeaderSize;

                if (allowedBlobSendChunkSize <= 0) {
                    throw SQLError.createSQLException(Messages.getString("Connection.15", new Object[] { packetHeaderSize }),
                            SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, getExceptionInterceptor());
                }

                blobSendChunkSize.setValue(allowedBlobSendChunkSize);
            }
        }

        checkTransactionIsolationLevel();

        this.session.checkForCharsetMismatch();

        if (this.session.getServerVariables().containsKey("sql_mode")) {
            String sqlModeAsString = this.session.getServerVariable("sql_mode");
            if (StringUtils.isStrictlyNumeric(sqlModeAsString)) {
                // Old MySQL servers used to have sql_mode as a numeric value.
                this.useAnsiQuotes = (Integer.parseInt(sqlModeAsString) & 4) > 0;
            } else if (sqlModeAsString != null) {
                this.useAnsiQuotes = sqlModeAsString.indexOf("ANSI_QUOTES") != -1;
                this.noBackslashEscapes = sqlModeAsString.indexOf("NO_BACKSLASH_ESCAPES") != -1;
            }
        }

        boolean overrideDefaultAutocommit = isAutoCommitNonDefaultOnServer();

        configureClientCharacterSet(false);

        if (!overrideDefaultAutocommit) {
            try {
                setAutoCommit(true); // to override anything the server is set to...reqd by JDBC spec.
            } catch (PasswordExpiredException ex) {
                if (this.disconnectOnExpiredPasswords.getValue()) {
                    throw ex;
                }
            } catch (SQLException ex) {
                if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || this.disconnectOnExpiredPasswords.getValue()) {
                    throw ex;
                }
            }
        }

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
     * Has the default autocommit value of 0 been changed on the server
     * via init_connect?
     * 
     * @return true if autocommit is not the default of '0' on the server.
     * 
     * @throws SQLException
     */
    private boolean isAutoCommitNonDefaultOnServer() throws SQLException {
        boolean overrideDefaultAutocommit = false;

        String initConnectValue = this.session.getServerVariable("init_connect");

        if (initConnectValue != null && initConnectValue.length() > 0) {
            if (!getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_elideSetAutoCommits).getValue()) {
                // auto-commit might have changed
                java.sql.ResultSet rs = null;
                java.sql.Statement stmt = null;

                try {
                    stmt = getMetadataSafeStatement();

                    rs = stmt.executeQuery("SELECT @@session.autocommit");

                    if (rs.next()) {
                        this.autoCommit = rs.getBoolean(1);
                        if (this.autoCommit != true) {
                            overrideDefaultAutocommit = true;
                        }
                    }

                } finally {
                    if (rs != null) {
                        try {
                            rs.close();
                        } catch (SQLException sqlEx) {
                            // do nothing
                        }
                    }

                    if (stmt != null) {
                        try {
                            stmt.close();
                        } catch (SQLException sqlEx) {
                            // do nothing
                        }
                    }
                }
            } else {
                if (getSession().isSetNeededForAutoCommitMode(true)) {
                    // we're not in standard autocommit=true mode
                    this.autoCommit = false;
                    overrideDefaultAutocommit = true;
                }
            }
        }

        return overrideDefaultAutocommit;
    }

    public boolean isClosed() {
        return this.isClosed;
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
     * Is the server in a sql_mode that doesn't allow us to use \\ to escape
     * things?
     * 
     * @return Returns the noBackslashEscapes.
     */
    public boolean isNoBackslashEscapesSet() {
        return this.noBackslashEscapes;
    }

    public boolean isReadInfoMsgEnabled() {
        return this.readInfoMsg;
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
        if (useSessionStatus && !this.isClosed && versionMeetsMinimum(5, 6, 5) && !this.useLocalSessionState.getValue()
                && this.readOnlyPropagatesToServer.getValue()) {
            java.sql.Statement stmt = null;
            java.sql.ResultSet rs = null;

            try {
                try {
                    stmt = getMetadataSafeStatement();

                    rs = stmt.executeQuery("select @@session.tx_read_only");
                    if (rs.next()) {
                        return rs.getInt(1) != 0; // mysql has a habit of tri+ state booleans
                    }
                } catch (PasswordExpiredException ex) {
                    if (this.disconnectOnExpiredPasswords.getValue()) {
                        throw SQLError.createSQLException(Messages.getString("Connection.16"), SQLError.SQL_STATE_GENERAL_ERROR, ex, getExceptionInterceptor());
                    }
                } catch (SQLException ex1) {
                    if (ex1.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || this.disconnectOnExpiredPasswords.getValue()) {
                        throw SQLError.createSQLException(Messages.getString("Connection.16"), SQLError.SQL_STATE_GENERAL_ERROR, ex1,
                                getExceptionInterceptor());
                    }
                }

            } finally {
                if (rs != null) {
                    try {
                        rs.close();
                    } catch (Exception ex) {
                        // ignore
                    }

                    rs = null;
                }

                if (stmt != null) {
                    try {
                        stmt.close();
                    } catch (Exception ex) {
                        // ignore
                    }

                    stmt = null;
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
            String myResourceId = getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_resourceId).getValue();

            if (otherResourceId != null || myResourceId != null) {
                directCompare = nullSafeCompare(otherResourceId, myResourceId);

                if (directCompare) {
                    return true;
                }
            }

            return false;
        }
    }

    private void createConfigCacheIfNeeded() throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.serverConfigCache != null) {
                return;
            }

            try {
                Class<?> factoryClass;

                factoryClass = Class.forName(getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_serverConfigCacheFactory).getStringValue());

                @SuppressWarnings("unchecked")
                CacheAdapterFactory<String, Map<String, String>> cacheFactory = ((CacheAdapterFactory<String, Map<String, String>>) factoryClass.newInstance());

                this.serverConfigCache = cacheFactory.getInstance(this, this.origHostInfo.getDatabaseUrl(), Integer.MAX_VALUE, Integer.MAX_VALUE, this.props);

                ExceptionInterceptor evictOnCommsError = new ExceptionInterceptor() {

                    public ExceptionInterceptor init(Properties config, Log log) {
                        return this;
                    }

                    public void destroy() {
                    }

                    @SuppressWarnings("synthetic-access")
                    public Exception interceptException(Exception sqlEx) {
                        if (sqlEx instanceof SQLException && ((SQLException) sqlEx).getSQLState() != null
                                && ((SQLException) sqlEx).getSQLState().startsWith("08")) {
                            ConnectionImpl.this.serverConfigCache.invalidate(getURL());
                        }
                        return null;
                    }
                };

                if (this.exceptionInterceptor == null) {
                    this.exceptionInterceptor = evictOnCommsError;
                } else {
                    ((ExceptionInterceptorChain) this.exceptionInterceptor).addRingZero(evictOnCommsError);
                }
            } catch (ClassNotFoundException e) {
                SQLException sqlEx = SQLError.createSQLException(Messages.getString("Connection.CantFindCacheFactory",
                        new Object[] { getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory).getValue(),
                                PropertyDefinitions.PNAME_parseInfoCacheFactory }),
                        getExceptionInterceptor());
                sqlEx.initCause(e);

                throw sqlEx;
            } catch (InstantiationException | IllegalAccessException | CJException e) {
                SQLException sqlEx = SQLError.createSQLException(Messages.getString("Connection.CantLoadCacheFactory",
                        new Object[] { getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory).getValue(),
                                PropertyDefinitions.PNAME_parseInfoCacheFactory }),
                        getExceptionInterceptor());
                sqlEx.initCause(e);

                throw sqlEx;
            }
        }
    }

    private final static String SERVER_VERSION_STRING_VAR_NAME = "server_version_string";

    /**
     * Loads the result of 'SHOW VARIABLES' into the serverVariables field so
     * that the driver can configure itself.
     * 
     * @throws SQLException
     *             if the 'SHOW VARIABLES' query fails for any reason.
     */
    private void loadServerVariables() throws SQLException {

        if (this.cacheServerConfiguration.getValue()) {
            createConfigCacheIfNeeded();

            Map<String, String> cachedVariableMap = this.serverConfigCache.get(getURL());

            if (cachedVariableMap != null) {
                String cachedServerVersion = cachedVariableMap.get(SERVER_VERSION_STRING_VAR_NAME);

                if (cachedServerVersion != null && getServerVersion() != null && cachedServerVersion.equals(getServerVersion().toString())) {
                    this.session.setServerVariables(cachedVariableMap);

                    return;
                }

                this.serverConfigCache.invalidate(getURL());
            }
        }

        java.sql.Statement stmt = null;
        java.sql.ResultSet results = null;

        try {
            stmt = getMetadataSafeStatement();

            String version = this.dbmd.getDriverVersion();

            if (version != null && version.indexOf('*') != -1) {
                StringBuilder buf = new StringBuilder(version.length() + 10);

                for (int i = 0; i < version.length(); i++) {
                    char c = version.charAt(i);

                    if (c == '*') {
                        buf.append("[star]");
                    } else {
                        buf.append(c);
                    }
                }

                version = buf.toString();
            }

            String versionComment = (this.propertySet.getBooleanReadableProperty(PropertyDefinitions.PNAME_paranoid).getValue() || version == null) ? ""
                    : "/* " + version + " */";

            this.session.setServerVariables(new HashMap<String, String>());

            try {
                if (versionMeetsMinimum(5, 1, 0)) {
                    StringBuilder queryBuf = new StringBuilder(versionComment).append("SELECT");
                    queryBuf.append("  @@session.auto_increment_increment AS auto_increment_increment");
                    queryBuf.append(", @@character_set_client AS character_set_client");
                    queryBuf.append(", @@character_set_connection AS character_set_connection");
                    queryBuf.append(", @@character_set_results AS character_set_results");
                    queryBuf.append(", @@character_set_server AS character_set_server");
                    queryBuf.append(", @@init_connect AS init_connect");
                    queryBuf.append(", @@interactive_timeout AS interactive_timeout");
                    if (!versionMeetsMinimum(5, 5, 0)) {
                        queryBuf.append(", @@language AS language");
                    }
                    queryBuf.append(", @@license AS license");
                    queryBuf.append(", @@lower_case_table_names AS lower_case_table_names");
                    queryBuf.append(", @@max_allowed_packet AS max_allowed_packet");
                    queryBuf.append(", @@net_buffer_length AS net_buffer_length");
                    queryBuf.append(", @@net_write_timeout AS net_write_timeout");
                    queryBuf.append(", @@query_cache_size AS query_cache_size");
                    queryBuf.append(", @@query_cache_type AS query_cache_type");
                    queryBuf.append(", @@sql_mode AS sql_mode");
                    queryBuf.append(", @@system_time_zone AS system_time_zone");
                    queryBuf.append(", @@time_zone AS time_zone");
                    queryBuf.append(", @@tx_isolation AS tx_isolation");
                    queryBuf.append(", @@wait_timeout AS wait_timeout");

                    results = stmt.executeQuery(queryBuf.toString());
                    if (results.next()) {
                        ResultSetMetaData rsmd = results.getMetaData();
                        for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                            this.session.getServerVariables().put(rsmd.getColumnLabel(i), results.getString(i));
                        }
                    }
                } else {
                    results = stmt.executeQuery(versionComment + "SHOW VARIABLES");
                    while (results.next()) {
                        this.session.getServerVariables().put(results.getString(1), results.getString(2));
                    }
                }

                results.close();
                results = null;
            } catch (PasswordExpiredException ex) {
                if (this.disconnectOnExpiredPasswords.getValue()) {
                    throw ex;
                }
            } catch (SQLException ex) {
                if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || this.disconnectOnExpiredPasswords.getValue()) {
                    throw ex;
                }
            }

            if (this.cacheServerConfiguration.getValue()) {
                this.session.getServerVariables().put(SERVER_VERSION_STRING_VAR_NAME, getServerVersion().toString());

                this.serverConfigCache.put(getURL(), this.session.getServerVariables());
            }
        } catch (SQLException e) {
            throw e;
        } finally {
            if (results != null) {
                try {
                    results.close();
                } catch (SQLException sqlE) {
                }
            }

            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException sqlE) {
                }
            }
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
        return this.lowerCaseTableNames;
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

        Object escapedSqlResult = EscapeProcessor.escapeSQL(sql, getMultiHostSafeProxy().getSession().getDefaultTimeZone(),
                getMultiHostSafeProxy().getSession().serverSupportsFracSecs(), getExceptionInterceptor());

        if (escapedSqlResult instanceof String) {
            return (String) escapedSqlResult;
        }

        return ((EscapeProcessorResult) escapedSqlResult).escapedSql;
    }

    private CallableStatement parseCallableStatement(String sql) throws SQLException {
        Object escapedSqlResult = EscapeProcessor.escapeSQL(sql, getMultiHostSafeProxy().getSession().getDefaultTimeZone(),
                getMultiHostSafeProxy().getSession().serverSupportsFracSecs(), getExceptionInterceptor());

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
        if (checkForClosedConnection) {
            checkClosed();
        }

        long pingMillisLifetime = getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_selfDestructOnPingSecondsLifetime).getValue();
        int pingMaxOperations = getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_selfDestructOnPingMaxOperations).getValue();

        if ((pingMillisLifetime > 0 && (System.currentTimeMillis() - this.connectionCreationTimeMillis) > pingMillisLifetime)
                || (pingMaxOperations > 0 && pingMaxOperations <= this.session.getCommandCount())) {

            close(); // TODO: do it via Listeners

            throw SQLError.createSQLException(Messages.getString("Connection.exceededConnectionLifetime"), SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE,
                    getExceptionInterceptor());
        }
        this.session.sendCommand(MysqlaConstants.COM_PING, null, null, false, null, timeoutMillis);
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

        if (!getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_cacheCallableStmts).getValue()) {

            cStmt = parseCallableStatement(sql);
        } else {
            synchronized (this.parsedCallableStatementCache) {
                CompoundCacheKey key = new CompoundCacheKey(getCatalog(), sql);

                CallableStatement.CallableStatementParamInfo cachedParamInfo = (CallableStatement.CallableStatementParamInfo) this.parsedCallableStatementCache
                        .get(key);

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
                throw SQLError.createSQLException(Messages.getString("Connection.17"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
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

        ((com.mysql.cj.jdbc.PreparedStatement) pStmt).setRetrieveGeneratedKeys(autoGenKeyIndex == java.sql.Statement.RETURN_GENERATED_KEYS);

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
            PreparedStatement pStmt = null;

            boolean canServerPrepare = true;

            String nativeSql = this.processEscapeCodesForPrepStmts.getValue() ? nativeSQL(sql) : sql;

            if (this.useServerPreparedStmts && this.emulateUnsupportedPstmts.getValue()) {
                canServerPrepare = canHandleAsServerPreparedStatement(nativeSql);
            }

            if (this.useServerPreparedStmts && canServerPrepare) {
                if (this.cachePrepStmts.getValue()) {
                    synchronized (this.serverSideStatementCache) {
                        pStmt = (com.mysql.cj.jdbc.ServerPreparedStatement) this.serverSideStatementCache.remove(sql);

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
                                    pStmt = (PreparedStatement) clientPrepareStatement(nativeSql, resultSetType, resultSetConcurrency, false);

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
                            pStmt = (PreparedStatement) clientPrepareStatement(nativeSql, resultSetType, resultSetConcurrency, false);
                        } else {
                            throw sqlEx;
                        }
                    }
                }
            } else {
                pStmt = (PreparedStatement) clientPrepareStatement(nativeSql, resultSetType, resultSetConcurrency, false);
            }

            return pStmt;
        }
    }

    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (this.pedantic.getValue()) {
            if (resultSetHoldability != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                throw SQLError.createSQLException(Messages.getString("Connection.17"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public java.sql.PreparedStatement prepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);

        ((com.mysql.cj.jdbc.PreparedStatement) pStmt).setRetrieveGeneratedKeys((autoGenKeyIndexes != null) && (autoGenKeyIndexes.length > 0));

        return pStmt;
    }

    public java.sql.PreparedStatement prepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);

        ((com.mysql.cj.jdbc.PreparedStatement) pStmt).setRetrieveGeneratedKeys((autoGenKeyColNames != null) && (autoGenKeyColNames.length > 0));

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

        this.forceClosedReason = reason;

        try {
            if (!skipLocalTeardown) {
                if (!getAutoCommit() && issueRollback) {
                    try {
                        rollback();
                    } catch (SQLException ex) {
                        sqlEx = ex;
                    }
                }

                reportMetrics();

                if (this.useUsageAdvisor.getValue()) {
                    if (!calledExplicitly) {
                        this.session.getProfilerEventHandler().consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_WARN, "", this.getCatalog(), this.getId(),
                                -1, -1, System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, this.pointOfOrigin, Messages.getString("Connection.18")));
                    }

                    long connectionLifeTime = System.currentTimeMillis() - this.connectionCreationTimeMillis;

                    if (connectionLifeTime < 500) {
                        this.session.getProfilerEventHandler().consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_WARN, "", this.getCatalog(), this.getId(),
                                -1, -1, System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, this.pointOfOrigin, Messages.getString("Connection.19")));
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

            if (this.statementInterceptors != null) {
                for (int i = 0; i < this.statementInterceptors.size(); i++) {
                    this.statementInterceptors.get(i).destroy();
                }
            }

            if (this.exceptionInterceptor != null) {
                this.exceptionInterceptor.destroy();
            }
        } finally {
            ProfilerEventHandlerFactory.removeInstance(this.session);

            this.openStatements.clear();
            this.statementInterceptors = null;
            this.exceptionInterceptor = null;
            this.nullStatementResultSetFactory = null;

            synchronized (getConnectionMutex()) {
                if (this.cancelTimer != null) {
                    this.cancelTimer.cancel();
                }
            }

            this.isClosed = true;
        }

        if (sqlEx != null) {
            throw sqlEx;
        }

    }

    public void recachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.cachePrepStmts.getValue() && pstmt.isPoolable()) {
                synchronized (this.serverSideStatementCache) {
                    this.serverSideStatementCache.put(pstmt.originalSql, pstmt);
                }
            }
        }
    }

    public void decachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.cachePrepStmts.getValue() && pstmt.isPoolable()) {
                synchronized (this.serverSideStatementCache) {
                    this.serverSideStatementCache.remove(pstmt.originalSql);
                }
            }
        }
    }

    /**
     * @param queryTimeMs
     */
    public void registerQueryExecutionTime(long queryTimeMs) {
        if (queryTimeMs > this.longestQueryTimeMs) {
            this.longestQueryTimeMs = queryTimeMs;

            repartitionPerformanceHistogram();
        }

        addToPerformanceHistogram(queryTimeMs, 1);

        if (queryTimeMs < this.shortestQueryTimeMs) {
            this.shortestQueryTimeMs = (queryTimeMs == 0) ? 1 : queryTimeMs;
        }

        this.numberOfQueriesIssued++;

        this.totalQueryTimeMs += queryTimeMs;
    }

    /**
     * Register a Statement instance as open.
     * 
     * @param stmt
     *            the Statement instance to remove
     */
    public void registerStatement(Statement stmt) {
        this.openStatements.addIfAbsent(stmt);
    }

    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        // this is a no-op
    }

    private void repartitionHistogram(int[] histCounts, long[] histBreakpoints, long currentLowerBound, long currentUpperBound) {

        if (this.oldHistCounts == null) {
            this.oldHistCounts = new int[histCounts.length];
            this.oldHistBreakpoints = new long[histBreakpoints.length];
        }

        System.arraycopy(histCounts, 0, this.oldHistCounts, 0, histCounts.length);

        System.arraycopy(histBreakpoints, 0, this.oldHistBreakpoints, 0, histBreakpoints.length);

        createInitialHistogram(histBreakpoints, currentLowerBound, currentUpperBound);

        for (int i = 0; i < HISTOGRAM_BUCKETS; i++) {
            addToHistogram(histCounts, histBreakpoints, this.oldHistBreakpoints[i], this.oldHistCounts[i], currentLowerBound, currentUpperBound);
        }
    }

    private void repartitionPerformanceHistogram() {
        checkAndCreatePerformanceHistogram();

        repartitionHistogram(this.perfMetricsHistCounts, this.perfMetricsHistBreakpoints,
                this.shortestQueryTimeMs == Long.MAX_VALUE ? 0 : this.shortestQueryTimeMs, this.longestQueryTimeMs);
    }

    private void repartitionTablesAccessedHistogram() {
        checkAndCreateTablesAccessedHistogram();

        repartitionHistogram(this.numTablesMetricsHistCounts, this.numTablesMetricsHistBreakpoints,
                this.minimumNumberTablesAccessed == Long.MAX_VALUE ? 0 : this.minimumNumberTablesAccessed, this.maximumNumberTablesAccessed);
    }

    private void reportMetrics() {
        if (this.gatherPerfMetrics.getValue()) {
            StringBuilder logMessage = new StringBuilder(256);

            logMessage.append("** Performance Metrics Report **\n");
            logMessage.append("\nLongest reported query: " + this.longestQueryTimeMs + " ms");
            logMessage.append("\nShortest reported query: " + this.shortestQueryTimeMs + " ms");
            logMessage.append("\nAverage query execution time: " + (this.totalQueryTimeMs / this.numberOfQueriesIssued) + " ms");
            logMessage.append("\nNumber of statements executed: " + this.numberOfQueriesIssued);
            logMessage.append("\nNumber of result sets created: " + this.numberOfResultSetsCreated);
            logMessage.append("\nNumber of statements prepared: " + this.numberOfPrepares);
            logMessage.append("\nNumber of prepared statement executions: " + this.numberOfPreparedExecutes);

            if (this.perfMetricsHistBreakpoints != null) {
                logMessage.append("\n\n\tTiming Histogram:\n");
                int maxNumPoints = 20;
                int highestCount = Integer.MIN_VALUE;

                for (int i = 0; i < (HISTOGRAM_BUCKETS); i++) {
                    if (this.perfMetricsHistCounts[i] > highestCount) {
                        highestCount = this.perfMetricsHistCounts[i];
                    }
                }

                if (highestCount == 0) {
                    highestCount = 1; // avoid DIV/0
                }

                for (int i = 0; i < (HISTOGRAM_BUCKETS - 1); i++) {

                    if (i == 0) {
                        logMessage.append("\n\tless than " + this.perfMetricsHistBreakpoints[i + 1] + " ms: \t" + this.perfMetricsHistCounts[i]);
                    } else {
                        logMessage.append("\n\tbetween " + this.perfMetricsHistBreakpoints[i] + " and " + this.perfMetricsHistBreakpoints[i + 1] + " ms: \t"
                                + this.perfMetricsHistCounts[i]);
                    }

                    logMessage.append("\t");

                    int numPointsToGraph = (int) (maxNumPoints * ((double) this.perfMetricsHistCounts[i] / (double) highestCount));

                    for (int j = 0; j < numPointsToGraph; j++) {
                        logMessage.append("*");
                    }

                    if (this.longestQueryTimeMs < this.perfMetricsHistCounts[i + 1]) {
                        break;
                    }
                }

                if (this.perfMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 2] < this.longestQueryTimeMs) {
                    logMessage.append("\n\tbetween ");
                    logMessage.append(this.perfMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 2]);
                    logMessage.append(" and ");
                    logMessage.append(this.perfMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 1]);
                    logMessage.append(" ms: \t");
                    logMessage.append(this.perfMetricsHistCounts[HISTOGRAM_BUCKETS - 1]);
                }
            }

            if (this.numTablesMetricsHistBreakpoints != null) {
                logMessage.append("\n\n\tTable Join Histogram:\n");
                int maxNumPoints = 20;
                int highestCount = Integer.MIN_VALUE;

                for (int i = 0; i < (HISTOGRAM_BUCKETS); i++) {
                    if (this.numTablesMetricsHistCounts[i] > highestCount) {
                        highestCount = this.numTablesMetricsHistCounts[i];
                    }
                }

                if (highestCount == 0) {
                    highestCount = 1; // avoid DIV/0
                }

                for (int i = 0; i < (HISTOGRAM_BUCKETS - 1); i++) {

                    if (i == 0) {
                        logMessage.append("\n\t" + this.numTablesMetricsHistBreakpoints[i + 1] + " tables or less: \t\t" + this.numTablesMetricsHistCounts[i]);
                    } else {
                        logMessage.append("\n\tbetween " + this.numTablesMetricsHistBreakpoints[i] + " and " + this.numTablesMetricsHistBreakpoints[i + 1]
                                + " tables: \t" + this.numTablesMetricsHistCounts[i]);
                    }

                    logMessage.append("\t");

                    int numPointsToGraph = (int) (maxNumPoints * ((double) this.numTablesMetricsHistCounts[i] / (double) highestCount));

                    for (int j = 0; j < numPointsToGraph; j++) {
                        logMessage.append("*");
                    }

                    if (this.maximumNumberTablesAccessed < this.numTablesMetricsHistBreakpoints[i + 1]) {
                        break;
                    }
                }

                if (this.numTablesMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 2] < this.maximumNumberTablesAccessed) {
                    logMessage.append("\n\tbetween ");
                    logMessage.append(this.numTablesMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 2]);
                    logMessage.append(" and ");
                    logMessage.append(this.numTablesMetricsHistBreakpoints[HISTOGRAM_BUCKETS - 1]);
                    logMessage.append(" tables: ");
                    logMessage.append(this.numTablesMetricsHistCounts[HISTOGRAM_BUCKETS - 1]);
                }
            }

            this.session.getLog().logInfo(logMessage);

            this.metricsLastReportedMs = System.currentTimeMillis();
        }
    }

    /**
     * Reports currently collected metrics if this feature is enabled and the
     * timeout has passed.
     */
    protected void reportMetricsIfNeeded() {
        if (this.gatherPerfMetrics.getValue()) {
            if ((System.currentTimeMillis() - this.metricsLastReportedMs) > getPropertySet()
                    .getIntegerReadableProperty(PropertyDefinitions.PNAME_reportMetricsIntervalMillis).getValue()) {
                reportMetrics();
            }
        }
    }

    public void reportNumberOfTablesAccessed(int numTablesAccessed) {
        if (numTablesAccessed < this.minimumNumberTablesAccessed) {
            this.minimumNumberTablesAccessed = numTablesAccessed;
        }

        if (numTablesAccessed > this.maximumNumberTablesAccessed) {
            this.maximumNumberTablesAccessed = numTablesAccessed;

            repartitionTablesAccessedHistogram();
        }

        addToTablesAccessedHistogram(numTablesAccessed, 1);
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
                if (this.autoCommit) {
                    throw SQLError.createSQLException(Messages.getString("Connection.20"), SQLError.SQL_STATE_CONNECTION_NOT_OPEN, getExceptionInterceptor());
                }
                try {
                    rollbackNoChecks();
                } catch (SQLException sqlEx) {
                    // We ignore non-transactional tables if told to do so
                    if (this.ignoreNonTxTables.getInitialValue() && (sqlEx.getErrorCode() == SQLError.ER_WARNING_NOT_COMPLETE_ROLLBACK)) {
                        return;
                    }
                    throw sqlEx;

                }
            } catch (SQLException sqlException) {
                if (SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlException.getSQLState())) {
                    throw SQLError.createSQLException(Messages.getString("Connection.21"), SQLError.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN,
                            getExceptionInterceptor());
                }

                throw sqlException;
            } finally {
                this.needsPing = this.reconnectAtTxEnd.getValue();
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
                                        SQLError.SQL_STATE_ILLEGAL_ARGUMENT, errno, getExceptionInterceptor());
                            }
                        }
                    }

                    // We ignore non-transactional tables if told to do so
                    if (this.ignoreNonTxTables.getValue() && (sqlEx.getErrorCode() != SQLError.ER_WARNING_NOT_COMPLETE_ROLLBACK)) {
                        throw sqlEx;
                    }

                    if (SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlEx.getSQLState())) {
                        throw SQLError.createSQLException(Messages.getString("Connection.23"), SQLError.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN,
                                getExceptionInterceptor());
                    }

                    throw sqlEx;
                } finally {
                    closeStatement(stmt);
                }
            } finally {
                this.needsPing = this.reconnectAtTxEnd.getValue();
            }
        }
    }

    private void rollbackNoChecks() throws SQLException {
        if (this.useLocalTransactionState.getValue()) {
            if (!this.session.inTransactionOnServer()) {
                return; // effectively a no-op
            }
        }

        execSQL(null, "rollback", -1, null, false, this.database, null, false);
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql) throws SQLException {

        String nativeSql = this.processEscapeCodesForPrepStmts.getValue() ? nativeSQL(sql) : sql;

        return ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.getCatalog(), DEFAULT_RESULT_SET_TYPE,
                DEFAULT_RESULT_SET_CONCURRENCY);
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        String nativeSql = this.processEscapeCodesForPrepStmts.getValue() ? nativeSQL(sql) : sql;

        PreparedStatement pStmt = ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.getCatalog(), DEFAULT_RESULT_SET_TYPE,
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
                throw SQLError.createSQLException(Messages.getString("Connection.17"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }
        }

        return serverPrepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {

        PreparedStatement pStmt = (PreparedStatement) serverPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyIndexes != null) && (autoGenKeyIndexes.length > 0));

        return pStmt;
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        PreparedStatement pStmt = (PreparedStatement) serverPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyColNames != null) && (autoGenKeyColNames.length > 0));

        return pStmt;
    }

    /**
     * If a connection is in auto-commit mode, than all its SQL statements will
     * be executed and committed as individual transactions. Otherwise, its SQL
     * statements are grouped into transactions that are terminated by either
     * commit() or rollback(). By default, new connections are in auto- commit
     * mode. The commit occurs when the statement completes or the next execute
     * occurs, whichever comes first. In the case of statements returning a
     * ResultSet, the statement completes when the last row of the ResultSet has
     * been retrieved or the ResultSet has been closed. In advanced cases, a
     * single statement may return multiple results as well as output parameter
     * values. Here the commit occurs when all results and output param values
     * have been retrieved.
     * <p>
     * <b>Note:</b> MySQL does not support transactions, so this method is a no-op.
     * </p>
     * 
     * @param autoCommitFlag
     *            -
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

                if (this.useLocalSessionState.getValue() && this.autoCommit == autoCommitFlag) {
                    needsSetOnServer = false;
                } else if (!this.autoReconnect.getValue()) {
                    needsSetOnServer = getSession().isSetNeededForAutoCommitMode(autoCommitFlag);
                }

                // this internal value must be set first as failover depends on it being set to true to fail over (which is done by most app servers and
                // connection pools at the end of a transaction), and the driver issues an implicit set based on this value when it (re)-connects to a
                // server so the value holds across connections
                this.autoCommit = autoCommitFlag;

                if (needsSetOnServer) {
                    execSQL(null, autoCommitFlag ? "SET autocommit=1" : "SET autocommit=0", -1, null, false, this.database, null, false);
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
                throw SQLError.createSQLException("Catalog can not be null", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
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
                if (this.lowerCaseTableNames) {
                    if (this.database.equalsIgnoreCase(catalog)) {
                        return;
                    }
                } else {
                    if (this.database.equals(catalog)) {
                        return;
                    }
                }
            }

            String quotedId = this.dbmd.getIdentifierQuoteString();

            if ((quotedId == null) || quotedId.equals(" ")) {
                quotedId = "";
            }

            StringBuilder query = new StringBuilder("USE ");
            query.append(StringUtils.quoteIdentifier(catalog, quotedId, this.pedantic.getValue()));

            execSQL(null, query.toString(), -1, null, false, this.database, null, false);

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

    public void setReadInfoMsgEnabled(boolean flag) {
        this.readInfoMsg = flag;
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
        // note this this is safe even inside a transaction
        if (this.readOnlyPropagatesToServer.getValue() && versionMeetsMinimum(5, 6, 5)) {
            if (!this.useLocalSessionState.getValue() || (readOnlyFlag != this.readOnly)) {
                execSQL(null, "set session transaction " + (readOnlyFlag ? "read only" : "read write"), -1, null, false, this.database, null, false);
            }
        }

        this.readOnly = readOnlyFlag;
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

    private void setSessionVariables() throws SQLException {
        String sessionVariables = getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_sessionVariables).getValue();
        if (sessionVariables != null) {
            List<String> variablesToSet = StringUtils.split(sessionVariables, ",", "\"'", "\"'", false);

            int numVariablesToSet = variablesToSet.size();

            java.sql.Statement stmt = null;

            try {
                stmt = getMetadataSafeStatement();

                for (int i = 0; i < numVariablesToSet; i++) {
                    String variableValuePair = variablesToSet.get(i);

                    if (variableValuePair.startsWith("@")) {
                        stmt.executeUpdate("SET " + variableValuePair);
                    } else {
                        stmt.executeUpdate("SET SESSION " + variableValuePair);
                    }
                }
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
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

            if (getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_alwaysSendSetIsolation).getValue()) {
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
                        throw SQLError.createSQLException(Messages.getString("Connection.25", new Object[] { level }), SQLError.SQL_STATE_DRIVER_NOT_CAPABLE,
                                getExceptionInterceptor());
                }

                execSQL(null, sql, -1, null, false, this.database, null, false);

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
        ModifiableProperty<Boolean> jdbcCompliantTruncation = getPropertySet()
                .<Boolean> getModifiableProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation);
        if (jdbcCompliantTruncation.getValue()) {
            String currentSqlMode = this.session.getServerVariable("sql_mode");

            boolean strictTransTablesIsSet = StringUtils.indexOfIgnoreCase(currentSqlMode, "STRICT_TRANS_TABLES") != -1;

            if (currentSqlMode == null || currentSqlMode.length() == 0 || !strictTransTablesIsSet) {
                StringBuilder commandBuf = new StringBuilder("SET sql_mode='");

                if (currentSqlMode != null && currentSqlMode.length() > 0) {
                    commandBuf.append(currentSqlMode);
                    commandBuf.append(",");
                }

                commandBuf.append("STRICT_TRANS_TABLES'");

                execSQL(null, commandBuf.toString(), -1, null, false, this.database, null, false);

                jdbcCompliantTruncation.setValue(false); // server's handling this for us now
            } else if (strictTransTablesIsSet) {
                // We didn't set it, but someone did, so we piggy back on it
                jdbcCompliantTruncation.setValue(false); // server's handling this for us now
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
                    SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());

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
    public void unregisterStatement(Statement stmt) {
        this.openStatements.remove(stmt);
    }

    public boolean useAnsiQuotedIdentifiers() {
        synchronized (getConnectionMutex()) {
            return this.useAnsiQuotes;
        }
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
                return (CachedResultSetMetaData) this.resultSetMetadataCache.get(sql);
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
            cachedMetaData = new CachedResultSetMetaData();

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
        return this.statementComment;
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
        this.statementComment = comment;
    }

    public void reportQueryTime(long millisOrNanos) {
        synchronized (getConnectionMutex()) {
            this.queryTimeCount++;
            this.queryTimeSum += millisOrNanos;
            this.queryTimeSumSquares += (millisOrNanos * millisOrNanos);
            this.queryTimeMean = ((this.queryTimeMean * (this.queryTimeCount - 1)) + millisOrNanos) / this.queryTimeCount;
        }
    }

    public boolean isAbonormallyLongQuery(long millisOrNanos) {
        synchronized (getConnectionMutex()) {
            if (this.queryTimeCount < 15) {
                return false; // need a minimum amount for this to make sense
            }

            double stddev = Math.sqrt((this.queryTimeSumSquares - ((this.queryTimeSum * this.queryTimeSum) / this.queryTimeCount)) / (this.queryTimeCount - 1));

            return millisOrNanos > (this.queryTimeMean + 5 * stddev);
        }
    }

    public void transactionBegun() throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.connectionLifecycleInterceptors != null) {
                IterateBlock<ConnectionLifecycleInterceptor> iter = new IterateBlock<ConnectionLifecycleInterceptor>(
                        this.connectionLifecycleInterceptors.iterator()) {

                    @Override
                    void forEach(ConnectionLifecycleInterceptor each) throws SQLException {
                        each.transactionBegun();
                    }
                };

                iter.doForAll();
            }
        }
    }

    public void transactionCompleted() throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.connectionLifecycleInterceptors != null) {
                IterateBlock<ConnectionLifecycleInterceptor> iter = new IterateBlock<ConnectionLifecycleInterceptor>(
                        this.connectionLifecycleInterceptors.iterator()) {

                    @Override
                    void forEach(ConnectionLifecycleInterceptor each) throws SQLException {
                        each.transactionCompleted();
                    }
                };

                iter.doForAll();
            }
        }
    }

    public boolean storesLowerCaseTableName() {
        return this.storesLowerCaseTableName;
    }

    private ExceptionInterceptor exceptionInterceptor;

    @Override
    public ExceptionInterceptor getExceptionInterceptor() {
        return this.exceptionInterceptor;
    }

    public boolean getRequiresEscapingEncoder() {
        return this.requiresEscapingEncoder;
    }

    public boolean isServerLocal() throws SQLException {
        try {
            return this.session.isServerLocal(this);
        } catch (CJException ex) {
            SQLException sqlEx = SQLExceptionsMapping.translateException(ex, getExceptionInterceptor());
            throw sqlEx;
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
                execSQL(null, "SET SQL_SELECT_LIMIT=" + (this.session.getSessionMaxRows() == -1 ? "DEFAULT" : this.session.getSessionMaxRows()), -1, null,
                        false, this.database, null, false);
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
            throw SQLError.createSQLException(Messages.getString("Connection.26"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
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
                throw SQLError.createSQLException(Messages.getString("Connection.26"), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            checkClosed();
            this.session.setSocketTimeout(executor, milliseconds);
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
                String clientInfoProvider = getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_clientInfoProvider).getStringValue();
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
                            SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
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
            throw SQLError.createSQLException("Unable to unwrap to " + iface.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
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

    public String getProcessHost() {
        try {
            long threadId = this.getId();
            java.sql.Statement processListStmt = getMetadataSafeStatement();
            String processHost = null;

            try {
                processHost = findProcessHost(threadId, processListStmt);

                if (processHost == null) {
                    // http://bugs.mysql.com/bug.php?id=44167 - connection ids on the wire wrap at 4 bytes even though they're 64-bit numbers
                    this.session.getLog().logWarn(String.format(
                            "Connection id %d not found in \"SHOW PROCESSLIST\", assuming 32-bit overflow, using SELECT CONNECTION_ID() instead", threadId));

                    ResultSet rs = processListStmt.executeQuery("SELECT CONNECTION_ID()");
                    if (rs.next()) {
                        threadId = rs.getLong(1);
                        processHost = findProcessHost(threadId, processListStmt);
                    } else {
                        this.session.getLog()
                                .logError("No rows returned for statement \"SELECT CONNECTION_ID()\", local connection check will most likely be incorrect");
                    }
                }
            } finally {
                processListStmt.close();
            }

            if (processHost == null) {
                this.session.getLog().logWarn(String.format(
                        "Cannot find process listing for connection %d in SHOW PROCESSLIST output, unable to determine if locally connected", threadId));
            }
            return processHost;
        } catch (SQLException ex) {
            throw ExceptionFactory.createException(ex.getMessage(), ex, this.exceptionInterceptor);
        }
    }

    private static String findProcessHost(long threadId, java.sql.Statement processListStmt) throws SQLException {
        String processHost = null;
        ResultSet rs = processListStmt.executeQuery("SHOW PROCESSLIST");
        while (rs.next()) {
            long id = rs.getLong(1);
            if (threadId == id) {
                processHost = rs.getString(3);
                break;
            }
        }
        return processHost;

    }

    @Override
    public MysqlaSession getSession() {
        return this.session;
    }

    @Override
    public String getHostPortPair() {
        return this.origHostInfo.getHostPortPair();
    }

}
