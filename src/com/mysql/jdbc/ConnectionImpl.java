/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.sql.Blob;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLPermission;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Enumeration;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Stack;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TreeMap;
import java.util.concurrent.Executor;

import com.mysql.jdbc.PreparedStatement.ParseInfo;
import com.mysql.jdbc.log.Log;
import com.mysql.jdbc.log.LogFactory;
import com.mysql.jdbc.log.LogUtils;
import com.mysql.jdbc.log.NullLogger;
import com.mysql.jdbc.profiler.ProfilerEvent;
import com.mysql.jdbc.profiler.ProfilerEventHandler;
import com.mysql.jdbc.util.LRUCache;

/**
 * A Connection represents a session with a specific database. Within the context of a Connection, SQL statements are executed and results are returned.
 * 
 * <P>
 * A Connection's database is able to provide information describing its tables, its supported SQL grammar, its stored procedures, the capabilities of this
 * connection, etc. This information is obtained with the getMetaData method.
 * </p>
 */
public class ConnectionImpl extends ConnectionPropertiesImpl implements MySQLConnection {

    private static final long serialVersionUID = 2877471301981509474L;

    private static final SQLPermission SET_NETWORK_TIMEOUT_PERM = new SQLPermission("setNetworkTimeout");

    private static final SQLPermission ABORT_PERM = new SQLPermission("abort");

    public static final String JDBC_LOCAL_CHARACTER_SET_RESULTS = "jdbc.local.character_set_results";

    public String getHost() {
        return this.host;
    }

    private MySQLConnection proxy = null;
    private InvocationHandler realProxy = null;

    public boolean isProxySet() {
        return this.proxy != null;
    }

    public void setProxy(MySQLConnection proxy) {
        this.proxy = proxy;
        this.realProxy = this.proxy instanceof MultiHostMySQLConnection ? ((MultiHostMySQLConnection) proxy).getThisAsProxy() : null;
    }

    // this connection has to be proxied when using multi-host settings so that statements get routed to the right physical connection
    // (works as "logical" connection)
    private MySQLConnection getProxy() {
        return (this.proxy != null) ? this.proxy : (MySQLConnection) this;
    }

    /**
     * @deprecated replaced by <code>getMultiHostSafeProxy()</code>
     */
    @Deprecated
    public MySQLConnection getLoadBalanceSafeProxy() {
        return getMultiHostSafeProxy();
    }

    public MySQLConnection getMultiHostSafeProxy() {
        return this.getProxy();
    }

    public Object getConnectionMutex() {
        return (this.realProxy != null) ? this.realProxy : getProxy();
    }

    class ExceptionInterceptorChain implements ExceptionInterceptor {
        List<Extension> interceptors;

        ExceptionInterceptorChain(String interceptorClasses) throws SQLException {
            this.interceptors = Util.loadExtensions(ConnectionImpl.this, ConnectionImpl.this.props, interceptorClasses, "Connection.BadExceptionInterceptor",
                    this);
        }

        void addRingZero(ExceptionInterceptor interceptor) throws SQLException {
            this.interceptors.add(0, interceptor);
        }

        public SQLException interceptException(SQLException sqlEx, Connection conn) {
            if (this.interceptors != null) {
                Iterator<Extension> iter = this.interceptors.iterator();

                while (iter.hasNext()) {
                    sqlEx = ((ExceptionInterceptor) iter.next()).interceptException(sqlEx, ConnectionImpl.this);
                }
            }

            return sqlEx;
        }

        public void destroy() {
            if (this.interceptors != null) {
                Iterator<Extension> iter = this.interceptors.iterator();

                while (iter.hasNext()) {
                    ((ExceptionInterceptor) iter.next()).destroy();
                }
            }

        }

        public void init(Connection conn, Properties properties) throws SQLException {
            if (this.interceptors != null) {
                Iterator<Extension> iter = this.interceptors.iterator();

                while (iter.hasNext()) {
                    ((ExceptionInterceptor) iter.next()).init(conn, properties);
                }
            }
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

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#equals(java.lang.Object)
         */
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

        /*
         * (non-Javadoc)
         * 
         * @see java.lang.Object#hashCode()
         */
        @Override
        public int hashCode() {
            return this.hashCode;
        }
    }

    /**
     * Marker for character set converter not being available (not written,
     * multibyte, etc) Used to prevent multiple instantiation requests.
     */
    private static final Object CHARSET_CONVERTER_NOT_AVAILABLE_MARKER = new Object();

    /**
     * The mapping between MySQL charset names and Java charset names.
     * Initialized by loadCharacterSetMapping()
     */
    public static Map<?, ?> charsetMap;

    /** Default logger class name */
    protected static final String DEFAULT_LOGGER_CLASS = "com.mysql.jdbc.log.StandardLogger";

    private final static int HISTOGRAM_BUCKETS = 20;

    /** Logger instance name */
    private static final String LOGGER_INSTANCE_NAME = "MySQL";

    /**
     * Map mysql transaction isolation level name to
     * java.sql.Connection.TRANSACTION_XXX
     */
    private static Map<String, Integer> mapTransIsolationNameToValue = null;

    /** Null logger shared by all connections at startup */
    private static final Log NULL_LOGGER = new NullLogger(LOGGER_INSTANCE_NAME);

    protected static Map<?, ?> roundRobinStatsMap;

    /**
     * Actual collation index to collation name map for given server URLs.
     */
    private static final Map<String, Map<Long, String>> dynamicIndexToCollationMapByUrl = new HashMap<String, Map<Long, String>>();

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

    private List<Extension> connectionLifecycleInterceptors;

    private static final Constructor<?> JDBC_4_CONNECTION_CTOR;

    private static final int DEFAULT_RESULT_SET_TYPE = ResultSet.TYPE_FORWARD_ONLY;

    private static final int DEFAULT_RESULT_SET_CONCURRENCY = ResultSet.CONCUR_READ_ONLY;

    static {
        mapTransIsolationNameToValue = new HashMap<String, Integer>(8);
        mapTransIsolationNameToValue.put("READ-UNCOMMITED", TRANSACTION_READ_UNCOMMITTED);
        mapTransIsolationNameToValue.put("READ-UNCOMMITTED", TRANSACTION_READ_UNCOMMITTED);
        mapTransIsolationNameToValue.put("READ-COMMITTED", TRANSACTION_READ_COMMITTED);
        mapTransIsolationNameToValue.put("REPEATABLE-READ", TRANSACTION_REPEATABLE_READ);
        mapTransIsolationNameToValue.put("SERIALIZABLE", TRANSACTION_SERIALIZABLE);

        if (Util.isJdbc4()) {
            try {
                JDBC_4_CONNECTION_CTOR = Class.forName("com.mysql.jdbc.JDBC4Connection")
                        .getConstructor(new Class[] { String.class, Integer.TYPE, Properties.class, String.class, String.class });
            } catch (SecurityException e) {
                throw new RuntimeException(e);
            } catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        } else {
            JDBC_4_CONNECTION_CTOR = null;
        }
    }

    protected static SQLException appendMessageToException(SQLException sqlEx, String messageToAppend, ExceptionInterceptor interceptor) {
        String origMessage = sqlEx.getMessage();
        String sqlState = sqlEx.getSQLState();
        int vendorErrorCode = sqlEx.getErrorCode();

        StringBuilder messageBuf = new StringBuilder(origMessage.length() + messageToAppend.length());
        messageBuf.append(origMessage);
        messageBuf.append(messageToAppend);

        SQLException sqlExceptionWithNewMessage = SQLError.createSQLException(messageBuf.toString(), sqlState, vendorErrorCode, interceptor);

        //
        // Try and maintain the original stack trace, only works on JDK-1.4 and newer
        //

        try {
            // Have to do this with reflection, otherwise older JVMs croak
            Method getStackTraceMethod = null;
            Method setStackTraceMethod = null;
            Object theStackTraceAsObject = null;

            Class<?> stackTraceElementClass = Class.forName("java.lang.StackTraceElement");
            Class<?> stackTraceElementArrayClass = Array.newInstance(stackTraceElementClass, new int[] { 0 }).getClass();

            getStackTraceMethod = Throwable.class.getMethod("getStackTrace", new Class[] {});

            setStackTraceMethod = Throwable.class.getMethod("setStackTrace", new Class[] { stackTraceElementArrayClass });

            if (getStackTraceMethod != null && setStackTraceMethod != null) {
                theStackTraceAsObject = getStackTraceMethod.invoke(sqlEx, new Object[0]);
                setStackTraceMethod.invoke(sqlExceptionWithNewMessage, new Object[] { theStackTraceAsObject });
            }
        } catch (NoClassDefFoundError noClassDefFound) {

        } catch (NoSuchMethodException noSuchMethodEx) {

        } catch (Throwable catchAll) {

        }

        return sqlExceptionWithNewMessage;
    }

    public Timer getCancelTimer() {
        synchronized (getConnectionMutex()) {
            if (this.cancelTimer == null) {
                boolean createdNamedTimer = false;

                // Use reflection magic to try this on JDK's 1.5 and newer, fallback to non-named timer on older VMs.
                try {
                    Constructor<Timer> ctr = Timer.class.getConstructor(new Class[] { String.class, Boolean.TYPE });

                    this.cancelTimer = ctr.newInstance(new Object[] { "MySQL Statement Cancellation Timer", Boolean.TRUE });
                    createdNamedTimer = true;
                } catch (Throwable t) {
                    createdNamedTimer = false;
                }

                if (!createdNamedTimer) {
                    this.cancelTimer = new Timer(true);
                }
            }

            return this.cancelTimer;
        }
    }

    /**
     * Creates a connection instance -- We need to provide factory-style methods
     * so we can support both JDBC3 (and older) and JDBC4 runtimes, otherwise
     * the class verifier complains when it tries to load JDBC4-only interface
     * classes that are present in JDBC4 method signatures.
     */

    protected static Connection getInstance(String hostToConnectTo, int portToConnectTo, Properties info, String databaseToConnectTo, String url)
            throws SQLException {
        if (!Util.isJdbc4()) {
            return new ConnectionImpl(hostToConnectTo, portToConnectTo, info, databaseToConnectTo, url);
        }

        return (Connection) Util.handleNewInstance(JDBC_4_CONNECTION_CTOR,
                new Object[] { hostToConnectTo, Integer.valueOf(portToConnectTo), info, databaseToConnectTo, url }, null);
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

    /**
     * For servers > 4.1.0, what character set is the metadata returned in?
     */
    private String characterSetMetadata = null;

    /**
     * The character set we want results and result metadata returned in (null ==
     * results in any charset, metadata in UTF-8).
     */
    private String characterSetResultsOnServer = null;

    /**
     * Holds cached mappings to charset converters to avoid static
     * synchronization and at the same time save memory (each charset converter
     * takes approx 65K of static data).
     */
    private Map<String, Object> charsetConverterMap = new HashMap<String, Object>(CharsetMapping.getNumberOfCharsetsConfigured());

    /** The point in time when this connection was created */
    private long connectionCreationTimeMillis = 0;

    /** ID used when profiling */
    private long connectionId;

    /** The database we're currently using (called Catalog in JDBC terms). */
    private String database = null;

    /** Internal DBMD to use for various database-version specific features */
    private DatabaseMetaData dbmd = null;

    private TimeZone defaultTimeZone;

    /** The event sink to use for profiling */
    private ProfilerEventHandler eventSink;

    /** Why was this connection implicitly closed, if known? (for diagnostics) */
    private Throwable forceClosedReason;

    /** Does the server support isolation levels? */
    private boolean hasIsolationLevels = false;

    /** Does this version of MySQL support quoted identifiers? */
    private boolean hasQuotedIdentifiers = false;

    /** The hostname we're connected to */
    private String host = null;

    /**
     * We need this 'bootstrapped', because 4.1 and newer will send fields back
     * with this even before we fill this dynamically from the server.
     */
    public Map<Integer, String> indexToMysqlCharset = new HashMap<Integer, String>();

    public Map<Integer, String> indexToCustomMysqlCharset = null; //new HashMap<Integer, String>();

    private Map<String, Integer> mysqlCharsetToCustomMblen = null; //new HashMap<String, Integer>();

    /** The I/O abstraction interface (network conn to MySQL server */
    private transient MysqlIO io = null;

    private boolean isClientTzUTC = false;

    /** Has this connection been closed? */
    private boolean isClosed = true;

    /** Is this connection associated with a global tx? */
    private boolean isInGlobalTx = false;

    /** Is this connection running inside a JDK-1.3 VM? */
    private boolean isRunningOnJDK13 = false;

    /** isolation level */
    private int isolationLevel = java.sql.Connection.TRANSACTION_READ_COMMITTED;

    private boolean isServerTzUTC = false;

    /** When did the last query finish? */
    private long lastQueryFinishedTime = 0;

    /** The logger we're going to use */
    private transient Log log = NULL_LOGGER;

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

    /** The max-rows setting for current session */
    private int sessionMaxRows = -1;

    /** When was the last time we reported metrics? */
    private long metricsLastReportedMs;

    private long minimumNumberTablesAccessed = Long.MAX_VALUE;

    /** The JDBC URL we're using */
    private String myURL = null;

    /** Does this connection need to be tested? */
    private boolean needsPing = false;

    private int netBufferLength = 16384;

    private boolean noBackslashEscapes = false;

    private long numberOfPreparedExecutes = 0;

    private long numberOfPrepares = 0;

    private long numberOfQueriesIssued = 0;

    private long numberOfResultSetsCreated = 0;

    private long[] numTablesMetricsHistBreakpoints;

    private int[] numTablesMetricsHistCounts;

    private long[] oldHistBreakpoints = null;

    private int[] oldHistCounts = null;

    /** A map of currently open statements */
    private Map<Statement, Statement> openStatements;

    private LRUCache parsedCallableStatementCache;

    private boolean parserKnowsUnicode = false;

    /** The password we used */
    private String password = null;

    private long[] perfMetricsHistBreakpoints;

    private int[] perfMetricsHistCounts;

    /** Point of origin where this Connection was created */
    private String pointOfOrigin;

    /** The port number we're connected to (defaults to 3306) */
    private int port = 3306;

    /** Properties for this connection specified by user */
    protected Properties props = null;

    /** Should we retrieve 'info' messages from the server? */
    private boolean readInfoMsg = false;

    /** Are we in read-only mode? */
    private boolean readOnly = false;

    /** Cache of ResultSet metadata */
    protected LRUCache resultSetMetadataCache;

    /** The timezone of the server */
    private TimeZone serverTimezoneTZ = null;

    /** The map of server variables that we retrieve at connection init. */
    private Map<String, String> serverVariables = null;

    private long shortestQueryTimeMs = Long.MAX_VALUE;

    private double totalQueryTimeMs = 0;

    /** Are transactions supported by the MySQL server we are connected to? */
    private boolean transactionsSupported = false;

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
    private Calendar sessionCalendar;

    private Calendar utcCalendar;

    private String origHostToConnectTo;

    // we don't want to be able to publicly clone this...

    private int origPortToConnectTo;

    private String origDatabaseToConnectTo;

    private String errorMessageEncoding = "Cp1252"; // to begin with, changes after we talk to the server

    private boolean usePlatformCharsetConverters;

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

    private List<StatementInterceptorV2> statementInterceptors;

    /**
     * If a CharsetEncoder is required for escaping. Needed for SJIS and related
     * problems with \u00A5.
     */
    private boolean requiresEscapingEncoder;

    private String hostPortPair;

    /**
     * '
     * For the delegate only
     */
    protected ConnectionImpl() {
    }

    /**
     * Creates a connection to a MySQL Server.
     * 
     * @param hostToConnectTo
     *            the hostname of the database server
     * @param portToConnectTo
     *            the port number the server is listening on
     * @param info
     *            a Properties[] list holding the user and password
     * @param databaseToConnectTo
     *            the database to connect to
     * @param url
     *            the URL of the connection
     * @param d
     *            the Driver instantation of the connection
     * @exception SQLException
     *                if a database access error occurs
     */
    public ConnectionImpl(String hostToConnectTo, int portToConnectTo, Properties info, String databaseToConnectTo, String url) throws SQLException {

        this.connectionCreationTimeMillis = System.currentTimeMillis();

        if (databaseToConnectTo == null) {
            databaseToConnectTo = "";
        }

        // Stash away for later, used to clone this connection for Statement.cancel and Statement.setQueryTimeout().
        //

        this.origHostToConnectTo = hostToConnectTo;
        this.origPortToConnectTo = portToConnectTo;
        this.origDatabaseToConnectTo = databaseToConnectTo;

        try {
            Blob.class.getMethod("truncate", new Class[] { Long.TYPE });

            this.isRunningOnJDK13 = false;
        } catch (NoSuchMethodException nsme) {
            this.isRunningOnJDK13 = true;
        }

        this.sessionCalendar = new GregorianCalendar();
        this.utcCalendar = new GregorianCalendar();
        this.utcCalendar.setTimeZone(TimeZone.getTimeZone("GMT"));

        //
        // Normally, this code would be in initializeDriverProperties, but we need to do this as early as possible, so we can start logging to the 'correct'
        // place as early as possible...this.log points to 'NullLogger' for every connection at startup to avoid NPEs and the overhead of checking for NULL at
        // every logging call.
        //
        // We will reset this to the configured logger during properties initialization.
        //
        this.log = LogFactory.getLogger(getLogger(), LOGGER_INSTANCE_NAME, getExceptionInterceptor());

        this.openStatements = new HashMap<Statement, Statement>();

        if (NonRegisteringDriver.isHostPropertiesList(hostToConnectTo)) {
            Properties hostSpecificProps = NonRegisteringDriver.expandHostKeyValues(hostToConnectTo);

            Enumeration<?> propertyNames = hostSpecificProps.propertyNames();

            while (propertyNames.hasMoreElements()) {
                String propertyName = propertyNames.nextElement().toString();
                String propertyValue = hostSpecificProps.getProperty(propertyName);

                info.setProperty(propertyName, propertyValue);
            }
        } else {

            if (hostToConnectTo == null) {
                this.host = "localhost";
                this.hostPortPair = this.host + ":" + portToConnectTo;
            } else {
                this.host = hostToConnectTo;

                if (hostToConnectTo.indexOf(":") == -1) {
                    this.hostPortPair = this.host + ":" + portToConnectTo;
                } else {
                    this.hostPortPair = this.host;
                }
            }
        }

        this.port = portToConnectTo;

        this.database = databaseToConnectTo;
        this.myURL = url;
        this.user = info.getProperty(NonRegisteringDriver.USER_PROPERTY_KEY);
        this.password = info.getProperty(NonRegisteringDriver.PASSWORD_PROPERTY_KEY);

        if ((this.user == null) || this.user.equals("")) {
            this.user = "";
        }

        if (this.password == null) {
            this.password = "";
        }

        this.props = info;

        initializeDriverProperties(info);

        // We store this per-connection, due to static synchronization issues in Java's built-in TimeZone class...
        this.defaultTimeZone = TimeUtil.getDefaultTimeZone(getCacheDefaultTimezone());

        this.isClientTzUTC = !this.defaultTimeZone.useDaylightTime() && this.defaultTimeZone.getRawOffset() == 0;

        if (getUseUsageAdvisor()) {
            this.pointOfOrigin = LogUtils.findCallingClassAndMethod(new Throwable());
        } else {
            this.pointOfOrigin = "";
        }

        try {
            this.dbmd = getMetaData(false, false);
            initializeSafeStatementInterceptors();
            createNewIO(false);
            unSafeStatementInterceptors();
        } catch (SQLException ex) {
            cleanup(ex);

            // don't clobber SQL exceptions
            throw ex;
        } catch (Exception ex) {
            cleanup(ex);

            StringBuilder mesg = new StringBuilder(128);

            if (!getParanoid()) {
                mesg.append("Cannot connect to MySQL server on ");
                mesg.append(this.host);
                mesg.append(":");
                mesg.append(this.port);
                mesg.append(".\n\n");
                mesg.append("Make sure that there is a MySQL server ");
                mesg.append("running on the machine/port you are trying ");
                mesg.append("to connect to and that the machine this software is running on ");
                mesg.append("is able to connect to this host/port (i.e. not firewalled). ");
                mesg.append("Also make sure that the server has not been started with the --skip-networking ");
                mesg.append("flag.\n\n");
            } else {
                mesg.append("Unable to connect to database.");
            }

            SQLException sqlEx = SQLError.createSQLException(mesg.toString(), SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE, getExceptionInterceptor());

            sqlEx.initCause(ex);

            throw sqlEx;
        }

        NonRegisteringDriver.trackConnection(this);
    }

    public void unSafeStatementInterceptors() throws SQLException {

        ArrayList<StatementInterceptorV2> unSafedStatementInterceptors = new ArrayList<StatementInterceptorV2>(this.statementInterceptors.size());

        for (int i = 0; i < this.statementInterceptors.size(); i++) {
            NoSubInterceptorWrapper wrappedInterceptor = (NoSubInterceptorWrapper) this.statementInterceptors.get(i);

            unSafedStatementInterceptors.add(wrappedInterceptor.getUnderlyingInterceptor());
        }

        this.statementInterceptors = unSafedStatementInterceptors;

        if (this.io != null) {
            this.io.setStatementInterceptors(this.statementInterceptors);
        }
    }

    public void initializeSafeStatementInterceptors() throws SQLException {
        this.isClosed = false;

        List<Extension> unwrappedInterceptors = Util.loadExtensions(this, this.props, getStatementInterceptors(), "MysqlIo.BadStatementInterceptor",
                getExceptionInterceptor());

        this.statementInterceptors = new ArrayList<StatementInterceptorV2>(unwrappedInterceptors.size());

        for (int i = 0; i < unwrappedInterceptors.size(); i++) {
            Extension interceptor = unwrappedInterceptors.get(i);

            // adapt older versions of statement interceptors, handle the case where something wants v2 functionality but wants to run with an older driver
            if (interceptor instanceof StatementInterceptor) {
                if (ReflectiveStatementInterceptorAdapter.getV2PostProcessMethod(interceptor.getClass()) != null) {
                    this.statementInterceptors.add(new NoSubInterceptorWrapper(new ReflectiveStatementInterceptorAdapter((StatementInterceptor) interceptor)));
                } else {
                    this.statementInterceptors.add(new NoSubInterceptorWrapper(new V1toV2StatementInterceptorAdapter((StatementInterceptor) interceptor)));
                }
            } else {
                this.statementInterceptors.add(new NoSubInterceptorWrapper((StatementInterceptorV2) interceptor));
            }
        }

    }

    public List<StatementInterceptorV2> getStatementInterceptorsInstances() {
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
        Map<Long, String> sortedCollationMap = null;
        Map<Integer, String> customCharset = null;
        Map<String, Integer> customMblen = null;

        if (getCacheServerConfiguration()) {
            synchronized (dynamicIndexToCharsetMapByUrl) {
                indexToCharset = dynamicIndexToCharsetMapByUrl.get(getURL());
                sortedCollationMap = dynamicIndexToCollationMapByUrl.get(getURL());
                customCharset = customIndexToCharsetMapByUrl.get(getURL());
                customMblen = customCharsetToMblenMapByUrl.get(getURL());
            }
        }

        if (indexToCharset == null) {
            indexToCharset = new HashMap<Integer, String>();

            if (versionMeetsMinimum(4, 1, 0) && getDetectCustomCollations()) {

                java.sql.Statement stmt = null;
                java.sql.ResultSet results = null;

                try {
                    sortedCollationMap = new TreeMap<Long, String>();
                    customCharset = new HashMap<Integer, String>();
                    customMblen = new HashMap<String, Integer>();

                    stmt = getMetadataSafeStatement();

                    try {
                        results = stmt.executeQuery("SHOW COLLATION");
                        if (versionMeetsMinimum(5, 0, 0)) {
                            Util.resultSetToMap(sortedCollationMap, results, 3, 2);
                        } else {
                            while (results.next()) {
                                sortedCollationMap.put(results.getLong(3), results.getString(2));
                            }
                        }
                    } catch (SQLException ex) {
                        if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || getDisconnectOnExpiredPasswords()) {
                            throw ex;
                        }
                    }

                    for (Iterator<Map.Entry<Long, String>> indexIter = sortedCollationMap.entrySet().iterator(); indexIter.hasNext();) {
                        Map.Entry<Long, String> indexEntry = indexIter.next();

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
                        } catch (SQLException ex) {
                            if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || getDisconnectOnExpiredPasswords()) {
                                throw ex;
                            }
                        }
                    }

                    if (getCacheServerConfiguration()) {
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
                if (getCacheServerConfiguration()) {
                    synchronized (dynamicIndexToCharsetMapByUrl) {
                        dynamicIndexToCharsetMapByUrl.put(getURL(), indexToCharset);
                    }
                }
            }

        }

        this.indexToMysqlCharset = Collections.unmodifiableMap(indexToCharset);
        if (customCharset != null) {
            this.indexToCustomMysqlCharset = Collections.unmodifiableMap(customCharset);
        }
        if (customMblen != null) {
            this.mysqlCharsetToCustomMblen = Collections.unmodifiableMap(customMblen);
        }
    }

    private boolean canHandleAsServerPreparedStatement(String sql) throws SQLException {
        if (sql == null || sql.length() == 0) {
            return true;
        }

        if (!this.useServerPreparedStmts) {
            return false;
        }

        if (getCachePreparedStatements()) {
            synchronized (this.serverSideStatementCheckCache) {
                Boolean flag = (Boolean) this.serverSideStatementCheckCache.get(sql);

                if (flag != null) {
                    return flag.booleanValue();
                }

                boolean canHandle = canHandleAsServerPreparedStatementNoCache(sql);

                if (sql.length() < getPreparedStatementCacheSqlLimit()) {
                    this.serverSideStatementCheckCache.put(sql, canHandle ? Boolean.TRUE : Boolean.FALSE);
                }

                return canHandle;
            }
        }

        return canHandleAsServerPreparedStatementNoCache(sql);
    }

    private boolean canHandleAsServerPreparedStatementNoCache(String sql) throws SQLException {

        // Can't use server-side prepare for CALL
        if (StringUtils.startsWithIgnoreCaseAndNonAlphaNumeric(sql, "CALL")) {
            return false;
        }

        boolean canHandleAsStatement = true;

        if (!versionMeetsMinimum(5, 0, 7) && (StringUtils.startsWithIgnoreCaseAndNonAlphaNumeric(sql, "SELECT")
                || StringUtils.startsWithIgnoreCaseAndNonAlphaNumeric(sql, "DELETE") || StringUtils.startsWithIgnoreCaseAndNonAlphaNumeric(sql, "INSERT")
                || StringUtils.startsWithIgnoreCaseAndNonAlphaNumeric(sql, "UPDATE") || StringUtils.startsWithIgnoreCaseAndNonAlphaNumeric(sql, "REPLACE"))) {

            // check for limit ?[,?]

            /*
             * The grammar for this (from the server) is: ULONG_NUM | ULONG_NUM ',' ULONG_NUM | ULONG_NUM OFFSET_SYM ULONG_NUM
             */

            int currentPos = 0;
            int statementLength = sql.length();
            int lastPosToLook = statementLength - 7; // "LIMIT ".length()
            boolean allowBackslashEscapes = !this.noBackslashEscapes;
            String quoteChar = this.useAnsiQuotes ? "\"" : "'";
            boolean foundLimitWithPlaceholder = false;

            while (currentPos < lastPosToLook) {
                int limitStart = StringUtils.indexOfIgnoreCase(currentPos, sql, "LIMIT ", quoteChar, quoteChar,
                        allowBackslashEscapes ? StringUtils.SEARCH_MODE__ALL : StringUtils.SEARCH_MODE__MRK_COM_WS);

                if (limitStart == -1) {
                    break;
                }

                currentPos = limitStart + 7;

                while (currentPos < statementLength) {
                    char c = sql.charAt(currentPos);

                    //
                    // Have we reached the end of what can be in a LIMIT clause?
                    //

                    if (!Character.isDigit(c) && !Character.isWhitespace(c) && c != ',' && c != '?') {
                        break;
                    }

                    if (c == '?') {
                        foundLimitWithPlaceholder = true;
                        break;
                    }

                    currentPos++;
                }
            }

            canHandleAsStatement = !foundLimitWithPlaceholder;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(sql, "XA ")) {
            canHandleAsStatement = false;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(sql, "CREATE TABLE")) {
            canHandleAsStatement = false;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(sql, "DO")) {
            canHandleAsStatement = false;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(sql, "SET")) {
            canHandleAsStatement = false;
        } else if (StringUtils.startsWithIgnoreCaseAndWs(sql, "SHOW WARNINGS") && versionMeetsMinimum(5, 7, 2)) {
            canHandleAsStatement = false;
        }

        return canHandleAsStatement;
    }

    /**
     * Changes the user on this connection by performing a re-authentication. If
     * authentication fails, the connection will remain under the context of the
     * current user.
     * 
     * @param userName
     *            the username to authenticate with
     * @param newPassword
     *            the password to authenticate with
     * @throws SQLException
     *             if authentication fails, or some other error occurs while
     *             performing the command.
     */
    public void changeUser(String userName, String newPassword) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();

            if ((userName == null) || userName.equals("")) {
                userName = "";
            }

            if (newPassword == null) {
                newPassword = "";
            }

            // reset maxRows to default value
            this.sessionMaxRows = -1;

            try {
                this.io.changeUser(userName, newPassword, this.database);
            } catch (SQLException ex) {
                if (versionMeetsMinimum(5, 6, 13) && "28000".equals(ex.getSQLState())) {
                    cleanup(ex);
                }
                throw ex;
            }
            this.user = userName;
            this.password = newPassword;

            if (versionMeetsMinimum(4, 1, 0)) {
                configureClientCharacterSet(true);
            }

            setSessionVariables();

            setupServerForTruncationChecks();
        }
    }

    private boolean characterSetNamesMatches(String mysqlEncodingName) {
        // set names is equivalent to character_set_client ..._results and ..._connection, but we set _results later, so don't check it here.
        return (mysqlEncodingName != null && mysqlEncodingName.equalsIgnoreCase(this.serverVariables.get("character_set_client"))
                && mysqlEncodingName.equalsIgnoreCase(this.serverVariables.get("character_set_connection")));
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

    public void checkClosed() throws SQLException {
        if (this.isClosed) {
            throwConnectionClosedException();
        }
    }

    public void throwConnectionClosedException() throws SQLException {
        SQLException ex = SQLError.createSQLException("No operations allowed after connection closed.", SQLError.SQL_STATE_CONNECTION_NOT_OPEN,
                getExceptionInterceptor());

        if (this.forceClosedReason != null) {
            ex.initCause(this.forceClosedReason);
        }

        throw ex;
    }

    /**
     * If useUnicode flag is set and explicit client character encoding isn't
     * specified then assign encoding from server if any.
     * 
     * @throws SQLException
     */
    private void checkServerEncoding() throws SQLException {
        if (getUseUnicode() && (getEncoding() != null)) {
            // spec'd by client, don't map
            return;
        }

        String serverCharset = this.serverVariables.get("character_set");

        if (serverCharset == null) {
            // must be 4.1.1 or newer?
            serverCharset = this.serverVariables.get("character_set_server");
        }

        String mappedServerEncoding = null;

        if (serverCharset != null) {
            try {
                mappedServerEncoding = CharsetMapping.getJavaEncodingForMysqlCharset(serverCharset);
            } catch (RuntimeException ex) {
                SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
                sqlEx.initCause(ex);
                throw sqlEx;
            }
        }

        //
        // First check if we can do the encoding ourselves
        //
        if (!getUseUnicode() && (mappedServerEncoding != null)) {
            SingleByteCharsetConverter converter = getCharsetConverter(mappedServerEncoding);

            if (converter != null) { // we know how to convert this ourselves
                setUseUnicode(true); // force the issue
                setEncoding(mappedServerEncoding);

                return;
            }
        }

        //
        // Now, try and find a Java I/O converter that can do the encoding for us
        //
        if (serverCharset != null) {
            if (mappedServerEncoding == null) {
                // We don't have a mapping for it, so try and canonicalize the name....
                if (Character.isLowerCase(serverCharset.charAt(0))) {
                    char[] ach = serverCharset.toCharArray();
                    ach[0] = Character.toUpperCase(serverCharset.charAt(0));
                    setEncoding(new String(ach));
                }
            }

            if (mappedServerEncoding == null) {
                throw SQLError.createSQLException(
                        "Unknown character encoding on server '" + serverCharset + "', use 'characterEncoding=' property " + " to provide correct mapping",
                        SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, getExceptionInterceptor());
            }

            //
            // Attempt to use the encoding, and bail out if it can't be used
            //
            try {
                StringUtils.getBytes("abc", mappedServerEncoding);
                setEncoding(mappedServerEncoding);
                setUseUnicode(true);
            } catch (UnsupportedEncodingException UE) {
                throw SQLError.createSQLException("The driver can not map the character encoding '" + getEncoding() + "' that your server is using "
                        + "to a character encoding your JVM understands. You can specify this mapping manually by adding \"useUnicode=true\" "
                        + "as well as \"characterEncoding=[an_encoding_your_jvm_understands]\" to your JDBC URL.", "0S100", getExceptionInterceptor());
            }
        }
    }

    /**
     * Set transaction isolation level to the value received from server if any.
     * Is called by connectionInit(...)
     * 
     * @throws SQLException
     */
    private void checkTransactionIsolationLevel() throws SQLException {
        String txIsolationName = null;

        if (versionMeetsMinimum(4, 0, 3)) {
            txIsolationName = "tx_isolation";
        } else {
            txIsolationName = "transaction_isolation";
        }

        String s = this.serverVariables.get(txIsolationName);

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
        if (this.io != null) {
            // checking this.io != null isn't enough if connection is used concurrently (the usual situation
            // with application servers which have additional thread management), this.io can become null
            // at any moment after this check, causing a race condition and NPEs on next calls;
            // but we may ignore them because at this stage null this.io means that we successfully closed all resources by other thread.
            try {
                this.io.forceClose();
                this.io.releaseResources();
            } catch (Throwable t) {
                // can't do anything about it, and we're forcibly aborting
            }
            this.io = null;
        }

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
            if (this.io != null) {
                if (isClosed()) {
                    this.io.forceClose();
                } else {
                    realClose(false, false, false, whyCleanedUp);
                }
            }
        } catch (SQLException sqlEx) {
            // ignore, we're going away.
        }

        this.isClosed = true;
    }

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

    /**
     * @see Connection#prepareStatement(String, int)
     */
    public java.sql.PreparedStatement clientPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        java.sql.PreparedStatement pStmt = clientPrepareStatement(sql);

        ((com.mysql.jdbc.PreparedStatement) pStmt).setRetrieveGeneratedKeys(autoGenKeyIndex == java.sql.Statement.RETURN_GENERATED_KEYS);

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
        checkClosed();

        String nativeSql = processEscapeCodesIfNeeded && getProcessEscapeCodesForPrepStmts() ? nativeSQL(sql) : sql;

        PreparedStatement pStmt = null;

        if (getCachePreparedStatements()) {
            PreparedStatement.ParseInfo pStmtInfo = this.cachedPreparedStatementParams.get(nativeSql);

            if (pStmtInfo == null) {
                pStmt = com.mysql.jdbc.PreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.database);

                this.cachedPreparedStatementParams.put(nativeSql, pStmt.getParseInfo());
            } else {
                pStmt = new com.mysql.jdbc.PreparedStatement(getMultiHostSafeProxy(), nativeSql, this.database, pStmtInfo);
            }
        } else {
            pStmt = com.mysql.jdbc.PreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.database);
        }

        pStmt.setResultSetType(resultSetType);
        pStmt.setResultSetConcurrency(resultSetConcurrency);

        return pStmt;
    }

    /**
     * @see java.sql.Connection#prepareStatement(String, int[])
     */
    public java.sql.PreparedStatement clientPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {

        PreparedStatement pStmt = (PreparedStatement) clientPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyIndexes != null) && (autoGenKeyIndexes.length > 0));

        return pStmt;
    }

    /**
     * @see java.sql.Connection#prepareStatement(String, String[])
     */
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
                new IterateBlock<Extension>(this.connectionLifecycleInterceptors.iterator()) {
                    @Override
                    void forEach(Extension each) throws SQLException {
                        ((ConnectionLifecycleInterceptor) each).close();
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

        if (this.openStatements != null) {
            List<Statement> currentlyOpenStatements = new ArrayList<Statement>(); // we need this to
            // avoid ConcurrentModificationEx

            for (Iterator<Statement> iter = this.openStatements.keySet().iterator(); iter.hasNext();) {
                currentlyOpenStatements.add(iter.next());
            }

            int numStmts = currentlyOpenStatements.size();

            for (int i = 0; i < numStmts; i++) {
                StatementImpl stmt = (StatementImpl) currentlyOpenStatements.get(i);

                try {
                    stmt.realClose(false, true);
                } catch (SQLException sqlEx) {
                    postponedException = sqlEx; // throw it later, cleanup all
                    // statements first
                }
            }

            if (postponedException != null) {
                throw postponedException;
            }
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
                    IterateBlock<Extension> iter = new IterateBlock<Extension>(this.connectionLifecycleInterceptors.iterator()) {

                        @Override
                        void forEach(Extension each) throws SQLException {
                            if (!((ConnectionLifecycleInterceptor) each).commit()) {
                                this.stopIterating = true;
                            }
                        }
                    };

                    iter.doForAll();

                    if (!iter.fullIteration()) {
                        return;
                    }
                }

                // no-op if _relaxAutoCommit == true
                if (this.autoCommit && !getRelaxAutoCommit()) {
                    throw SQLError.createSQLException("Can't call commit when autocommit=true", getExceptionInterceptor());
                } else if (this.transactionsSupported) {
                    if (getUseLocalTransactionState() && versionMeetsMinimum(5, 0, 0)) {
                        if (!this.io.inTransactionOnServer()) {
                            return; // effectively a no-op
                        }
                    }

                    execSQL(null, "commit", -1, null, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, false, this.database, null, false);
                }
            } catch (SQLException sqlException) {
                if (SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlException.getSQLState())) {
                    throw SQLError.createSQLException("Communications link failure during commit(). Transaction resolution unknown.",
                            SQLError.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN, getExceptionInterceptor());
                }

                throw sqlException;
            } finally {
                this.needsPing = this.getReconnectAtTxEnd();
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
        if (getEncoding() != null) {
            // Attempt to use the encoding, and bail out if it can't be used
            try {
                String testString = "abc";
                StringUtils.getBytes(testString, getEncoding());
            } catch (UnsupportedEncodingException UE) {
                // Try the MySQL character encoding, then....
                String oldEncoding = getEncoding();

                try {
                    setEncoding(CharsetMapping.getJavaEncodingForMysqlCharset(oldEncoding));
                } catch (RuntimeException ex) {
                    SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
                    sqlEx.initCause(ex);
                    throw sqlEx;
                }

                if (getEncoding() == null) {
                    throw SQLError.createSQLException("Java does not support the MySQL character encoding '" + oldEncoding + "'.",
                            SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, getExceptionInterceptor());
                }

                try {
                    String testString = "abc";
                    StringUtils.getBytes(testString, getEncoding());
                } catch (UnsupportedEncodingException encodingEx) {
                    throw SQLError.createSQLException("Unsupported character encoding '" + getEncoding() + "'.",
                            SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, getExceptionInterceptor());
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
        String realJavaEncoding = getEncoding();
        boolean characterSetAlreadyConfigured = false;

        try {
            if (versionMeetsMinimum(4, 1, 0)) {
                characterSetAlreadyConfigured = true;

                setUseUnicode(true);

                configureCharsetProperties();
                realJavaEncoding = getEncoding(); // we need to do this again to grab this for versions > 4.1.0

                try {

                    // Fault injection for testing server character set indices

                    if (this.props != null && this.props.getProperty("com.mysql.jdbc.faultInjection.serverCharsetIndex") != null) {
                        this.io.serverCharsetIndex = Integer.parseInt(this.props.getProperty("com.mysql.jdbc.faultInjection.serverCharsetIndex"));
                    }

                    String serverEncodingToSet = CharsetMapping.getJavaEncodingForCollationIndex(this.io.serverCharsetIndex);

                    if (serverEncodingToSet == null || serverEncodingToSet.length() == 0) {
                        if (realJavaEncoding != null) {
                            // user knows best, try it
                            setEncoding(realJavaEncoding);
                        } else {
                            throw SQLError.createSQLException(
                                    "Unknown initial character set index '" + this.io.serverCharsetIndex
                                            + "' received from server. Initial client character set can be forced via the 'characterEncoding' property.",
                                    SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                        }
                    }

                    // "latin1" on MySQL-4.1.0+ is actually CP1252, not ISO8859_1
                    if (versionMeetsMinimum(4, 1, 0) && "ISO8859_1".equalsIgnoreCase(serverEncodingToSet)) {
                        serverEncodingToSet = "Cp1252";
                    }
                    if ("UnicodeBig".equalsIgnoreCase(serverEncodingToSet) || "UTF-16".equalsIgnoreCase(serverEncodingToSet)
                            || "UTF-16LE".equalsIgnoreCase(serverEncodingToSet) || "UTF-32".equalsIgnoreCase(serverEncodingToSet)) {
                        serverEncodingToSet = "UTF-8";
                    }

                    setEncoding(serverEncodingToSet);

                } catch (ArrayIndexOutOfBoundsException outOfBoundsEx) {
                    if (realJavaEncoding != null) {
                        // user knows best, try it
                        setEncoding(realJavaEncoding);
                    } else {
                        throw SQLError.createSQLException(
                                "Unknown initial character set index '" + this.io.serverCharsetIndex
                                        + "' received from server. Initial client character set can be forced via the 'characterEncoding' property.",
                                SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                    }
                } catch (SQLException ex) {
                    throw ex;
                } catch (RuntimeException ex) {
                    SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
                    sqlEx.initCause(ex);
                    throw sqlEx;
                }

                if (getEncoding() == null) {
                    // punt?
                    setEncoding("ISO8859_1");
                }

                //
                // Has the user has 'forced' the character encoding via driver properties?
                //
                if (getUseUnicode()) {
                    if (realJavaEncoding != null) {

                        //
                        // Now, inform the server what character set we will be using from now-on...
                        //
                        if (realJavaEncoding.equalsIgnoreCase("UTF-8") || realJavaEncoding.equalsIgnoreCase("UTF8")) {
                            // charset names are case-sensitive

                            boolean utf8mb4Supported = versionMeetsMinimum(5, 5, 2);
                            boolean useutf8mb4 = utf8mb4Supported && (CharsetMapping.UTF8MB4_INDEXES.contains(this.io.serverCharsetIndex));

                            if (!getUseOldUTF8Behavior()) {
                                if (dontCheckServerMatch || !characterSetNamesMatches("utf8") || (utf8mb4Supported && !characterSetNamesMatches("utf8mb4"))) {
                                    execSQL(null, "SET NAMES " + (useutf8mb4 ? "utf8mb4" : "utf8"), -1, null, DEFAULT_RESULT_SET_TYPE,
                                            DEFAULT_RESULT_SET_CONCURRENCY, false, this.database, null, false);
                                    this.serverVariables.put("character_set_client", useutf8mb4 ? "utf8mb4" : "utf8");
                                    this.serverVariables.put("character_set_connection", useutf8mb4 ? "utf8mb4" : "utf8");
                                }
                            } else {
                                execSQL(null, "SET NAMES latin1", -1, null, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, false, this.database, null,
                                        false);
                                this.serverVariables.put("character_set_client", "latin1");
                                this.serverVariables.put("character_set_connection", "latin1");
                            }

                            setEncoding(realJavaEncoding);
                        } /* not utf-8 */else {
                            String mysqlCharsetName = CharsetMapping.getMysqlCharsetForJavaEncoding(realJavaEncoding.toUpperCase(Locale.ENGLISH), this);

                            /*
                             * if ("koi8_ru".equals(mysqlEncodingName)) { //
                             * This has a _different_ name in 4.1...
                             * mysqlEncodingName = "ko18r"; } else if
                             * ("euc_kr".equals(mysqlEncodingName)) { //
                             * Different name in 4.1 mysqlEncodingName =
                             * "euckr"; }
                             */

                            if (mysqlCharsetName != null) {

                                if (dontCheckServerMatch || !characterSetNamesMatches(mysqlCharsetName)) {
                                    execSQL(null, "SET NAMES " + mysqlCharsetName, -1, null, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, false,
                                            this.database, null, false);
                                    this.serverVariables.put("character_set_client", mysqlCharsetName);
                                    this.serverVariables.put("character_set_connection", mysqlCharsetName);
                                }
                            }

                            // Switch driver's encoding now, since the server knows what we're sending...
                            //
                            setEncoding(realJavaEncoding);
                        }
                    } else if (getEncoding() != null) {
                        // Tell the server we'll use the server default charset to send our queries from now on....
                        String mysqlCharsetName = getServerCharset();

                        if (getUseOldUTF8Behavior()) {
                            mysqlCharsetName = "latin1";
                        }

                        boolean ucs2 = false;
                        if ("ucs2".equalsIgnoreCase(mysqlCharsetName) || "utf16".equalsIgnoreCase(mysqlCharsetName)
                                || "utf16le".equalsIgnoreCase(mysqlCharsetName) || "utf32".equalsIgnoreCase(mysqlCharsetName)) {
                            mysqlCharsetName = "utf8";
                            ucs2 = true;
                            if (getCharacterSetResults() == null) {
                                setCharacterSetResults("UTF-8");
                            }
                        }

                        if (dontCheckServerMatch || !characterSetNamesMatches(mysqlCharsetName) || ucs2) {
                            try {
                                execSQL(null, "SET NAMES " + mysqlCharsetName, -1, null, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, false,
                                        this.database, null, false);
                                this.serverVariables.put("character_set_client", mysqlCharsetName);
                                this.serverVariables.put("character_set_connection", mysqlCharsetName);
                            } catch (SQLException ex) {
                                if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || getDisconnectOnExpiredPasswords()) {
                                    throw ex;
                                }
                            }
                        }

                        realJavaEncoding = getEncoding();
                    }

                }

                //
                // We know how to deal with any charset coming back from the database, so tell the server not to do conversion if the user hasn't 'forced' a
                // result-set character set
                //

                String onServer = null;
                boolean isNullOnServer = false;

                if (this.serverVariables != null) {
                    onServer = this.serverVariables.get("character_set_results");

                    isNullOnServer = onServer == null || "NULL".equalsIgnoreCase(onServer) || onServer.length() == 0;
                }

                if (getCharacterSetResults() == null) {

                    //
                    // Only send if needed, if we're caching server variables we -have- to send, because we don't know what it was before we cached them.
                    //
                    if (!isNullOnServer) {
                        try {
                            execSQL(null, "SET character_set_results = NULL", -1, null, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, false,
                                    this.database, null, false);
                        } catch (SQLException ex) {
                            if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || getDisconnectOnExpiredPasswords()) {
                                throw ex;
                            }
                        }
                        this.serverVariables.put(JDBC_LOCAL_CHARACTER_SET_RESULTS, null);
                    } else {
                        this.serverVariables.put(JDBC_LOCAL_CHARACTER_SET_RESULTS, onServer);
                    }
                } else {

                    if (getUseOldUTF8Behavior()) {
                        try {
                            execSQL(null, "SET NAMES latin1", -1, null, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, false, this.database, null,
                                    false);
                            this.serverVariables.put("character_set_client", "latin1");
                            this.serverVariables.put("character_set_connection", "latin1");
                        } catch (SQLException ex) {
                            if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || getDisconnectOnExpiredPasswords()) {
                                throw ex;
                            }
                        }
                    }
                    String charsetResults = getCharacterSetResults();
                    String mysqlEncodingName = null;

                    if ("UTF-8".equalsIgnoreCase(charsetResults) || "UTF8".equalsIgnoreCase(charsetResults)) {
                        mysqlEncodingName = "utf8";
                    } else if ("null".equalsIgnoreCase(charsetResults)) {
                        mysqlEncodingName = "NULL";
                    } else {
                        mysqlEncodingName = CharsetMapping.getMysqlCharsetForJavaEncoding(charsetResults.toUpperCase(Locale.ENGLISH), this);
                    }

                    //
                    // Only change the value if needed
                    //

                    if (mysqlEncodingName == null) {
                        throw SQLError.createSQLException("Can't map " + charsetResults + " given for characterSetResults to a supported MySQL encoding.",
                                SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
                    }

                    if (!mysqlEncodingName.equalsIgnoreCase(this.serverVariables.get("character_set_results"))) {
                        StringBuilder setBuf = new StringBuilder("SET character_set_results = ".length() + mysqlEncodingName.length());
                        setBuf.append("SET character_set_results = ").append(mysqlEncodingName);

                        try {
                            execSQL(null, setBuf.toString(), -1, null, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, false, this.database, null,
                                    false);
                        } catch (SQLException ex) {
                            if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || getDisconnectOnExpiredPasswords()) {
                                throw ex;
                            }
                        }

                        this.serverVariables.put(JDBC_LOCAL_CHARACTER_SET_RESULTS, mysqlEncodingName);

                        // We have to set errorMessageEncoding according to new value of charsetResults for server version 5.5 and higher
                        if (versionMeetsMinimum(5, 5, 0)) {
                            this.errorMessageEncoding = charsetResults;
                        }

                    } else {
                        this.serverVariables.put(JDBC_LOCAL_CHARACTER_SET_RESULTS, onServer);
                    }
                }

                if (getConnectionCollation() != null) {
                    StringBuilder setBuf = new StringBuilder("SET collation_connection = ".length() + getConnectionCollation().length());
                    setBuf.append("SET collation_connection = ").append(getConnectionCollation());

                    try {
                        execSQL(null, setBuf.toString(), -1, null, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, false, this.database, null, false);
                    } catch (SQLException ex) {
                        if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || getDisconnectOnExpiredPasswords()) {
                            throw ex;
                        }
                    }
                }
            } else {
                // Use what the server has specified
                realJavaEncoding = getEncoding(); // so we don't get
                // swapped out in the finally block....
            }
        } finally {
            // Failsafe, make sure that the driver's notion of character encoding matches what the user has specified.
            //
            setEncoding(realJavaEncoding);
        }

        /**
         * Check if we need a CharsetEncoder for escaping codepoints that are
         * transformed to backslash (0x5c) in the connection encoding.
         */
        try {
            CharsetEncoder enc = Charset.forName(getEncoding()).newEncoder();
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
            // fallback to String API - for Java 1.4
            try {
                byte bbuf[] = StringUtils.getBytes("\u00a5", getEncoding());
                if (bbuf[0] == '\\') {
                    this.requiresEscapingEncoder = true;
                } else {
                    bbuf = StringUtils.getBytes("\u20a9", getEncoding());
                    if (bbuf[0] == '\\') {
                        this.requiresEscapingEncoder = true;
                    }
                }
            } catch (UnsupportedEncodingException ueex) {
                throw SQLError.createSQLException("Unable to use encoding: " + getEncoding(), SQLError.SQL_STATE_GENERAL_ERROR, ueex,
                        getExceptionInterceptor());
            }
        }

        return characterSetAlreadyConfigured;
    }

    /**
     * Configures the client's timezone if required.
     * 
     * @throws SQLException
     *             if the timezone the server is configured to use can't be
     *             mapped to a Java timezone.
     */
    private void configureTimezone() throws SQLException {
        String configuredTimeZoneOnServer = this.serverVariables.get("timezone");

        if (configuredTimeZoneOnServer == null) {
            configuredTimeZoneOnServer = this.serverVariables.get("time_zone");

            if ("SYSTEM".equalsIgnoreCase(configuredTimeZoneOnServer)) {
                configuredTimeZoneOnServer = this.serverVariables.get("system_time_zone");
            }
        }

        String canonicalTimezone = getServerTimezone();

        if ((getUseTimezone() || !getUseLegacyDatetimeCode()) && configuredTimeZoneOnServer != null) {
            // user can override this with driver properties, so don't detect if that's the case
            if (canonicalTimezone == null || StringUtils.isEmptyOrWhitespaceOnly(canonicalTimezone)) {
                try {
                    canonicalTimezone = TimeUtil.getCanonicalTimezone(configuredTimeZoneOnServer, getExceptionInterceptor());
                } catch (IllegalArgumentException iae) {
                    throw SQLError.createSQLException(iae.getMessage(), SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                }
            }
        }

        if (canonicalTimezone != null && canonicalTimezone.length() > 0) {
            this.serverTimezoneTZ = TimeZone.getTimeZone(canonicalTimezone);

            //
            // The Calendar class has the behavior of mapping unknown timezones to 'GMT' instead of throwing an exception, so we must check for this...
            //
            if (!canonicalTimezone.equalsIgnoreCase("GMT") && this.serverTimezoneTZ.getID().equals("GMT")) {
                throw SQLError.createSQLException("No timezone mapping entry for '" + canonicalTimezone + "'", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }

            this.isServerTzUTC = !this.serverTimezoneTZ.useDaylightTime() && this.serverTimezoneTZ.getRawOffset() == 0;
        }
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
    public void createNewIO(boolean isForReconnect) throws SQLException {
        synchronized (getConnectionMutex()) {
            // Synchronization Not needed for *new* connections, but defintely for connections going through fail-over, since we might get the new connection up
            // and running *enough* to start sending cached or still-open server-side prepared statements over to the backend before we get a chance to
            // re-prepare them...

            Properties mergedProps = exposeAsProperties(this.props);

            if (!getHighAvailability()) {
                connectOneTryOnly(isForReconnect, mergedProps);

                return;
            }

            connectWithRetries(isForReconnect, mergedProps);
        }
    }

    private void connectWithRetries(boolean isForReconnect, Properties mergedProps) throws SQLException {
        double timeout = getInitialTimeout();
        boolean connectionGood = false;

        Exception connectionException = null;

        for (int attemptCount = 0; (attemptCount < getMaxReconnects()) && !connectionGood; attemptCount++) {
            try {
                if (this.io != null) {
                    this.io.forceClose();
                }

                coreConnect(mergedProps);
                pingInternal(false, 0);

                boolean oldAutoCommit;
                int oldIsolationLevel;
                boolean oldReadOnly;
                String oldCatalog;

                synchronized (getConnectionMutex()) {
                    this.connectionId = this.io.getThreadId();
                    this.isClosed = false;

                    // save state from old connection
                    oldAutoCommit = getAutoCommit();
                    oldIsolationLevel = this.isolationLevel;
                    oldReadOnly = isReadOnly(false);
                    oldCatalog = getCatalog();

                    this.io.setStatementInterceptors(this.statementInterceptors);
                }

                // Server properties might be different from previous connection, so initialize again...
                initializePropsFromServer();

                if (isForReconnect) {
                    // Restore state from old connection
                    setAutoCommit(oldAutoCommit);

                    if (this.hasIsolationLevels) {
                        setTransactionIsolation(oldIsolationLevel);
                    }

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
                    Messages.getString("Connection.UnableToConnectWithRetries", new Object[] { Integer.valueOf(getMaxReconnects()) }),
                    SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, getExceptionInterceptor());
            chainedEx.initCause(connectionException);

            throw chainedEx;
        }

        if (getParanoid() && !getHighAvailability()) {
            this.password = null;
            this.user = null;
        }

        if (isForReconnect) {
            //
            // Retrieve any 'lost' prepared statements if re-connecting
            //
            Iterator<Statement> statementIter = this.openStatements.values().iterator();

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

    private void coreConnect(Properties mergedProps) throws SQLException, IOException {
        int newPort = 3306;
        String newHost = "localhost";

        String protocol = mergedProps.getProperty(NonRegisteringDriver.PROTOCOL_PROPERTY_KEY);

        if (protocol != null) {
            // "new" style URL

            if ("tcp".equalsIgnoreCase(protocol)) {
                newHost = normalizeHost(mergedProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY));
                newPort = parsePortNumber(mergedProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306"));
            } else if ("pipe".equalsIgnoreCase(protocol)) {
                setSocketFactoryClassName(NamedPipeSocketFactory.class.getName());

                String path = mergedProps.getProperty(NonRegisteringDriver.PATH_PROPERTY_KEY);

                if (path != null) {
                    mergedProps.setProperty(NamedPipeSocketFactory.NAMED_PIPE_PROP_NAME, path);
                }
            } else {
                // normalize for all unknown protocols
                newHost = normalizeHost(mergedProps.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY));
                newPort = parsePortNumber(mergedProps.getProperty(NonRegisteringDriver.PORT_PROPERTY_KEY, "3306"));
            }
        } else {

            String[] parsedHostPortPair = NonRegisteringDriver.parseHostPortPair(this.hostPortPair);
            newHost = parsedHostPortPair[NonRegisteringDriver.HOST_NAME_INDEX];

            newHost = normalizeHost(newHost);

            if (parsedHostPortPair[NonRegisteringDriver.PORT_NUMBER_INDEX] != null) {
                newPort = parsePortNumber(parsedHostPortPair[NonRegisteringDriver.PORT_NUMBER_INDEX]);
            }
        }

        this.port = newPort;
        this.host = newHost;

        // reset max-rows to default value
        this.sessionMaxRows = -1;

        this.io = new MysqlIO(newHost, newPort, mergedProps, getSocketFactoryClassName(), getProxy(), getSocketTimeout(),
                this.largeRowSizeThreshold.getValueAsInt());
        this.io.doHandshake(this.user, this.password, this.database);
        if (versionMeetsMinimum(5, 5, 0)) {
            // error messages are returned according to character_set_results which, at this point, is set from the response packet
            this.errorMessageEncoding = this.io.getEncodingForHandshake();
        }
    }

    private String normalizeHost(String hostname) {
        if (hostname == null || StringUtils.isEmptyOrWhitespaceOnly(hostname)) {
            return "localhost";
        }

        return hostname;
    }

    private int parsePortNumber(String portAsString) throws SQLException {
        int portNumber = 3306;
        try {
            portNumber = Integer.parseInt(portAsString);
        } catch (NumberFormatException nfe) {
            throw SQLError.createSQLException("Illegal connection port value '" + portAsString + "'", SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE,
                    getExceptionInterceptor());
        }
        return portNumber;
    }

    private void connectOneTryOnly(boolean isForReconnect, Properties mergedProps) throws SQLException {
        Exception connectionNotEstablishedBecause = null;

        try {

            coreConnect(mergedProps);
            this.connectionId = this.io.getThreadId();
            this.isClosed = false;

            // save state from old connection
            boolean oldAutoCommit = getAutoCommit();
            int oldIsolationLevel = this.isolationLevel;
            boolean oldReadOnly = isReadOnly(false);
            String oldCatalog = getCatalog();

            this.io.setStatementInterceptors(this.statementInterceptors);

            // Server properties might be different from previous connection, so initialize again...
            initializePropsFromServer();

            if (isForReconnect) {
                // Restore state from old connection
                setAutoCommit(oldAutoCommit);

                if (this.hasIsolationLevels) {
                    setTransactionIsolation(oldIsolationLevel);
                }

                setCatalog(oldCatalog);

                setReadOnly(oldReadOnly);
            }
            return;

        } catch (Exception EEE) {

            if (EEE instanceof SQLException && ((SQLException) EEE).getErrorCode() == MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD
                    && !getDisconnectOnExpiredPasswords()) {
                return;
            }

            if (this.io != null) {
                this.io.forceClose();
            }

            connectionNotEstablishedBecause = EEE;

            if (EEE instanceof SQLException) {
                throw (SQLException) EEE;
            }

            SQLException chainedEx = SQLError.createSQLException(Messages.getString("Connection.UnableToConnect"),
                    SQLError.SQL_STATE_UNABLE_TO_CONNECT_TO_DATASOURCE, getExceptionInterceptor());
            chainedEx.initCause(connectionNotEstablishedBecause);

            throw chainedEx;
        }
    }

    private void createPreparedStatementCaches() throws SQLException {
        synchronized (getConnectionMutex()) {
            int cacheSize = getPreparedStatementCacheSize();

            try {
                Class<?> factoryClass;

                factoryClass = Class.forName(getParseInfoCacheFactory());

                @SuppressWarnings("unchecked")
                CacheAdapterFactory<String, ParseInfo> cacheFactory = ((CacheAdapterFactory<String, ParseInfo>) factoryClass.newInstance());

                this.cachedPreparedStatementParams = cacheFactory.getInstance(this, this.myURL, getPreparedStatementCacheSize(),
                        getPreparedStatementCacheSqlLimit(), this.props);

            } catch (ClassNotFoundException e) {
                SQLException sqlEx = SQLError.createSQLException(
                        Messages.getString("Connection.CantFindCacheFactory", new Object[] { getParseInfoCacheFactory(), "parseInfoCacheFactory" }),
                        getExceptionInterceptor());
                sqlEx.initCause(e);

                throw sqlEx;
            } catch (InstantiationException e) {
                SQLException sqlEx = SQLError.createSQLException(
                        Messages.getString("Connection.CantLoadCacheFactory", new Object[] { getParseInfoCacheFactory(), "parseInfoCacheFactory" }),
                        getExceptionInterceptor());
                sqlEx.initCause(e);

                throw sqlEx;
            } catch (IllegalAccessException e) {
                SQLException sqlEx = SQLError.createSQLException(
                        Messages.getString("Connection.CantLoadCacheFactory", new Object[] { getParseInfoCacheFactory(), "parseInfoCacheFactory" }),
                        getExceptionInterceptor());
                sqlEx.initCause(e);

                throw sqlEx;
            }

            if (getUseServerPreparedStmts()) {
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
        checkClosed();

        StatementImpl stmt = new StatementImpl(getMultiHostSafeProxy(), this.database);
        stmt.setResultSetType(resultSetType);
        stmt.setResultSetConcurrency(resultSetConcurrency);

        return stmt;
    }

    /**
     * @see Connection#createStatement(int, int, int)
     */
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (getPedantic()) {
            if (resultSetHoldability != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                throw SQLError.createSQLException("HOLD_CUSRORS_OVER_COMMIT is only supported holdability level", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        return createStatement(resultSetType, resultSetConcurrency);
    }

    public void dumpTestcaseQuery(String query) {
        System.err.println(query);
    }

    public Connection duplicate() throws SQLException {
        return new ConnectionImpl(this.origHostToConnectTo, this.origPortToConnectTo, this.props, this.origDatabaseToConnectTo, this.myURL);
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
    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, Buffer packet, int resultSetType, int resultSetConcurrency,
            boolean streamResults, String catalog, Field[] cachedMetadata) throws SQLException {
        return execSQL(callingStatement, sql, maxRows, packet, resultSetType, resultSetConcurrency, streamResults, catalog, cachedMetadata, false);
    }

    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, Buffer packet, int resultSetType, int resultSetConcurrency,
            boolean streamResults, String catalog, Field[] cachedMetadata, boolean isBatch) throws SQLException {
        synchronized (getConnectionMutex()) {
            //
            // Fall-back if the master is back online if we've issued queriesBeforeRetryMaster queries since we failed over
            //

            long queryStartTime = 0;

            int endOfQueryPacketPosition = 0;

            if (packet != null) {
                endOfQueryPacketPosition = packet.getPosition();
            }

            if (getGatherPerformanceMetrics()) {
                queryStartTime = System.currentTimeMillis();
            }

            this.lastQueryFinishedTime = 0; // we're busy!

            if ((getHighAvailability()) && (this.autoCommit || getAutoReconnectForPools()) && this.needsPing && !isBatch) {
                try {
                    pingInternal(false, 0);

                    this.needsPing = false;
                } catch (Exception Ex) {
                    createNewIO(true);
                }
            }

            try {
                if (packet == null) {
                    String encoding = null;

                    if (getUseUnicode()) {
                        encoding = getEncoding();
                    }

                    return this.io.sqlQueryDirect(callingStatement, sql, encoding, null, maxRows, resultSetType, resultSetConcurrency, streamResults, catalog,
                            cachedMetadata);
                }

                return this.io.sqlQueryDirect(callingStatement, null, null, packet, maxRows, resultSetType, resultSetConcurrency, streamResults, catalog,
                        cachedMetadata);
            } catch (java.sql.SQLException sqlE) {
                // don't clobber SQL exceptions

                if (getDumpQueriesOnException()) {
                    String extractedSql = extractSqlFromPacket(sql, packet, endOfQueryPacketPosition);
                    StringBuilder messageBuf = new StringBuilder(extractedSql.length() + 32);
                    messageBuf.append("\n\nQuery being executed when exception was thrown:\n");
                    messageBuf.append(extractedSql);
                    messageBuf.append("\n\n");

                    sqlE = appendMessageToException(sqlE, messageBuf.toString(), getExceptionInterceptor());
                }

                if ((getHighAvailability())) {
                    this.needsPing = true;
                } else {
                    String sqlState = sqlE.getSQLState();

                    if ((sqlState != null) && sqlState.equals(SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE)) {
                        cleanup(sqlE);
                    }
                }

                throw sqlE;
            } catch (Exception ex) {
                if (getHighAvailability()) {
                    this.needsPing = true;
                } else if (ex instanceof IOException) {
                    cleanup(ex);
                }

                SQLException sqlEx = SQLError.createSQLException(Messages.getString("Connection.UnexpectedException"), SQLError.SQL_STATE_GENERAL_ERROR,
                        getExceptionInterceptor());
                sqlEx.initCause(ex);

                throw sqlEx;
            } finally {
                if (getMaintainTimeStats()) {
                    this.lastQueryFinishedTime = System.currentTimeMillis();
                }

                if (getGatherPerformanceMetrics()) {
                    long queryTime = System.currentTimeMillis() - queryStartTime;

                    registerQueryExecutionTime(queryTime);
                }
            }
        }
    }

    public String extractSqlFromPacket(String possibleSqlQuery, Buffer queryPacket, int endOfQueryPacketPosition) throws SQLException {

        String extractedSql = null;

        if (possibleSqlQuery != null) {
            if (possibleSqlQuery.length() > getMaxQuerySizeToLog()) {
                StringBuilder truncatedQueryBuf = new StringBuilder(possibleSqlQuery.substring(0, getMaxQuerySizeToLog()));
                truncatedQueryBuf.append(Messages.getString("MysqlIO.25"));
                extractedSql = truncatedQueryBuf.toString();
            } else {
                extractedSql = possibleSqlQuery;
            }
        }

        if (extractedSql == null) {
            // This is probably from a client-side prepared statement

            int extractPosition = endOfQueryPacketPosition;

            boolean truncated = false;

            if (endOfQueryPacketPosition > getMaxQuerySizeToLog()) {
                extractPosition = getMaxQuerySizeToLog();
                truncated = true;
            }

            extractedSql = StringUtils.toString(queryPacket.getByteBuffer(), 5, (extractPosition - 5));

            if (truncated) {
                extractedSql += Messages.getString("MysqlIO.25");
            }
        }

        return extractedSql;

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
        // Might not have one of these if not tracking open resources
        if (this.openStatements != null) {
            synchronized (this.openStatements) {
                return this.openStatements.size();
            }
        }

        return 0;
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
     * Optimization to only use one calendar per-session, or calculate it for
     * each call, depending on user configuration
     */
    public Calendar getCalendarInstanceForSessionOrNew() {
        if (getDynamicCalendars()) {
            return Calendar.getInstance();
        }

        return getSessionLockedCalendar();
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
            return this.characterSetMetadata;
        }
    }

    /**
     * Returns the locally mapped instance of a charset converter (to avoid
     * overhead of static synchronization).
     * 
     * @param javaEncodingName
     *            the encoding name to retrieve
     * @return a character converter, or null if one couldn't be mapped.
     */
    public SingleByteCharsetConverter getCharsetConverter(String javaEncodingName) throws SQLException {
        if (javaEncodingName == null) {
            return null;
        }

        if (this.usePlatformCharsetConverters) {
            return null; // we'll use Java's built-in routines for this they're finally fast enough
        }

        SingleByteCharsetConverter converter = null;

        synchronized (this.charsetConverterMap) {
            Object asObject = this.charsetConverterMap.get(javaEncodingName);

            if (asObject == CHARSET_CONVERTER_NOT_AVAILABLE_MARKER) {
                return null;
            }

            converter = (SingleByteCharsetConverter) asObject;

            if (converter == null) {
                try {
                    converter = SingleByteCharsetConverter.getInstance(javaEncodingName, this);

                    if (converter == null) {
                        this.charsetConverterMap.put(javaEncodingName, CHARSET_CONVERTER_NOT_AVAILABLE_MARKER);
                    } else {
                        this.charsetConverterMap.put(javaEncodingName, converter);
                    }
                } catch (UnsupportedEncodingException unsupEncEx) {
                    this.charsetConverterMap.put(javaEncodingName, CHARSET_CONVERTER_NOT_AVAILABLE_MARKER);

                    converter = null;
                }
            }
        }

        return converter;
    }

    /**
     * @deprecated replaced by <code>getEncodingForIndex(int charsetIndex)</code>
     */
    @Deprecated
    public String getCharsetNameForIndex(int charsetIndex) throws SQLException {
        return getEncodingForIndex(charsetIndex);
    }

    /**
     * Returns the Java character encoding name for the given MySQL server
     * charset index
     * 
     * @param charsetIndex
     * @return the Java character encoding name for the given MySQL server
     *         charset index
     * @throws SQLException
     *             if the character set index isn't known by the driver
     */
    public String getEncodingForIndex(int charsetIndex) throws SQLException {
        String javaEncoding = null;

        if (getUseOldUTF8Behavior()) {
            return getEncoding();
        }

        if (charsetIndex != MysqlDefs.NO_CHARSET_INFO) {
            try {
                if (this.indexToMysqlCharset.size() > 0) {
                    javaEncoding = CharsetMapping.getJavaEncodingForMysqlCharset(this.indexToMysqlCharset.get(charsetIndex), getEncoding());
                }
                // checking against static maps if no custom charset found
                if (javaEncoding == null) {
                    javaEncoding = CharsetMapping.getJavaEncodingForCollationIndex(charsetIndex, getEncoding());
                }

            } catch (ArrayIndexOutOfBoundsException outOfBoundsEx) {
                throw SQLError.createSQLException("Unknown character set index for field '" + charsetIndex + "' received from server.",
                        SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
            } catch (RuntimeException ex) {
                SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
                sqlEx.initCause(ex);
                throw sqlEx;
            }

            // Punt
            if (javaEncoding == null) {
                javaEncoding = getEncoding();
            }
        } else {
            javaEncoding = getEncoding();
        }

        return javaEncoding;
    }

    /**
     * @return Returns the defaultTimeZone.
     */
    public TimeZone getDefaultTimeZone() {
        // If default time zone is cached then there is no need to get a new instance of it, just use the previous one.
        return getCacheDefaultTimezone() ? this.defaultTimeZone : TimeUtil.getDefaultTimeZone(false);
    }

    public String getErrorMessageEncoding() {
        return this.errorMessageEncoding;
    }

    /**
     * @see Connection#getHoldability()
     */
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
     * Returns the IO channel to the server
     * 
     * @return the IO channel to the server
     * @throws SQLException
     *             if the connection is closed.
     */
    public MysqlIO getIO() throws SQLException {
        if ((this.io == null) || this.isClosed) {
            throw SQLError.createSQLException("Operation not allowed on closed connection", SQLError.SQL_STATE_CONNECTION_NOT_OPEN, getExceptionInterceptor());
        }

        return this.io;
    }

    /**
     * Returns the log mechanism that should be used to log information from/for
     * this Connection.
     * 
     * @return the Log instance to use for logging messages.
     * @throws SQLException
     *             if an error occurs
     */
    public Log getLog() throws SQLException {
        return this.log;
    }

    public int getMaxBytesPerChar(String javaCharsetName) throws SQLException {
        return getMaxBytesPerChar(null, javaCharsetName);
    }

    public int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName) throws SQLException {

        String charset = null;

        try {
            // if we can get it by charsetIndex just doing it

            // getting charset name from dynamic maps in connection; we do it before checking against static maps because custom charset on server can be mapped
            // to index from our static map key's diapason 
            if (this.indexToCustomMysqlCharset != null) {
                charset = this.indexToCustomMysqlCharset.get(charsetIndex);
            }
            // checking against static maps if no custom charset found
            if (charset == null) {
                charset = CharsetMapping.getMysqlCharsetNameForCollationIndex(charsetIndex);
            }

            // if we didn't find charset name by index
            if (charset == null) {
                charset = CharsetMapping.getMysqlCharsetForJavaEncoding(javaCharsetName, this);
            }

            // checking against dynamic maps in connection
            Integer mblen = null;
            if (this.mysqlCharsetToCustomMblen != null) {
                mblen = this.mysqlCharsetToCustomMblen.get(charset);
            }

            // checking against static maps
            if (mblen == null) {
                mblen = CharsetMapping.getMblen(charset);
            }

            if (mblen != null) {
                return mblen.intValue();
            }
        } catch (SQLException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
            sqlEx.initCause(ex);
            throw sqlEx;
        }

        return 1; // we don't know
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

        return com.mysql.jdbc.DatabaseMetaData.getInstance(getMultiHostSafeProxy(), this.database, checkForInfoSchema);
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

    /**
     * Returns the packet buffer size the MySQL server reported upon connection
     */
    public int getNetBufferLength() {
        return this.netBufferLength;
    }

    /**
     * @deprecated replaced by <code>getServerCharset()</code>
     */
    @Deprecated
    public String getServerCharacterEncoding() {
        return getServerCharset();
    }

    /**
     * Returns the server's character set
     * 
     * @return the server's character set.
     */
    public String getServerCharset() {
        if (this.io.versionMeetsMinimum(4, 1, 0)) {
            String charset = null;
            if (this.indexToCustomMysqlCharset != null) {
                charset = this.indexToCustomMysqlCharset.get(this.io.serverCharsetIndex);
            }
            if (charset == null) {
                charset = CharsetMapping.getMysqlCharsetNameForCollationIndex(this.io.serverCharsetIndex);
            }
            return charset != null ? charset : this.serverVariables.get("character_set_server");
        }
        return this.serverVariables.get("character_set");
    }

    public int getServerMajorVersion() {
        return this.io.getServerMajorVersion();
    }

    public int getServerMinorVersion() {
        return this.io.getServerMinorVersion();
    }

    public int getServerSubMinorVersion() {
        return this.io.getServerSubMinorVersion();
    }

    public TimeZone getServerTimezoneTZ() {
        return this.serverTimezoneTZ;
    }

    public String getServerVariable(String variableName) {
        if (this.serverVariables != null) {
            return this.serverVariables.get(variableName);
        }

        return null;
    }

    public String getServerVersion() {
        return this.io.getServerVersion();
    }

    public Calendar getSessionLockedCalendar() {

        return this.sessionCalendar;
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
            if (this.hasIsolationLevels && !getUseLocalSessionState()) {
                java.sql.Statement stmt = null;
                java.sql.ResultSet rs = null;

                try {
                    stmt = getMetadataSafeStatement();

                    String query = null;

                    int offset = 0;

                    if (versionMeetsMinimum(4, 0, 3)) {
                        query = "SELECT @@session.tx_isolation";
                        offset = 1;
                    } else {
                        query = "SHOW VARIABLES LIKE 'transaction_isolation'";
                        offset = 2;
                    }

                    rs = stmt.executeQuery(query);

                    if (rs.next()) {
                        String s = rs.getString(offset);

                        if (s != null) {
                            Integer intTI = mapTransIsolationNameToValue.get(s);

                            if (intTI != null) {
                                return intTI.intValue();
                            }
                        }

                        throw SQLError.createSQLException("Could not map transaction isolation '" + s + " to a valid JDBC level.",
                                SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());
                    }

                    throw SQLError.createSQLException("Could not retrieve transaction isolation level from server", SQLError.SQL_STATE_GENERAL_ERROR,
                            getExceptionInterceptor());

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
        return this.myURL;
    }

    public String getUser() {
        return this.user;
    }

    public Calendar getUtcCalendar() {
        return this.utcCalendar;
    }

    /**
     * The first warning reported by calls on this Connection is returned.
     * <B>Note:</B> Sebsequent warnings will be changed to this
     * java.sql.SQLWarning
     * 
     * @return the first java.sql.SQLWarning or null
     * @exception SQLException
     *                if a database access error occurs
     */
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    public boolean hasSameProperties(Connection c) {
        return this.props.equals(c.getProperties());
    }

    public Properties getProperties() {
        return this.props;
    }

    public boolean hasTriedMaster() {
        return this.hasTriedMasterFlag;
    }

    public void incrementNumberOfPreparedExecutes() {
        if (getGatherPerformanceMetrics()) {
            this.numberOfPreparedExecutes++;

            // We need to increment this, because server-side prepared statements bypass any execution by the connection itself...
            this.numberOfQueriesIssued++;
        }
    }

    public void incrementNumberOfPrepares() {
        if (getGatherPerformanceMetrics()) {
            this.numberOfPrepares++;
        }
    }

    public void incrementNumberOfResultSetsCreated() {
        if (getGatherPerformanceMetrics()) {
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
        initializeProperties(info);

        String exceptionInterceptorClasses = getExceptionInterceptors();

        if (exceptionInterceptorClasses != null && !"".equals(exceptionInterceptorClasses)) {
            this.exceptionInterceptor = new ExceptionInterceptorChain(exceptionInterceptorClasses);
        }

        this.usePlatformCharsetConverters = getUseJvmCharsetConverters();

        this.log = LogFactory.getLogger(getLogger(), LOGGER_INSTANCE_NAME, getExceptionInterceptor());

        if (getProfileSql() || getUseUsageAdvisor()) {
            this.eventSink = ProfilerEventHandlerFactory.getInstance(getMultiHostSafeProxy());
        }

        if (getCachePreparedStatements()) {
            createPreparedStatementCaches();
        }

        if (getNoDatetimeStringSync() && getUseTimezone()) {
            throw SQLError.createSQLException("Can't enable noDatetimeStringSync and useTimezone configuration properties at the same time",
                    SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, getExceptionInterceptor());
        }

        if (getCacheCallableStatements()) {
            this.parsedCallableStatementCache = new LRUCache(getCallableStatementCacheSize());
        }

        if (getAllowMultiQueries()) {
            setCacheResultSetMetadata(false); // we don't handle this yet
        }

        if (getCacheResultSetMetadata()) {
            this.resultSetMetadataCache = new LRUCache(getMetadataCacheSize());
        }

        if (getSocksProxyHost() != null) {
            setSocketFactoryClassName("com.mysql.jdbc.SocksProxySocketFactory");
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
        String connectionInterceptorClasses = getConnectionLifecycleInterceptors();

        this.connectionLifecycleInterceptors = null;

        if (connectionInterceptorClasses != null) {
            this.connectionLifecycleInterceptors = Util.loadExtensions(this, this.props, connectionInterceptorClasses, "Connection.badLifecycleInterceptor",
                    getExceptionInterceptor());
        }

        setSessionVariables();

        //
        // the "boolean" type didn't come along until MySQL-4.1
        //

        if (!versionMeetsMinimum(4, 1, 0)) {
            setTransformedBitIsBoolean(false);
        }

        this.parserKnowsUnicode = versionMeetsMinimum(4, 1, 0);

        //
        // Users can turn off detection of server-side prepared statements
        //
        if (getUseServerPreparedStmts() && versionMeetsMinimum(4, 1, 0)) {
            this.useServerPreparedStmts = true;

            if (versionMeetsMinimum(5, 0, 0) && !versionMeetsMinimum(5, 0, 3)) {
                this.useServerPreparedStmts = false; // 4.1.2+ style prepared
                // statements
                // don't work on these versions
            }
        }

        //
        // If version is greater than 3.21.22 get the server variables.
        if (versionMeetsMinimum(3, 21, 22)) {
            loadServerVariables();

            if (versionMeetsMinimum(5, 0, 2)) {
                this.autoIncrementIncrement = getServerVariableAsInt("auto_increment_increment", 1);
            } else {
                this.autoIncrementIncrement = 1;
            }

            buildCollationMapping();

            LicenseConfiguration.checkLicenseType(this.serverVariables);

            String lowerCaseTables = this.serverVariables.get("lower_case_table_names");

            this.lowerCaseTableNames = "on".equalsIgnoreCase(lowerCaseTables) || "1".equalsIgnoreCase(lowerCaseTables) || "2".equalsIgnoreCase(lowerCaseTables);

            this.storesLowerCaseTableName = "1".equalsIgnoreCase(lowerCaseTables) || "on".equalsIgnoreCase(lowerCaseTables);

            configureTimezone();

            if (this.serverVariables.containsKey("max_allowed_packet")) {
                int serverMaxAllowedPacket = getServerVariableAsInt("max_allowed_packet", -1);
                // use server value if maxAllowedPacket hasn't been given, or max_allowed_packet is smaller
                if (serverMaxAllowedPacket != -1 && (serverMaxAllowedPacket < getMaxAllowedPacket() || getMaxAllowedPacket() <= 0)) {
                    setMaxAllowedPacket(serverMaxAllowedPacket);
                } else if (serverMaxAllowedPacket == -1 && getMaxAllowedPacket() == -1) {
                    setMaxAllowedPacket(65535);
                }

                if (getUseServerPrepStmts()) {
                    int preferredBlobSendChunkSize = getBlobSendChunkSize();

                    // LONG_DATA and MySQLIO packet header size
                    int packetHeaderSize = ServerPreparedStatement.BLOB_STREAM_READ_BUF_SIZE + 11;
                    int allowedBlobSendChunkSize = Math.min(preferredBlobSendChunkSize, getMaxAllowedPacket()) - packetHeaderSize;

                    if (allowedBlobSendChunkSize <= 0) {
                        throw SQLError.createSQLException(
                                "Connection setting too low for 'maxAllowedPacket'. "
                                        + "When 'useServerPrepStmts=true', 'maxAllowedPacket' must be higher than " + packetHeaderSize
                                        + ". Check also 'max_allowed_packet' in MySQL configuration files.",
                                SQLError.SQL_STATE_INVALID_CONNECTION_ATTRIBUTE, getExceptionInterceptor());
                    }

                    setBlobSendChunkSize(String.valueOf(allowedBlobSendChunkSize));
                }
            }

            if (this.serverVariables.containsKey("net_buffer_length")) {
                this.netBufferLength = getServerVariableAsInt("net_buffer_length", 16 * 1024);
            }

            checkTransactionIsolationLevel();

            if (!versionMeetsMinimum(4, 1, 0)) {
                checkServerEncoding();
            }

            this.io.checkForCharsetMismatch();

            if (this.serverVariables.containsKey("sql_mode")) {
                int sqlMode = 0;

                String sqlModeAsString = this.serverVariables.get("sql_mode");
                try {
                    sqlMode = Integer.parseInt(sqlModeAsString);
                } catch (NumberFormatException nfe) {
                    // newer versions of the server has this as a string-y list...
                    sqlMode = 0;

                    if (sqlModeAsString != null) {
                        if (sqlModeAsString.indexOf("ANSI_QUOTES") != -1) {
                            sqlMode |= 4;
                        }

                        if (sqlModeAsString.indexOf("NO_BACKSLASH_ESCAPES") != -1) {
                            this.noBackslashEscapes = true;
                        }
                    }
                }

                if ((sqlMode & 4) > 0) {
                    this.useAnsiQuotes = true;
                } else {
                    this.useAnsiQuotes = false;
                }
            }
        }

        boolean overrideDefaultAutocommit = isAutoCommitNonDefaultOnServer();

        configureClientCharacterSet(false);

        try {
            this.errorMessageEncoding = CharsetMapping.getCharacterEncodingForErrorMessages(this);
        } catch (SQLException ex) {
            throw ex;
        } catch (RuntimeException ex) {
            SQLException sqlEx = SQLError.createSQLException(ex.toString(), SQLError.SQL_STATE_ILLEGAL_ARGUMENT, null);
            sqlEx.initCause(ex);
            throw sqlEx;
        }

        if (versionMeetsMinimum(3, 23, 15)) {
            this.transactionsSupported = true;

            if (!overrideDefaultAutocommit) {
                try {
                    setAutoCommit(true); // to override anything the server is set to...reqd by JDBC spec.
                } catch (SQLException ex) {
                    if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || getDisconnectOnExpiredPasswords()) {
                        throw ex;
                    }
                }
            }
        } else {
            this.transactionsSupported = false;
        }

        if (versionMeetsMinimum(3, 23, 36)) {
            this.hasIsolationLevels = true;
        } else {
            this.hasIsolationLevels = false;
        }

        this.hasQuotedIdentifiers = versionMeetsMinimum(3, 23, 6);

        this.io.resetMaxBuf();

        //
        // If we're using MySQL 4.1.0 or newer, we need to figure out what character set metadata will be returned in, and then map that to a Java encoding name
        //
        // We've already set it, and it might be different than what was originally on the server, which is why we use the "special" key to retrieve it
        if (this.io.versionMeetsMinimum(4, 1, 0)) {
            String characterSetResultsOnServerMysql = this.serverVariables.get(JDBC_LOCAL_CHARACTER_SET_RESULTS);

            if (characterSetResultsOnServerMysql == null || StringUtils.startsWithIgnoreCaseAndWs(characterSetResultsOnServerMysql, "NULL")
                    || characterSetResultsOnServerMysql.length() == 0) {
                String defaultMetadataCharsetMysql = this.serverVariables.get("character_set_system");
                String defaultMetadataCharset = null;

                if (defaultMetadataCharsetMysql != null) {
                    defaultMetadataCharset = CharsetMapping.getJavaEncodingForMysqlCharset(defaultMetadataCharsetMysql);
                } else {
                    defaultMetadataCharset = "UTF-8";
                }

                this.characterSetMetadata = defaultMetadataCharset;
            } else {
                this.characterSetResultsOnServer = CharsetMapping.getJavaEncodingForMysqlCharset(characterSetResultsOnServerMysql);
                this.characterSetMetadata = this.characterSetResultsOnServer;
            }
        } else {
            this.characterSetMetadata = getEncoding();
        }

        //
        // Query cache is broken wrt. multi-statements before MySQL-4.1.10
        //

        if (versionMeetsMinimum(4, 1, 0) && !this.versionMeetsMinimum(4, 1, 10) && getAllowMultiQueries()) {
            if (isQueryCacheEnabled()) {
                setAllowMultiQueries(false);
            }
        }

        if (versionMeetsMinimum(5, 0, 0) && (getUseLocalTransactionState() || getElideSetAutoCommits()) && isQueryCacheEnabled()
                && !versionMeetsMinimum(6, 0, 10)) {
            // Can't trust the server status flag on the wire if query cache is enabled, due to Bug#36326
            setUseLocalTransactionState(false);
            setElideSetAutoCommits(false);
        }

        //
        // Server can do this more efficiently for us
        //

        setupServerForTruncationChecks();
    }

    private boolean isQueryCacheEnabled() {
        return "ON".equalsIgnoreCase(this.serverVariables.get("query_cache_type")) && !"0".equalsIgnoreCase(this.serverVariables.get("query_cache_size"));
    }

    private int getServerVariableAsInt(String variableName, int fallbackValue) throws SQLException {
        try {
            return Integer.parseInt(this.serverVariables.get(variableName));
        } catch (NumberFormatException nfe) {
            getLog().logWarn(Messages.getString("Connection.BadValueInServerVariables",
                    new Object[] { variableName, this.serverVariables.get(variableName), Integer.valueOf(fallbackValue) }));

            return fallbackValue;
        }
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

        String initConnectValue = this.serverVariables.get("init_connect");

        if (versionMeetsMinimum(4, 1, 2) && initConnectValue != null && initConnectValue.length() > 0) {
            if (!getElideSetAutoCommits()) {
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
                if (this.getIO().isSetNeededForAutoCommitMode(true)) {
                    // we're not in standard autocommit=true mode
                    this.autoCommit = false;
                    overrideDefaultAutocommit = true;
                }
            }
        }

        return overrideDefaultAutocommit;
    }

    public boolean isClientTzUTC() {
        return this.isClientTzUTC;
    }

    public boolean isClosed() {
        return this.isClosed;
    }

    public boolean isCursorFetchEnabled() throws SQLException {
        return (versionMeetsMinimum(5, 0, 2) && getUseCursorFetch());
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
        synchronized (getConnectionMutex()) {
            return false; // handled higher up
        }
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
     * Tests to see if the connection is in Read Only Mode. Note that prior to 5.6,
     * we cannot really put the database in read only mode, but we pretend we can by
     * returning the value of the readOnly flag
     * 
     * @return true if the connection is read only
     * @exception SQLException
     *                if a database access error occurs
     */
    public boolean isReadOnly() throws SQLException {
        return isReadOnly(true);
    }

    /**
     * Tests to see if the connection is in Read Only Mode. Note that prior to 5.6,
     * we cannot really put the database in read only mode, but we pretend we can by
     * returning the value of the readOnly flag
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
        if (useSessionStatus && !this.isClosed && versionMeetsMinimum(5, 6, 5) && !getUseLocalSessionState() && getReadOnlyPropagatesToServer()) {
            java.sql.Statement stmt = null;
            java.sql.ResultSet rs = null;

            try {
                try {
                    stmt = getMetadataSafeStatement();

                    rs = stmt.executeQuery("select @@session.tx_read_only");
                    if (rs.next()) {
                        return rs.getInt(1) != 0; // mysql has a habit of tri+ state booleans
                    }
                } catch (SQLException ex1) {
                    if (ex1.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || getDisconnectOnExpiredPasswords()) {
                        throw SQLError.createSQLException("Could not retrieve transation read-only status server", SQLError.SQL_STATE_GENERAL_ERROR, ex1,
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

    public boolean isRunningOnJDK13() {
        return this.isRunningOnJDK13;
    }

    public boolean isSameResource(Connection otherConnection) {
        synchronized (getConnectionMutex()) {
            if (otherConnection == null) {
                return false;
            }

            boolean directCompare = true;

            String otherHost = ((ConnectionImpl) otherConnection).origHostToConnectTo;
            String otherOrigDatabase = ((ConnectionImpl) otherConnection).origDatabaseToConnectTo;
            String otherCurrentCatalog = ((ConnectionImpl) otherConnection).database;

            if (!nullSafeCompare(otherHost, this.origHostToConnectTo)) {
                directCompare = false;
            } else if (otherHost != null && otherHost.indexOf(',') == -1 && otherHost.indexOf(':') == -1) {
                // need to check port numbers
                directCompare = (((ConnectionImpl) otherConnection).origPortToConnectTo == this.origPortToConnectTo);
            }

            if (directCompare) {
                if (!nullSafeCompare(otherOrigDatabase, this.origDatabaseToConnectTo) || !nullSafeCompare(otherCurrentCatalog, this.database)) {
                    directCompare = false;
                }
            }

            if (directCompare) {
                return true;
            }

            // Has the user explicitly set a resourceId?
            String otherResourceId = ((ConnectionImpl) otherConnection).getResourceId();
            String myResourceId = getResourceId();

            if (otherResourceId != null || myResourceId != null) {
                directCompare = nullSafeCompare(otherResourceId, myResourceId);

                if (directCompare) {
                    return true;
                }
            }

            return false;
        }
    }

    public boolean isServerTzUTC() {
        return this.isServerTzUTC;
    }

    private boolean usingCachedConfig = false;

    private void createConfigCacheIfNeeded() throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.serverConfigCache != null) {
                return;
            }

            try {
                Class<?> factoryClass;

                factoryClass = Class.forName(getServerConfigCacheFactory());

                @SuppressWarnings("unchecked")
                CacheAdapterFactory<String, Map<String, String>> cacheFactory = ((CacheAdapterFactory<String, Map<String, String>>) factoryClass.newInstance());

                this.serverConfigCache = cacheFactory.getInstance(this, this.myURL, Integer.MAX_VALUE, Integer.MAX_VALUE, this.props);

                ExceptionInterceptor evictOnCommsError = new ExceptionInterceptor() {

                    public void init(Connection conn, Properties config) throws SQLException {
                    }

                    public void destroy() {
                    }

                    @SuppressWarnings("synthetic-access")
                    public SQLException interceptException(SQLException sqlEx, Connection conn) {
                        if (sqlEx.getSQLState() != null && sqlEx.getSQLState().startsWith("08")) {
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
                SQLException sqlEx = SQLError.createSQLException(
                        Messages.getString("Connection.CantFindCacheFactory", new Object[] { getParseInfoCacheFactory(), "parseInfoCacheFactory" }),
                        getExceptionInterceptor());
                sqlEx.initCause(e);

                throw sqlEx;
            } catch (InstantiationException e) {
                SQLException sqlEx = SQLError.createSQLException(
                        Messages.getString("Connection.CantLoadCacheFactory", new Object[] { getParseInfoCacheFactory(), "parseInfoCacheFactory" }),
                        getExceptionInterceptor());
                sqlEx.initCause(e);

                throw sqlEx;
            } catch (IllegalAccessException e) {
                SQLException sqlEx = SQLError.createSQLException(
                        Messages.getString("Connection.CantLoadCacheFactory", new Object[] { getParseInfoCacheFactory(), "parseInfoCacheFactory" }),
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

        if (getCacheServerConfiguration()) {
            createConfigCacheIfNeeded();

            Map<String, String> cachedVariableMap = this.serverConfigCache.get(getURL());

            if (cachedVariableMap != null) {
                String cachedServerVersion = cachedVariableMap.get(SERVER_VERSION_STRING_VAR_NAME);

                if (cachedServerVersion != null && this.io.getServerVersion() != null && cachedServerVersion.equals(this.io.getServerVersion())) {
                    this.serverVariables = cachedVariableMap;
                    this.usingCachedConfig = true;

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

            String versionComment = (this.getParanoid() || version == null) ? "" : "/* " + version + " */";

            this.serverVariables = new HashMap<String, String>();

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
                            this.serverVariables.put(rsmd.getColumnLabel(i), results.getString(i));
                        }
                    }
                } else {
                    results = stmt.executeQuery(versionComment + "SHOW VARIABLES");
                    while (results.next()) {
                        this.serverVariables.put(results.getString(1), results.getString(2));
                    }
                }

                results.close();
                results = null;
            } catch (SQLException ex) {
                if (ex.getErrorCode() != MysqlErrorNumbers.ER_MUST_CHANGE_PASSWORD || getDisconnectOnExpiredPasswords()) {
                    throw ex;
                }
            }

            if (getCacheServerConfiguration()) {
                this.serverVariables.put(SERVER_VERSION_STRING_VAR_NAME, this.io.getServerVersion());

                this.serverConfigCache.put(getURL(), this.serverVariables);

                this.usingCachedConfig = true;
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

        Object escapedSqlResult = EscapeProcessor.escapeSQL(sql, serverSupportsConvertFn(), getMultiHostSafeProxy());

        if (escapedSqlResult instanceof String) {
            return (String) escapedSqlResult;
        }

        return ((EscapeProcessorResult) escapedSqlResult).escapedSql;
    }

    private CallableStatement parseCallableStatement(String sql) throws SQLException {
        Object escapedSqlResult = EscapeProcessor.escapeSQL(sql, serverSupportsConvertFn(), getMultiHostSafeProxy());

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

    public boolean parserKnowsUnicode() {
        return this.parserKnowsUnicode;
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

    public void pingInternal(boolean checkForClosedConnection, int timeoutMillis) throws SQLException {
        if (checkForClosedConnection) {
            checkClosed();
        }

        long pingMillisLifetime = getSelfDestructOnPingSecondsLifetime();
        int pingMaxOperations = getSelfDestructOnPingMaxOperations();

        if ((pingMillisLifetime > 0 && (System.currentTimeMillis() - this.connectionCreationTimeMillis) > pingMillisLifetime)
                || (pingMaxOperations > 0 && pingMaxOperations <= this.io.getCommandCount())) {

            close();

            throw SQLError.createSQLException(Messages.getString("Connection.exceededConnectionLifetime"), SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE,
                    getExceptionInterceptor());
        }
        // Need MySQL-3.22.1, but who uses anything older!?
        this.io.sendCommand(MysqlDefs.PING, null, null, false, null, timeoutMillis);
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
        if (versionMeetsMinimum(5, 0, 0)) {
            CallableStatement cStmt = null;

            if (!getCacheCallableStatements()) {

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

        throw SQLError.createSQLException("Callable statements not supported.", SQLError.SQL_STATE_DRIVER_NOT_CAPABLE, getExceptionInterceptor());
    }

    /**
     * @see Connection#prepareCall(String, int, int, int)
     */
    public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (getPedantic()) {
            if (resultSetHoldability != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                throw SQLError.createSQLException("HOLD_CUSRORS_OVER_COMMIT is only supported holdability level", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        CallableStatement cStmt = (com.mysql.jdbc.CallableStatement) prepareCall(sql, resultSetType, resultSetConcurrency);

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

    /**
     * @see Connection#prepareStatement(String, int)
     */
    public java.sql.PreparedStatement prepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);

        ((com.mysql.jdbc.PreparedStatement) pStmt).setRetrieveGeneratedKeys(autoGenKeyIndex == java.sql.Statement.RETURN_GENERATED_KEYS);

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

            String nativeSql = getProcessEscapeCodesForPrepStmts() ? nativeSQL(sql) : sql;

            if (this.useServerPreparedStmts && getEmulateUnsupportedPstmts()) {
                canServerPrepare = canHandleAsServerPreparedStatement(nativeSql);
            }

            if (this.useServerPreparedStmts && canServerPrepare) {
                if (this.getCachePreparedStatements()) {
                    synchronized (this.serverSideStatementCache) {
                        pStmt = (com.mysql.jdbc.ServerPreparedStatement) this.serverSideStatementCache.remove(sql);

                        if (pStmt != null) {
                            ((com.mysql.jdbc.ServerPreparedStatement) pStmt).setClosed(false);
                            pStmt.clearParameters();
                        }

                        if (pStmt == null) {
                            try {
                                pStmt = ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.database, resultSetType,
                                        resultSetConcurrency);
                                if (sql.length() < getPreparedStatementCacheSqlLimit()) {
                                    ((com.mysql.jdbc.ServerPreparedStatement) pStmt).isCached = true;
                                }

                                pStmt.setResultSetType(resultSetType);
                                pStmt.setResultSetConcurrency(resultSetConcurrency);
                            } catch (SQLException sqlEx) {
                                // Punt, if necessary
                                if (getEmulateUnsupportedPstmts()) {
                                    pStmt = (PreparedStatement) clientPrepareStatement(nativeSql, resultSetType, resultSetConcurrency, false);

                                    if (sql.length() < getPreparedStatementCacheSqlLimit()) {
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
                        if (getEmulateUnsupportedPstmts()) {
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

    /**
     * @see Connection#prepareStatement(String, int, int, int)
     */
    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (getPedantic()) {
            if (resultSetHoldability != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                throw SQLError.createSQLException("HOLD_CUSRORS_OVER_COMMIT is only supported holdability level", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    /**
     * @see Connection#prepareStatement(String, int[])
     */
    public java.sql.PreparedStatement prepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);

        ((com.mysql.jdbc.PreparedStatement) pStmt).setRetrieveGeneratedKeys((autoGenKeyIndexes != null) && (autoGenKeyIndexes.length > 0));

        return pStmt;
    }

    /**
     * @see Connection#prepareStatement(String, String[])
     */
    public java.sql.PreparedStatement prepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        java.sql.PreparedStatement pStmt = prepareStatement(sql);

        ((com.mysql.jdbc.PreparedStatement) pStmt).setRetrieveGeneratedKeys((autoGenKeyColNames != null) && (autoGenKeyColNames.length > 0));

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

                if (getUseUsageAdvisor()) {
                    if (!calledExplicitly) {
                        String message = "Connection implicitly closed by Driver. You should call Connection.close() from your code to free resources more efficiently and avoid resource leaks.";

                        this.eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_WARN, "", this.getCatalog(), this.getId(), -1, -1,
                                System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, this.pointOfOrigin, message));
                    }

                    long connectionLifeTime = System.currentTimeMillis() - this.connectionCreationTimeMillis;

                    if (connectionLifeTime < 500) {
                        String message = "Connection lifetime of < .5 seconds. You might be un-necessarily creating short-lived connections and should investigate connection pooling to be more efficient.";

                        this.eventSink.consumeEvent(new ProfilerEvent(ProfilerEvent.TYPE_WARN, "", this.getCatalog(), this.getId(), -1, -1,
                                System.currentTimeMillis(), 0, Constants.MILLIS_I18N, null, this.pointOfOrigin, message));
                    }
                }

                try {
                    closeAllOpenStatements();
                } catch (SQLException ex) {
                    sqlEx = ex;
                }

                if (this.io != null) {
                    try {
                        this.io.quit();
                    } catch (Exception e) {
                    }

                }
            } else {
                this.io.forceClose();
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
            this.openStatements = null;
            if (this.io != null) {
                this.io.releaseResources();
                this.io = null;
            }
            this.statementInterceptors = null;
            this.exceptionInterceptor = null;
            ProfilerEventHandlerFactory.removeInstance(this);

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
            if (pstmt.isPoolable()) {
                synchronized (this.serverSideStatementCache) {
                    this.serverSideStatementCache.put(pstmt.originalSql, pstmt);
                }
            }
        }
    }

    public void decachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException {
        synchronized (getConnectionMutex()) {
            if (pstmt.isPoolable()) {
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
        synchronized (this.openStatements) {
            this.openStatements.put(stmt, stmt);
        }
    }

    /**
     * @see Connection#releaseSavepoint(Savepoint)
     */
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
        if (getGatherPerformanceMetrics()) {
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

            this.log.logInfo(logMessage);

            this.metricsLastReportedMs = System.currentTimeMillis();
        }
    }

    /**
     * Reports currently collected metrics if this feature is enabled and the
     * timeout has passed.
     */
    protected void reportMetricsIfNeeded() {
        if (getGatherPerformanceMetrics()) {
            if ((System.currentTimeMillis() - this.metricsLastReportedMs) > getReportMetricsIntervalMillis()) {
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
     * Resets the server-side state of this connection. Doesn't work for MySQL
     * versions older than 4.0.6 or if isParanoid() is set (it will become a
     * no-op in these cases). Usually only used from connection pooling code.
     * 
     * @throws SQLException
     *             if the operation fails while resetting server state.
     */
    public void resetServerState() throws SQLException {
        if (!getParanoid() && ((this.io != null) && versionMeetsMinimum(4, 0, 6))) {
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
                    IterateBlock<Extension> iter = new IterateBlock<Extension>(this.connectionLifecycleInterceptors.iterator()) {

                        @Override
                        void forEach(Extension each) throws SQLException {
                            if (!((ConnectionLifecycleInterceptor) each).rollback()) {
                                this.stopIterating = true;
                            }
                        }
                    };

                    iter.doForAll();

                    if (!iter.fullIteration()) {
                        return;
                    }
                }
                // no-op if _relaxAutoCommit == true
                if (this.autoCommit && !getRelaxAutoCommit()) {
                    throw SQLError.createSQLException("Can't call rollback when autocommit=true", SQLError.SQL_STATE_CONNECTION_NOT_OPEN,
                            getExceptionInterceptor());
                } else if (this.transactionsSupported) {
                    try {
                        rollbackNoChecks();
                    } catch (SQLException sqlEx) {
                        // We ignore non-transactional tables if told to do so
                        if (getIgnoreNonTxTables() && (sqlEx.getErrorCode() == SQLError.ER_WARNING_NOT_COMPLETE_ROLLBACK)) {
                            return;
                        }
                        throw sqlEx;

                    }
                }
            } catch (SQLException sqlException) {
                if (SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlException.getSQLState())) {
                    throw SQLError.createSQLException("Communications link failure during rollback(). Transaction resolution unknown.",
                            SQLError.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN, getExceptionInterceptor());
                }

                throw sqlException;
            } finally {
                this.needsPing = this.getReconnectAtTxEnd();
            }
        }
    }

    /**
     * @see Connection#rollback(Savepoint)
     */
    public void rollback(final Savepoint savepoint) throws SQLException {

        synchronized (getConnectionMutex()) {
            if (versionMeetsMinimum(4, 0, 14) || versionMeetsMinimum(4, 1, 1)) {
                checkClosed();

                try {
                    if (this.connectionLifecycleInterceptors != null) {
                        IterateBlock<Extension> iter = new IterateBlock<Extension>(this.connectionLifecycleInterceptors.iterator()) {

                            @Override
                            void forEach(Extension each) throws SQLException {
                                if (!((ConnectionLifecycleInterceptor) each).rollback(savepoint)) {
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
                                    throw SQLError.createSQLException("Savepoint '" + savepoint.getSavepointName() + "' does not exist",
                                            SQLError.SQL_STATE_ILLEGAL_ARGUMENT, errno, getExceptionInterceptor());
                                }
                            }
                        }

                        // We ignore non-transactional tables if told to do so
                        if (getIgnoreNonTxTables() && (sqlEx.getErrorCode() != SQLError.ER_WARNING_NOT_COMPLETE_ROLLBACK)) {
                            throw sqlEx;
                        }

                        if (SQLError.SQL_STATE_COMMUNICATION_LINK_FAILURE.equals(sqlEx.getSQLState())) {
                            throw SQLError.createSQLException("Communications link failure during rollback(). Transaction resolution unknown.",
                                    SQLError.SQL_STATE_TRANSACTION_RESOLUTION_UNKNOWN, getExceptionInterceptor());
                        }

                        throw sqlEx;
                    } finally {
                        closeStatement(stmt);
                    }
                } finally {
                    this.needsPing = this.getReconnectAtTxEnd();
                }
            } else {
                throw SQLError.createSQLFeatureNotSupportedException();
            }
        }
    }

    private void rollbackNoChecks() throws SQLException {
        if (getUseLocalTransactionState() && versionMeetsMinimum(5, 0, 0)) {
            if (!this.io.inTransactionOnServer()) {
                return; // effectively a no-op
            }
        }

        execSQL(null, "rollback", -1, null, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, false, this.database, null, false);
    }

    /**
     * @see java.sql.Connection#prepareStatement(String)
     */
    public java.sql.PreparedStatement serverPrepareStatement(String sql) throws SQLException {

        String nativeSql = getProcessEscapeCodesForPrepStmts() ? nativeSQL(sql) : sql;

        return ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.getCatalog(), DEFAULT_RESULT_SET_TYPE,
                DEFAULT_RESULT_SET_CONCURRENCY);
    }

    /**
     * @see java.sql.Connection#prepareStatement(String, int)
     */
    public java.sql.PreparedStatement serverPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        String nativeSql = getProcessEscapeCodesForPrepStmts() ? nativeSQL(sql) : sql;

        PreparedStatement pStmt = ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.getCatalog(), DEFAULT_RESULT_SET_TYPE,
                DEFAULT_RESULT_SET_CONCURRENCY);

        pStmt.setRetrieveGeneratedKeys(autoGenKeyIndex == java.sql.Statement.RETURN_GENERATED_KEYS);

        return pStmt;
    }

    /**
     * @see java.sql.Connection#prepareStatement(String, int, int)
     */
    public java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        String nativeSql = getProcessEscapeCodesForPrepStmts() ? nativeSQL(sql) : sql;

        return ServerPreparedStatement.getInstance(getMultiHostSafeProxy(), nativeSql, this.getCatalog(), resultSetType, resultSetConcurrency);
    }

    /**
     * @see java.sql.Connection#prepareStatement(String, int, int, int)
     */
    public java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        if (getPedantic()) {
            if (resultSetHoldability != java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT) {
                throw SQLError.createSQLException("HOLD_CUSRORS_OVER_COMMIT is only supported holdability level", SQLError.SQL_STATE_ILLEGAL_ARGUMENT,
                        getExceptionInterceptor());
            }
        }

        return serverPrepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    /**
     * @see java.sql.Connection#prepareStatement(String, int[])
     */
    public java.sql.PreparedStatement serverPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {

        PreparedStatement pStmt = (PreparedStatement) serverPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyIndexes != null) && (autoGenKeyIndexes.length > 0));

        return pStmt;
    }

    /**
     * @see java.sql.Connection#prepareStatement(String, String[])
     */
    public java.sql.PreparedStatement serverPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        PreparedStatement pStmt = (PreparedStatement) serverPrepareStatement(sql);

        pStmt.setRetrieveGeneratedKeys((autoGenKeyColNames != null) && (autoGenKeyColNames.length > 0));

        return pStmt;
    }

    public boolean serverSupportsConvertFn() throws SQLException {
        return versionMeetsMinimum(4, 0, 2);
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
                IterateBlock<Extension> iter = new IterateBlock<Extension>(this.connectionLifecycleInterceptors.iterator()) {

                    @Override
                    void forEach(Extension each) throws SQLException {
                        if (!((ConnectionLifecycleInterceptor) each).setAutoCommit(autoCommitFlag)) {
                            this.stopIterating = true;
                        }
                    }
                };

                iter.doForAll();

                if (!iter.fullIteration()) {
                    return;
                }
            }

            if (getAutoReconnectForPools()) {
                setHighAvailability(true);
            }

            try {
                if (this.transactionsSupported) {

                    boolean needsSetOnServer = true;

                    if (this.getUseLocalSessionState() && this.autoCommit == autoCommitFlag) {
                        needsSetOnServer = false;
                    } else if (!this.getHighAvailability()) {
                        needsSetOnServer = this.getIO().isSetNeededForAutoCommitMode(autoCommitFlag);
                    }

                    // this internal value must be set first as failover depends on it being set to true to fail over (which is done by most app servers and
                    // connection pools at the end of a transaction), and the driver issues an implicit set based on this value when it (re)-connects to a
                    // server so the value holds across connections
                    this.autoCommit = autoCommitFlag;

                    if (needsSetOnServer) {
                        execSQL(null, autoCommitFlag ? "SET autocommit=1" : "SET autocommit=0", -1, null, DEFAULT_RESULT_SET_TYPE,
                                DEFAULT_RESULT_SET_CONCURRENCY, false, this.database, null, false);
                    }

                } else {
                    if ((autoCommitFlag == false) && !getRelaxAutoCommit()) {
                        throw SQLError.createSQLException("MySQL Versions Older than 3.23.15 do not support transactions",
                                SQLError.SQL_STATE_CONNECTION_NOT_OPEN, getExceptionInterceptor());
                    }

                    this.autoCommit = autoCommitFlag;
                }
            } finally {
                if (this.getAutoReconnectForPools()) {
                    setHighAvailability(false);
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
                IterateBlock<Extension> iter = new IterateBlock<Extension>(this.connectionLifecycleInterceptors.iterator()) {

                    @Override
                    void forEach(Extension each) throws SQLException {
                        if (!((ConnectionLifecycleInterceptor) each).setCatalog(catalog)) {
                            this.stopIterating = true;
                        }
                    }
                };

                iter.doForAll();

                if (!iter.fullIteration()) {
                    return;
                }
            }

            if (getUseLocalSessionState()) {
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
            query.append(StringUtils.quoteIdentifier(catalog, quotedId, getPedantic()));

            execSQL(null, query.toString(), -1, null, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, false, this.database, null, false);

            this.database = catalog;
        }
    }

    /**
     * @param failedOver
     *            The failedOver to set.
     */
    public void setFailedOver(boolean flag) {
        synchronized (getConnectionMutex()) {
            // handled higher up
        }
    }

    /**
     * @see Connection#setHoldability(int)
     */
    public void setHoldability(int arg0) throws SQLException {
        // do nothing
    }

    public void setInGlobalTx(boolean flag) {
        this.isInGlobalTx = flag;
    }

    // exposed for testing
    /**
     * @param preferSlaveDuringFailover
     *            The preferSlaveDuringFailover to set.
     */
    public void setPreferSlaveDuringFailover(boolean flag) {
        // no-op, handled further up in the wrapper
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
        checkClosed();

        setReadOnlyInternal(readOnlyFlag);
    }

    public void setReadOnlyInternal(boolean readOnlyFlag) throws SQLException {
        // note this this is safe even inside a transaction
        if (getReadOnlyPropagatesToServer() && versionMeetsMinimum(5, 6, 5)) {
            if (!getUseLocalSessionState() || (readOnlyFlag != this.readOnly)) {
                execSQL(null, "set session transaction " + (readOnlyFlag ? "read only" : "read write"), -1, null, DEFAULT_RESULT_SET_TYPE,
                        DEFAULT_RESULT_SET_CONCURRENCY, false, this.database, null, false);
            }
        }

        this.readOnly = readOnlyFlag;
    }

    /**
     * @see Connection#setSavepoint()
     */
    public java.sql.Savepoint setSavepoint() throws SQLException {
        MysqlSavepoint savepoint = new MysqlSavepoint(getExceptionInterceptor());

        setSavepoint(savepoint);

        return savepoint;
    }

    private void setSavepoint(MysqlSavepoint savepoint) throws SQLException {

        synchronized (getConnectionMutex()) {
            if (versionMeetsMinimum(4, 0, 14) || versionMeetsMinimum(4, 1, 1)) {
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
            } else {
                throw SQLError.createSQLFeatureNotSupportedException();
            }
        }
    }

    /**
     * @see Connection#setSavepoint(String)
     */
    public java.sql.Savepoint setSavepoint(String name) throws SQLException {
        synchronized (getConnectionMutex()) {
            MysqlSavepoint savepoint = new MysqlSavepoint(name, getExceptionInterceptor());

            setSavepoint(savepoint);

            return savepoint;
        }
    }

    private void setSessionVariables() throws SQLException {
        if (this.versionMeetsMinimum(4, 0, 0) && getSessionVariables() != null) {
            List<String> variablesToSet = StringUtils.split(getSessionVariables(), ",", "\"'", "\"'", false);

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

            if (this.hasIsolationLevels) {
                String sql = null;

                boolean shouldSendSet = false;

                if (getAlwaysSendSetIsolation()) {
                    shouldSendSet = true;
                } else {
                    if (level != this.isolationLevel) {
                        shouldSendSet = true;
                    }
                }

                if (getUseLocalSessionState()) {
                    shouldSendSet = this.isolationLevel != level;
                }

                if (shouldSendSet) {
                    switch (level) {
                        case java.sql.Connection.TRANSACTION_NONE:
                            throw SQLError.createSQLException("Transaction isolation level NONE not supported by MySQL", getExceptionInterceptor());

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
                            throw SQLError.createSQLException("Unsupported transaction isolation level '" + level + "'", SQLError.SQL_STATE_DRIVER_NOT_CAPABLE,
                                    getExceptionInterceptor());
                    }

                    execSQL(null, sql, -1, null, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, false, this.database, null, false);

                    this.isolationLevel = level;
                }
            } else {
                throw SQLError.createSQLException("Transaction Isolation Levels are not supported on MySQL versions older than 3.23.36.",
                        SQLError.SQL_STATE_DRIVER_NOT_CAPABLE, getExceptionInterceptor());
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
        if (getJdbcCompliantTruncation()) {
            if (versionMeetsMinimum(5, 0, 2)) {
                String currentSqlMode = this.serverVariables.get("sql_mode");

                boolean strictTransTablesIsSet = StringUtils.indexOfIgnoreCase(currentSqlMode, "STRICT_TRANS_TABLES") != -1;

                if (currentSqlMode == null || currentSqlMode.length() == 0 || !strictTransTablesIsSet) {
                    StringBuilder commandBuf = new StringBuilder("SET sql_mode='");

                    if (currentSqlMode != null && currentSqlMode.length() > 0) {
                        commandBuf.append(currentSqlMode);
                        commandBuf.append(",");
                    }

                    commandBuf.append("STRICT_TRANS_TABLES'");

                    execSQL(null, commandBuf.toString(), -1, null, DEFAULT_RESULT_SET_TYPE, DEFAULT_RESULT_SET_CONCURRENCY, false, this.database, null, false);

                    setJdbcCompliantTruncation(false); // server's handling this for us now
                } else if (strictTransTablesIsSet) {
                    // We didn't set it, but someone did, so we piggy back on it
                    setJdbcCompliantTruncation(false); // server's handling this for us now
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
            this.io.sendCommand(MysqlDefs.SHUTDOWN, null, null, false, null, 0);
        } catch (Exception ex) {
            SQLException sqlEx = SQLError.createSQLException(Messages.getString("Connection.UnhandledExceptionDuringShutdown"),
                    SQLError.SQL_STATE_GENERAL_ERROR, getExceptionInterceptor());

            sqlEx.initCause(ex);

            throw sqlEx;
        }
    }

    public boolean supportsIsolationLevel() {
        return this.hasIsolationLevels;
    }

    public boolean supportsQuotedIdentifiers() {
        return this.hasQuotedIdentifiers;
    }

    public boolean supportsTransactions() {
        return this.transactionsSupported;
    }

    /**
     * Remove the given statement from the list of open statements
     * 
     * @param stmt
     *            the Statement instance to remove
     */
    public void unregisterStatement(Statement stmt) {
        if (this.openStatements != null) {
            synchronized (this.openStatements) {
                this.openStatements.remove(stmt);
            }
        }
    }

    public boolean useAnsiQuotedIdentifiers() {
        synchronized (getConnectionMutex()) {
            return this.useAnsiQuotes;
        }
    }

    public boolean versionMeetsMinimum(int major, int minor, int subminor) throws SQLException {
        checkClosed();

        return this.io.versionMeetsMinimum(major, minor, subminor);
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
            resultSet.buildIndexMapping();
            resultSet.initializeWithMetadata();

            if (resultSet instanceof UpdatableResultSet) {
                ((UpdatableResultSet) resultSet).checkUpdatability();
            }

            resultSet.populateCachedMetaData(cachedMetaData);

            this.resultSetMetadataCache.put(sql, cachedMetaData);
        } else {
            resultSet.initializeFromCachedMetaData(cachedMetaData);
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

    public void initializeExtension(Extension ex) throws SQLException {
        ex.init(this, this.props);
    }

    public void transactionBegun() throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.connectionLifecycleInterceptors != null) {
                IterateBlock<Extension> iter = new IterateBlock<Extension>(this.connectionLifecycleInterceptors.iterator()) {

                    @Override
                    void forEach(Extension each) throws SQLException {
                        ((ConnectionLifecycleInterceptor) each).transactionBegun();
                    }
                };

                iter.doForAll();
            }
        }
    }

    public void transactionCompleted() throws SQLException {
        synchronized (getConnectionMutex()) {
            if (this.connectionLifecycleInterceptors != null) {
                IterateBlock<Extension> iter = new IterateBlock<Extension>(this.connectionLifecycleInterceptors.iterator()) {

                    @Override
                    void forEach(Extension each) throws SQLException {
                        ((ConnectionLifecycleInterceptor) each).transactionCompleted();
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
        synchronized (getConnectionMutex()) {
            SocketFactory factory = getIO().socketFactory;

            if (factory instanceof SocketMetadata) {
                return ((SocketMetadata) factory).isLocallyConnected(this);
            }
            getLog().logWarn(Messages.getString("Connection.NoMetadataOnSocketFactory"));
            return false;
        }
    }

    /**
     * Returns the sql select limit max-rows for this session.
     */
    public int getSessionMaxRows() {
        synchronized (getConnectionMutex()) {
            return this.sessionMaxRows;
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
            if (this.sessionMaxRows != max) {
                this.sessionMaxRows = max;
                execSQL(null, "SET SQL_SELECT_LIMIT=" + (this.sessionMaxRows == -1 ? "DEFAULT" : this.sessionMaxRows), -1, null, DEFAULT_RESULT_SET_TYPE,
                        DEFAULT_RESULT_SET_CONCURRENCY, false, this.database, null, false);
            }
        }
    }

    // JDBC-4.1
    // until we flip catalog/schema, this is a no-op
    public void setSchema(String schema) throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
        }
    }

    // JDBC-4.1
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
            throw SQLError.createSQLException("Executor can not be null", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
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

    // JDBC-4.1
    public void setNetworkTimeout(Executor executor, final int milliseconds) throws SQLException {
        synchronized (getConnectionMutex()) {
            SecurityManager sec = System.getSecurityManager();

            if (sec != null) {
                sec.checkPermission(SET_NETWORK_TIMEOUT_PERM);
            }

            if (executor == null) {
                throw SQLError.createSQLException("Executor can not be null", SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
            }

            checkClosed();
            final MysqlIO mysqlIo = this.io;

            executor.execute(new Runnable() {

                public void run() {
                    try {
                        setSocketTimeout(milliseconds); // for re-connects
                        mysqlIo.setSocketTimeout(milliseconds);
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }
    }

    // JDBC-4.1
    public int getNetworkTimeout() throws SQLException {
        synchronized (getConnectionMutex()) {
            checkClosed();
            return getSocketTimeout();
        }
    }

    public ProfilerEventHandler getProfilerEventHandlerInstance() {
        return this.eventSink;
    }

    public void setProfilerEventHandlerInstance(ProfilerEventHandler h) {
        this.eventSink = h;
    }
}
