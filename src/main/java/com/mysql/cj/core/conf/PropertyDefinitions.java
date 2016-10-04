/*
  Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.conf;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.mysql.cj.api.conf.PropertyDefinition;
import com.mysql.cj.api.conf.RuntimeProperty;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.WrongArgumentException;
import com.mysql.cj.core.io.SocksProxySocketFactory;
import com.mysql.cj.core.io.StandardSocketFactory;
import com.mysql.cj.core.log.StandardLogger;
import com.mysql.cj.core.profiler.LoggingProfilerEventHandler;
import com.mysql.cj.core.util.PerVmServerConfigCacheFactory;
import com.mysql.cj.jdbc.ha.StandardLoadBalanceExceptionChecker;
import com.mysql.cj.jdbc.util.PerConnectionLRUFactory;
import com.mysql.cj.mysqla.authentication.MysqlNativePasswordPlugin;

public class PropertyDefinitions {

    /*
     * Built-in system properties
     */
    public static final String SYSP_line_separator = "line.separator";
    public static final String SYSP_java_vendor = "java.vendor";
    public static final String SYSP_java_version = "java.version";
    public static final String SYSP_java_vm_vendor = "java.vm.vendor";
    public static final String SYSP_os_name = "os.name";
    public static final String SYSP_os_arch = "os.arch";
    public static final String SYSP_os_version = "os.version";
    public static final String SYSP_file_encoding = "file.encoding";

    /*
     * Custom system properties
     */
    public static final String SYSP_testsuite_url /*                          */ = "com.mysql.cj.testsuite.url";
    public final static String SYSP_testsuite_url_admin /*                    */ = "com.mysql.cj.testsuite.url.admin";
    public static final String SYSP_testsuite_url_cluster /*                  */ = "com.mysql.cj.testsuite.url.cluster";
    /** Connection string to server compiled with OpenSSL */
    public static final String SYSP_testsuite_url_openssl /*                  */ = "com.mysql.cj.testsuite.url.openssl";

    public static final String SYSP_testsuite_url_mysqlx /*                   */ = "com.mysql.cj.testsuite.mysqlx.url";

    public static final String SYSP_testsuite_cantGrant /*                    */ = "com.mysql.cj.testsuite.cantGrant";
    public static final String SYSP_testsuite_disable_multihost_tests /*      */ = "com.mysql.cj.testsuite.disable.multihost.tests"; // TODO should be more specific for different types of multi-host configs

    /** For testsuite.regression.DataSourceRegressionTest */
    public static final String SYSP_testsuite_ds_host /*                      */ = "com.mysql.cj.testsuite.ds.host";
    /** For testsuite.regression.DataSourceRegressionTest */
    public static final String SYSP_testsuite_ds_port /*                      */ = "com.mysql.cj.testsuite.ds.port";
    /** For testsuite.regression.DataSourceRegressionTest */
    public static final String SYSP_testsuite_ds_db /*                        */ = "com.mysql.cj.testsuite.ds.db";
    /** For testsuite.regression.DataSourceRegressionTest */
    public static final String SYSP_testsuite_ds_user /*                      */ = "com.mysql.cj.testsuite.ds.user";
    /** For testsuite.regression.DataSourceRegressionTest */
    public static final String SYSP_testsuite_ds_password /*                  */ = "com.mysql.cj.testsuite.ds.password";

    /** For testsuite.perf.LoadStorePerfTest */
    public static final String SYSP_testsuite_loadstoreperf_tabletype /*      */ = "com.mysql.cj.testsuite.loadstoreperf.tabletype"; // TODO document allowed types
    /** For testsuite.perf.LoadStorePerfTest */
    public static final String SYSP_testsuite_loadstoreperf_useBigResults /*  */ = "com.mysql.cj.testsuite.loadstoreperf.useBigResults";

    /** The system property that must exist to run the shutdown test in testsuite.simple.MiniAdminTest */
    public static final String SYSP_testsuite_miniAdminTest_runShutdown /*    */ = "com.mysql.cj.testsuite.miniAdminTest.runShutdown";

    /** Suppress debug output when running testsuite */
    public static final String SYSP_testsuite_noDebugOutput /*                */ = "com.mysql.cj.testsuite.noDebugOutput";
    /** Don't remove database object created by tests */
    public static final String SYSP_testsuite_retainArtifacts /*              */ = "com.mysql.cj.testsuite.retainArtifacts";
    public static final String SYSP_testsuite_runLongTests /*                 */ = "com.mysql.cj.testsuite.runLongTests";
    public static final String SYSP_testsuite_serverController_basedir /*     */ = "com.mysql.cj.testsuite.serverController.basedir";

    /*
     * Properties added internally after parsing connection string
     */
    public static final String PROTOCOL_PROPERTY_KEY = "PROTOCOL";
    public static final String PATH_PROPERTY_KEY = "PATH";
    public final static String TYPE_PROPERTY_KEY = "TYPE";
    /** Key used to retrieve the hostname value from the properties instance passed to the driver. */
    public static final String HOST_PROPERTY_KEY = "HOST";
    /** Key used to retrieve the port number value from the properties instance passed to the driver. */
    public static final String PORT_PROPERTY_KEY = "PORT";
    /** Key used to retrieve the database value from the properties instance passed to the driver. */
    public static final String DBNAME_PROPERTY_KEY = "DBNAME";
    /** Key used to retrieve the address value ("host:port") from the properties instance passed to the driver. */
    public static final String ADDRESS_PROPERTY_KEY = "ADDRESS";
    /** Key used to retried the host priority in a list of hosts. */
    public static final String PRIORITY_PROPERTY_KEY = "PRIORITY";

    /*
     * Categories of connection properties
     */
    public static final String CATEGORY_AUTH = Messages.getString("ConnectionProperties.categoryAuthentication");
    public static final String CATEGORY_CONNECTION = Messages.getString("ConnectionProperties.categoryConnection");
    public static final String CATEGORY_SESSION = Messages.getString("ConnectionProperties.categorySession");
    public static final String CATEGORY_NETWORK = Messages.getString("ConnectionProperties.categoryNetworking");
    public static final String CATEGORY_SECURITY = Messages.getString("ConnectionProperties.categorySecurity");
    public static final String CATEGORY_STATEMENTS = Messages.getString("ConnectionProperties.categoryStatements");
    public static final String CATEGORY_PREPARED_STATEMENTS = Messages.getString("ConnectionProperties.categoryPreparedStatements");
    public static final String CATEGORY_RESULT_SETS = Messages.getString("ConnectionProperties.categoryResultSets");
    public static final String CATEGORY_METADATA = Messages.getString("ConnectionProperties.categoryMetadata");
    public static final String CATEGORY_BLOBS = Messages.getString("ConnectionProperties.categoryBlobs");
    public static final String CATEGORY_DATETIMES = Messages.getString("ConnectionProperties.categoryDatetimes");
    public static final String CATEGORY_HA = Messages.getString("ConnectionProperties.categoryHA");
    public static final String CATEGORY_PERFORMANCE = Messages.getString("ConnectionProperties.categoryPerformance");
    public static final String CATEGORY_DEBUGING_PROFILING = Messages.getString("ConnectionProperties.categoryDebuggingProfiling");
    public static final String CATEGORY_EXCEPTIONS = Messages.getString("ConnectionProperties.categoryExceptions");
    public static final String CATEGORY_INTEGRATION = Messages.getString("ConnectionProperties.categoryIntegration");
    public static final String CATEGORY_JDBC = Messages.getString("ConnectionProperties.categoryJDBC");
    public static final String CATEGORY_MYSQLX = Messages.getString("ConnectionProperties.categoryMysqlx");

    public static final String[] PROPERTY_CATEGORIES = new String[] { CATEGORY_AUTH, CATEGORY_CONNECTION, CATEGORY_SESSION, CATEGORY_NETWORK, CATEGORY_SECURITY,
            CATEGORY_STATEMENTS, CATEGORY_PREPARED_STATEMENTS, CATEGORY_RESULT_SETS, CATEGORY_METADATA, CATEGORY_BLOBS, CATEGORY_DATETIMES, CATEGORY_HA,
            CATEGORY_PERFORMANCE, CATEGORY_DEBUGING_PROFILING, CATEGORY_EXCEPTIONS, CATEGORY_INTEGRATION, CATEGORY_JDBC, CATEGORY_MYSQLX };

    /*
     * Property modifiers
     */
    public static final boolean DEFAULT_VALUE_TRUE = true;
    public static final boolean DEFAULT_VALUE_FALSE = false;
    public static final String DEFAULT_VALUE_NULL_STRING = null;

    /** is modifiable in run-time */
    public static final boolean RUNTIME_MODIFIABLE = true;

    /** is not modifiable in run-time (will allow to set not-null value only once) */
    public static final boolean RUNTIME_NOT_MODIFIABLE = false;

    public static final String ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL = "convertToNull";
    public static final String ZERO_DATETIME_BEHAVIOR_EXCEPTION = "exception";
    public static final String ZERO_DATETIME_BEHAVIOR_ROUND = "round";

    /*
     * Connection properties names
     */
    public static final Map<String, PropertyDefinition<?>> PROPERTY_NAME_TO_PROPERTY_DEFINITION;
    public static final Map<String, String> PROPERTY_NAME_TO_ALIAS;

    public static final String PNAME_paranoid = "paranoid";
    public static final String PNAME_passwordCharacterEncoding = "passwordCharacterEncoding";
    public static final String PNAME_serverRSAPublicKeyFile = "serverRSAPublicKeyFile";
    public static final String PNAME_allowPublicKeyRetrieval = "allowPublicKeyRetrieval";
    public static final String PNAME_clientCertificateKeyStoreUrl = "clientCertificateKeyStoreUrl";
    public static final String PNAME_trustCertificateKeyStoreUrl = "trustCertificateKeyStoreUrl";
    public static final String PNAME_clientCertificateKeyStoreType = "clientCertificateKeyStoreType";
    public static final String PNAME_clientCertificateKeyStorePassword = "clientCertificateKeyStorePassword";
    public static final String PNAME_trustCertificateKeyStoreType = "trustCertificateKeyStoreType";
    public static final String PNAME_trustCertificateKeyStorePassword = "trustCertificateKeyStorePassword";
    public static final String PNAME_verifyServerCertificate = "verifyServerCertificate";
    public static final String PNAME_enabledSSLCipherSuites = "enabledSSLCipherSuites";
    public static final String PNAME_useUnbufferedInput = "useUnbufferedInput";
    public static final String PNAME_profilerEventHandler = "profilerEventHandler";
    public static final String PNAME_allowLoadLocalInfile = "allowLoadLocalInfile";
    public static final String PNAME_allowMultiQueries = "allowMultiQueries";
    public static final String PNAME_allowNanAndInf = "allowNanAndInf";
    public static final String PNAME_allowUrlInLocalInfile = "allowUrlInLocalInfile";
    public static final String PNAME_alwaysSendSetIsolation = "alwaysSendSetIsolation";
    public static final String PNAME_autoClosePStmtStreams = "autoClosePStmtStreams";
    public static final String PNAME_allowMasterDownConnections = "allowMasterDownConnections";
    public static final String PNAME_allowSlaveDownConnections = "allowSlaveDownConnections";
    public static final String PNAME_readFromMasterWhenNoSlaves = "readFromMasterWhenNoSlaves";
    public static final String PNAME_autoDeserialize = "autoDeserialize";
    public static final String PNAME_autoGenerateTestcaseScript = "autoGenerateTestcaseScript";
    public static final String PNAME_autoReconnect = "autoReconnect";
    public static final String PNAME_autoReconnectForPools = "autoReconnectForPools";
    public static final String PNAME_blobSendChunkSize = "blobSendChunkSize";
    public static final String PNAME_autoSlowLog = "autoSlowLog";
    public static final String PNAME_blobsAreStrings = "blobsAreStrings";
    public static final String PNAME_functionsNeverReturnBlobs = "functionsNeverReturnBlobs";
    public static final String PNAME_cacheCallableStmts = "cacheCallableStmts";
    public static final String PNAME_cachePrepStmts = "cachePrepStmts";
    public static final String PNAME_cacheResultSetMetadata = "cacheResultSetMetadata";
    public static final String PNAME_serverConfigCacheFactory = "serverConfigCacheFactory";
    public static final String PNAME_cacheServerConfiguration = "cacheServerConfiguration";
    public static final String PNAME_callableStmtCacheSize = "callableStmtCacheSize";
    public static final String PNAME_characterEncoding = "characterEncoding";
    public static final String PNAME_characterSetResults = "characterSetResults";
    public static final String PNAME_connectionAttributes = "connectionAttributes";
    public static final String PNAME_clientInfoProvider = "clientInfoProvider";
    public static final String PNAME_clobberStreamingResults = "clobberStreamingResults";
    public static final String PNAME_clobCharacterEncoding = "clobCharacterEncoding";
    public static final String PNAME_compensateOnDuplicateKeyUpdateCounts = "compensateOnDuplicateKeyUpdateCounts";
    public static final String PNAME_connectionCollation = "connectionCollation";
    public static final String PNAME_connectionLifecycleInterceptors = "connectionLifecycleInterceptors";
    public static final String PNAME_connectTimeout = "connectTimeout";
    public static final String PNAME_continueBatchOnError = "continueBatchOnError";
    public static final String PNAME_createDatabaseIfNotExist = "createDatabaseIfNotExist";
    public static final String PNAME_defaultFetchSize = "defaultFetchSize";
    public static final String PNAME_useServerPrepStmts = "useServerPrepStmts";
    public static final String PNAME_dontTrackOpenResources = "dontTrackOpenResources";
    public static final String PNAME_dumpQueriesOnException = "dumpQueriesOnException";
    public static final String PNAME_elideSetAutoCommits = "elideSetAutoCommits";
    public static final String PNAME_emptyStringsConvertToZero = "emptyStringsConvertToZero";
    public static final String PNAME_emulateLocators = "emulateLocators";
    public static final String PNAME_emulateUnsupportedPstmts = "emulateUnsupportedPstmts";
    public static final String PNAME_enablePacketDebug = "enablePacketDebug";
    public static final String PNAME_enableQueryTimeouts = "enableQueryTimeouts";
    public static final String PNAME_explainSlowQueries = "explainSlowQueries";
    public static final String PNAME_exceptionInterceptors = "exceptionInterceptors";
    public static final String PNAME_failOverReadOnly = "failOverReadOnly";

    public static final String PNAME_gatherPerfMetrics = "gatherPerfMetrics";
    public static final String PNAME_generateSimpleParameterMetadata = "generateSimpleParameterMetadata";
    public static final String PNAME_holdResultsOpenOverStatementClose = "holdResultsOpenOverStatementClose";
    public static final String PNAME_includeInnodbStatusInDeadlockExceptions = "includeInnodbStatusInDeadlockExceptions";
    public static final String PNAME_includeThreadDumpInDeadlockExceptions = "includeThreadDumpInDeadlockExceptions";
    public static final String PNAME_includeThreadNamesAsStatementComment = "includeThreadNamesAsStatementComment";
    public static final String PNAME_ignoreNonTxTables = "ignoreNonTxTables";
    public static final String PNAME_initialTimeout = "initialTimeout";
    public static final String PNAME_interactiveClient = "interactiveClient";
    public static final String PNAME_jdbcCompliantTruncation = "jdbcCompliantTruncation";
    public static final String PNAME_largeRowSizeThreshold = "largeRowSizeThreshold";
    public static final String PNAME_loadBalanceStrategy = "ha.loadBalanceStrategy";
    public static final String PNAME_loadBalanceBlacklistTimeout = "loadBalanceBlacklistTimeout";
    public static final String PNAME_loadBalancePingTimeout = "loadBalancePingTimeout";
    public static final String PNAME_loadBalanceValidateConnectionOnSwapServer = "loadBalanceValidateConnectionOnSwapServer";
    public static final String PNAME_loadBalanceConnectionGroup = "loadBalanceConnectionGroup";
    public static final String PNAME_loadBalanceExceptionChecker = "loadBalanceExceptionChecker";
    public static final String PNAME_loadBalanceSQLStateFailover = "loadBalanceSQLStateFailover";
    public static final String PNAME_loadBalanceSQLExceptionSubclassFailover = "loadBalanceSQLExceptionSubclassFailover";
    public static final String PNAME_loadBalanceAutoCommitStatementRegex = "loadBalanceAutoCommitStatementRegex";
    public static final String PNAME_loadBalanceAutoCommitStatementThreshold = "loadBalanceAutoCommitStatementThreshold";
    public static final String PNAME_localSocketAddress = "localSocketAddress";
    public static final String PNAME_locatorFetchBufferSize = "locatorFetchBufferSize";
    public static final String PNAME_logger = "logger";

    public static final String PNAME_logSlowQueries = "logSlowQueries";
    public static final String PNAME_logXaCommands = "logXaCommands";
    public static final String PNAME_maintainTimeStats = "maintainTimeStats";
    public static final String PNAME_maxQuerySizeToLog = "maxQuerySizeToLog";
    public static final String PNAME_maxReconnects = "maxReconnects";
    public static final String PNAME_retriesAllDown = "retriesAllDown";
    public static final String PNAME_maxRows = "maxRows";
    public static final String PNAME_metadataCacheSize = "metadataCacheSize";
    public static final String PNAME_netTimeoutForStreamingResults = "netTimeoutForStreamingResults";
    public static final String PNAME_noAccessToProcedureBodies = "noAccessToProcedureBodies";
    public static final String PNAME_noDatetimeStringSync = "noDatetimeStringSync";
    public static final String PNAME_nullCatalogMeansCurrent = "nullCatalogMeansCurrent";
    public static final String PNAME_nullNamePatternMatchesAll = "nullNamePatternMatchesAll";
    public static final String PNAME_packetDebugBufferSize = "packetDebugBufferSize";
    public static final String PNAME_padCharsWithSpace = "padCharsWithSpace";
    public static final String PNAME_pedantic = "pedantic";
    public static final String PNAME_pinGlobalTxToPhysicalConnection = "pinGlobalTxToPhysicalConnection";
    public static final String PNAME_populateInsertRowWithDefaultValues = "populateInsertRowWithDefaultValues";
    public static final String PNAME_prepStmtCacheSize = "prepStmtCacheSize";
    public static final String PNAME_prepStmtCacheSqlLimit = "prepStmtCacheSqlLimit";
    public static final String PNAME_parseInfoCacheFactory = "parseInfoCacheFactory";
    public static final String PNAME_processEscapeCodesForPrepStmts = "processEscapeCodesForPrepStmts";
    public static final String PNAME_profileSQL = "profileSQL";
    public static final String PNAME_propertiesTransform = "propertiesTransform";
    public static final String PNAME_queriesBeforeRetryMaster = "queriesBeforeRetryMaster";
    public static final String PNAME_queryTimeoutKillsConnection = "queryTimeoutKillsConnection";
    public static final String PNAME_reconnectAtTxEnd = "reconnectAtTxEnd";
    public static final String PNAME_reportMetricsIntervalMillis = "reportMetricsIntervalMillis";
    public static final String PNAME_requireSSL = "requireSSL";
    public static final String PNAME_resourceId = "resourceId";
    public static final String PNAME_resultSetSizeThreshold = "resultSetSizeThreshold";
    public static final String PNAME_rewriteBatchedStatements = "rewriteBatchedStatements";
    public static final String PNAME_rollbackOnPooledClose = "rollbackOnPooledClose";
    public static final String PNAME_secondsBeforeRetryMaster = "secondsBeforeRetryMaster";
    public static final String PNAME_selfDestructOnPingSecondsLifetime = "selfDestructOnPingSecondsLifetime";
    public static final String PNAME_selfDestructOnPingMaxOperations = "selfDestructOnPingMaxOperations";
    public static final String PNAME_ha_enableJMX = "ha.enableJMX";
    public static final String PNAME_loadBalanceHostRemovalGracePeriod = "loadBalanceHostRemovalGracePeriod";
    public static final String PNAME_serverTimezone = "serverTimezone";
    public static final String PNAME_sessionVariables = "sessionVariables";
    public static final String PNAME_slowQueryThresholdMillis = "slowQueryThresholdMillis";
    public static final String PNAME_slowQueryThresholdNanos = "slowQueryThresholdNanos";
    public static final String PNAME_socketFactory = "socketFactory";
    public static final String PNAME_socksProxyHost = "socksProxyHost";
    public static final String PNAME_socksProxyPort = "socksProxyPort";
    public static final String PNAME_socketTimeout = "socketTimeout";
    public static final String PNAME_statementInterceptors = "statementInterceptors";
    public static final String PNAME_strictUpdates = "strictUpdates";
    public static final String PNAME_overrideSupportsIntegrityEnhancementFacility = "overrideSupportsIntegrityEnhancementFacility";
    public static final String PNAME_tcpNoDelay = "tcpNoDelay";
    public static final String PNAME_tcpKeepAlive = "tcpKeepAlive";
    public static final String PNAME_tcpRcvBuf = "tcpRcvBuf";
    public static final String PNAME_tcpSndBuf = "tcpSndBuf";
    public static final String PNAME_tcpTrafficClass = "tcpTrafficClass";
    public static final String PNAME_tinyInt1isBit = "tinyInt1isBit";
    public static final String PNAME_traceProtocol = "traceProtocol";
    public static final String PNAME_treatUtilDateAsTimestamp = "treatUtilDateAsTimestamp";
    public static final String PNAME_transformedBitIsBoolean = "transformedBitIsBoolean";
    public static final String PNAME_useCompression = "useCompression";
    public static final String PNAME_useColumnNamesInFindColumn = "useColumnNamesInFindColumn";
    public static final String PNAME_useConfigs = "useConfigs";
    public static final String PNAME_useCursorFetch = "useCursorFetch";
    public static final String PNAME_useHostsInPrivileges = "useHostsInPrivileges";
    public static final String PNAME_useInformationSchema = "useInformationSchema";
    public static final String PNAME_useLocalSessionState = "useLocalSessionState";
    public static final String PNAME_useLocalTransactionState = "useLocalTransactionState";
    public static final String PNAME_sendFractionalSeconds = "sendFractionalSeconds";
    public static final String PNAME_useNanosForElapsedTime = "useNanosForElapsedTime";
    public static final String PNAME_useOldAliasMetadataBehavior = "useOldAliasMetadataBehavior";
    public static final String PNAME_useOldUTF8Behavior = "useOldUTF8Behavior";
    public static final String PNAME_useOnlyServerErrorMessages = "useOnlyServerErrorMessages";
    public static final String PNAME_useReadAheadInput = "useReadAheadInput";
    public static final String PNAME_useSSL = "useSSL";
    public static final String PNAME_useStreamLengthsInPrepStmts = "useStreamLengthsInPrepStmts";
    public static final String PNAME_ultraDevHack = "ultraDevHack";
    public static final String PNAME_useUsageAdvisor = "useUsageAdvisor";
    public static final String PNAME_yearIsDateType = "yearIsDateType";
    public static final String PNAME_zeroDateTimeBehavior = "zeroDateTimeBehavior";
    public static final String PNAME_useAffectedRows = "useAffectedRows";
    public static final String PNAME_maxAllowedPacket = "maxAllowedPacket";
    public static final String PNAME_authenticationPlugins = "authenticationPlugins";
    public static final String PNAME_disabledAuthenticationPlugins = "disabledAuthenticationPlugins";
    public static final String PNAME_defaultAuthenticationPlugin = "defaultAuthenticationPlugin";
    public static final String PNAME_disconnectOnExpiredPasswords = "disconnectOnExpiredPasswords";
    public static final String PNAME_getProceduresReturnsFunctions = "getProceduresReturnsFunctions";
    public static final String PNAME_detectCustomCollations = "detectCustomCollations";
    public static final String PNAME_dontCheckOnDuplicateKeyUpdateInSQL = "dontCheckOnDuplicateKeyUpdateInSQL";
    public static final String PNAME_readOnlyPropagatesToServer = "readOnlyPropagatesToServer";

    public static final String PNAME_useAsyncProtocol = "mysqlx.useAsyncProtocol";

    public static final String PNAME_enableEscapeProcessing = "enableEscapeProcessing";

    // TODO following names are used in code but have no definitions
    public static final String PNAME_user = "user";
    public static final String PNAME_password = "password";
    public static final String PNAME_replicationConnectionGroup = "replicationConnectionGroup";
    public static final String PNAME_resultSetScannerRegex = "resultSetScannerRegex";
    public static final String PNAME_clientInfoSetSPName = "clientInfoSetSPName";
    public static final String PNAME_clientInfoGetSPName = "clientInfoGetSPName";
    public static final String PNAME_clientInfoGetBulkSPName = "clientInfoGetBulkSPName";
    public static final String PNAME_clientInfoCatalog = "clientInfoCatalog";
    public static final String PNAME_autoConfigureForColdFusion = "autoConfigureForColdFusion";

    public static final String PNAME_testsuite_faultInjection_serverCharsetIndex = "com.mysql.cj.testsuite.faultInjection.serverCharsetIndex"; // was "com.mysql.jdbc.faultInjection.serverCharsetIndex"
    // ----------------

    /*
     * auth - Authentication property set
     * user auth.user
     * password auth.password
     */

    //
    // Yes, this looks goofy, but we're trying to avoid intern()ing here
    //
    private static final String STANDARD_LOGGER_NAME = StandardLogger.class.getName();

    static {
        PropertyDefinition<?>[] pdefs = new PropertyDefinition<?>[] { new BooleanPropertyDefinition(PNAME_paranoid, DEFAULT_VALUE_FALSE, RUNTIME_NOT_MODIFIABLE,
                Messages.getString("ConnectionProperties.paranoid"), "3.0.1", CATEGORY_SECURITY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_passwordCharacterEncoding, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.passwordCharacterEncoding"), "5.1.7", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_serverRSAPublicKeyFile, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.serverRSAPublicKeyFile"), "5.1.31", CATEGORY_SECURITY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_allowPublicKeyRetrieval, DEFAULT_VALUE_FALSE, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowPublicKeyRetrieval"), "5.1.31", CATEGORY_SECURITY, Integer.MIN_VALUE),

                // SSL Options

                new StringPropertyDefinition(PNAME_clientCertificateKeyStoreUrl, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientCertificateKeyStoreUrl"), "5.1.0", CATEGORY_SECURITY, 5),

                new StringPropertyDefinition(PNAME_trustCertificateKeyStoreUrl, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.trustCertificateKeyStoreUrl"), "5.1.0", CATEGORY_SECURITY, 8),

                new StringPropertyDefinition(PNAME_clientCertificateKeyStoreType, "JKS", RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientCertificateKeyStoreType"), "5.1.0", CATEGORY_SECURITY, 6),

                new StringPropertyDefinition(PNAME_clientCertificateKeyStorePassword, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientCertificateKeyStorePassword"), "5.1.0", CATEGORY_SECURITY, 7),

                new StringPropertyDefinition(PNAME_trustCertificateKeyStoreType, "JKS", RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.trustCertificateKeyStoreType"), "5.1.0", CATEGORY_SECURITY, 9),

                new StringPropertyDefinition(PNAME_trustCertificateKeyStorePassword, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.trustCertificateKeyStorePassword"), "5.1.0", CATEGORY_SECURITY, 10),

                new BooleanPropertyDefinition(PNAME_verifyServerCertificate, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.verifyServerCertificate"), "5.1.6", CATEGORY_SECURITY, 4),

                new StringPropertyDefinition(PNAME_enabledSSLCipherSuites, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.enabledSSLCipherSuites"), "5.1.35", CATEGORY_SECURITY, 11),

                new BooleanPropertyDefinition(PNAME_useUnbufferedInput, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useUnbufferedInput"), "3.0.11", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_profilerEventHandler, LoggingProfilerEventHandler.class.getName(), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.profilerEventHandler"), "5.1.6", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_allowLoadLocalInfile, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadDataLocal"), "3.0.3", CATEGORY_SECURITY, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_allowMultiQueries, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowMultiQueries"), "3.1.1", CATEGORY_SECURITY, 1),

                new BooleanPropertyDefinition(PNAME_allowNanAndInf, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowNANandINF"), "3.1.5", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_allowUrlInLocalInfile, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowUrlInLoadLocal"), "3.1.4", CATEGORY_SECURITY, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_alwaysSendSetIsolation, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.alwaysSendSetIsolation"), "3.1.7", CATEGORY_PERFORMANCE, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_autoClosePStmtStreams, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoClosePstmtStreams"), "3.1.12", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_allowMasterDownConnections, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowMasterDownConnections"), "5.1.27", CATEGORY_HA, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_allowSlaveDownConnections, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowSlaveDownConnections"), "6.0.2", CATEGORY_HA, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_readFromMasterWhenNoSlaves, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.readFromMasterWhenNoSlaves"), "6.0.2", CATEGORY_HA, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_autoDeserialize, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoDeserialize"), "3.1.5", CATEGORY_BLOBS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_autoGenerateTestcaseScript, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoGenerateTestcaseScript"), "3.1.9", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_autoReconnect, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoReconnect"), "1.1", CATEGORY_HA, 0),

                new BooleanPropertyDefinition(PNAME_autoReconnectForPools, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoReconnectForPools"), "3.1.3", CATEGORY_HA, 1),

                new MemorySizePropertyDefinition(PNAME_blobSendChunkSize, 1024 * 1024, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.blobSendChunkSize"), "3.1.9", CATEGORY_BLOBS, Integer.MIN_VALUE, 0, 0),

                new BooleanPropertyDefinition(PNAME_autoSlowLog, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.autoSlowLog"),
                        "5.1.4", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_blobsAreStrings, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.blobsAreStrings"), "5.0.8", CATEGORY_BLOBS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_functionsNeverReturnBlobs, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.functionsNeverReturnBlobs"), "5.0.8", CATEGORY_BLOBS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_cacheCallableStmts, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cacheCallableStatements"), "3.1.2", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_cachePrepStmts, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cachePrepStmts"), "3.0.10", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_cacheResultSetMetadata, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cacheRSMetadata"), "3.1.1", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_serverConfigCacheFactory, PerVmServerConfigCacheFactory.class.getName(), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.serverConfigCacheFactory"), "5.1.1", CATEGORY_PERFORMANCE, 12),

                new BooleanPropertyDefinition(PNAME_cacheServerConfiguration, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cacheServerConfiguration"), "3.1.5", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_callableStmtCacheSize, 100, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.callableStmtCacheSize"), "3.1.2", CATEGORY_PERFORMANCE, 5, 0, Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_characterEncoding, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.characterEncoding"), "1.1g", CATEGORY_SESSION, 5),

                new StringPropertyDefinition(PNAME_characterSetResults, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.characterSetResults"), "3.0.13", CATEGORY_SESSION, 6),

                new StringPropertyDefinition(PNAME_connectionAttributes, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionAttributes"), "5.1.25", CATEGORY_CONNECTION, 7),

                new StringPropertyDefinition(PNAME_clientInfoProvider, "com.mysql.cj.jdbc.CommentClientInfoProvider", RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientInfoProvider"), "5.1.0", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_clobberStreamingResults, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clobberStreamingResults"), "3.0.9", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_clobCharacterEncoding, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clobCharacterEncoding"), "5.0.0", CATEGORY_BLOBS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_compensateOnDuplicateKeyUpdateCounts, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.compensateOnDuplicateKeyUpdateCounts"), "5.1.7", CATEGORY_PREPARED_STATEMENTS,
                        Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_connectionCollation, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionCollation"), "3.0.13", CATEGORY_SESSION, 7),

                new StringPropertyDefinition(PNAME_connectionLifecycleInterceptors, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionLifecycleInterceptors"), "5.1.4", CATEGORY_CONNECTION, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_connectTimeout, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.connectTimeout"), "3.0.1",
                        CATEGORY_NETWORK, 9, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_continueBatchOnError, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.continueBatchOnError"), "3.0.3", CATEGORY_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_createDatabaseIfNotExist, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.createDatabaseIfNotExist"), "3.1.9", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_defaultFetchSize, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.defaultFetchSize"),
                        "3.1.9", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useServerPrepStmts, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useServerPrepStmts"), "3.1.0", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_dontTrackOpenResources, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.dontTrackOpenResources"), "3.1.7", CATEGORY_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_dumpQueriesOnException, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.dumpQueriesOnException"), "3.1.3", CATEGORY_EXCEPTIONS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_elideSetAutoCommits, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.eliseSetAutoCommit"), "3.1.3", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_emptyStringsConvertToZero, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.emptyStringsConvertToZero"), "3.1.8", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_emulateLocators, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.emulateLocators"), "3.1.0", CATEGORY_BLOBS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_emulateUnsupportedPstmts, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.emulateUnsupportedPstmts"), "3.1.7", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_enablePacketDebug, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.enablePacketDebug"), "3.1.3", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_enableQueryTimeouts, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.enableQueryTimeouts"), "5.0.6", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_explainSlowQueries, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.explainSlowQueries"), "3.1.2", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_exceptionInterceptors, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.exceptionInterceptors"), "5.1.8", CATEGORY_EXCEPTIONS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_failOverReadOnly, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.failoverReadOnly"), "3.0.12", CATEGORY_HA, 2),

                new BooleanPropertyDefinition(PNAME_gatherPerfMetrics, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.gatherPerfMetrics"), "3.1.2", CATEGORY_DEBUGING_PROFILING, 1),

                new BooleanPropertyDefinition(PNAME_generateSimpleParameterMetadata, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.generateSimpleParameterMetadata"), "5.0.5", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_holdResultsOpenOverStatementClose, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.holdRSOpenOverStmtClose"), "3.1.7", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_includeInnodbStatusInDeadlockExceptions, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.includeInnodbStatusInDeadlockExceptions"), "5.0.7", CATEGORY_EXCEPTIONS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_includeThreadDumpInDeadlockExceptions, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.includeThreadDumpInDeadlockExceptions"), "5.1.15", CATEGORY_EXCEPTIONS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_includeThreadNamesAsStatementComment, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.includeThreadNamesAsStatementComment"), "5.1.15", CATEGORY_EXCEPTIONS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_ignoreNonTxTables, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.ignoreNonTxTables"), "3.0.9", CATEGORY_EXCEPTIONS, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_initialTimeout, 2, RUNTIME_NOT_MODIFIABLE, Messages.getString("ConnectionProperties.initialTimeout"), "1.1",
                        CATEGORY_HA, 5, 1, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_interactiveClient, DEFAULT_VALUE_FALSE, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.interactiveClient"), "3.1.0", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_jdbcCompliantTruncation, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.jdbcCompliantTruncation"), "3.1.2", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new MemorySizePropertyDefinition(PNAME_largeRowSizeThreshold, 2048, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.largeRowSizeThreshold"), "5.1.1", CATEGORY_PERFORMANCE, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_loadBalanceStrategy, "random", RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceStrategy"), "5.0.6", CATEGORY_HA, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_loadBalanceBlacklistTimeout, 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceBlacklistTimeout"), "5.1.0", CATEGORY_HA, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_loadBalancePingTimeout, 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalancePingTimeout"), "5.1.13", CATEGORY_HA, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_loadBalanceValidateConnectionOnSwapServer, false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceValidateConnectionOnSwapServer"), "5.1.13", CATEGORY_HA, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_loadBalanceConnectionGroup, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceConnectionGroup"), "5.1.13", CATEGORY_HA, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_loadBalanceExceptionChecker, StandardLoadBalanceExceptionChecker.class.getName(), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceExceptionChecker"), "5.1.13", CATEGORY_HA, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_loadBalanceSQLStateFailover, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceSQLStateFailover"), "5.1.13", CATEGORY_HA, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_loadBalanceSQLExceptionSubclassFailover, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceSQLExceptionSubclassFailover"), "5.1.13", CATEGORY_HA, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_loadBalanceAutoCommitStatementRegex, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceAutoCommitStatementRegex"), "5.1.15", CATEGORY_HA, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_loadBalanceAutoCommitStatementThreshold, 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceAutoCommitStatementThreshold"), "5.1.15", CATEGORY_HA, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_localSocketAddress, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.localSocketAddress"), "5.0.5", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new MemorySizePropertyDefinition(PNAME_locatorFetchBufferSize, 1024 * 1024, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.locatorFetchBufferSize"), "3.2.1", CATEGORY_BLOBS, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_logger, STANDARD_LOGGER_NAME, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.logger", new Object[] { Log.class.getName(), STANDARD_LOGGER_NAME }), "3.1.1",
                        CATEGORY_DEBUGING_PROFILING, 0),

                new BooleanPropertyDefinition(PNAME_logSlowQueries, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.logSlowQueries"), "3.1.2", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_logXaCommands, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.logXaCommands"), "5.0.5", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_maintainTimeStats, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.maintainTimeStats"), "3.1.9", CATEGORY_PERFORMANCE, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_maxQuerySizeToLog, 2048, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.maxQuerySizeToLog"),
                        "3.1.3", CATEGORY_DEBUGING_PROFILING, 4, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_maxReconnects, 3, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.maxReconnects"), "1.1",
                        CATEGORY_HA, 4, 1, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_retriesAllDown, 120, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.retriesAllDown"), "5.1.6",
                        CATEGORY_HA, 4, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_maxRows, -1, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.maxRows"),
                        Messages.getString("ConnectionProperties.allVersions"), CATEGORY_RESULT_SETS, Integer.MIN_VALUE, -1, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_metadataCacheSize, 50, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.metadataCacheSize"),
                        "3.1.1", CATEGORY_PERFORMANCE, 5, 1, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_netTimeoutForStreamingResults, 600, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.netTimeoutForStreamingResults"), "5.1.0", CATEGORY_RESULT_SETS, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_noAccessToProcedureBodies, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.noAccessToProcedureBodies"), "5.0.3", CATEGORY_METADATA, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_noDatetimeStringSync, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.noDatetimeStringSync"), "3.1.7", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_nullCatalogMeansCurrent, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.nullCatalogMeansCurrent"), "3.1.8", CATEGORY_METADATA, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_nullNamePatternMatchesAll, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.nullNamePatternMatchesAll"), "3.1.8", CATEGORY_METADATA, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_packetDebugBufferSize, 20, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.packetDebugBufferSize"), "3.1.3", CATEGORY_DEBUGING_PROFILING, 7, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_padCharsWithSpace, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.padCharsWithSpace"), "5.0.6", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_pedantic, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.pedantic"),
                        "3.0.0", CATEGORY_JDBC, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_pinGlobalTxToPhysicalConnection, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.pinGlobalTxToPhysicalConnection"), "5.0.1", CATEGORY_HA, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_populateInsertRowWithDefaultValues, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.populateInsertRowWithDefaultValues"), "5.0.5", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_prepStmtCacheSize, 25, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.prepStmtCacheSize"),
                        "3.0.10", CATEGORY_PERFORMANCE, 10, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_prepStmtCacheSqlLimit, 256, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.prepStmtCacheSqlLimit"), "3.0.10", CATEGORY_PERFORMANCE, 11, 1, Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_parseInfoCacheFactory, PerConnectionLRUFactory.class.getName(), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.parseInfoCacheFactory"), "5.1.1", CATEGORY_PERFORMANCE, 12),

                new BooleanPropertyDefinition(PNAME_processEscapeCodesForPrepStmts, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.processEscapeCodesForPrepStmts"), "3.1.12", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_profileSQL, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.profileSQL"),
                        "3.1.0", CATEGORY_DEBUGING_PROFILING, 1),

                new StringPropertyDefinition(PNAME_propertiesTransform, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionPropertiesTransform"), "3.1.4", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_queriesBeforeRetryMaster, 50, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.queriesBeforeRetryMaster"), "3.0.2", CATEGORY_HA, 7, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_queryTimeoutKillsConnection, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.queryTimeoutKillsConnection"), "5.1.9", CATEGORY_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_reconnectAtTxEnd, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.reconnectAtTxEnd"), "3.0.10", CATEGORY_HA, 4),

                new IntegerPropertyDefinition(PNAME_reportMetricsIntervalMillis, 30000, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.reportMetricsIntervalMillis"), "3.1.2", CATEGORY_DEBUGING_PROFILING, 3, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_requireSSL, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.requireSSL"),
                        "3.1.0", CATEGORY_SECURITY, 3),

                new StringPropertyDefinition(PNAME_resourceId, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.resourceId"), "5.0.1", CATEGORY_HA, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_resultSetSizeThreshold, 100, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.resultSetSizeThreshold"), "5.0.5", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_rewriteBatchedStatements, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.rewriteBatchedStatements"), "3.1.13", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_rollbackOnPooledClose, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.rollbackOnPooledClose"), "3.0.15", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_secondsBeforeRetryMaster, 30, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.secondsBeforeRetryMaster"), "3.0.2", CATEGORY_HA, 8, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_selfDestructOnPingSecondsLifetime, 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.selfDestructOnPingSecondsLifetime"), "5.1.6", CATEGORY_HA, Integer.MAX_VALUE, 0,
                        Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_selfDestructOnPingMaxOperations, 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.selfDestructOnPingMaxOperations"), "5.1.6", CATEGORY_HA, Integer.MAX_VALUE, 0,
                        Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_ha_enableJMX, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.ha.enableJMX"), "5.1.27", CATEGORY_HA, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_loadBalanceHostRemovalGracePeriod, 15000, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceHostRemovalGracePeriod"), "6.0.3", CATEGORY_HA, Integer.MAX_VALUE, 0,
                        Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_serverTimezone, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.serverTimezone"), "3.0.2", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_sessionVariables, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.sessionVariables"), "3.1.8", CATEGORY_SESSION, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_slowQueryThresholdMillis, 2000, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.slowQueryThresholdMillis"), "3.1.2", CATEGORY_DEBUGING_PROFILING, 9, 0, Integer.MAX_VALUE),

                new LongPropertyDefinition(PNAME_slowQueryThresholdNanos, 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.slowQueryThresholdNanos"), "5.0.7", CATEGORY_DEBUGING_PROFILING, 10),

                new StringPropertyDefinition(PNAME_socketFactory, StandardSocketFactory.class.getName(), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.socketFactory"), "3.0.3", CATEGORY_NETWORK, 4),

                new StringPropertyDefinition(PNAME_socksProxyHost, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.socksProxyHost"), "5.1.34", CATEGORY_NETWORK, 1),

                new IntegerPropertyDefinition(PNAME_socksProxyPort, SocksProxySocketFactory.SOCKS_DEFAULT_PORT, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.socksProxyPort"), "5.1.34", CATEGORY_NETWORK, 2, 0, 65535),

                new IntegerPropertyDefinition(PNAME_socketTimeout, 0, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.socketTimeout"), "3.0.1",
                        CATEGORY_NETWORK, 10, 0, Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_statementInterceptors, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.statementInterceptors"), "5.1.1", CATEGORY_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_strictUpdates, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.strictUpdates"), "3.0.4", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_overrideSupportsIntegrityEnhancementFacility, false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.overrideSupportsIEF"), "3.1.12", CATEGORY_INTEGRATION, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_tcpNoDelay, Boolean.valueOf(StandardSocketFactory.TCP_NO_DELAY_DEFAULT_VALUE).booleanValue(),
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.tcpNoDelay"), "5.0.7", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_tcpKeepAlive, Boolean.valueOf(StandardSocketFactory.TCP_KEEP_ALIVE_DEFAULT_VALUE).booleanValue(),
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.tcpKeepAlive"), "5.0.7", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_tcpRcvBuf, Integer.parseInt(StandardSocketFactory.TCP_RCV_BUF_DEFAULT_VALUE), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.tcpSoRcvBuf"), "5.0.7", CATEGORY_NETWORK, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_tcpSndBuf, Integer.parseInt(StandardSocketFactory.TCP_SND_BUF_DEFAULT_VALUE), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.tcpSoSndBuf"), "5.0.7", CATEGORY_NETWORK, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_tcpTrafficClass, Integer.parseInt(StandardSocketFactory.TCP_TRAFFIC_CLASS_DEFAULT_VALUE),
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.tcpTrafficClass"), "5.0.7", CATEGORY_NETWORK, Integer.MIN_VALUE, 0, 255),

                new BooleanPropertyDefinition(PNAME_tinyInt1isBit, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.tinyInt1isBit"), "3.0.16", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_traceProtocol, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.traceProtocol"), "3.1.2", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_treatUtilDateAsTimestamp, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.treatUtilDateAsTimestamp"), "5.0.5", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_transformedBitIsBoolean, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.transformedBitIsBoolean"), "3.1.9", CATEGORY_RESULT_SETS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useCompression, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useCompression"), "3.0.17", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useColumnNamesInFindColumn, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useColumnNamesInFindColumn"), "5.1.7", CATEGORY_JDBC, Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_useConfigs, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useConfigs"), "3.1.5", CATEGORY_CONNECTION, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_useCursorFetch, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useCursorFetch"), "5.0.0", CATEGORY_PERFORMANCE, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_useHostsInPrivileges, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useHostsInPrivileges"), "3.0.2", CATEGORY_METADATA, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useInformationSchema, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useInformationSchema"), "5.0.0", CATEGORY_METADATA, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useLocalSessionState, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useLocalSessionState"), "3.1.7", CATEGORY_PERFORMANCE, 5),

                new BooleanPropertyDefinition(PNAME_useLocalTransactionState, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useLocalTransactionState"), "5.1.7", CATEGORY_PERFORMANCE, 6),

                new BooleanPropertyDefinition(PNAME_sendFractionalSeconds, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.sendFractionalSeconds"), "5.1.37", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useNanosForElapsedTime, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useNanosForElapsedTime"), "5.0.7", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useOldAliasMetadataBehavior, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useOldAliasMetadataBehavior"), "5.0.4", CATEGORY_JDBC, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useOldUTF8Behavior, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useOldUtf8Behavior"), "3.1.6", CATEGORY_SESSION, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useOnlyServerErrorMessages, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useOnlyServerErrorMessages"), "3.0.15", CATEGORY_DEBUGING_PROFILING, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useReadAheadInput, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useReadAheadInput"), "3.1.5", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useSSL, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.useSSL"), "3.0.2",
                        CATEGORY_SECURITY, 2),

                new BooleanPropertyDefinition(PNAME_useStreamLengthsInPrepStmts, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useStreamLengthsInPrepStmts"), "3.0.2", CATEGORY_PREPARED_STATEMENTS, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_ultraDevHack, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.ultraDevHack"), "2.0.3", CATEGORY_INTEGRATION, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useUsageAdvisor, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useUsageAdvisor"), "3.1.1", CATEGORY_DEBUGING_PROFILING, 10),

                new BooleanPropertyDefinition(PNAME_yearIsDateType, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.yearIsDateType"), "3.1.9", CATEGORY_DATETIMES, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_zeroDateTimeBehavior, ZERO_DATETIME_BEHAVIOR_EXCEPTION, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.zeroDateTimeBehavior",
                                new Object[] { ZERO_DATETIME_BEHAVIOR_EXCEPTION, ZERO_DATETIME_BEHAVIOR_ROUND, ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL }),
                        "3.1.4", CATEGORY_DATETIMES, Integer.MIN_VALUE,
                        new String[] { ZERO_DATETIME_BEHAVIOR_EXCEPTION, ZERO_DATETIME_BEHAVIOR_ROUND, ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL }),

                new BooleanPropertyDefinition(PNAME_useAffectedRows, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useAffectedRows"), "5.1.7", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_maxAllowedPacket, 65535, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.maxAllowedPacket"),
                        "5.1.8", CATEGORY_NETWORK, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_authenticationPlugins, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.authenticationPlugins"), "5.1.19", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_disabledAuthenticationPlugins, DEFAULT_VALUE_NULL_STRING, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.disabledAuthenticationPlugins"), "5.1.19", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_defaultAuthenticationPlugin, MysqlNativePasswordPlugin.class.getName(), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.defaultAuthenticationPlugin"), "5.1.19", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_disconnectOnExpiredPasswords, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.disconnectOnExpiredPasswords"), "5.1.23", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_getProceduresReturnsFunctions, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.getProceduresReturnsFunctions"), "5.1.26", CATEGORY_METADATA, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_detectCustomCollations, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.detectCustomCollations"), "5.1.29", CATEGORY_CONNECTION, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_dontCheckOnDuplicateKeyUpdateInSQL, DEFAULT_VALUE_FALSE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.dontCheckOnDuplicateKeyUpdateInSQL"), "5.1.32", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_readOnlyPropagatesToServer, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.readOnlyPropagatesToServer"), "5.1.35", CATEGORY_PERFORMANCE, Integer.MIN_VALUE),

                // TODO improve MySQLx properties descriptions

                new BooleanPropertyDefinition(PNAME_useAsyncProtocol, DEFAULT_VALUE_TRUE, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useAsyncProtocol"), "6.0.0", CATEGORY_MYSQLX, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_enableEscapeProcessing, DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.enableEscapeProcessing"), "6.0.1", CATEGORY_PERFORMANCE, Integer.MIN_VALUE) };

        HashMap<String, PropertyDefinition<?>> propertyNameToPropertyDefinitionMap = new HashMap<String, PropertyDefinition<?>>();
        for (PropertyDefinition<?> pdef : pdefs) {
            String pname = pdef.getName();
            propertyNameToPropertyDefinitionMap.put(pname, pdef);
        }
        PROPERTY_NAME_TO_PROPERTY_DEFINITION = Collections.unmodifiableMap(propertyNameToPropertyDefinitionMap);

        Map<String, String> aliases = new HashMap<String, String>() {
            private static final long serialVersionUID = -3808474490779118979L;

            {
                put(PNAME_paranoid, "paranoid");
                put(PNAME_passwordCharacterEncoding, "passwordCharacterEncoding");
                put(PNAME_serverRSAPublicKeyFile, "serverRSAPublicKeyFile");
                put(PNAME_allowPublicKeyRetrieval, "allowPublicKeyRetrieval");
                put(PNAME_clientCertificateKeyStoreUrl, "clientCertificateKeyStoreUrl");
                put(PNAME_trustCertificateKeyStoreUrl, "trustCertificateKeyStoreUrl");
                put(PNAME_clientCertificateKeyStoreType, "clientCertificateKeyStoreType");
                put(PNAME_clientCertificateKeyStorePassword, "clientCertificateKeyStorePassword");
                put(PNAME_trustCertificateKeyStoreType, "trustCertificateKeyStoreType");
                put(PNAME_trustCertificateKeyStorePassword, "trustCertificateKeyStorePassword");
                put(PNAME_verifyServerCertificate, "verifyServerCertificate");
                put(PNAME_enabledSSLCipherSuites, "enabledSSLCipherSuites");
                put(PNAME_useUnbufferedInput, "useUnbufferedInput");
                put(PNAME_profilerEventHandler, "profilerEventHandler");
                put(PNAME_allowLoadLocalInfile, "allowLoadLocalInfile");
                put(PNAME_allowMultiQueries, "allowMultiQueries");
                put(PNAME_allowNanAndInf, "allowNanAndInf");
                put(PNAME_allowUrlInLocalInfile, "allowUrlInLocalInfile");
                put(PNAME_alwaysSendSetIsolation, "alwaysSendSetIsolation");
                put(PNAME_autoClosePStmtStreams, "autoClosePStmtStreams");
                put(PNAME_allowMasterDownConnections, "allowMasterDownConnections");
                put(PNAME_allowSlaveDownConnections, "allowSlaveDownConnections");
                put(PNAME_readFromMasterWhenNoSlaves, "readFromMasterWhenNoSlaves");
                put(PNAME_autoDeserialize, "autoDeserialize");
                put(PNAME_autoGenerateTestcaseScript, "autoGenerateTestcaseScript");
                put(PNAME_autoReconnect, "autoReconnect");
                put(PNAME_autoReconnectForPools, "autoReconnectForPools");
                put(PNAME_blobSendChunkSize, "blobSendChunkSize");
                put(PNAME_autoSlowLog, "autoSlowLog");
                put(PNAME_blobsAreStrings, "blobsAreStrings");
                put(PNAME_functionsNeverReturnBlobs, "functionsNeverReturnBlobs");
                put(PNAME_cacheCallableStmts, "cacheCallableStmts");
                put(PNAME_cachePrepStmts, "cachePrepStmts");
                put(PNAME_cacheResultSetMetadata, "cacheResultSetMetadata");
                put(PNAME_serverConfigCacheFactory, "serverConfigCacheFactory");
                put(PNAME_cacheServerConfiguration, "cacheServerConfiguration");
                put(PNAME_callableStmtCacheSize, "callableStmtCacheSize");
                put(PNAME_characterEncoding, "characterEncoding");
                put(PNAME_characterSetResults, "characterSetResults");
                put(PNAME_connectionAttributes, "connectionAttributes");
                put(PNAME_clientInfoProvider, "clientInfoProvider");
                put(PNAME_clobberStreamingResults, "clobberStreamingResults");
                put(PNAME_clobCharacterEncoding, "clobCharacterEncoding");
                put(PNAME_compensateOnDuplicateKeyUpdateCounts, "compensateOnDuplicateKeyUpdateCounts");
                put(PNAME_connectionCollation, "connectionCollation");
                put(PNAME_connectionLifecycleInterceptors, "connectionLifecycleInterceptors");
                put(PNAME_connectTimeout, "connectTimeout");
                put(PNAME_continueBatchOnError, "continueBatchOnError");
                put(PNAME_createDatabaseIfNotExist, "createDatabaseIfNotExist");
                put(PNAME_defaultFetchSize, "defaultFetchSize");
                put(PNAME_useServerPrepStmts, "useServerPrepStmts");
                put(PNAME_dontTrackOpenResources, "dontTrackOpenResources");
                put(PNAME_dumpQueriesOnException, "dumpQueriesOnException");
                put(PNAME_elideSetAutoCommits, "elideSetAutoCommits");
                put(PNAME_emptyStringsConvertToZero, "emptyStringsConvertToZero");
                put(PNAME_emulateLocators, "emulateLocators");
                put(PNAME_emulateUnsupportedPstmts, "emulateUnsupportedPstmts");
                put(PNAME_enablePacketDebug, "enablePacketDebug");
                put(PNAME_enableQueryTimeouts, "enableQueryTimeouts");
                put(PNAME_explainSlowQueries, "explainSlowQueries");
                put(PNAME_exceptionInterceptors, "exceptionInterceptors");
                put(PNAME_failOverReadOnly, "failOverReadOnly");

                put(PNAME_gatherPerfMetrics, "gatherPerfMetrics");
                put(PNAME_generateSimpleParameterMetadata, "generateSimpleParameterMetadata");
                put(PNAME_holdResultsOpenOverStatementClose, "holdResultsOpenOverStatementClose");
                put(PNAME_includeInnodbStatusInDeadlockExceptions, "includeInnodbStatusInDeadlockExceptions");
                put(PNAME_includeThreadDumpInDeadlockExceptions, "includeThreadDumpInDeadlockExceptions");
                put(PNAME_includeThreadNamesAsStatementComment, "includeThreadNamesAsStatementComment");
                put(PNAME_ignoreNonTxTables, "ignoreNonTxTables");
                put(PNAME_initialTimeout, "initialTimeout");
                put(PNAME_interactiveClient, "interactiveClient");
                put(PNAME_jdbcCompliantTruncation, "jdbcCompliantTruncation");
                put(PNAME_largeRowSizeThreshold, "largeRowSizeThreshold");
                put(PNAME_loadBalanceStrategy, "loadBalanceStrategy");
                put(PNAME_loadBalanceBlacklistTimeout, "loadBalanceBlacklistTimeout");
                put(PNAME_loadBalancePingTimeout, "loadBalancePingTimeout");
                put(PNAME_loadBalanceValidateConnectionOnSwapServer, "loadBalanceValidateConnectionOnSwapServer");
                put(PNAME_loadBalanceConnectionGroup, "loadBalanceConnectionGroup");
                put(PNAME_loadBalanceExceptionChecker, "loadBalanceExceptionChecker");
                put(PNAME_loadBalanceSQLStateFailover, "loadBalanceSQLStateFailover");
                put(PNAME_loadBalanceSQLExceptionSubclassFailover, "loadBalanceSQLExceptionSubclassFailover");
                put(PNAME_loadBalanceAutoCommitStatementRegex, "loadBalanceAutoCommitStatementRegex");
                put(PNAME_loadBalanceAutoCommitStatementThreshold, "loadBalanceAutoCommitStatementThreshold");
                put(PNAME_localSocketAddress, "localSocketAddress");
                put(PNAME_locatorFetchBufferSize, "locatorFetchBufferSize");
                put(PNAME_logger, "logger");

                put(PNAME_logSlowQueries, "logSlowQueries");
                put(PNAME_logXaCommands, "logXaCommands");
                put(PNAME_maintainTimeStats, "maintainTimeStats");
                put(PNAME_maxQuerySizeToLog, "maxQuerySizeToLog");
                put(PNAME_maxReconnects, "maxReconnects");
                put(PNAME_retriesAllDown, "retriesAllDown");
                put(PNAME_maxRows, "maxRows");
                put(PNAME_metadataCacheSize, "metadataCacheSize");
                put(PNAME_netTimeoutForStreamingResults, "netTimeoutForStreamingResults");
                put(PNAME_noAccessToProcedureBodies, "noAccessToProcedureBodies");
                put(PNAME_noDatetimeStringSync, "noDatetimeStringSync");
                put(PNAME_nullCatalogMeansCurrent, "nullCatalogMeansCurrent");
                put(PNAME_nullNamePatternMatchesAll, "nullNamePatternMatchesAll");
                put(PNAME_packetDebugBufferSize, "packetDebugBufferSize");
                put(PNAME_padCharsWithSpace, "padCharsWithSpace");
                put(PNAME_pedantic, "pedantic");
                put(PNAME_pinGlobalTxToPhysicalConnection, "pinGlobalTxToPhysicalConnection");
                put(PNAME_populateInsertRowWithDefaultValues, "populateInsertRowWithDefaultValues");
                put(PNAME_prepStmtCacheSize, "prepStmtCacheSize");
                put(PNAME_prepStmtCacheSqlLimit, "prepStmtCacheSqlLimit");
                put(PNAME_parseInfoCacheFactory, "parseInfoCacheFactory");
                put(PNAME_processEscapeCodesForPrepStmts, "processEscapeCodesForPrepStmts");
                put(PNAME_profileSQL, "profileSQL");
                put(PNAME_propertiesTransform, "propertiesTransform");
                put(PNAME_queriesBeforeRetryMaster, "queriesBeforeRetryMaster");
                put(PNAME_queryTimeoutKillsConnection, "queryTimeoutKillsConnection");
                put(PNAME_reconnectAtTxEnd, "reconnectAtTxEnd");
                put(PNAME_reportMetricsIntervalMillis, "reportMetricsIntervalMillis");
                put(PNAME_requireSSL, "requireSSL");
                put(PNAME_resourceId, "resourceId");
                put(PNAME_resultSetSizeThreshold, "resultSetSizeThreshold");
                put(PNAME_rewriteBatchedStatements, "rewriteBatchedStatements");
                put(PNAME_rollbackOnPooledClose, "rollbackOnPooledClose");
                put(PNAME_secondsBeforeRetryMaster, "secondsBeforeRetryMaster");
                put(PNAME_selfDestructOnPingSecondsLifetime, "selfDestructOnPingSecondsLifetime");
                put(PNAME_selfDestructOnPingMaxOperations, "selfDestructOnPingMaxOperations");
                put(PNAME_ha_enableJMX, "replicationEnableJMX");
                put(PNAME_loadBalanceHostRemovalGracePeriod, "loadBalanceHostRemovalGracePeriod");
                put(PNAME_serverTimezone, "serverTimezone");
                put(PNAME_sessionVariables, "sessionVariables");
                put(PNAME_slowQueryThresholdMillis, "slowQueryThresholdMillis");
                put(PNAME_slowQueryThresholdNanos, "slowQueryThresholdNanos");
                put(PNAME_socketFactory, "socketFactory");
                put(PNAME_socksProxyHost, "socksProxyHost");
                put(PNAME_socksProxyPort, "socksProxyPort");
                put(PNAME_socketTimeout, "socketTimeout");
                put(PNAME_statementInterceptors, "statementInterceptors");
                put(PNAME_strictUpdates, "strictUpdates");
                put(PNAME_overrideSupportsIntegrityEnhancementFacility, "overrideSupportsIntegrityEnhancementFacility");
                put(PNAME_tcpNoDelay, "tcpNoDelay");
                put(PNAME_tcpKeepAlive, "tcpKeepAlive");
                put(PNAME_tcpRcvBuf, "tcpRcvBuf");
                put(PNAME_tcpSndBuf, "tcpSndBuf");
                put(PNAME_tcpTrafficClass, "tcpTrafficClass");
                put(PNAME_tinyInt1isBit, "tinyInt1isBit");
                put(PNAME_traceProtocol, "traceProtocol");
                put(PNAME_treatUtilDateAsTimestamp, "treatUtilDateAsTimestamp");
                put(PNAME_transformedBitIsBoolean, "transformedBitIsBoolean");
                put(PNAME_useCompression, "useCompression");
                put(PNAME_useColumnNamesInFindColumn, "useColumnNamesInFindColumn");
                put(PNAME_useConfigs, "useConfigs");
                put(PNAME_useCursorFetch, "useCursorFetch");
                put(PNAME_useHostsInPrivileges, "useHostsInPrivileges");
                put(PNAME_useInformationSchema, "useInformationSchema");
                put(PNAME_useLocalSessionState, "useLocalSessionState");
                put(PNAME_useLocalTransactionState, "useLocalTransactionState");
                put(PNAME_sendFractionalSeconds, "sendFractionalSeconds");
                put(PNAME_useNanosForElapsedTime, "useNanosForElapsedTime");
                put(PNAME_useOldAliasMetadataBehavior, "useOldAliasMetadataBehavior");
                put(PNAME_useOldUTF8Behavior, "useOldUTF8Behavior");
                put(PNAME_useOnlyServerErrorMessages, "useOnlyServerErrorMessages");
                put(PNAME_useReadAheadInput, "useReadAheadInput");
                put(PNAME_useSSL, "useSSL");
                put(PNAME_useStreamLengthsInPrepStmts, "useStreamLengthsInPrepStmts");
                put(PNAME_ultraDevHack, "ultraDevHack");
                put(PNAME_useUsageAdvisor, "useUsageAdvisor");
                put(PNAME_yearIsDateType, "yearIsDateType");
                put(PNAME_zeroDateTimeBehavior, "zeroDateTimeBehavior");
                put(PNAME_useAffectedRows, "useAffectedRows");
                put(PNAME_maxAllowedPacket, "maxAllowedPacket");
                put(PNAME_authenticationPlugins, "authenticationPlugins");
                put(PNAME_disabledAuthenticationPlugins, "disabledAuthenticationPlugins");
                put(PNAME_defaultAuthenticationPlugin, "defaultAuthenticationPlugin");
                put(PNAME_disconnectOnExpiredPasswords, "disconnectOnExpiredPasswords");
                put(PNAME_getProceduresReturnsFunctions, "getProceduresReturnsFunctions");
                put(PNAME_detectCustomCollations, "detectCustomCollations");
                put(PNAME_dontCheckOnDuplicateKeyUpdateInSQL, "dontCheckOnDuplicateKeyUpdateInSQL");
                put(PNAME_readOnlyPropagatesToServer, "readOnlyPropagatesToServer");
            }
        };
        PROPERTY_NAME_TO_ALIAS = Collections.unmodifiableMap(aliases);

    }

    public static PropertyDefinition<?> getPropertyDefinition(String propertyName) {
        return PROPERTY_NAME_TO_PROPERTY_DEFINITION.get(propertyName);
    }

    public static RuntimeProperty<?> createRuntimeProperty(String propertyName) {
        PropertyDefinition<?> pdef = getPropertyDefinition(propertyName);
        if (pdef != null) {
            return pdef.createRuntimeProperty();
        }
        // TODO improve message
        throw ExceptionFactory.createException(WrongArgumentException.class, "Connection property definition is not found for '" + propertyName + "'");
    }

    static class XmlMap {
        protected Map<Integer, Map<String, PropertyDefinition<?>>> ordered = new TreeMap<Integer, Map<String, PropertyDefinition<?>>>();
        protected Map<String, PropertyDefinition<?>> alpha = new TreeMap<String, PropertyDefinition<?>>();
    }

    /**
     * Returns a description of the connection properties as an XML document.
     * 
     * @return the connection properties as an XML document.
     */
    public static String exposeAsXml() {
        StringBuilder xmlBuf = new StringBuilder();
        xmlBuf.append("<ConnectionProperties>");

        int numCategories = PROPERTY_CATEGORIES.length;

        Map<String, XmlMap> propertyListByCategory = new HashMap<String, XmlMap>();

        for (int i = 0; i < numCategories; i++) {
            propertyListByCategory.put(PROPERTY_CATEGORIES[i], new XmlMap());
        }

        //
        // The following properties are not exposed as 'normal' properties, but they are settable nonetheless, so we need to have them documented, make sure
        // that they sort 'first' as #1 and #2 in the category
        //
        StringPropertyDefinition userDef = new StringPropertyDefinition(PropertyDefinitions.PNAME_user, DEFAULT_VALUE_NULL_STRING, RUNTIME_NOT_MODIFIABLE,
                Messages.getString("ConnectionProperties.Username"), Messages.getString("ConnectionProperties.allVersions"), CATEGORY_AUTH,
                Integer.MIN_VALUE + 1);

        StringPropertyDefinition passwordDef = new StringPropertyDefinition(PropertyDefinitions.PNAME_password, DEFAULT_VALUE_NULL_STRING,
                RUNTIME_NOT_MODIFIABLE, Messages.getString("ConnectionProperties.Password"), Messages.getString("ConnectionProperties.allVersions"),
                CATEGORY_AUTH, Integer.MIN_VALUE + 2);

        XmlMap connectionSortMaps = propertyListByCategory.get(CATEGORY_AUTH);
        TreeMap<String, PropertyDefinition<?>> userMap = new TreeMap<String, PropertyDefinition<?>>();
        userMap.put(userDef.getName(), userDef);

        connectionSortMaps.ordered.put(Integer.valueOf(userDef.getOrder()), userMap);

        TreeMap<String, PropertyDefinition<?>> passwordMap = new TreeMap<String, PropertyDefinition<?>>();
        passwordMap.put(passwordDef.getName(), passwordDef);

        connectionSortMaps.ordered.put(new Integer(passwordDef.getOrder()), passwordMap);

        for (PropertyDefinition<?> pdef : PROPERTY_NAME_TO_PROPERTY_DEFINITION.values()) {
            XmlMap sortMaps = propertyListByCategory.get(pdef.getCategory());
            int orderInCategory = pdef.getOrder();

            if (orderInCategory == Integer.MIN_VALUE) {
                sortMaps.alpha.put(pdef.getName(), pdef);
            } else {
                Integer order = Integer.valueOf(orderInCategory);
                Map<String, PropertyDefinition<?>> orderMap = sortMaps.ordered.get(order);

                if (orderMap == null) {
                    orderMap = new TreeMap<String, PropertyDefinition<?>>();
                    sortMaps.ordered.put(order, orderMap);
                }

                orderMap.put(pdef.getName(), pdef);
            }
        }

        for (int j = 0; j < numCategories; j++) {
            XmlMap sortMaps = propertyListByCategory.get(PROPERTY_CATEGORIES[j]);

            xmlBuf.append("\n <PropertyCategory name=\"");
            xmlBuf.append(PROPERTY_CATEGORIES[j]);
            xmlBuf.append("\">");

            for (Map<String, PropertyDefinition<?>> orderedEl : sortMaps.ordered.values()) {
                for (PropertyDefinition<?> pdef : orderedEl.values()) {
                    xmlBuf.append("\n  <Property name=\"");
                    xmlBuf.append(pdef.getName());

                    xmlBuf.append("\" default=\"");
                    if (pdef.getDefaultValue() != null) {
                        xmlBuf.append(pdef.getDefaultValue());
                    }
                    xmlBuf.append("\" sortOrder=\"");
                    xmlBuf.append(pdef.getOrder());
                    xmlBuf.append("\" since=\"");
                    xmlBuf.append(pdef.getSinceVersion());
                    xmlBuf.append("\">\n");
                    xmlBuf.append("    ");
                    String escapedDescription = pdef.getDescription();
                    escapedDescription = escapedDescription.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");

                    xmlBuf.append(escapedDescription);
                    xmlBuf.append("\n  </Property>");
                }
            }

            for (PropertyDefinition<?> pdef : sortMaps.alpha.values()) {
                xmlBuf.append("\n  <Property name=\"");
                xmlBuf.append(pdef.getName());

                xmlBuf.append("\" default=\"");
                if (pdef.getDefaultValue() != null) {
                    xmlBuf.append(pdef.getDefaultValue());
                }

                xmlBuf.append("\" sortOrder=\"alpha\" since=\"");
                xmlBuf.append(pdef.getSinceVersion());
                xmlBuf.append("\">\n");
                xmlBuf.append("    ");
                xmlBuf.append(pdef.getDescription());
                xmlBuf.append("\n  </Property>");
            }

            xmlBuf.append("\n </PropertyCategory>");
        }

        xmlBuf.append("\n</ConnectionProperties>");

        return xmlBuf.toString();
    }
}
