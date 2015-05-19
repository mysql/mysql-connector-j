/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.core.conf;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import com.mysql.cj.api.conf.PropertyDefinition;
import com.mysql.cj.api.conf.RuntimeProperty;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.authentication.MysqlNativePasswordPlugin;
import com.mysql.cj.core.io.SocksProxySocketFactory;
import com.mysql.cj.core.io.StandardSocketFactory;
import com.mysql.cj.core.log.StandardLogger;
import com.mysql.cj.core.profiler.LoggingProfilerEventHandler;
import com.mysql.cj.core.util.PerVmServerConfigCacheFactory;
import com.mysql.jdbc.NonRegisteringDriver;
import com.mysql.jdbc.ha.StandardLoadBalanceExceptionChecker;
import com.mysql.jdbc.util.PerConnectionLRUFactory;

public class PropertyDefinitions {
    public static final boolean DEFAULT_VALUE_TRUE = true;
    public static final boolean DEFAULT_VALUE_FALSE = false;

    // is modifiable in run-time
    public static final boolean RUNTIME_MODIFIABLE = true;

    // is not modifiable in run-time (will allow to set not-null value only once)
    public static final boolean RUNTIME_NOT_MODIFIABLE = false;

    //
    // Yes, this looks goofy, but we're trying to avoid intern()ing here
    //
    private static final String STANDARD_LOGGER_NAME = StandardLogger.class.getName();

    public static final String ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL = "convertToNull";

    public static final String ZERO_DATETIME_BEHAVIOR_EXCEPTION = "exception";

    public static final String ZERO_DATETIME_BEHAVIOR_ROUND = "round";

    public static final String CONNECTION_AND_AUTH_CATEGORY = Messages.getString("ConnectionProperties.categoryConnectionAuthentication");
    public static final String NETWORK_CATEGORY = Messages.getString("ConnectionProperties.categoryNetworking");
    public static final String SECURITY_CATEGORY = Messages.getString("ConnectionProperties.categorySecurity");
    public static final String DEBUGING_PROFILING_CATEGORY = Messages.getString("ConnectionProperties.categoryDebuggingProfiling");
    public static final String HA_CATEGORY = Messages.getString("ConnectionProperties.categorryHA");
    public static final String MISC_CATEGORY = Messages.getString("ConnectionProperties.categoryMisc");
    public static final String PERFORMANCE_CATEGORY = Messages.getString("ConnectionProperties.categoryPerformance");

    public static final String[] PROPERTY_CATEGORIES = new String[] { CONNECTION_AND_AUTH_CATEGORY, NETWORK_CATEGORY, HA_CATEGORY, SECURITY_CATEGORY,
            PERFORMANCE_CATEGORY, DEBUGING_PROFILING_CATEGORY, MISC_CATEGORY };

    public static final Map<String, PropertyDefinition<?>> PROPERTY_NAME_TO_PROPERTY_DEFINITION;

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
    public static final String PNAME_capitalizeTypeNames = "capitalizeTypeNames";
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
    public static final String PNAME_dynamicCalendars = "dynamicCalendars";
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
    public static final String PNAME_loadBalanceStrategy = "loadBalanceStrategy";
    public static final String PNAME_loadBalanceBlacklistTimeout = "loadBalanceBlacklistTimeout";
    public static final String PNAME_loadBalancePingTimeout = "loadBalancePingTimeout";
    public static final String PNAME_loadBalanceValidateConnectionOnSwapServer = "loadBalanceValidateConnectionOnSwapServer";
    public static final String PNAME_loadBalanceConnectionGroup = "loadBalanceConnectionGroup";
    public static final String PNAME_loadBalanceExceptionChecker = "loadBalanceExceptionChecker";
    public static final String PNAME_loadBalanceSQLStateFailover = "loadBalanceSQLStateFailover";
    public static final String PNAME_loadBalanceSQLExceptionSubclassFailover = "loadBalanceSQLExceptionSubclassFailover";
    public static final String PNAME_loadBalanceEnableJMX = "loadBalanceEnableJMX";
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
    public static final String PNAME_noTimezoneConversionForTimeType = "noTimezoneConversionForTimeType";
    public static final String PNAME_noTimezoneConversionForDateType = "noTimezoneConversionForDateType";
    public static final String PNAME_cacheDefaultTimezone = "cacheDefaultTimezone";
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
    public static final String PNAME_relaxAutoCommit = "relaxAutoCommit";
    public static final String PNAME_reportMetricsIntervalMillis = "reportMetricsIntervalMillis";
    public static final String PNAME_requireSSL = "requireSSL";
    public static final String PNAME_resourceId = "resourceId";
    public static final String PNAME_resultSetSizeThreshold = "resultSetSizeThreshold";
    public static final String PNAME_rewriteBatchedStatements = "rewriteBatchedStatements";
    public static final String PNAME_rollbackOnPooledClose = "rollbackOnPooledClose";
    public static final String PNAME_roundRobinLoadBalance = "roundRobinLoadBalance";
    public static final String PNAME_runningCTS13 = "runningCTS13";
    public static final String PNAME_secondsBeforeRetryMaster = "secondsBeforeRetryMaster";
    public static final String PNAME_selfDestructOnPingSecondsLifetime = "selfDestructOnPingSecondsLifetime";
    public static final String PNAME_selfDestructOnPingMaxOperations = "selfDestructOnPingMaxOperations";
    public static final String PNAME_replicationEnableJMX = "replicationEnableJMX";
    public static final String PNAME_serverTimezone = "serverTimezone";
    public static final String PNAME_sessionVariables = "sessionVariables";
    public static final String PNAME_slowQueryThresholdMillis = "slowQueryThresholdMillis";
    public static final String PNAME_slowQueryThresholdNanos = "slowQueryThresholdNanos";
    public static final String PNAME_socketFactory = "socketFactory";
    public static final String PNAME_socksProxyHost = "socksProxyHost";
    public static final String PNAME_socksProxyPort = "socksProxyPort";
    public static final String PNAME_socketTimeout = "socketTimeout";
    public static final String PNAME_statementInterceptors = "statementInterceptors";
    public static final String PNAME_strictFloatingPoint = "strictFloatingPoint";
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
    public static final String PNAME_useBlobToStoreUTF8OutsideBMP = "useBlobToStoreUTF8OutsideBMP";
    public static final String PNAME_utf8OutsideBmpExcludedColumnNamePattern = "utf8OutsideBmpExcludedColumnNamePattern";
    public static final String PNAME_utf8OutsideBmpIncludedColumnNamePattern = "utf8OutsideBmpIncludedColumnNamePattern";
    public static final String PNAME_useCompression = "useCompression";
    public static final String PNAME_useColumnNamesInFindColumn = "useColumnNamesInFindColumn";
    public static final String PNAME_useConfigs = "useConfigs";
    public static final String PNAME_useCursorFetch = "useCursorFetch";
    public static final String PNAME_useDynamicCharsetInfo = "useDynamicCharsetInfo";
    public static final String PNAME_useDirectRowUnpack = "useDirectRowUnpack";
    public static final String PNAME_useFastIntParsing = "useFastIntParsing";
    public static final String PNAME_useFastDateParsing = "useFastDateParsing";
    public static final String PNAME_useHostsInPrivileges = "useHostsInPrivileges";
    public static final String PNAME_useInformationSchema = "useInformationSchema";
    public static final String PNAME_useJDBCCompliantTimezoneShift = "useJDBCCompliantTimezoneShift";
    public static final String PNAME_useLocalSessionState = "useLocalSessionState";
    public static final String PNAME_useLocalTransactionState = "useLocalTransactionState";
    public static final String PNAME_useLegacyDatetimeCode = "useLegacyDatetimeCode";
    public static final String PNAME_useNanosForElapsedTime = "useNanosForElapsedTime";
    public static final String PNAME_useOldAliasMetadataBehavior = "useOldAliasMetadataBehavior";
    public static final String PNAME_useOldUTF8Behavior = "useOldUTF8Behavior";
    public static final String PNAME_useOnlyServerErrorMessages = "useOnlyServerErrorMessages";
    public static final String PNAME_useReadAheadInput = "useReadAheadInput";
    public static final String PNAME_useSSL = "useSSL";
    public static final String PNAME_useSSPSCompatibleTimezoneShift = "useSSPSCompatibleTimezoneShift";
    public static final String PNAME_useStreamLengthsInPrepStmts = "useStreamLengthsInPrepStmts";
    public static final String PNAME_useTimezone = "useTimezone";
    public static final String PNAME_ultraDevHack = "ultraDevHack";
    public static final String PNAME_useUnicode = "useUnicode";
    public static final String PNAME_useUsageAdvisor = "useUsageAdvisor";
    public static final String PNAME_yearIsDateType = "yearIsDateType";
    public static final String PNAME_zeroDateTimeBehavior = "zeroDateTimeBehavior";
    public static final String PNAME_useJvmCharsetConverters = "useJvmCharsetConverters";
    public static final String PNAME_useGmtMillisForDatetimes = "useGmtMillisForDatetimes";
    public static final String PNAME_dumpMetadataOnColumnNotFound = "dumpMetadataOnColumnNotFound";
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

    static {
        PropertyDefinition<?>[] pdefs = new PropertyDefinition[] {
                new BooleanPropertyDefinition(PNAME_paranoid, "paranoid", DEFAULT_VALUE_FALSE, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.paranoid"), "3.0.1", SECURITY_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_passwordCharacterEncoding, "passwordCharacterEncoding", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.passwordCharacterEncoding"), "5.1.7", SECURITY_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_serverRSAPublicKeyFile, "serverRSAPublicKeyFile", null, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.serverRSAPublicKeyFile"), "5.1.31", SECURITY_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_allowPublicKeyRetrieval, "allowPublicKeyRetrieval", DEFAULT_VALUE_FALSE, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowPublicKeyRetrieval"), "5.1.31", SECURITY_CATEGORY, Integer.MIN_VALUE),

                // SSL Options

                new StringPropertyDefinition(PNAME_clientCertificateKeyStoreUrl, "clientCertificateKeyStoreUrl", null, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientCertificateKeyStoreUrl"), "5.1.0", SECURITY_CATEGORY, 5),

                new StringPropertyDefinition(PNAME_trustCertificateKeyStoreUrl, "trustCertificateKeyStoreUrl", null, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.trustCertificateKeyStoreUrl"), "5.1.0", SECURITY_CATEGORY, 8),

                new StringPropertyDefinition(PNAME_clientCertificateKeyStoreType, "clientCertificateKeyStoreType", "JKS", RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientCertificateKeyStoreType"), "5.1.0", SECURITY_CATEGORY, 6),

                new StringPropertyDefinition(PNAME_clientCertificateKeyStorePassword, "clientCertificateKeyStorePassword", null, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientCertificateKeyStorePassword"), "5.1.0", SECURITY_CATEGORY, 7),

                new StringPropertyDefinition(PNAME_trustCertificateKeyStoreType, "trustCertificateKeyStoreType", "JKS", RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.trustCertificateKeyStoreType"), "5.1.0", SECURITY_CATEGORY, 9),

                new StringPropertyDefinition(PNAME_trustCertificateKeyStorePassword, "trustCertificateKeyStorePassword", null, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.trustCertificateKeyStorePassword"), "5.1.0", SECURITY_CATEGORY, 10),

                new BooleanPropertyDefinition(PNAME_verifyServerCertificate, "verifyServerCertificate", DEFAULT_VALUE_TRUE, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.verifyServerCertificate"), "5.1.6", SECURITY_CATEGORY, 4),

                new StringPropertyDefinition(PNAME_enabledSSLCipherSuites, "enabledSSLCipherSuites", null, RUNTIME_NOT_MODIFIABLE,
                        Messages.getString("ConnectionProperties.enabledSSLCipherSuites"), "5.1.35", SECURITY_CATEGORY, 11),

                new BooleanPropertyDefinition(PNAME_useUnbufferedInput, "useUnbufferedInput", DEFAULT_VALUE_TRUE, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useUnbufferedInput"), "3.0.11", MISC_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_profilerEventHandler, "profilerEventHandler", LoggingProfilerEventHandler.class.getName(),
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.profilerEventHandler"), "5.1.6", DEBUGING_PROFILING_CATEGORY,
                        Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_allowLoadLocalInfile, "allowLoadLocalInfile", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadDataLocal"), "3.0.3", SECURITY_CATEGORY, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_allowMultiQueries, "allowMultiQueries", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowMultiQueries"), "3.1.1", SECURITY_CATEGORY, 1),

                new BooleanPropertyDefinition(PNAME_allowNanAndInf, "allowNanAndInf", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowNANandINF"), "3.1.5", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_allowUrlInLocalInfile, "allowUrlInLocalInfile", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowUrlInLoadLocal"), "3.1.4", SECURITY_CATEGORY, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_alwaysSendSetIsolation, "alwaysSendSetIsolation", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.alwaysSendSetIsolation"), "3.1.7", PERFORMANCE_CATEGORY, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_autoClosePStmtStreams, "autoClosePStmtStreams", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoClosePstmtStreams"), "3.1.12", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_allowMasterDownConnections, "allowMasterDownConnections", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.allowMasterDownConnections"), "5.1.27", HA_CATEGORY, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_autoDeserialize, "autoDeserialize", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoDeserialize"), "3.1.5", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_autoGenerateTestcaseScript, "autoGenerateTestcaseScript", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoGenerateTestcaseScript"), "3.1.9", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_autoReconnect, "autoReconnect", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoReconnect"), "1.1", HA_CATEGORY, 0),

                new BooleanPropertyDefinition(PNAME_autoReconnectForPools, "autoReconnectForPools", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoReconnectForPools"), "3.1.3", HA_CATEGORY, 1),

                new MemorySizePropertyDefinition(PNAME_blobSendChunkSize, "blobSendChunkSize", 1024 * 1024, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.blobSendChunkSize"), "3.1.9", PERFORMANCE_CATEGORY, Integer.MIN_VALUE, 0, 0),

                new BooleanPropertyDefinition(PNAME_autoSlowLog, "autoSlowLog", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.autoSlowLog"), "5.1.4", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_blobsAreStrings, "blobsAreStrings", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.blobsAreStrings"), "5.0.8", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_functionsNeverReturnBlobs, "functionsNeverReturnBlobs", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.functionsNeverReturnBlobs"), "5.0.8", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_cacheCallableStmts, "cacheCallableStmts", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cacheCallableStatements"), "3.1.2", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_cachePrepStmts, "cachePrepStmts", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cachePrepStmts"), "3.0.10", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_cacheResultSetMetadata, "cacheResultSetMetadata", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cacheRSMetadata"), "3.1.1", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_serverConfigCacheFactory, "serverConfigCacheFactory", PerVmServerConfigCacheFactory.class.getName(),
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.serverConfigCacheFactory"), "5.1.1", PERFORMANCE_CATEGORY, 12),

                new BooleanPropertyDefinition(PNAME_cacheServerConfiguration, "cacheServerConfiguration", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cacheServerConfiguration"), "3.1.5", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_callableStmtCacheSize, "callableStmtCacheSize", 100, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.callableStmtCacheSize"), "3.1.2", PERFORMANCE_CATEGORY, 5, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_capitalizeTypeNames, "capitalizeTypeNames", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.capitalizeTypeNames"), "2.0.7", MISC_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_characterEncoding, "characterEncoding", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.characterEncoding"), "1.1g", MISC_CATEGORY, 5),

                new StringPropertyDefinition(PNAME_characterSetResults, "characterSetResults", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.characterSetResults"), "3.0.13", MISC_CATEGORY, 6),

                new StringPropertyDefinition(PNAME_connectionAttributes, "connectionAttributes", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionAttributes"), "5.1.25", MISC_CATEGORY, 7),

                new StringPropertyDefinition(PNAME_clientInfoProvider, "clientInfoProvider", "com.mysql.jdbc.CommentClientInfoProvider", RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clientInfoProvider"), "5.1.0", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_clobberStreamingResults, "clobberStreamingResults", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clobberStreamingResults"), "3.0.9", MISC_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_clobCharacterEncoding, "clobCharacterEncoding", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.clobCharacterEncoding"), "5.0.0", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_compensateOnDuplicateKeyUpdateCounts, "compensateOnDuplicateKeyUpdateCounts", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.compensateOnDuplicateKeyUpdateCounts"), "5.1.7", MISC_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_connectionCollation, "connectionCollation", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionCollation"), "3.0.13", MISC_CATEGORY, 7),

                new StringPropertyDefinition(PNAME_connectionLifecycleInterceptors, "connectionLifecycleInterceptors", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionLifecycleInterceptors"), "5.1.4", CONNECTION_AND_AUTH_CATEGORY, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_connectTimeout, "connectTimeout", 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectTimeout"), "3.0.1", CONNECTION_AND_AUTH_CATEGORY, 9, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_continueBatchOnError, "continueBatchOnError", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.continueBatchOnError"), "3.0.3", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_createDatabaseIfNotExist, "createDatabaseIfNotExist", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.createDatabaseIfNotExist"), "3.1.9", MISC_CATEGORY, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_defaultFetchSize, "defaultFetchSize", 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.defaultFetchSize"), "3.1.9", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useServerPrepStmts, "useServerPrepStmts", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useServerPrepStmts"), "3.1.0", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_dontTrackOpenResources, "dontTrackOpenResources", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.dontTrackOpenResources"), "3.1.7", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_dumpQueriesOnException, "dumpQueriesOnException", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.dumpQueriesOnException"), "3.1.3", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_dynamicCalendars, "dynamicCalendars", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.dynamicCalendars"), "3.1.5", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_elideSetAutoCommits, "elideSetAutoCommits", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.eliseSetAutoCommit"), "3.1.3", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_emptyStringsConvertToZero, "emptyStringsConvertToZero", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.emptyStringsConvertToZero"), "3.1.8", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_emulateLocators, "emulateLocators", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.emulateLocators"), "3.1.0", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_emulateUnsupportedPstmts, "emulateUnsupportedPstmts", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.emulateUnsupportedPstmts"), "3.1.7", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_enablePacketDebug, "enablePacketDebug", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.enablePacketDebug"), "3.1.3", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_enableQueryTimeouts, "enableQueryTimeouts", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.enableQueryTimeouts"), "5.0.6", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_explainSlowQueries, "explainSlowQueries", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.explainSlowQueries"), "3.1.2", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_exceptionInterceptors, "exceptionInterceptors", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.exceptionInterceptors"), "5.1.8", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_failOverReadOnly, "failOverReadOnly", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.failoverReadOnly"), "3.0.12", HA_CATEGORY, 2),

                new BooleanPropertyDefinition(PNAME_gatherPerfMetrics, "gatherPerfMetrics", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.gatherPerfMetrics"), "3.1.2", DEBUGING_PROFILING_CATEGORY, 1),

                new BooleanPropertyDefinition(PNAME_generateSimpleParameterMetadata, "generateSimpleParameterMetadata", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.generateSimpleParameterMetadata"), "5.0.5", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_holdResultsOpenOverStatementClose, "holdResultsOpenOverStatementClose", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.holdRSOpenOverStmtClose"), "3.1.7", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_includeInnodbStatusInDeadlockExceptions, "includeInnodbStatusInDeadlockExceptions", false,
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.includeInnodbStatusInDeadlockExceptions"), "5.0.7",
                        DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_includeThreadDumpInDeadlockExceptions, "includeThreadDumpInDeadlockExceptions", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.includeThreadDumpInDeadlockExceptions"), "5.1.15", DEBUGING_PROFILING_CATEGORY,
                        Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_includeThreadNamesAsStatementComment, "includeThreadNamesAsStatementComment", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.includeThreadNamesAsStatementComment"), "5.1.15", DEBUGING_PROFILING_CATEGORY,
                        Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_ignoreNonTxTables, "ignoreNonTxTables", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.ignoreNonTxTables"), "3.0.9", MISC_CATEGORY, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_initialTimeout, "initialTimeout", 2, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.initialTimeout"), "1.1", HA_CATEGORY, 5, 1, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_interactiveClient, "interactiveClient", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.interactiveClient"), "3.1.0", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_jdbcCompliantTruncation, "jdbcCompliantTruncation", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.jdbcCompliantTruncation"), "3.1.2", MISC_CATEGORY, Integer.MIN_VALUE),

                new MemorySizePropertyDefinition(PNAME_largeRowSizeThreshold, "largeRowSizeThreshold", 2048, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.largeRowSizeThreshold"), "5.1.1", PERFORMANCE_CATEGORY, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_loadBalanceStrategy, "loadBalanceStrategy", "random", RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceStrategy"), "5.0.6", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_loadBalanceBlacklistTimeout, "loadBalanceBlacklistTimeout", 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceBlacklistTimeout"), "5.1.0", MISC_CATEGORY, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_loadBalancePingTimeout, "loadBalancePingTimeout", 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalancePingTimeout"), "5.1.13", MISC_CATEGORY, Integer.MIN_VALUE, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_loadBalanceValidateConnectionOnSwapServer, "loadBalanceValidateConnectionOnSwapServer", false,
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.loadBalanceValidateConnectionOnSwapServer"), "5.1.13", MISC_CATEGORY,
                        Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_loadBalanceConnectionGroup, "loadBalanceConnectionGroup", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceConnectionGroup"), "5.1.13", MISC_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_loadBalanceExceptionChecker, "loadBalanceExceptionChecker",
                        StandardLoadBalanceExceptionChecker.class.getName(), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceExceptionChecker"), "5.1.13", MISC_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_loadBalanceSQLStateFailover, "loadBalanceSQLStateFailover", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceSQLStateFailover"), "5.1.13", MISC_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_loadBalanceSQLExceptionSubclassFailover, "loadBalanceSQLExceptionSubclassFailover", null,
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.loadBalanceSQLExceptionSubclassFailover"), "5.1.13", MISC_CATEGORY,
                        Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_loadBalanceEnableJMX, "loadBalanceEnableJMX", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceEnableJMX"), "5.1.13", MISC_CATEGORY, Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_loadBalanceAutoCommitStatementRegex, "loadBalanceAutoCommitStatementRegex", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceAutoCommitStatementRegex"), "5.1.15", MISC_CATEGORY, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_loadBalanceAutoCommitStatementThreshold, "loadBalanceAutoCommitStatementThreshold", 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceAutoCommitStatementThreshold"), "5.1.15", MISC_CATEGORY, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_localSocketAddress, "localSocketAddress", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.localSocketAddress"), "5.0.5", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE),

                new MemorySizePropertyDefinition(PNAME_locatorFetchBufferSize, "locatorFetchBufferSize", 1024 * 1024, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.locatorFetchBufferSize"), "3.2.1", PERFORMANCE_CATEGORY, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_logger, "logger", STANDARD_LOGGER_NAME, RUNTIME_MODIFIABLE, Messages.getString(
                        "ConnectionProperties.logger", new Object[] { Log.class.getName(), STANDARD_LOGGER_NAME }), "3.1.1", DEBUGING_PROFILING_CATEGORY, 0),

                new BooleanPropertyDefinition(PNAME_logSlowQueries, "logSlowQueries", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.logSlowQueries"), "3.1.2", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_logXaCommands, "logXaCommands", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.logXaCommands"), "5.0.5", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_maintainTimeStats, "maintainTimeStats", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.maintainTimeStats"), "3.1.9", PERFORMANCE_CATEGORY, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_maxQuerySizeToLog, "maxQuerySizeToLog", 2048, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.maxQuerySizeToLog"), "3.1.3", DEBUGING_PROFILING_CATEGORY, 4, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_maxReconnects, "maxReconnects", 3, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.maxReconnects"), "1.1", HA_CATEGORY, 4, 1, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_retriesAllDown, "retriesAllDown", 120, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.retriesAllDown"), "5.1.6", HA_CATEGORY, 4, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_maxRows, "maxRows", -1, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.maxRows"),
                        Messages.getString("ConnectionProperties.allVersions"), MISC_CATEGORY, Integer.MIN_VALUE, -1, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_metadataCacheSize, "metadataCacheSize", 50, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.metadataCacheSize"), "3.1.1", PERFORMANCE_CATEGORY, 5, 1, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_netTimeoutForStreamingResults, "netTimeoutForStreamingResults", 600, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.netTimeoutForStreamingResults"), "5.1.0", MISC_CATEGORY, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_noAccessToProcedureBodies, "noAccessToProcedureBodies", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.noAccessToProcedureBodies"), "5.0.3", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_noDatetimeStringSync, "noDatetimeStringSync", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.noDatetimeStringSync"), "3.1.7", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_noTimezoneConversionForTimeType, "noTimezoneConversionForTimeType", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.noTzConversionForTimeType"), "5.0.0", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_noTimezoneConversionForDateType, "noTimezoneConversionForDateType", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.noTzConversionForDateType"), "5.1.35", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_cacheDefaultTimezone, "cacheDefaultTimezone", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.cacheDefaultTimezone"), "5.1.35", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_nullCatalogMeansCurrent, "nullCatalogMeansCurrent", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.nullCatalogMeansCurrent"), "3.1.8", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_nullNamePatternMatchesAll, "nullNamePatternMatchesAll", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.nullNamePatternMatchesAll"), "3.1.8", MISC_CATEGORY, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_packetDebugBufferSize, "packetDebugBufferSize", 20, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.packetDebugBufferSize"), "3.1.3", DEBUGING_PROFILING_CATEGORY, 7, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_padCharsWithSpace, "padCharsWithSpace", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.padCharsWithSpace"), "5.0.6", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_pedantic, "pedantic", false, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.pedantic"),
                        "3.0.0", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_pinGlobalTxToPhysicalConnection, "pinGlobalTxToPhysicalConnection", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.pinGlobalTxToPhysicalConnection"), "5.0.1", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_populateInsertRowWithDefaultValues, "populateInsertRowWithDefaultValues", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.populateInsertRowWithDefaultValues"), "5.0.5", MISC_CATEGORY, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_prepStmtCacheSize, "prepStmtCacheSize", 25, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.prepStmtCacheSize"), "3.0.10", PERFORMANCE_CATEGORY, 10, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_prepStmtCacheSqlLimit, "prepStmtCacheSqlLimit", 256, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.prepStmtCacheSqlLimit"), "3.0.10", PERFORMANCE_CATEGORY, 11, 1, Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_parseInfoCacheFactory, "parseInfoCacheFactory", PerConnectionLRUFactory.class.getName(), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.parseInfoCacheFactory"), "5.1.1", PERFORMANCE_CATEGORY, 12),

                new BooleanPropertyDefinition(PNAME_processEscapeCodesForPrepStmts, "processEscapeCodesForPrepStmts", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.processEscapeCodesForPrepStmts"), "3.1.12", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_profileSQL, "profileSQL", false, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.profileSQL"),
                        "3.1.0", DEBUGING_PROFILING_CATEGORY, 1),

                new StringPropertyDefinition(PNAME_propertiesTransform, "propertiesTransform", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.connectionPropertiesTransform"), "3.1.4", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_queriesBeforeRetryMaster, "queriesBeforeRetryMaster", 50, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.queriesBeforeRetryMaster"), "3.0.2", HA_CATEGORY, 7, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_queryTimeoutKillsConnection, "queryTimeoutKillsConnection", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.queryTimeoutKillsConnection"), "5.1.9", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_reconnectAtTxEnd, "reconnectAtTxEnd", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.reconnectAtTxEnd"), "3.0.10", HA_CATEGORY, 4),

                new BooleanPropertyDefinition(PNAME_relaxAutoCommit, "relaxAutoCommit", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.relaxAutoCommit"), "2.0.13", MISC_CATEGORY, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_reportMetricsIntervalMillis, "reportMetricsIntervalMillis", 30000, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.reportMetricsIntervalMillis"), "3.1.2", DEBUGING_PROFILING_CATEGORY, 3, 0, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_requireSSL, "requireSSL", false, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.requireSSL"),
                        "3.1.0", SECURITY_CATEGORY, 3),

                new StringPropertyDefinition(PNAME_resourceId, "resourceId", null, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.resourceId"),
                        "5.0.1", HA_CATEGORY, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_resultSetSizeThreshold, "resultSetSizeThreshold", 100, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.resultSetSizeThreshold"), "5.0.5", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_rewriteBatchedStatements, "rewriteBatchedStatements", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.rewriteBatchedStatements"), "3.1.13", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_rollbackOnPooledClose, "rollbackOnPooledClose", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.rollbackOnPooledClose"), "3.0.15", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_roundRobinLoadBalance, "roundRobinLoadBalance", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.roundRobinLoadBalance"), "3.1.2", HA_CATEGORY, 5),

                new BooleanPropertyDefinition(PNAME_runningCTS13, "runningCTS13", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.runningCTS13"), "3.1.7", MISC_CATEGORY, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_secondsBeforeRetryMaster, "secondsBeforeRetryMaster", 30, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.secondsBeforeRetryMaster"), "3.0.2", HA_CATEGORY, 8, 0, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_selfDestructOnPingSecondsLifetime, "selfDestructOnPingSecondsLifetime", 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.selfDestructOnPingSecondsLifetime"), "5.1.6", HA_CATEGORY, Integer.MAX_VALUE, 0,
                        Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_selfDestructOnPingMaxOperations, "selfDestructOnPingMaxOperations", 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.selfDestructOnPingMaxOperations"), "5.1.6", HA_CATEGORY, Integer.MAX_VALUE, 0,
                        Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_replicationEnableJMX, "replicationEnableJMX", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.loadBalanceEnableJMX"), "5.1.27", HA_CATEGORY, Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_serverTimezone, "serverTimezone", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.serverTimezone"), "3.0.2", MISC_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_sessionVariables, "sessionVariables", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.sessionVariables"), "3.1.8", MISC_CATEGORY, Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_slowQueryThresholdMillis, "slowQueryThresholdMillis", 2000, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.slowQueryThresholdMillis"), "3.1.2", DEBUGING_PROFILING_CATEGORY, 9, 0, Integer.MAX_VALUE),

                new LongPropertyDefinition(PNAME_slowQueryThresholdNanos, "slowQueryThresholdNanos", 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.slowQueryThresholdNanos"), "5.0.7", DEBUGING_PROFILING_CATEGORY, 10),

                new StringPropertyDefinition(PNAME_socketFactory, "socketFactory", StandardSocketFactory.class.getName(), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.socketFactory"), "3.0.3", CONNECTION_AND_AUTH_CATEGORY, 4),

                new StringPropertyDefinition(PNAME_socksProxyHost, "socksProxyHost", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.socksProxyHost"), "5.1.34", NETWORK_CATEGORY, 1),

                new IntegerPropertyDefinition(PNAME_socksProxyPort, "socksProxyPort", SocksProxySocketFactory.SOCKS_DEFAULT_PORT, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.socksProxyPort"), "5.1.34", NETWORK_CATEGORY, 2, 0, 65535),

                new IntegerPropertyDefinition(PNAME_socketTimeout, "socketTimeout", 0, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.socketTimeout"), "3.0.1", CONNECTION_AND_AUTH_CATEGORY, 10, 0, Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_statementInterceptors, "statementInterceptors", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.statementInterceptors"), "5.1.1", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_strictFloatingPoint, "strictFloatingPoint", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.strictFloatingPoint"), "3.0.0", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_strictUpdates, "strictUpdates", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.strictUpdates"), "3.0.4", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_overrideSupportsIntegrityEnhancementFacility, "overrideSupportsIntegrityEnhancementFacility", false,
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.overrideSupportsIEF"), "3.1.12", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_tcpNoDelay, "tcpNoDelay", Boolean.valueOf(StandardSocketFactory.TCP_NO_DELAY_DEFAULT_VALUE).booleanValue(),
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.tcpNoDelay"), "5.0.7", NETWORK_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_tcpKeepAlive, "tcpKeepAlive", Boolean.valueOf(StandardSocketFactory.TCP_KEEP_ALIVE_DEFAULT_VALUE)
                        .booleanValue(), RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.tcpKeepAlive"), "5.0.7", NETWORK_CATEGORY,
                        Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_tcpRcvBuf, "tcpRcvBuf", Integer.parseInt(StandardSocketFactory.TCP_RCV_BUF_DEFAULT_VALUE),
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.tcpSoRcvBuf"), "5.0.7", NETWORK_CATEGORY, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_tcpSndBuf, "tcpSndBuf", Integer.parseInt(StandardSocketFactory.TCP_SND_BUF_DEFAULT_VALUE),
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.tcpSoSndBuf"), "5.0.7", NETWORK_CATEGORY, Integer.MIN_VALUE, 0,
                        Integer.MAX_VALUE),

                new IntegerPropertyDefinition(PNAME_tcpTrafficClass, "tcpTrafficClass",
                        Integer.parseInt(StandardSocketFactory.TCP_TRAFFIC_CLASS_DEFAULT_VALUE), RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.tcpTrafficClass"), "5.0.7", NETWORK_CATEGORY, Integer.MIN_VALUE, 0, 255),

                new BooleanPropertyDefinition(PNAME_tinyInt1isBit, "tinyInt1isBit", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.tinyInt1isBit"), "3.0.16", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_traceProtocol, "traceProtocol", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.traceProtocol"), "3.1.2", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_treatUtilDateAsTimestamp, "treatUtilDateAsTimestamp", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.treatUtilDateAsTimestamp"), "5.0.5", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_transformedBitIsBoolean, "transformedBitIsBoolean", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.transformedBitIsBoolean"), "3.1.9", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useBlobToStoreUTF8OutsideBMP, "useBlobToStoreUTF8OutsideBMP", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useBlobToStoreUTF8OutsideBMP"), "5.1.3", MISC_CATEGORY, 128),

                new StringPropertyDefinition(PNAME_utf8OutsideBmpExcludedColumnNamePattern, "utf8OutsideBmpExcludedColumnNamePattern", null,
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.utf8OutsideBmpExcludedColumnNamePattern"), "5.1.3", MISC_CATEGORY, 129),

                new StringPropertyDefinition(PNAME_utf8OutsideBmpIncludedColumnNamePattern, "utf8OutsideBmpIncludedColumnNamePattern", null,
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.utf8OutsideBmpIncludedColumnNamePattern"), "5.1.3", MISC_CATEGORY, 129),

                new BooleanPropertyDefinition(PNAME_useCompression, "useCompression", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useCompression"), "3.0.17", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useColumnNamesInFindColumn, "useColumnNamesInFindColumn", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useColumnNamesInFindColumn"), "5.1.7", MISC_CATEGORY, Integer.MAX_VALUE),

                new StringPropertyDefinition(PNAME_useConfigs, "useConfigs", null, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.useConfigs"),
                        "3.1.5", CONNECTION_AND_AUTH_CATEGORY, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_useCursorFetch, "useCursorFetch", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useCursorFetch"), "5.0.0", PERFORMANCE_CATEGORY, Integer.MAX_VALUE),

                new BooleanPropertyDefinition(PNAME_useDynamicCharsetInfo, "useDynamicCharsetInfo", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useDynamicCharsetInfo"), "5.0.6", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useDirectRowUnpack, "useDirectRowUnpack", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useDirectRowUnpack"), "5.1.1", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useFastIntParsing, "useFastIntParsing", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useFastIntParsing"), "3.1.4", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useFastDateParsing, "useFastDateParsing", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useFastDateParsing"), "5.0.5", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useHostsInPrivileges, "useHostsInPrivileges", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useHostsInPrivileges"), "3.0.2", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useInformationSchema, "useInformationSchema", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useInformationSchema"), "5.0.0", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useJDBCCompliantTimezoneShift, "useJDBCCompliantTimezoneShift", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useJDBCCompliantTimezoneShift"), "5.0.0", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useLocalSessionState, "useLocalSessionState", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useLocalSessionState"), "3.1.7", PERFORMANCE_CATEGORY, 5),

                new BooleanPropertyDefinition(PNAME_useLocalTransactionState, "useLocalTransactionState", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useLocalTransactionState"), "5.1.7", PERFORMANCE_CATEGORY, 6),

                new BooleanPropertyDefinition(PNAME_useLegacyDatetimeCode, "useLegacyDatetimeCode", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useLegacyDatetimeCode"), "5.1.6", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useNanosForElapsedTime, "useNanosForElapsedTime", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useNanosForElapsedTime"), "5.0.7", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useOldAliasMetadataBehavior, "useOldAliasMetadataBehavior", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useOldAliasMetadataBehavior"), "5.0.4", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useOldUTF8Behavior, "useOldUTF8Behavior", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useOldUtf8Behavior"), "3.1.6", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useOnlyServerErrorMessages, "useOnlyServerErrorMessages", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useOnlyServerErrorMessages"), "3.0.15", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useReadAheadInput, "useReadAheadInput", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useReadAheadInput"), "3.1.5", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useSSL, "useSSL", false, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.useSSL"), "3.0.2",
                        SECURITY_CATEGORY, 2),

                new BooleanPropertyDefinition(PNAME_useSSPSCompatibleTimezoneShift, "useSSPSCompatibleTimezoneShift", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useSSPSCompatibleTimezoneShift"), "5.0.5", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useStreamLengthsInPrepStmts, "useStreamLengthsInPrepStmts", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useStreamLengthsInPrepStmts"), "3.0.2", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useTimezone, "useTimezone", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useTimezone"), "3.0.2", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_ultraDevHack, "ultraDevHack", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.ultraDevHack"), "2.0.3", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useUnicode, "useUnicode", true, RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.useUnicode"),
                        "1.1g", MISC_CATEGORY, 0),

                new BooleanPropertyDefinition(PNAME_useUsageAdvisor, "useUsageAdvisor", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useUsageAdvisor"), "3.1.1", DEBUGING_PROFILING_CATEGORY, 10),

                new BooleanPropertyDefinition(PNAME_yearIsDateType, "yearIsDateType", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.yearIsDateType"), "3.1.9", MISC_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_zeroDateTimeBehavior, "zeroDateTimeBehavior", ZERO_DATETIME_BEHAVIOR_EXCEPTION, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.zeroDateTimeBehavior", new Object[] { ZERO_DATETIME_BEHAVIOR_EXCEPTION,
                                ZERO_DATETIME_BEHAVIOR_ROUND, ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL }), "3.1.4", MISC_CATEGORY, Integer.MIN_VALUE,
                        new String[] { ZERO_DATETIME_BEHAVIOR_EXCEPTION, ZERO_DATETIME_BEHAVIOR_ROUND, ZERO_DATETIME_BEHAVIOR_CONVERT_TO_NULL }),

                new BooleanPropertyDefinition(PNAME_useJvmCharsetConverters, "useJvmCharsetConverters", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useJvmCharsetConverters"), "5.0.1", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useGmtMillisForDatetimes, "useGmtMillisForDatetimes", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useGmtMillisForDatetimes"), "3.1.12", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_dumpMetadataOnColumnNotFound, "dumpMetadataOnColumnNotFound", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.dumpMetadataOnColumnNotFound"), "3.1.13", DEBUGING_PROFILING_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_useAffectedRows, "useAffectedRows", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.useAffectedRows"), "5.1.7", MISC_CATEGORY, Integer.MIN_VALUE),

                new IntegerPropertyDefinition(PNAME_maxAllowedPacket, "maxAllowedPacket", -1, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.maxAllowedPacket"), "5.1.8", NETWORK_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_authenticationPlugins, "authenticationPlugins", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.authenticationPlugins"), "5.1.19", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_disabledAuthenticationPlugins, "disabledAuthenticationPlugins", null, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.disabledAuthenticationPlugins"), "5.1.19", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE),

                new StringPropertyDefinition(PNAME_defaultAuthenticationPlugin, "defaultAuthenticationPlugin", MysqlNativePasswordPlugin.class.getName(),
                        RUNTIME_MODIFIABLE, Messages.getString("ConnectionProperties.defaultAuthenticationPlugin"), "5.1.19", CONNECTION_AND_AUTH_CATEGORY,
                        Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_disconnectOnExpiredPasswords, "disconnectOnExpiredPasswords", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.disconnectOnExpiredPasswords"), "5.1.23", CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_getProceduresReturnsFunctions, "getProceduresReturnsFunctions", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.getProceduresReturnsFunctions"), "5.1.26", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_detectCustomCollations, "detectCustomCollations", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.detectCustomCollations"), "5.1.29", MISC_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_dontCheckOnDuplicateKeyUpdateInSQL, "dontCheckOnDuplicateKeyUpdateInSQL", false, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.dontCheckOnDuplicateKeyUpdateInSQL"), "5.1.32", PERFORMANCE_CATEGORY, Integer.MIN_VALUE),

                new BooleanPropertyDefinition(PNAME_readOnlyPropagatesToServer, "readOnlyPropagatesToServer", true, RUNTIME_MODIFIABLE,
                        Messages.getString("ConnectionProperties.readOnlyPropagatesToServer"), "5.1.35", PERFORMANCE_CATEGORY, Integer.MIN_VALUE)

        };

        HashMap<String, PropertyDefinition<?>> propertyNameToPropertyDefinitionMap = new HashMap<String, PropertyDefinition<?>>();
        for (PropertyDefinition<?> pdef : pdefs) {
            String pname = pdef.getName();
            propertyNameToPropertyDefinitionMap.put(pname, pdef);
        }
        PROPERTY_NAME_TO_PROPERTY_DEFINITION = Collections.unmodifiableMap(propertyNameToPropertyDefinitionMap);

    }

    public static PropertyDefinition<?> getPropertyDefinition(String propertyName) {
        return PROPERTY_NAME_TO_PROPERTY_DEFINITION.get(propertyName);
    }

    public static RuntimeProperty<?> createRuntimeProperty(String propertyName) {
        PropertyDefinition<?> pdef = getPropertyDefinition(propertyName);
        return pdef.createRuntimeProperty();
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
        StringPropertyDefinition userDef = new StringPropertyDefinition(NonRegisteringDriver.USER_PROPERTY_KEY, "user", null, RUNTIME_NOT_MODIFIABLE,
                Messages.getString("ConnectionProperties.Username"), Messages.getString("ConnectionProperties.allVersions"), CONNECTION_AND_AUTH_CATEGORY,
                Integer.MIN_VALUE + 1);

        StringPropertyDefinition passwordDef = new StringPropertyDefinition(NonRegisteringDriver.PASSWORD_PROPERTY_KEY, "password", null,
                RUNTIME_NOT_MODIFIABLE, Messages.getString("ConnectionProperties.Password"), Messages.getString("ConnectionProperties.allVersions"),
                CONNECTION_AND_AUTH_CATEGORY, Integer.MIN_VALUE + 2);

        XmlMap connectionSortMaps = propertyListByCategory.get(CONNECTION_AND_AUTH_CATEGORY);
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
                    xmlBuf.append("\" required=\"");
                    xmlBuf.append(pdef.isRequired() ? "Yes" : "No");

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
                xmlBuf.append("\" required=\"");
                xmlBuf.append(pdef.isRequired() ? "Yes" : "No");

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
