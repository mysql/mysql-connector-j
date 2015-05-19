/*
  Copyright (c) 2002, 2015, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.jdbc;

import java.io.Serializable;

import com.mysql.cj.core.conf.DefaultPropertySet;
import com.mysql.cj.core.conf.PropertyDefinitions;

/**
 * Represents configurable properties for Connections and DataSources. Can also expose properties as JDBC DriverPropertyInfo if required as well.
 */
public class JdbcPropertySet extends DefaultPropertySet implements Serializable {

    private static final long serialVersionUID = -5999058409714319731L;

    public JdbcPropertySet() {
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_holdResultsOpenOverStatementClose));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowLoadLocalInfile));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowMultiQueries));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowNanAndInf));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowUrlInLocalInfile));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_alwaysSendSetIsolation));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_autoClosePStmtStreams));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowMasterDownConnections));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_autoDeserialize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_autoGenerateTestcaseScript));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_autoReconnect));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_autoReconnectForPools));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_autoSlowLog));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_blobsAreStrings));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_functionsNeverReturnBlobs));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_cacheCallableStmts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_cachePrepStmts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_cacheResultSetMetadata));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_cacheServerConfiguration));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_capitalizeTypeNames));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_clobberStreamingResults));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_compensateOnDuplicateKeyUpdateCounts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_continueBatchOnError));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_createDatabaseIfNotExist));

        // Think really long and hard about changing the default for this many, many applications have come to be acustomed to the latency profile of preparing
        // stuff client-side, rather than prepare (round-trip), execute (round-trip), close (round-trip).
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useServerPrepStmts));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_dontTrackOpenResources));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_dumpQueriesOnException));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_dynamicCalendars));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_elideSetAutoCommits));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_emptyStringsConvertToZero));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_emulateLocators));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_emulateUnsupportedPstmts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_enablePacketDebug));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_enableQueryTimeouts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_explainSlowQueries));

        /** When failed-over, set connection to read-only? */
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_failOverReadOnly));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_gatherPerfMetrics));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_generateSimpleParameterMetadata));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_includeInnodbStatusInDeadlockExceptions));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_includeThreadDumpInDeadlockExceptions));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_includeThreadNamesAsStatementComment));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_ignoreNonTxTables));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_interactiveClient));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_jdbcCompliantTruncation));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceValidateConnectionOnSwapServer));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceEnableJMX));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_logSlowQueries));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_logXaCommands));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_maintainTimeStats));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_noAccessToProcedureBodies));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_noDatetimeStringSync));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_noTimezoneConversionForTimeType));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_noTimezoneConversionForDateType));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_cacheDefaultTimezone));

        // TODO: rename this property according to WL#8120; default value is already changed as required by this WL
        // TODO: make this property consistent to nullNamePatternMatchesAll; nullCatalogMeansCurrent never cause an exception, but nullNamePatternMatchesAll does.
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_nullCatalogMeansCurrent));

        // TODO: rename this property according to WL#8120; default value is already changed as required by this WL
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_nullNamePatternMatchesAll));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_padCharsWithSpace));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_pedantic));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_pinGlobalTxToPhysicalConnection));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_populateInsertRowWithDefaultValues));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_processEscapeCodesForPrepStmts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_profileSQL));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_queryTimeoutKillsConnection));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_reconnectAtTxEnd));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_relaxAutoCommit));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_requireSSL));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_rewriteBatchedStatements));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_rollbackOnPooledClose));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_roundRobinLoadBalance));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_runningCTS13));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_replicationEnableJMX));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_strictFloatingPoint));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_strictUpdates));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_overrideSupportsIntegrityEnhancementFacility));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_tcpNoDelay));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_tcpKeepAlive));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_tinyInt1isBit));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_traceProtocol));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_treatUtilDateAsTimestamp));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_transformedBitIsBoolean));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useBlobToStoreUTF8OutsideBMP));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useCompression));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useColumnNamesInFindColumn));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useCursorFetch));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useDynamicCharsetInfo));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useDirectRowUnpack));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useFastIntParsing));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useFastDateParsing));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useHostsInPrivileges));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useInformationSchema));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useJDBCCompliantTimezoneShift));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useLocalSessionState));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useLocalTransactionState));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useLegacyDatetimeCode));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useNanosForElapsedTime));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useOldAliasMetadataBehavior));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useOldUTF8Behavior));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useOnlyServerErrorMessages));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useReadAheadInput));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useSSL));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useSSPSCompatibleTimezoneShift));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useStreamLengthsInPrepStmts));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useTimezone));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_ultraDevHack));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useUnbufferedInput));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useUnicode));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useUsageAdvisor));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_yearIsDateType));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useJvmCharsetConverters));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useGmtMillisForDatetimes));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_dumpMetadataOnColumnNotFound));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useAffectedRows));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_disconnectOnExpiredPasswords));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_getProceduresReturnsFunctions));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_detectCustomCollations));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_allowPublicKeyRetrieval));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_dontCheckOnDuplicateKeyUpdateInSQL));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_readOnlyPropagatesToServer));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_callableStmtCacheSize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_connectTimeout));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_defaultFetchSize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_initialTimeout));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceBlacklistTimeout));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalancePingTimeout));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementThreshold));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_maxQuerySizeToLog));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_maxReconnects));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_retriesAllDown));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_maxRows));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_metadataCacheSize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_netTimeoutForStreamingResults));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_packetDebugBufferSize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_prepStmtCacheSize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_prepStmtCacheSqlLimit));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_queriesBeforeRetryMaster));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_reportMetricsIntervalMillis));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_resultSetSizeThreshold));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_secondsBeforeRetryMaster));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_selfDestructOnPingSecondsLifetime));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_selfDestructOnPingMaxOperations));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_slowQueryThresholdMillis));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_socksProxyPort));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_socketTimeout));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_tcpRcvBuf));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_tcpSndBuf));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_tcpTrafficClass));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_maxAllowedPacket));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_slowQueryThresholdNanos));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_blobSendChunkSize));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_largeRowSizeThreshold));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_locatorFetchBufferSize));

        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_serverConfigCacheFactory));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_characterEncoding));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_characterSetResults));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_connectionAttributes));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_clientInfoProvider));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_clobCharacterEncoding));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_connectionCollation));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_connectionLifecycleInterceptors));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_exceptionInterceptors));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceStrategy));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceConnectionGroup));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceExceptionChecker));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceSQLStateFailover));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceSQLExceptionSubclassFailover));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_loadBalanceAutoCommitStatementRegex));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_localSocketAddress));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_logger));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_parseInfoCacheFactory));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_propertiesTransform));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_resourceId));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_serverTimezone));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_sessionVariables));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_socketFactory));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_socksProxyHost));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_statementInterceptors));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_utf8OutsideBmpExcludedColumnNamePattern));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_utf8OutsideBmpIncludedColumnNamePattern));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_useConfigs));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_zeroDateTimeBehavior));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_authenticationPlugins));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_disabledAuthenticationPlugins));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_defaultAuthenticationPlugin));
        addProperty(PropertyDefinitions.createRuntimeProperty(PropertyDefinitions.PNAME_serverRSAPublicKeyFile));
    }

}
