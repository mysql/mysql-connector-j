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

import java.sql.SQLException;

import com.mysql.cj.api.conf.ConnectionProperties;

public interface JdbcConnectionProperties extends ConnectionProperties {

    public JdbcPropertySet getPropertySet();

    public abstract boolean getIgnoreNonTxTables();

    public abstract int getInitialTimeout();

    public abstract boolean getInteractiveClient();

    /**
     * @return Returns the jdbcCompliantTruncation.
     */
    public abstract boolean getJdbcCompliantTruncation();

    /**
     * @return Returns the dontTrackOpenResources.
     */
    public abstract int getLocatorFetchBufferSize();

    public abstract String getLogger();

    /**
     * @return Returns the loggerClassName.
     */
    public abstract String getLoggerClassName();

    /**
     * @return Returns the logSlowQueries.
     */
    public abstract boolean getLogSlowQueries();

    /**
     * @return Returns the maxQuerySizeToLog.
     */
    public abstract int getMaxQuerySizeToLog();

    public abstract int getMaxReconnects();

    /**
     * Returns the number of queries that metadata can be cached if caching is
     * enabled.
     * 
     * @return the number of queries to cache metadata for.
     */
    public abstract int getMetadataCacheSize();

    /**
     * @return Returns the noDatetimeStringSync.
     */
    public abstract boolean getNoDatetimeStringSync();

    public abstract boolean getNullCatalogMeansCurrent();

    public abstract boolean getNullNamePatternMatchesAll();

    /**
     * @return Returns the packetDebugBufferSize.
     */
    public abstract int getPacketDebugBufferSize();

    public abstract boolean getPedantic();

    /**
     * @return Returns the prepStmtCacheSize.
     */
    public abstract int getPrepStmtCacheSize();

    /**
     * @return Returns the prepStmtCacheSqlLimit.
     */
    public abstract int getPrepStmtCacheSqlLimit();

    /**
     * @return Returns the propertiesTransform.
     */
    public abstract String getPropertiesTransform();

    public abstract int getQueriesBeforeRetryMaster();

    /**
     * @return Returns the reportMetricsIntervalMillis.
     */
    public abstract int getReportMetricsIntervalMillis();

    public abstract boolean getRequireSSL();

    /**
     * @return Returns the rollbackOnPooledClose.
     */
    public abstract boolean getRollbackOnPooledClose();

    /**
     * Returns whether or not hosts will be picked in a round-robin fashion.
     * 
     * @return Returns the roundRobinLoadBalance property.
     */
    public abstract boolean getRoundRobinLoadBalance();

    public abstract int getSecondsBeforeRetryMaster();

    /**
     * Returns the 'serverTimezone' property.
     * 
     * @return the configured server timezone property.
     */
    public abstract String getServerTimezone();

    /**
     * @return Returns the sessionVariables.
     */
    public abstract String getSessionVariables();

    /**
     * @return Returns the slowQueryThresholdMillis.
     */
    public abstract int getSlowQueryThresholdMillis();

    public abstract int getSocketTimeout();

    public abstract boolean getStrictUpdates();

    /**
     * @return Returns the tinyInt1isBit.
     */
    public abstract boolean getTinyInt1isBit();

    /**
     * @return Returns the logProtocol.
     */
    public abstract boolean getTraceProtocol();

    public abstract boolean getTransformedBitIsBoolean();

    public abstract boolean getUseCompression();

    public abstract boolean getUseHostsInPrivileges();

    public abstract boolean getUseInformationSchema();

    /**
     * @return Returns the useLocalSessionState.
     */
    public abstract boolean getUseLocalSessionState();

    /**
     * @return Returns the useOnlyServerErrorMessages.
     */
    public abstract boolean getUseOnlyServerErrorMessages();

    /**
     * @return Returns the useReadAheadInput.
     */
    public abstract boolean getUseReadAheadInput();

    public abstract boolean getUseServerPrepStmts();

    public abstract boolean getUseSSL();

    public abstract boolean getUseStreamLengthsInPrepStmts();

    public abstract boolean getUltraDevHack();

    public abstract boolean getYearIsDateType();

    /**
     * @return Returns the zeroDateTimeBehavior.
     */
    public abstract String getZeroDateTimeBehavior();

    /**
     * @param property
     */
    public abstract void setDetectServerPreparedStmts(boolean property);

    /**
     * @param flag
     *            The enablePacketDebug to set.
     */
    public abstract void setEnablePacketDebug(boolean flag);

    /**
     * @param property
     */
    public abstract void setIgnoreNonTxTables(boolean property);

    /**
     * @param property
     * @throws SQLException
     */
    public abstract void setInitialTimeout(int property) throws SQLException;

    /**
     * @param property
     */
    public abstract void setInteractiveClient(boolean property);

    /**
     * @param flag
     *            The jdbcCompliantTruncation to set.
     */
    public abstract void setJdbcCompliantTruncation(boolean flag);

    /**
     * @param locatorFetchBufferSize
     *            The locatorFetchBufferSize to set.
     */
    public abstract void setLocatorFetchBufferSize(String value) throws SQLException;

    /**
     * @param property
     */
    public abstract void setLogger(String property);

    /**
     * @param className
     *            The loggerClassName to set.
     */
    public abstract void setLoggerClassName(String className);

    /**
     * @param flag
     *            The logSlowQueries to set.
     */
    public abstract void setLogSlowQueries(boolean flag);

    /**
     * @param sizeInBytes
     *            The maxQuerySizeToLog to set.
     * @throws SQLException
     */
    public abstract void setMaxQuerySizeToLog(int sizeInBytes) throws SQLException;

    /**
     * @param property
     * @throws SQLException
     */
    public abstract void setMaxReconnects(int property) throws SQLException;

    /**
     * Sets the number of queries that metadata can be cached if caching is
     * enabled.
     * 
     * @param value
     *            the number of queries to cache metadata for.
     * @throws SQLException
     */
    public abstract void setMetadataCacheSize(int value) throws SQLException;

    /**
     * @param noDatetimeStringSync
     *            The noDatetimeStringSync to set.
     */
    public abstract void setNoDatetimeStringSync(boolean flag);

    public abstract void setNullCatalogMeansCurrent(boolean value);

    public abstract void setNullNamePatternMatchesAll(boolean value);

    /**
     * @param size
     *            The packetDebugBufferSize to set.
     * @throws SQLException
     */
    public abstract void setPacketDebugBufferSize(int size) throws SQLException;

    /**
     * @param property
     */
    public abstract void setPedantic(boolean property);

    /**
     * @param cacheSize
     *            The prepStmtCacheSize to set.
     * @throws SQLException
     */
    public abstract void setPrepStmtCacheSize(int cacheSize) throws SQLException;

    /**
     * @param cacheSqlLimit
     *            The prepStmtCacheSqlLimit to set.
     * @throws SQLException
     */
    public abstract void setPrepStmtCacheSqlLimit(int cacheSqlLimit) throws SQLException;

    /**
     * @param propertiesTransform
     *            The propertiesTransform to set.
     */
    public abstract void setPropertiesTransform(String value);

    /**
     * @param property
     * @throws SQLException
     */
    public abstract void setQueriesBeforeRetryMaster(int property) throws SQLException;

    /**
     * @param millis
     *            The reportMetricsIntervalMillis to set.
     * @throws SQLException
     */
    public abstract void setReportMetricsIntervalMillis(int millis) throws SQLException;

    /**
     * @param property
     */
    public abstract void setRequireSSL(boolean property);

    /**
     * @param rollbackOnPooledClose
     *            The rollbackOnPooledClose to set.
     */
    public abstract void setRollbackOnPooledClose(boolean flag);

    /**
     * Sets whether or not hosts will be picked in a round-robin fashion.
     * 
     * @param flag
     *            The roundRobinLoadBalance property to set.
     */
    public abstract void setRoundRobinLoadBalance(boolean flag);

    /**
     * @param property
     * @throws SQLException
     */
    public abstract void setSecondsBeforeRetryMaster(int property) throws SQLException;

    /**
     * @param property
     */
    public abstract void setServerTimezone(String property);

    /**
     * @param sessionVariables
     *            The sessionVariables to set.
     */
    public abstract void setSessionVariables(String variables);

    /**
     * @param millis
     *            The slowQueryThresholdMillis to set.
     * @throws SQLException
     */
    public abstract void setSlowQueryThresholdMillis(int millis) throws SQLException;

    /**
     * @param property
     * @throws SQLException
     */
    public abstract void setSocketTimeout(int property) throws SQLException;

    /**
     * @param property
     */
    public abstract void setStrictUpdates(boolean property);

    /**
     * @param tinyInt1isBit
     *            The tinyInt1isBit to set.
     */
    public abstract void setTinyInt1isBit(boolean flag);

    /**
     * @param flag
     *            The logProtocol to set.
     */
    public abstract void setTraceProtocol(boolean flag);

    public abstract void setTransformedBitIsBoolean(boolean flag);

    /**
     * @param property
     */
    public abstract void setUseCompression(boolean property);

    /**
     * @param property
     */
    public abstract void setUseHostsInPrivileges(boolean property);

    public abstract void setUseInformationSchema(boolean flag);

    /**
     * @param useLocalSessionState
     *            The useLocalSessionState to set.
     */
    public abstract void setUseLocalSessionState(boolean flag);

    /**
     * @param useOnlyServerErrorMessages
     *            The useOnlyServerErrorMessages to set.
     */
    public abstract void setUseOnlyServerErrorMessages(boolean flag);

    /**
     * @param useReadAheadInput
     *            The useReadAheadInput to set.
     */
    public abstract void setUseReadAheadInput(boolean flag);

    /**
     * @param flag
     *            The detectServerPreparedStmts to set.
     */
    public abstract void setUseServerPrepStmts(boolean flag);

    /**
     * @param property
     */
    public abstract void setUseSSL(boolean property);

    /**
     * @param property
     */
    public abstract void setUseStreamLengthsInPrepStmts(boolean property);

    /**
     * @param property
     */
    public abstract void setUltraDevHack(boolean flag);

    public abstract void setYearIsDateType(boolean flag);

    /**
     * @param zeroDateTimeBehavior
     *            The zeroDateTimeBehavior to set.
     */
    public abstract void setZeroDateTimeBehavior(String behavior);

    /**
     * @return Returns the useUnbufferedInput.
     */
    public abstract boolean useUnbufferedInput();

    public abstract boolean getUseCursorFetch();

    public abstract void setUseCursorFetch(boolean flag);

    public abstract boolean getOverrideSupportsIntegrityEnhancementFacility();

    public abstract void setOverrideSupportsIntegrityEnhancementFacility(boolean flag);

    public abstract boolean getAutoClosePStmtStreams();

    public abstract void setAutoClosePStmtStreams(boolean flag);

    public abstract boolean getProcessEscapeCodesForPrepStmts();

    public abstract void setProcessEscapeCodesForPrepStmts(boolean flag);

    public abstract String getResourceId();

    public abstract void setResourceId(String resourceId);

    public abstract boolean getRewriteBatchedStatements();

    public abstract void setRewriteBatchedStatements(boolean flag);

    public abstract boolean getPinGlobalTxToPhysicalConnection();

    public abstract void setPinGlobalTxToPhysicalConnection(boolean flag);

    public abstract boolean getNoAccessToProcedureBodies();

    public abstract void setNoAccessToProcedureBodies(boolean flag);

    public abstract boolean getUseOldAliasMetadataBehavior();

    public abstract void setUseOldAliasMetadataBehavior(boolean flag);

    public abstract boolean getTreatUtilDateAsTimestamp();

    public abstract void setTreatUtilDateAsTimestamp(boolean flag);

    public abstract String getLocalSocketAddress();

    public abstract void setLocalSocketAddress(String address);

    public abstract void setUseConfigs(String configs);

    public abstract String getUseConfigs();

    public abstract boolean getGenerateSimpleParameterMetadata();

    public abstract void setGenerateSimpleParameterMetadata(boolean flag);

    public abstract boolean getLogXaCommands();

    public abstract void setLogXaCommands(boolean flag);

    public abstract int getResultSetSizeThreshold();

    public abstract void setResultSetSizeThreshold(int threshold) throws SQLException;

    public abstract int getNetTimeoutForStreamingResults();

    public abstract void setNetTimeoutForStreamingResults(int value) throws SQLException;

    public abstract boolean getEnableQueryTimeouts();

    public abstract void setEnableQueryTimeouts(boolean flag);

    public abstract boolean getPadCharsWithSpace();

    public abstract void setPadCharsWithSpace(boolean flag);

    public abstract boolean getUseDynamicCharsetInfo();

    public abstract void setUseDynamicCharsetInfo(boolean flag);

    public abstract String getClientInfoProvider();

    public abstract void setClientInfoProvider(String classname);

    public abstract boolean getPopulateInsertRowWithDefaultValues();

    public abstract void setPopulateInsertRowWithDefaultValues(boolean flag);

    public abstract String getHaLoadBalanceStrategy();

    public abstract void setHaLoadBalanceStrategy(String strategy);

    public abstract boolean getTcpNoDelay();

    public abstract void setTcpNoDelay(boolean flag);

    public abstract boolean getTcpKeepAlive();

    public abstract void setTcpKeepAlive(boolean flag);

    public abstract int getTcpRcvBuf();

    public abstract void setTcpRcvBuf(int bufSize) throws SQLException;

    public abstract int getTcpSndBuf();

    public abstract void setTcpSndBuf(int bufSize) throws SQLException;

    public abstract int getTcpTrafficClass();

    public abstract void setTcpTrafficClass(int classFlags) throws SQLException;

    public abstract boolean getUseNanosForElapsedTime();

    public abstract void setUseNanosForElapsedTime(boolean flag);

    public abstract long getSlowQueryThresholdNanos();

    public abstract void setSlowQueryThresholdNanos(long nanos) throws SQLException;

    public abstract String getStatementInterceptors();

    public abstract void setStatementInterceptors(String value);

    public abstract boolean getUseDirectRowUnpack();

    public abstract void setUseDirectRowUnpack(boolean flag);

    public abstract String getLargeRowSizeThreshold();

    public abstract void setLargeRowSizeThreshold(String value) throws SQLException;

    public abstract boolean getUseBlobToStoreUTF8OutsideBMP();

    public abstract void setUseBlobToStoreUTF8OutsideBMP(boolean flag);

    public abstract String getUtf8OutsideBmpExcludedColumnNamePattern();

    public abstract void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern);

    public abstract String getUtf8OutsideBmpIncludedColumnNamePattern();

    public abstract void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern);

    public abstract boolean getIncludeInnodbStatusInDeadlockExceptions();

    public abstract void setIncludeInnodbStatusInDeadlockExceptions(boolean flag);

    public abstract boolean getIncludeThreadDumpInDeadlockExceptions();

    public abstract void setIncludeThreadDumpInDeadlockExceptions(boolean flag);

    public abstract boolean getIncludeThreadNamesAsStatementComment();

    public abstract void setIncludeThreadNamesAsStatementComment(boolean flag);

    public abstract boolean getBlobsAreStrings();

    public abstract void setBlobsAreStrings(boolean flag);

    public abstract boolean getFunctionsNeverReturnBlobs();

    public abstract void setFunctionsNeverReturnBlobs(boolean flag);

    public abstract boolean getAutoSlowLog();

    public abstract void setAutoSlowLog(boolean flag);

    public abstract String getConnectionLifecycleInterceptors();

    public abstract void setConnectionLifecycleInterceptors(String interceptors);

    public abstract int getSelfDestructOnPingSecondsLifetime();

    public abstract void setSelfDestructOnPingSecondsLifetime(int seconds) throws SQLException;

    public abstract int getSelfDestructOnPingMaxOperations();

    public abstract void setSelfDestructOnPingMaxOperations(int maxOperations) throws SQLException;

    public abstract boolean getUseColumnNamesInFindColumn();

    public abstract void setUseColumnNamesInFindColumn(boolean flag);

    public abstract boolean getUseLocalTransactionState();

    public abstract void setUseLocalTransactionState(boolean flag);

    public abstract boolean getCompensateOnDuplicateKeyUpdateCounts();

    public abstract void setCompensateOnDuplicateKeyUpdateCounts(boolean flag);

    public abstract void setUseAffectedRows(boolean flag);

    public abstract boolean getUseAffectedRows();

    public abstract int getLoadBalanceBlacklistTimeout();

    public abstract void setLoadBalanceBlacklistTimeout(int loadBalanceBlacklistTimeout) throws SQLException;

    public abstract void setRetriesAllDown(int retriesAllDown) throws SQLException;

    public abstract int getRetriesAllDown();

    public abstract void setExceptionInterceptors(String exceptionInterceptors);

    public abstract String getExceptionInterceptors();

    public abstract boolean getQueryTimeoutKillsConnection();

    public abstract void setQueryTimeoutKillsConnection(boolean queryTimeoutKillsConnection);

    public int getMaxAllowedPacket();

    public abstract int getLoadBalancePingTimeout();

    public abstract void setLoadBalancePingTimeout(int loadBalancePingTimeout) throws SQLException;

    public abstract boolean getLoadBalanceValidateConnectionOnSwapServer();

    public abstract void setLoadBalanceValidateConnectionOnSwapServer(boolean loadBalanceValidateConnectionOnSwapServer);

    public abstract String getLoadBalanceConnectionGroup();

    public abstract void setLoadBalanceConnectionGroup(String loadBalanceConnectionGroup);

    public abstract String getLoadBalanceExceptionChecker();

    public abstract void setLoadBalanceExceptionChecker(String loadBalanceExceptionChecker);

    public abstract String getLoadBalanceSQLStateFailover();

    public abstract void setLoadBalanceSQLStateFailover(String loadBalanceSQLStateFailover);

    public abstract String getLoadBalanceSQLExceptionSubclassFailover();

    public abstract void setLoadBalanceSQLExceptionSubclassFailover(String loadBalanceSQLExceptionSubclassFailover);

    public void setLoadBalanceAutoCommitStatementThreshold(int loadBalanceAutoCommitStatementThreshold) throws SQLException;

    public int getLoadBalanceAutoCommitStatementThreshold();

    public void setLoadBalanceAutoCommitStatementRegex(String loadBalanceAutoCommitStatementRegex);

    public String getLoadBalanceAutoCommitStatementRegex();

    public abstract void setAuthenticationPlugins(String authenticationPlugins);

    public abstract String getAuthenticationPlugins();

    public abstract void setDisabledAuthenticationPlugins(String disabledAuthenticationPlugins);

    public abstract String getDisabledAuthenticationPlugins();

    public abstract void setDefaultAuthenticationPlugin(String defaultAuthenticationPlugin);

    public abstract String getDefaultAuthenticationPlugin();

    public abstract void setParseInfoCacheFactory(String factoryClassname);

    public abstract String getParseInfoCacheFactory();

    public abstract void setServerConfigCacheFactory(String factoryClassname);

    public abstract String getServerConfigCacheFactory();

    public abstract void setDisconnectOnExpiredPasswords(boolean disconnectOnExpiredPasswords);

    public abstract boolean getDisconnectOnExpiredPasswords();

    public abstract boolean getAllowMasterDownConnections();

    public abstract void setAllowMasterDownConnections(boolean connectIfMasterDown);

    public abstract boolean getHaEnableJMX();

    public abstract void setHaEnableJMX(boolean replicationEnableJMX);

    public abstract void setGetProceduresReturnsFunctions(boolean getProcedureReturnsFunctions);

    public abstract boolean getGetProceduresReturnsFunctions();

    public abstract void setDetectCustomCollations(boolean detectCustomCollations);

    public abstract boolean getDetectCustomCollations();

    public void setDontCheckOnDuplicateKeyUpdateInSQL(boolean dontCheckOnDuplicateKeyUpdateInSQL);

    public boolean getDontCheckOnDuplicateKeyUpdateInSQL();

    public void setSocksProxyHost(String socksProxyHost);

    public String getSocksProxyHost();

    public void setSocksProxyPort(int socksProxyPort) throws SQLException;

    public int getSocksProxyPort();

    public boolean getReadOnlyPropagatesToServer();

    public void setReadOnlyPropagatesToServer(boolean flag);
}
