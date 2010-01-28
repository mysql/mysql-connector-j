/*
 Copyright  2002-2007 MySQL AB, 2008 Sun Microsystems
 All rights reserved. Use is subject to license terms.

  The MySQL Connector/J is licensed under the terms of the GPL,
  like most MySQL Connectors. There are special exceptions to the
  terms and conditions of the GPL as it is applied to this software,
  see the FLOSS License Exception available on mysql.com.

  This program is free software; you can redistribute it and/or
  modify it under the terms of the GNU General Public License as
  published by the Free Software Foundation; version 2 of the
  License.

  This program is distributed in the hope that it will be useful,  
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
  02110-1301 USA

 */

package com.mysql.jdbc;

import java.sql.SQLException;

public interface ConnectionProperties {

	/**
	 * Returns a description of the connection properties as an XML document.
	 * 
	 * @return the connection properties as an XML document.
	 * @throws SQLException
	 *             if an error occurs.
	 */
	public abstract String exposeAsXml() throws SQLException;

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getAllowLoadLocalInfile();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getAllowMultiQueries();

	/**
	 * @return Returns the allowNanAndInf.
	 */
	public abstract boolean getAllowNanAndInf();

	/**
	 * @return Returns the allowUrlInLocalInfile.
	 */
	public abstract boolean getAllowUrlInLocalInfile();

	/**
	 * @return Returns the alwaysSendSetIsolation.
	 */
	public abstract boolean getAlwaysSendSetIsolation();

	/**
	 * @return Returns the autoDeserialize.
	 */
	public abstract boolean getAutoDeserialize();

	public abstract boolean getAutoGenerateTestcaseScript();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getAutoReconnectForPools();

	/**
	 * @return Returns the blobSendChunkSize.
	 */
	public abstract int getBlobSendChunkSize();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns if cacheCallableStatements is enabled
	 */
	public abstract boolean getCacheCallableStatements();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the cachePreparedStatements.
	 */
	public abstract boolean getCachePreparedStatements();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return DOCUMENT ME!
	 */
	public abstract boolean getCacheResultSetMetadata();

	/**
	 * @return Returns the cacheServerConfiguration.
	 */
	public abstract boolean getCacheServerConfiguration();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the callableStatementCacheSize.
	 */
	public abstract int getCallableStatementCacheSize();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getCapitalizeTypeNames();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the characterSetResults.
	 */
	public abstract String getCharacterSetResults();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the clobberStreamingResults.
	 */
	public abstract boolean getClobberStreamingResults();

	public abstract String getClobCharacterEncoding();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the connectionCollation.
	 */
	public abstract String getConnectionCollation();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract int getConnectTimeout();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getContinueBatchOnError();

	public abstract boolean getCreateDatabaseIfNotExist();

	public abstract int getDefaultFetchSize();

	/**
	 * @return Returns the dontTrackOpenResources.
	 */
	public abstract boolean getDontTrackOpenResources();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the dumpQueriesOnException.
	 */
	public abstract boolean getDumpQueriesOnException();

	/**
	 * @return Returns the dynamicCalendars.
	 */
	public abstract boolean getDynamicCalendars();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the elideSetAutoCommits.
	 */
	public abstract boolean getElideSetAutoCommits();

	public abstract boolean getEmptyStringsConvertToZero();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getEmulateLocators();

	/**
	 * @return Returns the emulateUnsupportedPstmts.
	 */
	public abstract boolean getEmulateUnsupportedPstmts();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the enablePacketDebug.
	 */
	public abstract boolean getEnablePacketDebug();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract String getEncoding();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the explainSlowQueries.
	 */
	public abstract boolean getExplainSlowQueries();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the failOverReadOnly.
	 */
	public abstract boolean getFailOverReadOnly();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the gatherPerformanceMetrics.
	 */
	public abstract boolean getGatherPerformanceMetrics();

	/**
	 * @return Returns the holdResultsOpenOverStatementClose.
	 */
	public abstract boolean getHoldResultsOpenOverStatementClose();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getIgnoreNonTxTables();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract int getInitialTimeout();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getInteractiveClient();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the isInteractiveClient.
	 */
	public abstract boolean getIsInteractiveClient();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the jdbcCompliantTruncation.
	 */
	public abstract boolean getJdbcCompliantTruncation();

	/**
	 * @return Returns the dontTrackOpenResources.
	 */
	public abstract int getLocatorFetchBufferSize();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract String getLogger();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the loggerClassName.
	 */
	public abstract String getLoggerClassName();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the logSlowQueries.
	 */
	public abstract boolean getLogSlowQueries();

	public abstract boolean getMaintainTimeStats();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the maxQuerySizeToLog.
	 */
	public abstract int getMaxQuerySizeToLog();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract int getMaxReconnects();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract int getMaxRows();

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
	 * DOCUMENT ME!
	 * 
	 * @return Returns the packetDebugBufferSize.
	 */
	public abstract int getPacketDebugBufferSize();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getParanoid();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getPedantic();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the preparedStatementCacheSize.
	 */
	public abstract int getPreparedStatementCacheSize();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the preparedStatementCacheSqlLimit.
	 */
	public abstract int getPreparedStatementCacheSqlLimit();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getProfileSql();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the profileSQL flag
	 */
	public abstract boolean getProfileSQL();

	/**
	 * @return Returns the propertiesTransform.
	 */
	public abstract String getPropertiesTransform();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract int getQueriesBeforeRetryMaster();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getReconnectAtTxEnd();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getRelaxAutoCommit();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the reportMetricsIntervalMillis.
	 */
	public abstract int getReportMetricsIntervalMillis();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
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

	/**
	 * @return Returns the runningCTS13.
	 */
	public abstract boolean getRunningCTS13();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
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
	 * DOCUMENT ME!
	 * 
	 * @return Returns the slowQueryThresholdMillis.
	 */
	public abstract int getSlowQueryThresholdMillis();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract String getSocketFactoryClassName();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract int getSocketTimeout();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getStrictFloatingPoint();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getStrictUpdates();

	/**
	 * @return Returns the tinyInt1isBit.
	 */
	public abstract boolean getTinyInt1isBit();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the logProtocol.
	 */
	public abstract boolean getTraceProtocol();

	public abstract boolean getTransformedBitIsBoolean();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getUseCompression();

	/**
	 * @return Returns the useFastIntParsing.
	 */
	public abstract boolean getUseFastIntParsing();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getUseHostsInPrivileges();

	public abstract boolean getUseInformationSchema();

	/**
	 * @return Returns the useLocalSessionState.
	 */
	public abstract boolean getUseLocalSessionState();

	/**
	 * @return Returns the useOldUTF8Behavior.
	 */
	public abstract boolean getUseOldUTF8Behavior();

	/**
	 * @return Returns the useOnlyServerErrorMessages.
	 */
	public abstract boolean getUseOnlyServerErrorMessages();

	/**
	 * @return Returns the useReadAheadInput.
	 */
	public abstract boolean getUseReadAheadInput();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getUseServerPreparedStmts();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the useSqlStateCodes state.
	 */
	public abstract boolean getUseSqlStateCodes();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getUseSSL();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getUseStreamLengthsInPrepStmts();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getUseTimezone();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getUseUltraDevWorkAround();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the useUnbufferedInput.
	 */
	public abstract boolean getUseUnbufferedInput();

	/**
	 * DOCUMENT ME!
	 * 
	 * @return
	 */
	public abstract boolean getUseUnicode();

	/**
	 * Returns whether or not the driver advises of proper usage.
	 * 
	 * @return the value of useUsageAdvisor
	 */
	public abstract boolean getUseUsageAdvisor();

	public abstract boolean getYearIsDateType();

	/**
	 * @return Returns the zeroDateTimeBehavior.
	 */
	public abstract String getZeroDateTimeBehavior();

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setAllowLoadLocalInfile(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setAllowMultiQueries(boolean property);

	/**
	 * @param allowNanAndInf
	 *            The allowNanAndInf to set.
	 */
	public abstract void setAllowNanAndInf(boolean flag);

	/**
	 * @param allowUrlInLocalInfile
	 *            The allowUrlInLocalInfile to set.
	 */
	public abstract void setAllowUrlInLocalInfile(boolean flag);

	/**
	 * @param alwaysSendSetIsolation
	 *            The alwaysSendSetIsolation to set.
	 */
	public abstract void setAlwaysSendSetIsolation(boolean flag);

	/**
	 * @param autoDeserialize
	 *            The autoDeserialize to set.
	 */
	public abstract void setAutoDeserialize(boolean flag);

	public abstract void setAutoGenerateTestcaseScript(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The autoReconnect to set.
	 */
	public abstract void setAutoReconnect(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setAutoReconnectForConnectionPools(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The autoReconnectForPools to set.
	 */
	public abstract void setAutoReconnectForPools(boolean flag);

	/**
	 * @param blobSendChunkSize
	 *            The blobSendChunkSize to set.
	 */
	public abstract void setBlobSendChunkSize(String value) throws SQLException;

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The cacheCallableStatements to set.
	 */
	public abstract void setCacheCallableStatements(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The cachePreparedStatements to set.
	 */
	public abstract void setCachePreparedStatements(boolean flag);

	/**
	 * Sets whether or not we should cache result set metadata.
	 * 
	 * @param property
	 */
	public abstract void setCacheResultSetMetadata(boolean property);

	/**
	 * @param cacheServerConfiguration
	 *            The cacheServerConfiguration to set.
	 */
	public abstract void setCacheServerConfiguration(boolean flag);

	/**
	 * Configures the number of callable statements to cache. (this is
	 * configurable during the life of the connection).
	 * 
	 * @param size
	 *            The callableStatementCacheSize to set.
	 */
	public abstract void setCallableStatementCacheSize(int size);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setCapitalizeDBMDTypes(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The capitalizeTypeNames to set.
	 */
	public abstract void setCapitalizeTypeNames(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param encoding
	 *            The characterEncoding to set.
	 */
	public abstract void setCharacterEncoding(String encoding);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param characterSet
	 *            The characterSetResults to set.
	 */
	public abstract void setCharacterSetResults(String characterSet);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The clobberStreamingResults to set.
	 */
	public abstract void setClobberStreamingResults(boolean flag);

	public abstract void setClobCharacterEncoding(String encoding);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param collation
	 *            The connectionCollation to set.
	 */
	public abstract void setConnectionCollation(String collation);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param timeoutMs
	 */
	public abstract void setConnectTimeout(int timeoutMs);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setContinueBatchOnError(boolean property);

	public abstract void setCreateDatabaseIfNotExist(boolean flag);

	public abstract void setDefaultFetchSize(int n);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setDetectServerPreparedStmts(boolean property);

	/**
	 * @param dontTrackOpenResources
	 *            The dontTrackOpenResources to set.
	 */
	public abstract void setDontTrackOpenResources(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The dumpQueriesOnException to set.
	 */
	public abstract void setDumpQueriesOnException(boolean flag);

	/**
	 * @param dynamicCalendars
	 *            The dynamicCalendars to set.
	 */
	public abstract void setDynamicCalendars(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The elideSetAutoCommits to set.
	 */
	public abstract void setElideSetAutoCommits(boolean flag);

	public abstract void setEmptyStringsConvertToZero(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setEmulateLocators(boolean property);

	/**
	 * @param emulateUnsupportedPstmts
	 *            The emulateUnsupportedPstmts to set.
	 */
	public abstract void setEmulateUnsupportedPstmts(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The enablePacketDebug to set.
	 */
	public abstract void setEnablePacketDebug(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setEncoding(String property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The explainSlowQueries to set.
	 */
	public abstract void setExplainSlowQueries(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The failOverReadOnly to set.
	 */
	public abstract void setFailOverReadOnly(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The gatherPerformanceMetrics to set.
	 */
	public abstract void setGatherPerformanceMetrics(boolean flag);

	/**
	 * @param holdResultsOpenOverStatementClose
	 *            The holdResultsOpenOverStatementClose to set.
	 */
	public abstract void setHoldResultsOpenOverStatementClose(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setIgnoreNonTxTables(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setInitialTimeout(int property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setIsInteractiveClient(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The jdbcCompliantTruncation to set.
	 */
	public abstract void setJdbcCompliantTruncation(boolean flag);

	/**
	 * @param locatorFetchBufferSize
	 *            The locatorFetchBufferSize to set.
	 */
	public abstract void setLocatorFetchBufferSize(String value)
			throws SQLException;

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setLogger(String property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param className
	 *            The loggerClassName to set.
	 */
	public abstract void setLoggerClassName(String className);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The logSlowQueries to set.
	 */
	public abstract void setLogSlowQueries(boolean flag);

	public abstract void setMaintainTimeStats(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param sizeInBytes
	 *            The maxQuerySizeToLog to set.
	 */
	public abstract void setMaxQuerySizeToLog(int sizeInBytes);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setMaxReconnects(int property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setMaxRows(int property);

	/**
	 * Sets the number of queries that metadata can be cached if caching is
	 * enabled.
	 * 
	 * @param value
	 *            the number of queries to cache metadata for.
	 */
	public abstract void setMetadataCacheSize(int value);

	/**
	 * @param noDatetimeStringSync
	 *            The noDatetimeStringSync to set.
	 */
	public abstract void setNoDatetimeStringSync(boolean flag);

	public abstract void setNullCatalogMeansCurrent(boolean value);

	public abstract void setNullNamePatternMatchesAll(boolean value);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param size
	 *            The packetDebugBufferSize to set.
	 */
	public abstract void setPacketDebugBufferSize(int size);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setParanoid(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setPedantic(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param cacheSize
	 *            The preparedStatementCacheSize to set.
	 */
	public abstract void setPreparedStatementCacheSize(int cacheSize);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param cacheSqlLimit
	 *            The preparedStatementCacheSqlLimit to set.
	 */
	public abstract void setPreparedStatementCacheSqlLimit(int cacheSqlLimit);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setProfileSql(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The profileSQL to set.
	 */
	public abstract void setProfileSQL(boolean flag);

	/**
	 * @param propertiesTransform
	 *            The propertiesTransform to set.
	 */
	public abstract void setPropertiesTransform(String value);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setQueriesBeforeRetryMaster(int property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setReconnectAtTxEnd(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setRelaxAutoCommit(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param millis
	 *            The reportMetricsIntervalMillis to set.
	 */
	public abstract void setReportMetricsIntervalMillis(int millis);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setRequireSSL(boolean property);

	public abstract void setRetainStatementAfterResultSetClose(boolean flag);

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
	 * @param runningCTS13
	 *            The runningCTS13 to set.
	 */
	public abstract void setRunningCTS13(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setSecondsBeforeRetryMaster(int property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 *            DOCUMENT ME!
	 */
	public abstract void setServerTimezone(String property);

	/**
	 * @param sessionVariables
	 *            The sessionVariables to set.
	 */
	public abstract void setSessionVariables(String variables);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param millis
	 *            The slowQueryThresholdMillis to set.
	 */
	public abstract void setSlowQueryThresholdMillis(int millis);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setSocketFactoryClassName(String property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setSocketTimeout(int property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setStrictFloatingPoint(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setStrictUpdates(boolean property);

	/**
	 * @param tinyInt1isBit
	 *            The tinyInt1isBit to set.
	 */
	public abstract void setTinyInt1isBit(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The logProtocol to set.
	 */
	public abstract void setTraceProtocol(boolean flag);

	public abstract void setTransformedBitIsBoolean(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setUseCompression(boolean property);

	/**
	 * @param useFastIntParsing
	 *            The useFastIntParsing to set.
	 */
	public abstract void setUseFastIntParsing(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
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
	 * @param useOldUTF8Behavior
	 *            The useOldUTF8Behavior to set.
	 */
	public abstract void setUseOldUTF8Behavior(boolean flag);

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
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The detectServerPreparedStmts to set.
	 */
	public abstract void setUseServerPreparedStmts(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The useSqlStateCodes to set.
	 */
	public abstract void setUseSqlStateCodes(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setUseSSL(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setUseStreamLengthsInPrepStmts(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setUseTimezone(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param property
	 */
	public abstract void setUseUltraDevWorkAround(boolean property);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The useUnbufferedInput to set.
	 */
	public abstract void setUseUnbufferedInput(boolean flag);

	/**
	 * DOCUMENT ME!
	 * 
	 * @param flag
	 *            The useUnicode to set.
	 */
	public abstract void setUseUnicode(boolean flag);

	/**
	 * Sets whether or not the driver advises of proper usage.
	 * 
	 * @param useUsageAdvisorFlag
	 *            whether or not the driver advises of proper usage.
	 */
	public abstract void setUseUsageAdvisor(boolean useUsageAdvisorFlag);

	public abstract void setYearIsDateType(boolean flag);

	/**
	 * @param zeroDateTimeBehavior
	 *            The zeroDateTimeBehavior to set.
	 */
	public abstract void setZeroDateTimeBehavior(String behavior);

	/**
	 * DOCUMENT ME!
	 * 
	 * @return Returns the useUnbufferedInput.
	 */
	public abstract boolean useUnbufferedInput();

	public abstract boolean getUseCursorFetch();

	public abstract void setUseCursorFetch(boolean flag);

	public abstract boolean getOverrideSupportsIntegrityEnhancementFacility();

	public abstract void setOverrideSupportsIntegrityEnhancementFacility(
			boolean flag);

	public abstract boolean getNoTimezoneConversionForTimeType();

	public abstract void setNoTimezoneConversionForTimeType(boolean flag);

	public abstract boolean getUseJDBCCompliantTimezoneShift();

	public abstract void setUseJDBCCompliantTimezoneShift(boolean flag);

	public abstract boolean getAutoClosePStmtStreams();

	public abstract void setAutoClosePStmtStreams(boolean flag);

	public abstract boolean getProcessEscapeCodesForPrepStmts();

	public abstract void setProcessEscapeCodesForPrepStmts(boolean flag);

	public abstract boolean getUseGmtMillisForDatetimes();

	public abstract void setUseGmtMillisForDatetimes(boolean flag);

	public abstract boolean getDumpMetadataOnColumnNotFound();

	public abstract void setDumpMetadataOnColumnNotFound(boolean flag);

	public abstract String getResourceId();

	public abstract void setResourceId(String resourceId);

	public abstract boolean getRewriteBatchedStatements();

	public abstract void setRewriteBatchedStatements(boolean flag);

	public abstract boolean getJdbcCompliantTruncationForReads();

	public abstract void setJdbcCompliantTruncationForReads(
			boolean jdbcCompliantTruncationForReads);

	public abstract boolean getUseJvmCharsetConverters();

	public abstract void setUseJvmCharsetConverters(boolean flag);

	public abstract boolean getPinGlobalTxToPhysicalConnection();

	public abstract void setPinGlobalTxToPhysicalConnection(boolean flag);

	public abstract void setGatherPerfMetrics(boolean flag);

	public abstract boolean getGatherPerfMetrics();

	public abstract void setUltraDevHack(boolean flag);

	public abstract boolean getUltraDevHack();

	public abstract void setInteractiveClient(boolean property);

	public abstract void setSocketFactory(String name);

	public abstract String getSocketFactory();

	public abstract void setUseServerPrepStmts(boolean flag);

	public abstract boolean getUseServerPrepStmts();

	public abstract void setCacheCallableStmts(boolean flag);

	public abstract boolean getCacheCallableStmts();

	public abstract void setCachePrepStmts(boolean flag);

	public abstract boolean getCachePrepStmts();

	public abstract void setCallableStmtCacheSize(int cacheSize);

	public abstract int getCallableStmtCacheSize();

	public abstract void setPrepStmtCacheSize(int cacheSize);

	public abstract int getPrepStmtCacheSize();

	public abstract void setPrepStmtCacheSqlLimit(int sqlLimit);

	public abstract int getPrepStmtCacheSqlLimit();

	public abstract boolean getNoAccessToProcedureBodies();

	public abstract void setNoAccessToProcedureBodies(boolean flag);

	public abstract boolean getUseOldAliasMetadataBehavior();

	public abstract void setUseOldAliasMetadataBehavior(boolean flag);

	public abstract String getClientCertificateKeyStorePassword();

	public abstract void setClientCertificateKeyStorePassword(String value);

	public abstract String getClientCertificateKeyStoreType();

	public abstract void setClientCertificateKeyStoreType(String value);

	public abstract String getClientCertificateKeyStoreUrl();

	public abstract void setClientCertificateKeyStoreUrl(String value);

	public abstract String getTrustCertificateKeyStorePassword();

	public abstract void setTrustCertificateKeyStorePassword(String value);

	public abstract String getTrustCertificateKeyStoreType();

	public abstract void setTrustCertificateKeyStoreType(String value);

	public abstract String getTrustCertificateKeyStoreUrl();

	public abstract void setTrustCertificateKeyStoreUrl(String value);

	public abstract boolean getUseSSPSCompatibleTimezoneShift();

	public abstract void setUseSSPSCompatibleTimezoneShift(boolean flag);

	public abstract boolean getTreatUtilDateAsTimestamp();

	public abstract void setTreatUtilDateAsTimestamp(boolean flag);

	public abstract boolean getUseFastDateParsing();

	public abstract void setUseFastDateParsing(boolean flag);

	public abstract String getLocalSocketAddress();

	public abstract void setLocalSocketAddress(String address);

	public abstract void setUseConfigs(String configs);

	public abstract String getUseConfigs();

	public abstract boolean getGenerateSimpleParameterMetadata();

	public abstract void setGenerateSimpleParameterMetadata(boolean flag);

	public abstract boolean getLogXaCommands();

	public abstract void setLogXaCommands(boolean flag);

	public abstract int getResultSetSizeThreshold();

	public abstract void setResultSetSizeThreshold(int threshold);

	public abstract int getNetTimeoutForStreamingResults();

	public abstract void setNetTimeoutForStreamingResults(int value);

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
	
	public abstract String getLoadBalanceStrategy();

	public abstract void setLoadBalanceStrategy(String strategy);
	
	public abstract boolean getTcpNoDelay();

	public abstract void setTcpNoDelay(boolean flag);

	public abstract boolean getTcpKeepAlive();

	public abstract void setTcpKeepAlive(boolean flag);

	public abstract int getTcpRcvBuf();

	public abstract void setTcpRcvBuf(int bufSize);

	public abstract int getTcpSndBuf();
	
	public abstract void setTcpSndBuf(int bufSize);

	public abstract int getTcpTrafficClass();

	public abstract void setTcpTrafficClass(int classFlags);
	
	public abstract boolean getUseNanosForElapsedTime();

	public abstract void setUseNanosForElapsedTime(boolean flag);

	public abstract long getSlowQueryThresholdNanos();

	public abstract void setSlowQueryThresholdNanos(long nanos);
	
	public abstract String getStatementInterceptors();

	public abstract void setStatementInterceptors(String value);
	
	public abstract boolean getUseDirectRowUnpack();

	public abstract void setUseDirectRowUnpack(boolean flag);
	
	public abstract String getLargeRowSizeThreshold();

	public abstract void setLargeRowSizeThreshold(String value);
	
	public abstract boolean getUseBlobToStoreUTF8OutsideBMP();

	public abstract void setUseBlobToStoreUTF8OutsideBMP(boolean flag);
	
	public abstract String getUtf8OutsideBmpExcludedColumnNamePattern();

	public abstract void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern);

	public abstract String getUtf8OutsideBmpIncludedColumnNamePattern();

	public abstract void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern);
	
	public abstract boolean getIncludeInnodbStatusInDeadlockExceptions();
	
	public abstract void setIncludeInnodbStatusInDeadlockExceptions(boolean flag);
	
	public abstract boolean getBlobsAreStrings();

	public abstract void setBlobsAreStrings(boolean flag);

    public abstract boolean getFunctionsNeverReturnBlobs();

    public abstract void setFunctionsNeverReturnBlobs(boolean flag);
    
	public abstract boolean getAutoSlowLog();

	public abstract void setAutoSlowLog(boolean flag);
	
	public abstract String getConnectionLifecycleInterceptors();

	public abstract void setConnectionLifecycleInterceptors(String interceptors);
	
	public abstract String getProfilerEventHandler();

	public abstract  void setProfilerEventHandler(String handler);
	
	public boolean getVerifyServerCertificate();

	public abstract void setVerifyServerCertificate(boolean flag);
	
	public abstract boolean getUseLegacyDatetimeCode();

	public abstract void setUseLegacyDatetimeCode(boolean flag);
	
	public abstract int getSelfDestructOnPingSecondsLifetime();

	public abstract void setSelfDestructOnPingSecondsLifetime(int seconds);

	public abstract int getSelfDestructOnPingMaxOperations();

	public abstract void setSelfDestructOnPingMaxOperations(int maxOperations);
	
	public abstract boolean getUseColumnNamesInFindColumn();

	public abstract void setUseColumnNamesInFindColumn(boolean flag);
	
	public abstract boolean getUseLocalTransactionState();

	public abstract void setUseLocalTransactionState(boolean flag);
	
	public abstract boolean getCompensateOnDuplicateKeyUpdateCounts();

	public abstract void setCompensateOnDuplicateKeyUpdateCounts(boolean flag);
	
	public abstract void setUseAffectedRows(boolean flag);

	public abstract boolean getUseAffectedRows();
	
	public abstract void setPasswordCharacterEncoding(String characterSet);

	public abstract String getPasswordCharacterEncoding();
	
	public abstract int getLoadBalanceBlacklistTimeout();

	public abstract void setLoadBalanceBlacklistTimeout(int loadBalanceBlacklistTimeout);
	
	public abstract void setRetriesAllDown(int retriesAllDown);
	
	public abstract int getRetriesAllDown();

	public ExceptionInterceptor getExceptionInterceptor();
	
	public abstract void setExceptionInterceptors(String exceptionInterceptors);

	public abstract String getExceptionInterceptors();
	

	public abstract boolean getQueryTimeoutKillsConnection();

	public abstract void setQueryTimeoutKillsConnection(boolean queryTimeoutKillsConnection);
}
