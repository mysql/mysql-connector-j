package com.mysql.jdbc;

import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Timer;

import com.mysql.jdbc.log.Log;

public class LoadBalancedMySQLConnection implements MySQLConnection {
	
	private LoadBalancingConnectionProxy proxy;
	
	protected MySQLConnection getActiveMySQLConnection(){
		return this.proxy.currentConn;
	}
	
	public LoadBalancedMySQLConnection(LoadBalancingConnectionProxy proxy){
		this.proxy = proxy;
	}

	public void abortInternal() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().abortInternal();
	}

	public void changeUser(String userName, String newPassword)
			throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().changeUser(userName, newPassword);
	}

	public void checkClosed() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().checkClosed();
	}

	public void clearHasTriedMaster() {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().clearHasTriedMaster();
	}

	public void clearWarnings() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().clearWarnings();
	}


	public PreparedStatement clientPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().clientPrepareStatement(sql, resultSetType, resultSetConcurrency,
				resultSetHoldability);
	}

	public PreparedStatement clientPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().clientPrepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	public PreparedStatement clientPrepareStatement(String sql,
			int autoGenKeyIndex) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().clientPrepareStatement(sql, autoGenKeyIndex);
	}

	public PreparedStatement clientPrepareStatement(String sql,
			int[] autoGenKeyIndexes) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().clientPrepareStatement(sql, autoGenKeyIndexes);
	}

	public PreparedStatement clientPrepareStatement(String sql,
			String[] autoGenKeyColNames) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().clientPrepareStatement(sql, autoGenKeyColNames);
	}

	public PreparedStatement clientPrepareStatement(String sql)
			throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().clientPrepareStatement(sql);
	}

	public synchronized void close() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().close();
	}

	public void commit() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().commit();
	}

	public void createNewIO(boolean isForReconnect) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().createNewIO(isForReconnect);
	}

	public Statement createStatement() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().createStatement();
	}

	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().createStatement(resultSetType, resultSetConcurrency,
				resultSetHoldability);
	}

	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().createStatement(resultSetType, resultSetConcurrency);
	}

	public void dumpTestcaseQuery(String query) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().dumpTestcaseQuery(query);
	}

	public Connection duplicate() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().duplicate();
	}

	public ResultSetInternalMethods execSQL(StatementImpl callingStatement,
			String sql, int maxRows, Buffer packet, int resultSetType,
			int resultSetConcurrency, boolean streamResults, String catalog,
			Field[] cachedMetadata, boolean isBatch) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().execSQL(callingStatement, sql, maxRows, packet, resultSetType,
				resultSetConcurrency, streamResults, catalog, cachedMetadata, isBatch);
	}

	public ResultSetInternalMethods execSQL(StatementImpl callingStatement,
			String sql, int maxRows, Buffer packet, int resultSetType,
			int resultSetConcurrency, boolean streamResults, String catalog,
			Field[] cachedMetadata) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().execSQL(callingStatement, sql, maxRows, packet, resultSetType,
				resultSetConcurrency, streamResults, catalog, cachedMetadata);
	}

	public String extractSqlFromPacket(String possibleSqlQuery,
			Buffer queryPacket, int endOfQueryPacketPosition)
			throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().extractSqlFromPacket(possibleSqlQuery, queryPacket,
				endOfQueryPacketPosition);
	}




	public String exposeAsXml() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().exposeAsXml();
	}

	public boolean getAllowLoadLocalInfile() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getAllowLoadLocalInfile();
	}

	public boolean getAllowMultiQueries() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getAllowMultiQueries();
	}

	public boolean getAllowNanAndInf() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getAllowNanAndInf();
	}

	public boolean getAllowUrlInLocalInfile() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getAllowUrlInLocalInfile();
	}

	public boolean getAlwaysSendSetIsolation() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getAlwaysSendSetIsolation();
	}

	public boolean getAutoClosePStmtStreams() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getAutoClosePStmtStreams();
	}

	public boolean getAutoDeserialize() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getAutoDeserialize();
	}

	public boolean getAutoGenerateTestcaseScript() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getAutoGenerateTestcaseScript();
	}

	public boolean getAutoReconnectForPools() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getAutoReconnectForPools();
	}

	public boolean getAutoSlowLog() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getAutoSlowLog();
	}

	public int getBlobSendChunkSize() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getBlobSendChunkSize();
	}

	public boolean getBlobsAreStrings() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getBlobsAreStrings();
	}

	public boolean getCacheCallableStatements() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCacheCallableStatements();
	}

	public boolean getCacheCallableStmts() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCacheCallableStmts();
	}

	public boolean getCachePrepStmts() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCachePrepStmts();
	}

	public boolean getCachePreparedStatements() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCachePreparedStatements();
	}

	public boolean getCacheResultSetMetadata() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCacheResultSetMetadata();
	}

	public boolean getCacheServerConfiguration() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCacheServerConfiguration();
	}

	public int getCallableStatementCacheSize() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCallableStatementCacheSize();
	}

	public int getCallableStmtCacheSize() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCallableStmtCacheSize();
	}

	public boolean getCapitalizeTypeNames() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCapitalizeTypeNames();
	}

	public String getCharacterSetResults() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCharacterSetResults();
	}

	public String getClientCertificateKeyStorePassword() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getClientCertificateKeyStorePassword();
	}

	public String getClientCertificateKeyStoreType() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getClientCertificateKeyStoreType();
	}

	public String getClientCertificateKeyStoreUrl() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getClientCertificateKeyStoreUrl();
	}

	public String getClientInfoProvider() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getClientInfoProvider();
	}

	public String getClobCharacterEncoding() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getClobCharacterEncoding();
	}

	public boolean getClobberStreamingResults() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getClobberStreamingResults();
	}

	public boolean getCompensateOnDuplicateKeyUpdateCounts() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCompensateOnDuplicateKeyUpdateCounts();
	}

	public int getConnectTimeout() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getConnectTimeout();
	}

	public String getConnectionCollation() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getConnectionCollation();
	}

	public String getConnectionLifecycleInterceptors() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getConnectionLifecycleInterceptors();
	}

	public boolean getContinueBatchOnError() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getContinueBatchOnError();
	}

	public boolean getCreateDatabaseIfNotExist() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCreateDatabaseIfNotExist();
	}

	public int getDefaultFetchSize() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getDefaultFetchSize();
	}

	public boolean getDontTrackOpenResources() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getDontTrackOpenResources();
	}

	public boolean getDumpMetadataOnColumnNotFound() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getDumpMetadataOnColumnNotFound();
	}

	public boolean getDumpQueriesOnException() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getDumpQueriesOnException();
	}

	public boolean getDynamicCalendars() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getDynamicCalendars();
	}

	public boolean getElideSetAutoCommits() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getElideSetAutoCommits();
	}

	public boolean getEmptyStringsConvertToZero() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getEmptyStringsConvertToZero();
	}

	public boolean getEmulateLocators() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getEmulateLocators();
	}

	public boolean getEmulateUnsupportedPstmts() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getEmulateUnsupportedPstmts();
	}

	public boolean getEnablePacketDebug() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getEnablePacketDebug();
	}

	public boolean getEnableQueryTimeouts() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getEnableQueryTimeouts();
	}

	public String getEncoding() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getEncoding();
	}

	public String getExceptionInterceptors() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getExceptionInterceptors();
	}

	public boolean getExplainSlowQueries() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getExplainSlowQueries();
	}

	public boolean getFailOverReadOnly() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getFailOverReadOnly();
	}

	public boolean getFunctionsNeverReturnBlobs() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getFunctionsNeverReturnBlobs();
	}

	public boolean getGatherPerfMetrics() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getGatherPerfMetrics();
	}

	public boolean getGatherPerformanceMetrics() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getGatherPerformanceMetrics();
	}

	public boolean getGenerateSimpleParameterMetadata() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getGenerateSimpleParameterMetadata();
	}


	public boolean getIgnoreNonTxTables() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getIgnoreNonTxTables();
	}

	public boolean getIncludeInnodbStatusInDeadlockExceptions() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getIncludeInnodbStatusInDeadlockExceptions();
	}

	public int getInitialTimeout() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getInitialTimeout();
	}

	public boolean getInteractiveClient() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getInteractiveClient();
	}

	public boolean getIsInteractiveClient() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getIsInteractiveClient();
	}

	public boolean getJdbcCompliantTruncation() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getJdbcCompliantTruncation();
	}

	public boolean getJdbcCompliantTruncationForReads() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getJdbcCompliantTruncationForReads();
	}

	public String getLargeRowSizeThreshold() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getLargeRowSizeThreshold();
	}

	public int getLoadBalanceBlacklistTimeout() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getLoadBalanceBlacklistTimeout();
	}

	public int getLoadBalancePingTimeout() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getLoadBalancePingTimeout();
	}

	public String getLoadBalanceStrategy() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getLoadBalanceStrategy();
	}

	public boolean getLoadBalanceValidateConnectionOnSwapServer() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getLoadBalanceValidateConnectionOnSwapServer();
	}

	public String getLocalSocketAddress() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getLocalSocketAddress();
	}

	public int getLocatorFetchBufferSize() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getLocatorFetchBufferSize();
	}

	public boolean getLogSlowQueries() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getLogSlowQueries();
	}

	public boolean getLogXaCommands() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getLogXaCommands();
	}

	public String getLogger() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getLogger();
	}

	public String getLoggerClassName() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getLoggerClassName();
	}

	public boolean getMaintainTimeStats() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getMaintainTimeStats();
	}

	public int getMaxAllowedPacket() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getMaxAllowedPacket();
	}

	public int getMaxQuerySizeToLog() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getMaxQuerySizeToLog();
	}

	public int getMaxReconnects() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getMaxReconnects();
	}

	public int getMaxRows() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getMaxRows();
	}

	public int getMetadataCacheSize() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getMetadataCacheSize();
	}

	public int getNetTimeoutForStreamingResults() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getNetTimeoutForStreamingResults();
	}

	public boolean getNoAccessToProcedureBodies() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getNoAccessToProcedureBodies();
	}

	public boolean getNoDatetimeStringSync() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getNoDatetimeStringSync();
	}

	public boolean getNoTimezoneConversionForTimeType() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getNoTimezoneConversionForTimeType();
	}

	public boolean getNullCatalogMeansCurrent() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getNullCatalogMeansCurrent();
	}

	public boolean getNullNamePatternMatchesAll() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getNullNamePatternMatchesAll();
	}

	public boolean getOverrideSupportsIntegrityEnhancementFacility() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getOverrideSupportsIntegrityEnhancementFacility();
	}

	public int getPacketDebugBufferSize() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getPacketDebugBufferSize();
	}

	public boolean getPadCharsWithSpace() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getPadCharsWithSpace();
	}

	public boolean getParanoid() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getParanoid();
	}

	public String getPasswordCharacterEncoding() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getPasswordCharacterEncoding();
	}

	public boolean getPedantic() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getPedantic();
	}

	public boolean getPinGlobalTxToPhysicalConnection() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getPinGlobalTxToPhysicalConnection();
	}

	public boolean getPopulateInsertRowWithDefaultValues() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getPopulateInsertRowWithDefaultValues();
	}

	public int getPrepStmtCacheSize() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getPrepStmtCacheSize();
	}

	public int getPrepStmtCacheSqlLimit() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getPrepStmtCacheSqlLimit();
	}

	public int getPreparedStatementCacheSize() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getPreparedStatementCacheSize();
	}

	public int getPreparedStatementCacheSqlLimit() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getPreparedStatementCacheSqlLimit();
	}

	public boolean getProcessEscapeCodesForPrepStmts() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getProcessEscapeCodesForPrepStmts();
	}

	public boolean getProfileSQL() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getProfileSQL();
	}

	public boolean getProfileSql() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getProfileSql();
	}

	public String getProfilerEventHandler() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getProfilerEventHandler();
	}

	public String getPropertiesTransform() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getPropertiesTransform();
	}

	public int getQueriesBeforeRetryMaster() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getQueriesBeforeRetryMaster();
	}

	public boolean getQueryTimeoutKillsConnection() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getQueryTimeoutKillsConnection();
	}

	public boolean getReconnectAtTxEnd() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getReconnectAtTxEnd();
	}

	public boolean getRelaxAutoCommit() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getRelaxAutoCommit();
	}

	public int getReportMetricsIntervalMillis() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getReportMetricsIntervalMillis();
	}

	public boolean getRequireSSL() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getRequireSSL();
	}

	public String getResourceId() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getResourceId();
	}

	public int getResultSetSizeThreshold() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getResultSetSizeThreshold();
	}

	public boolean getRetainStatementAfterResultSetClose() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getRetainStatementAfterResultSetClose();
	}

	public int getRetriesAllDown() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getRetriesAllDown();
	}

	public boolean getRewriteBatchedStatements() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getRewriteBatchedStatements();
	}

	public boolean getRollbackOnPooledClose() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getRollbackOnPooledClose();
	}

	public boolean getRoundRobinLoadBalance() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getRoundRobinLoadBalance();
	}

	public boolean getRunningCTS13() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getRunningCTS13();
	}

	public int getSecondsBeforeRetryMaster() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getSecondsBeforeRetryMaster();
	}

	public int getSelfDestructOnPingMaxOperations() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getSelfDestructOnPingMaxOperations();
	}

	public int getSelfDestructOnPingSecondsLifetime() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getSelfDestructOnPingSecondsLifetime();
	}

	public String getServerTimezone() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getServerTimezone();
	}

	public String getSessionVariables() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getSessionVariables();
	}

	public int getSlowQueryThresholdMillis() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getSlowQueryThresholdMillis();
	}

	public long getSlowQueryThresholdNanos() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getSlowQueryThresholdNanos();
	}

	public String getSocketFactory() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getSocketFactory();
	}

	public String getSocketFactoryClassName() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getSocketFactoryClassName();
	}

	public int getSocketTimeout() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getSocketTimeout();
	}

	public String getStatementInterceptors() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getStatementInterceptors();
	}

	public boolean getStrictFloatingPoint() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getStrictFloatingPoint();
	}

	public boolean getStrictUpdates() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getStrictUpdates();
	}

	public boolean getTcpKeepAlive() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTcpKeepAlive();
	}

	public boolean getTcpNoDelay() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTcpNoDelay();
	}

	public int getTcpRcvBuf() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTcpRcvBuf();
	}

	public int getTcpSndBuf() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTcpSndBuf();
	}

	public int getTcpTrafficClass() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTcpTrafficClass();
	}

	public boolean getTinyInt1isBit() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTinyInt1isBit();
	}

	public boolean getTraceProtocol() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTraceProtocol();
	}

	public boolean getTransformedBitIsBoolean() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTransformedBitIsBoolean();
	}

	public boolean getTreatUtilDateAsTimestamp() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTreatUtilDateAsTimestamp();
	}

	public String getTrustCertificateKeyStorePassword() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTrustCertificateKeyStorePassword();
	}

	public String getTrustCertificateKeyStoreType() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTrustCertificateKeyStoreType();
	}

	public String getTrustCertificateKeyStoreUrl() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTrustCertificateKeyStoreUrl();
	}

	public boolean getUltraDevHack() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUltraDevHack();
	}

	public boolean getUseAffectedRows() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseAffectedRows();
	}

	public boolean getUseBlobToStoreUTF8OutsideBMP() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseBlobToStoreUTF8OutsideBMP();
	}

	public boolean getUseColumnNamesInFindColumn() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseColumnNamesInFindColumn();
	}

	public boolean getUseCompression() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseCompression();
	}

	public String getUseConfigs() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseConfigs();
	}

	public boolean getUseCursorFetch() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseCursorFetch();
	}

	public boolean getUseDirectRowUnpack() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseDirectRowUnpack();
	}

	public boolean getUseDynamicCharsetInfo() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseDynamicCharsetInfo();
	}

	public boolean getUseFastDateParsing() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseFastDateParsing();
	}

	public boolean getUseFastIntParsing() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseFastIntParsing();
	}

	public boolean getUseGmtMillisForDatetimes() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseGmtMillisForDatetimes();
	}

	public boolean getUseHostsInPrivileges() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseHostsInPrivileges();
	}

	public boolean getUseInformationSchema() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseInformationSchema();
	}

	public boolean getUseJDBCCompliantTimezoneShift() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseJDBCCompliantTimezoneShift();
	}

	public boolean getUseJvmCharsetConverters() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseJvmCharsetConverters();
	}

	public boolean getUseLegacyDatetimeCode() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseLegacyDatetimeCode();
	}

	public boolean getUseLocalSessionState() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseLocalSessionState();
	}

	public boolean getUseLocalTransactionState() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseLocalTransactionState();
	}

	public boolean getUseNanosForElapsedTime() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseNanosForElapsedTime();
	}

	public boolean getUseOldAliasMetadataBehavior() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseOldAliasMetadataBehavior();
	}

	public boolean getUseOldUTF8Behavior() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseOldUTF8Behavior();
	}

	public boolean getUseOnlyServerErrorMessages() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseOnlyServerErrorMessages();
	}

	public boolean getUseReadAheadInput() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseReadAheadInput();
	}

	public boolean getUseSSL() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseSSL();
	}

	public boolean getUseSSPSCompatibleTimezoneShift() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseSSPSCompatibleTimezoneShift();
	}

	public boolean getUseServerPrepStmts() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseServerPrepStmts();
	}

	public boolean getUseServerPreparedStmts() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseServerPreparedStmts();
	}

	public boolean getUseSqlStateCodes() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseSqlStateCodes();
	}

	public boolean getUseStreamLengthsInPrepStmts() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseStreamLengthsInPrepStmts();
	}

	public boolean getUseTimezone() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseTimezone();
	}

	public boolean getUseUltraDevWorkAround() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseUltraDevWorkAround();
	}

	public boolean getUseUnbufferedInput() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseUnbufferedInput();
	}

	public boolean getUseUnicode() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseUnicode();
	}

	public boolean getUseUsageAdvisor() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUseUsageAdvisor();
	}

	public String getUtf8OutsideBmpExcludedColumnNamePattern() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUtf8OutsideBmpExcludedColumnNamePattern();
	}

	public String getUtf8OutsideBmpIncludedColumnNamePattern() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUtf8OutsideBmpIncludedColumnNamePattern();
	}

	public boolean getVerifyServerCertificate() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getVerifyServerCertificate();
	}

	public boolean getYearIsDateType() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getYearIsDateType();
	}

	public String getZeroDateTimeBehavior() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getZeroDateTimeBehavior();
	}


	public void setAllowLoadLocalInfile(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setAllowLoadLocalInfile(property);
	}

	public void setAllowMultiQueries(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setAllowMultiQueries(property);
	}

	public void setAllowNanAndInf(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setAllowNanAndInf(flag);
	}

	public void setAllowUrlInLocalInfile(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setAllowUrlInLocalInfile(flag);
	}

	public void setAlwaysSendSetIsolation(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setAlwaysSendSetIsolation(flag);
	}

	public void setAutoClosePStmtStreams(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setAutoClosePStmtStreams(flag);
	}

	public void setAutoDeserialize(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setAutoDeserialize(flag);
	}

	public void setAutoGenerateTestcaseScript(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setAutoGenerateTestcaseScript(flag);
	}

	public void setAutoReconnect(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setAutoReconnect(flag);
	}

	public void setAutoReconnectForConnectionPools(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setAutoReconnectForConnectionPools(property);
	}

	public void setAutoReconnectForPools(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setAutoReconnectForPools(flag);
	}

	public void setAutoSlowLog(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setAutoSlowLog(flag);
	}

	public void setBlobSendChunkSize(String value) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setBlobSendChunkSize(value);
	}

	public void setBlobsAreStrings(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setBlobsAreStrings(flag);
	}

	public void setCacheCallableStatements(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCacheCallableStatements(flag);
	}

	public void setCacheCallableStmts(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCacheCallableStmts(flag);
	}

	public void setCachePrepStmts(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCachePrepStmts(flag);
	}

	public void setCachePreparedStatements(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCachePreparedStatements(flag);
	}

	public void setCacheResultSetMetadata(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCacheResultSetMetadata(property);
	}

	public void setCacheServerConfiguration(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCacheServerConfiguration(flag);
	}

	public void setCallableStatementCacheSize(int size) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCallableStatementCacheSize(size);
	}

	public void setCallableStmtCacheSize(int cacheSize) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCallableStmtCacheSize(cacheSize);
	}

	public void setCapitalizeDBMDTypes(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCapitalizeDBMDTypes(property);
	}

	public void setCapitalizeTypeNames(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCapitalizeTypeNames(flag);
	}

	public void setCharacterEncoding(String encoding) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCharacterEncoding(encoding);
	}

	public void setCharacterSetResults(String characterSet) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCharacterSetResults(characterSet);
	}

	public void setClientCertificateKeyStorePassword(String value) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setClientCertificateKeyStorePassword(value);
	}

	public void setClientCertificateKeyStoreType(String value) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setClientCertificateKeyStoreType(value);
	}

	public void setClientCertificateKeyStoreUrl(String value) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setClientCertificateKeyStoreUrl(value);
	}

	public void setClientInfoProvider(String classname) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setClientInfoProvider(classname);
	}

	public void setClobCharacterEncoding(String encoding) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setClobCharacterEncoding(encoding);
	}

	public void setClobberStreamingResults(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setClobberStreamingResults(flag);
	}

	public void setCompensateOnDuplicateKeyUpdateCounts(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCompensateOnDuplicateKeyUpdateCounts(flag);
	}

	public void setConnectTimeout(int timeoutMs) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setConnectTimeout(timeoutMs);
	}

	public void setConnectionCollation(String collation) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setConnectionCollation(collation);
	}

	public void setConnectionLifecycleInterceptors(String interceptors) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setConnectionLifecycleInterceptors(interceptors);
	}

	public void setContinueBatchOnError(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setContinueBatchOnError(property);
	}

	public void setCreateDatabaseIfNotExist(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCreateDatabaseIfNotExist(flag);
	}

	public void setDefaultFetchSize(int n) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setDefaultFetchSize(n);
	}

	public void setDetectServerPreparedStmts(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setDetectServerPreparedStmts(property);
	}

	public void setDontTrackOpenResources(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setDontTrackOpenResources(flag);
	}

	public void setDumpMetadataOnColumnNotFound(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setDumpMetadataOnColumnNotFound(flag);
	}

	public void setDumpQueriesOnException(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setDumpQueriesOnException(flag);
	}

	public void setDynamicCalendars(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setDynamicCalendars(flag);
	}

	public void setElideSetAutoCommits(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setElideSetAutoCommits(flag);
	}

	public void setEmptyStringsConvertToZero(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setEmptyStringsConvertToZero(flag);
	}

	public void setEmulateLocators(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setEmulateLocators(property);
	}

	public void setEmulateUnsupportedPstmts(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setEmulateUnsupportedPstmts(flag);
	}

	public void setEnablePacketDebug(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setEnablePacketDebug(flag);
	}

	public void setEnableQueryTimeouts(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setEnableQueryTimeouts(flag);
	}

	public void setEncoding(String property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setEncoding(property);
	}

	public void setExceptionInterceptors(String exceptionInterceptors) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setExceptionInterceptors(exceptionInterceptors);
	}

	public void setExplainSlowQueries(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setExplainSlowQueries(flag);
	}

	public void setFailOverReadOnly(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setFailOverReadOnly(flag);
	}

	public void setFunctionsNeverReturnBlobs(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setFunctionsNeverReturnBlobs(flag);
	}

	public void setGatherPerfMetrics(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setGatherPerfMetrics(flag);
	}

	public void setGatherPerformanceMetrics(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setGatherPerformanceMetrics(flag);
	}

	public void setGenerateSimpleParameterMetadata(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setGenerateSimpleParameterMetadata(flag);
	}


	public void setHoldResultsOpenOverStatementClose(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setHoldResultsOpenOverStatementClose(flag);
	}

	public void setIgnoreNonTxTables(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setIgnoreNonTxTables(property);
	}

	public void setIncludeInnodbStatusInDeadlockExceptions(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setIncludeInnodbStatusInDeadlockExceptions(flag);
	}

	public void setInitialTimeout(int property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setInitialTimeout(property);
	}

	public void setInteractiveClient(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setInteractiveClient(property);
	}

	public void setIsInteractiveClient(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setIsInteractiveClient(property);
	}

	public void setJdbcCompliantTruncation(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setJdbcCompliantTruncation(flag);
	}

	public void setJdbcCompliantTruncationForReads(
			boolean jdbcCompliantTruncationForReads) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setJdbcCompliantTruncationForReads(jdbcCompliantTruncationForReads);
	}

	public void setLargeRowSizeThreshold(String value) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setLargeRowSizeThreshold(value);
	}

	public void setLoadBalanceBlacklistTimeout(int loadBalanceBlacklistTimeout) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setLoadBalanceBlacklistTimeout(loadBalanceBlacklistTimeout);
	}

	public void setLoadBalancePingTimeout(int loadBalancePingTimeout) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setLoadBalancePingTimeout(loadBalancePingTimeout);
	}

	public void setLoadBalanceStrategy(String strategy) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setLoadBalanceStrategy(strategy);
	}

	public void setLoadBalanceValidateConnectionOnSwapServer(
			boolean loadBalanceValidateConnectionOnSwapServer) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setLoadBalanceValidateConnectionOnSwapServer(loadBalanceValidateConnectionOnSwapServer);
	}

	public void setLocalSocketAddress(String address) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setLocalSocketAddress(address);
	}

	public void setLocatorFetchBufferSize(String value) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setLocatorFetchBufferSize(value);
	}

	public void setLogSlowQueries(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setLogSlowQueries(flag);
	}

	public void setLogXaCommands(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setLogXaCommands(flag);
	}

	public void setLogger(String property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setLogger(property);
	}

	public void setLoggerClassName(String className) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setLoggerClassName(className);
	}

	public void setMaintainTimeStats(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setMaintainTimeStats(flag);
	}

	public void setMaxQuerySizeToLog(int sizeInBytes) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setMaxQuerySizeToLog(sizeInBytes);
	}

	public void setMaxReconnects(int property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setMaxReconnects(property);
	}

	public void setMaxRows(int property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setMaxRows(property);
	}

	public void setMetadataCacheSize(int value) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setMetadataCacheSize(value);
	}

	public void setNetTimeoutForStreamingResults(int value) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setNetTimeoutForStreamingResults(value);
	}

	public void setNoAccessToProcedureBodies(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setNoAccessToProcedureBodies(flag);
	}

	public void setNoDatetimeStringSync(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setNoDatetimeStringSync(flag);
	}

	public void setNoTimezoneConversionForTimeType(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setNoTimezoneConversionForTimeType(flag);
	}

	public void setNullCatalogMeansCurrent(boolean value) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setNullCatalogMeansCurrent(value);
	}

	public void setNullNamePatternMatchesAll(boolean value) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setNullNamePatternMatchesAll(value);
	}

	public void setOverrideSupportsIntegrityEnhancementFacility(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setOverrideSupportsIntegrityEnhancementFacility(flag);
	}

	public void setPacketDebugBufferSize(int size) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setPacketDebugBufferSize(size);
	}

	public void setPadCharsWithSpace(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setPadCharsWithSpace(flag);
	}

	public void setParanoid(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setParanoid(property);
	}

	public void setPasswordCharacterEncoding(String characterSet) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setPasswordCharacterEncoding(characterSet);
	}

	public void setPedantic(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setPedantic(property);
	}

	public void setPinGlobalTxToPhysicalConnection(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setPinGlobalTxToPhysicalConnection(flag);
	}

	public void setPopulateInsertRowWithDefaultValues(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setPopulateInsertRowWithDefaultValues(flag);
	}

	public void setPrepStmtCacheSize(int cacheSize) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setPrepStmtCacheSize(cacheSize);
	}

	public void setPrepStmtCacheSqlLimit(int sqlLimit) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setPrepStmtCacheSqlLimit(sqlLimit);
	}

	public void setPreparedStatementCacheSize(int cacheSize) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setPreparedStatementCacheSize(cacheSize);
	}

	public void setPreparedStatementCacheSqlLimit(int cacheSqlLimit) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setPreparedStatementCacheSqlLimit(cacheSqlLimit);
	}

	public void setProcessEscapeCodesForPrepStmts(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setProcessEscapeCodesForPrepStmts(flag);
	}

	public void setProfileSQL(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setProfileSQL(flag);
	}

	public void setProfileSql(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setProfileSql(property);
	}

	public void setProfilerEventHandler(String handler) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setProfilerEventHandler(handler);
	}

	public void setPropertiesTransform(String value) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setPropertiesTransform(value);
	}

	public void setQueriesBeforeRetryMaster(int property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setQueriesBeforeRetryMaster(property);
	}

	public void setQueryTimeoutKillsConnection(
			boolean queryTimeoutKillsConnection) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setQueryTimeoutKillsConnection(queryTimeoutKillsConnection);
	}

	public void setReconnectAtTxEnd(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setReconnectAtTxEnd(property);
	}

	public void setRelaxAutoCommit(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setRelaxAutoCommit(property);
	}

	public void setReportMetricsIntervalMillis(int millis) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setReportMetricsIntervalMillis(millis);
	}

	public void setRequireSSL(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setRequireSSL(property);
	}

	public void setResourceId(String resourceId) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setResourceId(resourceId);
	}

	public void setResultSetSizeThreshold(int threshold) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setResultSetSizeThreshold(threshold);
	}

	public void setRetainStatementAfterResultSetClose(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setRetainStatementAfterResultSetClose(flag);
	}

	public void setRetriesAllDown(int retriesAllDown) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setRetriesAllDown(retriesAllDown);
	}

	public void setRewriteBatchedStatements(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setRewriteBatchedStatements(flag);
	}

	public void setRollbackOnPooledClose(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setRollbackOnPooledClose(flag);
	}

	public void setRoundRobinLoadBalance(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setRoundRobinLoadBalance(flag);
	}

	public void setRunningCTS13(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setRunningCTS13(flag);
	}

	public void setSecondsBeforeRetryMaster(int property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setSecondsBeforeRetryMaster(property);
	}

	public void setSelfDestructOnPingMaxOperations(int maxOperations) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setSelfDestructOnPingMaxOperations(maxOperations);
	}

	public void setSelfDestructOnPingSecondsLifetime(int seconds) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setSelfDestructOnPingSecondsLifetime(seconds);
	}

	public void setServerTimezone(String property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setServerTimezone(property);
	}

	public void setSessionVariables(String variables) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setSessionVariables(variables);
	}

	public void setSlowQueryThresholdMillis(int millis) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setSlowQueryThresholdMillis(millis);
	}

	public void setSlowQueryThresholdNanos(long nanos) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setSlowQueryThresholdNanos(nanos);
	}

	public void setSocketFactory(String name) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setSocketFactory(name);
	}

	public void setSocketFactoryClassName(String property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setSocketFactoryClassName(property);
	}

	public void setSocketTimeout(int property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setSocketTimeout(property);
	}

	public void setStatementInterceptors(String value) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setStatementInterceptors(value);
	}

	public void setStrictFloatingPoint(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setStrictFloatingPoint(property);
	}

	public void setStrictUpdates(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setStrictUpdates(property);
	}

	public void setTcpKeepAlive(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTcpKeepAlive(flag);
	}

	public void setTcpNoDelay(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTcpNoDelay(flag);
	}

	public void setTcpRcvBuf(int bufSize) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTcpRcvBuf(bufSize);
	}

	public void setTcpSndBuf(int bufSize) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTcpSndBuf(bufSize);
	}

	public void setTcpTrafficClass(int classFlags) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTcpTrafficClass(classFlags);
	}

	public void setTinyInt1isBit(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTinyInt1isBit(flag);
	}

	public void setTraceProtocol(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTraceProtocol(flag);
	}

	public void setTransformedBitIsBoolean(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTransformedBitIsBoolean(flag);
	}

	public void setTreatUtilDateAsTimestamp(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTreatUtilDateAsTimestamp(flag);
	}

	public void setTrustCertificateKeyStorePassword(String value) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTrustCertificateKeyStorePassword(value);
	}

	public void setTrustCertificateKeyStoreType(String value) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTrustCertificateKeyStoreType(value);
	}

	public void setTrustCertificateKeyStoreUrl(String value) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTrustCertificateKeyStoreUrl(value);
	}

	public void setUltraDevHack(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUltraDevHack(flag);
	}

	public void setUseAffectedRows(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseAffectedRows(flag);
	}

	public void setUseBlobToStoreUTF8OutsideBMP(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseBlobToStoreUTF8OutsideBMP(flag);
	}

	public void setUseColumnNamesInFindColumn(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseColumnNamesInFindColumn(flag);
	}

	public void setUseCompression(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseCompression(property);
	}

	public void setUseConfigs(String configs) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseConfigs(configs);
	}

	public void setUseCursorFetch(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseCursorFetch(flag);
	}

	public void setUseDirectRowUnpack(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseDirectRowUnpack(flag);
	}

	public void setUseDynamicCharsetInfo(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseDynamicCharsetInfo(flag);
	}

	public void setUseFastDateParsing(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseFastDateParsing(flag);
	}

	public void setUseFastIntParsing(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseFastIntParsing(flag);
	}

	public void setUseGmtMillisForDatetimes(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseGmtMillisForDatetimes(flag);
	}

	public void setUseHostsInPrivileges(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseHostsInPrivileges(property);
	}

	public void setUseInformationSchema(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseInformationSchema(flag);
	}

	public void setUseJDBCCompliantTimezoneShift(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseJDBCCompliantTimezoneShift(flag);
	}

	public void setUseJvmCharsetConverters(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseJvmCharsetConverters(flag);
	}

	public void setUseLegacyDatetimeCode(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseLegacyDatetimeCode(flag);
	}

	public void setUseLocalSessionState(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseLocalSessionState(flag);
	}

	public void setUseLocalTransactionState(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseLocalTransactionState(flag);
	}

	public void setUseNanosForElapsedTime(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseNanosForElapsedTime(flag);
	}

	public void setUseOldAliasMetadataBehavior(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseOldAliasMetadataBehavior(flag);
	}

	public void setUseOldUTF8Behavior(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseOldUTF8Behavior(flag);
	}

	public void setUseOnlyServerErrorMessages(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseOnlyServerErrorMessages(flag);
	}

	public void setUseReadAheadInput(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseReadAheadInput(flag);
	}

	public void setUseSSL(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseSSL(property);
	}

	public void setUseSSPSCompatibleTimezoneShift(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseSSPSCompatibleTimezoneShift(flag);
	}

	public void setUseServerPrepStmts(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseServerPrepStmts(flag);
	}

	public void setUseServerPreparedStmts(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseServerPreparedStmts(flag);
	}

	public void setUseSqlStateCodes(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseSqlStateCodes(flag);
	}

	public void setUseStreamLengthsInPrepStmts(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseStreamLengthsInPrepStmts(property);
	}

	public void setUseTimezone(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseTimezone(property);
	}

	public void setUseUltraDevWorkAround(boolean property) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseUltraDevWorkAround(property);
	}

	public void setUseUnbufferedInput(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseUnbufferedInput(flag);
	}

	public void setUseUnicode(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseUnicode(flag);
	}

	public void setUseUsageAdvisor(boolean useUsageAdvisorFlag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUseUsageAdvisor(useUsageAdvisorFlag);
	}

	public void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUtf8OutsideBmpExcludedColumnNamePattern(regexPattern);
	}

	public void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setUtf8OutsideBmpIncludedColumnNamePattern(regexPattern);
	}

	public void setVerifyServerCertificate(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setVerifyServerCertificate(flag);
	}

	public void setYearIsDateType(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setYearIsDateType(flag);
	}

	public void setZeroDateTimeBehavior(String behavior) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setZeroDateTimeBehavior(behavior);
	}


	public boolean useUnbufferedInput() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().useUnbufferedInput();
	}

	public StringBuffer generateConnectionCommentBlock(StringBuffer buf) {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().generateConnectionCommentBlock(buf);
	}

	public int getActiveStatementCount() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getActiveStatementCount();
	}

	public boolean getAutoCommit() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getAutoCommit();
	}

	public int getAutoIncrementIncrement() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getAutoIncrementIncrement();
	}

	public CachedResultSetMetaData getCachedMetaData(String sql) {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCachedMetaData(sql);
	}

	public Calendar getCalendarInstanceForSessionOrNew() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCalendarInstanceForSessionOrNew();
	}

	public synchronized Timer getCancelTimer() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCancelTimer();
	}

	public String getCatalog() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCatalog();
	}

	public String getCharacterSetMetadata() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCharacterSetMetadata();
	}

	public SingleByteCharsetConverter getCharsetConverter(
			String javaEncodingName) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCharsetConverter(javaEncodingName);
	}

	public String getCharsetNameForIndex(int charsetIndex) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getCharsetNameForIndex(charsetIndex);
	}


	public TimeZone getDefaultTimeZone() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getDefaultTimeZone();
	}

	public String getErrorMessageEncoding() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getErrorMessageEncoding();
	}

	public ExceptionInterceptor getExceptionInterceptor() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getExceptionInterceptor();
	}

	public int getHoldability() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getHoldability();
	}

	public String getHost() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getHost();
	}

	public long getId() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getId();
	}

	public long getIdleFor() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getIdleFor();
	}

	public MysqlIO getIO() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getIO();
	}

	public MySQLConnection getLoadBalanceSafeProxy() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getLoadBalanceSafeProxy();
	}

	public Log getLog() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getLog();
	}

	public int getMaxBytesPerChar(String javaCharsetName) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getMaxBytesPerChar(javaCharsetName);
	}

	public DatabaseMetaData getMetaData() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getMetaData();
	}

	public Statement getMetadataSafeStatement() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getMetadataSafeStatement();
	}

	public Object getMutex() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getMutex();
	}

	public int getNetBufferLength() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getNetBufferLength();
	}

	public Properties getProperties() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getProperties();
	}

	public boolean getRequiresEscapingEncoder() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getRequiresEscapingEncoder();
	}

	public String getServerCharacterEncoding() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getServerCharacterEncoding();
	}

	public int getServerMajorVersion() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getServerMajorVersion();
	}

	public int getServerMinorVersion() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getServerMinorVersion();
	}

	public int getServerSubMinorVersion() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getServerSubMinorVersion();
	}

	public TimeZone getServerTimezoneTZ() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getServerTimezoneTZ();
	}

	public String getServerVariable(String variableName) {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getServerVariable(variableName);
	}

	public String getServerVersion() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getServerVersion();
	}

	public Calendar getSessionLockedCalendar() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getSessionLockedCalendar();
	}

	public String getStatementComment() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getStatementComment();
	}

	public List getStatementInterceptorsInstances() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getStatementInterceptorsInstances();
	}

	public int getTransactionIsolation() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTransactionIsolation();
	}

	public synchronized Map getTypeMap() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getTypeMap();
	}

	public String getURL() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getURL();
	}

	public String getUser() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUser();
	}

	public Calendar getUtcCalendar() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getUtcCalendar();
	}

	public SQLWarning getWarnings() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().getWarnings();
	}

	public boolean hasSameProperties(Connection c) {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().hasSameProperties(c);
	}

	public boolean hasTriedMaster() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().hasTriedMaster();
	}

	public void incrementNumberOfPreparedExecutes() {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().incrementNumberOfPreparedExecutes();
	}

	public void incrementNumberOfPrepares() {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().incrementNumberOfPrepares();
	}

	public void incrementNumberOfResultSetsCreated() {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().incrementNumberOfResultSetsCreated();
	}

	public void initializeExtension(Extension ex) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().initializeExtension(ex);
	}

	public void initializeResultsMetadataFromCache(String sql,
			CachedResultSetMetaData cachedMetaData,
			ResultSetInternalMethods resultSet) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().initializeResultsMetadataFromCache(sql, cachedMetaData, resultSet);
	}

	public void initializeSafeStatementInterceptors() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().initializeSafeStatementInterceptors();
	}

	public synchronized boolean isAbonormallyLongQuery(long millisOrNanos) {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().isAbonormallyLongQuery(millisOrNanos);
	}

	public boolean isClientTzUTC() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().isClientTzUTC();
	}


	public boolean isCursorFetchEnabled() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().isCursorFetchEnabled();
	}

	public boolean isInGlobalTx() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().isInGlobalTx();
	}

	public synchronized boolean isMasterConnection() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().isMasterConnection();
	}

	public boolean isNoBackslashEscapesSet() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().isNoBackslashEscapesSet();
	}

	public boolean isReadInfoMsgEnabled() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().isReadInfoMsgEnabled();
	}

	public boolean isReadOnly() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().isReadOnly();
	}

	public boolean isRunningOnJDK13() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().isRunningOnJDK13();
	}

	public synchronized boolean isSameResource(Connection otherConnection) {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().isSameResource(otherConnection);
	}

	public boolean isServerTzUTC() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().isServerTzUTC();
	}

	public boolean lowerCaseTableNames() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().lowerCaseTableNames();
	}

	public void maxRowsChanged(com.mysql.jdbc.Statement stmt) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().maxRowsChanged(stmt);
	}

	public String nativeSQL(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().nativeSQL(sql);
	}

	public boolean parserKnowsUnicode() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().parserKnowsUnicode();
	}

	public void ping() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().ping();
	}

	public void pingInternal(boolean checkForClosedConnection, int timeoutMillis)
			throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().pingInternal(checkForClosedConnection, timeoutMillis);
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().prepareCall(sql, resultSetType, resultSetConcurrency,
				resultSetHoldability);
	}

	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	public CallableStatement prepareCall(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().prepareCall(sql);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().prepareStatement(sql, resultSetType, resultSetConcurrency,
				resultSetHoldability);
	}

	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	public PreparedStatement prepareStatement(String sql, int autoGenKeyIndex)
			throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().prepareStatement(sql, autoGenKeyIndex);
	}

	public PreparedStatement prepareStatement(String sql,
			int[] autoGenKeyIndexes) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().prepareStatement(sql, autoGenKeyIndexes);
	}

	public PreparedStatement prepareStatement(String sql,
			String[] autoGenKeyColNames) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().prepareStatement(sql, autoGenKeyColNames);
	}

	public PreparedStatement prepareStatement(String sql) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().prepareStatement(sql);
	}

	public void realClose(boolean calledExplicitly, boolean issueRollback,
			boolean skipLocalTeardown, Throwable reason) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().realClose(calledExplicitly, issueRollback, skipLocalTeardown, reason);
	}

	public void recachePreparedStatement(ServerPreparedStatement pstmt)
			throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().recachePreparedStatement(pstmt);
	}

	public void registerQueryExecutionTime(long queryTimeMs) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().registerQueryExecutionTime(queryTimeMs);
	}

	public void registerStatement(com.mysql.jdbc.Statement stmt) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().registerStatement(stmt);
	}

	public void releaseSavepoint(Savepoint arg0) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().releaseSavepoint(arg0);
	}

	public void reportNumberOfTablesAccessed(int numTablesAccessed) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().reportNumberOfTablesAccessed(numTablesAccessed);
	}

	public synchronized void reportQueryTime(long millisOrNanos) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().reportQueryTime(millisOrNanos);
	}

	public void resetServerState() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().resetServerState();
	}

	public void rollback() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().rollback();
	}

	public void rollback(Savepoint savepoint) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().rollback(savepoint);
	}

	public PreparedStatement serverPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().serverPrepareStatement(sql, resultSetType, resultSetConcurrency,
				resultSetHoldability);
	}

	public PreparedStatement serverPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().serverPrepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	public PreparedStatement serverPrepareStatement(String sql,
			int autoGenKeyIndex) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().serverPrepareStatement(sql, autoGenKeyIndex);
	}

	public PreparedStatement serverPrepareStatement(String sql,
			int[] autoGenKeyIndexes) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().serverPrepareStatement(sql, autoGenKeyIndexes);
	}

	public PreparedStatement serverPrepareStatement(String sql,
			String[] autoGenKeyColNames) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().serverPrepareStatement(sql, autoGenKeyColNames);
	}

	public PreparedStatement serverPrepareStatement(String sql)
			throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().serverPrepareStatement(sql);
	}

	public boolean serverSupportsConvertFn() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().serverSupportsConvertFn();
	}

	public void setAutoCommit(boolean autoCommitFlag) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setAutoCommit(autoCommitFlag);
	}

	public void setCatalog(String catalog) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setCatalog(catalog);
	}

	public synchronized void setFailedOver(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setFailedOver(flag);
	}

	public void setHoldability(int arg0) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setHoldability(arg0);
	}

	public void setInGlobalTx(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setInGlobalTx(flag);
	}

	public void setPreferSlaveDuringFailover(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setPreferSlaveDuringFailover(flag);
	}

	public void setProxy(MySQLConnection proxy) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setProxy(proxy);
	}

	public void setReadInfoMsgEnabled(boolean flag) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setReadInfoMsgEnabled(flag);
	}

	public void setReadOnly(boolean readOnlyFlag) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setReadOnly(readOnlyFlag);
	}

	public void setReadOnlyInternal(boolean readOnlyFlag) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setReadOnlyInternal(readOnlyFlag);
	}

	public Savepoint setSavepoint() throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().setSavepoint();
	}

	public synchronized Savepoint setSavepoint(String name) throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().setSavepoint(name);
	}

	public void setStatementComment(String comment) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setStatementComment(comment);
	}

	public synchronized void setTransactionIsolation(int level)
			throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTransactionIsolation(level);
	}

	public synchronized void setTypeMap(Map map) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().setTypeMap(map);
	}

	public void shutdownServer() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().shutdownServer();
	}

	public boolean storesLowerCaseTableName() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().storesLowerCaseTableName();
	}

	public boolean supportsIsolationLevel() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().supportsIsolationLevel();
	}

	public boolean supportsQuotedIdentifiers() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().supportsQuotedIdentifiers();
	}

	public boolean supportsTransactions() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().supportsTransactions();
	}

	public void throwConnectionClosedException() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().throwConnectionClosedException();
	}

	public void transactionBegun() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().transactionBegun();
	}

	public void transactionCompleted() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().transactionCompleted();
	}

	public void unregisterStatement(com.mysql.jdbc.Statement stmt) {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().unregisterStatement(stmt);
	}

	public void unSafeStatementInterceptors() throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().unSafeStatementInterceptors();
	}

	public void unsetMaxRows(com.mysql.jdbc.Statement stmt) throws SQLException {
		// TODO Auto-generated method stub
		getActiveMySQLConnection().unsetMaxRows(stmt);
	}

	public boolean useAnsiQuotedIdentifiers() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().useAnsiQuotedIdentifiers();
	}

	public boolean useMaxRows() {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().useMaxRows();
	}

	public boolean versionMeetsMinimum(int major, int minor, int subminor)
			throws SQLException {
		// TODO Auto-generated method stub
		return getActiveMySQLConnection().versionMeetsMinimum(major, minor, subminor);
	}

	public boolean isClosed() throws SQLException {
		return getActiveMySQLConnection().isClosed();
	}

	public boolean getHoldResultsOpenOverStatementClose() {
		return getActiveMySQLConnection().getHoldResultsOpenOverStatementClose();
	}
	

}
