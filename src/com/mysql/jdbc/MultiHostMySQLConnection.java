/*
  Copyright (c) 2015, Oracle and/or its affiliates. All rights reserved.

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
import java.util.concurrent.Executor;

import com.mysql.jdbc.log.Log;
import com.mysql.jdbc.profiler.ProfilerEventHandler;

public class MultiHostMySQLConnection implements MySQLConnection {

    protected MultiHostConnectionProxy proxy;

    public MultiHostMySQLConnection(MultiHostConnectionProxy proxy) {
        this.proxy = proxy;
    }

    public MultiHostConnectionProxy getProxy() {
        return this.proxy;
    }

    protected MySQLConnection getActiveMySQLConnection() {
        synchronized (this.proxy) {
            return this.proxy.currentConnection;
        }
    }

    public void abortInternal() throws SQLException {
        getActiveMySQLConnection().abortInternal();
    }

    public void changeUser(String userName, String newPassword) throws SQLException {
        getActiveMySQLConnection().changeUser(userName, newPassword);
    }

    public void checkClosed() throws SQLException {
        getActiveMySQLConnection().checkClosed();
    }

    public void clearHasTriedMaster() {
        getActiveMySQLConnection().clearHasTriedMaster();
    }

    public void clearWarnings() throws SQLException {
        getActiveMySQLConnection().clearWarnings();
    }

    public PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return getActiveMySQLConnection().clientPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return getActiveMySQLConnection().clientPrepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public PreparedStatement clientPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        return getActiveMySQLConnection().clientPrepareStatement(sql, autoGenKeyIndex);
    }

    public PreparedStatement clientPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        return getActiveMySQLConnection().clientPrepareStatement(sql, autoGenKeyIndexes);
    }

    public PreparedStatement clientPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        return getActiveMySQLConnection().clientPrepareStatement(sql, autoGenKeyColNames);
    }

    public PreparedStatement clientPrepareStatement(String sql) throws SQLException {
        return getActiveMySQLConnection().clientPrepareStatement(sql);
    }

    public void close() throws SQLException {
        getActiveMySQLConnection().close();
    }

    public void commit() throws SQLException {
        getActiveMySQLConnection().commit();
    }

    public void createNewIO(boolean isForReconnect) throws SQLException {
        getActiveMySQLConnection().createNewIO(isForReconnect);
    }

    public Statement createStatement() throws SQLException {
        return getActiveMySQLConnection().createStatement();
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return getActiveMySQLConnection().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return getActiveMySQLConnection().createStatement(resultSetType, resultSetConcurrency);
    }

    public void dumpTestcaseQuery(String query) {
        getActiveMySQLConnection().dumpTestcaseQuery(query);
    }

    public Connection duplicate() throws SQLException {
        return getActiveMySQLConnection().duplicate();
    }

    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, Buffer packet, int resultSetType,
            int resultSetConcurrency, boolean streamResults, String catalog, Field[] cachedMetadata, boolean isBatch) throws SQLException {
        return getActiveMySQLConnection().execSQL(callingStatement, sql, maxRows, packet, resultSetType, resultSetConcurrency, streamResults, catalog,
                cachedMetadata, isBatch);
    }

    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, Buffer packet, int resultSetType,
            int resultSetConcurrency, boolean streamResults, String catalog, Field[] cachedMetadata) throws SQLException {
        return getActiveMySQLConnection().execSQL(callingStatement, sql, maxRows, packet, resultSetType, resultSetConcurrency, streamResults, catalog,
                cachedMetadata);
    }

    public String extractSqlFromPacket(String possibleSqlQuery, Buffer queryPacket, int endOfQueryPacketPosition) throws SQLException {
        return getActiveMySQLConnection().extractSqlFromPacket(possibleSqlQuery, queryPacket, endOfQueryPacketPosition);
    }

    public String exposeAsXml() throws SQLException {
        return getActiveMySQLConnection().exposeAsXml();
    }

    public boolean getAllowLoadLocalInfile() {
        return getActiveMySQLConnection().getAllowLoadLocalInfile();
    }

    public boolean getAllowMultiQueries() {
        return getActiveMySQLConnection().getAllowMultiQueries();
    }

    public boolean getAllowNanAndInf() {
        return getActiveMySQLConnection().getAllowNanAndInf();
    }

    public boolean getAllowUrlInLocalInfile() {
        return getActiveMySQLConnection().getAllowUrlInLocalInfile();
    }

    public boolean getAlwaysSendSetIsolation() {
        return getActiveMySQLConnection().getAlwaysSendSetIsolation();
    }

    public boolean getAutoClosePStmtStreams() {
        return getActiveMySQLConnection().getAutoClosePStmtStreams();
    }

    public boolean getAutoDeserialize() {
        return getActiveMySQLConnection().getAutoDeserialize();
    }

    public boolean getAutoGenerateTestcaseScript() {
        return getActiveMySQLConnection().getAutoGenerateTestcaseScript();
    }

    public boolean getAutoReconnectForPools() {
        return getActiveMySQLConnection().getAutoReconnectForPools();
    }

    public boolean getAutoSlowLog() {
        return getActiveMySQLConnection().getAutoSlowLog();
    }

    public int getBlobSendChunkSize() {
        return getActiveMySQLConnection().getBlobSendChunkSize();
    }

    public boolean getBlobsAreStrings() {
        return getActiveMySQLConnection().getBlobsAreStrings();
    }

    public boolean getCacheCallableStatements() {
        return getActiveMySQLConnection().getCacheCallableStatements();
    }

    public boolean getCacheCallableStmts() {
        return getActiveMySQLConnection().getCacheCallableStmts();
    }

    public boolean getCachePrepStmts() {
        return getActiveMySQLConnection().getCachePrepStmts();
    }

    public boolean getCachePreparedStatements() {
        return getActiveMySQLConnection().getCachePreparedStatements();
    }

    public boolean getCacheResultSetMetadata() {
        return getActiveMySQLConnection().getCacheResultSetMetadata();
    }

    public boolean getCacheServerConfiguration() {
        return getActiveMySQLConnection().getCacheServerConfiguration();
    }

    public int getCallableStatementCacheSize() {
        return getActiveMySQLConnection().getCallableStatementCacheSize();
    }

    public int getCallableStmtCacheSize() {
        return getActiveMySQLConnection().getCallableStmtCacheSize();
    }

    public boolean getCapitalizeTypeNames() {
        return getActiveMySQLConnection().getCapitalizeTypeNames();
    }

    public String getCharacterSetResults() {
        return getActiveMySQLConnection().getCharacterSetResults();
    }

    public String getClientCertificateKeyStorePassword() {
        return getActiveMySQLConnection().getClientCertificateKeyStorePassword();
    }

    public String getClientCertificateKeyStoreType() {
        return getActiveMySQLConnection().getClientCertificateKeyStoreType();
    }

    public String getClientCertificateKeyStoreUrl() {
        return getActiveMySQLConnection().getClientCertificateKeyStoreUrl();
    }

    public String getClientInfoProvider() {
        return getActiveMySQLConnection().getClientInfoProvider();
    }

    public String getClobCharacterEncoding() {
        return getActiveMySQLConnection().getClobCharacterEncoding();
    }

    public boolean getClobberStreamingResults() {
        return getActiveMySQLConnection().getClobberStreamingResults();
    }

    public boolean getCompensateOnDuplicateKeyUpdateCounts() {
        return getActiveMySQLConnection().getCompensateOnDuplicateKeyUpdateCounts();
    }

    public int getConnectTimeout() {
        return getActiveMySQLConnection().getConnectTimeout();
    }

    public String getConnectionCollation() {
        return getActiveMySQLConnection().getConnectionCollation();
    }

    public String getConnectionLifecycleInterceptors() {
        return getActiveMySQLConnection().getConnectionLifecycleInterceptors();
    }

    public boolean getContinueBatchOnError() {
        return getActiveMySQLConnection().getContinueBatchOnError();
    }

    public boolean getCreateDatabaseIfNotExist() {
        return getActiveMySQLConnection().getCreateDatabaseIfNotExist();
    }

    public int getDefaultFetchSize() {
        return getActiveMySQLConnection().getDefaultFetchSize();
    }

    public boolean getDontTrackOpenResources() {
        return getActiveMySQLConnection().getDontTrackOpenResources();
    }

    public boolean getDumpMetadataOnColumnNotFound() {
        return getActiveMySQLConnection().getDumpMetadataOnColumnNotFound();
    }

    public boolean getDumpQueriesOnException() {
        return getActiveMySQLConnection().getDumpQueriesOnException();
    }

    public boolean getDynamicCalendars() {
        return getActiveMySQLConnection().getDynamicCalendars();
    }

    public boolean getElideSetAutoCommits() {
        return getActiveMySQLConnection().getElideSetAutoCommits();
    }

    public boolean getEmptyStringsConvertToZero() {
        return getActiveMySQLConnection().getEmptyStringsConvertToZero();
    }

    public boolean getEmulateLocators() {
        return getActiveMySQLConnection().getEmulateLocators();
    }

    public boolean getEmulateUnsupportedPstmts() {
        return getActiveMySQLConnection().getEmulateUnsupportedPstmts();
    }

    public boolean getEnablePacketDebug() {
        return getActiveMySQLConnection().getEnablePacketDebug();
    }

    public boolean getEnableQueryTimeouts() {
        return getActiveMySQLConnection().getEnableQueryTimeouts();
    }

    public String getEncoding() {
        return getActiveMySQLConnection().getEncoding();
    }

    public String getExceptionInterceptors() {
        return getActiveMySQLConnection().getExceptionInterceptors();
    }

    public boolean getExplainSlowQueries() {
        return getActiveMySQLConnection().getExplainSlowQueries();
    }

    public boolean getFailOverReadOnly() {
        return getActiveMySQLConnection().getFailOverReadOnly();
    }

    public boolean getFunctionsNeverReturnBlobs() {
        return getActiveMySQLConnection().getFunctionsNeverReturnBlobs();
    }

    public boolean getGatherPerfMetrics() {
        return getActiveMySQLConnection().getGatherPerfMetrics();
    }

    public boolean getGatherPerformanceMetrics() {
        return getActiveMySQLConnection().getGatherPerformanceMetrics();
    }

    public boolean getGenerateSimpleParameterMetadata() {
        return getActiveMySQLConnection().getGenerateSimpleParameterMetadata();
    }

    public boolean getIgnoreNonTxTables() {
        return getActiveMySQLConnection().getIgnoreNonTxTables();
    }

    public boolean getIncludeInnodbStatusInDeadlockExceptions() {
        return getActiveMySQLConnection().getIncludeInnodbStatusInDeadlockExceptions();
    }

    public int getInitialTimeout() {
        return getActiveMySQLConnection().getInitialTimeout();
    }

    public boolean getInteractiveClient() {
        return getActiveMySQLConnection().getInteractiveClient();
    }

    public boolean getIsInteractiveClient() {
        return getActiveMySQLConnection().getIsInteractiveClient();
    }

    public boolean getJdbcCompliantTruncation() {
        return getActiveMySQLConnection().getJdbcCompliantTruncation();
    }

    public boolean getJdbcCompliantTruncationForReads() {
        return getActiveMySQLConnection().getJdbcCompliantTruncationForReads();
    }

    public String getLargeRowSizeThreshold() {
        return getActiveMySQLConnection().getLargeRowSizeThreshold();
    }

    public int getLoadBalanceBlacklistTimeout() {
        return getActiveMySQLConnection().getLoadBalanceBlacklistTimeout();
    }

    public int getLoadBalancePingTimeout() {
        return getActiveMySQLConnection().getLoadBalancePingTimeout();
    }

    public String getLoadBalanceStrategy() {
        return getActiveMySQLConnection().getLoadBalanceStrategy();
    }

    public boolean getLoadBalanceValidateConnectionOnSwapServer() {
        return getActiveMySQLConnection().getLoadBalanceValidateConnectionOnSwapServer();
    }

    public String getLocalSocketAddress() {
        return getActiveMySQLConnection().getLocalSocketAddress();
    }

    public int getLocatorFetchBufferSize() {
        return getActiveMySQLConnection().getLocatorFetchBufferSize();
    }

    public boolean getLogSlowQueries() {
        return getActiveMySQLConnection().getLogSlowQueries();
    }

    public boolean getLogXaCommands() {
        return getActiveMySQLConnection().getLogXaCommands();
    }

    public String getLogger() {
        return getActiveMySQLConnection().getLogger();
    }

    public String getLoggerClassName() {
        return getActiveMySQLConnection().getLoggerClassName();
    }

    public boolean getMaintainTimeStats() {
        return getActiveMySQLConnection().getMaintainTimeStats();
    }

    public int getMaxAllowedPacket() {
        return getActiveMySQLConnection().getMaxAllowedPacket();
    }

    public int getMaxQuerySizeToLog() {
        return getActiveMySQLConnection().getMaxQuerySizeToLog();
    }

    public int getMaxReconnects() {
        return getActiveMySQLConnection().getMaxReconnects();
    }

    public int getMaxRows() {
        return getActiveMySQLConnection().getMaxRows();
    }

    public int getMetadataCacheSize() {
        return getActiveMySQLConnection().getMetadataCacheSize();
    }

    public int getNetTimeoutForStreamingResults() {
        return getActiveMySQLConnection().getNetTimeoutForStreamingResults();
    }

    public boolean getNoAccessToProcedureBodies() {
        return getActiveMySQLConnection().getNoAccessToProcedureBodies();
    }

    public boolean getNoDatetimeStringSync() {
        return getActiveMySQLConnection().getNoDatetimeStringSync();
    }

    public boolean getNoTimezoneConversionForTimeType() {
        return getActiveMySQLConnection().getNoTimezoneConversionForTimeType();
    }

    public boolean getNoTimezoneConversionForDateType() {
        return getActiveMySQLConnection().getNoTimezoneConversionForDateType();
    }

    public boolean getCacheDefaultTimezone() {
        return getActiveMySQLConnection().getCacheDefaultTimezone();
    }

    public boolean getNullCatalogMeansCurrent() {
        return getActiveMySQLConnection().getNullCatalogMeansCurrent();
    }

    public boolean getNullNamePatternMatchesAll() {
        return getActiveMySQLConnection().getNullNamePatternMatchesAll();
    }

    public boolean getOverrideSupportsIntegrityEnhancementFacility() {
        return getActiveMySQLConnection().getOverrideSupportsIntegrityEnhancementFacility();
    }

    public int getPacketDebugBufferSize() {
        return getActiveMySQLConnection().getPacketDebugBufferSize();
    }

    public boolean getPadCharsWithSpace() {
        return getActiveMySQLConnection().getPadCharsWithSpace();
    }

    public boolean getParanoid() {
        return getActiveMySQLConnection().getParanoid();
    }

    public String getPasswordCharacterEncoding() {
        return getActiveMySQLConnection().getPasswordCharacterEncoding();
    }

    public boolean getPedantic() {
        return getActiveMySQLConnection().getPedantic();
    }

    public boolean getPinGlobalTxToPhysicalConnection() {
        return getActiveMySQLConnection().getPinGlobalTxToPhysicalConnection();
    }

    public boolean getPopulateInsertRowWithDefaultValues() {
        return getActiveMySQLConnection().getPopulateInsertRowWithDefaultValues();
    }

    public int getPrepStmtCacheSize() {
        return getActiveMySQLConnection().getPrepStmtCacheSize();
    }

    public int getPrepStmtCacheSqlLimit() {
        return getActiveMySQLConnection().getPrepStmtCacheSqlLimit();
    }

    public int getPreparedStatementCacheSize() {
        return getActiveMySQLConnection().getPreparedStatementCacheSize();
    }

    public int getPreparedStatementCacheSqlLimit() {
        return getActiveMySQLConnection().getPreparedStatementCacheSqlLimit();
    }

    public boolean getProcessEscapeCodesForPrepStmts() {
        return getActiveMySQLConnection().getProcessEscapeCodesForPrepStmts();
    }

    public boolean getProfileSQL() {
        return getActiveMySQLConnection().getProfileSQL();
    }

    public boolean getProfileSql() {
        return getActiveMySQLConnection().getProfileSql();
    }

    public String getProfilerEventHandler() {
        return getActiveMySQLConnection().getProfilerEventHandler();
    }

    public String getPropertiesTransform() {
        return getActiveMySQLConnection().getPropertiesTransform();
    }

    public int getQueriesBeforeRetryMaster() {
        return getActiveMySQLConnection().getQueriesBeforeRetryMaster();
    }

    public boolean getQueryTimeoutKillsConnection() {
        return getActiveMySQLConnection().getQueryTimeoutKillsConnection();
    }

    public boolean getReconnectAtTxEnd() {
        return getActiveMySQLConnection().getReconnectAtTxEnd();
    }

    public boolean getRelaxAutoCommit() {
        return getActiveMySQLConnection().getRelaxAutoCommit();
    }

    public int getReportMetricsIntervalMillis() {
        return getActiveMySQLConnection().getReportMetricsIntervalMillis();
    }

    public boolean getRequireSSL() {
        return getActiveMySQLConnection().getRequireSSL();
    }

    public String getResourceId() {
        return getActiveMySQLConnection().getResourceId();
    }

    public int getResultSetSizeThreshold() {
        return getActiveMySQLConnection().getResultSetSizeThreshold();
    }

    public boolean getRetainStatementAfterResultSetClose() {
        return getActiveMySQLConnection().getRetainStatementAfterResultSetClose();
    }

    public int getRetriesAllDown() {
        return getActiveMySQLConnection().getRetriesAllDown();
    }

    public boolean getRewriteBatchedStatements() {
        return getActiveMySQLConnection().getRewriteBatchedStatements();
    }

    public boolean getRollbackOnPooledClose() {
        return getActiveMySQLConnection().getRollbackOnPooledClose();
    }

    public boolean getRoundRobinLoadBalance() {
        return getActiveMySQLConnection().getRoundRobinLoadBalance();
    }

    public boolean getRunningCTS13() {
        return getActiveMySQLConnection().getRunningCTS13();
    }

    public int getSecondsBeforeRetryMaster() {
        return getActiveMySQLConnection().getSecondsBeforeRetryMaster();
    }

    public int getSelfDestructOnPingMaxOperations() {
        return getActiveMySQLConnection().getSelfDestructOnPingMaxOperations();
    }

    public int getSelfDestructOnPingSecondsLifetime() {
        return getActiveMySQLConnection().getSelfDestructOnPingSecondsLifetime();
    }

    public String getServerTimezone() {
        return getActiveMySQLConnection().getServerTimezone();
    }

    public String getSessionVariables() {
        return getActiveMySQLConnection().getSessionVariables();
    }

    public int getSlowQueryThresholdMillis() {
        return getActiveMySQLConnection().getSlowQueryThresholdMillis();
    }

    public long getSlowQueryThresholdNanos() {
        return getActiveMySQLConnection().getSlowQueryThresholdNanos();
    }

    public String getSocketFactory() {
        return getActiveMySQLConnection().getSocketFactory();
    }

    public String getSocketFactoryClassName() {
        return getActiveMySQLConnection().getSocketFactoryClassName();
    }

    public int getSocketTimeout() {
        return getActiveMySQLConnection().getSocketTimeout();
    }

    public String getStatementInterceptors() {
        return getActiveMySQLConnection().getStatementInterceptors();
    }

    public boolean getStrictFloatingPoint() {
        return getActiveMySQLConnection().getStrictFloatingPoint();
    }

    public boolean getStrictUpdates() {
        return getActiveMySQLConnection().getStrictUpdates();
    }

    public boolean getTcpKeepAlive() {
        return getActiveMySQLConnection().getTcpKeepAlive();
    }

    public boolean getTcpNoDelay() {
        return getActiveMySQLConnection().getTcpNoDelay();
    }

    public int getTcpRcvBuf() {
        return getActiveMySQLConnection().getTcpRcvBuf();
    }

    public int getTcpSndBuf() {
        return getActiveMySQLConnection().getTcpSndBuf();
    }

    public int getTcpTrafficClass() {
        return getActiveMySQLConnection().getTcpTrafficClass();
    }

    public boolean getTinyInt1isBit() {
        return getActiveMySQLConnection().getTinyInt1isBit();
    }

    public boolean getTraceProtocol() {
        return getActiveMySQLConnection().getTraceProtocol();
    }

    public boolean getTransformedBitIsBoolean() {
        return getActiveMySQLConnection().getTransformedBitIsBoolean();
    }

    public boolean getTreatUtilDateAsTimestamp() {
        return getActiveMySQLConnection().getTreatUtilDateAsTimestamp();
    }

    public String getTrustCertificateKeyStorePassword() {
        return getActiveMySQLConnection().getTrustCertificateKeyStorePassword();
    }

    public String getTrustCertificateKeyStoreType() {
        return getActiveMySQLConnection().getTrustCertificateKeyStoreType();
    }

    public String getTrustCertificateKeyStoreUrl() {
        return getActiveMySQLConnection().getTrustCertificateKeyStoreUrl();
    }

    public boolean getUltraDevHack() {
        return getActiveMySQLConnection().getUltraDevHack();
    }

    public boolean getUseAffectedRows() {
        return getActiveMySQLConnection().getUseAffectedRows();
    }

    public boolean getUseBlobToStoreUTF8OutsideBMP() {
        return getActiveMySQLConnection().getUseBlobToStoreUTF8OutsideBMP();
    }

    public boolean getUseColumnNamesInFindColumn() {
        return getActiveMySQLConnection().getUseColumnNamesInFindColumn();
    }

    public boolean getUseCompression() {
        return getActiveMySQLConnection().getUseCompression();
    }

    public String getUseConfigs() {
        return getActiveMySQLConnection().getUseConfigs();
    }

    public boolean getUseCursorFetch() {
        return getActiveMySQLConnection().getUseCursorFetch();
    }

    public boolean getUseDirectRowUnpack() {
        return getActiveMySQLConnection().getUseDirectRowUnpack();
    }

    public boolean getUseDynamicCharsetInfo() {
        return getActiveMySQLConnection().getUseDynamicCharsetInfo();
    }

    public boolean getUseFastDateParsing() {
        return getActiveMySQLConnection().getUseFastDateParsing();
    }

    public boolean getUseFastIntParsing() {
        return getActiveMySQLConnection().getUseFastIntParsing();
    }

    public boolean getUseGmtMillisForDatetimes() {
        return getActiveMySQLConnection().getUseGmtMillisForDatetimes();
    }

    public boolean getUseHostsInPrivileges() {
        return getActiveMySQLConnection().getUseHostsInPrivileges();
    }

    public boolean getUseInformationSchema() {
        return getActiveMySQLConnection().getUseInformationSchema();
    }

    public boolean getUseJDBCCompliantTimezoneShift() {
        return getActiveMySQLConnection().getUseJDBCCompliantTimezoneShift();
    }

    public boolean getUseJvmCharsetConverters() {
        return getActiveMySQLConnection().getUseJvmCharsetConverters();
    }

    public boolean getUseLegacyDatetimeCode() {
        return getActiveMySQLConnection().getUseLegacyDatetimeCode();
    }

    public boolean getSendFractionalSeconds() {
        return getActiveMySQLConnection().getSendFractionalSeconds();
    }

    public boolean getUseLocalSessionState() {
        return getActiveMySQLConnection().getUseLocalSessionState();
    }

    public boolean getUseLocalTransactionState() {
        return getActiveMySQLConnection().getUseLocalTransactionState();
    }

    public boolean getUseNanosForElapsedTime() {
        return getActiveMySQLConnection().getUseNanosForElapsedTime();
    }

    public boolean getUseOldAliasMetadataBehavior() {
        return getActiveMySQLConnection().getUseOldAliasMetadataBehavior();
    }

    public boolean getUseOldUTF8Behavior() {
        return getActiveMySQLConnection().getUseOldUTF8Behavior();
    }

    public boolean getUseOnlyServerErrorMessages() {
        return getActiveMySQLConnection().getUseOnlyServerErrorMessages();
    }

    public boolean getUseReadAheadInput() {
        return getActiveMySQLConnection().getUseReadAheadInput();
    }

    public boolean getUseSSL() {
        return getActiveMySQLConnection().getUseSSL();
    }

    public boolean getUseSSPSCompatibleTimezoneShift() {
        return getActiveMySQLConnection().getUseSSPSCompatibleTimezoneShift();
    }

    public boolean getUseServerPrepStmts() {
        return getActiveMySQLConnection().getUseServerPrepStmts();
    }

    public boolean getUseServerPreparedStmts() {
        return getActiveMySQLConnection().getUseServerPreparedStmts();
    }

    public boolean getUseSqlStateCodes() {
        return getActiveMySQLConnection().getUseSqlStateCodes();
    }

    public boolean getUseStreamLengthsInPrepStmts() {
        return getActiveMySQLConnection().getUseStreamLengthsInPrepStmts();
    }

    public boolean getUseTimezone() {
        return getActiveMySQLConnection().getUseTimezone();
    }

    public boolean getUseUltraDevWorkAround() {
        return getActiveMySQLConnection().getUseUltraDevWorkAround();
    }

    public boolean getUseUnbufferedInput() {
        return getActiveMySQLConnection().getUseUnbufferedInput();
    }

    public boolean getUseUnicode() {
        return getActiveMySQLConnection().getUseUnicode();
    }

    public boolean getUseUsageAdvisor() {
        return getActiveMySQLConnection().getUseUsageAdvisor();
    }

    public String getUtf8OutsideBmpExcludedColumnNamePattern() {
        return getActiveMySQLConnection().getUtf8OutsideBmpExcludedColumnNamePattern();
    }

    public String getUtf8OutsideBmpIncludedColumnNamePattern() {
        return getActiveMySQLConnection().getUtf8OutsideBmpIncludedColumnNamePattern();
    }

    public boolean getVerifyServerCertificate() {
        return getActiveMySQLConnection().getVerifyServerCertificate();
    }

    public boolean getYearIsDateType() {
        return getActiveMySQLConnection().getYearIsDateType();
    }

    public String getZeroDateTimeBehavior() {
        return getActiveMySQLConnection().getZeroDateTimeBehavior();
    }

    public void setAllowLoadLocalInfile(boolean property) {
        getActiveMySQLConnection().setAllowLoadLocalInfile(property);
    }

    public void setAllowMultiQueries(boolean property) {
        getActiveMySQLConnection().setAllowMultiQueries(property);
    }

    public void setAllowNanAndInf(boolean flag) {
        getActiveMySQLConnection().setAllowNanAndInf(flag);
    }

    public void setAllowUrlInLocalInfile(boolean flag) {
        getActiveMySQLConnection().setAllowUrlInLocalInfile(flag);
    }

    public void setAlwaysSendSetIsolation(boolean flag) {
        getActiveMySQLConnection().setAlwaysSendSetIsolation(flag);
    }

    public void setAutoClosePStmtStreams(boolean flag) {
        getActiveMySQLConnection().setAutoClosePStmtStreams(flag);
    }

    public void setAutoDeserialize(boolean flag) {
        getActiveMySQLConnection().setAutoDeserialize(flag);
    }

    public void setAutoGenerateTestcaseScript(boolean flag) {
        getActiveMySQLConnection().setAutoGenerateTestcaseScript(flag);
    }

    public void setAutoReconnect(boolean flag) {
        getActiveMySQLConnection().setAutoReconnect(flag);
    }

    public void setAutoReconnectForConnectionPools(boolean property) {
        getActiveMySQLConnection().setAutoReconnectForConnectionPools(property);
    }

    public void setAutoReconnectForPools(boolean flag) {
        getActiveMySQLConnection().setAutoReconnectForPools(flag);
    }

    public void setAutoSlowLog(boolean flag) {
        getActiveMySQLConnection().setAutoSlowLog(flag);
    }

    public void setBlobSendChunkSize(String value) throws SQLException {
        getActiveMySQLConnection().setBlobSendChunkSize(value);
    }

    public void setBlobsAreStrings(boolean flag) {
        getActiveMySQLConnection().setBlobsAreStrings(flag);
    }

    public void setCacheCallableStatements(boolean flag) {
        getActiveMySQLConnection().setCacheCallableStatements(flag);
    }

    public void setCacheCallableStmts(boolean flag) {
        getActiveMySQLConnection().setCacheCallableStmts(flag);
    }

    public void setCachePrepStmts(boolean flag) {
        getActiveMySQLConnection().setCachePrepStmts(flag);
    }

    public void setCachePreparedStatements(boolean flag) {
        getActiveMySQLConnection().setCachePreparedStatements(flag);
    }

    public void setCacheResultSetMetadata(boolean property) {
        getActiveMySQLConnection().setCacheResultSetMetadata(property);
    }

    public void setCacheServerConfiguration(boolean flag) {
        getActiveMySQLConnection().setCacheServerConfiguration(flag);
    }

    public void setCallableStatementCacheSize(int size) throws SQLException {
        getActiveMySQLConnection().setCallableStatementCacheSize(size);
    }

    public void setCallableStmtCacheSize(int cacheSize) throws SQLException {
        getActiveMySQLConnection().setCallableStmtCacheSize(cacheSize);
    }

    public void setCapitalizeDBMDTypes(boolean property) {
        getActiveMySQLConnection().setCapitalizeDBMDTypes(property);
    }

    public void setCapitalizeTypeNames(boolean flag) {
        getActiveMySQLConnection().setCapitalizeTypeNames(flag);
    }

    public void setCharacterEncoding(String encoding) {
        getActiveMySQLConnection().setCharacterEncoding(encoding);
    }

    public void setCharacterSetResults(String characterSet) {
        getActiveMySQLConnection().setCharacterSetResults(characterSet);
    }

    public void setClientCertificateKeyStorePassword(String value) {
        getActiveMySQLConnection().setClientCertificateKeyStorePassword(value);
    }

    public void setClientCertificateKeyStoreType(String value) {
        getActiveMySQLConnection().setClientCertificateKeyStoreType(value);
    }

    public void setClientCertificateKeyStoreUrl(String value) {
        getActiveMySQLConnection().setClientCertificateKeyStoreUrl(value);
    }

    public void setClientInfoProvider(String classname) {
        getActiveMySQLConnection().setClientInfoProvider(classname);
    }

    public void setClobCharacterEncoding(String encoding) {
        getActiveMySQLConnection().setClobCharacterEncoding(encoding);
    }

    public void setClobberStreamingResults(boolean flag) {
        getActiveMySQLConnection().setClobberStreamingResults(flag);
    }

    public void setCompensateOnDuplicateKeyUpdateCounts(boolean flag) {
        getActiveMySQLConnection().setCompensateOnDuplicateKeyUpdateCounts(flag);
    }

    public void setConnectTimeout(int timeoutMs) throws SQLException {
        getActiveMySQLConnection().setConnectTimeout(timeoutMs);
    }

    public void setConnectionCollation(String collation) {
        getActiveMySQLConnection().setConnectionCollation(collation);
    }

    public void setConnectionLifecycleInterceptors(String interceptors) {
        getActiveMySQLConnection().setConnectionLifecycleInterceptors(interceptors);
    }

    public void setContinueBatchOnError(boolean property) {
        getActiveMySQLConnection().setContinueBatchOnError(property);
    }

    public void setCreateDatabaseIfNotExist(boolean flag) {
        getActiveMySQLConnection().setCreateDatabaseIfNotExist(flag);
    }

    public void setDefaultFetchSize(int n) throws SQLException {
        getActiveMySQLConnection().setDefaultFetchSize(n);
    }

    public void setDetectServerPreparedStmts(boolean property) {
        getActiveMySQLConnection().setDetectServerPreparedStmts(property);
    }

    public void setDontTrackOpenResources(boolean flag) {
        getActiveMySQLConnection().setDontTrackOpenResources(flag);
    }

    public void setDumpMetadataOnColumnNotFound(boolean flag) {
        getActiveMySQLConnection().setDumpMetadataOnColumnNotFound(flag);
    }

    public void setDumpQueriesOnException(boolean flag) {
        getActiveMySQLConnection().setDumpQueriesOnException(flag);
    }

    public void setDynamicCalendars(boolean flag) {
        getActiveMySQLConnection().setDynamicCalendars(flag);
    }

    public void setElideSetAutoCommits(boolean flag) {
        getActiveMySQLConnection().setElideSetAutoCommits(flag);
    }

    public void setEmptyStringsConvertToZero(boolean flag) {
        getActiveMySQLConnection().setEmptyStringsConvertToZero(flag);
    }

    public void setEmulateLocators(boolean property) {
        getActiveMySQLConnection().setEmulateLocators(property);
    }

    public void setEmulateUnsupportedPstmts(boolean flag) {
        getActiveMySQLConnection().setEmulateUnsupportedPstmts(flag);
    }

    public void setEnablePacketDebug(boolean flag) {
        getActiveMySQLConnection().setEnablePacketDebug(flag);
    }

    public void setEnableQueryTimeouts(boolean flag) {
        getActiveMySQLConnection().setEnableQueryTimeouts(flag);
    }

    public void setEncoding(String property) {
        getActiveMySQLConnection().setEncoding(property);
    }

    public void setExceptionInterceptors(String exceptionInterceptors) {
        getActiveMySQLConnection().setExceptionInterceptors(exceptionInterceptors);
    }

    public void setExplainSlowQueries(boolean flag) {
        getActiveMySQLConnection().setExplainSlowQueries(flag);
    }

    public void setFailOverReadOnly(boolean flag) {
        getActiveMySQLConnection().setFailOverReadOnly(flag);
    }

    public void setFunctionsNeverReturnBlobs(boolean flag) {
        getActiveMySQLConnection().setFunctionsNeverReturnBlobs(flag);
    }

    public void setGatherPerfMetrics(boolean flag) {
        getActiveMySQLConnection().setGatherPerfMetrics(flag);
    }

    public void setGatherPerformanceMetrics(boolean flag) {
        getActiveMySQLConnection().setGatherPerformanceMetrics(flag);
    }

    public void setGenerateSimpleParameterMetadata(boolean flag) {
        getActiveMySQLConnection().setGenerateSimpleParameterMetadata(flag);
    }

    public void setHoldResultsOpenOverStatementClose(boolean flag) {
        getActiveMySQLConnection().setHoldResultsOpenOverStatementClose(flag);
    }

    public void setIgnoreNonTxTables(boolean property) {
        getActiveMySQLConnection().setIgnoreNonTxTables(property);
    }

    public void setIncludeInnodbStatusInDeadlockExceptions(boolean flag) {
        getActiveMySQLConnection().setIncludeInnodbStatusInDeadlockExceptions(flag);
    }

    public void setInitialTimeout(int property) throws SQLException {
        getActiveMySQLConnection().setInitialTimeout(property);
    }

    public void setInteractiveClient(boolean property) {
        getActiveMySQLConnection().setInteractiveClient(property);
    }

    public void setIsInteractiveClient(boolean property) {
        getActiveMySQLConnection().setIsInteractiveClient(property);
    }

    public void setJdbcCompliantTruncation(boolean flag) {
        getActiveMySQLConnection().setJdbcCompliantTruncation(flag);
    }

    public void setJdbcCompliantTruncationForReads(boolean jdbcCompliantTruncationForReads) {
        getActiveMySQLConnection().setJdbcCompliantTruncationForReads(jdbcCompliantTruncationForReads);
    }

    public void setLargeRowSizeThreshold(String value) throws SQLException {
        getActiveMySQLConnection().setLargeRowSizeThreshold(value);
    }

    public void setLoadBalanceBlacklistTimeout(int loadBalanceBlacklistTimeout) throws SQLException {
        getActiveMySQLConnection().setLoadBalanceBlacklistTimeout(loadBalanceBlacklistTimeout);
    }

    public void setLoadBalancePingTimeout(int loadBalancePingTimeout) throws SQLException {
        getActiveMySQLConnection().setLoadBalancePingTimeout(loadBalancePingTimeout);
    }

    public void setLoadBalanceStrategy(String strategy) {
        getActiveMySQLConnection().setLoadBalanceStrategy(strategy);
    }

    public void setLoadBalanceValidateConnectionOnSwapServer(boolean loadBalanceValidateConnectionOnSwapServer) {
        getActiveMySQLConnection().setLoadBalanceValidateConnectionOnSwapServer(loadBalanceValidateConnectionOnSwapServer);
    }

    public void setLocalSocketAddress(String address) {
        getActiveMySQLConnection().setLocalSocketAddress(address);
    }

    public void setLocatorFetchBufferSize(String value) throws SQLException {
        getActiveMySQLConnection().setLocatorFetchBufferSize(value);
    }

    public void setLogSlowQueries(boolean flag) {
        getActiveMySQLConnection().setLogSlowQueries(flag);
    }

    public void setLogXaCommands(boolean flag) {
        getActiveMySQLConnection().setLogXaCommands(flag);
    }

    public void setLogger(String property) {
        getActiveMySQLConnection().setLogger(property);
    }

    public void setLoggerClassName(String className) {
        getActiveMySQLConnection().setLoggerClassName(className);
    }

    public void setMaintainTimeStats(boolean flag) {
        getActiveMySQLConnection().setMaintainTimeStats(flag);
    }

    public void setMaxQuerySizeToLog(int sizeInBytes) throws SQLException {
        getActiveMySQLConnection().setMaxQuerySizeToLog(sizeInBytes);
    }

    public void setMaxReconnects(int property) throws SQLException {
        getActiveMySQLConnection().setMaxReconnects(property);
    }

    public void setMaxRows(int property) throws SQLException {
        getActiveMySQLConnection().setMaxRows(property);
    }

    public void setMetadataCacheSize(int value) throws SQLException {
        getActiveMySQLConnection().setMetadataCacheSize(value);
    }

    public void setNetTimeoutForStreamingResults(int value) throws SQLException {
        getActiveMySQLConnection().setNetTimeoutForStreamingResults(value);
    }

    public void setNoAccessToProcedureBodies(boolean flag) {
        getActiveMySQLConnection().setNoAccessToProcedureBodies(flag);
    }

    public void setNoDatetimeStringSync(boolean flag) {
        getActiveMySQLConnection().setNoDatetimeStringSync(flag);
    }

    public void setNoTimezoneConversionForTimeType(boolean flag) {
        getActiveMySQLConnection().setNoTimezoneConversionForTimeType(flag);
    }

    public void setNoTimezoneConversionForDateType(boolean flag) {
        getActiveMySQLConnection().setNoTimezoneConversionForDateType(flag);
    }

    public void setCacheDefaultTimezone(boolean flag) {
        getActiveMySQLConnection().setCacheDefaultTimezone(flag);
    }

    public void setNullCatalogMeansCurrent(boolean value) {
        getActiveMySQLConnection().setNullCatalogMeansCurrent(value);
    }

    public void setNullNamePatternMatchesAll(boolean value) {
        getActiveMySQLConnection().setNullNamePatternMatchesAll(value);
    }

    public void setOverrideSupportsIntegrityEnhancementFacility(boolean flag) {
        getActiveMySQLConnection().setOverrideSupportsIntegrityEnhancementFacility(flag);
    }

    public void setPacketDebugBufferSize(int size) throws SQLException {
        getActiveMySQLConnection().setPacketDebugBufferSize(size);
    }

    public void setPadCharsWithSpace(boolean flag) {
        getActiveMySQLConnection().setPadCharsWithSpace(flag);
    }

    public void setParanoid(boolean property) {
        getActiveMySQLConnection().setParanoid(property);
    }

    public void setPasswordCharacterEncoding(String characterSet) {
        getActiveMySQLConnection().setPasswordCharacterEncoding(characterSet);
    }

    public void setPedantic(boolean property) {
        getActiveMySQLConnection().setPedantic(property);
    }

    public void setPinGlobalTxToPhysicalConnection(boolean flag) {
        getActiveMySQLConnection().setPinGlobalTxToPhysicalConnection(flag);
    }

    public void setPopulateInsertRowWithDefaultValues(boolean flag) {
        getActiveMySQLConnection().setPopulateInsertRowWithDefaultValues(flag);
    }

    public void setPrepStmtCacheSize(int cacheSize) throws SQLException {
        getActiveMySQLConnection().setPrepStmtCacheSize(cacheSize);
    }

    public void setPrepStmtCacheSqlLimit(int sqlLimit) throws SQLException {
        getActiveMySQLConnection().setPrepStmtCacheSqlLimit(sqlLimit);
    }

    public void setPreparedStatementCacheSize(int cacheSize) throws SQLException {
        getActiveMySQLConnection().setPreparedStatementCacheSize(cacheSize);
    }

    public void setPreparedStatementCacheSqlLimit(int cacheSqlLimit) throws SQLException {
        getActiveMySQLConnection().setPreparedStatementCacheSqlLimit(cacheSqlLimit);
    }

    public void setProcessEscapeCodesForPrepStmts(boolean flag) {
        getActiveMySQLConnection().setProcessEscapeCodesForPrepStmts(flag);
    }

    public void setProfileSQL(boolean flag) {
        getActiveMySQLConnection().setProfileSQL(flag);
    }

    public void setProfileSql(boolean property) {
        getActiveMySQLConnection().setProfileSql(property);
    }

    public void setProfilerEventHandler(String handler) {
        getActiveMySQLConnection().setProfilerEventHandler(handler);
    }

    public void setPropertiesTransform(String value) {
        getActiveMySQLConnection().setPropertiesTransform(value);
    }

    public void setQueriesBeforeRetryMaster(int property) throws SQLException {
        getActiveMySQLConnection().setQueriesBeforeRetryMaster(property);
    }

    public void setQueryTimeoutKillsConnection(boolean queryTimeoutKillsConnection) {
        getActiveMySQLConnection().setQueryTimeoutKillsConnection(queryTimeoutKillsConnection);
    }

    public void setReconnectAtTxEnd(boolean property) {
        getActiveMySQLConnection().setReconnectAtTxEnd(property);
    }

    public void setRelaxAutoCommit(boolean property) {
        getActiveMySQLConnection().setRelaxAutoCommit(property);
    }

    public void setReportMetricsIntervalMillis(int millis) throws SQLException {
        getActiveMySQLConnection().setReportMetricsIntervalMillis(millis);
    }

    public void setRequireSSL(boolean property) {
        getActiveMySQLConnection().setRequireSSL(property);
    }

    public void setResourceId(String resourceId) {
        getActiveMySQLConnection().setResourceId(resourceId);
    }

    public void setResultSetSizeThreshold(int threshold) throws SQLException {
        getActiveMySQLConnection().setResultSetSizeThreshold(threshold);
    }

    public void setRetainStatementAfterResultSetClose(boolean flag) {
        getActiveMySQLConnection().setRetainStatementAfterResultSetClose(flag);
    }

    public void setRetriesAllDown(int retriesAllDown) throws SQLException {
        getActiveMySQLConnection().setRetriesAllDown(retriesAllDown);
    }

    public void setRewriteBatchedStatements(boolean flag) {
        getActiveMySQLConnection().setRewriteBatchedStatements(flag);
    }

    public void setRollbackOnPooledClose(boolean flag) {
        getActiveMySQLConnection().setRollbackOnPooledClose(flag);
    }

    public void setRoundRobinLoadBalance(boolean flag) {
        getActiveMySQLConnection().setRoundRobinLoadBalance(flag);
    }

    public void setRunningCTS13(boolean flag) {
        getActiveMySQLConnection().setRunningCTS13(flag);
    }

    public void setSecondsBeforeRetryMaster(int property) throws SQLException {
        getActiveMySQLConnection().setSecondsBeforeRetryMaster(property);
    }

    public void setSelfDestructOnPingMaxOperations(int maxOperations) throws SQLException {
        getActiveMySQLConnection().setSelfDestructOnPingMaxOperations(maxOperations);
    }

    public void setSelfDestructOnPingSecondsLifetime(int seconds) throws SQLException {
        getActiveMySQLConnection().setSelfDestructOnPingSecondsLifetime(seconds);
    }

    public void setServerTimezone(String property) {
        getActiveMySQLConnection().setServerTimezone(property);
    }

    public void setSessionVariables(String variables) {
        getActiveMySQLConnection().setSessionVariables(variables);
    }

    public void setSlowQueryThresholdMillis(int millis) throws SQLException {
        getActiveMySQLConnection().setSlowQueryThresholdMillis(millis);
    }

    public void setSlowQueryThresholdNanos(long nanos) throws SQLException {
        getActiveMySQLConnection().setSlowQueryThresholdNanos(nanos);
    }

    public void setSocketFactory(String name) {
        getActiveMySQLConnection().setSocketFactory(name);
    }

    public void setSocketFactoryClassName(String property) {
        getActiveMySQLConnection().setSocketFactoryClassName(property);
    }

    public void setSocketTimeout(int property) throws SQLException {
        getActiveMySQLConnection().setSocketTimeout(property);
    }

    public void setStatementInterceptors(String value) {
        getActiveMySQLConnection().setStatementInterceptors(value);
    }

    public void setStrictFloatingPoint(boolean property) {
        getActiveMySQLConnection().setStrictFloatingPoint(property);
    }

    public void setStrictUpdates(boolean property) {
        getActiveMySQLConnection().setStrictUpdates(property);
    }

    public void setTcpKeepAlive(boolean flag) {
        getActiveMySQLConnection().setTcpKeepAlive(flag);
    }

    public void setTcpNoDelay(boolean flag) {
        getActiveMySQLConnection().setTcpNoDelay(flag);
    }

    public void setTcpRcvBuf(int bufSize) throws SQLException {
        getActiveMySQLConnection().setTcpRcvBuf(bufSize);
    }

    public void setTcpSndBuf(int bufSize) throws SQLException {
        getActiveMySQLConnection().setTcpSndBuf(bufSize);
    }

    public void setTcpTrafficClass(int classFlags) throws SQLException {
        getActiveMySQLConnection().setTcpTrafficClass(classFlags);
    }

    public void setTinyInt1isBit(boolean flag) {
        getActiveMySQLConnection().setTinyInt1isBit(flag);
    }

    public void setTraceProtocol(boolean flag) {
        getActiveMySQLConnection().setTraceProtocol(flag);
    }

    public void setTransformedBitIsBoolean(boolean flag) {
        getActiveMySQLConnection().setTransformedBitIsBoolean(flag);
    }

    public void setTreatUtilDateAsTimestamp(boolean flag) {
        getActiveMySQLConnection().setTreatUtilDateAsTimestamp(flag);
    }

    public void setTrustCertificateKeyStorePassword(String value) {
        getActiveMySQLConnection().setTrustCertificateKeyStorePassword(value);
    }

    public void setTrustCertificateKeyStoreType(String value) {
        getActiveMySQLConnection().setTrustCertificateKeyStoreType(value);
    }

    public void setTrustCertificateKeyStoreUrl(String value) {
        getActiveMySQLConnection().setTrustCertificateKeyStoreUrl(value);
    }

    public void setUltraDevHack(boolean flag) {
        getActiveMySQLConnection().setUltraDevHack(flag);
    }

    public void setUseAffectedRows(boolean flag) {
        getActiveMySQLConnection().setUseAffectedRows(flag);
    }

    public void setUseBlobToStoreUTF8OutsideBMP(boolean flag) {
        getActiveMySQLConnection().setUseBlobToStoreUTF8OutsideBMP(flag);
    }

    public void setUseColumnNamesInFindColumn(boolean flag) {
        getActiveMySQLConnection().setUseColumnNamesInFindColumn(flag);
    }

    public void setUseCompression(boolean property) {
        getActiveMySQLConnection().setUseCompression(property);
    }

    public void setUseConfigs(String configs) {
        getActiveMySQLConnection().setUseConfigs(configs);
    }

    public void setUseCursorFetch(boolean flag) {
        getActiveMySQLConnection().setUseCursorFetch(flag);
    }

    public void setUseDirectRowUnpack(boolean flag) {
        getActiveMySQLConnection().setUseDirectRowUnpack(flag);
    }

    public void setUseDynamicCharsetInfo(boolean flag) {
        getActiveMySQLConnection().setUseDynamicCharsetInfo(flag);
    }

    public void setUseFastDateParsing(boolean flag) {
        getActiveMySQLConnection().setUseFastDateParsing(flag);
    }

    public void setUseFastIntParsing(boolean flag) {
        getActiveMySQLConnection().setUseFastIntParsing(flag);
    }

    public void setUseGmtMillisForDatetimes(boolean flag) {
        getActiveMySQLConnection().setUseGmtMillisForDatetimes(flag);
    }

    public void setUseHostsInPrivileges(boolean property) {
        getActiveMySQLConnection().setUseHostsInPrivileges(property);
    }

    public void setUseInformationSchema(boolean flag) {
        getActiveMySQLConnection().setUseInformationSchema(flag);
    }

    public void setUseJDBCCompliantTimezoneShift(boolean flag) {
        getActiveMySQLConnection().setUseJDBCCompliantTimezoneShift(flag);
    }

    public void setUseJvmCharsetConverters(boolean flag) {
        getActiveMySQLConnection().setUseJvmCharsetConverters(flag);
    }

    public void setUseLegacyDatetimeCode(boolean flag) {
        getActiveMySQLConnection().setUseLegacyDatetimeCode(flag);
    }

    public void setSendFractionalSeconds(boolean flag) {
        getActiveMySQLConnection().setSendFractionalSeconds(flag);
    }

    public void setUseLocalSessionState(boolean flag) {
        getActiveMySQLConnection().setUseLocalSessionState(flag);
    }

    public void setUseLocalTransactionState(boolean flag) {
        getActiveMySQLConnection().setUseLocalTransactionState(flag);
    }

    public void setUseNanosForElapsedTime(boolean flag) {
        getActiveMySQLConnection().setUseNanosForElapsedTime(flag);
    }

    public void setUseOldAliasMetadataBehavior(boolean flag) {
        getActiveMySQLConnection().setUseOldAliasMetadataBehavior(flag);
    }

    public void setUseOldUTF8Behavior(boolean flag) {
        getActiveMySQLConnection().setUseOldUTF8Behavior(flag);
    }

    public void setUseOnlyServerErrorMessages(boolean flag) {
        getActiveMySQLConnection().setUseOnlyServerErrorMessages(flag);
    }

    public void setUseReadAheadInput(boolean flag) {
        getActiveMySQLConnection().setUseReadAheadInput(flag);
    }

    public void setUseSSL(boolean property) {
        getActiveMySQLConnection().setUseSSL(property);
    }

    public void setUseSSPSCompatibleTimezoneShift(boolean flag) {
        getActiveMySQLConnection().setUseSSPSCompatibleTimezoneShift(flag);
    }

    public void setUseServerPrepStmts(boolean flag) {
        getActiveMySQLConnection().setUseServerPrepStmts(flag);
    }

    public void setUseServerPreparedStmts(boolean flag) {
        getActiveMySQLConnection().setUseServerPreparedStmts(flag);
    }

    public void setUseSqlStateCodes(boolean flag) {
        getActiveMySQLConnection().setUseSqlStateCodes(flag);
    }

    public void setUseStreamLengthsInPrepStmts(boolean property) {
        getActiveMySQLConnection().setUseStreamLengthsInPrepStmts(property);
    }

    public void setUseTimezone(boolean property) {
        getActiveMySQLConnection().setUseTimezone(property);
    }

    public void setUseUltraDevWorkAround(boolean property) {
        getActiveMySQLConnection().setUseUltraDevWorkAround(property);
    }

    public void setUseUnbufferedInput(boolean flag) {
        getActiveMySQLConnection().setUseUnbufferedInput(flag);
    }

    public void setUseUnicode(boolean flag) {
        getActiveMySQLConnection().setUseUnicode(flag);
    }

    public void setUseUsageAdvisor(boolean useUsageAdvisorFlag) {
        getActiveMySQLConnection().setUseUsageAdvisor(useUsageAdvisorFlag);
    }

    public void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern) {
        getActiveMySQLConnection().setUtf8OutsideBmpExcludedColumnNamePattern(regexPattern);
    }

    public void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern) {
        getActiveMySQLConnection().setUtf8OutsideBmpIncludedColumnNamePattern(regexPattern);
    }

    public void setVerifyServerCertificate(boolean flag) {
        getActiveMySQLConnection().setVerifyServerCertificate(flag);
    }

    public void setYearIsDateType(boolean flag) {
        getActiveMySQLConnection().setYearIsDateType(flag);
    }

    public void setZeroDateTimeBehavior(String behavior) {
        getActiveMySQLConnection().setZeroDateTimeBehavior(behavior);
    }

    public boolean useUnbufferedInput() {
        return getActiveMySQLConnection().useUnbufferedInput();
    }

    public StringBuilder generateConnectionCommentBlock(StringBuilder buf) {
        return getActiveMySQLConnection().generateConnectionCommentBlock(buf);
    }

    public int getActiveStatementCount() {
        return getActiveMySQLConnection().getActiveStatementCount();
    }

    public boolean getAutoCommit() throws SQLException {
        return getActiveMySQLConnection().getAutoCommit();
    }

    public int getAutoIncrementIncrement() {
        return getActiveMySQLConnection().getAutoIncrementIncrement();
    }

    public CachedResultSetMetaData getCachedMetaData(String sql) {
        return getActiveMySQLConnection().getCachedMetaData(sql);
    }

    public Calendar getCalendarInstanceForSessionOrNew() {
        return getActiveMySQLConnection().getCalendarInstanceForSessionOrNew();
    }

    public Timer getCancelTimer() {
        return getActiveMySQLConnection().getCancelTimer();
    }

    public String getCatalog() throws SQLException {
        return getActiveMySQLConnection().getCatalog();
    }

    public String getCharacterSetMetadata() {
        return getActiveMySQLConnection().getCharacterSetMetadata();
    }

    public SingleByteCharsetConverter getCharsetConverter(String javaEncodingName) throws SQLException {
        return getActiveMySQLConnection().getCharsetConverter(javaEncodingName);
    }

    /**
     * @deprecated replaced by <code>getEncodingForIndex(int charsetIndex)</code>
     */
    @Deprecated
    public String getCharsetNameForIndex(int charsetIndex) throws SQLException {
        return getEncodingForIndex(charsetIndex);
    }

    public String getEncodingForIndex(int collationIndex) throws SQLException {
        return getActiveMySQLConnection().getEncodingForIndex(collationIndex);
    }

    public TimeZone getDefaultTimeZone() {
        return getActiveMySQLConnection().getDefaultTimeZone();
    }

    public String getErrorMessageEncoding() {
        return getActiveMySQLConnection().getErrorMessageEncoding();
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return getActiveMySQLConnection().getExceptionInterceptor();
    }

    public int getHoldability() throws SQLException {
        return getActiveMySQLConnection().getHoldability();
    }

    public String getHost() {
        return getActiveMySQLConnection().getHost();
    }

    public long getId() {
        return getActiveMySQLConnection().getId();
    }

    public long getIdleFor() {
        return getActiveMySQLConnection().getIdleFor();
    }

    public MysqlIO getIO() throws SQLException {
        return getActiveMySQLConnection().getIO();
    }

    public MySQLConnection getLoadBalanceSafeProxy() {
        return getMultiHostSafeProxy();
    }

    public MySQLConnection getMultiHostSafeProxy() {
        return getActiveMySQLConnection().getMultiHostSafeProxy();
    }

    public Log getLog() throws SQLException {
        return getActiveMySQLConnection().getLog();
    }

    public int getMaxBytesPerChar(String javaCharsetName) throws SQLException {
        return getActiveMySQLConnection().getMaxBytesPerChar(javaCharsetName);
    }

    public int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName) throws SQLException {
        return getActiveMySQLConnection().getMaxBytesPerChar(charsetIndex, javaCharsetName);
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return getActiveMySQLConnection().getMetaData();
    }

    public Statement getMetadataSafeStatement() throws SQLException {
        return getActiveMySQLConnection().getMetadataSafeStatement();
    }

    public int getNetBufferLength() {
        return getActiveMySQLConnection().getNetBufferLength();
    }

    public Properties getProperties() {
        return getActiveMySQLConnection().getProperties();
    }

    public boolean getRequiresEscapingEncoder() {
        return getActiveMySQLConnection().getRequiresEscapingEncoder();
    }

    /**
     * @deprecated replaced by <code>getServerCharset()</code>
     */
    @Deprecated
    public String getServerCharacterEncoding() {
        return getServerCharset();
    }

    public String getServerCharset() {
        return getActiveMySQLConnection().getServerCharset();
    }

    public int getServerMajorVersion() {
        return getActiveMySQLConnection().getServerMajorVersion();
    }

    public int getServerMinorVersion() {
        return getActiveMySQLConnection().getServerMinorVersion();
    }

    public int getServerSubMinorVersion() {
        return getActiveMySQLConnection().getServerSubMinorVersion();
    }

    public TimeZone getServerTimezoneTZ() {
        return getActiveMySQLConnection().getServerTimezoneTZ();
    }

    public String getServerVariable(String variableName) {
        return getActiveMySQLConnection().getServerVariable(variableName);
    }

    public String getServerVersion() {
        return getActiveMySQLConnection().getServerVersion();
    }

    public Calendar getSessionLockedCalendar() {
        return getActiveMySQLConnection().getSessionLockedCalendar();
    }

    public String getStatementComment() {
        return getActiveMySQLConnection().getStatementComment();
    }

    public List<StatementInterceptorV2> getStatementInterceptorsInstances() {
        return getActiveMySQLConnection().getStatementInterceptorsInstances();
    }

    public int getTransactionIsolation() throws SQLException {
        return getActiveMySQLConnection().getTransactionIsolation();
    }

    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return getActiveMySQLConnection().getTypeMap();
    }

    public String getURL() {
        return getActiveMySQLConnection().getURL();
    }

    public String getUser() {
        return getActiveMySQLConnection().getUser();
    }

    public Calendar getUtcCalendar() {
        return getActiveMySQLConnection().getUtcCalendar();
    }

    public SQLWarning getWarnings() throws SQLException {
        return getActiveMySQLConnection().getWarnings();
    }

    public boolean hasSameProperties(Connection c) {
        return getActiveMySQLConnection().hasSameProperties(c);
    }

    public boolean hasTriedMaster() {
        return getActiveMySQLConnection().hasTriedMaster();
    }

    public void incrementNumberOfPreparedExecutes() {
        getActiveMySQLConnection().incrementNumberOfPreparedExecutes();
    }

    public void incrementNumberOfPrepares() {
        getActiveMySQLConnection().incrementNumberOfPrepares();
    }

    public void incrementNumberOfResultSetsCreated() {
        getActiveMySQLConnection().incrementNumberOfResultSetsCreated();
    }

    public void initializeExtension(Extension ex) throws SQLException {
        getActiveMySQLConnection().initializeExtension(ex);
    }

    public void initializeResultsMetadataFromCache(String sql, CachedResultSetMetaData cachedMetaData, ResultSetInternalMethods resultSet) throws SQLException {
        getActiveMySQLConnection().initializeResultsMetadataFromCache(sql, cachedMetaData, resultSet);
    }

    public void initializeSafeStatementInterceptors() throws SQLException {
        getActiveMySQLConnection().initializeSafeStatementInterceptors();
    }

    public boolean isAbonormallyLongQuery(long millisOrNanos) {
        return getActiveMySQLConnection().isAbonormallyLongQuery(millisOrNanos);
    }

    public boolean isClientTzUTC() {
        return getActiveMySQLConnection().isClientTzUTC();
    }

    public boolean isCursorFetchEnabled() throws SQLException {
        return getActiveMySQLConnection().isCursorFetchEnabled();
    }

    public boolean isInGlobalTx() {
        return getActiveMySQLConnection().isInGlobalTx();
    }

    public boolean isMasterConnection() {
        return getActiveMySQLConnection().isMasterConnection();
    }

    public boolean isNoBackslashEscapesSet() {
        return getActiveMySQLConnection().isNoBackslashEscapesSet();
    }

    public boolean isReadInfoMsgEnabled() {
        return getActiveMySQLConnection().isReadInfoMsgEnabled();
    }

    public boolean isReadOnly() throws SQLException {
        return getActiveMySQLConnection().isReadOnly();
    }

    public boolean isReadOnly(boolean useSessionStatus) throws SQLException {
        return getActiveMySQLConnection().isReadOnly(useSessionStatus);
    }

    public boolean isRunningOnJDK13() {
        return getActiveMySQLConnection().isRunningOnJDK13();
    }

    public boolean isSameResource(Connection otherConnection) {
        return getActiveMySQLConnection().isSameResource(otherConnection);
    }

    public boolean isServerTzUTC() {
        return getActiveMySQLConnection().isServerTzUTC();
    }

    public boolean lowerCaseTableNames() {
        return getActiveMySQLConnection().lowerCaseTableNames();
    }

    public String nativeSQL(String sql) throws SQLException {
        return getActiveMySQLConnection().nativeSQL(sql);
    }

    public boolean parserKnowsUnicode() {
        return getActiveMySQLConnection().parserKnowsUnicode();
    }

    public void ping() throws SQLException {
        getActiveMySQLConnection().ping();
    }

    public void pingInternal(boolean checkForClosedConnection, int timeoutMillis) throws SQLException {
        getActiveMySQLConnection().pingInternal(checkForClosedConnection, timeoutMillis);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return getActiveMySQLConnection().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return getActiveMySQLConnection().prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    public CallableStatement prepareCall(String sql) throws SQLException {
        return getActiveMySQLConnection().prepareCall(sql);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return getActiveMySQLConnection().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return getActiveMySQLConnection().prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public PreparedStatement prepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        return getActiveMySQLConnection().prepareStatement(sql, autoGenKeyIndex);
    }

    public PreparedStatement prepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        return getActiveMySQLConnection().prepareStatement(sql, autoGenKeyIndexes);
    }

    public PreparedStatement prepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        return getActiveMySQLConnection().prepareStatement(sql, autoGenKeyColNames);
    }

    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return getActiveMySQLConnection().prepareStatement(sql);
    }

    public void realClose(boolean calledExplicitly, boolean issueRollback, boolean skipLocalTeardown, Throwable reason) throws SQLException {
        getActiveMySQLConnection().realClose(calledExplicitly, issueRollback, skipLocalTeardown, reason);
    }

    public void recachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException {
        getActiveMySQLConnection().recachePreparedStatement(pstmt);
    }

    public void decachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException {
        getActiveMySQLConnection().decachePreparedStatement(pstmt);
    }

    public void registerQueryExecutionTime(long queryTimeMs) {
        getActiveMySQLConnection().registerQueryExecutionTime(queryTimeMs);
    }

    public void registerStatement(com.mysql.jdbc.Statement stmt) {
        getActiveMySQLConnection().registerStatement(stmt);
    }

    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        getActiveMySQLConnection().releaseSavepoint(arg0);
    }

    public void reportNumberOfTablesAccessed(int numTablesAccessed) {
        getActiveMySQLConnection().reportNumberOfTablesAccessed(numTablesAccessed);
    }

    public void reportQueryTime(long millisOrNanos) {
        getActiveMySQLConnection().reportQueryTime(millisOrNanos);
    }

    public void resetServerState() throws SQLException {
        getActiveMySQLConnection().resetServerState();
    }

    public void rollback() throws SQLException {
        getActiveMySQLConnection().rollback();
    }

    public void rollback(Savepoint savepoint) throws SQLException {
        getActiveMySQLConnection().rollback(savepoint);
    }

    public PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return getActiveMySQLConnection().serverPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    public PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return getActiveMySQLConnection().serverPrepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    public PreparedStatement serverPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        return getActiveMySQLConnection().serverPrepareStatement(sql, autoGenKeyIndex);
    }

    public PreparedStatement serverPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        return getActiveMySQLConnection().serverPrepareStatement(sql, autoGenKeyIndexes);
    }

    public PreparedStatement serverPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        return getActiveMySQLConnection().serverPrepareStatement(sql, autoGenKeyColNames);
    }

    public PreparedStatement serverPrepareStatement(String sql) throws SQLException {
        return getActiveMySQLConnection().serverPrepareStatement(sql);
    }

    public boolean serverSupportsConvertFn() throws SQLException {
        return getActiveMySQLConnection().serverSupportsConvertFn();
    }

    public void setAutoCommit(boolean autoCommitFlag) throws SQLException {
        getActiveMySQLConnection().setAutoCommit(autoCommitFlag);
    }

    public void setCatalog(String catalog) throws SQLException {
        getActiveMySQLConnection().setCatalog(catalog);
    }

    public void setFailedOver(boolean flag) {
        getActiveMySQLConnection().setFailedOver(flag);
    }

    public void setHoldability(int arg0) throws SQLException {
        getActiveMySQLConnection().setHoldability(arg0);
    }

    public void setInGlobalTx(boolean flag) {
        getActiveMySQLConnection().setInGlobalTx(flag);
    }

    public void setPreferSlaveDuringFailover(boolean flag) {
        getActiveMySQLConnection().setPreferSlaveDuringFailover(flag);
    }

    public void setProxy(MySQLConnection proxy) {
        getActiveMySQLConnection().setProxy(proxy);
    }

    public void setReadInfoMsgEnabled(boolean flag) {
        getActiveMySQLConnection().setReadInfoMsgEnabled(flag);
    }

    public void setReadOnly(boolean readOnlyFlag) throws SQLException {
        getActiveMySQLConnection().setReadOnly(readOnlyFlag);
    }

    public void setReadOnlyInternal(boolean readOnlyFlag) throws SQLException {
        getActiveMySQLConnection().setReadOnlyInternal(readOnlyFlag);
    }

    public Savepoint setSavepoint() throws SQLException {
        return getActiveMySQLConnection().setSavepoint();
    }

    public Savepoint setSavepoint(String name) throws SQLException {
        return getActiveMySQLConnection().setSavepoint(name);
    }

    public void setStatementComment(String comment) {
        getActiveMySQLConnection().setStatementComment(comment);
    }

    public void setTransactionIsolation(int level) throws SQLException {
        getActiveMySQLConnection().setTransactionIsolation(level);
    }

    public void shutdownServer() throws SQLException {
        getActiveMySQLConnection().shutdownServer();
    }

    public boolean storesLowerCaseTableName() {
        return getActiveMySQLConnection().storesLowerCaseTableName();
    }

    public boolean supportsIsolationLevel() {
        return getActiveMySQLConnection().supportsIsolationLevel();
    }

    public boolean supportsQuotedIdentifiers() {
        return getActiveMySQLConnection().supportsQuotedIdentifiers();
    }

    public boolean supportsTransactions() {
        return getActiveMySQLConnection().supportsTransactions();
    }

    public void throwConnectionClosedException() throws SQLException {
        getActiveMySQLConnection().throwConnectionClosedException();
    }

    public void transactionBegun() throws SQLException {
        getActiveMySQLConnection().transactionBegun();
    }

    public void transactionCompleted() throws SQLException {
        getActiveMySQLConnection().transactionCompleted();
    }

    public void unregisterStatement(com.mysql.jdbc.Statement stmt) {
        getActiveMySQLConnection().unregisterStatement(stmt);
    }

    public void unSafeStatementInterceptors() throws SQLException {
        getActiveMySQLConnection().unSafeStatementInterceptors();
    }

    public boolean useAnsiQuotedIdentifiers() {
        return getActiveMySQLConnection().useAnsiQuotedIdentifiers();
    }

    public boolean versionMeetsMinimum(int major, int minor, int subminor) throws SQLException {
        return getActiveMySQLConnection().versionMeetsMinimum(major, minor, subminor);
    }

    public boolean isClosed() throws SQLException {
        return getActiveMySQLConnection().isClosed();
    }

    public boolean getHoldResultsOpenOverStatementClose() {
        return getActiveMySQLConnection().getHoldResultsOpenOverStatementClose();
    }

    public String getLoadBalanceConnectionGroup() {
        return getActiveMySQLConnection().getLoadBalanceConnectionGroup();
    }

    public boolean getLoadBalanceEnableJMX() {
        return getActiveMySQLConnection().getLoadBalanceEnableJMX();
    }

    public String getLoadBalanceExceptionChecker() {
        return getActiveMySQLConnection().getLoadBalanceExceptionChecker();
    }

    public String getLoadBalanceSQLExceptionSubclassFailover() {
        return getActiveMySQLConnection().getLoadBalanceSQLExceptionSubclassFailover();
    }

    public String getLoadBalanceSQLStateFailover() {
        return getActiveMySQLConnection().getLoadBalanceSQLStateFailover();
    }

    public void setLoadBalanceConnectionGroup(String loadBalanceConnectionGroup) {
        getActiveMySQLConnection().setLoadBalanceConnectionGroup(loadBalanceConnectionGroup);

    }

    public void setLoadBalanceEnableJMX(boolean loadBalanceEnableJMX) {
        getActiveMySQLConnection().setLoadBalanceEnableJMX(loadBalanceEnableJMX);

    }

    public void setLoadBalanceExceptionChecker(String loadBalanceExceptionChecker) {
        getActiveMySQLConnection().setLoadBalanceExceptionChecker(loadBalanceExceptionChecker);

    }

    public void setLoadBalanceSQLExceptionSubclassFailover(String loadBalanceSQLExceptionSubclassFailover) {
        getActiveMySQLConnection().setLoadBalanceSQLExceptionSubclassFailover(loadBalanceSQLExceptionSubclassFailover);

    }

    public void setLoadBalanceSQLStateFailover(String loadBalanceSQLStateFailover) {
        getActiveMySQLConnection().setLoadBalanceSQLStateFailover(loadBalanceSQLStateFailover);

    }

    public boolean isProxySet() {
        return this.getActiveMySQLConnection().isProxySet();
    }

    public String getLoadBalanceAutoCommitStatementRegex() {
        return getActiveMySQLConnection().getLoadBalanceAutoCommitStatementRegex();
    }

    public int getLoadBalanceAutoCommitStatementThreshold() {
        return getActiveMySQLConnection().getLoadBalanceAutoCommitStatementThreshold();
    }

    public void setLoadBalanceAutoCommitStatementRegex(String loadBalanceAutoCommitStatementRegex) {
        getActiveMySQLConnection().setLoadBalanceAutoCommitStatementRegex(loadBalanceAutoCommitStatementRegex);

    }

    public void setLoadBalanceAutoCommitStatementThreshold(int loadBalanceAutoCommitStatementThreshold) throws SQLException {
        getActiveMySQLConnection().setLoadBalanceAutoCommitStatementThreshold(loadBalanceAutoCommitStatementThreshold);

    }

    public boolean getIncludeThreadDumpInDeadlockExceptions() {
        return getActiveMySQLConnection().getIncludeThreadDumpInDeadlockExceptions();
    }

    public void setIncludeThreadDumpInDeadlockExceptions(boolean flag) {
        getActiveMySQLConnection().setIncludeThreadDumpInDeadlockExceptions(flag);
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        getActiveMySQLConnection().setTypeMap(map);
    }

    public boolean getIncludeThreadNamesAsStatementComment() {
        return getActiveMySQLConnection().getIncludeThreadNamesAsStatementComment();
    }

    public void setIncludeThreadNamesAsStatementComment(boolean flag) {
        getActiveMySQLConnection().setIncludeThreadNamesAsStatementComment(flag);
    }

    public boolean isServerLocal() throws SQLException {
        return getActiveMySQLConnection().isServerLocal();
    }

    public void setAuthenticationPlugins(String authenticationPlugins) {
        getActiveMySQLConnection().setAuthenticationPlugins(authenticationPlugins);
    }

    public String getAuthenticationPlugins() {
        return getActiveMySQLConnection().getAuthenticationPlugins();
    }

    public void setDisabledAuthenticationPlugins(String disabledAuthenticationPlugins) {
        getActiveMySQLConnection().setDisabledAuthenticationPlugins(disabledAuthenticationPlugins);
    }

    public String getDisabledAuthenticationPlugins() {
        return getActiveMySQLConnection().getDisabledAuthenticationPlugins();
    }

    public void setDefaultAuthenticationPlugin(String defaultAuthenticationPlugin) {
        getActiveMySQLConnection().setDefaultAuthenticationPlugin(defaultAuthenticationPlugin);
    }

    public String getDefaultAuthenticationPlugin() {
        return getActiveMySQLConnection().getDefaultAuthenticationPlugin();
    }

    public void setParseInfoCacheFactory(String factoryClassname) {
        getActiveMySQLConnection().setParseInfoCacheFactory(factoryClassname);
    }

    public String getParseInfoCacheFactory() {
        return getActiveMySQLConnection().getParseInfoCacheFactory();
    }

    public void setSchema(String schema) throws SQLException {
        getActiveMySQLConnection().setSchema(schema);
    }

    public String getSchema() throws SQLException {
        return getActiveMySQLConnection().getSchema();
    }

    public void abort(Executor executor) throws SQLException {
        getActiveMySQLConnection().abort(executor);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        getActiveMySQLConnection().setNetworkTimeout(executor, milliseconds);
    }

    public int getNetworkTimeout() throws SQLException {
        return getActiveMySQLConnection().getNetworkTimeout();
    }

    public void setServerConfigCacheFactory(String factoryClassname) {
        getActiveMySQLConnection().setServerConfigCacheFactory(factoryClassname);
    }

    public String getServerConfigCacheFactory() {
        return getActiveMySQLConnection().getServerConfigCacheFactory();
    }

    public void setDisconnectOnExpiredPasswords(boolean disconnectOnExpiredPasswords) {
        getActiveMySQLConnection().setDisconnectOnExpiredPasswords(disconnectOnExpiredPasswords);
    }

    public boolean getDisconnectOnExpiredPasswords() {
        return getActiveMySQLConnection().getDisconnectOnExpiredPasswords();
    }

    public void setGetProceduresReturnsFunctions(boolean getProcedureReturnsFunctions) {
        getActiveMySQLConnection().setGetProceduresReturnsFunctions(getProcedureReturnsFunctions);
    }

    public boolean getGetProceduresReturnsFunctions() {
        return getActiveMySQLConnection().getGetProceduresReturnsFunctions();
    }

    public Object getConnectionMutex() {
        return getActiveMySQLConnection().getConnectionMutex();
    }

    public String getConnectionAttributes() throws SQLException {
        return getActiveMySQLConnection().getConnectionAttributes();
    }

    public boolean getAllowMasterDownConnections() {
        return false;
    }

    public void setAllowMasterDownConnections(boolean connectIfMasterDown) {
    }

    public boolean getReplicationEnableJMX() {
        return false;
    }

    public void setReplicationEnableJMX(boolean replicationEnableJMX) {
    }

    public void setDetectCustomCollations(boolean detectCustomCollations) {
        getActiveMySQLConnection().setDetectCustomCollations(detectCustomCollations);
    }

    public boolean getDetectCustomCollations() {
        return getActiveMySQLConnection().getDetectCustomCollations();
    }

    public int getSessionMaxRows() {
        return getActiveMySQLConnection().getSessionMaxRows();
    }

    public void setSessionMaxRows(int max) throws SQLException {
        getActiveMySQLConnection().setSessionMaxRows(max);
    }

    public ProfilerEventHandler getProfilerEventHandlerInstance() {
        return getActiveMySQLConnection().getProfilerEventHandlerInstance();
    }

    public void setProfilerEventHandlerInstance(ProfilerEventHandler h) {
        getActiveMySQLConnection().setProfilerEventHandlerInstance(h);
    }

    public String getServerRSAPublicKeyFile() {
        return getActiveMySQLConnection().getServerRSAPublicKeyFile();
    }

    public void setServerRSAPublicKeyFile(String serverRSAPublicKeyFile) throws SQLException {
        getActiveMySQLConnection().setServerRSAPublicKeyFile(serverRSAPublicKeyFile);
    }

    public boolean getAllowPublicKeyRetrieval() {
        return getActiveMySQLConnection().getAllowPublicKeyRetrieval();
    }

    public void setAllowPublicKeyRetrieval(boolean allowPublicKeyRetrieval) throws SQLException {
        getActiveMySQLConnection().setAllowPublicKeyRetrieval(allowPublicKeyRetrieval);
    }

    public void setDontCheckOnDuplicateKeyUpdateInSQL(boolean dontCheckOnDuplicateKeyUpdateInSQL) {
        getActiveMySQLConnection().setDontCheckOnDuplicateKeyUpdateInSQL(dontCheckOnDuplicateKeyUpdateInSQL);
    }

    public boolean getDontCheckOnDuplicateKeyUpdateInSQL() {
        return getActiveMySQLConnection().getDontCheckOnDuplicateKeyUpdateInSQL();
    }

    public void setSocksProxyHost(String socksProxyHost) {
        getActiveMySQLConnection().setSocksProxyHost(socksProxyHost);
    }

    public String getSocksProxyHost() {
        return getActiveMySQLConnection().getSocksProxyHost();
    }

    public void setSocksProxyPort(int socksProxyPort) throws SQLException {
        getActiveMySQLConnection().setSocksProxyPort(socksProxyPort);
    }

    public int getSocksProxyPort() {
        return getActiveMySQLConnection().getSocksProxyPort();
    }

    public boolean getReadOnlyPropagatesToServer() {
        return getActiveMySQLConnection().getReadOnlyPropagatesToServer();
    }

    public void setReadOnlyPropagatesToServer(boolean flag) {
        getActiveMySQLConnection().setReadOnlyPropagatesToServer(flag);
    }

    public String getEnabledSSLCipherSuites() {
        return getActiveMySQLConnection().getEnabledSSLCipherSuites();
    }

    public void setEnabledSSLCipherSuites(String cipherSuites) {
        getActiveMySQLConnection().setEnabledSSLCipherSuites(cipherSuites);
    }

    public boolean getEnableEscapeProcessing() {
        return getActiveMySQLConnection().getEnableEscapeProcessing();
    }

    public void setEnableEscapeProcessing(boolean flag) {
        getActiveMySQLConnection().setEnableEscapeProcessing(flag);
    }
}
