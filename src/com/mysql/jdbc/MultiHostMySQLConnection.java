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

package com.mysql.jdbc;

import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.Timer;
import java.util.concurrent.Executor;

import com.mysql.cj.api.Extension;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.exception.ExceptionInterceptor;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.mysqla.MysqlaSession;
import com.mysql.cj.mysqla.io.Buffer;
import com.mysql.cj.mysqla.io.MysqlaProtocol;
import com.mysql.jdbc.exceptions.SQLError;
import com.mysql.jdbc.interceptors.StatementInterceptorV2;

public class MultiHostMySQLConnection implements MysqlJdbcConnection {

    protected MultiHostConnectionProxy proxy;

    public MultiHostMySQLConnection(MultiHostConnectionProxy proxy) {
        this.proxy = proxy;
    }

    public MultiHostConnectionProxy getProxy() {
        return this.proxy;
    }

    protected MysqlJdbcConnection getActiveMySQLConnection() {
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

    public void checkClosed() {
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

    public void createNewIO(boolean isForReconnect) {
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

    public JdbcConnection duplicate() throws SQLException {
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

    public boolean getAutoSlowLog() {
        return getActiveMySQLConnection().getAutoSlowLog();
    }

    public boolean getBlobsAreStrings() {
        return getActiveMySQLConnection().getBlobsAreStrings();
    }

    public String getClientInfoProvider() {
        return getActiveMySQLConnection().getClientInfoProvider();
    }

    public boolean getCompensateOnDuplicateKeyUpdateCounts() {
        return getActiveMySQLConnection().getCompensateOnDuplicateKeyUpdateCounts();
    }

    public String getConnectionLifecycleInterceptors() {
        return getActiveMySQLConnection().getConnectionLifecycleInterceptors();
    }

    public boolean getEnableQueryTimeouts() {
        return getActiveMySQLConnection().getEnableQueryTimeouts();
    }

    public String getExceptionInterceptors() {
        return getActiveMySQLConnection().getExceptionInterceptors();
    }

    public boolean getFunctionsNeverReturnBlobs() {
        return getActiveMySQLConnection().getFunctionsNeverReturnBlobs();
    }

    public boolean getGenerateSimpleParameterMetadata() {
        return getActiveMySQLConnection().getGenerateSimpleParameterMetadata();
    }

    public boolean getIncludeInnodbStatusInDeadlockExceptions() {
        return getActiveMySQLConnection().getIncludeInnodbStatusInDeadlockExceptions();
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

    public String getHaLoadBalanceStrategy() {
        return getActiveMySQLConnection().getHaLoadBalanceStrategy();
    }

    public boolean getLoadBalanceValidateConnectionOnSwapServer() {
        return getActiveMySQLConnection().getLoadBalanceValidateConnectionOnSwapServer();
    }

    public String getLocalSocketAddress() {
        return getActiveMySQLConnection().getLocalSocketAddress();
    }

    public boolean getLogXaCommands() {
        return getActiveMySQLConnection().getLogXaCommands();
    }

    public int getMaxAllowedPacket() {
        return getActiveMySQLConnection().getMaxAllowedPacket();
    }

    public int getNetTimeoutForStreamingResults() {
        return getActiveMySQLConnection().getNetTimeoutForStreamingResults();
    }

    public boolean getNoAccessToProcedureBodies() {
        return getActiveMySQLConnection().getNoAccessToProcedureBodies();
    }

    public boolean getPadCharsWithSpace() {
        return getActiveMySQLConnection().getPadCharsWithSpace();
    }

    public boolean getPinGlobalTxToPhysicalConnection() {
        return getActiveMySQLConnection().getPinGlobalTxToPhysicalConnection();
    }

    public boolean getPopulateInsertRowWithDefaultValues() {
        return getActiveMySQLConnection().getPopulateInsertRowWithDefaultValues();
    }

    public boolean getQueryTimeoutKillsConnection() {
        return getActiveMySQLConnection().getQueryTimeoutKillsConnection();
    }

    public int getResultSetSizeThreshold() {
        return getActiveMySQLConnection().getResultSetSizeThreshold();
    }

    public int getRetriesAllDown() {
        return getActiveMySQLConnection().getRetriesAllDown();
    }

    public boolean getRewriteBatchedStatements() {
        return getActiveMySQLConnection().getRewriteBatchedStatements();
    }

    public int getSelfDestructOnPingMaxOperations() {
        return getActiveMySQLConnection().getSelfDestructOnPingMaxOperations();
    }

    public int getSelfDestructOnPingSecondsLifetime() {
        return getActiveMySQLConnection().getSelfDestructOnPingSecondsLifetime();
    }

    public long getSlowQueryThresholdNanos() {
        return getActiveMySQLConnection().getSlowQueryThresholdNanos();
    }

    public String getStatementInterceptors() {
        return getActiveMySQLConnection().getStatementInterceptors();
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

    public boolean getTreatUtilDateAsTimestamp() {
        return getActiveMySQLConnection().getTreatUtilDateAsTimestamp();
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

    public String getUseConfigs() {
        return getActiveMySQLConnection().getUseConfigs();
    }

    public boolean getUseDirectRowUnpack() {
        return getActiveMySQLConnection().getUseDirectRowUnpack();
    }

    public boolean getUseDynamicCharsetInfo() {
        return getActiveMySQLConnection().getUseDynamicCharsetInfo();
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

    public String getUtf8OutsideBmpExcludedColumnNamePattern() {
        return getActiveMySQLConnection().getUtf8OutsideBmpExcludedColumnNamePattern();
    }

    public String getUtf8OutsideBmpIncludedColumnNamePattern() {
        return getActiveMySQLConnection().getUtf8OutsideBmpIncludedColumnNamePattern();
    }

    public void setAutoSlowLog(boolean flag) {
        getActiveMySQLConnection().setAutoSlowLog(flag);
    }

    public void setBlobsAreStrings(boolean flag) {
        getActiveMySQLConnection().setBlobsAreStrings(flag);
    }

    public void setClientInfoProvider(String classname) {
        getActiveMySQLConnection().setClientInfoProvider(classname);
    }

    public void setCompensateOnDuplicateKeyUpdateCounts(boolean flag) {
        getActiveMySQLConnection().setCompensateOnDuplicateKeyUpdateCounts(flag);
    }

    public void setConnectionLifecycleInterceptors(String interceptors) {
        getActiveMySQLConnection().setConnectionLifecycleInterceptors(interceptors);
    }

    public void setEnableQueryTimeouts(boolean flag) {
        getActiveMySQLConnection().setEnableQueryTimeouts(flag);
    }

    public void setExceptionInterceptors(String exceptionInterceptors) {
        getActiveMySQLConnection().setExceptionInterceptors(exceptionInterceptors);
    }

    public void setFunctionsNeverReturnBlobs(boolean flag) {
        getActiveMySQLConnection().setFunctionsNeverReturnBlobs(flag);
    }

    public void setGenerateSimpleParameterMetadata(boolean flag) {
        getActiveMySQLConnection().setGenerateSimpleParameterMetadata(flag);
    }

    public void setIncludeInnodbStatusInDeadlockExceptions(boolean flag) {
        getActiveMySQLConnection().setIncludeInnodbStatusInDeadlockExceptions(flag);
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

    public void setHaLoadBalanceStrategy(String strategy) {
        getActiveMySQLConnection().setHaLoadBalanceStrategy(strategy);
    }

    public void setLoadBalanceValidateConnectionOnSwapServer(boolean loadBalanceValidateConnectionOnSwapServer) {
        getActiveMySQLConnection().setLoadBalanceValidateConnectionOnSwapServer(loadBalanceValidateConnectionOnSwapServer);
    }

    public void setLocalSocketAddress(String address) {
        getActiveMySQLConnection().setLocalSocketAddress(address);
    }

    public void setLogXaCommands(boolean flag) {
        getActiveMySQLConnection().setLogXaCommands(flag);
    }

    public void setNetTimeoutForStreamingResults(int value) throws SQLException {
        getActiveMySQLConnection().setNetTimeoutForStreamingResults(value);
    }

    public void setNoAccessToProcedureBodies(boolean flag) {
        getActiveMySQLConnection().setNoAccessToProcedureBodies(flag);
    }

    public void setPadCharsWithSpace(boolean flag) {
        getActiveMySQLConnection().setPadCharsWithSpace(flag);
    }

    public void setPinGlobalTxToPhysicalConnection(boolean flag) {
        getActiveMySQLConnection().setPinGlobalTxToPhysicalConnection(flag);
    }

    public void setPopulateInsertRowWithDefaultValues(boolean flag) {
        getActiveMySQLConnection().setPopulateInsertRowWithDefaultValues(flag);
    }

    public void setQueryTimeoutKillsConnection(boolean queryTimeoutKillsConnection) {
        getActiveMySQLConnection().setQueryTimeoutKillsConnection(queryTimeoutKillsConnection);
    }

    public void setResultSetSizeThreshold(int threshold) throws SQLException {
        getActiveMySQLConnection().setResultSetSizeThreshold(threshold);
    }

    public void setRetriesAllDown(int retriesAllDown) throws SQLException {
        getActiveMySQLConnection().setRetriesAllDown(retriesAllDown);
    }

    public void setRewriteBatchedStatements(boolean flag) {
        getActiveMySQLConnection().setRewriteBatchedStatements(flag);
    }

    public void setSelfDestructOnPingMaxOperations(int maxOperations) throws SQLException {
        getActiveMySQLConnection().setSelfDestructOnPingMaxOperations(maxOperations);
    }

    public void setSelfDestructOnPingSecondsLifetime(int seconds) throws SQLException {
        getActiveMySQLConnection().setSelfDestructOnPingSecondsLifetime(seconds);
    }

    public void setSlowQueryThresholdNanos(long nanos) throws SQLException {
        getActiveMySQLConnection().setSlowQueryThresholdNanos(nanos);
    }

    public void setStatementInterceptors(String value) {
        getActiveMySQLConnection().setStatementInterceptors(value);
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

    public void setTreatUtilDateAsTimestamp(boolean flag) {
        getActiveMySQLConnection().setTreatUtilDateAsTimestamp(flag);
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

    public void setUseConfigs(String configs) {
        getActiveMySQLConnection().setUseConfigs(configs);
    }

    public void setUseDirectRowUnpack(boolean flag) {
        getActiveMySQLConnection().setUseDirectRowUnpack(flag);
    }

    public void setUseDynamicCharsetInfo(boolean flag) {
        getActiveMySQLConnection().setUseDynamicCharsetInfo(flag);
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

    public void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern) {
        getActiveMySQLConnection().setUtf8OutsideBmpExcludedColumnNamePattern(regexPattern);
    }

    public void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern) {
        getActiveMySQLConnection().setUtf8OutsideBmpIncludedColumnNamePattern(regexPattern);
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

    public Timer getCancelTimer() {
        return getActiveMySQLConnection().getCancelTimer();
    }

    public String getCatalog() throws SQLException {
        return getActiveMySQLConnection().getCatalog();
    }

    public String getCharacterSetMetadata() {
        return getActiveMySQLConnection().getCharacterSetMetadata();
    }

    /**
     * @deprecated replaced by <code>getEncodingForIndex(int charsetIndex)</code>
     */
    @Deprecated
    public String getCharsetNameForIndex(int charsetIndex) throws SQLException {
        return getEncodingForIndex(charsetIndex);
    }

    public String getEncodingForIndex(int collationIndex) {
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

    public MysqlaProtocol getProtocol() {
        return getActiveMySQLConnection().getProtocol();
    }

    public MysqlJdbcConnection getMultiHostSafeProxy() {
        return getActiveMySQLConnection().getMultiHostSafeProxy();
    }

    public Log getLog() {
        return getActiveMySQLConnection().getLog();
    }

    public int getMaxBytesPerChar(String javaCharsetName) {
        return getActiveMySQLConnection().getMaxBytesPerChar(javaCharsetName);
    }

    public int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName) {
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

    public ServerVersion getServerVersion() {
        return getActiveMySQLConnection().getServerVersion();
    }

    public MysqlaSession getSession() {
        return getActiveMySQLConnection().getSession();
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

    public SQLWarning getWarnings() throws SQLException {
        return getActiveMySQLConnection().getWarnings();
    }

    public boolean hasSameProperties(JdbcConnection c) {
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

    public void initializeExtension(Extension ex) {
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

    public boolean isSameResource(JdbcConnection otherConnection) {
        return getActiveMySQLConnection().isSameResource(otherConnection);
    }

    public boolean lowerCaseTableNames() {
        return getActiveMySQLConnection().lowerCaseTableNames();
    }

    public String nativeSQL(String sql) throws SQLException {
        return getActiveMySQLConnection().nativeSQL(sql);
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

    public void setProxy(MysqlJdbcConnection proxy) {
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

    public boolean versionMeetsMinimum(int major, int minor, int subminor) {
        return getActiveMySQLConnection().versionMeetsMinimum(major, minor, subminor);
    }

    public boolean isClosed() throws SQLException {
        return getActiveMySQLConnection().isClosed();
    }

    public String getLoadBalanceConnectionGroup() {
        return getActiveMySQLConnection().getLoadBalanceConnectionGroup();
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

    public boolean getAllowMasterDownConnections() {
        return false;
    }

    public void setAllowMasterDownConnections(boolean connectIfMasterDown) {
    }

    public boolean getHaEnableJMX() {
        return false;
    }

    public void setHaEnableJMX(boolean replicationEnableJMX) {
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

    public SQLXML createSQLXML() throws SQLException {
        return getActiveMySQLConnection().createSQLXML();
    }

    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return getActiveMySQLConnection().createArrayOf(typeName, elements);
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return getActiveMySQLConnection().createStruct(typeName, attributes);
    }

    public Properties getClientInfo() throws SQLException {
        return getActiveMySQLConnection().getClientInfo();
    }

    public String getClientInfo(String name) throws SQLException {
        return getActiveMySQLConnection().getClientInfo(name);
    }

    public boolean isValid(int timeout) throws SQLException {
        synchronized (this.proxy) {
            return getActiveMySQLConnection().isValid(timeout);
        }
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        getActiveMySQLConnection().setClientInfo(properties);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        getActiveMySQLConnection().setClientInfo(name, value);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        //checkClosed();

        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }

    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

    /**
     * @throws SQLException
     * @see java.sql.Connection#createBlob()
     */
    public Blob createBlob() throws SQLException {
        return getActiveMySQLConnection().createBlob();
    }

    /**
     * @throws SQLException
     * @see java.sql.Connection#createClob()
     */
    public Clob createClob() throws SQLException {
        return getActiveMySQLConnection().createClob();
    }

    /**
     * @throws SQLException
     * @see java.sql.Connection#createNClob()
     */
    public NClob createNClob() throws SQLException {
        return getActiveMySQLConnection().createNClob();
    }

    protected ClientInfoProvider getClientInfoProviderImpl() throws SQLException {
        synchronized (this.proxy) {
            return ((ConnectionImpl) getActiveMySQLConnection()).getClientInfoProviderImpl();
        }
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

    public String getProcessHost() {
        return getActiveMySQLConnection().getProcessHost();
    }

    public boolean getReadOnlyPropagatesToServer() {
        return getActiveMySQLConnection().getReadOnlyPropagatesToServer();
    }

    public void setReadOnlyPropagatesToServer(boolean flag) {
        getActiveMySQLConnection().setReadOnlyPropagatesToServer(flag);
    }

    @Override
    public JdbcPropertySet getPropertySet() {
        return getActiveMySQLConnection().getPropertySet();
    }
}
