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
