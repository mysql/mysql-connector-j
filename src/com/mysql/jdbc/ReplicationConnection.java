/*
 Copyright (c) 2004, 2013, Oracle and/or its affiliates. All rights reserved.

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

import java.sql.CallableStatement;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executor;

import com.mysql.jdbc.log.Log;

/**
 * Connection that opens two connections, one two a replication master, and
 * another to one or more slaves, and decides to use master when the connection
 * is not read-only, and use slave(s) when the connection is read-only.
 * 
 * @version $Id: ReplicationConnection.java,v 1.1.2.1 2005/05/13 18:58:38
 *          mmatthews Exp $
 */
public class ReplicationConnection implements Connection, PingTarget {
	protected Connection currentConnection;

	protected Connection masterConnection;

	protected Connection slavesConnection;

	protected ReplicationConnection() {}
	
	public ReplicationConnection(Properties masterProperties,
			Properties slaveProperties) throws SQLException {
		NonRegisteringDriver driver = new NonRegisteringDriver();

		StringBuffer masterUrl = new StringBuffer("jdbc:mysql://");
        StringBuffer slaveUrl = new StringBuffer("jdbc:mysql:loadbalance://");

        String masterHost = masterProperties
        	.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
        
        if (masterHost != null) {
        	masterUrl.append(masterHost);
        }
 
        int numHosts = Integer.parseInt(slaveProperties.getProperty(
        		NonRegisteringDriver.NUM_HOSTS_PROPERTY_KEY));
        
        for(int i = 1; i <= numHosts; i++){
	        String slaveHost = slaveProperties
	        	.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY + "." + i);
	        
	        if (slaveHost != null) {
	        	if(i > 1){
	        		slaveUrl.append(',');
	        	}
	        	slaveUrl.append(slaveHost);
	        }
        }

        String masterDb = masterProperties
        	.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);

        masterUrl.append("/");
        
        if (masterDb != null) {
        	masterUrl.append(masterDb);
        }
        
        String slaveDb = slaveProperties
        	.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
        
        slaveUrl.append("/");
        
        if (slaveDb != null) {
        	slaveUrl.append(slaveDb);
        }
        
        slaveProperties.setProperty("roundRobinLoadBalance", "true");
        
        this.masterConnection = (com.mysql.jdbc.Connection) driver.connect(
                masterUrl.toString(), masterProperties);
        this.slavesConnection = (com.mysql.jdbc.Connection) driver.connect(
                slaveUrl.toString(), slaveProperties);
        this.slavesConnection.setReadOnly(true);
        
		this.currentConnection = this.masterConnection;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#clearWarnings()
	 */
	public synchronized void clearWarnings() throws SQLException {
		this.currentConnection.clearWarnings();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#close()
	 */
	public synchronized void close() throws SQLException {
		this.masterConnection.close();
		this.slavesConnection.close();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#commit()
	 */
	public synchronized void commit() throws SQLException {
		this.currentConnection.commit();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#createStatement()
	 */
	public Statement createStatement() throws SQLException {
		Statement stmt = this.currentConnection.createStatement();
		((com.mysql.jdbc.Statement) stmt).setPingTarget(this);
		
		return stmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#createStatement(int, int)
	 */
	public synchronized Statement createStatement(int resultSetType,
			int resultSetConcurrency) throws SQLException {
		Statement stmt = this.currentConnection.createStatement(resultSetType,
				resultSetConcurrency);
		
		((com.mysql.jdbc.Statement) stmt).setPingTarget(this);
		
		return stmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#createStatement(int, int, int)
	 */
	public synchronized Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		Statement stmt = this.currentConnection.createStatement(resultSetType,
				resultSetConcurrency, resultSetHoldability);
		
		((com.mysql.jdbc.Statement) stmt).setPingTarget(this);
		
		return stmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getAutoCommit()
	 */
	public synchronized boolean getAutoCommit() throws SQLException {
		return this.currentConnection.getAutoCommit();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getCatalog()
	 */
	public synchronized String getCatalog() throws SQLException {
		return this.currentConnection.getCatalog();
	}

	public synchronized Connection getCurrentConnection() {
		return this.currentConnection;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getHoldability()
	 */
	public synchronized int getHoldability() throws SQLException {
		return this.currentConnection.getHoldability();
	}

	public synchronized Connection getMasterConnection() {
		return this.masterConnection;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getMetaData()
	 */
	public synchronized DatabaseMetaData getMetaData() throws SQLException {
		return this.currentConnection.getMetaData();
	}

	public synchronized Connection getSlavesConnection() {
		return this.slavesConnection;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getTransactionIsolation()
	 */
	public synchronized int getTransactionIsolation() throws SQLException {
		return this.currentConnection.getTransactionIsolation();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getTypeMap()
	 */
	public synchronized Map<String, Class<?>> getTypeMap() throws SQLException {
		return this.currentConnection.getTypeMap();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getWarnings()
	 */
	public synchronized SQLWarning getWarnings() throws SQLException {
		return this.currentConnection.getWarnings();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#isClosed()
	 */
	public synchronized boolean isClosed() throws SQLException {
		return this.currentConnection.isClosed();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#isReadOnly()
	 */
	public synchronized boolean isReadOnly() throws SQLException {
		return this.currentConnection == this.slavesConnection;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#nativeSQL(java.lang.String)
	 */
	public synchronized String nativeSQL(String sql) throws SQLException {
		return this.currentConnection.nativeSQL(sql);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareCall(java.lang.String)
	 */
	public CallableStatement prepareCall(String sql) throws SQLException {
		return this.currentConnection.prepareCall(sql);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
	 */
	public synchronized CallableStatement prepareCall(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		return this.currentConnection.prepareCall(sql, resultSetType,
				resultSetConcurrency);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
	 */
	public synchronized CallableStatement prepareCall(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		return this.currentConnection.prepareCall(sql, resultSetType,
				resultSetConcurrency, resultSetHoldability);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String)
	 */
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.prepareStatement(sql);
		
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int)
	 */
	public synchronized PreparedStatement prepareStatement(String sql,
			int autoGeneratedKeys) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.prepareStatement(sql, autoGeneratedKeys);

		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
	 */
	public synchronized PreparedStatement prepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.prepareStatement(sql, resultSetType,
				resultSetConcurrency);
		
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int,
	 *      int)
	 */
	public synchronized PreparedStatement prepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.prepareStatement(sql, resultSetType,
				resultSetConcurrency, resultSetHoldability);

		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
	 */
	public synchronized PreparedStatement prepareStatement(String sql,
			int[] columnIndexes) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.prepareStatement(sql, columnIndexes);
		
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String,
	 *      java.lang.String[])
	 */
	public synchronized PreparedStatement prepareStatement(String sql,
			String[] columnNames) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.prepareStatement(sql, columnNames);

		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
	 */
	public synchronized void releaseSavepoint(Savepoint savepoint)
			throws SQLException {
		this.currentConnection.releaseSavepoint(savepoint);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#rollback()
	 */
	public synchronized void rollback() throws SQLException {
		this.currentConnection.rollback();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#rollback(java.sql.Savepoint)
	 */
	public synchronized void rollback(Savepoint savepoint) throws SQLException {
		this.currentConnection.rollback(savepoint);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#setAutoCommit(boolean)
	 */
	public synchronized void setAutoCommit(boolean autoCommit)
			throws SQLException {
		this.currentConnection.setAutoCommit(autoCommit);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#setCatalog(java.lang.String)
	 */
	public synchronized void setCatalog(String catalog) throws SQLException {
		this.currentConnection.setCatalog(catalog);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#setHoldability(int)
	 */
	public synchronized void setHoldability(int holdability)
			throws SQLException {
		this.currentConnection.setHoldability(holdability);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#setReadOnly(boolean)
	 */
	public synchronized void setReadOnly(boolean readOnly) throws SQLException {
		if (readOnly) {
			if (currentConnection != slavesConnection) {
				switchToSlavesConnection();
			}
		} else {
			if (currentConnection != masterConnection) {
				switchToMasterConnection();
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#setSavepoint()
	 */
	public synchronized Savepoint setSavepoint() throws SQLException {
		return this.currentConnection.setSavepoint();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#setSavepoint(java.lang.String)
	 */
	public synchronized Savepoint setSavepoint(String name) throws SQLException {
		return this.currentConnection.setSavepoint(name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#setTransactionIsolation(int)
	 */
	public synchronized void setTransactionIsolation(int level)
			throws SQLException {
		this.currentConnection.setTransactionIsolation(level);
	}

	// For testing

	private synchronized void switchToMasterConnection() throws SQLException {
		swapConnections(this.masterConnection, this.slavesConnection);
	}

	private synchronized void switchToSlavesConnection() throws SQLException {
		swapConnections(this.slavesConnection, this.masterConnection);
		this.slavesConnection.setReadOnly(true);
	}
	
	/**
	 * Swaps current context (catalog, autocommit and txn_isolation) from
	 * sourceConnection to targetConnection, and makes targetConnection
	 * the "current" connection that will be used for queries.
	 * 
	 * @param switchToConnection the connection to swap from
	 * @param switchFromConnection the connection to swap to
	 * 
	 * @throws SQLException if an error occurs
	 */
	private synchronized void swapConnections(Connection switchToConnection, 
			Connection switchFromConnection) throws SQLException {
		String switchFromCatalog = switchFromConnection.getCatalog();
		String switchToCatalog = switchToConnection.getCatalog();

		if (switchToCatalog != null && !switchToCatalog.equals(switchFromCatalog)) {
			switchToConnection.setCatalog(switchFromCatalog);
		} else if (switchFromCatalog != null) {
			switchToConnection.setCatalog(switchFromCatalog);
		}

		boolean switchToAutoCommit = switchToConnection.getAutoCommit();
		boolean switchFromConnectionAutoCommit = switchFromConnection.getAutoCommit();
		
		if (switchFromConnectionAutoCommit != switchToAutoCommit) {
			switchToConnection.setAutoCommit(switchFromConnectionAutoCommit);
		}

		int switchToIsolation = switchToConnection
				.getTransactionIsolation();

		int switchFromIsolation = switchFromConnection.getTransactionIsolation();
		
		if (switchFromIsolation != switchToIsolation) {
			switchToConnection
					.setTransactionIsolation(switchFromIsolation);
		}
		
		this.currentConnection = switchToConnection;
	}

	public synchronized void doPing() throws SQLException {
		if (this.masterConnection != null) {
			this.masterConnection.ping();
		}
		
		if (this.slavesConnection != null) {
			this.slavesConnection.ping();
		}
	}

	public synchronized void changeUser(String userName, String newPassword)
			throws SQLException {
		this.masterConnection.changeUser(userName, newPassword);
		this.slavesConnection.changeUser(userName, newPassword);
	}

	public synchronized void clearHasTriedMaster() {
		this.masterConnection.clearHasTriedMaster();
		this.slavesConnection.clearHasTriedMaster();
		
	}

	public synchronized PreparedStatement clientPrepareStatement(String sql)
			throws SQLException {
		PreparedStatement pstmt = this.currentConnection.clientPrepareStatement(sql);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public synchronized PreparedStatement clientPrepareStatement(String sql,
			int autoGenKeyIndex) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.clientPrepareStatement(sql, autoGenKeyIndex);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public synchronized PreparedStatement clientPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.clientPrepareStatement(sql, resultSetType, resultSetConcurrency);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public synchronized PreparedStatement clientPrepareStatement(String sql,
			int[] autoGenKeyIndexes) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.clientPrepareStatement(sql, autoGenKeyIndexes);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public synchronized PreparedStatement clientPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.clientPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public synchronized PreparedStatement clientPrepareStatement(String sql,
			String[] autoGenKeyColNames) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.clientPrepareStatement(sql, autoGenKeyColNames);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public synchronized int getActiveStatementCount() {
		return this.currentConnection.getActiveStatementCount();
	}

	public synchronized long getIdleFor() {
		return this.currentConnection.getIdleFor();
	}

	public synchronized Log getLog() throws SQLException {
		return this.currentConnection.getLog();
	}

	public synchronized String getServerCharacterEncoding() {
		return this.currentConnection.getServerCharacterEncoding();
	}

	public synchronized TimeZone getServerTimezoneTZ() {
		return this.currentConnection.getServerTimezoneTZ();
	}

	public synchronized String getStatementComment() {
		return this.currentConnection.getStatementComment();
	}

	public synchronized boolean hasTriedMaster() {
		return this.currentConnection.hasTriedMaster();
	}

	public synchronized void initializeExtension(Extension ex) throws SQLException {
		this.currentConnection.initializeExtension(ex);
	}

	public synchronized boolean isAbonormallyLongQuery(long millisOrNanos) {
		return this.currentConnection.isAbonormallyLongQuery(millisOrNanos);
	}

	public synchronized boolean isInGlobalTx() {
		return this.currentConnection.isInGlobalTx();
	}

	public synchronized boolean isMasterConnection() {
		return this.currentConnection.isMasterConnection();
	}

	public synchronized boolean isNoBackslashEscapesSet() {
		return this.currentConnection.isNoBackslashEscapesSet();
	}

	public synchronized boolean lowerCaseTableNames() {
		return this.currentConnection.lowerCaseTableNames();
	}

	public synchronized boolean parserKnowsUnicode() {
		return this.currentConnection.parserKnowsUnicode();
	}

	public synchronized void ping() throws SQLException {
		this.masterConnection.ping();
		this.slavesConnection.ping();
	}

	public synchronized void reportQueryTime(long millisOrNanos) {
		this.currentConnection.reportQueryTime(millisOrNanos);
	}

	public synchronized void resetServerState() throws SQLException {
		this.currentConnection.resetServerState();
	}

	public synchronized PreparedStatement serverPrepareStatement(String sql)
			throws SQLException {
		PreparedStatement pstmt = this.currentConnection.serverPrepareStatement(sql);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public synchronized PreparedStatement serverPrepareStatement(String sql,
			int autoGenKeyIndex) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.serverPrepareStatement(sql, autoGenKeyIndex);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public synchronized PreparedStatement serverPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.serverPrepareStatement(sql, resultSetType, resultSetConcurrency);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public synchronized PreparedStatement serverPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.serverPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public synchronized PreparedStatement serverPrepareStatement(String sql,
			int[] autoGenKeyIndexes) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.serverPrepareStatement(sql, autoGenKeyIndexes);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public synchronized PreparedStatement serverPrepareStatement(String sql,
			String[] autoGenKeyColNames) throws SQLException {
		PreparedStatement pstmt = this.currentConnection.serverPrepareStatement(sql, autoGenKeyColNames);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public synchronized void setFailedOver(boolean flag) {
		this.currentConnection.setFailedOver(flag);
	}

	public synchronized void setPreferSlaveDuringFailover(boolean flag) {
		this.currentConnection.setPreferSlaveDuringFailover(flag);
	}

	public synchronized void setStatementComment(String comment) {
		this.masterConnection.setStatementComment(comment);
		this.slavesConnection.setStatementComment(comment);
	}

	public synchronized void shutdownServer() throws SQLException {
		this.currentConnection.shutdownServer();
	}

	public synchronized boolean supportsIsolationLevel() {
		return this.currentConnection.supportsIsolationLevel();
	}

	public synchronized boolean supportsQuotedIdentifiers() {
		return this.currentConnection.supportsQuotedIdentifiers();
	}

	public synchronized boolean supportsTransactions() {
		return this.currentConnection.supportsTransactions();
	}

	public synchronized boolean versionMeetsMinimum(int major, int minor, int subminor)
			throws SQLException {
		return this.currentConnection.versionMeetsMinimum(major, minor, subminor);
	}

	public synchronized String exposeAsXml() throws SQLException {
		return this.currentConnection.exposeAsXml();
	}

	public synchronized boolean getAllowLoadLocalInfile() {
		return this.currentConnection.getAllowLoadLocalInfile();
	}

	public synchronized boolean getAllowMultiQueries() {
		return this.currentConnection.getAllowMultiQueries();
	}

	public synchronized boolean getAllowNanAndInf() {
		return this.currentConnection.getAllowNanAndInf();
	}

	public synchronized boolean getAllowUrlInLocalInfile() {
		return this.currentConnection.getAllowUrlInLocalInfile();
	}

	public synchronized boolean getAlwaysSendSetIsolation() {
		return this.currentConnection.getAlwaysSendSetIsolation();
	}

	public synchronized boolean getAutoClosePStmtStreams() {
		return this.currentConnection.getAutoClosePStmtStreams();
	}

	public synchronized boolean getAutoDeserialize() {
		return this.currentConnection.getAutoDeserialize();
	}

	public synchronized boolean getAutoGenerateTestcaseScript() {
		return this.currentConnection.getAutoGenerateTestcaseScript();
	}

	public synchronized boolean getAutoReconnectForPools() {
		return this.currentConnection.getAutoReconnectForPools();
	}

	public synchronized boolean getAutoSlowLog() {
		return this.currentConnection.getAutoSlowLog();
	}

	public synchronized int getBlobSendChunkSize() {
		return this.currentConnection.getBlobSendChunkSize();
	}

	public synchronized boolean getBlobsAreStrings() {
		return this.currentConnection.getBlobsAreStrings();
	}

	public synchronized boolean getCacheCallableStatements() {
		return this.currentConnection.getCacheCallableStatements();
	}

	public synchronized boolean getCacheCallableStmts() {
		return this.currentConnection.getCacheCallableStmts();
	}

	public synchronized boolean getCachePrepStmts() {
		return this.currentConnection.getCachePrepStmts();
	}

	public synchronized boolean getCachePreparedStatements() {
		return this.currentConnection.getCachePreparedStatements();
	}

	public synchronized boolean getCacheResultSetMetadata() {
		return this.currentConnection.getCacheResultSetMetadata();
	}

	public synchronized boolean getCacheServerConfiguration() {
		return this.currentConnection.getCacheServerConfiguration();
	}

	public synchronized int getCallableStatementCacheSize() {
		return this.currentConnection.getCallableStatementCacheSize();
	}

	public synchronized int getCallableStmtCacheSize() {
		return this.currentConnection.getCallableStmtCacheSize();
	}

	public synchronized boolean getCapitalizeTypeNames() {
		return this.currentConnection.getCapitalizeTypeNames();
	}

	public synchronized String getCharacterSetResults() {
		return this.currentConnection.getCharacterSetResults();
	}

	public synchronized String getClientCertificateKeyStorePassword() {
		return this.currentConnection.getClientCertificateKeyStorePassword();
	}

	public synchronized String getClientCertificateKeyStoreType() {
		return this.currentConnection.getClientCertificateKeyStoreType();
	}

	public synchronized String getClientCertificateKeyStoreUrl() {
		return this.currentConnection.getClientCertificateKeyStoreUrl();
	}

	public synchronized String getClientInfoProvider() {
		return this.currentConnection.getClientInfoProvider();
	}

	public synchronized String getClobCharacterEncoding() {
		return this.currentConnection.getClobCharacterEncoding();
	}

	public synchronized boolean getClobberStreamingResults() {
		return this.currentConnection.getClobberStreamingResults();
	}

	public synchronized int getConnectTimeout() {
		return this.currentConnection.getConnectTimeout();
	}

	public synchronized String getConnectionCollation() {
		return this.currentConnection.getConnectionCollation();
	}

	public synchronized String getConnectionLifecycleInterceptors() {
		return this.currentConnection.getConnectionLifecycleInterceptors();
	}

	public synchronized boolean getContinueBatchOnError() {
		return this.currentConnection.getContinueBatchOnError();
	}

	public synchronized boolean getCreateDatabaseIfNotExist() {
		return this.currentConnection.getCreateDatabaseIfNotExist();
	}

	public synchronized int getDefaultFetchSize() {
		return this.currentConnection.getDefaultFetchSize();
	}

	public synchronized boolean getDontTrackOpenResources() {
		return this.currentConnection.getDontTrackOpenResources();
	}

	public synchronized boolean getDumpMetadataOnColumnNotFound() {
		return this.currentConnection.getDumpMetadataOnColumnNotFound();
	}

	public synchronized boolean getDumpQueriesOnException() {
		return this.currentConnection.getDumpQueriesOnException();
	}

	public synchronized boolean getDynamicCalendars() {
		return this.currentConnection.getDynamicCalendars();
	}

	public synchronized boolean getElideSetAutoCommits() {
		return this.currentConnection.getElideSetAutoCommits();
	}

	public synchronized boolean getEmptyStringsConvertToZero() {
		return this.currentConnection.getEmptyStringsConvertToZero();
	}

	public synchronized boolean getEmulateLocators() {
		return this.currentConnection.getEmulateLocators();
	}

	public synchronized boolean getEmulateUnsupportedPstmts() {
		return this.currentConnection.getEmulateUnsupportedPstmts();
	}

	public synchronized boolean getEnablePacketDebug() {
		return this.currentConnection.getEnablePacketDebug();
	}

	public synchronized boolean getEnableQueryTimeouts() {
		return this.currentConnection.getEnableQueryTimeouts();
	}

	public synchronized String getEncoding() {
		return this.currentConnection.getEncoding();
	}

	public synchronized boolean getExplainSlowQueries() {
		return this.currentConnection.getExplainSlowQueries();
	}

	public synchronized boolean getFailOverReadOnly() {
		return this.currentConnection.getFailOverReadOnly();
	}

	public synchronized boolean getFunctionsNeverReturnBlobs() {
		return this.currentConnection.getFunctionsNeverReturnBlobs();
	}

	public synchronized boolean getGatherPerfMetrics() {
		return this.currentConnection.getGatherPerfMetrics();
	}

	public synchronized boolean getGatherPerformanceMetrics() {
		return this.currentConnection.getGatherPerformanceMetrics();
	}

	public synchronized boolean getGenerateSimpleParameterMetadata() {
		return this.currentConnection.getGenerateSimpleParameterMetadata();
	}

	public synchronized boolean getHoldResultsOpenOverStatementClose() {
		return this.currentConnection.getHoldResultsOpenOverStatementClose();
	}

	public synchronized boolean getIgnoreNonTxTables() {
		return this.currentConnection.getIgnoreNonTxTables();
	}

	public synchronized boolean getIncludeInnodbStatusInDeadlockExceptions() {
		return this.currentConnection.getIncludeInnodbStatusInDeadlockExceptions();
	}

	public synchronized int getInitialTimeout() {
		return this.currentConnection.getInitialTimeout();
	}

	public synchronized boolean getInteractiveClient() {
		return this.currentConnection.getInteractiveClient();
	}

	public synchronized boolean getIsInteractiveClient() {
		return this.currentConnection.getIsInteractiveClient();
	}

	public synchronized boolean getJdbcCompliantTruncation() {
		return this.currentConnection.getJdbcCompliantTruncation();
	}

	public synchronized boolean getJdbcCompliantTruncationForReads() {
		return this.currentConnection.getJdbcCompliantTruncationForReads();
	}

	public synchronized String getLargeRowSizeThreshold() {
		return this.currentConnection.getLargeRowSizeThreshold();
	}

	public synchronized String getLoadBalanceStrategy() {
		return this.currentConnection.getLoadBalanceStrategy();
	}

	public synchronized String getLocalSocketAddress() {
		return this.currentConnection.getLocalSocketAddress();
	}

	public synchronized int getLocatorFetchBufferSize() {
		return this.currentConnection.getLocatorFetchBufferSize();
	}

	public synchronized boolean getLogSlowQueries() {
		return this.currentConnection.getLogSlowQueries();
	}

	public synchronized boolean getLogXaCommands() {
		return this.currentConnection.getLogXaCommands();
	}

	public synchronized String getLogger() {
		return this.currentConnection.getLogger();
	}

	public synchronized String getLoggerClassName() {
		return this.currentConnection.getLoggerClassName();
	}

	public synchronized boolean getMaintainTimeStats() {
		return this.currentConnection.getMaintainTimeStats();
	}

	public synchronized int getMaxQuerySizeToLog() {
		return this.currentConnection.getMaxQuerySizeToLog();
	}

	public synchronized int getMaxReconnects() {
		return this.currentConnection.getMaxReconnects();
	}

	public synchronized int getMaxRows() {
		return this.currentConnection.getMaxRows();
	}

	public synchronized int getMetadataCacheSize() {
		return this.currentConnection.getMetadataCacheSize();
	}

	public synchronized int getNetTimeoutForStreamingResults() {
		return this.currentConnection.getNetTimeoutForStreamingResults();
	}

	public synchronized boolean getNoAccessToProcedureBodies() {
		return this.currentConnection.getNoAccessToProcedureBodies();
	}

	public synchronized boolean getNoDatetimeStringSync() {
		return this.currentConnection.getNoDatetimeStringSync();
	}

	public synchronized boolean getNoTimezoneConversionForTimeType() {
		return this.currentConnection.getNoTimezoneConversionForTimeType();
	}

	public synchronized boolean getNullCatalogMeansCurrent() {
		return this.currentConnection.getNullCatalogMeansCurrent();
	}

	public synchronized boolean getNullNamePatternMatchesAll() {
		return this.currentConnection.getNullNamePatternMatchesAll();
	}

	public synchronized boolean getOverrideSupportsIntegrityEnhancementFacility() {
		return this.currentConnection.getOverrideSupportsIntegrityEnhancementFacility();
	}

	public synchronized int getPacketDebugBufferSize() {
		return this.currentConnection.getPacketDebugBufferSize();
	}

	public synchronized boolean getPadCharsWithSpace() {
		return this.currentConnection.getPadCharsWithSpace();
	}

	public synchronized boolean getParanoid() {
		return this.currentConnection.getParanoid();
	}

	public synchronized boolean getPedantic() {
		return this.currentConnection.getPedantic();
	}

	public synchronized boolean getPinGlobalTxToPhysicalConnection() {
		return this.currentConnection.getPinGlobalTxToPhysicalConnection();
	}

	public synchronized boolean getPopulateInsertRowWithDefaultValues() {
		return this.currentConnection.getPopulateInsertRowWithDefaultValues();
	}

	public synchronized int getPrepStmtCacheSize() {
		return this.currentConnection.getPrepStmtCacheSize();
	}

	public synchronized int getPrepStmtCacheSqlLimit() {
		return this.currentConnection.getPrepStmtCacheSqlLimit();
	}

	public synchronized int getPreparedStatementCacheSize() {
		return this.currentConnection.getPreparedStatementCacheSize();
	}

	public synchronized int getPreparedStatementCacheSqlLimit() {
		return this.currentConnection.getPreparedStatementCacheSqlLimit();
	}

	public synchronized boolean getProcessEscapeCodesForPrepStmts() {
		return this.currentConnection.getProcessEscapeCodesForPrepStmts();
	}

	public synchronized boolean getProfileSQL() {
		return this.currentConnection.getProfileSQL();
	}

	public synchronized boolean getProfileSql() {
		return this.currentConnection.getProfileSql();
	}

	public synchronized String getProfilerEventHandler() {
		return this.currentConnection.getProfilerEventHandler();
	}

	public synchronized String getPropertiesTransform() {
		return this.currentConnection.getPropertiesTransform();
	}

	public synchronized int getQueriesBeforeRetryMaster() {
		return this.currentConnection.getQueriesBeforeRetryMaster();
	}

	public synchronized boolean getReconnectAtTxEnd() {
		return this.currentConnection.getReconnectAtTxEnd();
	}

	public synchronized boolean getRelaxAutoCommit() {
		return this.currentConnection.getRelaxAutoCommit();
	}

	public synchronized int getReportMetricsIntervalMillis() {
		return this.currentConnection.getReportMetricsIntervalMillis();
	}

	public synchronized boolean getRequireSSL() {
		return this.currentConnection.getRequireSSL();
	}

	public synchronized String getResourceId() {
		return this.currentConnection.getResourceId();
	}

	public synchronized int getResultSetSizeThreshold() {
		return this.currentConnection.getResultSetSizeThreshold();
	}

	public synchronized boolean getRewriteBatchedStatements() {
		return this.currentConnection.getRewriteBatchedStatements();
	}

	public synchronized boolean getRollbackOnPooledClose() {
		return this.currentConnection.getRollbackOnPooledClose();
	}

	public synchronized boolean getRoundRobinLoadBalance() {
		return this.currentConnection.getRoundRobinLoadBalance();
	}

	public synchronized boolean getRunningCTS13() {
		return this.currentConnection.getRunningCTS13();
	}

	public synchronized int getSecondsBeforeRetryMaster() {
		return this.currentConnection.getSecondsBeforeRetryMaster();
	}

	public synchronized int getSelfDestructOnPingMaxOperations() {
		return this.currentConnection.getSelfDestructOnPingMaxOperations();
	}

	public synchronized int getSelfDestructOnPingSecondsLifetime() {
		return this.currentConnection.getSelfDestructOnPingSecondsLifetime();
	}

	public synchronized String getServerTimezone() {
		return this.currentConnection.getServerTimezone();
	}

	public synchronized String getSessionVariables() {
		return this.currentConnection.getSessionVariables();
	}

	public synchronized int getSlowQueryThresholdMillis() {
		return this.currentConnection.getSlowQueryThresholdMillis();
	}

	public synchronized long getSlowQueryThresholdNanos() {
		return this.currentConnection.getSlowQueryThresholdNanos();
	}

	public synchronized String getSocketFactory() {
		return this.currentConnection.getSocketFactory();
	}

	public synchronized String getSocketFactoryClassName() {
		return this.currentConnection.getSocketFactoryClassName();
	}

	public synchronized int getSocketTimeout() {
		return this.currentConnection.getSocketTimeout();
	}

	public synchronized String getStatementInterceptors() {
		return this.currentConnection.getStatementInterceptors();
	}

	public synchronized boolean getStrictFloatingPoint() {
		return this.currentConnection.getStrictFloatingPoint();
	}

	public synchronized boolean getStrictUpdates() {
		return this.currentConnection.getStrictUpdates();
	}

	public synchronized boolean getTcpKeepAlive() {
		return this.currentConnection.getTcpKeepAlive();
	}

	public synchronized boolean getTcpNoDelay() {
		return this.currentConnection.getTcpNoDelay();
	}

	public synchronized int getTcpRcvBuf() {
		return this.currentConnection.getTcpRcvBuf();
	}

	public synchronized int getTcpSndBuf() {
		return this.currentConnection.getTcpSndBuf();
	}

	public synchronized int getTcpTrafficClass() {
		return this.currentConnection.getTcpTrafficClass();
	}

	public synchronized boolean getTinyInt1isBit() {
		return this.currentConnection.getTinyInt1isBit();
	}

	public synchronized boolean getTraceProtocol() {
		return this.currentConnection.getTraceProtocol();
	}

	public synchronized boolean getTransformedBitIsBoolean() {
		return this.currentConnection.getTransformedBitIsBoolean();
	}

	public synchronized boolean getTreatUtilDateAsTimestamp() {
		return this.currentConnection.getTreatUtilDateAsTimestamp();
	}

	public synchronized String getTrustCertificateKeyStorePassword() {
		return this.currentConnection.getTrustCertificateKeyStorePassword();
	}

	public synchronized String getTrustCertificateKeyStoreType() {
		return this.currentConnection.getTrustCertificateKeyStoreType();
	}

	public synchronized String getTrustCertificateKeyStoreUrl() {
		return this.currentConnection.getTrustCertificateKeyStoreUrl();
	}

	public synchronized boolean getUltraDevHack() {
		return this.currentConnection.getUltraDevHack();
	}

	public synchronized boolean getUseBlobToStoreUTF8OutsideBMP() {
		return this.currentConnection.getUseBlobToStoreUTF8OutsideBMP();
	}

	public synchronized boolean getUseCompression() {
		return this.currentConnection.getUseCompression();
	}

	public synchronized String getUseConfigs() {
		return this.currentConnection.getUseConfigs();
	}

	public synchronized boolean getUseCursorFetch() {
		return this.currentConnection.getUseCursorFetch();
	}

	public synchronized boolean getUseDirectRowUnpack() {
		return this.currentConnection.getUseDirectRowUnpack();
	}

	public synchronized boolean getUseDynamicCharsetInfo() {
		return this.currentConnection.getUseDynamicCharsetInfo();
	}

	public synchronized boolean getUseFastDateParsing() {
		return this.currentConnection.getUseFastDateParsing();
	}

	public synchronized boolean getUseFastIntParsing() {
		return this.currentConnection.getUseFastIntParsing();
	}

	public synchronized boolean getUseGmtMillisForDatetimes() {
		return this.currentConnection.getUseGmtMillisForDatetimes();
	}

	public synchronized boolean getUseHostsInPrivileges() {
		return this.currentConnection.getUseHostsInPrivileges();
	}

	public synchronized boolean getUseInformationSchema() {
		return this.currentConnection.getUseInformationSchema();
	}

	public synchronized boolean getUseJDBCCompliantTimezoneShift() {
		return this.currentConnection.getUseJDBCCompliantTimezoneShift();
	}

	public synchronized boolean getUseJvmCharsetConverters() {
		return this.currentConnection.getUseJvmCharsetConverters();
	}

	public synchronized boolean getUseLegacyDatetimeCode() {
		return this.currentConnection.getUseLegacyDatetimeCode();
	}

	public synchronized boolean getUseLocalSessionState() {
		return this.currentConnection.getUseLocalSessionState();
	}

	public synchronized boolean getUseNanosForElapsedTime() {
		return this.currentConnection.getUseNanosForElapsedTime();
	}

	public synchronized boolean getUseOldAliasMetadataBehavior() {
		return this.currentConnection.getUseOldAliasMetadataBehavior();
	}

	public synchronized boolean getUseOldUTF8Behavior() {
		return this.currentConnection.getUseOldUTF8Behavior();
	}

	public synchronized boolean getUseOnlyServerErrorMessages() {
		return this.currentConnection.getUseOnlyServerErrorMessages();
	}

	public synchronized boolean getUseReadAheadInput() {
		return this.currentConnection.getUseReadAheadInput();
	}

	public synchronized boolean getUseSSL() {
		return this.currentConnection.getUseSSL();
	}

	public synchronized boolean getUseSSPSCompatibleTimezoneShift() {
		return this.currentConnection.getUseSSPSCompatibleTimezoneShift();
	}

	public synchronized boolean getUseServerPrepStmts() {
		return this.currentConnection.getUseServerPrepStmts();
	}

	public synchronized boolean getUseServerPreparedStmts() {
		return this.currentConnection.getUseServerPreparedStmts();
	}

	public synchronized boolean getUseSqlStateCodes() {
		return this.currentConnection.getUseSqlStateCodes();
	}

	public synchronized boolean getUseStreamLengthsInPrepStmts() {
		return this.currentConnection.getUseStreamLengthsInPrepStmts();
	}

	public synchronized boolean getUseTimezone() {
		return this.currentConnection.getUseTimezone();
	}

	public synchronized boolean getUseUltraDevWorkAround() {
		return this.currentConnection.getUseUltraDevWorkAround();
	}

	public synchronized boolean getUseUnbufferedInput() {
		return this.currentConnection.getUseUnbufferedInput();
	}

	public synchronized boolean getUseUnicode() {
		return this.currentConnection.getUseUnicode();
	}

	public synchronized boolean getUseUsageAdvisor() {
		return this.currentConnection.getUseUsageAdvisor();
	}

	public synchronized String getUtf8OutsideBmpExcludedColumnNamePattern() {
		return this.currentConnection.getUtf8OutsideBmpExcludedColumnNamePattern();
	}

	public synchronized String getUtf8OutsideBmpIncludedColumnNamePattern() {
		return this.currentConnection.getUtf8OutsideBmpIncludedColumnNamePattern();
	}

	public synchronized boolean getVerifyServerCertificate() {
		return this.currentConnection.getVerifyServerCertificate();
	}

	public synchronized boolean getYearIsDateType() {
		return this.currentConnection.getYearIsDateType();
	}

	public synchronized String getZeroDateTimeBehavior() {
		return this.currentConnection.getZeroDateTimeBehavior();
	}

	public synchronized void setAllowLoadLocalInfile(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setAllowMultiQueries(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setAllowNanAndInf(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setAllowUrlInLocalInfile(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setAlwaysSendSetIsolation(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setAutoClosePStmtStreams(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setAutoDeserialize(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setAutoGenerateTestcaseScript(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setAutoReconnect(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setAutoReconnectForConnectionPools(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setAutoReconnectForPools(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setAutoSlowLog(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setBlobSendChunkSize(String value) throws SQLException {
		// not runtime configurable
		
	}

	public synchronized void setBlobsAreStrings(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setCacheCallableStatements(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setCacheCallableStmts(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setCachePrepStmts(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setCachePreparedStatements(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setCacheResultSetMetadata(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setCacheServerConfiguration(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setCallableStatementCacheSize(int size) {
		// not runtime configurable
		
	}

	public synchronized void setCallableStmtCacheSize(int cacheSize) {
		// not runtime configurable
		
	}

	public synchronized void setCapitalizeDBMDTypes(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setCapitalizeTypeNames(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setCharacterEncoding(String encoding) {
		// not runtime configurable
		
	}

	public synchronized void setCharacterSetResults(String characterSet) {
		// not runtime configurable
		
	}

	public synchronized void setClientCertificateKeyStorePassword(String value) {
		// not runtime configurable
		
	}

	public synchronized void setClientCertificateKeyStoreType(String value) {
		// not runtime configurable
		
	}

	public synchronized void setClientCertificateKeyStoreUrl(String value) {
		// not runtime configurable
		
	}

	public synchronized void setClientInfoProvider(String classname) {
		// not runtime configurable
		
	}

	public synchronized void setClobCharacterEncoding(String encoding) {
		// not runtime configurable
		
	}

	public synchronized void setClobberStreamingResults(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setConnectTimeout(int timeoutMs) {
		// not runtime configurable
		
	}

	public synchronized void setConnectionCollation(String collation) {
		// not runtime configurable
		
	}

	public synchronized void setConnectionLifecycleInterceptors(String interceptors) {
		// not runtime configurable
		
	}

	public synchronized void setContinueBatchOnError(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setCreateDatabaseIfNotExist(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setDefaultFetchSize(int n) {
		// not runtime configurable
		
	}

	public synchronized void setDetectServerPreparedStmts(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setDontTrackOpenResources(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setDumpMetadataOnColumnNotFound(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setDumpQueriesOnException(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setDynamicCalendars(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setElideSetAutoCommits(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setEmptyStringsConvertToZero(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setEmulateLocators(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setEmulateUnsupportedPstmts(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setEnablePacketDebug(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setEnableQueryTimeouts(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setEncoding(String property) {
		// not runtime configurable
		
	}

	public synchronized void setExplainSlowQueries(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setFailOverReadOnly(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setFunctionsNeverReturnBlobs(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setGatherPerfMetrics(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setGatherPerformanceMetrics(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setGenerateSimpleParameterMetadata(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setHoldResultsOpenOverStatementClose(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setIgnoreNonTxTables(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setIncludeInnodbStatusInDeadlockExceptions(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setInitialTimeout(int property) {
		// not runtime configurable
		
	}

	public synchronized void setInteractiveClient(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setIsInteractiveClient(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setJdbcCompliantTruncation(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setJdbcCompliantTruncationForReads(
			boolean jdbcCompliantTruncationForReads) {
		// not runtime configurable
		
	}

	public synchronized void setLargeRowSizeThreshold(String value) {
		// not runtime configurable
		
	}

	public synchronized void setLoadBalanceStrategy(String strategy) {
		// not runtime configurable
		
	}

	public synchronized void setLocalSocketAddress(String address) {
		// not runtime configurable
		
	}

	public synchronized void setLocatorFetchBufferSize(String value) throws SQLException {
		// not runtime configurable
		
	}

	public synchronized void setLogSlowQueries(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setLogXaCommands(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setLogger(String property) {
		// not runtime configurable
		
	}

	public synchronized void setLoggerClassName(String className) {
		// not runtime configurable
		
	}

	public synchronized void setMaintainTimeStats(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setMaxQuerySizeToLog(int sizeInBytes) {
		// not runtime configurable
		
	}

	public synchronized void setMaxReconnects(int property) {
		// not runtime configurable
		
	}

	public synchronized void setMaxRows(int property) {
		// not runtime configurable
		
	}

	public synchronized void setMetadataCacheSize(int value) {
		// not runtime configurable
		
	}

	public synchronized void setNetTimeoutForStreamingResults(int value) {
		// not runtime configurable
		
	}

	public synchronized void setNoAccessToProcedureBodies(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setNoDatetimeStringSync(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setNoTimezoneConversionForTimeType(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setNullCatalogMeansCurrent(boolean value) {
		// not runtime configurable
		
	}

	public synchronized void setNullNamePatternMatchesAll(boolean value) {
		// not runtime configurable
		
	}

	public synchronized void setOverrideSupportsIntegrityEnhancementFacility(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setPacketDebugBufferSize(int size) {
		// not runtime configurable
		
	}

	public synchronized void setPadCharsWithSpace(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setParanoid(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setPedantic(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setPinGlobalTxToPhysicalConnection(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setPopulateInsertRowWithDefaultValues(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setPrepStmtCacheSize(int cacheSize) {
		// not runtime configurable
		
	}

	public synchronized void setPrepStmtCacheSqlLimit(int sqlLimit) {
		// not runtime configurable
		
	}

	public synchronized void setPreparedStatementCacheSize(int cacheSize) {
		// not runtime configurable
		
	}

	public synchronized void setPreparedStatementCacheSqlLimit(int cacheSqlLimit) {
		// not runtime configurable
		
	}

	public synchronized void setProcessEscapeCodesForPrepStmts(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setProfileSQL(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setProfileSql(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setProfilerEventHandler(String handler) {
		// not runtime configurable
		
	}

	public synchronized void setPropertiesTransform(String value) {
		// not runtime configurable
		
	}

	public synchronized void setQueriesBeforeRetryMaster(int property) {
		// not runtime configurable
		
	}

	public synchronized void setReconnectAtTxEnd(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setRelaxAutoCommit(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setReportMetricsIntervalMillis(int millis) {
		// not runtime configurable
		
	}

	public synchronized void setRequireSSL(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setResourceId(String resourceId) {
		// not runtime configurable
		
	}

	public synchronized void setResultSetSizeThreshold(int threshold) {
		// not runtime configurable
		
	}

	public synchronized void setRetainStatementAfterResultSetClose(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setRewriteBatchedStatements(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setRollbackOnPooledClose(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setRoundRobinLoadBalance(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setRunningCTS13(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setSecondsBeforeRetryMaster(int property) {
		// not runtime configurable
		
	}

	public synchronized void setSelfDestructOnPingMaxOperations(int maxOperations) {
		// not runtime configurable
		
	}

	public synchronized void setSelfDestructOnPingSecondsLifetime(int seconds) {
		// not runtime configurable
		
	}

	public synchronized void setServerTimezone(String property) {
		// not runtime configurable
		
	}

	public synchronized void setSessionVariables(String variables) {
		// not runtime configurable
		
	}

	public synchronized void setSlowQueryThresholdMillis(int millis) {
		// not runtime configurable
		
	}

	public synchronized void setSlowQueryThresholdNanos(long nanos) {
		// not runtime configurable
		
	}

	public synchronized void setSocketFactory(String name) {
		// not runtime configurable
		
	}

	public synchronized void setSocketFactoryClassName(String property) {
		// not runtime configurable
		
	}

	public synchronized void setSocketTimeout(int property) {
		// not runtime configurable
		
	}

	public synchronized void setStatementInterceptors(String value) {
		// not runtime configurable
		
	}

	public synchronized void setStrictFloatingPoint(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setStrictUpdates(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setTcpKeepAlive(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setTcpNoDelay(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setTcpRcvBuf(int bufSize) {
		// not runtime configurable
		
	}

	public synchronized void setTcpSndBuf(int bufSize) {
		// not runtime configurable
		
	}

	public synchronized void setTcpTrafficClass(int classFlags) {
		// not runtime configurable
		
	}

	public synchronized void setTinyInt1isBit(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setTraceProtocol(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setTransformedBitIsBoolean(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setTreatUtilDateAsTimestamp(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setTrustCertificateKeyStorePassword(String value) {
		// not runtime configurable
		
	}

	public synchronized void setTrustCertificateKeyStoreType(String value) {
		// not runtime configurable
		
	}

	public synchronized void setTrustCertificateKeyStoreUrl(String value) {
		// not runtime configurable
		
	}

	public synchronized void setUltraDevHack(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseBlobToStoreUTF8OutsideBMP(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseCompression(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setUseConfigs(String configs) {
		// not runtime configurable
		
	}

	public synchronized void setUseCursorFetch(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseDirectRowUnpack(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseDynamicCharsetInfo(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseFastDateParsing(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseFastIntParsing(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseGmtMillisForDatetimes(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseHostsInPrivileges(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setUseInformationSchema(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseJDBCCompliantTimezoneShift(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseJvmCharsetConverters(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseLegacyDatetimeCode(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseLocalSessionState(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseNanosForElapsedTime(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseOldAliasMetadataBehavior(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseOldUTF8Behavior(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseOnlyServerErrorMessages(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseReadAheadInput(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseSSL(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setUseSSPSCompatibleTimezoneShift(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseServerPrepStmts(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseServerPreparedStmts(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseSqlStateCodes(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseStreamLengthsInPrepStmts(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setUseTimezone(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setUseUltraDevWorkAround(boolean property) {
		// not runtime configurable
		
	}

	public synchronized void setUseUnbufferedInput(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseUnicode(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setUseUsageAdvisor(boolean useUsageAdvisorFlag) {
		// not runtime configurable
		
	}

	public synchronized void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern) {
		// not runtime configurable
		
	}

	public synchronized void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern) {
		// not runtime configurable
		
	}

	public synchronized void setVerifyServerCertificate(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setYearIsDateType(boolean flag) {
		// not runtime configurable
		
	}

	public synchronized void setZeroDateTimeBehavior(String behavior) {
		// not runtime configurable
		
	}

	public synchronized boolean useUnbufferedInput() {
		return this.currentConnection.useUnbufferedInput();
	}

	public synchronized boolean isSameResource(Connection c) {
		return this.currentConnection.isSameResource(c);
	}

	public void setInGlobalTx(boolean flag) {
		this.currentConnection.setInGlobalTx(flag);
	}

	public boolean getUseColumnNamesInFindColumn() {
		return this.currentConnection.getUseColumnNamesInFindColumn();
	}

	public void setUseColumnNamesInFindColumn(boolean flag) {
		// not runtime configurable
	}

	public boolean getUseLocalTransactionState() {
		return this.currentConnection.getUseLocalTransactionState();
	}

	public void setUseLocalTransactionState(boolean flag) {
		// not runtime configurable
		
	}

	public boolean getCompensateOnDuplicateKeyUpdateCounts() {
		return this.currentConnection.getCompensateOnDuplicateKeyUpdateCounts();
	}

	public void setCompensateOnDuplicateKeyUpdateCounts(boolean flag) {
		// not runtime configurable
		
	}

	public boolean getUseAffectedRows() {
		return this.currentConnection.getUseAffectedRows();
	}

	public void setUseAffectedRows(boolean flag) {
		// not runtime configurable
		
	}

	public String getPasswordCharacterEncoding() {
		return this.currentConnection.getPasswordCharacterEncoding();
	}

	public void setPasswordCharacterEncoding(String characterSet) {
		this.currentConnection.setPasswordCharacterEncoding(characterSet);
	}

	public int getAutoIncrementIncrement() {
		return this.currentConnection.getAutoIncrementIncrement();
	}

	public int getLoadBalanceBlacklistTimeout() {
		return this.currentConnection.getLoadBalanceBlacklistTimeout();
	}

	public void setLoadBalanceBlacklistTimeout(int loadBalanceBlacklistTimeout) {
		this.currentConnection.setLoadBalanceBlacklistTimeout(loadBalanceBlacklistTimeout);
	}
	
	public int getLoadBalancePingTimeout() {
		return this.currentConnection.getLoadBalancePingTimeout();
	}

	public void setLoadBalancePingTimeout(int loadBalancePingTimeout) {
		this.currentConnection.setLoadBalancePingTimeout(loadBalancePingTimeout);
	}
	
	public boolean getLoadBalanceValidateConnectionOnSwapServer() {
		return this.currentConnection.getLoadBalanceValidateConnectionOnSwapServer();
	}

	public void setLoadBalanceValidateConnectionOnSwapServer(boolean loadBalanceValidateConnectionOnSwapServer) {
		this.currentConnection.setLoadBalanceValidateConnectionOnSwapServer(loadBalanceValidateConnectionOnSwapServer);
	}

	public int getRetriesAllDown() {
		return this.currentConnection.getRetriesAllDown();
	}

	public void setRetriesAllDown(int retriesAllDown) {
		this.currentConnection.setRetriesAllDown(retriesAllDown);
	}

	public ExceptionInterceptor getExceptionInterceptor() {
		return this.currentConnection.getExceptionInterceptor();
	}

	public String getExceptionInterceptors() {
		return this.currentConnection.getExceptionInterceptors();
	}

	public void setExceptionInterceptors(String exceptionInterceptors) {
		this.currentConnection.setExceptionInterceptors(exceptionInterceptors);
	}

	public boolean getQueryTimeoutKillsConnection() {
		return this.currentConnection.getQueryTimeoutKillsConnection();
	}

	public void setQueryTimeoutKillsConnection(
			boolean queryTimeoutKillsConnection) {
		this.currentConnection.setQueryTimeoutKillsConnection(queryTimeoutKillsConnection);
	}

	public boolean hasSameProperties(Connection c) {
		return this.masterConnection.hasSameProperties(c) && 
			this.slavesConnection.hasSameProperties(c);
	}
	
	public Properties getProperties() {
		Properties props = new Properties();
		props.putAll(this.masterConnection.getProperties());
		props.putAll(this.slavesConnection.getProperties());
		
		return props;
	}

   public String getHost() {
      return currentConnection.getHost();
   }

   public void setProxy(MySQLConnection proxy) {
      currentConnection.setProxy(proxy);
   }

 	public synchronized boolean getRetainStatementAfterResultSetClose() {
		return currentConnection.getRetainStatementAfterResultSetClose();
	}

 	public int getMaxAllowedPacket() {
		return currentConnection.getMaxAllowedPacket();
	}
 	
	public String getLoadBalanceConnectionGroup() {
		return currentConnection.getLoadBalanceConnectionGroup();
	}

	public boolean getLoadBalanceEnableJMX() {
		return currentConnection.getLoadBalanceEnableJMX();
	}

	public String getLoadBalanceExceptionChecker() {
		return currentConnection
				.getLoadBalanceExceptionChecker();
	}

	public String getLoadBalanceSQLExceptionSubclassFailover() {
		return currentConnection
				.getLoadBalanceSQLExceptionSubclassFailover();
	}

	public String getLoadBalanceSQLStateFailover() {
		return currentConnection
				.getLoadBalanceSQLStateFailover();
	}

	public void setLoadBalanceConnectionGroup(String loadBalanceConnectionGroup) {
		currentConnection
				.setLoadBalanceConnectionGroup(loadBalanceConnectionGroup);
		
	}

	public void setLoadBalanceEnableJMX(boolean loadBalanceEnableJMX) {
		currentConnection
				.setLoadBalanceEnableJMX(loadBalanceEnableJMX);
		
	}

	public void setLoadBalanceExceptionChecker(
			String loadBalanceExceptionChecker) {
		currentConnection
				.setLoadBalanceExceptionChecker(loadBalanceExceptionChecker);
		
	}

	public void setLoadBalanceSQLExceptionSubclassFailover(
			String loadBalanceSQLExceptionSubclassFailover) {
		currentConnection
				.setLoadBalanceSQLExceptionSubclassFailover(loadBalanceSQLExceptionSubclassFailover);
		
	}

	public void setLoadBalanceSQLStateFailover(
			String loadBalanceSQLStateFailover) {
		currentConnection
				.setLoadBalanceSQLStateFailover(loadBalanceSQLStateFailover);
		
	}

	public String getLoadBalanceAutoCommitStatementRegex() {
		return currentConnection.getLoadBalanceAutoCommitStatementRegex();
	}

	public int getLoadBalanceAutoCommitStatementThreshold() {
		return currentConnection.getLoadBalanceAutoCommitStatementThreshold();
	}

	public void setLoadBalanceAutoCommitStatementRegex(
			String loadBalanceAutoCommitStatementRegex) {
		currentConnection.setLoadBalanceAutoCommitStatementRegex(loadBalanceAutoCommitStatementRegex);
		
	}

	public void setLoadBalanceAutoCommitStatementThreshold(
			int loadBalanceAutoCommitStatementThreshold) {
		currentConnection.setLoadBalanceAutoCommitStatementThreshold(loadBalanceAutoCommitStatementThreshold);
		
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public boolean getIncludeThreadDumpInDeadlockExceptions() {
		return currentConnection.getIncludeThreadDumpInDeadlockExceptions();
	}

	public void setIncludeThreadDumpInDeadlockExceptions(boolean flag) {
		currentConnection.setIncludeThreadDumpInDeadlockExceptions(flag);
		
	}

	public boolean getIncludeThreadNamesAsStatementComment() {
		return currentConnection.getIncludeThreadNamesAsStatementComment();
	}

	public void setIncludeThreadNamesAsStatementComment(boolean flag) {
		currentConnection.setIncludeThreadNamesAsStatementComment(flag);
	}

	public synchronized boolean isServerLocal() throws SQLException {
		return this.currentConnection.isServerLocal();
	}

	public void setAuthenticationPlugins(String authenticationPlugins) {
		this.currentConnection.setAuthenticationPlugins(authenticationPlugins);
	}

	public String getAuthenticationPlugins() {
		return this.currentConnection.getAuthenticationPlugins();
	}

	public void setDisabledAuthenticationPlugins(
			String disabledAuthenticationPlugins) {
		this.currentConnection.setDisabledAuthenticationPlugins(disabledAuthenticationPlugins);
	}

	public String getDisabledAuthenticationPlugins() {
		return this.currentConnection.getDisabledAuthenticationPlugins();
	}

	public void setDefaultAuthenticationPlugin(
			String defaultAuthenticationPlugin) {
		this.currentConnection.setDefaultAuthenticationPlugin(defaultAuthenticationPlugin);
	}

	public String getDefaultAuthenticationPlugin() {
		return this.currentConnection.getDefaultAuthenticationPlugin();
	}

	public void setParseInfoCacheFactory(String factoryClassname) {
		this.currentConnection.setParseInfoCacheFactory(factoryClassname);
	}

	public String getParseInfoCacheFactory() {
		return this.currentConnection.getParseInfoCacheFactory();
	}

	public void setSchema(String schema) throws SQLException {
		this.currentConnection.setSchema(schema);
	}

	public String getSchema() throws SQLException {
		return this.currentConnection.getSchema();
	}

	public void abort(Executor executor) throws SQLException {
		this.currentConnection.abort(executor);
	}

	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		this.currentConnection.setNetworkTimeout(executor, milliseconds);
	}

	public int getNetworkTimeout() throws SQLException {
		return this.currentConnection.getNetworkTimeout();
	}

	public void setServerConfigCacheFactory(String factoryClassname) {
		this.currentConnection.setServerConfigCacheFactory(factoryClassname);
	}

	public String getServerConfigCacheFactory() {
		return this.currentConnection.getServerConfigCacheFactory();
	}

	public void setDisconnectOnExpiredPasswords(boolean disconnectOnExpiredPasswords) {
		this.currentConnection.setDisconnectOnExpiredPasswords(disconnectOnExpiredPasswords);
	}

	public boolean getDisconnectOnExpiredPasswords() {
		return this.currentConnection.getDisconnectOnExpiredPasswords();
	}
}
