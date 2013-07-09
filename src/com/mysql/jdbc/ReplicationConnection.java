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
	
	private Properties slaveProperties;
	
	private Properties masterProperties;
	
	private NonRegisteringDriver driver;

	protected ReplicationConnection() {}
	
	public ReplicationConnection(Properties masterProperties,
			Properties slaveProperties) throws SQLException {
		this.driver = new NonRegisteringDriver();
		this.slaveProperties = slaveProperties;
		this.masterProperties = masterProperties;

        this.initializeMasterConnection();
        this.initializeSlaveConnection();
        
		this.currentConnection = this.masterConnection;
	}
	
	private void initializeMasterConnection() throws SQLException {
		StringBuffer masterUrl = new StringBuffer("jdbc:mysql://");
		 
        String masterHost = masterProperties
        	.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
        
        if (masterHost != null) {
        	masterUrl.append(masterHost);
        }
 

        String masterDb = masterProperties
        	.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);

        masterUrl.append("/");
        
        if (masterDb != null) {
        	masterUrl.append(masterDb);
        }
        
        
        this.masterConnection = (com.mysql.jdbc.Connection) driver.connect(
                masterUrl.toString(), masterProperties);
	}
	
	
	private void initializeSlaveConnection() throws SQLException {
	    StringBuffer slaveUrl = new StringBuffer("jdbc:mysql:loadbalance://");
		
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

     
        String slaveDb = slaveProperties
        	.getProperty(NonRegisteringDriver.DBNAME_PROPERTY_KEY);
        
        slaveUrl.append("/");
        
        if (slaveDb != null) {
        	slaveUrl.append(slaveDb);
        }
        
        slaveProperties.setProperty("roundRobinLoadBalance", "true");

        this.slavesConnection = (com.mysql.jdbc.Connection) driver.connect(
                slaveUrl.toString(), slaveProperties);
        this.slavesConnection.setReadOnly(true);
		
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#clearWarnings()
	 */
	public void clearWarnings() throws SQLException {
		getCurrentConnection().clearWarnings();
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
	public void commit() throws SQLException {
		getCurrentConnection().commit();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#createStatement()
	 */
	public Statement createStatement() throws SQLException {
		Statement stmt = getCurrentConnection().createStatement();
		((com.mysql.jdbc.Statement) stmt).setPingTarget(this);
		
		return stmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#createStatement(int, int)
	 */
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency) throws SQLException {
		Statement stmt = getCurrentConnection().createStatement(resultSetType,
				resultSetConcurrency);
		
		((com.mysql.jdbc.Statement) stmt).setPingTarget(this);
		
		return stmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#createStatement(int, int, int)
	 */
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		Statement stmt = getCurrentConnection().createStatement(resultSetType,
				resultSetConcurrency, resultSetHoldability);
		
		((com.mysql.jdbc.Statement) stmt).setPingTarget(this);
		
		return stmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getAutoCommit()
	 */
	public boolean getAutoCommit() throws SQLException {
		return getCurrentConnection().getAutoCommit();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getCatalog()
	 */
	public String getCatalog() throws SQLException {
		return getCurrentConnection().getCatalog();
	}

	public synchronized Connection getCurrentConnection() {
		return this.currentConnection;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getHoldability()
	 */
	public int getHoldability() throws SQLException {
		return getCurrentConnection().getHoldability();
	}

	public synchronized Connection getMasterConnection() {
		return this.masterConnection;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getMetaData()
	 */
	public DatabaseMetaData getMetaData() throws SQLException {
		return getCurrentConnection().getMetaData();
	}

	public synchronized Connection getSlavesConnection() {
		return this.slavesConnection;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getTransactionIsolation()
	 */
	public int getTransactionIsolation() throws SQLException {
		return getCurrentConnection().getTransactionIsolation();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getTypeMap()
	 */
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		return getCurrentConnection().getTypeMap();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#getWarnings()
	 */
	public SQLWarning getWarnings() throws SQLException {
		return getCurrentConnection().getWarnings();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#isClosed()
	 */
	public boolean isClosed() throws SQLException {
		return getCurrentConnection().isClosed();
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
	public String nativeSQL(String sql) throws SQLException {
		return getCurrentConnection().nativeSQL(sql);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareCall(java.lang.String)
	 */
	public CallableStatement prepareCall(String sql) throws SQLException {
		return getCurrentConnection().prepareCall(sql);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int)
	 */
	public CallableStatement prepareCall(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		return getCurrentConnection().prepareCall(sql, resultSetType,
				resultSetConcurrency);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareCall(java.lang.String, int, int, int)
	 */
	public CallableStatement prepareCall(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		return getCurrentConnection().prepareCall(sql, resultSetType,
				resultSetConcurrency, resultSetHoldability);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String)
	 */
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().prepareStatement(sql);
		
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int)
	 */
	public PreparedStatement prepareStatement(String sql,
			int autoGeneratedKeys) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().prepareStatement(sql, autoGeneratedKeys);

		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
	 */
	public PreparedStatement prepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().prepareStatement(sql, resultSetType,
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
	public PreparedStatement prepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().prepareStatement(sql, resultSetType,
				resultSetConcurrency, resultSetHoldability);

		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
	 */
	public PreparedStatement prepareStatement(String sql,
			int[] columnIndexes) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().prepareStatement(sql, columnIndexes);
		
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String,
	 *      java.lang.String[])
	 */
	public PreparedStatement prepareStatement(String sql,
			String[] columnNames) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().prepareStatement(sql, columnNames);

		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#releaseSavepoint(java.sql.Savepoint)
	 */
	public void releaseSavepoint(Savepoint savepoint)
			throws SQLException {
		getCurrentConnection().releaseSavepoint(savepoint);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#rollback()
	 */
	public void rollback() throws SQLException {
		getCurrentConnection().rollback();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#rollback(java.sql.Savepoint)
	 */
	public void rollback(Savepoint savepoint) throws SQLException {
		getCurrentConnection().rollback(savepoint);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#setAutoCommit(boolean)
	 */
	public void setAutoCommit(boolean autoCommit)
			throws SQLException {
		getCurrentConnection().setAutoCommit(autoCommit);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#setCatalog(java.lang.String)
	 */
	public void setCatalog(String catalog) throws SQLException {
		getCurrentConnection().setCatalog(catalog);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#setHoldability(int)
	 */
	public void setHoldability(int holdability)
			throws SQLException {
		getCurrentConnection().setHoldability(holdability);
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
	public Savepoint setSavepoint() throws SQLException {
		return getCurrentConnection().setSavepoint();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#setSavepoint(java.lang.String)
	 */
	public Savepoint setSavepoint(String name) throws SQLException {
		return getCurrentConnection().setSavepoint(name);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#setTransactionIsolation(int)
	 */
	public void setTransactionIsolation(int level)
			throws SQLException {
		getCurrentConnection().setTransactionIsolation(level);
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
		boolean isMasterConn = this.isMasterConnection();
		if (this.masterConnection != null) {
			try {
				this.masterConnection.ping();
			} catch (SQLException e) {
				if (isMasterConn) {
					throw e;
				}
			}
		}
		
		if (this.slavesConnection != null) {
			try { 
				this.slavesConnection.ping();
			} catch (SQLException e) {
				try {
					this.initializeSlaveConnection();
				} catch (SQLException initE) {
					// do nothing here, we tried to reconnect.
				}
				if (!isMasterConn) {
					throw e;
				}
			}
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

	public PreparedStatement clientPrepareStatement(String sql)
			throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().clientPrepareStatement(sql);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public PreparedStatement clientPrepareStatement(String sql,
			int autoGenKeyIndex) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().clientPrepareStatement(sql, autoGenKeyIndex);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public PreparedStatement clientPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().clientPrepareStatement(sql, resultSetType, resultSetConcurrency);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public PreparedStatement clientPrepareStatement(String sql,
			int[] autoGenKeyIndexes) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().clientPrepareStatement(sql, autoGenKeyIndexes);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public PreparedStatement clientPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().clientPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public PreparedStatement clientPrepareStatement(String sql,
			String[] autoGenKeyColNames) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().clientPrepareStatement(sql, autoGenKeyColNames);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public int getActiveStatementCount() {
		return getCurrentConnection().getActiveStatementCount();
	}

	public long getIdleFor() {
		return getCurrentConnection().getIdleFor();
	}

	public Log getLog() throws SQLException {
		return getCurrentConnection().getLog();
	}

	public String getServerCharacterEncoding() {
		return getCurrentConnection().getServerCharacterEncoding();
	}

	public TimeZone getServerTimezoneTZ() {
		return getCurrentConnection().getServerTimezoneTZ();
	}

	public String getStatementComment() {
		return getCurrentConnection().getStatementComment();
	}

	public boolean hasTriedMaster() {
		return getCurrentConnection().hasTriedMaster();
	}

	public void initializeExtension(Extension ex) throws SQLException {
		getCurrentConnection().initializeExtension(ex);
	}

	public boolean isAbonormallyLongQuery(long millisOrNanos) {
		return getCurrentConnection().isAbonormallyLongQuery(millisOrNanos);
	}

	public boolean isInGlobalTx() {
		return getCurrentConnection().isInGlobalTx();
	}

	public boolean isMasterConnection() {
		return this.currentConnection == this.masterConnection;
	}

	public boolean isNoBackslashEscapesSet() {
		return getCurrentConnection().isNoBackslashEscapesSet();
	}

	public boolean lowerCaseTableNames() {
		return getCurrentConnection().lowerCaseTableNames();
	}

	public boolean parserKnowsUnicode() {
		return getCurrentConnection().parserKnowsUnicode();
	}

	public synchronized void ping() throws SQLException {
		try {
			this.masterConnection.ping();
		} catch (SQLException e) {
			if (this.isMasterConnection()) {
				throw e;
			}
		}
		try {
			this.slavesConnection.ping();
		} catch (SQLException e) {
			if (!this.isMasterConnection()) {
				throw e;
			}
		}
	}

	public void reportQueryTime(long millisOrNanos) {
		getCurrentConnection().reportQueryTime(millisOrNanos);
	}

	public void resetServerState() throws SQLException {
		getCurrentConnection().resetServerState();
	}

	public PreparedStatement serverPrepareStatement(String sql)
			throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().serverPrepareStatement(sql);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public PreparedStatement serverPrepareStatement(String sql,
			int autoGenKeyIndex) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().serverPrepareStatement(sql, autoGenKeyIndex);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public PreparedStatement serverPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().serverPrepareStatement(sql, resultSetType, resultSetConcurrency);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public PreparedStatement serverPrepareStatement(String sql,
			int resultSetType, int resultSetConcurrency,
			int resultSetHoldability) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().serverPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public PreparedStatement serverPrepareStatement(String sql,
			int[] autoGenKeyIndexes) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().serverPrepareStatement(sql, autoGenKeyIndexes);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public PreparedStatement serverPrepareStatement(String sql,
			String[] autoGenKeyColNames) throws SQLException {
		PreparedStatement pstmt = getCurrentConnection().serverPrepareStatement(sql, autoGenKeyColNames);
		((com.mysql.jdbc.Statement) pstmt).setPingTarget(this);
		
		return pstmt;
	}

	public void setFailedOver(boolean flag) {
		getCurrentConnection().setFailedOver(flag);
	}

	public void setPreferSlaveDuringFailover(boolean flag) {
		getCurrentConnection().setPreferSlaveDuringFailover(flag);
	}

	public synchronized void setStatementComment(String comment) {
		this.masterConnection.setStatementComment(comment);
		this.slavesConnection.setStatementComment(comment);
	}

	public void shutdownServer() throws SQLException {
		getCurrentConnection().shutdownServer();
	}

	public boolean supportsIsolationLevel() {
		return getCurrentConnection().supportsIsolationLevel();
	}

	public boolean supportsQuotedIdentifiers() {
		return getCurrentConnection().supportsQuotedIdentifiers();
	}

	public boolean supportsTransactions() {
		return getCurrentConnection().supportsTransactions();
	}

	public boolean versionMeetsMinimum(int major, int minor, int subminor)
			throws SQLException {
		return getCurrentConnection().versionMeetsMinimum(major, minor, subminor);
	}

	public String exposeAsXml() throws SQLException {
		return getCurrentConnection().exposeAsXml();
	}

	public boolean getAllowLoadLocalInfile() {
		return getCurrentConnection().getAllowLoadLocalInfile();
	}

	public boolean getAllowMultiQueries() {
		return getCurrentConnection().getAllowMultiQueries();
	}

	public boolean getAllowNanAndInf() {
		return getCurrentConnection().getAllowNanAndInf();
	}

	public boolean getAllowUrlInLocalInfile() {
		return getCurrentConnection().getAllowUrlInLocalInfile();
	}

	public boolean getAlwaysSendSetIsolation() {
		return getCurrentConnection().getAlwaysSendSetIsolation();
	}

	public boolean getAutoClosePStmtStreams() {
		return getCurrentConnection().getAutoClosePStmtStreams();
	}

	public boolean getAutoDeserialize() {
		return getCurrentConnection().getAutoDeserialize();
	}

	public boolean getAutoGenerateTestcaseScript() {
		return getCurrentConnection().getAutoGenerateTestcaseScript();
	}

	public boolean getAutoReconnectForPools() {
		return getCurrentConnection().getAutoReconnectForPools();
	}

	public boolean getAutoSlowLog() {
		return getCurrentConnection().getAutoSlowLog();
	}

	public int getBlobSendChunkSize() {
		return getCurrentConnection().getBlobSendChunkSize();
	}

	public boolean getBlobsAreStrings() {
		return getCurrentConnection().getBlobsAreStrings();
	}

	public boolean getCacheCallableStatements() {
		return getCurrentConnection().getCacheCallableStatements();
	}

	public boolean getCacheCallableStmts() {
		return getCurrentConnection().getCacheCallableStmts();
	}

	public boolean getCachePrepStmts() {
		return getCurrentConnection().getCachePrepStmts();
	}

	public boolean getCachePreparedStatements() {
		return getCurrentConnection().getCachePreparedStatements();
	}

	public boolean getCacheResultSetMetadata() {
		return getCurrentConnection().getCacheResultSetMetadata();
	}

	public boolean getCacheServerConfiguration() {
		return getCurrentConnection().getCacheServerConfiguration();
	}

	public int getCallableStatementCacheSize() {
		return getCurrentConnection().getCallableStatementCacheSize();
	}

	public int getCallableStmtCacheSize() {
		return getCurrentConnection().getCallableStmtCacheSize();
	}

	public boolean getCapitalizeTypeNames() {
		return getCurrentConnection().getCapitalizeTypeNames();
	}

	public String getCharacterSetResults() {
		return getCurrentConnection().getCharacterSetResults();
	}

	public String getClientCertificateKeyStorePassword() {
		return getCurrentConnection().getClientCertificateKeyStorePassword();
	}

	public String getClientCertificateKeyStoreType() {
		return getCurrentConnection().getClientCertificateKeyStoreType();
	}

	public String getClientCertificateKeyStoreUrl() {
		return getCurrentConnection().getClientCertificateKeyStoreUrl();
	}

	public String getClientInfoProvider() {
		return getCurrentConnection().getClientInfoProvider();
	}

	public String getClobCharacterEncoding() {
		return getCurrentConnection().getClobCharacterEncoding();
	}

	public boolean getClobberStreamingResults() {
		return getCurrentConnection().getClobberStreamingResults();
	}

	public int getConnectTimeout() {
		return getCurrentConnection().getConnectTimeout();
	}

	public String getConnectionCollation() {
		return getCurrentConnection().getConnectionCollation();
	}

	public String getConnectionLifecycleInterceptors() {
		return getCurrentConnection().getConnectionLifecycleInterceptors();
	}

	public boolean getContinueBatchOnError() {
		return getCurrentConnection().getContinueBatchOnError();
	}

	public boolean getCreateDatabaseIfNotExist() {
		return getCurrentConnection().getCreateDatabaseIfNotExist();
	}

	public int getDefaultFetchSize() {
		return getCurrentConnection().getDefaultFetchSize();
	}

	public boolean getDontTrackOpenResources() {
		return getCurrentConnection().getDontTrackOpenResources();
	}

	public boolean getDumpMetadataOnColumnNotFound() {
		return getCurrentConnection().getDumpMetadataOnColumnNotFound();
	}

	public boolean getDumpQueriesOnException() {
		return getCurrentConnection().getDumpQueriesOnException();
	}

	public boolean getDynamicCalendars() {
		return getCurrentConnection().getDynamicCalendars();
	}

	public boolean getElideSetAutoCommits() {
		return getCurrentConnection().getElideSetAutoCommits();
	}

	public boolean getEmptyStringsConvertToZero() {
		return getCurrentConnection().getEmptyStringsConvertToZero();
	}

	public boolean getEmulateLocators() {
		return getCurrentConnection().getEmulateLocators();
	}

	public boolean getEmulateUnsupportedPstmts() {
		return getCurrentConnection().getEmulateUnsupportedPstmts();
	}

	public boolean getEnablePacketDebug() {
		return getCurrentConnection().getEnablePacketDebug();
	}

	public boolean getEnableQueryTimeouts() {
		return getCurrentConnection().getEnableQueryTimeouts();
	}

	public String getEncoding() {
		return getCurrentConnection().getEncoding();
	}

	public boolean getExplainSlowQueries() {
		return getCurrentConnection().getExplainSlowQueries();
	}

	public boolean getFailOverReadOnly() {
		return getCurrentConnection().getFailOverReadOnly();
	}

	public boolean getFunctionsNeverReturnBlobs() {
		return getCurrentConnection().getFunctionsNeverReturnBlobs();
	}

	public boolean getGatherPerfMetrics() {
		return getCurrentConnection().getGatherPerfMetrics();
	}

	public boolean getGatherPerformanceMetrics() {
		return getCurrentConnection().getGatherPerformanceMetrics();
	}

	public boolean getGenerateSimpleParameterMetadata() {
		return getCurrentConnection().getGenerateSimpleParameterMetadata();
	}

	public boolean getHoldResultsOpenOverStatementClose() {
		return getCurrentConnection().getHoldResultsOpenOverStatementClose();
	}

	public boolean getIgnoreNonTxTables() {
		return getCurrentConnection().getIgnoreNonTxTables();
	}

	public boolean getIncludeInnodbStatusInDeadlockExceptions() {
		return getCurrentConnection().getIncludeInnodbStatusInDeadlockExceptions();
	}

	public int getInitialTimeout() {
		return getCurrentConnection().getInitialTimeout();
	}

	public boolean getInteractiveClient() {
		return getCurrentConnection().getInteractiveClient();
	}

	public boolean getIsInteractiveClient() {
		return getCurrentConnection().getIsInteractiveClient();
	}

	public boolean getJdbcCompliantTruncation() {
		return getCurrentConnection().getJdbcCompliantTruncation();
	}

	public boolean getJdbcCompliantTruncationForReads() {
		return getCurrentConnection().getJdbcCompliantTruncationForReads();
	}

	public String getLargeRowSizeThreshold() {
		return getCurrentConnection().getLargeRowSizeThreshold();
	}

	public String getLoadBalanceStrategy() {
		return getCurrentConnection().getLoadBalanceStrategy();
	}

	public String getLocalSocketAddress() {
		return getCurrentConnection().getLocalSocketAddress();
	}

	public int getLocatorFetchBufferSize() {
		return getCurrentConnection().getLocatorFetchBufferSize();
	}

	public boolean getLogSlowQueries() {
		return getCurrentConnection().getLogSlowQueries();
	}

	public boolean getLogXaCommands() {
		return getCurrentConnection().getLogXaCommands();
	}

	public String getLogger() {
		return getCurrentConnection().getLogger();
	}

	public String getLoggerClassName() {
		return getCurrentConnection().getLoggerClassName();
	}

	public boolean getMaintainTimeStats() {
		return getCurrentConnection().getMaintainTimeStats();
	}

	public int getMaxQuerySizeToLog() {
		return getCurrentConnection().getMaxQuerySizeToLog();
	}

	public int getMaxReconnects() {
		return getCurrentConnection().getMaxReconnects();
	}

	public int getMaxRows() {
		return getCurrentConnection().getMaxRows();
	}

	public int getMetadataCacheSize() {
		return getCurrentConnection().getMetadataCacheSize();
	}

	public int getNetTimeoutForStreamingResults() {
		return getCurrentConnection().getNetTimeoutForStreamingResults();
	}

	public boolean getNoAccessToProcedureBodies() {
		return getCurrentConnection().getNoAccessToProcedureBodies();
	}

	public boolean getNoDatetimeStringSync() {
		return getCurrentConnection().getNoDatetimeStringSync();
	}

	public boolean getNoTimezoneConversionForTimeType() {
		return getCurrentConnection().getNoTimezoneConversionForTimeType();
	}

	public boolean getNullCatalogMeansCurrent() {
		return getCurrentConnection().getNullCatalogMeansCurrent();
	}

	public boolean getNullNamePatternMatchesAll() {
		return getCurrentConnection().getNullNamePatternMatchesAll();
	}

	public boolean getOverrideSupportsIntegrityEnhancementFacility() {
		return getCurrentConnection().getOverrideSupportsIntegrityEnhancementFacility();
	}

	public int getPacketDebugBufferSize() {
		return getCurrentConnection().getPacketDebugBufferSize();
	}

	public boolean getPadCharsWithSpace() {
		return getCurrentConnection().getPadCharsWithSpace();
	}

	public boolean getParanoid() {
		return getCurrentConnection().getParanoid();
	}

	public boolean getPedantic() {
		return getCurrentConnection().getPedantic();
	}

	public boolean getPinGlobalTxToPhysicalConnection() {
		return getCurrentConnection().getPinGlobalTxToPhysicalConnection();
	}

	public boolean getPopulateInsertRowWithDefaultValues() {
		return getCurrentConnection().getPopulateInsertRowWithDefaultValues();
	}

	public int getPrepStmtCacheSize() {
		return getCurrentConnection().getPrepStmtCacheSize();
	}

	public int getPrepStmtCacheSqlLimit() {
		return getCurrentConnection().getPrepStmtCacheSqlLimit();
	}

	public int getPreparedStatementCacheSize() {
		return getCurrentConnection().getPreparedStatementCacheSize();
	}

	public int getPreparedStatementCacheSqlLimit() {
		return getCurrentConnection().getPreparedStatementCacheSqlLimit();
	}

	public boolean getProcessEscapeCodesForPrepStmts() {
		return getCurrentConnection().getProcessEscapeCodesForPrepStmts();
	}

	public boolean getProfileSQL() {
		return getCurrentConnection().getProfileSQL();
	}

	public boolean getProfileSql() {
		return getCurrentConnection().getProfileSql();
	}

	public String getProfilerEventHandler() {
		return getCurrentConnection().getProfilerEventHandler();
	}

	public String getPropertiesTransform() {
		return getCurrentConnection().getPropertiesTransform();
	}

	public int getQueriesBeforeRetryMaster() {
		return getCurrentConnection().getQueriesBeforeRetryMaster();
	}

	public boolean getReconnectAtTxEnd() {
		return getCurrentConnection().getReconnectAtTxEnd();
	}

	public boolean getRelaxAutoCommit() {
		return getCurrentConnection().getRelaxAutoCommit();
	}

	public int getReportMetricsIntervalMillis() {
		return getCurrentConnection().getReportMetricsIntervalMillis();
	}

	public boolean getRequireSSL() {
		return getCurrentConnection().getRequireSSL();
	}

	public String getResourceId() {
		return getCurrentConnection().getResourceId();
	}

	public int getResultSetSizeThreshold() {
		return getCurrentConnection().getResultSetSizeThreshold();
	}

	public boolean getRewriteBatchedStatements() {
		return getCurrentConnection().getRewriteBatchedStatements();
	}

	public boolean getRollbackOnPooledClose() {
		return getCurrentConnection().getRollbackOnPooledClose();
	}

	public boolean getRoundRobinLoadBalance() {
		return getCurrentConnection().getRoundRobinLoadBalance();
	}

	public boolean getRunningCTS13() {
		return getCurrentConnection().getRunningCTS13();
	}

	public int getSecondsBeforeRetryMaster() {
		return getCurrentConnection().getSecondsBeforeRetryMaster();
	}

	public int getSelfDestructOnPingMaxOperations() {
		return getCurrentConnection().getSelfDestructOnPingMaxOperations();
	}

	public int getSelfDestructOnPingSecondsLifetime() {
		return getCurrentConnection().getSelfDestructOnPingSecondsLifetime();
	}

	public String getServerTimezone() {
		return getCurrentConnection().getServerTimezone();
	}

	public String getSessionVariables() {
		return getCurrentConnection().getSessionVariables();
	}

	public int getSlowQueryThresholdMillis() {
		return getCurrentConnection().getSlowQueryThresholdMillis();
	}

	public long getSlowQueryThresholdNanos() {
		return getCurrentConnection().getSlowQueryThresholdNanos();
	}

	public String getSocketFactory() {
		return getCurrentConnection().getSocketFactory();
	}

	public String getSocketFactoryClassName() {
		return getCurrentConnection().getSocketFactoryClassName();
	}

	public int getSocketTimeout() {
		return getCurrentConnection().getSocketTimeout();
	}

	public String getStatementInterceptors() {
		return getCurrentConnection().getStatementInterceptors();
	}

	public boolean getStrictFloatingPoint() {
		return getCurrentConnection().getStrictFloatingPoint();
	}

	public boolean getStrictUpdates() {
		return getCurrentConnection().getStrictUpdates();
	}

	public boolean getTcpKeepAlive() {
		return getCurrentConnection().getTcpKeepAlive();
	}

	public boolean getTcpNoDelay() {
		return getCurrentConnection().getTcpNoDelay();
	}

	public int getTcpRcvBuf() {
		return getCurrentConnection().getTcpRcvBuf();
	}

	public int getTcpSndBuf() {
		return getCurrentConnection().getTcpSndBuf();
	}

	public int getTcpTrafficClass() {
		return getCurrentConnection().getTcpTrafficClass();
	}

	public boolean getTinyInt1isBit() {
		return getCurrentConnection().getTinyInt1isBit();
	}

	public boolean getTraceProtocol() {
		return getCurrentConnection().getTraceProtocol();
	}

	public boolean getTransformedBitIsBoolean() {
		return getCurrentConnection().getTransformedBitIsBoolean();
	}

	public boolean getTreatUtilDateAsTimestamp() {
		return getCurrentConnection().getTreatUtilDateAsTimestamp();
	}

	public String getTrustCertificateKeyStorePassword() {
		return getCurrentConnection().getTrustCertificateKeyStorePassword();
	}

	public String getTrustCertificateKeyStoreType() {
		return getCurrentConnection().getTrustCertificateKeyStoreType();
	}

	public String getTrustCertificateKeyStoreUrl() {
		return getCurrentConnection().getTrustCertificateKeyStoreUrl();
	}

	public boolean getUltraDevHack() {
		return getCurrentConnection().getUltraDevHack();
	}

	public boolean getUseBlobToStoreUTF8OutsideBMP() {
		return getCurrentConnection().getUseBlobToStoreUTF8OutsideBMP();
	}

	public boolean getUseCompression() {
		return getCurrentConnection().getUseCompression();
	}

	public String getUseConfigs() {
		return getCurrentConnection().getUseConfigs();
	}

	public boolean getUseCursorFetch() {
		return getCurrentConnection().getUseCursorFetch();
	}

	public boolean getUseDirectRowUnpack() {
		return getCurrentConnection().getUseDirectRowUnpack();
	}

	public boolean getUseDynamicCharsetInfo() {
		return getCurrentConnection().getUseDynamicCharsetInfo();
	}

	public boolean getUseFastDateParsing() {
		return getCurrentConnection().getUseFastDateParsing();
	}

	public boolean getUseFastIntParsing() {
		return getCurrentConnection().getUseFastIntParsing();
	}

	public boolean getUseGmtMillisForDatetimes() {
		return getCurrentConnection().getUseGmtMillisForDatetimes();
	}

	public boolean getUseHostsInPrivileges() {
		return getCurrentConnection().getUseHostsInPrivileges();
	}

	public boolean getUseInformationSchema() {
		return getCurrentConnection().getUseInformationSchema();
	}

	public boolean getUseJDBCCompliantTimezoneShift() {
		return getCurrentConnection().getUseJDBCCompliantTimezoneShift();
	}

	public boolean getUseJvmCharsetConverters() {
		return getCurrentConnection().getUseJvmCharsetConverters();
	}

	public boolean getUseLegacyDatetimeCode() {
		return getCurrentConnection().getUseLegacyDatetimeCode();
	}

	public boolean getUseLocalSessionState() {
		return getCurrentConnection().getUseLocalSessionState();
	}

	public boolean getUseNanosForElapsedTime() {
		return getCurrentConnection().getUseNanosForElapsedTime();
	}

	public boolean getUseOldAliasMetadataBehavior() {
		return getCurrentConnection().getUseOldAliasMetadataBehavior();
	}

	public boolean getUseOldUTF8Behavior() {
		return getCurrentConnection().getUseOldUTF8Behavior();
	}

	public boolean getUseOnlyServerErrorMessages() {
		return getCurrentConnection().getUseOnlyServerErrorMessages();
	}

	public boolean getUseReadAheadInput() {
		return getCurrentConnection().getUseReadAheadInput();
	}

	public boolean getUseSSL() {
		return getCurrentConnection().getUseSSL();
	}

	public boolean getUseSSPSCompatibleTimezoneShift() {
		return getCurrentConnection().getUseSSPSCompatibleTimezoneShift();
	}

	public boolean getUseServerPrepStmts() {
		return getCurrentConnection().getUseServerPrepStmts();
	}

	public boolean getUseServerPreparedStmts() {
		return getCurrentConnection().getUseServerPreparedStmts();
	}

	public boolean getUseSqlStateCodes() {
		return getCurrentConnection().getUseSqlStateCodes();
	}

	public boolean getUseStreamLengthsInPrepStmts() {
		return getCurrentConnection().getUseStreamLengthsInPrepStmts();
	}

	public boolean getUseTimezone() {
		return getCurrentConnection().getUseTimezone();
	}

	public boolean getUseUltraDevWorkAround() {
		return getCurrentConnection().getUseUltraDevWorkAround();
	}

	public boolean getUseUnbufferedInput() {
		return getCurrentConnection().getUseUnbufferedInput();
	}

	public boolean getUseUnicode() {
		return getCurrentConnection().getUseUnicode();
	}

	public boolean getUseUsageAdvisor() {
		return getCurrentConnection().getUseUsageAdvisor();
	}

	public String getUtf8OutsideBmpExcludedColumnNamePattern() {
		return getCurrentConnection().getUtf8OutsideBmpExcludedColumnNamePattern();
	}

	public String getUtf8OutsideBmpIncludedColumnNamePattern() {
		return getCurrentConnection().getUtf8OutsideBmpIncludedColumnNamePattern();
	}

	public boolean getVerifyServerCertificate() {
		return getCurrentConnection().getVerifyServerCertificate();
	}

	public boolean getYearIsDateType() {
		return getCurrentConnection().getYearIsDateType();
	}

	public String getZeroDateTimeBehavior() {
		return getCurrentConnection().getZeroDateTimeBehavior();
	}

	public void setAllowLoadLocalInfile(boolean property) {
		// not runtime configurable
		
	}

	public void setAllowMultiQueries(boolean property) {
		// not runtime configurable
		
	}

	public void setAllowNanAndInf(boolean flag) {
		// not runtime configurable
		
	}

	public void setAllowUrlInLocalInfile(boolean flag) {
		// not runtime configurable
		
	}

	public void setAlwaysSendSetIsolation(boolean flag) {
		// not runtime configurable
		
	}

	public void setAutoClosePStmtStreams(boolean flag) {
		// not runtime configurable
		
	}

	public void setAutoDeserialize(boolean flag) {
		// not runtime configurable
		
	}

	public void setAutoGenerateTestcaseScript(boolean flag) {
		// not runtime configurable
		
	}

	public void setAutoReconnect(boolean flag) {
		// not runtime configurable
		
	}

	public void setAutoReconnectForConnectionPools(boolean property) {
		// not runtime configurable
		
	}

	public void setAutoReconnectForPools(boolean flag) {
		// not runtime configurable
		
	}

	public void setAutoSlowLog(boolean flag) {
		// not runtime configurable
		
	}

	public void setBlobSendChunkSize(String value) throws SQLException {
		// not runtime configurable
		
	}

	public void setBlobsAreStrings(boolean flag) {
		// not runtime configurable
		
	}

	public void setCacheCallableStatements(boolean flag) {
		// not runtime configurable
		
	}

	public void setCacheCallableStmts(boolean flag) {
		// not runtime configurable
		
	}

	public void setCachePrepStmts(boolean flag) {
		// not runtime configurable
		
	}

	public void setCachePreparedStatements(boolean flag) {
		// not runtime configurable
		
	}

	public void setCacheResultSetMetadata(boolean property) {
		// not runtime configurable
		
	}

	public void setCacheServerConfiguration(boolean flag) {
		// not runtime configurable
		
	}

	public void setCallableStatementCacheSize(int size) {
		// not runtime configurable
		
	}

	public void setCallableStmtCacheSize(int cacheSize) {
		// not runtime configurable
		
	}

	public void setCapitalizeDBMDTypes(boolean property) {
		// not runtime configurable
		
	}

	public void setCapitalizeTypeNames(boolean flag) {
		// not runtime configurable
		
	}

	public void setCharacterEncoding(String encoding) {
		// not runtime configurable
		
	}

	public void setCharacterSetResults(String characterSet) {
		// not runtime configurable
		
	}

	public void setClientCertificateKeyStorePassword(String value) {
		// not runtime configurable
		
	}

	public void setClientCertificateKeyStoreType(String value) {
		// not runtime configurable
		
	}

	public void setClientCertificateKeyStoreUrl(String value) {
		// not runtime configurable
		
	}

	public void setClientInfoProvider(String classname) {
		// not runtime configurable
		
	}

	public void setClobCharacterEncoding(String encoding) {
		// not runtime configurable
		
	}

	public void setClobberStreamingResults(boolean flag) {
		// not runtime configurable
		
	}

	public void setConnectTimeout(int timeoutMs) {
		// not runtime configurable
		
	}

	public void setConnectionCollation(String collation) {
		// not runtime configurable
		
	}

	public void setConnectionLifecycleInterceptors(String interceptors) {
		// not runtime configurable
		
	}

	public void setContinueBatchOnError(boolean property) {
		// not runtime configurable
		
	}

	public void setCreateDatabaseIfNotExist(boolean flag) {
		// not runtime configurable
		
	}

	public void setDefaultFetchSize(int n) {
		// not runtime configurable
		
	}

	public void setDetectServerPreparedStmts(boolean property) {
		// not runtime configurable
		
	}

	public void setDontTrackOpenResources(boolean flag) {
		// not runtime configurable
		
	}

	public void setDumpMetadataOnColumnNotFound(boolean flag) {
		// not runtime configurable
		
	}

	public void setDumpQueriesOnException(boolean flag) {
		// not runtime configurable
		
	}

	public void setDynamicCalendars(boolean flag) {
		// not runtime configurable
		
	}

	public void setElideSetAutoCommits(boolean flag) {
		// not runtime configurable
		
	}

	public void setEmptyStringsConvertToZero(boolean flag) {
		// not runtime configurable
		
	}

	public void setEmulateLocators(boolean property) {
		// not runtime configurable
		
	}

	public void setEmulateUnsupportedPstmts(boolean flag) {
		// not runtime configurable
		
	}

	public void setEnablePacketDebug(boolean flag) {
		// not runtime configurable
		
	}

	public void setEnableQueryTimeouts(boolean flag) {
		// not runtime configurable
		
	}

	public void setEncoding(String property) {
		// not runtime configurable
		
	}

	public void setExplainSlowQueries(boolean flag) {
		// not runtime configurable
		
	}

	public void setFailOverReadOnly(boolean flag) {
		// not runtime configurable
		
	}

	public void setFunctionsNeverReturnBlobs(boolean flag) {
		// not runtime configurable
		
	}

	public void setGatherPerfMetrics(boolean flag) {
		// not runtime configurable
		
	}

	public void setGatherPerformanceMetrics(boolean flag) {
		// not runtime configurable
		
	}

	public void setGenerateSimpleParameterMetadata(boolean flag) {
		// not runtime configurable
		
	}

	public void setHoldResultsOpenOverStatementClose(boolean flag) {
		// not runtime configurable
		
	}

	public void setIgnoreNonTxTables(boolean property) {
		// not runtime configurable
		
	}

	public void setIncludeInnodbStatusInDeadlockExceptions(boolean flag) {
		// not runtime configurable
		
	}

	public void setInitialTimeout(int property) {
		// not runtime configurable
		
	}

	public void setInteractiveClient(boolean property) {
		// not runtime configurable
		
	}

	public void setIsInteractiveClient(boolean property) {
		// not runtime configurable
		
	}

	public void setJdbcCompliantTruncation(boolean flag) {
		// not runtime configurable
		
	}

	public void setJdbcCompliantTruncationForReads(
			boolean jdbcCompliantTruncationForReads) {
		// not runtime configurable
		
	}

	public void setLargeRowSizeThreshold(String value) {
		// not runtime configurable
		
	}

	public void setLoadBalanceStrategy(String strategy) {
		// not runtime configurable
		
	}

	public void setLocalSocketAddress(String address) {
		// not runtime configurable
		
	}

	public void setLocatorFetchBufferSize(String value) throws SQLException {
		// not runtime configurable
		
	}

	public void setLogSlowQueries(boolean flag) {
		// not runtime configurable
		
	}

	public void setLogXaCommands(boolean flag) {
		// not runtime configurable
		
	}

	public void setLogger(String property) {
		// not runtime configurable
		
	}

	public void setLoggerClassName(String className) {
		// not runtime configurable
		
	}

	public void setMaintainTimeStats(boolean flag) {
		// not runtime configurable
		
	}

	public void setMaxQuerySizeToLog(int sizeInBytes) {
		// not runtime configurable
		
	}

	public void setMaxReconnects(int property) {
		// not runtime configurable
		
	}

	public void setMaxRows(int property) {
		// not runtime configurable
		
	}

	public void setMetadataCacheSize(int value) {
		// not runtime configurable
		
	}

	public void setNetTimeoutForStreamingResults(int value) {
		// not runtime configurable
		
	}

	public void setNoAccessToProcedureBodies(boolean flag) {
		// not runtime configurable
		
	}

	public void setNoDatetimeStringSync(boolean flag) {
		// not runtime configurable
		
	}

	public void setNoTimezoneConversionForTimeType(boolean flag) {
		// not runtime configurable
		
	}

	public void setNullCatalogMeansCurrent(boolean value) {
		// not runtime configurable
		
	}

	public void setNullNamePatternMatchesAll(boolean value) {
		// not runtime configurable
		
	}

	public void setOverrideSupportsIntegrityEnhancementFacility(boolean flag) {
		// not runtime configurable
		
	}

	public void setPacketDebugBufferSize(int size) {
		// not runtime configurable
		
	}

	public void setPadCharsWithSpace(boolean flag) {
		// not runtime configurable
		
	}

	public void setParanoid(boolean property) {
		// not runtime configurable
		
	}

	public void setPedantic(boolean property) {
		// not runtime configurable
		
	}

	public void setPinGlobalTxToPhysicalConnection(boolean flag) {
		// not runtime configurable
		
	}

	public void setPopulateInsertRowWithDefaultValues(boolean flag) {
		// not runtime configurable
		
	}

	public void setPrepStmtCacheSize(int cacheSize) {
		// not runtime configurable
		
	}

	public void setPrepStmtCacheSqlLimit(int sqlLimit) {
		// not runtime configurable
		
	}

	public void setPreparedStatementCacheSize(int cacheSize) {
		// not runtime configurable
		
	}

	public void setPreparedStatementCacheSqlLimit(int cacheSqlLimit) {
		// not runtime configurable
		
	}

	public void setProcessEscapeCodesForPrepStmts(boolean flag) {
		// not runtime configurable
		
	}

	public void setProfileSQL(boolean flag) {
		// not runtime configurable
		
	}

	public void setProfileSql(boolean property) {
		// not runtime configurable
		
	}

	public void setProfilerEventHandler(String handler) {
		// not runtime configurable
		
	}

	public void setPropertiesTransform(String value) {
		// not runtime configurable
		
	}

	public void setQueriesBeforeRetryMaster(int property) {
		// not runtime configurable
		
	}

	public void setReconnectAtTxEnd(boolean property) {
		// not runtime configurable
		
	}

	public void setRelaxAutoCommit(boolean property) {
		// not runtime configurable
		
	}

	public void setReportMetricsIntervalMillis(int millis) {
		// not runtime configurable
		
	}

	public void setRequireSSL(boolean property) {
		// not runtime configurable
		
	}

	public void setResourceId(String resourceId) {
		// not runtime configurable
		
	}

	public void setResultSetSizeThreshold(int threshold) {
		// not runtime configurable
		
	}

	public void setRetainStatementAfterResultSetClose(boolean flag) {
		// not runtime configurable
		
	}

	public void setRewriteBatchedStatements(boolean flag) {
		// not runtime configurable
		
	}

	public void setRollbackOnPooledClose(boolean flag) {
		// not runtime configurable
		
	}

	public void setRoundRobinLoadBalance(boolean flag) {
		// not runtime configurable
		
	}

	public void setRunningCTS13(boolean flag) {
		// not runtime configurable
		
	}

	public void setSecondsBeforeRetryMaster(int property) {
		// not runtime configurable
		
	}

	public void setSelfDestructOnPingMaxOperations(int maxOperations) {
		// not runtime configurable
		
	}

	public void setSelfDestructOnPingSecondsLifetime(int seconds) {
		// not runtime configurable
		
	}

	public void setServerTimezone(String property) {
		// not runtime configurable
		
	}

	public void setSessionVariables(String variables) {
		// not runtime configurable
		
	}

	public void setSlowQueryThresholdMillis(int millis) {
		// not runtime configurable
		
	}

	public void setSlowQueryThresholdNanos(long nanos) {
		// not runtime configurable
		
	}

	public void setSocketFactory(String name) {
		// not runtime configurable
		
	}

	public void setSocketFactoryClassName(String property) {
		// not runtime configurable
		
	}

	public void setSocketTimeout(int property) {
		// not runtime configurable
		
	}

	public void setStatementInterceptors(String value) {
		// not runtime configurable
		
	}

	public void setStrictFloatingPoint(boolean property) {
		// not runtime configurable
		
	}

	public void setStrictUpdates(boolean property) {
		// not runtime configurable
		
	}

	public void setTcpKeepAlive(boolean flag) {
		// not runtime configurable
		
	}

	public void setTcpNoDelay(boolean flag) {
		// not runtime configurable
		
	}

	public void setTcpRcvBuf(int bufSize) {
		// not runtime configurable
		
	}

	public void setTcpSndBuf(int bufSize) {
		// not runtime configurable
		
	}

	public void setTcpTrafficClass(int classFlags) {
		// not runtime configurable
		
	}

	public void setTinyInt1isBit(boolean flag) {
		// not runtime configurable
		
	}

	public void setTraceProtocol(boolean flag) {
		// not runtime configurable
		
	}

	public void setTransformedBitIsBoolean(boolean flag) {
		// not runtime configurable
		
	}

	public void setTreatUtilDateAsTimestamp(boolean flag) {
		// not runtime configurable
		
	}

	public void setTrustCertificateKeyStorePassword(String value) {
		// not runtime configurable
		
	}

	public void setTrustCertificateKeyStoreType(String value) {
		// not runtime configurable
		
	}

	public void setTrustCertificateKeyStoreUrl(String value) {
		// not runtime configurable
		
	}

	public void setUltraDevHack(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseBlobToStoreUTF8OutsideBMP(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseCompression(boolean property) {
		// not runtime configurable
		
	}

	public void setUseConfigs(String configs) {
		// not runtime configurable
		
	}

	public void setUseCursorFetch(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseDirectRowUnpack(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseDynamicCharsetInfo(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseFastDateParsing(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseFastIntParsing(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseGmtMillisForDatetimes(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseHostsInPrivileges(boolean property) {
		// not runtime configurable
		
	}

	public void setUseInformationSchema(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseJDBCCompliantTimezoneShift(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseJvmCharsetConverters(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseLegacyDatetimeCode(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseLocalSessionState(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseNanosForElapsedTime(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseOldAliasMetadataBehavior(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseOldUTF8Behavior(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseOnlyServerErrorMessages(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseReadAheadInput(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseSSL(boolean property) {
		// not runtime configurable
		
	}

	public void setUseSSPSCompatibleTimezoneShift(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseServerPrepStmts(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseServerPreparedStmts(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseSqlStateCodes(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseStreamLengthsInPrepStmts(boolean property) {
		// not runtime configurable
		
	}

	public void setUseTimezone(boolean property) {
		// not runtime configurable
		
	}

	public void setUseUltraDevWorkAround(boolean property) {
		// not runtime configurable
		
	}

	public void setUseUnbufferedInput(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseUnicode(boolean flag) {
		// not runtime configurable
		
	}

	public void setUseUsageAdvisor(boolean useUsageAdvisorFlag) {
		// not runtime configurable
		
	}

	public void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern) {
		// not runtime configurable
		
	}

	public void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern) {
		// not runtime configurable
		
	}

	public void setVerifyServerCertificate(boolean flag) {
		// not runtime configurable
		
	}

	public void setYearIsDateType(boolean flag) {
		// not runtime configurable
		
	}

	public void setZeroDateTimeBehavior(String behavior) {
		// not runtime configurable
		
	}

	public boolean useUnbufferedInput() {
		return getCurrentConnection().useUnbufferedInput();
	}

	public boolean isSameResource(Connection c) {
		return getCurrentConnection().isSameResource(c);
	}

	public void setInGlobalTx(boolean flag) {
		getCurrentConnection().setInGlobalTx(flag);
	}

	public boolean getUseColumnNamesInFindColumn() {
		return getCurrentConnection().getUseColumnNamesInFindColumn();
	}

	public void setUseColumnNamesInFindColumn(boolean flag) {
		// not runtime configurable
	}

	public boolean getUseLocalTransactionState() {
		return getCurrentConnection().getUseLocalTransactionState();
	}

	public void setUseLocalTransactionState(boolean flag) {
		// not runtime configurable
		
	}

	public boolean getCompensateOnDuplicateKeyUpdateCounts() {
		return getCurrentConnection().getCompensateOnDuplicateKeyUpdateCounts();
	}

	public void setCompensateOnDuplicateKeyUpdateCounts(boolean flag) {
		// not runtime configurable
		
	}

	public boolean getUseAffectedRows() {
		return getCurrentConnection().getUseAffectedRows();
	}

	public void setUseAffectedRows(boolean flag) {
		// not runtime configurable
		
	}

	public String getPasswordCharacterEncoding() {
		return getCurrentConnection().getPasswordCharacterEncoding();
	}

	public void setPasswordCharacterEncoding(String characterSet) {
		getCurrentConnection().setPasswordCharacterEncoding(characterSet);
	}

	public int getAutoIncrementIncrement() {
		return getCurrentConnection().getAutoIncrementIncrement();
	}

	public int getLoadBalanceBlacklistTimeout() {
		return getCurrentConnection().getLoadBalanceBlacklistTimeout();
	}

	public void setLoadBalanceBlacklistTimeout(int loadBalanceBlacklistTimeout) {
		getCurrentConnection().setLoadBalanceBlacklistTimeout(loadBalanceBlacklistTimeout);
	}
	
	public int getLoadBalancePingTimeout() {
		return getCurrentConnection().getLoadBalancePingTimeout();
	}

	public void setLoadBalancePingTimeout(int loadBalancePingTimeout) {
		getCurrentConnection().setLoadBalancePingTimeout(loadBalancePingTimeout);
	}
	
	public boolean getLoadBalanceValidateConnectionOnSwapServer() {
		return getCurrentConnection().getLoadBalanceValidateConnectionOnSwapServer();
	}

	public void setLoadBalanceValidateConnectionOnSwapServer(boolean loadBalanceValidateConnectionOnSwapServer) {
		getCurrentConnection().setLoadBalanceValidateConnectionOnSwapServer(loadBalanceValidateConnectionOnSwapServer);
	}

	public int getRetriesAllDown() {
		return getCurrentConnection().getRetriesAllDown();
	}

	public void setRetriesAllDown(int retriesAllDown) {
		getCurrentConnection().setRetriesAllDown(retriesAllDown);
	}

	public ExceptionInterceptor getExceptionInterceptor() {
		return getCurrentConnection().getExceptionInterceptor();
	}

	public String getExceptionInterceptors() {
		return getCurrentConnection().getExceptionInterceptors();
	}

	public void setExceptionInterceptors(String exceptionInterceptors) {
		getCurrentConnection().setExceptionInterceptors(exceptionInterceptors);
	}

	public boolean getQueryTimeoutKillsConnection() {
		return getCurrentConnection().getQueryTimeoutKillsConnection();
	}

	public void setQueryTimeoutKillsConnection(
			boolean queryTimeoutKillsConnection) {
		getCurrentConnection().setQueryTimeoutKillsConnection(queryTimeoutKillsConnection);
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
      return getCurrentConnection().getHost();
   }

   public void setProxy(MySQLConnection proxy) {
      getCurrentConnection().setProxy(proxy);
   }

 	public boolean getRetainStatementAfterResultSetClose() {
		return getCurrentConnection().getRetainStatementAfterResultSetClose();
	}

 	public int getMaxAllowedPacket() {
		return getCurrentConnection().getMaxAllowedPacket();
	}
 	
	public String getLoadBalanceConnectionGroup() {
		return getCurrentConnection().getLoadBalanceConnectionGroup();
	}

	public boolean getLoadBalanceEnableJMX() {
		return getCurrentConnection().getLoadBalanceEnableJMX();
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
		return getCurrentConnection().getLoadBalanceAutoCommitStatementRegex();
	}

	public int getLoadBalanceAutoCommitStatementThreshold() {
		return getCurrentConnection().getLoadBalanceAutoCommitStatementThreshold();
	}

	public void setLoadBalanceAutoCommitStatementRegex(
			String loadBalanceAutoCommitStatementRegex) {
		getCurrentConnection().setLoadBalanceAutoCommitStatementRegex(loadBalanceAutoCommitStatementRegex);
		
	}

	public void setLoadBalanceAutoCommitStatementThreshold(
			int loadBalanceAutoCommitStatementThreshold) {
		getCurrentConnection().setLoadBalanceAutoCommitStatementThreshold(loadBalanceAutoCommitStatementThreshold);
		
	}

	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		// TODO Auto-generated method stub
		
	}

	public boolean getIncludeThreadDumpInDeadlockExceptions() {
		return getCurrentConnection().getIncludeThreadDumpInDeadlockExceptions();
	}

	public void setIncludeThreadDumpInDeadlockExceptions(boolean flag) {
		getCurrentConnection().setIncludeThreadDumpInDeadlockExceptions(flag);
		
	}

	public boolean getIncludeThreadNamesAsStatementComment() {
		return getCurrentConnection().getIncludeThreadNamesAsStatementComment();
	}

	public void setIncludeThreadNamesAsStatementComment(boolean flag) {
		getCurrentConnection().setIncludeThreadNamesAsStatementComment(flag);
	}

	public boolean isServerLocal() throws SQLException {
		return getCurrentConnection().isServerLocal();
	}

	public void setAuthenticationPlugins(String authenticationPlugins) {
		getCurrentConnection().setAuthenticationPlugins(authenticationPlugins);
	}

	public String getAuthenticationPlugins() {
		return getCurrentConnection().getAuthenticationPlugins();
	}

	public void setDisabledAuthenticationPlugins(
			String disabledAuthenticationPlugins) {
		getCurrentConnection().setDisabledAuthenticationPlugins(disabledAuthenticationPlugins);
	}

	public String getDisabledAuthenticationPlugins() {
		return getCurrentConnection().getDisabledAuthenticationPlugins();
	}

	public void setDefaultAuthenticationPlugin(
			String defaultAuthenticationPlugin) {
		getCurrentConnection().setDefaultAuthenticationPlugin(defaultAuthenticationPlugin);
	}

	public String getDefaultAuthenticationPlugin() {
		return getCurrentConnection().getDefaultAuthenticationPlugin();
	}

	public void setParseInfoCacheFactory(String factoryClassname) {
		getCurrentConnection().setParseInfoCacheFactory(factoryClassname);
	}

	public String getParseInfoCacheFactory() {
		return getCurrentConnection().getParseInfoCacheFactory();
	}

	public void setSchema(String schema) throws SQLException {
		getCurrentConnection().setSchema(schema);
	}

	public String getSchema() throws SQLException {
		return getCurrentConnection().getSchema();
	}

	public void abort(Executor executor) throws SQLException {
		getCurrentConnection().abort(executor);
	}

	public void setNetworkTimeout(Executor executor, int milliseconds)
			throws SQLException {
		getCurrentConnection().setNetworkTimeout(executor, milliseconds);
	}

	public int getNetworkTimeout() throws SQLException {
		return getCurrentConnection().getNetworkTimeout();
	}

	public void setServerConfigCacheFactory(String factoryClassname) {
		getCurrentConnection().setServerConfigCacheFactory(factoryClassname);
	}

	public String getServerConfigCacheFactory() {
		return getCurrentConnection().getServerConfigCacheFactory();
	}

	public void setDisconnectOnExpiredPasswords(boolean disconnectOnExpiredPasswords) {
		getCurrentConnection().setDisconnectOnExpiredPasswords(disconnectOnExpiredPasswords);
	}

	public boolean getDisconnectOnExpiredPasswords() {
		return getCurrentConnection().getDisconnectOnExpiredPasswords();
	}

	public void setGetProceduresReturnsFunctions(boolean getProcedureReturnsFunctions) {
		getCurrentConnection().setGetProceduresReturnsFunctions(getProcedureReturnsFunctions);
	}

	public boolean getGetProceduresReturnsFunctions() {
		return getCurrentConnection().getGetProceduresReturnsFunctions();
	}

	public void abortInternal() throws SQLException {
		getCurrentConnection().abortInternal();
	}

	public void checkClosed() throws SQLException {
		getCurrentConnection().checkClosed();
	}

	public Object getConnectionMutex() {
		return getCurrentConnection().getConnectionMutex();
	}

	public String getConnectionAttributes() throws SQLException {
		return getCurrentConnection().getConnectionAttributes();
	}
}
