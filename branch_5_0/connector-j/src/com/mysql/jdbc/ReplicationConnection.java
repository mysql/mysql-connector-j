/*
 Copyright (C) 2004 MySQL AB

 This program is free software; you can redistribute it and/or modify
 it under the terms of version 2 of the GNU General Public License as 
 published by the Free Software Foundation.

 There are special exceptions to the terms and conditions of the GPL 
 as it is applied to this software. View the full text of the 
 exception in file EXCEPTIONS-CONNECTOR-J in the directory of this 
 software distribution.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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

/**
 * Connection that opens two connections, one two a replication master, and
 * another to one or more slaves, and decides to use master when the connection
 * is not read-only, and use slave(s) when the connection is read-only.
 * 
 * @version $Id: ReplicationConnection.java,v 1.1.2.1 2005/05/13 18:58:38
 *          mmatthews Exp $
 */
public class ReplicationConnection implements java.sql.Connection {
	private Connection currentConnection;

	private Connection masterConnection;

	private Connection slavesConnection;

	public ReplicationConnection(Properties masterProperties,
			Properties slaveProperties) throws SQLException {
		Driver driver = new Driver();

		StringBuffer masterUrl = new StringBuffer("jdbc:mysql://");
        StringBuffer slaveUrl = new StringBuffer("jdbc:mysql://");

        String masterHost = masterProperties
        	.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
        
        if (masterHost != null) {
        	masterUrl.append(masterHost);
        }
 
        String slaveHost = slaveProperties
        	.getProperty(NonRegisteringDriver.HOST_PROPERTY_KEY);
        	
        if (slaveHost != null) {
        	slaveUrl.append(slaveHost);
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
        
        this.masterConnection = (com.mysql.jdbc.Connection) driver.connect(
                masterUrl.toString(), masterProperties);
        this.slavesConnection = (com.mysql.jdbc.Connection) driver.connect(
                slaveUrl.toString(), slaveProperties);
        
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
		return this.currentConnection.createStatement();
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#createStatement(int, int)
	 */
	public synchronized Statement createStatement(int resultSetType,
			int resultSetConcurrency) throws SQLException {
		return this.currentConnection.createStatement(resultSetType,
				resultSetConcurrency);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#createStatement(int, int, int)
	 */
	public synchronized Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		return this.currentConnection.createStatement(resultSetType,
				resultSetConcurrency, resultSetHoldability);
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
	public synchronized Map getTypeMap() throws SQLException {
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
		return this.currentConnection.prepareStatement(sql);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int)
	 */
	public synchronized PreparedStatement prepareStatement(String sql,
			int autoGeneratedKeys) throws SQLException {
		return this.currentConnection.prepareStatement(sql, autoGeneratedKeys);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int, int)
	 */
	public synchronized PreparedStatement prepareStatement(String sql,
			int resultSetType, int resultSetConcurrency) throws SQLException {
		return this.currentConnection.prepareStatement(sql, resultSetType,
				resultSetConcurrency);
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
		return this.currentConnection.prepareStatement(sql, resultSetType,
				resultSetConcurrency, resultSetHoldability);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String, int[])
	 */
	public synchronized PreparedStatement prepareStatement(String sql,
			int[] columnIndexes) throws SQLException {
		return this.currentConnection.prepareStatement(sql, columnIndexes);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#prepareStatement(java.lang.String,
	 *      java.lang.String[])
	 */
	public synchronized PreparedStatement prepareStatement(String sql,
			String[] columnNames) throws SQLException {
		return this.currentConnection.prepareStatement(sql, columnNames);
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

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.sql.Connection#setTypeMap(java.util.Map)
	 */
	public synchronized void setTypeMap(Map arg0) throws SQLException {
		this.currentConnection.setTypeMap(arg0);
	}

	private synchronized void switchToMasterConnection() throws SQLException {
		swapConnections(this.masterConnection, this.slavesConnection);
	}

	private synchronized void switchToSlavesConnection() throws SQLException {
		swapConnections(this.slavesConnection, this.masterConnection);
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
}
