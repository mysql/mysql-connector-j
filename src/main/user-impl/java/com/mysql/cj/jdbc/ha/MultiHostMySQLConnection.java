/*
 * Copyright (c) 2015, 2019, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License, version 2.0, as published by the
 * Free Software Foundation.
 *
 * This program is also distributed with certain software (including but not
 * limited to OpenSSL) that is licensed under separate terms, as designated in a
 * particular file or component or in included license documentation. The
 * authors of MySQL hereby grant you an additional permission to link the
 * program and your derivative works with the separately licensed software that
 * they have included with MySQL.
 *
 * Without limiting anything contained in the foregoing, this file, which is
 * part of MySQL Connector/J, is also subject to the Universal FOSS Exception,
 * version 1.0, a copy of which can be found at
 * http://oss.oracle.com/licenses/universal-foss-exception.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

package com.mysql.cj.jdbc.ha;

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
import java.util.concurrent.Executor;

import com.mysql.cj.Messages;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.Session;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.ClientInfoProvider;
import com.mysql.cj.jdbc.JdbcConnection;
import com.mysql.cj.jdbc.JdbcPreparedStatement;
import com.mysql.cj.jdbc.JdbcPropertySet;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.result.CachedResultSetMetaData;
import com.mysql.cj.jdbc.result.ResultSetInternalMethods;

/**
 * Each instance of MultiHostMySQLConnection is coupled with a MultiHostConnectionProxy instance.
 * 
 * While this class implements MySQLConnection directly, MultiHostConnectionProxy does the same but via a dynamic proxy.
 * 
 * Most of the methods in this class refer directly to the active connection from its MultiHostConnectionProxy pair, providing a non-proxied access to the
 * current active connection managed by this multi-host structure. The remaining methods either implement some local behavior or refer to the proxy itself
 * instead of the sub-connection.
 * 
 * Referring to the higher level proxy connection is needed when some operation needs to be extended to all open sub-connections existing in this multi-host
 * structure as opposed to just refer to the active current connection, such as with close() which is most likely required to close all sub-connections as
 * well.
 */
public class MultiHostMySQLConnection implements JdbcConnection {

    /**
     * thisAsProxy holds the proxy (MultiHostConnectionProxy or one of its subclasses) this connection is associated with.
     * It is used as a gateway to the current active sub-connection managed by this multi-host structure or as a target to where some of the methods implemented
     * here in this class refer to.
     */
    protected MultiHostConnectionProxy thisAsProxy;

    public MultiHostMySQLConnection(MultiHostConnectionProxy proxy) {
        this.thisAsProxy = proxy;
    }

    public MultiHostConnectionProxy getThisAsProxy() {
        return this.thisAsProxy;
    }

    @Override
    public JdbcConnection getActiveMySQLConnection() {
        synchronized (this.thisAsProxy) {
            return this.thisAsProxy.currentConnection;
        }
    }

    @Override
    public void abortInternal() throws SQLException {
        getActiveMySQLConnection().abortInternal();
    }

    @Override
    public void changeUser(String userName, String newPassword) throws SQLException {
        getActiveMySQLConnection().changeUser(userName, newPassword);
    }

    @Override
    public void checkClosed() {
        getActiveMySQLConnection().checkClosed();
    }

    @Deprecated
    @Override
    public void clearHasTriedMaster() {
        getActiveMySQLConnection().clearHasTriedMaster();
    }

    @Override
    public void clearWarnings() throws SQLException {
        getActiveMySQLConnection().clearWarnings();
    }

    @Override
    public PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return getActiveMySQLConnection().clientPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return getActiveMySQLConnection().clientPrepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement clientPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        return getActiveMySQLConnection().clientPrepareStatement(sql, autoGenKeyIndex);
    }

    @Override
    public PreparedStatement clientPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        return getActiveMySQLConnection().clientPrepareStatement(sql, autoGenKeyIndexes);
    }

    @Override
    public PreparedStatement clientPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        return getActiveMySQLConnection().clientPrepareStatement(sql, autoGenKeyColNames);
    }

    @Override
    public PreparedStatement clientPrepareStatement(String sql) throws SQLException {
        return getActiveMySQLConnection().clientPrepareStatement(sql);
    }

    @Override
    public void close() throws SQLException {
        getActiveMySQLConnection().close();
    }

    @Override
    public void commit() throws SQLException {
        getActiveMySQLConnection().commit();
    }

    @Override
    public void createNewIO(boolean isForReconnect) {
        getActiveMySQLConnection().createNewIO(isForReconnect);
    }

    @Override
    public Statement createStatement() throws SQLException {
        return getActiveMySQLConnection().createStatement();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return getActiveMySQLConnection().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return getActiveMySQLConnection().createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public int getActiveStatementCount() {
        return getActiveMySQLConnection().getActiveStatementCount();
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return getActiveMySQLConnection().getAutoCommit();
    }

    @Override
    public int getAutoIncrementIncrement() {
        return getActiveMySQLConnection().getAutoIncrementIncrement();
    }

    @Override
    public CachedResultSetMetaData getCachedMetaData(String sql) {
        return getActiveMySQLConnection().getCachedMetaData(sql);
    }

    @Override
    public String getCatalog() throws SQLException {
        return getActiveMySQLConnection().getCatalog();
    }

    @Override
    public String getCharacterSetMetadata() {
        return getActiveMySQLConnection().getCharacterSetMetadata();
    }

    @Override
    public ExceptionInterceptor getExceptionInterceptor() {
        return getActiveMySQLConnection().getExceptionInterceptor();
    }

    @Override
    public int getHoldability() throws SQLException {
        return getActiveMySQLConnection().getHoldability();
    }

    @Override
    public String getHost() {
        return getActiveMySQLConnection().getHost();
    }

    @Override
    public long getId() {
        return getActiveMySQLConnection().getId();
    }

    @Override
    public long getIdleFor() {
        return getActiveMySQLConnection().getIdleFor();
    }

    @Override
    public JdbcConnection getMultiHostSafeProxy() {
        return getThisAsProxy().getProxy();
    }

    @Override
    public JdbcConnection getMultiHostParentProxy() {
        return getThisAsProxy().getParentProxy();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return getActiveMySQLConnection().getMetaData();
    }

    @Override
    public Statement getMetadataSafeStatement() throws SQLException {
        return getActiveMySQLConnection().getMetadataSafeStatement();
    }

    @Override
    public Properties getProperties() {
        return getActiveMySQLConnection().getProperties();
    }

    @Override
    public ServerVersion getServerVersion() {
        return getActiveMySQLConnection().getServerVersion();
    }

    @Override
    public Session getSession() {
        return getActiveMySQLConnection().getSession();
    }

    @Override
    public String getStatementComment() {
        return getActiveMySQLConnection().getStatementComment();
    }

    @Override
    public List<QueryInterceptor> getQueryInterceptorsInstances() {
        return getActiveMySQLConnection().getQueryInterceptorsInstances();
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return getActiveMySQLConnection().getTransactionIsolation();
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return getActiveMySQLConnection().getTypeMap();
    }

    @Override
    public String getURL() {
        return getActiveMySQLConnection().getURL();
    }

    @Override
    public String getUser() {
        return getActiveMySQLConnection().getUser();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return getActiveMySQLConnection().getWarnings();
    }

    @Override
    public boolean hasSameProperties(JdbcConnection c) {
        return getActiveMySQLConnection().hasSameProperties(c);
    }

    @Deprecated
    @Override
    public boolean hasTriedMaster() {
        return getActiveMySQLConnection().hasTriedMaster();
    }

    @Override
    public void initializeResultsMetadataFromCache(String sql, CachedResultSetMetaData cachedMetaData, ResultSetInternalMethods resultSet) throws SQLException {
        getActiveMySQLConnection().initializeResultsMetadataFromCache(sql, cachedMetaData, resultSet);
    }

    @Override
    public void initializeSafeQueryInterceptors() throws SQLException {
        getActiveMySQLConnection().initializeSafeQueryInterceptors();
    }

    @Override
    public boolean isInGlobalTx() {
        return getActiveMySQLConnection().isInGlobalTx();
    }

    @Override
    public boolean isMasterConnection() {
        return getThisAsProxy().isMasterConnection();
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return getActiveMySQLConnection().isReadOnly();
    }

    @Override
    public boolean isReadOnly(boolean useSessionStatus) throws SQLException {
        return getActiveMySQLConnection().isReadOnly(useSessionStatus);
    }

    @Override
    public boolean isSameResource(JdbcConnection otherConnection) {
        return getActiveMySQLConnection().isSameResource(otherConnection);
    }

    @Override
    public boolean lowerCaseTableNames() {
        return getActiveMySQLConnection().lowerCaseTableNames();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return getActiveMySQLConnection().nativeSQL(sql);
    }

    @Override
    public void ping() throws SQLException {
        getActiveMySQLConnection().ping();
    }

    @Override
    public void pingInternal(boolean checkForClosedConnection, int timeoutMillis) throws SQLException {
        getActiveMySQLConnection().pingInternal(checkForClosedConnection, timeoutMillis);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return getActiveMySQLConnection().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return getActiveMySQLConnection().prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return getActiveMySQLConnection().prepareCall(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return getActiveMySQLConnection().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return getActiveMySQLConnection().prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        return getActiveMySQLConnection().prepareStatement(sql, autoGenKeyIndex);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        return getActiveMySQLConnection().prepareStatement(sql, autoGenKeyIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        return getActiveMySQLConnection().prepareStatement(sql, autoGenKeyColNames);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return getActiveMySQLConnection().prepareStatement(sql);
    }

    @Override
    public void realClose(boolean calledExplicitly, boolean issueRollback, boolean skipLocalTeardown, Throwable reason) throws SQLException {
        getActiveMySQLConnection().realClose(calledExplicitly, issueRollback, skipLocalTeardown, reason);
    }

    @Override
    public void recachePreparedStatement(JdbcPreparedStatement pstmt) throws SQLException {
        getActiveMySQLConnection().recachePreparedStatement(pstmt);
    }

    @Override
    public void decachePreparedStatement(JdbcPreparedStatement pstmt) throws SQLException {
        getActiveMySQLConnection().decachePreparedStatement(pstmt);
    }

    @Override
    public void registerStatement(com.mysql.cj.jdbc.JdbcStatement stmt) {
        getActiveMySQLConnection().registerStatement(stmt);
    }

    @Override
    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        getActiveMySQLConnection().releaseSavepoint(arg0);
    }

    @Override
    public void resetServerState() throws SQLException {
        getActiveMySQLConnection().resetServerState();
    }

    @Override
    public void rollback() throws SQLException {
        getActiveMySQLConnection().rollback();
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        getActiveMySQLConnection().rollback(savepoint);
    }

    @Override
    public PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return getActiveMySQLConnection().serverPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return getActiveMySQLConnection().serverPrepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement serverPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        return getActiveMySQLConnection().serverPrepareStatement(sql, autoGenKeyIndex);
    }

    @Override
    public PreparedStatement serverPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        return getActiveMySQLConnection().serverPrepareStatement(sql, autoGenKeyIndexes);
    }

    @Override
    public PreparedStatement serverPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        return getActiveMySQLConnection().serverPrepareStatement(sql, autoGenKeyColNames);
    }

    @Override
    public PreparedStatement serverPrepareStatement(String sql) throws SQLException {
        return getActiveMySQLConnection().serverPrepareStatement(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommitFlag) throws SQLException {
        getActiveMySQLConnection().setAutoCommit(autoCommitFlag);
    }

    @Override
    public void setDatabase(String dbName) throws SQLException {
        getActiveMySQLConnection().setDatabase(dbName);
    }

    @Override
    public String getDatabase() throws SQLException {
        return getActiveMySQLConnection().getDatabase();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        getActiveMySQLConnection().setCatalog(catalog);
    }

    @Override
    public void setFailedOver(boolean flag) {
        getActiveMySQLConnection().setFailedOver(flag);
    }

    @Override
    public void setHoldability(int arg0) throws SQLException {
        getActiveMySQLConnection().setHoldability(arg0);
    }

    @Override
    public void setInGlobalTx(boolean flag) {
        getActiveMySQLConnection().setInGlobalTx(flag);
    }

    @Override
    public void setProxy(JdbcConnection proxy) {
        getThisAsProxy().setProxy(proxy);
    }

    @Override
    public void setReadOnly(boolean readOnlyFlag) throws SQLException {
        getActiveMySQLConnection().setReadOnly(readOnlyFlag);
    }

    @Override
    public void setReadOnlyInternal(boolean readOnlyFlag) throws SQLException {
        getActiveMySQLConnection().setReadOnlyInternal(readOnlyFlag);
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return getActiveMySQLConnection().setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return getActiveMySQLConnection().setSavepoint(name);
    }

    @Override
    public void setStatementComment(String comment) {
        getActiveMySQLConnection().setStatementComment(comment);
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        getActiveMySQLConnection().setTransactionIsolation(level);
    }

    @Override
    public void shutdownServer() throws SQLException {
        getActiveMySQLConnection().shutdownServer();
    }

    @Override
    public boolean storesLowerCaseTableName() {
        return getActiveMySQLConnection().storesLowerCaseTableName();
    }

    @Override
    public void throwConnectionClosedException() throws SQLException {
        getActiveMySQLConnection().throwConnectionClosedException();
    }

    @Override
    public void transactionBegun() {
        getActiveMySQLConnection().transactionBegun();
    }

    @Override
    public void transactionCompleted() {
        getActiveMySQLConnection().transactionCompleted();
    }

    @Override
    public void unregisterStatement(com.mysql.cj.jdbc.JdbcStatement stmt) {
        getActiveMySQLConnection().unregisterStatement(stmt);
    }

    @Override
    public void unSafeQueryInterceptors() throws SQLException {
        getActiveMySQLConnection().unSafeQueryInterceptors();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return getThisAsProxy().isClosed;
    }

    @Override
    public boolean isProxySet() {
        return this.getActiveMySQLConnection().isProxySet();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        getActiveMySQLConnection().setTypeMap(map);
    }

    @Override
    public boolean isServerLocal() throws SQLException {
        return getActiveMySQLConnection().isServerLocal();
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        getActiveMySQLConnection().setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return getActiveMySQLConnection().getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        getActiveMySQLConnection().abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        getActiveMySQLConnection().setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return getActiveMySQLConnection().getNetworkTimeout();
    }

    @Override
    public Object getConnectionMutex() {
        return getActiveMySQLConnection().getConnectionMutex();
    }

    @Override
    public int getSessionMaxRows() {
        return getActiveMySQLConnection().getSessionMaxRows();
    }

    @Override
    public void setSessionMaxRows(int max) throws SQLException {
        getActiveMySQLConnection().setSessionMaxRows(max);
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return getActiveMySQLConnection().createSQLXML();
    }

    @Override
    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return getActiveMySQLConnection().createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return getActiveMySQLConnection().createStruct(typeName, attributes);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return getActiveMySQLConnection().getClientInfo();
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return getActiveMySQLConnection().getClientInfo(name);
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return getActiveMySQLConnection().isValid(timeout);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        getActiveMySQLConnection().setClientInfo(properties);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        getActiveMySQLConnection().setClientInfo(name, value);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }

    @Override
    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
        }
    }

    @Override
    public Blob createBlob() throws SQLException {
        return getActiveMySQLConnection().createBlob();
    }

    @Override
    public Clob createClob() throws SQLException {
        return getActiveMySQLConnection().createClob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return getActiveMySQLConnection().createNClob();
    }

    @Override
    public ClientInfoProvider getClientInfoProviderImpl() throws SQLException {
        synchronized (getThisAsProxy()) {
            return getActiveMySQLConnection().getClientInfoProviderImpl();
        }
    }

    @Override
    public JdbcPropertySet getPropertySet() {
        return getActiveMySQLConnection().getPropertySet();
    }

    @Override
    public String getHostPortPair() {
        return getActiveMySQLConnection().getHostPortPair();
    }

    @Override
    public void normalClose() {
        getActiveMySQLConnection().normalClose();
    }

    @Override
    public void cleanup(Throwable whyCleanedUp) {
        getActiveMySQLConnection().cleanup(whyCleanedUp);
    }
}
