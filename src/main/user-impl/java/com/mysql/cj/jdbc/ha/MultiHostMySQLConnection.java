/*
 * Copyright (c) 2015, 2018, Oracle and/or its affiliates. All rights reserved.
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

    public JdbcConnection getActiveMySQLConnection() {
        synchronized (this.thisAsProxy) {
            return this.thisAsProxy.currentConnection;
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

    @Deprecated
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

    public String getCatalog() throws SQLException {
        return getActiveMySQLConnection().getCatalog();
    }

    public String getCharacterSetMetadata() {
        return getActiveMySQLConnection().getCharacterSetMetadata();
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

    public JdbcConnection getMultiHostSafeProxy() {
        return getThisAsProxy().getProxy();
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return getActiveMySQLConnection().getMetaData();
    }

    public Statement getMetadataSafeStatement() throws SQLException {
        return getActiveMySQLConnection().getMetadataSafeStatement();
    }

    public Properties getProperties() {
        return getActiveMySQLConnection().getProperties();
    }

    public ServerVersion getServerVersion() {
        return getActiveMySQLConnection().getServerVersion();
    }

    public Session getSession() {
        return getActiveMySQLConnection().getSession();
    }

    public String getStatementComment() {
        return getActiveMySQLConnection().getStatementComment();
    }

    public List<QueryInterceptor> getQueryInterceptorsInstances() {
        return getActiveMySQLConnection().getQueryInterceptorsInstances();
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

    @Deprecated
    public boolean hasTriedMaster() {
        return getActiveMySQLConnection().hasTriedMaster();
    }

    public void initializeResultsMetadataFromCache(String sql, CachedResultSetMetaData cachedMetaData, ResultSetInternalMethods resultSet) throws SQLException {
        getActiveMySQLConnection().initializeResultsMetadataFromCache(sql, cachedMetaData, resultSet);
    }

    public void initializeSafeQueryInterceptors() throws SQLException {
        getActiveMySQLConnection().initializeSafeQueryInterceptors();
    }

    public boolean isInGlobalTx() {
        return getActiveMySQLConnection().isInGlobalTx();
    }

    public boolean isMasterConnection() {
        return getThisAsProxy().isMasterConnection();
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

    public void recachePreparedStatement(JdbcPreparedStatement pstmt) throws SQLException {
        getActiveMySQLConnection().recachePreparedStatement(pstmt);
    }

    public void decachePreparedStatement(JdbcPreparedStatement pstmt) throws SQLException {
        getActiveMySQLConnection().decachePreparedStatement(pstmt);
    }

    public void registerStatement(com.mysql.cj.jdbc.JdbcStatement stmt) {
        getActiveMySQLConnection().registerStatement(stmt);
    }

    public void releaseSavepoint(Savepoint arg0) throws SQLException {
        getActiveMySQLConnection().releaseSavepoint(arg0);
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

    public void setProxy(JdbcConnection proxy) {
        getThisAsProxy().setProxy(proxy);
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

    public void transactionBegun() {
        getActiveMySQLConnection().transactionBegun();
    }

    public void transactionCompleted() {
        getActiveMySQLConnection().transactionCompleted();
    }

    public void unregisterStatement(com.mysql.cj.jdbc.JdbcStatement stmt) {
        getActiveMySQLConnection().unregisterStatement(stmt);
    }

    public void unSafeQueryInterceptors() throws SQLException {
        getActiveMySQLConnection().unSafeQueryInterceptors();
    }

    public boolean isClosed() throws SQLException {
        return getThisAsProxy().isClosed;
    }

    public boolean isProxySet() {
        return this.getActiveMySQLConnection().isProxySet();
    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        getActiveMySQLConnection().setTypeMap(map);
    }

    public boolean isServerLocal() throws SQLException {
        return getActiveMySQLConnection().isServerLocal();
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

    public Object getConnectionMutex() {
        return getActiveMySQLConnection().getConnectionMutex();
    }

    public int getSessionMaxRows() {
        return getActiveMySQLConnection().getSessionMaxRows();
    }

    public void setSessionMaxRows(int max) throws SQLException {
        getActiveMySQLConnection().setSessionMaxRows(max);
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
        return getActiveMySQLConnection().isValid(timeout);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        getActiveMySQLConnection().setClientInfo(properties);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        getActiveMySQLConnection().setClientInfo(name, value);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        // This works for classes that aren't actually wrapping anything
        return iface.isInstance(this);
    }

    public <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(this);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, getExceptionInterceptor());
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
