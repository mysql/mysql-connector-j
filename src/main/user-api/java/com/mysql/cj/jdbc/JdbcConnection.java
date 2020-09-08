/*
 * Copyright (c) 2007, 2020, Oracle and/or its affiliates.
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

package com.mysql.cj.jdbc;

import java.sql.SQLException;
import java.util.List;

import com.mysql.cj.MysqlConnection;
import com.mysql.cj.ServerVersion;
import com.mysql.cj.TransactionEventHandler;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.jdbc.result.CachedResultSetMetaData;
import com.mysql.cj.jdbc.result.ResultSetInternalMethods;

/**
 * This interface contains methods that are considered the "vendor extension" to the JDBC API for MySQL's implementation of java.sql.Connection.
 * 
 * For those looking further into the driver implementation, it is not an API that is used for plugability of implementations inside our driver
 * (which is why there are still references to ConnectionImpl throughout the code).
 */
public interface JdbcConnection extends java.sql.Connection, MysqlConnection, TransactionEventHandler {

    public JdbcPropertySet getPropertySet();

    /**
     * Changes the user on this connection by performing a re-authentication. If
     * authentication fails, the connection is failed.
     * 
     * @param userName
     *            the username to authenticate with
     * @param newPassword
     *            the password to authenticate with
     * @throws SQLException
     *             if authentication fails, or some other error occurs while
     *             performing the command.
     */
    void changeUser(String userName, String newPassword) throws SQLException;

    @Deprecated
    void clearHasTriedMaster();

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
     * 
     * @param sql
     *            statement
     * @return prepared statement
     * @throws SQLException
     *             if an error occurs
     * @see java.sql.Connection#prepareStatement(String)
     */
    java.sql.PreparedStatement clientPrepareStatement(String sql) throws SQLException;

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
     * 
     * @param sql
     *            statement
     * @param autoGenKeyIndex
     *            autoGenKeyIndex
     * @return prepared statement
     * @throws SQLException
     *             if an error occurs
     * @see java.sql.Connection#prepareStatement(String, int)
     */
    java.sql.PreparedStatement clientPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException;

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
     * 
     * @param sql
     *            statement
     * @param resultSetType
     *            resultSetType
     * @param resultSetConcurrency
     *            resultSetConcurrency
     * @return prepared statement
     * @throws SQLException
     *             if an error occurs
     * 
     * @see java.sql.Connection#prepareStatement(String, int, int)
     */
    java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException;

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
     * 
     * @param sql
     *            statement
     * @param autoGenKeyIndexes
     *            autoGenKeyIndexes
     * @return prepared statement
     * @throws SQLException
     *             if an error occurs
     * 
     * @see java.sql.Connection#prepareStatement(String, int[])
     */
    java.sql.PreparedStatement clientPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException;

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
     * 
     * @param sql
     *            statement
     * @param resultSetType
     *            resultSetType
     * @param resultSetConcurrency
     *            resultSetConcurrency
     * @param resultSetHoldability
     *            resultSetHoldability
     * @return prepared statement
     * @throws SQLException
     *             if an error occurs
     * 
     * @see java.sql.Connection#prepareStatement(String, int, int, int)
     */
    java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException;

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
     * 
     * @param sql
     *            statement
     * @param autoGenKeyColNames
     *            autoGenKeyColNames
     * @return prepared statement
     * @throws SQLException
     *             if an error occurs
     * 
     * @see java.sql.Connection#prepareStatement(String, String[])
     */
    java.sql.PreparedStatement clientPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException;

    /**
     * Returns the number of statements active on this connection, which
     * haven't been .close()d.
     * 
     * @return the number of active statements
     */
    int getActiveStatementCount();

    /**
     * Reports how long this connection has been idle.
     * This time (reported in milliseconds) is updated once a query has
     * completed.
     * 
     * @return number of ms that this connection has been idle, 0 if the driver
     *         is busy retrieving results.
     */
    long getIdleFor();

    /**
     * Returns the comment that will be prepended to all statements
     * sent to the server.
     * 
     * @return the comment that will be prepended to all statements
     *         sent to the server.
     */
    String getStatementComment();

    /**
     * Has this connection tried to execute a query on the "source"
     * server (first host in a multiple host list).
     * 
     * @return true if it has tried
     */
    @Deprecated
    boolean hasTriedMaster();

    /**
     * Is this connection currently a participant in an XA transaction?
     * 
     * @return true if this connection currently a participant in an XA transaction
     */
    boolean isInGlobalTx();

    /**
     * Set the state of being in a global (XA) transaction.
     * 
     * @param flag
     *            the state flag
     */
    void setInGlobalTx(boolean flag);

    // TODO this and other multi-host connection specific methods should be moved to special interface
    /**
     * Is this connection connected to the first host in the list if
     * there is a list of servers in the URL?
     * 
     * @return true if this connection is connected to the first in
     *         the list.
     */
    boolean isSourceConnection();

    /**
     * Use {@link #isSourceConnection()} instead.
     * 
     * @return true if it's a source connection
     * @deprecated
     */
    @Deprecated
    default boolean isMasterConnection() {
        return isSourceConnection();
    }

    /**
     * Does this connection have the same resource name as the given
     * connection (for XA)?
     * 
     * @param c
     *            connection
     * @return true if it is the same one
     */
    boolean isSameResource(JdbcConnection c);

    /**
     * Is the server configured to use lower-case table names only?
     * 
     * @return true if lower_case_table_names is 'on'
     */
    boolean lowerCaseTableNames();

    /**
     * Detect if the connection is still good by sending a ping command
     * to the server.
     * 
     * @throws SQLException
     *             if the ping fails
     */
    void ping() throws SQLException;

    /**
     * Resets the server-side state of this connection. Doesn't work if isParanoid() is set
     * (it will become a no-op in this case). Usually only used from connection pooling code.
     * 
     * @throws SQLException
     *             if the operation fails while resetting server state.
     */
    void resetServerState() throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @param sql
     *            statement
     * @return prepared statement
     * @throws SQLException
     *             if an error occurs
     * @see java.sql.Connection#prepareStatement(String)
     */
    java.sql.PreparedStatement serverPrepareStatement(String sql) throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @param sql
     *            statement
     * @param autoGenKeyIndex
     *            autoGenKeyIndex
     * @return prepared statement
     * @throws SQLException
     *             if an error occurs
     * @see java.sql.Connection#prepareStatement(String, int)
     */
    java.sql.PreparedStatement serverPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @param sql
     *            statement
     * @param resultSetType
     *            resultSetType
     * @param resultSetConcurrency
     *            resultSetConcurrency
     * @return prepared statement
     * @throws SQLException
     *             if an error occurs
     * 
     * @see java.sql.Connection#prepareStatement(String, int, int)
     */
    java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @param sql
     *            statement
     * @param resultSetType
     *            resultSetType
     * @param resultSetConcurrency
     *            resultSetConcurrency
     * @param resultSetHoldability
     *            resultSetHoldability
     * @return prepared statement
     * @throws SQLException
     *             if an error occurs
     * 
     * @see java.sql.Connection#prepareStatement(String, int, int, int)
     */
    java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @param sql
     *            statement
     * @param autoGenKeyIndexes
     *            autoGenKeyIndexes
     * @return prepared statement
     * @throws SQLException
     *             if an error occurs
     * @see java.sql.Connection#prepareStatement(String, int[])
     */
    java.sql.PreparedStatement serverPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @param sql
     *            statement
     * @param autoGenKeyColNames
     *            autoGenKeyColNames
     * @return prepared statement
     * @throws SQLException
     *             if an error occurs
     * 
     * @see java.sql.Connection#prepareStatement(String, String[])
     */
    java.sql.PreparedStatement serverPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException;

    /**
     * @param flag
     *            The failedOver flag to set.
     */
    void setFailedOver(boolean flag);

    /**
     * Sets the comment that will be prepended to all statements
     * sent to the server. Do not use slash-star or star-slash tokens
     * in the comment as these will be added by the driver itself.
     * 
     * @param comment
     *            the comment that will be prepended to all statements
     *            sent to the server.
     */
    void setStatementComment(String comment);

    /**
     * Used by MiniAdmin to shutdown a MySQL server
     * 
     * @throws SQLException
     *             if the command can not be issued.
     */
    void shutdownServer() throws SQLException;

    /**
     * Returns the -session- value of 'auto_increment_increment' from the server if it exists,
     * or '1' if not.
     * 
     * @return the -session- value of 'auto_increment_increment'
     */
    int getAutoIncrementIncrement();

    /**
     * Does this connection have the same properties as another?
     * 
     * @param c
     *            connection
     * @return true if has the same properties
     */
    boolean hasSameProperties(JdbcConnection c);

    String getHost();

    String getHostPortPair();

    void setProxy(JdbcConnection proxy);

    /**
     * Is the server this connection is connected to "local" (i.e. same host) as the application?
     * 
     * @return true if the server is "local"
     * @throws SQLException
     *             if an error occurs
     */
    boolean isServerLocal() throws SQLException;

    /**
     * Returns the sql select limit max-rows for this session.
     * 
     * @return int max rows
     */
    int getSessionMaxRows();

    /**
     * Sets the sql select limit max-rows for this session if different from current.
     * 
     * @param max
     *            the new max-rows value to set.
     * @throws SQLException
     *             if a database error occurs issuing the statement that sets the limit.
     */
    void setSessionMaxRows(int max) throws SQLException;

    // **************************
    // moved from MysqlJdbcConnection
    // **************************

    /**
     * Clobbers the physical network connection and marks this connection as closed.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    void abortInternal() throws SQLException;

    boolean isProxySet();

    /**
     * Returns cached metadata (or null if not cached) for the given query, which must match _exactly_.
     * 
     * This method is synchronized by the caller on getMutex(), so if calling this method from internal code
     * in the driver, make sure it's synchronized on the mutex that guards communication with the server.
     * 
     * @param sql
     *            the query that is the key to the cache
     * @return metadata cached for the given SQL, or none if it doesn't
     *         exist.
     */
    CachedResultSetMetaData getCachedMetaData(String sql);

    /**
     * @return Returns the characterSetMetadata.
     */
    String getCharacterSetMetadata();

    java.sql.Statement getMetadataSafeStatement() throws SQLException;

    ServerVersion getServerVersion();

    List<QueryInterceptor> getQueryInterceptorsInstances();

    /**
     * Caches CachedResultSetMetaData that has been placed in the cache using the given SQL as a key.
     * 
     * This method is synchronized by the caller on getMutex(), so if calling this method from internal code
     * in the driver, make sure it's synchronized on the mutex that guards communication with the server.
     * 
     * @param sql
     *            the query that the metadata pertains too.
     * @param cachedMetaData
     *            metadata (if it exists) to populate the cache.
     * @param resultSet
     *            the result set to retreive metadata from, or apply to.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    void initializeResultsMetadataFromCache(String sql, CachedResultSetMetaData cachedMetaData, ResultSetInternalMethods resultSet) throws SQLException;

    void initializeSafeQueryInterceptors() throws SQLException;

    /**
     * Tests to see if the connection is in Read Only Mode.
     * 
     * @param useSessionStatus
     *            in some cases, for example when restoring connection with autoReconnect=true,
     *            we can rely only on saved readOnly state, so use useSessionStatus=false in that case
     * 
     * @return true if the connection is read only
     * @exception SQLException
     *                if a database access error occurs
     */
    boolean isReadOnly(boolean useSessionStatus) throws SQLException;

    void pingInternal(boolean checkForClosedConnection, int timeoutMillis) throws SQLException;

    /**
     * Closes connection and frees resources.
     * 
     * @param calledExplicitly
     *            is this being called from close()
     * @param issueRollback
     *            should a rollback() be issued?
     * @param skipLocalTeardown
     *            if true, driver tries to close connection normally, performing rollbacks,
     *            closing open statements etc; otherwise the force close is performed
     * @param reason
     *            the exception caused this method call
     * @throws SQLException
     *             if an error occurs
     */
    void realClose(boolean calledExplicitly, boolean issueRollback, boolean skipLocalTeardown, Throwable reason) throws SQLException;

    void recachePreparedStatement(JdbcPreparedStatement pstmt) throws SQLException;

    void decachePreparedStatement(JdbcPreparedStatement pstmt) throws SQLException;

    /**
     * Register a Statement instance as open.
     * 
     * @param stmt
     *            the Statement instance to remove
     */
    void registerStatement(JdbcStatement stmt);

    void setReadOnlyInternal(boolean readOnlyFlag) throws SQLException;

    boolean storesLowerCaseTableName();

    void throwConnectionClosedException() throws SQLException;

    /**
     * Remove the given statement from the list of open statements
     * 
     * @param stmt
     *            the Statement instance to remove
     */
    void unregisterStatement(JdbcStatement stmt);

    void unSafeQueryInterceptors() throws SQLException;

    JdbcConnection getMultiHostSafeProxy();

    JdbcConnection getMultiHostParentProxy();

    JdbcConnection getActiveMySQLConnection();

    /*
     * Non standard methods:
     */
    ClientInfoProvider getClientInfoProviderImpl() throws SQLException;

    /**
     * Set current database for this connection.
     * 
     * @param dbName
     *            the database for this connection to use
     * @throws SQLException
     *             if a database access error occurs
     */
    void setDatabase(String dbName) throws SQLException;

    /**
     * Retrieves this connection object's current database name.
     * 
     * @return current database name
     * @throws SQLException
     *             if an error occurs
     */
    String getDatabase() throws SQLException;
}
