/*
  Copyright (c) 2007, 2016, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.api.jdbc;

import java.sql.SQLException;
import java.util.List;
import java.util.Timer;

import com.mysql.cj.api.MysqlConnection;
import com.mysql.cj.api.jdbc.interceptors.StatementInterceptor;
import com.mysql.cj.api.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.core.ServerVersion;
import com.mysql.cj.jdbc.ServerPreparedStatement;
import com.mysql.cj.jdbc.StatementImpl;
import com.mysql.cj.jdbc.result.CachedResultSetMetaData;
import com.mysql.cj.mysqla.MysqlaSession;

/**
 * This interface contains methods that are considered the "vendor extension" to the JDBC API for MySQL's implementation of java.sql.Connection.
 * 
 * For those looking further into the driver implementation, it is not an API that is used for plugability of implementations inside our driver
 * (which is why there are still references to ConnectionImpl throughout the code).
 */
public interface JdbcConnection extends java.sql.Connection, MysqlConnection {

    public JdbcPropertySet getPropertySet();

    MysqlaSession getSession();

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
     * @see java.sql.Connection#prepareStatement(String)
     */
    java.sql.PreparedStatement clientPrepareStatement(String sql) throws SQLException;

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, int)
     */
    java.sql.PreparedStatement clientPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException;

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
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
     * @see java.sql.Connection#prepareStatement(String, int[])
     */
    java.sql.PreparedStatement clientPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException;

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
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
     * @see java.sql.Connection#prepareStatement(String, String[])
     */
    java.sql.PreparedStatement clientPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException;

    /**
     * Returns the number of statements active on this connection, which
     * haven't been .close()d.
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
     * Has this connection tried to execute a query on the "master"
     * server (first host in a multiple host list).
     */
    @Deprecated
    boolean hasTriedMaster();

    /**
     * Is this connection currently a participant in an XA transaction?
     */
    boolean isInGlobalTx();

    /**
     * Set the state of being in a global (XA) transaction.
     * 
     * @param flag
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
    boolean isMasterConnection();

    /**
     * Is the server in a sql_mode that doesn't allow us to use \\ to escape
     * things?
     * 
     * @return Returns the noBackslashEscapes.
     */
    boolean isNoBackslashEscapesSet();

    /**
     * Does this connection have the same resource name as the given
     * connection (for XA)?
     * 
     * @param c
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
     * Resets the server-side state of this connection. Doesn't work for MySQL
     * versions older than 4.0.6 or if isParanoid() is set (it will become a
     * no-op in these cases). Usually only used from connection pooling code.
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
     * @see java.sql.Connection#prepareStatement(String)
     */
    java.sql.PreparedStatement serverPrepareStatement(String sql) throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, int)
     */
    java.sql.PreparedStatement serverPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
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
     * @see java.sql.Connection#prepareStatement(String, int, int, int)
     */
    java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, int[])
     */
    java.sql.PreparedStatement serverPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, String[])
     */
    java.sql.PreparedStatement serverPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException;

    /**
     * @param failedOver
     *            The failedOver to set.
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

    void reportQueryTime(long millisOrNanos);

    boolean isAbonormallyLongQuery(long millisOrNanos);

    /**
     * Returns the -session- value of 'auto_increment_increment' from the server if it exists,
     * or '1' if not.
     */
    int getAutoIncrementIncrement();

    /**
     * Does this connection have the same properties as another?
     */
    boolean hasSameProperties(JdbcConnection c);

    String getHost();

    String getHostPortPair();

    void setProxy(JdbcConnection proxy);

    /**
     * Is the server this connection is connected to "local" (i.e. same host) as the application?
     */
    boolean isServerLocal() throws SQLException;

    int getSessionMaxRows();

    void setSessionMaxRows(int max) throws SQLException;

    // until we flip catalog/schema, this is a no-op
    void setSchema(String schema) throws SQLException;

    // **************************
    // moved from MysqlJdbcConnection
    // **************************

    void abortInternal() throws SQLException;

    void checkClosed();

    boolean isProxySet();

    JdbcConnection duplicate() throws SQLException;

    ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, PacketPayload packet, boolean streamResults, String catalog,
            ColumnDefinition cachedMetadata) throws SQLException;

    ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, PacketPayload packet, boolean streamResults, String catalog,
            ColumnDefinition cachedMetadata, boolean isBatch) throws SQLException;

    StringBuilder generateConnectionCommentBlock(StringBuilder buf);

    CachedResultSetMetaData getCachedMetaData(String sql);

    Timer getCancelTimer();

    String getCharacterSetMetadata();

    java.sql.Statement getMetadataSafeStatement() throws SQLException;

    boolean getRequiresEscapingEncoder();

    ServerVersion getServerVersion();

    List<StatementInterceptor> getStatementInterceptorsInstances();

    void incrementNumberOfPreparedExecutes();

    void incrementNumberOfPrepares();

    void incrementNumberOfResultSetsCreated();

    void initializeResultsMetadataFromCache(String sql, CachedResultSetMetaData cachedMetaData, ResultSetInternalMethods resultSet) throws SQLException;

    void initializeSafeStatementInterceptors() throws SQLException;

    boolean isReadInfoMsgEnabled();

    boolean isReadOnly(boolean useSessionStatus) throws SQLException;

    void pingInternal(boolean checkForClosedConnection, int timeoutMillis) throws SQLException;

    void realClose(boolean calledExplicitly, boolean issueRollback, boolean skipLocalTeardown, Throwable reason) throws SQLException;

    void recachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException;

    void decachePreparedStatement(ServerPreparedStatement pstmt) throws SQLException;

    void registerQueryExecutionTime(long queryTimeMs);

    void registerStatement(Statement stmt);

    void reportNumberOfTablesAccessed(int numTablesAccessed);

    void setReadInfoMsgEnabled(boolean flag);

    void setReadOnlyInternal(boolean readOnlyFlag) throws SQLException;

    boolean storesLowerCaseTableName();

    void throwConnectionClosedException() throws SQLException;

    void transactionBegun() throws SQLException;

    void transactionCompleted() throws SQLException;

    void unregisterStatement(Statement stmt);

    void unSafeStatementInterceptors() throws SQLException;

    boolean useAnsiQuotedIdentifiers();

    JdbcConnection getMultiHostSafeProxy();

    /*
     * Non standard methods:
     */
    ClientInfoProvider getClientInfoProviderImpl() throws SQLException;
}
