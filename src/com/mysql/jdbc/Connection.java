/*
  Copyright (c) 2007, 2014, Oracle and/or its affiliates. All rights reserved.

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
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executor;

import com.mysql.jdbc.log.Log;

/**
 * This interface contains methods that are considered the "vendor extension" to the JDBC API for MySQL's implementation of java.sql.Connection.
 * 
 * For those looking further into the driver implementation, it is not an API that is used for plugability of implementations inside our driver
 * (which is why there are still references to ConnectionImpl throughout the code).
 */
public interface Connection extends java.sql.Connection, ConnectionProperties {

    /**
     * Changes the user on this connection by performing a re-authentication. If
     * authentication fails, the connection will remain under the context of the
     * current user.
     * 
     * @param userName
     *            the username to authenticate with
     * @param newPassword
     *            the password to authenticate with
     * @throws SQLException
     *             if authentication fails, or some other error occurs while
     *             performing the command.
     */
    public abstract void changeUser(String userName, String newPassword) throws SQLException;

    public abstract void clearHasTriedMaster();

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String)
     */
    public abstract java.sql.PreparedStatement clientPrepareStatement(String sql) throws SQLException;

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, int)
     */
    public abstract java.sql.PreparedStatement clientPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException;

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, int, int)
     */
    public abstract java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException;

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, int[])
     */
    public abstract java.sql.PreparedStatement clientPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException;

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, int, int, int)
     */
    public abstract java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException;

    /**
     * Prepares a statement on the client, using client-side emulation
     * (irregardless of the configuration property 'useServerPrepStmts')
     * with the same semantics as the java.sql.Connection.prepareStatement()
     * method with the same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, String[])
     */
    public abstract java.sql.PreparedStatement clientPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException;

    /**
     * Returns the number of statements active on this connection, which
     * haven't been .close()d.
     */
    public abstract int getActiveStatementCount();

    /**
     * Reports how long this connection has been idle.
     * This time (reported in milliseconds) is updated once a query has
     * completed.
     * 
     * @return number of ms that this connection has been idle, 0 if the driver
     *         is busy retrieving results.
     */
    public abstract long getIdleFor();

    /**
     * Returns the log mechanism that should be used to log information from/for
     * this Connection.
     * 
     * @return the Log instance to use for logging messages.
     * @throws SQLException
     *             if an error occurs
     */
    public abstract Log getLog() throws SQLException;

    /**
     * Returns the server's character set
     * 
     * @return the server's character set.
     * @deprecated replaced by <code>Connection.getServerCharset()</code>
     */
    @Deprecated
    public abstract String getServerCharacterEncoding();

    /**
     * Returns the server's character set
     * 
     * @return the server's character set.
     */
    public abstract String getServerCharset();

    /**
     * Returns the TimeZone that represents the configured
     * timezone for the server.
     */
    public abstract TimeZone getServerTimezoneTZ();

    /**
     * Returns the comment that will be prepended to all statements
     * sent to the server.
     * 
     * @return the comment that will be prepended to all statements
     *         sent to the server.
     */
    public abstract String getStatementComment();

    /**
     * Has this connection tried to execute a query on the "master"
     * server (first host in a multiple host list).
     */
    public abstract boolean hasTriedMaster();

    /**
     * Is this connection currently a participant in an XA transaction?
     */
    public abstract boolean isInGlobalTx();

    /**
     * Set the state of being in a global (XA) transaction.
     * 
     * @param flag
     */
    public void setInGlobalTx(boolean flag);

    /**
     * Is this connection connected to the first host in the list if
     * there is a list of servers in the URL?
     * 
     * @return true if this connection is connected to the first in
     *         the list.
     */
    public abstract boolean isMasterConnection();

    /**
     * Is the server in a sql_mode that doesn't allow us to use \\ to escape
     * things?
     * 
     * @return Returns the noBackslashEscapes.
     */
    public abstract boolean isNoBackslashEscapesSet();

    /**
     * Does this connection have the same resource name as the given
     * connection (for XA)?
     * 
     * @param c
     */
    public abstract boolean isSameResource(Connection c);

    /**
     * Is the server configured to use lower-case table names only?
     * 
     * @return true if lower_case_table_names is 'on'
     */
    public abstract boolean lowerCaseTableNames();

    /**
     * Does the server this connection is connected to
     * support unicode?
     */
    public abstract boolean parserKnowsUnicode();

    /**
     * Detect if the connection is still good by sending a ping command
     * to the server.
     * 
     * @throws SQLException
     *             if the ping fails
     */
    public abstract void ping() throws SQLException;

    /**
     * Resets the server-side state of this connection. Doesn't work for MySQL
     * versions older than 4.0.6 or if isParanoid() is set (it will become a
     * no-op in these cases). Usually only used from connection pooling code.
     * 
     * @throws SQLException
     *             if the operation fails while resetting server state.
     */
    public abstract void resetServerState() throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String)
     */
    public abstract java.sql.PreparedStatement serverPrepareStatement(String sql) throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, int)
     */
    public abstract java.sql.PreparedStatement serverPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, int, int)
     */
    public abstract java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, int, int, int)
     */
    public abstract java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, int[])
     */
    public abstract java.sql.PreparedStatement serverPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException;

    /**
     * Prepares a statement on the server (irregardless of the
     * configuration property 'useServerPrepStmts') with the same semantics
     * as the java.sql.Connection.prepareStatement() method with the
     * same argument types.
     * 
     * @see java.sql.Connection#prepareStatement(String, String[])
     */
    public abstract java.sql.PreparedStatement serverPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException;

    /**
     * @param failedOver
     *            The failedOver to set.
     */
    public abstract void setFailedOver(boolean flag);

    /**
     * @param preferSlaveDuringFailover
     *            The preferSlaveDuringFailover to set.
     */
    public abstract void setPreferSlaveDuringFailover(boolean flag);

    /**
     * Sets the comment that will be prepended to all statements
     * sent to the server. Do not use slash-star or star-slash tokens
     * in the comment as these will be added by the driver itself.
     * 
     * @param comment
     *            the comment that will be prepended to all statements
     *            sent to the server.
     */
    public abstract void setStatementComment(String comment);

    /**
     * Used by MiniAdmin to shutdown a MySQL server
     * 
     * @throws SQLException
     *             if the command can not be issued.
     */
    public abstract void shutdownServer() throws SQLException;

    /**
     * Does the server this connection is connected to
     * support quoted isolation levels?
     */
    public abstract boolean supportsIsolationLevel();

    /**
     * Does the server this connection is connected to
     * support quoted identifiers?
     */
    public abstract boolean supportsQuotedIdentifiers();

    /**
     * Does the server this connection is connected to
     * support quoted identifiers?
     */
    public abstract boolean supportsTransactions();

    /**
     * Does the server this connection is connected to
     * meet or exceed the given version?
     */
    public abstract boolean versionMeetsMinimum(int major, int minor, int subminor) throws SQLException;

    public abstract void reportQueryTime(long millisOrNanos);

    public abstract boolean isAbonormallyLongQuery(long millisOrNanos);

    public abstract void initializeExtension(Extension ex) throws SQLException;

    /**
     * Returns the -session- value of 'auto_increment_increment' from the server if it exists,
     * or '1' if not.
     */
    public abstract int getAutoIncrementIncrement();

    /**
     * Does this connection have the same properties as another?
     */
    public boolean hasSameProperties(Connection c);

    /**
     * Returns the parsed and passed in properties for this connection.
     */
    public Properties getProperties();

    public String getHost();

    public void setProxy(MySQLConnection proxy);

    /**
     * Is the server this connection is connected to "local" (i.e. same host) as the application?
     */
    public boolean isServerLocal() throws SQLException;

    int getSessionMaxRows();

    void setSessionMaxRows(int max) throws SQLException;

    // JDBC-4.1
    // until we flip catalog/schema, this is a no-op
    void setSchema(String schema) throws SQLException;

    String getSchema() throws SQLException;

    // JDBC-4.1
    void abort(Executor executor) throws SQLException;

    // JDBC-4.1
    void setNetworkTimeout(Executor executor, final int milliseconds) throws SQLException;

    int getNetworkTimeout() throws SQLException;

    // **************************
    // moved from MySQLConnection
    // **************************

    void abortInternal() throws SQLException;

    void checkClosed() throws SQLException;

    Object getConnectionMutex();
}
