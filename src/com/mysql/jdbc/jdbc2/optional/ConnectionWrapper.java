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

package com.mysql.jdbc.jdbc2.optional;

import java.lang.reflect.Proxy;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import java.util.concurrent.Executor;

import com.mysql.cj.api.Extension;
import com.mysql.cj.api.ProfilerEventHandler;
import com.mysql.cj.api.exception.ExceptionInterceptor;
import com.mysql.cj.api.log.Log;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exception.ConnectionIsClosedException;
import com.mysql.cj.core.exception.ExceptionFactory;
import com.mysql.cj.core.exception.MysqlErrorNumbers;
import com.mysql.cj.mysqla.MysqlaSession;
import com.mysql.cj.mysqla.io.Buffer;
import com.mysql.cj.mysqla.io.MysqlaProtocol;
import com.mysql.jdbc.Field;
import com.mysql.jdbc.JdbcConnection;
import com.mysql.jdbc.JdbcConnectionProperties;
import com.mysql.jdbc.JdbcPropertySet;
import com.mysql.jdbc.MysqlJdbcConnection;
import com.mysql.jdbc.ResultSetInternalMethods;
import com.mysql.jdbc.StatementImpl;
import com.mysql.jdbc.exceptions.SQLError;

/**
 * This class serves as a wrapper for the org.gjt.mm.mysql.jdbc2.Connection class. It is returned to the application server which may wrap it again and then
 * return it to the application client in response to dataSource.getConnection().
 * 
 * All method invocations are forwarded to org.gjt.mm.mysql.jdbc2.Connection unless the close method was previously called, in which case a sqlException is
 * thrown. The close method performs a 'logical close' on the connection.
 * 
 * All sqlExceptions thrown by the physical connection are intercepted and sent to connectionEvent listeners before being thrown to client.
 */
public class ConnectionWrapper extends WrapperBase implements JdbcConnection {
    protected JdbcConnection mc = null;

    private String invalidHandleStr = "Logical handle no longer valid";

    private boolean closed;

    private boolean isForXa;

    protected static ConnectionWrapper getInstance(MysqlPooledConnection mysqlPooledConnection, JdbcConnection mysqlConnection, boolean forXa)
            throws SQLException {
        return new ConnectionWrapper(mysqlPooledConnection, mysqlConnection, forXa);
    }

    /**
     * Construct a new LogicalHandle and set instance variables
     * 
     * @param mysqlPooledConnection
     *            reference to object that instantiated this object
     * @param mysqlConnection
     *            physical connection to db
     * 
     * @throws SQLException
     *             if an error occurs.
     */
    public ConnectionWrapper(MysqlPooledConnection mysqlPooledConnection, JdbcConnection mysqlConnection, boolean forXa) throws SQLException {
        super(mysqlPooledConnection);

        this.mc = mysqlConnection;
        this.closed = false;
        this.isForXa = forXa;

        if (this.isForXa) {
            setInGlobalTx(false);
        }
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#setAutoCommit
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {

        if (autoCommit && isInGlobalTx()) {
            throw SQLError.createSQLException(Messages.getString("ConnectionWrapper.0"), SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION,
                    MysqlErrorNumbers.ER_XA_RMERR, this.exceptionInterceptor);
        }

        try {
            this.mc.setAutoCommit(autoCommit);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#getAutoCommit()
     */
    public boolean getAutoCommit() throws SQLException {

        try {
            return this.mc.getAutoCommit();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return false; // we don't reach this code, compiler can't tell
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#setCatalog()
     */
    public void setCatalog(String catalog) throws SQLException {

        try {
            this.mc.setCatalog(catalog);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @return the current catalog
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public String getCatalog() throws SQLException {

        try {
            return this.mc.getCatalog();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#isClosed()
     */
    public boolean isClosed() throws SQLException {
        return (this.closed || this.mc.isClosed());
    }

    public boolean isMasterConnection() {
        return this.mc.isMasterConnection();
    }

    /**
     * @see JdbcConnection#setHoldability(int)
     */
    public void setHoldability(int arg0) throws SQLException {

        try {
            this.mc.setHoldability(arg0);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }
    }

    /**
     * @see JdbcConnection#getHoldability()
     */
    public int getHoldability() throws SQLException {

        try {
            return this.mc.getHoldability();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return Statement.CLOSE_CURRENT_RESULT; // we don't reach this code,
        // compiler can't tell
    }

    /**
     * Allows clients to determine how long this connection has been idle.
     * 
     * @return how long the connection has been idle.
     */
    public long getIdleFor() {
        return this.mc.getIdleFor();
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @return a metadata instance
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public java.sql.DatabaseMetaData getMetaData() throws SQLException {

        try {
            return this.mc.getMetaData();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#setReadOnly()
     */
    public void setReadOnly(boolean readOnly) throws SQLException {

        try {
            this.mc.setReadOnly(readOnly);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#isReadOnly()
     */
    public boolean isReadOnly() throws SQLException {

        try {
            return this.mc.isReadOnly();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return false; // we don't reach this code, compiler can't tell
    }

    /**
     * @see JdbcConnection#setSavepoint()
     */
    public java.sql.Savepoint setSavepoint() throws SQLException {

        if (isInGlobalTx()) {
            throw SQLError.createSQLException(Messages.getString("ConnectionWrapper.0"), SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION,
                    MysqlErrorNumbers.ER_XA_RMERR, this.exceptionInterceptor);
        }

        try {
            return this.mc.setSavepoint();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * @see JdbcConnection#setSavepoint(String)
     */
    public java.sql.Savepoint setSavepoint(String arg0) throws SQLException {

        if (isInGlobalTx()) {
            throw SQLError.createSQLException(Messages.getString("ConnectionWrapper.0"), SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION,
                    MysqlErrorNumbers.ER_XA_RMERR, this.exceptionInterceptor);
        }

        try {
            return this.mc.setSavepoint(arg0);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#setTransactionIsolation()
     */
    public void setTransactionIsolation(int level) throws SQLException {

        try {
            this.mc.setTransactionIsolation(level);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#getTransactionIsolation()
     */
    public int getTransactionIsolation() throws SQLException {

        try {
            return this.mc.getTransactionIsolation();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return TRANSACTION_REPEATABLE_READ; // we don't reach this code,
        // compiler can't tell
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#getTypeMap()
     */
    public java.util.Map<String, Class<?>> getTypeMap() throws SQLException {

        try {
            return this.mc.getTypeMap();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#getWarnings
     */
    public java.sql.SQLWarning getWarnings() throws SQLException {

        try {
            return this.mc.getWarnings();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public void clearWarnings() throws SQLException {

        try {
            this.mc.clearWarnings();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }
    }

    /**
     * The physical connection is not actually closed. the physical connection
     * is closed when the application server calls
     * mysqlPooledConnection.close(). this object is de-referenced by the pooled
     * connection each time mysqlPooledConnection.getConnection() is called by
     * app server.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public void close() throws SQLException {
        try {
            close(true);
        } finally {
            this.unwrappedInterfaces = null;
        }
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @throws SQLException
     *             if an error occurs
     */
    public void commit() throws SQLException {

        if (isInGlobalTx()) {
            throw SQLError.createSQLException(Messages.getString("ConnectionWrapper.1"), SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION,
                    MysqlErrorNumbers.ER_XA_RMERR, this.exceptionInterceptor);
        }

        try {
            this.mc.commit();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#createStatement()
     */
    public java.sql.Statement createStatement() throws SQLException {

        try {
            return StatementWrapper.getInstance(this, this.pooledConnection, this.mc.createStatement());
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#createStatement()
     */
    public java.sql.Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {

        try {
            return StatementWrapper.getInstance(this, this.pooledConnection, this.mc.createStatement(resultSetType, resultSetConcurrency));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * @see JdbcConnection#createStatement(int, int, int)
     */
    public java.sql.Statement createStatement(int arg0, int arg1, int arg2) throws SQLException {

        try {
            return StatementWrapper.getInstance(this, this.pooledConnection, this.mc.createStatement(arg0, arg1, arg2));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#nativeSQL()
     */
    public String nativeSQL(String sql) throws SQLException {

        try {
            return this.mc.nativeSQL(sql);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#prepareCall()
     */
    public java.sql.CallableStatement prepareCall(String sql) throws SQLException {

        try {
            return CallableStatementWrapper.getInstance(this, this.pooledConnection, this.mc.prepareCall(sql));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#prepareCall()
     */
    public java.sql.CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {

        try {
            return CallableStatementWrapper.getInstance(this, this.pooledConnection, this.mc.prepareCall(sql, resultSetType, resultSetConcurrency));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * @see JdbcConnection#prepareCall(String, int, int, int)
     */
    public java.sql.CallableStatement prepareCall(String arg0, int arg1, int arg2, int arg3) throws SQLException {

        try {
            return CallableStatementWrapper.getInstance(this, this.pooledConnection, this.mc.prepareCall(arg0, arg1, arg2, arg3));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    public java.sql.PreparedStatement clientPrepare(String sql) throws SQLException {

        try {
            return new PreparedStatementWrapper(this, this.pooledConnection, this.mc.clientPrepareStatement(sql));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    public java.sql.PreparedStatement clientPrepare(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {

        try {
            return new PreparedStatementWrapper(this, this.pooledConnection, this.mc.clientPrepareStatement(sql, resultSetType, resultSetConcurrency));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#prepareStatement()
     */
    public java.sql.PreparedStatement prepareStatement(String sql) throws SQLException {

        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.prepareStatement(sql));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#prepareStatement()
     */
    public java.sql.PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {

        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.prepareStatement(sql, resultSetType, resultSetConcurrency));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * @see JdbcConnection#prepareStatement(String, int, int, int)
     */
    public java.sql.PreparedStatement prepareStatement(String arg0, int arg1, int arg2, int arg3) throws SQLException {

        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.prepareStatement(arg0, arg1, arg2, arg3));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * @see JdbcConnection#prepareStatement(String, int)
     */
    public java.sql.PreparedStatement prepareStatement(String arg0, int arg1) throws SQLException {

        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.prepareStatement(arg0, arg1));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * @see JdbcConnection#prepareStatement(String, int[])
     */
    public java.sql.PreparedStatement prepareStatement(String arg0, int[] arg1) throws SQLException {

        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.prepareStatement(arg0, arg1));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * @see JdbcConnection#prepareStatement(String, String[])
     */
    public java.sql.PreparedStatement prepareStatement(String arg0, String[] arg1) throws SQLException {

        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.prepareStatement(arg0, arg1));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // we don't reach this code, compiler can't tell
    }

    /**
     * @see JdbcConnection#releaseSavepoint(Savepoint)
     */
    public void releaseSavepoint(Savepoint arg0) throws SQLException {

        try {
            this.mc.releaseSavepoint(arg0);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }
    }

    /**
     * Passes call to method on physical connection instance. Notifies listeners
     * of any caught exceptions before re-throwing to client.
     * 
     * @see java.sql.Connection#rollback()
     */
    public void rollback() throws SQLException {

        if (isInGlobalTx()) {
            throw SQLError.createSQLException(Messages.getString("ConnectionWrapper.2"), SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION,
                    MysqlErrorNumbers.ER_XA_RMERR, this.exceptionInterceptor);
        }

        try {
            this.mc.rollback();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }
    }

    /**
     * @see JdbcConnection#rollback(Savepoint)
     */
    public void rollback(Savepoint arg0) throws SQLException {

        if (isInGlobalTx()) {
            throw SQLError.createSQLException(Messages.getString("ConnectionWrapper.2"), SQLError.SQL_STATE_INVALID_TRANSACTION_TERMINATION,
                    MysqlErrorNumbers.ER_XA_RMERR, this.exceptionInterceptor);
        }

        try {
            this.mc.rollback(arg0);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }
    }

    public boolean isSameResource(com.mysql.jdbc.JdbcConnection c) {
        if (c instanceof ConnectionWrapper) {
            return this.mc.isSameResource(((ConnectionWrapper) c).mc);
        }
        return this.mc.isSameResource(c);
    }

    protected void close(boolean fireClosedEvent) throws SQLException {
        synchronized (this.pooledConnection) {
            if (this.closed) {
                return;
            }

            if (!isInGlobalTx() && this.mc.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_rollbackOnPooledClose).getValue()
                    && !this.getAutoCommit()) {
                rollback();
            }

            if (fireClosedEvent) {
                this.pooledConnection.callConnectionEventListeners(MysqlPooledConnection.CONNECTION_CLOSED_EVENT, null);
            }

            // set closed status to true so that if application client tries to make additional calls a sqlException will be thrown. The physical connection is
            // re-used by the pooled connection each time getConnection is called.
            this.closed = true;
        }
    }

    public void checkClosed() {
        if (this.closed) {
            throw ExceptionFactory.createException(ConnectionIsClosedException.class, this.invalidHandleStr, this.exceptionInterceptor);
        }
    }

    public boolean isInGlobalTx() {
        return this.mc.isInGlobalTx();
    }

    public void setInGlobalTx(boolean flag) {
        this.mc.setInGlobalTx(flag);
    }

    public void ping() throws SQLException {
        if (this.mc != null) {
            this.mc.ping();
        }
    }

    public void changeUser(String userName, String newPassword) throws SQLException {

        try {
            this.mc.changeUser(userName, newPassword);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }
    }

    public void clearHasTriedMaster() {
        this.mc.clearHasTriedMaster();
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql) throws SQLException {

        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.clientPrepareStatement(sql));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.clientPrepareStatement(sql, autoGenKeyIndex));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.clientPrepareStatement(sql, resultSetType, resultSetConcurrency));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection,
                    this.mc.clientPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.clientPrepareStatement(sql, autoGenKeyIndexes));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    public java.sql.PreparedStatement clientPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.clientPrepareStatement(sql, autoGenKeyColNames));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    public int getActiveStatementCount() {
        return this.mc.getActiveStatementCount();
    }

    public Log getLog() {
        return this.mc.getLog();
    }

    /**
     * @deprecated replaced by <code>getServerCharset()</code>
     */
    @Deprecated
    public String getServerCharacterEncoding() {
        return getServerCharset();
    }

    public String getServerCharset() {
        return this.mc.getServerCharset();
    }

    public String getStatementComment() {
        return this.mc.getStatementComment();
    }

    public boolean hasTriedMaster() {
        return this.mc.hasTriedMaster();
    }

    public boolean isAbonormallyLongQuery(long millisOrNanos) {
        return this.mc.isAbonormallyLongQuery(millisOrNanos);
    }

    public boolean isNoBackslashEscapesSet() {
        return this.mc.isNoBackslashEscapesSet();
    }

    public boolean lowerCaseTableNames() {
        return this.mc.lowerCaseTableNames();
    }

    public void reportQueryTime(long millisOrNanos) {
        this.mc.reportQueryTime(millisOrNanos);
    }

    public void resetServerState() throws SQLException {

        try {
            this.mc.resetServerState();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql) throws SQLException {

        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.serverPrepareStatement(sql));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int autoGenKeyIndex) throws SQLException {
        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.serverPrepareStatement(sql, autoGenKeyIndex));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.serverPrepareStatement(sql, resultSetType, resultSetConcurrency));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
            throws SQLException {
        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection,
                    this.mc.serverPrepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, int[] autoGenKeyIndexes) throws SQLException {
        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.serverPrepareStatement(sql, autoGenKeyIndexes));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    public java.sql.PreparedStatement serverPrepareStatement(String sql, String[] autoGenKeyColNames) throws SQLException {
        try {
            return PreparedStatementWrapper.getInstance(this, this.pooledConnection, this.mc.serverPrepareStatement(sql, autoGenKeyColNames));
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null;
    }

    public void setFailedOver(boolean flag) {
        this.mc.setFailedOver(flag);

    }

    public void setStatementComment(String comment) {
        this.mc.setStatementComment(comment);

    }

    public void shutdownServer() throws SQLException {

        try {
            this.mc.shutdownServer();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

    }

    public boolean versionMeetsMinimum(int major, int minor, int subminor) {

        return this.mc.versionMeetsMinimum(major, minor, subminor);
    }

    public boolean getAutoSlowLog() {
        return this.mc.getAutoSlowLog();
    }

    public boolean getBlobsAreStrings() {
        return this.mc.getBlobsAreStrings();
    }

    public String getClientInfoProvider() {
        return this.mc.getClientInfoProvider();
    }

    public String getConnectionLifecycleInterceptors() {
        return this.mc.getConnectionLifecycleInterceptors();
    }

    public boolean getEnableQueryTimeouts() {
        return this.mc.getEnableQueryTimeouts();
    }

    public boolean getFunctionsNeverReturnBlobs() {
        return this.mc.getFunctionsNeverReturnBlobs();
    }

    public boolean getGenerateSimpleParameterMetadata() {
        return this.mc.getGenerateSimpleParameterMetadata();
    }

    public boolean getIncludeInnodbStatusInDeadlockExceptions() {
        return this.mc.getIncludeInnodbStatusInDeadlockExceptions();
    }

    public String getLargeRowSizeThreshold() {
        return this.mc.getLargeRowSizeThreshold();
    }

    public String getHaLoadBalanceStrategy() {
        return this.mc.getHaLoadBalanceStrategy();
    }

    public String getLocalSocketAddress() {
        return this.mc.getLocalSocketAddress();
    }

    public boolean getLogXaCommands() {
        return this.mc.getLogXaCommands();
    }

    public int getNetTimeoutForStreamingResults() {
        return this.mc.getNetTimeoutForStreamingResults();
    }

    public boolean getNoAccessToProcedureBodies() {
        return this.mc.getNoAccessToProcedureBodies();
    }

    public boolean getPadCharsWithSpace() {
        return this.mc.getPadCharsWithSpace();
    }

    public boolean getPinGlobalTxToPhysicalConnection() {
        return this.mc.getPinGlobalTxToPhysicalConnection();
    }

    public boolean getPopulateInsertRowWithDefaultValues() {
        return this.mc.getPopulateInsertRowWithDefaultValues();
    }

    public int getResultSetSizeThreshold() {
        return this.mc.getResultSetSizeThreshold();
    }

    public boolean getRewriteBatchedStatements() {
        return this.mc.getRewriteBatchedStatements();
    }

    public long getSlowQueryThresholdNanos() {
        return this.mc.getSlowQueryThresholdNanos();
    }

    public String getStatementInterceptors() {
        return this.mc.getStatementInterceptors();
    }

    public boolean getTcpKeepAlive() {
        return this.mc.getTcpKeepAlive();
    }

    public boolean getTcpNoDelay() {
        return this.mc.getTcpNoDelay();
    }

    public int getTcpRcvBuf() {
        return this.mc.getTcpRcvBuf();
    }

    public int getTcpSndBuf() {
        return this.mc.getTcpSndBuf();
    }

    public int getTcpTrafficClass() {
        return this.mc.getTcpTrafficClass();
    }

    public boolean getTreatUtilDateAsTimestamp() {
        return this.mc.getTreatUtilDateAsTimestamp();
    }

    public boolean getUseBlobToStoreUTF8OutsideBMP() {
        return this.mc.getUseBlobToStoreUTF8OutsideBMP();
    }

    public String getUseConfigs() {
        return this.mc.getUseConfigs();
    }

    public boolean getUseDirectRowUnpack() {
        return this.mc.getUseDirectRowUnpack();
    }

    public boolean getUseDynamicCharsetInfo() {
        return this.mc.getUseDynamicCharsetInfo();
    }

    public boolean getUseNanosForElapsedTime() {
        return this.mc.getUseNanosForElapsedTime();
    }

    public boolean getUseOldAliasMetadataBehavior() {
        return this.mc.getUseOldAliasMetadataBehavior();
    }

    public String getUtf8OutsideBmpExcludedColumnNamePattern() {
        return this.mc.getUtf8OutsideBmpExcludedColumnNamePattern();
    }

    public String getUtf8OutsideBmpIncludedColumnNamePattern() {
        return this.mc.getUtf8OutsideBmpIncludedColumnNamePattern();
    }

    public void setAutoSlowLog(boolean flag) {
        this.mc.setAutoSlowLog(flag);
    }

    public void setBlobsAreStrings(boolean flag) {
        this.mc.setBlobsAreStrings(flag);
    }

    public void setClientInfoProvider(String classname) {
        this.mc.setClientInfoProvider(classname);
    }

    public void setConnectionLifecycleInterceptors(String interceptors) {
        this.mc.setConnectionLifecycleInterceptors(interceptors);
    }

    public void setEnableQueryTimeouts(boolean flag) {
        this.mc.setEnableQueryTimeouts(flag);
    }

    public void setFunctionsNeverReturnBlobs(boolean flag) {
        this.mc.setFunctionsNeverReturnBlobs(flag);
    }

    public void setGenerateSimpleParameterMetadata(boolean flag) {
        this.mc.setGenerateSimpleParameterMetadata(flag);
    }

    public void setIncludeInnodbStatusInDeadlockExceptions(boolean flag) {
        this.mc.setIncludeInnodbStatusInDeadlockExceptions(flag);
    }

    public void setLargeRowSizeThreshold(String value) throws SQLException {
        this.mc.setLargeRowSizeThreshold(value);
    }

    public void setHaLoadBalanceStrategy(String strategy) {
        this.mc.setHaLoadBalanceStrategy(strategy);
    }

    public void setLocalSocketAddress(String address) {
        this.mc.setLocalSocketAddress(address);
    }

    public void setLogXaCommands(boolean flag) {
        this.mc.setLogXaCommands(flag);
    }

    public void setNetTimeoutForStreamingResults(int value) throws SQLException {
        this.mc.setNetTimeoutForStreamingResults(value);
    }

    public void setNoAccessToProcedureBodies(boolean flag) {
        this.mc.setNoAccessToProcedureBodies(flag);
    }

    public void setPadCharsWithSpace(boolean flag) {
        this.mc.setPadCharsWithSpace(flag);
    }

    public void setPinGlobalTxToPhysicalConnection(boolean flag) {
        this.mc.setPinGlobalTxToPhysicalConnection(flag);
    }

    public void setPopulateInsertRowWithDefaultValues(boolean flag) {
        this.mc.setPopulateInsertRowWithDefaultValues(flag);
    }

    public void setResultSetSizeThreshold(int threshold) throws SQLException {
        this.mc.setResultSetSizeThreshold(threshold);
    }

    public void setRewriteBatchedStatements(boolean flag) {
        this.mc.setRewriteBatchedStatements(flag);
    }

    public void setSlowQueryThresholdNanos(long nanos) throws SQLException {
        this.mc.setSlowQueryThresholdNanos(nanos);
    }

    public void setStatementInterceptors(String value) {
        this.mc.setStatementInterceptors(value);
    }

    public void setTcpKeepAlive(boolean flag) {
        this.mc.setTcpKeepAlive(flag);
    }

    public void setTcpNoDelay(boolean flag) {
        this.mc.setTcpNoDelay(flag);
    }

    public void setTcpRcvBuf(int bufSize) throws SQLException {
        this.mc.setTcpRcvBuf(bufSize);
    }

    public void setTcpSndBuf(int bufSize) throws SQLException {
        this.mc.setTcpSndBuf(bufSize);
    }

    public void setTcpTrafficClass(int classFlags) throws SQLException {
        this.mc.setTcpTrafficClass(classFlags);
    }

    public void setTreatUtilDateAsTimestamp(boolean flag) {
        this.mc.setTreatUtilDateAsTimestamp(flag);
    }

    public void setUseBlobToStoreUTF8OutsideBMP(boolean flag) {
        this.mc.setUseBlobToStoreUTF8OutsideBMP(flag);
    }

    public void setUseConfigs(String configs) {
        this.mc.setUseConfigs(configs);
    }

    public void setUseDirectRowUnpack(boolean flag) {
        this.mc.setUseDirectRowUnpack(flag);
    }

    public void setUseDynamicCharsetInfo(boolean flag) {
        this.mc.setUseDynamicCharsetInfo(flag);
    }

    public void setUseNanosForElapsedTime(boolean flag) {
        this.mc.setUseNanosForElapsedTime(flag);
    }

    public void setUseOldAliasMetadataBehavior(boolean flag) {
        this.mc.setUseOldAliasMetadataBehavior(flag);
    }

    public void setUtf8OutsideBmpExcludedColumnNamePattern(String regexPattern) {
        this.mc.setUtf8OutsideBmpExcludedColumnNamePattern(regexPattern);
    }

    public void setUtf8OutsideBmpIncludedColumnNamePattern(String regexPattern) {
        this.mc.setUtf8OutsideBmpIncludedColumnNamePattern(regexPattern);
    }

    public void initializeExtension(Extension ex) {
        this.mc.initializeExtension(ex);
    }

    public int getSelfDestructOnPingMaxOperations() {
        return this.mc.getSelfDestructOnPingMaxOperations();
    }

    public int getSelfDestructOnPingSecondsLifetime() {
        return this.mc.getSelfDestructOnPingSecondsLifetime();
    }

    public void setSelfDestructOnPingMaxOperations(int maxOperations) throws SQLException {
        this.mc.setSelfDestructOnPingMaxOperations(maxOperations);
    }

    public void setSelfDestructOnPingSecondsLifetime(int seconds) throws SQLException {
        this.mc.setSelfDestructOnPingSecondsLifetime(seconds);
    }

    public boolean getUseColumnNamesInFindColumn() {
        return this.mc.getUseColumnNamesInFindColumn();
    }

    public void setUseColumnNamesInFindColumn(boolean flag) {
        this.mc.setUseColumnNamesInFindColumn(flag);
    }

    public boolean getUseLocalTransactionState() {
        return this.mc.getUseLocalTransactionState();
    }

    public void setUseLocalTransactionState(boolean flag) {
        this.mc.setUseLocalTransactionState(flag);
    }

    public boolean getCompensateOnDuplicateKeyUpdateCounts() {
        return this.mc.getCompensateOnDuplicateKeyUpdateCounts();
    }

    public void setCompensateOnDuplicateKeyUpdateCounts(boolean flag) {
        this.mc.setCompensateOnDuplicateKeyUpdateCounts(flag);
    }

    public boolean getUseAffectedRows() {
        return this.mc.getUseAffectedRows();
    }

    public void setUseAffectedRows(boolean flag) {
        this.mc.setUseAffectedRows(flag);
    }

    public int getAutoIncrementIncrement() {
        return this.mc.getAutoIncrementIncrement();
    }

    public int getLoadBalanceBlacklistTimeout() {
        return this.mc.getLoadBalanceBlacklistTimeout();
    }

    public void setLoadBalanceBlacklistTimeout(int loadBalanceBlacklistTimeout) throws SQLException {
        this.mc.setLoadBalanceBlacklistTimeout(loadBalanceBlacklistTimeout);
    }

    public int getLoadBalancePingTimeout() {
        return this.mc.getLoadBalancePingTimeout();
    }

    public void setLoadBalancePingTimeout(int loadBalancePingTimeout) throws SQLException {
        this.mc.setLoadBalancePingTimeout(loadBalancePingTimeout);
    }

    public boolean getLoadBalanceValidateConnectionOnSwapServer() {
        return this.mc.getLoadBalanceValidateConnectionOnSwapServer();
    }

    public void setLoadBalanceValidateConnectionOnSwapServer(boolean loadBalanceValidateConnectionOnSwapServer) {
        this.mc.setLoadBalanceValidateConnectionOnSwapServer(loadBalanceValidateConnectionOnSwapServer);
    }

    public void setRetriesAllDown(int retriesAllDown) throws SQLException {
        this.mc.setRetriesAllDown(retriesAllDown);
    }

    public int getRetriesAllDown() {
        return this.mc.getRetriesAllDown();
    }

    public ExceptionInterceptor getExceptionInterceptor() {
        return this.pooledConnection.getExceptionInterceptor();
    }

    public String getExceptionInterceptors() {
        return this.mc.getExceptionInterceptors();
    }

    public void setExceptionInterceptors(String exceptionInterceptors) {
        this.mc.setExceptionInterceptors(exceptionInterceptors);
    }

    public boolean getQueryTimeoutKillsConnection() {
        return this.mc.getQueryTimeoutKillsConnection();
    }

    public void setQueryTimeoutKillsConnection(boolean queryTimeoutKillsConnection) {
        this.mc.setQueryTimeoutKillsConnection(queryTimeoutKillsConnection);
    }

    public boolean hasSameProperties(JdbcConnection c) {
        return this.mc.hasSameProperties(c);
    }

    public Properties getProperties() {
        return this.mc.getProperties();
    }

    public String getHost() {
        return this.mc.getHost();
    }

    public void setProxy(MysqlJdbcConnection conn) {
        this.mc.setProxy(conn);
    }

    public int getMaxAllowedPacket() {
        return this.mc.getMaxAllowedPacket();
    }

    public String getLoadBalanceConnectionGroup() {
        return this.mc.getLoadBalanceConnectionGroup();
    }

    public String getLoadBalanceExceptionChecker() {
        return this.mc.getLoadBalanceExceptionChecker();
    }

    public String getLoadBalanceSQLExceptionSubclassFailover() {
        return this.mc.getLoadBalanceSQLExceptionSubclassFailover();
    }

    public String getLoadBalanceSQLStateFailover() {
        return this.mc.getLoadBalanceSQLStateFailover();
    }

    public void setLoadBalanceConnectionGroup(String loadBalanceConnectionGroup) {
        this.mc.setLoadBalanceConnectionGroup(loadBalanceConnectionGroup);

    }

    public void setLoadBalanceExceptionChecker(String loadBalanceExceptionChecker) {
        this.mc.setLoadBalanceExceptionChecker(loadBalanceExceptionChecker);

    }

    public void setLoadBalanceSQLExceptionSubclassFailover(String loadBalanceSQLExceptionSubclassFailover) {
        this.mc.setLoadBalanceSQLExceptionSubclassFailover(loadBalanceSQLExceptionSubclassFailover);

    }

    public void setLoadBalanceSQLStateFailover(String loadBalanceSQLStateFailover) {
        this.mc.setLoadBalanceSQLStateFailover(loadBalanceSQLStateFailover);

    }

    public String getLoadBalanceAutoCommitStatementRegex() {
        return this.mc.getLoadBalanceAutoCommitStatementRegex();
    }

    public int getLoadBalanceAutoCommitStatementThreshold() {
        return this.mc.getLoadBalanceAutoCommitStatementThreshold();
    }

    public void setLoadBalanceAutoCommitStatementRegex(String loadBalanceAutoCommitStatementRegex) {
        this.mc.setLoadBalanceAutoCommitStatementRegex(loadBalanceAutoCommitStatementRegex);

    }

    public void setLoadBalanceAutoCommitStatementThreshold(int loadBalanceAutoCommitStatementThreshold) throws SQLException {
        this.mc.setLoadBalanceAutoCommitStatementThreshold(loadBalanceAutoCommitStatementThreshold);

    }

    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

        try {
            this.mc.setTypeMap(map);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }
    }

    public boolean getIncludeThreadDumpInDeadlockExceptions() {
        return this.mc.getIncludeThreadDumpInDeadlockExceptions();
    }

    public void setIncludeThreadDumpInDeadlockExceptions(boolean flag) {
        this.mc.setIncludeThreadDumpInDeadlockExceptions(flag);

    }

    public boolean getIncludeThreadNamesAsStatementComment() {
        return this.mc.getIncludeThreadNamesAsStatementComment();
    }

    public void setIncludeThreadNamesAsStatementComment(boolean flag) {
        this.mc.setIncludeThreadNamesAsStatementComment(flag);
    }

    public boolean isServerLocal() throws SQLException {
        return this.mc.isServerLocal();
    }

    public void setAuthenticationPlugins(String authenticationPlugins) {
        this.mc.setAuthenticationPlugins(authenticationPlugins);
    }

    public String getAuthenticationPlugins() {
        return this.mc.getAuthenticationPlugins();
    }

    public void setDisabledAuthenticationPlugins(String disabledAuthenticationPlugins) {
        this.mc.setDisabledAuthenticationPlugins(disabledAuthenticationPlugins);
    }

    public String getDisabledAuthenticationPlugins() {
        return this.mc.getDisabledAuthenticationPlugins();
    }

    public void setDefaultAuthenticationPlugin(String defaultAuthenticationPlugin) {
        this.mc.setDefaultAuthenticationPlugin(defaultAuthenticationPlugin);

    }

    public String getDefaultAuthenticationPlugin() {
        return this.mc.getDefaultAuthenticationPlugin();
    }

    public void setParseInfoCacheFactory(String factoryClassname) {
        this.mc.setParseInfoCacheFactory(factoryClassname);
    }

    public String getParseInfoCacheFactory() {
        return this.mc.getParseInfoCacheFactory();
    }

    public void setSchema(String schema) throws SQLException {
        this.mc.setSchema(schema);
    }

    public String getSchema() throws SQLException {
        return this.mc.getSchema();
    }

    public void abort(Executor executor) throws SQLException {
        this.mc.abort(executor);
    }

    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        this.mc.setNetworkTimeout(executor, milliseconds);
    }

    public int getNetworkTimeout() throws SQLException {
        return this.mc.getNetworkTimeout();
    }

    public void setServerConfigCacheFactory(String factoryClassname) {
        this.mc.setServerConfigCacheFactory(factoryClassname);
    }

    public String getServerConfigCacheFactory() {
        return this.mc.getServerConfigCacheFactory();
    }

    public void setDisconnectOnExpiredPasswords(boolean disconnectOnExpiredPasswords) {
        this.mc.setDisconnectOnExpiredPasswords(disconnectOnExpiredPasswords);
    }

    public boolean getDisconnectOnExpiredPasswords() {
        return this.mc.getDisconnectOnExpiredPasswords();
    }

    public void setGetProceduresReturnsFunctions(boolean getProcedureReturnsFunctions) {
        this.mc.setGetProceduresReturnsFunctions(getProcedureReturnsFunctions);
    }

    public boolean getGetProceduresReturnsFunctions() {
        return this.mc.getGetProceduresReturnsFunctions();
    }

    public void abortInternal() throws SQLException {
        this.mc.abortInternal();
    }

    public Object getConnectionMutex() {
        return this.mc.getConnectionMutex();
    }

    public boolean getAllowMasterDownConnections() {
        return this.mc.getAllowMasterDownConnections();
    }

    public void setAllowMasterDownConnections(boolean connectIfMasterDown) {
        this.mc.setAllowMasterDownConnections(connectIfMasterDown);
    }

    public boolean getHaEnableJMX() {
        return this.mc.getHaEnableJMX();
    }

    public void setHaEnableJMX(boolean replicationEnableJMX) {
        this.mc.setHaEnableJMX(replicationEnableJMX);

    }

    public void setDetectCustomCollations(boolean detectCustomCollations) {
        this.mc.setDetectCustomCollations(detectCustomCollations);
    }

    public boolean getDetectCustomCollations() {
        return this.mc.getDetectCustomCollations();
    }

    public int getSessionMaxRows() {
        return this.mc.getSessionMaxRows();
    }

    public void setSessionMaxRows(int max) throws SQLException {
        this.mc.setSessionMaxRows(max);
    }

    public Clob createClob() throws SQLException {

        try {
            return ((java.sql.Connection) this.mc).createClob();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // never reached, but compiler can't tell
    }

    public Blob createBlob() throws SQLException {

        try {
            return ((java.sql.Connection) this.mc).createBlob();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // never reached, but compiler can't tell
    }

    public NClob createNClob() throws SQLException {

        try {
            return ((java.sql.Connection) this.mc).createNClob();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // never reached, but compiler can't tell
    }

    public SQLXML createSQLXML() throws SQLException {

        try {
            return ((java.sql.Connection) this.mc).createSQLXML();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // never reached, but compiler can't tell
    }

    /**
     * Returns true if the connection has not been closed and is still valid.
     * The driver shall submit a query on the connection or use some other
     * mechanism that positively verifies the connection is still valid when
     * this method is called.
     * <p>
     * The query submitted by the driver to validate the connection shall be executed in the context of the current transaction.
     * 
     * @param timeout
     *            - The time in seconds to wait for the database operation used
     *            to validate the connection to complete. If the timeout period
     *            expires before the operation completes, this method returns
     *            false. A value of 0 indicates a timeout is not applied to the
     *            database operation.
     *            <p>
     * @return true if the connection is valid, false otherwise
     * @exception SQLException
     *                if the value supplied for <code>timeout</code> is less
     *                then 0
     * @since 1.6
     */
    public synchronized boolean isValid(int timeout) throws SQLException {
        try {
            return ((java.sql.Connection) this.mc).isValid(timeout);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return false; // never reached, but compiler can't tell
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        try {
            checkClosed();

            ((java.sql.Connection) this.mc).setClientInfo(name, value);
        } catch (SQLException sqlException) {
            try {
                checkAndFireConnectionError(sqlException);
            } catch (SQLException sqlEx2) {
                SQLClientInfoException clientEx = new SQLClientInfoException();
                clientEx.initCause(sqlEx2);

                throw clientEx;
            }
        }
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        try {
            checkClosed();

            ((java.sql.Connection) this.mc).setClientInfo(properties);
        } catch (SQLException sqlException) {
            try {
                checkAndFireConnectionError(sqlException);
            } catch (SQLException sqlEx2) {
                SQLClientInfoException clientEx = new SQLClientInfoException();
                clientEx.initCause(sqlEx2);

                throw clientEx;
            }
        }
    }

    public String getClientInfo(String name) throws SQLException {

        try {
            return ((java.sql.Connection) this.mc).getClientInfo(name);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // never reached, but compiler can't tell
    }

    public Properties getClientInfo() throws SQLException {

        try {
            return ((java.sql.Connection) this.mc).getClientInfo();
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // never reached, but compiler can't tell
    }

    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException {

        try {
            return ((java.sql.Connection) this.mc).createArrayOf(typeName, elements);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // never reached, but compiler can't tell
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {

        try {
            return ((java.sql.Connection) this.mc).createStruct(typeName, attributes);
        } catch (SQLException sqlException) {
            checkAndFireConnectionError(sqlException);
        }

        return null; // never reached, but compiler can't tell
    }

    /**
     * Returns an object that implements the given interface to allow access to
     * non-standard methods, or standard methods not exposed by the proxy. The
     * result may be either the object found to implement the interface or a
     * proxy for that object. If the receiver implements the interface then that
     * is the object. If the receiver is a wrapper and the wrapped object
     * implements the interface then that is the object. Otherwise the object is
     * the result of calling <code>unwrap</code> recursively on the wrapped
     * object. If the receiver is not a wrapper and does not implement the
     * interface, then an <code>SQLException</code> is thrown.
     * 
     * @param iface
     *            A Class defining an interface that the result must implement.
     * @return an object that implements the interface. May be a proxy for the
     *         actual implementing object.
     * @throws java.sql.SQLException
     *             If no object found that implements the interface
     * @since 1.6
     */
    public synchronized <T> T unwrap(java.lang.Class<T> iface) throws java.sql.SQLException {
        try {
            if ("java.sql.Connection".equals(iface.getName()) || "java.sql.Wrapper.class".equals(iface.getName())) {
                return iface.cast(this);
            }

            if (this.unwrappedInterfaces == null) {
                this.unwrappedInterfaces = new HashMap();
            }

            Object cachedUnwrapped = this.unwrappedInterfaces.get(iface);

            if (cachedUnwrapped == null) {
                cachedUnwrapped = Proxy.newProxyInstance(this.mc.getClass().getClassLoader(), new Class[] { iface },
                        new ConnectionErrorFiringInvocationHandler(this.mc));
                this.unwrappedInterfaces.put(iface, cachedUnwrapped);
            }

            return iface.cast(cachedUnwrapped);
        } catch (ClassCastException cce) {
            throw SQLError.createSQLException(Messages.getString("Common.UnableToUnwrap", new Object[] { iface.toString() }),
                    SQLError.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
        }
    }

    /**
     * Returns true if this either implements the interface argument or is
     * directly or indirectly a wrapper for an object that does. Returns false
     * otherwise. If this implements the interface then return true, else if
     * this is a wrapper then return the result of recursively calling <code>isWrapperFor</code> on the wrapped object. If this does not
     * implement the interface and is not a wrapper, return false. This method
     * should be implemented as a low-cost operation compared to <code>unwrap</code> so that callers can use this method to avoid
     * expensive <code>unwrap</code> calls that may fail. If this method returns
     * true then calling <code>unwrap</code> with the same argument should
     * succeed.
     * 
     * @param interfaces
     *            a Class defining an interface.
     * @return true if this implements the interface or directly or indirectly
     *         wraps an object that does.
     * @throws java.sql.SQLException
     *             if an error occurs while determining whether this is a
     *             wrapper for an object with the given interface.
     * @since 1.6
     */
    public boolean isWrapperFor(Class<?> iface) throws SQLException {

        boolean isInstance = iface.isInstance(this);

        if (isInstance) {
            return true;
        }

        return (iface.getName().equals(JdbcConnection.class.getName()) || iface.getName().equals(JdbcConnectionProperties.class.getName()));
    }

    public void setDontCheckOnDuplicateKeyUpdateInSQL(boolean dontCheckOnDuplicateKeyUpdateInSQL) {
        this.mc.setDontCheckOnDuplicateKeyUpdateInSQL(dontCheckOnDuplicateKeyUpdateInSQL);
    }

    public boolean getDontCheckOnDuplicateKeyUpdateInSQL() {
        return this.mc.getDontCheckOnDuplicateKeyUpdateInSQL();
    }

    public void setSocksProxyHost(String socksProxyHost) {
        this.mc.setSocksProxyHost(socksProxyHost);
    }

    public String getSocksProxyHost() {
        return this.mc.getSocksProxyHost();
    }

    public void setSocksProxyPort(int socksProxyPort) throws SQLException {
        this.mc.setSocksProxyPort(socksProxyPort);
    }

    public int getSocksProxyPort() {
        return this.mc.getSocksProxyPort();
    }

    @Override
    public String getProcessHost() {
        return this.mc.getProcessHost();
    }

    @Override
    public MysqlaProtocol getProtocol() {
        return this.mc.getProtocol();
    }

    @Override
    public MysqlaSession getSession() {
        return this.mc.getSession();
    }

    @Override
    public ProfilerEventHandler getProfilerEventHandlerInstance() {
        return this.mc.getProfilerEventHandlerInstance();
    }

    @Override
    public void setProfilerEventHandlerInstance(ProfilerEventHandler h) {
        this.mc.setProfilerEventHandlerInstance(h);
    }

    @Override
    public long getId() {
        return this.mc.getId();
    }

    @Override
    public String getURL() {
        return this.mc.getURL();
    }

    @Override
    public String getUser() {
        return this.mc.getUser();
    }

    @Override
    public TimeZone getDefaultTimeZone() {
        return this.mc.getDefaultTimeZone();
    }

    @Override
    public String getEncodingForIndex(int collationIndex) {
        return this.mc.getEncodingForIndex(collationIndex);
    }

    @Override
    public String getErrorMessageEncoding() {
        return this.mc.getErrorMessageEncoding();
    }

    @Override
    public int getMaxBytesPerChar(String javaCharsetName) {
        return this.mc.getMaxBytesPerChar(javaCharsetName);
    }

    @Override
    public int getMaxBytesPerChar(Integer charsetIndex, String javaCharsetName) {
        return this.mc.getMaxBytesPerChar(charsetIndex, javaCharsetName);
    }

    @Override
    public void createNewIO(boolean isForReconnect) {
        this.mc.createNewIO(isForReconnect);
    }

    @Override
    public boolean isProxySet() {
        return this.mc.isProxySet();
    }

    @Override
    public JdbcConnection duplicate() throws SQLException {
        return this.mc.duplicate();
    }

    @Override
    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, Buffer packet, int resultSetType,
            int resultSetConcurrency, boolean streamResults, String catalog, Field[] cachedMetadata) throws SQLException {
        return this.mc.execSQL(callingStatement, sql, maxRows, packet, resultSetType, resultSetConcurrency, streamResults, catalog, cachedMetadata);
    }

    @Override
    public ResultSetInternalMethods execSQL(StatementImpl callingStatement, String sql, int maxRows, Buffer packet, int resultSetType,
            int resultSetConcurrency, boolean streamResults, String catalog, Field[] cachedMetadata, boolean isBatch) throws SQLException {
        return this.mc.execSQL(callingStatement, sql, maxRows, packet, resultSetType, resultSetConcurrency, streamResults, catalog, cachedMetadata, isBatch);
    }

    @Override
    public int getNetBufferLength() {
        return this.mc.getNetBufferLength();
    }

    public boolean getReadOnlyPropagatesToServer() {
        return this.mc.getReadOnlyPropagatesToServer();
    }

    public void setReadOnlyPropagatesToServer(boolean flag) {
        this.mc.setReadOnlyPropagatesToServer(flag);
    }

    @Override
    public JdbcPropertySet getPropertySet() {
        return this.mc.getPropertySet();
    }

}
