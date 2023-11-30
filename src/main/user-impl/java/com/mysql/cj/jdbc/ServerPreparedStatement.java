/*
 * Copyright (c) 2002, 2023, Oracle and/or its affiliates.
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

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.sql.ParameterMetaData;
import java.sql.SQLException;
import java.sql.Wrapper;
import java.util.ArrayList;

import com.mysql.cj.BindValue;
import com.mysql.cj.CancelQueryTask;
import com.mysql.cj.Messages;
import com.mysql.cj.NativeSession;
import com.mysql.cj.PreparedQuery;
import com.mysql.cj.QueryBindings;
import com.mysql.cj.QueryInfo;
import com.mysql.cj.ServerPreparedQuery;
import com.mysql.cj.conf.PropertyKey;
import com.mysql.cj.exceptions.CJException;
import com.mysql.cj.exceptions.ExceptionFactory;
import com.mysql.cj.exceptions.ExceptionInterceptor;
import com.mysql.cj.exceptions.MysqlErrorNumbers;
import com.mysql.cj.exceptions.WrongArgumentException;
import com.mysql.cj.jdbc.exceptions.MySQLStatementCancelledException;
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.jdbc.result.ResultSetMetaData;
import com.mysql.cj.protocol.ColumnDefinition;
import com.mysql.cj.protocol.Message;

/**
 * JDBC Interface for MySQL-4.1 and newer server-side PreparedStatements.
 */
public class ServerPreparedStatement extends ClientPreparedStatement {

    private boolean hasOnDuplicateKeyUpdate = false;

    /** Has this prepared statement been marked invalid? */
    private boolean invalid = false;

    /** If this statement has been marked invalid, what was the reason? */
    private CJException invalidationException;

    protected boolean isCacheable = false;
    protected boolean isCached = false;

    /**
     * Creates a prepared statement instance
     *
     * @param conn
     *            the connection creating us.
     * @param sql
     *            the SQL containing the statement to prepare.
     * @param db
     *            the database in use when we were created.
     * @param resultSetType
     *            ResultSet type
     * @param resultSetConcurrency
     *            ResultSet concurrency
     * @return new ServerPreparedStatement
     * @throws SQLException
     *             If an error occurs
     */
    protected static ServerPreparedStatement getInstance(JdbcConnection conn, String sql, String db, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new ServerPreparedStatement(conn, sql, db, resultSetType, resultSetConcurrency);
    }

    /**
     * Creates a new ServerPreparedStatement object.
     *
     * @param conn
     *            the connection creating us.
     * @param sql
     *            the SQL containing the statement to prepare.
     * @param db
     *            the database in use when we were created.
     * @param resultSetType
     *            ResultSet type
     * @param resultSetConcurrency
     *            ResultSet concurrency
     *
     * @throws SQLException
     *             If an error occurs
     */
    protected ServerPreparedStatement(JdbcConnection conn, String sql, String db, int resultSetType, int resultSetConcurrency) throws SQLException {
        super(conn, db);

        checkNullOrEmptyQuery(sql);
        String statementComment = this.session.getProtocol().getQueryComment();
        PreparedQuery prepQuery = (PreparedQuery) this.query;
        prepQuery.setOriginalSql(statementComment == null ? sql : "/* " + statementComment + " */ " + sql);
        prepQuery.setQueryInfo(new QueryInfo(prepQuery.getOriginalSql(), this.session, this.charEncoding));

        this.hasOnDuplicateKeyUpdate = prepQuery.getQueryInfo().containsOnDuplicateKeyUpdate();

        try {
            serverPrepare(sql);
        } catch (CJException | SQLException sqlEx) {
            realClose(false, true);

            throw SQLExceptionsMapping.translateException(sqlEx, this.exceptionInterceptor);
        }

        setResultSetType(resultSetType);
        setResultSetConcurrency(resultSetConcurrency);
    }

    @Override
    protected void initQuery() {
        this.query = ServerPreparedQuery.getInstance(this.session);
    }

    @Override
    public String toString() {
        StringBuilder toStringBuf = new StringBuilder();
        toStringBuf.append(this.getClass().getName() + "[");
        toStringBuf.append(((ServerPreparedQuery) this.query).getServerStatementId());
        toStringBuf.append("]: ");
        toStringBuf.append(((PreparedQuery) this.query).asSql());
        return toStringBuf.toString();
    }

    @Override
    public void addBatch() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.query.addBatch(((PreparedQuery) this.query).getQueryBindings().clone());
        }
    }

    @Override
    protected JdbcConnection checkClosed() {
        if (this.invalid) {
            throw this.invalidationException;
        }

        return super.checkClosed();
    }

    @Override
    public void clearParameters() {
        synchronized (checkClosed().getConnectionMutex()) {
            ((ServerPreparedQuery) this.query).clearParameters(true);
        }
    }

    protected void setClosed(boolean flag) {
        this.isClosed = flag;
    }

    @Override
    public void close() throws SQLException {
        JdbcConnection locallyScopedConn = this.connection;

        if (locallyScopedConn == null) {
            return; // already closed
        }

        synchronized (locallyScopedConn.getConnectionMutex()) {
            if (this.isClosed) {
                return; // already closed
            }

            if (this.isCacheable && isPoolable()) {
                clearParameters();
                clearAttributes();

                this.isClosed = true;

                this.connection.recachePreparedStatement(this);
                this.isCached = true;
                return;
            }

            this.isClosed = false;
            realClose(true, true);
        }
    }

    @Override
    protected long[] executeBatchSerially(long batchTimeout) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            JdbcConnection locallyScopedConn = this.connection;

            if (locallyScopedConn.isReadOnly()) {
                throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.2") + Messages.getString("ServerPreparedStatement.3"),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            clearWarnings();

            // Store this for later, we're going to 'swap' them out
            // as we execute each batched statement...
            BindValue[] oldBindValues = ((ServerPreparedQuery) this.query).getQueryBindings().getBindValues();

            try {
                long[] updateCounts = null;

                if (this.query.getBatchedArgs() != null) {
                    int nbrCommands = this.query.getBatchedArgs().size();
                    updateCounts = new long[nbrCommands];

                    if (this.retrieveGeneratedKeys) {
                        this.batchedGeneratedKeys = new ArrayList<>(nbrCommands);
                    }

                    for (int i = 0; i < nbrCommands; i++) {
                        updateCounts[i] = -3;
                    }

                    SQLException sqlEx = null;

                    int commandIndex = 0;

                    BindValue[] previousBindValuesForBatch = null;

                    CancelQueryTask timeoutTask = null;

                    try {
                        timeoutTask = startQueryTimer(this, batchTimeout);

                        for (commandIndex = 0; commandIndex < nbrCommands; commandIndex++) {
                            Object arg = this.query.getBatchedArgs().get(commandIndex);

                            try {
                                if (arg instanceof String) {
                                    updateCounts[commandIndex] = executeUpdateInternal((String) arg, true, this.retrieveGeneratedKeys);

                                    // limit one generated key per OnDuplicateKey statement
                                    getBatchedGeneratedKeys(this.results.getFirstCharOfQuery() == 'I' && containsOnDuplicateKeyInString((String) arg) ? 1 : 0);
                                } else {
                                    ((PreparedQuery) this.query).setQueryBindings((QueryBindings) arg);
                                    BindValue[] parameterBindings = ((QueryBindings) arg).getBindValues();

                                    // We need to check types each time, as the user might have bound different types in each addBatch()

                                    if (previousBindValuesForBatch != null) {
                                        for (int j = 0; j < parameterBindings.length; j++) {
                                            if (parameterBindings[j].getMysqlType() != previousBindValuesForBatch[j].getMysqlType()) {
                                                ((ServerPreparedQuery) this.query).getQueryBindings().getSendTypesToServer().set(true);
                                                break;
                                            }
                                        }
                                    }

                                    try {
                                        updateCounts[commandIndex] = executeUpdateInternal(false, true);
                                    } finally {
                                        previousBindValuesForBatch = parameterBindings;
                                    }

                                    // limit one generated key per OnDuplicateKey statement
                                    getBatchedGeneratedKeys(containsOnDuplicateKeyUpdate() ? 1 : 0);
                                }
                            } catch (SQLException ex) {
                                updateCounts[commandIndex] = EXECUTE_FAILED;

                                if (this.continueBatchOnError && !(ex instanceof MySQLTimeoutException) && !(ex instanceof MySQLStatementCancelledException)
                                        && !hasDeadlockOrTimeoutRolledBackTx(ex)) {
                                    sqlEx = ex;
                                } else {
                                    long[] newUpdateCounts = new long[commandIndex];
                                    System.arraycopy(updateCounts, 0, newUpdateCounts, 0, commandIndex);

                                    throw SQLError.createBatchUpdateException(ex, newUpdateCounts, this.exceptionInterceptor);
                                }
                            }
                        }
                    } finally {
                        stopQueryTimer(timeoutTask, false, false);
                        resetCancelledState();
                    }

                    if (sqlEx != null) {
                        throw SQLError.createBatchUpdateException(sqlEx, updateCounts, this.exceptionInterceptor);
                    }
                }

                return updateCounts != null ? updateCounts : new long[0];
            } finally {
                ((ServerPreparedQuery) this.query).getQueryBindings().setBindValues(oldBindValues);
                ((ServerPreparedQuery) this.query).getQueryBindings().getSendTypesToServer().set(true);

                clearBatch();
            }
        }
    }

    private static SQLException appendMessageToException(SQLException sqlEx, String messageToAppend, ExceptionInterceptor interceptor) {
        String sqlState = sqlEx.getSQLState();
        int vendorErrorCode = sqlEx.getErrorCode();

        SQLException sqlExceptionWithNewMessage = SQLError.createSQLException(sqlEx.getMessage() + messageToAppend, sqlState, vendorErrorCode, interceptor);
        sqlExceptionWithNewMessage.setStackTrace(sqlEx.getStackTrace());

        return sqlExceptionWithNewMessage;
    }

    @Override
    protected <M extends Message> com.mysql.cj.jdbc.result.ResultSetInternalMethods executeInternal(int maxRowsToRetrieve, M sendPacket,
            boolean createStreamingResultSet, boolean queryIsSelectOnly, ColumnDefinition metadata, boolean isBatch) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ((PreparedQuery) this.query).getQueryBindings().setNumberOfExecutions(((PreparedQuery) this.query).getQueryBindings().getNumberOfExecutions() + 1);

            // We defer to server-side execution
            try {
                return serverExecute(maxRowsToRetrieve, createStreamingResultSet, metadata);
            } catch (SQLException sqlEx) {
                // don't wrap SQLExceptions
                if (this.session.getPropertySet().getBooleanProperty(PropertyKey.enablePacketDebug).getValue()) {
                    this.session.dumpPacketRingBuffer();
                }

                if (this.dumpQueriesOnException.getValue()) {
                    String extractedSql = toString();
                    StringBuilder messageBuf = new StringBuilder(extractedSql.length() + 32);
                    messageBuf.append("\n\nQuery being executed when exception was thrown:\n");
                    messageBuf.append(extractedSql);
                    messageBuf.append("\n\n");

                    sqlEx = appendMessageToException(sqlEx, messageBuf.toString(), this.exceptionInterceptor);
                }

                throw sqlEx;
            } catch (Exception ex) {
                if (this.session.getPropertySet().getBooleanProperty(PropertyKey.enablePacketDebug).getValue()) {
                    this.session.dumpPacketRingBuffer();
                }

                SQLException sqlEx = SQLError.createSQLException(ex.toString(), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, ex, this.exceptionInterceptor);

                if (this.dumpQueriesOnException.getValue()) {
                    String extractedSql = toString();
                    StringBuilder messageBuf = new StringBuilder(extractedSql.length() + 32);
                    messageBuf.append("\n\nQuery being executed when exception was thrown:\n");
                    messageBuf.append(extractedSql);
                    messageBuf.append("\n\n");

                    sqlEx = appendMessageToException(sqlEx, messageBuf.toString(), this.exceptionInterceptor);
                }

                throw sqlEx;
            }
        }
    }

    /**
     * Returns the structure representing the value that (can be)/(is)
     * bound at the given parameter index.
     *
     * @param parameterIndex
     *            1-based
     * @param forLongData
     *            is this for a stream?
     * @return {@link BindValue}
     * @throws SQLException
     *             if a database access error occurs or this method is called on a closed PreparedStatement
     */
    protected BindValue getBinding(int parameterIndex, boolean forLongData) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            int i = getCoreParameterIndex(parameterIndex);
            return ((ServerPreparedQuery) this.query).getQueryBindings().getBinding(i, forLongData);
        }
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            ColumnDefinition resultFields = ((ServerPreparedQuery) this.query).getResultFields();

            return resultFields == null || resultFields.getFields() == null ? null
                    : new ResultSetMetaData(this.session, resultFields.getFields(),
                            this.session.getPropertySet().getBooleanProperty(PropertyKey.useOldAliasMetadataBehavior).getValue(),
                            this.session.getPropertySet().getBooleanProperty(PropertyKey.yearIsDateType).getValue(), this.exceptionInterceptor);
        }
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.parameterMetaData == null) {
                this.parameterMetaData = new MysqlParameterMetadata(this.session, ((ServerPreparedQuery) this.query).getParameterFields(),
                        ((PreparedQuery) this.query).getParameterCount(), this.exceptionInterceptor);
            }

            return this.parameterMetaData;
        }
    }

    @Override
    public boolean isNull(int paramIndex) {
        throw new IllegalArgumentException(Messages.getString("ServerPreparedStatement.7"));
    }

    @Override
    public void realClose(boolean calledExplicitly, boolean closeOpenResults) throws SQLException {
        JdbcConnection locallyScopedConn = this.connection;
        if (locallyScopedConn == null) {
            return; // already closed
        }

        synchronized (locallyScopedConn.getConnectionMutex()) {
            if (this.connection != null) {
                //
                // Don't communicate with the server if we're being called from the finalizer...
                //
                // This will leak server resources, but if we don't do this, we'll deadlock (potentially, because there's no guarantee when, what order, and
                // what concurrency finalizers will be called with). Well-behaved programs won't rely on finalizers to clean up their statements.
                //

                CJException exceptionDuringClose = null;

                if (this.isCached) {
                    locallyScopedConn.decachePreparedStatement(this);
                    this.isCached = false;
                }

                super.realClose(calledExplicitly, closeOpenResults);

                ((ServerPreparedQuery) this.query).clearParameters(false);

                // Finally deallocate the prepared statement.
                if (calledExplicitly && !locallyScopedConn.isClosed()) {
                    synchronized (locallyScopedConn.getConnectionMutex()) {
                        try {
                            ((NativeSession) locallyScopedConn.getSession()).getProtocol().sendCommand(
                                    this.commandBuilder.buildComStmtClose(null, ((ServerPreparedQuery) this.query).getServerStatementId()), true, 0);
                        } catch (CJException sqlEx) {
                            exceptionDuringClose = sqlEx;
                        }
                    }
                }

                if (exceptionDuringClose != null) {
                    throw exceptionDuringClose;
                }
            }
        }
    }

    /**
     * Used by Connection when auto-reconnecting to retrieve 'lost' prepared
     * statements.
     *
     * @throws CJException
     *             if an error occurs.
     */
    protected void rePrepare() {
        synchronized (checkClosed().getConnectionMutex()) {
            this.invalidationException = null;

            try {
                serverPrepare(((PreparedQuery) this.query).getOriginalSql());
            } catch (Exception ex) {
                this.invalidationException = ExceptionFactory.createException(ex.getMessage(), ex);
            }

            if (this.invalidationException != null) {
                this.invalid = true;

                this.query.closeQuery();

                if (this.results != null) {
                    try {
                        this.results.close();
                    } catch (Exception ex) {
                    }
                }

                if (this.generatedKeysResults != null) {
                    try {
                        this.generatedKeysResults.close();
                    } catch (Exception ex) {
                    }
                }

                try {
                    closeAllOpenResults();
                } catch (Exception e) {
                }

                if (this.connection != null && !this.dontTrackOpenResources.getValue()) {
                    this.connection.unregisterStatement(this);
                }
            }
        }
    }

    /**
     * Tells the server to execute this prepared statement with the current
     * parameter bindings.
     *
     * <pre>
     *    -   Server gets the command 'COM_EXECUTE' to execute the
     *        previously         prepared query. If there is any param markers;
     *  then client will send the data in the following format:
     *
     *  [COM_EXECUTE:1]
     *  [STMT_ID:4]
     *  [NULL_BITS:(param_count+7)/8)]
     *  [TYPES_SUPPLIED_BY_CLIENT(0/1):1]
     *  [[length]data]
     *  [[length]data] .. [[length]data].
     *
     *  (Note: Except for string/binary types; all other types will not be
     *  supplied with length field)
     * </pre>
     *
     * @param maxRowsToRetrieve
     *            rows limit
     * @param createStreamingResultSet
     *            should c/J create a streaming result?
     * @param metadata
     *            use this metadata instead of the one provided on wire
     * @return result set
     * @throws SQLException
     *             if a database access error occurs or this method is called on a closed PreparedStatement
     */
    protected ResultSetInternalMethods serverExecute(int maxRowsToRetrieve, boolean createStreamingResultSet, ColumnDefinition metadata) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.results = ((ServerPreparedQuery) this.query).serverExecute(maxRowsToRetrieve, createStreamingResultSet, metadata, this.resultSetFactory);
            return this.results;
        }
    }

    protected void serverPrepare(String sql) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            SQLException t = null;

            try {
                ServerPreparedQuery q = (ServerPreparedQuery) this.query;
                q.serverPrepare(sql);
            } catch (IOException ioEx) {
                t = SQLError.createCommunicationsException(this.connection, this.session.getProtocol().getPacketSentTimeHolder(),
                        this.session.getProtocol().getPacketReceivedTimeHolder(), ioEx, this.exceptionInterceptor);
            } catch (CJException sqlEx) {
                SQLException ex = SQLExceptionsMapping.translateException(sqlEx);

                if (this.dumpQueriesOnException.getValue()) {
                    StringBuilder messageBuf = new StringBuilder(((PreparedQuery) this.query).getOriginalSql().length() + 32);
                    messageBuf.append("\n\nQuery being prepared when exception was thrown:\n\n");
                    messageBuf.append(((PreparedQuery) this.query).getOriginalSql());

                    ex = appendMessageToException(ex, messageBuf.toString(), this.exceptionInterceptor);
                }

                t = ex;
            } finally {
                // Leave the I/O channel in a known state...there might be packets out there that we're not interested in
                try {
                    this.session.clearInputStream();
                } catch (Exception e) {
                    if (t == null) {
                        t = SQLError.createCommunicationsException(this.connection, this.session.getProtocol().getPacketSentTimeHolder(),
                                this.session.getProtocol().getPacketReceivedTimeHolder(), e, this.exceptionInterceptor);
                    }
                }
                if (t != null) {
                    throw t;
                }
            }
        }
    }

    @Override
    protected void checkBounds(int parameterIndex, int parameterIndexOffset) throws SQLException {
        int paramCount = ((PreparedQuery) this.query).getParameterCount();
        if (paramCount == 0) {
            throw ExceptionFactory.createException(WrongArgumentException.class, Messages.getString("ServerPreparedStatement.8"),
                    this.session.getExceptionInterceptor());
        }

        if (parameterIndex < 0 || parameterIndex > paramCount) {
            throw ExceptionFactory.createException(WrongArgumentException.class,
                    Messages.getString("ServerPreparedStatement.9") + (parameterIndex + 1) + Messages.getString("ServerPreparedStatement.10") + paramCount,
                    this.session.getExceptionInterceptor());
        }
    }

    @Deprecated
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        checkClosed();

        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        checkClosed();

        setString(parameterIndex, x.toString());
    }

    @Override
    public long getServerStatementId() {
        return ((ServerPreparedQuery) this.query).getServerStatementId();
    }

    @Override
    protected boolean containsOnDuplicateKeyUpdate() {
        return this.hasOnDuplicateKeyUpdate;
    }

    @Override
    protected ClientPreparedStatement prepareBatchedInsertSQL(JdbcConnection localConn, int numBatches) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {
                ClientPreparedStatement pstmt = ((Wrapper) localConn.prepareStatement(((PreparedQuery) this.query).getQueryInfo().getSqlForBatch(numBatches),
                        this.resultSetConcurrency, this.query.getResultType().getIntValue())).unwrap(ClientPreparedStatement.class);
                pstmt.setRetrieveGeneratedKeys(this.retrieveGeneratedKeys);

                getQueryAttributesBindings().runThroughAll(a -> ((JdbcStatement) pstmt).setAttribute(a.getName(), a.getValue()));

                return pstmt;
            } catch (CJException e) {
                SQLException sqlEx = SQLError.createSQLException(Messages.getString("ServerPreparedStatement.27"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
                sqlEx.initCause(e);

                throw sqlEx;
            }
        }
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        super.setPoolable(poolable);

        if (!poolable && this.isCached) {
            this.connection.decachePreparedStatement(this);
            this.isCached = false;

            if (this.isClosed) {
                this.isClosed = false;
                realClose(true, true);
            }
        }
    }

}
