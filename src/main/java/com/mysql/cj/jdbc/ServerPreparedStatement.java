/*
  Copyright (c) 2002, 2017, Oracle and/or its affiliates. All rights reserved.

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

package com.mysql.cj.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Wrapper;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;

import com.mysql.cj.api.PreparedQuery;
import com.mysql.cj.api.ProfilerEvent;
import com.mysql.cj.api.QueryBindings;
import com.mysql.cj.api.exceptions.ExceptionInterceptor;
import com.mysql.cj.api.jdbc.JdbcConnection;
import com.mysql.cj.api.jdbc.result.ResultSetInternalMethods;
import com.mysql.cj.api.mysqla.io.NativeProtocol.IntegerDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringLengthDataType;
import com.mysql.cj.api.mysqla.io.NativeProtocol.StringSelfDataType;
import com.mysql.cj.api.mysqla.io.PacketPayload;
import com.mysql.cj.api.mysqla.result.ColumnDefinition;
import com.mysql.cj.core.Messages;
import com.mysql.cj.core.MysqlType;
import com.mysql.cj.core.conf.PropertyDefinitions;
import com.mysql.cj.core.exceptions.CJException;
import com.mysql.cj.core.exceptions.ExceptionFactory;
import com.mysql.cj.core.exceptions.MysqlErrorNumbers;
import com.mysql.cj.core.profiler.ProfilerEventHandlerFactory;
import com.mysql.cj.core.profiler.ProfilerEventImpl;
import com.mysql.cj.core.util.LogUtils;
import com.mysql.cj.core.util.StringUtils;
import com.mysql.cj.core.util.TimeUtil;
import com.mysql.cj.jdbc.exceptions.MySQLStatementCancelledException;
import com.mysql.cj.jdbc.exceptions.MySQLTimeoutException;
import com.mysql.cj.jdbc.exceptions.SQLError;
import com.mysql.cj.jdbc.exceptions.SQLExceptionsMapping;
import com.mysql.cj.jdbc.result.ResultSetMetaData;
import com.mysql.cj.mysqla.CancelQueryTask;
import com.mysql.cj.mysqla.ClientPreparedQuery;
import com.mysql.cj.mysqla.MysqlaConstants;
import com.mysql.cj.mysqla.ParseInfo;
import com.mysql.cj.mysqla.ServerPreparedQuery;
import com.mysql.cj.mysqla.ServerPreparedQueryBindValue;
import com.mysql.cj.mysqla.ServerPreparedQueryBindings;

/**
 * JDBC Interface for MySQL-4.1 and newer server-side PreparedStatements.
 */
public class ServerPreparedStatement extends PreparedStatement {

    private boolean hasOnDuplicateKeyUpdate = false;

    /**
     * Flag indicating whether or not the long parameters have been 'switched'
     * back to normal parameters. We can not execute() if clearParameters()
     * hasn't been called in this case.
     */
    private boolean detectedLongParameterSwitch = false;

    /** Has this prepared statement been marked invalid? */
    private boolean invalid = false;

    /** If this statement has been marked invalid, what was the reason? */
    private CJException invalidationException;

    /** Do we need to send/resend types to the server? */
    private AtomicBoolean sendTypesToServer = new AtomicBoolean(false);

    /**
     * Creates a prepared statement instance
     */

    protected static ServerPreparedStatement getInstance(JdbcConnection conn, String sql, String catalog, int resultSetType, int resultSetConcurrency)
            throws SQLException {
        return new ServerPreparedStatement(conn, sql, catalog, resultSetType, resultSetConcurrency);
    }

    /**
     * Creates a new ServerPreparedStatement object.
     * 
     * @param conn
     *            the connection creating us.
     * @param sql
     *            the SQL containing the statement to prepare.
     * @param catalog
     *            the catalog in use when we were created.
     * 
     * @throws SQLException
     *             If an error occurs
     */
    protected ServerPreparedStatement(JdbcConnection conn, String sql, String catalog, int resultSetType, int resultSetConcurrency) throws SQLException {
        super(conn, catalog);
        this.query = ServerPreparedQuery.getInstance(this.session, this.query.getId());

        checkNullOrEmptyQuery(sql);

        int startOfStatement = findStartOfStatement(sql);

        this.firstCharOfStmt = StringUtils.firstAlphaCharUc(sql, startOfStatement);

        this.hasOnDuplicateKeyUpdate = this.firstCharOfStmt == 'I' && containsOnDuplicateKeyInString(sql);

        this.useAutoSlowLog = this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_autoSlowLog).getValue();

        String statementComment = this.session.getProtocol().getQueryComment();

        ((PreparedQuery) this.query).setOriginalSql(statementComment == null ? sql : "/* " + statementComment + " */ " + sql);

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
    public void addBatch() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            QueryBindings queryBindings = ((PreparedQuery) this.query).getQueryBindings();
            this.query.addBatch(queryBindings.clone());
        }
    }

    @Override
    public String asSql(boolean quoteStreamsAndUnknowns) throws SQLException {

        synchronized (checkClosed().getConnectionMutex()) {

            PreparedStatement pStmtForSub = null;

            try {
                pStmtForSub = PreparedStatement.getInstance(this.connection, ((PreparedQuery) this.query).getOriginalSql(), this.getCurrentCatalog());

                int numParameters = ((PreparedQuery) pStmtForSub.query).getParameterCount();
                int ourNumParameters = ((PreparedQuery) this.query).getParameterCount();

                ServerPreparedQueryBindValue[] parameterBindings = ((ServerPreparedQuery) this.query).getQueryBindings().getBindValues();

                for (int i = 0; (i < numParameters) && (i < ourNumParameters); i++) {
                    if (parameterBindings[i] != null) {
                        if (parameterBindings[i].isNull()) {
                            pStmtForSub.setNull(i + 1, MysqlType.NULL);
                        } else {
                            ServerPreparedQueryBindValue bindValue = parameterBindings[i];

                            //
                            // Handle primitives first
                            //
                            switch (bindValue.bufferType) {

                                case MysqlaConstants.FIELD_TYPE_TINY:
                                    pStmtForSub.setByte(i + 1, (byte) bindValue.longBinding);
                                    break;
                                case MysqlaConstants.FIELD_TYPE_SHORT:
                                    pStmtForSub.setShort(i + 1, (short) bindValue.longBinding);
                                    break;
                                case MysqlaConstants.FIELD_TYPE_LONG:
                                    pStmtForSub.setInt(i + 1, (int) bindValue.longBinding);
                                    break;
                                case MysqlaConstants.FIELD_TYPE_LONGLONG:
                                    pStmtForSub.setLong(i + 1, bindValue.longBinding);
                                    break;
                                case MysqlaConstants.FIELD_TYPE_FLOAT:
                                    pStmtForSub.setFloat(i + 1, bindValue.floatBinding);
                                    break;
                                case MysqlaConstants.FIELD_TYPE_DOUBLE:
                                    pStmtForSub.setDouble(i + 1, bindValue.doubleBinding);
                                    break;
                                default:
                                    pStmtForSub.setObject(i + 1, parameterBindings[i].value);
                                    break;
                            }
                        }
                    }
                }

                return pStmtForSub.asSql(quoteStreamsAndUnknowns);
            } finally {
                if (pStmtForSub != null) {
                    try {
                        pStmtForSub.close();
                    } catch (SQLException sqlEx) {
                        // ignore
                    }
                }
            }
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
            this.detectedLongParameterSwitch = ((ServerPreparedQuery) this.query).clearParametersInternal(true);
        }
    }

    protected boolean isCached = false;

    private boolean useAutoSlowLog;

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

            if (this.isCached && isPoolable() && !this.isClosed) {
                clearParameters();

                this.isClosed = true;

                this.connection.recachePreparedStatement(this);
                return;
            }

            this.isClosed = false;
            realClose(true, true);
        }
    }

    @Override
    protected long[] executeBatchSerially(int batchTimeout) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            JdbcConnection locallyScopedConn = this.connection;

            if (locallyScopedConn.isReadOnly()) {
                throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.2") + Messages.getString("ServerPreparedStatement.3"),
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }

            clearWarnings();

            // Store this for later, we're going to 'swap' them out
            // as we execute each batched statement...
            ServerPreparedQueryBindValue[] oldBindValues = ((ServerPreparedQuery) this.query).getQueryBindings().getBindValues();

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

                    ServerPreparedQueryBindValue[] previousBindValuesForBatch = null;

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
                                    ((ServerPreparedQuery) this.query).setQueryBindings((QueryBindings) arg);
                                    ServerPreparedQueryBindValue[] parameterBindings = ((ServerPreparedQuery) this.query).getQueryBindings().getBindValues();

                                    // We need to check types each time, as the user might have bound different types in each addBatch()

                                    if (previousBindValuesForBatch != null) {
                                        for (int j = 0; j < parameterBindings.length; j++) {
                                            if (parameterBindings[j].bufferType != previousBindValuesForBatch[j].bufferType) {
                                                this.sendTypesToServer.set(true);

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
                                    getBatchedGeneratedKeys(containsOnDuplicateKeyUpdateInSQL() ? 1 : 0);
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

                return (updateCounts != null) ? updateCounts : new long[0];
            } finally {
                ((ServerPreparedQuery) this.query).getQueryBindings().setBindValues(oldBindValues);
                this.sendTypesToServer.set(true);

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
    protected com.mysql.cj.api.jdbc.result.ResultSetInternalMethods executeInternal(int maxRowsToRetrieve, PacketPayload sendPacket,
            boolean createStreamingResultSet, boolean queryIsSelectOnly, ColumnDefinition metadata, boolean isBatch) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            this.numberOfExecutions++;

            // We defer to server-side execution
            try {
                return serverExecute(maxRowsToRetrieve, createStreamingResultSet, metadata);
            } catch (SQLException sqlEx) {
                // don't wrap SQLExceptions
                if (this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enablePacketDebug).getValue()) {
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
                if (this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_enablePacketDebug).getValue()) {
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

    @Override
    protected PacketPayload fillSendPacket() throws SQLException {
        return null; // we don't use this type of packet
    }

    @Override
    protected PacketPayload fillSendPacket(QueryBindings bindings) throws SQLException {
        return null; // we don't use this type of packet
    }

    /**
     * Returns the structure representing the value that (can be)/(is)
     * bound at the given parameter index.
     * 
     * @param parameterIndex
     *            1-based
     * @param forLongData
     *            is this for a stream?
     * @throws SQLException
     */
    protected ServerPreparedQueryBindValue getBinding(int parameterIndex, boolean forLongData) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            ServerPreparedQueryBindValue[] parameterBindings = ((ServerPreparedQuery) this.query).getQueryBindings().getBindValues();

            if (parameterBindings.length == 0) {
                throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.8"), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }

            parameterIndex--;

            if ((parameterIndex < 0) || (parameterIndex >= parameterBindings.length)) {
                throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.9") + (parameterIndex + 1)
                        + Messages.getString("ServerPreparedStatement.10") + parameterBindings.length, MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);
            }

            if (parameterBindings[parameterIndex] == null) {
                parameterBindings[parameterIndex] = new ServerPreparedQueryBindValue();
            } else {
                if (parameterBindings[parameterIndex].isLongData && !forLongData) {
                    this.detectedLongParameterSwitch = true;
                }
            }

            return parameterBindings[parameterIndex];
        }
    }

    @Override
    public java.sql.ResultSetMetaData getMetaData() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            ColumnDefinition resultFields = ((ServerPreparedQuery) this.query).getResultFields();
            if (resultFields == null || resultFields.getFields() == null) {
                return null;
            }

            return new ResultSetMetaData(this.session, resultFields.getFields(),
                    this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_useOldAliasMetadataBehavior).getValue(),
                    this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_yearIsDateType).getValue(), this.exceptionInterceptor);
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

    /**
     * Closes this connection and frees all resources.
     * 
     * @param calledExplicitly
     *            was this called from close()?
     * @param closeOpenResults
     *            should open result sets be closed?
     * 
     * @throws SQLException
     *             if an error occurs
     */
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

                if (calledExplicitly && !this.connection.isClosed()) {
                    synchronized (this.connection.getConnectionMutex()) {
                        try {

                            this.session.sendCommand(this.commandBuilder.buildComStmtClose(this.session.getSharedSendPacket(),
                                    ((ServerPreparedQuery) this.query).getServerStatementId()), true, 0);
                        } catch (CJException sqlEx) {
                            exceptionDuringClose = sqlEx;
                        }
                    }
                }

                if (this.isCached) {
                    this.connection.decachePreparedStatement(this);
                    this.isCached = false;
                }
                super.realClose(calledExplicitly, closeOpenResults);

                this.detectedLongParameterSwitch = ((ServerPreparedQuery) this.query).clearParametersInternal(false);

                closeQuery();

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
     * @param createStreamingResultSet
     * @param metadata
     *            use this metadata instead of the one provided on wire
     * 
     * @throws SQLException
     */
    protected ResultSetInternalMethods serverExecute(int maxRowsToRetrieve, boolean createStreamingResultSet, ColumnDefinition metadata) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            ServerPreparedQuery q = (ServerPreparedQuery) this.query;

            q.serverExecute(maxRowsToRetrieve, createStreamingResultSet, metadata);

            if (this.session.shouldIntercept()) {
                ResultSetInternalMethods interceptedResults = this.session.invokeQueryInterceptorsPre(() -> {
                    return q.getOriginalSql();
                }, this, true);

                if (interceptedResults != null) {
                    return interceptedResults;
                }
            }

            ServerPreparedQueryBindValue[] parameterBindings = ((ServerPreparedQuery) this.query).getQueryBindings().getBindValues();

            if (this.detectedLongParameterSwitch) {
                // Check when values were bound
                boolean firstFound = false;
                long boundTimeToCheck = 0;

                for (int i = 0; i < q.getParameterCount() - 1; i++) {
                    if (parameterBindings[i].isLongData) {
                        if (firstFound && boundTimeToCheck != parameterBindings[i].boundBeforeExecutionNum) {
                            throw SQLError.createSQLException(
                                    Messages.getString("ServerPreparedStatement.11") + Messages.getString("ServerPreparedStatement.12"),
                                    MysqlErrorNumbers.SQL_STATE_DRIVER_NOT_CAPABLE, this.exceptionInterceptor);
                        }
                        firstFound = true;
                        boundTimeToCheck = parameterBindings[i].boundBeforeExecutionNum;
                    }
                }

                // Okay, we've got all "newly"-bound streams, so reset server-side state to clear out previous bindings

                ((ServerPreparedQuery) this.query).serverResetStatement();
            }

            // Check bindings
            for (int i = 0; i < q.getParameterCount(); i++) {
                if (!parameterBindings[i].isSet()) {
                    throw SQLError.createSQLException(
                            Messages.getString("ServerPreparedStatement.13") + (i + 1) + Messages.getString("ServerPreparedStatement.14"),
                            MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
                }
            }

            //
            // Send all long data
            //
            for (int i = 0; i < q.getParameterCount(); i++) {
                if (parameterBindings[i].isLongData) {
                    serverLongData(i, parameterBindings[i]);
                }
            }

            //
            // store the parameter values
            //

            PacketPayload packet = this.session.getSharedSendPacket();
            packet.writeInteger(IntegerDataType.INT1, MysqlaConstants.COM_STMT_EXECUTE);
            packet.writeInteger(IntegerDataType.INT4, ((ServerPreparedQuery) this.query).getServerStatementId());

            // we only create cursor-backed result sets if
            // a) The query is a SELECT
            // b) The server supports it
            // c) We know it is forward-only (note this doesn't preclude updatable result sets)
            // d) The user has set a fetch size
            ColumnDefinition resultFields = ((ServerPreparedQuery) this.query).getResultFields();
            if (resultFields != null && resultFields.getFields() != null && this.useCursorFetch && getResultSetType() == ResultSet.TYPE_FORWARD_ONLY
                    && getFetchSize() > 0) {
                packet.writeInteger(IntegerDataType.INT1, OPEN_CURSOR_FLAG);
            } else {
                packet.writeInteger(IntegerDataType.INT1, 0); // placeholder for flags
            }

            packet.writeInteger(IntegerDataType.INT4, 1); // placeholder for parameter iterations

            /* Reserve place for null-marker bytes */
            int nullCount = (q.getParameterCount() + 7) / 8;

            int nullBitsPosition = packet.getPosition();

            for (int i = 0; i < nullCount; i++) {
                packet.writeInteger(IntegerDataType.INT1, 0);
            }

            byte[] nullBitsBuffer = new byte[nullCount];

            /* In case if buffers (type) altered, indicate to server */
            packet.writeInteger(IntegerDataType.INT1, this.sendTypesToServer.get() ? (byte) 1 : (byte) 0);

            if (this.sendTypesToServer.get()) {
                /*
                 * Store types of parameters in first in first package that is sent to the server.
                 */
                for (int i = 0; i < q.getParameterCount(); i++) {
                    packet.writeInteger(IntegerDataType.INT2, parameterBindings[i].bufferType);
                }
            }

            //
            // store the parameter values
            //
            for (int i = 0; i < q.getParameterCount(); i++) {
                if (!parameterBindings[i].isLongData) {
                    if (!parameterBindings[i].isNull()) {
                        storeBinding(packet, parameterBindings[i]);
                    } else {
                        nullBitsBuffer[i / 8] |= (1 << (i & 7));
                    }
                }
            }

            //
            // Go back and write the NULL flags to the beginning of the packet
            //
            int endPosition = packet.getPosition();
            packet.setPosition(nullBitsPosition);
            packet.writeBytes(StringLengthDataType.STRING_FIXED, nullBitsBuffer);
            packet.setPosition(endPosition);

            long begin = 0;

            boolean gatherPerformanceMetrics = this.gatherPerfMetrics.getValue();

            if (this.profileSQL || this.logSlowQueries || gatherPerformanceMetrics) {
                begin = this.session.getCurrentTimeNanosOrMillis();
            }

            resetCancelledState();

            CancelQueryTask timeoutTask = null;

            try {
                // Get this before executing to avoid a shared packet pollution in the case some other query is issued internally, such as when using I_S.
                String queryAsString = "";
                if (this.profileSQL || this.logSlowQueries || gatherPerformanceMetrics) {
                    queryAsString = asSql(true);
                }

                timeoutTask = startQueryTimer(this, this.timeoutInMillis);

                statementBegins();

                PacketPayload resultPacket = this.session.sendCommand(packet, false, 0);

                long queryEndTime = 0L;

                if (this.logSlowQueries || gatherPerformanceMetrics || this.profileSQL) {
                    queryEndTime = this.session.getCurrentTimeNanosOrMillis();
                }

                if (timeoutTask != null) {
                    stopQueryTimer(timeoutTask, true, true);
                    timeoutTask = null;
                }

                boolean queryWasSlow = false;

                if (this.logSlowQueries || gatherPerformanceMetrics) {
                    long elapsedTime = queryEndTime - begin;

                    if (this.logSlowQueries) {
                        if (this.useAutoSlowLog) {
                            queryWasSlow = elapsedTime > this.slowQueryThresholdMillis.getValue();
                        } else {
                            queryWasSlow = this.session.getProtocol().getMetricsHolder().isAbonormallyLongQuery(elapsedTime);

                            this.session.getProtocol().getMetricsHolder().reportQueryTime(elapsedTime);
                        }
                    }

                    if (queryWasSlow) {

                        StringBuilder mesgBuf = new StringBuilder(48 + q.getOriginalSql().length());
                        mesgBuf.append(Messages.getString("ServerPreparedStatement.15"));
                        mesgBuf.append(this.session.getSlowQueryThreshold());
                        mesgBuf.append(Messages.getString("ServerPreparedStatement.15a"));
                        mesgBuf.append(elapsedTime);
                        mesgBuf.append(Messages.getString("ServerPreparedStatement.16"));

                        mesgBuf.append("as prepared: ");
                        mesgBuf.append(q.getOriginalSql());
                        mesgBuf.append("\n\n with parameters bound:\n\n");
                        mesgBuf.append(queryAsString);

                        this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_SLOW_QUERY, "", this.getCurrentCatalog(),
                                this.session.getThreadId(), getId(), 0, System.currentTimeMillis(), elapsedTime, this.session.getQueryTimingUnits(), null,
                                LogUtils.findCallingClassAndMethod(new Throwable()), mesgBuf.toString()));
                    }

                    if (gatherPerformanceMetrics) {
                        this.session.registerQueryExecutionTime(elapsedTime);
                    }
                }

                this.session.incrementNumberOfPreparedExecutes();

                if (this.profileSQL) {
                    this.eventSink = ProfilerEventHandlerFactory.getInstance(this.session);

                    this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_EXECUTE, "", this.getCurrentCatalog(), this.connectionId,
                            this.query.getId(), -1, System.currentTimeMillis(), this.session.getCurrentTimeNanosOrMillis() - begin,
                            this.session.getQueryTimingUnits(), null, LogUtils.findCallingClassAndMethod(new Throwable()), truncateQueryToLog(queryAsString)));
                }

                com.mysql.cj.api.jdbc.result.ResultSetInternalMethods rs = this.session.getProtocol().readAllResults(maxRowsToRetrieve,
                        createStreamingResultSet, resultPacket, true, metadata != null ? metadata : resultFields, this.resultSetFactory);

                if (this.session.shouldIntercept()) {
                    ResultSetInternalMethods interceptedResults = this.session.invokeQueryInterceptorsPost(() -> {
                        return q.getOriginalSql();
                    }, this, rs, true);

                    if (interceptedResults != null) {
                        rs = interceptedResults;
                    }
                }

                if (this.profileSQL) {
                    long fetchEndTime = this.session.getCurrentTimeNanosOrMillis();

                    this.eventSink.consumeEvent(new ProfilerEventImpl(ProfilerEvent.TYPE_FETCH, "", this.getCurrentCatalog(),
                            this.connection.getSession().getThreadId(), getId(), rs.getResultId(), System.currentTimeMillis(), (fetchEndTime - queryEndTime),
                            this.session.getQueryTimingUnits(), null, LogUtils.findCallingClassAndMethod(new Throwable()), null));
                }

                if (queryWasSlow && this.explainSlowQueries.getValue()) {
                    this.session.getProtocol().explainSlowQuery(queryAsString, queryAsString);
                }

                this.sendTypesToServer.set(false);
                this.results = rs;

                if (this.session.hadWarnings()) {
                    this.session.getProtocol().scanForAndThrowDataTruncation();
                }

                return rs;
            } catch (IOException ioEx) {
                throw SQLError.createCommunicationsException(this.connection, this.session.getProtocol().getPacketSentTimeHolder().getLastPacketSentTime(),
                        this.session.getProtocol().getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx, this.exceptionInterceptor);
            } catch (SQLException | CJException sqlEx) {
                if (this.session.shouldIntercept()) {
                    this.session.invokeQueryInterceptorsPost(() -> {
                        return q.getOriginalSql();
                    }, this, null, true);
                }

                throw sqlEx;
            } finally {
                this.statementExecuting.set(false);
                stopQueryTimer(timeoutTask, false, false);
            }
        }
    }

    /**
     * Sends stream-type data parameters to the server.
     * 
     * <pre>
     *  Long data handling:
     * 
     *  - Server gets the long data in pieces with command type 'COM_LONG_DATA'.
     *  - The packet recieved will have the format as:
     *    [COM_LONG_DATA:     1][STMT_ID:4][parameter_number:2][type:2][data]
     *  - Checks if the type is specified by client, and if yes reads the type,
     *    and  stores the data in that format.
     *  - It's up to the client to check for read data ended. The server doesn't
     *    care;  and also server doesn't notify to the client that it got the
     *    data  or not; if there is any error; then during execute; the error
     *    will  be returned
     * </pre>
     * 
     * @param parameterIndex
     * @param longData
     * 
     * @throws SQLException
     *             if an error occurs.
     */
    private void serverLongData(int parameterIndex, ServerPreparedQueryBindValue longData) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            PacketPayload packet = this.session.getSharedSendPacket();

            Object value = longData.value;

            if (value instanceof byte[]) {
                this.session.sendCommand(this.commandBuilder.buildComStmtSendLongData(packet, ((ServerPreparedQuery) this.query).getServerStatementId(),
                        parameterIndex, (byte[]) value), true, 0);
            } else if (value instanceof InputStream) {
                ((ServerPreparedQuery) this.query).storeStream(parameterIndex, packet, (InputStream) value);
            } else if (value instanceof java.sql.Blob) {
                ((ServerPreparedQuery) this.query).storeStream(parameterIndex, packet, ((java.sql.Blob) value).getBinaryStream());
            } else if (value instanceof Reader) {
                ((ServerPreparedQuery) this.query).storeReader(parameterIndex, packet, (Reader) value);
            } else {
                throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.18") + value.getClass().getName() + "'",
                        MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT, this.exceptionInterceptor);
            }
        }
    }

    protected void serverPrepare(String sql) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {

                ServerPreparedQuery q = (ServerPreparedQuery) this.query;
                q.serverPrepare(sql);
            } catch (IOException ioEx) {
                throw SQLError.createCommunicationsException(this.connection, this.session.getProtocol().getPacketSentTimeHolder().getLastPacketSentTime(),
                        this.session.getProtocol().getPacketReceivedTimeHolder().getLastPacketReceivedTime(), ioEx, this.exceptionInterceptor);
            } catch (CJException sqlEx) {
                SQLException ex = SQLExceptionsMapping.translateException(sqlEx);

                if (this.dumpQueriesOnException.getValue()) {
                    StringBuilder messageBuf = new StringBuilder(((PreparedQuery) this.query).getOriginalSql().length() + 32);
                    messageBuf.append("\n\nQuery being prepared when exception was thrown:\n\n");
                    messageBuf.append(((PreparedQuery) this.query).getOriginalSql());

                    ex = appendMessageToException(ex, messageBuf.toString(), this.exceptionInterceptor);
                }

                throw ex;
            } finally {
                // Leave the I/O channel in a known state...there might be packets out there that we're not interested in
                this.session.clearInputStream();
            }
        }
    }

    private String truncateQueryToLog(String sql) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            String queryStr = null;

            int maxQuerySizeToLog = this.session.getPropertySet().getIntegerReadableProperty(PropertyDefinitions.PNAME_maxQuerySizeToLog).getValue();
            if (sql.length() > maxQuerySizeToLog) {
                StringBuilder queryBuf = new StringBuilder(maxQuerySizeToLog + 12);
                queryBuf.append(sql.substring(0, maxQuerySizeToLog));
                queryBuf.append(Messages.getString("MysqlIO.25"));

                queryStr = queryBuf.toString();
            } else {
                queryStr = sql;
            }

            return queryStr;
        }
    }

    @Override
    public void setArray(int i, Array x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (x == null) {
                setNull(parameterIndex, MysqlType.BINARY);
            } else {
                ServerPreparedQueryBindValue binding = getBinding(parameterIndex, true);
                this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_BLOB, this.numberOfExecutions));
                binding.value = x;
                binding.isLongData = true;
                binding.bindLength = this.useStreamLengthsInPrepStmts.getValue() ? length : -1;
            }
        }
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (x == null) {
                setNull(parameterIndex, MysqlType.DECIMAL);
            } else {

                ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
                this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_NEWDECIMAL, this.numberOfExecutions));
                binding.value = StringUtils.fixDecimalExponent(x.toPlainString());
            }
        }
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (x == null) {
                setNull(parameterIndex, MysqlType.BINARY);
            } else {
                ServerPreparedQueryBindValue binding = getBinding(parameterIndex, true);
                this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_BLOB, this.numberOfExecutions));
                binding.value = x;
                binding.isLongData = true;
                binding.bindLength = this.useStreamLengthsInPrepStmts.getValue() ? length : -1;
            }
        }
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (x == null) {
                setNull(parameterIndex, MysqlType.BINARY);
            } else {
                ServerPreparedQueryBindValue binding = getBinding(parameterIndex, true);
                this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_BLOB, this.numberOfExecutions));
                binding.value = x;
                binding.isLongData = true;
                binding.bindLength = this.useStreamLengthsInPrepStmts.getValue() ? x.length() : -1;
            }
        }
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setByte(parameterIndex, (x ? (byte) 1 : (byte) 0));
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        checkClosed();

        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_TINY, this.numberOfExecutions));
        binding.longBinding = x;
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        checkClosed();

        if (x == null) {
            setNull(parameterIndex, MysqlType.BINARY);
        } else {
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_VAR_STRING, this.numberOfExecutions));
            binding.value = x;
        }
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x, boolean checkForIntroducer, boolean escapeForMBChars) throws SQLException {
        setBytes(parameterIndex, x);
    }

    @Override
    public void setBytesNoEscapeNoQuotes(int parameterIndex, byte[] parameterAsBytes) throws SQLException {
        setBytes(parameterIndex, parameterAsBytes);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (reader == null) {
                setNull(parameterIndex, MysqlType.BINARY);
            } else {
                ServerPreparedQueryBindValue binding = getBinding(parameterIndex, true);
                this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_BLOB, this.numberOfExecutions));
                binding.value = reader;
                binding.isLongData = true;
                binding.bindLength = this.useStreamLengthsInPrepStmts.getValue() ? length : -1;
            }
        }
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (x == null) {
                setNull(parameterIndex, MysqlType.BINARY);
            } else {
                ServerPreparedQueryBindValue binding = getBinding(parameterIndex, true);
                this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_BLOB, this.numberOfExecutions));
                binding.value = x.getCharacterStream();
                binding.isLongData = true;
                binding.bindLength = this.useStreamLengthsInPrepStmts.getValue() ? x.length() : -1;
            }
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setDateInternal(parameterIndex, x, this.session.getDefaultTimeZone());
        }
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setDateInternal(parameterIndex, x, cal.getTimeZone());
        }
    }

    private void setDateInternal(int parameterIndex, Date x, TimeZone tz) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, MysqlType.DATE);
        } else {
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_DATE, this.numberOfExecutions));
            binding.value = x;
            binding.tz = tz;
        }
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {

            if (!this.session.getPropertySet().getBooleanReadableProperty(PropertyDefinitions.PNAME_allowNanAndInf).getValue()
                    && (x == Double.POSITIVE_INFINITY || x == Double.NEGATIVE_INFINITY || Double.isNaN(x))) {
                throw SQLError.createSQLException(Messages.getString("PreparedStatement.64", new Object[] { x }), MysqlErrorNumbers.SQL_STATE_ILLEGAL_ARGUMENT,
                        this.exceptionInterceptor);

            }

            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_DOUBLE, this.numberOfExecutions));
            binding.doubleBinding = x;
        }
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        checkClosed();

        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_FLOAT, this.numberOfExecutions));
        binding.floatBinding = x;
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();

        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_LONG, this.numberOfExecutions));
        binding.longBinding = x;
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        checkClosed();

        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_LONGLONG, this.numberOfExecutions));
        binding.longBinding = x;
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();

        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_NULL, this.numberOfExecutions));
        binding.setNull(true);
        binding.setParameterType(MysqlType.NULL);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        checkClosed();

        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_NULL, this.numberOfExecutions));
        binding.setNull(true);
        binding.setParameterType(MysqlType.NULL);
    }

    @Override
    public void setRef(int i, Ref x) throws SQLException {
        throw SQLError.createSQLFeatureNotSupportedException();
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        checkClosed();

        ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
        this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_SHORT, this.numberOfExecutions));
        binding.longBinding = x;
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();

        if (x == null) {
            setNull(parameterIndex, MysqlType.VARCHAR);
        } else {
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_VAR_STRING, this.numberOfExecutions));
            binding.value = x;
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimeInternal(parameterIndex, x, this.session.getDefaultTimeZone());
        }
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimeInternal(parameterIndex, x, cal.getTimeZone());
        }
    }

    private void setTimeInternal(int parameterIndex, Time x, TimeZone tz) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, MysqlType.TIME);
        } else {
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_TIME, this.numberOfExecutions));
            binding.value = x;
            binding.tz = tz;
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimestampInternal(parameterIndex, x, this.session.getDefaultTimeZone());
        }
    }

    @Override
    public void setTimestamp(int parameterIndex, java.sql.Timestamp x, Calendar cal) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            setTimestampInternal(parameterIndex, x, cal.getTimeZone());
        }
    }

    private void setTimestampInternal(int parameterIndex, java.sql.Timestamp x, TimeZone tz) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, MysqlType.TIMESTAMP);
        } else {
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, false);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_DATETIME, this.numberOfExecutions));

            if (!this.sendFractionalSeconds.getValue()) {
                x = TimeUtil.truncateFractionalSeconds(x);
            }

            binding.value = x;
            binding.tz = tz;
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

    /**
     * Method storeBinding.
     * 
     * @param packet
     * @param bindValue
     * @param mysql
     * 
     * @throws SQLException
     */
    private void storeBinding(PacketPayload packet, ServerPreparedQueryBindValue bindValue) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {
                Object value = bindValue.value;

                //
                // Handle primitives first
                //
                switch (bindValue.bufferType) {

                    case MysqlaConstants.FIELD_TYPE_TINY:
                        packet.writeInteger(IntegerDataType.INT1, bindValue.longBinding);
                        return;
                    case MysqlaConstants.FIELD_TYPE_SHORT:
                        packet.writeInteger(IntegerDataType.INT2, bindValue.longBinding);
                        return;
                    case MysqlaConstants.FIELD_TYPE_LONG:
                        packet.writeInteger(IntegerDataType.INT4, bindValue.longBinding);
                        return;
                    case MysqlaConstants.FIELD_TYPE_LONGLONG:
                        packet.writeInteger(IntegerDataType.INT8, bindValue.longBinding);
                        return;
                    case MysqlaConstants.FIELD_TYPE_FLOAT:
                        packet.writeInteger(IntegerDataType.INT4, Float.floatToIntBits(bindValue.floatBinding));
                        return;
                    case MysqlaConstants.FIELD_TYPE_DOUBLE:
                        packet.writeInteger(IntegerDataType.INT8, Double.doubleToLongBits(bindValue.doubleBinding));
                        return;
                    case MysqlaConstants.FIELD_TYPE_TIME:
                        storeTime(packet, (Time) value, bindValue.tz);
                        return;
                    case MysqlaConstants.FIELD_TYPE_DATE:
                    case MysqlaConstants.FIELD_TYPE_DATETIME:
                    case MysqlaConstants.FIELD_TYPE_TIMESTAMP:
                        storeDateTime(packet, (java.util.Date) value, bindValue.tz, bindValue.bufferType);
                        return;
                    case MysqlaConstants.FIELD_TYPE_VAR_STRING:
                    case MysqlaConstants.FIELD_TYPE_STRING:
                    case MysqlaConstants.FIELD_TYPE_VARCHAR:
                    case MysqlaConstants.FIELD_TYPE_DECIMAL:
                    case MysqlaConstants.FIELD_TYPE_NEWDECIMAL:
                        if (value instanceof byte[]) {
                            packet.writeBytes(StringSelfDataType.STRING_LENENC, (byte[]) value);
                        } else if (!((ClientPreparedQuery) this.query).isLoadDataQuery()) {
                            packet.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes((String) value, this.charEncoding));
                        } else {
                            packet.writeBytes(StringSelfDataType.STRING_LENENC, StringUtils.getBytes((String) value));
                        }

                        return;
                }

            } catch (SQLException | CJException uEE) {
                throw SQLError.createSQLException(
                        Messages.getString("ServerPreparedStatement.22")
                                + this.session.getPropertySet().getStringReadableProperty(PropertyDefinitions.PNAME_characterEncoding).getValue() + "'",
                        MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR, uEE, this.exceptionInterceptor);
            }
        }
    }

    private void storeTime(PacketPayload intoBuf, Time tm, TimeZone tz) throws SQLException {

        intoBuf.ensureCapacity(9);
        intoBuf.writeInteger(IntegerDataType.INT1, 8); // length
        intoBuf.writeInteger(IntegerDataType.INT1, 0); // neg flag
        intoBuf.writeInteger(IntegerDataType.INT4, 0); // tm->day, not used

        Calendar cal = Calendar.getInstance(tz);

        cal.setTime(tm);
        intoBuf.writeInteger(IntegerDataType.INT1, cal.get(Calendar.HOUR_OF_DAY));
        intoBuf.writeInteger(IntegerDataType.INT1, cal.get(Calendar.MINUTE));
        intoBuf.writeInteger(IntegerDataType.INT1, cal.get(Calendar.SECOND));
    }

    /**
     * @param intoBuf
     * @param dt
     * @param mysql
     * @param bufferType
     * @throws SQLException
     */
    private void storeDateTime(PacketPayload intoBuf, java.util.Date dt, TimeZone tz, int bufferType) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            Calendar cal = Calendar.getInstance(tz);

            cal.setTime(dt);

            if (dt instanceof java.sql.Date) {
                cal.set(Calendar.HOUR_OF_DAY, 0);
                cal.set(Calendar.MINUTE, 0);
                cal.set(Calendar.SECOND, 0);
            }

            byte length = (byte) 7;

            if (dt instanceof java.sql.Timestamp) {
                length = (byte) 11;
            }

            intoBuf.ensureCapacity(length);

            intoBuf.writeInteger(IntegerDataType.INT1, length); // length

            int year = cal.get(Calendar.YEAR);
            int month = cal.get(Calendar.MONTH) + 1;
            int date = cal.get(Calendar.DAY_OF_MONTH);

            intoBuf.writeInteger(IntegerDataType.INT2, year);
            intoBuf.writeInteger(IntegerDataType.INT1, month);
            intoBuf.writeInteger(IntegerDataType.INT1, date);

            if (dt instanceof java.sql.Date) {
                intoBuf.writeInteger(IntegerDataType.INT1, 0);
                intoBuf.writeInteger(IntegerDataType.INT1, 0);
                intoBuf.writeInteger(IntegerDataType.INT1, 0);
            } else {
                intoBuf.writeInteger(IntegerDataType.INT1, cal.get(Calendar.HOUR_OF_DAY));
                intoBuf.writeInteger(IntegerDataType.INT1, cal.get(Calendar.MINUTE));
                intoBuf.writeInteger(IntegerDataType.INT1, cal.get(Calendar.SECOND));
            }

            if (length == 11) {
                //  MySQL expects microseconds, not nanos
                intoBuf.writeInteger(IntegerDataType.INT4, ((java.sql.Timestamp) dt).getNanos() / 1000);
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder toStringBuf = new StringBuilder();

        toStringBuf.append("com.mysql.cj.jdbc.ServerPreparedStatement[");
        toStringBuf.append(((ServerPreparedQuery) this.query).getServerStatementId());
        toStringBuf.append("] - ");

        try {
            toStringBuf.append(asSql());
        } catch (SQLException sqlEx) {
            toStringBuf.append(Messages.getString("ServerPreparedStatement.6"));
            toStringBuf.append(sqlEx);
        }

        return toStringBuf.toString();
    }

    @Override
    public long getServerStatementId() {
        return ((ServerPreparedQuery) this.query).getServerStatementId();
    }

    private boolean hasCheckedRewrite = false;
    private boolean canRewrite = false;

    @Override
    public boolean canRewriteAsMultiValueInsertAtSqlLevel() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (!this.hasCheckedRewrite) {
                this.hasCheckedRewrite = true;
                this.canRewrite = canRewrite(((PreparedQuery) this.query).getOriginalSql(), isOnDuplicateKeyUpdate(), getLocationOfOnDuplicateKeyUpdate(), 0);
                // We need to client-side parse this to get the VALUES clause, etc.
                ((PreparedQuery) this.query).setParseInfo(new ParseInfo(((PreparedQuery) this.query).getOriginalSql(), this.session, this.charEncoding));
            }

            return this.canRewrite;
        }
    }

    public boolean canRewriteAsMultivalueInsertStatement() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (!canRewriteAsMultiValueInsertAtSqlLevel()) {
                return false;
            }

            ServerPreparedQueryBindValue[] currentBindValues = null;
            ServerPreparedQueryBindValue[] previousBindValues = null;

            int nbrCommands = this.query.getBatchedArgs().size();

            // Can't have type changes between sets of bindings for this to work...

            for (int commandIndex = 0; commandIndex < nbrCommands; commandIndex++) {
                Object arg = this.query.getBatchedArgs().get(commandIndex);

                if (!(arg instanceof String)) {

                    currentBindValues = ((QueryBindings) arg).getBindValues();

                    // We need to check types each time, as the user might have bound different types in each addBatch()

                    if (previousBindValues != null) {
                        ServerPreparedQueryBindValue[] parameterBindings = ((ServerPreparedQuery) this.query).getQueryBindings().getBindValues();
                        for (int j = 0; j < parameterBindings.length; j++) {
                            if (currentBindValues[j].bufferType != previousBindValues[j].bufferType) {
                                return false;
                            }
                        }
                    }
                }
            }

            return true;
        }
    }

    private int locationOfOnDuplicateKeyUpdate = -2;

    @Override
    protected int getLocationOfOnDuplicateKeyUpdate() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            if (this.locationOfOnDuplicateKeyUpdate == -2) {
                this.locationOfOnDuplicateKeyUpdate = ParseInfo.getOnDuplicateKeyLocation(((PreparedQuery) this.query).getOriginalSql(),
                        this.dontCheckOnDuplicateKeyUpdateInSQL, this.rewriteBatchedStatements.getValue(),
                        this.session.getServerSession().isNoBackslashEscapesSet());
            }

            return this.locationOfOnDuplicateKeyUpdate;
        }
    }

    protected boolean isOnDuplicateKeyUpdate() throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            return getLocationOfOnDuplicateKeyUpdate() != -1;
        }
    }

    @Override
    protected int setOneBatchedParameterSet(java.sql.PreparedStatement batchedStatement, int batchedParamIndex, Object paramSet) throws SQLException {
        ServerPreparedQueryBindValue[] paramArg = ((ServerPreparedQueryBindings) paramSet).getBindValues();

        for (int j = 0; j < paramArg.length; j++) {
            if (paramArg[j].isNull()) {
                batchedStatement.setNull(batchedParamIndex++, MysqlType.NULL.getJdbcType());
            } else {
                if (paramArg[j].isLongData) {
                    Object value = paramArg[j].value;

                    if (value instanceof InputStream) {
                        batchedStatement.setBinaryStream(batchedParamIndex++, (InputStream) value, (int) paramArg[j].bindLength);
                    } else {
                        batchedStatement.setCharacterStream(batchedParamIndex++, (Reader) value, (int) paramArg[j].bindLength);
                    }
                } else {

                    switch (paramArg[j].bufferType) {

                        case MysqlaConstants.FIELD_TYPE_TINY:
                            batchedStatement.setByte(batchedParamIndex++, (byte) paramArg[j].longBinding);
                            break;
                        case MysqlaConstants.FIELD_TYPE_SHORT:
                            batchedStatement.setShort(batchedParamIndex++, (short) paramArg[j].longBinding);
                            break;
                        case MysqlaConstants.FIELD_TYPE_LONG:
                            batchedStatement.setInt(batchedParamIndex++, (int) paramArg[j].longBinding);
                            break;
                        case MysqlaConstants.FIELD_TYPE_LONGLONG:
                            batchedStatement.setLong(batchedParamIndex++, paramArg[j].longBinding);
                            break;
                        case MysqlaConstants.FIELD_TYPE_FLOAT:
                            batchedStatement.setFloat(batchedParamIndex++, paramArg[j].floatBinding);
                            break;
                        case MysqlaConstants.FIELD_TYPE_DOUBLE:
                            batchedStatement.setDouble(batchedParamIndex++, paramArg[j].doubleBinding);
                            break;
                        case MysqlaConstants.FIELD_TYPE_TIME:
                            batchedStatement.setTime(batchedParamIndex++, (Time) paramArg[j].value);
                            break;
                        case MysqlaConstants.FIELD_TYPE_DATE:
                            batchedStatement.setDate(batchedParamIndex++, (Date) paramArg[j].value);
                            break;
                        case MysqlaConstants.FIELD_TYPE_DATETIME:
                        case MysqlaConstants.FIELD_TYPE_TIMESTAMP:
                            batchedStatement.setTimestamp(batchedParamIndex++, (Timestamp) paramArg[j].value);
                            break;
                        case MysqlaConstants.FIELD_TYPE_VAR_STRING:
                        case MysqlaConstants.FIELD_TYPE_STRING:
                        case MysqlaConstants.FIELD_TYPE_VARCHAR:
                        case MysqlaConstants.FIELD_TYPE_DECIMAL:
                        case MysqlaConstants.FIELD_TYPE_NEWDECIMAL:
                            Object value = paramArg[j].value;

                            if (value instanceof byte[]) {
                                batchedStatement.setBytes(batchedParamIndex, (byte[]) value);
                            } else {
                                batchedStatement.setString(batchedParamIndex, (String) value);
                            }

                            // If we ended up here as a multi-statement, we're not working with a server prepared statement

                            if (batchedStatement instanceof ServerPreparedStatement) {
                                ServerPreparedQueryBindValue asBound = ((ServerPreparedStatement) batchedStatement).getBinding(batchedParamIndex, false);
                                asBound.bufferType = paramArg[j].bufferType;
                            }

                            batchedParamIndex++;

                            break;
                        default:
                            throw new IllegalArgumentException(Messages.getString("ServerPreparedStatement.26", new Object[] { batchedParamIndex }));
                    }
                }
            }
        }

        return batchedParamIndex;
    }

    @Override
    protected boolean containsOnDuplicateKeyUpdateInSQL() {
        return this.hasOnDuplicateKeyUpdate;
    }

    @Override
    protected PreparedStatement prepareBatchedInsertSQL(JdbcConnection localConn, int numBatches) throws SQLException {
        synchronized (checkClosed().getConnectionMutex()) {
            try {
                //PreparedStatement pstmt = new ServerPreparedStatement(localConn, this.parseInfo.getSqlForBatch(numBatches), this.getCurrentCatalog(), this.resultSetConcurrency, this.resultSetType);
                PreparedStatement pstmt = ((Wrapper) localConn.prepareStatement(((PreparedQuery) this.query).getParseInfo().getSqlForBatch(numBatches),
                        this.resultSetConcurrency, this.resultSetType)).unwrap(PreparedStatement.class);
                pstmt.setRetrieveGeneratedKeys(this.retrieveGeneratedKeys);

                return pstmt;
            } catch (UnsupportedEncodingException e) {
                SQLException sqlEx = SQLError.createSQLException(Messages.getString("ServerPreparedStatement.27"), MysqlErrorNumbers.SQL_STATE_GENERAL_ERROR,
                        this.exceptionInterceptor);
                sqlEx.initCause(e);

                throw sqlEx;
            }
        }
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        // can't take if characterEncoding isn't utf8
        if (!this.charEncoding.equalsIgnoreCase("UTF-8") && !this.charEncoding.equalsIgnoreCase("utf8")) {
            throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.28"), this.exceptionInterceptor);
        }

        checkClosed();

        if (reader == null) {
            setNull(parameterIndex, MysqlType.BINARY);
        } else {
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, true);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_BLOB, this.numberOfExecutions));
            binding.value = reader;
            binding.isLongData = true;
            binding.bindLength = this.useStreamLengthsInPrepStmts.getValue() ? length : -1;
        }
    }

    @Override
    public void setNClob(int parameterIndex, NClob x) throws SQLException {
        setNClob(parameterIndex, x.getCharacterStream(), this.useStreamLengthsInPrepStmts.getValue() ? x.length() : -1);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        // can't take if characterEncoding isn't utf8
        if (!this.charEncoding.equalsIgnoreCase("UTF-8") && !this.charEncoding.equalsIgnoreCase("utf8")) {
            throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.29"), this.exceptionInterceptor);
        }

        checkClosed();

        if (reader == null) {
            setNull(parameterIndex, MysqlType.TEXT);
        } else {
            ServerPreparedQueryBindValue binding = getBinding(parameterIndex, true);
            this.sendTypesToServer.compareAndSet(false, binding.resetToType(MysqlaConstants.FIELD_TYPE_BLOB, this.numberOfExecutions));
            binding.value = reader;
            binding.isLongData = true;
            binding.bindLength = this.useStreamLengthsInPrepStmts.getValue() ? length : -1;
        }
    }

    @Override
    public void setNString(int parameterIndex, String x) throws SQLException {
        if (this.charEncoding.equalsIgnoreCase("UTF-8") || this.charEncoding.equalsIgnoreCase("utf8")) {
            setString(parameterIndex, x);
        } else {
            throw SQLError.createSQLException(Messages.getString("ServerPreparedStatement.30"), this.exceptionInterceptor);
        }
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        super.setSQLXML(parameterIndex, xmlObject);
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        if (!poolable) {
            this.connection.decachePreparedStatement(this);
        }
        super.setPoolable(poolable);
    }

}
